# 命令处理完整流程（强一致版）

## 核心原则

**响应客户端之前，事件必须已持久化到 Event Store。**

- 无异步 flush buffer
- 无批量延迟刷盘
- 每次命令处理完成后同步写入 DB，等待确认后才返回结果
- Actor 崩溃不会丢失任何已确认的命令

## 时序图

```
Client       GraphQL      Gateway        VirtualActorRuntime     Actor(聚合)       WASM Pool       EventStore
  │              │            │                  │                   │                 │                │
  │── Mutation ─▶│            │                  │                   │                 │                │
  │  createItem  │            │                  │                   │                 │                │
  │              │── cmd ────▶│                  │                   │                 │                │
  │              │ (含 command_type              │                   │                 │                │
  │              │  ="create-item")              │                   │                 │                │
  │              │            │─ bloom 快筛 ────▶│                   │                 │                │
  │              │            │  (命中则查DB)     │                   │                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │─ validate-X() [仅当存在] ───────────────────────────▶│                │
  │              │            │◀─ Ok ───────────────────────────────────────────────│                │
  │              │            │                  │                   │                 │                │
  │              │            │─ send(agg_id) ──▶│                   │                 │                │
  │              │            │                  │── 查找/激活 Actor ▶│                 │                │
  │              │            │                  │   (透明寻址)       │                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │─ handle-X(state) ────────────▶│
  │              │            │                  │                   │◀─ new_events ─────────────────│
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │── persist(同步等待，含事务级幂等) ──▶│
  │              │            │                  │                   │◀── ack(已落盘) ─────────────────│
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │── apply(自定义或默认)─┐          │
  │              │            │                  │                   │   state += events    │          │
  │              │            │                  │                   │◀─────────────────────┘          │
  │              │            │                  │                   │                 │                │
  │◀── Result ──│◀───────────│◀─────────────────│◀── Ok(version) ──│                 │                │
```

关键顺序：**validate-X（Gateway 前置，可选） → persist → ack → apply（自定义或默认策略） → 响应客户端**

- validate-X 在 Gateway 层前置执行（如果存在），不依赖聚合状态，冷聚合无需激活即可拒绝格式错误的请求
- 无 validate-X 时 Gateway 跳过校验，直接路由到 Actor
- apply 阶段：有自定义 apply-events 则调用 WASM 组件，无则使用 Host 默认策略（JSON 深度合并）
- 事件先落盘，再更新内存状态。即使 apply 过程中崩溃，重新激活时从 DB 重建即可恢复正确状态

## Command Gateway（入口层）

```rust
pub struct CommandGateway {
    runtime: Arc<VirtualActorRuntime>,
    event_store: Arc<dyn EventStore>,
    idempotency_bloom: RollingBloomFilter,
    wasm_pool: Arc<WasmPoolManager>,
    command_registry: CommandRegistry,  // 启动时缓存的命令定义
    shutting_down: Arc<AtomicBool>,     // 优雅关闭标志
}

impl CommandGateway {
    pub async fn execute(&self, command: IncomingCommand) -> Result<CommandResult> {
        // 0. 关闭检查：防止 HTTP listener 关闭前已进入 Gateway 的请求继续进入 Actor mailbox
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(Error::service_unavailable("服务正在关闭，请重试"));
        }

        // 1. 双层幂等检查（布隆过滤器 + Event Store 内事务级幂等）
        if self.idempotency_bloom.might_contain(&command.command_id) {
            if self.event_store.idempotency_exists(&command.command_id).await? {
                return Ok(CommandResult::duplicate());
            }
        }

        // 2. 前置 validate-X（仅当该命令有 validate 函数且尚未执行时）
        let cmd_def = self.command_registry.get(&command.module, &command.command_type);
        if !command.validated {
            if let Some(validate_fn) = cmd_def.and_then(|c| c.validate_fn.as_ref()) {
                let mut instance = self.wasm_pool.acquire(&command.module).await?;
                instance.call_validate(validate_fn, &command.data)?;
                drop(instance);
            }
        }
        // 无 validate 函数或已在源节点执行时：直接跳过，进入 Actor 处理

        // 3. 透明寻址：运行时自动激活/路由
        //    幂等键在 Event Store 同一事务中写入，无窗口丢失风险
        let result = self.runtime.send(&command.aggregate_id, command.clone()).await?;

        // 4. 写入布隆过滤器（仅加速后续查询，非正确性依赖）
        self.idempotency_bloom.insert(&command.command_id);

        Ok(result)
    }
}
```

## Virtual Actor Runtime（核心调度器）

```rust
use dashmap::DashMap;

pub struct VirtualActorRuntime {
    /// 活跃聚合：aggregate_id → Actor 句柄
    active: DashMap<String, ActorHandle>,
    /// 激活锁：防止同一聚合并发激活（per-key 粒度）
    activation_locks: DashMap<String, Arc<Mutex<()>>>,
    /// 全局递增的 Actor generation（用于防止驱逐竞态下误删新 Actor）
    next_generation: AtomicU64,
    /// 内存预算
    max_active: usize,
    /// 空闲超时
    idle_timeout: Duration,
    /// 驱逐保护期：Actor 至少空闲此时长才可被驱逐
    min_evict_idle: Duration,
    /// 基础设施
    event_store: Arc<dyn EventStore>,
    snapshot_store: Arc<dyn SnapshotStore>,
    wasm_pool: Arc<WasmPoolManager>,
}

impl VirtualActorRuntime {
    /// 透明寻址：调用方无需关心聚合是否在内存中
    pub async fn send(
        &self,
        aggregate_id: &str,
        command: IncomingCommand,
    ) -> Result<CommandResult> {
        let handle = self.get_or_activate(aggregate_id, &command.module).await?;
        handle.send(command).await
    }

    /// 获取已激活的 Actor，或按需激活（防止并发激活同一聚合）
    async fn get_or_activate(&self, aggregate_id: &str, module: &str) -> Result<ActorHandle> {
        // 快速路径：Actor 已在内存中
        if let Some(handle) = self.active.get(aggregate_id) {
            return Ok(handle.clone());
        }

        // 慢路径：获取 per-key 激活锁，防止同一聚合并发激活
        let lock = self.activation_locks
            .entry(aggregate_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        // Double-check：持锁后再次检查，可能已被其他线程激活
        if let Some(handle) = self.active.get(aggregate_id) {
            return Ok(handle.clone());
        }

        let handle = self.activate(aggregate_id, module).await?;
        // 注意：不在此处清理 activation_locks，防止竞态。
        // 场景：线程 A 完成激活并 remove 锁 → 线程 B 同时 entry().or_insert_with() 获取新锁
        // → 线程 C 也获取到不同的锁实例 → A、C 同时激活同一聚合，破坏互斥。
        // 锁条目的内存开销极小（每个 aggregate_id 仅一个 Arc<Mutex<()>>），
        // 由后台定时任务清理长期未使用的条目即可（见 run_lock_cleaner）。
        Ok(handle)
    }

    /// 激活聚合：从持久化状态恢复到内存
    async fn activate(&self, aggregate_id: &str, module: &str) -> Result<ActorHandle> {
        // 内存压力检查（最多尝试 max_evict_retries 次，防止死循环）
        let mut evict_attempts = 0;
        while self.active.len() >= self.max_active {
            if evict_attempts >= self.max_evict_retries {
                return Err(Error::overloaded(
                    "内存预算已满且无法驱逐，系统过载"
                ));
            }
            if !self.evict_one().await {
                // 无可驱逐候选者，直接返回过载错误
                return Err(Error::overloaded(
                    "所有 Actor 均有待处理消息或未达最小空闲时间，无法腾出空间"
                ));
            }
            evict_attempts += 1;
        }

        // 从快照+增量事件恢复状态
        let (state, version) = self.recover_state(aggregate_id, module).await?;

        // 创建 Virtual Actor
        let (handle, actor) = VirtualActor::new(
            aggregate_id.to_string(),
            module.to_string(),
            state,
            version,
            self.wasm_pool.clone(),
            self.event_store.clone(),
            self.snapshot_store.clone(),
            self.idle_timeout,
        );

        self.active.insert(aggregate_id.to_string(), handle.clone());

        let active_ref = self.active.clone();
        let agg_id = aggregate_id.to_string();
        let generation = handle.generation();
        tokio::spawn(async move {
            actor.run().await;
            // 仅移除自身 generation 的条目，防止驱逐竞态下误删新激活的 Actor
            active_ref.remove_if(&agg_id, |_, h| h.generation() == generation);
        });

        Ok(handle)
    }

    /// 从快照+增量事件恢复聚合状态
    async fn recover_state(&self, aggregate_id: &str, module: &str) -> Result<(Vec<u8>, u64)> {
        let (base_state, from_version) = match self.snapshot_store.load(module, aggregate_id).await? {
            Some(snap) => (snap.state, snap.version),
            None => (vec![], 0),
        };

        let events = self.event_store.load_events_after(aggregate_id, from_version).await?;
        let version = from_version + events.len() as u64;

        let state = if events.is_empty() {
            base_state
        } else {
            let event_data: Vec<&[u8]> = events.iter().map(|e| e.data.as_slice()).collect();
            if self.has_custom_apply_events(module) {
                // 有自定义 apply-events：调用 WASM 组件实现
                let mut instance = self.wasm_pool.acquire(module).await?;
                instance.call_apply_events(&base_state, &event_data)?
            } else {
                // 无 apply-events：使用 Host 默认策略（JSON 深度合并）
                let event_vecs: Vec<Vec<u8>> = events.iter().map(|e| e.data.clone()).collect();
                default_apply_events(&base_state, &event_vecs)?
            }
        };

        Ok((state, version))
    }

    /// LRU 驱逐（带保护：不驱逐有待处理消息或刚活跃的 Actor）
    /// 返回 true 表示成功驱逐了一个 Actor（已等待其退出），false 表示无可驱逐候选者
    ///
    /// 安全性说明：evict_one 发送 Deactivate 并通过 exit_rx 同步等待 Actor 退出。
    /// Actor 处理完 mailbox 中所有待处理命令后收到 Deactivate，保存快照并退出，
    /// 退出时通过 exit_tx 通知驱逐方，驱逐方确认后从 active 中移除。
    /// 同步等待确保 activate() 中的 active.len() 检查准确反映可用槽位。
    async fn evict_one(&self) -> bool {
        let now = now_millis();
        let min_idle_ms = self.min_evict_idle.as_millis() as u64;

        let candidate = self.active.iter()
            .filter(|entry| {
                let handle = entry.value();
                // 保护条件：正在处理消息或有待处理消息的 Actor 不可驱逐
                !handle.is_busy()
                // 保护条件：未达到最小空闲时间的 Actor 不可驱逐
                && (now - handle.last_active()) > min_idle_ms
            })
            .min_by_key(|entry| entry.value().last_active())
            .map(|entry| entry.key().clone());

        if let Some(id) = candidate {
            if let Some(handle) = self.active.get(&id) {
                // 发送 Deactivate 并同步等待 Actor 退出
                let exit_rx = handle.send_deactivate_and_wait().await;
                // 等待 Actor 完成快照保存并退出（带超时保护）
                let _ = tokio::time::timeout(
                    Duration::from_secs(5),
                    exit_rx,
                ).await;
            }
            // Actor 已退出，从 active 中移除
            self.active.remove(&id);
            true
        } else {
            false
        }
    }
}
```

## Virtual Actor（强一致版）

```rust
pub struct VirtualActor {
    aggregate_id: String,
    module_name: String,
    state: Vec<u8>,             // 内存中的当前状态（始终与 DB 一致）
    version: u64,               // 已持久化的版本号
    has_custom_apply: bool,     // 是否有自定义 apply-events 实现
    fencing_token: Option<u64>, // 集群模式 Some(token)，单机模式 None（见 cluster.md）
    wasm_pool: Arc<WasmPoolManager>,
    event_store: Arc<dyn EventStore>,
    snapshot_store: Arc<dyn SnapshotStore>,
    mailbox: mpsc::Receiver<ActorMessage>,
    handle: ActorHandle,        // 持有自身 handle 引用，用于统一更新 last_active
    idle_timeout: Duration,
    last_snapshot_version: Arc<AtomicU64>,  // 与 spawn 任务共享，失败时可回退
    snapshot_threshold: u64,
}

impl VirtualActor {
    /// Actor 主循环
    pub async fn run(mut self) {
        let mut idle_check = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                Some(msg) = self.mailbox.recv() => {
                    // 统一通过 handle 更新 last_active，驱逐器和自身空闲检测共用同一时钟
                    self.handle.touch();
                    match msg {
                        ActorMessage::Command { command, reply_tx } => {
                            self.handle.set_processing(true);
                            let result = self.process_command(command).await;
                            self.handle.set_processing(false);
                            let _ = reply_tx.send(result);
                        }
                        ActorMessage::Deactivate => {
                            self.on_deactivate().await;
                            self.handle.notify_exit().await;
                            return;
                        }
                    }
                }
                _ = idle_check.tick() => {
                    let idle_ms = now_millis() - self.handle.last_active();
                    if idle_ms > self.idle_timeout.as_millis() as u64 {
                        self.on_deactivate().await;
                        self.handle.notify_exit().await;
                        return;
                    }
                }
            }
        }
    }

    /// 处理单个命令（强一致：持久化后才响应）
    async fn process_command(&mut self, cmd: IncomingCommand) -> Result<CommandResult> {
        // 注意：validate-X 已在 Gateway 层前置执行（如果存在），此处不再重复调用

        // 1. WASM handle-X（按命令类型路由，基于内存中的当前状态决策）
        let handle_fn = format!("handle-{}", cmd.command_type);
        let mut instance = self.wasm_pool.acquire(&self.module_name).await?;
        let new_events = instance.call_function(&handle_fn, &[self.state.clone(), cmd.data.clone()])?;
        drop(instance);

        // 1.5 No-op 处理：handle 返回空事件列表表示命令合法但无副作用
        //     不写幂等键、不写事件，直接返回成功。
        //     重试安全：状态不变 → handle 再次返回空事件 → 再次 no-op。
        //     前提：handle 必须是确定性的。
        if new_events.is_empty() {
            return Ok(CommandResult {
                success: true,
                version: self.version,
                event_count: 0,
                error: None,
            });
        }

        // 2. 同步持久化到 Event Store（事件 + 幂等键在同一事务中写入）
        let events_to_persist: Vec<PendingEvent> = new_events.iter().enumerate()
            .map(|(i, data)| PendingEvent {
                aggregate_id: self.aggregate_id.clone(),
                aggregate_type: self.module_name.clone(),
                event_type: extract_event_type(data),
                version: self.version + 1 + i as u64,
                data: data.clone(),
            }).collect();

        self.event_store.append_with_idempotency(
            &self.aggregate_id,
            &events_to_persist,
            self.version,
            &cmd.command_id,
            self.fencing_token,  // 单机模式 None，集群模式 Some(token)（见 cluster.md）
        ).await?;

        // 3. DB 已确认 → 安全更新内存状态
        let event_refs: Vec<&[u8]> = new_events.iter().map(|e| e.as_slice()).collect();
        self.state = if self.has_custom_apply {
            // 有自定义 apply-events：调用 WASM 组件实现
            let mut inst = self.wasm_pool.acquire(&self.module_name).await?;
            inst.call_apply_events(&self.state, &event_refs)?
        } else {
            // 无 apply-events：使用 Host 默认策略（JSON 深度合并）
            default_apply_events(&self.state, &new_events)?
        };
        self.version += new_events.len() as u64;

        // 4. 异步快照判断（快照丢失不影响正确性）
        self.maybe_snapshot();

        Ok(CommandResult {
            success: true,
            version: self.version,
            event_count: new_events.len(),
            error: None,
        })
    }

    /// 快照判断（异步，不阻塞响应）
    /// 使用 Arc<AtomicU64> 统一管理快照版本，spawn 任务失败时通过 CAS 安全回退
    fn maybe_snapshot(&self) {
        let last = self.last_snapshot_version.load(Ordering::Relaxed);
        let events_since = self.version - last;
        if events_since >= self.snapshot_threshold {
            // 乐观更新：先设置为当前版本，防止后续命令重复触发快照
            let snapshot_version = self.version;
            self.last_snapshot_version.store(snapshot_version, Ordering::Relaxed);
            let snapshot = Snapshot {
                aggregate_id: self.aggregate_id.clone(),
                aggregate_type: self.module_name.clone(),
                version: self.version,
                state: self.state.clone(),
                created_at: now_millis(),
            };
            let store = self.snapshot_store.clone();
            let last_snapshot_version = self.last_snapshot_version.clone();
            tokio::spawn(async move {
                if store.save(&snapshot).await.is_err() {
                    // 保存失败：仅当值未被后续快照更新时才回退（CAS 防止 ABA）
                    let _ = last_snapshot_version.compare_exchange(
                        snapshot_version,
                        last,  // 回退到本次快照之前的版本
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                }
            });
        }
    }

    /// 休眠：保存快照后退出（无需刷盘，所有事件已同步持久化）
    async fn on_deactivate(&self) {
        let snapshot = Snapshot {
            aggregate_id: self.aggregate_id.clone(),
            aggregate_type: self.module_name.clone(),
            version: self.version,
            state: self.state.clone(),
            created_at: now_millis(),
        };
        let _ = self.snapshot_store.save(&snapshot).await;
    }
}
```

## 关键设计：持久化在响应之前

```
强一致（当前方案）：
  handle → persist(等待DB确认) → 更新内存 → 响应客户端
  保证：客户端收到成功 = 事件已落盘 = 零数据丢失
```

## IncomingCommand 结构（统一定义）

```rust
/// 入站命令（所有文档中 IncomingCommand 的权威定义）
/// 其他文档引用此定义，不再重复。
pub struct IncomingCommand {
    pub command_id: String,      // 幂等键（全局唯一）
    pub aggregate_id: String,    // 聚合根 ID
    pub module: String,          // 模块名（如 "inventory"），由 GraphQL resolver 填充
    pub command_type: String,    // 命令类型 kebab-case（如 "create-item"）
    pub data: Vec<u8>,           // 序列化的命令参数
    pub validated: bool,         // 集群模式：true 表示 validate 已在源节点执行，
                                 // owner 节点跳过（详见 cluster.md）
                                 // 单机模式：始终为 false
    pub module_version: Option<String>,  // 集群模式：源节点的组件版本哈希，
                                         // 用于滚动升级时检测版本不一致（详见 cluster.md）
                                         // 单机模式：始终为 None
}
```

## Actor 句柄与消息

```rust
pub struct ActorHandle {
    tx: mpsc::Sender<ActorMessage>,
    last_active: Arc<AtomicU64>,
    processing: Arc<AtomicBool>,  // Actor 是否正在处理命令
    generation: u64,  // 唯一标识此 Actor 实例，防止驱逐竞态误删
    exit_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,  // 通知驱逐方 Actor 已退出
}

impl ActorHandle {
    pub async fn send(&self, command: IncomingCommand) -> Result<CommandResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.try_send(ActorMessage::Command { command, reply_tx })
            .map_err(|_| Error::overloaded("Actor 邮箱已满"))?;
        reply_rx.await.map_err(|_| Error::internal("Actor 无响应"))?
    }

    /// 发送 Deactivate 并返回等待 Actor 退出的 receiver
    pub async fn send_deactivate_and_wait(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        *self.exit_tx.lock().await = Some(tx);
        let _ = self.tx.send(ActorMessage::Deactivate).await;
        rx
    }

    pub fn last_active(&self) -> u64 {
        self.last_active.load(Ordering::Relaxed)
    }

    /// 由 Actor 在处理消息时调用，统一时钟源
    pub fn touch(&self) {
        self.last_active.store(now_millis(), Ordering::Relaxed);
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// 标记 Actor 开始/结束处理命令
    pub fn set_processing(&self, value: bool) {
        self.processing.store(value, Ordering::Release);
    }

    /// 综合判断 Actor 是否繁忙（正在处理 + mailbox 有排队消息）
    pub fn is_busy(&self) -> bool {
        self.processing.load(Ordering::Acquire)
        || self.tx.max_capacity() - self.tx.capacity() > 0
    }

    /// Actor 退出时调用，通知等待方
    pub async fn notify_exit(&self) {
        if let Some(tx) = self.exit_tx.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

enum ActorMessage {
    Command {
        command: IncomingCommand,
        reply_tx: oneshot::Sender<Result<CommandResult>>,
    },
    Deactivate,
}
```

## Virtual Actor 生命周期

```
                    ┌──────────┐
                    │ 不存在    │ (聚合从未被访问或已休眠)
                    └─────┬────┘
                          │ 首次命令到达
                          ▼
              ┌───────────────────────┐
              │ 激活中 (Activating)    │
              │ 加载快照 + 增量事件    │
              │ 调用 apply-events     │
              └───────────┬───────────┘
                          │ 恢复完成
                          ▼
              ┌───────────────────────┐
              │ 活跃 (Active)          │◀──── 收到命令
              │ handle → persist → ack │
              │ 内存状态始终 = DB 状态  │────── 处理完毕，等待下一个
              └───────────┬───────────┘
                          │ 空闲超时 / 内存压力驱逐
                          ▼
              ┌───────────────────────┐
              │ 休眠中 (Deactivating)  │
              │ 保存快照（无需刷盘）   │
              │ 释放内存               │
              └───────────┬───────────┘
                          │
                          ▼
                    ┌──────────┐
                    │ 不存在    │ (等待下次激活)
                    └──────────┘
```

## 内存管理

### 激活锁清理（防止锁表无限增长）

```rust
impl VirtualActorRuntime {
    /// 定期清理不再需要的激活锁条目
    /// 清理条件：锁当前未被持有（try_lock 成功说明无人等待激活）
    /// 无论 Actor 是否在 active 中，只要锁空闲即可安全清理：
    /// - Actor 在 active 中：激活已完成，锁使命结束
    /// - Actor 不在 active 中（已休眠）：锁同样无用，下次激活会重新创建
    async fn run_lock_cleaner(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        loop {
            interval.tick().await;
            let stale_keys: Vec<String> = self.activation_locks.iter()
                .filter(|entry| {
                    // 锁当前未被持有（无线程正在等待激活）
                    entry.value().try_lock().is_ok()
                })
                .map(|entry| entry.key().clone())
                .collect();

            for key in stale_keys {
                self.activation_locks.remove(&key);
            }
        }
    }
}
```

### LRU 驱逐

```rust
impl VirtualActorRuntime {
    async fn run_evictor(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let now = now_millis();
            let idle_ms = self.idle_timeout.as_millis() as u64;

            let to_evict: Vec<String> = self.active.iter()
                .filter(|entry| {
                    let handle = entry.value();
                    !handle.has_pending_messages()
                    && (now - handle.last_active()) > idle_ms
                })
                .map(|entry| entry.key().clone())
                .take(self.max_evict_per_tick)  // 每轮最多驱逐 N 个，防止雪崩
                .collect();

            for id in to_evict {
                // 仅发送 Deactivate，Actor 自行退出后从 active 移除
                if let Some(handle) = self.active.get(&id) {
                    handle.send_deactivate().await;
                }
            }
        }
    }
}
```

## 配置

```rust
pub struct VirtualActorConfig {
    pub max_active: usize,              // 最大活跃聚合数量
    pub idle_timeout: Duration,          // 空闲超时（默认 5 分钟）
    pub min_evict_idle: Duration,        // 驱逐保护期（默认 30 秒）
    pub mailbox_capacity: usize,         // Actor 邮箱容量（默认 64）
    pub snapshot_threshold: u64,         // 快照阈值（默认 100）
    pub max_evict_per_tick: usize,       // 每轮驱逐上限（默认 10）
    pub max_evict_retries: usize,        // 激活时驱逐最大重试次数（默认 3）
    pub shutdown_timeout: Duration,      // 优雅关闭超时（默认 30 秒）
}
```

## 错误处理

| 场景 | 处理方式 | 数据安全 |
|------|----------|----------|
| validate 失败（仅当存在时） | 返回 400，无副作用 | 安全 |
| handle 失败（业务拒绝） | 返回领域错误，状态不变 | 安全 |
| persist 失败（DB 不可用） | 返回 503，内存状态不变 | 安全 |
| persist 成功但 apply 崩溃 | 重新激活时从 DB 重建 | 安全 |
| Actor 崩溃 | 下次访问自动重新激活 | 安全 |
| 快照保存失败 | 仅告警，下次激活慢一些 | 安全 |

## 一致性保证

**exactly-once 语义**（通过事务级幂等实现）：
- 每个命令同步持久化 → 不丢
- 幂等键与事件在同一 DB 事务中写入 → 无窗口丢失
- Actor 崩溃后从 DB 重建 → 状态正确
- 客户端超时重试 + command_id → 安全重试

## 优雅关闭（Graceful Shutdown）

服务关闭时必须保证：已接收的命令处理完毕、所有活跃 Actor 保存快照。

### 关闭流程

```
SIGTERM / Kubernetes preStop
    │
    ├── 1. Gateway 设置 shutdown flag（拒绝新请求，返回 503）
    │      注意：HTTP listener 关闭前，已进入 Gateway 但尚未 send 到 Actor 的请求
    │      会被 shutdown flag 拦截，避免关闭过程中仍有新命令进入 mailbox
    │
    ├── 2. 停止接收新连接（HTTP listener 关闭）
    │
    ├── 3. 等待所有 Actor mailbox 排空（已接收的命令处理完毕）
    │      超时：shutdown_timeout（默认 30s）
    │
    ├── 4. 向所有活跃 Actor 发送 Deactivate（保存快照）
    │
    ├── 5. 等待所有 Actor 退出
    │      超时：额外 10s
    │
    └── 6. 关闭连接池、释放资源
```

### 实现

```rust
impl VirtualActorRuntime {
    pub async fn graceful_shutdown(&self, gateway_shutdown: &AtomicBool, timeout: Duration) {
        // 阶段 0：设置 Gateway shutdown 标志，拒绝新请求
        gateway_shutdown.store(true, Ordering::Release);

        // 阶段 1：等待所有 Actor 处理完当前命令
        let deadline = Instant::now() + timeout;
        loop {
            let busy_count = self.active.iter()
                .filter(|e| e.value().has_pending_messages())
                .count();
            if busy_count == 0 || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 阶段 2：通知所有 Actor 休眠（保存快照）
        let handles: Vec<(String, ActorHandle)> = self.active.iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        for (_, handle) in &handles {
            handle.send_deactivate().await;
        }

        // 阶段 3：等待所有 Actor 退出
        let extra_deadline = Instant::now() + Duration::from_secs(10);
        while !self.active.is_empty() && Instant::now() < extra_deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if !self.active.is_empty() {
            tracing::warn!(
                "优雅关闭超时，{} 个 Actor 未正常退出",
                self.active.len()
            );
        }
    }
}
```

### Kubernetes 集成

```yaml
spec:
  terminationGracePeriodSeconds: 45  # > shutdown_timeout + 10s
  containers:
    - lifecycle:
        preStop:
          httpGet:
            path: /admin/shutdown
            port: 9090
```

Host Server 收到 `/admin/shutdown` 后触发 `graceful_shutdown`。

## 背压传播（GraphQL → Gateway → Actor）

### 问题

Actor mailbox 满时返回 503，但如果 GraphQL 层不限制入站请求，高并发下大量请求会堆积在
Gateway 层等待 Actor mailbox 空间，消耗连接和内存资源。需要端到端的背压传播机制。

### 多层背压设计

```
客户端 ←── HTTP 503 ←── GraphQL 层 ←── Gateway ←── Actor mailbox
                         │                │              │
                    连接数限制        并发请求限制     邮箱有界
                    请求超时          关闭检查         try_send
```

### GraphQL / HTTP 层限制

```rust
pub struct HttpServerConfig {
    /// 最大并发连接数（超出时拒绝新连接）
    pub max_connections: usize,          // 默认 10,000
    /// 单连接最大并发请求数（HTTP/2 multiplexing 场景）
    pub max_requests_per_connection: usize, // 默认 100
    /// 请求体大小限制
    pub max_request_body: usize,         // 默认 1 MB
    /// 请求处理超时（从接收到响应的总时间）
    pub request_timeout: Duration,       // 默认 30s
    /// 慢启动：服务启动后逐步放开连接数，避免冷启动雪崩
    pub slow_start_duration: Duration,   // 默认 10s
}
```

### Gateway 层并发控制

```rust
use tokio::sync::Semaphore;

pub struct CommandGateway {
    runtime: Arc<VirtualActorRuntime>,
    event_store: Arc<dyn EventStore>,
    idempotency_bloom: RollingBloomFilter,
    wasm_pool: Arc<WasmPoolManager>,
    command_registry: CommandRegistry,
    shutting_down: Arc<AtomicBool>,
    /// 全局并发命令数限制（防止 Gateway 层请求堆积）
    concurrency_limiter: Arc<Semaphore>,
    /// 请求排队超时（等待 semaphore 的最大时间）
    queue_timeout: Duration,
}

impl CommandGateway {
    pub async fn execute(&self, command: IncomingCommand) -> Result<CommandResult> {
        // 0. 关闭检查
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(Error::service_unavailable("服务正在关闭，请重试"));
        }

        // 1. 并发控制：超时则返回 503，触发客户端退避重试
        let _permit = tokio::time::timeout(
            self.queue_timeout,
            self.concurrency_limiter.acquire(),
        ).await
            .map_err(|_| Error::service_unavailable("请求排队超时，系统繁忙"))?
            .map_err(|_| Error::internal("信号量已关闭"))?;

        // 后续流程不变：幂等检查 → validate → Actor 路由
        // ...
    }
}
```

### 配置

```rust
pub struct BackpressureConfig {
    /// Gateway 最大并发命令数（建议 = Actor mailbox 总容量 × 2）
    pub max_concurrent_commands: usize,  // 默认 2048
    /// 排队超时
    pub queue_timeout: Duration,         // 默认 5s
    /// HTTP 层配置
    pub http: HttpServerConfig,
}
```

### 背压信号传播

| 层 | 触发条件 | 响应 | 客户端行为 |
|----|----------|------|-----------|
| HTTP | 连接数 > max_connections | 拒绝新连接（TCP RST） | DNS 轮询到其他节点 |
| GraphQL | 请求超时 > request_timeout | 504 Gateway Timeout | 重试（带退避） |
| Gateway | semaphore 等待 > queue_timeout | 503 Service Unavailable | 重试（带退避） |
| Actor | mailbox try_send 失败 | 503 Actor 邮箱已满 | 重试（带退避） |

### 客户端退避建议

```
重试策略：指数退避 + 抖动
  base_delay = 100ms
  max_delay = 5s
  delay = min(base_delay × 2^attempt + random(0, 100ms), max_delay)
```
