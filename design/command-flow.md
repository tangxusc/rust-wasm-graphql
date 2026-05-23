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
  │              │            │─ 幂等检查 ──────▶│                   │                 │                │
  │              │            │  (bloom+KV)      │                   │                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │─ validate-create-item() ────────────────────────────▶│                │
  │              │            │◀─ Ok ───────────────────────────────────────────────│                │
  │              │            │                  │                   │                 │                │
  │              │            │─ send(agg_id) ──▶│                   │                 │                │
  │              │            │                  │── 查找/激活 Actor ▶│                 │                │
  │              │            │                  │   (透明寻址)       │                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │─ handle-create-item(state) ───▶│
  │              │            │                  │                   │◀─ new_events ─────────────────│
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │── persist(同步等待) ────────────▶│
  │              │            │                  │                   │◀── ack(已落盘) ─────────────────│
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │── apply locally ─┐               │
  │              │            │                  │                   │   state += events│               │
  │              │            │                  │                   │◀─────────────────┘               │
  │              │            │                  │                   │                 │                │
  │◀── Result ──│◀───────────│◀─────────────────│◀── Ok(version) ──│                 │                │
```

关键顺序：**validate-X（Gateway 前置） → persist → ack → apply locally → 响应客户端**

validate-X 在 Gateway 层前置执行，不依赖聚合状态，冷聚合无需激活即可拒绝格式错误的请求。
事件先落盘，再更新内存状态。即使 apply 过程中崩溃，重新激活时从 DB 重建即可恢复正确状态。

## Command Gateway（入口层）

```rust
pub struct CommandGateway {
    runtime: Arc<VirtualActorRuntime>,
    idempotency_bloom: RollingBloomFilter,
    wasm_pool: Arc<WasmPoolManager>,
}

impl CommandGateway {
    pub async fn execute(&self, command: IncomingCommand) -> Result<CommandResult> {
        // 1. 双层幂等检查（布隆过滤器 + Event Store 内事务级幂等）
        if self.idempotency_bloom.might_contain(&command.command_id) {
            if self.runtime.idempotency_exists(&command.aggregate_id, &command.command_id).await? {
                return Ok(CommandResult::duplicate());
            }
        }

        // 2. 前置 validate-X（按命令类型路由，不依赖聚合状态）
        let validate_fn = format!("validate-{}", command.command_type);
        let mut instance = self.wasm_pool.acquire(&command.module).await?;
        instance.call_function(&validate_fn, &[command.data.clone()])?;
        drop(instance);

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
        // 激活完成后清理锁（可选，防止锁表无限增长）
        self.activation_locks.remove(aggregate_id);
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
        tokio::spawn(async move {
            actor.run().await;
            active_ref.remove(&agg_id);
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
            let mut instance = self.wasm_pool.acquire(module).await?;
            let event_data: Vec<&[u8]> = events.iter().map(|e| e.data.as_slice()).collect();
            instance.call_apply_events(&base_state, &event_data)?
        };

        Ok((state, version))
    }

    /// LRU 驱逐（带保护：不驱逐有待处理消息或刚活跃的 Actor）
    /// 返回 true 表示成功驱逐了一个 Actor，false 表示无可驱逐候选者
    async fn evict_one(&self) -> bool {
        let now = now_millis();
        let min_idle_ms = self.min_evict_idle.as_millis() as u64;

        let candidate = self.active.iter()
            .filter(|entry| {
                let handle = entry.value();
                // 保护条件：有待处理消息的 Actor 不可驱逐
                !handle.has_pending_messages()
                // 保护条件：未达到最小空闲时间的 Actor 不可驱逐
                && (now - handle.last_active()) > min_idle_ms
            })
            .min_by_key(|entry| entry.value().last_active())
            .map(|entry| entry.key().clone());

        if let Some(id) = candidate {
            if let Some((_, handle)) = self.active.remove(&id) {
                handle.send_deactivate().await;
            }
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
    wasm_pool: Arc<WasmPoolManager>,
    event_store: Arc<dyn EventStore>,
    snapshot_store: Arc<dyn SnapshotStore>,
    mailbox: mpsc::Receiver<ActorMessage>,
    handle: ActorHandle,        // 持有自身 handle 引用，用于统一更新 last_active
    idle_timeout: Duration,
    last_snapshot_version: u64,
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
                            let result = self.process_command(command).await;
                            let _ = reply_tx.send(result);
                        }
                        ActorMessage::Deactivate => {
                            self.on_deactivate().await;
                            return;
                        }
                    }
                }
                _ = idle_check.tick() => {
                    let idle_ms = now_millis() - self.handle.last_active();
                    if idle_ms > self.idle_timeout.as_millis() as u64 {
                        self.on_deactivate().await;
                        return;
                    }
                }
            }
        }
    }

    /// 处理单个命令（强一致：持久化后才响应）
    async fn process_command(&mut self, cmd: IncomingCommand) -> Result<CommandResult> {
        // 注意：validate-X 已在 Gateway 层前置执行，此处不再重复调用
        // 这样冷聚合无需激活即可拒绝格式错误的请求

        // 1. WASM handle-X（按命令类型路由，基于内存中的当前状态决策）
        let handle_fn = format!("handle-{}", cmd.command_type);
        let mut instance = self.wasm_pool.acquire(&self.module_name).await?;
        let new_events = instance.call_function(&handle_fn, &[self.state.clone(), cmd.data.clone()])?;
        drop(instance);

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
        ).await?;

        // 3. DB 已确认 → 安全更新内存状态
        let mut inst = self.wasm_pool.acquire(&self.module_name).await?;
        let event_refs: Vec<&[u8]> = new_events.iter().map(|e| e.as_slice()).collect();
        self.state = inst.call_apply_events(&self.state, &event_refs)?;
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
    fn maybe_snapshot(&mut self) {
        let events_since = self.version - self.last_snapshot_version;
        if events_since >= self.snapshot_threshold {
            let snapshot = Snapshot {
                aggregate_id: self.aggregate_id.clone(),
                aggregate_type: self.module_name.clone(),
                version: self.version,
                state: self.state.clone(),
                created_at: now_millis(),
            };
            let store = self.snapshot_store.clone();
            tokio::spawn(async move { let _ = store.save(&snapshot).await; });
            self.last_snapshot_version = self.version;
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
异步刷盘（已废弃）：
  handle → 更新内存 → 响应客户端 → ... → 异步刷盘
  问题：崩溃时丢失未刷盘事件

强一致（当前方案）：
  handle → persist(等待DB确认) → 更新内存 → 响应客户端
  保证：客户端收到成功 = 事件已落盘 = 零数据丢失
```

## Actor 句柄与消息

```rust
pub struct ActorHandle {
    tx: mpsc::Sender<ActorMessage>,
    last_active: Arc<AtomicU64>,
}

impl ActorHandle {
    pub async fn send(&self, command: IncomingCommand) -> Result<CommandResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(ActorMessage::Command { command, reply_tx })
            .await
            .map_err(|_| Error::overloaded("Actor 邮箱已满"))?;
        // 注意：此处不更新 last_active，由 Actor 处理消息时统一更新
        // 确保驱逐器和 Actor 自身空闲检测使用同一时钟源
        reply_rx.await.map_err(|_| Error::internal("Actor 无响应"))?
    }

    pub async fn send_deactivate(&self) {
        let _ = self.tx.send(ActorMessage::Deactivate).await;
    }

    pub fn last_active(&self) -> u64 {
        self.last_active.load(Ordering::Relaxed)
    }

    /// 由 Actor 在处理消息时调用，统一时钟源
    pub fn touch(&self) {
        self.last_active.store(now_millis(), Ordering::Relaxed);
    }

    /// 检查 mailbox 中是否有待处理消息（用于驱逐保护）
    pub fn has_pending_messages(&self) -> bool {
        // capacity - available permits = 当前排队消息数
        self.tx.max_capacity() - self.tx.capacity() > 0
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
                if let Some((_, handle)) = self.active.remove(&id) {
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
| validate 失败 | 返回 400，无副作用 | 安全 |
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
    ├── 1. 停止接收新请求（HTTP listener 关闭）
    │
    ├── 2. 等待所有 Actor mailbox 排空（已接收的命令处理完毕）
    │      超时：shutdown_timeout（默认 30s）
    │
    ├── 3. 向所有活跃 Actor 发送 Deactivate（保存快照）
    │
    ├── 4. 等待所有 Actor 退出
    │      超时：额外 10s
    │
    └── 5. 关闭连接池、释放资源
```

### 实现

```rust
impl VirtualActorRuntime {
    pub async fn graceful_shutdown(&self, timeout: Duration) {
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
