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
  │              │── cmd ────▶│                  │                   │                 │                │
  │              │            │─ 幂等检查 ──────▶│                   │                 │                │
  │              │            │  (bloom+KV)      │                   │                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │─ send(agg_id) ──▶│                   │                 │                │
  │              │            │                  │── 查找/激活 Actor ▶│                 │                │
  │              │            │                  │   (透明寻址)       │                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │─ validate() ───▶│                │
  │              │            │                  │                   │◀─ Ok ───────────│                │
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │─ handle(state) ▶│                │
  │              │            │                  │                   │◀─ new_events ───│                │
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

关键顺序：**persist → ack → apply locally → 响应客户端**

事件先落盘，再更新内存状态。即使 apply 过程中崩溃，重新激活时从 DB 重建即可恢复正确状态。

## Command Gateway（入口层）

```rust
pub struct CommandGateway {
    runtime: Arc<VirtualActorRuntime>,
    idempotency_bloom: BloomFilter,
    idempotency_store: Arc<dyn IdempotencyStore>,
}

impl CommandGateway {
    pub async fn execute(&self, command: IncomingCommand) -> Result<CommandResult> {
        // 1. 双层幂等检查
        if self.idempotency_bloom.might_contain(&command.command_id) {
            if self.idempotency_store.exists(&command.command_id).await? {
                return Ok(CommandResult::duplicate());
            }
        }

        // 2. 透明寻址：运行时自动激活/路由
        let result = self.runtime.send(&command.aggregate_id, command.clone()).await?;

        // 3. 记录幂等键（事件已持久化，此时记录安全）
        self.idempotency_bloom.insert(&command.command_id);
        self.idempotency_store.record(&command.command_id, Duration::from_secs(86400)).await?;

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
    /// 内存预算
    max_active: usize,
    /// 空闲超时
    idle_timeout: Duration,
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

    /// 获取已激活的 Actor，或按需激活
    async fn get_or_activate(&self, aggregate_id: &str, module: &str) -> Result<ActorHandle> {
        // 快速路径：Actor 已在内存中
        if let Some(handle) = self.active.get(aggregate_id) {
            return Ok(handle.clone());
        }
        // 慢路径：需要激活
        self.activate(aggregate_id, module).await
    }

    /// 激活聚合：从持久化状态恢复到内存
    async fn activate(&self, aggregate_id: &str, module: &str) -> Result<ActorHandle> {
        // 内存压力检查
        while self.active.len() >= self.max_active {
            self.evict_one().await;
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
        let (base_state, from_version) = match self.snapshot_store.load(aggregate_id).await? {
            Some(snap) => (snap.state, snap.version),
            None => (vec![], 0),
        };

        let events = self.event_store.load_events_after(aggregate_id, from_version).await?;
        let version = from_version + events.len() as u64;

        let state = if events.is_empty() {
            base_state
        } else {
            let mut instance = self.wasm_pool.acquire(module)?;
            let event_data: Vec<&[u8]> = events.iter().map(|e| e.data.as_slice()).collect();
            instance.call_apply_events(&base_state, &event_data)?
        };

        Ok((state, version))
    }

    /// LRU 驱逐
    async fn evict_one(&self) {
        let oldest = self.active.iter()
            .min_by_key(|entry| entry.value().last_active())
            .map(|entry| entry.key().clone());
        if let Some(id) = oldest {
            if let Some((_, handle)) = self.active.remove(&id) {
                handle.send_deactivate().await;
            }
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
    idle_timeout: Duration,
    last_active: Instant,
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
                    self.last_active = Instant::now();
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
                    if self.last_active.elapsed() > self.idle_timeout {
                        self.on_deactivate().await;
                        return;
                    }
                }
            }
        }
    }

    /// 处理单个命令（强一致：持久化后才响应）
    async fn process_command(&mut self, cmd: IncomingCommand) -> Result<CommandResult> {
        // 1. WASM validate（快速格式校验）
        let mut instance = self.wasm_pool.acquire(&self.module_name)?;
        instance.call_validate(&cmd.data)?;

        // 2. WASM handle（基于内存中的当前状态决策）
        let new_events = instance.call_handle(&self.state, &cmd.data)?;
        drop(instance);

        // 3. 同步持久化到 Event Store（等待 DB 确认）
        let events_to_persist: Vec<PendingEvent> = new_events.iter().enumerate()
            .map(|(i, data)| PendingEvent {
                aggregate_id: self.aggregate_id.clone(),
                aggregate_type: self.module_name.clone(),
                event_type: extract_event_type(data),
                version: self.version + 1 + i as u64,
                data: data.clone(),
            }).collect();

        self.event_store.append(
            &self.aggregate_id,
            &events_to_persist,
            self.version,
        ).await?;

        // 4. DB 已确认 → 安全更新内存状态
        let mut inst = self.wasm_pool.acquire(&self.module_name)?;
        let event_refs: Vec<&[u8]> = new_events.iter().map(|e| e.as_slice()).collect();
        self.state = inst.call_apply_events(&self.state, &event_refs)?;
        self.version += new_events.len() as u64;

        // 5. 异步快照判断（快照丢失不影响正确性）
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
        self.last_active.store(now_millis(), Ordering::Relaxed);
        reply_rx.await.map_err(|_| Error::internal("Actor 无响应"))?
    }

    pub async fn send_deactivate(&self) {
        let _ = self.tx.send(ActorMessage::Deactivate).await;
    }

    pub fn last_active(&self) -> u64 {
        self.last_active.load(Ordering::Relaxed)
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
                .filter(|entry| now - entry.value().last_active() > idle_ms)
                .map(|entry| entry.key().clone())
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
    pub mailbox_capacity: usize,         // Actor 邮箱容量（默认 64）
    pub snapshot_threshold: u64,         // 快照阈值（默认 100）
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

**exactly-once 语义**（通过幂等实现）：
- 每个命令同步持久化 → 不丢
- 幂等键去重 → 不重复
- Actor 崩溃后从 DB 重建 → 状态正确
- 客户端超时重试 + command_id → 安全重试
