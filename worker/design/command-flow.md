# 命令处理流程（单机版）

## 核心原则

**响应客户端之前，事件必须已持久化到 Event Store。**

- 每次命令处理完成后同步写入 DB，等待确认后才返回结果
- Actor 崩溃不会丢失任何已确认的命令

## 时序图

```
Client       GraphQL       VirtualActorRuntime     Actor(聚合)       WASM          EventStore
  │              │                  │                   │               │                │
  │── Mutation ─▶│                  │                   │               │                │
  │  createItem  │                  │                   │               │                │
  │              │── send(agg_id) ─▶│                   │               │                │
  │              │                  │── 查找/激活 Actor ▶│               │                │
  │              │                  │                   │               │                │
  │              │                  │                   │─ handle-X() ─▶│                │
  │              │                  │                   │◀─ events ────│                │
  │              │                  │                   │               │                │
  │              │                  │                   │── persist(同步) ──────────────▶│
  │              │                  │                   │◀── ack ───────────────────────│
  │              │                  │                   │               │                │
  │              │                  │                   │── apply ──┐   │                │
  │              │                  │                   │◀──────────┘   │                │
  │              │                  │                   │               │                │
  │◀── Result ──│◀─────────────────│◀── Ok(version) ──│               │                │
```

关键顺序：**handle → persist → ack → apply → 响应客户端**

## IncomingCommand 结构

```rust
pub struct IncomingCommand {
    pub aggregate_id: String,       // 聚合根 ID
    pub expected_version: u64,      // 客户端期望的聚合版本（乐观并发控制）
    pub module: String,             // 模块名（如 "inventory"）
    pub command_type: String,       // 命令类型 kebab-case（如 "create-item"）
    pub data: Vec<u8>,              // 序列化的命令参数
}
```

### 乐观并发控制

客户端必须携带 `expectedVersion`（聚合当前版本号）：
- 版本匹配 → 命令执行，版本递增
- 版本不匹配 → 返回 VersionConflict，客户端需重新读取状态后重试

重试安全性：命令成功后版本已递增，相同 `expectedVersion` 的重试必然冲突，天然幂等。

## Virtual Actor Runtime

```rust
use dashmap::DashMap;

pub struct VirtualActorRuntime {
    active: DashMap<String, ActorHandle>,
    activation_locks: DashMap<String, Arc<Mutex<()>>>,
    next_generation: AtomicU64,
    max_active: usize,
    idle_timeout: Duration,
    event_store: Arc<dyn EventStore>,
    wasm_engine: Arc<WasmEngine>,
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

        // 慢路径：per-key 激活锁，防止并发激活同一聚合
        let lock = self.activation_locks
            .entry(aggregate_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        // Double-check
        if let Some(handle) = self.active.get(aggregate_id) {
            return Ok(handle.clone());
        }

        self.activate(aggregate_id, module).await
    }

    /// 激活聚合：从快照+增量事件恢复到内存
    async fn activate(&self, aggregate_id: &str, module: &str) -> Result<ActorHandle> {
        // 内存压力检查
        while self.active.len() >= self.max_active {
            if !self.evict_one().await {
                return Err(Error::overloaded("内存预算已满"));
            }
        }

        // 从快照+增量事件恢复状态
        let (state, version) = self.recover_state(aggregate_id, module).await?;

        let (handle, actor) = VirtualActor::new(
            aggregate_id.to_string(),
            module.to_string(),
            state,
            version,
            self.wasm_engine.clone(),
            self.event_store.clone(),
            self.idle_timeout,
        );

        self.active.insert(aggregate_id.to_string(), handle.clone());

        let active_ref = self.active.clone();
        let agg_id = aggregate_id.to_string();
        let generation = handle.generation();
        tokio::spawn(async move {
            actor.run().await;
            active_ref.remove_if(&agg_id, |_, h| h.generation() == generation);
        });

        Ok(handle)
    }

    /// 从快照+增量事件恢复聚合状态
    async fn recover_state(&self, aggregate_id: &str, module: &str) -> Result<(Vec<u8>, u64)> {
        let (base_state, from_version) = match self.event_store
            .load_snapshot(module, aggregate_id).await?
        {
            Some(snap) => (snap.state, snap.version),
            None => (vec![], 0),
        };

        let events = self.event_store
            .load_events_after(aggregate_id, from_version).await?;
        let version = from_version + events.len() as u64;

        let state = if events.is_empty() {
            base_state
        } else {
            let event_data: Vec<&[u8]> = events.iter().map(|e| e.data.as_slice()).collect();
            self.wasm_engine.call_apply_events(module, &base_state, &event_data)?
        };

        Ok((state, version))
    }

    /// LRU 驱逐：不驱逐有待处理消息的 Actor
    async fn evict_one(&self) -> bool {
        let candidate = self.active.iter()
            .filter(|entry| !entry.value().is_busy())
            .min_by_key(|entry| entry.value().last_active())
            .map(|entry| entry.key().clone());

        if let Some(id) = candidate {
            if let Some(handle) = self.active.get(&id) {
                let generation = handle.generation();
                handle.send_deactivate().await;
                self.active.remove_if(&id, |_, h| h.generation() == generation);
            }
            true
        } else {
            false
        }
    }
}
```

## Virtual Actor

```rust
pub struct VirtualActor {
    aggregate_id: String,
    module_name: String,
    state: Vec<u8>,
    version: u64,
    wasm_engine: Arc<WasmEngine>,
    event_store: Arc<dyn EventStore>,
    mailbox: mpsc::Receiver<ActorMessage>,
    handle: ActorHandle,
    idle_timeout: Duration,
    snapshot_threshold: u64,
    last_snapshot_version: u64,
}

impl VirtualActor {
    pub async fn run(mut self) {
        let mut idle_check = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                Some(msg) = self.mailbox.recv() => {
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
                    if self.handle.idle_ms() > self.idle_timeout.as_millis() as u64 {
                        self.on_deactivate().await;
                        return;
                    }
                }
            }
        }
    }

    /// 处理单个命令（强一致：持久化后才响应）
    async fn process_command(&mut self, cmd: IncomingCommand) -> Result<CommandResult> {
        // 1. WASM handle-X
        let handle_fn = format!("handle-{}", cmd.command_type);
        let new_events = self.wasm_engine.call_handle(
            &self.module_name, &handle_fn, &self.state, &cmd.data
        )?;

        // 空事件 = 命令合法但无副作用
        if new_events.is_empty() {
            return Ok(CommandResult::noop(self.version));
        }

        // 1.5 版本校验（乐观并发控制）
        if cmd.expected_version != self.version {
            return Err(Error::version_conflict(self.version, cmd.expected_version));
        }

        // 2. 同步持久化（version 唯一约束保证不会重复写入）
        let events_to_persist: Vec<PendingEvent> = new_events.iter().enumerate()
            .map(|(i, data)| PendingEvent {
                aggregate_id: self.aggregate_id.clone(),
                aggregate_type: self.module_name.clone(),
                version: self.version + 1 + i as u64,
                data: data.clone(),
            }).collect();

        self.event_store.append(
            &self.aggregate_id,
            &events_to_persist,
            self.version,
        ).await?;

        // 3. 更新内存状态
        let event_refs: Vec<&[u8]> = new_events.iter().map(|e| e.as_slice()).collect();
        self.state = self.wasm_engine.call_apply_events(
            &self.module_name, &self.state, &event_refs
        )?;
        self.version += new_events.len() as u64;

        // 4. 快照判断（异步，不阻塞响应）
        self.maybe_snapshot();

        Ok(CommandResult::ok(self.version, new_events.len()))
    }

    fn maybe_snapshot(&mut self) {
        let events_since = self.version - self.last_snapshot_version;
        if events_since >= self.snapshot_threshold {
            self.last_snapshot_version = self.version;
            let snapshot = Snapshot {
                aggregate_id: self.aggregate_id.clone(),
                aggregate_type: self.module_name.clone(),
                version: self.version,
                state: self.state.clone(),
            };
            let store = self.event_store.clone();
            tokio::spawn(async move {
                let _ = store.save_snapshot(&snapshot).await;
            });
        }
    }

    /// 休眠前保存快照
    async fn on_deactivate(&self) {
        let snapshot = Snapshot {
            aggregate_id: self.aggregate_id.clone(),
            aggregate_type: self.module_name.clone(),
            version: self.version,
            state: self.state.clone(),
        };
        let _ = self.event_store.save_snapshot(&snapshot).await;
    }
}
```

## Actor 句柄与消息

```rust
pub struct ActorHandle {
    tx: mpsc::Sender<ActorMessage>,
    last_active: Arc<AtomicU64>,
    generation: u64,
}

impl ActorHandle {
    pub async fn send(&self, command: IncomingCommand) -> Result<CommandResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.try_send(ActorMessage::Command { command, reply_tx })
            .map_err(|_| Error::overloaded("Actor 邮箱已满"))?;
        reply_rx.await.map_err(|_| Error::internal("Actor 无响应"))?
    }

    pub fn is_busy(&self) -> bool {
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
              │  不存在   │
              └─────┬────┘
                    │ 首次命令到达
                    ▼
        ┌───────────────────────┐
        │ 激活中 (Activating)    │
        │ 加载快照 + 增量事件    │
        └───────────┬───────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │ 活跃 (Active)          │◀── 收到命令
        │ handle → persist → ack │
        └───────────┬───────────┘
                    │ 空闲超时 / LRU 驱逐
                    ▼
        ┌───────────────────────┐
        │ 休眠 (Deactivating)    │
        │ 保存快照 → 释放内存    │
        └───────────┬───────────┘
                    │
                    ▼
              ┌──────────┐
              │  不存在   │
              └──────────┘
```

## 配置

```rust
pub struct RuntimeConfig {
    pub max_active: usize,           // 最大活跃聚合数量（默认 10000）
    pub idle_timeout: Duration,       // 空闲超时（默认 5 分钟）
    pub mailbox_capacity: usize,      // Actor 邮箱容量（默认 64）
    pub snapshot_threshold: u64,      // 快照阈值（默认 100 个事件）
    pub shutdown_timeout: Duration,   // 优雅关闭超时（默认 30 秒）
}
```

## 优雅关闭

```rust
impl VirtualActorRuntime {
    pub async fn graceful_shutdown(&self, timeout: Duration) {
        // 1. 等待所有 Actor 处理完当前命令
        let deadline = Instant::now() + timeout;
        loop {
            let busy = self.active.iter().filter(|e| e.value().is_busy()).count();
            if busy == 0 || Instant::now() > deadline { break; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 2. 通知所有 Actor 休眠（保存快照）
        for entry in self.active.iter() {
            entry.value().send_deactivate().await;
        }
    }
}
```

## 错误处理

| 场景 | 处理方式 | 数据安全 |
|------|----------|----------|
| handle 失败（业务拒绝） | 返回领域错误，状态不变 | 安全 |
| persist 失败（DB 不可用） | 返回 503，内存状态不变 | 安全 |
| persist 成功但 apply 崩溃 | 重新激活时从 DB 重建 | 安全 |
| Actor 崩溃 | 下次访问自动重新激活 | 安全 |
| 快照保存失败 | 仅告警，下次激活慢一些 | 安全 |
