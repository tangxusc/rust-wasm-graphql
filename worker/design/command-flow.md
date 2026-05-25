# 命令处理流程（单机版）

## 核心原则

**响应客户端之前，事件必须已持久化到 Event Store。**

- 每次命令处理完成后同步写入 DB，等待确认后才返回结果
- Actor 崩溃不会丢失任何已确认的命令

## 时序图

```
Client       GraphQL       RuntimeHandle       Runtime(主循环)     Actor(channel=1)    WASM          EventStore
  │              │                │                  │                   │               │                │
  │── Mutation ─▶│                │                  │                   │               │                │
  │              │── send(cmd) ──▶│                  │                   │               │                │
  │              │                │── RuntimeMsg ───▶│                   │               │                │
  │              │                │                  │── spawn task ────▶│               │                │
  │              │                │                  │   actor_tx.send   │               │                │
  │              │                │                  │                   │─ handle-X() ─▶│                │
  │              │                │                  │                   │◀─ events ────│                │
  │              │                │                  │                   │── persist ───────────────────▶│
  │              │                │                  │                   │◀── ack ──────────────────────│
  │              │                │                  │                   │── apply ──┐   │                │
  │              │                │                  │                   │◀──────────┘   │                │
  │◀── Result ──│◀───────────────│◀─────────────────│◀──────────────────│               │                │
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

## Virtual Actor Runtime（Channel 架构，无 Buffer）

**设计核心：所有通信通过 channel，Actor channel 容量=1，无命令堆积。**

Runtime 作为单线程消息循环运行，所有寻址/激活/驱逐决策串行化，天然无竞态。
Actor 通过容量为 1 的 channel 接收消息，同一时刻最多处理 1 条命令，无排队。

### 数据结构

```rust
use std::collections::{HashMap, BTreeMap};

/// 聚合的全局唯一标识：(模块名, aggregate_id)
type ActorKey = (String, String);

/// 外部调用方持有的句柄，通过 channel 与 Runtime 通信
#[derive(Clone)]
pub struct RuntimeHandle {
    tx: mpsc::Sender<RuntimeMessage>,
}

enum RuntimeMessage {
    /// 发送命令到指定聚合
    Send {
        command: IncomingCommand,
        reply: oneshot::Sender<Result<CommandResult>>,
    },
    /// Actor 确认已退出，Runtime 可安全移除
    ActorStopped {
        key: ActorKey,
        generation: u64,
    },
    /// 优雅关闭
    Shutdown {
        ack: oneshot::Sender<()>,
    },
}

pub struct VirtualActorRuntime {
    rx: mpsc::Receiver<RuntimeMessage>,
    handle: RuntimeHandle,
    /// 活跃 Actor 表，复合键避免跨模块碰撞
    active: HashMap<ActorKey, ActorHandle>,
    /// LRU 索引：generation → ActorKey
    lru_index: BTreeMap<u64, ActorKey>,
    /// 反向索引：ActorKey → generation
    lru_reverse: HashMap<ActorKey, u64>,
    next_generation: u64,
    max_active: usize,
    /// 已发送 Evict 但尚未收到 ActorStopped 的数量（用于容量计算）
    evicting_count: usize,
    idle_timeout: Duration,
    command_send_timeout: Duration,
    event_store: Arc<dyn EventStore>,
    wasm_engine: Arc<WasmEngine>,
}
```

### RuntimeHandle（外部调用入口）

```rust
impl RuntimeHandle {
    pub async fn send(&self, command: IncomingCommand) -> Result<CommandResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        // 使用 try_send 实现背压：channel 满时立即返回 503，避免阻塞 HTTP handler
        match self.tx.try_send(RuntimeMessage::Send { command, reply: reply_tx }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(Error::overloaded("系统繁忙，请稍后重试"));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(Error::internal("Runtime 已关闭"));
            }
        }
        reply_rx.await.map_err(|_| Error::internal("Runtime 无响应"))?
    }

    pub async fn shutdown(&self) {
        let (ack_tx, ack_rx) = oneshot::channel();
        let _ = self.tx.send(RuntimeMessage::Shutdown { ack: ack_tx }).await;
        let _ = ack_rx.await;
    }
}
```

### 命令路由（热路径 spawn task，冷路径激活 Actor）

```rust
impl VirtualActorRuntime {
    fn handle_send(
        &mut self,
        command: IncomingCommand,
        reply: oneshot::Sender<Result<CommandResult>>,
    ) {
        let key: ActorKey = (command.module.clone(), command.aggregate_id.clone());

        if let Some(handle) = self.active.get(&key) {
            // 热路径：Actor 已存在，spawn task 发送到 Actor channel
            self.update_lru(&key);
            let actor_tx = handle.tx.clone();
            let send_timeout = self.command_send_timeout;
            tokio::spawn(async move {
                // channel 容量=1，若 Actor 正忙则等待（带超时保护）
                let msg = ActorMessage::Command { command, reply_tx: reply };
                match tokio::time::timeout(send_timeout, actor_tx.send(msg)).await {
                    Ok(Ok(())) => {} // Actor 会通过 reply_tx 返回结果
                    Ok(Err(e)) => {
                        // Actor 已退出（channel closed），客户端重试
                        let cmd_msg = e.0;
                        if let ActorMessage::Command { reply_tx, .. } = cmd_msg {
                            let _ = reply_tx.send(Err(Error::actor_unavailable("聚合暂不可用，请重试")));
                        }
                    }
                    Err(_) => {
                        // 超时：Actor 长时间繁忙，通知客户端重试
                        // reply 已被移入 msg，超时后无法恢复，客户端 HTTP 层会超时
                    }
                }
            });
        } else {
            // 冷路径：激活新 Actor
            self.spawn_activation(key, command, reply);
        }
    }
}
```

```rust
impl VirtualActorRuntime {
    pub fn spawn(
        config: RuntimeConfig,
        event_store: Arc<dyn EventStore>,
        wasm_engine: Arc<WasmEngine>,
    ) -> RuntimeHandle {
        let (tx, rx) = mpsc::channel(config.runtime_channel_capacity);
        let handle = RuntimeHandle { tx };
        let runtime = Self {
            rx,
            handle: handle.clone(),
            active: HashMap::new(),
            lru_index: BTreeMap::new(),
            lru_reverse: HashMap::new(),
            next_generation: 0,
            max_active: config.max_active,
            evicting_count: 0,
            idle_timeout: config.idle_timeout,
            command_send_timeout: config.command_send_timeout,
            event_store,
            wasm_engine,
        };
        tokio::spawn(runtime.run());
        handle
    }

    async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                RuntimeMessage::Send { command, reply } => {
                    self.handle_send(command, reply);
                }
                RuntimeMessage::ActorStopped { key, generation } => {
                    self.handle_actor_stopped(&key, generation);
                }
                RuntimeMessage::Shutdown { ack } => {
                    self.handle_shutdown().await;
                    let _ = ack.send(());
                    return;
                }
            }
        }
    }
}
```

### Actor 句柄与消息

```rust
pub struct ActorHandle {
    tx: mpsc::Sender<ActorMessage>,
    generation: u64,
    last_active: Arc<AtomicU64>,
}

enum ActorMessage {
    Command {
        command: IncomingCommand,
        reply_tx: oneshot::Sender<Result<CommandResult>>,
    },
    /// 驱逐/关闭信号，Actor 完成当前命令后退出
    Evict,
}

impl ActorHandle {
    pub fn generation(&self) -> u64 { self.generation }
    pub fn touch(&self) { /* 更新 last_active 时间戳 */ }
    pub fn idle_ms(&self) -> u64 { /* 计算空闲毫秒数 */ 0 }
}
```

### 激活流程

```rust
impl VirtualActorRuntime {
    fn spawn_activation(
        &mut self,
        key: ActorKey,
        first_command: IncomingCommand,
        reply: oneshot::Sender<Result<CommandResult>>,
    ) {
        // 有效活跃数 = 总活跃数 - 正在驱逐的数量（已发 Evict 但未收到 Stopped）
        while self.active.len() - self.evicting_count >= self.max_active {
            if !self.evict_one() {
                let _ = reply.send(Err(Error::overloaded("内存预算已满")));
                return;
            }
        }

        // Actor channel 容量=1，无 buffer 堆积
        let (actor_tx, actor_rx) = mpsc::channel(1);
        let generation = self.next_generation;
        self.next_generation += 1;
        let handle = ActorHandle {
            tx: actor_tx,
            generation,
            last_active: Arc::new(AtomicU64::new(now_ms())),
        };
        self.active.insert(key.clone(), handle);
        self.update_lru(&key);

        let event_store = self.event_store.clone();
        let wasm_engine = self.wasm_engine.clone();
        let idle_timeout = self.idle_timeout;
        let runtime_tx = self.handle.tx.clone();

        tokio::spawn(async move {
            let (ref module, ref aggregate_id) = key;

            // 聚合类型冲突校验：确保同一 aggregate_id 不被不同模块占用
            if let Ok(true) = event_store.check_aggregate_type_conflict(aggregate_id, module).await {
                let _ = reply.send(Err(Error::type_conflict(
                    aggregate_id, module,
                    "该 aggregate_id 已被其他模块使用"
                )));
                let _ = runtime_tx.send(RuntimeMessage::ActorStopped {
                    key, generation,
                }).await;
                return;
            }

            // 从快照+增量事件恢复状态
            let (state, version) = match recover_state(
                aggregate_id, module, &event_store, &wasm_engine
            ).await {
                Ok(sv) => sv,
                Err(e) => {
                    let _ = reply.send(Err(e));
                    let _ = runtime_tx.send(RuntimeMessage::ActorStopped {
                        key, generation,
                    }).await;
                    return;
                }
            };

            let mut actor = VirtualActor {
                key: key.clone(),
                state,
                version,
                wasm_engine,
                event_store,
                rx: actor_rx,
                idle_timeout,
                snapshot_threshold: 100,
                last_snapshot_version: version,
                runtime_tx,
                generation,
            };

            // 处理首条命令
            let result = actor.process_command(first_command).await;
            let _ = reply.send(result);

            // 进入常规消息循环
            actor.run().await;
        });
    }
}
```

### 状态恢复

```rust
/// 毒事件错误：聚合因事件回放失败而无法激活
#[derive(Debug)]
pub struct PoisonAggregateError {
    pub aggregate_id: String,
    pub module: String,
    pub from_version: u64,
    pub cause: String,
}

async fn recover_state(
    aggregate_id: &str,
    module: &str,
    event_store: &Arc<dyn EventStore>,
    wasm_engine: &Arc<WasmEngine>,
) -> Result<(Vec<u8>, u64)> {
    let (base_state, from_version) = match event_store
        .load_snapshot(module, aggregate_id).await?
    {
        Some(snap) => (snap.state, snap.version),
        None => (vec![], 0),
    };

    let events = event_store
        .load_events_after(module, aggregate_id, from_version).await?;
    let version = events.last().map(|e| e.version).unwrap_or(from_version);

    let state = if events.is_empty() {
        base_state
    } else {
        let engine = wasm_engine.clone();
        let module_name = module.to_string();
        let base = base_state.clone();
        let event_data: Vec<Vec<u8>> = events.iter().map(|e| e.data.clone()).collect();
        let apply_result = tokio::task::spawn_blocking(move || {
            let refs: Vec<&[u8]> = event_data.iter().map(|e| e.as_slice()).collect();
            engine.call_apply_events(&module_name, &base, &refs)
        }).await.map_err(|e| Error::internal(format!("WASM 任务失败: {e}")))?;

        match apply_result {
            Ok(new_state) => new_state,
            Err(e) => {
                // 毒事件：apply-events 失败，聚合无法激活
                tracing::error!(
                    aggregate_id, module, from_version,
                    "apply-events 失败，聚合可能包含毒事件: {e}"
                );
                return Err(Error::poison_aggregate(PoisonAggregateError {
                    aggregate_id: aggregate_id.to_string(),
                    module: module.to_string(),
                    from_version,
                    cause: e.to_string(),
                }));
            }
        }
    };

    Ok((state, version))
}
```

## Virtual Actor

```rust
pub struct VirtualActor {
    key: ActorKey,
    state: Vec<u8>,
    version: u64,
    wasm_engine: Arc<WasmEngine>,
    event_store: Arc<dyn EventStore>,
    rx: mpsc::Receiver<ActorMessage>,
    idle_timeout: Duration,
    snapshot_threshold: u64,
    last_snapshot_version: u64,
    runtime_tx: mpsc::Sender<RuntimeMessage>,
    generation: u64,
}

impl VirtualActor {
    /// 消息循环：逐条处理，无排队
    pub async fn run(&mut self) {
        let mut idle_check = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(ActorMessage::Command { command, reply_tx }) => {
                            let result = self.process_command(command).await;
                            let _ = reply_tx.send(result);
                        }
                        Some(ActorMessage::Evict) => {
                            self.on_deactivate().await;
                            self.notify_stopped().await;
                            return;
                        }
                        None => {
                            // channel 关闭（不应发生，防御性处理）
                            self.on_deactivate().await;
                            self.notify_stopped().await;
                            return;
                        }
                    }
                }
                _ = idle_check.tick() => {
                    if self.idle_exceeded() {
                        self.on_deactivate().await;
                        self.notify_stopped().await;
                        return;
                    }
                }
            }
        }
    }

    fn idle_exceeded(&self) -> bool {
        // 检查距上次命令处理是否超过 idle_timeout
        false // 实际实现基于时间戳比较
    }

    async fn notify_stopped(&self) {
        let _ = self.runtime_tx.send(RuntimeMessage::ActorStopped {
            key: self.key.clone(),
            generation: self.generation,
        }).await;
    }

    async fn on_deactivate(&self) {
        let snapshot = Snapshot {
            aggregate_id: self.key.1.clone(),
            aggregate_type: self.key.0.clone(),
            version: self.version,
            state: self.state.clone(),
        };
        let _ = self.event_store.save_snapshot(&snapshot).await;
    }
}
```

### 命令处理（强一致：持久化后才响应）

```rust
impl VirtualActor {
    pub async fn process_command(&mut self, cmd: IncomingCommand) -> Result<CommandResult> {
        // 1. 版本校验
        if cmd.expected_version != self.version {
            return Err(Error::version_conflict(self.version, cmd.expected_version));
        }

        // 2. 可选前置校验（超时保护）
        let (ref module, _) = self.key;
        if let Some(validate_fn) = self.wasm_engine.get_validate_fn(module, &cmd.command_type) {
            let engine = self.wasm_engine.clone();
            let module = module.clone();
            let data = cmd.data.clone();
            tokio::time::timeout(
                Duration::from_secs(5),
                tokio::task::spawn_blocking(move || {
                    engine.call_validate(&module, &validate_fn, &data)
                })
            ).await
            .map_err(|_| Error::timeout("validate 执行超时"))?
            .map_err(|e| Error::internal(format!("WASM 任务失败: {e}")))??;
        }

        // 3. WASM handle-X（超时保护）
        let handle_fn = format!("handle-{}", cmd.command_type);
        let engine = self.wasm_engine.clone();
        let module = self.key.0.clone();
        let state = self.state.clone();
        let data = cmd.data.clone();
        let new_events = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(move || {
                engine.call_handle(&module, &handle_fn, &state, &data)
            })
        ).await
        .map_err(|_| Error::timeout("handle 执行超时"))?
        .map_err(|e| Error::internal(format!("WASM 任务失败: {e}")))??;

        if new_events.is_empty() {
            return Ok(CommandResult::noop(self.version));
        }

        // 4. 同步持久化
        let events_to_persist: Vec<PendingEvent> = new_events.iter().enumerate()
            .map(|(i, data)| {
                let event: Value = serde_json::from_slice(data)
                    .map_err(|e| Error::invalid_event(format!("事件非合法 JSON: {e}")))?;
                let event_type = event["type"].as_str()
                    .ok_or_else(|| Error::invalid_event(
                        format!("事件缺少 type 字段（索引 {}）", i)
                    ))?
                    .to_string();
                Ok(PendingEvent {
                    aggregate_id: self.key.1.clone(),
                    aggregate_type: self.key.0.clone(),
                    event_type,
                    version: self.version + 1 + i as u64,
                    data: data.clone(),
                })
            }).collect::<Result<Vec<_>>>()?;

        self.event_store.append(&self.key.1, &events_to_persist, self.version).await?;

        // 5. 更新内存状态（persist 已成功，apply 失败则 Actor 必须自杀）
        let engine = self.wasm_engine.clone();
        let module = self.key.0.clone();
        let current_state = self.state.clone();
        let events_for_apply = new_events.clone();
        let apply_result = tokio::task::spawn_blocking(move || {
            let refs: Vec<&[u8]> = events_for_apply.iter().map(|e| e.as_slice()).collect();
            engine.call_apply_events(&module, &current_state, &refs)
        }).await;

        match apply_result {
            Ok(Ok(new_state)) => {
                self.state = new_state;
                self.version += new_events.len() as u64;
            }
            _ => {
                // apply 失败：事件已持久化但内存状态不可信，Actor 必须退出
                self.notify_stopped().await;
                return Err(Error::internal(
                    "apply-events 失败，事件已持久化，Actor 将重新激活"
                ));
            }
        }

        // 6. 快照判断（异步，不阻塞响应）
        self.maybe_snapshot();

        Ok(CommandResult::ok(self.version, new_events.len()))
    }

    fn maybe_snapshot(&mut self) {
        let events_since = self.version - self.last_snapshot_version;
        if events_since >= self.snapshot_threshold {
            self.last_snapshot_version = self.version;
            let snapshot = Snapshot {
                aggregate_id: self.key.1.clone(),
                aggregate_type: self.key.0.clone(),
                version: self.version,
                state: self.state.clone(),
            };
            let store = self.event_store.clone();
            tokio::spawn(async move {
                let _ = store.save_snapshot(&snapshot).await;
            });
        }
    }
}
```

### LRU 与驱逐

```rust
impl VirtualActorRuntime {
    fn update_lru(&mut self, key: &ActorKey) {
        let gen = self.next_generation;
        self.next_generation += 1;
        if let Some(old_gen) = self.lru_reverse.insert(key.clone(), gen) {
            self.lru_index.remove(&old_gen);
        }
        self.lru_index.insert(gen, key.clone());
    }

    /// 驱逐最旧 Actor：通过 channel 发送 Evict 消息
    fn evict_one(&mut self) -> bool {
        let candidate = self.lru_index.iter().next()
            .map(|(gen, key)| (*gen, key.clone()));

        if let Some((gen, key)) = candidate {
            if let Some(handle) = self.active.get(&key) {
                // 通过 Actor 的 channel 发送 Evict（非阻塞 try_send）
                // 若 channel 满（Actor 正忙），跳过此 Actor，尝试下一个
                match handle.tx.try_send(ActorMessage::Evict) {
                    Ok(()) => {
                        // Evict 已发送，但不立即移除 active 条目
                        // 等 Actor 发回 ActorStopped 后再移除
                        self.lru_index.remove(&gen);
                        self.lru_reverse.remove(&key);
                        self.evicting_count += 1;
                        true
                    }
                    Err(_) => {
                        // Actor 正忙，跳过，尝试下一个 LRU 候选
                        self.evict_next_candidate(gen)
                    }
                }
            } else {
                self.lru_index.remove(&gen);
                self.lru_reverse.remove(&key);
                true
            }
        } else {
            false
        }
    }

    fn evict_next_candidate(&mut self, skip_gen: u64) -> bool {
        let candidate = self.lru_index.range((skip_gen + 1)..)
            .next()
            .map(|(gen, key)| (*gen, key.clone()));

        if let Some((gen, key)) = candidate {
            if let Some(handle) = self.active.get(&key) {
                if handle.tx.try_send(ActorMessage::Evict).is_ok() {
                    self.lru_index.remove(&gen);
                    self.lru_reverse.remove(&key);
                    self.evicting_count += 1;
                    return true;
                }
            }
        }
        false
    }

    /// Actor 确认退出后，安全移除 active 条目
    fn handle_actor_stopped(&mut self, key: &ActorKey, generation: u64) {
        if let Some(handle) = self.active.get(key) {
            if handle.generation == generation {
                self.active.remove(key);
                self.evicting_count = self.evicting_count.saturating_sub(1);
            }
        }
    }
}
```

### 优雅关闭

```rust
impl VirtualActorRuntime {
    async fn handle_shutdown(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);

        // 向所有 Actor 发送 Evict
        for (_, handle) in &self.active {
            let _ = handle.tx.send(ActorMessage::Evict).await;
        }

        // 等待所有 ActorStopped 消息（带超时）
        let remaining_count = self.active.len();
        let mut stopped = 0;
        while stopped < remaining_count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, self.rx.recv()).await {
                Ok(Some(RuntimeMessage::ActorStopped { key, generation })) => {
                    self.handle_actor_stopped(&key, generation);
                    stopped += 1;
                }
                _ => break, // 超时或 channel 关闭
            }
        }
    }
}
```

**关于快照丢失：** 若超时前部分 Actor 未完成快照保存，下次启动时该聚合需从上一个有效快照
重放更多事件，仅影响激活延迟，不影响数据正确性。

## Virtual Actor 生命周期

```
              ┌──────────┐
              │  不存在   │
              └─────┬────┘
                    │ 首次命令到达 Runtime
                    ▼
        ┌───────────────────────┐
        │ 激活中 (Activating)    │
        │ 加载快照 + 增量事件    │
        │ 处理首条命令           │
        └───────────┬───────────┘
                    ▼
        ┌───────────────────────┐
        │ 活跃 (Active)          │◀── channel(1) 逐条接收命令
        │ handle → persist → ack │
        └───────────┬───────────┘
                    │ 空闲超时 / Evict 消息
                    ▼
        ┌───────────────────────┐
        │ 退出 (Stopping)        │
        │ 保存快照               │
        │ 发送 ActorStopped      │
        └───────────┬───────────┘
                    │ Runtime 收到 Stopped，移除 active
                    ▼
              ┌──────────┐
              │  不存在   │
              └──────────┘
```

## 配置

```rust
pub struct RuntimeConfig {
    pub max_active: usize,              // 最大活跃聚合数量（默认 10000）
    pub idle_timeout: Duration,          // 空闲超时（默认 5 分钟）
    pub runtime_channel_capacity: usize, // Runtime 主 channel 容量（默认 1024）
    pub snapshot_threshold: u64,         // 快照阈值（默认 100 个事件）
    pub shutdown_timeout: Duration,      // 优雅关闭超时（默认 30 秒）
    pub command_send_timeout: Duration,  // 命令发送到 Actor 的超时（默认 15 秒）
    pub wasm_validate_timeout: Duration, // validate 调用超时（默认 5 秒）
    pub wasm_handle_timeout: Duration,   // handle 调用超时（默认 10 秒）
    pub wasm_fuel_limit: u64,            // WASM 单次调用 fuel 上限（默认 1_000_000）
}
```

注意：Actor channel 容量固定为 1，不可配置。这是架构约束而非调优参数。

## 错误处理

| 场景 | 处理方式 | 数据安全 |
|------|----------|----------|
| handle 失败（业务拒绝） | 返回领域错误，状态不变 | 安全 |
| persist 失败（DB 不可用） | 返回 503，内存状态不变 | 安全 |
| persist 成功但 apply 失败 | Actor 自杀，通知 Runtime 移除；下次访问从 DB 重建 | 安全 |
| Actor 崩溃 | 下次访问自动重新激活 | 安全 |
| 快照保存失败 | 仅告警，下次激活慢一些 | 安全 |
| Runtime channel 满 | 返回 503（try_send 立即失败），客户端重试 | 安全 |
| WASM 执行超时 | 返回 408，Actor 状态不变 | 安全 |
| Actor 被驱逐时有等待中的发送方 | SendError → 客户端收到重试提示 | 安全 |
| 命令发送到 Actor 超时 | 返回 408，客户端重试 | 安全 |
| 事件缺少 type 字段 | 返回错误，拒绝持久化，状态不变 | 安全 |
| 毒事件（apply-events 回放失败） | 返回 PoisonAggregate 错误，记录日志，需人工介入 | 安全（数据在 DB） |

## 设计优势（对比有 Buffer 的 Mailbox 方案）

| 维度 | 无 Buffer (channel=1) | 有 Buffer (mailbox=64) |
|------|----------------------|------------------------|
| 驱逐竞态 | 不存在（无排队命令） | 需要 drain/reject |
| 驱逐延迟 | 等待当前 1 条命令完成 | 等待最多 64 条命令排空 |
| 背压传递 | 直接传递到调用方 | 隐藏在 mailbox 中 |
| 内存占用 | 更低（无 buffer 开销） | 每 Actor 额外 buffer 内存 |
| 代码复杂度 | 低（无 drain/reject 逻辑） | 高（需处理排空/拒绝/中间状态） |
| 吞吐影响 | 同一聚合串行等待 | 同一聚合也是串行（buffer 只是缓冲） |
