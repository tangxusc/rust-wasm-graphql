// Virtual Actor Runtime（Actor 生命周期管理，LRU 驱逐，背压控制）

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::actor::{ActorMessage, VirtualActor, WasmEngineApi};
use crate::config::RuntimeConfig;
use crate::error::WorkerError;
use crate::event_store::EventStore;
use crate::types::{ActorKey, CommandResult, IncomingCommand};

/// Runtime 消息
#[derive(Debug)]
pub enum RuntimeMessage {
    /// 发送命令到指定聚合
    Send {
        command: IncomingCommand,
        reply: oneshot::Sender<Result<CommandResult, WorkerError>>,
    },
    /// Actor 确认已退出
    ActorStopped {
        key: ActorKey,
        generation: u64,
    },
    /// 优雅关闭
    Shutdown {
        ack: oneshot::Sender<()>,
    },
}

/// Actor 句柄（Runtime 持有）
struct ActorHandle {
    tx: mpsc::Sender<ActorMessage>,
    generation: u64,
    last_active: Arc<AtomicU64>,
}

impl ActorHandle {
    fn touch(&self) {
        self.last_active.store(now_ms(), Ordering::Relaxed);
    }
}

/// 外部调用方持有的句柄，通过 channel 与 Runtime 通信
#[derive(Clone)]
pub struct RuntimeHandle {
    tx: mpsc::Sender<RuntimeMessage>,
}

impl RuntimeHandle {
    /// 发送命令到 Runtime（try_send 实现背压）
    pub async fn send(&self, command: IncomingCommand) -> Result<CommandResult, WorkerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        match self.tx.try_send(RuntimeMessage::Send { command, reply: reply_tx }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(WorkerError::Overloaded("系统繁忙，请稍后重试".into()));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(WorkerError::Internal("Runtime 已关闭".into()));
            }
        }
        reply_rx.await.map_err(|_| WorkerError::Internal("Runtime 无响应".into()))?
    }

    /// 优雅关闭
    pub async fn shutdown(&self) {
        let (ack_tx, ack_rx) = oneshot::channel();
        let _ = self.tx.send(RuntimeMessage::Shutdown { ack: ack_tx }).await;
        let _ = ack_rx.await;
    }
}

/// Virtual Actor 运行时
pub struct VirtualActorRuntime {
    rx: mpsc::Receiver<RuntimeMessage>,
    handle: RuntimeHandle,
    active: HashMap<ActorKey, ActorHandle>,
    lru_index: BTreeMap<u64, ActorKey>,
    lru_reverse: HashMap<ActorKey, u64>,
    next_generation: u64,
    max_active: usize,
    evicting_count: usize,
    idle_timeout: Duration,
    command_send_timeout: Duration,
    shutdown_timeout: Duration,
    event_store: Arc<dyn EventStore>,
    wasm_engine: Arc<dyn WasmEngineApi>,
    config: RuntimeConfig,
}

impl VirtualActorRuntime {
    /// 启动 Runtime（返回 RuntimeHandle）
    pub fn spawn(
        config: RuntimeConfig,
        event_store: Arc<dyn EventStore>,
        wasm_engine: Arc<dyn WasmEngineApi>,
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
            shutdown_timeout: config.shutdown_timeout,
            event_store,
            wasm_engine,
            config: config.clone(),
        };
        tokio::spawn(runtime.run());
        handle
    }

    /// 主消息循环
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

    /// 命令路由：热路径直接发送，冷路径激活 Actor
    fn handle_send(
        &mut self,
        command: IncomingCommand,
        reply: oneshot::Sender<Result<CommandResult, WorkerError>>,
    ) {
        let key: ActorKey = (command.module.clone(), command.aggregate_id.clone());

        if let Some(handle) = self.active.get(&key) {
            // 热路径：Actor 已存在，先复制需要的数据避免借用冲突
            let actor_tx = handle.tx.clone();
            self.update_lru(&key);
            let send_timeout = self.command_send_timeout;
            tokio::spawn(async move {
                let msg = ActorMessage::Command { command, reply_tx: reply };
                match tokio::time::timeout(send_timeout, actor_tx.send(msg)).await {
                    Ok(Ok(())) => {} // Actor 会通过 reply_tx 返回结果
                    Ok(Err(e)) => {
                        let cmd_msg = e.0;
                        if let ActorMessage::Command { reply_tx, .. } = cmd_msg {
                            let _ = reply_tx.send(Err(WorkerError::ActorUnavailable(
                                "聚合暂不可用，请重试".into(),
                            )));
                        }
                    }
                    Err(_) => {
                        // 超时：客户端 HTTP 层会处理
                    }
                }
            });
        } else {
            // 冷路径：激活新 Actor
            self.spawn_activation(key, command, reply);
        }
    }

    /// Actor 确认退出后，安全移除
    fn handle_actor_stopped(&mut self, key: &ActorKey, generation: u64) {
        if let Some(handle) = self.active.get(key) {
            if handle.generation == generation {
                self.active.remove(key);
                if let Some(gen) = self.lru_reverse.remove(key) {
                    self.lru_index.remove(&gen);
                }
                self.evicting_count = self.evicting_count.saturating_sub(1);
            }
        }
    }

    /// 更新 LRU 索引（最近使用置顶）
    fn update_lru(&mut self, key: &ActorKey) {
        let gen = self.next_generation;
        self.next_generation += 1;
        if let Some(old_gen) = self.lru_reverse.insert(key.clone(), gen) {
            self.lru_index.remove(&old_gen);
        }
        self.lru_index.insert(gen, key.clone());
    }

    /// 激活新 Actor（恢复状态 + 处理首条命令 + 进入消息循环）
    fn spawn_activation(
        &mut self,
        key: ActorKey,
        first_command: IncomingCommand,
        reply: oneshot::Sender<Result<CommandResult, WorkerError>>,
    ) {
        // 内存预算管理
        while self.active.len() - self.evicting_count >= self.max_active {
            if !self.evict_one() {
                let _ = reply.send(Err(WorkerError::Overloaded("内存预算已满".into())));
                return;
            }
        }

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
        let runtime_tx = self.handle.tx.clone();
        let actor_config = self.config.clone();

        tokio::spawn(async move {
            let (ref module, ref aggregate_id) = key;

            // 检查聚合类型冲突
            if let Ok(true) = event_store.check_aggregate_type_conflict(aggregate_id, module).await {
                let _ = reply.send(Err(WorkerError::TypeConflict {
                    aggregate_id: aggregate_id.clone(),
                    module: module.clone(),
                    message: "该 aggregate_id 已被其他模块使用".into(),
                }));
                let _ = runtime_tx.send(RuntimeMessage::ActorStopped { key, generation }).await;
                return;
            }

            // 从快照 + 增量事件恢复状态
            let (state, version) = match recover_state(
                aggregate_id, module, &event_store, &wasm_engine,
            ).await {
                Ok(sv) => sv,
                Err(e) => {
                    let _ = reply.send(Err(e));
                    let _ = runtime_tx.send(RuntimeMessage::ActorStopped { key, generation }).await;
                    return;
                }
            };

            let mut actor = VirtualActor::new(
                key.clone(),
                state,
                version,
                wasm_engine,
                event_store,
                actor_rx,
                &actor_config,
                runtime_tx,
                generation,
            );

            // 处理首条命令
            let result = actor.process_command(first_command).await;
            let _ = reply.send(result);

            // 进入常规消息循环
            actor.run().await;
        });
    }

    /// 驱逐最旧 Actor
    fn evict_one(&mut self) -> bool {
        let candidate = self.lru_index.iter().next()
            .map(|(gen, key)| (*gen, key.clone()));

        if let Some((gen, key)) = candidate {
            if let Some(handle) = self.active.get(&key) {
                match handle.tx.try_send(ActorMessage::Evict) {
                    Ok(()) => {
                        self.lru_index.remove(&gen);
                        self.lru_reverse.remove(&key);
                        self.evicting_count += 1;
                        true
                    }
                    Err(_) => {
                        self.lru_index.remove(&gen);
                        self.lru_reverse.remove(&key);
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

    /// 优雅关闭
    async fn handle_shutdown(&mut self) {
        let deadline = Instant::now() + self.shutdown_timeout;

        // 向所有 Actor 发送 Evict
        for (_, handle) in &self.active {
            let _ = handle.tx.send(ActorMessage::Evict).await;
        }

        // 等待所有 ActorStopped 消息
        let remaining = self.active.len();
        let mut stopped = 0;
        while stopped < remaining {
            let remaining_time = deadline.saturating_duration_since(Instant::now());
            if remaining_time.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining_time, self.rx.recv()).await {
                Ok(Some(RuntimeMessage::ActorStopped { key, generation })) => {
                    self.handle_actor_stopped(&key, generation);
                    stopped += 1;
                }
                _ => break,
            }
        }
    }
}

/// 从快照和增量事件恢复聚合状态
async fn recover_state(
    aggregate_id: &str,
    module: &str,
    event_store: &Arc<dyn EventStore>,
    wasm_engine: &Arc<dyn WasmEngineApi>,
) -> Result<(Vec<u8>, u64), WorkerError> {
    let (base_state, from_version) = match event_store.load_snapshot(module, aggregate_id).await? {
        Some(snap) => (snap.state, snap.version),
        None => (vec![], 0),
    };

    let events = event_store.load_events_after(module, aggregate_id, from_version).await?;
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
        })
        .await
        .map_err(|e| WorkerError::Internal(format!("WASM 任务失败: {e}")))?;

        match apply_result {
            Ok(new_state) => new_state,
            Err(e) => {
                // 毒事件：apply-events 失败，聚合无法激活
                tracing::error!(
                    aggregate_id = aggregate_id,
                    module = module,
                    from_version = from_version,
                    "apply-events 失败，聚合可能包含毒事件: {e}"
                );
                return Err(WorkerError::PoisonAggregate(
                    crate::error::PoisonAggregateError {
                        aggregate_id: aggregate_id.to_string(),
                        module: module.to_string(),
                        from_version,
                        cause: e.to_string(),
                    },
                ));
            }
        }
    };

    Ok((state, version))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ===== 测试 =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::WasmEngineApi;
    use crate::event_store::EventStore;
    use crate::types::{DomainEvent, PendingEvent, Snapshot};
    use std::collections::HashMap;
    use std::sync::Mutex;

    // === Mock 实现 ===

    struct MockEventStore {
        events: Mutex<HashMap<String, Vec<PendingEvent>>>,
        snapshots: Mutex<HashMap<String, Snapshot>>,
        type_conflict: Mutex<bool>,
    }

    impl MockEventStore {
        fn new() -> Self {
            Self {
                events: Mutex::new(HashMap::new()),
                snapshots: Mutex::new(HashMap::new()),
                type_conflict: Mutex::new(false),
            }
        }

        fn set_type_conflict(&self, v: bool) { *self.type_conflict.lock().unwrap() = v; }
    }

    #[async_trait::async_trait]
    impl EventStore for MockEventStore {
        async fn append(&self, _aggregate_id: &str, events: &[PendingEvent], _expected_version: u64) -> Result<(), crate::error::StoreError> {
            let mut map = self.events.lock().unwrap();
            map.entry(_aggregate_id.to_string()).or_default().extend(events.to_vec());
            Ok(())
        }

        async fn load_events_after(&self, _aggregate_type: &str, _aggregate_id: &str, _after_version: u64) -> Result<Vec<DomainEvent>, crate::error::StoreError> {
            Ok(vec![])
        }

        async fn get_current_version(&self, _aggregate_type: &str, _aggregate_id: &str) -> Result<Option<u64>, crate::error::StoreError> {
            Ok(Some(0))
        }

        async fn check_aggregate_type_conflict(&self, _aggregate_id: &str, _expected_type: &str) -> Result<bool, crate::error::StoreError> {
            Ok(*self.type_conflict.lock().unwrap())
        }

        async fn load_snapshot(&self, _aggregate_type: &str, _aggregate_id: &str) -> Result<Option<Snapshot>, crate::error::StoreError> {
            Ok(None)
        }

        async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(), crate::error::StoreError> {
            let mut map = self.snapshots.lock().unwrap();
            map.insert(snapshot.aggregate_id.clone(), snapshot.clone());
            Ok(())
        }
    }

    struct MockWasmEngine;

    impl WasmEngineApi for MockWasmEngine {
        fn call_validate(&self, _module: &str, _fn_name: &str, _command: &[u8]) -> Result<(), WorkerError> {
            Ok(())
        }

        fn call_handle(&self, _module: &str, _fn_name: &str, _state: &[u8], _command: &[u8]) -> Result<Vec<Vec<u8>>, WorkerError> {
            Ok(vec![br#"{"type":"Tested","amount":1}"#.to_vec()])
        }

        fn call_apply_events(&self, _module: &str, _snapshot: &[u8], _events: &[&[u8]]) -> Result<Vec<u8>, WorkerError> {
            Ok(vec![1, 2, 3])
        }

        fn get_validate_fn(&self, _module: &str, _command_type: &str) -> Option<String> {
            None
        }
    }

    fn make_command(aggregate_id: &str, module: &str, expected_version: u64) -> IncomingCommand {
        IncomingCommand {
            aggregate_id: aggregate_id.to_string(),
            expected_version,
            module: module.to_string(),
            command_type: "increment".to_string(),
            data: br#"{"amount":1}"#.to_vec(),
        }
    }

    fn spawn_runtime(max_active: usize) -> RuntimeHandle {
        let config = RuntimeConfig { max_active, ..Default::default() };
        VirtualActorRuntime::spawn(
            config,
            Arc::new(MockEventStore::new()),
            Arc::new(MockWasmEngine),
        )
    }

    // === 测试用例 ===

    #[tokio::test]
    async fn test_send_to_preexisting_actor() {
        let handle = spawn_runtime(100);
        let cmd = make_command("agg-1", "counter", 0);
        // 第一次 send 激活 actor
        let r1 = handle.send(cmd).await;
        assert!(r1.is_ok(), "首次 send 应成功: {:?}", r1.err());
        let result = r1.unwrap();
        assert!(result.success);

        // 第二次 send 走热路径（同一聚合已在内存）
        let cmd2 = make_command("agg-1", "counter", result.version);
        let r2 = handle.send(cmd2).await;
        assert!(r2.is_ok(), "热路径 send 应成功: {:?}", r2.err());
        assert!(r2.unwrap().success);
    }

    #[tokio::test]
    async fn test_send_activates_cold_path() {
        let handle = spawn_runtime(100);
        let cmd = make_command("new-agg", "counter", 0);
        let result = handle.send(cmd).await.unwrap();
        assert!(result.success);
        assert_eq!(result.version, 1);
    }

    #[tokio::test]
    async fn test_type_conflict_during_activation() {
        let store = Arc::new(MockEventStore::new());
        store.set_type_conflict(true);

        let rt = VirtualActorRuntime::spawn(
            RuntimeConfig::default(),
            store,
            Arc::new(MockWasmEngine),
        );

        let cmd = make_command("conflict-agg", "counter", 0);
        let result = rt.send(cmd).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("已被其他模块使用") || err.contains("TypeConflict"));
    }

    #[tokio::test]
    async fn test_multiple_different_aggregates() {
        let handle = spawn_runtime(100);

        let r1 = handle.send(make_command("agg-a", "counter", 0)).await.unwrap();
        let r2 = handle.send(make_command("agg-b", "counter", 0)).await.unwrap();
        assert!(r1.success);
        assert!(r2.success);
    }

    #[tokio::test]
    async fn test_sequential_commands_same_aggregate() {
        let handle = spawn_runtime(100);

        let r1 = handle.send(make_command("seq-agg", "counter", 0)).await.unwrap();
        assert_eq!(r1.version, 1);

        let r2 = handle.send(make_command("seq-agg", "counter", 1)).await.unwrap();
        assert_eq!(r2.version, 2);
    }

    #[tokio::test]
    async fn test_max_active_enforcement() {
        // 设置 max_active=2，发送到 3 个不同聚合
        let handle = spawn_runtime(2);

        let _ = handle.send(make_command("a", "counter", 0)).await.unwrap();
        let _ = handle.send(make_command("b", "counter", 0)).await.unwrap();

        // 第三个需要驱逐一个旧的
        let r3 = handle.send(make_command("c", "counter", 0)).await;
        assert!(r3.is_ok() || r3.as_ref().unwrap().success);
    }

    #[tokio::test]
    async fn test_runtime_shutdown() {
        let config = RuntimeConfig::default();
        let rt = VirtualActorRuntime::spawn(
            config,
            Arc::new(MockEventStore::new()),
            Arc::new(MockWasmEngine),
        );

        // 发送一个命令以确保有活跃 actor
        let _ = rt.send(make_command("sd-agg", "counter", 0)).await.unwrap();

        rt.shutdown().await;
        // shutdown 返回即表示关闭完成
    }

    #[tokio::test]
    async fn test_version_conflict_via_runtime() {
        let handle = spawn_runtime(100);

        // 先执行一次
        let r1 = handle.send(make_command("vc-agg", "counter", 0)).await.unwrap();
        assert!(r1.success);

        // 用旧版本重试
        let r2 = handle.send(make_command("vc-agg", "counter", 0)).await.unwrap();
        assert!(!r2.success);
        assert!(r2.error.unwrap().contains("冲突"));
    }

    #[tokio::test]
    async fn test_lru_update_on_command() {
        let handle = spawn_runtime(5);

        // 发送多个不同聚合的命令以触发 LRU 更新
        for i in 0..10 {
            let cmd = make_command(&format!("lru-agg-{}", i), "counter", 0);
            let result = handle.send(cmd).await;
            assert!(result.is_ok(), "聚合 {}: {:?}", i, result.err());
        }
    }

    #[tokio::test]
    async fn test_runtime_handle_clone() {
        let handle = spawn_runtime(100);
        let handle2 = handle.clone();

        let r1 = handle.send(make_command("clone-a", "counter", 0)).await.unwrap();
        let r2 = handle2.send(make_command("clone-b", "counter", 0)).await.unwrap();
        assert!(r1.success);
        assert!(r2.success);
    }

    #[tokio::test]
    async fn test_empty_events_noop() {
        use crate::actor::WasmEngineApi;

        // 返回空事件的 mock engine
        struct NoopEngine;
        impl WasmEngineApi for NoopEngine {
            fn call_validate(&self, _: &str, _: &str, _: &[u8]) -> Result<(), WorkerError> { Ok(()) }
            fn call_handle(&self, _: &str, _: &str, _: &[u8], _: &[u8]) -> Result<Vec<Vec<u8>>, WorkerError> { Ok(vec![]) }
            fn call_apply_events(&self, _: &str, _: &[u8], _: &[&[u8]]) -> Result<Vec<u8>, WorkerError> { Ok(vec![]) }
            fn get_validate_fn(&self, _: &str, _: &str) -> Option<String> { None }
        }

        let rt = VirtualActorRuntime::spawn(
            RuntimeConfig::default(),
            Arc::new(MockEventStore::new()),
            Arc::new(NoopEngine),
        );

        let cmd = make_command("noop-agg", "counter", 0);
        let result = rt.send(cmd).await.unwrap();
        assert!(result.success);
        assert_eq!(result.event_count, 0);
    }

    #[tokio::test]
    async fn test_max_active_with_eviction() {
        // 用很小的 max_active 强制驱逐
        let config = RuntimeConfig { max_active: 1, ..Default::default() };
        let rt = VirtualActorRuntime::spawn(
            config,
            Arc::new(MockEventStore::new()),
            Arc::new(MockWasmEngine),
        );

        // 连续创建新聚合会触发 LRU 驱逐
        for i in 0..5 {
            let cmd = make_command(&format!("ev-agg-{}", i), "counter", 0);
            let result = rt.send(cmd).await;
            assert!(result.is_ok(), "聚合 {} 应成功: {:?}", i, result.err());
        }
    }

    #[tokio::test]
    async fn test_runtime_overloaded_channel_full() {
        // 使用极小 channel 容量制造背压
        let config = RuntimeConfig {
            runtime_channel_capacity: 1,
            max_active: 100,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(1);
        // 填满 channel
        let (reply_tx, _reply_rx) = oneshot::channel();
        tx.try_send(RuntimeMessage::Send {
            command: make_command("fill", "counter", 0),
            reply: reply_tx,
        }).unwrap();

        // 创建 RuntimeHandle 并尝试发送（channel 已满）
        let handle = RuntimeHandle { tx };
        let result = handle.send(make_command("over", "counter", 0)).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("繁忙"));
    }

    #[tokio::test]
    async fn test_runtime_closed_handle() {
        // 关闭 channel 后发送应失败
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let handle = RuntimeHandle { tx };
        let result = handle.send(make_command("closed", "counter", 0)).await;
        assert!(result.is_err());
    }

    // === 状态恢复测试 ===

    #[tokio::test]
    async fn test_recover_state_no_snapshot_no_events() {
        let store: Arc<dyn EventStore> = Arc::new(MockEventStore::new());
        let engine: Arc<dyn WasmEngineApi> = Arc::new(MockWasmEngine);
        let (state, version) = recover_state("agg-new", "counter", &store, &engine).await.unwrap();
        assert_eq!(state, Vec::<u8>::new());
        assert_eq!(version, 0);
    }

    // === RuntimeMessage 枚举 ===

    #[test]
    fn test_shutdown_message() {
        let (tx, _) = oneshot::channel();
        let _ = RuntimeMessage::Shutdown { ack: tx };
    }

    #[test]
    fn test_actor_stopped_message() {
        let _ = RuntimeMessage::ActorStopped {
            key: ("counter".into(), "agg".into()),
            generation: 42,
        };
    }

    #[test]
    fn test_send_message_debug() {
        let (tx, _) = oneshot::channel();
        let msg = RuntimeMessage::Send {
            command: make_command("deb", "counter", 0),
            reply: tx,
        };
        let _ = format!("{:?}", msg);
    }

    // === 更多运行时边缘测试 ===

    #[tokio::test]
    async fn test_handle_actor_stopped_wrong_generation() {
        // 通过发送两条命令来验证 ActorStopped + 重新激活的流程
        let handle = spawn_runtime(100);
        let _ = handle.send(make_command("gen-test", "counter", 0)).await.unwrap();
        // Actor 应该仍然存在（热路径）
        let r2 = handle.send(make_command("gen-test", "counter", 1)).await.unwrap();
        assert!(r2.success);
    }

    #[tokio::test]
    async fn test_send_to_actor_timeout() {
        // Actor channel 容量=1，Actor 忙时第二个命令会触发 timeout
        let handle = spawn_runtime(100);
        let cmd1 = make_command("timeout-test", "counter", 0);
        // 快速连续发送两个命令到同一聚合
        let _r1 = handle.send(cmd1).await;
        // 由于 Actor 在处理 cmd1，cmd2 可能需要等待（channel 容量=1）
        let cmd2 = make_command("timeout-test", "counter", 0);
        let r2 = handle.send(cmd2).await;
        // 这可能是错误（版本冲突）或成功，取决于时序
        // 关键是不应该 panic
        let _ = r2;
    }

    #[tokio::test]
    async fn test_validate_call_during_activation() {
        // 测试带 validate 的命令路径
        use crate::actor::WasmEngineApi;

        struct ValidateEngine;
        impl WasmEngineApi for ValidateEngine {
            fn call_validate(&self, _: &str, _: &str, _: &[u8]) -> Result<(), WorkerError> {
                // validate 失败
                Err(WorkerError::WasmExecution("validate 拒绝".into()))
            }
            fn call_handle(&self, _: &str, _: &str, _: &[u8], _: &[u8]) -> Result<Vec<Vec<u8>>, WorkerError> {
                Ok(vec![])
            }
            fn call_apply_events(&self, _: &str, _: &[u8], _: &[&[u8]]) -> Result<Vec<u8>, WorkerError> {
                Ok(vec![])
            }
            fn get_validate_fn(&self, _: &str, _: &str) -> Option<String> {
                Some("validate-increment".into())
            }
        }

        let rt = VirtualActorRuntime::spawn(
            RuntimeConfig::default(),
            Arc::new(MockEventStore::new()),
            Arc::new(ValidateEngine),
        );

        let cmd = make_command("validate-fail", "counter", 0);
        let result = rt.send(cmd).await;
        // validate 失败应该导致命令失败
        assert!(result.is_err() || !result.unwrap().success);
    }

    #[tokio::test]
    async fn test_shutdown_no_actors() {
        // 没有活跃 actor 时也能正常关闭
        let config = RuntimeConfig::default();
        let rt = VirtualActorRuntime::spawn(
            config,
            Arc::new(MockEventStore::new()),
            Arc::new(MockWasmEngine),
        );
        rt.shutdown().await;
    }

    #[tokio::test]
    async fn test_multiple_modules_different_aggregates() {
        let handle = spawn_runtime(100);
        // 不同模块的同一 aggregate_id 是独立的 actor
        let r1 = handle.send(make_command("shared-id", "module-a", 0)).await.unwrap();
        let r2 = handle.send(make_command("shared-id", "module-b", 0)).await.unwrap();
        assert!(r1.success);
        assert!(r2.success);
    }

    // === ActorHandle 测试 ===

    #[test]
    fn test_actor_handle_touch() {
        let (tx, _rx) = mpsc::channel::<ActorMessage>(1);
        let handle = ActorHandle {
            tx,
            generation: 1,
            last_active: Arc::new(AtomicU64::new(0)),
        };
        handle.touch();
        let val = handle.last_active.load(Ordering::Relaxed);
        assert!(val > 0, "touch() 应更新 last_active");
    }

    // === Runtime 错误路径测试 ===

    #[tokio::test]
    async fn test_runtime_channel_closed_error() {
        // 当 receiver 被 drop 时 send 应返回错误
        let (tx, rx) = mpsc::channel::<RuntimeMessage>(1);
        let handle = RuntimeHandle { tx };
        drop(rx); // 关闭接收端，使 send 返回 Closed
        let result = handle.send(make_command("closed2", "counter", 0)).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("已关闭"));
    }

}

