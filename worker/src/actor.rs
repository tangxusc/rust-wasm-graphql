// Virtual Actor（单聚合状态管理）

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::config::RuntimeConfig;
use crate::error::WorkerError;
use crate::event_store::EventStore;
use crate::types::{ActorKey, CommandResult, IncomingCommand, PendingEvent, Snapshot};

/// Actor 消息
pub enum ActorMessage {
    /// 命令消息
    Command {
        command: IncomingCommand,
        reply_tx: oneshot::Sender<Result<CommandResult, WorkerError>>,
    },
    /// 驱逐信号（Actor 完成当前命令后退出）
    Evict,
}

/// WASM 引擎接口（抽象，便于测试）
pub trait WasmEngineApi: Send + Sync {
    fn call_validate(&self, module: &str, fn_name: &str, command: &[u8]) -> Result<(), WorkerError>;
    fn call_handle(&self, module: &str, fn_name: &str, state: &[u8], command: &[u8]) -> Result<Vec<Vec<u8>>, WorkerError>;
    fn call_apply_events(&self, module: &str, snapshot: &[u8], events: &[&[u8]]) -> Result<Vec<u8>, WorkerError>;
    fn get_validate_fn(&self, module: &str, command_type: &str) -> Option<String>;
}

/// Virtual Actor：单聚合状态管理和命令处理
pub struct VirtualActor {
    pub key: ActorKey,
    pub state: Vec<u8>,
    pub version: u64,
    wasm_engine: Arc<dyn WasmEngineApi>,
    event_store: Arc<dyn EventStore>,
    rx: mpsc::Receiver<ActorMessage>,
    idle_timeout: std::time::Duration,
    snapshot_threshold: u64,
    last_snapshot_version: u64,
    runtime_tx: mpsc::Sender<crate::runtime::RuntimeMessage>,
    generation: u64,
    validate_timeout: std::time::Duration,
    handle_timeout: std::time::Duration,
    last_active: std::time::Instant,
}

impl VirtualActor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: ActorKey,
        state: Vec<u8>,
        version: u64,
        wasm_engine: Arc<dyn WasmEngineApi>,
        event_store: Arc<dyn EventStore>,
        rx: mpsc::Receiver<ActorMessage>,
        config: &RuntimeConfig,
        runtime_tx: mpsc::Sender<crate::runtime::RuntimeMessage>,
        generation: u64,
    ) -> Self {
        Self {
            key,
            state,
            version,
            wasm_engine,
            event_store,
            rx,
            idle_timeout: config.idle_timeout,
            snapshot_threshold: config.snapshot_threshold,
            last_snapshot_version: version,
            runtime_tx,
            generation,
            validate_timeout: config.wasm_validate_timeout,
            handle_timeout: config.wasm_handle_timeout,
            last_active: std::time::Instant::now(),
        }
    }

    /// 消息循环：逐条处理命令
    pub async fn run(&mut self) {
        let mut idle_check = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(ActorMessage::Command { command, reply_tx }) => {
                            self.last_active = std::time::Instant::now();
                            let result = self.process_command(command).await;
                            let _ = reply_tx.send(result);
                        }
                        Some(ActorMessage::Evict) => {
                            self.on_deactivate().await;
                            self.notify_stopped().await;
                            return;
                        }
                        None => {
                            // channel 关闭
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
        self.last_active.elapsed() > self.idle_timeout
    }

    /// 处理单条命令：handle → persist → ack → apply → respond
    pub async fn process_command(&mut self, cmd: IncomingCommand) -> Result<CommandResult, WorkerError> {
        // 1. 版本校验（乐观并发控制）
        if cmd.expected_version != self.version {
            return Ok(CommandResult::err(
                self.version,
                format!("版本冲突: 当前={}, 期望={}", self.version, cmd.expected_version),
            ));
        }

        // 2. 可选前置校验
        let (ref module, ref aggregate_id) = self.key;
        if let Some(validate_fn) = self.wasm_engine.get_validate_fn(module, &cmd.command_type) {
            let engine = self.wasm_engine.clone();
            let module_name = module.clone();
            let data = cmd.data.clone();
            let timeout_dur = self.validate_timeout;

            tokio::time::timeout(timeout_dur, tokio::task::spawn_blocking(move || {
                engine.call_validate(&module_name, &validate_fn, &data)
            }))
            .await
            .map_err(|_| WorkerError::Timeout("validate 执行超时".into()))?
            .map_err(|e| WorkerError::Internal(format!("WASM 任务失败: {e}")))??;
        }

        // 3. WASM handle-X
        let handle_fn = format!("handle-{}", cmd.command_type);
        let engine = self.wasm_engine.clone();
        let module_name = module.clone();
        let state = self.state.clone();
        let data = cmd.data.clone();
        let timeout_dur = self.handle_timeout;

        let new_events: Vec<Vec<u8>> = tokio::time::timeout(timeout_dur, tokio::task::spawn_blocking(move || {
            engine.call_handle(&module_name, &handle_fn, &state, &data)
        }))
        .await
        .map_err(|_| WorkerError::Timeout("handle 执行超时".into()))?
        .map_err(|e| WorkerError::Internal(format!("WASM 任务失败: {e}")))??;

        // 无事件 → noop
        if new_events.is_empty() {
            return Ok(CommandResult::noop(self.version));
        }

        // 4. 构建待持久化事件并同步写入
        let events_to_persist: Vec<PendingEvent> = new_events
            .iter()
            .enumerate()
            .map(|(i, data)| {
                let event: Value = serde_json::from_slice(data)
                    .map_err(|e| WorkerError::InvalidEvent(format!("事件非合法 JSON: {e}")))?;
                let event_type = event["type"]
                    .as_str()
                    .ok_or_else(|| WorkerError::InvalidEvent(format!("事件缺少 type 字段（索引 {}）", i)))?
                    .to_string();
                Ok(PendingEvent {
                    aggregate_id: aggregate_id.clone(),
                    aggregate_type: module.clone(),
                    event_type,
                    version: self.version + 1 + i as u64,
                    data: data.clone(),
                })
            })
            .collect::<Result<Vec<_>, WorkerError>>()?;

        self.event_store.append(aggregate_id, &events_to_persist, self.version).await?;

        // 5. 更新内存状态（persist 已成功）
        let engine = self.wasm_engine.clone();
        let module_name = module.clone();
        let current_state = self.state.clone();
        let events_for_apply = new_events.clone();

        let apply_result = tokio::task::spawn_blocking(move || {
            let refs: Vec<&[u8]> = events_for_apply.iter().map(|e| e.as_slice()).collect();
            engine.call_apply_events(&module_name, &current_state, &refs)
        }).await;

        match apply_result {
            Ok(Ok(new_state)) => {
                self.state = new_state;
                self.version += new_events.len() as u64;
            }
            _ => {
                // apply 失败：事件已持久化，内存状态不可信，Actor 必须退出
                self.notify_stopped().await;
                return Err(WorkerError::Internal(
                    "apply-events 失败，事件已持久化，Actor 将重新激活".into(),
                ));
            }
        }

        // 6. 异步快照（不阻塞响应）
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

    async fn on_deactivate(&self) {
        let snapshot = Snapshot {
            aggregate_id: self.key.1.clone(),
            aggregate_type: self.key.0.clone(),
            version: self.version,
            state: self.state.clone(),
        };
        let _ = self.event_store.save_snapshot(&snapshot).await;
    }

    async fn notify_stopped(&self) {
        let _ = self.runtime_tx.send(crate::runtime::RuntimeMessage::ActorStopped {
            key: self.key.clone(),
            generation: self.generation,
        }).await;
    }
}

// ===== 测试用的 Mock =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StoreError;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MockEventStore {
        events: Mutex<HashMap<String, Vec<PendingEvent>>>,
        snapshots: Mutex<HashMap<String, Snapshot>>,
        append_fail: Mutex<bool>,
    }

    impl MockEventStore {
        fn new() -> Self {
            Self {
                events: Mutex::new(HashMap::new()),
                snapshots: Mutex::new(HashMap::new()),
                append_fail: Mutex::new(false),
            }
        }

        fn set_append_fail(&self, fail: bool) {
            *self.append_fail.lock().unwrap() = fail;
        }
    }

    #[async_trait::async_trait]
    impl EventStore for MockEventStore {
        async fn append(&self, _aggregate_id: &str, events: &[PendingEvent], _expected_version: u64) -> Result<(), StoreError> {
            if *self.append_fail.lock().unwrap() {
                return Err(StoreError::ConnectionError("mock failure".into()));
            }
            let mut map = self.events.lock().unwrap();
            let key = _aggregate_id.to_string();
            map.entry(key).or_default().extend(events.to_vec());
            Ok(())
        }

        async fn load_events_after(&self, _aggregate_type: &str, _aggregate_id: &str, _after_version: u64) -> Result<Vec<crate::types::DomainEvent>, StoreError> {
            Ok(vec![])
        }

        async fn get_current_version(&self, _aggregate_type: &str, aggregate_id: &str) -> Result<Option<u64>, StoreError> {
            let map = self.events.lock().unwrap();
            let max = map.get(aggregate_id)
                .and_then(|events| events.iter().map(|e| e.version).max());
            Ok(max)
        }

        async fn check_aggregate_type_conflict(&self, _aggregate_id: &str, _expected_type: &str) -> Result<bool, StoreError> {
            Ok(false)
        }

        async fn load_snapshot(&self, _aggregate_type: &str, aggregate_id: &str) -> Result<Option<Snapshot>, StoreError> {
            let map = self.snapshots.lock().unwrap();
            Ok(map.get(aggregate_id).cloned())
        }

        async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(), StoreError> {
            let mut map = self.snapshots.lock().unwrap();
            map.insert(snapshot.aggregate_id.clone(), snapshot.clone());
            Ok(())
        }
    }

    struct MockWasmEngine {
        handle_results: Mutex<HashMap<String, Result<Vec<Vec<u8>>, WorkerError>>>,
        validate_results: Mutex<HashMap<String, Result<(), WorkerError>>>,
        apply_results: Mutex<HashMap<String, Result<Vec<u8>, WorkerError>>>,
        validate_fns: Mutex<HashMap<String, Option<String>>>,
    }

    impl MockWasmEngine {
        fn new() -> Self {
            Self {
                handle_results: Mutex::new(HashMap::new()),
                validate_results: Mutex::new(HashMap::new()),
                apply_results: Mutex::new(HashMap::new()),
                validate_fns: Mutex::new(HashMap::new()),
            }
        }

        fn set_handle(&self, key: &str, result: Result<Vec<Vec<u8>>, WorkerError>) {
            self.handle_results.lock().unwrap().insert(key.to_string(), result);
        }

        fn set_validate(&self, key: &str, result: Result<(), WorkerError>) {
            self.validate_results.lock().unwrap().insert(key.to_string(), result);
        }

        fn set_apply(&self, key: &str, result: Result<Vec<u8>, WorkerError>) {
            self.apply_results.lock().unwrap().insert(key.to_string(), result);
        }

        fn set_validate_fn(&self, cmd: &str, fn_name: Option<String>) {
            self.validate_fns.lock().unwrap().insert(cmd.to_string(), fn_name);
        }
    }

    impl WasmEngineApi for MockWasmEngine {
        fn call_validate(&self, _module: &str, fn_name: &str, _command: &[u8]) -> Result<(), WorkerError> {
            self.validate_results.lock().unwrap().remove(fn_name).unwrap_or(Ok(()))
        }

        fn call_handle(&self, _module: &str, fn_name: &str, _state: &[u8], _command: &[u8]) -> Result<Vec<Vec<u8>>, WorkerError> {
            self.handle_results.lock().unwrap().remove(fn_name).unwrap_or(Ok(vec![]))
        }

        fn call_apply_events(&self, _module: &str, _snapshot: &[u8], _events: &[&[u8]]) -> Result<Vec<u8>, WorkerError> {
            self.apply_results.lock().unwrap().remove("apply-events").unwrap_or(Ok(vec![]))
        }

        fn get_validate_fn(&self, _module: &str, command_type: &str) -> Option<String> {
            self.validate_fns.lock().unwrap().get(command_type).cloned().unwrap_or(None)
        }
    }

    fn make_command(aggregate_id: &str, expected_version: u64, command_type: &str) -> IncomingCommand {
        IncomingCommand {
            aggregate_id: aggregate_id.to_string(),
            expected_version,
            module: "counter".to_string(),
            command_type: command_type.to_string(),
            data: br#"{"amount":5}"#.to_vec(),
        }
    }

    fn make_event_json(event_type: &str, extra: &str) -> Vec<u8> {
        format!(r#"{{"type":"{}",{}}}"#, event_type, extra).into_bytes()
    }

    fn setup_actor() -> (VirtualActor, mpsc::Sender<ActorMessage>, mpsc::Receiver<crate::runtime::RuntimeMessage>) {
        let (actor_tx, actor_rx) = mpsc::channel(1);
        let (runtime_tx, runtime_rx) = mpsc::channel(64);
        let config = RuntimeConfig::default();

        let actor = VirtualActor::new(
            ("counter".into(), "agg-1".into()),
            vec![],
            0,
            Arc::new(MockWasmEngine::new()),
            Arc::new(MockEventStore::new()),
            actor_rx,
            &config,
            runtime_tx,
            1,
        );

        (actor, actor_tx, runtime_rx)
    }

    #[tokio::test]
    async fn test_version_conflict_rejected() {
        let (mut actor, _, _) = setup_actor();
        actor.version = 5;
        let cmd = make_command("agg-1", 3, "increment");
        let result = actor.process_command(cmd).await.unwrap();
        assert!(!result.success);
        assert!(result.error.unwrap().contains("版本冲突"));
    }

    #[tokio::test]
    async fn test_handle_no_events_noop() {
        let (mut actor, _, _) = setup_actor();
        let cmd = make_command("agg-1", 0, "increment");
        // 默认 mock 返回空 events，即 noop
        let result = actor.process_command(cmd).await.unwrap();
        assert!(result.success);
        assert_eq!(result.event_count, 0);
        assert_eq!(result.version, 0);
    }

    #[tokio::test]
    async fn test_handle_persist_and_apply() {
        let (mut actor, _, _) = setup_actor();
        let engine = Arc::new(MockWasmEngine::new());
        engine.set_handle("handle-increment", Ok(vec![make_event_json("Incremented", r#""amount":5"#)]));
        engine.set_apply("apply-events", Ok(br#"{"count":5}"#.to_vec()));
        actor.wasm_engine = engine;

        let cmd = make_command("agg-1", 0, "increment");
        let result = actor.process_command(cmd).await.unwrap();
        assert!(result.success);
        assert_eq!(result.version, 1);
        assert_eq!(result.event_count, 1);
        assert_eq!(actor.version, 1);
    }

    #[tokio::test]
    async fn test_validate_blocked() {
        let (mut actor, _, _) = setup_actor();
        let engine = Arc::new(MockWasmEngine::new());
        engine.set_validate_fn("increment", Some("validate-increment".into()));
        engine.set_validate("validate-increment", Err(WorkerError::WasmExecution("校验失败".into())));
        actor.wasm_engine = engine;

        let cmd = make_command("agg-1", 0, "increment");
        let result = actor.process_command(cmd).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("校验失败"));
    }

    #[tokio::test]
    async fn test_persist_failure_returns_error() {
        let (mut actor, _, _) = setup_actor();
        let engine = Arc::new(MockWasmEngine::new());
        engine.set_handle("handle-increment", Ok(vec![make_event_json("Incremented", r#""amount":5"#)]));
        actor.wasm_engine = engine;

        let store = Arc::new(MockEventStore::new());
        store.set_append_fail(true);
        actor.event_store = store;

        let cmd = make_command("agg-1", 0, "increment");
        let result = actor.process_command(cmd).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_event_missing_type_field() {
        let (mut actor, _, _) = setup_actor();
        let engine = Arc::new(MockWasmEngine::new());
        engine.set_handle("handle-increment", Ok(vec![br#"{"amount":5}"#.to_vec()]));
        actor.wasm_engine = engine;

        let cmd = make_command("agg-1", 0, "increment");
        let result = actor.process_command(cmd).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("缺少 type"));
    }

    #[tokio::test]
    async fn test_event_invalid_json() {
        let (mut actor, _, _) = setup_actor();
        let engine = Arc::new(MockWasmEngine::new());
        engine.set_handle("handle-increment", Ok(vec![b"not valid json".to_vec()]));
        actor.wasm_engine = engine;

        let cmd = make_command("agg-1", 0, "increment");
        let result = actor.process_command(cmd).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("非合法 JSON"));
    }

    #[tokio::test]
    async fn test_snapshot_threshold_triggered() {
        let (mut actor, _, runtime_rx) = setup_actor();
        actor.version = 99;
        actor.last_snapshot_version = 0;
        actor.snapshot_threshold = 100;

        let engine = Arc::new(MockWasmEngine::new());
        engine.set_handle("handle-increment", Ok(vec![make_event_json("Incremented", r#""amount":1"#)]));
        engine.set_apply("apply-events", Ok(br#"{"count":100}"#.to_vec()));
        actor.wasm_engine = engine;

        let cmd = make_command("agg-1", 99, "increment");
        let result = actor.process_command(cmd).await.unwrap();
        assert!(result.success);
        assert_eq!(actor.last_snapshot_version, 100);

        // 等待异步快照完成
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(runtime_rx);
    }

    #[tokio::test]
    async fn test_snapshot_below_threshold_not_triggered() {
        let (mut actor, _, _) = setup_actor();
        actor.version = 50;
        actor.last_snapshot_version = 0;
        actor.snapshot_threshold = 100;

        let engine = Arc::new(MockWasmEngine::new());
        engine.set_handle("handle-increment", Ok(vec![make_event_json("Incremented", r#""amount":1"#)]));
        engine.set_apply("apply-events", Ok(br#"{"count":51}"#.to_vec()));
        actor.wasm_engine = engine;

        let cmd = make_command("agg-1", 50, "increment");
        let result = actor.process_command(cmd).await.unwrap();
        assert!(result.success);
        // 未达到阈值，last_snapshot_version 不变
        assert_eq!(actor.last_snapshot_version, 0);
    }

    #[tokio::test]
    async fn test_evict_message_causes_deactivation() {
        let (mut actor, actor_tx, mut runtime_rx) = setup_actor();

        // 发送 Evict 消息
        let _ = actor_tx.try_send(ActorMessage::Evict);

        // 运行 actor（应该立即收到 Evict 并退出）
        tokio::time::timeout(std::time::Duration::from_secs(1), actor.run())
            .await
            .unwrap();

        // 验证发送了 ActorStopped
        let stopped = runtime_rx.try_recv();
        assert!(stopped.is_ok(), "应该发送 ActorStopped");
    }

    #[tokio::test]
    async fn test_run_with_command_then_evict() {
        let (mut actor, actor_tx, mut runtime_rx) = setup_actor();
        actor.idle_timeout = std::time::Duration::from_secs(3600); // 长时间，避免空闲触发退出

        // 在 spawn 中模拟发送命令
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = make_command("agg-1", 0, "increment");
        let _ = tokio::spawn(async move {
            let _ = actor_tx.send(ActorMessage::Command { command: cmd, reply_tx }).await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let _ = actor_tx.send(ActorMessage::Evict).await;
        });

        // 运行 actor 循环
        tokio::time::timeout(std::time::Duration::from_secs(2), actor.run())
            .await
            .unwrap();

        // 验证收到 ActorStopped
        let _ = runtime_rx.try_recv();

        // 验证命令已处理
        let result = reply_rx.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_apply_failure_after_persist_notifies_stopped() {
        let (mut actor, _, mut runtime_rx) = setup_actor();

        // handle 返回事件
        let engine = Arc::new(MockWasmEngine::new());
        engine.set_handle("handle-increment", Ok(vec![make_event_json("Incremented", r#""amount":1"#)]));
        // apply 失败
        engine.set_apply("apply-events", Err(WorkerError::WasmExecution("apply crash".into())));
        actor.wasm_engine = engine;

        let cmd = make_command("agg-1", 0, "increment");
        let result = actor.process_command(cmd).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("apply-events 失败"));

        // 应该收到了 ActorStopped
        assert!(runtime_rx.try_recv().is_ok(), "apply 失败后应发送 ActorStopped");
    }

    #[tokio::test]
    async fn test_multiple_events_in_single_command() {
        let (mut actor, _, _) = setup_actor();

        let engine = Arc::new(MockWasmEngine::new());
        let events = vec![
            make_event_json("EventA", r#""a":1"#),
            make_event_json("EventB", r#""b":2"#),
            make_event_json("EventC", r#""c":3"#),
        ];
        engine.set_handle("handle-increment", Ok(events));
        engine.set_apply("apply-events", Ok(br#"{"count":3}"#.to_vec()));
        actor.wasm_engine = engine;

        let cmd = make_command("agg-1", 0, "increment");
        let result = actor.process_command(cmd).await.unwrap();
        assert!(result.success);
        assert_eq!(result.event_count, 3);
        assert_eq!(actor.version, 3);
    }

    #[tokio::test]
    async fn test_noop_returns_version_unchanged() {
        let (mut actor, _, _) = setup_actor();
        actor.version = 5;

        // handle 返回空列表 → noop
        let engine = Arc::new(MockWasmEngine::new());
        engine.set_handle("handle-increment", Ok(vec![]));
        actor.wasm_engine = engine;

        let cmd = make_command("agg-1", 5, "increment");
        let result = actor.process_command(cmd).await.unwrap();
        assert!(result.success);
        assert_eq!(result.version, 5); // 版本不变
        assert_eq!(result.event_count, 0);
        assert_eq!(actor.version, 5); // actor 版本不变
    }

    #[tokio::test]
    async fn test_run_exits_on_channel_close() {
        let (mut actor, actor_tx, mut runtime_rx) = setup_actor();
        actor.idle_timeout = std::time::Duration::from_secs(3600);

        // drop sender 使得 rx.recv() 返回 None
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = make_command("agg-1", 0, "increment");
        let _ = actor_tx.send(ActorMessage::Command { command: cmd, reply_tx }).await;
        drop(actor_tx);

        tokio::time::timeout(std::time::Duration::from_secs(2), actor.run())
            .await
            .unwrap();

        // 验证收到 ActorStopped
        let stopped = runtime_rx.try_recv();
        assert!(stopped.is_ok(), "channel 关闭后应发送 ActorStopped");
        let _ = reply_rx;
    }

    #[tokio::test]
    async fn test_run_exits_on_idle_timeout() {
        let (mut actor, _actor_tx, mut runtime_rx) = setup_actor();
        // 设置超短空闲超时使其在首次 idle_check 时触发
        actor.last_active = std::time::Instant::now() - std::time::Duration::from_secs(1);
        actor.idle_timeout = std::time::Duration::from_millis(1);

        tokio::time::timeout(std::time::Duration::from_secs(10), actor.run())
            .await
            .unwrap();

        // 验证发送了 ActorStopped
        let stopped = runtime_rx.try_recv();
        assert!(stopped.is_ok(), "空闲超时后应发送 ActorStopped");
    }

    // === MockEventStore 方法直接测试（覆盖 trait 实现中的未覆盖分支）===

    #[tokio::test]
    async fn test_mock_store_load_events_after() {
        let store = MockEventStore::new();
        let result = store.load_events_after("counter", "agg-1", 0).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_mock_store_get_current_version() {
        let store = MockEventStore::new();
        let result = store.get_current_version("counter", "agg-1").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_mock_store_check_type_conflict() {
        let store = MockEventStore::new();
        let result = store.check_aggregate_type_conflict("agg-1", "counter").await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_mock_store_load_snapshot() {
        let store = MockEventStore::new();
        let result = store.load_snapshot("counter", "agg-1").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_mock_store_save_snapshot() {
        let store = MockEventStore::new();
        let snap = Snapshot {
            aggregate_id: "agg-1".into(),
            aggregate_type: "counter".into(),
            version: 1,
            state: vec![1, 2, 3],
        };
        store.save_snapshot(&snap).await.unwrap();
        let loaded = store.load_snapshot("counter", "agg-1").await.unwrap().unwrap();
        assert_eq!(loaded.version, 1);
    }
}
