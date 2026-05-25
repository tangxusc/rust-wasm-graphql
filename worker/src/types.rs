// 领域类型定义

use serde::{Deserialize, Serialize};

/// 客户端发送的命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingCommand {
    pub aggregate_id: String,
    pub expected_version: u64,
    pub module: String,
    pub command_type: String,
    pub data: Vec<u8>,
}

/// 命令处理结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult {
    pub success: bool,
    pub version: u64,
    pub event_count: usize,
    pub error: Option<String>,
}

impl CommandResult {
    /// 成功结果
    pub fn ok(version: u64, event_count: usize) -> Self {
        Self { success: true, version, event_count, error: None }
    }

    /// 无操作结果（无事件产生）
    pub fn noop(version: u64) -> Self {
        Self { success: true, version, event_count: 0, error: None }
    }

    /// 失败结果
    pub fn err(version: u64, error: impl Into<String>) -> Self {
        Self { success: false, version, event_count: 0, error: Some(error.into()) }
    }
}

/// 已持久化的领域事件（从数据库加载）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEvent {
    pub id: i64,
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub event_type: String,
    pub version: u64,
    pub data: Vec<u8>,
}

/// 待持久化的事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEvent {
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub event_type: String,
    pub version: u64,
    pub data: Vec<u8>,
}

/// 聚合快照
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub version: u64,
    pub state: Vec<u8>,
}

/// 聚合键：(模块名, aggregate_id)
pub type ActorKey = (String, String);

// ===== 测试 =====

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_result_ok() {
        let result = CommandResult::ok(5, 3);
        assert!(result.success);
        assert_eq!(result.version, 5);
        assert_eq!(result.event_count, 3);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_command_result_noop() {
        let result = CommandResult::noop(7);
        assert!(result.success);
        assert_eq!(result.version, 7);
        assert_eq!(result.event_count, 0);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_command_result_err() {
        let result = CommandResult::err(3, "版本冲突");
        assert!(!result.success);
        assert_eq!(result.version, 3);
        assert_eq!(result.event_count, 0);
        assert_eq!(result.error.unwrap(), "版本冲突");
    }

    #[test]
    fn test_incoming_command_serialization() {
        let cmd = IncomingCommand {
            aggregate_id: "agg-1".into(),
            expected_version: 3,
            module: "counter".into(),
            command_type: "increment".into(),
            data: br#"{"amount":5}"#.to_vec(),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let decoded: IncomingCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.aggregate_id, "agg-1");
        assert_eq!(decoded.expected_version, 3);
        assert_eq!(decoded.module, "counter");
        assert_eq!(decoded.command_type, "increment");
        assert_eq!(decoded.data, br#"{"amount":5}"#.as_slice());
    }

    #[test]
    fn test_domain_event_serialization() {
        let event = DomainEvent {
            id: 42,
            aggregate_id: "agg-1".into(),
            aggregate_type: "counter".into(),
            event_type: "Incremented".into(),
            version: 5,
            data: br#"{"type":"Incremented","amount":10}"#.to_vec(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let decoded: DomainEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, 42);
        assert_eq!(decoded.version, 5);
        assert_eq!(decoded.event_type, "Incremented");
    }

    #[test]
    fn test_pending_event_has_no_id() {
        let event = PendingEvent {
            aggregate_id: "agg-1".into(),
            aggregate_type: "counter".into(),
            event_type: "Incremented".into(),
            version: 1,
            data: vec![1, 2, 3],
        };
        // 验证 PendingEvent 没有 id/created_at 字段（与 DomainEvent 的区别）
        assert_eq!(event.aggregate_id, "agg-1");
        assert_eq!(event.version, 1);
    }

    #[test]
    fn test_snapshot_serialization() {
        let snap = Snapshot {
            aggregate_id: "agg-1".into(),
            aggregate_type: "counter".into(),
            version: 100,
            state: br#"{"count":50}"#.to_vec(),
        };
        let json = serde_json::to_string(&snap).unwrap();
        let decoded: Snapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.version, 100);
        assert_eq!(decoded.aggregate_type, "counter");
        assert_eq!(decoded.state, br#"{"count":50}"#);
    }
}
