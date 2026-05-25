// 错误类型定义

use std::fmt;

/// 毒事件错误：聚合因事件回放失败而无法激活
#[derive(Debug, Clone)]
pub struct PoisonAggregateError {
    pub aggregate_id: String,
    pub module: String,
    pub from_version: u64,
    pub cause: String,
}

impl fmt::Display for PoisonAggregateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "聚合 {}/{} 在版本 {} 处回放事件失败: {}",
            self.module, self.aggregate_id, self.from_version, self.cause
        )
    }
}

/// 事件存储错误
#[derive(Debug, Clone)]
pub enum StoreError {
    /// 版本冲突（乐观并发控制）
    VersionConflict { aggregate_id: String, expected_version: u64 },
    /// 数据库连接错误
    ConnectionError(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VersionConflict { aggregate_id, expected_version } => {
                write!(f, "版本冲突: aggregate_id={}, expected_version={}", aggregate_id, expected_version)
            }
            Self::ConnectionError(msg) => write!(f, "数据库错误: {}", msg),
        }
    }
}

impl std::error::Error for StoreError {}

/// Worker 主要错误类型
#[derive(Debug)]
pub enum WorkerError {
    /// 版本冲突（乐观并发控制）
    VersionConflict { current_version: u64, expected_version: u64 },
    /// 系统过载（channel 满）
    Overloaded(String),
    /// Actor 暂不可用
    ActorUnavailable(String),
    /// 聚合类型冲突（aggregate_id 被其他模块占用）
    TypeConflict { aggregate_id: String, module: String, message: String },
    /// 毒事件：聚合无法激活
    PoisonAggregate(PoisonAggregateError),
    /// 模块未找到
    ModuleNotFound(String),
    /// 命令未找到
    CommandNotFound { module: String, command: String },
    /// 无效事件
    InvalidEvent(String),
    /// 无效输入
    InvalidInput(String),
    /// 缺少字段
    MissingField(String),
    /// WASM 执行错误
    WasmExecution(String),
    /// 超时
    Timeout(String),
    /// 内部错误
    Internal(String),
    /// 事件存储错误
    Store(StoreError),
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VersionConflict { current_version, expected_version } => {
                write!(f, "版本冲突: 当前={}, 期望={}", current_version, expected_version)
            }
            Self::Overloaded(msg) => write!(f, "系统过载: {}", msg),
            Self::ActorUnavailable(msg) => write!(f, "Actor 不可用: {}", msg),
            Self::TypeConflict { aggregate_id, module, message } => {
                write!(f, "类型冲突: aggregate_id={}, module={}, {}", aggregate_id, module, message)
            }
            Self::PoisonAggregate(e) => write!(f, "毒事件: {}", e),
            Self::ModuleNotFound(name) => write!(f, "模块未找到: {}", name),
            Self::CommandNotFound { module, command } => write!(f, "命令未找到: {}.{}", module, command),
            Self::InvalidEvent(msg) => write!(f, "无效事件: {}", msg),
            Self::InvalidInput(msg) => write!(f, "无效输入: {}", msg),
            Self::MissingField(name) => write!(f, "缺少字段: {}", name),
            Self::WasmExecution(msg) => write!(f, "WASM 执行错误: {}", msg),
            Self::Timeout(msg) => write!(f, "超时: {}", msg),
            Self::Internal(msg) => write!(f, "内部错误: {}", msg),
            Self::Store(e) => write!(f, "存储错误: {}", e),
        }
    }
}

impl std::error::Error for WorkerError {}

impl From<StoreError> for WorkerError {
    fn from(e: StoreError) -> Self {
        WorkerError::Store(e)
    }
}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        StoreError::ConnectionError(e.to_string())
    }
}

impl From<sqlx::Error> for WorkerError {
    fn from(e: sqlx::Error) -> Self {
        WorkerError::Store(StoreError::ConnectionError(e.to_string()))
    }
}

// ===== 测试 =====

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_conflict_formatting() {
        let err = WorkerError::VersionConflict { current_version: 10, expected_version: 9 };
        let msg = err.to_string();
        assert!(msg.contains("版本冲突"));
        assert!(msg.contains("10"));
        assert!(msg.contains("9"));
    }

    #[test]
    fn test_overloaded_formatting() {
        let err = WorkerError::Overloaded("channel 已满".into());
        assert!(err.to_string().contains("系统过载"));
    }

    #[test]
    fn test_actor_unavailable_formatting() {
        let err = WorkerError::ActorUnavailable("test".into());
        assert!(err.to_string().contains("Actor 不可用"));
    }

    #[test]
    fn test_type_conflict_formatting() {
        let err = WorkerError::TypeConflict {
            aggregate_id: "agg-1".into(),
            module: "counter".into(),
            message: "已被占用".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("类型冲突"));
        assert!(msg.contains("agg-1"));
    }

    #[test]
    fn test_module_not_found_formatting() {
        let err = WorkerError::ModuleNotFound("test-mod".into());
        assert!(err.to_string().contains("模块未找到"));
        assert!(err.to_string().contains("test-mod"));
    }

    #[test]
    fn test_command_not_found_formatting() {
        let err = WorkerError::CommandNotFound { module: "counter".into(), command: "noop".into() };
        let msg = err.to_string();
        assert!(msg.contains("命令未找到"));
        assert!(msg.contains("counter"));
        assert!(msg.contains("noop"));
    }

    #[test]
    fn test_invalid_event_formatting() {
        let err = WorkerError::InvalidEvent("bad format".into());
        assert!(err.to_string().contains("无效事件"));
        assert!(err.to_string().contains("bad format"));
    }

    #[test]
    fn test_invalid_input_formatting() {
        let err = WorkerError::InvalidInput("wrong".into());
        assert!(err.to_string().contains("无效输入"));
        assert!(err.to_string().contains("wrong"));
    }

    #[test]
    fn test_missing_field_formatting() {
        let err = WorkerError::MissingField("name".into());
        assert!(err.to_string().contains("缺少字段"));
        assert!(err.to_string().contains("name"));
    }

    #[test]
    fn test_wasm_execution_formatting() {
        let err = WorkerError::WasmExecution("panic".into());
        assert!(err.to_string().contains("WASM 执行错误"));
        assert!(err.to_string().contains("panic"));
    }

    #[test]
    fn test_timeout_formatting() {
        let err = WorkerError::Timeout("handle timeout".into());
        assert!(err.to_string().contains("超时"));
        assert!(err.to_string().contains("handle timeout"));
    }

    #[test]
    fn test_internal_formatting() {
        let err = WorkerError::Internal("something broke".into());
        assert!(err.to_string().contains("内部错误"));
        assert!(err.to_string().contains("something broke"));
    }

    #[test]
    fn test_store_error_display() {
        let err = WorkerError::Store(StoreError::ConnectionError("db down".into()));
        assert!(err.to_string().contains("存储错误"));
        assert!(err.to_string().contains("db down"));
    }

    #[test]
    fn test_poison_aggregate_formatting() {
        let poison = PoisonAggregateError {
            aggregate_id: "agg-1".into(),
            module: "counter".into(),
            from_version: 5,
            cause: "WASM panic".into(),
        };
        let msg = poison.to_string();
        assert!(msg.contains("agg-1"));
        assert!(msg.contains("counter"));
        assert!(msg.contains("5"));
    }

    #[test]
    fn test_store_error_version_conflict() {
        let err = StoreError::VersionConflict { aggregate_id: "agg-1".into(), expected_version: 3 };
        assert!(err.to_string().contains("agg-1"));
        assert!(err.to_string().contains("3"));
    }

    #[test]
    fn test_store_error_connection() {
        let err = StoreError::ConnectionError("timeout".into());
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_worker_error_from_store_error() {
        let store_err = StoreError::ConnectionError("test".into());
        let worker_err: WorkerError = store_err.into();
        matches!(worker_err, WorkerError::Store(_));
    }

    #[test]
    fn test_poison_aggregate_display() {
        let err = WorkerError::PoisonAggregate(PoisonAggregateError {
            aggregate_id: "a".into(),
            module: "m".into(),
            from_version: 0,
            cause: "c".into(),
        });
        assert!(err.to_string().contains("毒事件"));
    }

    #[test]
    fn test_store_error_is_std_error() {
        let err = StoreError::ConnectionError("test".into());
        // StoreError should implement std::error::Error
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_worker_error_is_std_error() {
        let err = WorkerError::Internal("test".into());
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_worker_error_clone() {
        let poison = PoisonAggregateError {
            aggregate_id: "a".into(), module: "m".into(),
            from_version: 0, cause: "c".into(),
        };
        let err = WorkerError::PoisonAggregate(poison.clone());
        // verify clone works
        let _cloned = format!("{:?}", err);
    }

    #[test]
    fn test_store_error_clone() {
        let err = StoreError::VersionConflict { aggregate_id: "x".into(), expected_version: 1 };
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }
}
