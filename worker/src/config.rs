// 运行时配置

use std::time::Duration;

/// Virtual Actor 运行时配置
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// 最大活跃聚合数量
    pub max_active: usize,
    /// 空闲超时（超过此时间无命令则休眠）
    pub idle_timeout: Duration,
    /// Runtime 主 channel 容量
    pub runtime_channel_capacity: usize,
    /// 快照阈值（事件数量），达到后触发异步快照
    pub snapshot_threshold: u64,
    /// 优雅关闭超时
    pub shutdown_timeout: Duration,
    /// 命令发送到 Actor channel 的超时
    pub command_send_timeout: Duration,
    /// validate 调用超时
    pub wasm_validate_timeout: Duration,
    /// handle 调用超时
    pub wasm_handle_timeout: Duration,
    /// WASM 单次调用 fuel 上限（防止无限循环）
    pub wasm_fuel_limit: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_active: 10_000,
            idle_timeout: Duration::from_secs(5 * 60),
            runtime_channel_capacity: 1024,
            snapshot_threshold: 100,
            shutdown_timeout: Duration::from_secs(30),
            command_send_timeout: Duration::from_secs(15),
            wasm_validate_timeout: Duration::from_secs(5),
            wasm_handle_timeout: Duration::from_secs(10),
            wasm_fuel_limit: 1_000_000,
        }
    }
}

/// 事件存储连接配置
#[derive(Debug, Clone)]
pub struct EventStoreConfig {
    /// 数据库连接 URL
    pub database_url: String,
    /// 最大连接数
    pub max_connections: u32,
    /// 最小连接数
    pub min_connections: u32,
    /// 获取连接超时
    pub acquire_timeout: Duration,
    /// 语句超时
    pub statement_timeout: Duration,
}

impl Default for EventStoreConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://worker:worker@localhost:5432/worker_test".into(),
            max_connections: 32,
            min_connections: 4,
            acquire_timeout: Duration::from_secs(3),
            statement_timeout: Duration::from_secs(5),
        }
    }
}

// ===== 测试 =====

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_config_defaults() {
        let config = RuntimeConfig::default();
        assert_eq!(config.max_active, 10_000);
        assert_eq!(config.idle_timeout, Duration::from_secs(300));
        assert_eq!(config.runtime_channel_capacity, 1024);
        assert_eq!(config.snapshot_threshold, 100);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
        assert_eq!(config.command_send_timeout, Duration::from_secs(15));
        assert_eq!(config.wasm_validate_timeout, Duration::from_secs(5));
        assert_eq!(config.wasm_handle_timeout, Duration::from_secs(10));
        assert_eq!(config.wasm_fuel_limit, 1_000_000);
    }

    #[test]
    fn test_runtime_config_custom() {
        let config = RuntimeConfig {
            max_active: 100,
            snapshot_threshold: 50,
            ..Default::default()
        };
        assert_eq!(config.max_active, 100);
        assert_eq!(config.snapshot_threshold, 50);
        // 未覆盖的保持默认值
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_event_store_config_defaults() {
        let config = EventStoreConfig::default();
        assert_eq!(config.max_connections, 32);
        assert_eq!(config.min_connections, 4);
        assert_eq!(config.acquire_timeout, Duration::from_secs(3));
        assert_eq!(config.statement_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_event_store_config_custom_url() {
        let config = EventStoreConfig {
            database_url: "postgres://custom:5432/db".into(),
            ..Default::default()
        };
        assert_eq!(config.database_url, "postgres://custom:5432/db");
    }
}
