// Worker 主入口：基于 Virtual Actor 模型的事件溯源系统（单机版）

use std::path::PathBuf;

use clap::Parser;

/// 基于 Virtual Actor 模型的事件溯源系统（单机版）
#[derive(Parser)]
#[command(name = "worker")]
#[command(about = "加载 WASM 聚合组件并通过 GraphQL 暴露命令处理接口")]
pub struct Cli {
    /// WASM 模块目录
    #[arg(long = "wasm-dir", default_value = "./wasm-modules")]
    pub wasm_dir: String,

    /// PostgreSQL 数据库连接串
    #[arg(long = "db-url", env = "DATABASE_URL", default_value = "postgres://worker:worker@localhost:5432/worker_test")]
    pub db_url: String,

    /// 监听地址
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub addr: String,

    /// 最大活跃聚合数量
    #[arg(long, default_value = "10000")]
    pub max_active: usize,

    /// 快照阈值（事件数量）
    #[arg(long, default_value = "100")]
    pub snapshot_threshold: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化 tracing
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("worker=info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .init();

    let cli = Cli::parse();

    let (_handle, port) = worker::app::start_server(
        &cli.db_url,
        &PathBuf::from(&cli.wasm_dir),
        &cli.addr,
        cli.max_active,
        cli.snapshot_threshold,
    )
    .await?;

    tracing::info!("服务器启动于 http://localhost:{}", port);
    tracing::info!("按 Ctrl+C 关闭服务");

    // 等待信号
    tokio::signal::ctrl_c().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parse_defaults() {
        let cli = Cli::parse_from(["worker", "--wasm-dir", "/tmp/test"]);
        assert_eq!(cli.wasm_dir, "/tmp/test");
        assert_eq!(cli.addr, "0.0.0.0:8080");
        assert_eq!(cli.max_active, 10000);
        assert_eq!(cli.snapshot_threshold, 100);
    }

    #[test]
    fn test_cli_parse_custom() {
        let cli = Cli::parse_from([
            "worker",
            "--wasm-dir", "/tmp/wasm",
            "--db-url", "postgres://localhost/test",
            "--addr", "127.0.0.1:3000",
            "--max-active", "500",
            "--snapshot-threshold", "50",
        ]);
        assert_eq!(cli.wasm_dir, "/tmp/wasm");
        assert_eq!(cli.db_url, "postgres://localhost/test");
        assert_eq!(cli.addr, "127.0.0.1:3000");
        assert_eq!(cli.max_active, 500);
        assert_eq!(cli.snapshot_threshold, 50);
    }

    #[test]
    fn test_cli_parse_env_db_url() {
        let cli = Cli::parse_from(["worker", "--wasm-dir", "/tmp"]);
        assert!(cli.db_url.contains("localhost"));
    }
}
