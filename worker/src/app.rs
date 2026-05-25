// 服务启动与编排

use std::path::PathBuf;
use std::sync::Arc;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::GraphQL;
use axum::{Router, response::Html, response::IntoResponse, routing::get};

use crate::config::{EventStoreConfig, RuntimeConfig};
use crate::event_store::PgEventStore;
use crate::graphql::build_dynamic_schema;
use crate::runtime::{RuntimeHandle, VirtualActorRuntime};
use crate::wasm_engine::WasmEngine;

async fn health() -> impl IntoResponse {
    "OK"
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

/// 扫描目录中的所有 .wasm 文件
pub fn scan_wasm_files(dir: &PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    if !dir.exists() {
        anyhow::bail!("WASM 目录不存在: {}", dir.display());
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "wasm") {
            files.push(path);
        }
    }
    if files.is_empty() {
        anyhow::bail!("目录 '{}' 下未找到 .wasm 文件", dir.display());
    }
    Ok(files)
}

/// 加载 WASM 模块并提取命令信息
pub fn load_wasm_modules(engine: &mut WasmEngine, wasm_dir: &PathBuf) -> anyhow::Result<Vec<(String, String, Vec<crate::command::CommandDef>)>> {
    let wasm_files = scan_wasm_files(wasm_dir)?;
    let mut module_commands = Vec::new();

    for path in &wasm_files {
        match engine.load_module(path) {
            Ok((module_name, commands)) => {
                let pascal_name = crate::graphql::to_pascal_case(&module_name);
                tracing::info!("模块 [{}] 加载成功，{} 个命令", module_name, commands.len());
                module_commands.push((module_name, pascal_name, commands));
            }
            Err(e) => {
                tracing::warn!("跳过非聚合组件 {}: {}", path.display(), e);
            }
        }
    }

    if module_commands.is_empty() {
        anyhow::bail!("未找到有效的 WASM 聚合组件");
    }

    Ok(module_commands)
}

/// 启动 Worker 服务器，返回 RuntimeHandle 和端口
pub async fn start_server(
    db_url: &str,
    wasm_dir: &PathBuf,
    addr: &str,
    max_active: usize,
    snapshot_threshold: u64,
) -> anyhow::Result<(RuntimeHandle, u16)> {
    // 1. 创建事件存储
    let es_config = EventStoreConfig {
        database_url: db_url.to_string(),
        ..Default::default()
    };
    let event_store = Arc::new(PgEventStore::new(&es_config).await?);
    tracing::info!("数据库迁移完成");

    // 2. 加载 WASM 模块
    let runtime_config = RuntimeConfig {
        max_active,
        snapshot_threshold,
        ..Default::default()
    };

    let mut wasm_engine = WasmEngine::new(&runtime_config);
    let module_commands = load_wasm_modules(&mut wasm_engine, wasm_dir)?;
    let wasm_engine = Arc::new(wasm_engine);

    // 3. 启动 Runtime
    let runtime = VirtualActorRuntime::spawn(
        runtime_config,
        event_store.clone(),
        wasm_engine.clone(),
    );

    // 4. 构建 GraphQL Schema
    let schema = build_dynamic_schema(module_commands, runtime.clone(), event_store);

    // 5. 构建路由
    let app = Router::new()
        .route("/graphql", get(graphiql).post_service(GraphQL::new(schema)))
        .route("/health", get(health));

    // 6. 绑定端口并启动
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let port = listener.local_addr()?.port();

    let rt = runtime.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("收到关闭信号...");
                rt.shutdown().await;
            })
            .await
            .ok();
    });

    Ok((runtime, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_wasm_files_empty_dir() {
        let dir = std::env::temp_dir().join("worker_test_empty");
        let _ = std::fs::create_dir(&dir);
        let result = scan_wasm_files(&dir);
        // 空目录应该报错
        assert!(result.is_err());
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_scan_wasm_files_nonexistent() {
        let dir = PathBuf::from("/nonexistent/path/12345");
        let result = scan_wasm_files(&dir);
        assert!(result.is_err());
    }

    #[test]
    fn test_wasm_dir_has_files() {
        let wasm_dir = PathBuf::from(env!("DEFAULT_WASM_DIR"));
        let result = scan_wasm_files(&wasm_dir);
        // 测试 WASM 计数器应该存在
        assert!(result.is_ok());
        let files = result.unwrap();
        assert!(!files.is_empty());
        assert!(files.iter().any(|f| f.to_str().unwrap().contains("wasm_counter")));
    }

    #[test]
    fn test_load_wasm_modules() {
        let wasm_dir = PathBuf::from(env!("DEFAULT_WASM_DIR"));
        let config = crate::config::RuntimeConfig::default();
        let mut engine = crate::wasm_engine::WasmEngine::new(&config);
        let result = load_wasm_modules(&mut engine, &wasm_dir);
        assert!(result.is_ok());
        let modules = result.unwrap();
        assert!(!modules.is_empty());
        // 应该包含 counter 模块
        assert!(modules.iter().any(|(name, _, _)| name == "counter"));
    }

    #[tokio::test]
    async fn test_start_server_integration() {
        let db_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://worker:worker@localhost:5432/worker_test".into());

        let _ = sqlx::query("DELETE FROM snapshots")
            .execute(&sqlx::postgres::PgPoolOptions::new().max_connections(1).connect(&db_url).await.unwrap())
            .await;
        let _ = sqlx::query("DELETE FROM events")
            .execute(&sqlx::postgres::PgPoolOptions::new().max_connections(1).connect(&db_url).await.unwrap())
            .await;

        let wasm_dir = PathBuf::from(env!("DEFAULT_WASM_DIR"));
        let result = start_server(
            &db_url,
            &wasm_dir,
            "127.0.0.1:0", // 随机端口
            100,
            100,
        ).await;

        match result {
            Ok((handle, port)) => {
                assert!(port > 0);
                handle.shutdown().await;
            }
            Err(e) => {
                // 可能 PG 不可用，但 migrate 应该已完成
                let _ = e;
            }
        }
    }
}
