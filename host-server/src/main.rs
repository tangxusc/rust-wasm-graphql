//! Rust + WASM + GraphQL 示例项目 - 主服务器入口
//!
//! 本程序：
//! 1. 加载编译好的 WASM 模块（由 build.rs 自动编译）
//! 2. 运行时内省 WASM 组件导出，动态生成 GraphQL schema
//! 3. 启动 HTTP 服务器，提供 GraphQL 端点和 GraphiQL 交互界面

mod graphql;
mod wasm_engine;

use std::sync::Arc;

use async_graphql_axum::GraphQL;
use async_graphql::http::GraphiQLSource;
use axum::{Router, response::Html, response::IntoResponse, routing::get};

use crate::wasm_engine::WasmEngine;

/// WASM 模块路径，由 build.rs 在编译时通过环境变量注入
const WASM_MODULE_PATH: &str = env!("WASM_MODULE_PATH");

/// GraphiQL 交互界面处理函数
async fn graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .finish(),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("正在加载 WASM 模块: {}", WASM_MODULE_PATH);

    // 初始化 WASM 引擎，加载编译好的模块
    let wasm_engine = Arc::new(WasmEngine::new(WASM_MODULE_PATH)?);

    println!("WASM 模块加载成功");
    println!("发现 {} 个导出函数:", wasm_engine.descriptors().len());
    for desc in wasm_engine.descriptors() {
        println!("  - {} (GraphQL: {})", desc.wit_name, desc.graphql_name);
    }

    let schema = graphql::build_dynamic_schema(wasm_engine)?;

    // 配置路由：GraphQL 端点 + GraphiQL 界面
    let app = Router::new()
        .route("/graphql", get(graphiql).post_service(GraphQL::new(schema)));

    let addr = "0.0.0.0:8080";
    println!("服务器启动于 http://{}", addr);
    println!("GraphiQL 界面: http://localhost:8080/graphql");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
