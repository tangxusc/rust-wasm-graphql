mod graphql;
mod wasm_engine;

use std::sync::Arc;

use async_graphql_axum::GraphQL;
use async_graphql::http::GraphiQLSource;
use axum::{Router, response::Html, response::IntoResponse, routing::get};
use clap::Parser;

use crate::wasm_engine::WasmEngine;

const DEFAULT_WASM_PATH: &str = env!("DEFAULT_WASM_PATH");

#[derive(Parser)]
#[command(name = "host-server")]
#[command(about = "加载任意 WASM 组件并暴露为 GraphQL 接口")]
struct Cli {
    /// WASM 组件文件路径
    #[arg(long = "wasm", default_value = DEFAULT_WASM_PATH)]
    wasm_path: String,

    /// 监听地址
    #[arg(long, default_value = "0.0.0.0:8080")]
    addr: String,
}

async fn health() -> impl IntoResponse {
    "OK"
}

async fn graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .finish(),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    println!("正在加载 WASM 模块: {}", cli.wasm_path);

    let wasm_engine = Arc::new(WasmEngine::new(&cli.wasm_path)?);

    println!("WASM 模块加载成功");
    println!("发现 {} 个导出函数:", wasm_engine.descriptors().len());
    for desc in wasm_engine.descriptors() {
        println!("  - {} (GraphQL: {})", desc.wit_name, desc.graphql_name);
    }

    let schema = graphql::build_dynamic_schema(wasm_engine)?;

    let app = Router::new()
        .route("/graphql", get(graphiql).post_service(GraphQL::new(schema)))
        .route("/health", get(health));

    println!("服务器启动于 http://{}", cli.addr);
    println!("GraphiQL 界面: http://{}/graphql", cli.addr);

    let listener = tokio::net::TcpListener::bind(&cli.addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
