use std::sync::Arc;

use async_graphql_axum::GraphQL;
use async_graphql::http::GraphiQLSource;
use axum::{Router, response::Html, response::IntoResponse, routing::get};
use clap::Parser;

use host_server::graphql;
use host_server::wasm_registry::WasmRegistry;

const DEFAULT_WASM_DIR: &str = env!("DEFAULT_WASM_DIR");

#[derive(Parser)]
#[command(name = "host-server")]
#[command(about = "加载任意 WASM 组件并暴露为 GraphQL 接口")]
struct Cli {
    /// WASM 模块目录（加载目录下所有 .wasm 文件）
    #[arg(long = "wasm-dir", default_value = DEFAULT_WASM_DIR)]
    wasm_dir: String,

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

    println!("正在扫描 WASM 模块目录: {}", cli.wasm_dir);

    let registry = Arc::new(WasmRegistry::from_dir(std::path::Path::new(&cli.wasm_dir))?);

    println!("加载了 {} 个模块:", registry.modules().len());
    for module in registry.modules() {
        println!("  [{}] ({}):", module.module_name, module.source_path.display());
        for desc in module.engine.descriptors() {
            println!("    - {} (GraphQL: {})", desc.wit_name, desc.graphql_name);
        }
    }

    let schema = graphql::build_dynamic_schema(registry)?;

    let app = Router::new()
        .route("/graphql", get(graphiql).post_service(GraphQL::new(schema)))
        .route("/health", get(health));

    println!("服务器启动于 http://{}", cli.addr);
    println!("GraphiQL 界面: http://{}/graphql", cli.addr);

    let listener = tokio::net::TcpListener::bind(&cli.addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
