use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use host_server::graphql::build_dynamic_schema;
use host_server::wasm_registry::WasmRegistry;

use async_graphql_axum::GraphQL;
use axum::{Router, routing::get};
use reqwest::Client;
use serde_json::Value;

fn find_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn start_test_server() -> String {
    let wasm_dir = env!("DEFAULT_WASM_DIR");
    let registry = Arc::new(WasmRegistry::from_dir(std::path::Path::new(wasm_dir)).unwrap());
    let schema = build_dynamic_schema(registry).unwrap();

    let app = Router::new()
        .route("/graphql", get(|| async { "OK" }).post_service(GraphQL::new(schema)))
        .route("/health", get(|| async { "OK" }));

    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    format!("http://127.0.0.1:{}", port)
}

async fn graphql_query(base_url: &str, query: &str) -> Value {
    let client = Client::new();
    let resp = client
        .post(format!("{}/graphql", base_url))
        .json(&serde_json::json!({ "query": query }))
        .send()
        .await
        .unwrap();
    resp.json::<Value>().await.unwrap()
}

#[tokio::test]
async fn test_health_endpoint() {
    let base_url = start_test_server().await;
    let client = Client::new();
    let resp = client.get(format!("{}/health", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "OK");
}

#[tokio::test]
async fn test_calculator_add() {
    let base_url = start_test_server().await;
    let result = graphql_query(&base_url, "{ calculator { add(a: 3, b: 4) } }").await;
    assert_eq!(result["data"]["calculator"]["add"], 7);
}

#[tokio::test]
async fn test_calculator_add_negative() {
    let base_url = start_test_server().await;
    let result = graphql_query(&base_url, "{ calculator { add(a: -5, b: 3) } }").await;
    assert_eq!(result["data"]["calculator"]["add"], -2);
}

#[tokio::test]
async fn test_calculator_fibonacci() {
    let base_url = start_test_server().await;
    let result = graphql_query(&base_url, "{ calculator { fibonacci(n: 10) } }").await;
    assert_eq!(result["data"]["calculator"]["fibonacci"], "55");
}

#[tokio::test]
async fn test_calculator_fibonacci_zero() {
    let base_url = start_test_server().await;
    let result = graphql_query(&base_url, "{ calculator { fibonacci(n: 0) } }").await;
    assert_eq!(result["data"]["calculator"]["fibonacci"], "0");
}

#[tokio::test]
async fn test_calculator_to_uppercase() {
    let base_url = start_test_server().await;
    let result = graphql_query(
        &base_url,
        r#"{ calculator { toUppercase(input: "hello world") } }"#,
    ).await;
    assert_eq!(result["data"]["calculator"]["toUppercase"], "HELLO WORLD");
}

#[tokio::test]
async fn test_strings_reverse() {
    let base_url = start_test_server().await;
    let result = graphql_query(
        &base_url,
        r#"{ strings { reverse(input: "abcde") } }"#,
    ).await;
    assert_eq!(result["data"]["strings"]["reverse"], "edcba");
}

#[tokio::test]
async fn test_strings_char_count() {
    let base_url = start_test_server().await;
    let result = graphql_query(
        &base_url,
        r#"{ strings { charCount(input: "hello") } }"#,
    ).await;
    assert_eq!(result["data"]["strings"]["charCount"], 5);
}

#[tokio::test]
async fn test_strings_repeat() {
    let base_url = start_test_server().await;
    let result = graphql_query(
        &base_url,
        r#"{ strings { repeat(input: "ab", times: 3) } }"#,
    ).await;
    assert_eq!(result["data"]["strings"]["repeat"], "ababab");
}

#[tokio::test]
async fn test_strings_char_count_unicode() {
    let base_url = start_test_server().await;
    let result = graphql_query(
        &base_url,
        r#"{ strings { charCount(input: "你好世界") } }"#,
    ).await;
    assert_eq!(result["data"]["strings"]["charCount"], 4);
}

#[tokio::test]
async fn test_multiple_modules_single_query() {
    let base_url = start_test_server().await;
    let result = graphql_query(
        &base_url,
        r#"{ calculator { add(a: 1, b: 2) } strings { charCount(input: "hi") } }"#,
    ).await;
    assert_eq!(result["data"]["calculator"]["add"], 3);
    assert_eq!(result["data"]["strings"]["charCount"], 2);
}

#[tokio::test]
async fn test_invalid_query_returns_error() {
    let base_url = start_test_server().await;
    let result = graphql_query(&base_url, "{ nonExistent { foo } }").await;
    assert!(result["errors"].is_array());
}

#[tokio::test]
async fn test_graphql_introspection() {
    let base_url = start_test_server().await;
    let result = graphql_query(
        &base_url,
        "{ __schema { queryType { name } } }",
    ).await;
    assert_eq!(result["data"]["__schema"]["queryType"]["name"], "Query");
}
