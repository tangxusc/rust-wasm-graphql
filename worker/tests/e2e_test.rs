// 端到端集成测试

mod common;

use std::net::TcpListener;
use std::sync::Arc;

use async_graphql_axum::GraphQL;
use axum::{Router, routing::get};
use reqwest::Client;
use serde_json::Value;

use common::clean_test_data;
use worker::config::{EventStoreConfig, RuntimeConfig};
use worker::event_store::PgEventStore;
use worker::graphql::build_dynamic_schema;
use worker::runtime::VirtualActorRuntime;
use worker::wasm_engine::WasmEngine;

fn find_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn start_test_server() -> String {
    let pool = common::create_test_pool().await;
    clean_test_data(&pool).await;
    PgEventStore::run_migrations(&pool).await.expect("迁移失败");

    let event_store = Arc::new(PgEventStore::from_pool(pool));
    let config = RuntimeConfig::default();

    let wasm_dir = env!("DEFAULT_WASM_DIR");

    let mut wasm_engine = WasmEngine::new(&config);
    let (module_name, commands) = wasm_engine
        .load_module(&std::path::PathBuf::from(format!("{}/wasm_counter.wasm", wasm_dir)))
        .expect("加载计数器 WASM 失败");

    assert_eq!(module_name, "counter");
    assert!(!commands.is_empty());

    let wasm_engine = Arc::new(wasm_engine);

    let runtime = VirtualActorRuntime::spawn(config, event_store.clone(), wasm_engine.clone());

    let pascal_name = worker::graphql::to_pascal_case(&module_name);
    let schema = build_dynamic_schema(
        vec![(module_name, pascal_name, commands)],
        runtime,
        event_store,
    );

    let app = Router::new()
        .route("/graphql", get(|| async { "OK" }).post_service(GraphQL::new(schema)))
        .route("/health", get(|| async { "OK" }));

    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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
async fn test_graphql_health_query() {
    let base_url = start_test_server().await;
    let result = graphql_query(&base_url, "{ health }").await;
    assert_eq!(result["data"]["health"], true);
}

#[tokio::test]
async fn test_counter_increment_single() {
    let base_url = start_test_server().await;
    let mutation = r#"
        mutation {
            counter {
                increment(aggregateId: "test-1", expectedVersion: "0", amount: 5) {
                    success
                    version
                    eventCount
                }
            }
        }
    "#;
    let result = graphql_query(&base_url, mutation).await;
    let cmd_result = &result["data"]["counter"]["increment"];
    assert_eq!(cmd_result["success"], true);
    assert_eq!(cmd_result["version"], "1");
    assert_eq!(cmd_result["eventCount"], 1);
}

#[tokio::test]
async fn test_counter_increment_sequence() {
    let base_url = start_test_server().await;

    // 第一次递增
    let m1 = r#"
        mutation {
            counter {
                increment(aggregateId: "test-seq", expectedVersion: "0", amount: 3) {
                    version
                }
            }
        }
    "#;
    let r1 = graphql_query(&base_url, m1).await;
    assert_eq!(r1["data"]["counter"]["increment"]["version"], "1");

    // 第二次递增
    let m2 = r#"
        mutation {
            counter {
                increment(aggregateId: "test-seq", expectedVersion: "1", amount: 7) {
                    version
                    success
                }
            }
        }
    "#;
    let r2 = graphql_query(&base_url, m2).await;
    assert_eq!(r2["data"]["counter"]["increment"]["version"], "2");
    assert_eq!(r2["data"]["counter"]["increment"]["success"], true);
}

#[tokio::test]
async fn test_version_conflict() {
    let base_url = start_test_server().await;

    // 第一次命令
    let m1 = r#"
        mutation {
            counter {
                increment(aggregateId: "test-conflict", expectedVersion: "0", amount: 1) {
                    version
                }
            }
        }
    "#;
    graphql_query(&base_url, m1).await;

    // 用旧版本重试
    let m2 = r#"
        mutation {
            counter {
                increment(aggregateId: "test-conflict", expectedVersion: "0", amount: 2) {
                    success
                    version
                    error
                }
            }
        }
    "#;
    let r2 = graphql_query(&base_url, m2).await;
    let cmd = &r2["data"]["counter"]["increment"];
    assert_eq!(cmd["success"], false, "版本冲突应该返回失败");
    assert!(cmd["error"].as_str().unwrap_or("").contains("冲突"), "错误消息应包含'冲突'");
}

#[tokio::test]
async fn test_concurrent_increments_different_aggregates() {
    let base_url = start_test_server().await;

    let m1 = async {
        graphql_query(&base_url, r#"
            mutation {
                counter {
                    increment(aggregateId: "concurrent-a", expectedVersion: "0", amount: 5) {
                        version success
                    }
                }
            }
        "#).await
    };

    let m2 = async {
        graphql_query(&base_url, r#"
            mutation {
                counter {
                    increment(aggregateId: "concurrent-b", expectedVersion: "0", amount: 3) {
                        version success
                    }
                }
            }
        "#).await
    };

    let (r1, r2) = tokio::join!(m1, m2);
    let cmd1 = &r1["data"]["counter"]["increment"];
    let cmd2 = &r2["data"]["counter"]["increment"];
    assert_eq!(cmd1["success"], true);
    assert_eq!(cmd2["success"], true);
}

#[tokio::test]
async fn test_aggregate_version_query() {
    let base_url = start_test_server().await;

    // 先执行命令创建事件
    graphql_query(&base_url, r#"
        mutation {
            counter {
                increment(aggregateId: "test-version-query", expectedVersion: "0", amount: 10) {
                    version
                }
            }
        }
    "#).await;

    // 查询版本
    let q = r#"
        query {
            aggregateVersion(aggregateType: "counter", aggregateId: "test-version-query")
        }
    "#;
    let result = graphql_query(&base_url, q).await;
    assert_eq!(result["data"]["aggregateVersion"], "1");
}

#[tokio::test]
async fn test_aggregate_version_nonexistent() {
    let base_url = start_test_server().await;
    let q = r#"
        query {
            aggregateVersion(aggregateType: "counter", aggregateId: "no-such-agg")
        }
    "#;
    let result = graphql_query(&base_url, q).await;
    assert!(result["data"]["aggregateVersion"].is_null());
}

#[tokio::test]
async fn test_invalid_aggregate_id_rejected() {
    let base_url = start_test_server().await;
    let mutation = r#"
        mutation {
            counter {
                increment(aggregateId: "hello world", expectedVersion: "0", amount: 5) {
                    success
                }
            }
        }
    "#;
    let result = graphql_query(&base_url, mutation).await;
    assert!(result["errors"].is_array(), "非法 aggregateId 应返回错误");
}

#[tokio::test]
async fn test_graphql_introspection() {
    let base_url = start_test_server().await;
    let result = graphql_query(&base_url, "{ __schema { queryType { name } } }").await;
    assert_eq!(result["data"]["__schema"]["queryType"]["name"], "Query");
}

#[tokio::test]
async fn test_fuzz_aggregate_id_invalid_chars() {
    let base_url = start_test_server().await;
    let invalid_ids = vec![
        "hello world",
        "../etc/passwd",
        "a;DROP TABLE",
        "a\nb",
        "",
    ];

    for id in &invalid_ids {
        if id.is_empty() { continue; }
        let mutation = format!(r#"
            mutation {{
                counter {{
                    increment(aggregateId: "{}", expectedVersion: "0", amount: 1) {{
                        success
                    }}
                }}
            }}
        "#, id);
        let result = graphql_query(&base_url, &mutation).await;
        assert!(
            result["errors"].is_array() || result["data"]["counter"]["increment"]["success"] == false,
            "非法 aggregateId '{}' 应被拒绝", id
        );
    }
}
