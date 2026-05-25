// 事件存储集成测试

mod common;

use common::{clean_test_data, create_test_store};
use worker::error::StoreError;
use worker::event_store::EventStore;
use worker::types::{PendingEvent, Snapshot};

/// 创建测试用 PendingEvent
fn make_event(aggregate_id: &str, aggregate_type: &str, event_type: &str, version: u64, data: &[u8]) -> PendingEvent {
    PendingEvent {
        aggregate_id: aggregate_id.into(),
        aggregate_type: aggregate_type.into(),
        event_type: event_type.into(),
        version,
        data: data.to_vec(),
    }
}

#[tokio::test]
async fn test_append_single_event() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    let event = make_event("agg-1", "counter", "Incremented", 1, br#"{"type":"Incremented","amount":5}"#);
    store.append("agg-1", &[event], 0).await.unwrap();

    let loaded = store.load_events_after("counter", "agg-1", 0).await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].version, 1);
    assert_eq!(loaded[0].event_type, "Incremented");
    assert_eq!(loaded[0].aggregate_type, "counter");
}

#[tokio::test]
async fn test_append_multiple_events() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    let events = vec![
        make_event("agg-2", "counter", "Incremented", 1, br#"{"amount":3}"#),
        make_event("agg-2", "counter", "Incremented", 2, br#"{"amount":7}"#),
    ];
    store.append("agg-2", &events, 0).await.unwrap();

    let loaded = store.load_events_after("counter", "agg-2", 0).await.unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].version, 1);
    assert_eq!(loaded[1].version, 2);
}

#[tokio::test]
async fn test_version_conflict_detected() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    let event1 = make_event("agg-3", "counter", "Incremented", 1, br#"{"amount":1}"#);
    store.append("agg-3", &[event1], 0).await.unwrap();

    let event2 = make_event("agg-3", "counter", "Incremented", 1, br#"{"amount":1}"#);
    let result = store.append("agg-3", &[event2], 0).await;
    assert!(matches!(result, Err(StoreError::VersionConflict { .. })));
}

#[tokio::test]
async fn test_load_events_after_returns_only_newer() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    let events = vec![
        make_event("agg-4", "counter", "Incremented", 1, br#"{"amount":1}"#),
        make_event("agg-4", "counter", "Incremented", 2, br#"{"amount":2}"#),
        make_event("agg-4", "counter", "Incremented", 3, br#"{"amount":3}"#),
    ];
    store.append("agg-4", &events, 0).await.unwrap();

    let loaded = store.load_events_after("counter", "agg-4", 1).await.unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].version, 2);
    assert_eq!(loaded[1].version, 3);
}

#[tokio::test]
async fn test_load_events_empty_aggregate() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    let loaded = store.load_events_after("counter", "nonexistent", 0).await.unwrap();
    assert!(loaded.is_empty());
}

#[tokio::test]
async fn test_save_and_load_snapshot() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    let snap = Snapshot {
        aggregate_id: "agg-5".into(),
        aggregate_type: "counter".into(),
        version: 10,
        state: br#"{"count":50}"#.to_vec(),
    };
    store.save_snapshot(&snap).await.unwrap();

    let loaded = store.load_snapshot("counter", "agg-5").await.unwrap();
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.version, 10);
    assert_eq!(loaded.state, br#"{"count":50}"#);
}

#[tokio::test]
async fn test_snapshot_version_guard() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    // 保存高版本快照
    let snap_high = Snapshot {
        aggregate_id: "agg-6".into(),
        aggregate_type: "counter".into(),
        version: 20,
        state: br#"{"count":100}"#.to_vec(),
    };
    store.save_snapshot(&snap_high).await.unwrap();

    // 尝试用低版本覆盖（版本守卫应拒绝）
    let snap_low = Snapshot {
        aggregate_id: "agg-6".into(),
        aggregate_type: "counter".into(),
        version: 10,
        state: br#"{"count":50}"#.to_vec(),
    };
    store.save_snapshot(&snap_low).await.unwrap();

    // 加载到的仍应是高版本
    let loaded = store.load_snapshot("counter", "agg-6").await.unwrap().unwrap();
    assert_eq!(loaded.version, 20);
    assert_eq!(loaded.state, br#"{"count":100}"#);
}

#[tokio::test]
async fn test_get_current_version_returns_max() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    let events = vec![
        make_event("agg-7", "counter", "Incremented", 1, br#"{}"#),
        make_event("agg-7", "counter", "Incremented", 2, br#"{}"#),
        make_event("agg-7", "counter", "Incremented", 3, br#"{}"#),
    ];
    store.append("agg-7", &events, 0).await.unwrap();

    let version = store.get_current_version("counter", "agg-7").await.unwrap();
    assert_eq!(version, Some(3));
}

#[tokio::test]
async fn test_get_current_version_nonexistent() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    let version = store.get_current_version("counter", "no-such-agg").await.unwrap();
    assert_eq!(version, None);
}

#[tokio::test]
async fn test_check_aggregate_type_conflict() {
    let store = create_test_store().await;
    clean_test_data(&store.pool).await;

    // 先写入 inventory 模块的事件
    let event = make_event("item-1", "inventory", "ItemCreated", 1, br#"{}"#);
    store.append("item-1", &[event], 0).await.unwrap();

    // 其他模块使用相同 aggregate_id 应该检测到冲突
    let conflict = store.check_aggregate_type_conflict("item-1", "orders").await.unwrap();
    assert!(conflict, "不同模块使用相同 aggregate_id 应该有冲突");

    // 同模块不冲突
    let no_conflict = store.check_aggregate_type_conflict("item-1", "inventory").await.unwrap();
    assert!(!no_conflict, "同模块不冲突");
}
