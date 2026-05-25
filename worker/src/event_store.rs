// 事件存储（PostgreSQL）

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use crate::config::EventStoreConfig;
use crate::error::StoreError;
use crate::types::{DomainEvent, PendingEvent, Snapshot};

/// 事件存储抽象 trait
#[async_trait]
pub trait EventStore: Send + Sync {
    /// 同步追加事件（版本唯一约束保证乐观并发控制）
    async fn append(
        &self,
        aggregate_id: &str,
        events: &[PendingEvent],
        expected_version: u64,
    ) -> Result<(), StoreError>;

    /// 加载指定版本之后的增量事件
    async fn load_events_after(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        after_version: u64,
    ) -> Result<Vec<DomainEvent>, StoreError>;

    /// 查询聚合当前版本号
    async fn get_current_version(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<u64>, StoreError>;

    /// 检查 aggregate_id 是否已被其他模块占用
    async fn check_aggregate_type_conflict(
        &self,
        aggregate_id: &str,
        expected_type: &str,
    ) -> Result<bool, StoreError>;

    /// 加载快照
    async fn load_snapshot(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<Snapshot>, StoreError>;

    /// 保存快照（版本守卫：仅允许更新到更高版本）
    async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(), StoreError>;
}

/// PostgreSQL 事件存储实现
pub struct PgEventStore {
    pub pool: PgPool,
}

impl PgEventStore {
    /// 创建 PgEventStore 并执行自动迁移
    pub async fn new(config: &EventStoreConfig) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(|_conn, _meta| Box::pin(async { Ok(()) }))
            .connect(&config.database_url)
            .await
            .map_err(|e| StoreError::ConnectionError(format!("连接数据库失败: {e}")))?;

        Self::run_migrations(&pool).await?;
        Ok(Self { pool })
    }

    /// 使用已有连接池创建（用于测试）
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 运行数据库迁移
    pub async fn run_migrations(pool: &PgPool) -> Result<(), StoreError> {
        // events 表：UNIQUE 约束自动创建索引，无需额外 CREATE INDEX
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS events (
                id              BIGSERIAL PRIMARY KEY,
                aggregate_id    VARCHAR(255) NOT NULL,
                aggregate_type  VARCHAR(128) NOT NULL,
                event_type      VARCHAR(255) NOT NULL,
                version         BIGINT NOT NULL,
                data            BYTEA NOT NULL,
                metadata        JSONB,
                created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE (aggregate_type, aggregate_id, version)
            )"
        ).execute(pool).await.map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS snapshots (
                aggregate_id    VARCHAR(255) NOT NULL,
                aggregate_type  VARCHAR(128) NOT NULL,
                version         BIGINT NOT NULL,
                state           BYTEA NOT NULL,
                created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (aggregate_type, aggregate_id)
            )"
        ).execute(pool).await.map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl EventStore for PgEventStore {
    async fn append(
        &self,
        aggregate_id: &str,
        events: &[PendingEvent],
        expected_version: u64,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin()
            .await
            .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        for (i, event) in events.iter().enumerate() {
            let version = expected_version + 1 + i as u64;
            let result = sqlx::query(
                "INSERT INTO events (aggregate_id, aggregate_type, event_type, version, data)
                 VALUES ($1, $2, $3, $4, $5)"
            )
            .bind(aggregate_id)
            .bind(&event.aggregate_type)
            .bind(&event.event_type)
            .bind(version as i64)
            .bind(&event.data)
            .execute(&mut *tx)
            .await;

            match result {
                Ok(_) => {}
                Err(sqlx::Error::Database(ref db_err))
                    if db_err.constraint() == Some("events_aggregate_type_aggregate_id_version_key") =>
                {
                    let _ = tx.rollback().await;
                    return Err(StoreError::VersionConflict {
                        aggregate_id: aggregate_id.to_string(),
                        expected_version,
                    });
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    return Err(StoreError::ConnectionError(e.to_string()));
                }
            }
        }

        tx.commit()
            .await
            .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        Ok(())
    }

    async fn load_events_after(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        after_version: u64,
    ) -> Result<Vec<DomainEvent>, StoreError> {
        let rows = sqlx::query(
            "SELECT id, aggregate_id, aggregate_type, event_type, version, data
             FROM events
             WHERE aggregate_type = $1 AND aggregate_id = $2 AND version > $3
             ORDER BY version ASC"
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(after_version as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        let events = rows.iter().map(|row| {
            DomainEvent {
                id: row.get::<i64, _>("id"),
                aggregate_id: row.get::<String, _>("aggregate_id"),
                aggregate_type: row.get::<String, _>("aggregate_type"),
                event_type: row.get::<String, _>("event_type"),
                version: row.get::<i64, _>("version") as u64,
                data: row.get::<Vec<u8>, _>("data"),
            }
        }).collect();

        Ok(events)
    }

    async fn get_current_version(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<u64>, StoreError> {
        let row: Option<(Option<i64>,)> = sqlx::query_as(
            "SELECT MAX(version) FROM events
             WHERE aggregate_type = $1 AND aggregate_id = $2"
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        Ok(row.and_then(|(v,)| v.map(|v| v as u64)))
    }

    async fn check_aggregate_type_conflict(
        &self,
        aggregate_id: &str,
        expected_type: &str,
    ) -> Result<bool, StoreError> {
        let row: (bool,) = sqlx::query_as(
            "SELECT EXISTS(
                SELECT 1 FROM events
                WHERE aggregate_id = $1 AND aggregate_type != $2
                LIMIT 1
            )"
        )
        .bind(aggregate_id)
        .bind(expected_type)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        Ok(row.0)
    }

    async fn load_snapshot(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<Snapshot>, StoreError> {
        let row = sqlx::query(
            "SELECT aggregate_id, aggregate_type, version, state
             FROM snapshots
             WHERE aggregate_type = $1 AND aggregate_id = $2"
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        Ok(row.map(|r| Snapshot {
            aggregate_id: r.get::<String, _>("aggregate_id"),
            aggregate_type: r.get::<String, _>("aggregate_type"),
            version: r.get::<i64, _>("version") as u64,
            state: r.get::<Vec<u8>, _>("state"),
        }))
    }

    async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(), StoreError> {
        // 版本守卫：仅当新版本 >= 旧版本时才更新
        sqlx::query(
            "INSERT INTO snapshots (aggregate_id, aggregate_type, version, state)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE
             SET version = $3, state = $4, created_at = NOW()
             WHERE snapshots.version < $3"
        )
        .bind(&snapshot.aggregate_id)
        .bind(&snapshot.aggregate_type)
        .bind(snapshot.version as i64)
        .bind(&snapshot.state)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        Ok(())
    }
}

// ===== 单元测试 =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DomainEvent, PendingEvent, Snapshot};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// 内存 Mock 实现
    struct MockStore {
        events: Mutex<HashMap<String, Vec<PendingEvent>>>,
        snapshots: Mutex<HashMap<String, Snapshot>>,
    }

    impl MockStore {
        fn new() -> Self {
            Self {
                events: Mutex::new(HashMap::new()),
                snapshots: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl EventStore for MockStore {
        async fn append(&self, aggregate_id: &str, events: &[PendingEvent], _expected_version: u64) -> Result<(), StoreError> {
            let mut map = self.events.lock().unwrap();
            map.entry(aggregate_id.to_string())
                .or_default()
                .extend(events.to_vec());
            Ok(())
        }

        async fn load_events_after(&self, _aggregate_type: &str, aggregate_id: &str, after_version: u64) -> Result<Vec<DomainEvent>, StoreError> {
            let map = self.events.lock().unwrap();
            let events = map.get(aggregate_id)
                .map(|list| {
                    list.iter()
                        .filter(|e| e.version > after_version)
                        .enumerate()
                        .map(|(i, e)| DomainEvent {
                            id: (i + 1) as i64,
                            aggregate_id: e.aggregate_id.clone(),
                            aggregate_type: e.aggregate_type.clone(),
                            event_type: e.event_type.clone(),
                            version: e.version,
                            data: e.data.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            Ok(events)
        }

        async fn get_current_version(&self, _aggregate_type: &str, aggregate_id: &str) -> Result<Option<u64>, StoreError> {
            let map = self.events.lock().unwrap();
            let max = map.get(aggregate_id)
                .and_then(|events| events.iter().map(|e| e.version).max());
            Ok(max)
        }

        async fn check_aggregate_type_conflict(&self, _aggregate_id: &str, _expected_type: &str) -> Result<bool, StoreError> {
            Ok(false)
        }

        async fn load_snapshot(&self, _aggregate_type: &str, aggregate_id: &str) -> Result<Option<Snapshot>, StoreError> {
            let map = self.snapshots.lock().unwrap();
            Ok(map.get(aggregate_id).cloned())
        }

        async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(), StoreError> {
            let mut map = self.snapshots.lock().unwrap();
            map.insert(snapshot.aggregate_id.clone(), snapshot.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_mock_append_and_load() {
        let store = MockStore::new();
        let event = PendingEvent {
            aggregate_id: "agg-1".into(),
            aggregate_type: "counter".into(),
            event_type: "Incremented".into(),
            version: 1,
            data: vec![1, 2, 3],
        };
        store.append("agg-1", &[event], 0).await.unwrap();
        let loaded = store.load_events_after("counter", "agg-1", 0).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].version, 1);
    }

    #[tokio::test]
    async fn test_mock_empty_events() {
        let store = MockStore::new();
        let loaded = store.load_events_after("counter", "no-agg", 0).await.unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn test_mock_save_and_load_snapshot() {
        let store = MockStore::new();
        let snap = Snapshot {
            aggregate_id: "agg-1".into(),
            aggregate_type: "counter".into(),
            version: 10,
            state: vec![4, 5, 6],
        };
        store.save_snapshot(&snap).await.unwrap();
        let loaded = store.load_snapshot("counter", "agg-1").await.unwrap().unwrap();
        assert_eq!(loaded.version, 10);
    }

    #[tokio::test]
    async fn test_mock_no_snapshot() {
        let store = MockStore::new();
        let result = store.load_snapshot("counter", "no-agg").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_mock_get_current_version() {
        let store = MockStore::new();
        assert_eq!(store.get_current_version("counter", "no-agg").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_mock_type_conflict_default() {
        let store = MockStore::new();
        let result = store.check_aggregate_type_conflict("any", "any").await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_mock_append_multiple() {
        let store = MockStore::new();
        let e1 = PendingEvent {
            aggregate_id: "agg-multi".into(),
            aggregate_type: "counter".into(),
            event_type: "A".into(),
            version: 1,
            data: vec![1],
        };
        let e2 = PendingEvent {
            aggregate_id: "agg-multi".into(),
            aggregate_type: "counter".into(),
            event_type: "B".into(),
            version: 2,
            data: vec![2],
        };
        store.append("agg-multi", &[e1, e2], 0).await.unwrap();
        let loaded = store.load_events_after("counter", "agg-multi", 0).await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].event_type, "A");
        assert_eq!(loaded[1].event_type, "B");
    }

    #[tokio::test]
    async fn test_mock_load_after_version() {
        let store = MockStore::new();
        for i in 1..=5 {
            let e = PendingEvent {
                aggregate_id: "agg-after".into(),
                aggregate_type: "counter".into(),
                event_type: "Test".into(),
                version: i,
                data: vec![],
            };
            store.append("agg-after", &[e], 0).await.unwrap();
        }
        let loaded = store.load_events_after("counter", "agg-after", 3).await.unwrap();
        assert_eq!(loaded.len(), 2); // 版本 4 和 5
        assert_eq!(loaded[0].version, 4);
    }

    #[tokio::test]
    async fn test_mock_snapshot_overwrite() {
        let store = MockStore::new();
        let s1 = Snapshot {
            aggregate_id: "agg-snap".into(),
            aggregate_type: "counter".into(),
            version: 10,
            state: vec![1],
        };
        store.save_snapshot(&s1).await.unwrap();
        // 更高版本覆盖
        let s2 = Snapshot { version: 20, state: vec![2], ..s1.clone() };
        store.save_snapshot(&s2).await.unwrap();

        let loaded = store.load_snapshot("counter", "agg-snap").await.unwrap().unwrap();
        assert_eq!(loaded.version, 20);
        assert_eq!(loaded.state, vec![2]);
    }

    #[tokio::test]
    async fn test_mock_get_version_after_multiple_appends() {
        let store = MockStore::new();
        for i in 1..=10 {
            let e = PendingEvent {
                aggregate_id: "v10".into(),
                aggregate_type: "counter".into(),
                event_type: "Test".into(),
                version: i,
                data: vec![],
            };
            store.append("v10", &[e], 0).await.unwrap();
        }
        assert_eq!(store.get_current_version("counter", "v10").await.unwrap(), Some(10));
    }
}

