# 事件存储设计（单机版，PostgreSQL）

## 设计目标

- 单一 PostgreSQL 实例承载事件和快照
- 同步写入：每次命令的事件同步写入 DB，等待确认后才响应客户端
- 乐观并发控制：通过 `UNIQUE(aggregate_id, version)` 约束保证版本一致性
- 快照存储在同库中，简化部署

## 数据模型

```sql
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    VARCHAR(255) NOT NULL,
    aggregate_type  VARCHAR(128) NOT NULL,
    event_type      VARCHAR(255) NOT NULL,
    version         BIGINT NOT NULL,
    data            BYTEA NOT NULL,
    metadata        JSONB,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE (aggregate_id, version)
);

CREATE INDEX idx_events_aggregate ON events (aggregate_id, version);

-- 快照表（每个聚合仅保留最新一份）
CREATE TABLE snapshots (
    aggregate_id    VARCHAR(255) NOT NULL,
    aggregate_type  VARCHAR(128) NOT NULL,
    version         BIGINT NOT NULL,
    state           BYTEA NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (aggregate_type, aggregate_id)
);
```

## 幂等性保证

不使用独立幂等键表，通过版本号实现天然幂等：

```
客户端发送命令（expectedVersion=5）→ 成功写入 version=6 → 返回成功
客户端重试同一命令（expectedVersion=5）→ Actor 内版本已是 6 → VersionConflict → 返回冲突
```

客户端收到 VersionConflict 后应重新查询聚合状态，判断是否需要重试。

## Trait 定义

```rust
#[derive(Debug, Clone)]
pub struct DomainEvent {
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub event_type: String,
    pub version: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct PendingEvent {
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub version: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub version: u64,
    pub state: Vec<u8>,
}

#[derive(Debug)]
pub enum StoreError {
    VersionConflict { aggregate_id: String, expected_version: u64 },
    ConnectionError(String),
}

#[async_trait]
pub trait EventStore: Send + Sync {
    /// 同步追加事件（version 唯一约束保证乐观并发控制）
    async fn append(
        &self,
        aggregate_id: &str,
        events: &[PendingEvent],
        expected_version: u64,
    ) -> Result<(), StoreError>;

    /// 加载指定版本之后的增量事件
    async fn load_events_after(
        &self,
        aggregate_id: &str,
        after_version: u64,
    ) -> Result<Vec<DomainEvent>, StoreError>;

    /// 加载快照
    async fn load_snapshot(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<Snapshot>, StoreError>;

    /// 保存快照（覆盖写入）
    async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(), StoreError>;
}
```

## 实现

```rust
pub struct PgEventStore {
    pool: PgPool,
}

#[async_trait]
impl EventStore for PgEventStore {
    async fn append(
        &self,
        aggregate_id: &str,
        events: &[PendingEvent],
        expected_version: u64,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;

        for (i, event) in events.iter().enumerate() {
            let version = expected_version + 1 + i as u64;
            sqlx::query(
                "INSERT INTO events (aggregate_id, aggregate_type, event_type, version, data) \
                 VALUES ($1, $2, $3, $4, $5)"
            )
            .bind(aggregate_id)
            .bind(&event.aggregate_type)
            .bind("domain_event")
            .bind(version as i64)
            .bind(&event.data)
            .execute(&mut *tx).await
            .map_err(|e| match e {
                sqlx::Error::Database(ref db_err) if db_err.code() == Some("23505".into()) =>
                    StoreError::VersionConflict {
                        aggregate_id: aggregate_id.to_string(),
                        expected_version,
                    },
                _ => StoreError::ConnectionError(e.to_string()),
            })?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO snapshots (aggregate_id, aggregate_type, version, state) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE \
             SET version = $3, state = $4, created_at = NOW()"
        )
        .bind(&snapshot.aggregate_id)
        .bind(&snapshot.aggregate_type)
        .bind(snapshot.version as i64)
        .bind(&snapshot.state)
        .execute(&self.pool).await?;
        Ok(())
    }
}
```

## 快照策略

| 触发条件 | 阻塞命令处理？ | 说明 |
|----------|---------------|------|
| 事件计数达阈值（默认 100） | 否（异步 spawn） | 常规运行中 |
| Actor 休眠（空闲超时/LRU 驱逐） | 是（休眠流程的一部分） | 加速下次激活 |
| 服务优雅关闭 | 是 | 保证重启后快速恢复 |

## 连接池配置

```rust
pub struct EventStoreConfig {
    pub database_url: String,
    pub max_connections: u32,     // 默认 32
    pub min_connections: u32,     // 默认 4
    pub acquire_timeout: Duration, // 默认 3s
    pub statement_timeout: Duration, // 默认 5s
}
```
