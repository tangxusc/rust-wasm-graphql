# 事件存储设计（强一致版）

## 核心变化

- **分片架构**：按 aggregate_id hash 分布到多个 PostgreSQL 实例
- **同步写入**：每次命令的事件同步写入 DB，等待确认后才响应客户端
- **CDC 替代 Outbox 轮询**：通过 WAL 监听实现零延迟事件发布
- **版本号校验**：Virtual Actor 串行处理保证无并发，版本号用于恢复校验和防御性检查

## 分片策略

```
aggregate_id → xxh3_64 hash → shard_id = hash % shard_count
```

```rust
pub struct ShardedEventStore {
    shards: Vec<Arc<dyn EventStoreShard>>,
    shard_count: usize,
}

impl ShardedEventStore {
    pub fn shard_for(&self, aggregate_id: &str) -> &Arc<dyn EventStoreShard> {
        let hash = xxhash_rust::xxh3::xxh3_64(aggregate_id.as_bytes());
        let idx = (hash as usize) % self.shard_count;
        &self.shards[idx]
    }
}
```

### 分片数量建议

| 集群规模 | 分片数 | 单分片 TPS | 总 TPS |
|----------|--------|-----------|--------|
| 开发 | 1 | 30,000 | 30,000 |
| 小型生产 | 4 | 30,000 | 120,000 |
| 中型生产 | 16 | 30,000 | 480,000 |
| 大型生产 | 64 | 30,000 | 1,920,000 |

## 数据模型

### events 表（每个分片独立）

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

-- 批量写入优化：减少索引数量
CREATE INDEX idx_events_aggregate ON events (aggregate_id, version);
```

### 写入优化配置（PostgreSQL）

```sql
-- 针对同步写入场景的调优
ALTER TABLE events SET (autovacuum_vacuum_scale_factor = 0.01);
ALTER TABLE events SET (fillfactor = 90);

-- 连接池预热，减少首次连接延迟
-- 推荐使用 PgBouncer 或 sqlx 内置连接池

-- 分区表（按时间，便于归档）
CREATE TABLE events (
    -- ... 同上
) PARTITION BY RANGE (created_at);

CREATE TABLE events_2024_q1 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
```

## Trait 定义

```rust
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct DomainEvent {
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub event_type: String,
    pub version: u64,
    pub data: Vec<u8>,
    pub metadata: Option<EventMetadata>,
}

#[derive(Debug, Clone)]
pub struct EventMetadata {
    pub command_id: String,
    pub timestamp: u64,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PendingEvent {
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub event_type: String,
    pub version: u64,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub enum StoreError {
    VersionConflict { expected: u64, actual: u64 },
    ConnectionError(String),
    SerializationError(String),
}

/// 单分片事件存储接口
#[async_trait]
pub trait EventStoreShard: Send + Sync {
    /// 同步追加事件（核心写入路径）
    /// 调用方等待此方法返回后才响应客户端，保证零数据丢失
    async fn append(
        &self,
        aggregate_id: &str,
        events: &[PendingEvent],
        expected_version: u64,
    ) -> Result<(), StoreError>;
    async fn batch_append(&self, events: &[PendingEvent]) -> Result<(), StoreError>;

    /// 加载聚合的全部事件（Virtual Actor 激活时使用）
    async fn load_events(&self, aggregate_id: &str) -> Result<Vec<DomainEvent>, StoreError>;

    /// 加载指定版本之后的增量事件（配合快照恢复）
    async fn load_events_after(
        &self,
        aggregate_id: &str,
        after_version: u64,
    ) -> Result<Vec<DomainEvent>, StoreError>;

    /// 获取聚合当前版本号
    async fn current_version(&self, aggregate_id: &str) -> Result<u64, StoreError>;
}
```

## 同步写入实现

```rust
/// 同步写入 — 单次事务写入单个聚合的事件，等待 DB 确认
/// 这是强一致性的核心：此方法返回 Ok 意味着事件已落盘
async fn append(
    &self,
    aggregate_id: &str,
    events: &[PendingEvent],
    expected_version: u64,
) -> Result<(), StoreError> {
    if events.is_empty() { return Ok(()); }

    let mut tx = self.pool.begin().await
        .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

    for (i, event) in events.iter().enumerate() {
        let version = expected_version + 1 + i as u64;
        sqlx::query(
            "INSERT INTO events (aggregate_id, aggregate_type, event_type, version, data) \
             VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(aggregate_id)
        .bind(&event.aggregate_type)
        .bind(&event.event_type)
        .bind(version as i64)
        .bind(&event.data)
        .execute(&mut *tx)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(ref db_err) if db_err.code() == Some("23505".into()) => {
                StoreError::VersionConflict {
                    expected: expected_version,
                    actual: version,
                }
            }
            _ => StoreError::ConnectionError(e.to_string()),
        })?;
    }

    tx.commit().await
        .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

    Ok(())
}
```

### 写入性能优化（在保证同步确认的前提下）

| 优化手段 | 说明 | 影响 |
|----------|------|------|
| 连接池预热 | 避免首次连接延迟 | 减少 P99 |
| 多行 INSERT | 单条 SQL 写入多个事件 | 减少网络往返 |
| 预编译语句 | prepared statement 复用 | 减少解析开销 |
| synchronous_commit=on | PostgreSQL 默认，保证 WAL 落盘 | 零丢失 |
| 分片并行 | 不同聚合写入不同分片，互不阻塞 | 线性扩展 |
```

## 幂等存储（双层设计）

### 第一层：本地布隆过滤器（零 IO）

```rust
use probabilistic_collections::bloom::BloomFilter;

pub struct LocalIdempotencyFilter {
    filter: RwLock<BloomFilter<str>>,
    false_positive_rate: f64,  // 0.01 (1%)
}

impl LocalIdempotencyFilter {
    /// 快速排除：返回 false 则一定不存在
    pub fn might_contain(&self, command_id: &str) -> bool {
        self.filter.read().unwrap().contains(command_id)
    }

    pub fn insert(&self, command_id: &str) {
        self.filter.write().unwrap().insert(command_id);
    }
}
```

### 第二层：KV 精确检查（仅布隆过滤器命中时触发）

```rust
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    async fn exists(&self, command_id: &str) -> Result<bool>;
    async fn record(&self, command_id: &str, ttl: Duration) -> Result<()>;
}
```

Key 格式：`cmd:{command_id}`，TTL 24-72 小时。

### 性能对比

| 方案 | 延迟 | 适用场景 |
|------|------|----------|
| 仅布隆过滤器 | ~10ns | 99% 的非重复请求直接放行 |
| 布隆 + KV | ~200μs | 1% 误判时精确检查 |
| 仅 KV | ~200μs | 每次都有网络 IO |

双层设计下，99% 的请求无需任何网络 IO 即可完成幂等判断。
