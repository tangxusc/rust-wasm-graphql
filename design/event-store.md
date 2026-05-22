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

-- 幂等键表（与 events 同库，支持同事务写入）
CREATE TABLE idempotency_keys (
    command_id      VARCHAR(128) PRIMARY KEY,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 幂等键自动过期清理（72 小时）
CREATE INDEX idx_idempotency_created ON idempotency_keys (created_at);
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

-- 分区自动管理（推荐使用 pg_partman 或自定义定时任务）
-- 自动创建未来分区 + 归档过期分区
-- 示例：按季度分区，提前创建下一季度分区
```

### 分区自动管理

生产环境必须自动管理分区，避免手动创建导致遗漏：

```sql
-- 方案 A：使用 pg_partman（推荐）
SELECT partman.create_parent(
    p_parent_table := 'public.events',
    p_control := 'created_at',
    p_type := 'native',
    p_interval := '3 months',
    p_premake := 2  -- 提前创建 2 个未来分区
);

-- 方案 B：自定义定时任务（pg_cron）
-- 每月 1 日检查并创建下季度分区
SELECT cron.schedule('create-event-partitions', '0 0 1 * *', $$
    SELECT partman.run_maintenance('public.events');
$$);
```

归档策略：超过保留期（如 1 年）的分区可 detach 后归档到冷存储（S3）。

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
    VersionConflict {
        aggregate_id: String,
        expected_version: u64,
        attempted_version: u64,
    },
    DuplicateCommand(String),
    ConnectionError(String),
    SerializationError(String),
}

/// 单分片事件存储接口
#[async_trait]
pub trait EventStoreShard: Send + Sync {
    /// 同步追加事件 + 幂等键（同一事务，原子保证）
    /// 调用方等待此方法返回后才响应客户端，保证零数据丢失
    async fn append_with_idempotency(
        &self,
        aggregate_id: &str,
        events: &[PendingEvent],
        expected_version: u64,
        command_id: &str,
    ) -> Result<(), StoreError>;

    /// 批量追加（无幂等键，用于数据迁移等场景）
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

    /// 检查幂等键是否存在
    async fn idempotency_exists(&self, command_id: &str) -> Result<bool, StoreError>;
}
```

## 同步写入实现（含事务级幂等）

```rust
/// 同步写入 — 事件 + 幂等键在同一事务中写入，原子保证
/// 此方法返回 Ok 意味着：事件已落盘 + 幂等键已记录，无窗口丢失
async fn append_with_idempotency(
    &self,
    aggregate_id: &str,
    events: &[PendingEvent],
    expected_version: u64,
    command_id: &str,
) -> Result<(), StoreError> {
    if events.is_empty() { return Ok(()); }

    let mut tx = self.pool.begin().await
        .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

    // 写入幂等键（ON CONFLICT 检测重复命令）
    let inserted = sqlx::query(
        "INSERT INTO idempotency_keys (command_id) VALUES ($1) ON CONFLICT DO NOTHING"
    )
    .bind(command_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

    if inserted.rows_affected() == 0 {
        return Err(StoreError::DuplicateCommand(command_id.to_string()));
    }

    // 写入事件
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
                    aggregate_id: aggregate_id.to_string(),
                    expected_version,
                    attempted_version: version,
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

### 第一层：滚动布隆过滤器（零 IO，防无限增长）

```rust
use probabilistic_collections::bloom::BloomFilter;
use parking_lot::RwLock;

/// 滚动布隆过滤器：两代交替，防止无限增长导致误判率飙升
/// 轮换操作使用单一写锁保护，确保原子性
pub struct RollingBloomFilter {
    inner: RwLock<BloomFilterInner>,
    capacity: usize,
    false_positive_rate: f64,
}

struct BloomFilterInner {
    current: BloomFilter<str>,
    previous: BloomFilter<str>,
    inserted: usize,
    rotate_threshold: usize,
}

impl RollingBloomFilter {
    pub fn new(capacity: usize, fp_rate: f64) -> Self {
        Self {
            inner: RwLock::new(BloomFilterInner {
                current: BloomFilter::with_rate(fp_rate, capacity),
                previous: BloomFilter::with_rate(fp_rate, capacity),
                inserted: 0,
                rotate_threshold: (capacity as f64 * 0.8) as usize,
            }),
            capacity,
            false_positive_rate: fp_rate,
        }
    }

    /// 快速排除：返回 false 则一定不存在
    pub fn might_contain(&self, command_id: &str) -> bool {
        let inner = self.inner.read();
        inner.current.contains(command_id) || inner.previous.contains(command_id)
    }

    pub fn insert(&self, command_id: &str) {
        let mut inner = self.inner.write();
        inner.current.insert(command_id);
        inner.inserted += 1;
        if inner.inserted >= inner.rotate_threshold {
            // 轮换在同一写锁内完成，保证原子性
            let new_filter = BloomFilter::with_rate(self.false_positive_rate, self.capacity);
            let old_current = std::mem::replace(&mut inner.current, new_filter);
            inner.previous = old_current;
            inner.inserted = 0;
        }
    }
}
```

**容量规划**：

| 日命令量 | 单代容量 | 内存占用 | 轮换频率 |
|----------|---------|---------|---------|
| 100万/天 | 200万 | ~2.4 MB | ~1次/天 |
| 1000万/天 | 2000万 | ~24 MB | ~1次/天 |
| 1亿/天 | 2亿 | ~240 MB | ~1次/天 |

注：进程重启后布隆过滤器为空，所有请求穿透到 DB 层的 `idempotency_keys` 表。
由于幂等键已在 Event Store 同事务中写入，DB 层查询是正确性兜底，布隆过滤器仅为性能优化。

### 第二层：DB 精确检查（事务内，仅布隆过滤器命中时触发）

幂等键存储在 Event Store 同库的 `idempotency_keys` 表中（见上方 DDL），
通过 `append_with_idempotency` 在同一事务中原子写入。

**重要**：幂等键与事件在同一事务中写入，因此幂等键存储在 aggregate_id 所在的分片。
Gateway 层的幂等检查必须按 aggregate_id 路由到正确分片：

```rust
impl ShardedEventStore {
    pub async fn idempotency_exists(&self, aggregate_id: &str, command_id: &str) -> Result<bool> {
        // 幂等键随事件写入 aggregate_id 所在分片，查询时必须路由到同一分片
        let shard = self.shard_for(aggregate_id);
        shard.idempotency_exists(command_id).await
    }
}
```

### 性能对比

| 方案 | 延迟 | 适用场景 |
|------|------|----------|
| 仅布隆过滤器 | ~10ns | 99% 的非重复请求直接放行 |
| 布隆 + KV | ~200μs | 1% 误判时精确检查 |
| 仅 KV | ~200μs | 每次都有网络 IO |

双层设计下，99% 的请求无需任何网络 IO 即可完成幂等判断。
