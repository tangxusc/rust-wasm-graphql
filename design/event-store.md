# 事件存储设计（单实例 PostgreSQL）

## 设计目标

- **单一 PostgreSQL 实例**：当前阶段不考虑分片，后续可迁移至分布式数据库
- **同步写入**：每次命令的事件同步写入 DB，等待确认后才响应客户端
- **事务级幂等**：幂等键与事件在同一事务中写入，原子保证
- **CDC 事件发布**：通过 WAL 监听实现零延迟事件发布

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

-- 幂等键表（与 events 同库，支持同事务写入）
CREATE TABLE idempotency_keys (
    command_id      VARCHAR(128) PRIMARY KEY,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 幂等键自动过期清理（72 小时）
CREATE INDEX idx_idempotency_created ON idempotency_keys (created_at);
```

## Trait 定义

```rust
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

/// 事件存储接口
#[async_trait]
pub trait EventStore: Send + Sync {
    /// 同步追加事件 + 幂等键（同一事务，原子保证）
    /// 调用方等待此方法返回后才响应客户端，保证零数据丢失
    ///
    /// fencing_token：集群模式下传 Some(token)，用于防止脑裂双写（详见 cluster.md）；
    /// 单机模式传 None，跳过 fencing 检查。
    async fn append_with_idempotency(
        &self,
        aggregate_id: &str,
        events: &[PendingEvent],
        expected_version: u64,
        command_id: &str,
        fencing_token: Option<u64>,
    ) -> Result<(), StoreError>;

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

## 同步写入实现

```rust
pub struct PgEventStore {
    pool: PgPool,
}

impl PgEventStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventStore for PgEventStore {
    async fn append_with_idempotency(
        &self,
        aggregate_id: &str,
        events: &[PendingEvent],
        expected_version: u64,
        command_id: &str,
        _fencing_token: Option<u64>,
    ) -> Result<(), StoreError> {
        if events.is_empty() { return Ok(()); }

        let mut tx = self.pool.begin().await
            .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        // 写入幂等键（ON CONFLICT 检测重复命令）
        let inserted = sqlx::query(
            "INSERT INTO idempotency_keys (command_id) VALUES ($1) \
             ON CONFLICT DO NOTHING"
        )
        .bind(command_id)
        .execute(&mut *tx).await
        .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        if inserted.rows_affected() == 0 {
            return Err(StoreError::DuplicateCommand(command_id.to_string()));
        }

        // 写入事件
        for (i, event) in events.iter().enumerate() {
            let version = expected_version + 1 + i as u64;
            sqlx::query(
                "INSERT INTO events \
                 (aggregate_id, aggregate_type, event_type, version, data) \
                 VALUES ($1, $2, $3, $4, $5)"
            )
            .bind(aggregate_id)
            .bind(&event.aggregate_type)
            .bind(&event.event_type)
            .bind(version as i64)
            .bind(&event.data)
            .execute(&mut *tx).await
            .map_err(|e| match e {
                sqlx::Error::Database(ref db_err)
                    if db_err.code() == Some("23505".into()) =>
                {
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
}
```

## 幂等存储（双层设计）

### command_id 约定

**command_id 必须全局唯一。** 推荐格式：`{aggregate_id}:{uuid}` 或纯 UUID。

### 第一层：滚动布隆过滤器（内存，零 IO）

```rust
use probabilistic_collections::bloom::BloomFilter;
use parking_lot::RwLock;

/// 滚动布隆过滤器：两代交替，防止无限增长
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
            let new_filter = BloomFilter::with_rate(
                self.false_positive_rate, self.capacity
            );
            let old_current = std::mem::replace(&mut inner.current, new_filter);
            inner.previous = old_current;
            inner.inserted = 0;
        }
    }
}
```

### 第二层：DB 精确检查（事务内）

幂等键存储在 `idempotency_keys` 表中，通过 `append_with_idempotency` 在同一事务中原子写入。

双层设计下，99% 的请求无需网络 IO 即可完成幂等判断：

| 方案 | 延迟 | 适用场景 |
|------|------|----------|
| 仅布隆过滤器 | ~10ns | 99% 的非重复请求直接放行 |
| 布隆命中 → DB 精确检查 | ~200μs | 1% 误判时精确检查 |

注：进程重启后布隆过滤器为空，需要启动预热避免穿透到 DB 层。

### 启动预热

服务启动时从 `idempotency_keys` 表加载最近 72 小时的 key 填充布隆过滤器，
避免重启后短时间内大量重复请求穿透到 DB：

```rust
impl RollingBloomFilter {
    /// 启动时从 DB 预热布隆过滤器
    pub async fn warm_up(
        &self,
        pool: &PgPool,
        retention: Duration,
    ) -> Result<usize> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(retention).unwrap();

        // 分批加载，避免一次性加载百万级 key
        let mut cursor = String::new();
        let batch_size = 10_000;
        let mut total = 0;

        loop {
            let keys: Vec<String> = sqlx::query_scalar(
                "SELECT command_id FROM idempotency_keys \
                 WHERE created_at > $1 AND command_id > $2 \
                 ORDER BY command_id LIMIT $3"
            )
            .bind(&cutoff)
            .bind(&cursor)
            .bind(batch_size as i64)
            .fetch_all(pool).await?;

            if keys.is_empty() { break; }

            for key in &keys {
                self.insert(key);
            }
            cursor = keys.last().unwrap().clone();
            total += keys.len();

            if keys.len() < batch_size { break; }
        }

        tracing::info!("布隆过滤器预热完成，加载 {} 个幂等键", total);
        Ok(total)
    }
}
```

启动流程中调用：

```rust
// 服务启动时预热（在接受请求之前）
gateway.idempotency_bloom
    .warm_up(&pool, Duration::from_secs(72 * 3600))
    .await?;
```

## 幂等键过期清理

幂等键表会随时间无限增长，需要定期清理过期条目。采用 pg_cron 定时任务：

```sql
-- 安装 pg_cron 扩展（需 superuser）
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 每小时清理超过 72 小时的幂等键
SELECT cron.schedule(
    'cleanup_idempotency_keys',
    '0 * * * *',
    $$DELETE FROM idempotency_keys WHERE created_at < NOW() - INTERVAL '72 hours'$$
);
```

如果环境不支持 pg_cron，可在应用层实现定时清理：

```rust
impl PgEventStore {
    /// 定期清理过期幂等键（建议每小时执行一次）
    pub async fn cleanup_expired_keys(&self, retention: Duration) -> Result<u64> {
        let cutoff = chrono::Utc::now() - chrono::Duration::from_std(retention).unwrap();
        let result = sqlx::query("DELETE FROM idempotency_keys WHERE created_at < $1")
            .bind(cutoff)
            .execute(&self.pool).await
            .map_err(|e| StoreError::ConnectionError(e.to_string()))?;
        Ok(result.rows_affected())
    }
}
```

清理策略：
- 保留时长 72 小时（覆盖客户端最长重试窗口）
- 每小时执行一次，每次删除量可控
- 清理不影响正确性：过期后重复命令会被 version conflict 拦截

## 连接池与超时配置

### 连接池大小

```rust
pub struct EventStoreConfig {
    /// 最大连接数 = CPU 核数 × 4
    /// 30K TPS / 单连接吞吐(~200 TPS) ≈ 150
    pub max_connections: u32,        // 默认 128
    /// 最小空闲连接数
    pub min_connections: u32,        // 默认 16
    /// 连接获取超时
    pub acquire_timeout: Duration,   // 默认 3s
    /// 单事务超时（防止长事务阻塞 WAL）
    pub statement_timeout: Duration, // 默认 5s
    /// 连接最大生命周期（防止连接泄漏）
    pub max_lifetime: Duration,      // 默认 30min
    /// 空闲连接回收时间
    pub idle_timeout: Duration,      // 默认 10min
    /// 数据库连接字符串
    pub database_url: String,
}
```

### 初始化

```rust
impl PgEventStore {
    pub async fn new(config: &EventStoreConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(config.acquire_timeout)
            .max_lifetime(config.max_lifetime)
            .idle_timeout(config.idle_timeout)
            .after_connect(|conn, _| Box::pin(async move {
                sqlx::query(&format!(
                    "SET statement_timeout = '{}ms'",
                    config.statement_timeout.as_millis()
                ))
                .execute(conn).await?;
                Ok(())
            }))
            .connect(&config.database_url).await?;
        Ok(Self { pool })
    }
}
```

### 写入重试策略

| 错误类型 | 是否重试 | 说明 |
|----------|----------|------|
| ConnectionError | 是（1次） | 网络闪断，重试安全（事务未提交则无副作用） |
| VersionConflict | 否 | 业务冲突，客户端决策 |
| DuplicateCommand | 否 | 幂等拦截，正常流程 |
| StatementTimeout | 否 | DB 负载过高，返回 503 让客户端退避重试 |

### 连接池大小计算

```
目标 TPS = 30,000
单事务延迟 ≈ 3ms（含 fsync）
单连接 TPS = 1000ms / 3ms ≈ 333
所需连接数 = 30,000 / 333 ≈ 90

考虑突发流量和长尾延迟，建议 max_connections = 128
```

注：实际部署前应通过基准测试验证，不同硬件和网络环境下延迟差异较大。

## 未来演进

当前设计为单实例 PostgreSQL，后续迁移至分布式数据库时需关注：
- `EventStore` trait 已预留 `fencing_token` 参数，集群模式下启用防脑裂检查
- 迁移时替换 `PgEventStore` 实现即可，上层代码无需修改
- 分片策略、CDC 拓扑等在迁移时重新规划
