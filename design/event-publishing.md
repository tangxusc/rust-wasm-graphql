# 领域事件发布（高性能版：CDC + Kafka）

## 核心变化

- **CDC 替代 Outbox 轮询**：通过 PostgreSQL WAL 监听实现近实时事件捕获
- **零额外写入**：无需 outbox 表，事件写入 events 表即自动触发发布
- **更低延迟**：WAL 监听延迟 < 10ms，远优于轮询模式的 poll_interval

## 架构对比

### 旧方案：Outbox 轮询

```
写入 events + outbox (同事务) → 轮询 outbox → 发送 Kafka → 标记 published
延迟：poll_interval (100ms-1s)
额外写入：每事件 2 次 INSERT
```

### 新方案：CDC (Change Data Capture)

```
写入 events → PostgreSQL WAL → Debezium/自研 CDC → Kafka
延迟：< 10ms
额外写入：0
```

## CDC 实现方案

### 方案 A：Debezium（推荐生产环境）

```yaml
# Debezium connector 配置
{
  "name": "events-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "pg-shard-0",
    "database.port": "5432",
    "database.dbname": "eventstore",
    "table.include.list": "public.events",
    "topic.prefix": "domain.events",
    "publication.name": "events_pub",
    "slot.name": "events_slot",
    "plugin.name": "pgoutput",
    "transforms": "route",
    "transforms.route.type": "io.debezium.transforms.ByLogicalTableRouter",
    "transforms.route.topic.regex": "(.*)events(.*)",
    "transforms.route.topic.replacement": "domain.events.$1"
  }
}
```

### 方案 B：自研 WAL 监听（轻量级/嵌入式）

```rust
use tokio_postgres::replication::LogicalReplicationStream;

pub struct WalListener {
    slot_name: String,
    publication: String,
    kafka_producer: Arc<KafkaPublisher>,
    connect_config: tokio_postgres::Config,
    max_retries: u32,
    base_backoff: Duration,
}

impl WalListener {
    pub async fn run(&self) -> ! {
        let mut consecutive_failures = 0u32;

        loop {
            match self.run_stream().await {
                Ok(()) => {
                    // stream 正常结束（不应发生），重置计数器后重连
                    consecutive_failures = 0;
                    tracing::warn!("WAL stream 意外结束，立即重连");
                }
                Err(e) => {
                    consecutive_failures += 1;
                    let backoff = self.calculate_backoff(consecutive_failures);
                    tracing::error!(
                        "WAL 监听失败 (连续第 {consecutive_failures} 次): {e}, \
                         {backoff:?} 后重试"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// 指数退避：base * 2^(failures-1)，上限 60s
    fn calculate_backoff(&self, failures: u32) -> Duration {
        let multiplier = 2u64.pow((failures - 1).min(6));
        let backoff = self.base_backoff * multiplier as u32;
        backoff.min(Duration::from_secs(60))
    }

    async fn run_stream(&self) -> Result<()> {
        let (client, connection) = self.connect_config.connect(tokio_postgres::NoTls).await?;
        tokio::spawn(connection);

        let stream = client
            .copy_both_simple::<bytes::Bytes>(&format!(
                "START_REPLICATION SLOT {} LOGICAL 0/0 (proto_version '1', publication_names '{}')",
                self.slot_name, self.publication
            ))
            .await?;

        let mut stream = LogicalReplicationStream::new(stream);

        while let Some(msg) = stream.next().await {
            match msg? {
                ReplicationMessage::XLogData(data) => {
                    let events = self.parse_wal_events(&data.data())?;
                    self.kafka_producer.publish(&events).await?;
                    let lsn = data.wal_end();
                    stream.standby_status_update(lsn, lsn, PgLsn::from(0), 0, 0).await?;
                }
                ReplicationMessage::PrimaryKeepAlive(ka) if ka.reply() == 1 => {
                    let lsn = ka.wal_end();
                    stream.standby_status_update(lsn, lsn, PgLsn::from(0), 0, 0).await?;
                }
                _ => {}
            }
        }

        Ok(())
    }
}
```

## Kafka Topic 设计

### 命名约定

```
domain.events.{aggregate_type}
```

示例：
- `domain.events.inventory`
- `domain.events.order`
- `domain.events.payment`

### Partition 策略

使用 `aggregate_id` 作为 message key：
- 同一聚合的事件落在同一 partition → 保证顺序
- 不同聚合天然并行消费
- Partition 数量建议 = 消费者实例数 × 2（预留扩展）

### Message 结构

```json
{
  "key": "item-001",
  "headers": {
    "event-type": "item-created",
    "aggregate-type": "inventory",
    "version": "3",
    "timestamp": "1716278400000"
  },
  "value": "<序列化的事件数据 bytes>"
}
```

## Fallback：Outbox 模式（CDC 不可用时）

如果部署环境无法使用 CDC（如 SQLite 本地开发），退回 Outbox 轮询：

```rust
pub struct OutboxRelay {
    event_store: Arc<dyn EventStoreShard>,
    publisher: Arc<dyn EventPublisher>,
    poll_interval: Duration,        // 50-100ms
    batch_size: usize,              // 500
}

impl OutboxRelay {
    pub async fn run(&self) -> ! {
        loop {
            match self.process_batch().await {
                Ok(count) if count > 0 => continue,
                Ok(_) => tokio::time::sleep(self.poll_interval).await,
                Err(e) => {
                    tracing::error!("Outbox relay 错误: {e}");
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }

    async fn process_batch(&self) -> Result<usize> {
        let unpublished = self.event_store.fetch_unpublished(self.batch_size).await?;
        let count = unpublished.len();
        if count == 0 { return Ok(0); }

        self.publisher.publish(&unpublished).await?;
        let ids: Vec<i64> = unpublished.iter().map(|e| e.id).collect();
        self.event_store.mark_published_batch(&ids).await?;
        Ok(count)
    }
}
```

## EventPublisher Trait

```rust
#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, events: &[DomainEvent]) -> Result<()>;
}
```

## Kafka 高性能实现

```rust
pub struct KafkaPublisher {
    producer: FutureProducer,
    topic_prefix: String,
}

impl KafkaPublisher {
    pub fn new(brokers: &str, topic_prefix: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("linger.ms", "5")              // 批量发送延迟
            .set("batch.num.messages", "1000")   // 批量大小
            .set("compression.type", "lz4")      // 压缩
            .set("acks", "all")                  // 所有 ISR 副本确认，保证不丢消息
            .set("retries", "3")
            .set("enable.idempotence", "true")   // 生产者幂等，防止重复投递
            .create()
            .unwrap();

        Self { producer, topic_prefix: topic_prefix.to_string() }
    }
}

#[async_trait]
impl EventPublisher for KafkaPublisher {
    async fn publish(&self, events: &[DomainEvent]) -> Result<()> {
        let futures: Vec<_> = events.iter().map(|event| {
            let topic = format!("{}.{}", self.topic_prefix, event.aggregate_type);
            let record = FutureRecord::to(&topic)
                .key(event.aggregate_id.as_bytes())
                .payload(&event.data)
                .headers(OwnedHeaders::new()
                    .insert(Header { key: "event-type", value: Some(event.event_type.as_bytes()) })
                    .insert(Header { key: "version", value: Some(event.version.to_string().as_bytes()) })
                );
            self.producer.send(record, Timeout::After(Duration::from_secs(5)))
        }).collect();

        // 并行等待所有发送完成
        for result in futures::future::join_all(futures).await {
            result.map_err(|(e, _)| Error::publish(e))?;
        }
        Ok(())
    }
}
```

## 消费者端设计

### Projection 消费者

```rust
pub struct ProjectionConsumer {
    consumer: StreamConsumer,
    projection: Arc<dyn Projection>,
}

impl ProjectionConsumer {
    pub async fn run(&self) -> ! {
        loop {
            match self.consumer.recv().await {
                Ok(msg) => {
                    let event = self.deserialize_message(&msg);
                    if let Err(e) = self.projection.apply(&event).await {
                        tracing::error!("投影应用失败: {e}");
                        // 不提交 offset，下次重试
                        continue;
                    }
                    self.consumer.commit_message(&msg, CommitMode::Async).unwrap();
                }
                Err(e) => tracing::error!("消费错误: {e}"),
            }
        }
    }
}

#[async_trait]
pub trait Projection: Send + Sync {
    async fn apply(&self, event: &DomainEvent) -> Result<()>;
}
```

### 消费者注意事项

| 关切 | 处理方式 |
|------|----------|
| 至少一次投递 | 消费者需幂等处理（用 aggregate_id + version 去重） |
| 顺序保证 | 同 partition（同聚合）内有序 |
| 消费者组 | 不同投影用不同 consumer group 独立消费 |
| 重放 | offset reset 从头重放，用于重建投影 |
| 延迟监控 | consumer lag 告警（Kafka 自带指标） |

## 部署拓扑

```
┌─────────────────┐         ┌──────────────────────┐
│  Host Server    │────────▶│  PostgreSQL (分片)    │
│  (批量写入)     │         │  events 表           │
└─────────────────┘         └──────────┬───────────┘
                                       │ WAL (CDC)
                            ┌──────────▼───────────┐
                            │  Debezium / WAL监听   │
                            └──────────┬───────────┘
                                       │
                            ┌──────────▼───────────┐
                            │       Kafka          │
                            │  domain.events.*     │
                            └──────────┬───────────┘
                                       │
                 ┌─────────────────────┼───────────────────────┐
                 ▼                     ▼                       ▼
       ┌─────────────────┐  ┌─────────────────┐    ┌──────────────────┐
       │ Projection:查询  │  │ Projection:报表  │    │ 外部系统通知      │
       │ (更新读模型)     │  │ (聚合统计)       │    │ (webhook/邮件)    │
       └─────────────────┘  └─────────────────┘    └──────────────────┘
```

## 性能指标

| 指标 | CDC 模式 | Outbox 轮询模式 |
|------|----------|----------------|
| 发布延迟 | < 10ms | 50-1000ms |
| 额外写入 IO | 0 | 1x (outbox INSERT) |
| 吞吐上限 | WAL 带宽限制 (~100K/s) | 轮询频率限制 |
| 运维复杂度 | 中（需管理 replication slot） | 低 |
| 数据一致性 | 强（WAL 保证） | 强（同事务） |
