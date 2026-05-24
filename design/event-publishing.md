# 领域事件发布（高性能版：CDC + Kafka）

## 核心变化

- **CDC 替代 Outbox 轮询**：通过 PostgreSQL WAL 监听实现近实时事件捕获
- **零额外写入**：无需 outbox 表，事件写入 events 表即自动触发发布
- **更低延迟**：WAL 监听延迟 < 10ms，远优于轮询模式的 poll_interval

## CDC 实现方案

### Debezium（推荐生产环境）

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
        let send_futures: Vec<_> = events.iter().map(|event| {
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
        for result in futures::future::join_all(send_futures).await {
            result.map_err(|(e, _)| Error::publish(e))?;
        }
        Ok(())
    }
}
```

## 消费者端设计

### 事件消费者

```rust
pub struct EventConsumer {
    consumer: StreamConsumer,
    handler: Arc<dyn EventHandler>,
}

impl EventConsumer {
    pub async fn run(&self) -> ! {
        loop {
            match self.consumer.recv().await {
                Ok(msg) => {
                    let event = self.deserialize_message(&msg);
                    if let Err(e) = self.handler.handle(&event).await {
                        tracing::error!("事件处理失败: {e}");
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
pub trait EventHandler: Send + Sync {
    async fn handle(&self, event: &DomainEvent) -> Result<()>;
}
```

### 消费者注意事项

| 关切 | 处理方式 |
|------|----------|
| 至少一次投递 | 消费者需幂等处理（用 aggregate_id + version 去重） |
| 顺序保证 | 同 partition（同聚合）内有序 |
| 消费者组 | 不同消费者用不同 consumer group 独立消费 |
| 重放 | offset reset 从头重放 |
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
       │ 消费者:查询      │  │ 消费者:报表      │    │ 外部系统通知      │
       │ (更新读模型)     │  │ (聚合统计)       │    │ (webhook/邮件)    │
       └─────────────────┘  └─────────────────┘    └──────────────────┘
```

## 性能指标

| 指标 | CDC 模式 |
|------|----------|
| 发布延迟 | < 10ms |
| 额外写入 IO | 0 |
| 吞吐上限 | WAL 带宽限制 (~100K/s) |
| 运维复杂度 | 中（需管理 replication slot） |
| 数据一致性 | 强（WAL 保证） |
