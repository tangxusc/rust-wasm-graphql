# 读模型与 Projection 设计

## 概述

事件溯源架构中，读模型（Read Model）通过消费领域事件构建查询优化的视图。
Projection 是将事件流转换为读模型的过程。

## 架构

```
Event Store (写入侧)
    │
    │ CDC / Kafka
    ▼
┌─────────────────────────────────────────────┐
│           Projection Engine                  │
│                                              │
│  ┌──────────────┐  ┌──────────────┐         │
│  │ inventory    │  │ order        │         │
│  │ projection   │  │ projection   │  ...    │
│  └──────┬───────┘  └──────┬───────┘         │
└─────────┼──────────────────┼────────────────┘
          │                  │
          ▼                  ▼
┌──────────────┐    ┌──────────────┐
│  Read DB     │    │  Read DB     │
│  (PostgreSQL │    │  (Redis /    │
│   / ES)      │    │   MongoDB)   │
└──────────────┘    └──────────────┘
          │                  │
          ▼                  ▼
┌─────────────────────────────────────────────┐
│         GraphQL Query Layer                  │
└─────────────────────────────────────────────┘
```

## Projection Trait

```rust
#[async_trait]
pub trait Projection: Send + Sync {
    /// 投影名称（用于 consumer group 和 offset 管理）
    fn name(&self) -> &str;

    /// 处理单个事件，更新读模型
    async fn apply(&self, event: &DomainEvent) -> Result<()>;

    /// 重置投影（用于重建）
    async fn reset(&self) -> Result<()>;
}
```

## 读模型存储选型

| 查询模式 | 推荐存储 | 理由 |
|----------|---------|------|
| 单实体查询（by ID） | Redis / PostgreSQL | 低延迟点查 |
| 列表/分页查询 | PostgreSQL | SQL 灵活性 |
| 全文搜索 | Elasticsearch | 倒排索引 |
| 聚合统计/报表 | ClickHouse / TimescaleDB | 列式存储 |

## 一致性模型

读模型是**最终一致**的。事件写入 Event Store 后，经过 CDC → Kafka → Projection 管道，
读模型的更新存在延迟。

| 指标 | 目标值 |
|------|--------|
| 正常延迟 | < 100ms |
| P99 延迟 | < 500ms |
| 最大容忍延迟 | < 5s（超过则告警） |

### 客户端处理最终一致性

```graphql
# 方案 1：Mutation 返回足够信息，客户端无需立即查询
mutation {
  inventory {
    createItem(...) {
      success
      version        # 客户端可用此版本做乐观 UI 更新
      eventCount
    }
  }
}

# 方案 2：查询时指定最小版本（read-your-writes）
query {
  inventory {
    item(id: "item-001", minVersion: 5) {
      name
      quantity
    }
  }
}
```

### minVersion 语义定义

当客户端指定 `minVersion` 时，采用**短轮询+超时**策略：

| 读模型版本 vs minVersion | 行为 |
|--------------------------|------|
| version >= minVersion | 立即返回数据 |
| version < minVersion | 短轮询等待（最多 max_wait），超时后返回当前数据 + 标记 |

```rust
pub struct ReadYourWritesPolicy {
    pub max_wait: Duration,        // 最大等待时间（默认 2s）
    pub poll_interval: Duration,   // 轮询间隔（默认 50ms）
}

/// 查询时等待读模型追上指定版本
pub async fn query_with_min_version(
    db: &PgPool,
    aggregate_id: &str,
    min_version: u64,
    policy: &ReadYourWritesPolicy,
) -> Result<QueryResult> {
    let deadline = Instant::now() + policy.max_wait;

    loop {
        let row = sqlx::query("SELECT * FROM inventory_view WHERE id = $1")
            .bind(aggregate_id)
            .fetch_optional(db).await?;

        match row {
            Some(r) if r.version >= min_version as i64 => {
                return Ok(QueryResult { data: r, stale: false });
            }
            _ if Instant::now() >= deadline => {
                // 超时：返回当前数据（可能为 None）+ stale 标记
                return Ok(QueryResult { data: row, stale: true });
            }
            _ => {
                tokio::time::sleep(policy.poll_interval).await;
            }
        }
    }
}
```

GraphQL 返回中增加 `stale` 标记：

```graphql
type InventoryItem {
  id: String!
  name: String!
  quantity: Int!
  version: Int!
  stale: Boolean!    # true 表示读模型尚未追上请求的 minVersion
}
```

## Projection 实现示例

```rust
pub struct InventoryProjection {
    db: Arc<PgPool>,
}

#[async_trait]
impl Projection for InventoryProjection {
    fn name(&self) -> &str { "inventory-read-model" }

    async fn apply(&self, event: &DomainEvent) -> Result<()> {
        match event.event_type.as_str() {
            "item-created" => {
                let data: ItemCreatedData = serde_json::from_slice(&event.data)?;
                sqlx::query(
                    "INSERT INTO inventory_view (id, name, quantity, version, updated_at) \
                     VALUES ($1, $2, $3, $4, NOW()) \
                     ON CONFLICT (id) DO NOTHING"
                )
                .bind(&event.aggregate_id)
                .bind(&data.name)
                .bind(data.quantity as i32)
                .bind(event.version as i64)
                .execute(&*self.db).await?;
            }
            "stock-adjusted" => {
                let data: StockAdjustedData = serde_json::from_slice(&event.data)?;
                sqlx::query(
                    "UPDATE inventory_view SET quantity = $1, version = $2, updated_at = NOW() \
                     WHERE id = $3 AND version < $2"
                )
                .bind(data.new_quantity as i32)
                .bind(event.version as i64)
                .bind(&event.aggregate_id)
                .execute(&*self.db).await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        sqlx::query("TRUNCATE inventory_view").execute(&*self.db).await?;
        Ok(())
    }
}
```

## 幂等性保证

Projection 消费者必须幂等——同一事件可能被投递多次（Kafka at-least-once）。

策略：使用 `(aggregate_id, version)` 作为幂等键：

```sql
-- UPDATE 中的 WHERE version < $2 天然幂等
-- INSERT 中的 ON CONFLICT DO NOTHING 天然幂等
```

## 投影重建

当读模型 schema 变更或投影逻辑修改时，需要重建。

### 蓝绿重建策略（零停机）

为避免重建期间查询返回空或不完整数据，采用蓝绿部署模式：

```
旧投影（蓝）：继续服务查询
新投影（绿）：后台从头消费事件，构建新读模型
切换：新投影追上实时后，原子切换查询路由
清理：删除旧投影数据
```

```rust
impl ProjectionEngine {
    /// 蓝绿重建：新投影追上实时后原子切换，查询零停机
    pub async fn rebuild_blue_green(&self, projection: &dyn Projection) -> Result<()> {
        let new_name = format!("{}_rebuild_{}", projection.name(), now_millis());

        // 1. 创建新的 consumer group，从头消费
        let new_consumer = self.create_consumer(&new_name, OffsetReset::Earliest).await?;
        tracing::info!("投影 {} 蓝绿重建开始，新 group: {new_name}", projection.name());

        // 2. 新投影写入临时表/索引（与旧表并存）
        let new_projection = projection.clone_for_rebuild(&new_name)?;
        self.run_until_caught_up(&new_consumer, &*new_projection).await?;

        // 3. 追上实时后，原子切换查询路由
        self.swap_active(projection.name(), &new_name).await?;
        tracing::info!("投影 {} 切换完成", projection.name());

        // 4. 清理旧投影数据和 consumer group
        projection.reset().await?;
        self.delete_consumer_group(projection.name()).await?;

        Ok(())
    }

    /// 判断新投影是否已追上实时（consumer lag < 阈值）
    async fn run_until_caught_up(
        &self,
        consumer: &StreamConsumer,
        projection: &dyn Projection,
    ) -> Result<()> {
        loop {
            let msg = consumer.recv().await?;
            let event = self.deserialize_message(&msg);
            projection.apply(&event).await?;
            consumer.commit_message(&msg, CommitMode::Async)?;

            // 每 1000 条检查一次 lag
            if self.check_lag(consumer).await? < 100 {
                break;
            }
        }
        Ok(())
    }
}
```

### 简单重建（允许短暂不可用）

开发环境或可接受短暂降级的场景：

```rust
impl ProjectionEngine {
    pub async fn rebuild_simple(&self, projection: &dyn Projection) -> Result<()> {
        // 1. 重置读模型
        projection.reset().await?;

        // 2. 重置 Kafka consumer offset 到最早
        self.reset_offset(projection.name()).await?;

        // 3. 从头消费所有事件
        tracing::info!("投影 {} 开始重建（查询暂时不可用）", projection.name());
        Ok(())
    }
}
```

## 与 GraphQL Query 层的集成

现有的 Query 自动映射（WASM 函数 → GraphQL Query 字段）保持不变。
Projection 读模型作为额外的查询数据源：

```rust
pub fn build_dynamic_schema(
    registry: &WasmRegistry,
    gateway: Arc<CommandGateway>,
    read_db: Arc<PgPool>,
) -> Schema {
    let mut query = Object::new("Query");
    let mut mutation = Object::new("Mutation");

    for module in registry.modules() {
        // 现有逻辑：WASM 查询函数 → Query 字段
        if module.has_query_functions() {
            let module_obj = build_module_query(&module);
            query = query.field(/* ... */);
        }

        // 新增：Projection 读模型 → Query 字段
        if module.is_aggregate() {
            let read_obj = build_read_model_query(&module, read_db.clone());
            query = query.field(/* ... */);
        }

        // 聚合模块 → Mutation 字段
        if module.is_aggregate() {
            let module_obj = build_aggregate_mutation(&module, gateway.clone());
            mutation = mutation.field(/* ... */);
        }
    }

    Schema::build(query, Some(mutation), None).finish()
}
```

## 监控

| 指标 | 告警阈值 | 含义 |
|------|----------|------|
| consumer_lag | > 10,000 | 投影落后事件流过多 |
| projection_latency_p99 | > 5s | 读模型更新延迟过高 |
| projection_errors | > 0 | 投影应用失败（需人工检查） |
| rebuild_duration | > 1h | 重建耗时过长 |
