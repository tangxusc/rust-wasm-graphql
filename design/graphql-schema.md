# GraphQL 层设计（高性能版）

## command_id 与 aggregate_id 的传递方式

采用**约定式方案**：命令参数中内嵌元数据字段，Host 在解析时自动识别并提取。

### WIT 约定

每个命令通过 `{command-name}-params` record 定义参数结构，Host 通过 WIT 内省自动生成 GraphQL 参数：

```wit
// 参数类型定义（Host 自动读取字段信息生成 GraphQL schema）
record create-item-params {
    name: string,
    quantity: u32,
}

record adjust-stock-params {
    delta: s32,
}
```

`commandId` 和 `aggregateId` 由 Host 自动注入为每个 mutation 的必填参数，无需在 WIT record 中定义。

### 生成的 GraphQL Schema

```graphql
type Mutation {
  # 每个聚合模块生成一个命名空间
  inventory: InventoryMutation!
  order: OrderMutation!
}

type InventoryMutation {
  createItem(
    commandId: String!      # 幂等键
    aggregateId: String!    # 聚合根 ID
    name: String!
    quantity: Int!
  ): CommandResult!

  adjustStock(
    commandId: String!
    aggregateId: String!
    delta: Int!
  ): CommandResult!
}

# 统一的命令执行结果
type CommandResult {
  success: Boolean!
  version: Int!             # 新版本号
  eventCount: Int!          # 产出的事件数量
  error: String            # 业务错误信息（如有）
}
```

### 客户端调用示例

```graphql
mutation {
  inventory {
    createItem(
      commandId: "550e8400-e29b-41d4-a716-446655440000"
      aggregateId: "item-001"
      name: "螺丝刀"
      quantity: 100
    ) {
      success
      version
      eventCount
    }
  }
}
```

### Host 侧元数据提取

```rust
/// 约定的元数据字段名（WIT kebab-case）
const COMMAND_ID_FIELD: &str = "command-id";
const AGGREGATE_ID_FIELD: &str = "aggregate-id";

/// 从 GraphQL 参数中提取元数据，剩余参数序列化为命令数据
pub fn extract_command_meta(
    args: &IndexMap<Name, Value>,
) -> Result<IncomingCommand> {
    let command_id = args
        .get("commandId")
        .and_then(|v| v.as_str())
        .ok_or(Error::missing_field("commandId"))?
        .to_string();

    let aggregate_id = args
        .get("aggregateId")
        .and_then(|v| v.as_str())
        .ok_or(Error::missing_field("aggregateId"))?
        .to_string();

    let domain_args: IndexMap<Name, Value> = args
        .iter()
        .filter(|(k, _)| k.as_str() != "commandId" && k.as_str() != "aggregateId")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // 领域参数序列化为 bytes（传递给 WASM）
    let data = serde_json::to_vec(&domain_args)?;

    Ok(IncomingCommand { command_id, aggregate_id, data, module: String::new(), command_type: String::new() })
}
```

### Schema 构建（适配 Virtual Actor）

```rust
/// Mutation resolver 通过 Gateway 透明寻址到 Virtual Actor
pub fn build_dynamic_schema(
    registry: &WasmRegistry,
    gateway: Arc<CommandGateway>,
) -> Schema {
    let mut query = Object::new("Query");
    let mut mutation = Object::new("Mutation");

    for module in registry.modules() {
        if module.is_aggregate() {
            // 聚合模块 → Mutation 字段，每个命令生成独立的 mutation field
            let module_obj = build_aggregate_mutation(&module, gateway.clone());
            mutation = mutation.field(/* ... */);
        }
        if module.has_query_functions() {
            // 查询函数 → Query 字段（现有逻辑不变）
            let module_obj = build_module_query(&module);
            query = query.field(/* ... */);
        }
    }

    Schema::build(query, Some(mutation), None).finish()
}

/// 聚合模块的 Mutation 字段构建（基于 WIT 内省发现的命令列表）
fn build_aggregate_mutation(module: &WasmEngine, gateway: Arc<CommandGateway>) -> Object {
    let module_name = module.name().to_string();
    let mut obj = Object::new(format!("{}Mutation", module.pascal_name()));

    for cmd in module.commands() {
        let gateway = gateway.clone();
        let module_name = module_name.clone();
        let command_type = cmd.name.clone();

        obj = obj.field(
            Field::new(cmd.graphql_name(), TypeRef::named_nn("CommandResult"), move |ctx| {
                let gateway = gateway.clone();
                let module_name = module_name.clone();
                let command_type = command_type.clone();

                FieldFuture::new(async move {
                    let args = ctx.args;
                    let mut command = extract_command_meta(&args)?;
                    command.module = module_name;
                    command.command_type = command_type;

                    // 透明寻址：Gateway → VirtualActorRuntime → Actor
                    let result = gateway.execute(command).await?;

                    Ok(Some(FieldValue::owned_any(result)))
                })
            })
            .argument(InputValue::new("commandId", TypeRef::named_nn(TypeRef::STRING)))
            .argument(InputValue::new("aggregateId", TypeRef::named_nn(TypeRef::STRING)))
            .arguments(cmd.domain_args_as_graphql())
        );
    }

    obj
}
```

### 模块类型识别

Host 通过 WIT 内省判断模块类型：

| 导出接口 | 模块类型 | GraphQL 映射 |
|----------|----------|-------------|
| 仅包含普通函数 | 查询模块 | Query 字段 |
| 包含 `validate-X` + `handle-X` 对 + `apply-events` | 聚合模块 | Mutation 字段 |
| 两者都有 | 混合模块 | Query + Mutation |

### GraphQL Subscription（事件推送）

高性能场景下可通过 Subscription 实时推送领域事件：

```graphql
type Subscription {
  # 订阅特定聚合的事件流
  events(aggregateId: String!): DomainEventPayload!
  
  # 订阅特定类型的所有事件
  eventsByType(aggregateType: String!): DomainEventPayload!
}

type DomainEventPayload {
  aggregateId: String!
  eventType: String!
  version: Int!
  data: JSON!
  timestamp: String!
}
```

实现基于 Kafka 消费者 → tokio broadcast channel → GraphQL Subscription stream。
