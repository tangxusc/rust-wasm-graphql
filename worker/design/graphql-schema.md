# GraphQL 层设计（单机版）

## 设计原则

- GraphQL Mutation 作为命令入口，直接路由到 VirtualActorRuntime
- GraphQL Query 提供基础查询能力（健康检查、聚合版本查询）
- `expectedVersion` 和 `aggregateId` 由 Host 自动注入为每个 mutation 的必填参数
- `aggregateId` 在 GraphQL 层进行格式校验，拒绝非法输入
- 命令参数从 WIT record 类型自动生成 GraphQL schema

## 生成的 GraphQL Schema 示例

```graphql
"""64 位无符号整数，序列化为字符串避免 JSON/GraphQL Int 32 位溢出"""
scalar UInt64

type Query {
  health: Boolean!
  aggregateVersion(
    aggregateType: String!
    aggregateId: String!
  ): UInt64
}

type Mutation {
  inventory: InventoryMutation!
}

type InventoryMutation {
  createItem(
    aggregateId: String!
    expectedVersion: UInt64!
    name: String!
    quantity: Int!
  ): CommandResult!

  adjustStock(
    aggregateId: String!
    expectedVersion: UInt64!
    delta: Int!
  ): CommandResult!
}

type CommandResult {
  success: Boolean!
  version: UInt64!
  eventCount: Int!
  error: String
}
```

> **注意**：`UInt64` 自定义 scalar 在 JSON 中序列化为字符串（如 `"12345"`），
> 客户端需将其解析为 64 位整数。这避免了 GraphQL `Int`（32 位有符号）在高频
> 聚合场景下的溢出风险。

## 客户端调用示例

```graphql
mutation {
  inventory {
    createItem(
      aggregateId: "item-001"
      expectedVersion: "0"
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

> 注意：`expectedVersion` 和返回的 `version` 均为字符串形式的 64 位整数。

## Schema 构建

```rust
pub fn build_dynamic_schema(
    registry: &WasmRegistry,
    runtime: Arc<VirtualActorRuntime>,
    event_store: Arc<dyn EventStore>,
) -> Schema {
    // Query 类型：提供健康检查和聚合版本查询
    let query = build_query_type(event_store);

    let mut mutation = Object::new("Mutation");
    for module in registry.modules() {
        if module.is_aggregate() {
            let module_obj = build_aggregate_mutation(&module, runtime.clone());
            mutation = mutation.field(/* namespace field */);
        }
    }

    Schema::build(query, Some(mutation), None).finish()
}

fn build_query_type(event_store: Arc<dyn EventStore>) -> Object {
    let store = event_store.clone();
    Object::new("Query")
        .field(Field::new("health", TypeRef::named_nn(TypeRef::BOOLEAN), |_ctx| {
            FieldFuture::new(async { Ok(Some(FieldValue::value(true))) })
        }))
        .field(
            Field::new("aggregateVersion", TypeRef::named(TypeRef::INT), move |ctx| {
                let store = store.clone();
                FieldFuture::new(async move {
                    let aggregate_type = ctx.args.try_get("aggregateType")?.string()?;
                    let aggregate_id = ctx.args.try_get("aggregateId")?.string()?;
                    validate_aggregate_id(aggregate_id)?;
                    match store.get_current_version(aggregate_type, aggregate_id).await? {
                        Some(version) => Ok(Some(FieldValue::value(version as i64))),
                        None => Ok(None),
                    }
                })
            })
            .argument(InputValue::new("aggregateType", TypeRef::named_nn(TypeRef::STRING)))
            .argument(InputValue::new("aggregateId", TypeRef::named_nn(TypeRef::STRING)))
        )
}

fn build_aggregate_mutation(module: &WasmEngine, runtime: Arc<VirtualActorRuntime>) -> Object {
    let module_name = module.name().to_string();
    let mut obj = Object::new(format!("{}Mutation", module.pascal_name()));

    for cmd in module.commands() {
        let runtime = runtime.clone();
        let module_name = module_name.clone();
        let command_type = cmd.name.clone();

        obj = obj.field(
            Field::new(cmd.graphql_name(), TypeRef::named_nn("CommandResult"), move |ctx| {
                let runtime = runtime.clone();
                let module_name = module_name.clone();
                let command_type = command_type.clone();

                FieldFuture::new(async move {
                    let args = ctx.args;
                    let mut command = extract_command_meta(&args)?;
                    command.module = module_name;
                    command.command_type = command_type;

                    let result = runtime.send(&command.aggregate_id, command).await?;
                    Ok(Some(FieldValue::owned_any(result)))
                })
            })
            .argument(InputValue::new("aggregateId", TypeRef::named_nn(TypeRef::STRING)))
            .argument(InputValue::new("expectedVersion", TypeRef::named_nn(TypeRef::INT)))
            .arguments(cmd.domain_args_as_graphql())
        );
    }

    obj
}
```

## 元数据提取

```rust
/// aggregateId 格式校验：仅允许字母数字和 -_，长度 1-128
fn validate_aggregate_id(id: &str) -> Result<(), Error> {
    if id.is_empty() || id.len() > 128 {
        return Err(Error::invalid_input("aggregateId 长度必须在 1-128 之间"));
    }
    if !id.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
        return Err(Error::invalid_input("aggregateId 仅允许字母数字和 -_"));
    }
    Ok(())
}

/// 从 GraphQL 参数中提取 aggregateId/expectedVersion，剩余参数转为 kebab-case 后序列化
pub fn extract_command_meta(args: &IndexMap<Name, Value>) -> Result<IncomingCommand> {
    let aggregate_id = args.get("aggregateId")
        .and_then(|v| v.as_str())
        .ok_or(Error::missing_field("aggregateId"))?
        .to_string();

    validate_aggregate_id(&aggregate_id)?;

    // UInt64 scalar 以字符串形式传入，需解析为 u64
    let expected_version: u64 = args.get("expectedVersion")
        .and_then(|v| v.as_str())
        .ok_or(Error::missing_field("expectedVersion"))?
        .parse()
        .map_err(|_| Error::invalid_input("expectedVersion 必须为有效的非负整数"))?;

    // 领域参数：camelCase → kebab-case（与 WIT record 字段名一致）
    let domain_args: IndexMap<String, Value> = args.iter()
        .filter(|(k, _)| !["aggregateId", "expectedVersion"].contains(&k.as_str()))
        .map(|(k, v)| (camel_to_kebab(k.as_str()), v.clone()))
        .collect();

    let data = serde_json::to_vec(&domain_args)?;

    Ok(IncomingCommand {
        aggregate_id,
        expected_version,
        data,
        module: String::new(),
        command_type: String::new(),
    })
}
```

## 模块类型识别

Host 通过 WIT 内省判断模块类型：

| 导出接口 | 模块类型 | GraphQL 映射 |
|----------|----------|-------------|
| 包含至少一个 `handle-X` 函数 | 聚合模块 | Mutation 字段 |
| 仅包含普通函数（无 `handle-X`） | 查询模块 | Query 字段 |
