# GraphQL 层设计（单机版）

## 设计原则

- GraphQL Mutation 作为命令入口，直接路由到 VirtualActorRuntime
- `expectedVersion` 和 `aggregateId` 由 Host 自动注入为每个 mutation 的必填参数
- 命令参数从 WIT record 类型自动生成 GraphQL schema

## 生成的 GraphQL Schema 示例

```graphql
type Mutation {
  inventory: InventoryMutation!
}

type InventoryMutation {
  createItem(
    aggregateId: String!
    expectedVersion: Int!
    name: String!
    quantity: Int!
  ): CommandResult!

  adjustStock(
    aggregateId: String!
    expectedVersion: Int!
    delta: Int!
  ): CommandResult!
}

type CommandResult {
  success: Boolean!
  version: Int!
  eventCount: Int!
  error: String
}
```

## 客户端调用示例

```graphql
mutation {
  inventory {
    createItem(
      aggregateId: "item-001"
      expectedVersion: 0
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

## Schema 构建

```rust
pub fn build_dynamic_schema(
    registry: &WasmRegistry,
    runtime: Arc<VirtualActorRuntime>,
) -> Schema {
    let mut mutation = Object::new("Mutation");

    for module in registry.modules() {
        if module.is_aggregate() {
            let module_obj = build_aggregate_mutation(&module, runtime.clone());
            mutation = mutation.field(/* namespace field */);
        }
    }

    Schema::build(Object::new("Query"), Some(mutation), None).finish()
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
/// 从 GraphQL 参数中提取 aggregateId/expectedVersion，剩余参数转为 kebab-case 后序列化
pub fn extract_command_meta(args: &IndexMap<Name, Value>) -> Result<IncomingCommand> {
    let aggregate_id = args.get("aggregateId")
        .and_then(|v| v.as_str())
        .ok_or(Error::missing_field("aggregateId"))?
        .to_string();

    let expected_version = args.get("expectedVersion")
        .and_then(|v| v.as_u64())
        .ok_or(Error::missing_field("expectedVersion"))?;

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
