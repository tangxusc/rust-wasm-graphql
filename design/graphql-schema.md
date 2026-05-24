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

/// 从 GraphQL 参数中提取元数据，剩余参数转为 kebab-case 后序列化为命令数据
///
/// 命名转换链：
///   WIT record 字段: item-name (kebab-case)
///   → GraphQL 参数: itemName (camelCase)
///   → Host 序列化: "item-name" (kebab-case，与 WIT 定义一致)
///   → WASM 组件反序列化: #[serde(rename_all = "kebab-case")] 或逐字段 rename
///
/// Host 负责将 GraphQL camelCase 转回 WIT kebab-case，确保 WASM 组件
/// 收到的 JSON key 与 WIT record 字段名一致，消除命名约定的隐式耦合。
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

    // 领域参数：key 从 camelCase 转为 kebab-case（与 WIT record 字段名一致）
    let domain_args: IndexMap<String, Value> = args
        .iter()
        .filter(|(k, _)| k.as_str() != "commandId" && k.as_str() != "aggregateId")
        .map(|(k, v)| (camel_to_kebab(k.as_str()), v.clone()))
        .collect();

    let data = serde_json::to_vec(&domain_args)?;

    Ok(IncomingCommand { command_id, aggregate_id, data, module: String::new(), command_type: String::new(), validated: false })
}

/// camelCase → kebab-case 转换
/// "itemName" → "item-name", "stockCount" → "stock-count"
fn camel_to_kebab(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('-');
            result.push(c.to_lowercase().next().unwrap());
        } else {
            result.push(c);
        }
    }
    result
}
```

> **序列化约定**：Host 将 GraphQL camelCase 参数名转为 WIT kebab-case 后传递给 WASM 组件。
> WASM 组件的 Rust 结构体应使用 `#[serde(rename_all = "kebab-case")]` 进行反序列化。
>
> 示例：
> ```rust
> #[derive(Deserialize)]
> #[serde(rename_all = "kebab-case")]
> struct CreateItemParams {
>     item_name: String,    // JSON key: "item-name"
>     stock_count: u32,     // JSON key: "stock-count"
> }
> ```

> **注**：`IncomingCommand` 完整定义见 [command-flow.md](./command-flow.md#incomingcommand-结构统一定义)。
> 此处 `module` 和 `command_type` 由 GraphQL resolver 填充，`validated` 单机模式始终为 false。

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
| 仅包含普通函数（无 `handle-X`） | 查询模块 | Query 字段 |
| 包含至少一个 `handle-X` 函数（`validate-X` 和 `apply-events` 均可选） | 聚合模块 | Mutation 字段 |
| 同时包含普通函数和 `handle-X` 函数 | 混合模块 | Query + Mutation |

