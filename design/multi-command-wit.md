# 多命令 WIT 接口设计

## 设计动机

当一个聚合需要处理多种命令类型时（如 `CreateItem`、`AdjustStock`、`DeleteItem`），
通过 WIT 级别为每个命令类型导出 `handle-{cmd}` 函数（必须）和可选的 `validate-{cmd}` 函数，
让 Host 通过 WIT 内省自动发现所有命令，实现零配置的命令路由和 GraphQL schema 生成。

核心收益：
1. **Host 感知命令类型** — 支持命令级限流、监控、权限控制
2. **GraphQL schema 自动生成** — Host 通过 `{cmd}-params` record 获取参数签名
3. **错误精确定位** — validate 失败时 Host 明确知道是哪个命令的校验逻辑出错
4. **低门槛接入** — 最小化聚合仅需 `handle-X` + `X-params` record

## WIT 接口规范

### 命名约定

```
validate-{command-name}: func(command: list<u8>) -> result<_, string>;           [可选]
handle-{command-name}:   func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;  [必须]
apply-events:            func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>; [可选]
```

- `{command-name}` 使用 WIT kebab-case（如 `create-item`、`adjust-stock`）
- `handle-{cmd}` 是命令的唯一必要标识
- `validate-{cmd}` 可选：有则 Gateway 前置调用，无则跳过参数校验
- `apply-events` 可选：有则调用组件实现，无则 Host 使用 JSON 深度合并默认策略

### 完整 WIT 示例

```wit
package example:inventory;

interface aggregate {
    /// --- 命令：create-item（有 validate） ---
    validate-create-item: func(command: list<u8>) -> result<_, string>;
    handle-create-item: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    /// --- 命令：adjust-stock（有 validate） ---
    validate-adjust-stock: func(command: list<u8>) -> result<_, string>;
    handle-adjust-stock: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    /// --- 命令：delete-item（无 validate，Gateway 跳过前置校验） ---
    handle-delete-item: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    /// --- 事件应用（可选，省略时 Host 使用 JSON 深度合并） ---
    apply-events: func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>;
}

world inventory-aggregate {
    export aggregate;
}
```

### 命名转换规则

| WIT 函数名 | 提取的命令名 | GraphQL mutation 名 | GraphQL 类型名 |
|------------|-------------|--------------------|--------------| 
| `validate-create-item` | `create-item` | `createItem` | `CreateItemInput` |
| `handle-create-item` | `create-item` | — (内部路由) | — |
| `validate-adjust-stock` | `adjust-stock` | `adjustStock` | `AdjustStockInput` |
| `handle-delete-item` | `delete-item` | `deleteItem` | `DeleteItemInput` |

转换规则：
- WIT kebab-case → GraphQL camelCase（`create-item` → `createItem`）
- 类型名使用 PascalCase + `Input` 后缀
- Host 通过前缀 `handle-` 识别命令函数，去掉前缀后得到命令名
- 如果存在同名的 `validate-X`，则关联为该命令的前置校验

## Host 侧命令发现机制

### 内省算法

Host 启动时加载 WASM 组件，通过 `wit-parser` 内省导出函数，自动发现命令：

```rust
use std::collections::HashMap;

/// 从 WIT 导出函数中提取命令定义
pub struct CommandDiscovery;

impl CommandDiscovery {
    /// 扫描导出函数，以 handle-X 为基准发现命令，validate-X 为可选关联
    pub fn discover(functions: &[ExportedFunction]) -> Result<Vec<CommandDef>, String> {
        let mut validates: HashMap<&str, &ExportedFunction> = HashMap::new();
        let mut handles: HashMap<&str, &ExportedFunction> = HashMap::new();

        for func in functions {
            if let Some(cmd_name) = func.wit_name.strip_prefix("validate-") {
                validates.insert(cmd_name, func);
            } else if let Some(cmd_name) = func.wit_name.strip_prefix("handle-") {
                handles.insert(cmd_name, func);
            }
        }

        // 检查孤立的 validate（有 validate 但无 handle）— 仍报错
        for cmd_name in validates.keys() {
            if !handles.contains_key(cmd_name) {
                return Err(format!(
                    "命令 '{}' 有 validate-{} 但缺少 handle-{}（validate 不能独立存在）",
                    cmd_name, cmd_name, cmd_name
                ));
            }
        }

        // 以 handle-X 为基准构建命令列表，validate-X 为可选关联
        let mut commands = Vec::new();
        for (cmd_name, handle_fn) in &handles {
            commands.push(CommandDef {
                name: cmd_name.to_string(),
                validate_fn: validates.get(cmd_name).map(|f| f.wit_name.clone()),
                handle_fn: handle_fn.wit_name.clone(),
            });
        }

        Ok(commands)
    }
}

/// 命令定义
pub struct CommandDef {
    pub name: String,           // kebab-case: "create-item"
    pub validate_fn: Option<String>,  // Some("validate-create-item") 或 None
    pub handle_fn: String,      // "handle-create-item"（必须存在）
}

impl CommandDef {
    /// GraphQL mutation 字段名（camelCase）
    pub fn graphql_name(&self) -> String {
        to_camel_case(&self.name)
    }

    /// 是否有前置校验
    pub fn has_validate(&self) -> bool {
        self.validate_fn.is_some()
    }
}
```

### 聚合模块判定

```rust
impl WasmEngine {
    /// 检查组件是否为聚合模块
    /// 条件：至少一个 handle-X 函数（validate-X 和 apply-events 均为可选）
    pub fn is_aggregate(&self) -> bool {
        CommandDiscovery::discover(&self.functions)
            .map(|cmds| !cmds.is_empty())
            .unwrap_or(false)
    }

    /// 是否有自定义 apply-events 实现
    pub fn has_custom_apply_events(&self) -> bool {
        self.functions.iter().any(|f| f.wit_name == "apply-events")
    }

    /// 获取该聚合支持的所有命令
    pub fn commands(&self) -> Vec<CommandDef> {
        CommandDiscovery::discover(&self.functions).unwrap_or_default()
    }
}
```

## 命令路由

### Gateway 层路由

Gateway 收到 GraphQL mutation 请求时，已知命令类型（从 GraphQL field name 反推）：

> **注意**：以下为简化版 Gateway 代码，仅展示命令路由逻辑。
> 完整流程（含幂等检查、关闭检查、布隆过滤器）见 [command-flow.md](./command-flow.md)。

```rust
pub struct CommandGateway {
    runtime: Arc<VirtualActorRuntime>,
    wasm_pool: Arc<WasmPoolManager>,
    command_registry: CommandRegistry,  // 启动时缓存的命令定义
}

impl CommandGateway {
    pub async fn execute(&self, command: IncomingCommand) -> Result<CommandResult> {
        // 1. 前置 validate（仅当该命令有 validate 函数时执行）
        let cmd_def = self.command_registry.get(&command.module, &command.command_type);
        if let Some(validate_fn) = cmd_def.and_then(|c| c.validate_fn.as_ref()) {
            let mut instance = self.wasm_pool.acquire(&command.module).await?;
            instance.call_validate(validate_fn, &command.data)?;
            drop(instance);
        }
        // 无 validate 函数时：直接跳过，进入 Actor 处理

        // 2. 透明寻址到 Virtual Actor
        let result = self.runtime.send(&command.aggregate_id, command).await?;
        Ok(result)
    }
}
```

### Actor 内命令处理

Actor 收到命令后，路由到对应的 `handle-X` 函数：

```rust
impl VirtualActor {
    async fn process_command(&mut self, cmd: IncomingCommand) -> Result<CommandResult> {
        // 路由到对应的 handle-X 函数
        let handle_fn = format!("handle-{}", cmd.command_type);
        let mut instance = self.wasm_pool.acquire(&self.module_name).await?;
        let new_events = instance.call_function(
            &handle_fn, 
            &[self.state.clone(), cmd.data.clone()]
        )?;
        drop(instance);

        // 后续流程不变：persist → apply-events → 响应
        self.persist_and_apply(new_events, &cmd.command_id).await
    }
}
```

### IncomingCommand 结构（更新）

```rust
pub struct IncomingCommand {
    pub command_id: String,      // 幂等键
    pub aggregate_id: String,    // 聚合根 ID
    pub module: String,          // 模块名（如 "inventory"）
    pub command_type: String,    // 命令类型 kebab-case（如 "create-item"）
    pub data: Vec<u8>,           // 序列化的命令参数
    pub validated: bool,         // 集群模式：true 表示 validate 已在源节点执行，owner 节点跳过
}
```

## GraphQL Schema 自动生成

### 生成逻辑

Host 通过 WIT 内省发现命令后，为每个聚合模块自动生成 Mutation 命名空间：

```rust
fn build_aggregate_mutation(module: &WasmEngine, gateway: Arc<CommandGateway>) -> Object {
    let module_name = module.name().to_string();
    let mut obj = Object::new(format!("{}Mutation", module.pascal_name()));

    for cmd in module.commands() {
        let gateway = gateway.clone();
        let module_name = module_name.clone();
        let command_type = cmd.name.clone();

        obj = obj.field(
            Field::new(
                cmd.graphql_name(),
                TypeRef::named_nn("CommandResult"),
                move |ctx| {
                    let gateway = gateway.clone();
                    let module_name = module_name.clone();
                    let command_type = command_type.clone();

                    FieldFuture::new(async move {
                        let args = ctx.args;
                        let mut command = extract_command_meta(&args)?;
                        command.module = module_name;
                        command.command_type = command_type;
                        let result = gateway.execute(command).await?;
                        Ok(Some(FieldValue::owned_any(result)))
                    })
                },
            )
            .argument(InputValue::new("commandId", TypeRef::named_nn(TypeRef::STRING)))
            .argument(InputValue::new("aggregateId", TypeRef::named_nn(TypeRef::STRING)))
            // 领域参数从 validate-X 的函数签名中提取（见下文）
            .arguments(cmd.domain_args_as_graphql())
        );
    }

    obj
}
```

### 生成的 GraphQL Schema 示例

```graphql
type Mutation {
  inventory: InventoryMutation!
}

type InventoryMutation {
  createItem(
    commandId: String!
    aggregateId: String!
    name: String!
    quantity: Int!
  ): CommandResult!

  adjustStock(
    commandId: String!
    aggregateId: String!
    delta: Int!
  ): CommandResult!

  deleteItem(
    commandId: String!
    aggregateId: String!
    reason: String
  ): CommandResult!
}

type CommandResult {
  success: Boolean!
  version: Int!
  eventCount: Int!
  error: String
}
```

### 命令参数签名提取

由于 `validate-X` 和 `handle-X` 的参数都是 `list<u8>`（序列化字节），
Host 无法从函数签名直接推断领域参数。需要额外的参数描述机制：

#### 方案：WIT record 类型约定

为每个命令定义对应的 record 类型，命名约定为 `{command-name}-params`：

```wit
package example:inventory;

interface aggregate {
    // 参数类型定义（Host 通过 WIT 内省读取字段信息）
    record create-item-params {
        name: string,
        quantity: u32,
    }

    record adjust-stock-params {
        delta: s32,
    }

    record delete-item-params {
        reason: option<string>,
    }

    // 命令函数（运行时使用）
    validate-create-item: func(command: list<u8>) -> result<_, string>;
    handle-create-item: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    validate-adjust-stock: func(command: list<u8>) -> result<_, string>;
    handle-adjust-stock: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    // delete-item 无 validate（可选）
    handle-delete-item: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    // apply-events 可选，省略时 Host 使用 JSON 深度合并
    apply-events: func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>;
}
```

Host 通过 `wit-parser` 读取 `{command-name}-params` record 的字段定义，
自动映射为 GraphQL 参数：

```rust
impl CommandDef {
    /// 从 WIT record 类型中提取领域参数，转为 GraphQL InputValue
    pub fn domain_args_as_graphql(&self) -> Vec<InputValue> {
        let record_name = format!("{}-params", self.name);
        // 从 WIT 类型定义中查找对应的 record
        let record = self.wit_types.get(&record_name);
        match record {
            Some(Type::Record(fields)) => {
                fields.iter().map(|f| {
                    InputValue::new(
                        to_camel_case(&f.name),
                        wit_type_to_graphql(&f.ty),
                    )
                }).collect()
            }
            None => vec![], // 无参数命令
        }
    }
}

/// WIT 类型 → GraphQL 类型映射
fn wit_type_to_graphql(ty: &wit_parser::Type) -> TypeRef {
    match ty {
        Type::String => TypeRef::named_nn(TypeRef::STRING),
        Type::U32 | Type::U64 | Type::S32 | Type::S64 => TypeRef::named_nn(TypeRef::INT),
        Type::F32 | Type::F64 => TypeRef::named_nn(TypeRef::FLOAT),
        Type::Bool => TypeRef::named_nn(TypeRef::BOOLEAN),
        Type::Option(inner) => wit_type_to_graphql(inner).nullable(),
        _ => TypeRef::named_nn("JSON"),
    }
}
```

## WASM 组件实现示例

```rust
// example/wasm-inventory/src/lib.rs

use serde::{Deserialize, Serialize};
wit_bindgen::generate!({ world: "inventory-aggregate" });

struct Component;

impl Guest for Component {
    // ========== create-item 命令 ==========

    fn validate_create_item(command: Vec<u8>) -> Result<(), String> {
        let params: CreateItemParams = serde_json::from_slice(&command)
            .map_err(|e| format!("反序列化失败: {e}"))?;
        if params.name.is_empty() {
            return Err("名称不能为空".into());
        }
        if params.quantity == 0 {
            return Err("数量必须大于0".into());
        }
        Ok(())
    }

    fn handle_create_item(state: Vec<u8>, command: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        let current = load_state(&state)?;
        if current.created {
            return Err("物品已存在".into());
        }
        let params: CreateItemParams = serde_json::from_slice(&command)
            .map_err(|e| e.to_string())?;
        let event = DomainEvent::ItemCreated {
            name: params.name,
            quantity: params.quantity,
            timestamp: 0,
        };
        Ok(vec![serde_json::to_vec(&event).unwrap()])
    }

    // ========== adjust-stock 命令 ==========

    fn validate_adjust_stock(command: Vec<u8>) -> Result<(), String> {
        let params: AdjustStockParams = serde_json::from_slice(&command)
            .map_err(|e| format!("反序列化失败: {e}"))?;
        if params.delta == 0 {
            return Err("调整量不能为0".into());
        }
        Ok(())
    }

    fn handle_adjust_stock(state: Vec<u8>, command: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        let current = load_state(&state)?;
        if !current.created {
            return Err("物品不存在".into());
        }
        let params: AdjustStockParams = serde_json::from_slice(&command)
            .map_err(|e| e.to_string())?;
        let new_qty = current.quantity as i64 + params.delta as i64;
        if new_qty < 0 {
            return Err(format!("库存不足，当前: {}", current.quantity));
        }
        let event = DomainEvent::StockAdjusted {
            delta: params.delta,
            new_quantity: new_qty as u32,
            timestamp: 0,
        };
        Ok(vec![serde_json::to_vec(&event).unwrap()])
    }

    // ========== delete-item 命令（无 validate，Gateway 跳过前置校验） ==========

    fn handle_delete_item(state: Vec<u8>, command: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        let current = load_state(&state)?;
        if !current.created {
            return Err("物品不存在，无法删除".into());
        }
        let params: DeleteItemParams = serde_json::from_slice(&command)
            .map_err(|e| e.to_string())?;
        let event = DomainEvent::ItemDeleted {
            reason: params.reason.unwrap_or_default(),
            timestamp: 0,
        };
        Ok(vec![serde_json::to_vec(&event).unwrap()])
    }

    // ========== 事件应用（所有命令共享） ==========

    fn apply_events(snapshot: Vec<u8>, events: Vec<Vec<u8>>) -> Result<Vec<u8>, String> {
        let mut state = load_state(&snapshot)?;
        for (i, event_data) in events.iter().enumerate() {
            let event: DomainEvent = serde_json::from_slice(event_data)
                .map_err(|e| format!("事件 #{i} 反序列化失败: {e}"))?;
            match event {
                DomainEvent::ItemCreated { name, quantity, .. } => {
                    state.name = name;
                    state.quantity = quantity;
                    state.created = true;
                }
                DomainEvent::StockAdjusted { new_quantity, .. } => {
                    state.quantity = new_quantity;
                }
                DomainEvent::ItemDeleted { .. } => {
                    state.created = false;
                    state.quantity = 0;
                }
            }
        }
        serde_json::to_vec(&state).map_err(|e| format!("状态序列化失败: {e}"))
    }
}

// --- 数据结构 ---

#[derive(Deserialize)]
struct CreateItemParams {
    name: String,
    quantity: u32,
}

#[derive(Deserialize)]
struct AdjustStockParams {
    delta: i32,
}

#[derive(Deserialize)]
struct DeleteItemParams {
    reason: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum DomainEvent {
    ItemCreated { name: String, quantity: u32, timestamp: u64 },
    StockAdjusted { delta: i32, new_quantity: u32, timestamp: u64 },
    ItemDeleted { reason: String, timestamp: u64 },
}

#[derive(Serialize, Deserialize, Default)]
struct InventoryState {
    name: String,
    quantity: u32,
    created: bool,
}

fn load_state(data: &[u8]) -> Result<InventoryState, String> {
    if data.is_empty() {
        Ok(InventoryState::default())
    } else {
        serde_json::from_slice(data).map_err(|e| format!("状态反序列化失败: {e}"))
    }
}

export_inventory_aggregate!(Component);
```

## 命令处理时序图

### 有 validate 的命令（如 create-item）

```
Client       GraphQL      Gateway        VirtualActorRuntime     Actor(聚合)       WASM Pool       EventStore
  │              │            │                  │                   │                 │                │
  │── Mutation ─▶│            │                  │                   │                 │                │
  │  createItem  │            │                  │                   │                 │                │
  │              │── cmd ────▶│                  │                   │                 │                │
  │              │ (含 command_type              │                   │                 │                │
  │              │  ="create-item")              │                   │                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │─ validate-create-item() ────────────────────────────▶│                │
  │              │            │◀─ Ok ───────────────────────────────────────────────│                │
  │              │            │                  │                   │                 │                │
  │              │            │─ send(agg_id) ──▶│                   │                 │                │
  │              │            │                  │── 查找/激活 Actor ▶│                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │─ handle-create-item(state) ───▶│
  │              │            │                  │                   │◀─ new_events ─────────────────│
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │── persist(同步等待) ────────────▶│
  │              │            │                  │                   │◀── ack ─────────────────────────│
  │              │            │                  │                   │                 │                │
  │◀── Result ──│◀───────────│◀─────────────────│◀── Ok(version) ──│                 │                │
```

### 无 validate 的命令（如 delete-item）

```
Client       GraphQL      Gateway        VirtualActorRuntime     Actor(聚合)       WASM Pool       EventStore
  │              │            │                  │                   │                 │                │
  │── Mutation ─▶│            │                  │                   │                 │                │
  │  deleteItem  │            │                  │                   │                 │                │
  │              │── cmd ────▶│                  │                   │                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │  [无 validate-delete-item，跳过校验]  │                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │─ send(agg_id) ──▶│                   │                 │                │
  │              │            │                  │── 查找/激活 Actor ▶│                 │                │
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │─ handle-delete-item(state) ───▶│
  │              │            │                  │                   │◀─ new_events ─────────────────│
  │              │            │                  │                   │                 │                │
  │              │            │                  │                   │── persist(同步等待) ────────────▶│
  │              │            │                  │                   │◀── ack ─────────────────────────│
  │              │            │                  │                   │                 │                │
  │◀── Result ──│◀───────────│◀─────────────────│◀── Ok(version) ──│                 │                │
```

关键变化：
- Gateway 根据命令定义判断是否有 `validate-{cmd}`，有则调用，无则跳过
- Actor 始终调用 `handle-{cmd}`，与 validate 是否存在无关

## 启动时校验

Host 加载 WASM 组件时执行校验，启动失败优于运行时错误：

```rust
impl WasmRegistry {
    fn validate_aggregate_module(&self, engine: &WasmEngine) -> Result<(), String> {
        let functions = &engine.functions;

        // 1. [已移除] apply-events 不再是硬性要求，改为检测并记录
        let has_apply_events = functions.iter().any(|f| f.wit_name == "apply-events");
        if !has_apply_events {
            tracing::warn!(
                "聚合模块 '{}' 未导出 apply-events，将使用 Host 默认策略（JSON 深度合并）",
                engine.name()
            );
        }

        // 2. 命令发现：以 handle-X 为基准，validate-X 可选
        let commands = CommandDiscovery::discover(functions)?;
        if commands.is_empty() {
            return Err("聚合模块至少需要一个命令（handle-X 函数）".into());
        }

        // 3. 每个命令必须有对应的 params record（用于 GraphQL schema 生成）
        for cmd in &commands {
            let params_record = format!("{}-params", cmd.name);
            if !engine.wit_types.contains_key(&params_record) {
                return Err(format!(
                    "命令 '{}' 缺少参数类型定义 '{}'", cmd.name, params_record
                ));
            }
        }

        // 4. 函数签名校验（仅校验存在的函数）
        for cmd in &commands {
            if let Some(ref validate_fn) = cmd.validate_fn {
                self.validate_function_signature(
                    functions, validate_fn,
                    &["list<u8>"], "result<_, string>"
                )?;
            }
            self.validate_function_signature(
                functions, &cmd.handle_fn,
                &["list<u8>", "list<u8>"], "result<list<list<u8>>, string>"
            )?;
        }

        // 5. 如果有 apply-events，校验其签名
        if has_apply_events {
            self.validate_function_signature(
                functions, "apply-events",
                &["list<u8>", "list<list<u8>>"], "result<list<u8>, string>"
            )?;
        }

        Ok(())
    }
}
```

## 错误处理

### 启动时错误（快速失败）

| 错误场景 | 处理方式 |
|----------|----------|
| `validate-X` 无对应 `handle-X` | 启动失败，日志报告缺失函数 |
| `handle-X` 无对应 `validate-X` | **正常**（validate 可选） |
| 缺少 `apply-events` | **正常**（输出 warning，使用 Host 默认策略） |
| 缺少 `{cmd}-params` record | 启动失败，无法生成 GraphQL schema |
| 函数签名不匹配 | 启动失败，报告期望 vs 实际签名 |

### 运行时错误

| 错误场景 | 处理方式 | HTTP 状态 |
|----------|----------|-----------|
| 未知命令类型 | 返回错误（不应发生，GraphQL schema 已约束） | 400 |
| `validate-X` 返回 Err | 返回校验错误，无副作用（仅当 validate 存在时） | 400 |
| `handle-X` 返回 Err | 返回业务错误，状态不变 | 422 |
| WASM 实例 trap | 返回内部错误，状态不变 | 500 |
| 默认 apply-events 解析失败 | 事件非合法 JSON，返回内部错误 | 500 |

## 命令级监控与限流

方案 C 的核心优势：Host 在路由层即可感知命令类型，支持细粒度运维能力。

### 命令级指标

```rust
/// 每个命令类型独立的指标
pub struct CommandMetrics {
    pub module: String,
    pub command_type: String,
    pub validate_duration: Histogram,
    pub handle_duration: Histogram,
    pub success_count: Counter,
    pub failure_count: Counter,
    pub rejection_count: Counter,  // validate 拒绝
}
```

### 命令级限流

```rust
pub struct CommandRateLimiter {
    /// 每个 (module, command_type) 独立的限流器
    limiters: HashMap<(String, String), RateLimiter>,
}

impl CommandRateLimiter {
    pub fn check(&self, module: &str, command_type: &str) -> Result<(), Error> {
        let key = (module.to_string(), command_type.to_string());
        if let Some(limiter) = self.limiters.get(&key) {
            limiter.check().map_err(|_| Error::rate_limited(module, command_type))
        } else {
            Ok(())
        }
    }
}
```

配置示例：

```yaml
rate_limits:
  inventory:
    create-item: 100/s    # 创建操作限流较严
    adjust-stock: 10000/s # 库存调整允许高频
    delete-item: 10/s     # 删除操作严格限流
```

## 设计权衡

### 优势

| 维度 | 说明 |
|------|------|
| 类型安全 | 每个命令有独立的函数签名，编译期即可发现接口不匹配 |
| 自动发现 | Host 通过 WIT 内省自动发现所有命令，零配置 |
| GraphQL 自动生成 | 结合 `{cmd}-params` record，完全自动生成 mutation schema |
| 命令级运维 | 限流、监控、权限控制可精确到单个命令类型 |
| 错误隔离 | validate/handle 失败时 Host 明确知道是哪个命令 |
| 并行开发 | 不同命令可由不同开发者独立实现，互不影响 |

### 劣势与缓解

| 维度 | 说明 | 缓解措施 |
|------|------|----------|
| WIT 接口膨胀 | 每新增命令需加 2 个函数 + 1 个 record | 代码生成工具（宏/模板） |
| 编译耦合 | 新增命令需重新编译 WASM 组件 | 本就需要重新编译 |
| 函数数量多 | N 个命令 = 2N+1 个导出函数 | WIT 内省自动处理，开发者无感 |
| 命名约定强依赖 | 前缀 `validate-`/`handle-` 是硬编码约定 | 启动时严格校验，快速失败 |
