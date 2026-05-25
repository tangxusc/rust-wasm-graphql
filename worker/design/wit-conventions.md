# WASM 组件 WIT 接口约定（单机版）

## 聚合组件接口规范

每个 WASM 组件按命令类型导出函数：

```
handle-{command-name}:   func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;  [必须]
validate-{command-name}: func(command: list<u8>) -> result<_, string>;                                [可选]
apply-events:            func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>; [必须]
```

- `handle-{cmd}` 是命令的唯一必要标识
- `validate-{cmd}` 可选：有则在 handle 前调用，无则跳过
- `apply-events` 必须实现：负责将事件应用到聚合状态，确保领域事件语义完整
- 每个命令需定义 `{command-name}-params` record（用于 GraphQL schema 生成）

## 完整 WIT 示例

```wit
package example:inventory;

interface aggregate {
    record create-item-params {
        name: string,
        quantity: u32,
    }

    record adjust-stock-params {
        delta: s32,
    }

    validate-create-item: func(command: list<u8>) -> result<_, string>;
    handle-create-item: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    handle-adjust-stock: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    apply-events: func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>;
}

world inventory-aggregate {
    export aggregate;
}
```

## 最小化 WIT 示例

```wit
package example:counter;

interface aggregate {
    record increment-params {
        amount: u32,
    }

    handle-increment: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;
    apply-events: func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>;
}

world counter-aggregate {
    export aggregate;
}
```

## 命名转换规则

| WIT 函数名 | 提取的命令名 | GraphQL mutation 名 |
|------------|-------------|-------------------|
| `handle-create-item` | `create-item` | `createItem` |
| `validate-create-item` | `create-item` | — (内部调用) |
| `handle-adjust-stock` | `adjust-stock` | `adjustStock` |

转换规则：
- WIT kebab-case → GraphQL camelCase
- Host 通过前缀 `handle-` 识别命令函数
- 如果存在同名 `validate-X`，则关联为前置校验

## Host 侧命令发现

```rust
pub struct CommandDef {
    pub name: String,                  // kebab-case: "create-item"
    pub validate_fn: Option<String>,   // Some("validate-create-item") 或 None
    pub handle_fn: String,             // "handle-create-item"
}

impl CommandDiscovery {
    /// 以 handle-X 为基准发现命令，validate-X 为可选关联
    pub fn discover(functions: &[ExportedFunction]) -> Result<Vec<CommandDef>, String> {
        let mut validates: HashMap<&str, &ExportedFunction> = HashMap::new();
        let mut handles: HashMap<&str, &ExportedFunction> = HashMap::new();

        for func in functions {
            if let Some(cmd) = func.wit_name.strip_prefix("validate-") {
                validates.insert(cmd, func);
            } else if let Some(cmd) = func.wit_name.strip_prefix("handle-") {
                handles.insert(cmd, func);
            }
        }

        // 孤立的 validate（有 validate 但无 handle）报错
        for cmd in validates.keys() {
            if !handles.contains_key(cmd) {
                return Err(format!("validate-{cmd} 缺少对应的 handle-{cmd}"));
            }
        }

        let commands = handles.iter().map(|(cmd, handle_fn)| CommandDef {
            name: cmd.to_string(),
            validate_fn: validates.get(cmd).map(|f| f.wit_name.clone()),
            handle_fn: handle_fn.wit_name.clone(),
        }).collect();

        Ok(commands)
    }
}
```

## WASM 引擎（简化版，带 fuel 超时保护）

```rust
pub struct WasmEngine {
    engine: wasmtime::Engine,
    modules: HashMap<String, ModuleInfo>,
    /// 单次调用允许消耗的最大 fuel（防止无限循环）
    fuel_limit: u64,
}

struct ModuleInfo {
    component: wasmtime::component::Component,
    linker: wasmtime::component::Linker<WasiState>,
    commands: Vec<CommandDef>,
}

impl WasmEngine {
    pub fn new(fuel_limit: u64) -> Self {
        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        let engine = wasmtime::Engine::new(&config).unwrap();
        Self { engine, modules: HashMap::new(), fuel_limit }
    }

    /// 每次调用创建新实例，fuel 限制防止无限循环
    pub fn call_handle(
        &self,
        module: &str,
        func_name: &str,
        state: &[u8],
        command: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let info = self.modules.get(module).ok_or(Error::module_not_found(module))?;
        let mut store = wasmtime::Store::new(&self.engine, WasiState::new());
        store.set_fuel(self.fuel_limit)?;
        let instance = info.linker.instantiate(&mut store, &info.component)?;
        call_wasm_func(&mut store, &instance, func_name, &[state, command])
    }

    /// 调用组件的 apply-events 实现重建聚合状态
    pub fn call_apply_events(
        &self,
        module: &str,
        snapshot: &[u8],
        events: &[&[u8]],
    ) -> Result<Vec<u8>> {
        let info = self.modules.get(module).ok_or(Error::module_not_found(module))?;
        let mut store = wasmtime::Store::new(&self.engine, WasiState::new());
        store.set_fuel(self.fuel_limit)?;
        let instance = info.linker.instantiate(&mut store, &info.component)?;
        call_wasm_apply(&mut store, &instance, snapshot, events)
    }
}
```

## 领域事件设计规范

`apply-events` 为必须实现的函数，事件必须遵循领域事件最佳实践：

### 事件设计原则

1. **事件是已发生的事实** — 使用过去时命名（`ItemCreated`、`StockAdjusted`）
2. **事件自描述** — 包含足够信息独立解读，无需参照前序状态
3. **事件携带语义** — 记录业务意图和增量，而非最终状态值
4. **事件不可变** — 一旦持久化，永不修改

### 事件结构要求

每个事件必须包含 `type` 字段标识事件类型：

```json
{
    "type": "StockAdjusted",
    "delta": -5,
    "reason": "sale",
    "operator": "user-001"
}
```

**禁止**仅携带最终值的 patch 式事件：

```json
// 错误示例：丢失业务语义，无法审计
{ "quantity": 95 }
```

## WASM 组件实现示例

```rust
use serde::{Deserialize, Serialize};
wit_bindgen::generate!({ world: "inventory-aggregate" });

struct Component;

impl Guest for Component {
    fn validate_create_item(command: Vec<u8>) -> Result<(), String> {
        let params: CreateItemParams = serde_json::from_slice(&command)
            .map_err(|e| format!("反序列化失败: {e}"))?;
        if params.name.is_empty() { return Err("名称不能为空".into()); }
        if params.quantity == 0 { return Err("数量必须大于0".into()); }
        Ok(())
    }

    fn handle_create_item(state: Vec<u8>, command: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        let current: InventoryState = load_state(&state)?;
        if current.created { return Err("物品已存在".into()); }
        let params: CreateItemParams = serde_json::from_slice(&command).map_err(|e| e.to_string())?;
        let event = json!({
            "type": "ItemCreated",
            "name": params.name,
            "quantity": params.quantity,
        });
        Ok(vec![serde_json::to_vec(&event).unwrap()])
    }

    fn handle_adjust_stock(state: Vec<u8>, command: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        let current: InventoryState = load_state(&state)?;
        if !current.created { return Err("物品不存在".into()); }
        let params: AdjustStockParams = serde_json::from_slice(&command).map_err(|e| e.to_string())?;
        let new_qty = current.quantity as i64 + params.delta as i64;
        if new_qty < 0 { return Err(format!("库存不足，当前: {}", current.quantity)); }
        let event = json!({
            "type": "StockAdjusted",
            "delta": params.delta,
            "previous_quantity": current.quantity,
        });
        Ok(vec![serde_json::to_vec(&event).unwrap()])
    }

    fn apply_events(snapshot: Vec<u8>, events: Vec<Vec<u8>>) -> Result<Vec<u8>, String> {
        let mut state: InventoryState = load_state(&snapshot)?;
        for event_data in &events {
            let event: Value = serde_json::from_slice(event_data)
                .map_err(|e| format!("事件反序列化失败: {e}"))?;
            match event["type"].as_str() {
                Some("ItemCreated") => {
                    state.name = event["name"].as_str().unwrap_or_default().to_string();
                    state.quantity = event["quantity"].as_u64().unwrap_or(0) as u32;
                    state.created = true;
                }
                Some("StockAdjusted") => {
                    let delta = event["delta"].as_i64().unwrap_or(0) as i32;
                    state.quantity = (state.quantity as i64 + delta as i64) as u32;
                }
                _ => {}
            }
        }
        serde_json::to_vec(&state).map_err(|e| e.to_string())
    }
}
```

## 启动时校验

Host 加载 WASM 组件时执行校验，启动失败优于运行时错误：

| 检查项 | 失败处理 |
|--------|----------|
| `validate-X` 无对应 `handle-X` | 启动失败 |
| `handle-X` 无对应 `validate-X` | 正常（validate 可选） |
| 缺少 `apply-events` | 启动失败（必须实现） |
| 缺少 `{cmd}-params` record | 启动失败 |

## 设计约束

- 所有聚合接口函数禁止 IO 操作（确保确定性和可测试性）
- 事件序列化必须使用 JSON 格式，且包含 `type` 字段
- `apply-events` 为必须实现，组件负责完整的状态重建逻辑
- 事件应携带业务语义（增量、意图），而非状态快照
- 状态体积控制在 10KB 以内（影响内存占用和快照大小）
