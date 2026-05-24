# WASM 组件 WIT 接口约定（单机版）

## 聚合组件接口规范

每个 WASM 组件按命令类型导出函数：

```
handle-{command-name}:   func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;  [必须]
validate-{command-name}: func(command: list<u8>) -> result<_, string>;                                [可选]
apply-events:            func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>; [可选]
```

- `handle-{cmd}` 是命令的唯一必要标识
- `validate-{cmd}` 可选：有则在 handle 前调用，无则跳过
- `apply-events` 可选：有则调用组件实现，无则 Host 使用 JSON 深度合并
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

## WASM 引擎（简化版，无实例池）

```rust
pub struct WasmEngine {
    engine: wasmtime::Engine,
    modules: HashMap<String, ModuleInfo>,
}

struct ModuleInfo {
    component: wasmtime::component::Component,
    linker: wasmtime::component::Linker<WasiState>,
    commands: Vec<CommandDef>,
    has_apply_events: bool,
}

impl WasmEngine {
    /// 每次调用创建新实例（简化版，无实例池）
    pub fn call_handle(
        &self,
        module: &str,
        func_name: &str,
        state: &[u8],
        command: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let info = self.modules.get(module).ok_or(Error::module_not_found(module))?;
        let mut store = wasmtime::Store::new(&self.engine, WasiState::new());
        let instance = info.linker.instantiate(&mut store, &info.component)?;
        call_wasm_func(&mut store, &instance, func_name, &[state, command])
    }

    /// 调用 apply-events（有自定义实现时）或使用默认 JSON 深度合并
    pub fn call_apply_events(
        &self,
        module: &str,
        snapshot: &[u8],
        events: &[&[u8]],
    ) -> Result<Vec<u8>> {
        let info = self.modules.get(module).ok_or(Error::module_not_found(module))?;
        if info.has_apply_events {
            let mut store = wasmtime::Store::new(&self.engine, WasiState::new());
            let instance = info.linker.instantiate(&mut store, &info.component)?;
            call_wasm_apply(&mut store, &instance, snapshot, events)
        } else {
            default_apply_events(snapshot, events)
        }
    }
}
```

## 默认 apply-events 策略（JSON 深度合并）

省略 `apply-events` 时 Host 使用内置策略：

- 事件必须为 JSON 格式
- 对象字段递归合并，后值覆盖前值
- 数组字段整体替换
- null 值表示删除字段

**约束：事件必须携带字段最终值，而非增量值。**

```rust
fn default_apply_events(snapshot: &[u8], events: &[&[u8]]) -> Result<Vec<u8>> {
    let mut state: Value = if snapshot.is_empty() {
        Value::Object(Default::default())
    } else {
        serde_json::from_slice(snapshot)?
    };

    for event in events {
        let patch: Value = serde_json::from_slice(event)?;
        deep_merge(&mut state, &patch);
    }

    Ok(serde_json::to_vec(&state)?)
}
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
            "quantity": new_qty as u32,
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
                    state.quantity = event["quantity"].as_u64().unwrap_or(0) as u32;
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
| 缺少 `apply-events` | 正常（warning，使用默认策略） |
| 缺少 `{cmd}-params` record | 启动失败 |

## 设计约束

- 所有聚合接口函数禁止 IO 操作（确保确定性和可测试性）
- 事件序列化推荐 JSON（兼容默认 apply-events 策略）
- 状态体积控制在 10KB 以内（影响内存占用和快照大小）
