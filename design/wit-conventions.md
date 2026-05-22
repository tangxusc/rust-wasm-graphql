# WASM 组件 WIT 接口约定（高性能版）

## 聚合组件接口规范

每个实现事件溯源的 WASM 组件必须导出以下标准接口：

```wit
package example:inventory;

interface aggregate {
    /// 参数格式校验（不依赖聚合状态）
    /// 输入：序列化的命令数据
    /// 输出：Ok(()) 或错误描述
    /// 性能要求：< 5μs（纯 CPU，无 IO）
    validate: func(command: list<u8>) -> result<_, string>;

    /// 批量应用事件，重建聚合状态
    /// 输入：快照状态（空 list 表示初始状态）+ 增量事件列表
    /// 输出：最新聚合状态
    /// 调用时机：Actor 启动/恢复时（非每次命令）
    apply-events: func(snapshot: list<u8>, events: list<list<u8>>) -> list<u8>;

    /// 命令处理：基于当前状态决策，产出新事件
    /// 输入：当前聚合状态 + 命令数据
    /// 输出：新领域事件列表 或 业务错误
    /// 性能要求：< 50μs（纯 CPU，无 IO）
    handle: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;
}

world inventory-aggregate {
    export aggregate;
}
```

## 高性能设计约束

### 1. 函数必须是纯计算

所有聚合接口函数**禁止 IO 操作**（网络、文件、随机数）：
- 确保实例可安全复用（实例池）
- 确保确定性（相同输入 → 相同输出）
- 确保可测试性

### 2. 内存分配最小化

```rust
// 好：复用 buffer
fn apply_events(snapshot: Vec<u8>, events: Vec<Vec<u8>>) -> Vec<u8> {
    let mut state = deserialize(&snapshot);
    for event in events {
        apply_single(&mut state, &event);  // 原地修改
    }
    serialize(&state)
}

// 差：每次事件都序列化/反序列化
fn apply_events(snapshot: Vec<u8>, events: Vec<Vec<u8>>) -> Vec<u8> {
    let mut state_bytes = snapshot;
    for event in events {
        let state = deserialize(&state_bytes);  // 每次都分配
        let new_state = apply(state, &event);
        state_bytes = serialize(&new_state);    // 每次都分配
    }
    state_bytes
}
```

### 3. 状态序列化体积控制

Actor 内存常驻聚合状态，状态体积直接影响内存占用：

| 聚合状态大小 | 10万聚合内存占用 | 建议 |
|-------------|----------------|------|
| 100 bytes | ~10 MB | 理想 |
| 1 KB | ~100 MB | 可接受 |
| 10 KB | ~1 GB | 需要优化 |
| 100 KB | ~10 GB | 不可接受，需拆分聚合 |

## 命令与事件的序列化

WASM 边界使用 `list<u8>` 传递结构化数据，内部序列化格式由组件自行决定。

### 推荐格式：MessagePack

| 格式 | 优势 | 劣势 |
|------|------|------|
| MessagePack | 紧凑、快速、跨语言 | 需要 schema 约定 |
| JSON | 可读性好、调试方便 | 体积大、解析慢 |
| Bincode | Rust 原生最快 | 仅 Rust 生态 |
| Protobuf | 强 schema、版本兼容 | 编译复杂 |

建议开发阶段用 JSON（方便调试），生产用 MessagePack。

### 高性能场景序列化选择

在百万 TPS 目标下，序列化开销成为关键路径：

| 格式 | 序列化 100B 耗时 | 适用 |
|------|-----------------|------|
| Bincode | ~50ns | Rust-only 组件（最快） |
| MessagePack | ~200ns | 跨语言组件 |
| JSON | ~500ns | 仅开发调试 |

**建议**：生产环境统一使用 Bincode（如果所有 WASM 组件都是 Rust 编写）。

### 命令数据结构示例

```rust
// WASM 组件内部定义
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Command {
    CreateItem {
        command_id: String,
        aggregate_id: String,
        name: String,
        quantity: u32,
    },
    AdjustStock {
        command_id: String,
        aggregate_id: String,
        delta: i32,
    },
}
```

### 事件数据结构示例

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DomainEvent {
    ItemCreated {
        name: String,
        quantity: u32,
        timestamp: u64,
    },
    StockAdjusted {
        delta: i32,
        new_quantity: u32,
        timestamp: u64,
    },
}
```

### 聚合状态结构示例

```rust
#[derive(Serialize, Deserialize, Default)]
pub struct InventoryState {
    pub name: String,
    pub quantity: u32,
    pub created: bool,
}
```

## WASM 组件实现示例

```rust
// example/wasm-inventory/src/lib.rs

use serde::{Deserialize, Serialize};
wit_bindgen::generate!({ world: "inventory-aggregate" });

struct Component;
impl Guest for Component {
    fn validate(command: Vec<u8>) -> Result<(), String> {
        let cmd: Command = serde_json::from_slice(&command)
            .map_err(|e| format!("命令反序列化失败: {e}"))?;
        match cmd {
            Command::CreateItem { name, quantity, .. } => {
                if name.is_empty() { return Err("名称不能为空".into()); }
                if quantity == 0 { return Err("数量必须大于0".into()); }
                Ok(())
            }
            Command::AdjustStock { delta, .. } => {
                if delta == 0 { return Err("调整量不能为0".into()); }
                Ok(())
            }
        }
    }

    fn apply_events(snapshot: Vec<u8>, events: Vec<Vec<u8>>) -> Vec<u8> {
        let mut state: InventoryState = if snapshot.is_empty() {
            InventoryState::default()
        } else {
            serde_json::from_slice(&snapshot).unwrap()
        };

        for event_data in events {
            let event: DomainEvent = serde_json::from_slice(&event_data).unwrap();
            match event {
                DomainEvent::ItemCreated { name, quantity, .. } => {
                    state.name = name;
                    state.quantity = quantity;
                    state.created = true;
                }
                DomainEvent::StockAdjusted { new_quantity, .. } => {
                    state.quantity = new_quantity;
                }
            }
        }

        serde_json::to_vec(&state).unwrap()
    }

    fn handle(state: Vec<u8>, command: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        let current: InventoryState = if state.is_empty() {
            InventoryState::default()
        } else {
            serde_json::from_slice(&state).map_err(|e| e.to_string())?
        };
        let cmd: Command = serde_json::from_slice(&command).map_err(|e| e.to_string())?;

        match cmd {
            Command::CreateItem { name, quantity, .. } => {
                if current.created {
                    return Err("物品已存在".into());
                }
                let event = DomainEvent::ItemCreated {
                    name, quantity, timestamp: 0,
                };
                Ok(vec![serde_json::to_vec(&event).unwrap()])
            }
            Command::AdjustStock { delta, .. } => {
                if !current.created {
                    return Err("物品不存在".into());
                }
                let new_qty = current.quantity as i64 + delta as i64;
                if new_qty < 0 {
                    return Err(format!("库存不足，当前: {}", current.quantity));
                }
                let event = DomainEvent::StockAdjusted {
                    delta, new_quantity: new_qty as u32, timestamp: 0,
                };
                Ok(vec![serde_json::to_vec(&event).unwrap()])
            }
        }
    }
}

export_inventory_aggregate!(Component);
```

## Host 侧识别聚合组件

Host 通过 WIT 内省判断组件是否为聚合模块：

```rust
impl WasmEngine {
    /// 检查组件是否导出了完整的聚合接口
    pub fn is_aggregate(&self) -> bool {
        let required = ["validate", "apply-events", "handle"];
        required.iter().all(|name| {
            self.functions.iter().any(|f| f.wit_name == *name)
        })
    }
}
```

## 接口演进策略

当领域模型变更时：

1. **新增事件类型** — 向后兼容，旧事件不受影响
2. **修改事件结构** — 使用事件版本号 + upcaster 模式
3. **废弃事件类型** — apply_events 中保留处理逻辑，handle 不再产出

```rust
// Upcaster 示例：v1 事件升级为 v2
fn upcast_event(event_data: &[u8], version: u32) -> Vec<u8> {
    match version {
        1 => {
            let v1: EventV1 = deserialize(event_data);
            let v2 = EventV2 { /* 从 v1 转换 */ };
            serialize(&v2)
        }
        _ => event_data.to_vec(),
    }
}
```
