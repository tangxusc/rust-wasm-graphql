# WASM 组件 WIT 接口约定（高性能版）

## 聚合组件接口规范

每个实现事件溯源的 WASM 组件，按命令类型导出独立的 `validate-{cmd}` 和 `handle-{cmd}` 函数对，
加上一个共享的 `apply-events` 函数。

### 函数签名约定

```
validate-{command-name}: func(command: list<u8>) -> result<_, string>;
handle-{command-name}:   func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;
apply-events:            func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>;
```

- `{command-name}` 使用 WIT kebab-case（如 `create-item`、`adjust-stock`）
- 每个命令必须同时导出 `validate-` 和 `handle-` 两个函数（成对出现）
- `apply-events` 保持单一入口（事件应用与命令类型无关）
- 每个命令需定义对应的 `{command-name}-params` record 类型（用于 GraphQL schema 生成）

### 完整 WIT 示例

```wit
package example:inventory;

interface aggregate {
    // 参数类型定义（Host 通过 WIT 内省读取字段信息，自动生成 GraphQL schema）
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

    /// --- 命令：create-item ---
    /// 格式校验（不依赖状态，纯格式/范围检查）
    /// 性能要求：< 5μs（纯 CPU，无 IO）
    validate-create-item: func(command: list<u8>) -> result<_, string>;
    /// 业务决策（依赖当前状态），产出新事件
    /// 性能要求：< 50μs（纯 CPU，无 IO）
    handle-create-item: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    /// --- 命令：adjust-stock ---
    validate-adjust-stock: func(command: list<u8>) -> result<_, string>;
    handle-adjust-stock: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    /// --- 命令：delete-item ---
    validate-delete-item: func(command: list<u8>) -> result<_, string>;
    handle-delete-item: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    /// --- 事件应用（所有命令共享，与命令类型无关） ---
    /// 输入：快照状态（空 list 表示初始状态）+ 增量事件列表
    /// 调用时机：Actor 启动/恢复时（非每次命令）
    /// 错误处理：返回 Err 时 Host 会记录错误并停止激活，防止无限崩溃循环
    apply-events: func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>;
}

world inventory-aggregate {
    export aggregate;
}
```

## validate 与 handle 的职责边界

| 层 | 职责 | 示例 | 需要状态？ |
|----|------|------|-----------|
| validate-X | 格式校验、范围检查、必填字段 | "名称不能为空"、"数量必须>0" | 否 |
| handle-X | 业务规则、状态依赖的决策 | "库存不足"、"物品已存在" | 是 |

validate 的价值：在不加载聚合状态的情况下快速拒绝格式错误的请求。
对于冷聚合（需要从快照+事件恢复），validate 可以节省一次完整的激活开销。

## 命名转换规则

| WIT 函数名 | 提取的命令名 | GraphQL mutation 名 | GraphQL 类型名 |
|------------|-------------|--------------------|--------------| 
| `validate-create-item` | `create-item` | `createItem` | `CreateItemInput` |
| `handle-create-item` | `create-item` | — (内部路由) | — |
| `validate-adjust-stock` | `adjust-stock` | `adjustStock` | `AdjustStockInput` |
| `handle-delete-item` | `delete-item` | `deleteItem` | `DeleteItemInput` |

转换规则：
- WIT kebab-case → GraphQL camelCase（`create-item` → `createItem`）
- 类型名使用 PascalCase + `Input` 后缀
- Host 通过前缀 `validate-` 和 `handle-` 识别命令函数，去掉前缀后得到命令名
- 同名的 `validate-X` 和 `handle-X` 构成一个完整命令

## Host 侧识别聚合组件

Host 通过 WIT 内省判断组件是否为聚合模块：

```rust
impl WasmEngine {
    /// 检查组件是否为聚合模块
    /// 条件：至少一对 validate-X / handle-X + 必须有 apply-events
    pub fn is_aggregate(&self) -> bool {
        let has_apply_events = self.functions.iter()
            .any(|f| f.wit_name == "apply-events");
        let has_command_pair = CommandDiscovery::discover(&self.functions)
            .map(|cmds| !cmds.is_empty())
            .unwrap_or(false);
        has_apply_events && has_command_pair
    }

    /// 获取该聚合支持的所有命令
    pub fn commands(&self) -> Vec<CommandDef> {
        CommandDiscovery::discover(&self.functions).unwrap_or_default()
    }
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

### 推荐格式

| 格式 | 优势 | 劣势 | 适用 |
|------|------|------|------|
| Bincode | Rust 原生最快（~50ns/100B） | 仅 Rust 生态 | Rust-only 组件（生产首选） |
| MessagePack | 紧凑、快速（~200ns/100B）、跨语言 | 需要 schema 约定 | 跨语言组件 |
| JSON | 可读性好、调试方便（~500ns/100B） | 体积大、解析慢 | 仅开发调试 |

### 命令参数结构示例

```rust
// 每个命令独立的参数结构（与 WIT record 对应）
#[derive(Serialize, Deserialize)]
pub struct CreateItemParams {
    pub name: String,
    pub quantity: u32,
}

#[derive(Serialize, Deserialize)]
pub struct AdjustStockParams {
    pub delta: i32,
}
```

### 事件数据结构示例

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DomainEvent {
    ItemCreated { name: String, quantity: u32, timestamp: u64 },
    StockAdjusted { delta: i32, new_quantity: u32, timestamp: u64 },
    ItemDeleted { reason: String, timestamp: u64 },
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

## Host 侧崩溃循环防护

当 `apply-events` 返回错误（事件数据损坏或格式不兼容）时，Actor 无法激活。
如果不加防护，每次访问该聚合都会触发激活 → 失败 → 重试的无限循环。

```rust
const MAX_ACTIVATION_RETRIES: u32 = 3;
const ACTIVATION_COOLDOWN: Duration = Duration::from_secs(60);

pub struct ActivationFailureTracker {
    failures: DashMap<String, (u32, Instant)>,
}

impl ActivationFailureTracker {
    pub fn is_corrupted(&self, aggregate_id: &str) -> bool {
        self.failures.get(aggregate_id)
            .map(|entry| {
                let (count, last_attempt) = entry.value();
                *count >= MAX_ACTIVATION_RETRIES
                && last_attempt.elapsed() < ACTIVATION_COOLDOWN
            })
            .unwrap_or(false)
    }

    pub fn record_failure(&self, aggregate_id: &str) {
        self.failures
            .entry(aggregate_id.to_string())
            .and_modify(|(count, ts)| { *count += 1; *ts = Instant::now(); })
            .or_insert((1, Instant::now()));
    }

    pub fn clear(&self, aggregate_id: &str) {
        self.failures.remove(aggregate_id);
    }
}
```

### 损坏聚合的恢复策略

| 方案 | 适用场景 |
|------|----------|
| 修复 WASM 组件（兼容旧事件格式） | 组件升级导致的不兼容 |
| 事件补偿（写入修正事件） | 单个事件数据损坏 |
| 手动重置快照 + 跳过损坏事件 | 严重损坏，需 DBA 介入 |

## 接口演进策略

当领域模型变更时：

1. **新增命令** — 在 WIT 中添加新的 `validate-X` / `handle-X` 对 + `X-params` record，重新编译组件
2. **废弃命令** — 从 WIT 中移除对应函数对，Host 重启后自动从 GraphQL schema 中消失
3. **新增事件类型** — 向后兼容，旧事件不受影响，`apply-events` 中添加新分支
4. **修改事件结构** — 使用事件版本号 + upcaster 模式
5. **废弃事件类型** — `apply-events` 中保留处理逻辑，`handle-X` 不再产出

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