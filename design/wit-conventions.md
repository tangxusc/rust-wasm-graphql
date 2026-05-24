# WASM 组件 WIT 接口约定（高性能版）

## 聚合组件接口规范

每个实现事件溯源的 WASM 组件，按命令类型导出 `handle-{cmd}` 函数（必须），
可选导出 `validate-{cmd}` 函数和共享的 `apply-events` 函数。

### 函数签名约定

```
validate-{command-name}: func(command: list<u8>) -> result<_, string>;           [可选]
handle-{command-name}:   func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;  [必须]
apply-events:            func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>; [可选]
```

- `{command-name}` 使用 WIT kebab-case（如 `create-item`、`adjust-stock`）
- `handle-{cmd}` 是命令的唯一必要标识，每个命令必须导出
- `validate-{cmd}` 可选：有则 Gateway 前置调用，无则跳过参数校验
- `apply-events` 可选：有则调用组件实现，无则 Host 使用内置默认策略（JSON 深度合并）
- 每个命令需定义对应的 `{command-name}-params` record 类型（用于 GraphQL schema 生成）
- 孤立的 `validate-X`（无对应 `handle-X`）仍然报错——validate 不能独立存在

### 完整 WIT 示例（所有可选函数均实现）

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
    /// [可选] 格式校验（不依赖状态，纯格式/范围检查）
    validate-create-item: func(command: list<u8>) -> result<_, string>;
    /// [必须] 业务决策（依赖当前状态），产出新事件
    handle-create-item: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    /// --- 命令：adjust-stock ---
    validate-adjust-stock: func(command: list<u8>) -> result<_, string>;
    handle-adjust-stock: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    /// --- 命令：delete-item（此命令省略了 validate，Gateway 将跳过前置校验） ---
    handle-delete-item: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;

    /// --- [可选] 事件应用（所有命令共享，与命令类型无关） ---
    /// 省略时 Host 使用内置默认策略（JSON 深度合并）
    apply-events: func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string>;
}

world inventory-aggregate {
    export aggregate;
}
```

### 最小化 WIT 示例（仅必须函数）

```wit
package example:counter;

interface aggregate {
    record increment-params {
        amount: u32,
    }

    // 无 validate-increment：Gateway 跳过前置校验
    // 无 apply-events：Host 使用 JSON 深度合并默认策略
    handle-increment: func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>;
}

world counter-aggregate {
    export aggregate;
}
```

## validate 与 handle 的职责边界

| 层 | 职责 | 示例 | 需要状态？ | 是否必须？ |
|----|------|------|-----------|-----------|
| validate-X | 格式校验、范围检查、必填字段 | "名称不能为空"、"数量必须>0" | 否 | **可选** |
| handle-X | 业务规则、状态依赖的决策 | "库存不足"、"物品已存在" | 是 | **必须** |

validate 的价值：在不加载聚合状态的情况下快速拒绝格式错误的请求。
对于冷聚合（需要从快照+事件恢复），validate 可以节省一次完整的激活开销。

**何时可以省略 validate**：
- 命令参数极简（如仅一个 ID），格式校验无实质意义
- 命令的所有校验都依赖状态（如 "物品必须存在"），纯格式校验无法覆盖
- 快速原型阶段，后续可按需补充

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
- Host 通过前缀 `handle-` 识别命令函数，去掉前缀后得到命令名
- 如果存在同名的 `validate-X`，则关联为该命令的前置校验

## Host 侧识别聚合组件

Host 通过 WIT 内省判断组件是否为聚合模块：

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

## 可选函数与默认策略

### validate-{cmd} 省略规则

| 场景 | 行为 |
|------|------|
| 有 `validate-X` | Gateway 前置调用，失败返回 400 |
| 无 `validate-X` | Gateway 跳过校验，直接路由到 Actor |
| 有 `validate-X` 但无 `handle-X` | **启动报错**（validate 不能独立存在） |

省略 validate 的代价：无效请求会打到 Actor（可能触发冷聚合激活）。
建议：对于可能有大量无效输入的命令（如用户直接输入的表单），仍保留 validate。

### apply-events 省略规则

| 场景 | 行为 |
|------|------|
| 有 `apply-events` | 调用组件实现（行为不变） |
| 无 `apply-events` | Host 使用内置默认策略：JSON 深度合并 |

**⚠️⚠️⚠️ 快速原型模式警告 ⚠️⚠️⚠️**

> **默认 JSON 深度合并策略仅适用于快速原型和开发阶段。生产环境强烈建议始终自行实现 `apply-events`。**
>
> 原因：默认策略的语义是"状态字段覆盖"，与事件溯源的核心理念（事件是增量变更记录）存在根本冲突。
> 开发者在事件溯源中自然倾向于使用增量语义（如 `{"delta": -5}`），但默认策略会将其误解为
> "将 delta 字段设为 -5"而非"库存减少 5"，导致**静默的数据错误**——系统不会报错，但状态是错的。

**默认策略：JSON Deep Merge + 数组整体替换 + null 删除字段**

> **⚠️ 硬性约束：使用默认策略时，事件必须携带字段的最终值，而非增量值。**
>
> 例如库存调整事件应为 `{"quantity": 95}`（最终值），而非 `{"delta": -5}`（增量值）。
> 深度合并是"后值覆盖前值"语义，无法执行累加、减法等增量运算。
>
> 如果你的事件是增量语义（delta、append、remove 等操作），**必须**自行实现 `apply-events`。
>
> **null 语义**：事件中值为 `null` 的字段表示"从状态中删除该字段"。
> 如果需要表达"字段存在但值为空"，请使用空字符串 `""` 或特殊标记值。

**Host 启动时的警告日志**：

```rust
if !engine.has_custom_apply_events() {
    tracing::warn!(
        "⚠️  聚合模块 '{}' 未导出 apply-events，使用 Host 默认策略（JSON 深度合并）。\
         此模式仅适用于快速原型，生产环境请自行实现 apply-events。\
         默认策略要求事件携带字段最终值（非增量值），否则会产生静默数据错误。",
        engine.name()
    );
}
```

约束：省略 `apply-events` 时，事件数据必须为 JSON 格式。

合并规则：
1. 初始状态：snapshot（空则为 `{}`）
2. 按时间顺序遍历事件，对每个事件执行 deep merge：
   - 对象字段：递归合并，后值覆盖前值
   - 数组字段：整体替换（后事件的数组完全替换前事件的同名数组）
   - 标量字段：后值覆盖前值

```rust
/// Host 内置的默认 apply-events 实现
fn default_apply_events(snapshot: &[u8], events: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let mut state: serde_json::Value = if snapshot.is_empty() {
        serde_json::Value::Object(Default::default())
    } else {
        serde_json::from_slice(snapshot)
            .map_err(|e| format!("快照非合法 JSON: {e}"))?
    };

    for event in events {
        let event_val: serde_json::Value = serde_json::from_slice(event)
            .map_err(|e| format!("事件非合法 JSON: {e}"))?;
        deep_merge(&mut state, &event_val);
    }

    serde_json::to_vec(&state).map_err(|e| format!("状态序列化失败: {e}"))
}

/// 深度合并：对象递归合并，null 删除字段，数组整体替换，标量覆盖
fn deep_merge(base: &mut Value, patch: &Value) {
    match (base, patch) {
        (Value::Object(base_map), Value::Object(patch_map)) => {
            for (k, v) in patch_map {
                if v.is_null() {
                    // null 语义：删除该字段
                    base_map.remove(k);
                } else if v.is_object() {
                    let entry = base_map
                        .entry(k.clone())
                        .or_insert(Value::Object(Default::default()));
                    if entry.is_object() {
                        deep_merge(entry, v);
                    } else {
                        *entry = v.clone();
                    }
                } else {
                    base_map.insert(k.clone(), v.clone());
                }
            }
        }
        (base, patch) => { *base = patch.clone(); }
    }
}
```

**合并示例**：

```json
// snapshot（初始状态）
{}

// event1: ItemCreated
{"name": "Widget", "quantity": 10, "tags": ["new"]}

// event2: StockAdjusted
{"quantity": 20}

// event3: TagsUpdated
{"tags": ["popular", "sale"]}

// event4: ItemDeleted（null 表示删除字段）
{"created": false, "name": null, "quantity": null, "tags": null}

// 最终状态（深度合并结果）：
{"created": false}
```

**何时应自行实现 apply-events**：
- 事件使用非 JSON 序列化格式（Bincode、MessagePack）
- 需要复杂的事件应用逻辑（如计算派生字段、维护索引）
- 事件是增量 delta 而非字段覆盖语义
- 需要事件版本升级（upcaster）

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
| Bincode | Rust 原生最快（~50ns/100B） | 仅 Rust 生态 | Rust-only 组件（生产首选，需自行实现 apply-events） |
| MessagePack | 紧凑、快速（~200ns/100B）、跨语言 | 需要 schema 约定 | 跨语言组件（需自行实现 apply-events） |
| JSON | 可读性好、调试方便（~500ns/100B） | 体积大、解析慢 | 开发调试 / 使用 Host 默认 apply-events 策略时**必须** |

> **注意**：省略 `apply-events` 时，事件和状态必须使用 JSON 格式，因为 Host 默认策略依赖 JSON 解析进行深度合并。

### 命令参数结构示例

```rust
// 每个命令独立的参数结构（与 WIT record 对应）
// 使用 kebab-case 反序列化，与 Host 传递的 JSON key 格式一致
//
// 命名转换链：
//   WIT record: item-name (kebab-case)
//   → GraphQL: itemName (camelCase)
//   → Host 序列化: "item-name" (kebab-case，Host 负责 camelCase → kebab-case 转换)
//   → WASM 反序列化: #[serde(rename_all = "kebab-case")]
//
// 单词字段（如 name、quantity）无需 rename，多词字段必须使用 kebab-case rename。

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateItemParams {
    pub name: String,
    pub quantity: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
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
当使用默认策略时，事件非合法 JSON 也会导致激活失败。
如果不加防护，每次访问该聚合都会触发激活 → 失败 → 重试的无限循环。

```rust
const MAX_ACTIVATION_RETRIES: u32 = 3;
const ACTIVATION_COOLDOWN: Duration = Duration::from_secs(60);

/// 使用带 TTL 的缓存追踪激活失败，条目在冷却期后自动过期，防止内存无限增长
pub struct ActivationFailureTracker {
    /// key: aggregate_id, value: 连续失败次数
    /// TTL = ACTIVATION_COOLDOWN，过期后自动清除（允许重新尝试激活）
    failures: moka::sync::Cache<String, u32>,
}

impl ActivationFailureTracker {
    pub fn new() -> Self {
        Self {
            failures: moka::sync::Cache::builder()
                .time_to_live(ACTIVATION_COOLDOWN)
                .max_capacity(100_000)
                .build(),
        }
    }

    pub fn is_corrupted(&self, aggregate_id: &str) -> bool {
        self.failures.get(aggregate_id)
            .map(|count| count >= MAX_ACTIVATION_RETRIES)
            .unwrap_or(false)
    }

    pub fn record_failure(&self, aggregate_id: &str) {
        let count = self.failures.get(aggregate_id).unwrap_or(0);
        self.failures.insert(aggregate_id.to_string(), count + 1);
    }

    pub fn clear(&self, aggregate_id: &str) {
        self.failures.invalidate(aggregate_id);
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

1. **新增命令（完整版）** — 添加 `validate-X` + `handle-X` + `X-params` record，重新编译组件
2. **新增命令（精简版）** — 仅添加 `handle-X` + `X-params` record（无前置校验）
3. **废弃命令** — 从 WIT 中移除对应函数，Host 重启后自动从 GraphQL schema 中消失
4. **新增事件类型** — 向后兼容，旧事件不受影响，`apply-events` 中添加新分支
5. **修改事件结构** — 使用事件版本号 + upcaster 模式（需自行实现 apply-events）
6. **废弃事件类型** — `apply-events` 中保留处理逻辑，`handle-X` 不再产出
7. **迁移到自定义 apply-events** — 从默认策略迁移时，新增 `apply-events` 导出即可，Host 重启后自动切换

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