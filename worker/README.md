# Worker — 基于 Virtual Actor 模型的事件溯源系统（单机版）

WASM 组件承载领域逻辑，Worker 运行时自动将组件导出映射为 GraphQL API，零胶水代码。单机部署，强一致零数据丢失。

## 数据流

```
Client → GraphQL → VirtualActorRuntime → Actor → EventStore(PostgreSQL)
```

## 核心设计

| 原则 | 实现方式 |
|------|----------|
| 强一致性 | 事件持久化到 PostgreSQL 确认后才响应客户端 |
| 按需激活 | 首次访问时从快照+事件恢复，后续命令直接操作内存状态 |
| 自动休眠 | 空闲超时或内存压力时保存快照并释放内存 |
| 位置透明 | 调用方只需 `(module, aggregate_id)`，运行时自动激活/路由 |
| 无锁串行 | 每个 Virtual Actor 单线程处理，channel 容量=1，无并发问题 |
| 故障恢复 | Actor 崩溃后从快照+事件自动重新激活 |

### 一致性铁律

```
客户端收到成功响应 → 事件一定已落盘
客户端收到失败响应 → 无副作用（状态未变）
客户端超时（未收到响应） → 重新查询聚合状态确认命令是否已生效
```

### 命令处理顺序

```
版本校验 → 可选前置校验(validate-X) → WASM handle-X → 同步持久化 → 内存apply → 响应客户端
```

## 快速开始

### 1. 前置条件

```bash
# 添加 WASM 编译目标
rustup target add wasm32-wasip1

# 安装 cargo-component（用于编译 WASM 组件）
cargo install cargo-component
```

需要本地运行 PostgreSQL，创建数据库和用户：

```bash
# 创建用户和数据库（示例）
createuser worker -P      # 密码: worker
createdb worker_test -O worker
```

### 2. 构建

```bash
# 在 workspace 根目录构建 worker（build.rs 会自动编译测试用 WASM 计数器组件）
cargo build -p worker
```

编译产物：
- `target/debug/worker` — Worker 可执行文件
- `target/wasm32-wasip1/release/wasm_counter.wasm` — 测试用计数器聚合组件

如果只需要编译 WASM 组件（不编译 worker）：

```bash
cargo component build --release --manifest-path worker/tests/wasm-counter/Cargo.toml --target-dir target
```

### 3. 运行

```bash
# 使用默认参数启动
cargo run -p worker

# 完整参数启动
cargo run -p worker -- \
    --wasm-dir target/wasm32-wasip1/release/ \
    --db-url "postgres://worker:worker@localhost:5432/worker_test" \
    --addr 0.0.0.0:8080 \
    --max-active 10000 \
    --snapshot-threshold 100

# 通过环境变量指定数据库连接
DATABASE_URL="postgres://worker:worker@localhost:5432/worker_test" \
    cargo run -p worker -- --wasm-dir target/wasm32-wasip1/release/

# 直接运行编译好的二进制
./target/debug/worker --wasm-dir target/wasm32-wasip1/release/ --addr 127.0.0.1:8080
```

启动后访问 `http://localhost:8080/graphql` 打开 GraphQL Playground。

### 4. 验证

```bash
# 健康检查
curl http://localhost:8080/health

# 发送 GraphQL 命令
curl -X POST http://localhost:8080/graphql \
    -H "Content-Type: application/json" \
    -d '{
        "query": "mutation { counter { increment(aggregateId: \"test\", expectedVersion: \"0\", amount: 5) { success version eventCount } } }"
    }'

# 查询聚合版本
curl -X POST http://localhost:8080/graphql \
    -H "Content-Type: application/json" \
    -d '{
        "query": "query { aggregateVersion(aggregateType: \"counter\", aggregateId: \"test\") }"
    }'
```

### 5. 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--wasm-dir` | `./wasm-modules` | WASM 聚合组件目录 |
| `--db-url` | `postgres://worker:worker@localhost:5432/worker_test` | PostgreSQL 连接串（支持 `DATABASE_URL` 环境变量） |
| `--addr` | `0.0.0.0:8080` | 监听地址 |
| `--max-active` | `10000` | 最大活跃聚合数量（内存预算控制） |
| `--snapshot-threshold` | `100` | 快照阈值（事件数量达到后触发异步快照） |

## API

服务启动后访问 `http://localhost:8080/graphql` 使用 GraphQL Playground。

### Query

```graphql
# 健康检查
query { health }

# 查询聚合当前版本
query {
  aggregateVersion(aggregateType: "counter", aggregateId: "my-counter")
}
```

### Mutation

```graphql
mutation {
  counter {
    increment(aggregateId: "my-counter", expectedVersion: "0", amount: 5) {
      success
      version
      eventCount
      error
    }
  }
}
```

- `aggregateId`：聚合根 ID，仅允许字母数字和 `-_`，长度 1-128
- `expectedVersion`：乐观并发控制，需与聚合当前版本匹配（首次为 `"0"`），版本冲突时客户端应重新查询后重试
- `version` 和 `expectedVersion` 使用 `UInt64` 标量，序列化为字符串避免 32 位溢出

### CommandResult 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `success` | `Boolean!` | 命令是否成功 |
| `version` | `UInt64!` | 命令执行后的聚合版本 |
| `eventCount` | `Int!` | 产生的事件数量 |
| `error` | `String` | 失败时的错误信息 |

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│                    GraphQL API (axum)                        │
│              Query (查询)  │  Mutation (命令入口)             │
└──────────────┬─────────────┴────────────────┬───────────────┘
               │                              │
               │               ┌──────────────▼──────────────┐
               │               │   Virtual Actor Runtime      │
               │               │                              │
               │               │   ┌─ active ──────────────┐  │
               │               │   │ agg-001 → Actor (热)   │  │
               │               │   │ agg-002 → Actor (温)   │  │
               │               │   └────────────────────────┘  │
               │               │                              │
               │               │   内存预算: max_active 聚合   │
               │               │   激活: 快照+事件 → 内存状态  │
               │               │   休眠: 内存状态 → 快照 → 释放│
               │               └──────────────┬──────────────┘
               │                              │ 同步写入
      ┌────────┴──────────────────────────────▼──────────────┐
      │          PostgreSQL (events + snapshots)              │
      └──────────────────────────────────────────────────────┘
```

## 项目结构

```
worker/
├── src/
│   ├── main.rs          # CLI 入口，解析参数并启动服务
│   ├── lib.rs           # 库入口，导出所有公共模块
│   ├── app.rs           # 服务启动与编排（加载 WASM、创建 Runtime、构建路由）
│   ├── runtime.rs       # Virtual Actor 运行时（Actor 生命周期、LRU 驱逐、背压控制）
│   ├── actor.rs         # Virtual Actor（单聚合状态管理、命令处理）
│   ├── wasm_engine.rs   # WASM 引擎（组件加载、函数调用、WIT 内省）
│   ├── event_store.rs   # PostgreSQL 事件存储（事件追加、快照、乐观并发控制）
│   ├── graphql.rs       # 动态 GraphQL Schema 构建（async-graphql dynamic API）
│   ├── command.rs       # 命令发现（从 WIT 元数据识别 handle-X/validate-X）
│   ├── config.rs        # 运行时配置（超时、连接池、燃料限制等）
│   ├── types.rs         # 领域类型定义（Command、Event、Snapshot）
│   └── error.rs         # 错误类型定义（版本冲突、毒事件、超时等）
├── tests/
│   ├── e2e_test.rs          # 端到端集成测试（启动完整 HTTP 服务验证全链路）
│   ├── fuzz_test.rs         # 模糊测试
│   ├── event_store_test.rs  # 事件存储集成测试（需 PostgreSQL）
│   ├── common/mod.rs        # 测试公共工具
│   └── wasm-counter/        # 测试用 WASM 计数器聚合组件
├── design/                  # 详细设计文档
│   ├── overview.md          # 架构总览、核心原则、性能目标
│   ├── command-flow.md      # 命令处理流程（Actor 生命周期、LRU 驱逐、优雅关闭）
│   ├── graphql-schema.md    # GraphQL Schema 设计
│   ├── event-store.md       # 事件存储 + 快照设计
│   └── wit-conventions.md   # WASM 组件 WIT 接口约定
├── build.rs                 # 构建脚本（自动编译测试用 WASM 组件）
└── Cargo.toml
```

## 核心模块说明

### Virtual Actor Runtime (`runtime.rs`)

单线程消息循环，所有寻址/激活/驱逐决策串行化。Actor 通过容量=1 的 channel 接收消息，同一时刻最多处理 1 条命令。

- **热路径**：Actor 已存在时 spawn task 直接发送到 Actor channel
- **冷路径**：Actor 不存在时，从快照+增量事件恢复状态，然后处理首条命令
- **LRU 驱逐**：活跃数超过 `max_active` 时驱逐最久未使用的 Actor
- **背压控制**：Runtime channel 满时 `try_send` 立即返回过载错误
- **优雅关闭**：向所有 Actor 发送 Evict，等待快照保存完成后退出

### Virtual Actor (`actor.rs`)

单聚合状态管理，命令处理流程：

1. 版本校验（乐观并发控制）
2. 可选前置校验（`validate-X`，超时 5s）
3. WASM `handle-X` 调用（超时 10s，fuel 限制防无限循环）
4. 同步持久化事件到 PostgreSQL
5. 内存状态 apply
6. 异步快照（事件数达阈值时触发）

### Event Store (`event_store.rs`)

PostgreSQL 存储，两张表：

- **events** — `UNIQUE(aggregate_type, aggregate_id, version)` 约束保证乐观并发控制
- **snapshots** — `PRIMARY KEY(aggregate_type, aggregate_id)`，版本守卫防覆盖

通过 `EventStore` trait 抽象存储层，方便测试时替换为内存 Mock。

### WASM 引擎 (`wasm_engine.rs`)

- 每次调用创建新 `Store` 实例（线程安全）
- Fuel 计量防止 WASM 无限循环
- WIT 内省自动提取模块名和命令列表
- `spawn_blocking` 执行 WASM 调用避免阻塞 async runtime

### GraphQL Schema (`graphql.rs`)

使用 `async-graphql` dynamic API 自动构建 Schema：

- Query：`health`、`aggregateVersion`
- Mutation：按模块命名空间组织，如 `counter { increment(...) }`
- 自定义 `UInt64` Scalar（字符串序列化）
- `CommandResult` 输出类型

## WASM 组件接口约定

每个聚合组件需导出以下函数：

```
handle-{command-name}:   func(state: list<u8>, command: list<u8>) -> result<list<list<u8>>, string>  [必须]
validate-{command-name}: func(command: list<u8>) -> result<_, string>                                [可选]
apply-events:            func(snapshot: list<u8>, events: list<list<u8>>) -> result<list<u8>, string> [必须]
```

- 模块名从 WIT `package` 字段的第二段提取（如 `example:counter` → `counter`）
- WIT kebab-case 自动转 GraphQL camelCase（如 `create-item` → `createItem`）
- 事件必须为 JSON 格式且包含 `type` 字段
- 启动时校验：孤立的 `validate-X`、缺少 `apply-events` 均导致启动失败

## 错误处理

| 场景 | 处理方式 | 数据安全 |
|------|----------|----------|
| 版本冲突 | 返回错误，客户端重试 | 安全 |
| handle 失败（业务拒绝） | 返回领域错误，状态不变 | 安全 |
| persist 失败（DB 不可用） | 返回 503，内存状态不变 | 安全 |
| persist 成功但 apply 失败 | Actor 退出，下次从 DB 重建 | 安全 |
| Actor 崩溃 | 下次访问自动重新激活 | 安全 |
| Runtime channel 满 | 返回 503（背压），客户端重试 | 安全 |
| WASM 执行超时 | 返回超时错误，Actor 状态不变 | 安全 |
| 毒事件（apply-events 回放失败） | 返回错误，记录日志 | 数据在 DB 中 |

## 运行测试

```bash
# 运行所有测试（需 PostgreSQL）
DATABASE_URL="postgres://worker:worker@localhost:5432/worker_test" cargo test

# 仅运行单元测试
cargo test --lib

# 仅运行端到端测试
cargo test --test e2e_test

# 运行模糊测试
cargo test --test fuzz_test
```

## 与完整版的差异

| 能力 | 单机版 | 完整版 |
|------|--------|--------|
| WASM 实例管理 | 每次调用创建新实例 | 预热实例池 + 熔断器 |
| 幂等检查 | 仅 DB 事务级 | 布隆过滤器 + DB 双层 |
| 快照存储 | PostgreSQL 表 | 独立 KV（Redis/sled） |
| 事件发布 | 无 | CDC + Kafka |
| 集群 | 无 | 一致性哈希 + Lease |
| 热更新 | 无（重启生效） | 文件监听 + 灰度发布 |
| 可观测性 | 基础日志（tracing） | OpenTelemetry 全链路 |

## 技术栈

- **WASM 运行时**: Wasmtime 27（component-model）
- **GraphQL**: async-graphql 7 + axum 0.7
- **数据库**: PostgreSQL（sqlx 0.7）
- **异步运行时**: Tokio
- **序列化**: serde / serde_json
- **CLI**: clap 4
