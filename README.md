# rust-wasm-graphql

将任意 WASM Component 自动暴露为 GraphQL API 的运行时服务器。

## 项目定位

本项目解决的核心问题是：**如何零代码地将 WASM 组件转化为可调用的 Web API**。

传统做法中，你需要为每个 WASM 模块手写 HTTP handler、参数解析、类型转换等胶水代码。本项目通过自动内省 WASM Component 的 WIT 接口定义，动态生成对应的 GraphQL Schema，实现"编译即上线"的开发体验。

### 核心职责

- **WIT 接口内省** — 解析 WASM 组件内嵌的 WIT 元数据，提取导出函数签名（参数名、类型、返回值）
- **动态 Schema 生成** — 将 WIT 函数映射为 GraphQL Query 字段，自动处理类型转换（WIT → GraphQL）
- **WASM 运行时** — 基于 Wasmtime 加载并执行 WASM Component，支持 WASI 接口
- **GraphQL 服务** — 提供标准 GraphQL endpoint 和内置 GraphiQL 交互界面

### 架构概览

```
┌─────────────┐      ┌──────────────┐      ┌─────────────────┐
│  GraphQL    │      │  Dynamic     │      │   Wasmtime      │
│  Client     │─────▶│  Schema      │─────▶│   WASM Runtime  │
│  (GraphiQL) │      │  (async-gql) │      │   (Component)   │
└─────────────┘      └──────────────┘      └─────────────────┘
                            ▲
                            │ 内省
                     ┌──────┴───────┐
                     │  WIT 元数据   │
                     │  (wit-parser) │
                     └──────────────┘
```

## 项目结构

```
├── example/
│   ├── wit/              WIT 接口定义
│   ├── wasm-lib/         示例 WASM 组件（calculator）
│   └── wasm-lib2/        示例 WASM 组件（string-utils）
└── host-server/          GraphQL 宿主服务器
    ├── src/
    │   ├── lib.rs        库入口（导出公共模块）
    │   ├── main.rs       CLI 入口
    │   ├── wasm_engine.rs    WASM 组件加载与调用
    │   ├── wasm_registry.rs  多模块注册管理
    │   └── graphql.rs        动态 Schema 生成
    ├── tests/
    │   └── e2e_test.rs   端到端集成测试
    └── build.rs          自动编译示例 WASM 组件
```

## 支持的 WIT 类型

| WIT 类型 | GraphQL 类型 |
|----------|-------------|
| s8, s16, s32, u8, u16, u32 | Int |
| s64, u64 | String（避免精度丢失） |
| f32, f64 | Float |
| bool | Int |
| string | String |

## 前置依赖

- Rust toolchain（rustup）
- `wasm32-wasip1` 编译目标
- `cargo-component` 工具

```bash
rustup target add wasm32-wasip1
cargo install cargo-component
```

## 快速开始

### 1. 编译并启动服务（使用内置示例）

```bash
cargo build
cargo run
```

`host-server` 的 build.rs 会自动编译 `wasm-lib` 为 WASM 组件。启动后访问：

- GraphiQL 界面：http://localhost:8080/graphql
- 健康检查：http://localhost:8080/health

### 2. 加载自定义 WASM 组件

```bash
cargo run -- --wasm-dir /path/to/your_components/
```

指定监听地址：

```bash
cargo run -- --wasm-dir ./my_components --addr 127.0.0.1:3000
```

> 注意：目录中的非组件 `.wasm` 文件会被自动跳过并输出提示。

### 3. 调用 GraphQL API

启动后，所有 WASM 导出函数自动映射为 GraphQL Query 字段。以内置示例为例：

```graphql
query {
  calculator {
    add(a: 1, b: 2)
    fibonacci(n: 10)
    toUppercase(input: "hello")
  }
  strings {
    reverse(input: "world")
    charCount(input: "hello")
    repeat(input: "ab", times: 3)
  }
}
```

返回：

```json
{
  "data": {
    "calculator": {
      "add": 3,
      "fibonacci": "55",
      "toUppercase": "HELLO"
    },
    "strings": {
      "reverse": "dlrow",
      "charCount": 5,
      "repeat": "ababab"
    }
  }
}
```

## 测试

项目包含完整的测试套件，覆盖单元测试和端到端集成测试。

```bash
# 运行所有测试
cargo test

# 仅运行单元测试
cargo test --lib

# 仅运行端到端集成测试
cargo test --test e2e_test
```

### 测试结构

- **单元测试**（`src/*.rs` 中的 `#[cfg(test)] mod tests`）
  - `wasm_engine` — kebab_to_camel 转换、WIT 类型映射、组件加载与调用
  - `graphql` — to_pascal_case 转换、TypeRef 映射
  - `wasm_registry` — 目录扫描、重复检测、错误处理
- **端到端测试**（`tests/e2e_test.rs`）
  - 启动完整 HTTP 服务器
  - 通过 HTTP 发送 GraphQL 请求
  - 验证 calculator 和 strings 模块的所有函数
  - 测试多模块联合查询、错误处理、GraphQL 内省

## 编写自定义 WASM 组件

### 1. 定义 WIT 接口

```wit
// wit/world.wit
package example:my-service;

interface ops {
    my-func: func(x: s32) -> s32;
}

world my-world {
    export ops;
}
```

### 2. 实现组件

```rust
// src/lib.rs
mod bindings {
    wit_bindgen::generate!({
        world: "my-world",
        path: "../wit",
    });
}

use bindings::exports::example::my_service::ops::Guest;

struct Component;

impl Guest for Component {
    fn my_func(x: i32) -> i32 {
        x * 2
    }
}

bindings::export!(Component with_types_in bindings);
```

### 3. 编译为 WASM Component

```bash
cargo component build --release
```

### 4. 启动服务

```bash
cargo run -- --wasm target/wasm32-wasip1/release/my_component.wasm
```

## 命名转换规则

WIT 使用 kebab-case，GraphQL 使用 camelCase，转换自动完成：

| WIT 名称 | GraphQL 名称 |
|----------|-------------|
| `my-func` | `myFunc` |
| `to-uppercase` | `toUppercase` |
| `get-user-name` | `getUserName` |

## License

MIT
