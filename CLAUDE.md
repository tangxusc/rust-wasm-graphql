# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
## 语言偏好

使用中文回复

## 测试要求

测试覆盖率80%
除非明确要求,否则必须端到端测试

## 开发模式

TDD开发,e2e测试(包含模糊测试)
修改代码需同步维护文档
遵循编程语言最佳实践
依赖缺失,自动安装
不要提交未验证的主流程改动
端到端测试失败时,应先修复失败原因,再交付结果
复杂任务,逐步实现,逐步验证,逐步推进

## 代码注释约束

所有代码注释必须使用中文,简短明确
注释传达代码的意图、目的或关键逻辑
注释需解释“为什么这么做”
必要时可注明注意事项、边界条件或算法思路

## 核心原则：证据胜于声称（Evidence Over Claim）
优先级：用户指令 > 本约束集 > 模型默认行为

### 实施前强制条件
在以下条件全部满足前，禁止编写任何实现代码：
已完成至少 3 个澄清性问题并得到用户回复
已探查代码库，检查了至少 3 个相关文件的实现
已提供至少 2 个可选方案并说明各自权衡
用户已书面确认设计方案
设计文档已通过自检并保存

### 调试强制流程
遇到错误时，必须顺序执行：
1. Observation：收集错误上下文（输出、堆栈、状态、复现步骤）
2. Hypothesis：提出可验证的根因假设
3. Verification：设计验证实验并执行
4. Implementation：修复 → Test → Green
5. Evidence：提供测试通过的输出证据

### 不确定性声明
若出现以下情况，必须暂停并请求用户澄清：
- 设计中有无法确认的假设
- 代码库中有不确定的实现可供参考但无法确认可复用性
- 依赖关系存在不确定或冲突

### 禁止行为清单
- 不验证假设的修复
- 不运行测试的完成确认
- 不探查代码库的方案假设
- 输出包含“应该能工作”“可能解决了”等不确定性措辞的完成声明（所有完成声明必须配有实际验证证据）

## 开发模式

TDD开发,e2e测试
修改代码需同步维护文档
遵循编程语言最佳实践

## Build Commands

除非明确要求,否则不使用release编译

```bash
# Build host-server (default member, also triggers WASM component builds via build.rs)
cargo build

# Build a single WASM component manually
cargo component build --release --manifest-path example/wasm-lib/Cargo.toml

# Run the server (loads all WASM modules from default directory)
cargo run -- --wasm-dir target/wasm32-wasip1/release/

# Run with custom directory
cargo run -- --wasm-dir /path/to/wasm/modules --addr 127.0.0.1:3000
```

## Test Commands

```bash
# 运行所有测试（单元测试 + 端到端测试）
cargo test

# 仅运行单元测试
cargo test --lib

# 仅运行端到端集成测试
cargo test --test e2e_test

# 运行特定模块的测试
cargo test --lib wasm_engine::tests
cargo test --lib graphql::tests
cargo test --lib wasm_registry::tests
```

## Prerequisites

```bash
rustup target add wasm32-wasip1
cargo install cargo-component
```

## Architecture

This project auto-exposes WASM Component exports as a GraphQL API with zero glue code.

**Data flow:** GraphQL request → dynamic schema resolver → Wasmtime component call → response

Key modules in `host-server/src/`:

- `lib.rs` — 库入口，导出所有公共模块供测试和集成使用
- `wasm_engine.rs` — Loads a single WASM component, introspects its WIT metadata via `wit-parser`, and calls exported functions via Wasmtime. Each `call_function` creates a fresh `Store` (thread-safe with `Arc`).
- `wasm_registry.rs` — Manages multiple `WasmEngine` instances. Scans a directory for `.wasm` files, extracts module names from WIT interface package metadata (e.g. `example:calculator` → `calculator`), detects duplicate module names. Gracefully skips non-component `.wasm` files.
- `graphql.rs` — Builds an `async-graphql` dynamic schema using namespace nesting: each WASM module becomes a GraphQL object type under Query (e.g. `query { calculator { add(a:1, b:2) } }`).
- `main.rs` — CLI entry point with `--wasm-dir` and `--addr` args.

**build.rs** compiles all example WASM components during `cargo build` and sets `DEFAULT_WASM_DIR` env var.

## Test Architecture

- **单元测试** — 内嵌在各模块中（`#[cfg(test)] mod tests`），覆盖辅助函数和核心逻辑
- **端到端测试** — `host-server/tests/e2e_test.rs`，启动完整 HTTP 服务器并通过 GraphQL 请求验证全链路

## WASM Component Conventions

- Components use `crate-type = ["cdylib"]` and `cargo-component` for building
- WIT interface definitions live alongside each component or in shared `example/wit/`
- Module name is derived from the exported interface's WIT `package` field (second segment after colon)
- WIT kebab-case names auto-convert to GraphQL camelCase; module names convert to PascalCase for type names

## Language

Project comments, CLI messages, and documentation are in Chinese (中文). Maintain this convention.
