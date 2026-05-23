# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## test

测试覆盖率80%
除非明确要求,否则必须端到端测试

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
