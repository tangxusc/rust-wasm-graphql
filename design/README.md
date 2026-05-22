# 设计文档

本目录包含高性能事件溯源（Event Sourcing）与命令处理（Command Handler）的架构设计。

目标：强一致零数据丢失，基于 Virtual Actor 模型 + 同步持久化 + 分片架构。

## 文档索引

- [overview.md](./overview.md) — 架构总览、性能目标、核心设计原则
- [command-flow.md](./command-flow.md) — 命令处理流程（Actor 模型、背压、LRU 驱逐）
- [graphql-schema.md](./graphql-schema.md) — GraphQL 层设计（command_id / aggregate_id / Actor 路由集成）
- [event-store.md](./event-store.md) — 事件存储设计（分片、批量写入、双层幂等）
- [snapshot.md](./snapshot.md) — 快照存储与触发策略（KV 存储、多触发时机）
- [event-publishing.md](./event-publishing.md) — 领域事件发布（CDC + Kafka）
- [wasm-instance-pool.md](./wasm-instance-pool.md) — WASM 实例池（预热、无锁获取、自动归还）
- [wit-conventions.md](./wit-conventions.md) — WASM 组件 WIT 接口约定与性能约束
