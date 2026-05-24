# 设计文档

本目录包含高性能事件溯源（Event Sourcing）与命令处理（Command Handler）的架构设计。

目标：强一致零数据丢失，基于 Virtual Actor 模型 + 同步持久化 + 分片架构。

## 文档索引

- [overview.md](./overview.md) — 架构总览、性能目标、核心设计原则
- [command-flow.md](./command-flow.md) — 命令处理流程（Actor 模型、背压、LRU 驱逐、优雅关闭）
- [graphql-schema.md](./graphql-schema.md) — GraphQL 层设计（command_id / aggregate_id / Actor 路由集成）
- [event-store.md](./event-store.md) — 事件存储设计（单实例 PostgreSQL、事务级幂等、滚动布隆过滤器）
- [snapshot.md](./snapshot.md) — 快照存储与触发策略（KV 存储、多触发时机）
- [event-publishing.md](./event-publishing.md) — 领域事件发布（CDC + Kafka，acks=all）
- [wasm-instance-pool.md](./wasm-instance-pool.md) — WASM 实例池（预热、无锁获取、pooled/temporary 分离）
- [wit-conventions.md](./wit-conventions.md) — WASM 组件 WIT 接口约定、崩溃循环防护
- [multi-command-wit.md](./multi-command-wit.md) — 多命令 WIT 接口设计（命令发现、路由、监控限流）
- [cluster.md](./cluster.md) — 集群方案（一致性哈希、Lease 防双激活、故障迁移）
- [observability.md](./observability.md) — 可观测性设计（OpenTelemetry、指标、追踪、健康检查）
- [hot-reload.md](./hot-reload.md) — WASM 组件热更新（文件监听、灰度发布、回滚机制）
