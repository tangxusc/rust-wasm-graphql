# 设计文档（单机版）

基于 Virtual Actor 模型的事件溯源系统，单机部署，强一致零数据丢失。

数据流：`Client → GraphQL → VirtualActorRuntime → Actor → EventStore(PG)`

## 文档索引

- [overview.md](./overview.md) — 架构总览、核心原则、性能目标
- [command-flow.md](./command-flow.md) — 命令处理流程（Virtual Actor 生命周期、LRU 驱逐、优雅关闭）
- [graphql-schema.md](./graphql-schema.md) — GraphQL 层设计（Schema 自动生成、命令路由）
- [event-store.md](./event-store.md) — 事件存储 + 快照（PostgreSQL，事务级幂等）
- [wit-conventions.md](./wit-conventions.md) — WASM 组件 WIT 接口约定

## 与完整版的差异

| 能力 | 单机版 | 完整版 |
|------|--------|--------|
| WASM 实例管理 | 每次调用创建新实例 | 预热实例池 + 熔断器 |
| 幂等检查 | 仅 DB 事务级 | 布隆过滤器 + DB 双层 |
| 快照存储 | PostgreSQL 表 | 独立 KV（Redis/sled） |
| 事件发布 | 无 | CDC + Kafka |
| 集群 | 无 | 一致性哈希 + Lease |
| 热更新 | 无（重启生效） | 文件监听 + 灰度发布 |
| 可观测性 | 基础日志 | OpenTelemetry 全链路 |
