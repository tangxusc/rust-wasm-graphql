# 可观测性设计

## 设计目标

为 Host Server 提供完整的可观测性支持，覆盖三大支柱：
- **指标（Metrics）**：系统健康度、性能瓶颈定位
- **追踪（Tracing）**：请求全链路追踪，跨组件关联
- **日志（Logging）**：结构化日志，与追踪关联

## 技术选型

| 支柱 | 选型 | 理由 |
|------|------|------|
| 指标 | Prometheus（OpenTelemetry 导出） | 生态成熟，Grafana 集成 |
| 追踪 | OpenTelemetry + Jaeger/Tempo | 标准协议，厂商中立 |
| 日志 | tracing crate + JSON 格式 | Rust 生态标准，与 OTel 集成 |

## 集成架构

```
┌─────────────────────────────────────────────────────────┐
│  Host Server                                             │
│                                                           │
│  tracing crate ──→ OpenTelemetry SDK ──→ OTLP Exporter  │
│  (spans + logs)     (批量导出)            (gRPC)         │
│                                                           │
│  metrics crate ──→ Prometheus Exporter ──→ /metrics      │
└──────────────────────────────────┬───────────────────────┘
                                   │ OTLP gRPC
                          ┌────────▼────────┐
                          │  OTel Collector  │
                          └───┬────────┬────┘
                              │        │
                    ┌─────────▼┐  ┌────▼──────┐
                    │  Jaeger   │  │  Loki     │
                    │  (追踪)   │  │  (日志)   │
                    └──────────┘  └───────────┘
```

## 指标定义

### 命令处理指标

```rust
/// 命令级指标（每个 module × command_type 独立）
pub struct CommandMetrics {
    /// 命令处理总耗时（从 Gateway 入口到响应客户端）
    pub command_duration: Histogram,
    /// validate 阶段耗时
    pub validate_duration: Histogram,
    /// handle 阶段耗时（WASM 调用）
    pub handle_duration: Histogram,
    /// persist 阶段耗时（DB 写入等待）
    pub persist_duration: Histogram,
    /// 命令成功计数
    pub success_total: Counter,
    /// 命令失败计数（按错误类型标签区分）
    pub failure_total: Counter,
    /// validate 拒绝计数
    pub rejection_total: Counter,
    /// 幂等命中计数
    pub idempotent_hit_total: Counter,
}

/// 标签维度
/// labels: { module, command_type, status }
```

### Virtual Actor 生命周期指标

```rust
pub struct ActorMetrics {
    /// 当前活跃 Actor 数量（gauge）
    pub active_actors: Gauge,
    /// Actor 激活计数
    pub activation_total: Counter,
    /// Actor 休眠计数（按原因：idle_timeout / lru_eviction / shutdown）
    pub deactivation_total: Counter,
    /// Actor 激活耗时（从开始恢复到可接收命令）
    pub activation_duration: Histogram,
    /// Actor 恢复事件数（反映快照质量）
    pub recovery_events_count: Histogram,
    /// Actor mailbox 排队深度（gauge，采样）
    pub mailbox_depth: Gauge,
    /// Actor 处理命令耗时（不含排队等待）
    pub processing_duration: Histogram,
    /// 驱逐失败计数（无可驱逐候选者）
    pub eviction_failed_total: Counter,
}

/// 标签维度
/// labels: { module, aggregate_id (高基数，仅在 debug 模式启用) }
```

### WASM 实例池指标

```rust
pub struct WasmPoolMetrics {
    /// 池当前可用实例数（gauge）
    pub available_instances: Gauge,
    /// acquire 命中池（无需创建）计数
    pub pool_hit_total: Counter,
    /// acquire 池耗尽（创建临时实例）计数
    pub pool_miss_total: Counter,
    /// WASM 函数 trap 计数
    pub trap_total: Counter,
    /// 实例补充计数
    pub replenish_total: Counter,
}

/// 标签维度
/// labels: { module }
```

### Event Store 指标

```rust
pub struct EventStoreMetrics {
    /// 事件写入耗时（含事务提交）
    pub write_duration: Histogram,
    /// 事件加载耗时
    pub load_duration: Histogram,
    /// 版本冲突计数
    pub version_conflict_total: Counter,
    /// 连接池使用率（gauge）
    pub pool_usage_ratio: Gauge,
    /// 连接获取等待耗时
    pub acquire_duration: Histogram,
}
```

### 系统级指标

```rust
pub struct SystemMetrics {
    /// Gateway 并发请求数（gauge）
    pub concurrent_requests: Gauge,
    /// 背压拒绝计数（503）
    pub backpressure_rejected_total: Counter,
    /// 布隆过滤器误判率（采样估算）
    pub bloom_false_positive_rate: Gauge,
    /// 快照保存成功/失败计数
    pub snapshot_save_total: Counter,
    pub snapshot_save_failed_total: Counter,
}
```

## 分布式追踪

### Span 层次结构

```
[GraphQL Request]                          ← root span
  ├── [Gateway.execute]                    ← command_id, aggregate_id
  │     ├── [idempotency_check]            ← bloom_hit: bool
  │     ├── [validate]                     ← module, command_type (仅当存在时)
  │     └── [actor.send]                   ← aggregate_id
  │           ├── [actor.activate]         ← 仅冷启动时出现
  │           │     ├── [snapshot.load]
  │           │     └── [apply_events]     ← event_count
  │           ├── [wasm.handle]            ← function_name
  │           ├── [event_store.persist]    ← event_count, version
  │           ├── [wasm.apply_events]      ← 仅有自定义实现时
  │           └── [snapshot.maybe_save]    ← 仅触发时出现
  └── [response]
```

### Span 属性约定

```rust
// 所有 span 共享的属性
span.set_attribute("service.name", "host-server");
span.set_attribute("service.version", env!("CARGO_PKG_VERSION"));

// 命令相关
span.set_attribute("command.id", command_id);
span.set_attribute("command.module", module);
span.set_attribute("command.type", command_type);
span.set_attribute("aggregate.id", aggregate_id);

// Actor 相关
span.set_attribute("actor.generation", generation);
span.set_attribute("actor.state", "hot|cold|activating");

// 结果
span.set_attribute("command.success", true);
span.set_attribute("command.version", new_version);
span.set_attribute("command.event_count", event_count);
```

### 采样策略

```rust
pub struct TracingSamplerConfig {
    /// 默认采样率（生产环境建议 1%-10%）
    pub default_rate: f64,           // 默认 0.01
    /// 错误请求始终采样
    pub always_sample_errors: bool,  // 默认 true
    /// 慢请求阈值（超过则强制采样）
    pub slow_threshold: Duration,    // 默认 100ms
    /// 特定聚合强制采样（调试用）
    pub force_sample_aggregates: HashSet<String>,
}
```

## 结构化日志

### 日志级别约定

| 级别 | 使用场景 |
|------|----------|
| ERROR | 数据可能不一致、需要人工介入（persist 失败、lease 过期写入） |
| WARN | 可自愈但需关注（快照保存失败、池耗尽、驱逐失败、默认 apply-events 模式） |
| INFO | 关键生命周期事件（Actor 激活/休眠、服务启动/关闭、组件加载） |
| DEBUG | 请求级详情（命令处理各阶段耗时、WASM 调用参数） |
| TRACE | 内部状态变更（mailbox 深度变化、布隆过滤器 rotate） |

### 日志格式（JSON）

```json
{
  "timestamp": "2026-05-24T10:30:00.123Z",
  "level": "INFO",
  "target": "host_server::virtual_actor",
  "message": "Actor 激活完成",
  "span": { "name": "actor.activate" },
  "fields": {
    "aggregate_id": "item-001",
    "module": "inventory",
    "recovery_events": 42,
    "duration_ms": 8.5
  },
  "trace_id": "abc123...",
  "span_id": "def456..."
}
```

## 健康检查端点

### /health（存活探针）

```rust
/// Kubernetes liveness probe
/// 仅检查进程是否正常运行，不检查依赖
async fn health_liveness() -> StatusCode {
    StatusCode::OK
}
```

### /ready（就绪探针）

```rust
/// Kubernetes readiness probe
/// 检查所有依赖是否可用
async fn health_readiness(
    pool: &PgPool,
    wasm_pool: &WasmPoolManager,
    gateway: &CommandGateway,
) -> (StatusCode, Json<ReadinessResponse>) {
    let checks = vec![
        ("database", check_database(pool).await),
        ("wasm_pool", check_wasm_pool(wasm_pool)),
        ("not_shutting_down", !gateway.is_shutting_down()),
    ];

    let all_healthy = checks.iter().all(|(_, ok)| *ok);
    let status = if all_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(ReadinessResponse { checks }))
}
```

### /metrics（Prometheus 指标）

```
GET /metrics → Prometheus 文本格式
```

### /admin/status（运维状态）

```rust
/// 详细运维状态（非 Kubernetes 探针，供人工查看）
#[derive(Serialize)]
pub struct AdminStatus {
    pub uptime_seconds: u64,
    pub active_actors: usize,
    pub max_active: usize,
    pub wasm_modules: Vec<ModuleStatus>,
    pub event_store_pool: PoolStatus,
    pub bloom_filter_size: usize,
    pub shutting_down: bool,
}
```

## 告警规则建议

| 指标 | 条件 | 严重度 | 含义 |
|------|------|--------|------|
| `command_duration_p99` | > 50ms 持续 5min | Warning | 命令处理变慢 |
| `persist_duration_p99` | > 10ms 持续 5min | Warning | DB 写入变慢 |
| `version_conflict_total` | > 100/min | Info | 高并发冲突 |
| `backpressure_rejected_total` | > 0 持续 1min | Warning | 系统过载 |
| `pool_miss_total` | > pool_hit_total × 0.1 | Warning | WASM 池不足 |
| `trap_total` | > 10/min | Critical | WASM 组件异常 |
| `active_actors` | > max_active × 0.9 | Warning | 内存压力 |
| `eviction_failed_total` | > 0 持续 1min | Critical | 无法腾出空间 |
| `snapshot_save_failed_total` | > 5/min | Warning | 快照存储异常 |

## 配置

```rust
pub struct ObservabilityConfig {
    /// OTLP 导出端点
    pub otlp_endpoint: String,           // 默认 "http://localhost:4317"
    /// Prometheus 指标端口
    pub metrics_port: u16,               // 默认 9090
    /// 追踪采样配置
    pub tracing: TracingSamplerConfig,
    /// 日志级别
    pub log_level: String,               // 默认 "info"
    /// 是否启用 aggregate_id 高基数标签（仅调试）
    pub enable_high_cardinality: bool,   // 默认 false
}
```
