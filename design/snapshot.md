# 快照存储与触发策略（高性能版）

## 核心变化

- 快照主要用于 **Actor 恢复**（启动/崩溃后重建），而非每次命令处理
- Actor 内存常驻状态后，快照的读取频率大幅降低
- 快照生成由 Actor 异步触发，不阻塞命令处理

## 为什么用 KV 存储

快照的访问模式：

| 操作 | 模式 | 频率 |
|------|------|------|
| 读取 | `GET snapshot:{aggregate_id}` | Actor 启动/恢复时（低频） |
| 写入 | `PUT snapshot:{aggregate_id}` | 每 N 个事件后（低频） |
| 删除 | `DEL snapshot:{aggregate_id}` | 聚合销毁或 WASM 升级时 |

无范围查询、无联表、无事务需求。KV 存储是最佳匹配。

## 存储选型

| 场景 | 推荐 | 理由 |
|------|------|------|
| 开发/单机 | sled 或 redb | 嵌入式，零运维，Rust 原生 |
| 生产/低延迟 | Redis Cluster | 内存级读取，分片扩展 |
| 生产/大快照 | Redis + S3 分层 | 热数据 Redis，冷数据 S3 |
| 强一致要求 | TiKV / FoundationDB | 分布式强一致 KV |

## 数据结构

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub aggregate_id: String,
    pub aggregate_type: String,
    pub version: u64,         // 快照对应的最后一个事件版本号
    pub state: Vec<u8>,       // WASM apply_events 输出的序列化状态
    pub created_at: u64,      // Unix 毫秒时间戳
}
```

## Key 设计

```
snapshot:{aggregate_type}:{aggregate_id}
```

示例：`snapshot:inventory:item-001`

每个聚合只保留最新一份快照（覆盖写入）。

## Trait 定义

```rust
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// 加载快照（需要 aggregate_type 构造完整 key）
    async fn load(&self, aggregate_type: &str, aggregate_id: &str) -> Result<Option<Snapshot>>;
    async fn save(&self, snapshot: &Snapshot) -> Result<()>;
    async fn delete(&self, aggregate_type: &str, aggregate_id: &str) -> Result<()>;
    async fn delete_by_type(&self, aggregate_type: &str) -> Result<u64>;
}
```

## 触发策略

### 策略一：事件计数阈值（主策略）

Actor 在内存中跟踪自上次快照以来的事件数，达到阈值时异步生成快照：

```rust
pub struct SnapshotPolicy {
    /// 事件计数阈值（推荐 100-500，高性能场景可调高）
    pub threshold: u64,
    /// 是否启用
    pub enabled: bool,
}
```

### 策略二：Virtual Actor 休眠时生成（最重要）

当 Virtual Actor 因空闲超时或 LRU 驱逐而休眠时，保存当前状态为快照。这是 Virtual Actor 模型下最关键的快照时机——直接决定下次激活的速度：

```rust
impl VirtualActor {
    /// 休眠前的清理：保存快照（强一致版无需 flush，事件已同步写入）
    /// 使用 &self 因为 Actor 即将销毁，无需更新内部状态
    async fn on_deactivate(&self) {
        let snapshot = Snapshot {
            aggregate_id: self.aggregate_id.clone(),
            aggregate_type: self.module_name.clone(),
            version: self.version,
            state: self.state.clone(),
            created_at: now_millis(),
        };
        if let Err(e) = self.snapshot_store.save(&snapshot).await {
            tracing::warn!("[{}] 休眠快照保存失败: {e}", self.aggregate_id);
        }
    }
}
```

### 策略三：定时批量快照

后台任务定期扫描高版本聚合，批量生成快照：

```rust
pub struct SnapshotScheduler {
    interval: Duration,             // 每 5 分钟
    min_events_since: u64,          // 至少 50 个新事件才值得快照
}
```

### 触发时机总结

| 触发条件 | 阻塞命令处理？ | 适用场景 |
|----------|---------------|----------|
| 事件计数达阈值 | 否（异步） | 常规运行中的高频聚合 |
| Virtual Actor 休眠 | 是（休眠流程的一部分） | 空闲超时 / LRU 驱逐 |
| 定时批量 | 否（后台任务） | 兜底保障 |
| 服务优雅关闭 | 是（等待完成） | 服务重启 |

### 实现（集成在 Virtual Actor 中）

```rust
impl VirtualActor {
    fn maybe_snapshot(&self) {
        let last = self.last_snapshot_version.load(Ordering::Relaxed);
        let events_since = self.version - last;

        if self.policy.enabled && events_since >= self.policy.threshold {
            // 乐观更新：先设置为当前版本，防止后续命令重复触发快照
            self.last_snapshot_version.store(self.version, Ordering::Relaxed);
            let snapshot = Snapshot {
                aggregate_id: self.aggregate_id.clone(),
                aggregate_type: self.module_name.clone(),
                version: self.version,
                state: self.state.clone(),
                created_at: now_millis(),
            };
            let store = self.snapshot_store.clone();
            let version_before = self.version;
            let last_snapshot_version = self.last_snapshot_version.clone();
            tokio::spawn(async move {
                if let Err(e) = store.save(&snapshot).await {
                    tracing::warn!("快照保存失败: {e}");
                    // 保存失败：回退版本，下次命令会重新尝试快照
                    last_snapshot_version.store(version_before - 1, Ordering::Relaxed);
                }
            });
        }
    }
}
```

## 快照失效与 WASM 组件升级

当 WASM 组件升级（领域模型变更）时，旧快照的序列化格式可能不兼容：

1. 组件升级时清除该聚合类型的所有快照
2. Virtual Actor 下次激活时自动从事件全量重建
3. 重建后在休眠时生成新格式快照

```rust
impl VirtualActorRuntime {
    pub async fn on_module_upgrade(&self, aggregate_type: &str) -> Result<()> {
        // 清除旧快照
        let count = self.snapshot_store.delete_by_type(aggregate_type).await?;
        tracing::info!("已清除 {aggregate_type} 的 {count} 个旧快照");

        // 休眠内存中该类型的所有聚合（强制下次从事件重建）
        let to_deactivate: Vec<String> = self.active.iter()
            .filter(|entry| entry.value().aggregate_type() == aggregate_type)
            .map(|entry| entry.key().clone())
            .collect();

        for id in to_deactivate {
            self.deactivate(&id).await;
        }
        Ok(())
    }
}
```

## 恢复流程性能（Virtual Actor 激活）

| 场景 | 事件数 | 无快照激活 | 有快照激活 |
|------|--------|-----------|-----------|
| 新聚合 | 0 | 0ms | 0ms |
| 活跃聚合 | 1,000 | ~50ms | ~5ms (快照 + 少量增量) |
| 高频聚合 | 100,000 | ~5s | ~10ms |
| 历史聚合 | 1,000,000 | ~50s | ~15ms |

Virtual Actor 模型下快照的重要性更高——因为聚合会频繁休眠/激活，快照质量直接决定激活延迟。
