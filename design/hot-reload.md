# WASM 组件热更新设计

## 设计目标

支持 WASM 组件的运行时热更新，无需重启服务：
- **零停机**：更新期间持续服务请求
- **灰度发布**：逐步切流到新版本，异常时快速回滚
- **数据安全**：确保事件溯源的正确性不因更新而破坏

## 更新触发方式

### 文件系统监听（推荐）

```rust
use notify::{Watcher, RecursiveMode, Event, EventKind};

pub struct HotReloadWatcher {
    wasm_dir: PathBuf,
    registry: Arc<RwLock<WasmRegistry>>,
    pool_manager: Arc<RwLock<WasmPoolManager>>,
    actor_runtime: Arc<VirtualActorRuntime>,
}

impl HotReloadWatcher {
    pub async fn start(self) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);

        let mut watcher = notify::recommended_watcher(move |event: Result<Event, _>| {
            if let Ok(event) = event {
                match event.kind {
                    EventKind::Create(_) | EventKind::Modify(_) => {
                        let _ = tx.blocking_send(event);
                    }
                    _ => {}
                }
            }
        })?;

        watcher.watch(&self.wasm_dir, RecursiveMode::NonRecursive)?;

        // 防抖：文件写入可能触发多次事件，等待稳定后再处理
        let debounce = Duration::from_millis(500);
        let mut pending: HashMap<PathBuf, Instant> = HashMap::new();

        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    for path in event.paths {
                        if path.extension() == Some("wasm".as_ref()) {
                            pending.insert(path, Instant::now());
                        }
                    }
                }
                _ = tokio::time::sleep(debounce) => {
                    let now = Instant::now();
                    let ready: Vec<PathBuf> = pending.iter()
                        .filter(|(_, ts)| now.duration_since(**ts) >= debounce)
                        .map(|(p, _)| p.clone())
                        .collect();

                    for path in ready {
                        pending.remove(&path);
                        if let Err(e) = self.handle_update(&path).await {
                            tracing::error!("热更新失败 {:?}: {e}", path);
                        }
                    }
                }
            }
        }
    }
}
```

### API 触发

```rust
/// POST /admin/reload?module=inventory
/// 手动触发指定模块的热更新
async fn admin_reload(
    module: Query<String>,
    state: State<AppState>,
) -> Result<Json<ReloadResult>> {
    state.hot_reloader.reload_module(&module).await
}
```

## 更新流程

### 单模块热更新时序

```
新 .wasm 文件写入 wasm_dir
    │
    ├── 1. 文件系统事件触发（防抖 500ms）
    │
    ├── 2. 加载并校验新组件
    │      - WIT 接口兼容性检查
    │      - 函数签名校验
    │      - 沙箱内试运行（可选）
    │
    ├── 3. 创建新实例池（预热）
    │      - 新池与旧池并存
    │
    ├── 4. 原子切换 WasmPoolManager 中的模块引用
    │      - 新请求使用新池
    │      - 旧池中正在使用的实例继续完成当前调用
    │
    ├── 5. 清除该聚合类型的所有快照
    │      - 下次激活时从事件全量重建
    │
    ├── 6. 休眠该类型所有活跃 Actor
    │      - 发送 Deactivate，等待退出
    │      - 下次访问时用新组件重新激活
    │
    ├── 7. 等待旧池所有实例归还后释放
    │
    └── 8. 更新 GraphQL schema（如果命令列表变化）
```

### 实现

```rust
impl HotReloadWatcher {
    async fn handle_update(&self, wasm_path: &Path) -> Result<()> {
        let module_name = extract_module_name(wasm_path)?;
        tracing::info!("检测到模块更新: {module_name}");

        // 1. 加载并校验新组件
        let new_component = self.load_and_validate(wasm_path).await?;

        // 2. 创建新实例池
        let new_pool = WasmInstancePool::new(
            self.engine.clone(),
            Arc::new(new_component),
            &module_name,
            self.pool_size,
        )?;

        // 3. 原子切换（旧池引用计数归零后自动释放）
        {
            let mut manager = self.pool_manager.write().await;
            manager.replace_pool(&module_name, Arc::new(new_pool));
        }

        // 4. 清除快照 + 休眠 Actor
        self.actor_runtime.on_module_upgrade(&module_name).await?;

        // 5. 更新 GraphQL schema（如果需要）
        self.maybe_rebuild_schema(&module_name).await?;

        tracing::info!("模块 {module_name} 热更新完成");
        Ok(())
    }

    async fn load_and_validate(&self, path: &Path) -> Result<Component> {
        let bytes = tokio::fs::read(path).await?;
        let component = Component::from_binary(&self.engine, &bytes)?;

        // WIT 接口兼容性检查
        let new_commands = CommandDiscovery::discover_from_component(&component)?;
        let old_commands = self.registry.read().await
            .get_commands(&extract_module_name(path)?);

        self.check_compatibility(&old_commands, &new_commands)?;

        Ok(component)
    }
}
```

## 兼容性检查

### 向后兼容规则

| 变更类型 | 是否兼容 | 说明 |
|----------|----------|------|
| 新增命令（handle-X + X-params） | 兼容 | GraphQL schema 新增字段 |
| 删除命令 | **不兼容** | 客户端可能正在调用 |
| 修改命令参数（新增可选字段） | 兼容 | 旧客户端不传新字段 |
| 修改命令参数（删除字段/改类型） | **不兼容** | 旧客户端请求会失败 |
| 修改 apply-events 逻辑 | 兼容 | 事件格式不变即可 |
| 新增事件类型 | 兼容 | apply-events 需处理新类型 |

### 兼容性检查实现

```rust
impl HotReloadWatcher {
    fn check_compatibility(
        &self,
        old: &[CommandDef],
        new: &[CommandDef],
    ) -> Result<()> {
        // 检查是否有命令被删除
        for old_cmd in old {
            if !new.iter().any(|c| c.name == old_cmd.name) {
                return Err(Error::incompatible(format!(
                    "命令 '{}' 在新版本中被删除，不兼容热更新。\
                     如需删除命令，请使用滚动重启方式部署。",
                    old_cmd.name
                )));
            }
        }

        // 检查参数签名变更（仅检测破坏性变更）
        for old_cmd in old {
            if let Some(new_cmd) = new.iter().find(|c| c.name == old_cmd.name) {
                self.check_params_compatible(old_cmd, new_cmd)?;
            }
        }

        Ok(())
    }
}
```

## 灰度发布

### 基于流量比例的灰度

```rust
pub struct CanaryConfig {
    /// 灰度比例（0.0 - 1.0）
    pub traffic_ratio: f64,
    /// 灰度持续时间（观察期）
    pub observation_period: Duration,
    /// 自动晋升条件：错误率低于此阈值
    pub error_rate_threshold: f64,
    /// 自动回滚条件：错误率高于此阈值
    pub rollback_threshold: f64,
}

pub struct CanaryDeployment {
    module_name: String,
    old_pool: Arc<WasmInstancePool>,
    new_pool: Arc<WasmInstancePool>,
    config: CanaryConfig,
    metrics: CanaryMetrics,
}

impl CanaryDeployment {
    /// 根据灰度比例选择实例池
    pub async fn acquire(&self) -> Result<PooledInstance> {
        let use_new = rand::random::<f64>() < self.config.traffic_ratio;
        if use_new {
            self.new_pool.acquire().await
        } else {
            self.old_pool.acquire().await
        }
    }

    /// 灰度观察：定期检查错误率，决定晋升或回滚
    pub async fn observe(&self) -> CanaryDecision {
        let error_rate = self.metrics.new_version_error_rate();

        if error_rate > self.config.rollback_threshold {
            CanaryDecision::Rollback
        } else if error_rate < self.config.error_rate_threshold
            && self.metrics.elapsed() > self.config.observation_period
        {
            CanaryDecision::Promote
        } else {
            CanaryDecision::Continue
        }
    }
}

enum CanaryDecision {
    Continue,   // 继续观察
    Promote,    // 全量切换到新版本
    Rollback,   // 回滚到旧版本
}
```

### 灰度流程

```
新组件部署
    │
    ├── 1. 校验通过，创建新池
    │
    ├── 2. 进入灰度阶段（traffic_ratio = 0.1）
    │      - 10% 请求使用新组件
    │      - 90% 请求使用旧组件
    │
    ├── 3. 观察期（默认 5 分钟）
    │      - 监控新版本错误率
    │      - 错误率 > rollback_threshold → 自动回滚
    │
    ├── 4a. 晋升（错误率正常）
    │      - traffic_ratio → 1.0
    │      - 清除快照，休眠 Actor
    │      - 释放旧池
    │
    └── 4b. 回滚（错误率异常）
           - traffic_ratio → 0.0
           - 释放新池
           - 告警通知
```

## 回滚机制

### 自动回滚触发条件

| 条件 | 阈值 | 动作 |
|------|------|------|
| 新版本错误率 | > 5% | 自动回滚 |
| 新版本 WASM trap 率 | > 1% | 自动回滚 |
| 新版本 P99 延迟 | > 旧版本 3 倍 | 告警（不自动回滚） |
| 兼容性检查失败 | — | 拒绝更新 |

### 手动回滚

```rust
/// POST /admin/rollback?module=inventory
/// 手动回滚到上一个版本
async fn admin_rollback(
    module: Query<String>,
    state: State<AppState>,
) -> Result<Json<RollbackResult>> {
    state.hot_reloader.rollback(&module).await
}
```

### 版本历史

保留最近 N 个版本的组件文件，支持快速回滚：

```
wasm_dir/
├── inventory.wasm              ← 当前版本
├── .versions/
│   ├── inventory.wasm.v3       ← 上一版本
│   ├── inventory.wasm.v2
│   └── inventory.wasm.v1
```

## 与事件溯源的交互

### 关键约束

**事件格式必须向后兼容。** 新版本的 `apply-events` 必须能正确处理所有历史事件。

| 场景 | 处理方式 |
|------|----------|
| 新版本新增事件类型 | apply-events 中添加新分支，旧事件不受影响 |
| 新版本修改事件结构 | 使用 upcaster 模式，apply-events 中兼容新旧格式 |
| 新版本删除事件类型 | apply-events 中保留处理逻辑（历史事件仍需重放） |
| 新版本修改 handle 逻辑 | 安全：仅影响未来命令产出的事件 |

### 更新后的 Actor 恢复

```
Actor 被休眠（热更新触发）
    │
    ├── 快照已清除
    │
    └── 下次访问时重新激活
         ├── 从 Event Store 加载全部事件
         ├── 使用新版本 apply-events 重建状态
         └── 新快照保存（新格式）
```

## 配置

```rust
pub struct HotReloadConfig {
    /// 是否启用热更新
    pub enabled: bool,                    // 默认 false（生产环境手动开启）
    /// 文件变更防抖时间
    pub debounce: Duration,              // 默认 500ms
    /// 是否启用灰度发布
    pub canary_enabled: bool,            // 默认 true
    /// 灰度配置
    pub canary: CanaryConfig,
    /// 保留历史版本数量
    pub max_versions: usize,             // 默认 5
    /// 兼容性检查是否为强制（false 时仅告警不阻止）
    pub strict_compatibility: bool,      // 默认 true
}
```

## 与集群模式的交互

集群模式下，热更新需要协调多节点：

1. **组件分发**：通过共享存储（S3/NFS）或 gRPC 推送分发新组件到所有节点
2. **版本一致性**：所有节点必须在短时间窗口内完成切换，避免版本不一致
3. **协调方式**：通过 etcd 写入版本变更事件，各节点 watch 后自行执行热更新

```
管理员上传新组件到共享存储
    │
    ├── 写入 etcd: module-versions/inventory = { version: "v4", path: "s3://..." }
    │
    ├── Node A watch 到变更 → 下载 → 热更新
    ├── Node B watch 到变更 → 下载 → 热更新
    └── Node C watch 到变更 → 下载 → 热更新
```

注意：集群模式下 `module_version` 哈希会在热更新后自动更新，
确保转发请求时版本检查能正确检测到节点间的版本差异（见 [cluster.md](./cluster.md)）。