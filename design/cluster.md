# 集群方案（多节点 Virtual Actor 分布）

## 问题

单机 Virtual Actor Runtime 无法满足 500,000+ TPS 的集群目标。
多节点部署需要解决：
- 同一聚合只能在一个节点上激活（防止双写）
- 请求路由到正确的节点
- 节点故障时 Actor 自动迁移

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Load Balancer (L4)                         │
└──────────┬──────────────────┬──────────────────┬────────────┘
           │                  │                  │
    ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
    │   Node A    │    │   Node B    │    │   Node C    │
    │             │    │             │    │             │
    │ Actor: 1,4  │    │ Actor: 2,5  │    │ Actor: 3,6  │
    │ (owner)     │    │ (owner)     │    │ (owner)     │
    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
           │                  │                  │
    ┌──────▼──────────────────▼──────────────────▼────────────┐
    │              Placement Service (etcd/ZooKeeper)           │
    │         aggregate_id → node 映射 + lease 管理            │
    └─────────────────────────────────────────────────────────┘
```

## Actor 分布策略

### 一致性哈希（默认）

```rust
use consistent_hash::ConsistentHash;

pub struct ActorPlacement {
    ring: ConsistentHash<NodeId>,
    virtual_nodes: usize,  // 每个物理节点 150 个虚拟节点
}

impl ActorPlacement {
    /// 确定聚合应归属的节点
    pub fn locate(&self, aggregate_id: &str) -> NodeId {
        self.ring.get(aggregate_id).clone()
    }

    /// 节点加入/离开时重新平衡
    pub fn rebalance(&mut self, nodes: &[NodeId]) {
        self.ring = ConsistentHash::new();
        for node in nodes {
            self.ring.add(node, self.virtual_nodes);
        }
    }
}
```

## Lease 机制（防止双激活）

同一聚合在任意时刻只能被一个节点持有。通过分布式 Lease + Fencing Token 保证：

```rust
pub struct ActorLease {
    store: Arc<dyn LeaseStore>,  // etcd / ZooKeeper / Redis
    node_id: NodeId,
    ttl: Duration,               // 默认 30s
    renew_interval: Duration,    // 默认 10s（TTL 的 1/3）
}

#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// 尝试获取 lease，成功返回 lease_id 和 fencing_token（单调递增 epoch）
    async fn acquire(&self, key: &str, node: &NodeId, ttl: Duration) -> Result<Option<LeaseGrant>>;
    /// 续约
    async fn renew(&self, lease_id: &LeaseId) -> Result<()>;
    /// 释放
    async fn release(&self, lease_id: &LeaseId) -> Result<()>;
    /// 监听 lease 变更（用于故障检测）
    async fn watch(&self, prefix: &str) -> Result<LeaseEventStream>;
}

#[derive(Debug, Clone)]
pub struct LeaseGrant {
    pub lease_id: LeaseId,
    pub fencing_token: u64,  // 单调递增，每次 acquire 自增
}

impl ActorLease {
    /// Actor 激活前必须获取 lease
    pub async fn acquire_for(&self, aggregate_id: &str) -> Result<LeaseGuard> {
        let key = format!("actor-lease/{}", aggregate_id);
        let grant = self.store.acquire(&key, &self.node_id, self.ttl).await?
            .ok_or(Error::lease_held_by_other())?;

        // 启动后台续约
        let guard = LeaseGuard::new(
            grant.lease_id,
            grant.fencing_token,
            self.store.clone(),
            self.renew_interval,
        );
        Ok(guard)
    }
}

/// RAII 守卫：Drop 时释放 lease，续约失败时标记 Actor 为 poisoned
pub struct LeaseGuard {
    lease_id: LeaseId,
    fencing_token: u64,
    poisoned: Arc<AtomicBool>,
    _renew_task: JoinHandle<()>,
}

impl LeaseGuard {
    pub fn fencing_token(&self) -> u64 { self.fencing_token }

    /// 检查 lease 是否仍然有效（续约未失败）
    pub fn is_valid(&self) -> bool { !self.poisoned.load(Ordering::Acquire) }
}
```

### Fencing Token 防脑裂

**问题**：节点 GC 暂停或网络分区 > TTL 时，lease 过期但旧节点的 Actor 仍在内存中运行，
可能与新节点的 Actor 同时写入 Event Store，导致数据不一致。

**解决方案**：Event Store 写入时携带 fencing_token，拒绝过期 token 的写入。

```rust
/// Event Store 分片接口（与 event-store.md 统一定义）
/// fencing_token 为 Option<u64>：集群模式传 Some(token)，单机模式传 None 跳过检查
/// 集群模式下 token 必须 >= 当前记录的最大 token，否则拒绝写入（防脑裂）
#[async_trait]
pub trait EventStoreShard: Send + Sync {
    async fn append_with_idempotency(
        &self,
        aggregate_id: &str,
        events: &[PendingEvent],
        expected_version: u64,
        command_id: &str,
        fencing_token: Option<u64>,
    ) -> Result<(), StoreError>;
}
```

```sql
-- 每个聚合记录最后写入的 fencing_token
CREATE TABLE aggregate_fencing (
    aggregate_id    VARCHAR(255) PRIMARY KEY,
    fencing_token   BIGINT NOT NULL DEFAULT 0
);
```

写入逻辑（在同一事务中）：

```sql
-- 检查并更新 fencing token（原子操作）
-- 使用严格小于（<）：同一 token 的重复写入也会被拒绝，
-- 防止网络重传场景下绕过 version conflict 检查
INSERT INTO aggregate_fencing (aggregate_id, fencing_token)
VALUES ($1, $2)
ON CONFLICT (aggregate_id) DO UPDATE
SET fencing_token = $2
WHERE aggregate_fencing.fencing_token < $2;

-- 如果 rows_affected = 0，说明 token 过期或重复，拒绝写入
```

**rows_affected 语义说明**（PostgreSQL `ON CONFLICT DO UPDATE ... WHERE` 行为）：

| 场景 | rows_affected | 含义 |
|------|---------------|------|
| 新聚合（无冲突，INSERT 成功） | 1 | 首次写入，新行创建 |
| 已有聚合，新 token 更大（WHERE 满足） | 1 | UPDATE 成功，token 更新 |
| 已有聚合，token 过期或重复（WHERE 不满足） | 0 | UPDATE 被 WHERE 拒绝，写入被阻止 |

因此 `rows_affected = 0` 精确表示"token 过期或重复"，安全拒绝写入。
不会与"新聚合首次写入"混淆（首次写入走 INSERT 路径，返回 1）。

Actor 处理命令时的检查：

```rust
impl VirtualActor {
    async fn process_command(&mut self, cmd: IncomingCommand) -> Result<CommandResult> {
        // 写入前检查 lease 是否仍然有效
        if !self.lease_guard.is_valid() {
            self.poison_and_deactivate().await;
            return Err(Error::lease_expired(
                "Lease 续约失败，Actor 已失效，请重试"
            ));
        }

        // ... validate, handle ...

        // 持久化时携带 fencing_token（集群模式 Some，单机模式 None）
        let persist_result = self.event_store.append_with_idempotency(
            &self.aggregate_id,
            &events_to_persist,
            self.version,
            &cmd.command_id,
            Some(self.lease_guard.fencing_token()),
        ).await;

        // Fencing token 被拒绝：说明 lease 已被其他节点接管，立即自我 poison + 休眠
        if let Err(StoreError::FencingTokenExpired { .. }) = &persist_result {
            self.poison_and_deactivate().await;
        }
        persist_result?;

        // ...
    }

    /// 标记自身为 poisoned 并触发休眠，从 active 表中移除
    /// 后续请求将触发重新激活（获取新 lease）
    async fn poison_and_deactivate(&mut self) {
        self.poisoned = true;
        // 不保存快照：状态可能已过期（其他节点可能已写入新事件）
        self.handle.notify_exit().await;
    }
}
```

**Fencing 失败恢复流程**：

```
Actor 写入 Event Store 被 fencing_token 拒绝
    │
    ├── 1. 标记 self.poisoned = true
    │
    ├── 2. 不保存快照（状态可能已过期）
    │
    ├── 3. 通知 Runtime 自身已退出（notify_exit）
    │
    ├── 4. 返回错误给客户端（"请重试"）
    │
    └── 5. Actor 从 active 表移除，后续请求触发重新激活
           - 重新激活时获取新 lease（如果本节点仍是 owner）
           - 或路由到新 owner 节点（哈希环已更新）
```

**保证**：即使旧节点在 lease 过期后仍尝试写入，Event Store 会因 fencing_token 过期而拒绝，
新节点的写入不会被覆盖。旧 Actor 立即自我 poison 并退出，不会持续占用 active 槽位。
客户端收到错误后重试即可路由到正确节点。

### Lease 与 Actor 生命周期绑定

```
激活 Actor:
  1. acquire lease → 成功
  2. 从快照+事件恢复状态
  3. 开始处理命令
  4. 后台定期续约 lease

休眠 Actor:
  1. 保存快照
  2. release lease
  3. 释放内存

Lease 过期（节点故障）:
  1. 其他节点检测到 lease 释放
  2. 新节点 acquire lease
  3. 从快照+事件恢复（可能重放少量已持久化但未快照的事件）
```

## 请求路由

### validate 执行位置约定

**validate-X 在接收请求的节点执行一次，转发到 owner 节点时不再重复执行。**

原因：validate 是无状态的纯格式校验（不依赖聚合状态），在任何节点执行结果相同。
重复执行只会浪费 WASM 实例池资源。

**滚动升级安全**：转发时携带源节点的组件版本哈希。owner 节点收到请求后检查版本是否一致，
不一致时重新执行 validate（防止滚动升级期间新旧版本校验逻辑差异导致的问题）。

实现：转发请求时携带 `validated: true` 和 `module_version` 标记。

> **IncomingCommand 权威定义**见 [command-flow.md](./command-flow.md#incomingcommand-结构统一定义)。
> 集群模式扩展字段：

```rust
// 集群模式扩展（追加到 IncomingCommand）
pub module_version: Option<String>,  // 源节点的组件版本哈希（仅集群模式转发时填充）
```

### 方案：客户端侧路由（推荐）

每个节点都持有完整的一致性哈希环，收到请求后判断目标节点：

```rust
impl ClusterGateway {
    pub async fn route_command(&self, mut command: IncomingCommand) -> Result<CommandResult> {
        let target_node = self.placement.locate(&command.aggregate_id);

        if target_node == self.local_node_id {
            // 本地处理（走完整 Gateway 流程）
            // 如果是转发来的请求，检查版本一致性
            if command.validated {
                if let Some(ref source_version) = command.module_version {
                    let local_version = self.module_versions.get(&command.module);
                    if local_version.as_deref() != Some(source_version.as_str()) {
                        // 版本不一致（滚动升级中），重新执行 validate
                        command.validated = false;
                    }
                }
            }
            self.local_gateway.execute(command).await
        } else {
            // 本地先执行 validate（如果有且尚未执行）
            if !command.validated {
                let cmd_def = self.command_registry.get(&command.module, &command.command_type);
                if let Some(validate_fn) = cmd_def.and_then(|c| c.validate_fn.as_ref()) {
                    let mut instance = self.wasm_pool.acquire(&command.module).await?;
                    instance.call_validate(validate_fn, &command.data)?;
                    drop(instance);
                }
                command.validated = true;
                command.module_version = self.module_versions.get(&command.module).cloned();
            }
            // 转发到目标节点（gRPC），owner 节点版本一致时跳过 validate
            self.forward_to(target_node, command).await
        }
    }
}
```

### 组件版本哈希

启动时或热更新后，对每个 WASM 组件计算内容哈希作为版本标识：

```rust
pub struct ClusterGateway {
    // ...
    /// 模块名 → 组件内容 SHA-256 前 16 字节 hex
    module_versions: HashMap<String, String>,
}

impl ClusterGateway {
    fn compute_module_version(wasm_bytes: &[u8]) -> String {
        use sha2::{Sha256, Digest};
        let hash = Sha256::digest(wasm_bytes);
        hex::encode(&hash[..16])
    }
}
```

### 路由表更新

节点通过 etcd watch 监听成员变更，实时更新本地哈希环：

```rust
impl ClusterMembership {
    pub async fn watch_members(&self) {
        let mut stream = self.lease_store.watch("cluster/members/").await.unwrap();
        while let Some(event) = stream.next().await {
            match event {
                MemberEvent::Join(node) => self.placement.add_node(node),
                MemberEvent::Leave(node) => {
                    self.placement.remove_node(node);
                    // 触发受影响聚合的重新分配
                    self.rebalance_affected(node).await;
                }
            }
        }
    }
}
```

## 节点故障与 Actor 迁移

### 故障检测

```
Node B 故障
    │
    ├── etcd lease 过期（TTL 30s）
    │
    ├── 其他节点收到 watch 通知
    │
    ├── 一致性哈希环移除 Node B
    │
    └── Node B 上的聚合重新映射到存活节点
        （下次访问时按需激活，无需主动迁移）
```

### 迁移策略：惰性迁移（推荐）

不主动迁移 Actor，而是在下次请求到达时按需激活：

| 策略 | 优势 | 劣势 |
|------|------|------|
| 惰性迁移 | 简单、无额外开销 | 首次请求有激活延迟 |
| 主动迁移 | 无冷启动延迟 | 复杂、可能迁移不需要的 Actor |

惰性迁移适合本系统：快照恢复延迟 < 50ms，对用户几乎无感知。

## 分片与集群的关系

```
Event Store 分片（数据层）：按 aggregate_id hash 分布到 PG 实例
Actor 分布（计算层）：按 aggregate_id hash 分布到 Host 节点

两者独立：
- 一个 Host 节点可能持有映射到不同 PG 分片的 Actor
- 一个 PG 分片可能服务来自不同 Host 节点的写入
```

## 配置

```rust
pub struct ClusterConfig {
    pub node_id: NodeId,
    pub etcd_endpoints: Vec<String>,
    pub lease_ttl: Duration,           // 默认 30s
    pub lease_renew_interval: Duration, // 默认 10s
    pub virtual_nodes: usize,          // 默认 150
    pub grpc_port: u16,                // 节点间通信端口
    pub forward_timeout: Duration,     // 转发超时（默认 5s）
}
```

## 节点发现与 Bootstrap

### 启动流程

新节点加入集群时的完整 bootstrap 流程：

```
节点启动
    │
    ├── 1. 连接 etcd（使用配置的 etcd_endpoints）
    │
    ├── 2. 注册自身（写入成员信息 + 创建会话 lease）
    │      key: cluster/members/{node_id}
    │      value: { addr, grpc_port, started_at, status: "joining" }
    │
    ├── 3. 获取当前成员列表（读取 cluster/members/ 前缀下所有 key）
    │
    ├── 4. 构建一致性哈希环（基于所有活跃成员）
    │
    ├── 5. 标记自身状态为 "active"
    │
    └── 6. 开始 watch 成员变更 + 接收请求
```

### 成员注册

```rust
impl ClusterMembership {
    /// 节点启动时注册自身
    pub async fn bootstrap(&self) -> Result<()> {
        // 1. 创建会话 lease（节点故障时自动注销）
        let session_lease = self.lease_store
            .create_session(self.config.lease_ttl)
            .await?;

        // 2. 注册成员信息（绑定到会话 lease，故障自动清除）
        let member_key = format!("cluster/members/{}", self.config.node_id);
        let member_info = MemberInfo {
            node_id: self.config.node_id.clone(),
            addr: self.config.advertise_addr.clone(),
            grpc_port: self.config.grpc_port,
            started_at: now_millis(),
        };
        self.lease_store
            .put_with_lease(&member_key, &member_info, &session_lease)
            .await?;

        // 3. 加载现有成员，构建哈希环
        let members = self.lease_store
            .get_prefix("cluster/members/")
            .await?;
        self.placement.rebalance(&members);

        tracing::info!(
            "节点 {} 加入集群，当前成员数: {}",
            self.config.node_id, members.len()
        );

        // 4. 启动成员变更监听
        tokio::spawn(self.watch_members());

        Ok(())
    }
}
```

### 首次部署（无现有集群）

首个节点启动时 `cluster/members/` 为空，哈希环仅包含自身，所有请求本地处理。
后续节点加入后，哈希环自动更新，新请求根据更新后的哈希环路由。
已有 Actor 在下次访问时惰性迁移到正确节点。

## 集群限流协调

### 问题

`multi-command-wit.md` 设计了命令级限流，但各节点的 `CommandRateLimiter` 是本地独立的。
在 N 个节点的集群中，如果每个节点配置 `create-item: 100/s`，实际集群总限流为 `N × 100/s`。

### 方案对比

| 方案 | 精确度 | 延迟开销 | 复杂度 | 适用场景 |
|------|--------|----------|--------|----------|
| 本地限流 ÷ 节点数 | 近似（节点数变化时抖动） | 0 | 低 | 大多数场景 |
| 集中式限流（Redis） | 精确 | +1-2ms | 中 | 严格限流要求 |
| 令牌桶广播（gossip） | 最终一致 | ~100ms 收敛 | 高 | 大规模集群 |

### 推荐方案：本地限流 ÷ 节点数（默认）

```rust
impl CommandRateLimiter {
    /// 根据当前集群节点数动态调整本地限流阈值
    pub fn adjust_for_cluster(&mut self, node_count: usize) {
        for limiter in self.limiters.values_mut() {
            limiter.set_rate(limiter.base_rate() / node_count as f64);
        }
    }
}
```

节点加入/离开时通过 membership watch 回调触发 `adjust_for_cluster`。

局限性：
- 负载不均衡时，部分节点可能先达到限流阈值而其他节点仍有余量
- 节点数变化瞬间存在短暂抖动

对于需要严格精确限流的命令，可按需为特定命令启用 Redis 集中式限流：

```rust
pub enum RateLimitStrategy {
    Local,                    // 本地限流 ÷ 节点数（默认，零延迟）
    Centralized(RedisPool),   // Redis 集中式（精确，+1-2ms）
}
```
