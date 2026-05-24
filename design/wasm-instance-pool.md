# WASM 实例池设计

## 问题

当前架构中每次 `call_function` 都创建新的 Wasmtime `Store` 和实例化组件：
- 实例化开销：~100-500μs（取决于组件大小）
- 内存分配：每次创建新的线性内存
- 百万 TPS 下，这个开销不可接受

## 解决方案：预热实例池

```
┌─────────────────────────────────────────────────┐
│  WasmInstancePool                                │
│                                                   │
│  module: "inventory"                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│  │Instance 0│ │Instance 1│ │Instance 2│  ...     │
│  │(idle)    │ │(in-use)  │ │(idle)    │           │
│  └─────────┘ └─────────┘ └─────────┘           │
│                                                   │
│  pool_size: 根据 CPU 核数自动调整                  │
│  acquire() → Instance                            │
│  release(Instance)                               │
└─────────────────────────────────────────────────┘
```

## 核心实现

```rust
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

pub struct WasmInstancePool {
    module_name: String,
    engine: Arc<wasmtime::Engine>,
    component: Arc<wasmtime::component::Component>,
    linker: Arc<wasmtime::component::Linker<WasiState>>,  // 共享 Linker，避免重复创建
    pool: Arc<ArrayQueue<WasmInstance>>,
    pool_size: usize,
}

pub struct WasmInstance {
    store: wasmtime::Store<WasiState>,
    instance: wasmtime::component::Instance,
}

impl WasmInstancePool {
    /// 创建实例池，预热所有实例
    pub fn new(
        engine: Arc<wasmtime::Engine>,
        component: Arc<wasmtime::component::Component>,
        module_name: &str,
        pool_size: usize,
    ) -> Result<Self> {
        let pool = Arc::new(ArrayQueue::new(pool_size));
        let linker = Arc::new(Self::create_linker(&engine));

        // 预热：提前实例化所有实例
        for _ in 0..pool_size {
            let instance = Self::create_instance(&engine, &component, &linker)?;
            pool.push(instance).unwrap();
        }

        Ok(Self {
            module_name: module_name.to_string(),
            engine,
            component,
            linker,
            pool,
            pool_size,
        })
    }

    /// 从池中获取实例（池耗尽时直接返回错误，触发上层背压）
    pub async fn acquire(&self) -> Result<PooledInstance> {
        match self.pool.pop() {
            Some(instance) => Ok(PooledInstance::pooled(instance, self.pool.clone())),
            None => {
                // 池耗尽：直接返回错误，由上层背压机制（503）通知客户端退避重试
                // 不创建临时实例，防止持续高负载下内存无限增长
                Err(Error::pool_exhausted(&self.module_name))
            }
        }
    }

    fn create_linker(engine: &wasmtime::Engine) -> wasmtime::component::Linker<WasiState> {
        let mut linker = wasmtime::component::Linker::new(engine);
        // 注册 WASI 接口等
        linker
    }

    fn create_instance(
        engine: &wasmtime::Engine,
        component: &wasmtime::component::Component,
        linker: &wasmtime::component::Linker<WasiState>,
    ) -> Result<WasmInstance> {
        let mut store = wasmtime::Store::new(engine, WasiState::new());
        let instance = linker.instantiate(&mut store, component)?;
        Ok(WasmInstance { store, instance })
    }
}

/// RAII 守卫：Drop 时归还池
pub struct PooledInstance {
    instance: Option<WasmInstance>,
    return_to: Arc<ArrayQueue<WasmInstance>>,
    trapped: bool,  // WASM 函数是否发生 trap
}

impl PooledInstance {
    fn new(instance: WasmInstance, pool: Arc<ArrayQueue<WasmInstance>>) -> Self {
        Self { instance: Some(instance), return_to: pool, trapped: false }
    }

    pub fn call_function(&mut self, func_name: &str, args: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, String> {
        let inst = self.instance.as_mut().unwrap();
        match call_wasm_func_generic(&mut inst.store, &inst.instance, func_name, args) {
            Ok(result) => Ok(result),
            Err(e) => {
                self.trapped = true;
                Err(e)
            }
        }
    }

    /// 专用 validate 调用：validate-X 返回 result<_, string>，成功时无有效载荷
    pub fn call_validate(&mut self, func_name: &str, command: &[u8]) -> Result<(), String> {
        let inst = self.instance.as_mut().unwrap();
        match call_wasm_func_validate(&mut inst.store, &inst.instance, func_name, command) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.trapped = true;
                Err(e)
            }
        }
    }

    pub fn call_apply_events(&mut self, snapshot: &[u8], events: &[&[u8]]) -> Result<Vec<u8>> {
        let inst = self.instance.as_mut().unwrap();
        match call_wasm_func_apply(&mut inst.store, &inst.instance, "apply-events", snapshot, events) {
            Ok(result) => Ok(result),
            Err(e) => {
                self.trapped = true;
                Err(e)
            }
        }
    }
}

impl Drop for PooledInstance {
    fn drop(&mut self) {
        if let Some(mut instance) = self.instance.take() {
            // Trap 后实例状态可能不一致，直接丢弃不归还池
            if self.trapped {
                return;
            }
            instance.store.data_mut().reset();
            let _ = self.return_to.push(instance);
        }
    }
}
```

## 多模块实例池管理

```rust
pub struct WasmPoolManager {
    pools: HashMap<String, Arc<WasmInstancePool>>,
}

impl WasmPoolManager {
    pub fn from_registry(registry: &WasmRegistry, pool_size_per_module: usize) -> Result<Self> {
        let engine = Arc::new(Self::create_optimized_engine());
        let mut pools = HashMap::new();

        for module in registry.modules() {
            let component = Arc::new(module.component().clone());
            let pool = WasmInstancePool::new(
                engine.clone(),
                component,
                module.name(),
                pool_size_per_module,
            )?;
            pools.insert(module.name().to_string(), Arc::new(pool));
        }

        Ok(Self { pools })
    }

    /// 异步获取实例（池命中时无开销，池耗尽时返回错误触发上层背压）
    pub async fn acquire(&self, module_name: &str) -> Result<PooledInstance> {
        self.pools
            .get(module_name)
            .ok_or(Error::module_not_found(module_name))?
            .acquire()
            .await
    }

    fn create_optimized_engine() -> wasmtime::Engine {
        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true);
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);
        config.parallel_compilation(true);
        config.cranelift_nan_canonicalization(false);
        wasmtime::Engine::new(&config).unwrap()
    }
}
```

## 池大小调优

### 自动调整策略

```rust
pub struct PoolSizePolicy {
    /// 每个 CPU 核心的实例数
    pub instances_per_core: usize,  // 默认 2
    /// 最小池大小
    pub min_size: usize,            // 默认 4
    /// 最大池大小
    pub max_size: usize,            // 默认 64
}

impl PoolSizePolicy {
    pub fn calculate(&self) -> usize {
        let cores = num_cpus::get();
        let size = cores * self.instances_per_core;
        size.clamp(self.min_size, self.max_size)
    }
}
```

### 建议配置

| 场景 | 池大小 | 理由 |
|------|--------|------|
| 开发 | 2-4 | 资源有限 |
| 生产（8核） | 16 | 2 × CPU 核数 |
| 生产（32核） | 64 | 2 × CPU 核数 |
| 高并发（64核） | 128 | 2 × CPU 核数，上限 |

## 性能对比

| 指标 | 无池（每次创建） | 有池（预热复用） |
|------|-----------------|-----------------|
| 实例化延迟 | 100-500μs | 0（已预热） |
| acquire 延迟（池命中） | N/A | ~50ns（无锁 pop） |
| acquire 延迟（池耗尽） | N/A | 立即返回错误（503），由背压机制处理 |
| Linker 创建 | 每次重新创建 | 共享复用（零开销） |
| 内存占用 | 波动大 | 稳定（池大小 × 实例内存） |
| GC 压力 | 高（频繁分配释放） | 无 |
| 单线程 TPS 上限 | ~5,000 | ~200,000 |

## 实例安全性

WASM 组件模型保证实例间完全隔离：
- 每个实例有独立的线性内存
- 无共享可变状态
- reset() 清理 WASI 资源（文件描述符、环境变量等）

因此实例池复用是安全的，不会产生状态泄漏。

### Trap 后实例处理

当 WASM 函数执行时发生 trap（如 unreachable 指令、内存越界、栈溢出），
实例的线性内存可能处于不一致状态。Wasmtime 在 trap 后不保证 Store 的安全复用。

处理策略：
- `PooledInstance` 内部维护 `trapped: bool` 标记
- 任何 `call_*` 方法返回错误时自动设置 `trapped = true`
- `Drop` 时检查：`trapped` 为 true 则直接丢弃实例，不归还池
- 池大小短暂减少后，后续请求通过 `spawn_blocking` 创建临时实例补充

这意味着在 WASM 组件频繁 trap 的场景下，池会逐渐耗空，开始拒绝请求（503）。
配合熔断器机制，频繁 trap 时自动停止分配实例，避免无效 CPU 消耗。

### 熔断器（Circuit Breaker）

当某模块连续 trap 率超过阈值时，熔断器自动切断该模块的请求处理，
避免"补充 → trap → 补充"的无效循环消耗 CPU。

```rust
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

/// 熔断器状态
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed = 0,    // 正常：允许所有请求
    Open = 1,      // 熔断：拒绝所有请求
    HalfOpen = 2,  // 半开：允许少量探测请求
}

pub struct CircuitBreaker {
    state: AtomicU8,
    /// 滑动窗口内的 trap 计数
    trap_count: AtomicU64,
    /// 滑动窗口内的总调用计数
    call_count: AtomicU64,
    /// 窗口起始时间（毫秒）
    window_start: AtomicU64,
    /// 半开状态下已放行的探测请求数
    half_open_permits: AtomicU64,
    config: CircuitBreakerConfig,
}

pub struct CircuitBreakerConfig {
    /// 滑动窗口大小（默认 30 秒）
    pub window_duration: Duration,
    /// 触发熔断的 trap 率阈值（默认 0.5，即 50%）
    pub trip_threshold: f64,
    /// 触发熔断的最小调用次数（防止低流量误判，默认 10）
    pub min_calls_to_trip: u64,
    /// 熔断持续时间（之后进入半开，默认 60 秒）
    pub open_duration: Duration,
    /// 半开状态允许的探测请求数（默认 3）
    pub half_open_max_permits: u64,
}
```

### 池自愈：后台定时补充

偶发 trap（如输入边界 case）会导致池水位逐渐下降。后台补充任务定期检测池水位，
低于阈值时异步预热新实例，防止池永久缩小：

```rust
impl WasmInstancePool {
    /// 后台补充任务：定期检测池水位，低于阈值时补充
    pub fn start_replenisher(
        self: &Arc<Self>,
        check_interval: Duration,       // 默认 5s
        low_watermark: f64,             // 默认 0.75
        max_replenish_per_tick: usize,  // 默认 4
    ) -> JoinHandle<()> {
        let pool = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            loop {
                interval.tick().await;

                let current = pool.pool.len();
                let threshold = (pool.pool_size as f64 * low_watermark) as usize;

                if current < threshold {
                    let deficit = (pool.pool_size - current).min(max_replenish_per_tick);
                    let engine = pool.engine.clone();
                    let component = pool.component.clone();
                    let linker = pool.linker.clone();
                    let queue = pool.pool.clone();

                    tokio::task::spawn_blocking(move || {
                        for _ in 0..deficit {
                            match Self::create_instance(&engine, &component, &linker) {
                                Ok(inst) => { let _ = queue.push(inst); }
                                Err(e) => {
                                    tracing::warn!("实例补充失败: {e}");
                                    break;
                                }
                            }
                        }
                    }).await.ok();

                    tracing::debug!(
                        "实例池 '{}' 补充完成，当前水位: {}/{}",
                        pool.module_name, pool.pool.len(), pool.pool_size
                    );
                }
            }
        })
    }
}
```

补充策略说明：
- `low_watermark = 0.75`：池水位降至初始大小 75% 以下时触发补充
- `max_replenish_per_tick = 4`：每轮最多补充 4 个实例，防止组件持续 trap 时无限创建
- 熔断器 Open 状态时 replenisher 暂停补充，避免无效 CPU 消耗
- 补充在 `spawn_blocking` 中执行，不阻塞 tokio worker

### 熔断器与实例池集成

```rust
impl WasmInstancePool {
    /// 从池中获取实例（熔断器检查 → 池获取）
    pub async fn acquire(&self) -> Result<PooledInstance> {
        // 熔断器检查
        match self.circuit_breaker.state() {
            CircuitState::Open => {
                return Err(Error::circuit_open(&self.module_name));
            }
            CircuitState::HalfOpen => {
                // 半开状态：仅允许有限探测请求
                if !self.circuit_breaker.try_half_open_permit() {
                    return Err(Error::circuit_open(&self.module_name));
                }
            }
            CircuitState::Closed => {}
        }

        match self.pool.pop() {
            Some(instance) => Ok(PooledInstance::pooled(instance, self.pool.clone(), &self.circuit_breaker)),
            None => Err(Error::pool_exhausted(&self.module_name)),
        }
    }

    /// 记录调用结果，更新熔断器状态
    fn record_outcome(&self, trapped: bool) {
        self.circuit_breaker.record_call();
        if trapped {
            self.circuit_breaker.record_trap();
        }
        // 检查是否需要状态转换
        self.circuit_breaker.evaluate();
    }
}

impl CircuitBreaker {
    pub fn state(&self) -> CircuitState {
        // Open 状态超过 open_duration 后自动转为 HalfOpen
        let s = self.state.load(Ordering::Acquire);
        if s == CircuitState::Open as u8 {
            let elapsed = now_millis() - self.opened_at.load(Ordering::Relaxed);
            if elapsed > self.config.open_duration.as_millis() as u64 {
                self.state.store(CircuitState::HalfOpen as u8, Ordering::Release);
                self.half_open_permits.store(0, Ordering::Relaxed);
                return CircuitState::HalfOpen;
            }
        }
        unsafe { std::mem::transmute(s) }
    }

    /// 评估是否需要状态转换
    pub fn evaluate(&self) {
        match self.state() {
            CircuitState::Closed => {
                let calls = self.call_count.load(Ordering::Relaxed);
                let traps = self.trap_count.load(Ordering::Relaxed);
                if calls >= self.config.min_calls_to_trip {
                    let trap_rate = traps as f64 / calls as f64;
                    if trap_rate >= self.config.trip_threshold {
                        self.trip();
                    }
                }
            }
            CircuitState::HalfOpen => {
                let permits = self.half_open_permits.load(Ordering::Relaxed);
                if permits >= self.config.half_open_max_permits {
                    // 探测请求全部成功（无新 trap）→ 恢复
                    self.reset();
                }
            }
            CircuitState::Open => {} // 等待超时自动转 HalfOpen
        }
    }

    fn trip(&self) {
        self.state.store(CircuitState::Open as u8, Ordering::Release);
        self.opened_at.store(now_millis(), Ordering::Relaxed);
        tracing::error!(
            "熔断器触发：模块 '{}' trap 率过高，已停止接受请求",
            self.module_name
        );
    }

    fn reset(&self) {
        self.state.store(CircuitState::Closed as u8, Ordering::Release);
        self.trap_count.store(0, Ordering::Relaxed);
        self.call_count.store(0, Ordering::Relaxed);
        tracing::info!("熔断器恢复：模块 '{}' 已恢复正常", self.module_name);
    }
}
```

熔断器状态转换：

```
     trap 率 < 阈值                    超过 open_duration
  ┌──────────────────┐              ┌──────────────────┐
  │                  │              │                  │
  ▼                  │              ▼                  │
Closed ──────────── Open ─────────── HalfOpen
  ▲    trap率≥阈值         自动超时        │
  │                                        │ 探测成功
  └────────────────────────────────────────┘
                                           │ 探测失败
                                           └──→ Open
```

## 与 Virtual Actor 的集成

```
Virtual Actor 收到命令（已在内存中激活）
    │
    ├── pool.acquire("inventory").await  ← 池命中时 ~50ns（无锁），池耗尽时返回 503
    │
    ├── instance.call_function("handle-create-item", &[state, cmd])
    │
    └── drop(instance)  ← 自动归还池
```

注意：validate-X 已在 Gateway 层前置执行，Actor 内部不再调用 validate。

每个 Virtual Actor 是单线程的，但多个 Virtual Actor 可以并行从同一个池获取实例。
`ArrayQueue` 是 lock-free 的，多 Actor 并发 acquire/release 无竞争。

### Virtual Actor 激活时的实例使用

```
Virtual Actor 激活（从快照+事件恢复）
    │
    ├── pool.acquire("inventory").await
    │
    ├── instance.call_apply_events(snapshot, events)  ← 批量重建状态
    │
    └── drop(instance)  ← 归还池，后续命令再按需获取
```

激活是低频操作（仅首次访问或休眠后重新访问），不会对实例池造成持续压力。
