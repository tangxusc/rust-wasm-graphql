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

        // 预热：提前实例化所有实例
        for _ in 0..pool_size {
            let instance = Self::create_instance(&engine, &component)?;
            pool.push(instance).unwrap();
        }

        Ok(Self {
            module_name: module_name.to_string(),
            engine,
            component,
            pool,
            pool_size,
        })
    }

    /// 从池中获取实例（无锁）
    pub fn acquire(&self) -> Result<PooledInstance> {
        match self.pool.pop() {
            Some(instance) => Ok(PooledInstance {
                instance: Some(instance),
                pool: self.pool.clone(),
            }),
            None => {
                // 池耗尽：创建临时实例（不归还池）
                let instance = Self::create_instance(&self.engine, &self.component)?;
                Ok(PooledInstance {
                    instance: Some(instance),
                    pool: self.pool.clone(),
                })
            }
        }
    }

    fn create_instance(
        engine: &wasmtime::Engine,
        component: &wasmtime::component::Component,
    ) -> Result<WasmInstance> {
        let mut store = wasmtime::Store::new(engine, WasiState::new());
        let linker = wasmtime::component::Linker::new(engine);
        let instance = linker.instantiate(&mut store, component)?;
        Ok(WasmInstance { store, instance })
    }
}

/// RAII 守卫：Drop 时自动归还实例到池
pub struct PooledInstance {
    instance: Option<WasmInstance>,
    pool: Arc<ArrayQueue<WasmInstance>>,
}

impl PooledInstance {
    pub fn call_validate(&mut self, command: &[u8]) -> Result<(), String> {
        let inst = self.instance.as_mut().unwrap();
        // 调用 WASM validate 函数
        call_wasm_func(&mut inst.store, &inst.instance, "validate", command)
    }

    pub fn call_handle(&mut self, state: &[u8], command: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let inst = self.instance.as_mut().unwrap();
        call_wasm_func_handle(&mut inst.store, &inst.instance, "handle", state, command)
    }

    pub fn call_apply_events(&mut self, snapshot: &[u8], events: &[&[u8]]) -> Result<Vec<u8>> {
        let inst = self.instance.as_mut().unwrap();
        call_wasm_func_apply(&mut inst.store, &inst.instance, "apply-events", snapshot, events)
    }

    /// 重置实例状态（清理 WASI 资源），使其可安全复用
    fn reset(&mut self) {
        if let Some(inst) = self.instance.as_mut() {
            inst.store.data_mut().reset();
        }
    }
}

impl Drop for PooledInstance {
    fn drop(&mut self) {
        if let Some(mut instance) = self.instance.take() {
            instance.store.data_mut().reset();
            // 尝试归还池，池满则丢弃
            let _ = self.pool.push(instance);
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

    pub fn acquire(&self, module_name: &str) -> Result<PooledInstance> {
        self.pools
            .get(module_name)
            .ok_or(Error::module_not_found(module_name))?
            .acquire()
    }

    fn create_optimized_engine() -> wasmtime::Engine {
        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true);
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);
        config.parallel_compilation(true);
        // 启用实例复用优化
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
| acquire 延迟 | N/A | ~50ns（无锁 pop） |
| 内存占用 | 波动大 | 稳定（池大小 × 实例内存） |
| GC 压力 | 高（频繁分配释放） | 无 |
| 单线程 TPS 上限 | ~5,000 | ~200,000 |

## 实例安全性

WASM 组件模型保证实例间完全隔离：
- 每个实例有独立的线性内存
- 无共享可变状态
- reset() 清理 WASI 资源（文件描述符、环境变量等）

因此实例池复用是安全的，不会产生状态泄漏。

## 与 Virtual Actor 的集成

```
Virtual Actor 收到命令（已在内存中激活）
    │
    ├── pool.acquire("inventory")  ← 无锁，~50ns
    │
    ├── instance.call_validate(cmd)
    │
    ├── instance.call_handle(state, cmd)
    │
    └── drop(instance)  ← 自动归还池
```

每个 Virtual Actor 是单线程的，但多个 Virtual Actor 可以并行从同一个池获取实例。
`ArrayQueue` 是 lock-free 的，多 Actor 并发 acquire/release 无竞争。

### Virtual Actor 激活时的实例使用

```
Virtual Actor 激活（从快照+事件恢复）
    │
    ├── pool.acquire("inventory")
    │
    ├── instance.call_apply_events(snapshot, events)  ← 批量重建状态
    │
    └── drop(instance)  ← 归还池，后续命令再按需获取
```

激活是低频操作（仅首次访问或休眠后重新访问），不会对实例池造成持续压力。
