// WASM 引擎（组件加载和调用）

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use wasmtime::component::{Component, ComponentExportIndex, Linker, Val};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};
use wit_parser::decoding::{decode, DecodedWasm};
use wit_parser::WorldItem;

use crate::command::{discover_commands, CommandDef, ExportedFunctionInfo};
use crate::config::RuntimeConfig;
use crate::error::WorkerError;

/// WASI 宿主状态
struct WasiState {
    wasi: WasiCtx,
    table: wasmtime::component::ResourceTable,
}

impl WasiView for WasiState {
    fn table(&mut self) -> &mut wasmtime::component::ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

/// 模块元信息
struct ModuleInfo {
    component: Component,
    linker: Linker<WasiState>,
    commands: Vec<CommandDef>,
    /// 接口导出索引（用于查找嵌套在接口下的函数）
    interface_export: ComponentExportIndex,
    source_path: PathBuf,
}

/// WASM 引擎：加载聚合组件并提供 handle/validate/apply-events 调用
pub struct WasmEngine {
    engine: Engine,
    modules: HashMap<String, ModuleInfo>,
    fuel_limit: u64,
}

impl WasmEngine {
    /// 创建新的 WASM 引擎（配置 fuel 限制）
    pub fn new(config: &RuntimeConfig) -> Self {
        let mut wasm_config = Config::new();
        wasm_config.wasm_component_model(true);
        wasm_config.consume_fuel(true);
        let engine = Engine::new(&wasm_config).expect("创建 Wasmtime Engine 失败");
        Self {
            engine,
            modules: HashMap::new(),
            fuel_limit: config.wasm_fuel_limit,
        }
    }

    /// 加载 WASM 组件并提取命令定义
    pub fn load_module(&mut self, wasm_path: &Path) -> Result<(String, Vec<CommandDef>), WorkerError> {
        let wasm_bytes = std::fs::read(wasm_path)
            .map_err(|e| WorkerError::WasmExecution(format!("读取文件失败: {e}")))?;

        // 从 WIT 元数据提取函数列表用于命令发现
        let (wit_funcs, _module_name) = extract_wit_functions(&wasm_bytes)
            .map_err(|e| WorkerError::WasmExecution(format!("WIT 解析失败: {e}")))?;

        // 命令发现
        let func_infos: Vec<ExportedFunctionInfo> = wit_funcs
            .iter()
            .map(|(name, _, _)| ExportedFunctionInfo { wit_name: name.clone() })
            .collect();
        let commands = discover_commands(&func_infos)
            .map_err(|e| WorkerError::WasmExecution(format!("命令发现失败: {e}")))?;

        // 编译组件
        let component = Component::new(&self.engine, &wasm_bytes)
            .map_err(|e| WorkerError::WasmExecution(format!("组件编译失败: {e}")))?;

        let mut linker = Linker::new(&self.engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)
            .map_err(|e| WorkerError::WasmExecution(format!("WASI 链接失败: {e}")))?;

        // 查找接口导出索引（用于后续函数查找）
        let interface_export = find_interface_export(&component, &self.engine)?;

        // 从 WIT 包名提取模块名（如 "test:counter" -> "counter"）
        let module_name = extract_module_name(&wasm_bytes)
            .map_err(|e| WorkerError::WasmExecution(format!("提取模块名失败: {e}")))?;

        self.modules.insert(module_name.clone(), ModuleInfo {
            component,
            linker,
            commands: commands.clone(),
            interface_export,
            source_path: wasm_path.to_path_buf(),
        });

        Ok((module_name, commands))
    }

    /// 获取模块的命令定义
    pub fn get_commands(&self, module: &str) -> Option<&[CommandDef]> {
        self.modules.get(module).map(|m| m.commands.as_slice())
    }

    /// 获取模块名列表
    pub fn module_names(&self) -> Vec<String> {
        self.modules.keys().cloned().collect()
    }

    /// 检查模块是否有指定 validate 函数，返回函数名
    pub fn get_validate_fn(&self, module: &str, command_type: &str) -> Option<String> {
        self.modules.get(module)
            .and_then(|m| m.commands.iter().find(|c| c.name == command_type))
            .and_then(|c| c.validate_fn.clone())
    }

    /// 创建新 Store 实例
    fn new_store(&self) -> Store<WasiState> {
        let wasi = WasiCtxBuilder::new().build();
        Store::new(&self.engine, WasiState {
            wasi,
            table: wasmtime::component::ResourceTable::new(),
        })
    }

    /// 调用 validate-X 函数（可选，用于前置校验）
    pub fn call_validate(
        &self,
        module: &str,
        fn_name: &str,
        command: &[u8],
    ) -> Result<(), WorkerError> {
        let info = self.modules.get(module)
            .ok_or_else(|| WorkerError::ModuleNotFound(module.into()))?;

        let mut store = self.new_store();
        store.set_fuel(self.fuel_limit)
            .map_err(|e| WorkerError::WasmExecution(format!("fuel 设置失败: {e}")))?;

        let instance = info.linker.instantiate(&mut store, &info.component)
            .map_err(|e| WorkerError::WasmExecution(format!("实例化失败: {e}")))?;

        let func_export = instance
            .get_export(&mut store, Some(&info.interface_export), fn_name)
            .ok_or_else(|| WorkerError::CommandNotFound {
                module: module.into(),
                command: fn_name.into(),
            })?;
        let func = instance.get_func(&mut store, &func_export)
            .ok_or_else(|| WorkerError::WasmExecution("获取函数失败".into()))?;

        let params = vec![bytes_to_val(command)];

        let mut results = vec![Val::Bool(false); func.results(&store).len()];
        func.call(&mut store, &params, &mut results)
            .map_err(|e| WorkerError::WasmExecution(format!("validate 调用失败: {e}")))?;
        func.post_return(&mut store)
            .map_err(|e| WorkerError::WasmExecution(format!("post_return 失败: {e}")))?;

        // validate 返回 result<_, string>
        match &results[0] {
            Val::Result(Ok(_)) => Ok(()),
            Val::Result(Err(Some(err))) => {
                let err_msg = val_to_string(err).unwrap_or_default();
                Err(WorkerError::WasmExecution(format!("validate 拒绝: {err_msg}")))
            }
            Val::Result(Err(None)) => {
                Err(WorkerError::WasmExecution("validate 拒绝（无详情）".into()))
            }
            other => Err(WorkerError::WasmExecution(
                format!("validate 返回了意外的结果类型: {other:?}")
            )),
        }
    }

    /// 调用 handle-X 函数，返回新事件列表
    pub fn call_handle(
        &self,
        module: &str,
        fn_name: &str,
        state: &[u8],
        command: &[u8],
    ) -> Result<Vec<Vec<u8>>, WorkerError> {
        let info = self.modules.get(module)
            .ok_or_else(|| WorkerError::ModuleNotFound(module.into()))?;

        let mut store = self.new_store();
        store.set_fuel(self.fuel_limit)
            .map_err(|e| WorkerError::WasmExecution(format!("fuel 设置失败: {e}")))?;

        let instance = info.linker.instantiate(&mut store, &info.component)
            .map_err(|e| WorkerError::WasmExecution(format!("实例化失败: {e}")))?;

        let func_export = instance
            .get_export(&mut store, Some(&info.interface_export), fn_name)
            .ok_or_else(|| WorkerError::CommandNotFound {
                module: module.into(),
                command: fn_name.into(),
            })?;
        let func = instance.get_func(&mut store, &func_export)
            .ok_or_else(|| WorkerError::WasmExecution("获取函数失败".into()))?;

        let params = vec![bytes_to_val(state), bytes_to_val(command)];
        let mut results = vec![Val::Bool(false); func.results(&store).len()];
        func.call(&mut store, &params, &mut results)
            .map_err(|e| WorkerError::WasmExecution(format!("handle 调用失败: {e}")))?;
        func.post_return(&mut store)
            .map_err(|e| WorkerError::WasmExecution(format!("post_return 失败: {e}")))?;

        // 解析 result<list<list<u8>>, string>
        parse_events_result(&results[0])
    }

    /// 调用 apply-events 函数，用于事件回放重建状态
    pub fn call_apply_events(
        &self,
        module: &str,
        snapshot: &[u8],
        events: &[&[u8]],
    ) -> Result<Vec<u8>, WorkerError> {
        let info = self.modules.get(module)
            .ok_or_else(|| WorkerError::ModuleNotFound(module.into()))?;

        let mut store = self.new_store();
        store.set_fuel(self.fuel_limit)
            .map_err(|e| WorkerError::WasmExecution(format!("fuel 设置失败: {e}")))?;

        let instance = info.linker.instantiate(&mut store, &info.component)
            .map_err(|e| WorkerError::WasmExecution(format!("实例化失败: {e}")))?;

        let func_export = instance
            .get_export(&mut store, Some(&info.interface_export), "apply-events")
            .ok_or_else(|| WorkerError::WasmExecution(
                format!("模块 {} 缺少 apply-events 函数", module)
            ))?;
        let func = instance.get_func(&mut store, &func_export)
            .ok_or_else(|| WorkerError::WasmExecution("获取函数失败".into()))?;

        // 构建 list<list<u8>>
        let events_list: Vec<Val> = events.iter().map(|e| bytes_to_val(e)).collect();
        let params = vec![bytes_to_val(snapshot), Val::List(events_list)];

        let mut results = vec![Val::Bool(false); func.results(&store).len()];
        func.call(&mut store, &params, &mut results)
            .map_err(|e| WorkerError::WasmExecution(format!("apply-events 调用失败: {e}")))?;
        func.post_return(&mut store)
            .map_err(|e| WorkerError::WasmExecution(format!("post_return 失败: {e}")))?;

        // 解析 result<list<u8>, string>
        parse_state_result(&results[0])
    }
}

/// 查找组件的接口导出索引（用于函数查找）
fn find_interface_export(component: &Component, engine: &Engine) -> Result<ComponentExportIndex, WorkerError> {
    use wasmtime::component::types::ComponentItem;

    let comp_type = component.component_type();
    for (iface_name, item) in comp_type.exports(engine) {
        if let ComponentItem::ComponentInstance(_) = item {
            return component.export_index(None, iface_name)
                .map(|(_, idx)| idx)
                .ok_or_else(|| WorkerError::WasmExecution("接口导出索引未找到".into()));
        }
    }
    Err(WorkerError::WasmExecution("未找到接口导出".into()))
}

// WasmEngineApi trait 桥接实现（转发到具体方法）
impl crate::actor::WasmEngineApi for WasmEngine {
    fn call_validate(&self, module: &str, fn_name: &str, command: &[u8]) -> Result<(), WorkerError> {
        WasmEngine::call_validate(self, module, fn_name, command)
    }

    fn call_handle(&self, module: &str, fn_name: &str, state: &[u8], command: &[u8]) -> Result<Vec<Vec<u8>>, WorkerError> {
        WasmEngine::call_handle(self, module, fn_name, state, command)
    }

    fn call_apply_events(&self, module: &str, snapshot: &[u8], events: &[&[u8]]) -> Result<Vec<u8>, WorkerError> {
        WasmEngine::call_apply_events(self, module, snapshot, events)
    }

    fn get_validate_fn(&self, module: &str, command_type: &str) -> Option<String> {
        WasmEngine::get_validate_fn(self, module, command_type)
    }
}

/// 将 Vec<u8> 转为 Val::List(Val::U8)
fn bytes_to_val(bytes: &[u8]) -> Val {
    Val::List(bytes.iter().map(|&b| Val::U8(b)).collect())
}

/// 将 Val (list<u8>) 转为 Vec<u8>
fn val_to_bytes(val: &Val) -> Option<Vec<u8>> {
    match val {
        Val::List(items) => {
            let bytes: Option<Vec<u8>> = items.iter().map(|v| {
                match v {
                    Val::U8(b) => Some(*b),
                    _ => None,
                }
            }).collect();
            bytes
        }
        _ => None,
    }
}

/// 将 Val (string) 转为 String
fn val_to_string(val: &Val) -> Option<String> {
    match val {
        Val::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// 解析 result<list<list<u8>>, string>
fn parse_events_result(val: &Val) -> Result<Vec<Vec<u8>>, WorkerError> {
    match val {
        Val::Result(Ok(Some(payload))) => {
            let payload_ref: &Val = payload;
            match payload_ref {
                Val::List(events) => {
                    let mut result = Vec::new();
                    for event_val in events {
                        if let Some(bytes) = val_to_bytes(event_val) {
                            result.push(bytes);
                        } else {
                            return Err(WorkerError::WasmExecution("事件格式错误".into()));
                        }
                    }
                    Ok(result)
                }
                other => Err(WorkerError::WasmExecution(
                    format!("handle 返回了非 list 类型: {other:?}")
                )),
            }
        }
        Val::Result(Ok(None)) => {
            // result 成功但没有值（空列表场景）
            Ok(Vec::new())
        }
        Val::Result(Err(Some(err))) => {
            let msg = val_to_string(err).unwrap_or_default();
            Err(WorkerError::WasmExecution(msg))
        }
        Val::Result(Err(None)) => {
            Err(WorkerError::WasmExecution("handle 返回错误（无详情）".into()))
        }
        other => Err(WorkerError::WasmExecution(
            format!("handle 返回了意外的结果: {other:?}")
        )),
    }
}

/// 解析 result<list<u8>, string>
fn parse_state_result(val: &Val) -> Result<Vec<u8>, WorkerError> {
    match val {
        Val::Result(Ok(Some(payload))) => {
            val_to_bytes(payload)
                .ok_or_else(|| WorkerError::WasmExecution("apply-events 返回非 bytes".into()))
        }
        Val::Result(Ok(None)) => {
            // result 成功但没有值 —— 返回空状态
            Ok(Vec::new())
        }
        Val::Result(Err(Some(err))) => {
            let msg = val_to_string(err).unwrap_or_default();
            Err(WorkerError::WasmExecution(msg))
        }
        Val::Result(Err(None)) => {
            Err(WorkerError::WasmExecution("apply-events 返回错误（无详情）".into()))
        }
        other => Err(WorkerError::WasmExecution(
            format!("apply-events 返回了意外的结果: {other:?}")
        )),
    }
}

/// 从 WIT 元数据提取函数名和参数/返回类型
fn extract_wit_functions(
    wasm_bytes: &[u8],
) -> Result<(Vec<(String, Vec<(String, String)>, String)>, String)> {
    let decoded = decode(wasm_bytes)
        .map_err(|e| anyhow!("WIT 解码失败: {e}"))?;

    let (resolve, world_id) = match decoded {
        DecodedWasm::Component(resolve, world_id) => (resolve, world_id),
        DecodedWasm::WitPackage(..) => {
            return Err(anyhow!("期望组件，得到 WIT 包"));
        }
    };

    let world = &resolve.worlds[world_id];
    let mut module_name = String::from("unknown");
    let mut funcs = Vec::new();

    for (_key, item) in &world.exports {
        match item {
            WorldItem::Interface { id, .. } => {
                let iface = &resolve.interfaces[*id];
                if let Some(pkg_id) = iface.package {
                    let pkg = &resolve.packages[pkg_id];
                    module_name = pkg.name.name.clone();
                }
                for (func_name, func) in &iface.functions {
                    let params: Vec<(String, String)> = func.params.iter()
                        .map(|(name, ty)| (name.clone(), format!("{ty:?}")))
                        .collect();
                    let result_type = format!("{:?}", func.results);
                    funcs.push((func_name.clone(), params, result_type));
                }
            }
            WorldItem::Function(func) => {
                let params: Vec<(String, String)> = func.params.iter()
                    .map(|(name, ty)| (name.clone(), format!("{ty:?}")))
                    .collect();
                let result_type = format!("{:?}", func.results);
                funcs.push((func.name.clone(), params, result_type));
            }
            WorldItem::Type(_) => {}
        }
    }

    Ok((funcs, module_name))
}

/// 从 WASM 组件的 WIT 包名中提取模块名（取冒号后的第二部分）
fn extract_module_name(wasm_bytes: &[u8]) -> Result<String> {
    let decoded = decode(wasm_bytes)
        .map_err(|e| anyhow!("WIT 解码失败: {e}"))?;

    let (resolve, world_id) = match decoded {
        DecodedWasm::Component(resolve, world_id) => (resolve, world_id),
        DecodedWasm::WitPackage(..) => return Err(anyhow!("期望组件，得到 WIT 包")),
    };

    let world = &resolve.worlds[world_id];

    // 优先从导出接口的包名获取
    for (_key, item) in &world.exports {
        if let WorldItem::Interface { id, .. } = item {
            let iface = &resolve.interfaces[*id];
            if let Some(pkg_id) = iface.package {
                let pkg = &resolve.packages[pkg_id];
                let full_name = &pkg.name.name;
                // 取冒号后的第二部分（如 "test:counter" -> "counter"）
                return Ok(full_name.split(':').nth(1).unwrap_or(full_name).to_string());
            }
        }
    }

    // 降级：从 world 包名获取
    if let Some(pkg_id) = world.package {
        let pkg = &resolve.packages[pkg_id];
        let full_name = &pkg.name.name;
        return Ok(full_name.split(':').nth(1).unwrap_or(full_name).to_string());
    }

    Ok("unknown".to_string())
}

// ===== 测试 =====

#[cfg(test)]
mod tests {
    use super::*;

    fn test_wasm_path() -> std::path::PathBuf {
        let wasm_dir = env!("DEFAULT_WASM_DIR");
        std::path::PathBuf::from(format!("{}/wasm_counter.wasm", wasm_dir))
    }

    #[test]
    fn test_load_counter_component() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let result = engine.load_module(&test_wasm_path());
        assert!(result.is_ok(), "加载计数器 WASM 失败: {:?}", result.err());
        let (module_name, commands) = result.unwrap();
        assert_eq!(module_name, "counter");
        let cmd_names: Vec<&str> = commands.iter().map(|c| c.name.as_str()).collect();
        assert!(cmd_names.contains(&"increment"), "应该有 increment 命令");
    }

    #[test]
    fn test_call_handle_increment() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let (module_name, _) = engine.load_module(&test_wasm_path()).unwrap();

        // 初始状态为空
        let state = vec![];
        let command = br#"{"amount":5}"#.to_vec();
        let events = engine.call_handle(&module_name, "handle-increment", &state, &command).unwrap();

        assert!(!events.is_empty(), "应产生至少一个事件");
        // 验证事件 JSON 格式
        let event: serde_json::Value = serde_json::from_slice(&events[0]).unwrap();
        assert_eq!(event["type"], "Incremented");
        assert_eq!(event["amount"], 5);
    }

    #[test]
    fn test_validate_increment_valid() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let (module_name, _) = engine.load_module(&test_wasm_path()).unwrap();

        let command = br#"{"amount":10}"#.to_vec();
        let result = engine.call_validate(&module_name, "validate-increment", &command);
        assert!(result.is_ok(), "amount=10 应通过校验");
    }

    #[test]
    fn test_validate_increment_rejects_zero() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let (module_name, _) = engine.load_module(&test_wasm_path()).unwrap();

        let command = br#"{"amount":0}"#.to_vec();
        let result = engine.call_validate(&module_name, "validate-increment", &command);
        assert!(result.is_err(), "amount=0 应被拒绝");
        let err = result.unwrap_err();
        assert!(err.to_string().contains("validate 拒绝"));
    }

    #[test]
    fn test_apply_events_reconstructs_state() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let (module_name, _) = engine.load_module(&test_wasm_path()).unwrap();

        let event1 = br#"{"type":"Incremented","amount":3}"#.to_vec();
        let event2 = br#"{"type":"Incremented","amount":7}"#.to_vec();
        let snapshot: Vec<u8> = vec![];
        let events: Vec<&[u8]> = vec![&event1, &event2];
        let new_state = engine.call_apply_events(&module_name, &snapshot, &events).unwrap();

        // 状态应为 {"count":10}
        let state: serde_json::Value = serde_json::from_slice(&new_state).unwrap();
        assert_eq!(state["count"], 10);
    }

    #[test]
    fn test_apply_events_empty_list() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let (module_name, _) = engine.load_module(&test_wasm_path()).unwrap();

        let snapshot = br#"{"count":42}"#.to_vec();
        let events: Vec<&[u8]> = vec![];
        let new_state = engine.call_apply_events(&module_name, &snapshot, &events).unwrap();

        let state: serde_json::Value = serde_json::from_slice(&new_state).unwrap();
        assert_eq!(state["count"], 42);
    }

    #[test]
    fn test_command_not_found() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let (module_name, _) = engine.load_module(&test_wasm_path()).unwrap();

        let result = engine.call_handle(&module_name, "handle-nonexistent", &[], &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_module_not_found() {
        let config = RuntimeConfig::default();
        let engine = WasmEngine::new(&config);

        let result = engine.get_commands("no-such-module");
        assert!(result.is_none());
    }

    #[test]
    fn test_module_names() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        engine.load_module(&test_wasm_path()).unwrap();
        let names = engine.module_names();
        assert!(names.contains(&"counter".to_string()));
    }

    #[test]
    fn test_get_validate_fn_present() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        engine.load_module(&test_wasm_path()).unwrap();

        let fn_name = WasmEngine::get_validate_fn(&engine, "counter", "increment");
        assert!(fn_name.is_some());
        assert_eq!(fn_name.unwrap(), "validate-increment");
    }

    #[test]
    fn test_get_commands_returns_correct() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        engine.load_module(&test_wasm_path()).unwrap();

        let cmds = engine.get_commands("counter");
        assert!(cmds.is_some());
        let cmds = cmds.unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].name, "increment");
        assert_eq!(cmds[0].graphql_name, "increment");
        assert!(cmds[0].handle_fn.contains("handle"));
    }

    #[test]
    fn test_call_validate_missing_module() {
        let config = RuntimeConfig::default();
        let engine = WasmEngine::new(&config);
        let result = engine.call_validate("no-module", "validate-increment", &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("模块未找到"));
    }

    #[test]
    fn test_call_handle_missing_module() {
        let config = RuntimeConfig::default();
        let engine = WasmEngine::new(&config);
        let result = engine.call_handle("no-module", "handle-increment", &[], &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("模块未找到"));
    }

    #[test]
    fn test_load_module_invalid_path() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let result = engine.load_module(&std::path::PathBuf::from("/nonexistent/file.wasm"));
        assert!(result.is_err());
    }

    #[test]
    fn test_get_validate_fn_not_present() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        engine.load_module(&test_wasm_path()).unwrap();

        let fn_name = WasmEngine::get_validate_fn(&engine, "counter", "no-such-command");
        assert!(fn_name.is_none());
    }

    #[test]
    fn test_get_validate_fn_missing_module() {
        let config = RuntimeConfig::default();
        let engine = WasmEngine::new(&config);
        let fn_name = engine.get_validate_fn("no-module", "increment");
        assert!(fn_name.is_none());
    }

    #[test]
    fn test_module_names_empty() {
        let config = RuntimeConfig::default();
        let engine = WasmEngine::new(&config);
        assert!(engine.module_names().is_empty());
    }

    #[test]
    fn test_module_names_after_load() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        engine.load_module(&test_wasm_path()).unwrap();
        let names = engine.module_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "counter");
    }

    #[test]
    fn test_create_new_store_works() {
        let config = RuntimeConfig::default();
        let engine = WasmEngine::new(&config);
        // new_store() 是私有方法，通过调用 load + handle 间接测试
    }

    #[test]
    fn test_bytes_to_val_roundtrip() {
        let data: Vec<u8> = vec![1, 2, 3, 4, 5];
        let val = super::bytes_to_val(&data);
        let result = super::val_to_bytes(&val);
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_bytes_to_val_empty() {
        let val = super::bytes_to_val(&[]);
        match val {
            Val::List(items) => assert!(items.is_empty()),
            _ => panic!("应该是空列表"),
        }
    }

    #[test]
    fn test_val_to_bytes_non_list() {
        let result = super::val_to_bytes(&Val::Bool(false));
        assert!(result.is_none());
    }

    #[test]
    fn test_val_to_bytes_mixed_types() {
        // list 中包含非 U8 元素的处理
        let val = Val::List(vec![Val::U8(1), Val::Bool(false), Val::U8(3)]);
        let result = super::val_to_bytes(&val);
        assert!(result.is_none(), "含有非 U8 元素应返回 None");
    }

    #[test]
    fn test_val_to_string_valid() {
        let result = super::val_to_string(&Val::String("hello".into()));
        assert_eq!(result, Some("hello".into()));
    }

    #[test]
    fn test_val_to_string_non_string() {
        let result = super::val_to_string(&Val::Bool(false));
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_events_result_ok_empty() {
        let val = Val::Result(Ok(None)); // 空 success
        let result = super::parse_events_result(&val);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_events_result_err_with_message() {
        let val = Val::Result(Err(Some(Box::new(Val::String("业务错误".into())))));
        let result = super::parse_events_result(&val);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("业务错误"));
    }

    #[test]
    fn test_parse_events_result_err_no_message() {
        let val = Val::Result(Err(None));
        let result = super::parse_events_result(&val);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("无详情"));
    }

    #[test]
    fn test_parse_events_result_unexpected_type() {
        let val = Val::Bool(true); // 不是 result 类型
        let result = super::parse_events_result(&val);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("意外"));
    }

    #[test]
    fn test_parse_state_result_ok_empty() {
        let val = Val::Result(Ok(None));
        let result = super::parse_state_result(&val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn test_parse_state_result_err() {
        let val = Val::Result(Err(Some(Box::new(Val::String("apply错误".into())))));
        let result = super::parse_state_result(&val);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("apply错误"));
    }

    #[test]
    fn test_parse_state_result_unexpected_type() {
        let val = Val::U32(42);
        let result = super::parse_state_result(&val);
        assert!(result.is_err());
    }

    #[test]
    fn test_call_handle_increment_with_call_apply_events() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let (module_name, _) = engine.load_module(&test_wasm_path()).unwrap();

        // 完整的 handle → apply 流程
        let state = vec![];
        let cmd = br#"{"amount":10}"#.to_vec();
        let events = engine.call_handle(&module_name, "handle-increment", &state, &cmd).unwrap();
        assert!(!events.is_empty());

        let event_refs: Vec<&[u8]> = events.iter().map(|e| e.as_slice()).collect();
        let new_state = engine.call_apply_events(&module_name, &state, &event_refs).unwrap();
        let s: serde_json::Value = serde_json::from_slice(&new_state).unwrap();
        assert_eq!(s["count"], 10);
    }

    #[test]
    fn test_extract_wit_from_counter_wasm() {
        let wasm_path = test_wasm_path();
        let bytes = std::fs::read(&wasm_path).unwrap();
        let (funcs, module_name) = super::extract_wit_functions(&bytes).unwrap();
        assert_eq!(module_name, "counter");
        assert!(!funcs.is_empty());
        // 应该包含 handle-increment, validate-increment, apply-events
        let names: Vec<&str> = funcs.iter().map(|(n, _, _)| n.as_str()).collect();
        assert!(names.iter().any(|n| n.contains("handle")));
        assert!(names.iter().any(|n| n.contains("validate")));
        assert!(names.iter().any(|n| n.contains("apply")));
    }

    #[test]
    fn test_extract_module_name_from_counter() {
        let wasm_path = test_wasm_path();
        let bytes = std::fs::read(&wasm_path).unwrap();
        let name = super::extract_module_name(&bytes).unwrap();
        assert_eq!(name, "counter");
    }

    #[test]
    fn test_call_handle_multiple_commands() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        let (module_name, _) = engine.load_module(&test_wasm_path()).unwrap();

        // 验证命令名正确
        let cmds = engine.get_commands(&module_name).unwrap();
        assert!(!cmds.is_empty());

        // 对每个命令验证 handle-X 函数名格式
        for cmd in cmds {
            assert!(cmd.handle_fn.starts_with("handle-"));
            assert_eq!(cmd.graphql_name, crate::graphql::kebab_to_camel(&cmd.name));
        }
    }

    #[test]
    fn test_engine_ignores_non_component() {
        let config = RuntimeConfig::default();
        let mut engine = WasmEngine::new(&config);
        // 尝试加载非组件文件
        let invalid_path = std::env::temp_dir().join("not_a_component.wasm");
        std::fs::write(&invalid_path, b"this is not a valid wasm component").ok();
        if invalid_path.exists() {
            let result = engine.load_module(&invalid_path);
            assert!(result.is_err());
            let _ = std::fs::remove_file(&invalid_path);
        }
    }
}
