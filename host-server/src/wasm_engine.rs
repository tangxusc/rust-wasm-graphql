use anyhow::{anyhow, Result};
use wasmtime::component::{Component, ComponentExportIndex, Linker, ResourceTable, Val};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};
use wit_parser::decoding::{decode, DecodedWasm};
use wit_parser::{Results, Type as WitAstType, WorldItem};

#[derive(Debug, Clone)]
pub enum WitType {
    S8,
    S16,
    S32,
    S64,
    U8,
    U16,
    U32,
    U64,
    Float32,
    Float64,
    Bool,
    String,
}

#[derive(Debug, Clone)]
pub struct ParamDescriptor {
    pub wit_name: String,
    pub graphql_name: String,
    pub ty: WitType,
}

#[derive(Debug, Clone)]
pub struct FunctionDescriptor {
    pub wit_name: String,
    pub graphql_name: String,
    pub params: Vec<ParamDescriptor>,
    pub result_type: WitType,
}

struct State {
    wasi: WasiCtx,
    table: ResourceTable,
}

impl WasiView for State {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

pub struct WasmEngine {
    engine: Engine,
    component: Component,
    linker: Linker<State>,
    descriptors: Vec<FunctionDescriptor>,
    interface_export: ComponentExportIndex,
}

impl WasmEngine {
    pub fn new(wasm_path: &str) -> Result<Self> {
        let wasm_bytes = std::fs::read(wasm_path)
            .map_err(|e| anyhow!("failed to read wasm file '{}': {}", wasm_path, e))?;

        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config)?;
        let component = Component::new(&engine, &wasm_bytes)?;
        let mut linker = Linker::new(&engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)?;

        let wit_funcs = extract_wit_functions(&wasm_bytes)?;
        let (descriptors, interface_export) =
            Self::introspect(&component, &engine, &wit_funcs)?;

        Ok(Self { engine, component, linker, descriptors, interface_export })
    }

    pub fn descriptors(&self) -> &[FunctionDescriptor] {
        &self.descriptors
    }

    fn introspect(
        component: &Component,
        engine: &Engine,
        wit_funcs: &[(String, Vec<(String, WitType)>, WitType)],
    ) -> Result<(Vec<FunctionDescriptor>, ComponentExportIndex)> {
        use wasmtime::component::types::ComponentItem;

        let comp_type = component.component_type();
        let mut interface_export_idx = None;
        let mut descriptors = Vec::new();

        for (iface_name, item) in comp_type.exports(engine) {
            if let ComponentItem::ComponentInstance(instance) = item {
                let (_, idx) = component.export_index(None, iface_name)
                    .ok_or_else(|| anyhow!("interface export index not found"))?;
                interface_export_idx = Some(idx);

                for (func_name, func_item) in instance.exports(engine) {
                    if let ComponentItem::ComponentFunc(_) = func_item {
                        let wit_func = wit_funcs.iter()
                            .find(|(name, _, _)| name == func_name);

                        let params = if let Some((_, wit_params, _)) = wit_func {
                            wit_params.iter().map(|(pname, pty)| {
                                ParamDescriptor {
                                    wit_name: pname.clone(),
                                    graphql_name: kebab_to_camel(pname),
                                    ty: pty.clone(),
                                }
                            }).collect()
                        } else {
                            Vec::new()
                        };

                        let result_type = wit_func
                            .map(|(_, _, rt)| rt.clone())
                            .unwrap_or(WitType::String);

                        descriptors.push(FunctionDescriptor {
                            wit_name: func_name.to_string(),
                            graphql_name: kebab_to_camel(func_name),
                            params,
                            result_type,
                        });
                    }
                }
                break;
            }
        }

        let interface_export = interface_export_idx
            .ok_or_else(|| anyhow!("no exported interface found in component"))?;

        Ok((descriptors, interface_export))
    }

    fn new_store(&self) -> Store<State> {
        let wasi = WasiCtxBuilder::new().build();
        Store::new(&self.engine, State { wasi, table: ResourceTable::new() })
    }

    pub fn call_function(&self, wit_name: &str, args: &[Val]) -> Result<Val> {
        let mut store = self.new_store();
        let instance = self.linker.instantiate(&mut store, &self.component)?;

        let func_export = instance
            .get_export(&mut store, Some(&self.interface_export), wit_name)
            .ok_or_else(|| anyhow!("function '{}' not found", wit_name))?;
        let func = instance.get_func(&mut store, &func_export)
            .ok_or_else(|| anyhow!("'{}' is not a function", wit_name))?;

        let result_count = func.results(&store).len();
        let mut results = vec![Val::Bool(false); result_count];
        func.call(&mut store, args, &mut results)?;
        func.post_return(&mut store)?;

        results.into_iter().next()
            .ok_or_else(|| anyhow!("function '{}' returned no results", wit_name))
    }
}

fn kebab_to_camel(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;
    for ch in s.chars() {
        if ch == '-' {
            capitalize_next = true;
        } else if capitalize_next {
            result.extend(ch.to_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}

fn wit_ast_type_to_wit_type(ty: &WitAstType) -> WitType {
    match ty {
        WitAstType::S8 => WitType::S8,
        WitAstType::S16 => WitType::S16,
        WitAstType::S32 => WitType::S32,
        WitAstType::S64 => WitType::S64,
        WitAstType::U8 => WitType::U8,
        WitAstType::U16 => WitType::U16,
        WitAstType::U32 => WitType::U32,
        WitAstType::U64 => WitType::U64,
        WitAstType::F32 => WitType::Float32,
        WitAstType::F64 => WitType::Float64,
        WitAstType::Bool => WitType::Bool,
        WitAstType::String => WitType::String,
        _ => WitType::String,
    }
}

fn extract_wit_functions(
    wasm_bytes: &[u8],
) -> Result<Vec<(String, Vec<(String, WitType)>, WitType)>> {
    let decoded = decode(wasm_bytes)
        .map_err(|e| anyhow!("failed to decode WIT from component: {}", e))?;

    let (resolve, world_id) = match decoded {
        DecodedWasm::Component(resolve, world_id) => (resolve, world_id),
        DecodedWasm::WitPackage(..) => {
            return Err(anyhow!("expected a component, got a WIT package"));
        }
    };

    let world = &resolve.worlds[world_id];
    let mut funcs = Vec::new();

    for (_key, item) in &world.exports {
        match item {
            WorldItem::Interface { id, .. } => {
                let iface = &resolve.interfaces[*id];
                for (func_name, func) in &iface.functions {
                    let params: Vec<(String, WitType)> = func.params.iter()
                        .map(|(name, ty)| (name.clone(), wit_ast_type_to_wit_type(ty)))
                        .collect();

                    let result_type = match &func.results {
                        Results::Anon(ty) => wit_ast_type_to_wit_type(ty),
                        Results::Named(named) => named.first()
                            .map(|(_, ty)| wit_ast_type_to_wit_type(ty))
                            .unwrap_or(WitType::String),
                    };

                    funcs.push((func_name.clone(), params, result_type));
                }
            }
            WorldItem::Function(func) => {
                let params: Vec<(String, WitType)> = func.params.iter()
                    .map(|(name, ty)| (name.clone(), wit_ast_type_to_wit_type(ty)))
                    .collect();

                let result_type = match &func.results {
                    Results::Anon(ty) => wit_ast_type_to_wit_type(ty),
                    Results::Named(named) => named.first()
                        .map(|(_, ty)| wit_ast_type_to_wit_type(ty))
                        .unwrap_or(WitType::String),
                };

                funcs.push((func.name.clone(), params, result_type));
            }
            WorldItem::Type(_) => {}
        }
    }

    Ok(funcs)
}
