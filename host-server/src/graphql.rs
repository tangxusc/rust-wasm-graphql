use std::sync::Arc;

use anyhow::Result;
use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, Schema, TypeRef,
};
use wasmtime::component::Val;

use crate::wasm_engine::WitType;
use crate::wasm_registry::WasmRegistry;

fn wit_type_to_typeref(ty: &WitType) -> TypeRef {
    match ty {
        WitType::S8 | WitType::S16 | WitType::S32
        | WitType::U8 | WitType::U16 | WitType::U32
        | WitType::Bool => TypeRef::named_nn(TypeRef::INT),
        WitType::S64 | WitType::U64 => TypeRef::named_nn(TypeRef::STRING),
        WitType::Float32 | WitType::Float64 => TypeRef::named_nn(TypeRef::FLOAT),
        WitType::String => TypeRef::named_nn(TypeRef::STRING),
    }
}

fn accessor_to_val(
    accessor: &async_graphql::dynamic::ValueAccessor<'_>,
    ty: &WitType,
) -> async_graphql::Result<Val> {
    match ty {
        WitType::S8 => Ok(Val::S8(accessor.i64()? as i8)),
        WitType::S16 => Ok(Val::S16(accessor.i64()? as i16)),
        WitType::S32 => Ok(Val::S32(accessor.i64()? as i32)),
        WitType::S64 => Ok(Val::S64(accessor.i64()?)),
        WitType::U8 => Ok(Val::U8(accessor.u64()? as u8)),
        WitType::U16 => Ok(Val::U16(accessor.u64()? as u16)),
        WitType::U32 => Ok(Val::U32(accessor.u64()? as u32)),
        WitType::U64 => Ok(Val::U64(accessor.u64()?)),
        WitType::Float32 => Ok(Val::Float32(accessor.f32()?)),
        WitType::Float64 => Ok(Val::Float64(accessor.f64()?)),
        WitType::Bool => Ok(Val::Bool(accessor.boolean()?)),
        WitType::String => Ok(Val::String(accessor.string()?.to_string())),
    }
}

fn val_to_field_value(val: Val) -> FieldValue<'static> {
    match val {
        Val::S8(n) => FieldValue::value(async_graphql::Value::from(n as i64)),
        Val::S16(n) => FieldValue::value(async_graphql::Value::from(n as i64)),
        Val::S32(n) => FieldValue::value(async_graphql::Value::from(n as i64)),
        Val::S64(n) => FieldValue::value(async_graphql::Value::from(n.to_string())),
        Val::U8(n) => FieldValue::value(async_graphql::Value::from(n as i64)),
        Val::U16(n) => FieldValue::value(async_graphql::Value::from(n as i64)),
        Val::U32(n) => FieldValue::value(async_graphql::Value::from(n as i64)),
        Val::U64(n) => FieldValue::value(async_graphql::Value::from(n.to_string())),
        Val::Float32(n) => FieldValue::value(async_graphql::Value::from(n as f64)),
        Val::Float64(n) => FieldValue::value(async_graphql::Value::from(n)),
        Val::Bool(b) => FieldValue::value(async_graphql::Value::from(b)),
        Val::String(s) => FieldValue::value(async_graphql::Value::from(s)),
        _ => FieldValue::value(async_graphql::Value::Null),
    }
}

fn to_pascal_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = true;
    for ch in s.chars() {
        if ch == '-' || ch == '_' {
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

pub fn build_dynamic_schema(registry: Arc<WasmRegistry>) -> Result<Schema> {
    let mut query = Object::new("Query");
    let mut extra_types: Vec<Object> = Vec::new();

    for (module_idx, module) in registry.modules().iter().enumerate() {
        let type_name = to_pascal_case(&module.module_name);
        let mut module_obj = Object::new(&type_name);

        for desc in module.engine.descriptors() {
            let registry_clone = registry.clone();
            let wit_name = desc.wit_name.clone();
            let mi = module_idx;
            let params_meta: Vec<(String, WitType)> = desc.params.iter()
                .map(|p| (p.graphql_name.clone(), p.ty.clone()))
                .collect();

            let mut field = Field::new(
                &desc.graphql_name,
                wit_type_to_typeref(&desc.result_type),
                move |ctx| {
                    let registry = registry_clone.clone();
                    let wit_name = wit_name.clone();
                    let params_meta = params_meta.clone();

                    FieldFuture::new(async move {
                        let mut vals = Vec::with_capacity(params_meta.len());
                        for (gql_name, wit_ty) in &params_meta {
                            let accessor = ctx.args.try_get(gql_name)?;
                            vals.push(accessor_to_val(&accessor, wit_ty)?);
                        }

                        let result = tokio::task::spawn_blocking(move || {
                            registry.modules()[mi].engine.call_function(&wit_name, &vals)
                        })
                        .await
                        .map_err(|e| async_graphql::Error::new(e.to_string()))?
                        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

                        Ok(Some(val_to_field_value(result)))
                    })
                },
            );

            for param in &desc.params {
                field = field.argument(InputValue::new(
                    &param.graphql_name,
                    wit_type_to_typeref(&param.ty),
                ));
            }

            module_obj = module_obj.field(field);
        }

        let field_name = &module.module_name;
        let module_type_ref = TypeRef::named_nn(&type_name);
        let module_field = Field::new(field_name, module_type_ref, |_ctx| {
            FieldFuture::new(async { Ok(Some(FieldValue::NULL)) })
        });
        query = query.field(module_field);
        extra_types.push(module_obj);
    }

    let mut builder = Schema::build("Query", None, None).register(query);
    for obj in extra_types {
        builder = builder.register(obj);
    }
    builder
        .enable_federation()
        .finish()
        .map_err(|e| anyhow::anyhow!("{}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pascal_case_kebab() {
        assert_eq!(to_pascal_case("hello-world"), "HelloWorld");
    }

    #[test]
    fn test_to_pascal_case_snake() {
        assert_eq!(to_pascal_case("hello_world"), "HelloWorld");
    }

    #[test]
    fn test_to_pascal_case_single_word() {
        assert_eq!(to_pascal_case("calculator"), "Calculator");
    }

    #[test]
    fn test_to_pascal_case_empty() {
        assert_eq!(to_pascal_case(""), "");
    }

    #[test]
    fn test_to_pascal_case_mixed() {
        assert_eq!(to_pascal_case("string-utils"), "StringUtils");
    }

    #[test]
    fn test_wit_type_to_typeref_integers() {
        let int_ref = TypeRef::named_nn(TypeRef::INT);
        assert_eq!(wit_type_to_typeref(&WitType::S8), int_ref);
        assert_eq!(wit_type_to_typeref(&WitType::S16), int_ref);
        assert_eq!(wit_type_to_typeref(&WitType::S32), int_ref);
        assert_eq!(wit_type_to_typeref(&WitType::U8), int_ref);
        assert_eq!(wit_type_to_typeref(&WitType::U16), int_ref);
        assert_eq!(wit_type_to_typeref(&WitType::U32), int_ref);
        assert_eq!(wit_type_to_typeref(&WitType::Bool), int_ref);
    }

    #[test]
    fn test_wit_type_to_typeref_large_ints() {
        let str_ref = TypeRef::named_nn(TypeRef::STRING);
        assert_eq!(wit_type_to_typeref(&WitType::S64), str_ref);
        assert_eq!(wit_type_to_typeref(&WitType::U64), str_ref);
    }
}
