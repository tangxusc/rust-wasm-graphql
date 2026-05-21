use std::sync::Arc;

use anyhow::Result;
use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, Schema, TypeRef,
};
use wasmtime::component::Val;

use crate::wasm_engine::{WasmEngine, WitType};

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

pub fn build_dynamic_schema(engine: Arc<WasmEngine>) -> Result<Schema> {
    let mut query = Object::new("Query");

    for desc in engine.descriptors() {
        let engine_clone = engine.clone();
        let wit_name = desc.wit_name.clone();
        let params_meta: Vec<(String, WitType)> = desc.params.iter()
            .map(|p| (p.graphql_name.clone(), p.ty.clone()))
            .collect();

        let mut field = Field::new(
            &desc.graphql_name,
            wit_type_to_typeref(&desc.result_type),
            move |ctx| {
                let engine = engine_clone.clone();
                let wit_name = wit_name.clone();
                let params_meta = params_meta.clone();

                FieldFuture::new(async move {
                    let mut vals = Vec::with_capacity(params_meta.len());
                    for (gql_name, wit_ty) in &params_meta {
                        let accessor = ctx.args.try_get(gql_name)?;
                        vals.push(accessor_to_val(&accessor, wit_ty)?);
                    }

                    let result = tokio::task::spawn_blocking(move || {
                        engine.call_function(&wit_name, &vals)
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

        query = query.field(field);
    }

    Schema::build("Query", None, None)
        .register(query)
        .finish()
        .map_err(|e| anyhow::anyhow!("{}", e))
}
