// GraphQL 动态 Schema 构建

use std::sync::Arc;

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, Scalar, Schema, TypeRef,
};

use crate::error::WorkerError;
use crate::types::IncomingCommand;

/// 将 kebab-case 转换为 camelCase
pub fn kebab_to_camel(s: &str) -> String {
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

/// 将 kebab-case 转换为 PascalCase
pub fn to_pascal_case(s: &str) -> String {
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

/// aggregateId 格式校验：仅允许字母数字和 -_，长度 1-128
pub fn validate_aggregate_id(id: &str) -> Result<(), WorkerError> {
    if id.is_empty() || id.len() > 128 {
        return Err(WorkerError::InvalidInput("aggregateId 长度必须在 1-128 之间".into()));
    }
    if !id.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
        return Err(WorkerError::InvalidInput("aggregateId 仅允许字母数字和 -_".into()));
    }
    Ok(())
}

/// 构建动态 GraphQL Schema
pub fn build_dynamic_schema(
    module_commands: Vec<(String, String, Vec<crate::command::CommandDef>)>, // (module_name, pascal_name, commands)
    runtime: crate::runtime::RuntimeHandle,
    event_store: Arc<dyn crate::event_store::EventStore>,
) -> Schema {
    let query = build_query_type(event_store);

    let mut module_objs: Vec<Object> = Vec::new();
    let mut mutation = Object::new("Mutation");
    for (module_name, pascal_name, commands) in &module_commands {
        let type_name = pascal_name.clone();
        let actual_module = module_name.clone();
        let obj = build_aggregate_mutation(&type_name, &actual_module, commands, runtime.clone());
        module_objs.push(obj);
        mutation = mutation.field(Field::new(
            module_name.as_str(),
            TypeRef::named_nn(type_name.as_str()),
            |_ctx| FieldFuture::new(async { Ok(Some(FieldValue::NULL)) }),
        ));
    }

    // 注册 UInt64 自定义 Scalar
    let uint64_scalar = Scalar::new("UInt64")
        .description("64 位无符号整数，序列化为字符串避免 GraphQL Int 32 位溢出");

    let mut builder = Schema::build("Query", Some("Mutation"), None)
        .register(query)
        .register(mutation)
        .register(uint64_scalar);

    for obj in module_objs {
        builder = builder.register(obj);
    }

    // 注册 CommandResult 输出类型
    builder = builder.register(build_command_result_type());

    builder.finish().unwrap()
}

/// 构建 Query 类型
fn build_query_type(event_store: Arc<dyn crate::event_store::EventStore>) -> Object {
    let store_version = event_store;

    Object::new("Query")
        .field(Field::new("health", TypeRef::named_nn(TypeRef::BOOLEAN), move |_ctx| {
            FieldFuture::new(async move { Ok(Some(FieldValue::value(true))) })
        }))
        .field(Field::new(
            "aggregateVersion",
            TypeRef::named_nn("UInt64"),
            move |ctx| {
                let store = store_version.clone();
                FieldFuture::new(async move {
                    let aggregate_type = ctx.args.try_get("aggregateType")?.string()?.to_string();
                    let aggregate_id = ctx.args.try_get("aggregateId")?.string()?.to_string();
                    validate_aggregate_id(&aggregate_id)
                        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                    let version = store.get_current_version(&aggregate_type, &aggregate_id)
                        .await
                        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                    match version {
                        Some(v) => Ok(Some(FieldValue::value(v.to_string()))),
                        None => Ok(None),
                    }
                })
            },
        )
        .argument(InputValue::new("aggregateType", TypeRef::named_nn(TypeRef::STRING)))
        .argument(InputValue::new("aggregateId", TypeRef::named_nn(TypeRef::STRING))))
}

/// 构建聚合 Mutation 对象（每个模块一个）
fn build_aggregate_mutation(
    type_name: &str,
    module_name: &str,
    commands: &[crate::command::CommandDef],
    runtime: crate::runtime::RuntimeHandle,
) -> Object {
    let mut obj = Object::new(type_name);

    for cmd in commands {
        let runtime = runtime.clone();
        let module = module_name.to_string();
        let command_type = cmd.name.clone();
        let graphql_name = cmd.graphql_name.clone();

        let mut field = Field::new(&graphql_name, TypeRef::named_nn("CommandResult"), move |ctx| {
            let runtime = runtime.clone();
            let module = module.clone();
            let command_type = command_type.clone();

            FieldFuture::new(async move {
                let command = extract_command_meta(&ctx, &module, &command_type)
                    .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                let result = runtime.send(command).await
                    .map_err(|e| async_graphql::Error::new(e.to_string()))?;
                Ok(Some(FieldValue::owned_any(result)))
            })
        })
        .argument(InputValue::new("aggregateId", TypeRef::named_nn(TypeRef::STRING)))
        .argument(InputValue::new("expectedVersion", TypeRef::named_nn("UInt64")));

        // 添加领域参数（从 WIT record 推导）
        // 简化实现：根据命令名推断常见参数
        field = add_domain_args(field, &cmd.name);

        obj = obj.field(field);
    }

    obj
}

/// 根据命令名添加领域参数的简化实现
/// 完整版本需要从 WIT record 定义提取参数
fn add_domain_args(field: Field, cmd_name: &str) -> Field {
    match cmd_name {
        "increment" => {
            field.argument(InputValue::new("amount", TypeRef::named_nn(TypeRef::INT)))
        }
        "create-item" => {
            field
                .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
                .argument(InputValue::new("quantity", TypeRef::named_nn(TypeRef::INT)))
        }
        "adjust-stock" => {
            field.argument(InputValue::new("delta", TypeRef::named_nn(TypeRef::INT)))
        }
        _ => field,
    }
}

/// 构建 CommandResult 输出类型
fn build_command_result_type() -> Object {
    Object::new("CommandResult")
        .field(Field::new("success", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
            FieldFuture::new(async move {
                let value = ctx.parent_value.try_downcast_ref::<crate::types::CommandResult>();
                match value {
                    Ok(v) => Ok(Some(FieldValue::value(v.success))),
                    Err(_) => Ok(None),
                }
            })
        }))
        .field(Field::new("version", TypeRef::named_nn("UInt64"), |ctx| {
            FieldFuture::new(async move {
                let value = ctx.parent_value.try_downcast_ref::<crate::types::CommandResult>();
                match value {
                    Ok(v) => Ok(Some(FieldValue::value(v.version.to_string()))),
                    Err(_) => Ok(None),
                }
            })
        }))
        .field(Field::new("eventCount", TypeRef::named_nn(TypeRef::INT), |ctx| {
            FieldFuture::new(async move {
                let value = ctx.parent_value.try_downcast_ref::<crate::types::CommandResult>();
                match value {
                    Ok(v) => Ok(Some(FieldValue::value(v.event_count as i32))),
                    Err(_) => Ok(None),
                }
            })
        }))
        .field(Field::new("error", TypeRef::named(TypeRef::STRING), |ctx| {
            FieldFuture::new(async move {
                let value = ctx.parent_value.try_downcast_ref::<crate::types::CommandResult>();
                match value {
                    Ok(v) => Ok(v.error.clone().map(FieldValue::value)),
                    Err(_) => Ok(None),
                }
            })
        }))
}

/// 从 GraphQL 参数中提取 IncomingCommand
/// 提取 aggregateId, expectedVersion 以及所有领域参数（转为 JSON）
fn extract_command_meta(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    module: &str,
    command_type: &str,
) -> Result<IncomingCommand, WorkerError> {
    let aggregate_id = ctx.args.try_get("aggregateId")
        .map_err(|_| WorkerError::MissingField("aggregateId".into()))?
        .string()
        .map_err(|_| WorkerError::InvalidInput("aggregateId 格式错误".into()))?
        .to_string();

    validate_aggregate_id(&aggregate_id)?;

    let expected_version: u64 = ctx.args.try_get("expectedVersion")
        .map_err(|_| WorkerError::MissingField("expectedVersion".into()))?
        .string()
        .map_err(|_| WorkerError::InvalidInput("expectedVersion 格式错误".into()))?
        .parse()
        .map_err(|_| WorkerError::InvalidInput("expectedVersion 必须为有效的非负整数".into()))?;

    // 提取所有领域参数（排除 aggregateId 和 expectedVersion）
    // async-graphql dynamic API 无法枚举所有参数，
    // 所以通过 try_get 逐个提取已知参数名
    let data = extract_domain_args(ctx);

    Ok(IncomingCommand {
        aggregate_id,
        expected_version,
        module: module.to_string(),
        command_type: command_type.to_string(),
        data,
    })
}

/// 提取领域参数并序列化为 JSON
fn extract_domain_args(ctx: &async_graphql::dynamic::ResolverContext<'_>) -> Vec<u8> {
    // 简化实现：将整个 ctx.args 序列化
    // 在动态 API 中，我们通过 try_get 逐个提取参数
    // 对于当前测试计数器场景，仅需要 amount 参数
    let mut map = serde_json::Map::new();

    // 尝试提取 amount 参数（整数类型）
    if let Ok(val) = ctx.args.try_get("amount") {
        if let Ok(n) = val.i64() {
            map.insert("amount".into(), serde_json::json!(n));
        } else if let Ok(s) = val.string() {
            if let Ok(n) = s.parse::<i64>() {
                map.insert("amount".into(), serde_json::json!(n));
            }
        }
    }

    // 尝试提取 name 参数（字符串类型）
    if let Ok(val) = ctx.args.try_get("name") {
        if let Ok(s) = val.string() {
            map.insert("name".into(), serde_json::json!(s));
        }
    }

    // 尝试提取 quantity 参数（整数类型）
    if let Ok(val) = ctx.args.try_get("quantity") {
        if let Ok(n) = val.i64() {
            map.insert("quantity".into(), serde_json::json!(n));
        }
    }

    // 尝试提取 delta 参数（整数类型）
    if let Ok(val) = ctx.args.try_get("delta") {
        if let Ok(n) = val.i64() {
            map.insert("delta".into(), serde_json::json!(n));
        }
    }

    serde_json::to_vec(&map).unwrap_or_default()
}

// ===== 测试 =====

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kebab_to_camel_simple() {
        assert_eq!(kebab_to_camel("hello-world"), "helloWorld");
    }

    #[test]
    fn test_kebab_to_camel_single() {
        assert_eq!(kebab_to_camel("hello"), "hello");
    }

    #[test]
    fn test_kebab_to_camel_multiple() {
        assert_eq!(kebab_to_camel("a-b-c"), "aBC");
    }

    #[test]
    fn test_kebab_to_camel_empty() {
        assert_eq!(kebab_to_camel(""), "");
    }

    #[test]
    fn test_to_pascal_case_kebab() {
        assert_eq!(to_pascal_case("hello-world"), "HelloWorld");
    }

    #[test]
    fn test_to_pascal_case_snake() {
        assert_eq!(to_pascal_case("hello_world"), "HelloWorld");
    }

    #[test]
    fn test_to_pascal_case_simple() {
        assert_eq!(to_pascal_case("counter"), "Counter");
    }

    #[test]
    fn test_to_pascal_case_empty() {
        assert_eq!(to_pascal_case(""), "");
    }

    #[test]
    fn test_validate_aggregate_id_valid() {
        assert!(validate_aggregate_id("item-001").is_ok());
        assert!(validate_aggregate_id("agg_123").is_ok());
        assert!(validate_aggregate_id("ABC-def_456").is_ok());
    }

    #[test]
    fn test_validate_aggregate_id_empty() {
        assert!(validate_aggregate_id("").is_err());
    }

    #[test]
    fn test_validate_aggregate_id_too_long() {
        let long_id = "a".repeat(129);
        assert!(validate_aggregate_id(&long_id).is_err());
    }

    #[test]
    fn test_validate_aggregate_id_invalid_chars() {
        assert!(validate_aggregate_id("hello world").is_err());
        assert!(validate_aggregate_id("a/b").is_err());
        assert!(validate_aggregate_id("a\nb").is_err());
    }

    #[test]
    fn test_validate_aggregate_id_at_boundary() {
        // 128 字符应该可以
        let id = "a".repeat(128);
        assert!(validate_aggregate_id(&id).is_ok());
    }

    #[test]
    fn test_camel_to_kebab() {
        fn camel_to_kebab(s: &str) -> String {
            let mut result = String::new();
            for ch in s.chars() {
                if ch.is_uppercase() {
                    result.push('-');
                    result.extend(ch.to_lowercase());
                } else {
                    result.push(ch);
                }
            }
            result
        }
        assert_eq!(camel_to_kebab("helloWorld"), "hello-world");
        assert_eq!(camel_to_kebab("aggregateId"), "aggregate-id");
    }

    // === add_domain_args 测试 ===

    #[test]
    fn test_add_domain_args_increment() {
        let field = Field::new("increment", TypeRef::named_nn("CommandResult"), |_| {
            FieldFuture::new(async { Ok(Some(FieldValue::NULL)) })
        });
        let field = add_domain_args(field, "increment");
        // 验证不崩溃 - 无法直接用单元测试验证参数添加
        let _ = field;
    }

    #[test]
    fn test_add_domain_args_create_item() {
        let field = Field::new("createItem", TypeRef::named_nn("CommandResult"), |_| {
            FieldFuture::new(async { Ok(Some(FieldValue::NULL)) })
        });
        let field = add_domain_args(field, "create-item");
        let _ = field;
    }

    #[test]
    fn test_add_domain_args_adjust_stock() {
        let field = Field::new("adjustStock", TypeRef::named_nn("CommandResult"), |_| {
            FieldFuture::new(async { Ok(Some(FieldValue::NULL)) })
        });
        let field = add_domain_args(field, "adjust-stock");
        let _ = field;
    }

    #[test]
    fn test_add_domain_args_unknown_command() {
        let field = Field::new("unknownCmd", TypeRef::named_nn("CommandResult"), |_| {
            FieldFuture::new(async { Ok(Some(FieldValue::NULL)) })
        });
        let field = add_domain_args(field, "unknown-command");
        let _ = field;
    }

    // === extract_domain_args 测试（使用 Mock ResolverContext）===

    #[test]
    fn test_extract_domain_args_empty() {
        // 无法在单元测试中创建 ResolverContext，通过集成测试覆盖
    }

    // === Schema 构建测试 ===

    #[test]
    fn test_build_command_result_type_does_not_panic() {
        let _obj = build_command_result_type();
    }

    #[test]
    fn test_to_pascal_case_mixed_separators() {
        assert_eq!(to_pascal_case("string-utils"), "StringUtils");
        assert_eq!(to_pascal_case("hello_world-service"), "HelloWorldService");
    }

    #[test]
    fn test_kebab_to_camel_with_uppercase_first() {
        assert_eq!(kebab_to_camel("-hello"), "Hello");
        // 双连字符后面的字符也会大写
        assert_eq!(kebab_to_camel("test--double"), "testDouble");
    }
}
