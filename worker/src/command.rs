// 命令发现（WIT 元数据解析）

use std::collections::HashMap;

/// 导出的 WIT 函数信息（轻量结构体，不依赖 wasmtime）
#[derive(Debug, Clone)]
pub struct ExportedFunctionInfo {
    pub wit_name: String,
}

/// 发现的命令定义
#[derive(Debug, Clone)]
pub struct CommandDef {
    /// kebab-case 命令名（如 "create-item"）
    pub name: String,
    /// camelCase GraphQL 名（如 "createItem"）
    pub graphql_name: String,
    /// 可选的前置校验函数名（如 "validate-create-item"）
    pub validate_fn: Option<String>,
    /// 命令处理函数名（如 "handle-create-item"）
    pub handle_fn: String,
}

/// 将 kebab-case 转换为 camelCase
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

/// 以 handle-X 为基准发现命令，validate-X 为可选关联
/// 校验规则：
/// - handle-X 识别的命令：X 不能为空
/// - validate-X 必须有对应的 handle-X（否则报错：孤立 validate）
/// - apply-events 必须存在
pub fn discover_commands(functions: &[ExportedFunctionInfo]) -> Result<Vec<CommandDef>, String> {
    let mut validates: HashMap<&str, &ExportedFunctionInfo> = HashMap::new();
    let mut handles: HashMap<&str, &ExportedFunctionInfo> = HashMap::new();
    let mut has_apply_events = false;

    for func in functions {
        let name = func.wit_name.as_str();
        if name == "apply-events" {
            has_apply_events = true;
        } else if let Some(cmd) = name.strip_prefix("validate-") {
            if cmd.is_empty() {
                return Err(format!("validate 前缀后命令名为空: {}", name));
            }
            validates.insert(cmd, func);
        } else if let Some(cmd) = name.strip_prefix("handle-") {
            if cmd.is_empty() {
                return Err(format!("handle 前缀后命令名为空: {}", name));
            }
            handles.insert(cmd, func);
        }
        // 非 handle-/validate- 前缀的函数被忽略
    }

    // 检查孤立 validate
    for cmd in validates.keys() {
        if !handles.contains_key(cmd) {
            return Err(format!("validate-{cmd} 缺少对应的 handle-{cmd}"));
        }
    }

    // 检查 apply-events
    if !has_apply_events {
        return Err("缺少必须的 apply-events 函数".into());
    }

    // 检查至少有一个命令
    if handles.is_empty() {
        return Err("未发现任何 handle-X 命令函数".into());
    }

    // 构建命令列表（按命令名排序确保稳定性）
    let mut cmd_names: Vec<&str> = handles.keys().copied().collect();
    cmd_names.sort();

    let commands = cmd_names.iter().map(|cmd| {
        let handle_ref = handles[cmd];
        CommandDef {
            name: cmd.to_string(),
            graphql_name: kebab_to_camel(cmd),
            validate_fn: validates.get(cmd).map(|f| f.wit_name.clone()),
            handle_fn: handle_ref.wit_name.clone(),
        }
    }).collect();

    Ok(commands)
}

/// 创建 ExportedFunctionInfo 的便捷宏
pub fn func(name: &str) -> ExportedFunctionInfo {
    ExportedFunctionInfo { wit_name: name.to_string() }
}

// ===== 测试 =====

#[cfg(test)]
mod tests {
    use super::*;

    fn discover(funcs: &[&str]) -> Result<Vec<CommandDef>, String> {
        let infos: Vec<ExportedFunctionInfo> = funcs.iter().map(|n| func(n)).collect();
        discover_commands(&infos)
    }

    #[test]
    fn test_single_command_no_validate() {
        let cmds = discover(&["handle-create-item", "apply-events"]).unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].name, "create-item");
        assert_eq!(cmds[0].graphql_name, "createItem");
        assert_eq!(cmds[0].handle_fn, "handle-create-item");
        assert!(cmds[0].validate_fn.is_none());
    }

    #[test]
    fn test_command_with_validate() {
        let cmds = discover(&["handle-create-item", "validate-create-item", "apply-events"]).unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].name, "create-item");
        assert_eq!(cmds[0].validate_fn.as_deref(), Some("validate-create-item"));
    }

    #[test]
    fn test_multiple_commands() {
        let cmds = discover(&[
            "handle-create-item",
            "handle-adjust-stock",
            "validate-create-item",
            "apply-events",
        ]).unwrap();
        assert_eq!(cmds.len(), 2);
        // 按名称排序确保稳定
        assert_eq!(cmds[0].name, "adjust-stock");
        assert_eq!(cmds[1].name, "create-item");
    }

    #[test]
    fn test_orphaned_validate_error() {
        let result = discover(&["validate-create-item", "apply-events"]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("缺少对应的 handle-create-item"));
    }

    #[test]
    fn test_missing_apply_events_error() {
        let result = discover(&["handle-create-item"]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("apply-events"));
    }

    #[test]
    fn test_empty_export_list_error() {
        let result = discover(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_non_handle_functions_ignored() {
        let cmds = discover(&["handle-increment", "some-utility", "apply-events"]).unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].name, "increment");
    }

    #[test]
    fn test_multi_dash_command_name() {
        let cmds = discover(&["handle-increment-amount", "apply-events"]).unwrap();
        assert_eq!(cmds[0].name, "increment-amount");
        assert_eq!(cmds[0].graphql_name, "incrementAmount");
    }

    #[test]
    fn test_empty_command_name_after_prefix() {
        let result = discover(&["handle-", "apply-events"]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("命令名为空"));
    }

    #[test]
    fn test_validate_without_handle_among_others() {
        let result = discover(&["handle-valid-cmd", "validate-orphaned", "apply-events"]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("validate-orphaned"));
    }
}
