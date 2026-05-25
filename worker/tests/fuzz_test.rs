/// 模糊/属性测试（基于 proptest）
///
/// 覆盖：
/// - 命令发现（discover_commands）
/// - 字符串转换（kebab_to_camel, to_pascal_case）
/// - aggregateId 校验（validate_aggregate_id）
/// - 类型序列化往返
/// - 错误类型格式化
use proptest::prelude::*;

// ===== 辅助策略 =====

/// 生成函数名列表
fn func_name_strategy() -> impl Strategy<Value=String> {
    prop::collection::vec(any::<u8>(), 0..64).prop_map(|v| String::from_utf8_lossy(&v).to_string())
}

/// 生成带 handle-/validate- 前缀的函数名
fn prefixed_func_name() -> impl Strategy<Value=String> {
    (prop_oneof!["handle-", "validate-", ""], r"[a-zA-Z][a-zA-Z0-9_-]{0,15}")
        .prop_map(|(p, n)| format!("{p}{n}"))
}

// ===== 命令发现模糊测试 =====

proptest! {
    #[test]
    fn fuzz_discover_commands_no_panic(
        names in prop::collection::vec(func_name_strategy(), 0..50)
    ) {
        let funcs: Vec<worker::command::ExportedFunctionInfo> = names
            .iter()
            .map(|n| worker::command::ExportedFunctionInfo { wit_name: n.clone() })
            .collect();

        let result = worker::command::discover_commands(&funcs);

        match result {
            Ok(cmds) => {
                prop_assert!(!cmds.is_empty(), "成功时命令列表不应为空");
                for cmd in &cmds {
                    prop_assert!(!cmd.graphql_name.contains('-'),
                        "camelCase 含连字符: {}", cmd.graphql_name);
                    prop_assert!(!cmd.name.is_empty());
                    prop_assert!(!cmd.handle_fn.is_empty());
                }
            }
            Err(e) => {
                prop_assert!(!e.is_empty(), "错误信息不应为空");
            }
        }
    }

    #[test]
    fn fuzz_discover_commands_valid(
        names in prop::collection::vec(prefixed_func_name(), 1..30)
    ) {
        let mut all_names = names;
        all_names.push("apply-events".to_string());

        let has_handle = all_names.iter().any(|n| n.starts_with("handle-") && n != "handle-");

        let funcs: Vec<worker::command::ExportedFunctionInfo> = all_names
            .iter()
            .map(|n| worker::command::ExportedFunctionInfo { wit_name: n.clone() })
            .collect();

        let result = worker::command::discover_commands(&funcs);
        if has_handle {
            // 结果可能是 Ok 或因孤立 validate 而 Err，但不应 panic
            let _ = result;
        }
    }
}

// ===== 字符串转换模糊测试 =====

proptest! {
    #[test]
    fn fuzz_string_conversion_no_panic(
        s in prop::string::string_regex("\\PC{0,64}").unwrap()
    ) {
        let camel = worker::graphql::kebab_to_camel(&s);
        let pascal = worker::graphql::to_pascal_case(&s);

        // 输出不含特殊分隔符（核心不变量）
        prop_assert!(!camel.contains('-'), "camelCase 不应含连字符: '{}'", camel);
        prop_assert!(!pascal.contains('-'), "PascalCase 不应含连字符: '{}'", pascal);
        prop_assert!(!pascal.contains('_'), "PascalCase 不应含下划线: '{}'", pascal);

        // 非 ASCII 字符在其位置原样保留
        // 对于纯 ASCII 输入，输出应也是 ASCII（即不引入非 ASCII 字符）
        if s.is_ascii() {
            prop_assert!(camel.is_ascii(), "ASCII 输入产生非 ASCII 输出: '{}'", camel);
            prop_assert!(pascal.is_ascii(), "ASCII 输入产生非 ASCII 输出: '{}'", pascal);
        }
    }

    #[test]
    fn fuzz_string_roundtrip_properties(
        parts in prop::collection::vec(r"[a-z]{1,10}", 1..5)
    ) {
        let kebab = parts.join("-");
        let camel = worker::graphql::kebab_to_camel(&kebab);
        let pascal = worker::graphql::to_pascal_case(&kebab);

        prop_assert!(!camel.contains('-'), "kebab 转 camel 不应含连字符");
        if !pascal.is_empty() {
            prop_assert!(pascal.chars().next().unwrap().is_uppercase(),
                "PascalCase 首字母应大写: {}", pascal);
        }
    }
}

// ===== aggregateId 校验模糊测试 =====

proptest! {
    #[test]
    fn fuzz_aggregate_id_no_panic(
        raw in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        let s = String::from_utf8_lossy(&raw).to_string();
        let result = worker::graphql::validate_aggregate_id(&s);

        match result {
            Ok(()) => {
                prop_assert!(!s.is_empty(), "空字符串不应通过");
                prop_assert!(s.len() <= 128, "超长字符串不应通过: {}", s.len());
                for ch in s.chars() {
                    prop_assert!(ch.is_alphanumeric() || ch == '-' || ch == '_',
                        "非法字符不应通过: U+{:04X}", ch as u32);
                }
            }
            Err(e) => {
                prop_assert!(!e.to_string().is_empty(), "错误信息不应为空");
            }
        }
    }

    #[test]
    fn fuzz_aggregate_id_valid_passes(
        s in r"[a-zA-Z0-9_-]{1,128}"
    ) {
        let result = worker::graphql::validate_aggregate_id(&s);
        prop_assert!(result.is_ok(), "有效 id '{}' 应通过: {:?}", s, result.err());
    }

    #[test]
    fn fuzz_aggregate_id_invalid_rejected(
        bad in r"[ \t\r\n@#$%^&*()/\\]",
        prefix in r"[a-zA-Z0-9]{1,16}"
    ) {
        let s = format!("{}{}suffix", prefix, bad);
        let result = worker::graphql::validate_aggregate_id(&s);
        prop_assert!(result.is_err(), "含非法字符应被拒绝: '{}'", s);
    }
}

// ===== 类型序列化往返模糊测试 =====

proptest! {
    #[test]
    fn fuzz_incoming_command_roundtrip(
        aggregate_id in "\\PC{0,32}",
        expected_version: u64,
        module in "\\PC{0,16}",
        command_type in "\\PC{0,16}",
        data in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        let cmd = worker::types::IncomingCommand {
            aggregate_id,
            expected_version,
            module,
            command_type,
            data: data.clone(),
        };

        let json = serde_json::to_vec(&cmd).unwrap();
        let decoded: worker::types::IncomingCommand = serde_json::from_slice(&json).unwrap();

        prop_assert_eq!(decoded.aggregate_id, cmd.aggregate_id);
        prop_assert_eq!(decoded.expected_version, cmd.expected_version);
        prop_assert_eq!(decoded.module, cmd.module);
        prop_assert_eq!(decoded.command_type, cmd.command_type);
        prop_assert_eq!(decoded.data, data);
    }

    #[test]
    fn fuzz_domain_event_roundtrip(
        id: i64,
        aggregate_id in "\\PC{0,32}",
        aggregate_type in "\\PC{0,16}",
        event_type in "\\PC{0,32}",
        version: u64,
        data in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        let event = worker::types::DomainEvent {
            id,
            aggregate_id: aggregate_id.clone(),
            aggregate_type: aggregate_type.clone(),
            event_type: event_type.clone(),
            version,
            data: data.clone(),
        };

        let json = serde_json::to_vec(&event).unwrap();
        let decoded: worker::types::DomainEvent = serde_json::from_slice(&json).unwrap();

        prop_assert_eq!(decoded.id, id);
        prop_assert_eq!(decoded.aggregate_id, aggregate_id);
        prop_assert_eq!(decoded.aggregate_type, aggregate_type);
        prop_assert_eq!(decoded.event_type, event_type);
        prop_assert_eq!(decoded.version, version);
        prop_assert_eq!(decoded.data, data);
    }

    #[test]
    fn fuzz_snapshot_roundtrip(
        aggregate_id in "\\PC{0,32}",
        aggregate_type in "\\PC{0,16}",
        version: u64,
        state in prop::collection::vec(any::<u8>(), 0..1024),
    ) {
        let snap = worker::types::Snapshot {
            aggregate_id: aggregate_id.clone(),
            aggregate_type: aggregate_type.clone(),
            version,
            state: state.clone(),
        };

        let json = serde_json::to_vec(&snap).unwrap();
        let decoded: worker::types::Snapshot = serde_json::from_slice(&json).unwrap();

        prop_assert_eq!(decoded.aggregate_id, aggregate_id);
        prop_assert_eq!(decoded.aggregate_type, aggregate_type);
        prop_assert_eq!(decoded.version, version);
        prop_assert_eq!(decoded.state, state);
    }

    #[test]
    fn fuzz_command_result_consistency(
        version: u64,
        event_count: usize,
        error_msg in "\\PC{0,128}",
    ) {
        let ok_r = worker::types::CommandResult::ok(version, event_count);
        prop_assert!(ok_r.success);
        prop_assert_eq!(ok_r.version, version);
        prop_assert_eq!(ok_r.event_count, event_count);
        prop_assert!(ok_r.error.is_none());

        let err_r = worker::types::CommandResult::err(version, &error_msg);
        prop_assert!(!err_r.success);
        prop_assert_eq!(err_r.version, version);
        prop_assert_eq!(err_r.event_count, 0);
        prop_assert_eq!(err_r.error.unwrap(), error_msg);

        let noop_r = worker::types::CommandResult::noop(version);
        prop_assert!(noop_r.success);
        prop_assert_eq!(noop_r.version, version);
        prop_assert_eq!(noop_r.event_count, 0);
        prop_assert!(noop_r.error.is_none());
    }

    #[test]
    fn fuzz_malicious_json_no_panic(
        raw in prop::collection::vec(any::<u8>(), 0..1024),
    ) {
        let _ = serde_json::from_slice::<worker::types::IncomingCommand>(&raw);
        let _ = serde_json::from_slice::<worker::types::DomainEvent>(&raw);
        let _ = serde_json::from_slice::<worker::types::Snapshot>(&raw);
        let _ = serde_json::from_slice::<worker::types::CommandResult>(&raw);
        let _ = serde_json::from_slice::<worker::types::PendingEvent>(&raw);
    }
}

// ===== 错误类型模糊测试 =====

proptest! {
    #[test]
    fn fuzz_worker_error_display_non_empty(
        msg in "\\PC{0,128}",
    ) {
        let errors: Vec<worker::error::WorkerError> = vec![
            worker::error::WorkerError::Overloaded(msg.clone()),
            worker::error::WorkerError::ActorUnavailable(msg.clone()),
            worker::error::WorkerError::ModuleNotFound(msg.clone()),
            worker::error::WorkerError::InvalidEvent(msg.clone()),
            worker::error::WorkerError::InvalidInput(msg.clone()),
            worker::error::WorkerError::MissingField(msg.clone()),
            worker::error::WorkerError::WasmExecution(msg.clone()),
            worker::error::WorkerError::Timeout(msg.clone()),
            worker::error::WorkerError::Internal(msg.clone()),
        ];

        for err in &errors {
            prop_assert!(!err.to_string().is_empty());
        }
    }

    #[test]
    fn fuzz_version_conflict_formatting(
        current: u64,
        expected: u64,
    ) {
        let err = worker::error::WorkerError::VersionConflict {
            current_version: current,
            expected_version: expected,
        };
        prop_assert!(!err.to_string().is_empty());
    }

    #[test]
    fn fuzz_store_error_roundtrip(
        aggregate_id in "\\PC{0,32}",
        version: u64,
        msg in "\\PC{0,128}",
    ) {
        let vc = worker::error::StoreError::VersionConflict {
            aggregate_id: aggregate_id.clone(), expected_version: version,
        };
        prop_assert!(!vc.to_string().is_empty());

        let ce = worker::error::StoreError::ConnectionError(msg.clone());
        prop_assert!(!ce.to_string().is_empty());

        // Clone 保持一致性
        prop_assert_eq!(vc.to_string(), vc.clone().to_string());
        prop_assert_eq!(ce.to_string(), ce.clone().to_string());
    }
}

// ===== ActorKey 测试 =====

proptest! {
    #[test]
    fn fuzz_actor_key_tuple(
        module in "\\PC{0,32}",
        aggregate_id in "\\PC{0,64}",
    ) {
        let key: worker::types::ActorKey = (module.clone(), aggregate_id.clone());
        prop_assert_eq!(key.0, module);
        prop_assert_eq!(key.1, aggregate_id);
    }
}

// ===== 针对特定边界值的确定性模糊测试 =====

#[test]
fn fuzz_deterministic_edge_cases() {
    // 空输入
    let result = worker::command::discover_commands(&[]);
    assert!(result.is_err()); // 空的函数列表应报错

    // 仅 apply-events（无命令）
    let funcs = vec![worker::command::ExportedFunctionInfo { wit_name: "apply-events".into() }];
    let result = worker::command::discover_commands(&funcs);
    assert!(result.is_err()); // 没有 handle-X 应报错

    // 仅 handle-X（无 apply-events）
    let funcs = vec![worker::command::ExportedFunctionInfo { wit_name: "handle-test".into() }];
    let result = worker::command::discover_commands(&funcs);
    assert!(result.is_err()); // 缺少 apply-events

    // 孤立 validate
    let funcs = vec![
        worker::command::ExportedFunctionInfo { wit_name: "validate-test".into() },
        worker::command::ExportedFunctionInfo { wit_name: "apply-events".into() },
    ];
    let result = worker::command::discover_commands(&funcs);
    assert!(result.is_err()); // 孤立 validate
    assert!(result.unwrap_err().contains("validate-test"));

    // handle- 后命令名为空
    let funcs = vec![
        worker::command::ExportedFunctionInfo { wit_name: "handle-".into() },
        worker::command::ExportedFunctionInfo { wit_name: "apply-events".into() },
    ];
    let result = worker::command::discover_commands(&funcs);
    assert!(result.is_err());

    // validate- 后命令名为空
    let funcs = vec![
        worker::command::ExportedFunctionInfo { wit_name: "validate-".into() },
        worker::command::ExportedFunctionInfo { wit_name: "handle-test".into() },
        worker::command::ExportedFunctionInfo { wit_name: "apply-events".into() },
    ];
    let result = worker::command::discover_commands(&funcs);
    assert!(result.is_err());
}

#[test]
fn fuzz_string_convert_edge_cases() {
    // 空输入
    assert_eq!(worker::graphql::kebab_to_camel(""), "");
    assert_eq!(worker::graphql::to_pascal_case(""), "");

    // 只有连字符
    assert!(!worker::graphql::kebab_to_camel("---").contains('-'));
    assert!(!worker::graphql::to_pascal_case("---").contains('-'));

    // 数字
    assert_eq!(worker::graphql::kebab_to_camel("123"), "123");
    assert_eq!(worker::graphql::to_pascal_case("123"), "123");
}

#[test]
fn fuzz_aggregate_id_edge_cases() {
    assert!(worker::graphql::validate_aggregate_id("").is_err());
    assert!(worker::graphql::validate_aggregate_id(&"a".repeat(129)).is_err());
    assert!(worker::graphql::validate_aggregate_id(&"a".repeat(128)).is_ok());
    assert!(worker::graphql::validate_aggregate_id("a b").is_err());
    assert!(worker::graphql::validate_aggregate_id("a\tb").is_err());
    assert!(worker::graphql::validate_aggregate_id("a\nb").is_err());
    assert!(worker::graphql::validate_aggregate_id("hello world").is_err());
    assert!(worker::graphql::validate_aggregate_id("../etc/passwd").is_err());
    assert!(worker::graphql::validate_aggregate_id("a;DROP TABLE").is_err());
    assert!(worker::graphql::validate_aggregate_id("test-001").is_ok());
    assert!(worker::graphql::validate_aggregate_id("agg_123").is_ok());
}

#[test]
fn fuzz_types_malformed_json() {
    // 各种格式错误的 JSON 不应 panic
    let inputs: Vec<&[u8]> = vec![
        b"", b"{", b"}", b"[", b"]", b"null", b"42", b"\"string\"",
        b"{\"key\":}", b"{\"key\": [}", b"{\"a\":1,\"b\":2",
        &[0xFF, 0xFE, 0xFD], // 非 UTF-8
        b"\x00\x00\x00",      // null bytes
        b"\x1b\x5b\x31\x6d",  // ANSI escape
    ];

    for input in &inputs {
        let _ = serde_json::from_slice::<worker::types::IncomingCommand>(input);
        let _ = serde_json::from_slice::<worker::types::DomainEvent>(input);
        let _ = serde_json::from_slice::<worker::types::Snapshot>(input);
        let _ = serde_json::from_slice::<worker::types::CommandResult>(input);
    }
}
