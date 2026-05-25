use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let test_counter = manifest_dir.join("tests").join("wasm-counter");

    // 使用 workspace target 目录，避免 wasm-counter 独立 workspace 导致分离
    let workspace_root = manifest_dir.parent().unwrap();
    let workspace_target = workspace_root.join("target");

    if test_counter.exists() {
        println!("cargo:rerun-if-changed=tests/wasm-counter/src/lib.rs");
        println!("cargo:rerun-if-changed=tests/wasm-counter/Cargo.toml");
        println!("cargo:rerun-if-changed=tests/wasm-counter/wit/world.wit");

        let status = Command::new("cargo")
            .args([
                "component", "build", "--release",
                "--manifest-path",
                test_counter.join("Cargo.toml").to_str().unwrap(),
                "--target-dir",
                workspace_target.to_str().unwrap(),
            ])
            .status()
            .expect("cargo component build failed for test counter");

        if !status.success() {
            panic!("test counter WASM component build failed");
        }
    }

    // 设置默认 WASM 目录
    let wasm_dir = workspace_target
        .join("wasm32-wasip1")
        .join("release");

    println!("cargo:rustc-env=DEFAULT_WASM_DIR={}", wasm_dir.display());
}
