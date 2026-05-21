use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let workspace_root = manifest_dir.parent().unwrap();
    let wasm_lib_dir = workspace_root.join("wasm-lib");

    println!("cargo:rerun-if-changed=../wasm-lib/src/lib.rs");
    println!("cargo:rerun-if-changed=../wasm-lib/Cargo.toml");
    println!("cargo:rerun-if-changed=../wit/world.wit");

    let status = Command::new("cargo")
        .args(["component", "build", "--release", "--manifest-path"])
        .arg(wasm_lib_dir.join("Cargo.toml").to_str().unwrap())
        .status()
        .expect("cargo component build 失败，请确认已安装 cargo-component");

    if !status.success() {
        panic!("wasm-lib component 编译失败");
    }

    let component_path = workspace_root
        .join("target")
        .join("wasm32-wasip1")
        .join("release")
        .join("wasm_lib.wasm");

    println!(
        "cargo:rustc-env=WASM_MODULE_PATH={}",
        component_path.display()
    );
}
