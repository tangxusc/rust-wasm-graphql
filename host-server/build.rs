use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let workspace_root = manifest_dir.parent().unwrap();
    let example_dir = workspace_root.join("example");

    let modules = vec![
        ("wasm-lib", "wasm_lib"),
        ("wasm-lib2", "wasm_lib2"),
    ];

    for (dir_name, _artifact_name) in &modules {
        let lib_dir = example_dir.join(dir_name);
        if !lib_dir.exists() {
            continue;
        }

        println!("cargo:rerun-if-changed=../example/{}/src/lib.rs", dir_name);
        println!("cargo:rerun-if-changed=../example/{}/Cargo.toml", dir_name);
        println!("cargo:rerun-if-changed=../example/{}/wit", dir_name);

        let status = Command::new("cargo")
            .args(["component", "build", "--release", "--manifest-path"])
            .arg(lib_dir.join("Cargo.toml").to_str().unwrap())
            .status()
            .expect("cargo component build failed");

        if !status.success() {
            panic!("{} component build failed", dir_name);
        }
    }

    println!("cargo:rerun-if-changed=../example/wit/world.wit");

    let wasm_dir = workspace_root
        .join("target")
        .join("wasm32-wasip1")
        .join("release");

    println!("cargo:rustc-env=DEFAULT_WASM_DIR={}", wasm_dir.display());
}
