use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};

use crate::wasm_engine::WasmEngine;

pub struct ModuleInfo {
    pub engine: WasmEngine,
    pub module_name: String,
    pub source_path: PathBuf,
}

pub struct WasmRegistry {
    modules: Vec<ModuleInfo>,
}

impl WasmRegistry {
    pub fn from_dir(dir: &Path) -> Result<Self> {
        let wasm_files = scan_wasm_files(dir)?;
        if wasm_files.is_empty() {
            return Err(anyhow!("目录 '{}' 下未找到 .wasm 文件", dir.display()));
        }

        let mut modules = Vec::new();
        for path in wasm_files {
            match WasmEngine::new(path.to_str().unwrap_or_default()) {
                Ok(engine) => {
                    let module_name = engine.module_name().to_string();
                    modules.push(ModuleInfo { engine, module_name, source_path: path });
                }
                Err(_) => {
                    eprintln!("跳过非组件文件: {}", path.display());
                }
            }
        }

        if modules.is_empty() {
            return Err(anyhow!("目录 '{}' 下未找到有效的 WASM 组件", dir.display()));
        }

        Self::check_duplicate_names(&modules)?;
        Ok(Self { modules })
    }

    pub fn modules(&self) -> &[ModuleInfo] {
        &self.modules
    }

    fn check_duplicate_names(modules: &[ModuleInfo]) -> Result<()> {
        let mut seen: HashMap<&str, &Path> = HashMap::new();
        for m in modules {
            if let Some(prev_path) = seen.get(m.module_name.as_str()) {
                return Err(anyhow!(
                    "模块名称冲突: \"{}\" 同时存在于:\n  - {}\n  - {}",
                    m.module_name,
                    prev_path.display(),
                    m.source_path.display()
                ));
            }
            seen.insert(&m.module_name, &m.source_path);
        }
        Ok(())
    }
}

fn scan_wasm_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    scan_dir_recursive(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn scan_dir_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| anyhow!("无法读取目录 '{}': {}", dir.display(), e))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            scan_dir_recursive(&path, files)?;
        } else if path.extension().is_some_and(|ext| ext == "wasm") {
            files.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_scan_wasm_files_empty_dir() {
        let dir = TempDir::new().unwrap();
        let files = scan_wasm_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_scan_wasm_files_finds_wasm() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.wasm"), b"fake").unwrap();
        fs::write(dir.path().join("b.txt"), b"not wasm").unwrap();
        let files = scan_wasm_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("a.wasm"));
    }

    #[test]
    fn test_scan_wasm_files_recursive() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(dir.path().join("a.wasm"), b"fake").unwrap();
        fs::write(sub.join("b.wasm"), b"fake").unwrap();
        let files = scan_wasm_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_scan_wasm_files_sorted() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("z.wasm"), b"fake").unwrap();
        fs::write(dir.path().join("a.wasm"), b"fake").unwrap();
        let files = scan_wasm_files(dir.path()).unwrap();
        assert!(files[0] < files[1]);
    }

    #[test]
    fn test_scan_wasm_files_nonexistent_dir() {
        let result = scan_wasm_files(Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }

    #[test]
    fn test_from_dir_empty_error() {
        let dir = TempDir::new().unwrap();
        let result = WasmRegistry::from_dir(dir.path());
        assert!(result.is_err());
    }
}
