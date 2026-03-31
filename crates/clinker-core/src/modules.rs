use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cxl::ast::Module;
use cxl::parser::{ModuleParseResult, Parser};

/// Maximum .cxl module file size (1 MB).
const MAX_MODULE_FILE_SIZE: u64 = 1_048_576;

/// A loaded and parsed module with its evaluated constants.
#[derive(Debug)]
pub struct LoadedModule {
    pub module: Module,
    pub node_count: u32,
}

/// Registry of loaded CXL modules, keyed by module path.
/// Deduplicated: if two transforms both `use validators`, only one parse + one Arc.
#[derive(Debug, Default)]
pub struct ModuleRegistry {
    modules: HashMap<String, Arc<LoadedModule>>,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a module by its import path (e.g. "validators" or "shared.dates").
    pub fn get(&self, module_path: &str) -> Option<&Arc<LoadedModule>> {
        self.modules.get(module_path)
    }

    /// Number of loaded modules.
    pub fn len(&self) -> usize {
        self.modules.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }
}

/// Error during module loading.
#[derive(Debug)]
pub struct ModuleLoadError {
    pub module_path: String,
    pub file_path: PathBuf,
    pub kind: ModuleLoadErrorKind,
}

#[derive(Debug)]
pub enum ModuleLoadErrorKind {
    NotFound,
    TooLarge(u64),
    ReadError(std::io::Error),
    NotUtf8,
    ParseErrors(Vec<String>),
    DuplicateFunction(String),
    DuplicateConstant(String),
    ConstantError(String),
    RecursiveCall(String),
}

impl std::fmt::Display for ModuleLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            ModuleLoadErrorKind::NotFound => {
                write!(f, "module '{}' not found at {}", self.module_path, self.file_path.display())
            }
            ModuleLoadErrorKind::TooLarge(size) => {
                write!(f, "module file {} is too large ({} bytes, max {})", self.file_path.display(), size, MAX_MODULE_FILE_SIZE)
            }
            ModuleLoadErrorKind::ReadError(e) => {
                write!(f, "failed to read module file {}: {}", self.file_path.display(), e)
            }
            ModuleLoadErrorKind::NotUtf8 => {
                write!(f, "module file {} is not valid UTF-8", self.file_path.display())
            }
            ModuleLoadErrorKind::ParseErrors(errs) => {
                write!(f, "parse errors in module '{}': {}", self.module_path, errs.join("; "))
            }
            ModuleLoadErrorKind::DuplicateFunction(name) => {
                write!(f, "duplicate function '{}' in module '{}'", name, self.module_path)
            }
            ModuleLoadErrorKind::DuplicateConstant(name) => {
                write!(f, "duplicate constant '{}' in module '{}'", name, self.module_path)
            }
            ModuleLoadErrorKind::ConstantError(msg) => {
                write!(f, "constant error in module '{}': {}", self.module_path, msg)
            }
            ModuleLoadErrorKind::RecursiveCall(msg) => {
                write!(f, "recursive call in module '{}': {}", self.module_path, msg)
            }
        }
    }
}

impl std::error::Error for ModuleLoadError {}

/// Resolve a module import path to a filesystem path.
/// `use validators` → `{rules_path}/validators.cxl`
/// `use reporting.fiscal` → `{rules_path}/reporting/fiscal.cxl`
pub fn resolve_module_path(module_path: &[Box<str>], rules_path: &Path) -> PathBuf {
    let mut path = rules_path.to_path_buf();
    for segment in module_path {
        path.push(&**segment);
    }
    path.set_extension("cxl");
    path
}

/// Load modules from disk into the registry.
/// `module_paths` is a list of (import_path_string, path_segments) pairs.
/// Deduplicates: if the same module_path appears twice, it is loaded once.
pub fn load_modules(
    module_imports: &[(String, Vec<Box<str>>)],
    rules_path: &Path,
) -> Result<ModuleRegistry, Vec<ModuleLoadError>> {
    let mut registry = ModuleRegistry::new();
    let mut errors = Vec::new();

    for (module_key, path_segments) in module_imports {
        // Skip if already loaded (deduplication)
        if registry.modules.contains_key(module_key) {
            continue;
        }

        let file_path = resolve_module_path(path_segments, rules_path);

        match load_single_module(module_key, &file_path) {
            Ok(loaded) => {
                registry.modules.insert(module_key.clone(), Arc::new(loaded));
            }
            Err(e) => {
                errors.push(e);
            }
        }
    }

    if errors.is_empty() {
        Ok(registry)
    } else {
        Err(errors)
    }
}

fn load_single_module(module_path: &str, file_path: &Path) -> Result<LoadedModule, ModuleLoadError> {
    // Check file exists
    let metadata = std::fs::metadata(file_path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ModuleLoadError {
                module_path: module_path.to_string(),
                file_path: file_path.to_path_buf(),
                kind: ModuleLoadErrorKind::NotFound,
            }
        } else {
            ModuleLoadError {
                module_path: module_path.to_string(),
                file_path: file_path.to_path_buf(),
                kind: ModuleLoadErrorKind::ReadError(e),
            }
        }
    })?;

    // Check file size
    if metadata.len() > MAX_MODULE_FILE_SIZE {
        return Err(ModuleLoadError {
            module_path: module_path.to_string(),
            file_path: file_path.to_path_buf(),
            kind: ModuleLoadErrorKind::TooLarge(metadata.len()),
        });
    }

    // Read file
    let raw_bytes = std::fs::read(file_path).map_err(|e| ModuleLoadError {
        module_path: module_path.to_string(),
        file_path: file_path.to_path_buf(),
        kind: ModuleLoadErrorKind::ReadError(e),
    })?;

    // Strip UTF-8 BOM if present
    let bytes = if raw_bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        &raw_bytes[3..]
    } else {
        &raw_bytes
    };

    // Validate UTF-8
    let source = std::str::from_utf8(bytes).map_err(|_| ModuleLoadError {
        module_path: module_path.to_string(),
        file_path: file_path.to_path_buf(),
        kind: ModuleLoadErrorKind::NotUtf8,
    })?;

    // Parse
    let ModuleParseResult { module, errors, node_count } = Parser::parse_module(source);

    if !errors.is_empty() {
        return Err(ModuleLoadError {
            module_path: module_path.to_string(),
            file_path: file_path.to_path_buf(),
            kind: ModuleLoadErrorKind::ParseErrors(
                errors.iter().map(|e| e.message.clone()).collect(),
            ),
        });
    }

    // Check for duplicate function names
    let mut fn_names = HashMap::new();
    for f in &module.functions {
        if let Some(_prev) = fn_names.insert(&*f.name, ()) {
            return Err(ModuleLoadError {
                module_path: module_path.to_string(),
                file_path: file_path.to_path_buf(),
                kind: ModuleLoadErrorKind::DuplicateFunction(f.name.to_string()),
            });
        }
    }

    // Check for duplicate constant names
    let mut const_names = HashMap::new();
    for c in &module.constants {
        if let Some(_prev) = const_names.insert(&*c.name, ()) {
            return Err(ModuleLoadError {
                module_path: module_path.to_string(),
                file_path: file_path.to_path_buf(),
                kind: ModuleLoadErrorKind::DuplicateConstant(c.name.to_string()),
            });
        }
    }

    // Validate constant dependency graph (cycle detection, field reference rejection)
    if let Err(e) = cxl::module_eval::toposort_constants(&module.constants) {
        return Err(ModuleLoadError {
            module_path: module_path.to_string(),
            file_path: file_path.to_path_buf(),
            kind: ModuleLoadErrorKind::ConstantError(e.message),
        });
    }

    // Phase C: reject recursive function calls
    if let Err(e) = cxl::module_eval::check_recursive_calls(&module.functions) {
        return Err(ModuleLoadError {
            module_path: module_path.to_string(),
            file_path: file_path.to_path_buf(),
            kind: ModuleLoadErrorKind::RecursiveCall(e.message),
        });
    }

    Ok(LoadedModule { module, node_count })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_rules_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn write_module(rules_dir: &Path, rel_path: &str, content: &str) {
        let full_path = rules_dir.join(rel_path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&full_path, content).unwrap();
    }

    #[test]
    fn test_module_registry_dedup() {
        let dir = temp_rules_dir();
        write_module(dir.path(), "validators.cxl", "fn is_valid(x) = x > 0");

        let imports = vec![
            ("validators".to_string(), vec!["validators".into()]),
            ("validators".to_string(), vec!["validators".into()]),
        ];
        let registry = load_modules(&imports, dir.path()).unwrap();
        assert_eq!(registry.len(), 1);

        // Both lookups return the same Arc
        let m1 = registry.get("validators").unwrap();
        let m2 = registry.get("validators").unwrap();
        assert!(Arc::ptr_eq(m1, m2));
    }

    #[test]
    fn test_module_missing_file_error() {
        let dir = temp_rules_dir();
        let imports = vec![
            ("nonexistent".to_string(), vec!["nonexistent".into()]),
        ];
        let errors = load_modules(&imports, dir.path()).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].kind, ModuleLoadErrorKind::NotFound));
        let msg = errors[0].to_string();
        assert!(msg.contains("nonexistent.cxl"), "error should name expected path: {}", msg);
    }

    #[test]
    fn test_module_rules_path_cli_override() {
        let dir = temp_rules_dir();
        let custom_dir = dir.path().join("custom");
        fs::create_dir_all(&custom_dir).unwrap();
        write_module(&custom_dir, "validators.cxl", "fn check(x) = x > 0");

        let imports = vec![
            ("validators".to_string(), vec!["validators".into()]),
        ];
        let registry = load_modules(&imports, &custom_dir).unwrap();
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_module_use_nested_path_filesystem() {
        let dir = temp_rules_dir();
        write_module(dir.path(), "reporting/fiscal.cxl", "fn quarter(m) = m / 3 + 1");

        let imports = vec![
            ("reporting.fiscal".to_string(), vec!["reporting".into(), "fiscal".into()]),
        ];
        let registry = load_modules(&imports, dir.path()).unwrap();
        assert_eq!(registry.len(), 1);
        let m = registry.get("reporting.fiscal").unwrap();
        assert_eq!(m.module.functions.len(), 1);
    }

    #[test]
    fn test_module_deep_nested_path_filesystem() {
        let dir = temp_rules_dir();
        write_module(dir.path(), "shared/utils/string_helpers.cxl", "fn clean(v) = v.trim()");

        let imports = vec![
            ("shared.utils.string_helpers".to_string(), vec!["shared".into(), "utils".into(), "string_helpers".into()]),
        ];
        let registry = load_modules(&imports, dir.path()).unwrap();
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_module_fn_duplicate_name() {
        let dir = temp_rules_dir();
        write_module(dir.path(), "bad.cxl", "fn f(x) = x\nfn f(y) = y + 1");

        let imports = vec![("bad".to_string(), vec!["bad".into()])];
        let errors = load_modules(&imports, dir.path()).unwrap_err();
        assert!(matches!(errors[0].kind, ModuleLoadErrorKind::DuplicateFunction(ref n) if n == "f"));
    }

    #[test]
    fn test_module_const_duplicate_name() {
        let dir = temp_rules_dir();
        write_module(dir.path(), "bad.cxl", "let X = 1\nlet X = 2");

        let imports = vec![("bad".to_string(), vec!["bad".into()])];
        let errors = load_modules(&imports, dir.path()).unwrap_err();
        assert!(matches!(errors[0].kind, ModuleLoadErrorKind::DuplicateConstant(ref n) if n == "X"));
    }

    #[test]
    fn test_module_empty_file() {
        let dir = temp_rules_dir();
        write_module(dir.path(), "empty.cxl", "");

        let imports = vec![("empty".to_string(), vec!["empty".into()])];
        let registry = load_modules(&imports, dir.path()).unwrap();
        let m = registry.get("empty").unwrap();
        assert!(m.module.functions.is_empty());
        assert!(m.module.constants.is_empty());
    }

    #[test]
    fn test_module_constants_only_filesystem() {
        let dir = temp_rules_dir();
        write_module(dir.path(), "consts.cxl", "let A = 1\nlet B = 2");

        let imports = vec![("consts".to_string(), vec!["consts".into()])];
        let registry = load_modules(&imports, dir.path()).unwrap();
        let m = registry.get("consts").unwrap();
        assert!(m.module.functions.is_empty());
        assert_eq!(m.module.constants.len(), 2);
    }

    #[test]
    fn test_module_functions_only_filesystem() {
        let dir = temp_rules_dir();
        write_module(dir.path(), "fns.cxl", "fn add(a, b) = a + b\nfn neg(x) = -x");

        let imports = vec![("fns".to_string(), vec!["fns".into()])];
        let registry = load_modules(&imports, dir.path()).unwrap();
        let m = registry.get("fns").unwrap();
        assert_eq!(m.module.functions.len(), 2);
        assert!(m.module.constants.is_empty());
    }

    #[test]
    fn test_module_with_bom() {
        let dir = temp_rules_dir();
        let path = dir.path().join("bom.cxl");
        let mut content = vec![0xEF, 0xBB, 0xBF]; // UTF-8 BOM
        content.extend_from_slice(b"fn check(x) = x > 0");
        fs::write(&path, content).unwrap();

        let imports = vec![("bom".to_string(), vec!["bom".into()])];
        let registry = load_modules(&imports, dir.path()).unwrap();
        assert_eq!(registry.get("bom").unwrap().module.functions.len(), 1);
    }

    #[test]
    fn test_resolve_module_path_simple() {
        let p = resolve_module_path(&["validators".into()], Path::new("/rules"));
        assert_eq!(p, PathBuf::from("/rules/validators.cxl"));
    }

    #[test]
    fn test_resolve_module_path_nested() {
        let p = resolve_module_path(&["reporting".into(), "fiscal".into()], Path::new("/rules"));
        assert_eq!(p, PathBuf::from("/rules/reporting/fiscal.cxl"));
    }
}
