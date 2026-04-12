use clinker_bench_support::workspace_root;
use clinker_core::config::parse_config;
use std::fs;

/// Validates every YAML pipeline config in benches/pipelines/ (except future/)
/// parses successfully with parse_config(). Catches YAML syntax issues and
/// config validation failures early.
#[test]
fn test_all_bench_pipeline_configs_parse() {
    let root = workspace_root().join("benches/pipelines");
    let mut count = 0;
    for entry in glob::glob(root.join("**/*.yaml").to_str().unwrap()).unwrap() {
        let path = entry.unwrap();
        if path.components().any(|c| c.as_os_str() == "future") {
            continue;
        }
        let yaml = fs::read_to_string(&path).unwrap();
        parse_config(&yaml).unwrap_or_else(|e| {
            panic!("Failed to parse {}: {e}", path.display());
        });
        count += 1;
    }
    assert!(
        count > 0,
        "No pipeline configs found — check workspace_root()"
    );
}

/// Verifies that a deliberately-broken sentinel YAML in future/ does NOT cause
/// the harness to fail. Guards against glob skip regression.
#[test]
fn test_future_dir_excluded_from_parse_harness() {
    let root = workspace_root().join("benches/pipelines/future");
    let mut future_count = 0;
    for entry in glob::glob(root.join("*.yaml").to_str().unwrap()).unwrap() {
        let _path = entry.unwrap();
        future_count += 1;
    }
    assert!(
        future_count > 0,
        "Expected at least one YAML in future/ to prove exclusion works"
    );
}
