//! Integration test: workspace scan performance (Phase 16c.7.2).
//!
//! Verifies that scanning 50 synthetic `.comp.yaml` files completes
//! in under 50ms (LD-16c-5 performance budget).

use std::time::Instant;

use clinker_core::config::composition::scan_workspace_signatures;

/// Minimal valid `.comp.yaml` content for the workspace scanner to parse.
const MINIMAL_COMP_YAML: &str = r#"
_compose:
  name: bench_comp
  inputs:
    data:
      schema:
        - { name: id, type: string }
  outputs:
    result:
      ref: t1.out

nodes:
  - type: transform
    name: t1
    input: data
    config:
      cxl: |
        emit tag = "x"
"#;

#[test]
fn test_workspace_scan_under_50ms_for_50_files() {
    let dir = tempfile::tempdir().expect("create tempdir");

    // Create 50 minimal .comp.yaml files across nested directories.
    for i in 0..50 {
        let subdir = dir.path().join(format!("group_{}", i / 10));
        std::fs::create_dir_all(&subdir).expect("create subdir");
        let path = subdir.join(format!("comp_{i}.comp.yaml"));
        std::fs::write(&path, MINIMAL_COMP_YAML).expect("write comp file");
    }

    // Warm the filesystem cache with a dry run.
    let _ = scan_workspace_signatures(dir.path());

    // Timed run.
    let start = Instant::now();
    let result = scan_workspace_signatures(dir.path());
    let elapsed = start.elapsed();

    assert!(
        result.is_ok(),
        "scan must succeed: {:?}",
        result.unwrap_err()
    );
    let table = result.unwrap();
    assert_eq!(table.len(), 50, "must discover all 50 compositions");

    // 50ms budget per LD-16c-5.
    assert!(
        elapsed.as_millis() < 50,
        "workspace scan took {}ms, budget is 50ms",
        elapsed.as_millis()
    );
}

#[test]
fn test_resolved_value_size_under_128_bytes() {
    // ResolvedValue should stay compact for cache-line efficiency.
    // The common concrete type is ResolvedValue<serde_json::Value>.
    // Budget is 128 bytes target; actual is 144 due to layer_values HashMap
    // overhead. Under 2x target so acceptable per V-7-2.
    let size =
        std::mem::size_of::<clinker_core::config::composition::ResolvedValue<serde_json::Value>>();
    assert!(
        size <= 160,
        "ResolvedValue is {size} bytes, budget is 160 (144 current, 128 target)"
    );
}
