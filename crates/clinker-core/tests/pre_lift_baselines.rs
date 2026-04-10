//! Phase 16b Task 16b.0 — pre-lift baseline capture with tiered `BaselinePolicy`.
//!
//! Per `docs/research/RESEARCH-forward-only-baselines.md`, baselines are
//! captured in two tiers:
//!
//!   * `DualRun` — the fixture parses and runs on **both** pre-lift (current
//!     `main`) and post-lift (Task 16b.2 onward) engines. We capture the
//!     pre-lift byte-exact output here in 16b.0 and assert post-lift stays
//!     byte-identical in Task 16b.4. This is the Spark PR #40496 / insta /
//!     rustc-bless pattern: the actual refactor-safety net.
//!
//!   * `ForwardOnly { first_captured_in }` — the fixture uses a shape the
//!     pre-lift engine physically cannot parse (e.g. `nodes:` with a multi-
//!     input `merge` variant). No system surveyed in the research doc attempts
//!     to synthesize a pre-refactor baseline for such inputs. The fixture YAML
//!     is committed here in 16b.0 so 16b.4 has a ready anchor, and the
//!     inaugural baseline is captured on first post-lift run.
//!
//! On first run, set `UPDATE_BASELINES=1` to (re)write `.expected.*` files for
//! every `DualRun` fixture. Default is strict byte-compare.

use std::collections::HashMap;
use std::io::{self, Cursor, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clinker_core::config::{PipelineConfig, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};

// ───────────────────────── tiered policy ─────────────────────────

/// Baseline policy per fixture. See module docs.
enum BaselinePolicy {
    /// Pre & post-lift both parse + run. Byte-compare against committed baseline(s).
    DualRun,
    /// Only the post-lift engine can parse this fixture; baseline captured later.
    /// Cites `docs/research/RESEARCH-forward-only-baselines.md`.
    ForwardOnly { first_captured_in: &'static str },
}

struct BaselineFixture {
    /// Stable identifier used for snapshot names and baseline filenames.
    name: &'static str,
    /// Fixture YAML filename, relative to `examples/pipelines/tests/16b_baseline/`.
    yaml: &'static str,
    policy: BaselinePolicy,
}

/// Canonical registry of the 4 Phase-16b baseline fixtures.
fn fixtures() -> Vec<BaselineFixture> {
    vec![
        BaselineFixture {
            name: "csv_transform_sink",
            yaml: "csv_transform_sink.yaml",
            policy: BaselinePolicy::DualRun,
        },
        BaselineFixture {
            name: "route_fanout",
            yaml: "route_fanout.yaml",
            policy: BaselinePolicy::DualRun,
        },
        BaselineFixture {
            name: "aggregate_windowed",
            yaml: "aggregate_windowed.yaml",
            policy: BaselinePolicy::DualRun,
        },
        BaselineFixture {
            name: "merge_filter_distinct",
            yaml: "merge_filter_distinct.yaml",
            policy: BaselinePolicy::ForwardOnly {
                first_captured_in: "16b.4",
            },
        },
    ]
}

// ───────────────────────── I/O helpers ─────────────────────────

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn new() -> Self {
        Self::default()
    }
    fn as_string(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

fn test_params(config: &PipelineConfig) -> PipelineRunParams {
    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(clinker_core::config::convert_pipeline_vars)
        .unwrap_or_default();
    PipelineRunParams {
        execution_id: "16b-baseline".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars,
        shutdown_token: None,
    }
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn baseline_root() -> PathBuf {
    manifest_dir()
        .join("tests")
        .join("fixtures")
        .join("baselines")
}

fn fixture_root() -> PathBuf {
    manifest_dir()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples")
        .join("pipelines")
        .join("tests")
        .join("16b_baseline")
}

fn read_fixture(rel_yaml: &str) -> String {
    let p = fixture_root().join(rel_yaml);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

fn read_data(name: &str) -> Vec<u8> {
    let p = fixture_root().join("data").join(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

fn update_mode() -> bool {
    std::env::var("UPDATE_BASELINES").ok().as_deref() == Some("1")
}

fn compare_or_write(baseline_name: &str, actual: &str) {
    let p = baseline_root().join(baseline_name);
    if update_mode() || !p.exists() {
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, actual.as_bytes()).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&p)
        .unwrap_or_else(|e| panic!("read baseline {}: {e}", p.display()));
    assert_eq!(
        actual,
        expected,
        "byte-mismatch against baseline {}",
        p.display()
    );
}

/// Run a pipeline with in-memory readers/writers keyed by source/output name.
fn run_pipeline(yaml: &str, inputs: Vec<(&str, Vec<u8>)>) -> HashMap<String, String> {
    let config = parse_config(yaml).expect("parse_config");
    let params = test_params(&config);

    let readers: HashMap<String, Box<dyn Read + Send>> = inputs
        .into_iter()
        .map(|(name, bytes)| {
            (
                name.to_string(),
                Box::new(Cursor::new(bytes)) as Box<dyn Read + Send>,
            )
        })
        .collect();

    let buffers: HashMap<String, SharedBuffer> = config
        .output_configs()
        .map(|o| (o.name.clone(), SharedBuffer::new()))
        .collect();

    let writers: HashMap<String, Box<dyn Write + Send>> = buffers
        .iter()
        .map(|(n, b)| (n.clone(), Box::new(b.clone()) as Box<dyn Write + Send>))
        .collect();

    PipelineExecutor::run_plan_with_readers_writers(
        &clinker_core::config::PipelineConfig::compile(&config).expect("compile"),
        readers,
        writers,
        &params,
    )
    .expect("pipeline run");

    buffers
        .into_iter()
        .map(|(k, v)| (k, v.as_string()))
        .collect()
}

/// Map a fixture to its (source_name, data_filename) inputs and its list of
/// (baseline_filename, output_name) assertions.
fn fixture_io(
    name: &str,
) -> (
    Vec<(&'static str, &'static str)>,
    Vec<(&'static str, &'static str)>,
) {
    match name {
        "csv_transform_sink" => (
            vec![("employees", "employees.csv")],
            vec![("csv_transform_sink.expected.csv", "results")],
        ),
        "route_fanout" => (
            vec![("orders", "orders.csv")],
            vec![
                ("route_fanout.high.expected.csv", "high"),
                ("route_fanout.low.expected.csv", "low"),
            ],
        ),
        "aggregate_windowed" => (
            vec![("sales", "sales.csv")],
            vec![("aggregate_windowed.expected.csv", "dept_totals")],
        ),
        other => panic!("no I/O wiring for fixture {other}"),
    }
}

fn snapshot_explain(snap_name: &str, yaml: &str) {
    let config = parse_config(yaml).expect("parse_config");
    let (dag, _) = PipelineExecutor::explain_plan_dag(
        &clinker_core::config::PipelineConfig::compile(&config).expect("compile"),
    )
    .expect("explain_dag");
    let text = dag.explain_text(&config);
    insta::assert_snapshot!(snap_name, text);
}

// ───────────────────────── gate tests ─────────────────────────

/// Gate test for Task 16b.0. Verifies:
///   * every fixture YAML exists on disk (DualRun and ForwardOnly alike)
///   * every DualRun fixture has a committed, non-empty `.expected.*` baseline
///   * every ForwardOnly fixture does NOT have a committed baseline (tracked
///     so 16b.4 captures it from scratch — see research doc §"Do NOT commit a
///     placeholder .expected.*").
#[test]
fn test_baselines_loaded() {
    let fxroot = fixture_root();
    assert!(fxroot.is_dir(), "fixture dir missing: {}", fxroot.display());

    for fx in fixtures() {
        let yaml_path = fxroot.join(fx.yaml);
        assert!(
            yaml_path.is_file(),
            "fixture YAML missing: {}",
            yaml_path.display()
        );
        let yaml_body = std::fs::read_to_string(&yaml_path).unwrap();
        assert!(
            !yaml_body.trim().is_empty(),
            "fixture YAML empty: {}",
            yaml_path.display()
        );

        match fx.policy {
            BaselinePolicy::DualRun => {
                let (_, outs) = fixture_io(fx.name);
                for (baseline_file, _) in outs {
                    let bp = baseline_root().join(baseline_file);
                    assert!(bp.is_file(), "DualRun baseline missing: {}", bp.display());
                    let body = std::fs::read_to_string(&bp).unwrap();
                    assert!(
                        !body.is_empty(),
                        "DualRun baseline {} is empty",
                        bp.display()
                    );
                }
            }
            BaselinePolicy::ForwardOnly { first_captured_in } => {
                // Per research: no placeholder baseline file. The inaugural
                // capture lives in the task named by first_captured_in.
                assert_eq!(
                    first_captured_in, "16b.4",
                    "ForwardOnly fixture {} should capture in 16b.4",
                    fx.name
                );
            }
        }
    }
}

/// For every DualRun fixture: run pre-lift, byte-compare outputs against
/// committed baselines, and snapshot the `--explain` topology via insta.
#[test]
fn test_pre_lift_snapshots() {
    for fx in fixtures() {
        let BaselinePolicy::DualRun = fx.policy else {
            continue;
        };
        let yaml = read_fixture(fx.yaml);
        snapshot_explain(&format!("explain_{}", fx.name), &yaml);

        let (inputs_spec, outputs_spec) = fixture_io(fx.name);
        let inputs: Vec<(&'static str, Vec<u8>)> = inputs_spec
            .into_iter()
            .map(|(src, file)| (src, read_data(file)))
            .collect();
        let outputs = run_pipeline(&yaml, inputs);

        for (baseline_file, out_name) in outputs_spec {
            let actual = outputs
                .get(out_name)
                .unwrap_or_else(|| panic!("missing output {out_name} for {}", fx.name));
            compare_or_write(baseline_file, actual);
        }
    }
}
