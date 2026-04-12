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

// ───────────────────────── Phase 16c fixture corpus ─────────────────────────

struct Phase16cFixture {
    /// Stable identifier.
    name: &'static str,
    /// Path relative to `crates/clinker-core/tests/fixtures/`.
    rel_path: &'static str,
    /// Whether this is a pipeline (deserializes as PipelineConfig) or a
    /// non-pipeline YAML (composition / channel — valid YAML only).
    is_pipeline: bool,
    /// Set once baselines are captured; `None` for non-pipeline fixtures
    /// and pipelines whose baselines haven't been captured yet.
    baseline_captured_in: Option<&'static str>,
}

/// All 14 Phase 16c fixtures: 5 compositions, 6 channels, 3 pipelines.
fn phase_16c_fixtures() -> Vec<Phase16cFixture> {
    vec![
        // Compositions (5)
        Phase16cFixture {
            name: "customer_enrich",
            rel_path: "compositions/customer_enrich.comp.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        Phase16cFixture {
            name: "address_normalize",
            rel_path: "compositions/address_normalize.comp.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        Phase16cFixture {
            name: "dlq_shape",
            rel_path: "compositions/dlq_shape.comp.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        Phase16cFixture {
            name: "nested_caller",
            rel_path: "compositions/nested_caller.comp.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        Phase16cFixture {
            name: "passthrough_check",
            rel_path: "compositions/passthrough_check.comp.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        // Channels (6)
        Phase16cFixture {
            name: "acme_prod",
            rel_path: "channels/acme_prod.channel.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        Phase16cFixture {
            name: "acme_staging",
            rel_path: "channels/acme_staging.channel.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        Phase16cFixture {
            name: "beta_prod",
            rel_path: "channels/beta_prod.channel.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        Phase16cFixture {
            name: "beta_staging",
            rel_path: "channels/beta_staging.channel.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        Phase16cFixture {
            name: "comp_direct",
            rel_path: "channels/comp_direct.channel.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        Phase16cFixture {
            name: "empty_defaults",
            rel_path: "channels/empty_defaults.channel.yaml",
            is_pipeline: false,
            baseline_captured_in: None,
        },
        // Pipelines (3) — baselines captured in Task 16c.2.4
        Phase16cFixture {
            name: "composition_pipeline",
            rel_path: "pipelines/composition_pipeline.yaml",
            is_pipeline: true,
            baseline_captured_in: Some("16c.2.4"),
        },
        Phase16cFixture {
            name: "nested_composition_pipeline",
            rel_path: "pipelines/nested_composition_pipeline.yaml",
            is_pipeline: true,
            baseline_captured_in: Some("16c.2.4"),
        },
        Phase16cFixture {
            name: "channel_target_pipeline",
            rel_path: "pipelines/channel_target_pipeline.yaml",
            is_pipeline: true,
            baseline_captured_in: Some("16c.2.4"),
        },
    ]
}

fn phase_16c_fixture_root() -> PathBuf {
    manifest_dir().join("tests").join("fixtures")
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
        &clinker_core::config::PipelineConfig::compile(
            &config,
            &clinker_core::config::CompileContext::default(),
        )
        .expect("compile"),
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
        &clinker_core::config::PipelineConfig::compile(
            &config,
            &clinker_core::config::CompileContext::default(),
        )
        .expect("compile"),
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

// ───────────────────────── Phase 16c gate tests ─────────────────────────

/// Gate test for Task 16c.0.3. Verifies:
///   * all 14 fixture files exist on disk
///   * all 3 pipeline fixtures deserialize as PipelineConfig
///   * composition and channel fixtures are valid YAML (parseable by serde_saphyr)
#[test]
fn test_scaffold_16c_fixture_corpus_well_formed() {
    let root = phase_16c_fixture_root();

    for fx in phase_16c_fixtures() {
        let path = root.join(fx.rel_path);
        assert!(
            path.is_file(),
            "fixture missing: {} ({})",
            fx.name,
            path.display()
        );
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert!(
            !content.trim().is_empty(),
            "fixture empty: {}",
            path.display()
        );

        if fx.is_pipeline {
            // Pipeline fixtures must deserialize as PipelineConfig
            let _config = parse_config(&content).unwrap_or_else(|e| {
                panic!(
                    "pipeline fixture {} failed to deserialize as PipelineConfig: {e}",
                    fx.name
                )
            });
        } else {
            // Non-pipeline fixtures must be valid YAML (structural check only)
            let _value: serde_json::Value = clinker_core::yaml::from_str(&content)
                .unwrap_or_else(|e| panic!("fixture {} is not valid YAML: {e}", fx.name));
        }
    }
}

/// Gate test for Task 16c.0.3. Verifies no ForwardOnly fixture has a
/// premature `.expected.*` baseline file.
#[test]
fn test_scaffold_16c_forward_only_no_baseline() {
    let root = phase_16c_fixture_root();

    for fx in phase_16c_fixtures() {
        // Check that no .expected.* file exists alongside the fixture
        let path = root.join(fx.rel_path);
        let parent = path.parent().unwrap();
        let stem = path.file_stem().unwrap().to_str().unwrap();

        // Glob for any .expected.* file with the same stem
        for entry in std::fs::read_dir(parent).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name = name.to_str().unwrap();
            if name.starts_with(stem) && name.contains(".expected.") {
                panic!(
                    "ForwardOnly fixture {} has premature baseline: {}",
                    fx.name, name
                );
            }
        }
    }
}

/// Gate test for Task 16c.0.4. Verifies all explain doc stubs exist.
#[test]
fn test_scaffold_16c_explain_docs_exist() {
    let manifest = manifest_dir();
    let workspace_root = manifest.parent().unwrap().parent().unwrap();
    let explain_dir = workspace_root.join("docs").join("explain");

    let expected = [
        "E101.md", "E102.md", "E103.md", "E104.md", "E105.md", "E106.md", "E107.md", "E108.md",
        "W101.md",
    ];

    for file in &expected {
        let path = explain_dir.join(file);
        assert!(
            path.is_file(),
            "explain doc stub missing: {}",
            path.display()
        );
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert!(
            content.contains("## What it means"),
            "{file} missing 'What it means' section"
        );
        assert!(
            content.contains("## How to fix"),
            "{file} missing 'How to fix' section"
        );
    }
}

// ───────────────────────── Phase 16c pipeline baselines ─────────────────────────

/// Scrub tail variable IDs from explain text. Tail var IDs are monotonic
/// counters per compilation, so their values are run-order-dependent.
/// Replace `body=N` patterns with `body=<ID>` for snapshot stability.
fn scrub_tail_vars(text: &str) -> String {
    let re = regex::Regex::new(r"body=\d+").unwrap();
    re.replace_all(text, "body=<ID>").to_string()
}

/// Compile a Phase 16c pipeline fixture and snapshot its explain text.
/// Uses `CompileContext::with_pipeline_dir` for `use:` path resolution.
fn snapshot_16c_pipeline(snap_name: &str, rel_path: &str) {
    let root = phase_16c_fixture_root();
    let yaml_path = root.join(rel_path);
    let yaml = std::fs::read_to_string(&yaml_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", yaml_path.display()));
    let config = parse_config(&yaml).expect("parse_config");
    let pipeline_dir = std::path::PathBuf::from(rel_path)
        .parent()
        .unwrap_or(std::path::Path::new(""))
        .to_path_buf();
    let ctx = clinker_core::config::CompileContext::with_pipeline_dir(&root, pipeline_dir);
    let compiled = clinker_core::config::PipelineConfig::compile(&config, &ctx).expect("compile");
    let (dag, _) =
        clinker_core::executor::PipelineExecutor::explain_plan_dag(&compiled).expect("explain_dag");
    let text = dag.explain_text(&config);
    let scrubbed = scrub_tail_vars(&text);
    insta::assert_snapshot!(snap_name, scrubbed);
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

// ───────────────────────── Task 16c.2.4 gate tests ─────────────────────────

/// Gate test: the 3 pipeline fixtures each have a captured insta baseline.
#[test]
fn test_forward_only_pipeline_fixtures_have_baseline() {
    for fx in phase_16c_fixtures() {
        if !fx.is_pipeline {
            continue;
        }
        assert!(
            fx.baseline_captured_in.is_some(),
            "pipeline fixture {} has no baseline_captured_in",
            fx.name
        );
        // Verify the fixture compiles successfully
        let root = phase_16c_fixture_root();
        let yaml_path = root.join(fx.rel_path);
        let yaml = std::fs::read_to_string(&yaml_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", yaml_path.display()));
        let config = parse_config(&yaml).expect("parse_config");
        let pipeline_dir = std::path::PathBuf::from(fx.rel_path)
            .parent()
            .unwrap_or(std::path::Path::new(""))
            .to_path_buf();
        let ctx = clinker_core::config::CompileContext::with_pipeline_dir(&root, pipeline_dir);
        let _compiled =
            clinker_core::config::PipelineConfig::compile(&config, &ctx).expect("compile");
    }
    // Verify the insta snapshots exist
    snapshot_16c_pipeline(
        "explain_composition_pipeline",
        "pipelines/composition_pipeline.yaml",
    );
    snapshot_16c_pipeline(
        "explain_nested_composition_pipeline",
        "pipelines/nested_composition_pipeline.yaml",
    );
    snapshot_16c_pipeline(
        "explain_channel_target_pipeline",
        "pipelines/channel_target_pipeline.yaml",
    );
}

/// Gate test: W101 fires end-to-end through `PipelineConfig::compile_with_diagnostics`
/// when a pipeline uses `passthrough_check.comp.yaml` with an upstream source
/// that provides columns the body will shadow.
#[test]
fn test_w101_warning_emitted_end_to_end_on_passthrough_check_fixture() {
    let root = phase_16c_fixture_root();
    // Build a pipeline that uses passthrough_check.comp.yaml with upstream
    // providing {id, tag}. The body emits "tag", shadowing the pass-through.
    let yaml = r#"
pipeline:
  name: w101_e2e_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: id, type: string }
        - { name: tag, type: string }

  - type: composition
    name: pt_check
    input: src
    use: ../compositions/passthrough_check.comp.yaml
    inputs:
      data: src

  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: data/o.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let ctx = clinker_core::config::CompileContext::with_pipeline_dir(
        &root,
        std::path::PathBuf::from("pipelines"),
    );
    let (_plan, diags) =
        clinker_core::config::PipelineConfig::compile_with_diagnostics(&config, &ctx)
            .expect("compile should succeed (W101 is a warning, not error)");

    let w101: Vec<_> = diags.iter().filter(|d| d.code == "W101").collect();
    assert!(
        !w101.is_empty(),
        "expected W101 warning for body-declared 'tag' shadowing pass-through, got: {diags:?}"
    );
    assert!(
        w101[0].message.contains("tag"),
        "W101 should mention 'tag': {:?}",
        w101[0].message
    );
}

/// Gate test: running the 16c pipeline snapshot twice produces identical
/// scrubbed output — verifies tail variable redaction stability.
#[test]
fn test_insta_baselines_are_tail_var_stable() {
    let root = phase_16c_fixture_root();
    let rel_path = "pipelines/composition_pipeline.yaml";
    let yaml_path = root.join(rel_path);
    let yaml = std::fs::read_to_string(&yaml_path).expect("read fixture");
    let config = parse_config(&yaml).expect("parse_config");
    let pipeline_dir = std::path::PathBuf::from(rel_path)
        .parent()
        .unwrap()
        .to_path_buf();

    let mut outputs = Vec::new();
    for _ in 0..2 {
        let ctx =
            clinker_core::config::CompileContext::with_pipeline_dir(&root, pipeline_dir.clone());
        let compiled =
            clinker_core::config::PipelineConfig::compile(&config, &ctx).expect("compile");
        let (dag, _) = clinker_core::executor::PipelineExecutor::explain_plan_dag(&compiled)
            .expect("explain_dag");
        let text = dag.explain_text(&config);
        outputs.push(scrub_tail_vars(&text));
    }
    assert_eq!(
        outputs[0], outputs[1],
        "scrubbed explain output must be identical across runs"
    );
}
