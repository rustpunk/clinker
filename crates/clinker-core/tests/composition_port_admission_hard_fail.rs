//! Composition port-admission diagnostic surfacing.
//!
//! Counterpart to `nested_composition_hard_fail.rs`. Where that test
//! pins the body-interior shape (wrapped in
//! `PipelineError::CompositionBodyError`), this one pins the
//! boundary-admit shape: when the engine clones records from the
//! parent producer's `node_buffers` slot into the composition
//! body's input port, the clone admission charges the **shared**
//! `MemoryArbitrator` (compositions do not get their own budget — body
//! operators share the parent pipeline's instance). If the doubled
//! charge crosses the hard limit, the dispatcher returns a bare
//! `PipelineError::MemoryBudgetExceeded` whose `node` field carries
//! the call-site composition name and whose `detail` discriminates
//! the site from the generic `node_buffer admission` string used by
//! `admit_node_buffer`.
//!
//! The two-path split between this shape and `CompositionBodyError`
//! is documented on the variants in `crates/clinker-core/src/error.rs`
//! and at the admit sites in `crates/clinker-core/src/executor/
//! dispatch.rs` — it is a *diagnostic* split (which name the user
//! sees), not an enforcement split. The explicit
//! `CompositionBodyError` arm in this test's assertion defends the
//! split against drift: future readers who silently wrap the
//! boundary admit will see this test fail with a pointed message.
//!
//! Per-row admission cost is `size_of::<Value>() * cols +
//! size_of::<(Record, u64)>()` (see `estimate_node_buffer_bytes` in
//! `executor/dispatch.rs`); cols=1 for the single-column passthrough
//! schema yields ~168 B/row. 5_000 rows give an upstream admission
//! charge of ~840 KiB (under the 1 MiB hard limit) and a clone
//! charge of another ~840 KiB on top, placing the hard-fail at the
//! `collect_port_records` clone admission before the body's first
//! operator runs.

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, PipelineConfig};
use clinker_core::error::PipelineError;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use clinker_core::pipeline::memory::BudgetCategory;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

const PIPELINE_YAML: &str = r#"
pipeline:
  name: composition_port_admission_hard_fail
  memory: { limit: "1M" }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: id, type: string }
  - type: composition
    name: port_enrich_call
    input: src
    use: ../compositions/passthrough_check.comp.yaml
    inputs:
      data: src
  - type: output
    name: out
    input: port_enrich_call
    config:
      name: out
      type: csv
      path: out.csv
"#;

const TOTAL_ROWS: usize = 5_000;

fn build_src_csv() -> String {
    let mut s = String::with_capacity(TOTAL_ROWS * 16);
    s.push_str("id\n");
    for i in 1..=TOTAL_ROWS as u64 {
        s.push_str(&format!("id_{i}\n"));
    }
    s
}

fn fixture_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

#[tokio::test(flavor = "multi_thread")]
async fn port_admission_hard_fail_is_bare_with_call_site_name() {
    let csv = build_src_csv();
    let config: PipelineConfig =
        clinker_core::yaml::from_str(PIPELINE_YAML).expect("parse pipeline YAML");
    let ctx =
        CompileContext::with_pipeline_dir(fixture_workspace_root(), PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile pipeline");

    let mut readers: clinker_core::executor::SourceReaders = HashMap::new();
    readers.insert(
        "src".to_string(),
        clinker_core::executor::single_file_reader(
            "src.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    );

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "composition-port-admission-hard-fail".to_string(),
        batch_id: "batch-0".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let err = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .await
        .expect_err("1 MiB budget must abort the port-records clone admission");

    match err {
        PipelineError::MemoryBudgetExceeded {
            node,
            source,
            used,
            limit,
            detail,
        } => {
            assert_eq!(
                node, "port_enrich_call",
                "boundary admit must name the user-visible composition call-site, \
                 not a body-internal operator",
            );
            assert!(
                matches!(source, BudgetCategory::NodeBuffer),
                "port admission must surface under NodeBuffer category; got {source:?}",
            );
            assert_eq!(
                detail.as_deref(),
                Some("composition port-records clone admission"),
                "detail string must discriminate the port-clone site from \
                 the generic `node_buffer admission` used by admit_node_buffer",
            );
            assert!(
                used > limit,
                "reported used ({used}) must exceed limit ({limit}) at hard-fail",
            );
        }
        PipelineError::CompositionBodyError { .. } => {
            panic!(
                "port-admission path must NOT be wrapped in CompositionBodyError — \
                 the two-path diagnostic model documents this split (see \
                 error.rs::CompositionBodyError doc-comment and \
                 executor/dispatch.rs::collect_port_records). Wrapping the \
                 boundary admit would put the call-site name in both the \
                 wrapper and the inner `node` field with no information gain.",
            );
        }
        other => panic!("expected bare MemoryBudgetExceeded; got: {other:?}"),
    }
}
