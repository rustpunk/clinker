//! Composition diagnostic surfacing for body-interior hard-fail.
//!
//! Mirrors the `diamond_hard_fail` topology but moves it inside a
//! composition body. The parent pipeline calls the body via a
//! `Composition` node; the body's interior Transform widens the
//! schema and fans out to two consumers, so producer-side spill is
//! disabled and the admit's counter charge crosses the hard limit.
//!
//! The body's executor wraps the inner `MemoryBudgetExceeded` as
//! `PipelineError::CompositionBodyError { composition_name, inner }`
//! at `execute_composition_body`. The user-visible failure
//! location is therefore the composition's *call-site* name in the
//! parent pipeline — `enrich_call` — even though the actual budget
//! breach happened inside the body's `stage_split` Transform. The
//! test destructures both the wrapper and the inner so the
//! diagnostic chain is pinned in both directions.

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
  name: nested_composition_hard_fail
  memory: { limit: "1M" }
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      schema:
        - { name: id, type: string }
        - { name: region, type: string }
        - { name: value, type: int }
  - type: composition
    name: enrich_call
    input: events
    use: ../compositions/issue_123_nested_hard_fail.comp.yaml
    inputs:
      records: events
  - type: output
    name: out
    input: enrich_call
    config:
      name: out
      type: csv
      path: out.csv
"#;

const TOTAL_ROWS: usize = 2_000;

fn build_events_csv() -> String {
    let mut s = String::with_capacity(TOTAL_ROWS * 32);
    s.push_str("id,region,value\n");
    for i in 1..=TOTAL_ROWS as u64 {
        let region = match i % 3 {
            0 => 'a',
            1 => 'b',
            _ => 'c',
        };
        s.push_str(&format!("id_{i},{region},{i}\n"));
    }
    s
}

fn fixture_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_composition_hard_fail_names_the_call_site() {
    let csv = build_events_csv();
    let config: PipelineConfig =
        clinker_core::yaml::from_str(PIPELINE_YAML).expect("parse pipeline YAML");
    let ctx =
        CompileContext::with_pipeline_dir(fixture_workspace_root(), PathBuf::from("pipelines"));
    let plan = config.compile(&ctx).expect("compile pipeline");

    let mut readers: clinker_core::executor::SourceReaders = HashMap::new();
    readers.insert(
        "events".to_string(),
        clinker_core::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    );

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "nested-composition-hard-fail".to_string(),
        batch_id: "batch-0".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let err = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .await
        .expect_err("1 MiB budget must abort the body's widening admit");

    match err {
        PipelineError::CompositionBodyError {
            composition_name,
            inner,
        } => {
            assert_eq!(
                composition_name, "enrich_call",
                "composition wrapper must name the call-site, not the body interior",
            );
            match *inner {
                PipelineError::MemoryBudgetExceeded {
                    node,
                    source,
                    used,
                    limit,
                    ..
                } => {
                    assert_eq!(
                        node, "stage_split",
                        "inner hard-fail must name the body's widening producer",
                    );
                    assert!(
                        matches!(source, BudgetCategory::NodeBuffer),
                        "inner hard-fail must surface as NodeBuffer category; got {source:?}",
                    );
                    assert!(
                        used > limit,
                        "reported used ({used}) must exceed limit ({limit}) at hard-fail",
                    );
                }
                other => panic!("expected inner MemoryBudgetExceeded; got: {other:?}"),
            }
        }
        other => panic!("expected outer CompositionBodyError; got: {other:?}"),
    }
}
