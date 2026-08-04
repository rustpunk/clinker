//! Source-scoped identity coverage for failure evidence and retraction state.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    DlqEntry, ExecutionReport, PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders,
    SourceRowId,
};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::plan::CompiledPlan;

fn compile_failure_pipeline(granularity: &str, memory_limit: &str) -> CompiledPlan {
    let yaml = format!(
        r#"
pipeline:
  name: source_row_identity_retraction
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      dlq_granularity: {granularity}
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
        - {{ name: note, type: string }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      dlq_granularity: {granularity}
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
        - {{ name: note, type: string }}
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: transform
    name: validate
    input: merged
    config:
      cxl: |
        emit parsed = value.to_int()
  - type: output
    name: out
    input: validate
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#,
    );
    parse_config(&yaml)
        .expect("failure pipeline parses")
        .compile(&CompileContext::default())
        .expect("failure pipeline compiles")
}

fn slot(path: &str, csv: String) -> FileSlot {
    FileSlot::new(PathBuf::from(path), Box::new(Cursor::new(csv.into_bytes())))
}

fn run_failure_pipeline(
    plan: &CompiledPlan,
    src_a: String,
    src_b: String,
) -> (ExecutionReport, String) {
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            SourceInput::Files(vec![slot("a.csv", src_a)]),
        ),
        (
            "src_b".to_string(),
            SourceInput::Files(vec![slot("b.csv", src_b)]),
        ),
    ]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "source-row-identity-retraction".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(plan, readers, writers, &params)
        .expect("failure pipeline executes");
    (report, output.as_string())
}

fn small_source(prefix: &str, failing: bool) -> String {
    let first = if failing { "bad" } else { "10" };
    format!("id,value,note\n{prefix}1,{first},first\n{prefix}2,20,second\n")
}

fn identities_by_source(entries: &[DlqEntry]) -> BTreeMap<String, BTreeSet<SourceRowId>> {
    let mut identities = BTreeMap::<String, BTreeSet<SourceRowId>>::new();
    for entry in entries {
        identities
            .entry(entry.source_name.to_string())
            .or_default()
            .insert(entry.source_row);
    }
    identities
}

#[test]
fn dlq_row_and_document_evidence_distinguish_same_ordinals() {
    let row_plan = compile_failure_pipeline("record", "1G");
    let (row_report, _) =
        run_failure_pipeline(&row_plan, small_source("a", true), small_source("b", true));
    assert_eq!(row_report.dlq_entries.len(), 2);
    assert!(row_report.dlq_entries.iter().all(|entry| entry.trigger));
    let row_ids = identities_by_source(&row_report.dlq_entries);
    assert_eq!(row_ids["src_a"].len(), 1);
    assert_eq!(row_ids["src_b"].len(), 1);
    let a_row = *row_ids["src_a"].first().expect("src_a row identity");
    let b_row = *row_ids["src_b"].first().expect("src_b row identity");
    assert_eq!(a_row.ordinal(), b_row.ordinal());
    assert_ne!(a_row.source(), b_row.source());

    let document_plan = compile_failure_pipeline("document", "1G");
    let (document_report, output) = run_failure_pipeline(
        &document_plan,
        small_source("a", true),
        small_source("b", true),
    );
    assert_eq!(document_report.dlq_entries.len(), 4);
    assert_eq!(document_report.counters.ok_count, 0);
    assert!(output.lines().nth(1).is_none());
    let document_ids = identities_by_source(&document_report.dlq_entries);
    assert_eq!(document_ids["src_a"].len(), 2);
    assert_eq!(document_ids["src_b"].len(), 2);
    assert_eq!(
        document_ids
            .values()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>()
            .len(),
        4
    );
    assert_eq!(
        document_report
            .dlq_entries
            .iter()
            .filter(|entry| entry.trigger)
            .count(),
        2,
        "each source document keeps its own root cause"
    );
}

fn large_source(prefix: &str) -> String {
    const ROWS: usize = 320;
    let note = "x".repeat(8 * 1024);
    let mut csv = String::from("id,value,note\n");
    csv.push_str(&format!("{prefix}000,bad,{note}\n"));
    for index in 1..ROWS {
        csv.push_str(&format!("{prefix}{index:03},{index},{note}\n"));
    }
    csv
}

#[test]
fn dlq_document_collateral_preserves_identity_and_records_when_spilled() {
    if clinker_exec::pipeline::memory::rss_bytes().is_none() {
        return;
    }

    let src_a = large_source("a");
    let src_b = large_source("b");
    let resident_plan = compile_failure_pipeline("document", "1G");
    let spilled_plan = compile_failure_pipeline("document", "1M");
    let (resident, _) = run_failure_pipeline(&resident_plan, src_a.clone(), src_b.clone());
    let (spilled, output) = run_failure_pipeline(&spilled_plan, src_a, src_b);

    assert_eq!(resident.dlq_entries.len(), 640);
    assert_eq!(spilled.dlq_entries.len(), resident.dlq_entries.len());
    assert_eq!(spilled.counters.ok_count, 0);
    assert!(output.lines().nth(1).is_none());
    assert!(
        spilled
            .per_stage_spill_bytes
            .get("out")
            .is_some_and(|bytes| *bytes > 0),
        "the document collateral bucket must spill: {:?}",
        spilled.per_stage_spill_bytes
    );

    let evidence = |entries: &[DlqEntry]| {
        entries
            .iter()
            .map(|entry| {
                let id = entry
                    .original_record
                    .get("id")
                    .expect("id field")
                    .to_string();
                let note = entry
                    .original_record
                    .get("note")
                    .expect("note field")
                    .to_string();
                (id, note.len(), entry.source_row, entry.trigger)
            })
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        evidence(&spilled.dlq_entries),
        evidence(&resident.dlq_entries)
    );
    assert!(
        spilled.dlq_entries.iter().all(|entry| entry
            .original_record
            .get("note")
            .expect("note field")
            .to_string()
            .len()
            == 8 * 1024),
        "spill must retain each complete original record"
    );
}

#[test]
fn dlq_retry_reuses_compiled_plan_with_fresh_attempt_state() {
    let plan = compile_failure_pipeline("document", "1G");
    let (failed, failed_output) =
        run_failure_pipeline(&plan, small_source("a", true), small_source("b", true));
    assert_eq!(failed.dlq_entries.len(), 4);
    assert!(failed_output.lines().nth(1).is_none());

    let (retried, retried_output) =
        run_failure_pipeline(&plan, small_source("a", false), small_source("b", false));
    assert!(retried.dlq_entries.is_empty());
    assert_eq!(retried.counters.ok_count, 4);
    assert_eq!(retried_output.lines().skip(1).count(), 4);
}

#[test]
fn dlq_carriers_require_typed_identity_without_composite_dedup_keys() {
    let compact = |source: &str| source.split_whitespace().collect::<String>();
    let document_dlq = compact(include_str!("../src/executor/document_dlq.rs"));
    let dlq = compact(include_str!("../src/executor/dlq.rs"));

    assert!(dlq.contains("pubsource_row:crate::executor::stream_event::SourceRowId"));
    assert!(
        !document_dlq.contains("fnrecord_error_to_document_buffer_if_doc_dlq<R>")
            && !document_dlq.contains("R:Into<crate::executor::stream_event::SourceRowId>")
            && !document_dlq.contains("letrow_num=row_num.into()"),
        "document failure admission must accept SourceRowId directly"
    );
    assert!(
        !document_dlq.contains("HashSet<(Arc<str>,crate::executor::stream_event::SourceRowId)>"),
        "typed source identity must be the document collateral dedup key"
    );
}

fn compile_retraction_pipeline(failing_total: i64) -> CompiledPlan {
    let yaml = format!(
        r#"
pipeline:
  name: typed_commit_retraction
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      correlation_key: order_id
      schema:
        - {{ name: order_id, type: string }}
        - {{ name: department, type: string }}
        - {{ name: amount, type: int }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      correlation_key: order_id
      schema:
        - {{ name: order_id, type: string }}
        - {{ name: department, type: string }}
        - {{ name: amount, type: int }}
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: aggregate
    name: totals
    input: merged
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit total = sum(amount)
        emit n = count(*)
  - type: transform
    name: post_check
    input: totals
    config:
      cxl: |
        emit department = department
        emit total = total
        emit n = n
        emit ratio = 1 / (total - {failing_total})
  - type: output
    name: out
    input: post_check
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#,
    );
    parse_config(&yaml)
        .expect("retraction pipeline parses")
        .compile(&CompileContext::default())
        .expect("retraction pipeline compiles")
}

fn run_retraction_pipeline(
    plan: &CompiledPlan,
    src_a: &str,
    src_b: &str,
) -> (ExecutionReport, String) {
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            SourceInput::Files(vec![slot("a.csv", src_a.to_string())]),
        ),
        (
            "src_b".to_string(),
            SourceInput::Files(vec![slot("b.csv", src_b.to_string())]),
        ),
    ]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "typed-commit-retraction".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(plan, readers, writers, &params)
        .expect("retraction pipeline executes");
    (report, output.as_string())
}

const RETRACT_A: &str = "\
order_id,department,amount
A1,HR,10
A2,HR,10
A3,HR,10
A4,ENG,100
A5,ENG,200
";

const RETRACT_B: &str = "\
order_id,department,amount
B1,HR,10
B2,HR,10
B3,HR,10
B4,ENG,300
";

const ENG_ONLY_A: &str = "\
order_id,department,amount
A4,ENG,100
A5,ENG,200
";

const ENG_ONLY_B: &str = "\
order_id,department,amount
B4,ENG,300
";

fn sorted_output_lines(output: &str) -> Vec<String> {
    let mut lines = output.lines().map(str::to_string).collect::<Vec<_>>();
    lines.sort();
    lines
}

#[test]
fn commit_detects_same_ordinal_contributors_from_both_sources() {
    let plan = compile_retraction_pipeline(60);
    let (retracted, retracted_output) = run_retraction_pipeline(&plan, RETRACT_A, RETRACT_B);
    let (baseline, baseline_output) = run_retraction_pipeline(&plan, ENG_ONLY_A, ENG_ONLY_B);

    assert_eq!(baseline.counters.dlq_count, 0);
    assert_eq!(
        retracted
            .counters
            .retraction
            .synthetic_ck_fanout_rows_expanded_total,
        6,
        "commit detection must harvest all six contributors despite ordinal collisions"
    );
    assert_eq!(
        sorted_output_lines(&retracted_output),
        sorted_output_lines(&baseline_output),
        "retraction must remove both sources' HR contributions"
    );
}

#[test]
fn commit_recompute_retracts_exact_row_and_preserves_colliding_source() {
    let plan = compile_retraction_pipeline(10);
    let src_a = "order_id,department,amount\nA1,HR,10\n";
    let src_b = "order_id,department,amount\nB1,ENG,20\n";
    let (report, output) = run_retraction_pipeline(&plan, src_a, src_b);

    assert!(
        !output.contains("HR"),
        "the failing src_a row must be retracted"
    );
    assert!(
        output.contains("ENG,20"),
        "src_b row 1 has the same ordinal but a distinct SourceRowId and must survive: {output}"
    );
    assert_eq!(report.counters.ok_count, 1);
    assert_eq!(report.counters.retraction.groups_recomputed, 1);
}

#[test]
fn commit_retry_reuses_compiled_plan_with_fresh_retraction_state() {
    let plan = compile_retraction_pipeline(60);
    let (first, first_output) = run_retraction_pipeline(&plan, RETRACT_A, RETRACT_B);
    assert!(first.counters.dlq_count > 0);
    assert!(!first_output.contains("HR"));

    let (retried, retried_output) = run_retraction_pipeline(&plan, ENG_ONLY_A, ENG_ONLY_B);
    assert_eq!(retried.counters.dlq_count, 0);
    assert_eq!(retried.counters.retraction.groups_recomputed, 0);
    assert!(retried_output.contains("ENG,600"));
}

#[test]
fn commit_state_uses_source_row_id_without_source_name_reconstruction() {
    let compact = |source: &str| source.split_whitespace().collect::<String>();
    let detect = compact(include_str!("../src/executor/commit/detect.rs"));
    let commit = compact(include_str!("../src/executor/commit/mod.rs"));
    let dispatch = compact(include_str!("../src/executor/commit/dispatch.rs"));
    let recompute = compact(include_str!("../src/executor/commit/recompute_agg.rs"));

    assert!(detect.contains("pub(crate)typeRetractRow=SourceRowId;"));
    assert!(
        !detect.contains("SourceRowId,Arc<str>")
            && !detect.contains("source_name_arc_of(&err.original_record)")
            && !detect.contains("letpair=(event.source_row"),
        "affected-row detection must use SourceRowId as the complete identity"
    );
    assert!(
        !commit.contains("pub(crate)source_name:std::sync::Arc<str>")
            && !dispatch.contains("source_name:std::sync::Arc::clone(&entry.source_name)"),
        "commit harvest must not reconstruct source identity from display names"
    );
    assert!(
        !recompute.contains("for(row_id,_)inretract_ids"),
        "aggregate recompute must consume SourceRowId directly"
    );
}
