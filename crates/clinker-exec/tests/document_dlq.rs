//! Document-level DLQ tests for `dlq_granularity: document`.
//!
//! Locks the load-bearing semantics: one record failure dead-letters the
//! WHOLE document (root-cause trigger + `document_rejected` collaterals,
//! each entry counting toward the DLQ rate), clean documents in the same
//! run stream through intact, the default `record` policy is unchanged, a
//! single large document is rejected atomically, the document grain is the
//! OUTERMOST envelope level (a nested-envelope failure rejects the whole
//! interchange, not just the inner transaction set), and the policy is
//! rejected at compile time when combined with per-source-file output
//! fan-out.
//!
//! Most tests use a multi-file CSV glob (each file is one flat document)
//! the way the per-document Aggregate flush tests do; the nested-grain test
//! drives a multi-level X12 interchange (ISA → GS → ST) so the
//! outermost-grain decision is observable.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{DlqEntry, PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_record::PipelineCounters;

/// A `validate` transform coerces `value` to an int, so a non-numeric cell
/// triggers a per-record eval failure. The source's `dlq_granularity`
/// is templated so the same pipeline drives both the `document` and the
/// default `record` policy.
fn validate_yaml(dlq_granularity: &str) -> String {
    format!(
        r#"
pipeline:
  name: doc_dlq
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      dlq_granularity: {dlq_granularity}
      files:
        on_no_match: skip
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
  - type: transform
    name: validate
    input: events
    config:
      cxl: |
        emit id = id
        emit val = value.to_int()
  - type: output
    name: out
    input: validate
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
    )
}

/// Run the validate pipeline over a set of in-memory CSV files, each fed as
/// a distinct `FileSlot` (hence a distinct document). Returns the run
/// counters, the DLQ entries, and the success-sink body lines.
fn run_doc_dlq(
    dlq_granularity: &str,
    files: &[(&str, &str)],
) -> (PipelineCounters, Vec<DlqEntry>, Vec<String>) {
    let yaml = validate_yaml(dlq_granularity);
    let config = parse_config(&yaml).expect("parse document-dlq pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile document-dlq pipeline");

    let slots: Vec<FileSlot> = files
        .iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(*name),
                Box::new(Cursor::new(body.as_bytes().to_vec())),
            )
        })
        .collect();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run document-dlq pipeline");

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    (report.counters, report.dlq_entries, body)
}

#[test]
fn one_fail_rejects_whole_document() {
    // Document A has three records, one of which (`a2`) fails int coercion.
    // The whole document is dead-lettered: one trigger plus two
    // `document_rejected` collaterals, and zero records of document A reach
    // the success sink.
    let (counters, dlq_entries, body) = run_doc_dlq(
        "document",
        &[("a.csv", "id,value\na1,100\na2,bad\na3,300\n")],
    );

    assert_eq!(
        counters.dlq_count, 3,
        "all 3 records of the failing document are dead-lettered"
    );
    assert_eq!(
        counters.ok_count, 0,
        "no record of the rejected document emits"
    );
    assert_eq!(dlq_entries.len(), 3);

    let triggers = dlq_entries.iter().filter(|e| e.trigger).count();
    let collaterals = dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(triggers, 1, "exactly one root-cause trigger");
    assert_eq!(collaterals, 2, "two collateral siblings");

    for e in dlq_entries.iter().filter(|e| !e.trigger) {
        assert_eq!(
            e.category,
            clinker_core_types::dlq::DlqErrorCategory::DocumentRejected,
            "collateral carries the document_rejected category"
        );
    }
    assert!(
        body.is_empty(),
        "no record of the rejected document reaches the success sink, got {body:?}"
    );
}

#[test]
fn multiple_failing_records_all_accounted_for() {
    // A document with TWO failing records (`a2`, `a4`) plus three clean ones.
    // The first failure is the trigger; every OTHER record — the second
    // failing record AND every clean sibling — is a collateral. The invariant
    // is that a rejected N-record document contributes exactly N entries, so a
    // non-first failing record must not vanish from both sink and DLQ.
    let (counters, dlq_entries, body) = run_doc_dlq(
        "document",
        &[(
            "a.csv",
            "id,value\na1,100\na2,bad\na3,300\na4,nope\na5,500\n",
        )],
    );

    assert_eq!(
        counters.dlq_count, 5,
        "all 5 records of the document are accounted for as DLQ entries"
    );
    assert_eq!(dlq_entries.len(), 5);
    assert_eq!(
        counters.ok_count, 0,
        "no record of the rejected document emits"
    );

    let triggers = dlq_entries.iter().filter(|e| e.trigger).count();
    let collaterals = dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(
        triggers, 1,
        "exactly one root-cause trigger (the FIRST failure), even with two failing records"
    );
    assert_eq!(
        collaterals, 4,
        "the second failing record and all three clean siblings are collaterals"
    );
    for e in dlq_entries.iter().filter(|e| !e.trigger) {
        assert_eq!(
            e.category,
            clinker_core_types::dlq::DlqErrorCategory::DocumentRejected,
            "every collateral — failing or clean — carries the document_rejected category"
        );
    }
    // Every source row 1..=5 appears exactly once across all entries: no row
    // is dropped and none is double-counted.
    let mut rows: Vec<u64> = dlq_entries.iter().map(|e| e.source_row).collect();
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![1, 2, 3, 4, 5],
        "every source row is accounted for exactly once"
    );
    assert!(
        body.is_empty(),
        "no record of the rejected document reaches the success sink, got {body:?}"
    );
}

#[test]
fn clean_documents_stream_through() {
    // Document A fails on `a2`; document B is all-valid. B reaches the
    // success sink intact; `ok_count` counts only B's records.
    let (counters, dlq_entries, body) = run_doc_dlq(
        "document",
        &[
            ("a.csv", "id,value\na1,100\na2,bad\na3,300\n"),
            ("b.csv", "id,value\nb1,400\nb2,500\n"),
        ],
    );

    assert_eq!(
        counters.dlq_count, 3,
        "document A's 3 records dead-lettered"
    );
    assert_eq!(counters.ok_count, 2, "only document B's 2 records emit");
    assert_eq!(
        body,
        vec!["b1,400,400".to_string(), "b2,500,500".to_string()],
        "the clean document streams through intact"
    );
    assert!(
        !body.iter().any(|r| r.starts_with("a")),
        "no record of the rejected document A reaches the sink"
    );
    assert_eq!(dlq_entries.len(), 3);
}

#[test]
fn record_level_policy_unchanged() {
    // The same input under the default `record` policy: only the failing
    // record (`a2`) is dead-lettered; its document siblings stream. No
    // document-level rejection, no regression.
    let (counters, dlq_entries, body) =
        run_doc_dlq("record", &[("a.csv", "id,value\na1,100\na2,bad\na3,300\n")]);

    assert_eq!(counters.dlq_count, 1, "only the failing record is DLQ'd");
    assert_eq!(counters.ok_count, 2, "the two valid siblings stream");
    assert_eq!(dlq_entries.len(), 1);
    assert!(
        dlq_entries[0].trigger,
        "the failing record is its own root cause"
    );
    assert_ne!(
        dlq_entries[0].category,
        clinker_core_types::dlq::DlqErrorCategory::DocumentRejected,
        "no document_rejected category under the record policy"
    );
    assert_eq!(
        body,
        vec!["a1,100,100".to_string(), "a3,300,300".to_string()],
        "the valid siblings reach the sink under the record policy"
    );
}

#[test]
fn all_clean_emits_nothing_to_dlq() {
    // A control: every document valid → zero DLQ, every record streams.
    let (counters, dlq_entries, body) = run_doc_dlq(
        "document",
        &[
            ("a.csv", "id,value\na1,100\na2,200\n"),
            ("b.csv", "id,value\nb1,300\n"),
        ],
    );
    assert_eq!(counters.dlq_count, 0);
    assert_eq!(dlq_entries.len(), 0);
    assert_eq!(counters.ok_count, 3, "all three records stream");
    assert_eq!(
        body,
        vec![
            "a1,100,100".to_string(),
            "a2,200,200".to_string(),
            "b1,300,300".to_string()
        ],
    );
}

#[test]
fn large_document_rejects_every_record() {
    // A single large document (200 valid records + 1 failing tail record,
    // one file, no internal boundary until EOF) is the worst-case shape the
    // per-document buffer must hold and reject atomically: one trigger plus
    // 200 collaterals, EACH counting toward the DLQ rate (a rejected
    // N-record document contributes N to the rate), and nothing reaches the
    // success sink. Proves the per-document buffering scales past a couple
    // of records and the whole-document reject is complete.
    let mut doc = String::from("id,value\n");
    for i in 0..200 {
        doc.push_str(&format!("r{i},{i}\n"));
    }
    doc.push_str("bad,not_an_int\n");

    let yaml = validate_yaml("document");
    let config = parse_config(&yaml).expect("parse large-document pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile large-document pipeline");

    let slots = vec![FileSlot::new(
        PathBuf::from("big.csv"),
        Box::new(Cursor::new(doc.as_bytes().to_vec())),
    )];
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run large-document pipeline");

    assert_eq!(
        report.counters.dlq_count, 201,
        "one trigger + 200 collaterals: every record of the rejected document \
         counts toward the DLQ rate"
    );
    assert_eq!(report.counters.ok_count, 0);
    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert!(
        body.is_empty(),
        "no record of the rejected document reaches the sink, got {body:?}"
    );
}

/// The canonical 106-byte ISA header, interchange control number
/// `000000001` (`*` element, `:` sub-element, `~` terminator).
const ISA: &str = "ISA*00*          *00*          *ZZ*SENDER         \
    *ZZ*RECEIVER       *240101*1200*U*00401*000000001*0*P*:~";

/// A single X12 interchange (one ISA..IEA / one file) holding TWO
/// transaction sets in one functional group. The FIRST set's `BEG` segment
/// carries a non-numeric first element (`XX`); the SECOND set is clean. A
/// Transform coerces that element to an int, so the first set fails — and
/// under the outermost (interchange) grain the WHOLE interchange, including
/// the clean second set whose records arrive AFTER the first set's close,
/// is dead-lettered.
fn two_set_interchange(first_begin_e01: &str) -> String {
    format!(
        "{ISA}\
        GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~\
        ST*850*0001~\
        BEG*{first_begin_e01}*NE*PO12345**20240101~\
        PO1*1*10*EA*9.99~\
        SE*4*0001~\
        ST*850*0002~\
        BEG*00*NE*PO67890**20240102~\
        PO1*2*20*EA*1.99~\
        SE*4*0002~\
        GE*2*1~\
        IEA*1*000000001~"
    )
}

const X12_DOC_DLQ_YAML: &str = r#"
pipeline:
  name: x12_doc_dlq
error_handling:
  strategy: continue
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      dlq_granularity: document
      schema:
        - { name: seg_id, type: string }
        - { name: set_ref, type: string }
        - { name: e01, type: string }
  - type: transform
    name: validate
    input: interchange
    config:
      cxl: |
        emit seg = seg_id
        emit v = e01.to_int()
  - type: output
    name: out
    input: validate
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// Run an X12 interchange fixture through the document-DLQ pipeline,
/// returning the run counters and the success-sink body line count.
fn run_x12_doc_dlq(fixture: &str) -> (PipelineCounters, Vec<DlqEntry>, usize) {
    let config = parse_config(X12_DOC_DLQ_YAML).expect("parse x12 document-dlq pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile x12 document-dlq pipeline");

    let file = FileSlot::new(
        PathBuf::from("po.x12"),
        Box::new(Cursor::new(fixture.as_bytes().to_vec())),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "interchange".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run x12 document-dlq pipeline");
    let body_lines = report.counters.ok_count.try_into().unwrap_or(usize::MAX);
    (report.counters, report.dlq_entries, body_lines)
}

#[test]
fn nested_envelope_failure_rejects_whole_interchange() {
    // A failure in the FIRST transaction set rejects the WHOLE interchange
    // (the outermost / file grain), including the clean SECOND set whose
    // records arrive after the first set's inner close — the document is
    // not decided until the interchange's outermost close. This pins both
    // the outermost grain and robustness to a record arriving after an
    // inner-level boundary.
    let (counters, dlq_entries, _) = run_x12_doc_dlq(&two_set_interchange("XX"));

    assert_eq!(counters.ok_count, 0, "the whole interchange is rejected");
    assert!(
        counters.dlq_count >= 2,
        "the interchange's records across BOTH sets dead-letter, not just \
         the failing first set's: {}",
        counters.dlq_count
    );
    let triggers = dlq_entries.iter().filter(|e| e.trigger).count();
    assert_eq!(
        triggers, 1,
        "exactly one root-cause trigger for the interchange"
    );
    assert!(
        dlq_entries
            .iter()
            .filter(|e| !e.trigger)
            .all(|e| e.category == clinker_core_types::dlq::DlqErrorCategory::DocumentRejected),
        "every collateral carries the document_rejected category"
    );
}

#[test]
fn nested_envelope_clean_interchange_streams_through() {
    // A control: every transaction set in the interchange is valid, so the
    // whole interchange streams to the sink with no DLQ entries.
    let (counters, dlq_entries, ok) = run_x12_doc_dlq(&two_set_interchange("00"));
    assert_eq!(
        counters.dlq_count, 0,
        "a clean interchange emits no DLQ entries"
    );
    assert_eq!(dlq_entries.len(), 0);
    assert!(ok > 0, "the clean interchange's records reach the sink");
}

// --- Structural-count validation: malformed envelope → document DLQ ---

/// An X12 document-DLQ pipeline whose source `dlq_granularity` is templated,
/// so the same pipeline drives both the `document` opt-in and the default
/// `record` policy for the structural-count tests.
fn x12_structural_yaml(dlq_granularity: &str) -> String {
    format!(
        r#"
pipeline:
  name: x12_structural
error_handling:
  strategy: continue
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      dlq_granularity: {dlq_granularity}
      schema:
        - {{ name: seg_id, type: string }}
        - {{ name: set_ref, type: string }}
        - {{ name: e01, type: string }}
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// One X12 interchange (one file) holding a single transaction set whose
/// `SE01` segment count is templated — pass a wrong count to trip the SE
/// structural-count check, the right count (`4`) for a clean control. Two
/// body segments (`BEG`, `PO1`) stream as records BEFORE the SE trailer
/// fires, so a reject has already-streamed records to dead-letter as
/// collaterals.
fn x12_se_count(se01: &str) -> String {
    format!(
        "{ISA}\
        GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~\
        ST*850*0001~\
        BEG*00*NE*PO12345**20240101~\
        PO1*1*10*EA*9.99~\
        SE*{se01}*0001~\
        GE*1*1~\
        IEA*1*000000001~"
    )
}

/// Like [`x12_se_count`] but the `GE01` functional-group set count is
/// templated (the right value is `1`). The SE trailer is correct so only the
/// GE count can fail.
fn x12_ge_count(ge01: &str) -> String {
    format!(
        "{ISA}\
        GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~\
        ST*850*0001~\
        BEG*00*NE*PO12345**20240101~\
        PO1*1*10*EA*9.99~\
        SE*4*0001~\
        GE*{ge01}*1~\
        IEA*1*000000001~"
    )
}

/// Like [`x12_se_count`] but the `IEA01` interchange functional-group count
/// is templated (the right value is `1`). SE and GE trailers are correct so
/// only the IEA count can fail.
fn x12_iea_count(iea01: &str) -> String {
    format!(
        "{ISA}\
        GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~\
        ST*850*0001~\
        BEG*00*NE*PO12345**20240101~\
        PO1*1*10*EA*9.99~\
        SE*4*0001~\
        GE*1*1~\
        IEA*{iea01}*000000001~"
    )
}

/// A ZERO-body malformed interchange: an `ISA` header with NO functional
/// group and an `IEA` claiming a nonzero group count. `close_interchange`
/// fires the IEA structural-count error before any body record streams, so
/// the failed file's level stack never opened — the document context for the
/// reject must be synthesized from the file grain.
fn x12_zero_body_iea_count(iea01: &str) -> String {
    format!("{ISA}IEA*{iea01}*000000001~")
}

/// Run an X12 fixture through the structural-count pipeline at the given
/// granularity, returning the full run `Result` so a test can assert either
/// a successful document-DLQ run or a run-aborting error.
fn run_x12_structural(
    dlq_granularity: &str,
    fixture: &str,
) -> Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError> {
    let yaml = x12_structural_yaml(dlq_granularity);
    let config = parse_config(&yaml).expect("parse x12 structural pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile x12 structural pipeline");
    let file = FileSlot::new(
        PathBuf::from("po.x12"),
        Box::new(Cursor::new(fixture.as_bytes().to_vec())),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "interchange".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
}

/// Run several named X12 files as one multi-file glob source through the
/// structural-count pipeline at the given granularity, returning the run
/// `Result` plus the sorted success-sink body lines (each `seg_id,set_ref`).
#[allow(clippy::type_complexity)]
fn run_x12_structural_multi(
    dlq_granularity: &str,
    files: &[(&str, String)],
) -> Result<
    (clinker_exec::executor::ExecutionReport, Vec<String>),
    clinker_plan::error::PipelineError,
> {
    let yaml = x12_structural_yaml(dlq_granularity);
    let config = parse_config(&yaml).expect("parse x12 structural pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile x12 structural pipeline");
    let slots: Vec<FileSlot> = files
        .iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(*name),
                Box::new(Cursor::new(body.as_bytes().to_vec())),
            )
        })
        .collect();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "interchange".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)?;
    let mut body: Vec<String> = buf.as_string().lines().skip(1).map(String::from).collect();
    body.sort();
    Ok((report, body))
}

#[test]
fn x12_multi_file_bad_in_the_middle_rejects_only_the_bad_file() {
    // A multi-file glob source [good, bad (wrong SE count), good] under the
    // `document` opt-in: dead-lettering the malformed middle file must NOT
    // stop ingestion of the clean third file. The two good files stream to
    // the sink and ONLY the bad file's records dead-letter — silently
    // dropping the good tail (the way a `break`-the-source implementation
    // would) is a data-loss regression on the prior loud run-abort.
    let (report, body) = run_x12_structural_multi(
        "document",
        &[
            ("a.x12", x12_se_count("4")),  // clean
            ("b.x12", x12_se_count("99")), // malformed: wrong SE count
            ("c.x12", x12_se_count("4")),  // clean — must still be read
        ],
    )
    .expect("a malformed file dead-letters; the run does not abort");

    // Both clean files reached the sink; the bad file contributed nothing.
    assert!(
        report.counters.ok_count > 0,
        "the clean files' records reach the sink"
    );
    assert!(
        !body.is_empty(),
        "the clean files stream through, got an empty sink"
    );
    // Each clean single-set interchange emits the same body segments, so the
    // good output is exactly double a single clean file's output.
    let (single_clean, _) =
        run_x12_structural_multi("document", &[("solo.x12", x12_se_count("4"))])
            .expect("a single clean file runs");
    assert_eq!(
        report.counters.ok_count,
        single_clean.counters.ok_count * 2,
        "exactly the two clean files' records reach the sink — the third file \
         was not silently dropped after the malformed second"
    );

    // Exactly one malformed file was dead-lettered: one StructuralValidation
    // trigger, its message naming the bad file's count mismatch.
    let triggers: Vec<&DlqEntry> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(
        triggers.len(),
        1,
        "exactly one file (the malformed middle one) is dead-lettered"
    );
    assert_eq!(
        triggers[0].category,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation
    );
    assert!(
        triggers[0]
            .error_message
            .contains("SE segment count mismatch"),
        "got {:?}",
        triggers[0].error_message
    );
}

#[test]
fn x12_zero_body_malformed_file_dead_letters() {
    // A ZERO-body malformed interchange (an ISA with no functional group and
    // an IEA claiming a nonzero count) dead-letters under the `document`
    // opt-in rather than aborting — even though no body record ever streamed,
    // so the file's level stack never opened. The reject's document context is
    // synthesized from the file grain. One StructuralValidation trigger, no
    // collaterals (no record of the file streamed), nothing at the sink.
    let report = run_x12_structural("document", &x12_zero_body_iea_count("5"))
        .expect("a zero-body malformed file dead-letters rather than aborting");
    assert_eq!(report.counters.ok_count, 0, "no record reaches the sink");
    let triggers: Vec<&DlqEntry> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(
        triggers.len(),
        1,
        "exactly one root-cause trigger for the zero-body malformed file"
    );
    assert_eq!(
        triggers[0].category,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation
    );
    assert!(
        triggers[0]
            .error_message
            .contains("IEA functional-group count mismatch"),
        "got {:?}",
        triggers[0].error_message
    );
    assert_eq!(
        report.dlq_entries.iter().filter(|e| !e.trigger).count(),
        0,
        "a zero-body file has no already-streamed records, so no collaterals"
    );
}

#[test]
fn x12_zero_body_bad_then_good_continues() {
    // The (A) regression guard: a glob [zero-body-bad, good] under the
    // `document` opt-in must dead-letter the zero-body file AND keep reading
    // the good file. Before the synthesized-context fix the empty level stack
    // fell through to a loud run-abort, silently dropping the trailing good
    // file.
    let (report, body) = run_x12_structural_multi(
        "document",
        &[
            ("bad.x12", x12_zero_body_iea_count("5")), // zero-body malformed
            ("good.x12", x12_se_count("4")),           // clean — must still be read
        ],
    )
    .expect("a zero-body malformed file dead-letters; the run does not abort");

    assert!(
        !body.is_empty() && report.counters.ok_count > 0,
        "the good file after a zero-body malformed one still streams to the sink"
    );
    let (single_clean, _) =
        run_x12_structural_multi("document", &[("solo.x12", x12_se_count("4"))])
            .expect("a single clean file runs");
    assert_eq!(
        report.counters.ok_count, single_clean.counters.ok_count,
        "exactly the one clean file's records reach the sink"
    );
    let triggers: Vec<&DlqEntry> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(
        triggers.len(),
        1,
        "only the zero-body file is dead-lettered"
    );
    assert!(
        triggers[0]
            .error_message
            .contains("IEA functional-group count mismatch"),
        "got {:?}",
        triggers[0].error_message
    );
}

#[test]
fn x12_multi_file_bad_first_then_good_continues() {
    // [bad, good]: the malformed FIRST file dead-letters and the good second
    // file streams. Guards the cursor-advance from the very first slot.
    let (report, body) = run_x12_structural_multi(
        "document",
        &[
            ("bad.x12", x12_se_count("99")),
            ("good.x12", x12_se_count("4")),
        ],
    )
    .expect("does not abort");
    let (single_clean, _) =
        run_x12_structural_multi("document", &[("solo.x12", x12_se_count("4"))])
            .expect("a single clean file runs");
    assert!(!body.is_empty(), "the good second file streams");
    assert_eq!(
        report.counters.ok_count, single_clean.counters.ok_count,
        "exactly the one clean file's records reach the sink"
    );
    assert_eq!(
        report.dlq_entries.iter().filter(|e| e.trigger).count(),
        1,
        "only the malformed first file is dead-lettered"
    );
}

#[test]
fn x12_multi_file_all_bad_dead_letters_each_and_emits_nothing() {
    // [bad, bad]: every file is malformed — each dead-letters independently
    // (two triggers) and nothing reaches the sink. The run still does NOT
    // abort under the opt-in.
    let (report, body) = run_x12_structural_multi(
        "document",
        &[("a.x12", x12_se_count("99")), ("b.x12", x12_ge_count("9"))],
    )
    .expect("does not abort even when every file is malformed");
    assert_eq!(report.counters.ok_count, 0, "nothing reaches the sink");
    assert!(body.is_empty());
    let triggers: Vec<&DlqEntry> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(
        triggers.len(),
        2,
        "each malformed file is dead-lettered independently"
    );
    assert!(
        triggers
            .iter()
            .all(|e| e.category == clinker_core_types::dlq::DlqErrorCategory::StructuralValidation),
        "both triggers carry the structural_validation category"
    );
}

#[test]
fn x12_multi_file_bad_bad_good_streams_only_the_good() {
    // [bad, bad, good]: two consecutive malformed files each dead-letter, and
    // the trailing good file still streams — the advance is robust across
    // back-to-back rejects.
    let (report, body) = run_x12_structural_multi(
        "document",
        &[
            ("a.x12", x12_se_count("99")),
            ("b.x12", x12_iea_count("9")),
            ("c.x12", x12_se_count("4")),
        ],
    )
    .expect("does not abort");
    let (single_clean, _) =
        run_x12_structural_multi("document", &[("solo.x12", x12_se_count("4"))])
            .expect("a single clean file runs");
    assert!(!body.is_empty(), "the trailing good file streams");
    assert_eq!(
        report.counters.ok_count, single_clean.counters.ok_count,
        "exactly the one clean file's records reach the sink"
    );
    assert_eq!(
        report.dlq_entries.iter().filter(|e| e.trigger).count(),
        2,
        "both malformed files are dead-lettered"
    );
}

#[test]
fn x12_se_count_mismatch_rejects_whole_document() {
    // A wrong SE segment count condemns the WHOLE interchange under the
    // `document` opt-in: the body segments that already streamed dead-letter
    // (one StructuralValidation trigger + DocumentRejected collaterals), and
    // ZERO records reach the success sink. The trailer count is only known
    // mid-stream, so the reject lands at the file's close — but no record of
    // the malformed envelope is ever written out.
    let report = run_x12_structural("document", &x12_se_count("99"))
        .expect("structural-count failure dead-letters rather than aborting");

    assert_eq!(
        report.counters.ok_count, 0,
        "no record of the malformed interchange reaches the sink"
    );
    assert!(
        report.counters.dlq_count >= 1,
        "the malformed interchange's already-streamed records dead-letter, got {}",
        report.counters.dlq_count
    );
    let triggers: Vec<&DlqEntry> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(triggers.len(), 1, "exactly one root-cause trigger");
    assert_eq!(
        triggers[0].category,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation,
        "the trigger carries the structural_validation category"
    );
    assert!(
        triggers[0]
            .error_message
            .contains("SE segment count mismatch"),
        "the trigger message names the count mismatch, got {:?}",
        triggers[0].error_message
    );
    assert!(
        report
            .dlq_entries
            .iter()
            .filter(|e| !e.trigger)
            .all(|e| e.category == clinker_core_types::dlq::DlqErrorCategory::DocumentRejected),
        "every collateral carries the document_rejected category"
    );
}

#[test]
fn x12_ge_count_mismatch_rejects_at_file_grain() {
    // A GE functional-group count mismatch rejects the whole source file
    // (the outermost grain), not just the offending group.
    let report = run_x12_structural("document", &x12_ge_count("9"))
        .expect("GE count failure dead-letters rather than aborting");
    assert_eq!(report.counters.ok_count, 0, "the whole file dead-letters");
    let triggers: Vec<&DlqEntry> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(triggers.len(), 1);
    assert_eq!(
        triggers[0].category,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation
    );
    assert!(
        triggers[0]
            .error_message
            .contains("GE transaction-set count mismatch"),
        "got {:?}",
        triggers[0].error_message
    );
}

#[test]
fn x12_iea_count_mismatch_rejects_at_file_grain() {
    // An IEA interchange-level count mismatch rejects the whole file.
    let report = run_x12_structural("document", &x12_iea_count("9"))
        .expect("IEA count failure dead-letters rather than aborting");
    assert_eq!(report.counters.ok_count, 0, "the whole file dead-letters");
    let triggers: Vec<&DlqEntry> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(triggers.len(), 1);
    assert_eq!(
        triggers[0].category,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation
    );
    assert!(
        triggers[0]
            .error_message
            .contains("IEA functional-group count mismatch"),
        "got {:?}",
        triggers[0].error_message
    );
}

#[test]
fn x12_clean_interchange_streams_under_structural_opt_in() {
    // The control: a well-formed interchange under the same `document`
    // opt-in streams normally with no DLQ entries — the opt-in only changes
    // behavior on a malformed envelope.
    let report = run_x12_structural("document", &x12_se_count("4"))
        .expect("a clean interchange runs without error");
    assert_eq!(
        report.counters.dlq_count, 0,
        "no DLQ entries for a clean file"
    );
    assert!(
        report.counters.ok_count > 0,
        "the clean interchange's records reach the sink"
    );
}

#[test]
fn x12_count_mismatch_aborts_under_record_granularity() {
    // Without the opt-in (default `record` granularity) a structural-count
    // mismatch still ABORTS the run — the document-DLQ behavior is gated on
    // `dlq_granularity: document`.
    let err = run_x12_structural("record", &x12_se_count("99"))
        .expect_err("a count mismatch aborts the run under the default record policy");
    let msg = format!("{err}");
    assert!(
        msg.contains("SE segment count mismatch"),
        "the run aborts with the structural-count error, got {msg:?}"
    );
}

#[test]
fn x12_genuine_corruption_aborts_even_under_opt_in() {
    // Genuine corruption that is NOT a count mismatch — here a segment
    // appearing AFTER the IEA interchange trailer — keeps aborting even under
    // the `document` opt-in. Only `StructuralCount` is reclassified to the
    // document DLQ; structural corruption stays run-aborting.
    let corrupt = format!(
        "{ISA}\
        GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~\
        ST*850*0001~\
        BEG*00*NE*PO12345**20240101~\
        PO1*1*10*EA*9.99~\
        SE*4*0001~\
        GE*1*1~\
        IEA*1*000000001~\
        ST*850*0002~"
    );
    let err = run_x12_structural("document", &corrupt)
        .expect_err("post-trailer corruption aborts even under the document opt-in");
    let msg = format!("{err}");
    assert!(
        msg.contains("after the IEA"),
        "genuine corruption aborts with its own error, not a doc-DLQ reject, got {msg:?}"
    );
}

/// Run an arbitrary fixture through a templated document-DLQ pipeline of the
/// named `format`, returning the full run `Result`. Shared by the EDIFACT and
/// HL7 structural-count tests so each format exercises the same disposition.
fn run_structural(
    format: &str,
    dlq_granularity: &str,
    file_name: &str,
    fixture: &str,
) -> Result<clinker_exec::executor::ExecutionReport, clinker_plan::error::PipelineError> {
    let yaml = format!(
        r#"
pipeline:
  name: structural
error_handling:
  strategy: continue
nodes:
  - type: source
    name: msgs
    config:
      name: msgs
      type: {format}
      glob: ./*.{format}
      dlq_granularity: {dlq_granularity}
      schema:
        - {{ name: seg_id, type: string }}
        - {{ name: set_ref, type: string }}
  - type: output
    name: out
    input: msgs
    config:
      name: out
      type: csv
      path: out.csv
"#
    );
    let config = parse_config(&yaml).expect("parse structural pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile structural pipeline");
    let file = FileSlot::new(
        PathBuf::from(file_name),
        Box::new(Cursor::new(fixture.as_bytes().to_vec())),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "msgs".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
}

/// One ORDERS interchange whose `UNT` segment count is templated (the right
/// count is `4`). `UNH`, `BGM`, `NAD` stream as body records before the UNT
/// trailer fires.
fn edifact_unt_count(unt_count: &str) -> String {
    format!(
        "UNB+UNOA:1+SENDER+RECEIVER+240101:1200+REF1'\
         UNH+M1+ORDERS:D:96A:UN'\
         BGM+220+PO12345+9'\
         NAD+BY+5021376940009::9'\
         UNT+{unt_count}+M1'\
         UNZ+1+REF1'"
    )
}

#[test]
fn edifact_unt_count_mismatch_rejects_whole_document() {
    // A wrong UNT segment count condemns the whole EDIFACT interchange under
    // the `document` opt-in: the body segments dead-letter (StructuralValidation
    // trigger + DocumentRejected collaterals) and no record reaches the sink.
    let report = run_structural(
        "edifact",
        "document",
        "po.edifact",
        &edifact_unt_count("99"),
    )
    .expect("EDIFACT count failure dead-letters rather than aborting");
    assert_eq!(report.counters.ok_count, 0, "no record reaches the sink");
    let triggers: Vec<&DlqEntry> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(triggers.len(), 1);
    assert_eq!(
        triggers[0].category,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation
    );
    assert!(
        triggers[0]
            .error_message
            .contains("UNT segment count mismatch"),
        "got {:?}",
        triggers[0].error_message
    );
}

#[test]
fn edifact_clean_interchange_streams_under_opt_in() {
    // The control: a well-formed UNT count streams normally under the same
    // opt-in.
    let report = run_structural("edifact", "document", "po.edifact", &edifact_unt_count("4"))
        .expect("a clean EDIFACT interchange runs without error");
    assert_eq!(report.counters.dlq_count, 0);
    assert!(report.counters.ok_count > 0);
}

#[test]
fn edifact_count_mismatch_aborts_under_record_granularity() {
    // Without the opt-in, a UNT count mismatch still aborts the run.
    let err = run_structural("edifact", "record", "po.edifact", &edifact_unt_count("99"))
        .expect_err("a count mismatch aborts under the default record policy");
    assert!(
        format!("{err}").contains("UNT segment count mismatch"),
        "got {err}"
    );
}

/// An FHS..FTS file wrapping a BHS..BTS batch of two ADT messages whose
/// `BTS` batch message count is templated (the right count is `2`). The two
/// MSH-led messages stream as body records before the BTS trailer fires.
fn hl7_bts_count(bts_count: &str) -> String {
    let msh1 = "MSH|^~\\&|ADMIT|HOSP|REG|HOSP|20240101120000||ADT^A01|ADT0001|P|2.5";
    let msh2 = "MSH|^~\\&|ADMIT|HOSP|REG|HOSP|20240101120100||ADT^A01|ADT0002|P|2.5";
    format!(
        "FHS|^~\\&|LAB|HOSP|EHR|HOSP|20240102|FILE7\r\
         BHS|^~\\&|LAB|HOSP|EHR|HOSP|20240102|BATCH3\r\
         {msh1}\rPID|1||PT1\r\
         {msh2}\rPID|1||PT2\r\
         BTS|{bts_count}\r\
         FTS|1"
    )
}

#[test]
fn hl7_bts_count_mismatch_rejects_whole_document() {
    // A wrong BTS batch message count condemns the whole HL7 file under the
    // `document` opt-in: the streamed message segments dead-letter
    // (StructuralValidation trigger + DocumentRejected collaterals) and no
    // record reaches the sink.
    let report = run_structural("hl7", "document", "batch.hl7", &hl7_bts_count("99"))
        .expect("HL7 count failure dead-letters rather than aborting");
    assert_eq!(report.counters.ok_count, 0, "no record reaches the sink");
    let triggers: Vec<&DlqEntry> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(triggers.len(), 1);
    assert_eq!(
        triggers[0].category,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation
    );
    assert!(
        triggers[0]
            .error_message
            .contains("BTS batch message count mismatch"),
        "got {:?}",
        triggers[0].error_message
    );
}

#[test]
fn hl7_count_mismatch_aborts_under_record_granularity() {
    // Without the opt-in, a BTS count mismatch still aborts the run.
    let err = run_structural("hl7", "record", "batch.hl7", &hl7_bts_count("99"))
        .expect_err("a count mismatch aborts under the default record policy");
    assert!(
        format!("{err}").contains("BTS batch message count mismatch"),
        "got {err}"
    );
}

#[test]
fn document_dlq_with_per_source_file_fanout_is_rejected_at_compile_time() {
    // Document-level DLQ buffers each document and flushes it to one writer;
    // per-source-file fan-out routes each record to a file-keyed writer. The
    // two are mutually exclusive, so the combination is rejected at compile
    // time (E343) rather than silently dropping every clean document.
    let yaml = r#"
pipeline:
  name: doc_dlq_fanout
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      dlq_granularity: document
      schema:
        - { name: id, type: string }
  - type: output
    name: out
    input: events
    config:
      name: out
      type: csv
      path: "out/{source_file}.csv"
"#;
    // The combination is rejected during config validation (which runs at
    // parse time), before the plan is built.
    let err =
        parse_config(yaml).expect_err("document-DLQ + per-source-file fan-out must be rejected");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("E343"),
        "expected the E343 document-DLQ + fan-out rejection, got: {msg}"
    );
}

#[test]
fn document_dlq_with_fail_fast_is_rejected_at_compile_time() {
    // `dlq_granularity: document` keeps the run going past a bad document;
    // `strategy: fail_fast` aborts on the first error. The two give
    // contradictory dispositions, so the combination is rejected at config
    // validation (E344) rather than silently ignoring the granularity field.
    let yaml = r#"
pipeline:
  name: doc_dlq_fail_fast
error_handling:
  strategy: fail_fast
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      dlq_granularity: document
      schema:
        - { name: id, type: string }
  - type: output
    name: out
    input: events
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err =
        parse_config(yaml).expect_err("document-DLQ + fail_fast must be rejected as contradictory");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("E344"),
        "expected the E344 document-DLQ + fail_fast rejection, got: {msg}"
    );
}

#[test]
fn upstream_spill_still_rejects_whole_document() {
    // Regression guard for the document-DLQ + record-spill combination.
    // The inter-stage buffer feeding the Output is forced to disk under a
    // tight memory budget; the Output reloads those records from the spill
    // BEFORE deciding each document. Because a record's document context —
    // including the source file the grain keys on — now survives the spill
    // round-trip, a document one record of which fails is still rejected
    // whole even when its other records passed through a spilling stage:
    // every record dead-letters and none reach the success sink.
    //
    // RSS-gated: without an RSS reading the buffer stays in memory and never
    // spills, so skip rather than assert a false negative (matching the
    // existing soft-spill coverage).
    if clinker_exec::pipeline::memory::rss_bytes().is_none() {
        return;
    }

    // One large document: many records with a long free-text field (so the
    // inter-stage buffer is heap-dominated and crosses the spill floor),
    // plus one record whose `value` is non-numeric and fails int coercion.
    let note = "free-text customer note that comfortably exceeds the inline boundary";
    let mut doc = String::from("id,value,note\n");
    const ROWS: usize = 4_000;
    for i in 0..ROWS {
        doc.push_str(&format!("r{i},{i},{note} #{i}\n"));
    }
    doc.push_str(&format!("bad,not_an_int,{note} #bad\n"));

    let yaml = r#"
pipeline:
  name: doc_dlq_spill
  memory: { limit: "1M", backpressure: spill }
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      dlq_granularity: document
      files:
        on_no_match: skip
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
        - { name: note, type: string }
  - type: transform
    name: validate
    input: events
    config:
      cxl: |
        emit id = id
        emit note = note
        emit val = value.to_int()
  - type: output
    name: out
    input: validate
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse upstream-spill pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile upstream-spill pipeline");

    let slots = vec![FileSlot::new(
        PathBuf::from("big.csv"),
        Box::new(Cursor::new(doc.as_bytes().to_vec())),
    )];
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run upstream-spill pipeline");

    // An UPSTREAM stage must actually have spilled — otherwise this guards
    // nothing about the document-context-through-spill round-trip. Per-stage
    // spill bytes are keyed by the spilling node's name; the document-DLQ
    // driver's own bucket spill is keyed by the Output node ("out"), so a
    // non-"out" spilling stage is the inter-stage buffer feeding the Output
    // (the producer that drains its records to disk before the Output
    // reloads them). Pinning the upstream stage keeps a future regression in
    // the doc-context-through-spill round-trip from slipping past a driver-
    // only spill.
    let upstream_spilled: u64 = report
        .per_stage_spill_bytes
        .iter()
        .filter(|(node, _)| node.as_str() != "out")
        .map(|(_, &bytes)| bytes)
        .sum();
    assert!(
        upstream_spilled > 0,
        "an upstream inter-stage buffer must spill under the 1 MiB budget so \
         the Output reloads records from disk; per-stage spill = {:?}",
        report.per_stage_spill_bytes
    );

    // The whole document is rejected despite the upstream spill: every
    // record dead-letters (one trigger + collaterals) and none reaches the
    // sink. If the spill had dropped document identity, the reloaded
    // records would be treated as non-governed and streamed to the sink.
    assert_eq!(
        report.counters.ok_count, 0,
        "no record of the rejected document reaches the sink across the spill"
    );
    let trigger_count = report.dlq_entries.iter().filter(|e| e.trigger).count();
    assert_eq!(trigger_count, 1, "exactly one root-cause trigger");
    assert_eq!(
        report.counters.dlq_count as usize,
        ROWS + 1,
        "every record of the document dead-letters across the spill round-trip"
    );
    let body: Vec<String> = buf.as_string().lines().skip(1).map(String::from).collect();
    assert!(
        body.is_empty(),
        "the success sink is empty — the whole document was rejected, got {} rows",
        body.len()
    );
}
