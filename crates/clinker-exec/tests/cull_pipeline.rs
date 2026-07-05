//! Cull node integration tests.
//!
//! Exercises the per-correlation-group record removal arm end-to-end against
//! in-memory CSV readers/writers: the group-level `drop_group_when`
//! predicate, the kept/removed split across the main and `removed_to`
//! producer-side ports, the unchanged (unwidened) schema on both ports,
//! bounded-memory spill under pressure, and idempotent re-run byte-equality.

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};

/// The two rendered output streams of a Cull run: the main port (`main`) and
/// the `removed_to` side-output port (`removed`), plus the run's cumulative
/// on-disk spill volume.
#[derive(Debug)]
struct CullOutputs {
    main: String,
    removed: String,
    cumulative_spill_bytes: u64,
    dlq_count: usize,
}

/// Run a single-source → cull → (main output + audit output) pipeline over
/// `csv_input`, returning both rendered output streams. Panics on run failure;
/// use [`run_cull_result`] when the run is expected to error.
fn run_cull(yaml: &str, csv_input: &str) -> CullOutputs {
    run_cull_result(yaml, csv_input).expect("cull run")
}

/// Result-returning variant of [`run_cull`]: surfaces the run error instead of
/// panicking, so a test can assert on a typed [`clinker_plan::error::PipelineError`]
/// (for example the drop-decision memory-budget gate).
fn run_cull_result(
    yaml: &str,
    csv_input: &str,
) -> Result<CullOutputs, clinker_plan::error::PipelineError> {
    let config = parse_config(yaml).expect("fixture pipeline must parse");
    let plan = config
        .compile(&CompileContext::default())
        .expect("fixture pipeline must compile");

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        primary,
        clinker_exec::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);

    let main_buf = SharedBuffer::new();
    let removed_buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "out".to_string(),
            Box::new(main_buf.clone()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "audit".to_string(),
            Box::new(removed_buf.clone()) as Box<dyn std::io::Write + Send>,
        ),
    ]);

    let params = PipelineRunParams {
        execution_id: "cull-test".to_string(),
        batch_id: "cull-test".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)?;
    Ok(CullOutputs {
        main: main_buf.as_string(),
        removed: removed_buf.as_string(),
        cumulative_spill_bytes: report.cumulative_spill_bytes,
        dlq_count: report.dlq_entries.len(),
    })
}

/// Group-error cull: per account, a group containing any row whose status is
/// `error` is removed (routed to the audit side output); clean groups flow to
/// the main output. `sum(if status == 'error' then 1 else 0) > 0` is the
/// group-level "any row is an error" predicate — CXL's bare aggregates are
/// `sum/count/min/max/avg/collect`, so the disjunction is expressed via a
/// summed indicator rather than a non-existent bare `any()`.
const ERROR_CULL_PIPELINE: &str = r#"
pipeline:
  name: cull_errors
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: test.csv
      schema:
        - { name: account, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: cull
    name: drop_bad
    input: events
    config:
      partition_by: [account]
      removed_to: removed
      rules:
        - name: drop_error_groups
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
  - type: output
    name: out
    input: drop_bad
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: drop_bad.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

#[test]
fn error_group_routed_to_removed_clean_group_to_main() {
    // Account A has one `error` row → the whole A group is removed. Account B
    // is clean → it flows to the main output.
    let csv = "account,amount,status\n\
               A,100,ok\n\
               A,200,error\n\
               B,300,ok\n\
               B,400,ok\n";
    let out = run_cull(ERROR_CULL_PIPELINE, csv);

    assert_eq!(out.dlq_count, 0, "removed rows are not DLQ entries");

    // Main output: only account B's two rows, none of A's.
    let main_data: Vec<&str> = out.main.lines().skip(1).filter(|l| !l.is_empty()).collect();
    assert_eq!(
        main_data.len(),
        2,
        "main carries B's two rows: {}",
        out.main
    );
    assert!(out.main.contains("B,300,ok"));
    assert!(out.main.contains("B,400,ok"));
    assert!(
        !out.main.contains("A,"),
        "the error group must not reach the main output: {}",
        out.main
    );

    // Removed output: both of account A's rows (including the clean one — the
    // WHOLE group is removed when the predicate holds).
    let removed_data: Vec<&str> = out
        .removed
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(
        removed_data.len(),
        2,
        "removed carries both of A's rows: {}",
        out.removed
    );
    assert!(out.removed.contains("A,100,ok"));
    assert!(out.removed.contains("A,200,error"));
    assert!(
        !out.removed.contains("B,"),
        "clean group B must not reach the removed output: {}",
        out.removed
    );
}

#[test]
fn no_trigger_all_to_main_removed_empty() {
    // No group contains an error → every row flows to the main output and the
    // removed side output gets only a header (no data rows).
    let csv = "account,amount,status\n\
               A,100,ok\n\
               B,200,ok\n";
    let out = run_cull(ERROR_CULL_PIPELINE, csv);

    assert_eq!(out.dlq_count, 0);
    let main_data: Vec<&str> = out.main.lines().skip(1).filter(|l| !l.is_empty()).collect();
    assert_eq!(main_data.len(), 2, "all rows flow to main: {}", out.main);
    let removed_data: Vec<&str> = out
        .removed
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .collect();
    assert!(
        removed_data.is_empty(),
        "no group removed, so the removed output carries no data rows: {}",
        out.removed
    );
}

#[test]
fn removed_port_carries_unwidened_schema() {
    // Cull does not widen: the removed output's columns are exactly the input
    // columns (account, amount, status) — no `$meta.*` or other engine
    // column leaks onto either port.
    let csv = "account,amount,status\n\
               A,100,error\n";
    let out = run_cull(ERROR_CULL_PIPELINE, csv);

    let removed_header = out.removed.lines().next().unwrap_or("");
    assert_eq!(
        removed_header, "account,amount,status",
        "the removed port carries the unchanged input schema: {removed_header:?}"
    );
    assert!(
        !out.removed.contains("$"),
        "no engine-stamped column may leak onto the removed port: {}",
        out.removed
    );
}

/// The single-rule error cull with a trailing `#` line-comment on its
/// `drop_group_when`. Comments run to end-of-line and were once rejected at
/// compile time; each rule is now parsed independently, so the comment is
/// scoped to its own rule. Behaviour must match [`ERROR_CULL_PIPELINE`]
/// exactly — the comment is inert.
const ERROR_CULL_PIPELINE_COMMENTED: &str = r#"
pipeline:
  name: cull_errors
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: test.csv
      schema:
        - { name: account, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: cull
    name: drop_bad
    input: events
    config:
      partition_by: [account]
      removed_to: removed
      rules:
        - name: drop_error_groups
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0  # any error row in the group"
  - type: output
    name: out
    input: drop_bad
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: drop_bad.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

#[test]
fn comment_in_predicate_behaves_identically() {
    // A `#`-commented predicate produces output byte-identical to the same
    // predicate without the comment — the comment affects only source text,
    // never the compiled decision.
    let csv = "account,amount,status\n\
               A,100,ok\n\
               A,200,error\n\
               B,300,ok\n\
               B,400,ok\n";
    let plain = run_cull(ERROR_CULL_PIPELINE, csv);
    let commented = run_cull(ERROR_CULL_PIPELINE_COMMENTED, csv);

    assert_eq!(
        commented.main, plain.main,
        "commented predicate must yield the same main output"
    );
    assert_eq!(
        commented.removed, plain.removed,
        "commented predicate must yield the same removed output"
    );
    assert_eq!(commented.dlq_count, plain.dlq_count);
    // Sanity: the shared expectation is that account A (one error row) is
    // removed and account B flows to main.
    assert!(commented.main.contains("B,300,ok"));
    assert!(!commented.main.contains("A,"));
    assert!(commented.removed.contains("A,100,ok"));
    assert!(commented.removed.contains("A,200,error"));
}

/// A two-rule cull where one rule's predicate carries a trailing `#`
/// comment. The rules OR-combine: a group is removed if it holds an error
/// row (commented rule) OR its summed amount exceeds a threshold (plain
/// rule). Each rule contributes independently.
const MULTI_RULE_COMMENTED_PIPELINE: &str = r#"
pipeline:
  name: cull_multi_rule
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: test.csv
      schema:
        - { name: account, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: cull
    name: drop_bad
    input: events
    config:
      partition_by: [account]
      removed_to: removed
      rules:
        - name: drop_error_groups
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0  # any error row"
        - name: drop_big_groups
          drop_group_when: "sum(amount) > 500"
  - type: output
    name: out
    input: drop_bad
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: drop_bad.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

#[test]
fn multi_rule_one_commented_or_combines() {
    // A: has an error row -> removed by the commented rule (sum amount 300,
    //    below the 500 threshold, so only the error rule fires).
    // B: clean, summed amount 900 > 500 -> removed by the plain rule.
    // C: clean, summed amount 100 -> kept (neither rule fires).
    let csv = "account,amount,status\n\
               A,100,ok\n\
               A,200,error\n\
               B,500,ok\n\
               B,400,ok\n\
               C,100,ok\n";
    let out = run_cull(MULTI_RULE_COMMENTED_PIPELINE, csv);

    assert_eq!(out.dlq_count, 0);

    // Only account C survives to the main output.
    let main_data: Vec<&str> = out.main.lines().skip(1).filter(|l| !l.is_empty()).collect();
    assert_eq!(main_data, vec!["C,100,ok"], "main output: {}", out.main);

    // A (error rule) and B (threshold rule) are both removed — four rows.
    let mut removed_data: Vec<&str> = out
        .removed
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .collect();
    removed_data.sort_unstable();
    assert_eq!(
        removed_data,
        vec!["A,100,ok", "A,200,error", "B,400,ok", "B,500,ok"],
        "removed output: {}",
        out.removed
    );
}

/// Count-threshold cull parameterized on the memory limit, with the `spill`
/// backpressure policy so a sub-baseline limit forces the disk path. A group
/// is removed when it holds more than 100 rows.
fn count_cull_pipeline(memory_limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: cull_count_spill
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: test.csv
      schema:
        - {{ name: account, type: string }}
        - {{ name: seq, type: int }}
  - type: cull
    name: drop_big
    input: events
    config:
      partition_by: [account]
      removed_to: removed
      rules:
        - name: drop_large_groups
          drop_group_when: "count(*) > 100"
  - type: output
    name: out
    input: drop_big
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: drop_big.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#
    )
}

/// Build a wide input: `small_groups` accounts with a handful of rows each,
/// plus one "big" account with `big_rows` rows that trips the `count(*) > 100`
/// removal predicate.
fn count_input(small_groups: usize, big_rows: usize) -> String {
    let mut csv = String::from("account,seq\n");
    for g in 0..small_groups {
        for r in 0..3 {
            csv.push_str(&format!("small-{g:05},{r}\n"));
        }
    }
    for r in 0..big_rows {
        csv.push_str(&format!("BIG,{r}\n"));
    }
    csv
}

#[test]
fn cull_spills_under_memory_pressure() {
    // Many small groups plus one big group (150 rows, ~38 KB) that trips the
    // `count(*) > 100` removal predicate. A 96K budget (≈77K soft) sits below
    // the total raw-record footprint, so the cross-group resident peak trips
    // the soft threshold and evicts small groups to disk, yet every single
    // group — including the big one — still fits the finalize reload under the
    // 96K hard limit (so this is the spill success path, not the fail-loud
    // single-giant-group case). The output must be byte-identical to the same
    // input run with an ample budget (spill is a memory strategy, never a data
    // transform) AND the run must report on-disk spill volume.
    let csv = count_input(200, 150);

    let spilled = run_cull(&count_cull_pipeline("96K"), &csv);
    let in_memory = run_cull(&count_cull_pipeline("512M"), &csv);

    assert_eq!(spilled.dlq_count, 0, "spilling run produces no DLQ entries");
    assert!(
        spilled.cumulative_spill_bytes > 0,
        "the 4K budget must force the disk spill path"
    );
    assert_eq!(
        in_memory.cumulative_spill_bytes, 0,
        "the 512M budget keeps everything in memory"
    );

    // Spill is transparent: identical split regardless of budget. Compare
    // sorted line sets so any reordering across the spill round-trip is caught.
    let mut spilled_main: Vec<&str> = spilled.main.lines().collect();
    let mut memory_main: Vec<&str> = in_memory.main.lines().collect();
    spilled_main.sort_unstable();
    memory_main.sort_unstable();
    assert_eq!(
        spilled_main, memory_main,
        "spilled main output must match the in-memory run"
    );

    let mut spilled_removed: Vec<&str> = spilled.removed.lines().collect();
    let mut memory_removed: Vec<&str> = in_memory.removed.lines().collect();
    spilled_removed.sort_unstable();
    memory_removed.sort_unstable();
    assert_eq!(
        spilled_removed, memory_removed,
        "spilled removed output must match the in-memory run"
    );

    // The BIG group (150 rows > 100) is removed; the 200 small groups (3 rows
    // each = 600 rows) survive to the main output.
    let main_data = spilled
        .main
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .count();
    assert_eq!(main_data, 600, "all 600 small-group rows survive to main");
    let removed_data = spilled
        .removed
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .count();
    assert_eq!(removed_data, 150, "the 150-row BIG group is removed");
    assert!(
        !spilled.main.contains("BIG,"),
        "the removed BIG group must not reach the main output"
    );
}

/// Build `groups` single-row partition groups: a huge partition cardinality
/// over tiny raw records. The raw-record buffer stays small per group (one row
/// each) and spills freely under a sub-baseline `spill` budget, isolating the
/// O(groups) drop-decision aggregate as the state under test.
fn distinct_group_input(groups: usize) -> String {
    let mut csv = String::from("account,seq\n");
    for g in 0..groups {
        csv.push_str(&format!("acct-{g:06},0\n"));
    }
    csv
}

#[test]
fn cull_decision_state_fails_loud_when_group_cardinality_exceeds_budget() {
    // 20_000 single-row groups. The raw-record buffer is one tiny row per group
    // and spills freely under the sub-baseline `spill` budget, but the
    // per-group drop-decision aggregate is O(distinct groups), runs in-memory,
    // and can neither spill nor back-pressure. A budget far below that decision
    // state's footprint must surface a typed memory-budget error naming the
    // drop-decision state — not an OOM crash, and not a silently-truncated
    // result. (`count(*) > 100` never fires for one-row groups, so no group is
    // removed; the failure is purely the unbounded decision state, decoupled
    // from the raw-buffer spill path exercised by
    // `cull_spills_under_memory_pressure`.)
    let csv = distinct_group_input(20_000);
    let err = run_cull_result(&count_cull_pipeline("32K"), &csv)
        .expect_err("an O(groups) decision state above the budget must fail loud");
    match &err {
        clinker_plan::error::PipelineError::MemoryBudgetExceeded {
            node,
            used,
            limit,
            detail,
            ..
        } => {
            assert_eq!(node, "drop_big", "the error must name the Cull node");
            assert!(
                *used > *limit,
                "reported use ({used}) must exceed the limit ({limit})"
            );
            assert_eq!(
                detail.as_deref(),
                Some("Cull drop-decision aggregate state"),
                "the detail must name the drop-decision state, not the raw buffer",
            );
        }
        other => panic!("expected MemoryBudgetExceeded for the decision state; got {other:?}"),
    }
}

#[test]
fn idempotent_rerun_is_byte_stable() {
    // Re-running Cull over its own main output must be a fixed point: the main
    // output of run 1 contains only kept groups, none of which trip the
    // removal predicate, so run 2 keeps all of them and removes nothing.
    let csv = "account,amount,status\n\
               A,100,ok\n\
               A,200,error\n\
               B,300,ok\n";
    let first = run_cull(ERROR_CULL_PIPELINE, csv);
    // Feed the first run's main output back in.
    let second = run_cull(ERROR_CULL_PIPELINE, &first.main);
    assert_eq!(
        second.main, first.main,
        "re-running over kept groups is a fixed point: {} vs {}",
        first.main, second.main
    );
    let second_removed: Vec<&str> = second
        .removed
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .collect();
    assert!(
        second_removed.is_empty(),
        "no group is removed on the second pass: {}",
        second.removed
    );
}

/// A Cull whose `drop_group_when` uses ordered comparisons over non-numeric
/// and nullable aggregates: a string `max(name) > 'M'`, a date
/// `max(hired) >= '2020-01-01'`, and a group whose comparison operand is null.
/// These exercise the aggregate residual evaluator's ordered comparison over
/// String / Date / Null operands — the path that previously hard-errored.
const TYPED_COMPARE_PIPELINE: &str = r#"
pipeline:
  name: cull_typed_compare
error_handling:
  strategy: continue
nodes:
  - type: source
    name: people
    config:
      name: people
      type: csv
      path: test.csv
      schema:
        - { name: team, type: string }
        - { name: name, type: string }
        - { name: hired, type: date }
  - type: cull
    name: drop_recent_or_late
    input: people
    config:
      partition_by: [team]
      removed_to: removed
      rules:
        # String ordering: a team whose lexically-greatest name sorts after 'M'.
        - name: late_alphabet
          drop_group_when: "max(name) > 'M'"
        # Date ordering: a team whose most recent hire is in 2020 or later.
        # `#YYYY-MM-DD#` is the CXL date-literal syntax, so both sides of the
        # comparison are Dates (a string literal would be a type error).
        - name: recent_hire
          drop_group_when: "max(hired) >= #2020-01-01#"
  - type: output
    name: out
    input: drop_recent_or_late
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: drop_recent_or_late.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

#[test]
fn ordered_comparison_over_string_date_and_null_operands() {
    // team alpha: names Alice/Bob (max 'Bob' < 'M'), hires 2018/2019 (max
    // < 2020) → kept. team zeta: name Zoe (> 'M') → removed by the string
    // rule. team gamma: name Ann (< 'M') but hire 2021 (>= 2020) → removed by
    // the date rule. team nullteam: a single row whose date is empty (null);
    // `max(hired)` over the group is null, so `max(hired) >= '2020-01-01'`
    // compares against null → false (three-valued), and `max(name) = 'Kit'`
    // < 'M' → false, so the group is KEPT (a null aggregate operand must not
    // hard-error and must not remove the group).
    let csv = "team,name,hired\n\
               alpha,Alice,2018-03-01\n\
               alpha,Bob,2019-06-01\n\
               zeta,Zoe,2017-01-01\n\
               gamma,Ann,2021-09-01\n\
               nullteam,Kit,\n";
    let out = run_cull(TYPED_COMPARE_PIPELINE, csv);

    assert_eq!(
        out.dlq_count, 0,
        "no null/typed comparison may DLQ or error"
    );

    // alpha and nullteam kept; zeta and gamma removed.
    assert!(out.main.contains("alpha,Alice"), "alpha kept: {}", out.main);
    assert!(
        out.main.contains("nullteam,Kit"),
        "the null-aggregate group is kept, not errored: {}",
        out.main
    );
    assert!(
        !out.main.contains("zeta,") && !out.main.contains("gamma,"),
        "removed groups must not reach main: {}",
        out.main
    );
    assert!(
        out.removed.contains("zeta,Zoe"),
        "zeta removed by the string `max(name) > 'M'` rule: {}",
        out.removed
    );
    assert!(
        out.removed.contains("gamma,Ann"),
        "gamma removed by the date `max(hired) >= '2020-01-01'` rule: {}",
        out.removed
    );
    assert!(
        !out.removed.contains("nullteam,"),
        "the null-aggregate group must not be removed: {}",
        out.removed
    );
}

/// A Cull partitioned on a JSON column that carries BOTH empty-string and null
/// values. The dispatch buffer and the decision aggregate must key these as
/// two distinct groups (not collapse both to null), and each group must get
/// its own deterministic decision. JSON is used because a CSV empty cell is an
/// empty string, never a true null — JSON distinguishes `""` from `null`.
const EMPTY_NULL_PARTITION_PIPELINE: &str = r#"
pipeline:
  name: cull_empty_null_partition
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: json
      path: test.json
      schema:
        - { name: account, type: string }
        - { name: status, type: string }
  - type: cull
    name: drop_errors
    input: rows
    config:
      partition_by: [account]
      removed_to: removed
      rules:
        - name: any_error
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
  - type: output
    name: out
    input: drop_errors
    config:
      name: out
      type: json
      path: out.json
  - type: output
    name: audit
    input: drop_errors.removed
    config:
      name: audit
      type: json
      path: audit.json
"#;

#[test]
fn empty_string_and_null_partitions_are_distinct_deterministic_groups() {
    // Three distinct partition groups:
    //   account=""    → one row, status=error      → REMOVED
    //   account=null  → one row, status=ok          → KEPT
    //   account="A"   → one row, status=ok          → KEPT
    // The empty-string and null groups MUST be treated separately: collapsing
    // them (the divergence bug) would route both by one nondeterministic
    // decision. Their decisions here differ, so a wrong merge is observable.
    let json = r#"[
        {"account": "", "status": "error"},
        {"account": null, "status": "ok"},
        {"account": "A", "status": "ok"}
    ]"#;

    let config = parse_config(EMPTY_NULL_PARTITION_PIPELINE).expect("parse");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "rows".to_string(),
        clinker_exec::executor::single_file_reader(
            "test.json",
            Box::new(std::io::Cursor::new(json.as_bytes().to_vec())),
        ),
    )]);
    let main_buf = SharedBuffer::new();
    let removed_buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "out".to_string(),
            Box::new(main_buf.clone()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "audit".to_string(),
            Box::new(removed_buf.clone()) as Box<dyn std::io::Write + Send>,
        ),
    ]);
    let params = PipelineRunParams {
        execution_id: "cull-empty-null".to_string(),
        batch_id: "cull-empty-null".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("empty/null partition cull run");
    assert!(report.dlq_entries.is_empty(), "no DLQ entries");

    let main = main_buf.as_string();
    let removed = removed_buf.as_string();

    // The null and "A" groups are kept; the empty-string group is removed.
    // The empty-string error row must NOT leak into main (which the
    // empty↔null merge bug could cause by routing the merged group as "keep").
    assert!(
        removed.contains("\"error\""),
        "the empty-string group (status=error) must be removed: removed={removed}"
    );
    assert!(
        !main.contains("\"error\""),
        "the error row must not reach main: main={main}"
    );
    // The null and "A" groups (both ok) are kept. Two kept rows total.
    let ok_count = main.matches("\"ok\"").count();
    assert_eq!(
        ok_count, 2,
        "the null-account and \"A\" groups are both kept: main={main}"
    );
}

/// A single downstream node that draws from BOTH Cull ports must receive the
/// union of kept + removed records. Here a Merge consumes `drop_bad`
/// (main/kept) and `drop_bad.removed` (side/removed); the merged output must
/// contain every input row exactly once. Before the per-successor
/// accumulation fix, the second port's admission overwrote the first's, losing
/// one port's records.
const BOTH_PORTS_TO_ONE_SUCCESSOR_PIPELINE: &str = r#"
pipeline:
  name: cull_both_ports_one_successor
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: test.csv
      schema:
        - { name: account, type: string }
        - { name: status, type: string }
  - type: cull
    name: drop_bad
    input: events
    config:
      partition_by: [account]
      removed_to: removed
      rules:
        - name: any_error
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
  - type: merge
    name: recombined
    inputs: [drop_bad, drop_bad.removed]
  - type: output
    name: out
    input: recombined
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn one_successor_drawing_both_ports_receives_the_union() {
    // Account A has an error → removed; account B is clean → kept. A Merge
    // draws both Cull ports, so its output must carry all FOUR rows (both of
    // A's removed rows + both of B's kept rows), each exactly once.
    let csv = "account,status\n\
               A,ok\n\
               A,error\n\
               B,ok\n\
               B,ok\n";
    let config = parse_config(BOTH_PORTS_TO_ONE_SUCCESSOR_PIPELINE).expect("parse");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let out_buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out_buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "cull-both-ports".to_string(),
        batch_id: "cull-both-ports".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("both-ports merge run");
    assert!(report.dlq_entries.is_empty(), "no DLQ entries");

    let out = out_buf.as_string();
    let data: Vec<&str> = out.lines().skip(1).filter(|l| !l.is_empty()).collect();
    assert_eq!(
        data.len(),
        4,
        "the Merge of both Cull ports must carry all four rows (2 removed + 2 kept), \
         not lose one port to an overwrite: {out}"
    );
    // Both A's (removed-port) rows AND both B's (main-port) rows are present.
    assert_eq!(
        out.matches("A,").count(),
        2,
        "both removed-port rows (account A) survive: {out}"
    );
    assert_eq!(
        out.matches("B,").count(),
        2,
        "both main-port rows (account B) survive: {out}"
    );
}
