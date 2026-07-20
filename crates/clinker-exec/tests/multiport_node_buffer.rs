//! Multi-output-port fan-in integration tests (issue #514).
//!
//! A single Merge or Combine that consumes more than one output port of the
//! same multi-output producer (a Route node's branches, or a Cull node's
//! `main` + `removed_to` ports) must observe every port. Before the
//! port-aware node-buffer key these consumers double-drained one un-ported
//! slot: the first drain took the union, the second found nothing, so one
//! port's records were silently dropped.
//!
//! - `route_two_branches_into_one_merge_unions_both` — Route → one Merge from
//!   two branches; the merged output must be the multiset union of both.
//! - `cull_both_ports_into_one_combine_sees_both_sides` — Cull `main` +
//!   `removed_to` → one Combine; the join must observe the build (removed)
//!   side, which is empty under the bug.
//! - `merge_of_two_distinct_sources_is_unchanged` — single-output regression:
//!   a Merge of two distinct sources routes through the `producer_port: None`
//!   default path and stays byte-identical.

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_plan::config::{CompileContext, parse_config};

/// Run `yaml` with the given `(source_name, csv)` readers, capturing every
/// named output writer. Returns each output's rendered string keyed by output
/// node name. Panics on run failure.
fn run(yaml: &str, sources: &[(&str, &str)], outputs: &[&str]) -> (HashMap<String, String>, usize) {
    let config = parse_config(yaml).expect("fixture pipeline must parse");
    let plan = config
        .compile(&CompileContext::default())
        .expect("fixture pipeline must compile");

    let mut readers: SourceReaders = HashMap::new();
    for (name, csv) in sources {
        readers.insert(
            (*name).to_string(),
            clinker_exec::executor::single_file_reader(
                "test.csv",
                Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
            ),
        );
    }

    let bufs: HashMap<String, SharedBuffer> = outputs
        .iter()
        .map(|o| ((*o).to_string(), SharedBuffer::new()))
        .collect();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = bufs
        .iter()
        .map(|(name, buf)| {
            (
                name.clone(),
                Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();

    let params = PipelineRunParams {
        execution_id: "multiport-test".to_string(),
        batch_id: "multiport-test".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("multiport run");
    let rendered = bufs
        .into_iter()
        .map(|(name, buf)| (name, buf.as_string()))
        .collect();
    (rendered, report.dlq_entries.len())
}

/// Data rows (header stripped, blank lines dropped), sorted for multiset
/// comparison.
fn sorted_data_rows(rendered: &str) -> Vec<String> {
    let mut rows: Vec<String> = rendered
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    rows.sort();
    rows
}

// ── Test 1: Route → one Merge from two branches ──────────────────────────

const ROUTE_TWO_BRANCH_MERGE: &str = r#"
pipeline:
  name: route_two_branch_merge
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: test.csv
      schema:
        - { name: id, type: string }
        - { name: region, type: string }
  - type: route
    name: by_region
    input: events
    config:
      mode: exclusive
      conditions:
        a: "region == \"a\""
        b: "region == \"b\""
      default: a
  - type: merge
    name: recombined
    inputs: [by_region.a, by_region.b]
  - type: output
    name: out
    input: recombined
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn route_two_branches_into_one_merge_unions_both() {
    // Three rows route to branch `a`, two rows to branch `b`. A single Merge
    // draws both branches, so its output must carry all five rows — every
    // branch observed, nothing dropped.
    let csv = "id,region\n\
               r1,a\n\
               r2,a\n\
               r3,a\n\
               r4,b\n\
               r5,b\n";
    let (out, dlq) = run(ROUTE_TWO_BRANCH_MERGE, &[("events", csv)], &["out"]);
    assert_eq!(dlq, 0, "no DLQ entries");

    let rows = sorted_data_rows(&out["out"]);
    assert_eq!(
        rows,
        vec![
            "r1,a".to_string(),
            "r2,a".to_string(),
            "r3,a".to_string(),
            "r4,b".to_string(),
            "r5,b".to_string(),
        ],
        "the Merge of both Route branches must carry the union of both ports \
         (3 from branch `a` + 2 from branch `b`), not drop a port: {}",
        out["out"]
    );
}

// ── Test 2: Cull main + removed_to → one Combine ─────────────────────────

const CULL_BOTH_PORTS_COMBINE: &str = r#"
pipeline:
  name: cull_both_ports_combine
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
        - { name: region, type: string }
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
  - type: combine
    name: joined
    input:
      kept: drop_bad
      dropped: drop_bad.removed
    config:
      where: "kept.region == dropped.region"
      match: all
      on_miss: skip
      drive: kept
      cxl: |
        emit region = kept.region
        emit kept_account = kept.account
        emit dropped_account = dropped.account
        emit dropped_status = dropped.status
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn cull_both_ports_into_one_combine_sees_both_sides() {
    // Account A holds an error row → the whole A group goes to the `removed`
    // port (the Combine build side). Account B is clean → it goes to the
    // `main`/kept port (the Combine driver side). Both share region `east`, so
    // the driver (kept B) inner-joins each removed-port A row.
    //
    // Correct: two matched rows (B × A/ok, B × A/error) — the build side is
    // observed. Under the bug the build side drains an already-emptied slot, so
    // no driver row matches and `on_miss: skip` yields zero rows.
    let csv = "account,region,status\n\
               A,east,ok\n\
               A,east,error\n\
               B,east,ok\n";
    let (out, dlq) = run(CULL_BOTH_PORTS_COMBINE, &[("events", csv)], &["out"]);
    assert_eq!(dlq, 0, "no DLQ entries");

    let rows = sorted_data_rows(&out["out"]);
    assert_eq!(
        rows,
        vec!["east,B,A,error".to_string(), "east,B,A,ok".to_string(),],
        "the Combine must observe the removed (build) port: the kept driver row \
         (B) joins both removed-port rows (A/ok, A/error). An empty build side \
         drops every match: {}",
        out["out"]
    );
}

// ── Test 3: single-output Merge regression ───────────────────────────────

const MERGE_TWO_DISTINCT_SOURCES: &str = r#"
pipeline:
  name: merge_two_distinct_sources
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      path: test.csv
      schema:
        - { name: k, type: string }
        - { name: v, type: int }
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: test.csv
      schema:
        - { name: k, type: string }
        - { name: v, type: int }
  - type: merge
    name: both
    inputs: [left, right]
  - type: output
    name: out
    input: both
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn merge_of_two_distinct_sources_is_unchanged() {
    // Two distinct single-output sources feed one Merge. Every edge carries
    // `producer_port: None`, so this exercises the unchanged default path. The
    // declaration-order concat output must be byte-identical to the expected
    // rendering.
    let left = "k,v\n\
                a,1\n\
                b,2\n";
    let right = "k,v\n\
                 c,3\n\
                 d,4\n";
    let (out, dlq) = run(
        MERGE_TWO_DISTINCT_SOURCES,
        &[("left", left), ("right", right)],
        &["out"],
    );
    assert_eq!(dlq, 0, "no DLQ entries");
    assert_eq!(
        out["out"], "k,v\na,1\nb,2\nc,3\nd,4\n",
        "concat Merge of two single-output sources must be byte-identical: {}",
        out["out"]
    );
}

// ── Test 4: inclusive-mode Route, a record matching both branches ────────

const INCLUSIVE_ROUTE_BOTH_BRANCHES_MERGE: &str = r#"
pipeline:
  name: inclusive_route_both_branches
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: test.csv
      schema:
        - { name: id, type: string }
        - { name: value, type: int }
  - type: route
    name: by_value
    input: events
    config:
      mode: inclusive
      conditions:
        hi: "value >= 10"
        big: "value >= 100"
      default: none
  - type: merge
    name: recombined
    inputs: [by_value.hi, by_value.big]
  - type: output
    name: out
    input: recombined
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn inclusive_route_duplicates_a_row_across_both_merged_branches() {
    // Inclusive mode: a record matching two branches flows down BOTH. A Merge of
    // both branches must therefore carry that record twice — once per branch —
    // and must not dedup. `value=100` matches `hi` and `big`; `value=50` matches
    // only `hi`; `value=5` matches neither (goes to the unreferenced `none`
    // default, so it reaches the Merge zero times).
    let csv = "id,value\n\
               a,100\n\
               b,50\n\
               c,5\n";
    let (out, dlq) = run(
        INCLUSIVE_ROUTE_BOTH_BRANCHES_MERGE,
        &[("events", csv)],
        &["out"],
    );
    assert_eq!(dlq, 0, "no DLQ entries");

    let rows = sorted_data_rows(&out["out"]);
    assert_eq!(
        rows,
        vec!["a,100".to_string(), "a,100".to_string(), "b,50".to_string(),],
        "inclusive Route must duplicate the both-branch row (a) across the two \
         merged ports and keep the single-branch row (b) once: {}",
        out["out"]
    );
}

// ── Test 5: spliced passthrough between a Cull port and a Combine ─────────

const CULL_PORT_THROUGH_PASSTHROUGH_COMBINE: &str = r#"
pipeline:
  name: cull_port_through_passthrough
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
        - { name: region, type: string }
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
  - type: transform
    name: relay
    input: drop_bad.removed
    config:
      cxl: |
        emit account = account
        emit region = region
        emit status = status
  - type: combine
    name: joined
    input:
      kept: drop_bad
      dropped: relay
    config:
      where: "kept.region == dropped.region"
      match: all
      on_miss: skip
      drive: kept
      cxl: |
        emit region = kept.region
        emit kept_account = kept.account
        emit dropped_account = dropped.account
        emit dropped_status = dropped.status
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn combine_build_side_reaches_through_a_passthrough_transform() {
    // The Combine build side draws the Cull `removed` port through a
    // single-output passthrough Transform (`relay`). The build predecessor is
    // the Transform (a distinct node, resolved by name), so its slot is keyed
    // `(relay, None)` — the removed records must survive the extra hop and the
    // driver (kept) must still join them.
    let csv = "account,region,status\n\
               A,east,ok\n\
               A,east,error\n\
               B,east,ok\n";
    let (out, dlq) = run(
        CULL_PORT_THROUGH_PASSTHROUGH_COMBINE,
        &[("events", csv)],
        &["out"],
    );
    assert_eq!(dlq, 0, "no DLQ entries");

    let rows = sorted_data_rows(&out["out"]);
    assert_eq!(
        rows,
        vec!["east,B,A,error".to_string(), "east,B,A,ok".to_string()],
        "the removed port routed through a passthrough Transform must still \
         reach the Combine build side: {}",
        out["out"]
    );
}

// ── Test 6: Combine driver + build both from the same Route ──────────────

const ROUTE_TWO_BRANCHES_ONE_COMBINE: &str = r#"
pipeline:
  name: route_two_branches_one_combine
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: test.csv
      schema:
        - { name: key, type: string }
        - { name: side, type: string }
  - type: route
    name: by_side
    input: events
    config:
      mode: exclusive
      conditions:
        left: "side == \"L\""
        right: "side == \"R\""
      default: left
  - type: combine
    name: joined
    input:
      l: by_side.left
      r: by_side.right
    config:
      where: "l.key == r.key"
      match: all
      on_miss: skip
      drive: l
      cxl: |
        emit key = l.key
        emit left_side = l.side
        emit right_side = r.side
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn combine_driver_and_build_from_same_route_resolve_by_port() {
    // The Combine draws its driver from `by_side.left` and its build from
    // `by_side.right` — both ports of the SAME Route node. Driver and build
    // resolve to the same predecessor NodeIndex, so the slot key's port
    // component is what keeps them apart: driver drains `(route, left)`, build
    // drains `(route, right)`. Records with a shared `key` across the two
    // branches must join.
    let csv = "key,side\n\
               1,L\n\
               1,R\n\
               2,L\n";
    let (out, dlq) = run(ROUTE_TWO_BRANCHES_ONE_COMBINE, &[("events", csv)], &["out"]);
    assert_eq!(dlq, 0, "no DLQ entries");

    let rows = sorted_data_rows(&out["out"]);
    assert_eq!(
        rows,
        vec!["1,L,R".to_string()],
        "key=1 exists on both the left (driver) and right (build) Route ports, \
         so the port-keyed slots must let them join (key=2 has no right side): {}",
        out["out"]
    );
}
