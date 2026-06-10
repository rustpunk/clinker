//! Integration tests for `merge.mode: interleave`.
//!
//! These exercise the round-robin Merge arm's ordering correctness:
//! per-source FIFO is preserved; cross-source order follows the
//! seeded fastrand schedule (or a deterministic round-robin when
//! `interleave_seed` is absent). Live-channel back-pressure ("a slow
//! upstream doesn't starve a fast one") is exercised by
//! [`interleave_does_not_block_peer_on_slow_source`] — see that test
//! for the topology and assertion.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use clinker_bench_support::io::{SharedBuffer, fast_reader, slow_reader};
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

fn slot(name: &str, csv: &str) -> FileSlot {
    FileSlot::new(PathBuf::from(format!("{name}.csv")), fast_reader(csv))
}

fn slow_slot(name: &str, csv: &str, delay: Duration) -> FileSlot {
    FileSlot::new(
        PathBuf::from(format!("{name}.csv")),
        slow_reader(csv, delay),
    )
}

fn writer(buf: &SharedBuffer) -> Box<dyn std::io::Write + Send> {
    Box::new(buf.clone())
}

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// Build a two-source Merge pipeline. `merge_config_yaml` is spliced
/// in *as* the Merge `config:` block (e.g. `"mode: interleave"` or
/// `"mode: interleave\n      interleave_seed: 42"`); empty string omits
/// the `config:` key entirely, leaving the default `MergeBody`
/// (`mode: concat`, no seed).
fn pipeline_yaml(merge_config_yaml: &str) -> String {
    let merge_config_block = if merge_config_yaml.is_empty() {
        String::new()
    } else {
        format!("    config:\n      {merge_config_yaml}\n")
    };
    format!(
        r#"
pipeline:
  name: merge_interleave_test
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: tag, type: string }}
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: tag, type: string }}
  - type: merge
    name: merged
    inputs: [src_a, src_b]
{merge_config_block}  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// CSV body for `src_a`: ids 1..=count with a stable `a-N` tag.
fn src_a_csv(count: u32) -> String {
    let mut s = String::from("id,tag\n");
    for i in 1..=count {
        s.push_str(&format!("{i},a-{i}\n"));
    }
    s
}

/// CSV body for `src_b`: ids offset by 100 with a stable `b-N` tag.
fn src_b_csv(count: u32) -> String {
    let mut s = String::from("id,tag\n");
    for i in 1..=count {
        s.push_str(&format!("{},b-{}\n", 100 + i, i));
    }
    s
}

/// Run the pipeline once and return the output body lines (header skipped).
fn run_pipeline(yaml: &str, a_count: u32, b_count: u32) -> Vec<String> {
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot("a", &src_a_csv(a_count))]),
        ),
        (
            "src_b".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot("b", &src_b_csv(b_count))]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("pipeline executes");
    assert_eq!(report.counters.dlq_count, 0);
    let output = buf.as_string();
    output.lines().skip(1).map(|s| s.to_string()).collect()
}

/// Extract the `tag` column (`a-N` / `b-N`) from a CSV body row of the
/// shape `id,tag`. Robust against a possible trailing carriage return.
fn tag_of(row: &str) -> &str {
    row.split(',').nth(1).unwrap_or("").trim_end_matches('\r')
}

/// Per-source FIFO check: a-tags appear in increasing N order across
/// the output, and b-tags do the same — the relative interleave
/// between a-* and b-* may vary, but each source's subsequence is
/// arrival-ordered.
fn assert_per_source_fifo(body: &[String]) {
    let mut last_a: u32 = 0;
    let mut last_b: u32 = 0;
    for row in body {
        let tag = tag_of(row);
        if let Some(rest) = tag.strip_prefix("a-") {
            let n: u32 = rest.parse().expect("a-tag parses");
            assert!(
                n > last_a,
                "src_a subsequence not FIFO: saw {tag} after a-{last_a}"
            );
            last_a = n;
        } else if let Some(rest) = tag.strip_prefix("b-") {
            let n: u32 = rest.parse().expect("b-tag parses");
            assert!(
                n > last_b,
                "src_b subsequence not FIFO: saw {tag} after b-{last_b}"
            );
            last_b = n;
        } else {
            panic!("unexpected tag in output row {row:?}");
        }
    }
}

/// `mode: interleave` preserves per-source FIFO for both sources and
/// emits every record exactly once.
#[test]
fn interleave_preserves_per_source_fifo() {
    let yaml = pipeline_yaml("mode: interleave");
    let body = run_pipeline(&yaml, 3, 3);
    assert_eq!(body.len(), 6, "expected 6 records, got {body:?}");
    assert_per_source_fifo(&body);

    // Every record appears exactly once.
    let mut tags: Vec<&str> = body.iter().map(|r| tag_of(r)).collect();
    tags.sort();
    assert_eq!(
        tags,
        vec!["a-1", "a-2", "a-3", "b-1", "b-2", "b-3"],
        "every input record must appear exactly once"
    );
}

/// Without `mode:` the Merge defaults to `concat`, which drains
/// predecessors in declaration order (all `src_a` before any `src_b`).
#[test]
fn concat_default_drains_in_declaration_order() {
    let yaml = pipeline_yaml("");
    let body = run_pipeline(&yaml, 3, 3);
    assert_eq!(body.len(), 6);

    // First three rows are src_a; last three are src_b.
    let tags: Vec<&str> = body.iter().map(|r| tag_of(r)).collect();
    assert_eq!(
        tags,
        vec!["a-1", "a-2", "a-3", "b-1", "b-2", "b-3"],
        "concat must drain src_a before src_b in declaration order"
    );
}

/// `interleave_seed: 42` produces the same output across two
/// independent runs. The seeded `fastrand::Rng` schedule is
/// deterministic regardless of executor scheduling because the
/// Interleave arm consumes pre-buffered records in a single fold.
#[test]
fn seeded_interleave_is_reproducible() {
    let yaml = pipeline_yaml("mode: interleave\n      interleave_seed: 42");
    let first = run_pipeline(&yaml, 5, 5);
    let second = run_pipeline(&yaml, 5, 5);
    assert_eq!(
        first, second,
        "same seed must yield identical ordering across runs"
    );
    assert_per_source_fifo(&first);
    assert_eq!(first.len(), 10);
}

/// Live-channel back-pressure: a slow `src_a` (50 ms sleep at the
/// reader between every record) does not block `src_b` from
/// flowing through the Merge.interleave arm.
///
/// The interleave arm reads whichever source channel is ready via a
/// crossbeam `Select`, so `src_b`'s records arrive into the Merge fold
/// immediately while `src_a` is still sleeping between rows. The arm
/// pulls every `src_b` record in the first ~5 ms, then waits 50 ms for
/// each `src_a` record. Result: every `src_b` record sits in the first
/// half of the output stream.
///
/// Discriminator: the position of the last `b-` tag in the output must
/// be `< 5` (first half of 10 total records). A regime that pre-buffers
/// each source into a Vec and round-robins them would place the last
/// `b-` at position 9; readiness-driven selection places it at the
/// front.
#[test]
fn interleave_does_not_block_peer_on_slow_source() {
    use std::time::Instant;

    let yaml = pipeline_yaml("mode: interleave");
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            // 5 records, each preceded by 50 ms — total ~250 ms.
            clinker_exec::executor::SourceInput::Files(vec![slow_slot(
                "a",
                &src_a_csv(5),
                Duration::from_millis(50),
            )]),
        ),
        (
            "src_b".to_string(),
            // 5 records, produced as fast as the channel admits.
            clinker_exec::executor::SourceInput::Files(vec![slot("b", &src_b_csv(5))]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let start = Instant::now();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("pipeline executes");
    let elapsed = start.elapsed();

    assert_eq!(report.counters.dlq_count, 0);
    let output = buf.as_string();
    let body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    assert_eq!(body.len(), 10, "expected 10 records, got {body:?}");
    assert_per_source_fifo(&body);

    // Discriminator assertion: the LAST b-tag must appear in the
    // first half of the output. Pre-buffered round-robin would place
    // it at the back; readiness-driven selection places it at the front.
    let last_b_pos = body
        .iter()
        .rposition(|row| tag_of(row).starts_with("b-"))
        .expect("at least one b-tag in output");
    assert!(
        last_b_pos < 5,
        "Merge.interleave failed to back-pressure: last src_b record \
         landed at position {last_b_pos} (>=5) in {body:?}. \
         Pre-buffered round-robin would place b records throughout \
         the output; readiness-driven selection concentrates them at \
         the front because src_a is still sleeping when src_b finishes \
         producing."
    );

    // Sanity: pipeline finished within a reasonable window. 5 ×
    // 50 ms slow-side delays + overhead should fit well under 2 s
    // even under CI jitter; failure here suggests something
    // hangs.
    assert!(
        elapsed < Duration::from_secs(2),
        "pipeline took {elapsed:?}, expected < 2s",
    );
}

/// Four-Source `mode: interleave` pipeline YAML. Each Source emits
/// the same two-column schema (`id, tag`) so they share an
/// `output_schema` at the Merge boundary. Output sinks to `out`.
fn pipeline_yaml_four_sources() -> String {
    r#"
pipeline:
  name: merge_interleave_fairness_four
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: src_c
    config:
      name: src_c
      type: csv
      path: c.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: src_d
    config:
      name: src_d
      type: csv
      path: d.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: merge
    name: merged
    inputs: [src_a, src_b, src_c, src_d]
    config:
      mode: interleave
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#
    .to_string()
}

/// CSV body for a tag-prefixed source: `id` is the row number,
/// `tag` is `"{prefix}-{N}"`. Mirrors `src_a_csv` / `src_b_csv` but
/// parametric over the prefix so the four-source fairness test can
/// build `a-*`, `b-*`, `c-*`, `d-*` streams from the same routine.
fn tagged_csv(prefix: char, count: u32) -> String {
    let mut s = String::from("id,tag\n");
    for i in 1..=count {
        s.push_str(&format!("{i},{prefix}-{i}\n"));
    }
    s
}

/// Generalized per-source FIFO check for an arbitrary set of tag
/// prefixes. Each source's subsequence in `body` (rows whose tag
/// begins `{prefix}-`) must be strictly increasing in the numeric
/// suffix. Cross-source order is unconstrained.
fn assert_per_source_fifo_n(body: &[String], prefixes: &[char]) {
    let mut last: HashMap<char, u32> = prefixes.iter().map(|p| (*p, 0u32)).collect();
    for row in body {
        let tag = tag_of(row);
        let prefix = tag.chars().next().expect("tag has at least one char");
        assert!(
            prefixes.contains(&prefix),
            "unexpected tag prefix `{prefix}` in row {row:?}; \
             allowed: {prefixes:?}"
        );
        let n: u32 = tag
            .strip_prefix(&format!("{prefix}-"))
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("tag does not match `{prefix}-N`: {tag:?}"));
        let prev = *last.get(&prefix).unwrap();
        assert!(
            n > prev,
            "src_{prefix} subsequence not FIFO: saw {tag} after {prefix}-{prev}",
        );
        last.insert(prefix, n);
    }
}

/// Multi-source fairness under live `Merge.interleave` with four
/// Source predecessors at mixed produce rates.
///
/// Topology: src_a (0 ms / row), src_b (10 ms), src_c (25 ms),
/// src_d (50 ms), 10 rows each, all feeding one `merge.interleave`
/// into a CSV sink.
///
/// Surfaces any fairness regression in the fused arm's round-robin
/// poll cursor (`dispatch.rs::merge_fused_interleave`) before #74
/// extends the same pattern to Transform-arm predecessors. The
/// invariants below are what the live-channel design promises:
///
/// 1. **Per-source FIFO** survives for every source (each source's
///    records appear in `1, 2, …, 10` order — cross-source order is
///    unconstrained).
/// 2. **No starvation:** every source's final record (`{prefix}-10`)
///    reaches output. If the merge dropped a closed receiver early,
///    a source's tail would go missing.
/// 3. **Parallel ingest, not serialized:** the slow sources' records
///    are temporally intermixed in the output rather than emitted as
///    four declaration-ordered contiguous blocks. Concretely, the two
///    slowest sources' output-position spans overlap — `concat`-style
///    serialization would emit all of `src_c` before any of `src_d`,
///    leaving the spans disjoint and ordered. This is a structural
///    witness, not a wall-clock one: it depends only on the relative
///    order the readiness-driven `select` produces, so uniform CI
///    runner slowdown cannot flip it. (Earlier revisions asserted an
///    absolute runtime bound calibrated from one `sleep` sample; the
///    parallel/serial wall-clock separation is only ~1.7x — narrower
///    than the several-fold inflation CI runners impose on short
///    sleeps — so a wall-clock bound either flaps or, widened enough
///    to stop flapping, rubber-stamps a fully serialized run.)
/// 4. **No single source monopolizes a contiguous tail:** in
///    particular, the slowest source's records spread across the
///    second half of the output. (The fastest source's records
///    necessarily cluster early — `0 ms / row` means src_a fully
///    drains within microseconds while peers are still on their
///    first inter-row sleep. Asserting it lands in both quartiles
///    would require artificial pacing antagonistic to live ingest;
///    the slowest-source spread is the spec-meaningful direction.)
#[test]
fn interleave_fairness_under_four_predecessors() {
    use std::time::Instant;

    let yaml = pipeline_yaml_four_sources();
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            SourceInput::Files(vec![slow_slot(
                "a",
                &tagged_csv('a', 10),
                Duration::from_millis(0),
            )]),
        ),
        (
            "src_b".to_string(),
            SourceInput::Files(vec![slow_slot(
                "b",
                &tagged_csv('b', 10),
                Duration::from_millis(10),
            )]),
        ),
        (
            "src_c".to_string(),
            SourceInput::Files(vec![slow_slot(
                "c",
                &tagged_csv('c', 10),
                Duration::from_millis(25),
            )]),
        ),
        (
            "src_d".to_string(),
            SourceInput::Files(vec![slow_slot(
                "d",
                &tagged_csv('d', 10),
                Duration::from_millis(50),
            )]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let start = Instant::now();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("pipeline executes");
    let elapsed = start.elapsed();

    assert_eq!(report.counters.dlq_count, 0);
    let output = buf.as_string();
    let body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    assert_eq!(body.len(), 40, "expected 40 records (10 × 4), got {body:?}");

    // Invariant 1 — per-source FIFO for all four sources.
    assert_per_source_fifo_n(&body, &['a', 'b', 'c', 'd']);

    // Invariant 2 — no starvation: every source's last record appears.
    for prefix in ['a', 'b', 'c', 'd'] {
        let last_tag = format!("{prefix}-10");
        assert!(
            body.iter().any(|r| tag_of(r) == last_tag),
            "src_{prefix}'s tail record `{last_tag}` missing from output {body:?}",
        );
        let count = body
            .iter()
            .filter(|r| tag_of(r).starts_with(&format!("{prefix}-")))
            .count();
        assert_eq!(
            count, 10,
            "src_{prefix} contributed {count} records, expected 10"
        );
    }

    // Invariant 3 — parallel ingest, asserted structurally on output
    // order rather than wall time. The two slowest sources, src_c
    // (25 ms/row, ~250 ms total) and src_d (50 ms/row, ~500 ms total),
    // are both still producing late in the run, so the readiness-driven
    // `select` necessarily interleaves them: src_d's first record (its
    // reader sleeps 50 ms, then emits — well before src_c finishes
    // ~250 ms later) lands earlier in the output than src_c's last
    // record. Their output-position spans therefore overlap.
    //
    // `concat`-style serialization is the regression this rejects: it
    // drains predecessors in declaration order (all of src_c, then all
    // of src_d), leaving src_d's minimum position strictly after src_c's
    // maximum — disjoint, ordered spans with no overlap. The check below
    // fails on exactly that shape. Because it reads relative order, not
    // elapsed time, uniform runner slowdown shifts every record's
    // arrival together and cannot flip the inequality.
    let first_pos = |prefix: char| {
        body.iter()
            .position(|r| tag_of(r).starts_with(&format!("{prefix}-")))
            .unwrap_or_else(|| panic!("src_{prefix} has no record in output {body:?}"))
    };
    let last_pos = |prefix: char| {
        body.iter()
            .rposition(|r| tag_of(r).starts_with(&format!("{prefix}-")))
            .unwrap_or_else(|| panic!("src_{prefix} has no record in output {body:?}"))
    };
    let d_first = first_pos('d');
    let c_last = last_pos('c');
    assert!(
        d_first < c_last,
        "src_d's first record is at position {d_first}, at or after src_c's \
         last at {c_last}: the two slowest sources' output spans do not \
         overlap, so the merge drained one source before the other rather \
         than ingesting them in parallel. output: {body:?}"
    );

    // Anti-hang ceiling: a pure deadlock guard, deliberately far above
    // the ~500 ms run so runner inflation never approaches it. It does
    // NOT discriminate parallel from serial ingest — Invariant 3 above
    // owns that — it only fails if the merge wedges.
    assert!(
        elapsed < Duration::from_secs(30),
        "pipeline took {elapsed:?}, expected well under 30s — the merge \
         appears to have hung rather than drained",
    );

    // Invariant 4 — the slowest source's records span the second
    // half of the output. With src_d producing one record every
    // 50 ms over a ~500 ms run and faster peers fully drained
    // earlier, at least one src_d record must land in the back
    // half of the merged stream (positions 20..40). If the merge
    // ever decided to buffer src_d to completion before emitting
    // a peer-finished signal, src_d's records would land at the
    // tail; if it ever pre-buffered all sources, they'd land
    // wherever the round-robin cursor places them. Either
    // regression breaks live back-pressure.
    let half = body.len() / 2;
    let d_in_back_half = body[half..].iter().any(|r| tag_of(r).starts_with("d-"));
    assert!(
        d_in_back_half,
        "slowest source src_d has no record in the back half of output {body:?} — \
         live interleave should spread the slow source across the full run",
    );

    // Invariant 4b — no source's records form one giant
    // contiguous block. The fairness regression of concern is
    // "merge greedily drains one channel before turning to the
    // next." Reject any run of more than 12 same-prefix rows
    // (more than 30% of the output is one source) — that's a
    // generous margin over the natural 10-record bound per
    // source, allowing for the early src_a cluster.
    let mut run_prefix: Option<char> = None;
    let mut run_len = 0usize;
    let mut max_run = 0usize;
    for row in &body {
        let p = tag_of(row).chars().next().expect("non-empty tag");
        if Some(p) == run_prefix {
            run_len += 1;
        } else {
            run_prefix = Some(p);
            run_len = 1;
        }
        max_run = max_run.max(run_len);
    }
    assert!(
        max_run <= 12,
        "longest contiguous same-source run is {max_run} (>12 of 40); \
         the merge arm is draining one source before any other gets a turn. \
         output: {body:?}",
    );
}

/// Two different seeds produce different interleavings. With 8+8
/// records the probability of two distinct fastrand schedules
/// coinciding bit-for-bit is negligible; this would only fail on a
/// regression that ignores the seed.
#[test]
fn distinct_seeds_diverge() {
    let yaml_one = pipeline_yaml("mode: interleave\n      interleave_seed: 1");
    let yaml_two = pipeline_yaml("mode: interleave\n      interleave_seed: 2");
    let with_one = run_pipeline(&yaml_one, 8, 8);
    let with_two = run_pipeline(&yaml_two, 8, 8);
    assert_per_source_fifo(&with_one);
    assert_per_source_fifo(&with_two);
    assert_ne!(
        with_one, with_two,
        "distinct seeds must yield distinct round-robin schedules"
    );
}

/// Shutdown latency on the fused `Merge.interleave -> streaming-Output`
/// path: a tripped token must unwind the merge `select()` loop promptly
/// instead of draining both sources to natural EOF.
///
/// This shape is distinct from the `source -> transform -> output` SIGINT
/// test: an unseeded interleave fuses straight into the streaming Output
/// writer, so the merge select loop — not a per-operator chunk loop — is
/// the only place a shutdown poll can land. A clean run never trips
/// shutdown, so the example-pipeline golden diff cannot exercise this path.
#[test]
fn interleave_shutdown_unwinds_mid_stream() {
    use clinker_exec::pipeline::shutdown::ShutdownToken;

    let yaml = pipeline_yaml("mode: interleave");
    let config = parse_config(&yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();

    // 6000 rows per source at 1 ms each, on concurrent ingest threads, so
    // a clean run takes ~6 s; the interrupted run must return far sooner.
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slow_slot(
                "a",
                &src_a_csv(6000),
                Duration::from_millis(1),
            )]),
        ),
        (
            "src_b".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slow_slot(
                "b",
                &src_b_csv(6000),
                Duration::from_millis(1),
            )]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let token = ShutdownToken::detached();
    let token_for_run = token.clone();
    let params = PipelineRunParams {
        execution_id: "interleave-sigint".to_string(),
        batch_id: "interleave-sigint".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: Some(token_for_run),
        ..Default::default()
    };

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let report =
            PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
                .expect("an interrupted run drains gracefully and returns Ok");
        let _ = done_tx.send(report);
    });

    // Let the merge interleave a few hundred rows, then signal shutdown.
    std::thread::sleep(Duration::from_millis(300));
    token.request();

    // The recv_timeout is only an anti-hang guard, generous because draining
    // the in-flight batch is gated by per-row sleep granularity and so runs
    // several-fold slower on macOS than Linux.
    let report = done_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("interrupted fused-merge run must terminate within the shutdown bound");
    worker.join().expect("executor thread did not panic");

    assert!(
        report.interrupted,
        "report must flag the fused-merge run interrupted"
    );
    // Promptness is asserted by row count, which is identical across platforms
    // — a run honoring the token stops within a couple of batches of the
    // signal, far short of the full 12000-row input; one that ignores it
    // ingests all 12000. Wall-clock here is unreliable (per-row sleep is
    // several-fold slower on macOS).
    assert!(
        report.counters.total_count < 12000,
        "interrupted run did not stop early; ingested {} of 12000 rows",
        report.counters.total_count
    );
}
