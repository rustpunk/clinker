//! Executes every scenario under `examples/scenarios/` end to end and compares
//! its output byte-for-byte against a committed golden.
//!
//! # Why this exists
//!
//! The pre-existing example gate (`examples_explain.rs`) runs `--explain`, which
//! is plan-only: it proves a pipeline *compiles*, never that it computes the
//! right answer. A pipeline can plan cleanly, run cleanly, exit 0, and write
//! silently wrong output. This harness closes that gap for the scenario corpus.
//!
//! # Why this lives in the `clinker` crate
//!
//! `CARGO_BIN_EXE_clinker` is only defined for integration tests of the package
//! that declares the binary. Driving the real CLI — rather than calling the
//! library — is deliberate: the scenario READMEs tell a reader to run
//! `clinker run pipeline.yaml`, and this asserts that exact command works.
//!
//! # Guarding the generator/golden coupling
//!
//! Scenario inputs are generated, not committed, so a golden is only meaningful
//! for the exact bytes that produced it. Each scenario therefore pins the
//! digest of its generated input, and that digest is asserted **before** any
//! output byte is compared. Generator drift then fails as "the input changed",
//! which is actionable, rather than as a baffling diff against expectations
//! that were computed from different data.
//!
//! # Re-blessing
//!
//! ```text
//! UPDATE_SCENARIO_GOLDENS=1 cargo test -p clinker --test scenarios -- --nocapture
//! ```
//!
//! rewrites every golden and prints each scenario's current input digest for
//! pasting back into [`GATES`]. `--nocapture` is required rather than optional:
//! libtest swallows stdout on a passing test, and a re-bless run passes, so
//! without it the digest this instruction tells you to copy is never shown.
//!
//! Review the resulting diff: a change in *shape* is the signal, a change in
//! every row usually means the generator moved.

use std::path::{Path, PathBuf};
use std::process::Command;

use clinker_scenarios::{REGISTRY, Scenario, materialize};

/// One scenario's expectations.
struct Gate {
    /// Directory name under `examples/scenarios/`.
    id: &'static str,
    /// blake3 digest of the generated input, from `clinker_scenarios`.
    ///
    /// Pinned so a generator change cannot silently invalidate the goldens
    /// below. Refresh with a re-bless run (see the module docs); printing the
    /// new digest needs `-- --nocapture`.
    input_digest: &'static str,
    /// Output files, relative to the scenario directory, compared byte-for-byte.
    outputs: &'static [&'static str],
    /// Expected `total, ok, written, dlq` from the run summary.
    counters: Counters,
    /// Set when the scenario is known not to hold on current `main`. The
    /// harness then requires the mismatch, so the gate stays honest and the
    /// test fails loudly once the referenced issue is fixed.
    known_broken: Option<&'static str>,
}

#[derive(Debug, PartialEq, Eq)]
struct Counters {
    total: u64,
    ok: u64,
    written: u64,
    dlq: u64,
}

/// DLQ columns that legitimately differ between two runs of identical input:
/// a per-entry UUID and a wall-clock stamp. They are blanked before comparison.
/// Every other DLQ column — source row, triggering value, error category and
/// the full original record — is compared verbatim.
const VOLATILE_DLQ_COLUMNS: usize = 2;

const GATES: &[Gate] = &[
    Gate {
        id: "01-storefront-orders",
        input_digest: "56c1401e7ea6",
        outputs: &["output/billable_lines.csv"],
        counters: Counters {
            total: 48,
            ok: 38,
            written: 38,
            dlq: 0,
        },
        known_broken: None,
    },
    Gate {
        id: "03-support-triage",
        input_digest: "d4c149c9653f",
        outputs: &[
            "output/urgent.csv",
            "output/standard.csv",
            "output/backlog.json",
            "output/rejected.csv",
        ],
        counters: Counters {
            total: 60,
            ok: 54,
            written: 54,
            dlq: 6,
        },
        known_broken: None,
    },
];

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/clinker; the workspace root is two up.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/clinker")
        .to_path_buf()
}

fn updating() -> bool {
    std::env::var("UPDATE_SCENARIO_GOLDENS").ok().as_deref() == Some("1")
}

/// Blank the leading `n` comma-separated columns of every data row.
///
/// The volatile DLQ columns are a UUID and an RFC 3339 timestamp, neither of
/// which can contain a comma or a quote, so splitting on the first `n` commas is
/// safe without a full CSV parse. Later columns — which may be quoted and may
/// contain commas — are left untouched.
fn blank_leading_columns(csv: &str, n: usize) -> String {
    let mut out = String::with_capacity(csv.len());
    for (i, line) in csv.lines().enumerate() {
        if i == 0 || line.is_empty() {
            out.push_str(line);
            out.push('\n');
            continue;
        }
        let mut rest = line;
        for _ in 0..n {
            match rest.split_once(',') {
                Some((_, tail)) => rest = tail,
                None => break,
            }
            out.push_str("<volatile>,");
        }
        out.push_str(rest);
        out.push('\n');
    }
    out
}

/// Normalize a single output for comparison.
fn normalize(rel_path: &str, raw: &[u8]) -> Vec<u8> {
    if rel_path.contains("rejected") {
        let text = String::from_utf8_lossy(raw);
        blank_leading_columns(&text, VOLATILE_DLQ_COLUMNS).into_bytes()
    } else {
        raw.to_vec()
    }
}

/// Parse the `Pipeline complete: N total, N ok, N written, N dlq` summary.
fn parse_counters(stdout: &str) -> Option<Counters> {
    let line = stdout.lines().find(|l| l.contains("Pipeline complete:"))?;
    let tail = line.split("Pipeline complete:").nth(1)?;
    let num = |label: &str| -> Option<u64> {
        // `split_whitespace` already skips leading whitespace, so the segment
        // needs no trimming before the count is read off the front.
        tail.split(',')
            .find(|p| p.trim_end().ends_with(label))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    };
    Some(Counters {
        total: num("total")?,
        ok: num("ok")?,
        written: num("written")?,
        dlq: num("dlq")?,
    })
}

fn find_scenario(id: &str) -> &'static Scenario {
    REGISTRY
        .iter()
        .find(|s| s.id == id)
        .unwrap_or_else(|| panic!("gate names scenario '{id}', absent from the generator registry"))
}

/// One problem found while running a gate.
///
/// `tolerable` distinguishes the two kinds. A golden mismatch is what a
/// `known_broken` marker is *for* and may be suppressed. Everything else —
/// generator-digest drift, a wrong exit code, a missing output file, an
/// unparseable summary — indicates the scenario is not doing what the gate
/// believes at all, and must surface even on a known-broken scenario. Without
/// that split, a scenario parked on an issue could silently start crashing, or
/// its input could drift, and the eventual un-parking would bless goldens
/// against data nobody reasoned about.
struct Failure {
    tolerable: bool,
    message: String,
}

impl Failure {
    /// A golden mismatch — the defect class `known_broken` may suppress.
    fn golden(message: String) -> Self {
        Self {
            tolerable: true,
            message,
        }
    }

    /// Anything else; surfaces regardless of `known_broken`.
    fn fatal(message: String) -> Self {
        Self {
            tolerable: false,
            message,
        }
    }
}

/// Run one gate, returning a description of every mismatch found.
///
/// Collects rather than asserting so one run reports every scenario's problems
/// at once, matching the aggregate-report style of `examples_explain.rs`.
fn run_gate(gate: &Gate) -> Vec<Failure> {
    let mut failures = Vec::new();
    let root = repo_root();
    let scenario_dir = root.join("examples/scenarios").join(gate.id);
    let scenario = find_scenario(gate.id);

    // Work in a temporary copy so the suite never mutates the checkout and
    // parallel test threads cannot collide over one `output/` directory.
    let tmp = tempfile::tempdir().expect("tempdir");
    let work = tmp.path();
    std::fs::copy(
        scenario_dir.join("pipeline.yaml"),
        work.join("pipeline.yaml"),
    )
    .expect("copy pipeline.yaml");
    let schema_dir = scenario_dir.join("schema");
    if schema_dir.is_dir() {
        let dest = work.join("schema");
        std::fs::create_dir_all(&dest).expect("create schema dir");
        for entry in std::fs::read_dir(&schema_dir).expect("read schema dir") {
            let entry = entry.expect("schema entry");
            std::fs::copy(entry.path(), dest.join(entry.file_name())).expect("copy schema file");
        }
    }
    std::fs::create_dir_all(work.join("output")).expect("create output dir");

    // Generate the input, then pin it. This ordering is the point: a moved
    // generator must fail here, not as an unexplained output diff below.
    let data = (scenario.generate)();
    let digest = data.digest();
    let short = &digest[..12];
    if updating() {
        println!("{}: input_digest = \"{short}\"", gate.id);
    } else if short != gate.input_digest {
        failures.push(Failure::fatal(format!(
            "{}: generated input digest is {short}, gate pins {}. The generator moved; \
             bump GENERATOR_VERSION, re-bless with \
             `UPDATE_SCENARIO_GOLDENS=1 cargo test -p clinker --test scenarios -- --nocapture` \
             (--nocapture is required to see the new digest), then review the golden diff \
             before trusting it.",
            gate.id, gate.input_digest
        )));
        return failures;
    }
    materialize(&data, &work.join("data"), true).expect("materialize input");

    let output = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(work)
        .arg("run")
        .arg("pipeline.yaml")
        // The default batch id is a fresh UUID v7; pin it so nothing
        // batch-derived can vary between runs.
        .args(["--batch-id", "scenario-gate-fixed-batch"])
        .arg("--force")
        .output()
        .expect("spawn clinker");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    // Documented contract (`clinker --help`): 0 is clean, 2 means the run
    // completed but produced DLQ entries. Asserting the exact code — rather
    // than just "did not fail" — is what keeps a scenario that starts
    // dead-lettering unexpectedly from passing silently.
    let expected_exit = if gate.counters.dlq > 0 { 2 } else { 0 };
    match output.status.code() {
        Some(code) if code == expected_exit => {}
        Some(code) => {
            failures.push(Failure::fatal(format!(
                "{}: exit code {code}, expected {expected_exit} ({} DLQ entries).\
                 \nstdout:\n{stdout}\nstderr:\n{stderr}",
                gate.id, gate.counters.dlq
            )));
            return failures;
        }
        None => {
            failures.push(Failure::fatal(format!("{}: terminated by signal", gate.id)));
            return failures;
        }
    }

    // The run summary is written to stdout.
    match parse_counters(&stdout) {
        Some(actual) if actual != gate.counters => failures.push(Failure::fatal(format!(
            "{}: run summary {:?} does not match the gate's {:?}",
            gate.id, actual, gate.counters
        ))),
        None => failures.push(Failure::fatal(format!(
            "{}: could not find a 'Pipeline complete:' summary in stdout:\n{stdout}",
            gate.id
        ))),
        Some(_) => {}
    }

    for rel in gate.outputs {
        let produced = work.join(rel);
        let golden = scenario_dir
            .join("expected")
            .join(Path::new(rel).file_name().expect("output file name"));

        let Ok(raw) = std::fs::read(&produced) else {
            failures.push(Failure::fatal(format!(
                "{}: expected output {rel} was not written",
                gate.id
            )));
            continue;
        };
        let actual = normalize(rel, &raw);

        if updating() {
            std::fs::create_dir_all(golden.parent().expect("expected dir"))
                .expect("create expected dir");
            std::fs::write(&golden, &actual).expect("write golden");
            continue;
        }

        match std::fs::read(&golden) {
            Ok(expected) if expected == actual => {}
            Ok(expected) => failures.push(Failure::golden(format!(
                "{}: {rel} does not match its golden.\n{}",
                gate.id,
                first_difference(&expected, &actual)
            ))),
            Err(_) => failures.push(Failure::fatal(format!(
                "{}: no committed golden at {}. Create it with \
                 UPDATE_SCENARIO_GOLDENS=1 and review the result.",
                gate.id,
                golden.display()
            ))),
        }
    }

    failures
}

/// Render the first differing line, which is far more useful than a byte offset.
fn first_difference(expected: &[u8], actual: &[u8]) -> String {
    let e = String::from_utf8_lossy(expected);
    let a = String::from_utf8_lossy(actual);
    for (i, (le, la)) in e.lines().zip(a.lines()).enumerate() {
        if le != la {
            return format!(
                "  first difference at line {}:\n  expected: {le}\n  actual:   {la}",
                i + 1
            );
        }
    }
    format!(
        "  line counts differ: expected {}, actual {}",
        e.lines().count(),
        a.lines().count()
    )
}

#[test]
fn every_scenario_matches_its_golden() {
    let mut report = Vec::new();

    for gate in GATES {
        let failures = run_gate(gate);

        // Anything that is not a golden mismatch surfaces regardless of the
        // marker: a parked scenario must still be running, reading the input
        // the gate pins, and exiting as expected.
        report.extend(
            failures
                .iter()
                .filter(|f| !f.tolerable)
                .map(|f| f.message.clone()),
        );

        match gate.known_broken {
            // A gate marked known-broken must actually be broken *in the way
            // the marker claims*. If the golden now matches, the marker is
            // stale and the test says so rather than quietly continuing to skip
            // a now-working scenario.
            Some(issue) if !failures.iter().any(|f| f.tolerable) && !updating() => {
                report.push(format!(
                    "{}: marked known-broken against {issue}, but its golden now matches. \
                     Remove `known_broken` and land the golden.",
                    gate.id
                ));
            }
            Some(_) => {}
            None => report.extend(
                failures
                    .iter()
                    .filter(|f| f.tolerable)
                    .map(|f| f.message.clone()),
            ),
        }
    }

    assert!(
        report.is_empty(),
        "scenario gate failures:\n\n{}\n",
        report.join("\n\n")
    );
}

#[test]
fn every_gate_names_a_real_scenario_directory() {
    let root = repo_root();
    for gate in GATES {
        let dir = root.join("examples/scenarios").join(gate.id);
        assert!(
            dir.join("pipeline.yaml").is_file(),
            "gate {} has no pipeline.yaml at {}",
            gate.id,
            dir.display()
        );
        assert!(
            REGISTRY.iter().any(|s| s.id == gate.id),
            "gate {} is absent from the generator registry",
            gate.id
        );
    }
}

#[test]
fn every_registered_scenario_has_a_gate() {
    // The coverage direction that actually matters, and the one the enclosing
    // `for gate in GATES` loops cannot express. A contributor who adds a
    // scenario to REGISTRY, commits its pipeline, README and goldens, but
    // forgets the GATES entry would otherwise get a fully green suite while
    // that scenario is never executed and its committed golden asserts nothing.
    for scenario in REGISTRY {
        assert!(
            GATES.iter().any(|g| g.id == scenario.id),
            "scenario '{}' is in the generator registry but has no gate, so it is \
             generated and never executed. Add a Gate entry with its pinned input \
             digest, expected outputs and run counters.",
            scenario.id
        );
    }
}

#[test]
fn blanking_leaves_later_columns_untouched() {
    // The DLQ's trailing columns embed the original record and may contain
    // quoted commas; blanking must not disturb them.
    let csv = "a,b,c\nuuid,2026-01-01T00:00:00Z,\"has,comma\"\n";
    assert_eq!(
        blank_leading_columns(csv, 2),
        "a,b,c\n<volatile>,<volatile>,\"has,comma\"\n"
    );
}

#[test]
fn counter_parsing_reads_the_run_summary() {
    let line =
        "2026-07-25T22:08:30Z INFO clinker: Pipeline complete: 60 total, 57 ok, 57 written, 3 dlq";
    assert_eq!(
        parse_counters(line),
        Some(Counters {
            total: 60,
            ok: 57,
            written: 57,
            dlq: 3
        })
    );
    assert_eq!(parse_counters("nothing here"), None);
}
