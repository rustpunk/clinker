//! The real `clinker` binary and the test memory levers.
//!
//! A subprocess test holds a debug binary's ledger to a small capacity with
//! `CLINKER_TEST_LEDGER_CAPACITY`, set on the child only so parallel tests
//! stay independent, while `memory.limit` stays ample. The binary always
//! measures its own baseline: the in-process harness default that feature
//! unification enables for the executor under `cargo test` never reaches it.

#[cfg(debug_assertions)]
use std::path::Path;
use std::process::Command;
#[cfg(debug_assertions)]
use std::process::Output;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// The in-process harness's injected baseline, which the binary must never
/// report.
const IN_PROCESS_BASELINE_BYTES: u64 = 16 << 20;

/// Source → Route(a, b, c) → three Outputs. Every row is admitted to the
/// Source's own node buffer, which spills once a 1 MiB limit's soft
/// threshold is crossed.
#[cfg(debug_assertions)]
const ROUTE_FANOUT: &str = r#"pipeline:
  name: ledger_capacity_env
  memory:
    backpressure: spill
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
        - { name: payload, type: string }
        - { name: value, type: int }
        - { name: ts, type: int }
  - type: route
    name: by_region
    input: events
    config:
      mode: exclusive
      conditions:
        a: "region == \"a\""
        b: "region == \"b\""
      default: c
  - type: sink
    name: out_a
    input: by_region.a
    config:
      name: out_a
      type: csv
      path: out_a.csv
  - type: sink
    name: out_b
    input: by_region.b
    config:
      name: out_b
      type: csv
      path: out_b.csv
  - type: sink
    name: out_c
    input: by_region.c
    config:
      name: out_c
      type: csv
      path: out_c.csv
"#;

#[cfg(debug_assertions)]
const SPILL_REPORT: &str = "=== Spill Volume (actual, per stage) ===";

#[cfg(debug_assertions)]
fn events_csv() -> String {
    let mut csv = String::from("id,region,payload,value,ts\n");
    let mut id = 0u64;
    for (region, count) in [('a', 1_500), ('b', 300), ('c', 200)] {
        for _ in 0..count {
            id += 1;
            csv.push_str(&format!("id_{id},{region},payload_{id},{id},{id}\n"));
        }
    }
    csv
}

/// Run the route fan-out in a fresh directory at `--memory-limit 512M`,
/// with `capacity` set on the child when given. Returns the process output
/// and the three written files.
#[cfg(debug_assertions)]
fn run_route_fanout(capacity: Option<&str>) -> (Output, [Vec<u8>; 3]) {
    let dir = tempfile::tempdir().expect("create tempdir");
    std::fs::write(dir.path().join("events.csv"), events_csv()).expect("write input");
    std::fs::write(dir.path().join("pipeline.yaml"), ROUTE_FANOUT).expect("write pipeline");
    let mut command = Command::new(clinker_bin());
    command
        .args(["run", "pipeline.yaml", "--memory-limit", "512M"])
        .current_dir(dir.path())
        .env_remove("CLINKER_TEST_LEDGER_CAPACITY");
    if let Some(capacity) = capacity {
        command.env("CLINKER_TEST_LEDGER_CAPACITY", capacity);
    }
    let output = command.output().expect("spawn clinker");
    let read = |name: &str| read_output(dir.path(), name, &output);
    let files = [read("out_a.csv"), read("out_b.csv"), read("out_c.csv")];
    (output, files)
}

#[cfg(debug_assertions)]
fn read_output(dir: &Path, name: &str, output: &Output) -> Vec<u8> {
    std::fs::read(dir.join(name)).unwrap_or_else(|error| {
        panic!(
            "{name} was not written ({error}); stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// Only a debug binary reads the capacity variable, so a release-profile test
/// run would see the ample and held runs behave identically.
#[cfg(debug_assertions)]
#[test]
fn env_capacity_forces_spill_in_the_debug_binary() {
    let (ample, ample_files) = run_route_fanout(None);
    let ample_stdout = String::from_utf8_lossy(&ample.stdout);
    assert!(
        ample.status.success(),
        "the 512M run completes; stderr:\n{}",
        String::from_utf8_lossy(&ample.stderr)
    );
    assert!(
        !ample_stdout.contains(SPILL_REPORT),
        "nothing spills at 512M:\n{ample_stdout}"
    );

    let (held, held_files) = run_route_fanout(Some("1048576"));
    let held_stdout = String::from_utf8_lossy(&held.stdout);
    assert!(
        held.status.success(),
        "the run held to 1 MiB completes; stderr:\n{}",
        String::from_utf8_lossy(&held.stderr)
    );
    assert!(
        held_stdout.contains(SPILL_REPORT),
        "a 1 MiB ledger capacity spills the Source's buffer:\n{held_stdout}"
    );
    assert_eq!(
        held_files, ample_files,
        "holding the ledger to a small capacity changes no output byte"
    );
}

/// A one-row pipeline under the pausing policy, which the startup check
/// judges against the process's baseline.
const PAUSING: &str = r#"pipeline:
  name: ledger_capacity_real_baseline
  memory:
    backpressure: pause
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// The baseline an E312 names: the figure in "baseline resident memory
/// (N bytes)".
fn reported_baseline(stderr: &str) -> u64 {
    let marker = "baseline resident memory (";
    let start = stderr
        .find(marker)
        .unwrap_or_else(|| panic!("an E312 names the baseline; got:\n{stderr}"))
        + marker.len();
    let digits: String = stderr[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits
        .parse()
        .unwrap_or_else(|error| panic!("baseline figure {digits:?}: {error}"))
}

#[test]
fn cli_binary_reads_real_process_memory() {
    let dir = tempfile::tempdir().expect("create tempdir");
    std::fs::write(dir.path().join("in.csv"), "amount\n1\n").expect("write input");
    std::fs::write(dir.path().join("pipeline.yaml"), PAUSING).expect("write pipeline");
    let output = Command::new(clinker_bin())
        .args(["run", "pipeline.yaml", "--memory-limit", "1M"])
        .current_dir(dir.path())
        .env_remove("CLINKER_TEST_LEDGER_CAPACITY")
        .output()
        .expect("spawn clinker");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && stderr.contains("E312"),
        "a 1M limit under pause is refused at startup; stderr:\n{stderr}"
    );
    let baseline = reported_baseline(&stderr);
    assert_ne!(
        baseline, IN_PROCESS_BASELINE_BYTES,
        "the binary measured its baseline rather than taking the in-process default"
    );
    assert!(
        baseline > 1 << 20,
        "a measured baseline is above the 1M limit it refused: {baseline}"
    );
}
