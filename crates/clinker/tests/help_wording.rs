//! `clinker --help` must describe the tool the way the docs do: a
//! bounded-memory finite-batch ETL engine, not a "streaming ETL engine".
//! `docs/user/src/non-goals.md` lists unbounded stream processing as an
//! explicit non-goal, so the CLI summary contradicting that is a bug
//! (issue #451). These tests pin the wording at the CLI boundary.

use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Run `clinker <flag>` and return its stdout with runs of whitespace
/// collapsed to single spaces. clap reflows `long_about` to a
/// width-dependent line length, so normalizing here keeps the substring
/// checks independent of where any given clap version wraps a line.
fn help_stdout_normalized(flag: &str) -> String {
    let output = Command::new(clinker_bin())
        .arg(flag)
        .output()
        .expect("run clinker help");
    assert!(
        output.status.success(),
        "`clinker {flag}` should exit 0, got {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("help output is utf-8");
    stdout.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The short `-h` summary (clap `about`) frames Clinker as a batch engine.
#[test]
fn short_help_uses_batch_framing() {
    let stdout = help_stdout_normalized("-h");
    assert!(
        stdout.contains("batch"),
        "`clinker -h` summary should call Clinker a batch engine.\nstdout: {stdout}"
    );
    assert!(
        !stdout.contains("streaming ETL engine"),
        "`clinker -h` must not call Clinker a \"streaming ETL engine\".\nstdout: {stdout}"
    );
}

/// The long `--help` text (clap `long_about`) frames Clinker as a bounded
/// finite-batch job and explicitly disclaims stream-processor semantics, so
/// it reads consistently with the non-goals doc.
#[test]
fn long_help_disclaims_stream_processing() {
    let stdout = help_stdout_normalized("--help");
    assert!(
        stdout.contains("batch"),
        "`clinker --help` should describe a batch engine.\nstdout: {stdout}"
    );
    assert!(
        !stdout.contains("streaming ETL engine"),
        "`clinker --help` must not call Clinker a \"streaming ETL engine\".\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("not a long-running stream processor"),
        "`clinker --help` should explicitly disclaim long-running stream \
         processing so per-record evaluation is not misread as event-stream \
         processing.\nstdout: {stdout}"
    );
}
