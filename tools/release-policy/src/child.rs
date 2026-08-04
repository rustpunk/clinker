//! Bounded, shell-free child process execution.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use wait_timeout::ChildExt;

use crate::error::GateError;
use crate::limits::{
    MAX_CHILD_ARGUMENT_BYTES, MAX_CHILD_ARGUMENTS, MAX_CHILD_ENVIRONMENT,
    MAX_CHILD_ENVIRONMENT_BYTES, MAX_CHILD_OUTPUT_BYTES, os_len,
};

const MAX_CHILD_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const TERMINATION_GRACE: Duration = Duration::from_millis(250);
const ALLOWED_ENVIRONMENT: &[&str] = &[
    "CI",
    "CARGO_BUILD_JOBS",
    "CARGO_INCREMENTAL",
    "CLINKER_FILESYSTEM_PROFILE",
    "CLINKER_FILESYSTEM_ROOT",
    "GH_TOKEN",
    "GITHUB_ACTOR",
    "GITHUB_REF",
    "GITHUB_REPOSITORY",
    "GITHUB_RUN_ID",
    "GITHUB_SHA",
    "GITHUB_TOKEN",
    "LANG",
    "LC_ALL",
    "NO_COLOR",
    "PATH",
    "RUNNER_TEMP",
    "SOURCE_DATE_EPOCH",
    "TZ",
];

/// Explicit process input. No shell is involved and the inherited environment is cleared.
#[derive(Debug, Clone)]
pub struct ChildSpec {
    /// Executable path.
    pub program: PathBuf,
    /// Individual argv elements.
    pub arguments: Vec<OsString>,
    /// Explicit environment entries from the documented allowlist.
    pub environment: BTreeMap<OsString, OsString>,
    /// Wall-clock execution deadline.
    pub timeout: Duration,
    /// Maximum retained bytes in each output lane.
    pub output_limit: usize,
}

/// How the process finished after its whole process group was accounted for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    /// The process exited itself with the optional platform exit code.
    Exited(Option<i32>),
    /// The deadline elapsed and process-group teardown completed.
    TimedOut,
}

/// Bounded observation of one child invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildResult {
    /// Executable observed by the runner.
    pub program: PathBuf,
    /// Exact argv observed by the runner.
    pub arguments: Vec<OsString>,
    /// Exit or timeout classification.
    pub termination: Termination,
    /// Retained standard output prefix.
    pub stdout: Vec<u8>,
    /// Retained standard error prefix.
    pub stderr: Vec<u8>,
    /// Whether standard output exceeded its retention cap.
    pub stdout_truncated: bool,
    /// Whether standard error exceeded its retention cap.
    pub stderr_truncated: bool,
}

#[derive(Debug)]
struct Captured {
    bytes: Vec<u8>,
    truncated: bool,
}

/// Run one explicit command with bounded resources and process-group teardown.
pub fn run(spec: ChildSpec) -> Result<ChildResult, GateError> {
    validate_spec(&spec)?;

    let mut command = Command::new(&spec.program);
    command
        .args(&spec.arguments)
        .env_clear()
        .envs(&spec.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);

    let mut child = command
        .spawn()
        .map_err(|error| GateError::io("spawn child process", &error))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| GateError::internal("child.stdout", "child stdout pipe is missing"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| GateError::internal("child.stderr", "child stderr pipe is missing"))?;
    let output_limit = spec.output_limit;
    let stdout_reader = thread::spawn(move || capture(stdout, output_limit));
    let stderr_reader = thread::spawn(move || capture(stderr, output_limit));

    let termination = match child
        .wait_timeout(spec.timeout)
        .map_err(|error| GateError::io("wait for child process", &error))?
    {
        Some(status) => Termination::Exited(status.code()),
        None => {
            terminate_group(&mut child)?;
            Termination::TimedOut
        }
    };
    let stdout = join_capture(stdout_reader, "stdout")?;
    let stderr = join_capture(stderr_reader, "stderr")?;

    Ok(ChildResult {
        program: spec.program,
        arguments: spec.arguments,
        termination,
        stdout: stdout.bytes,
        stderr: stderr.bytes,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    })
}

/// Run one explicit command while streaming standard output into a file.
///
/// Standard error remains bounded by [`ChildSpec::output_limit`]. Standard
/// output is drained without retaining it in memory, written up to
/// `file_limit`, and reported as truncated if the child emits any additional
/// byte.
///
/// # Errors
///
/// Returns an error when the child specification is invalid, the output limit
/// is zero, the child cannot be spawned or reaped, or either output lane cannot
/// be drained.
pub fn run_stdout_to_file(
    spec: ChildSpec,
    output: File,
    file_limit: u64,
) -> Result<ChildResult, GateError> {
    validate_spec(&spec)?;
    if file_limit == 0 {
        return Err(GateError::usage(
            "child file output limit must be greater than zero",
        ));
    }

    let mut command = Command::new(&spec.program);
    command
        .args(&spec.arguments)
        .env_clear()
        .envs(&spec.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);

    let mut child = command
        .spawn()
        .map_err(|error| GateError::io("spawn child process", &error))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| GateError::internal("child.stdout", "child stdout pipe is missing"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| GateError::internal("child.stderr", "child stderr pipe is missing"))?;
    let stderr_limit = spec.output_limit;
    let stdout_reader = thread::spawn(move || capture_to_file(stdout, output, file_limit));
    let stderr_reader = thread::spawn(move || capture(stderr, stderr_limit));

    let termination = match child
        .wait_timeout(spec.timeout)
        .map_err(|error| GateError::io("wait for child process", &error))?
    {
        Some(status) => Termination::Exited(status.code()),
        None => {
            terminate_group(&mut child)?;
            Termination::TimedOut
        }
    };
    let stdout = join_capture(stdout_reader, "stdout")?;
    let stderr = join_capture(stderr_reader, "stderr")?;

    Ok(ChildResult {
        program: spec.program,
        arguments: spec.arguments,
        termination,
        stdout: stdout.bytes,
        stderr: stderr.bytes,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    })
}

fn validate_spec(spec: &ChildSpec) -> Result<(), GateError> {
    if spec.program.as_os_str().is_empty() {
        return Err(GateError::usage("child program must not be empty"));
    }
    if spec.arguments.len() > MAX_CHILD_ARGUMENTS {
        return Err(GateError::usage(format!(
            "child argv exceeds the {MAX_CHILD_ARGUMENTS}-argument limit"
        )));
    }
    let argument_bytes = spec
        .arguments
        .iter()
        .map(|value| os_len(value))
        .sum::<usize>();
    if argument_bytes > MAX_CHILD_ARGUMENT_BYTES {
        return Err(GateError::usage(format!(
            "child argv exceeds the {MAX_CHILD_ARGUMENT_BYTES}-byte limit"
        )));
    }
    if spec.environment.len() > MAX_CHILD_ENVIRONMENT {
        return Err(GateError::usage(format!(
            "child environment exceeds the {MAX_CHILD_ENVIRONMENT}-entry limit"
        )));
    }
    let environment_bytes = spec
        .environment
        .iter()
        .map(|(key, value)| os_len(key) + os_len(value))
        .sum::<usize>();
    if environment_bytes > MAX_CHILD_ENVIRONMENT_BYTES {
        return Err(GateError::usage(format!(
            "child environment exceeds the {MAX_CHILD_ENVIRONMENT_BYTES}-byte limit"
        )));
    }
    for key in spec.environment.keys() {
        if !environment_allowed(key) {
            return Err(GateError::policy(
                "child.environment",
                "child environment contains a non-allowlisted name",
            ));
        }
    }
    if spec.timeout.is_zero() || spec.timeout > MAX_CHILD_TIMEOUT {
        return Err(GateError::usage(
            "child timeout must be between one nanosecond and one hour",
        ));
    }
    if spec.output_limit == 0 || spec.output_limit > MAX_CHILD_OUTPUT_BYTES {
        return Err(GateError::usage(format!(
            "child output limit must be between 1 and {MAX_CHILD_OUTPUT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn environment_allowed(key: &OsStr) -> bool {
    ALLOWED_ENVIRONMENT
        .iter()
        .any(|allowed| key == OsStr::new(allowed))
}

fn capture(mut reader: impl Read, limit: usize) -> io::Result<Captured> {
    let mut retained = Vec::with_capacity(limit);
    let mut truncated = false;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(retained.len());
        let keep = remaining.min(read);
        retained.extend_from_slice(&buffer[..keep]);
        truncated |= keep < read;
    }
    Ok(Captured {
        bytes: retained,
        truncated,
    })
}

fn capture_to_file(mut reader: impl Read, mut output: File, limit: u64) -> io::Result<Captured> {
    let mut written = 0_u64;
    let mut truncated = false;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(written);
        let keep = usize::try_from(remaining.min(read as u64)).unwrap_or(read);
        output.write_all(&buffer[..keep])?;
        written += keep as u64;
        truncated |= keep < read;
    }
    output.flush()?;
    Ok(Captured {
        bytes: Vec::new(),
        truncated,
    })
}

fn join_capture(
    handle: thread::JoinHandle<io::Result<Captured>>,
    lane: &'static str,
) -> Result<Captured, GateError> {
    handle
        .join()
        .map_err(|_| GateError::internal("child.capture", format!("{lane} reader panicked")))?
        .map_err(|error| GateError::io("capture child output", &error))
}

fn terminate_group(child: &mut std::process::Child) -> Result<(), GateError> {
    let process_group = i32::try_from(child.id())
        .map(Pid::from_raw)
        .map_err(|_| GateError::internal("child.pid", "child identifier exceeded pid range"))?;
    let _ = killpg(process_group, Signal::SIGTERM);
    let exited = child
        .wait_timeout(TERMINATION_GRACE)
        .map_err(|error| GateError::io("wait for child termination", &error))?
        .is_some();
    // Kill any descendant that ignored SIGTERM even if the group leader exited first.
    let _ = killpg(process_group, Signal::SIGKILL);
    if !exited {
        child
            .wait()
            .map_err(|error| GateError::io("reap killed child process", &error))?;
    }
    Ok(())
}
