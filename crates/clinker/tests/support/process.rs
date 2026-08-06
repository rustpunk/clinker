//! Bounded direct-child runner used only by CLI integration tests.

use std::io::{self, BufReader};
use std::process::{ChildStdout, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

const DEFAULT_TAIL_BYTES: usize = 64 * 1024;
const DEFAULT_EVENT_LIMIT: usize = 256;
const MAX_MACHINE_RECORD_BYTES: usize = 16 * 1024;
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StdoutMode {
    Drain,
    CloseAfterLines(usize),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ProcessConfig {
    deadline: Duration,
    stdout_tail_bytes: usize,
    stderr_tail_bytes: usize,
    event_limit: usize,
    stdout_mode: StdoutMode,
}

impl ProcessConfig {
    pub(crate) fn new(deadline: Duration) -> Self {
        Self {
            deadline,
            stdout_tail_bytes: DEFAULT_TAIL_BYTES,
            stderr_tail_bytes: DEFAULT_TAIL_BYTES,
            event_limit: DEFAULT_EVENT_LIMIT,
            stdout_mode: StdoutMode::Drain,
        }
    }

    pub(crate) fn stdout_tail_bytes(mut self, bytes: usize) -> Self {
        self.stdout_tail_bytes = bytes;
        self
    }

    pub(crate) fn stderr_tail_bytes(mut self, bytes: usize) -> Self {
        self.stderr_tail_bytes = bytes;
        self
    }

    pub(crate) fn stdout_mode(mut self, mode: StdoutMode) -> Self {
        self.stdout_mode = mode;
        self
    }
}

#[derive(Debug, Clone)]
struct BoundedTail {
    retained: Vec<u8>,
    total_bytes: usize,
    capacity: usize,
}

impl BoundedTail {
    fn new(capacity: usize) -> Self {
        Self {
            retained: Vec::with_capacity(capacity),
            total_bytes: 0,
            capacity,
        }
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.total_bytes = self.total_bytes.saturating_add(bytes.len());
        if self.capacity == 0 {
            return;
        }
        if bytes.len() >= self.capacity {
            self.retained.clear();
            self.retained
                .extend_from_slice(&bytes[bytes.len() - self.capacity..]);
            return;
        }
        let overflow = self
            .retained
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(self.capacity);
        if overflow > 0 {
            self.retained.drain(..overflow);
        }
        self.retained.extend_from_slice(bytes);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProtocolDrain {
    events: Vec<Value>,
    parse_error: Option<String>,
    tail: BoundedTail,
}

impl ProtocolDrain {
    pub(crate) fn events(&self) -> &[Value] {
        &self.events
    }

    pub(crate) fn events_mut(&mut self) -> &mut Vec<Value> {
        &mut self.events
    }

    pub(crate) fn set_parse_error(&mut self, message: impl Into<String>) {
        self.parse_error = Some(message.into());
    }

    pub(crate) fn retained_tail(&self) -> &[u8] {
        &self.tail.retained
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.tail.total_bytes
    }
}

#[derive(Debug)]
pub(crate) struct DiagnosticDrain {
    tail: BoundedTail,
}

impl DiagnosticDrain {
    pub(crate) fn retained_tail(&self) -> &[u8] {
        &self.tail.retained
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.tail.total_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlledOutcome {
    Success,
    CompletedWithDlq,
    Cancelled,
    Failed,
    Incomplete,
}

#[derive(Debug)]
pub(crate) struct ProcessResult {
    status: ExitStatus,
    timed_out: bool,
    pub(crate) stdout: ProtocolDrain,
    pub(crate) stderr: DiagnosticDrain,
}

impl ProcessResult {
    pub(crate) fn outcome(&self) -> ControlledOutcome {
        self.outcome_for(&self.stdout)
    }

    pub(crate) fn outcome_for(&self, stdout: &ProtocolDrain) -> ControlledOutcome {
        reconcile(stdout, &self.status, self.timed_out)
    }

    pub(crate) fn status_code(&self) -> Option<i32> {
        self.status.code()
    }

    pub(crate) fn timed_out(&self) -> bool {
        self.timed_out
    }

    pub(crate) fn reaped(&self) -> bool {
        true
    }
}

pub(crate) fn run_child(mut command: Command, config: ProcessConfig) -> io::Result<ProcessResult> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("child stdout pipe was not created"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("child stderr pipe was not created"))?;

    let stdout_handle = thread::spawn(move || {
        drain_protocol(
            stdout,
            config.stdout_tail_bytes,
            config.event_limit,
            config.stdout_mode,
        )
    });
    let stderr_handle = thread::spawn(move || drain_diagnostics(stderr, config.stderr_tail_bytes));

    let started = Instant::now();
    let (status, timed_out) = loop {
        match child.try_wait() {
            Ok(Some(status)) => break (status, false),
            Ok(None) if started.elapsed() >= config.deadline => {
                let _ = child.kill();
                break (child.wait()?, true);
            }
            Ok(None) => thread::sleep(WAIT_POLL_INTERVAL),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = join_drain(stdout_handle, "stdout");
                let _ = join_drain(stderr_handle, "stderr");
                return Err(error);
            }
        }
    };

    let stdout = join_drain(stdout_handle, "stdout")??;
    let stderr = join_drain(stderr_handle, "stderr")??;
    Ok(ProcessResult {
        status,
        timed_out,
        stdout,
        stderr,
    })
}

fn join_drain<T>(
    handle: thread::JoinHandle<io::Result<T>>,
    stream: &str,
) -> io::Result<io::Result<T>> {
    handle
        .join()
        .map_err(|_| io::Error::other(format!("{stream} drain thread panicked")))
}

fn drain_protocol(
    stdout: ChildStdout,
    tail_bytes: usize,
    event_limit: usize,
    mode: StdoutMode,
) -> io::Result<ProtocolDrain> {
    drain_protocol_reader(BufReader::new(stdout), tail_bytes, event_limit, mode)
}

fn drain_protocol_reader(
    mut reader: impl io::BufRead,
    tail_bytes: usize,
    event_limit: usize,
    mode: StdoutMode,
) -> io::Result<ProtocolDrain> {
    let mut line = Vec::new();
    let mut line_count = 0_usize;
    let mut drain = ProtocolDrain {
        events: Vec::new(),
        parse_error: None,
        tail: BoundedTail::new(tail_bytes),
    };
    loop {
        let Some(record) = read_bounded_record(&mut reader, &mut line, &mut drain.tail)? else {
            break;
        };
        line_count = line_count.saturating_add(1);
        if record.oversized {
            drain.parse_error.get_or_insert_with(|| {
                format!("machine record exceeds {MAX_MACHINE_RECORD_BYTES}-byte limit")
            });
        } else if !record.terminated && drain.parse_error.is_none() {
            drain.parse_error = Some("machine record was not newline terminated".to_owned());
        }
        if record.oversized {
            if matches!(mode, StdoutMode::CloseAfterLines(limit) if line_count >= limit) {
                break;
            }
            continue;
        }
        let record = line
            .strip_suffix(b"\n")
            .unwrap_or(&line)
            .strip_suffix(b"\r")
            .unwrap_or_else(|| line.strip_suffix(b"\n").unwrap_or(&line));
        match std::str::from_utf8(record)
            .map_err(|error| error.to_string())
            .and_then(|text| serde_json::from_str(text).map_err(|error| error.to_string()))
        {
            Ok(event) if drain.events.len() < event_limit => drain.events.push(event),
            Ok(_) => {
                drain.parse_error.get_or_insert_with(|| {
                    format!("machine stream exceeded {event_limit}-event retention limit")
                });
            }
            Err(error) => {
                drain
                    .parse_error
                    .get_or_insert_with(|| format!("malformed machine record: {error}"));
            }
        }
        if matches!(mode, StdoutMode::CloseAfterLines(limit) if line_count >= limit) {
            break;
        }
    }
    Ok(drain)
}

#[derive(Clone, Copy)]
struct BoundedRecord {
    terminated: bool,
    oversized: bool,
}

fn read_bounded_record(
    reader: &mut impl io::BufRead,
    retained: &mut Vec<u8>,
    tail: &mut BoundedTail,
) -> io::Result<Option<BoundedRecord>> {
    retained.clear();
    let mut total = 0_usize;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if total == 0 {
                Ok(None)
            } else {
                Ok(Some(BoundedRecord {
                    terminated: false,
                    oversized: total > MAX_MACHINE_RECORD_BYTES,
                }))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let segment = &available[..consumed];
        tail.extend(segment);
        total = total.saturating_add(segment.len());
        if retained.len() <= MAX_MACHINE_RECORD_BYTES {
            let remaining = MAX_MACHINE_RECORD_BYTES
                .saturating_add(1)
                .saturating_sub(retained.len());
            retained.extend_from_slice(&segment[..segment.len().min(remaining)]);
        }
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(BoundedRecord {
                terminated: true,
                oversized: total > MAX_MACHINE_RECORD_BYTES,
            }));
        }
    }
}

fn drain_diagnostics(mut stderr: impl io::Read, tail_bytes: usize) -> io::Result<DiagnosticDrain> {
    let mut tail = BoundedTail::new(tail_bytes);
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let bytes_read = stderr.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        tail.extend(&buffer[..bytes_read]);
    }
    Ok(DiagnosticDrain { tail })
}

fn reconcile(stdout: &ProtocolDrain, status: &ExitStatus, timed_out: bool) -> ControlledOutcome {
    if timed_out || stdout.parse_error.is_some() || stdout.events.is_empty() {
        return ControlledOutcome::Incomplete;
    }
    let Some(status_code) = status.code() else {
        return ControlledOutcome::Incomplete;
    };
    let first = &stdout.events[0];
    let Some(batch_id) = first["batch_id"].as_str() else {
        return ControlledOutcome::Incomplete;
    };
    let Some(execution_id) = first["execution_id"].as_str() else {
        return ControlledOutcome::Incomplete;
    };
    if first["event"] != "started" {
        return ControlledOutcome::Incomplete;
    }
    for (sequence, event) in stdout.events.iter().enumerate() {
        if event["protocol"] != "clinker.run"
            || event["schema"] != 1
            || event["event"].as_str().is_none()
            || event["seq"].as_u64() != u64::try_from(sequence).ok()
            || event["batch_id"] != batch_id
            || event["execution_id"] != execution_id
            || !event["plan_identity"].is_object()
        {
            return ControlledOutcome::Incomplete;
        }
    }
    let terminals = stdout
        .events
        .iter()
        .filter(|event| {
            matches!(
                event["event"].as_str(),
                Some("completed" | "failed" | "cancelled")
            )
        })
        .collect::<Vec<_>>();
    if terminals.len() != 1 || !std::ptr::eq(terminals[0], stdout.events.last().unwrap()) {
        return ControlledOutcome::Incomplete;
    }
    let terminal = terminals[0];
    match terminal["event"].as_str() {
        Some("completed") if publication_is_valid(&stdout.events, terminal, true) => {
            match (terminal["result"].as_str(), terminal["exit_code"].as_i64()) {
                (Some("success"), Some(0)) if status_code == 0 => ControlledOutcome::Success,
                (Some("completed_with_dlq"), Some(2)) if status_code == 2 => {
                    ControlledOutcome::CompletedWithDlq
                }
                _ => ControlledOutcome::Incomplete,
            }
        }
        Some("failed") => {
            let embedded = terminal["exit_code"].as_i64();
            if embedded == Some(i64::from(status_code))
                && matches!(status_code, 1 | 3 | 4)
                && failure_is_valid(&terminal["failure"])
                && publication_is_valid(&stdout.events, terminal, false)
            {
                ControlledOutcome::Failed
            } else {
                ControlledOutcome::Incomplete
            }
        }
        Some("cancelled") => {
            let embedded = terminal["exit_code"].as_i64();
            if status_code == 130 && embedded.is_none_or(|code| code == 130) {
                ControlledOutcome::Cancelled
            } else {
                ControlledOutcome::Incomplete
            }
        }
        _ => ControlledOutcome::Incomplete,
    }
}

fn failure_is_valid(failure: &Value) -> bool {
    let Some(code) = failure["code"].as_str() else {
        return false;
    };
    let Some(category) = failure["category"].as_str() else {
        return false;
    };
    let Some(retry) = failure["retry"].as_str() else {
        return false;
    };
    let Some(message) = failure["message"].as_str() else {
        return false;
    };
    code.split('.').count() >= 3
        && !message.is_empty()
        && message.len() <= 240
        && matches!(
            category,
            "security_policy"
                | "source_protocol"
                | "internal_invariant"
                | "configuration"
                | "infrastructure"
                | "publication"
                | "observability"
        )
        && matches!(
            retry,
            "retry_with_backoff" | "do_not_retry" | "policy_required"
        )
}

fn publication_is_valid(events: &[Value], terminal: &Value, required: bool) -> bool {
    let chunks = events
        .iter()
        .filter(|event| event["event"] == "publication_artifacts")
        .collect::<Vec<_>>();
    let publication = &terminal["publication"];
    if publication.is_null() {
        return !required && chunks.is_empty();
    }
    let Some(complete) = publication["complete"].as_bool() else {
        return false;
    };
    let Some(cleanup_debt_count) = publication["cleanup_debt_count"].as_u64() else {
        return false;
    };
    let Some(artifact_count) = publication["artifact_count"].as_u64() else {
        return false;
    };
    let Some(state_counts) = publication["state_counts"].as_object() else {
        return false;
    };
    let states = [
        "staging",
        "ready",
        "promoting",
        "published",
        "visible_unsynchronized",
        "unpublished",
    ];
    let Some(expected_counts) = states
        .map(|state| state_counts.get(state).and_then(Value::as_u64))
        .into_iter()
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    if expected_counts.iter().sum::<u64>() != artifact_count
        || artifact_count > 4096
        || (complete
            && (expected_counts[3] != artifact_count
                || expected_counts
                    .iter()
                    .enumerate()
                    .any(|(index, count)| index != 3 && *count != 0)))
        || (required && (!complete || cleanup_debt_count != 0))
    {
        return false;
    }
    if artifact_count == 0 {
        return chunks.is_empty();
    }
    if chunks.is_empty() {
        return false;
    }

    let mut observed_counts = [0_u64; 6];
    let mut observed_artifacts = 0_u64;
    let mut ids = std::collections::BTreeSet::new();
    for (expected_index, event) in chunks.iter().enumerate() {
        let chunk = &event["publication"];
        if chunk["chunk_index"].as_u64() != u64::try_from(expected_index).ok()
            || chunk["chunk_count"].as_u64() != u64::try_from(chunks.len()).ok()
        {
            return false;
        }
        let Some(artifacts) = chunk["artifacts"]
            .as_array()
            .filter(|values| !values.is_empty())
        else {
            return false;
        };
        for artifact in artifacts {
            let Some(artifact_id) = artifact["artifact_id"].as_str() else {
                return false;
            };
            let Some(kind) = artifact["kind"].as_str() else {
                return false;
            };
            let Some(state) = artifact["state"].as_str() else {
                return false;
            };
            let Some(state_index) = states.iter().position(|candidate| *candidate == state) else {
                return false;
            };
            if artifact_id.is_empty()
                || !ids.insert(artifact_id)
                || !matches!(kind, "primary" | "fan_out" | "split" | "dlq" | "sidecar")
            {
                return false;
            }
            observed_artifacts = observed_artifacts.saturating_add(1);
            observed_counts[state_index] = observed_counts[state_index].saturating_add(1);
        }
    }
    observed_artifacts == artifact_count && observed_counts.as_slice() == expected_counts.as_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_machine_record_is_discarded_with_bounded_memory() {
        let mut bytes = vec![b'x'; MAX_MACHINE_RECORD_BYTES + 512];
        bytes.push(b'\n');
        bytes.extend_from_slice(br#"{"event":"after"}"#);
        bytes.push(b'\n');
        let drain = drain_protocol_reader(
            std::io::Cursor::new(&bytes),
            128,
            DEFAULT_EVENT_LIMIT,
            StdoutMode::Drain,
        )
        .expect("bounded drain");

        assert!(
            drain
                .parse_error
                .as_deref()
                .is_some_and(|error| error.contains("exceeds"))
        );
        assert_eq!(drain.events, [serde_json::json!({"event": "after"})]);
        assert_eq!(drain.total_bytes(), bytes.len());
        assert!(drain.retained_tail().len() <= 128);
    }
}
