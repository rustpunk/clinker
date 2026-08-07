//! Fixed-memory, transport-neutral telemetry production.
//!
//! The producer owns one preallocated byte arena split into disjoint ordinary
//! and high-severity lanes. Privacy policy, attribute limits, sampling, rate
//! limiting, and severity routing all run before a byte becomes retained.
//! Producers never block, grow the arena, spill, or write the metrics spool.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::Instant;

use clinker_plan::config::{FieldPolicyAction, ResolvedFieldPolicy, ResolvedObservabilityPolicy};
use clinker_record::Value;
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};

/// Closed severity vocabulary used for lane routing.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Severity {
    const fn lane(self) -> AdmissionLane {
        match self {
            Self::Trace | Self::Debug | Self::Info => AdmissionLane::Ordinary,
            Self::Warn | Self::Error => AdmissionLane::HighSeverity,
        }
    }
}

/// One explicitly requested event field. Unlisted fields remain denied.
#[derive(Clone, Copy, Debug)]
pub struct SignalField<'a> {
    pub name: &'a str,
    pub value: SignalValue<'a>,
}

/// Borrowed producer-side field value. Record values remain typed until exact
/// event-field policy has authorized their serialization.
#[derive(Clone, Copy, Debug)]
pub enum SignalValue<'a> {
    Text(&'a str),
    Record(&'a Value),
}

impl<'a> SignalField<'a> {
    #[must_use]
    pub const fn new(name: &'a str, value: &'a str) -> Self {
        Self {
            name,
            value: SignalValue::Text(value),
        }
    }

    /// Keep an explicitly selected record value typed until deployment policy
    /// has allowed, hashed, or replaced this exact event-field pair.
    #[must_use]
    pub const fn from_record(name: &'a str, value: &'a Value) -> Self {
        Self {
            name,
            value: SignalValue::Record(value),
        }
    }
}

/// Borrowed producer-side log event.
#[derive(Clone, Copy, Debug)]
pub struct LogEvent<'a> {
    pub event: &'a str,
    pub severity: Severity,
    pub message: &'a str,
    pub fields: &'a [SignalField<'a>],
}

/// Closed fixed-cardinality metric keys.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKey {
    TransformStarted,
    TransformCompleted,
    TransformRecords,
    TransformErrors,
}

impl MetricKey {
    const ALL: [Self; 4] = [
        Self::TransformStarted,
        Self::TransformCompleted,
        Self::TransformRecords,
        Self::TransformErrors,
    ];
    const COUNT: usize = Self::ALL.len();

    const fn index(self) -> usize {
        match self {
            Self::TransformStarted => 0,
            Self::TransformCompleted => 1,
            Self::TransformRecords => 2,
            Self::TransformErrors => 3,
        }
    }
}

/// Closed span names retained by the executor.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanName {
    Transform,
}

/// Bounded span lifecycle fact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanPhase {
    Start,
    End,
}

/// Bounded span result fact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

/// Borrowed producer-side trace fact. No record or error payload is accepted.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct SpanFact<'a> {
    pub name: SpanName,
    pub phase: SpanPhase,
    pub status: SpanStatus,
    pub logical_node: &'a str,
}

/// Lane selected before serialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionLane {
    Ordinary,
    HighSeverity,
}

/// Why an optional signal was not retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    Sampled,
    RateLimited,
    Contended,
    Full,
    Oversize,
    InvalidLogicalIdentity,
}

/// Non-blocking producer outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionOutcome {
    Accepted { lane: AdmissionLane, bytes: u64 },
    Dropped(DropReason),
}

impl AdmissionOutcome {
    #[must_use]
    pub const fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted { .. })
    }

    #[must_use]
    pub const fn is_full(self) -> bool {
        matches!(self, Self::Dropped(DropReason::Full))
    }
}

/// Arena construction failure. Enabled telemetry must reserve its complete
/// fixed baseline before execution starts.
#[derive(Debug)]
pub enum TelemetryArenaError {
    Disabled,
    CapacityOutOfRange,
    Allocation(std::collections::TryReserveError),
}

impl fmt::Display for TelemetryArenaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("telemetry policy is disabled"),
            Self::CapacityOutOfRange => {
                f.write_str("telemetry capacity cannot be represented on this platform")
            }
            Self::Allocation(_) => f.write_str("telemetry arena reservation failed"),
        }
    }
}

impl std::error::Error for TelemetryArenaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Allocation(error) => Some(error),
            Self::Disabled | Self::CapacityOutOfRange => None,
        }
    }
}

/// Factory for the one fixed telemetry arena.
pub struct TelemetryArena;

impl TelemetryArena {
    /// Reserve the complete arena and return its clonable producer plus sole
    /// bounded receiver.
    pub fn new(
        policy: &ResolvedObservabilityPolicy,
    ) -> Result<(TelemetryProducer, TelemetryReceiver), TelemetryArenaError> {
        if !policy.is_enabled() {
            return Err(TelemetryArenaError::Disabled);
        }

        let arena_bytes = usize::try_from(policy.arena_bytes())
            .map_err(|_| TelemetryArenaError::CapacityOutOfRange)?;
        let ordinary_bytes = usize::try_from(policy.ordinary_lane_bytes())
            .map_err(|_| TelemetryArenaError::CapacityOutOfRange)?;
        let high_bytes = usize::try_from(policy.high_severity_lane_bytes())
            .map_err(|_| TelemetryArenaError::CapacityOutOfRange)?;
        let slot_bytes = usize::try_from(policy.max_batch_bytes())
            .map_err(|_| TelemetryArenaError::CapacityOutOfRange)?;

        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(arena_bytes)
            .map_err(TelemetryArenaError::Allocation)?;
        bytes.resize(arena_bytes, 0);

        let ordinary = LaneState::new(0, ordinary_bytes, slot_bytes)?;
        let high = LaneState::new(ordinary_bytes, high_bytes, slot_bytes)?;
        let queue_capacity = ordinary
            .slots
            .len()
            .checked_add(high.slots.len())
            .ok_or(TelemetryArenaError::CapacityOutOfRange)?;
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(queue_capacity)
            .map_err(TelemetryArenaError::Allocation)?;

        let retained = Arc::new(RetainedAccounting::default());
        let metrics = Arc::new(MetricCounters::default());
        let stats = Arc::new(ProducerCounters::default());
        let shared = Arc::new(Mutex::new(ArenaState {
            bytes: bytes.into_boxed_slice(),
            ordinary,
            high,
            queue,
            rate: RateLimiter::new(policy.rate_limit_per_second(), policy.rate_limit_burst()),
        }));

        let producer = TelemetryProducer {
            policy: Arc::new(policy.clone()),
            shared: Arc::clone(&shared),
            metrics: Arc::clone(&metrics),
            stats: Arc::clone(&stats),
            retained: Arc::clone(&retained),
            sample_sequence: Arc::new(AtomicU64::new(0)),
        };
        let receiver = TelemetryReceiver {
            shared,
            metrics,
            retained,
        };
        Ok((producer, receiver))
    }

    /// Disabled policies perform no allocation and create no producer.
    pub fn optional(
        policy: &ResolvedObservabilityPolicy,
    ) -> Result<Option<(TelemetryProducer, TelemetryReceiver)>, TelemetryArenaError> {
        if policy.is_enabled() {
            Self::new(policy).map(Some)
        } else {
            Ok(None)
        }
    }
}

/// Clonable, non-blocking telemetry producer.
#[derive(Clone)]
pub struct TelemetryProducer {
    policy: Arc<ResolvedObservabilityPolicy>,
    shared: Arc<Mutex<ArenaState>>,
    metrics: Arc<MetricCounters>,
    stats: Arc<ProducerCounters>,
    retained: Arc<RetainedAccounting>,
    sample_sequence: Arc<AtomicU64>,
}

impl TelemetryProducer {
    /// Apply exact field policy and admit one log record without blocking.
    pub fn emit_log(&self, event: LogEvent<'_>) -> AdmissionOutcome {
        if !valid_logical_identity(event.event) {
            self.stats.invalid.fetch_add(1, Ordering::Relaxed);
            return AdmissionOutcome::Dropped(DropReason::InvalidLogicalIdentity);
        }

        let privacy = PrivacyScan::new(&self.policy, event.event, event.fields);
        self.stats
            .denied_fields
            .fetch_add(privacy.denied, Ordering::Relaxed);
        self.stats
            .truncated_fields
            .fetch_add(privacy.truncated, Ordering::Relaxed);
        self.stats
            .attribute_limit_drops
            .fetch_add(privacy.limit_drops, Ordering::Relaxed);

        let filtered = FilteredLog {
            event,
            policy: &self.policy,
        };
        self.admit(event.severity.lane(), &QueuedSignal::Log(filtered))
    }

    /// Admit one closed trace fact without accepting arbitrary attributes.
    pub fn emit_span(&self, span: SpanFact<'_>) -> AdmissionOutcome {
        if !valid_logical_identity(span.logical_node) {
            self.stats.invalid.fetch_add(1, Ordering::Relaxed);
            return AdmissionOutcome::Dropped(DropReason::InvalidLogicalIdentity);
        }
        let lane = if span.status == SpanStatus::Error {
            AdmissionLane::HighSeverity
        } else {
            AdmissionLane::Ordinary
        };
        self.admit(lane, &QueuedSignal::Trace(span))
    }

    /// Coalesce a closed metric key with saturating arithmetic.
    pub fn record_metric(&self, key: MetricKey, delta: u64) {
        self.metrics.add(key, delta);
    }

    /// Return exact fixed-capacity and aggregate outcome accounting.
    #[must_use]
    pub fn snapshot(&self) -> ArenaSnapshot {
        ArenaSnapshot {
            owned_bytes: self.policy.arena_bytes(),
            ordinary_capacity_bytes: self.policy.ordinary_lane_bytes(),
            high_capacity_bytes: self.policy.high_severity_lane_bytes(),
            retained_bytes: self.retained.total.load(Ordering::Acquire),
            ordinary_retained_bytes: self.retained.ordinary.load(Ordering::Acquire),
            high_retained_bytes: self.retained.high.load(Ordering::Acquire),
            peak_retained_bytes: self.retained.peak.load(Ordering::Acquire),
            accepted: self.stats.accepted.load(Ordering::Relaxed),
            denied_fields: self.stats.denied_fields.load(Ordering::Relaxed),
            truncated_fields: self.stats.truncated_fields.load(Ordering::Relaxed),
            attribute_limit_drops: self.stats.attribute_limit_drops.load(Ordering::Relaxed),
            sampled_drops: self.stats.sampled.load(Ordering::Relaxed),
            rate_limited_drops: self.stats.rate_limited.load(Ordering::Relaxed),
            contention_drops: self.stats.contended.load(Ordering::Relaxed),
            full_drops: self.stats.full.load(Ordering::Relaxed),
            oversize_drops: self.stats.oversize.load(Ordering::Relaxed),
            invalid_drops: self.stats.invalid.load(Ordering::Relaxed),
        }
    }

    fn admit<T: Serialize>(&self, lane: AdmissionLane, signal: &T) -> AdmissionOutcome {
        let sequence = self.sample_sequence.fetch_add(1, Ordering::Relaxed);
        if !sequence.is_multiple_of(u64::from(self.policy.sample_every())) {
            self.stats.sampled.fetch_add(1, Ordering::Relaxed);
            return AdmissionOutcome::Dropped(DropReason::Sampled);
        }

        let mut shared = match self.shared.try_lock() {
            Ok(shared) => shared,
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => {
                self.stats.contended.fetch_add(1, Ordering::Relaxed);
                return AdmissionOutcome::Dropped(DropReason::Contended);
            }
        };
        if !shared.rate.allow() {
            self.stats.rate_limited.fetch_add(1, Ordering::Relaxed);
            return AdmissionOutcome::Dropped(DropReason::RateLimited);
        }

        match shared.admit(lane, signal) {
            Ok(bytes) => {
                let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
                self.retained.add(lane, bytes);
                self.stats.accepted.fetch_add(1, Ordering::Relaxed);
                AdmissionOutcome::Accepted { lane, bytes }
            }
            Err(DropReason::Full) => {
                self.stats.full.fetch_add(1, Ordering::Relaxed);
                AdmissionOutcome::Dropped(DropReason::Full)
            }
            Err(DropReason::Oversize) => {
                self.stats.oversize.fetch_add(1, Ordering::Relaxed);
                AdmissionOutcome::Dropped(DropReason::Oversize)
            }
            Err(reason) => AdmissionOutcome::Dropped(reason),
        }
    }
}

/// Sole bounded receiver for structured telemetry facts.
pub struct TelemetryReceiver {
    shared: Arc<Mutex<ArenaState>>,
    metrics: Arc<MetricCounters>,
    retained: Arc<RetainedAccounting>,
}

impl TelemetryReceiver {
    /// Drain currently retained facts into one bounded transport-neutral batch.
    /// Returns immediately when the arena is busy or no signal is ready.
    pub fn try_recv_batch(&self) -> Option<TelemetryBatch> {
        let mut shared = match self.shared.try_lock() {
            Ok(shared) => shared,
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => return None,
        };

        let queued = shared.queue.len();
        let mut logs = Vec::with_capacity(queued);
        let mut traces = Vec::with_capacity(queued);
        let mut serialized_bytes = 0_u64;
        while let Some(token) = shared.queue.pop_front() {
            let (offset, len) = shared.slot_location(token);
            let parsed =
                serde_json::from_slice::<StoredSignal>(&shared.bytes[offset..offset + len]);
            if let Ok(signal) = parsed {
                match signal {
                    StoredSignal::Log(log) => logs.push(log),
                    StoredSignal::Trace(trace) => traces.push(trace),
                }
                serialized_bytes = serialized_bytes.saturating_add(len as u64);
            }
            shared.release(token);
            self.retained.subtract(token.lane, len as u64);
        }
        drop(shared);

        let metrics = self.metrics.drain();
        if logs.is_empty() && traces.is_empty() && metrics.is_empty() {
            return None;
        }
        Some(TelemetryBatch {
            logs,
            metrics,
            traces,
            serialized_bytes,
        })
    }
}

/// Receiver-owned typed log record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogRecord {
    pub event: String,
    pub severity: Severity,
    pub message: String,
    pub fields: BTreeMap<String, String>,
}

/// Receiver-owned fixed metric point.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetricPoint {
    pub key: MetricKey,
    pub value: u64,
}

/// Receiver-owned bounded trace fact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TraceSpan {
    pub name: SpanName,
    pub phase: SpanPhase,
    pub status: SpanStatus,
    pub logical_node: String,
}

/// One transport-neutral bounded three-signal batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TelemetryBatch {
    logs: Vec<LogRecord>,
    metrics: Vec<MetricPoint>,
    traces: Vec<TraceSpan>,
    serialized_bytes: u64,
}

impl TelemetryBatch {
    #[must_use]
    pub fn logs(&self) -> &[LogRecord] {
        &self.logs
    }

    #[must_use]
    pub fn metrics(&self) -> &[MetricPoint] {
        &self.metrics
    }

    #[must_use]
    pub fn traces(&self) -> &[TraceSpan] {
        &self.traces
    }

    #[must_use]
    pub fn serialized_bytes(&self) -> u64 {
        self.serialized_bytes
    }

    #[must_use]
    pub fn presence(&self) -> SignalPresence {
        SignalPresence {
            logs: !self.logs.is_empty(),
            metrics: !self.metrics.is_empty(),
            traces: !self.traces.is_empty(),
        }
    }

    #[must_use]
    pub fn metric(&self, key: MetricKey) -> u64 {
        self.metrics
            .iter()
            .find(|point| point.key == key)
            .map_or(0, |point| point.value)
    }
}

/// Exact signal presence for delivery routing.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SignalPresence {
    pub logs: bool,
    pub metrics: bool,
    pub traces: bool,
}

/// Exact fixed capacity and aggregate producer outcome accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArenaSnapshot {
    pub owned_bytes: u64,
    pub ordinary_capacity_bytes: u64,
    pub high_capacity_bytes: u64,
    pub retained_bytes: u64,
    pub ordinary_retained_bytes: u64,
    pub high_retained_bytes: u64,
    pub peak_retained_bytes: u64,
    pub accepted: u64,
    pub denied_fields: u64,
    pub truncated_fields: u64,
    pub attribute_limit_drops: u64,
    pub sampled_drops: u64,
    pub rate_limited_drops: u64,
    pub contention_drops: u64,
    pub full_drops: u64,
    pub oversize_drops: u64,
    pub invalid_drops: u64,
}

#[derive(Serialize)]
#[serde(tag = "signal", content = "data", rename_all = "snake_case")]
enum QueuedSignal<'a> {
    Log(FilteredLog<'a>),
    Trace(SpanFact<'a>),
}

#[derive(Deserialize)]
#[serde(tag = "signal", content = "data", rename_all = "snake_case")]
enum StoredSignal {
    Log(LogRecord),
    Trace(TraceSpan),
}

struct FilteredLog<'a> {
    event: LogEvent<'a>,
    policy: &'a ResolvedObservabilityPolicy,
}

impl Serialize for FilteredLog<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Header<'a, F> {
            event: &'a str,
            severity: Severity,
            message: &'a str,
            fields: F,
        }

        Header {
            event: self.event.event,
            severity: self.event.severity,
            message: self.event.message,
            fields: FilteredFields {
                policy: self.policy,
                event: self.event.event,
                fields: self.event.fields,
            },
        }
        .serialize(serializer)
    }
}

struct FilteredFields<'a> {
    policy: &'a ResolvedObservabilityPolicy,
    event: &'a str,
    fields: &'a [SignalField<'a>],
}

impl Serialize for FilteredFields<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let limit = self.policy.max_attributes_per_event() as usize;
        let mut map = serializer.serialize_map(None)?;
        let mut retained = 0_usize;
        for field in self.fields {
            let Some(rule) = field_rule(self.policy, self.event, field.name) else {
                continue;
            };
            if retained == limit {
                break;
            }
            retained += 1;
            match rule.action() {
                FieldPolicyAction::Allow => match field.value {
                    SignalValue::Text(value) => map.serialize_entry(
                        field.name,
                        truncate_utf8(value, self.policy.max_attribute_bytes() as usize),
                    )?,
                    SignalValue::Record(value) => {
                        let value = render_record_value_bounded(
                            value,
                            self.policy.max_attribute_bytes() as usize,
                        );
                        map.serialize_entry(field.name, &value)?;
                    }
                },
                FieldPolicyAction::Hash => {
                    let hash = match field.value {
                        SignalValue::Text(value) => blake3::hash(value.as_bytes()),
                        SignalValue::Record(value) => hash_record_value(value),
                    };
                    map.serialize_entry(field.name, &HashValue(hash))?;
                }
                FieldPolicyAction::Replace => {
                    let replacement = rule.replacement().unwrap_or("[redacted]");
                    map.serialize_entry(
                        field.name,
                        truncate_utf8(replacement, self.policy.max_attribute_bytes() as usize),
                    )?;
                }
            }
        }
        map.end()
    }
}

struct HashValue(blake3::Hash);

impl fmt::Display for HashValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "blake3:{}", self.0.to_hex())
    }
}

impl Serialize for HashValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

struct PrivacyScan {
    denied: u64,
    truncated: u64,
    limit_drops: u64,
}

impl PrivacyScan {
    fn new(policy: &ResolvedObservabilityPolicy, event: &str, fields: &[SignalField<'_>]) -> Self {
        let mut denied = 0_u64;
        let mut truncated = 0_u64;
        let mut limit_drops = 0_u64;
        let mut retained = 0_u32;
        for field in fields {
            let Some(rule) = field_rule(policy, event, field.name) else {
                denied += 1;
                continue;
            };
            if retained == policy.max_attributes_per_event() {
                limit_drops += 1;
                continue;
            }
            retained += 1;
            let candidate = match rule.action() {
                FieldPolicyAction::Allow => match field.value {
                    SignalValue::Text(value) => {
                        Some(value.len() > policy.max_attribute_bytes() as usize)
                    }
                    SignalValue::Record(value) => Some(record_value_exceeds(
                        value,
                        policy.max_attribute_bytes() as usize,
                    )),
                },
                FieldPolicyAction::Replace => rule
                    .replacement()
                    .map(|replacement| replacement.len() > policy.max_attribute_bytes() as usize),
                FieldPolicyAction::Hash => None,
            };
            if candidate == Some(true) {
                truncated += 1;
            }
        }
        Self {
            denied,
            truncated,
            limit_drops,
        }
    }
}

/// `fmt::Write` target that never retains more than one configured attribute.
/// Returning `fmt::Error` at the boundary stops recursive `Value::Display`
/// formatting immediately, so arrays and maps cannot force an unbounded
/// intermediate allocation.
struct BoundedString {
    value: String,
    max_bytes: usize,
}

impl BoundedString {
    fn new(max_bytes: usize) -> Self {
        Self {
            value: String::with_capacity(max_bytes.min(256)),
            max_bytes,
        }
    }
}

impl fmt::Write for BoundedString {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let remaining = self.max_bytes.saturating_sub(self.value.len());
        if value.len() <= remaining {
            self.value.push_str(value);
            return Ok(());
        }
        self.value.push_str(truncate_utf8(value, remaining));
        Err(fmt::Error)
    }
}

fn render_record_value_bounded(value: &Value, max_bytes: usize) -> String {
    let mut bounded = BoundedString::new(max_bytes);
    let _ = fmt::write(&mut bounded, format_args!("{value}"));
    bounded.value
}

struct LimitProbe {
    remaining: usize,
}

impl fmt::Write for LimitProbe {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if value.len() <= self.remaining {
            self.remaining -= value.len();
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

fn record_value_exceeds(value: &Value, max_bytes: usize) -> bool {
    let mut probe = LimitProbe {
        remaining: max_bytes,
    };
    fmt::write(&mut probe, format_args!("{value}")).is_err()
}

struct HashWriter<'a>(&'a mut blake3::Hasher);

impl fmt::Write for HashWriter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0.update(value.as_bytes());
        Ok(())
    }
}

fn hash_record_value(value: &Value) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    let mut writer = HashWriter(&mut hasher);
    let _ = fmt::write(&mut writer, format_args!("{value}"));
    hasher.finalize()
}

fn field_rule<'a>(
    policy: &'a ResolvedObservabilityPolicy,
    event: &str,
    field: &str,
) -> Option<&'a ResolvedFieldPolicy> {
    policy
        .field_policies()
        .iter()
        .find(|rule| rule.event() == event && rule.field() == field)
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn valid_logical_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[derive(Clone, Copy)]
struct SlotToken {
    lane: AdmissionLane,
    index: usize,
}

#[derive(Clone, Copy, Default)]
struct SlotMeta {
    len: usize,
    occupied: bool,
}

struct LaneState {
    start: usize,
    slot_bytes: usize,
    slots: Vec<SlotMeta>,
    next: usize,
}

impl LaneState {
    fn new(start: usize, capacity: usize, slot_bytes: usize) -> Result<Self, TelemetryArenaError> {
        if slot_bytes == 0 {
            return Err(TelemetryArenaError::CapacityOutOfRange);
        }
        let slot_count = capacity / slot_bytes;
        if slot_count == 0 {
            return Err(TelemetryArenaError::CapacityOutOfRange);
        }
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(slot_count)
            .map_err(TelemetryArenaError::Allocation)?;
        slots.resize(slot_count, SlotMeta::default());
        Ok(Self {
            start,
            slot_bytes,
            slots,
            next: 0,
        })
    }

    fn free_slot(&mut self) -> Option<usize> {
        for distance in 0..self.slots.len() {
            let index = (self.next + distance) % self.slots.len();
            if !self.slots[index].occupied {
                self.next = (index + 1) % self.slots.len();
                return Some(index);
            }
        }
        None
    }

    fn offset(&self, index: usize) -> usize {
        self.start + index * self.slot_bytes
    }
}

struct ArenaState {
    bytes: Box<[u8]>,
    ordinary: LaneState,
    high: LaneState,
    queue: VecDeque<SlotToken>,
    rate: RateLimiter,
}

impl ArenaState {
    fn admit<T: Serialize>(&mut self, lane: AdmissionLane, value: &T) -> Result<usize, DropReason> {
        let index = match lane {
            AdmissionLane::Ordinary => self.ordinary.free_slot(),
            AdmissionLane::HighSeverity => self.high.free_slot(),
        }
        .ok_or(DropReason::Full)?;
        let (offset, slot_bytes) = match lane {
            AdmissionLane::Ordinary => (self.ordinary.offset(index), self.ordinary.slot_bytes),
            AdmissionLane::HighSeverity => (self.high.offset(index), self.high.slot_bytes),
        };

        let mut writer = FixedWriter::new(&mut self.bytes[offset..offset + slot_bytes]);
        if serde_json::to_writer(&mut writer, value).is_err() {
            return Err(DropReason::Oversize);
        }
        let len = writer.len();
        let slot = match lane {
            AdmissionLane::Ordinary => &mut self.ordinary.slots[index],
            AdmissionLane::HighSeverity => &mut self.high.slots[index],
        };
        slot.len = len;
        slot.occupied = true;
        self.queue.push_back(SlotToken { lane, index });
        Ok(len)
    }

    fn slot_location(&self, token: SlotToken) -> (usize, usize) {
        let lane = match token.lane {
            AdmissionLane::Ordinary => &self.ordinary,
            AdmissionLane::HighSeverity => &self.high,
        };
        (lane.offset(token.index), lane.slots[token.index].len)
    }

    fn release(&mut self, token: SlotToken) {
        let slot = match token.lane {
            AdmissionLane::Ordinary => &mut self.ordinary.slots[token.index],
            AdmissionLane::HighSeverity => &mut self.high.slots[token.index],
        };
        slot.len = 0;
        slot.occupied = false;
    }
}

struct FixedWriter<'a> {
    bytes: &'a mut [u8],
    len: usize,
}

impl<'a> FixedWriter<'a> {
    fn new(bytes: &'a mut [u8]) -> Self {
        Self { bytes, len: 0 }
    }

    const fn len(&self) -> usize {
        self.len
    }
}

impl Write for FixedWriter<'_> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let end = self
            .len
            .checked_add(input.len())
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::WriteZero, "telemetry slot is full"))?;
        self.bytes[self.len..end].copy_from_slice(input);
        self.len = end;
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct RateLimiter {
    tokens: u64,
    capacity: u64,
    per_second: u64,
    last_refill: Instant,
}

impl RateLimiter {
    fn new(per_second: u32, burst: u32) -> Self {
        Self {
            tokens: u64::from(burst),
            capacity: u64::from(burst),
            per_second: u64::from(per_second),
            last_refill: Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let refill = elapsed
            .as_nanos()
            .saturating_mul(u128::from(self.per_second))
            / 1_000_000_000;
        if refill > 0 {
            let refill = u64::try_from(refill).unwrap_or(u64::MAX);
            self.tokens = self.tokens.saturating_add(refill).min(self.capacity);
            self.last_refill = now;
        }
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }
}

struct MetricCounters {
    values: [AtomicU64; MetricKey::COUNT],
}

impl Default for MetricCounters {
    fn default() -> Self {
        Self {
            values: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl MetricCounters {
    fn add(&self, key: MetricKey, delta: u64) {
        let counter = &self.values[key.index()];
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(delta))
        });
    }

    fn drain(&self) -> Vec<MetricPoint> {
        let mut points = Vec::with_capacity(MetricKey::COUNT);
        for key in MetricKey::ALL {
            let value = self.values[key.index()].swap(0, Ordering::AcqRel);
            if value != 0 {
                points.push(MetricPoint { key, value });
            }
        }
        points
    }
}

#[derive(Default)]
struct ProducerCounters {
    accepted: AtomicU64,
    denied_fields: AtomicU64,
    truncated_fields: AtomicU64,
    attribute_limit_drops: AtomicU64,
    sampled: AtomicU64,
    rate_limited: AtomicU64,
    contended: AtomicU64,
    full: AtomicU64,
    oversize: AtomicU64,
    invalid: AtomicU64,
}

#[derive(Default)]
struct RetainedAccounting {
    total: AtomicU64,
    ordinary: AtomicU64,
    high: AtomicU64,
    peak: AtomicU64,
}

impl RetainedAccounting {
    fn add(&self, lane: AdmissionLane, bytes: u64) {
        let total = self.total.fetch_add(bytes, Ordering::AcqRel) + bytes;
        match lane {
            AdmissionLane::Ordinary => {
                self.ordinary.fetch_add(bytes, Ordering::AcqRel);
            }
            AdmissionLane::HighSeverity => {
                self.high.fetch_add(bytes, Ordering::AcqRel);
            }
        }
        let _ = self
            .peak
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |peak| {
                (total > peak).then_some(total)
            });
    }

    fn subtract(&self, lane: AdmissionLane, bytes: u64) {
        self.total.fetch_sub(bytes, Ordering::AcqRel);
        match lane {
            AdmissionLane::Ordinary => {
                self.ordinary.fetch_sub(bytes, Ordering::AcqRel);
            }
            AdmissionLane::HighSeverity => {
                self.high.fetch_sub(bytes, Ordering::AcqRel);
            }
        }
    }
}
