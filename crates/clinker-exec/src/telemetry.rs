//! Fixed-memory, transport-neutral telemetry production.
//!
//! The producer owns one preallocated byte arena split into disjoint ordinary
//! and high-severity lanes. Privacy policy, attribute limits, sampling, rate
//! limiting, and severity routing all run before a byte becomes retained.
//! Producers never block, grow the arena, spill, or write the metrics spool.

use std::borrow::Cow;
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

/// Engine-supplied identity of the run that produced a signal.
///
/// These are not record data and never derive from a source, so exact
/// per-(event, field) privacy policy does not gate them: a deployment that
/// declares an event without also declaring three correlation rules still gets
/// telemetry it can join to the machine stream and the lineage events.
/// `S` is `&str` on the producer side and `String` once received.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunCorrelation<S> {
    pub execution_id: S,
    pub batch_id: S,
    pub pipeline_name: S,
}

impl RunCorrelation<String> {
    /// Bound each identity the way the producer side already bounds it.
    ///
    /// A consumer outside this crate has the same three strings but not the
    /// cap, and an identity that is bounded on the log records and unbounded on
    /// the run's own envelopes would name one run two ways.
    pub fn bounded(execution_id: &str, batch_id: &str, pipeline_name: &str) -> Self {
        Self {
            execution_id: bounded_identity(execution_id).into_owned(),
            batch_id: bounded_identity(batch_id).into_owned(),
            pipeline_name: bounded_identity(pipeline_name).into_owned(),
        }
    }
}

/// Borrowed producer-side log event.
#[derive(Clone, Copy, Debug)]
pub struct LogEvent<'a> {
    pub event: &'a str,
    pub severity: Severity,
    pub message: &'a str,
    pub correlation: RunCorrelation<&'a str>,
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

/// Bounded span result fact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

/// Borrowed producer-side trace fact. No record or error payload is accepted.
///
/// One admitted fact is one complete span, closed at both ends. A span is
/// emitted after its work finishes rather than as a start fact plus a later
/// end fact, because a collector has no representation for half a span: both
/// wall-clock boundaries are required, and independent admission of two halves
/// lets sampling, lane routing, or a full arena deliver one without the other.
/// The live "this transform has begun" signal is the `TransformStarted`
/// metric, which is still recorded before the work runs.
#[derive(Clone, Copy, Debug)]
pub struct SpanFact<'a> {
    pub name: SpanName,
    pub status: SpanStatus,
    /// The authored pipeline node this span covers, verbatim — prefixed with
    /// the composition call sites it sits under when it is a body node, since a
    /// body name identifies a node only within its own scope. Configuration
    /// applies no grammar to a node name, so neither does this: the name is
    /// carried whole under a fixed identity ceiling and nothing else.
    pub logical_node: &'a str,
    /// Span boundaries as Unix nanoseconds, `started_at <= ended_at`.
    pub started_at_unix_nanos: u64,
    pub ended_at_unix_nanos: u64,
}

impl Serialize for SpanFact<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            name: SpanName,
            status: SpanStatus,
            logical_node: &'a str,
            started_at_unix_nanos: u64,
            ended_at_unix_nanos: u64,
        }

        let logical_node = bounded_identity(self.logical_node);
        Wire {
            name: self.name,
            status: self.status,
            logical_node: &logical_node,
            started_at_unix_nanos: self.started_at_unix_nanos,
            ended_at_unix_nanos: self.ended_at_unix_nanos,
        }
        .serialize(serializer)
    }
}

/// Wall clock as Unix nanoseconds, saturating at the epoch.
#[must_use]
pub fn unix_nanos_now() -> u64 {
    u64::try_from(
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(i64::MAX)
            .max(0),
    )
    .unwrap_or(0)
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
    pub fn reserve(
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
            sample_sequence: Arc::new(LaneSampling::default()),
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
            Self::reserve(policy).map(Some)
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
    sample_sequence: Arc<LaneSampling>,
}

/// One admission counter per disjoint lane.
#[derive(Default)]
struct LaneSampling {
    ordinary: AtomicU64,
    high: AtomicU64,
}

impl LaneSampling {
    fn next(&self, lane: AdmissionLane) -> u64 {
        match lane {
            AdmissionLane::Ordinary => self.ordinary.fetch_add(1, Ordering::Relaxed),
            AdmissionLane::HighSeverity => self.high.fetch_add(1, Ordering::Relaxed),
        }
    }
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
    ///
    /// A span carries no identity grammar. Its `logical_node` is an authored
    /// pipeline node name, and configuration constrains those only for
    /// duplication — so any rule imposed here would reject names the planner
    /// accepts and compiles, leaving that transform's metrics and authored log
    /// events in the collector with its span missing and nothing to explain the
    /// hole. Serialization instead bounds the name to the same identity ceiling
    /// the run correlation ids use, and marks it when that ceiling bites — which
    /// keeps the fixed arena budget without discarding the fact.
    pub fn emit_span(&self, span: SpanFact<'_>) -> AdmissionOutcome {
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
        // Sampling counts within the destination lane, never across lanes. The
        // two lanes exist so ordinary volume cannot starve high severity; one
        // shared counter would have reintroduced exactly that coupling, letting
        // nine Info events per Error discard the same nine tenths of Errors.
        let sequence = self.sample_sequence.next(lane);
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

        // The token is spent before the arena is asked, because asking needs
        // the same lock and the limiter is what bounds how often we ask.
        //
        // Given back only when the refusal cost nothing. A full arena is
        // detected before the signal is serialized, so charging for it let a
        // burst of drops exhaust the budget and silence the error an operator
        // configured observability to see. An oversize is detected by
        // serializing into the slot and finding it does not fit -- that work
        // is what the budget exists to bound, so it stays spent.
        match shared.admit(lane, signal) {
            Ok(bytes) => {
                let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
                self.retained.add(lane, bytes);
                self.stats.accepted.fetch_add(1, Ordering::Relaxed);
                AdmissionOutcome::Accepted { lane, bytes }
            }
            Err(DropReason::Full) => {
                shared.rate.refund();
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
    pub correlation: RunCorrelation<String>,
    pub fields: BTreeMap<String, String>,
}

/// Receiver-owned fixed metric point.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetricPoint {
    pub key: MetricKey,
    pub value: u64,
}

/// Receiver-owned bounded trace fact. One value is one complete span.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TraceSpan {
    pub name: SpanName,
    pub status: SpanStatus,
    pub logical_node: String,
    pub started_at_unix_nanos: u64,
    pub ended_at_unix_nanos: u64,
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
            correlation: RunCorrelation<&'a str>,
            fields: F,
        }

        Header {
            event: self.event.event,
            severity: self.event.severity,
            message: self.event.message,
            correlation: self.event.correlation,
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
                        &bounded_utf8(value, self.policy.max_attribute_bytes() as usize),
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
                        &bounded_utf8(replacement, self.policy.max_attribute_bytes() as usize),
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

/// Render one typed record value, marked when the byte cap cut it short.
///
/// [`BoundedString`] reports the cut by returning `fmt::Error`, which is the
/// only signal available here: the rendering is produced incrementally and
/// never materializes the full value to compare lengths against.
fn render_record_value_bounded(value: &Value, max_bytes: usize) -> String {
    let mut bounded = BoundedString::new(max_bytes);
    if fmt::write(&mut bounded, format_args!("{value}")).is_ok() {
        return bounded.value;
    }
    mark_truncated(&bounded.value, max_bytes)
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

/// Ceiling on one exported identity string: a run correlation id, or the
/// logical node name on a span.
///
/// Matches the largest `--batch-id` the CLI admits. An identity exists to be
/// joined against the machine stream and the lineage events, both of which
/// carry it whole, so a lower ceiling here would export an identifier that
/// silently matches neither.
pub(crate) const MAX_IDENTITY_BYTES: usize = 256;

/// Appended to any exported value a byte cap forced short.
///
/// A consumer reading an exported attribute has no other way to tell a whole
/// value from its prefix. Capped at four bytes, an `amount` of `123456789`
/// exports as `1234`, an ISO timestamp exports as a shorter but well-formed
/// date, and an array exports as a plausible shorter array — each of which a
/// dashboard or alert rule will happily compute against. The marker is a single
/// character that appears in no number, timestamp, or bare identifier, so those
/// consumers fail on the value instead of trusting it.
const TRUNCATION_MARKER: &str = "…";

/// Return `value` under `max_bytes`, marked when the cap forced it short.
///
/// The marker is charged against the same cap, so a value that fits is returned
/// whole and a marked result stays within `max_bytes` at every cap wide enough
/// to hold the marker. `max_attribute_bytes` also admits caps below the marker's
/// own width; there the marker alone is the result and it is the one case that
/// exceeds the cap, by at most two bytes. Nothing sizes the arena from this cap
/// — the lanes are reserved from `arena_bytes` and each event is bounded by
/// `max_batch_bytes` — so the fixed budget is unchanged either way.
pub(crate) fn bounded_utf8(value: &str, max_bytes: usize) -> Cow<'_, str> {
    if value.len() <= max_bytes {
        return Cow::Borrowed(value);
    }
    Cow::Owned(mark_truncated(value, max_bytes))
}

/// Bound one exported identity string against [`MAX_IDENTITY_BYTES`].
pub(crate) fn bounded_identity(value: &str) -> Cow<'_, str> {
    bounded_utf8(value, MAX_IDENTITY_BYTES)
}

/// Cut `value` to leave room for [`TRUNCATION_MARKER`] and append it.
///
/// `value` may already sit at the cap, so the head is re-cut rather than
/// assumed short enough.
///
/// The marker is emitted whole even at a cap too narrow to hold it. Cutting it
/// to fit produces nothing at all — the marker is one three-byte character, and
/// its first two bytes are not a character — so the alternative is an exported
/// empty string, which a consumer cannot tell from a field that genuinely holds
/// one. A whole marker at a one- or two-byte cap overshoots by at most two
/// bytes and stays legible as what it is.
fn mark_truncated(value: &str, max_bytes: usize) -> String {
    let head = truncate_utf8(value, max_bytes.saturating_sub(TRUNCATION_MARKER.len()));
    let mut marked = String::with_capacity(head.len() + TRUNCATION_MARKER.len());
    marked.push_str(head);
    marked.push_str(TRUNCATION_MARKER);
    marked
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

/// Return whether an event name is one deployment field policy can address.
///
/// This mirrors the dotted-identifier grammar `clinker-plan` enforces on a log
/// directive's `name`, where an author gets a diagnostic naming the offending
/// input. The two rules agree by construction: the planner's grammar is the
/// stricter of the pair, so a compiled directive always passes here and a
/// rejection means the event reached the producer from somewhere other than an
/// authored directive. Node names carry no such grammar and are not checked
/// here — see [`TelemetryProducer::emit_span`].
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
            let whole = u64::try_from(refill).unwrap_or(u64::MAX);
            self.tokens = self.tokens.saturating_add(whole).min(self.capacity);
            // Advanced by the time those whole tokens cost, not to now. Moving
            // it to now threw away whatever fraction of the next token had
            // already accrued, so a caller arriving a little more often than
            // one token's worth of time lost most of a token every call and
            // the sustained rate settled below the configured one.
            let consumed = u128::from(whole)
                .saturating_mul(1_000_000_000)
                .checked_div(u128::from(self.per_second))
                .unwrap_or(0);
            self.last_refill = u64::try_from(consumed)
                .ok()
                .and_then(|nanos| {
                    self.last_refill
                        .checked_add(std::time::Duration::from_nanos(nanos))
                })
                .unwrap_or(now);
        }
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }

    /// Return a token taken for a signal the arena then refused for free.
    ///
    /// The budget bounds two things at once: how many signals reach a
    /// collector, and how much work is spent deciding. A refusal that cost
    /// nothing bounds neither, so charging for it let a burst of drops
    /// exhaust the bucket and silence the error an operator had configured
    /// observability to see. A refusal reached by serializing the signal is
    /// the work the budget exists to bound and stays spent -- see the call
    /// site, which refunds `Full` and not `Oversize`.
    fn refund(&mut self) {
        self.tokens = self.tokens.saturating_add(1).min(self.capacity);
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

#[cfg(test)]
mod tests {
    use clinker_plan::config::{ClinkerToml, ResolvedObservabilityPolicy};
    use clinker_record::Value;

    use super::{
        AdmissionOutcome, DropReason, LogEvent, MAX_IDENTITY_BYTES, RunCorrelation, Severity,
        SignalField, SpanFact, SpanName, SpanStatus, TRUNCATION_MARKER, TelemetryArena,
        TelemetryProducer, TelemetryReceiver, bounded_utf8,
    };

    /// A policy whose only variable is the attribute cap under test. Declares
    /// one `allow` field and one `replace` field so both rendering paths are
    /// reachable from the same event.
    fn policy(max_attribute_bytes: &str, replacement: &str) -> ResolvedObservabilityPolicy {
        let text = format!(
            r#"
[observability]
arena_bytes = "768KB"
ordinary_lane_bytes = "512KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 8
max_attribute_bytes = "{max_attribute_bytes}"
drop_policy = "drop_newest"
sample_every = 1
rate_limit_per_second = 100000
rate_limit_burst = 100000
flush_timeout_ms = 1000

[observability.otlp]
endpoint = "https://collector.invalid"
connect_timeout_ms = 100
request_timeout_ms = 200
retry_max_attempts = 1
retry_total_timeout_ms = 500
max_response_bytes = "1KB"

[observability.otlp.auth]
mode = "none"

[observability.lineage]
queue_bytes = "1KB"
max_event_bytes = "512B"
drop_policy = "drop_newest"
flush_timeout_ms = 500
identity_mode = "local_diagnostic_paths"

[[observability.field_policy]]
event = "transform.seen"
field = "amount"
action = "allow"

[[observability.field_policy]]
event = "transform.seen"
field = "region"
action = "replace"
replacement = "{replacement}"
"#
        );
        ClinkerToml::parse(&text)
            .expect("telemetry policy parses")
            .resolve_observability(None)
            .expect("telemetry policy resolves")
    }

    fn arena(max_attribute_bytes: &str) -> (TelemetryProducer, TelemetryReceiver) {
        TelemetryArena::reserve(&policy(max_attribute_bytes, "[region]")).expect("arena reserves")
    }

    fn correlation() -> RunCorrelation<&'static str> {
        RunCorrelation {
            execution_id: "execution-1",
            batch_id: "batch-1",
            pipeline_name: "pipeline-1",
        }
    }

    fn span(logical_node: &str) -> SpanFact<'_> {
        SpanFact {
            name: SpanName::Transform,
            status: SpanStatus::Ok,
            logical_node,
            started_at_unix_nanos: 10,
            ended_at_unix_nanos: 20,
        }
    }

    /// Admit one span and return the logical node name that reached the
    /// receiver, or `None` when the span never arrived.
    fn exported_node_name(logical_node: &str) -> Option<String> {
        let (producer, receiver) = arena("256B");
        let outcome = producer.emit_span(span(logical_node));
        assert!(
            outcome.is_accepted(),
            "span for {logical_node:?} was not admitted: {outcome:?}"
        );
        let batch = receiver.try_recv_batch()?;
        batch
            .traces()
            .iter()
            .map(|trace| trace.logical_node.clone())
            .next()
    }

    /// Admit one log carrying `value` under `field` and return the attribute
    /// the receiver saw.
    fn exported_attribute(
        max_attribute_bytes: &str,
        field: &str,
        value: SignalField<'_>,
    ) -> String {
        let (producer, receiver) = arena(max_attribute_bytes);
        let outcome = producer.emit_log(LogEvent {
            event: "transform.seen",
            severity: Severity::Info,
            message: "seen",
            correlation: correlation(),
            fields: &[value],
        });
        assert!(outcome.is_accepted(), "log was not admitted: {outcome:?}");
        let batch = receiver
            .try_recv_batch()
            .expect("admitted log is drainable");
        batch.logs()[0]
            .fields
            .get(field)
            .unwrap_or_else(|| panic!("field {field} is missing: {:?}", batch.logs()[0]))
            .clone()
    }

    /// Configuration validates node names only for duplication, so every one of
    /// these compiles and runs. Each must reach a collector as its own span:
    /// dropping one leaves that transform's metrics and authored log events in
    /// place with the span missing, which reads as a collector fault.
    #[test]
    fn every_node_name_configuration_accepts_reaches_the_collector() {
        for name in [
            "normalize orders",
            "orders+returns",
            "stage:normalize",
            "récapitulatif",
            "订单",
            "normalize/orders",
            "(unnamed)",
        ] {
            assert_eq!(
                exported_node_name(name).as_deref(),
                Some(name),
                "a node named {name:?} must export its span verbatim"
            );
        }
    }

    /// The conventional name shape keeps working; without this the test above
    /// would pass on an exporter that mangles every name equally.
    #[test]
    fn conventional_node_name_is_unchanged() {
        assert_eq!(
            exported_node_name("normalize_orders").as_deref(),
            Some("normalize_orders")
        );
    }

    /// A name past the identity ceiling is exported marked rather than as a
    /// shorter name that would read as a different, real node.
    #[test]
    fn node_name_over_the_identity_ceiling_is_marked() {
        let name = "n".repeat(MAX_IDENTITY_BYTES + 40);
        let exported = exported_node_name(&name).expect("an over-long name still exports a span");
        assert!(
            exported.len() <= MAX_IDENTITY_BYTES,
            "the identity ceiling still binds: {} bytes",
            exported.len()
        );
        assert!(
            exported.ends_with(TRUNCATION_MARKER),
            "a shortened node name must say so: {exported:?}"
        );
        assert!(
            exported.starts_with("nnn"),
            "the retained prefix is the authored name: {exported:?}"
        );
    }

    /// The event name is a different vocabulary from the node name: it is
    /// matched by deployment field policy and `clinker-plan` enforces a dotted
    /// identifier grammar on it with a diagnostic. That gate stays.
    #[test]
    fn event_name_outside_the_authored_grammar_is_refused() {
        let (producer, _receiver) = arena("256B");
        let outcome = producer.emit_log(LogEvent {
            event: "transform seen",
            severity: Severity::Info,
            message: "seen",
            correlation: correlation(),
            fields: &[],
        });
        assert_eq!(
            outcome,
            AdmissionOutcome::Dropped(DropReason::InvalidLogicalIdentity)
        );
        assert_eq!(producer.snapshot().invalid_drops, 1);
    }

    #[test]
    fn attribute_under_the_cap_is_exported_whole_and_unmarked() {
        let exported = exported_attribute("64B", "amount", SignalField::new("amount", "123456789"));
        assert_eq!(exported, "123456789");
        assert!(!exported.contains(TRUNCATION_MARKER));
    }

    #[test]
    fn attribute_over_the_cap_is_marked_and_stays_within_the_cap() {
        let exported = exported_attribute("4B", "amount", SignalField::new("amount", "123456789"));
        assert!(
            exported.len() <= 4,
            "the byte cap still binds: {exported:?} is {} bytes",
            exported.len()
        );
        assert!(
            exported.ends_with(TRUNCATION_MARKER),
            "a shortened amount must not read as a complete one: {exported:?}"
        );
        assert_ne!(exported, "1234", "the bare prefix is a plausible amount");
    }

    /// The cap lands inside a two-byte character. The retained prefix must stop
    /// at the preceding boundary, and the marker still has to fit.
    #[test]
    fn attribute_cut_inside_a_character_keeps_valid_utf8() {
        let exported = exported_attribute("8B", "amount", SignalField::new("amount", "ααααα"));
        assert_eq!(exported, "αα…");
        assert!(exported.len() <= 8, "{} bytes", exported.len());
    }

    /// The typed record path renders incrementally and learns of the cut only
    /// from its writer, so it needs its own coverage.
    #[test]
    fn typed_record_value_over_the_cap_is_marked() {
        let value = Value::Integer(123_456_789);
        let exported =
            exported_attribute("4B", "amount", SignalField::from_record("amount", &value));
        assert_eq!(exported, "1…");
    }

    #[test]
    fn typed_record_value_under_the_cap_is_exported_whole() {
        let value = Value::Integer(12);
        let exported =
            exported_attribute("4B", "amount", SignalField::from_record("amount", &value));
        assert_eq!(exported, "12");
    }

    /// A replacement is author-supplied text and can exceed the cap like any
    /// other value.
    #[test]
    fn replacement_over_the_cap_is_marked() {
        let policy = policy("6B", "[redacted-by-policy]");
        let (producer, receiver) = TelemetryArena::reserve(&policy).expect("arena reserves");
        let outcome = producer.emit_log(LogEvent {
            event: "transform.seen",
            severity: Severity::Info,
            message: "seen",
            correlation: correlation(),
            fields: &[SignalField::new("region", "north")],
        });
        assert!(outcome.is_accepted(), "log was not admitted: {outcome:?}");
        let batch = receiver
            .try_recv_batch()
            .expect("admitted log is drainable");
        let exported = batch.logs()[0]
            .fields
            .get("region")
            .expect("replaced field is retained");
        assert_eq!(exported, "[re…");
        assert!(exported.len() <= 6, "{} bytes", exported.len());
    }

    /// `max_attribute_bytes` accepts any nonzero size, including caps narrower
    /// than the marker. Those export the marker alone: no data prefix a consumer
    /// would read as the whole value, and nothing empty either — an empty export
    /// is exactly what a field holding the empty string produces.
    #[test]
    fn cap_narrower_than_the_marker_exports_the_marker_alone() {
        for cap in 0..TRUNCATION_MARKER.len() {
            let bounded = bounded_utf8("123456789", cap);
            assert_eq!(
                bounded, TRUNCATION_MARKER,
                "cap {cap} must export the whole marker and nothing else"
            );
        }
    }

    /// The end-to-end reading of the case above, across the caps either side of
    /// the marker's own width. Every one of these is a policy a deployment can
    /// write, and at every one of them a cut value has to stay legible as cut.
    #[test]
    fn attribute_at_every_cap_around_the_marker_width_is_marked() {
        for (cap, expected) in [("1B", "…"), ("2B", "…"), ("3B", "…"), ("4B", "1…")] {
            let exported =
                exported_attribute(cap, "amount", SignalField::new("amount", "123456789"));
            assert_eq!(exported, expected, "cap {cap}");
        }
    }

    /// The typed record path renders incrementally into its own bounded writer,
    /// so it reaches the same narrow caps by a different route.
    #[test]
    fn typed_record_value_at_every_cap_around_the_marker_width_is_marked() {
        let value = Value::Integer(123_456_789);
        for (cap, expected) in [("1B", "…"), ("2B", "…"), ("3B", "…"), ("4B", "1…")] {
            let exported =
                exported_attribute(cap, "amount", SignalField::from_record("amount", &value));
            assert_eq!(exported, expected, "cap {cap}");
        }
    }

    /// The other half of the distinction: a field that genuinely holds the empty
    /// string is already under every cap, so it exports whole and unmarked.
    /// Without this the marking tests would pass on an exporter that emitted the
    /// marker for everything.
    #[test]
    fn genuinely_empty_attribute_is_exported_unmarked_at_every_narrow_cap() {
        for cap in ["1B", "2B", "3B", "4B"] {
            let exported = exported_attribute(cap, "amount", SignalField::new("amount", ""));
            assert_eq!(exported, "", "cap {cap}");
        }
    }

    #[test]
    fn value_exactly_at_the_cap_is_not_marked() {
        let bounded = bounded_utf8("1234", 4);
        assert_eq!(bounded, "1234");
    }
}
