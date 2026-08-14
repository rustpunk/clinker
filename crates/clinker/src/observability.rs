//! What a run reports about its own telemetry.
//!
//! These are the counters the `--machine ndjson-v1` terminal carries: what the
//! executor's fixed arena admitted and refused, and what an exporter made of
//! it. They keep their shape whether or not a collector was configured, and
//! whether or not this build has an exporter compiled in at all — an operator
//! reading a terminal is reading the same fields either way.
//!
//! [`export`] is the exporter that fills the delivery half of them.

#[cfg(feature = "otlp")]
pub(crate) mod export;

use clinker_exec::telemetry::ArenaSnapshot;
use serde::Serialize;

#[cfg(feature = "otlp")]
pub(crate) use export::{OtlpRuntimeBundle, OtlpWorker};

/// The exporter worker in a build that has no exporter.
///
/// Uninhabited on purpose: `Option<OtlpWorker>` is then statically `None`, and
/// the run's terminal paths keep one shape across both builds rather than
/// carrying a `#[cfg]` at every place a worker is finished or read. The
/// methods below are the ones those paths call; each discharges a value that
/// cannot exist.
#[cfg(not(feature = "otlp"))]
pub(crate) enum OtlpWorker {}

#[cfg(not(feature = "otlp"))]
impl OtlpWorker {
    pub(crate) fn progress_handle(&self) -> std::sync::Arc<std::sync::Mutex<ObservabilitySummary>> {
        match *self {}
    }

    pub(crate) fn finish(
        self,
        _snapshot: crate::lifecycle::RunLifecycleSnapshot,
    ) -> ObservabilitySummary {
        match self {}
    }
}

/// Fixed aggregate counters suitable for the machine terminal.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct ObservabilitySummary {
    pub(crate) logs: SignalSummary,
    pub(crate) metrics: SignalSummary,
    pub(crate) traces: SignalSummary,
    /// Whether the exporter finished flushing within its deadline.
    ///
    /// When false the counters are what had been recorded when the deadline
    /// expired rather than a final accounting, and deliveries may still have
    /// been in flight. A supervisor reading a low accepted count needs to know
    /// which of those two it is looking at, because the answers are "the
    /// collector rejected them" and "we stopped counting" — and only one of
    /// those is a collector problem.
    pub(crate) flush_complete: bool,
    /// What the fixed arena admitted and refused, before any export.
    ///
    /// The per-signal groups above are export-side, and an exporter can only
    /// count what reached it. Without this a run that discarded most of its
    /// signals at admission reported a clean, complete-looking export of a
    /// silently truncated dataset. Absent only when no arena was reserved.
    ///
    /// A run that reserved one always reports it, including on a terminal
    /// written before the flush that would have pushed a final accounting: the
    /// arena is read as it stands and marked `counts_complete: false`. The
    /// alternative was an absent field on exactly those runs, which says "no
    /// arena was reserved" about a run that reserved one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) admission: Option<AdmissionSummary>,
}

/// Aggregate-only visibility for one closed signal kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct SignalSummary {
    pub(crate) accepted: u64,
    pub(crate) rejected: u64,
    pub(crate) attempts: u64,
    pub(crate) failures: u64,
}

/// Exact arena-admission accounting for the machine terminal.
///
/// Drop reasons are named as OpenTelemetry `error.type` values on a
/// processed-items counter — `queue_full` rather than the internal `full` —
/// so a later mapping onto SDK self-observability metrics is a rename of
/// nothing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct AdmissionSummary {
    /// Whether the counters below are a final accounting.
    ///
    /// They are read from the producer's arena, and the arena keeps changing
    /// while the export worker drains it — `undecodable` is credited at drain.
    /// A flush that expired on its deadline detaches that worker rather than
    /// joining it, so the read that follows lands mid-drain and varies run to
    /// run. These counters exist so a truncated view stops looking complete,
    /// which a partial read reporting itself as final would defeat: a
    /// supervisor must be able to tell "nothing was lost" from "we could not
    /// finish counting".
    pub(crate) counts_complete: bool,
    /// Logs and spans the arena took. Metric points are coalesced into fixed
    /// counters rather than admitted as signals, so none are counted here.
    pub(crate) accepted: u64,
    pub(crate) dropped: AdmissionDrops,
    pub(crate) lanes: AdmissionLanes,
    /// What became of the fields of records that *were* accepted. These reduce
    /// what an accepted record says; they never discard one, so they are kept
    /// apart from `dropped` and must not be added into a loss total.
    pub(crate) fields: FieldPolicyCounts,
    /// Times the arena resumed from a poisoned lock.
    ///
    /// Not a drop and not a quantity of anything lost — a condition. Telemetry
    /// panicked while holding its own guard, and the arena chose to carry on
    /// rather than take the run down with it. A non-zero value says the
    /// counters beside it were produced by a subsystem that faulted mid-run,
    /// which is a different claim from "this run dropped signals" and belongs
    /// nowhere inside `dropped`.
    pub(crate) arena_recoveries: u64,
    pub(crate) retained_bytes: u64,
    pub(crate) peak_retained_bytes: u64,
    pub(crate) capacity_bytes: u64,
}

/// One counter per reason a signal never became exportable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct AdmissionDrops {
    pub(crate) sampled: u64,
    pub(crate) rate_limited: u64,
    pub(crate) queue_full: u64,
    pub(crate) contended: u64,
    pub(crate) oversize: u64,
    pub(crate) invalid_identity: u64,
    /// Counted in `accepted`, but unreadable at drain and so never exported.
    /// It is the one loss that is also a member of `accepted`.
    pub(crate) undecodable: u64,
}

impl AdmissionDrops {
    fn total(self) -> u64 {
        [
            self.sampled,
            self.rate_limited,
            self.queue_full,
            self.contended,
            self.oversize,
            self.invalid_identity,
            self.undecodable,
        ]
        .into_iter()
        .fold(0, u64::saturating_add)
    }
}

/// The two disjoint lanes, split for the drops that can bite an `error`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct AdmissionLanes {
    pub(crate) ordinary: AdmissionLaneSummary,
    pub(crate) high_severity: AdmissionLaneSummary,
}

/// Sampling and capacity refusals attributed to one lane.
///
/// Rate limiting and lock contention are properties of the shared arena rather
/// than of a lane, so they have no per-lane spelling and stay in `dropped`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct AdmissionLaneSummary {
    pub(crate) sampled: u64,
    pub(crate) queue_full: u64,
    pub(crate) retained_bytes: u64,
    pub(crate) capacity_bytes: u64,
}

/// What became of the fields of the records that were accepted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct FieldPolicyCounts {
    pub(crate) denied: u64,
    pub(crate) truncated: u64,
    pub(crate) limit_dropped: u64,
    /// Requested record values that were not on the record when the event
    /// fired, so the attribute was never built.
    ///
    /// The three counters above are policy doing what an operator configured
    /// it to do. This one is not configured by anyone: the event asked for a
    /// column and the record did not have it. Most such requests are refused
    /// when the pipeline compiles (E374); what survives to here is the case
    /// the planner cannot decide, a selector naming a column that reaches the
    /// transform through an open composition port.
    ///
    /// Counted only on events admission kept, because the record is read only
    /// after the event has a slot — under a sampling policy this sees one miss
    /// per sampled event, not one per record. It answers "is this happening"
    /// rather than "how often".
    pub(crate) missing: u64,
}

impl AdmissionSummary {
    /// Read the arena's accounting, marked with whether it can still change.
    ///
    /// `counts_complete` is the flush's own completeness: a flush that ran to
    /// completion joined the worker, so nothing is left to credit and the
    /// numbers are final. One that expired abandoned a worker that is still
    /// draining, and every counter below is whatever it had reached.
    pub(crate) fn from_arena(snapshot: ArenaSnapshot, counts_complete: bool) -> Self {
        Self {
            counts_complete,
            accepted: snapshot.accepted,
            dropped: AdmissionDrops {
                sampled: snapshot.sampled_drops,
                rate_limited: snapshot.rate_limited_drops,
                queue_full: snapshot.full_drops,
                contended: snapshot.contention_drops,
                oversize: snapshot.oversize_drops,
                invalid_identity: snapshot.invalid_drops,
                undecodable: snapshot.undecodable_drops,
            },
            lanes: AdmissionLanes {
                ordinary: AdmissionLaneSummary {
                    sampled: snapshot.ordinary_sampled_drops,
                    queue_full: snapshot.ordinary_full_drops,
                    retained_bytes: snapshot.ordinary_retained_bytes,
                    capacity_bytes: snapshot.ordinary_capacity_bytes,
                },
                high_severity: AdmissionLaneSummary {
                    sampled: snapshot.high_sampled_drops,
                    queue_full: snapshot.high_full_drops,
                    retained_bytes: snapshot.high_retained_bytes,
                    capacity_bytes: snapshot.high_capacity_bytes,
                },
            },
            fields: FieldPolicyCounts {
                denied: snapshot.denied_fields,
                truncated: snapshot.truncated_fields,
                limit_dropped: snapshot.attribute_limit_drops,
                missing: snapshot.missing_field_drops,
            },
            arena_recoveries: snapshot.arena_recoveries,
            retained_bytes: snapshot.retained_bytes,
            peak_retained_bytes: snapshot.peak_retained_bytes,
            capacity_bytes: snapshot.owned_bytes,
        }
    }

    /// Every signal this run lost, by any admission reason.
    pub(crate) fn dropped_total(self) -> u64 {
        self.dropped.total()
    }

    /// The standard-error line this accounting is worth, or `None` when it has
    /// nothing to say about lost telemetry.
    ///
    /// Built here rather than at the point it is printed so the suppression
    /// rule can be checked against a snapshot: whether a run collides with its
    /// own drain thread and refuses a signal is a property of the moment, so
    /// no run is a reliable witness to the nothing-to-report case.
    ///
    /// Four separate claims keep the line, and any one of them alone. Counters
    /// that are not final are not evidence of a clean run — all-zero mid-drain
    /// numbers describe a count that stopped, not a run that lost nothing. A
    /// missing field is an attribute the collector never received, and an
    /// arena recovery is telemetry having panicked under its own guard;
    /// neither is a signal count, and nobody configured either. The `denied`
    /// and `truncated` field counters stay out of it by contrast, because they
    /// are policy doing exactly what an operator asked it to do.
    pub(crate) fn standard_error_line(self) -> Option<String> {
        if self.dropped_total() == 0
            && self.counts_complete
            && self.arena_recoveries == 0
            && self.fields.missing == 0
        {
            return None;
        }
        let dropped = self.dropped;
        let lanes = self.lanes;
        Some(format!(
            "clinker: telemetry admission outcome: accepted={} dropped={} sampled={} rate_limited={} queue_full={} contended={} oversize={} invalid_identity={} undecodable={} ordinary_sampled={} ordinary_queue_full={} high_sampled={} high_queue_full={} missing_fields={} arena_recoveries={} counts_complete={}",
            self.accepted,
            self.dropped_total(),
            dropped.sampled,
            dropped.rate_limited,
            dropped.queue_full,
            dropped.contended,
            dropped.oversize,
            dropped.invalid_identity,
            dropped.undecodable,
            lanes.ordinary.sampled,
            lanes.ordinary.queue_full,
            lanes.high_severity.sampled,
            lanes.high_severity.queue_full,
            self.fields.missing,
            self.arena_recoveries,
            self.counts_complete,
        ))
    }
}
