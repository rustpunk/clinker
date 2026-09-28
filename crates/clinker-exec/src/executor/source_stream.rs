//! Per-source live ingest channel.
//!
//! [`SourceIngestChannel`] wraps a bounded `crossbeam_channel::Sender`
//! so a Source-reader thread can push records and document-boundary
//! punctuations into the executor's dispatch loop through the paired
//! `Receiver`. Channel capacity is bounded so the producer's `send`
//! blocks the calling OS thread when the consumer falls behind,
//! supplying back-pressure end-to-end without an intermediate spill
//! tier.
//!
//! Payload is [`StreamEvent`]: either a typed source-record event or
//! a [`Punctuation`] marking a document boundary. Source ingest emits
//! one `DocumentOpen` before the first body record of each source
//! file and one `DocumentClose` after the last.

use std::sync::Arc;

use clinker_plan::plan::PlanNodeId;
use clinker_record::{DocumentId, Record};

use crate::executor::stream_event::{Punctuation, SourceRowId};

/// One decoded source attempt. Successful and rejected attempts share this
/// carrier so an ordered physical file can stage one complete population.
#[derive(Debug, Clone)]
pub(crate) enum SourceAttemptEvent {
    Record(Record, SourceRowId),
    Rejection(Box<crate::executor::SourceRejectionEvent>),
}

impl SourceAttemptEvent {
    pub(crate) fn source_row(&self) -> SourceRowId {
        match self {
            Self::Record(_, source_row) => *source_row,
            Self::Rejection(event) => event.source_row,
        }
    }
}

/// Stable identity of one physical-file population decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct AttemptPopulationId {
    pub(crate) source: PlanNodeId,
    pub(crate) document: DocumentId,
}

/// Exact decoded/rejected population applied before an ordered file releases
/// any attempt or punctuation effect.
#[derive(Debug, Clone)]
pub(crate) struct AttemptPopulationDelta {
    pub(crate) id: AttemptPopulationId,
    pub(crate) source_name: Arc<str>,
    pub(crate) attempted: u64,
    pub(crate) rejected: u64,
}

/// Source-channel payload. Attempts are either direct (accounted when
/// consumed) or name the already-applied ordered-file population that covers
/// them. Rejections are consumed before downstream [`StreamEvent`] buffers are
/// built.
///
/// Not `Clone`: an attempt carries its own charge on the Source's handle,
/// which exactly one event may release.
#[derive(Debug)]
pub(crate) enum SourceStreamEvent {
    Population(AttemptPopulationDelta),
    Attempt {
        event: SourceAttemptEvent,
        population: Option<AttemptPopulationId>,
        /// The attempt's charge on its Source's handle while it is queued.
        queued: QueuedCharge,
    },
    Punctuation(Punctuation),
}

/// The heap a queued attempt holds outside the run's ledger: the part of a
/// record's storage no allocation of this run admitted (foreign-provider or
/// legacy heap), and for a rejection also the `Box` it travels in. The
/// record's fixed shell is not counted: it lives in the channel's
/// preallocated slot array, which is constant in input size. The one rule
/// for every Source channel, ordered or not.
pub(crate) fn queued_charge_bytes(
    event: &SourceAttemptEvent,
    resources: &clinker_record::owned_storage::AllocationResources,
) -> u64 {
    match event {
        SourceAttemptEvent::Record(record, _) => record.unaccounted_heap_size(resources) as u64,
        SourceAttemptEvent::Rejection(event) => {
            crate::source::order_barrier::unaccounted_rejection_event_bytes(event, resources)
        }
    }
}

/// One queued attempt's charge on its Source's handle, taken before the
/// attempt is sent and released exactly once: when the walk takes the
/// attempt off the channel, or when the attempt is dropped unconsumed (a
/// failed send, cancellation, the channel destroyed with it still queued).
/// Charged unchecked. A zero-byte charge holds no handle and takes no lock,
/// so a fully admitted record costs nothing here. When the Source's consumer
/// has already unregistered, the release touches only the handle's own
/// counter: unregistration returned the handle's charge to the ledger.
pub(crate) struct QueuedCharge {
    charge: Option<(Arc<crate::pipeline::memory::ConsumerHandle>, u64)>,
}

impl QueuedCharge {
    /// A charge of nothing, for attempts that hold no unadmitted heap and for
    /// events built outside a Source channel.
    pub(crate) fn none() -> Self {
        Self { charge: None }
    }

    /// Charge `event`'s queued size to `handle`.
    pub(crate) fn take(
        handle: &Arc<crate::pipeline::memory::ConsumerHandle>,
        event: &SourceAttemptEvent,
        resources: &clinker_record::owned_storage::AllocationResources,
    ) -> Self {
        Self::take_releasing(handle, event, resources, 0)
    }

    /// Charge `event`'s queued size to `handle` and release `released` of the
    /// handle's other charge in the same ledger step: the hand-off of a
    /// record the Source already charged elsewhere (a resident ordered
    /// spool) to the queue, so its bytes are never charged twice or to
    /// nothing at any instant.
    pub(crate) fn take_releasing(
        handle: &Arc<crate::pipeline::memory::ConsumerHandle>,
        event: &SourceAttemptEvent,
        resources: &clinker_record::owned_storage::AllocationResources,
        released: u64,
    ) -> Self {
        let bytes = queued_charge_bytes(event, resources);
        if bytes == 0 && released == 0 {
            return Self::none();
        }
        handle.take_over(handle, released, bytes);
        if bytes == 0 {
            return Self::none();
        }
        Self {
            charge: Some((Arc::clone(handle), bytes)),
        }
    }

    /// The bytes this charge holds.
    pub(crate) fn bytes(&self) -> u64 {
        self.charge.as_ref().map_or(0, |(_, bytes)| *bytes)
    }
}

impl Drop for QueuedCharge {
    fn drop(&mut self) {
        if let Some((handle, bytes)) = self.charge.take() {
            handle.sub_bytes(bytes);
        }
    }
}

impl std::fmt::Debug for QueuedCharge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuedCharge")
            .field("bytes", &self.bytes())
            .finish()
    }
}

/// Error surface for [`SourceIngestChannel`] sends.
#[derive(Debug)]
pub(crate) enum SourceStreamError {
    /// Consumer dropped the receiver before this push completed.
    Closed,
    /// This attempt already emitted the Source's `u64::MAX` ordinal.
    OrdinalExhausted { source: PlanNodeId },
    /// The per-file ordering barrier rejected or failed the staged file.
    OrderViolation(Box<clinker_plan::error::PipelineError>),
}

impl std::fmt::Display for SourceStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "source stream closed: consumer dropped receiver"),
            Self::OrdinalExhausted { source } => write!(
                f,
                "source row identity exhausted for {source}: ordinal cannot advance beyond u64::MAX"
            ),
            Self::OrderViolation(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for SourceStreamError {}

/// Crossbeam-bounded live ingest channel for one Source.
///
/// Capacity is bounded; the producer's `send` blocks the calling OS
/// thread when the channel is full, providing back-pressure to the
/// upstream reader. The consumer is a paired `crossbeam_channel::Receiver`
/// returned by [`Self::new`] and consumed via `recv` by the dispatch
/// loop's Source arm.
pub(crate) struct SourceIngestChannel {
    tx: crossbeam_channel::Sender<SourceStreamEvent>,
    /// Shared with the registered `SourceConsumer` wrapper. It charges
    /// exactly the heap the queued attempts hold outside the run's ledger:
    /// each attempt carries a [`QueuedCharge`] on it from just before its send
    /// until it leaves the channel. The records' admitted bytes are charged
    /// when they are allocated, in the Source consumer's name through
    /// `allocation_resources`, so none is charged twice. Punctuations carry
    /// no charge. An ordered Source's barrier also charges its staged rows
    /// here.
    consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
    /// Source-scoped identity to mint for the next successfully sent record.
    /// `None` means the preceding send used `u64::MAX`; another body record
    /// must fail the attempt instead of wrapping to zero.
    next_row_id: Option<SourceRowId>,
    source: PlanNodeId,
    allocation_resources: clinker_record::owned_storage::AllocationResources,
    /// Present only for a source declaring record-level `sort_order`.
    order_barrier: Option<crate::source::order_barrier::SourceFileOrderBarrier>,
}

impl SourceIngestChannel {
    /// Borrow the same run authority used to exclude admitted row storage from
    /// queued-attempt charges. Decoder allocations keep their own grants.
    pub(super) fn allocation_resources(
        &self,
    ) -> &clinker_record::owned_storage::AllocationResources {
        &self.allocation_resources
    }

    #[cfg(test)]
    pub(super) fn assert_allocation_domain(
        &self,
        resources: &clinker_record::owned_storage::AllocationResources,
    ) {
        assert_eq!(self.allocation_resources.identity(), resources.identity());
        if let Some(barrier) = &self.order_barrier {
            assert_eq!(barrier.allocation_identity(), resources.identity());
        }
    }

    /// Default channel capacity. Bounds the in-flight depth between the
    /// ingest thread and the dispatch loop's Source arm; the producer's
    /// `send` blocks once this many events are buffered, so the value
    /// paces back-pressure.
    pub(crate) const DEFAULT_CAPACITY: usize = 1024;

    /// Whether this source needs explicit physical-file lifecycle events.
    /// Ordinary sources retain the historical record-driven boundary path;
    /// an order barrier also needs zero-record files to reach verification.
    pub(crate) fn has_order_barrier(&self) -> bool {
        self.order_barrier.is_some()
    }

    /// Create a new channel + paired receiver. The receiver is what the
    /// dispatch loop's Source arm consumes via `recv`. The
    /// `consumer_handle` is shared with the pipeline-scoped
    /// arbitrator's `SourceConsumer` wrapper.
    pub(crate) fn new(
        capacity: usize,
        consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
        source: PlanNodeId,
        allocation_resources: clinker_record::owned_storage::AllocationResources,
    ) -> (Self, crossbeam_channel::Receiver<SourceStreamEvent>) {
        let (tx, rx) = crossbeam_channel::bounded(capacity);
        (
            Self {
                tx,
                consumer_handle,
                next_row_id: Some(SourceRowId::first(source)),
                source,
                allocation_resources,
                order_barrier: None,
            },
            rx,
        )
    }

    /// Create the same bounded channel with a per-physical-file order barrier
    /// inserted before its sender.
    // Retained-row accounting and allocation admission have separate lifetimes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_ordered(
        capacity: usize,
        consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
        source: PlanNodeId,
        config: crate::source::order_barrier::SourceOrderConfig,
        memory: Arc<crate::pipeline::memory::MemoryArbitrator>,
        spill_dir: std::path::PathBuf,
        spill_compress: bool,
        allocation_resources: clinker_record::owned_storage::AllocationResources,
    ) -> (Self, crossbeam_channel::Receiver<SourceStreamEvent>) {
        let (tx, rx) = crossbeam_channel::bounded(capacity);
        let order_barrier = crate::source::order_barrier::SourceFileOrderBarrier::new(
            config,
            tx.clone(),
            Arc::clone(&consumer_handle),
            memory,
            spill_dir,
            spill_compress,
            allocation_resources.clone(),
        );
        (
            Self {
                tx,
                consumer_handle,
                next_row_id: Some(SourceRowId::first(source)),
                source,
                allocation_resources,
                order_barrier: Some(order_barrier),
            },
            rx,
        )
    }

    /// Push a body record from the Source ingest thread driving a sync
    /// format reader. Blocks the calling thread when the channel is at
    /// capacity, preserving the bounded-channel back-pressure
    /// semantics. Returns the exact typed identity placed on the channel so
    /// document-level structural carriers can retain the selected record and
    /// its identity as one pair.
    pub(crate) fn push(&mut self, record: Record) -> Result<SourceRowId, SourceStreamError> {
        // Block here if the arbitrator has paused this consumer (e.g.
        // `BackPressurePreferred` policy elected this Source as the
        // pause victim). The fast path is lock-free; the slow path
        // parks the calling thread on a `Condvar` until `resume()`
        // notifies. Routed through the shared `ConsumerHandle` so
        // pause/resume from the arbitrator side and the producer-
        // side wait participate in the same primitive.
        self.consumer_handle.wait_while_paused();
        let row_id = self
            .next_row_id
            .ok_or(SourceStreamError::OrdinalExhausted {
                source: self.source,
            })?;
        self.send_attempt(SourceAttemptEvent::Record(record, row_id))?;
        self.next_row_id = row_id.checked_next();
        Ok(row_id)
    }

    /// Send one attempt, or stage it in the order barrier. An unordered
    /// attempt is charged to the Source's handle before the send, so it is
    /// never queued uncharged; a failed send returns the attempt, whose drop
    /// releases the charge at once. The barrier charges the attempts it
    /// releases under the same rule.
    fn send_attempt(&mut self, event: SourceAttemptEvent) -> Result<(), SourceStreamError> {
        if let Some(barrier) = self.order_barrier.as_mut() {
            barrier.observe_attempt(event)?;
        } else {
            let queued =
                QueuedCharge::take(&self.consumer_handle, &event, &self.allocation_resources);
            self.tx
                .send(SourceStreamEvent::Attempt {
                    event,
                    population: None,
                    queued,
                })
                .map_err(|_| SourceStreamError::Closed)?;
        }
        self.update_usage();
        Ok(())
    }

    /// Reserve the next source-scoped identity for a rejected input attempt.
    ///
    /// Structural reader failures do not yield a body [`Record`] to send, but
    /// their representative DLQ record still needs a unique identity in the
    /// same attempt sequence as successfully decoded rows. Reserving here keeps
    /// the counter source-scoped and prevents the next successful record from
    /// reusing the rejected attempt's identity.
    pub(crate) fn reserve_rejected_row_id(&mut self) -> Result<SourceRowId, SourceStreamError> {
        let row_id = self
            .next_row_id
            .ok_or(SourceStreamError::OrdinalExhausted {
                source: self.source,
            })?;
        self.next_row_id = row_id.checked_next();
        Ok(row_id)
    }

    /// Push a source rejection at its exact source-stream position.
    /// The full original record is bounded by the same channel capacity and
    /// charged under the same queued-attempt rule as successful rows.
    pub(crate) fn push_rejection(
        &mut self,
        event: crate::executor::dlq::SourceRejectionEvent,
    ) -> Result<(), SourceStreamError> {
        self.consumer_handle.wait_while_paused();
        self.send_attempt(SourceAttemptEvent::Rejection(Box::new(event)))
    }

    /// Push a document-boundary punctuation. One `DocumentOpen` and
    /// one `DocumentClose` per file; the executor's dispatch loop
    /// forwards them through downstream stages with operator-specific
    /// behavior (Aggregate / Output flush; Merge dedupes; Transform /
    /// Route pass through). Punctuations carry no record bytes, so they
    /// carry no charge on the `ConsumerHandle`.
    pub(crate) fn push_punctuation(&mut self, punct: Punctuation) -> Result<(), SourceStreamError> {
        if let Some(barrier) = self.order_barrier.as_mut() {
            barrier.observe_punctuation(punct).map(|_| ())
        } else {
            self.tx
                .send(SourceStreamEvent::Punctuation(punct))
                .map_err(|_| SourceStreamError::Closed)
        }
    }

    /// Bring an ordered Source's barrier figure (staged rows, rows being
    /// released, spill readers) up to date on the handle. The queued
    /// attempts' charges travel with the attempts and need no refresh, so an
    /// unordered Source has nothing to do here.
    fn update_usage(&self) {
        if let Some(barrier) = &self.order_barrier {
            barrier.refresh_accounted_charge();
        }
    }
}

/// `MemoryConsumer` wrapper for a `SourceIngestChannel`.
/// Holds an `Arc<ConsumerHandle>` shared with the source ingest thread. The
/// handle charges exactly the heap the Source's queued attempts hold outside
/// the run's ledger, each attempt's [`QueuedCharge`] from just before its
/// send until it leaves the channel, plus an ordered Source's barrier
/// figure. The records' admitted bytes are charged when allocated, in this
/// consumer's name, so its `peak_charged_bytes` (the ledger's mark) covers
/// both, and `current_usage` is the one charged figure the policies rank by.
///
/// Sources do not spill: `try_spill` returns `Ok(0)` and the
/// arbitrator's policy is expected to choose `pause` instead via
/// `BackPressurePreferred`. `spill_priority = i32::MAX` so the
/// `Priority` fallback ranks Sources last among the spill candidates
/// when no back-pressureable consumer is available.
/// `can_back_pressure = true`; `pause` / `resume` forward to the
/// handle's pause flag, which the ingest thread reads at every
/// `push` boundary.
pub struct SourceConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl SourceConsumer {
    pub fn new(handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl crate::pipeline::memory::MemoryConsumer for SourceConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn peak_charged_bytes(&self) -> Option<u64> {
        Some(self.handle.peak_bytes())
    }

    fn spill_priority(&self) -> i32 {
        i32::MAX
    }

    fn try_spill(
        &self,
        _target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        Ok(0)
    }

    fn can_back_pressure(&self) -> bool {
        true
    }

    fn pause(&self) {
        self.handle.pause();
    }

    fn resume(&self) {
        self.handle.resume();
    }

    fn is_paused(&self) -> bool {
        self.handle.is_paused()
    }

    fn is_active(&self) -> bool {
        self.handle.is_active()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::{ConsumerHandle, MemoryConsumer};
    use clinker_plan::plan::EntityRef;

    fn admitted_record(
        resources: &clinker_record::owned_storage::AllocationResources,
        value: clinker_record::Value,
    ) -> Record {
        use clinker_record::owned_storage::{OwnedValues, SharedStorage};
        let scope = resources.scope().unwrap();
        let mut values = OwnedValues::try_with_capacity(4, &scope).unwrap();
        values.try_push(value, &scope).unwrap();
        Record::from_owned_values(
            SharedStorage::from_arc(Arc::new(clinker_record::Schema::new(vec!["v".into()]))),
            values,
        )
        .unwrap()
    }

    #[test]
    fn source_queue_samples_actual_local_foreign_and_mixed_owners() {
        use crate::executor::preparation::ExecutorResources;
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use crate::pipeline::shutdown::ShutdownToken;
        use clinker_format::preparation::MemoryOnlyResources;
        use clinker_record::{FieldStr, Value};
        use std::num::NonZeroUsize;
        let arb = Arc::new(MemoryArbitrator::with_policy(
            1024 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let observer = arb.writer_resource_observer();
        let weak = Arc::downgrade(&arb);
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::MIN,
            None,
        )
        .unwrap();
        let resources = provider.allocation();
        let foreign = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let foreign_resources = foreign.resources().allocation().clone();
        let local_text =
            FieldStr::try_new(&"local shared".repeat(60), &resources.scope().unwrap()).unwrap();
        let foreign_text = FieldStr::try_new(
            &"foreign shared".repeat(60),
            &foreign_resources.scope().unwrap(),
        )
        .unwrap();
        let escaped_local_bytes = observer.usage().memory;
        let escaped_foreign_bytes = foreign.used();
        let rows = [
            admitted_record(&resources, Value::String(local_text.clone())),
            admitted_record(&resources, Value::String(foreign_text.clone())),
            admitted_record(&foreign_resources, Value::String(local_text.clone())),
        ];
        assert_eq!(rows[0].unaccounted_heap_size(&resources), 0);
        assert_eq!(
            rows[1].unaccounted_heap_size(&resources),
            foreign_text.heap_size()
        );
        assert!(rows[2].unaccounted_heap_size(&resources) > 0);
        let handle = ConsumerHandle::new();
        let (mut channel, rx) =
            SourceIngestChannel::new(4, handle.clone(), PlanNodeId::new(7), resources.clone());
        let mut charges = Vec::new();
        for (i, record) in rows.into_iter().enumerate() {
            charges.push(record.unaccounted_heap_size(&resources) as u64);
            let row = channel.push(record).unwrap();
            assert_eq!(row.ordinal(), i as u64 + 1);
            assert_eq!(handle.bytes(), charges.iter().sum::<u64>());
        }
        let event = crate::executor::dlq::SourceRejectionEvent {
            source_row: channel.reserve_rejected_row_id().unwrap(),
            source_name: Arc::from("rows"),
            source_file: Arc::from("data.csv"),
            row: 4,
            kind: crate::executor::dlq::SourceRejectionKind::DeclaredType,
            message: "type mismatch".into(),
            triggering_field: "v".into(),
            triggering_value: Value::String(foreign_text.clone()),
            original_record: admitted_record(&resources, Value::String(local_text.clone())),
            failed_at: crate::executor::DlqFailureStamp::now(),
        };
        let diagnostic = event.source_name.len()
            + event.source_file.len()
            + event.message.len()
            + event.triggering_field.len();
        assert_eq!(
            event.unaccounted_heap_size(&resources),
            diagnostic + foreign_text.heap_size()
        );
        charges
            .push((std::mem::size_of_val(&event) + event.unaccounted_heap_size(&resources)) as u64);
        channel.push_rejection(event).unwrap();
        assert_eq!(handle.bytes(), charges.iter().sum::<u64>());
        assert_eq!(rx.len(), 4);
        // Each dequeue releases exactly that attempt's charge.
        let first = rx.recv().unwrap();
        let second = rx.recv().unwrap();
        drop(first);
        assert_eq!(handle.bytes(), charges[1..].iter().sum::<u64>());
        drop(second);
        assert_eq!(handle.bytes(), charges[2..].iter().sum::<u64>());
        assert!(charges[1] > 0, "the foreign leaf is charged while queued");
        assert!(observer.usage().memory > escaped_local_bytes);
        assert!(foreign.used() > escaped_foreign_bytes);
        let queued_local = observer.usage().memory;
        let queued_foreign = foreign.used();
        drop(rx);
        // The bounded channel retains buffered messages until its last endpoint
        // drops. Disconnect alone is not destruction of those allocations.
        assert_eq!(observer.usage().memory, queued_local);
        assert_eq!(foreign.used(), queued_foreign);
        let record = admitted_record(&resources, Value::Null);
        assert!(matches!(
            channel.push(record),
            Err(SourceStreamError::Closed)
        ));
        assert_eq!(observer.usage().memory, queued_local);
        assert_eq!(foreign.used(), queued_foreign);
        drop(channel);
        assert_eq!(observer.usage().memory, escaped_local_bytes);
        assert_eq!(foreign.used(), escaped_foreign_bytes);
        drop(provider);
        drop(arb);
        assert!(weak.upgrade().is_none());
        assert!(observer.is_closed());
        assert_eq!(observer.usage().memory, escaped_local_bytes);
        drop(local_text);
        drop(foreign_text);
        assert_eq!(observer.usage().memory, 0);
        assert_eq!(foreign.used(), 0);
    }

    #[test]
    fn ordered_channel_refresh_preserves_decoded_queue_attribution() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use clinker_format::preparation::MemoryOnlyResources;
        use clinker_plan::config::{CompileContext, PipelineConfig};
        use clinker_record::owned_storage::{OwnedValues, SharedStorage};
        use clinker_record::{FieldStr, Schema, Value, synthetic_document_context};
        use std::num::NonZeroUsize;
        let config: PipelineConfig = clinker_plan::yaml::from_str(
            r#"
pipeline: { name: ordered_queue_owner }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      schema: [{ name: key, type: int }, { name: payload, type: string }]
      sort_order: [key]
  - type: sink
    name: out
    input: rows
    config: { name: out, type: csv, path: out.csv }
"#,
        )
        .unwrap();
        let plan = config.compile(&CompileContext::default()).unwrap();
        let order = &plan.dag().order_contract().source_orders[0];
        let order_config = crate::source::order_barrier::SourceOrderConfig::from_compiled(
            order,
            order.source_id,
            "rows",
            &plan.config().source_bodies().next().unwrap().schema,
        )
        .unwrap();
        // Independent finite storage permits deliberately tiny spill pressure;
        // the public lazy-source regression separately binds real run admission.
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let resources = provider.resources().allocation().clone();
        let memory = Arc::new(MemoryArbitrator::with_policy(
            1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let directory = tempfile::tempdir().unwrap();
        let handle = ConsumerHandle::new();
        let (mut channel, rx) = SourceIngestChannel::new_ordered(
            32,
            handle.clone(),
            order.source_id,
            order_config,
            memory.clone(),
            directory.path().to_path_buf(),
            false,
            resources.clone(),
        );
        let doc = synthetic_document_context();
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["key".into(), "payload".into()])));
        let scope = resources.scope().unwrap();
        let text = FieldStr::try_new(&"payload".repeat(300), &scope).unwrap();
        let make = |key| {
            let mut values = OwnedValues::try_with_capacity(2, &scope).unwrap();
            values.try_push(Value::Integer(key), &scope).unwrap();
            values
                .try_push(Value::String(text.clone()), &scope)
                .unwrap();
            let mut record = Record::from_owned_values(schema.clone(), values).unwrap();
            record.set_doc_ctx(doc.clone());
            record
        };
        channel
            .push_punctuation(Punctuation::document_open(doc.clone()))
            .unwrap();
        for key in 1..=3 {
            channel.push(make(key)).unwrap();
        }
        assert!(
            memory.cumulative_spill_bytes() > 0,
            "fixture must actually spill"
        );
        channel
            .push_punctuation(Punctuation::document_close(doc.clone()))
            .unwrap();
        let decoded_queue = handle.bytes();
        channel.update_usage();
        assert_eq!(
            handle.bytes(),
            decoded_queue,
            "channel refresh must preserve the barrier's decoded-row estimate"
        );
        channel
            .push_punctuation(Punctuation::document_open(doc.clone()))
            .unwrap();
        channel.push(make(4)).unwrap();
        let published = handle.bytes();
        channel
            .order_barrier
            .as_ref()
            .unwrap()
            .refresh_accounted_charge();
        assert_eq!(
            handle.bytes(),
            published,
            "a later file must not reinstall the old original-row estimate"
        );
        drop(channel);
        drop(rx);
        drop(text);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn source_peak_reads_its_attributed_record_allocations() {
        use crate::executor::preparation::ExecutorResources;
        use crate::pipeline::memory::ledger::Requester;
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use crate::pipeline::shutdown::ShutdownToken;
        use clinker_plan::runtime_error::{ConsumerLabel, MemorySurface};
        use clinker_record::{FieldStr, Value};
        use std::alloc::Layout;
        use std::num::NonZeroUsize;

        const KIB: u64 = 1024;
        let arb = Arc::new(MemoryArbitrator::with_policy(
            4 * 1024 * KIB,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::MIN,
            None,
        )
        .unwrap();
        let run = provider.allocation();
        let handle = ConsumerHandle::new();
        let consumer = Arc::new(SourceConsumer::new(handle.clone()));
        let id = arb.register_node_consumer(
            consumer.clone(),
            handle.clone(),
            ConsumerLabel {
                node: "events".to_string(),
                surface: MemorySurface::RowsRead,
            },
        );
        let view = provider.attributed_allocation(Requester::for_consumer(id));
        assert_eq!(view.identity(), run.identity());
        let (mut channel, rx) =
            SourceIngestChannel::new(4, handle.clone(), PlanNodeId::new(7), view.clone());

        let text =
            FieldStr::try_new(&"x".repeat(64 * KIB as usize), &view.scope().unwrap()).unwrap();
        channel
            .push(admitted_record(&view, Value::String(text.clone())))
            .unwrap();
        let held = |arb: &MemoryArbitrator| {
            arb.ledger_snapshot(0, Requester::governed())
                .holders
                .iter()
                .find(|holder| holder.consumer == id)
                .map_or(0, |holder| holder.charged)
        };
        assert!(
            held(&arb) >= 64 * KIB,
            "the Source's governed record bytes are charged in its name"
        );
        assert_eq!(handle.bytes(), 0, "the Source's handle charges nothing");
        let lease = view
            .scope()
            .unwrap()
            .reserve(Layout::new::<[u8; 64]>())
            .unwrap();
        assert!(
            lease.is_accounted_by(&run),
            "a lease from the Source's view is the run's"
        );
        drop(lease);
        let peak = consumer.peak_charged_bytes().unwrap();
        assert!(
            peak >= 64 * KIB,
            "the Source's peak covers its record allocations: {peak}"
        );

        drop(channel);
        drop(rx);
        drop(text);
        assert_eq!(held(&arb), 0, "the dropped records return their bytes");
        assert_eq!(
            consumer.peak_charged_bytes(),
            Some(peak),
            "a release never lowers the mark"
        );
        assert_eq!(
            arb.per_node_peak_charged_bytes().get("events"),
            Some(&peak),
            "the node reports its Source's mark"
        );
        arb.unregister_consumer(id);
        assert_eq!(arb.per_node_peak_charged_bytes().get("events"), Some(&peak));
    }

    /// A run with admission, a foreign provider, and a Source handle bound to
    /// the run's ledger.
    fn charged_source_channel() -> (
        Arc<crate::pipeline::memory::MemoryArbitrator>,
        crate::executor::preparation::ExecutorResources,
        clinker_format::preparation::MemoryOnlyResources,
        Arc<ConsumerHandle>,
        crate::pipeline::memory::ConsumerId,
    ) {
        use crate::executor::preparation::ExecutorResources;
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use crate::pipeline::shutdown::ShutdownToken;
        use clinker_format::preparation::MemoryOnlyResources;
        use clinker_plan::runtime_error::{ConsumerLabel, MemorySurface};
        use std::num::NonZeroUsize;
        let arb = Arc::new(MemoryArbitrator::with_policy(
            1024 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::MIN,
            None,
        )
        .unwrap();
        let foreign = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let handle = ConsumerHandle::new();
        let id = arb.register_node_consumer(
            Arc::new(SourceConsumer::new(handle.clone())),
            handle.clone(),
            ConsumerLabel {
                node: "rows".to_string(),
                surface: MemorySurface::RowsRead,
            },
        );
        (arb, provider, foreign, handle, id)
    }

    #[test]
    fn queued_charge_is_released_when_the_event_leaves_the_channel() {
        use clinker_record::{FieldStr, Value};
        let (arb, provider, foreign, handle, id) = charged_source_channel();
        let resources = provider.allocation();
        let foreign_resources = foreign.resources().allocation().clone();
        let (mut channel, rx) =
            SourceIngestChannel::new(8, handle.clone(), PlanNodeId::new(7), resources.clone());
        let local_text =
            FieldStr::try_new(&"local".repeat(100), &resources.scope().unwrap()).unwrap();
        let foreign_text =
            FieldStr::try_new(&"foreign".repeat(100), &foreign_resources.scope().unwrap()).unwrap();

        // A fully admitted record holds nothing outside the ledger.
        channel
            .push(admitted_record(
                &resources,
                Value::String(local_text.clone()),
            ))
            .unwrap();
        assert_eq!(handle.bytes(), 0);
        let mut charges = vec![0];
        for record in [
            admitted_record(&resources, Value::String(foreign_text.clone())),
            admitted_record(&foreign_resources, Value::String(local_text.clone())),
        ] {
            let charge = record.unaccounted_heap_size(&resources) as u64;
            assert!(charge > 0, "the fixture queues unadmitted heap");
            charges.push(charge);
            channel.push(record).unwrap();
            assert_eq!(handle.bytes(), charges.iter().sum::<u64>());
        }

        // The walk's path: the charge comes off the attempt while the record
        // is still held, before it is routed on.
        for charge in &charges {
            let SourceStreamEvent::Attempt {
                event: SourceAttemptEvent::Record(record, _),
                queued,
                ..
            } = rx.recv().unwrap()
            else {
                panic!("a queued record");
            };
            assert_eq!(queued.bytes(), *charge);
            let (held, charged) = (handle.bytes(), arb.charged_bytes());
            drop(queued);
            assert_eq!(handle.bytes(), held - charge);
            assert_eq!(
                arb.charged_bytes(),
                charged - charge,
                "the ledger releases exactly that attempt's charge"
            );
            drop(record);
        }
        assert_eq!(handle.bytes(), 0);
        drop(channel);
        drop(rx);
        arb.unregister_consumer(id);
    }

    #[test]
    fn queued_charge_is_released_when_an_unconsumed_channel_is_destroyed() {
        use clinker_record::{FieldStr, Value};
        let (arb, provider, foreign, handle, id) = charged_source_channel();
        let resources = provider.allocation();
        let foreign_resources = foreign.resources().allocation().clone();
        let (mut channel, rx) =
            SourceIngestChannel::new(8, handle.clone(), PlanNodeId::new(7), resources.clone());
        let foreign_text =
            FieldStr::try_new(&"foreign".repeat(100), &foreign_resources.scope().unwrap()).unwrap();
        let mut queued = 0;
        for _ in 0..3 {
            let record = admitted_record(&resources, Value::String(foreign_text.clone()));
            queued += record.unaccounted_heap_size(&resources) as u64;
            channel.push(record).unwrap();
        }
        assert!(queued > 0, "the fixture queues unadmitted heap");
        assert_eq!(handle.bytes(), queued);
        let charged = arb.charged_bytes();

        // Disconnect alone destroys nothing: the attempts stay in the channel
        // until its last endpoint drops, and so do their charges.
        drop(rx);
        assert_eq!(handle.bytes(), queued);
        assert_eq!(arb.charged_bytes(), charged);

        // A failed send returns its attempt, whose charge is released at once.
        let record = admitted_record(&resources, Value::String(foreign_text.clone()));
        assert!(record.unaccounted_heap_size(&resources) > 0);
        assert!(matches!(
            channel.push(record),
            Err(SourceStreamError::Closed)
        ));
        assert_eq!(handle.bytes(), queued);
        assert_eq!(arb.charged_bytes(), charged);

        drop(channel);
        assert_eq!(handle.bytes(), 0);
        assert!(
            arb.charged_bytes() <= charged - queued,
            "destroying the channel releases every queued attempt's charge"
        );
        arb.unregister_consumer(id);
    }

    #[test]
    fn source_consumer_reports_handle_bytes_and_never_spills() {
        let handle = ConsumerHandle::new();
        handle.set_bytes(16 * 1024);
        let consumer = SourceConsumer::new(handle.clone());
        assert_eq!(consumer.current_usage(), 16 * 1024);
        // Sources don't spill: try_spill always reports zero freed.
        assert_eq!(consumer.try_spill(u64::MAX).unwrap(), 0);
        assert!(!handle.take_spill_request());
    }

    #[test]
    fn source_consumer_is_back_pressureable_and_routes_pause_to_handle() {
        let handle = ConsumerHandle::new();
        let consumer = SourceConsumer::new(handle.clone());
        assert!(consumer.can_back_pressure());
        // Source spill priority sits above the integer range used by
        // other consumers; the Priority policy ranks Sources last,
        // matching BackPressurePreferred's prefer-pause posture.
        assert_eq!(consumer.spill_priority(), i32::MAX);
        consumer.pause();
        assert!(handle.is_paused());
        consumer.resume();
        assert!(!handle.is_paused());
    }

    #[test]
    fn source_channels_preserve_allocation_domain_and_release_run_ownership() {
        use crate::executor::preparation::ExecutorResources;
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use crate::pipeline::shutdown::ShutdownToken;
        use clinker_plan::config::{CompileContext, PipelineConfig};
        use std::alloc::Layout;
        use std::num::NonZeroUsize;

        let memory = Arc::new(MemoryArbitrator::with_policy(
            4096,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let weak = Arc::downgrade(&memory);
        let observer = memory.writer_resource_observer();
        let provider = ExecutorResources::new(
            memory.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::MIN,
            None,
        )
        .unwrap();
        let allocation = provider.allocation();
        let writers = provider.resources();
        assert_eq!(allocation.identity(), writers.allocation().identity());
        let scope = allocation.scope().unwrap();
        let lease = scope.reserve(Layout::new::<[u8; 64]>()).unwrap();
        let foreign =
            clinker_format::preparation::MemoryOnlyResources::new(NonZeroUsize::new(4096).unwrap());
        let foreign_resources = foreign.resources();
        let foreign_lease = foreign_resources
            .allocation()
            .reserve(scope.owner(), Layout::new::<[u8; 64]>())
            .unwrap();
        assert_eq!(lease.owner(), foreign_lease.owner());
        assert!(!foreign_lease.is_accounted_by(&allocation));
        assert!(lease.is_accounted_by(writers.allocation()));

        let (ordinary, ordinary_rx) = SourceIngestChannel::new(
            2,
            ConsumerHandle::new(),
            PlanNodeId::new(7),
            allocation.clone(),
        );
        ordinary.assert_allocation_domain(&allocation);
        let config: PipelineConfig = clinker_plan::yaml::from_str(
            r#"
pipeline: { name: allocation_domain }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      schema: [{ name: key, type: int }]
      sort_order: [key]
  - type: sink
    name: out
    input: rows
    config: { name: out, type: csv, path: out.csv }
"#,
        )
        .unwrap();
        let plan = PipelineConfig::compile(&config, &CompileContext::default()).unwrap();
        let order = &plan.dag().order_contract().source_orders[0];
        let source_schema = &plan.config().source_bodies().next().unwrap().schema;
        let order_config = crate::source::order_barrier::SourceOrderConfig::from_compiled(
            order,
            order.source_id,
            "rows",
            source_schema,
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (ordered, ordered_rx) = SourceIngestChannel::new_ordered(
            2,
            ConsumerHandle::new(),
            order.source_id,
            order_config,
            memory.clone(),
            dir.path().to_path_buf(),
            false,
            allocation.clone(),
        );
        ordered.assert_allocation_domain(&allocation);
        assert_eq!(memory.consumer_count(), 1);
        assert_eq!(observer.usage().memory, 64);
        drop(ordered);
        drop(ordered_rx);
        drop(ordinary);
        drop(ordinary_rx);
        assert_eq!(memory.consumer_count(), 1);
        drop(writers);
        drop(provider);
        drop(memory);
        assert!(weak.upgrade().is_none());
        assert!(observer.is_closed());
        assert!(!observer.has_managed_handle());
        assert_eq!(observer.usage().memory, 64);
        drop(lease);
        assert_eq!(observer.usage().memory, 0);
    }
}
