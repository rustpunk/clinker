//! Bounded-batch inter-stage handoff carrying `StreamEvent` batches.
//!
//! [`EventBatch`] is the streaming counterpart of
//! [`crate::executor::node_buffer::NodeBuffer`]: where a `NodeBuffer`
//! holds a stage's *entire* output before the consumer drains it (full
//! materialization, charged as one slot against the arbitrator), an
//! `EventBatch` is one bounded slice of that output. A streaming stage
//! fills one batch, hands it off to its downstream consumer, and starts
//! a fresh one — it never accumulates or admits a whole-stage
//! `node_buffers` slot. When the consumer is a streaming `Output` thread
//! fed over a bounded channel (the fused `Source → Transform → Output`
//! chain), the channel's blocking `send` paces the producer, so peak
//! inter-stage memory for the stage is one batch plus the channel's
//! in-flight bound rather than the whole stage.
//!
//! The events inside a batch — records and `DocumentOpen` /
//! `DocumentClose` punctuations — stay in strict arrival order, and that
//! order is preserved *across* batch boundaries: a document whose records
//! are split over N batches keeps its trailing `DocumentClose` after the
//! last record of that document, even when the close lands in a later
//! batch than some of the document's records. This is the documented
//! anti-requirement — the punctuation-drop in the existing fused
//! Transform path (`transform_fused_consume`) is exactly what the batch
//! substrate must not inherit.
//!
//! Batches are accumulated through an [`EventBatcher`], which fills the
//! current batch up to a configured `batch_size` and flushes a full
//! batch to a sink closure. Because a batch's cut point is purely a
//! count threshold and the batcher never reorders events relative to the
//! order they were pushed, the trailing-punctuation invariant holds by
//! construction: a `DocumentClose` pushed after a document's last record
//! is appended after that record in whichever batch it lands in, and no
//! earlier batch can contain it.

use std::path::Path;
use std::sync::Arc;

use crate::executor::stream_event::{Punctuation, SourceRowId, StreamEvent};
use crate::pipeline::memory::{ConsumerHandle, MemoryArbitrator};
use clinker_plan::error::PipelineError;
use clinker_record::owned_storage::AllocationResources;

/// Default per-batch event count when no `pipeline.batch_size` knob and
/// no per-Transform override is set.
///
/// 2048 events balances the in-flight footprint of one batch against the
/// per-flush bookkeeping cost: small enough that one batch (plus the
/// streaming channel's in-flight bound) is a negligible fraction of the
/// default 512 MiB budget for typical record widths, large enough that
/// the per-flush handoff cost is amortized over thousands of records.
pub const DEFAULT_BATCH_SIZE: usize = 2048;

/// One bounded slice of a streaming stage's output.
///
/// Holds records and document-boundary punctuations interleaved in the
/// order they were produced. A batch is the unit the streaming stage
/// hands off to its downstream consumer; its event count is bounded by
/// the owning [`EventBatcher`]'s `batch_size`, so a single in-flight
/// batch caps the stage's own working set for one flush cycle.
#[derive(Debug, Default)]
pub(crate) struct EventBatch {
    events: Vec<StreamEvent>,
}

impl EventBatch {
    /// Construct an empty batch with capacity reserved for `cap` events.
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            events: Vec::with_capacity(cap),
        }
    }

    /// Number of events (records and punctuations) in the batch.
    pub(crate) fn len(&self) -> usize {
        self.events.len()
    }

    /// `true` when the batch carries no events.
    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Vector backing not already represented by each row's legacy inline
    /// `(Record, SourceRowId)` charge. Includes unused capacity and event tags.
    pub(crate) fn retained_container_overhead(&self) -> u64 {
        let records = self.events.iter().filter(|event| event.is_record()).count();
        (self.events.capacity() * std::mem::size_of::<StreamEvent>()
            - records * std::mem::size_of::<(clinker_record::Record, SourceRowId)>()) as u64
    }

    /// Consume the batch into its event vector in arrival order.
    pub(crate) fn into_events(self) -> Vec<StreamEvent> {
        self.events
    }

    /// Heuristic in-memory footprint of the batch's records, mirroring
    /// [`crate::executor::node_buffer::NodeBuffer::estimated_memory_bytes`]:
    /// records contribute the shared
    /// [`crate::executor::node_buffer::record_byte_cost`] per row,
    /// punctuations contribute `0` (they are O(1) per document and never
    /// spill). This full physical diagnostic includes logical slots even when
    /// intrinsic grants already account for them; runtime attribution uses
    /// [`Self::unaccounted_estimated_bytes`] instead.
    #[cfg(test)]
    pub(crate) fn estimated_bytes(&self) -> u64 {
        self.events
            .iter()
            .map(|event| match event {
                StreamEvent::Record(record, _) => {
                    crate::executor::node_buffer::record_byte_cost(record.schema().column_count())
                }
                StreamEvent::Punctuation(_) => 0,
            })
            .sum()
    }

    /// Existing fixed-row heuristic for actual records, excluding only private
    /// value slots already charged to the receiving run's allocation ledger.
    pub(crate) fn unaccounted_estimated_bytes(&self, resources: &AllocationResources) -> u64 {
        self.events
            .iter()
            .map(|event| match event {
                StreamEvent::Record(record, _) => {
                    crate::executor::node_buffer::unaccounted_record_byte_cost(record, resources)
                }
                StreamEvent::Punctuation(_) => 0,
            })
            .sum()
    }

    /// Append a record event, preserving arrival order.
    pub(crate) fn push_record<R>(&mut self, record: clinker_record::Record, row_id: R)
    where
        R: Into<SourceRowId>,
    {
        self.events.push(StreamEvent::record(record, row_id.into()));
    }

    /// Append a punctuation event, preserving arrival order.
    ///
    /// A `DocumentClose` pushed after the document's last record lands
    /// after that record in the event stream and therefore in whichever
    /// batch the flush boundary places it — the trailing-punctuation
    /// invariant the substrate guarantees across batch splits.
    pub(crate) fn push_punctuation(&mut self, punct: Punctuation) {
        self.events.push(StreamEvent::punctuation(punct));
    }
}

/// Accumulates [`StreamEvent`]s into bounded [`EventBatch`]es and flushes
/// a full batch to a sink as soon as it reaches `batch_size`.
///
/// The batcher never reorders events: each `push_*` appends to the
/// current batch, and a flush hands off the whole current batch and
/// starts a fresh one. Because the cut point is a pure count threshold
/// applied in arrival order, punctuation ordering is preserved across
/// batch boundaries — a document's `DocumentClose` cannot be flushed
/// before a record that was pushed earlier, and records pushed after a
/// `DocumentOpen` cannot precede it. The owner calls [`Self::finish`]
/// once the upstream is exhausted to flush any partial trailing batch.
///
/// The sink is a fallible closure so a downstream send error (a closed
/// channel, a spill-quota overrun) propagates out of the push that
/// triggered the flush rather than being swallowed.
pub(crate) struct EventBatcher<F, E>
where
    F: FnMut(EventBatch) -> Result<(), E>,
{
    current: EventBatch,
    batch_size: usize,
    sink: F,
}

impl<F, E> EventBatcher<F, E>
where
    F: FnMut(EventBatch) -> Result<(), E>,
{
    /// Construct a batcher that flushes full `batch_size`-event batches
    /// to `sink`. A `batch_size` of zero is clamped to one so every
    /// event still flushes (a zero threshold would never flush and would
    /// accumulate the whole stage in memory — the opposite of the
    /// substrate's purpose).
    pub(crate) fn new(batch_size: usize, sink: F) -> Self {
        let batch_size = batch_size.max(1);
        Self {
            current: EventBatch::with_capacity(batch_size),
            batch_size,
            sink,
        }
    }

    /// Push a record, flushing the current batch first if it is full.
    pub(crate) fn push_record<R>(
        &mut self,
        record: clinker_record::Record,
        row_id: R,
    ) -> Result<(), E>
    where
        R: Into<SourceRowId>,
    {
        self.flush_if_full()?;
        self.current.push_record(record, row_id);
        Ok(())
    }

    /// Push a punctuation, flushing the current batch first if it is
    /// full. Pushing the punctuation after the flush keeps it ordered
    /// after every record already accumulated in the prior batch.
    pub(crate) fn push_punctuation(&mut self, punct: Punctuation) -> Result<(), E> {
        self.flush_if_full()?;
        self.current.push_punctuation(punct);
        Ok(())
    }

    /// Flush the current batch when it has reached `batch_size` events.
    fn flush_if_full(&mut self) -> Result<(), E> {
        if self.current.len() >= self.batch_size {
            self.flush_current()?;
        }
        Ok(())
    }

    /// Hand the current batch to the sink and start a fresh one. A no-op
    /// on an empty current batch so a flush at an exact boundary does not
    /// emit a spurious empty batch.
    fn flush_current(&mut self) -> Result<(), E> {
        if self.current.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(
            &mut self.current,
            EventBatch::with_capacity(self.batch_size),
        );
        (self.sink)(batch)
    }

    /// Flush any partial trailing batch. Called once the upstream is
    /// exhausted so the final under-full batch reaches the sink.
    pub(crate) fn finish(mut self) -> Result<(), E> {
        self.flush_current()
    }
}

/// Per-batch arbitrator accounting for a streaming inter-stage handoff.
///
/// Where [`crate::executor::dispatch::admit_node_buffer`] charges a
/// blocking stage's *entire* output as one `NodeBufferConsumer` slot, a
/// streaming stage registers exactly one consumer wrapper for its logical
/// slot and then drives this handle once per flushed [`EventBatch`]:
///
/// - **Charge on flush.** [`Self::charge_and_route`] adds the batch's
///   [`EventBatch::unaccounted_estimated_bytes`] to the shared
///   [`ConsumerHandle`] before the events leave the producer, so the
///   arbitrator's `current_usage` for the slot reflects "batches in
///   flight," never the whole stage.
/// - **Discharge on consume.** The downstream writer thread holds a clone
///   of the same `ConsumerHandle` and subtracts each consumed record's
///   [`crate::executor::node_buffer::unaccounted_record_byte_cost`] as it drains, so
///   the live count tracks exactly what is still buffered between the
///   producer and the writer.
///
/// On a soft-threshold trip (`MemoryArbitrator::should_spill_self()`,
/// which observes RSS without driving the pausing arbitration round) —
/// and when `spill_allowed` matches the compiled buffer classification — the
/// flushed batch's records round-trip through a
/// `SpillFile<SourceRowId>` on disk instead of being held in the producer's
/// working set: each maximal run of consecutive records is written out,
/// re-read, and forwarded to the writer one at a time, relieving the
/// in-memory peak for that batch. Punctuations are forwarded in place
/// between the runs they separate, so the records-and-punctuations
/// interleaving the producer emitted survives the spill round-trip
/// byte-for-byte in arrival order — a `DocumentOpen` that frames a run
/// still precedes that run's records, and a `DocumentClose` still trails
/// them, exactly as on the in-memory path.
///
/// One `StreamingChargeHandle` lives per producer slot for the slot's
/// whole lifetime; the wrapper it pairs with is unregistered when the
/// stream finishes.
pub(crate) struct StreamingChargeHandle {
    handle: Arc<ConsumerHandle>,
    arbitrator: Arc<MemoryArbitrator>,
    spill_root_path: Arc<Path>,
    node_name: String,
    spill_allowed: bool,
    /// Workspace `[storage.spill] compress` policy, resolved per spilled run
    /// against the run's schema width and `batch_size` so `auto` skips LZ4 on
    /// narrow streaming spills where the per-frame fixed cost outweighs the
    /// savings.
    spill_compress: clinker_plan::config::CompressMode,
    /// Run-wide batch size, the `auto` heuristic's rows-per-batch projection.
    batch_size: usize,
    allocation_resources: AllocationResources,
}

impl StreamingChargeHandle {
    pub(crate) fn allocation_resources(&self) -> &AllocationResources {
        &self.allocation_resources
    }
    /// Attach retained producer storage to this slot's single counter. Atomic
    /// deltas preserve concurrent Sink subtraction; drop releases only this
    /// owner's remaining portion of the counter.
    pub(crate) fn retain_bytes(&self, bytes: u64) -> StreamingReservation {
        StreamingReservation::retain(self.handle.clone(), self.arbitrator.clone(), bytes)
    }

    pub(crate) fn retain_from_prior(
        &self,
        bytes: u64,
        prior: Option<StreamingReservation>,
    ) -> StreamingReservation {
        self.handle.add_bytes(bytes);
        drop(prior);
        self.arbitrator.sample_peak_consumer_usage();
        StreamingReservation {
            handle: self.handle.clone(),
            arbitrator: self.arbitrator.clone(),
            bytes,
        }
    }

    pub(crate) fn own_charged_batch(
        &self,
        batch: EventBatch,
        reservation: StreamingReservation,
    ) -> Result<ChargedBatch, PipelineError> {
        if !Arc::ptr_eq(&self.handle, &reservation.handle)
            || batch.unaccounted_estimated_bytes(&self.allocation_resources) != reservation.bytes
        {
            drop(batch);
            drop(reservation);
            return Err(PipelineError::Internal {
                op: "streaming-handoff",
                node: self.node_name.clone(),
                detail: "batch ownership does not match its streaming slot".into(),
            });
        }
        Ok(ChargedBatch { batch, reservation })
    }

    /// Route a batch whose rows already belong to this counter. Each accepted
    /// send moves ownership to the Sink without changing the total charge.
    /// Failed routing drops and releases only the unsent remainder.
    pub(crate) fn route_charged_batch(
        &self,
        charged: ChargedBatch,
        send: impl FnMut(StreamEvent) -> Result<(), PipelineError>,
    ) -> Result<(), PipelineError> {
        let ChargedBatch {
            batch,
            mut reservation,
        } = charged;
        self.route_batch(batch, &mut reservation, send)
    }
    /// Construct a charge handle for one streaming producer slot.
    ///
    /// `handle` is the shared [`ConsumerHandle`] already registered with
    /// the arbitrator as a `NodeBufferConsumer`; the same `Arc` is cloned
    /// into the writer thread so the discharge side subtracts from the
    /// counter this side adds to. `spill_allowed` mirrors
    /// `dispatch::node_buffer_spill_allowed` for the slot. `spill_compress`
    /// and `batch_size` resolve the spill-file compression mode per run.
    // Keep allocation admission separate from retained-row and spill ownership.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        handle: Arc<ConsumerHandle>,
        arbitrator: Arc<MemoryArbitrator>,
        spill_root_path: Arc<Path>,
        node_name: String,
        spill_allowed: bool,
        spill_compress: clinker_plan::config::CompressMode,
        batch_size: usize,
        allocation_resources: AllocationResources,
    ) -> Self {
        Self {
            handle,
            arbitrator,
            spill_root_path,
            node_name,
            spill_allowed,
            spill_compress,
            batch_size,
            allocation_resources,
        }
    }

    /// Charge one flushed batch against the arbitrator and route its
    /// events downstream through `send`.
    ///
    /// Adds the batch's estimated bytes to the slot's handle and samples
    /// the arbitrator's peak charged usage. When `spill_allowed` and the
    /// soft RSS threshold has tripped, each maximal run of consecutive
    /// records is spilled to a `SpillFile<SourceRowId>` and streamed back out from
    /// disk one at a time (relieving the producer's in-memory peak for the
    /// batch) with its spill bytes recorded against the disk quota; an
    /// over-quota total surfaces the structured `MemoryBudgetExceeded`
    /// shape with `detail: "spill quota exceeded"`. Punctuations are
    /// forwarded in place between the runs they separate, so the spill path
    /// emits the batch's events in the same arrival order the in-memory
    /// path does — the records-and-punctuations interleaving the producer
    /// built is preserved across the disk round-trip. Otherwise the events
    /// go straight to `send` in memory.
    ///
    /// `send` is the producer's bounded-channel send; its blocking
    /// back-pressure paces the producer when the writer falls behind.
    pub(crate) fn charge_and_route(
        &self,
        batch: EventBatch,
        send: impl FnMut(StreamEvent) -> Result<(), PipelineError>,
    ) -> Result<(), PipelineError> {
        let bytes = batch.unaccounted_estimated_bytes(&self.allocation_resources);
        let reservation = self.retain_bytes(bytes);
        self.route_charged_batch(self.own_charged_batch(batch, reservation)?, send)
    }

    fn route_batch(
        &self,
        batch: EventBatch,
        reservation: &mut StreamingReservation,
        mut send: impl FnMut(StreamEvent) -> Result<(), PipelineError>,
    ) -> Result<(), PipelineError> {
        if self.spill_allowed && self.arbitrator.should_spill_self() {
            // Spill record *runs* in place rather than partitioning the
            // batch into all-records-then-all-punctuations: a punctuation
            // flushes the run that preceded it through disk and is then
            // forwarded at its own offset, so a `[DocumentOpen, r0, r1,
            // DocumentClose]` batch re-emits in that exact order instead of
            // moving the open after the records it frames.
            let mut run: Vec<(clinker_record::Record, SourceRowId)> = Vec::new();
            for event in batch.into_events() {
                match event {
                    StreamEvent::Record(record, rn) => run.push((record, rn)),
                    StreamEvent::Punctuation(p) => {
                        self.spill_and_forward_run(
                            std::mem::take(&mut run),
                            reservation,
                            &mut send,
                        )?;
                        self.publish(StreamEvent::punctuation(p), reservation, &mut send)?;
                    }
                }
            }
            self.spill_and_forward_run(run, reservation, &mut send)?;
            return Ok(());
        }

        for event in batch.into_events() {
            self.publish(event, reservation, &mut send)?;
        }
        Ok(())
    }

    fn ownership_error(&self) -> PipelineError {
        PipelineError::Internal {
            op: "streaming-handoff",
            node: self.node_name.clone(),
            detail: "row ownership does not match its streaming reservation".into(),
        }
    }

    /// Publication moves only this row's ownership. A synchronous consumer may
    /// have already discharged it before send returns, so detach is not atomic.
    fn publish(
        &self,
        event: StreamEvent,
        reservation: &mut StreamingReservation,
        send: &mut impl FnMut(StreamEvent) -> Result<(), PipelineError>,
    ) -> Result<(), PipelineError> {
        let bytes = match &event {
            StreamEvent::Record(record, _) => {
                crate::executor::node_buffer::unaccounted_record_byte_cost(
                    record,
                    &self.allocation_resources,
                )
            }
            StreamEvent::Punctuation(_) => 0,
        };
        let remaining = reservation
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| self.ownership_error())?;
        send(event)?;
        reservation.bytes = remaining;
        Ok(())
    }

    /// Spill one run of consecutive records to disk, then re-read and
    /// forward them in order through `send`.
    ///
    /// The writer's discharge path stays uniform — it always sees
    /// `StreamEvent::Record`s and subtracts their per-record cost
    /// regardless of whether the batch round-tripped through disk. An
    /// empty run is a no-op (no spill file). Records the spilled file's
    /// on-disk size against the arbitrator's disk quota and surfaces
    /// `SpillCapExceeded` (E320) when the cumulative total exceeds the cap.
    fn spill_and_forward_run(
        &self,
        run: Vec<(clinker_record::Record, SourceRowId)>,
        reservation: &mut StreamingReservation,
        send: &mut impl FnMut(StreamEvent) -> Result<(), PipelineError>,
    ) -> Result<(), PipelineError> {
        // Resolve the compression mode against this run's schema width and the
        // run-wide batch size, so the streaming spill file matches what
        // `--explain` projects for the slot.
        let column_count = run
            .first()
            .map(|(r, _)| r.schema().column_count())
            .unwrap_or(0);
        let compress = self
            .spill_compress
            .resolve_for_schema(column_count, self.batch_size as u64);
        let original_bytes = run
            .iter()
            .map(|(record, _)| {
                crate::executor::node_buffer::unaccounted_record_byte_cost(
                    record,
                    &self.allocation_resources,
                )
            })
            .sum::<u64>();
        let remaining = reservation
            .bytes
            .checked_sub(original_bytes)
            .ok_or_else(|| self.ownership_error())?;
        #[cfg(test)]
        observe_spill_transition(SpillTransition::BeforeWrite, reservation, None);
        let result = crate::executor::node_buffer_spill::spill_node_buffer(
            run,
            Some(self.spill_root_path.as_ref()),
            compress,
        );
        // The spill call owns and drops the original rows on success and error.
        // Their intrinsic grants and this producer portion have independent
        // lifetimes; subsequent decoded rows require their own legacy charge.
        reservation.resize(remaining);
        #[cfg(test)]
        observe_spill_transition(SpillTransition::AfterWrite, reservation, None);
        let Some((file, _count)) = result? else {
            return Ok(());
        };
        let file_bytes = std::fs::metadata(file.path()).map(|m| m.len()).unwrap_or(0);
        if self
            .arbitrator
            .record_spill_bytes(&self.node_name, file_bytes)
        {
            return Err(PipelineError::spill_cap_exceeded(
                self.node_name.clone(),
                self.arbitrator.max_spill_bytes(),
                file_bytes,
                self.arbitrator.cumulative_spill_bytes(),
            ));
        }
        #[cfg(test)]
        observe_spill_transition(SpillTransition::BeforeRead, reservation, Some(file.path()));
        for item in file.reader()? {
            let (record, rn) = item?;
            let bytes = crate::executor::node_buffer::unaccounted_record_byte_cost(
                &record,
                &self.allocation_resources,
            );
            let retained = reservation
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| self.ownership_error())?;
            reservation.resize(retained);
            self.publish(StreamEvent::record(record, rn), reservation, send)?;
        }
        Ok(())
    }
}

/// One portion of a streaming slot's charge, distinct from queued rows.
pub(crate) struct StreamingReservation {
    arbitrator: Arc<MemoryArbitrator>,
    handle: Arc<ConsumerHandle>,
    bytes: u64,
}
impl StreamingReservation {
    pub(crate) fn retain(
        handle: Arc<ConsumerHandle>,
        arbitrator: Arc<MemoryArbitrator>,
        bytes: u64,
    ) -> Self {
        handle.add_bytes(bytes);
        arbitrator.sample_peak_consumer_usage();
        Self {
            handle,
            arbitrator,
            bytes,
        }
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn resize(&mut self, bytes: u64) {
        if bytes > self.bytes {
            self.handle.add_bytes(bytes - self.bytes);
        } else {
            self.handle.sub_bytes(self.bytes - bytes);
        }
        self.bytes = bytes;
        self.arbitrator.sample_peak_consumer_usage();
    }
    /// Replace a consumed row's producer-owned heap with the existing stream
    /// estimate in one atomic delta on the same counter. The Sink may subtract
    /// other queued rows concurrently. No bytes are copied or newly admitted.
    pub(crate) fn transfer_row(&mut self, pending: &mut Self, retained: u64, row_bytes: u64) {
        debug_assert!(Arc::ptr_eq(&self.handle, &pending.handle));
        self.resize(retained.saturating_add(row_bytes));
        self.bytes = retained;
        pending.bytes = pending.bytes.saturating_add(row_bytes);
    }
}
impl Drop for StreamingReservation {
    fn drop(&mut self) {
        self.handle.sub_bytes(self.bytes);
    }
}
pub(crate) struct ChargedBatch {
    batch: EventBatch,
    reservation: StreamingReservation,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpillTransition {
    BeforeWrite,
    AfterWrite,
    BeforeRead,
}

#[cfg(test)]
type SpillObserver = Box<dyn FnMut(SpillTransition, u64, u64, Option<&Path>)>;

#[cfg(test)]
thread_local! {
    static SPILL_OBSERVER: std::cell::RefCell<Option<SpillObserver>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn observe_spill_transition(
    transition: SpillTransition,
    reservation: &StreamingReservation,
    path: Option<&Path>,
) {
    SPILL_OBSERVER.with_borrow_mut(|observer| {
        if let Some(observer) = observer {
            observer(
                transition,
                reservation.bytes,
                reservation.handle.bytes(),
                path,
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::owned_storage::SharedStorage;
    use std::sync::Arc;

    use clinker_record::{
        DocumentContext, DocumentId, Record, Schema, Value, synthetic_document_context,
    };

    use crate::executor::stream_event::PunctuationKind;

    fn legacy_test_resources() -> AllocationResources {
        clinker_format::preparation::MemoryOnlyResources::new(
            std::num::NonZeroUsize::new(1024 * 1024).unwrap(),
        )
        .resources()
        .allocation()
        .clone()
    }

    struct SpillRig {
        charge: StreamingChargeHandle,
        _provider: crate::executor::preparation::ExecutorResources,
        directory: tempfile::TempDir,
    }

    impl SpillRig {
        fn new(compress: bool) -> Self {
            let arbitrator = Arc::new(MemoryArbitrator::with_policy(
                1024 * 1024,
                0.8,
                0.7,
                Box::new(crate::pipeline::memory::NoOpPolicy),
            ));
            let provider = crate::executor::preparation::ExecutorResources::new(
                arbitrator.clone(),
                crate::pipeline::shutdown::ShutdownToken::detached(),
                None,
                std::num::NonZeroUsize::MIN,
                None,
            )
            .unwrap();
            let directory = tempfile::tempdir().unwrap();
            let charge = StreamingChargeHandle::new(
                ConsumerHandle::new(),
                arbitrator,
                Arc::from(directory.path()),
                "stream-owner".into(),
                true,
                if compress {
                    clinker_plan::config::CompressMode::On
                } else {
                    clinker_plan::config::CompressMode::Off
                },
                8,
                provider.allocation(),
            );
            Self {
                charge,
                _provider: provider,
                directory,
            }
        }

        fn force_spill(&self) {
            // Admit fixtures first; forcing pressure must not prevent their
            // real intrinsic grants from being established.
            // Keep the finite admission limit: lowering it below live grants
            // is correctly refused. The peak arm alone forces the spill path.
            self.charge.arbitrator.set_peak_rss_for_test(u64::MAX);
            assert!(self.charge.arbitrator.should_spill_self());
        }
    }

    fn governed_record(resources: &AllocationResources, values: Vec<Value>) -> Record {
        let scope = resources.scope().unwrap();
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(
            (0..values.len()).map(|i| format!("c{i}").into()).collect(),
        )));
        let mut owned =
            clinker_record::owned_storage::OwnedValues::try_with_capacity(values.len(), &scope)
                .unwrap();
        for value in values {
            owned.try_push(value, &scope).unwrap();
        }
        let record = Record::from_owned_values(schema, owned).unwrap();
        assert!(record.values_are_accounted_by(resources));
        record
    }

    struct ObserveSpill;
    impl ObserveSpill {
        fn install(
            observer: impl FnMut(SpillTransition, u64, u64, Option<&Path>) + 'static,
        ) -> Self {
            SPILL_OBSERVER.with_borrow_mut(|slot| {
                assert!(slot.is_none());
                *slot = Some(Box::new(observer));
            });
            Self
        }
    }
    impl Drop for ObserveSpill {
        fn drop(&mut self) {
            SPILL_OBSERVER.with_borrow_mut(|slot| *slot = None);
        }
    }

    #[test]
    fn mixed_width_batch_charges_each_actual_owner_and_immediate_consumer() {
        let mut rig = SpillRig::new(false);
        rig.charge.spill_allowed = false;
        let foreign = SpillRig::new(false);
        let resources = rig.charge.allocation_resources();
        let local = governed_record(resources, vec![Value::Integer(1), Value::Integer(2)]);
        let remote = governed_record(
            foreign.charge.allocation_resources(),
            vec![Value::Integer(3); 3],
        );
        let pair = std::mem::size_of::<(Record, SourceRowId)>() as u64;
        let slots = std::mem::size_of::<Value>() as u64;
        let costs = [pair, pair + 3 * slots, pair + slots];
        let mut batch = EventBatch::with_capacity(6);
        batch.push_record(local, 1);
        batch.push_punctuation(Punctuation::document_close(distinct_doc()));
        batch.push_record(remote, 2);
        batch.push_record(rec(3), 3);
        assert_eq!(
            batch.unaccounted_estimated_bytes(resources),
            costs.iter().sum::<u64>()
        );
        assert_eq!(batch.estimated_bytes(), 3 * pair + 6 * slots);
        let local_observer = rig.charge.arbitrator.writer_resource_observer();
        let foreign_observer = foreign.charge.arbitrator.writer_resource_observer();
        assert!(local_observer.usage().memory > 0);
        assert!(foreign_observer.usage().memory > 0);
        let baseline = rig.charge.retain_bytes(317);
        let mut remaining = costs.iter().sum::<u64>();
        let mut index = 0;
        rig.charge
            .charge_and_route(batch, |event| {
                assert_eq!(rig.charge.handle.bytes(), 317 + remaining);
                if let StreamEvent::Record(record, id) = event {
                    let cost = crate::executor::node_buffer::unaccounted_record_byte_cost(
                        &record, resources,
                    );
                    assert_eq!(id.ordinal(), index as u64 + 1);
                    assert_eq!(cost, costs[index]);
                    rig.charge.handle.sub_bytes(cost);
                    remaining -= cost;
                    index += 1;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(index, 3);
        assert_eq!(rig.charge.handle.bytes(), 317);
        assert_eq!(local_observer.usage().memory, 0);
        assert_eq!(foreign_observer.usage().memory, 0);
        drop(baseline);
        assert_eq!(rig.charge.handle.bytes(), 0);
    }

    #[test]
    fn spill_retires_original_run_and_charges_each_legacy_reload() {
        for compress in [false, true] {
            let rig = SpillRig::new(compress);
            let resources = rig.charge.allocation_resources();
            let observer = rig.charge.arbitrator.writer_resource_observer();
            let mut first = governed_record(resources, vec![Value::Integer(7)]);
            let doc = distinct_doc();
            first.set_doc_ctx(doc.clone());
            let pair = std::mem::size_of::<(Record, SourceRowId)>() as u64;
            let full = crate::executor::node_buffer::record_byte_cost(1);
            let mut batch = EventBatch::with_capacity(4);
            batch.push_record(first, 7);
            batch.push_punctuation(Punctuation::document_close(doc.clone()));
            batch.push_record(rec(8), 8);
            let baseline = rig.charge.retain_bytes(211);
            let transitions = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let observed = transitions.clone();
            let intrinsic = observer.clone();
            let _guard = ObserveSpill::install(move |stage, producer, shared, _| {
                observed
                    .borrow_mut()
                    .push((stage, producer, shared, intrinsic.usage().memory));
            });
            rig.force_spill();
            let mut ids = Vec::new();
            rig.charge
                .charge_and_route(batch, |event| {
                    match event {
                        StreamEvent::Record(record, id) => {
                            assert!(!record.values_are_accounted_by(resources));
                            let cost = crate::executor::node_buffer::unaccounted_record_byte_cost(
                                &record, resources,
                            );
                            assert_eq!(cost, full);
                            assert_eq!(
                                record.get("id").or_else(|| record.get("c0")),
                                Some(&Value::Integer(id.ordinal() as i64))
                            );
                            if id.ordinal() == 7 {
                                assert_eq!(record.doc_ctx().id(), doc.id());
                            }
                            let later = if id.ordinal() == 7 { full } else { 0 };
                            assert_eq!(rig.charge.handle.bytes(), 211 + later + full);
                            assert_eq!(observer.usage().memory, 0);
                            rig.charge.handle.sub_bytes(cost);
                            ids.push(id.ordinal());
                        }
                        StreamEvent::Punctuation(p) => {
                            assert_eq!(ids, [7]);
                            assert_eq!(p.doc_id(), doc.id());
                            assert_eq!(rig.charge.handle.bytes(), 211 + full);
                        }
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(ids, [7, 8]);
            let observed = transitions.borrow();
            assert_eq!(observed.len(), 6);
            assert_eq!(observed[0].0, SpillTransition::BeforeWrite);
            assert_eq!(
                (observed[0].1, observed[0].2),
                (pair + full, 211 + pair + full)
            );
            assert!(observed[0].3 > 0);
            assert_eq!(
                observed[1],
                (SpillTransition::AfterWrite, full, 211 + full, 0)
            );
            assert_eq!(observed[4], (SpillTransition::AfterWrite, 0, 211, 0));
            assert_eq!(rig.charge.handle.bytes(), 211);
            assert_eq!(std::fs::read_dir(rig.directory.path()).unwrap().count(), 0);
            drop(baseline);
            assert_eq!(rig.charge.handle.bytes(), 0);
        }
    }

    #[test]
    fn failed_reload_send_preserves_queued_prefix_and_escaped_leaf() {
        for fail_at in [0, 1] {
            let rig = SpillRig::new(false);
            let resources = rig.charge.allocation_resources();
            let observer = rig.charge.arbitrator.writer_resource_observer();
            let leaf = clinker_record::FieldStr::try_new(
                "shared payload retained beyond both spill and failed publication",
                &resources.scope().unwrap(),
            )
            .unwrap();
            let leaf_bytes = observer.usage().memory;
            assert!(leaf_bytes > 0);
            let mut batch = EventBatch::with_capacity(4);
            for i in 0..3 {
                batch.push_record(
                    governed_record(resources, vec![Value::String(leaf.clone())]),
                    i,
                );
            }
            let baseline = rig.charge.retain_bytes(173);
            rig.force_spill();
            let full = crate::executor::node_buffer::record_byte_cost(1);
            let mut queued = Vec::new();
            let result = rig.charge.charge_and_route(batch, |event| {
                assert_eq!(
                    observer.usage().memory,
                    leaf_bytes,
                    "original vectors have dropped but escaped leaf still owns its grant"
                );
                assert_eq!(
                    rig.charge.handle.bytes(),
                    173 + (queued.len() as u64 + 1) * full
                );
                if queued.len() == fail_at {
                    return Err(PipelineError::Interrupted);
                }
                queued.push(event);
                Ok(())
            });
            assert!(matches!(result, Err(PipelineError::Interrupted)));
            assert_eq!(queued.len(), fail_at);
            assert_eq!(rig.charge.handle.bytes(), 173 + fail_at as u64 * full);
            assert_eq!(observer.usage().memory, leaf_bytes);
            for event in queued {
                let StreamEvent::Record(record, _) = event else {
                    unreachable!()
                };
                assert_eq!(record.values(), &[Value::String(leaf.clone())]);
                rig.charge.handle.sub_bytes(
                    crate::executor::node_buffer::unaccounted_record_byte_cost(&record, resources),
                );
            }
            assert_eq!(rig.charge.handle.bytes(), 173);
            assert_eq!(std::fs::read_dir(rig.directory.path()).unwrap().count(), 0);
            drop(leaf);
            assert_eq!(observer.usage().memory, 0);
            drop(baseline);
            assert_eq!(rig.charge.handle.bytes(), 0);
        }
    }

    #[test]
    fn blocked_reload_publication_retains_exact_charge_until_consumed() {
        let rig = SpillRig::new(true);
        let mut batch = EventBatch::with_capacity(1);
        batch.push_record(
            governed_record(rig.charge.allocation_resources(), vec![Value::Integer(42)]),
            42,
        );
        let full = crate::executor::node_buffer::record_byte_cost(1);
        let baseline = rig.charge.retain_bytes(139);
        rig.force_spill();
        let barrier = std::sync::Barrier::new(2);
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);
        std::thread::scope(|threads| {
            let charge = &rig.charge;
            let barrier = &barrier;
            let producer = threads.spawn(move || {
                charge.charge_and_route(batch, |event| {
                    sender.send(event).unwrap();
                    barrier.wait();
                    Ok(())
                })
            });
            let event = receiver.recv().unwrap();
            assert_eq!(charge.handle.bytes(), 139 + full);
            assert_eq!(
                charge.arbitrator.writer_resource_observer().usage().memory,
                0
            );
            let StreamEvent::Record(record, id) = event else {
                unreachable!()
            };
            assert_eq!(id.ordinal(), 42);
            assert!(!record.values_are_accounted_by(charge.allocation_resources()));
            charge.handle.sub_bytes(full);
            drop(record);
            assert_eq!(charge.handle.bytes(), 139);
            barrier.wait();
            producer.join().unwrap().unwrap();
        });
        assert_eq!(rig.charge.handle.bytes(), 139);
        drop(baseline);
        assert_eq!(rig.charge.handle.bytes(), 0);
    }

    #[test]
    fn spill_failures_retire_only_original_and_unsent_ownership() {
        // Creation failure, quota refusal, reader-open failure, decode failure.
        // File mutation happens only in this thread's test observation seam.
        for failure in 0..4 {
            let mut rig = SpillRig::new(false);
            let mut batch = EventBatch::with_capacity(3);
            batch.push_record(
                governed_record(rig.charge.allocation_resources(), vec![Value::Integer(1)]),
                1,
            );
            batch.push_punctuation(Punctuation::document_close(distinct_doc()));
            batch.push_record(
                governed_record(rig.charge.allocation_resources(), vec![Value::Integer(2)]),
                2,
            );
            let observer = rig.charge.arbitrator.writer_resource_observer();
            let baseline = rig.charge.retain_bytes(191);
            if failure == 0 {
                rig.charge.spill_root_path =
                    Arc::from(rig.directory.path().join("missing").as_path());
            }
            if failure == 1 {
                rig.charge.arbitrator.set_max_spill_bytes(1).unwrap();
            }
            let snapshots = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let seen = snapshots.clone();
            let _guard = ObserveSpill::install(move |stage, producer, shared, path| {
                seen.borrow_mut().push((stage, producer, shared));
                if stage == SpillTransition::BeforeRead {
                    if failure == 2 {
                        std::fs::remove_file(path.unwrap()).unwrap();
                    }
                    if failure == 3 {
                        let mut contents = std::fs::read(path.unwrap()).unwrap();
                        let header_end =
                            contents.iter().position(|byte| *byte == b'\n').unwrap() + 1;
                        contents.truncate(header_end);
                        // A complete zero-length frame is a typed decode
                        // failure, whereas EOF alone is a valid empty stream.
                        contents.extend_from_slice(&0_u32.to_le_bytes());
                        std::fs::write(path.unwrap(), contents).unwrap();
                    }
                }
            });
            rig.force_spill();
            let result = rig
                .charge
                .charge_and_route(batch, |_| panic!("failed spill must publish nothing"));
            assert!(result.is_err());
            if failure == 1 {
                assert!(matches!(
                    result,
                    Err(PipelineError::SpillCapExceeded { .. })
                ));
            } else {
                assert!(matches!(result, Err(PipelineError::Spill(_))));
            }
            let pair = std::mem::size_of::<(Record, SourceRowId)>() as u64;
            assert_eq!(
                snapshots.borrow()[0],
                (SpillTransition::BeforeWrite, 2 * pair, 191 + 2 * pair)
            );
            assert_eq!(
                snapshots.borrow()[1],
                (SpillTransition::AfterWrite, pair, 191 + pair)
            );
            assert_eq!(rig.charge.handle.bytes(), 191);
            assert_eq!(observer.usage().memory, 0);
            assert_eq!(std::fs::read_dir(rig.directory.path()).unwrap().count(), 0);
            drop(baseline);
            assert_eq!(rig.charge.handle.bytes(), 0);
        }
    }

    #[test]
    fn later_decode_failure_preserves_published_prefix_ownership() {
        let rig = SpillRig::new(false);
        let resources = rig.charge.allocation_resources();
        let mut batch = EventBatch::with_capacity(4);
        for id in 1..=2 {
            batch.push_record(
                governed_record(resources, vec![Value::Integer(id)]),
                id as u64,
            );
        }
        batch.push_punctuation(Punctuation::document_close(distinct_doc()));
        batch.push_record(governed_record(resources, vec![Value::Integer(3)]), 3);
        let baseline = rig.charge.retain_bytes(227);
        let _guard = ObserveSpill::install(|stage, _, _, path| {
            if stage != SpillTransition::BeforeRead {
                return;
            }
            let path = path.unwrap();
            let mut bytes = std::fs::read(path).unwrap();
            assert_eq!(bytes[0], 0, "uncompressed fixture");
            let first = bytes.iter().position(|byte| *byte == b'\n').unwrap() + 1;
            let length = u32::from_le_bytes(bytes[first..first + 4].try_into().unwrap()) as usize;
            assert_eq!(
                bytes[first + 4],
                0,
                "first frame is a synthetic-context record"
            );
            let second = first + 4 + length;
            assert!(second + 4 < bytes.len());
            assert_eq!(bytes[second + 4], 0);
            // Preserve the real first row and second frame length. Corrupt only
            // the second discriminator, so failure occurs after publication.
            bytes[second + 4] = u8::MAX;
            std::fs::write(path, bytes).unwrap();
        });
        rig.force_spill();
        let pair = std::mem::size_of::<(Record, SourceRowId)>() as u64;
        let full = crate::executor::node_buffer::record_byte_cost(1);
        let mut queued = Vec::new();
        let result = rig.charge.charge_and_route(batch, |event| {
            assert!(queued.is_empty());
            let StreamEvent::Record(record, id) = &event else {
                panic!("later punctuation cannot escape decode failure")
            };
            assert_eq!(id.ordinal(), 1);
            assert_eq!(record.values(), &[Value::Integer(1)]);
            assert_eq!(
                rig.charge.handle.bytes(),
                227 + pair + full,
                "later original run remains producer-owned"
            );
            queued.push(event);
            Ok(())
        });
        assert!(matches!(result, Err(PipelineError::Spill(_))));
        assert_eq!(queued.len(), 1);
        assert_eq!(rig.charge.handle.bytes(), 227 + full);
        assert_eq!(
            rig.charge
                .arbitrator
                .writer_resource_observer()
                .usage()
                .memory,
            0
        );
        drop(queued);
        rig.charge.handle.sub_bytes(full);
        assert_eq!(rig.charge.handle.bytes(), 227);
        assert_eq!(std::fs::read_dir(rig.directory.path()).unwrap().count(), 0);
        drop(baseline);
        assert_eq!(rig.charge.handle.bytes(), 0);
    }

    fn owned_charge() -> StreamingChargeHandle {
        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            1024 * 1024,
            0.8,
            0.7,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        StreamingChargeHandle::new(
            ConsumerHandle::new(),
            arbitrator,
            Arc::from(std::path::Path::new(".")),
            "output".into(),
            false,
            clinker_plan::config::CompressMode::Off,
            2,
            legacy_test_resources(),
        )
    }

    #[test]
    fn output_owner_deltas_preserve_concurrent_sink_subtraction() {
        let charge = owned_charge();
        charge.handle.add_bytes(10_000);
        let mut owner = charge.retain_bytes(100);
        let sink = charge.handle.clone();
        let consumer = std::thread::spawn(move || {
            for _ in 0..10_000 {
                sink.sub_bytes(1);
            }
        });
        for i in 0..10_000 {
            owner.resize(100 + i % 2);
        }
        consumer.join().unwrap();
        assert_eq!(charge.handle.bytes(), owner.bytes());
        drop(owner);
        assert_eq!(charge.handle.bytes(), 0);
    }

    #[test]
    fn output_owned_batch_releases_rows_on_early_drop() {
        let charge = owned_charge();
        let mut batch = EventBatch::with_capacity(3);
        batch.push_record(rec(1), 1u64);
        let owner = charge.retain_bytes(batch.estimated_bytes());
        let batch = charge.own_charged_batch(batch, owner).unwrap();
        assert!(charge.handle.bytes() > 0);
        drop(batch);
        assert_eq!(charge.handle.bytes(), 0);
    }

    #[test]
    fn output_owned_batch_releases_unsent_rows_on_failure() {
        let charge = owned_charge();
        let mut batch = EventBatch::with_capacity(3);
        for i in 0..3 {
            batch.push_record(rec(i), i as u64);
        }
        let bytes = batch.estimated_bytes();
        let mut frontier = charge.retain_bytes(700);
        let mut pending = charge.retain_bytes(0);
        frontier.transfer_row(&mut pending, 200, bytes);
        assert_eq!(charge.handle.bytes(), 200 + bytes);
        let batch = charge.own_charged_batch(batch, pending).unwrap();
        let mut sent = 0;
        let result = charge.route_charged_batch(batch, |event| {
            if sent == 1 {
                return Err(PipelineError::Interrupted);
            }
            let StreamEvent::Record(record, _) = event else {
                unreachable!()
            };
            charge
                .handle
                .sub_bytes(crate::executor::node_buffer::record_byte_cost(
                    record.schema().column_count(),
                ));
            sent += 1;
            Ok(())
        });
        assert!(matches!(result, Err(PipelineError::Interrupted)));
        assert_eq!(
            charge.handle.bytes(),
            200,
            "sent rows belong to Sink; failed and unvisited rows drop with the token"
        );
        drop(frontier);
        assert_eq!(charge.handle.bytes(), 0);
    }

    fn rec(id: i64) -> Record {
        Record::new(
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into()]))),
            vec![Value::Integer(id)],
        )
    }

    /// Build a document context with a fresh, distinct id so two
    /// documents in one test compare unequal (the process-wide synthetic
    /// singleton shares one id and cannot distinguish documents).
    fn distinct_doc() -> SharedStorage<DocumentContext> {
        SharedStorage::from_arc(Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from(""),
            clinker_record::EnvelopeRecord::empty(),
        )))
    }

    /// Sink closure type the test driver hands each batcher. Records
    /// every flushed batch's length and flattens its events so a test
    /// can assert both the per-batch cut points and the flattened order.
    type TestSink = Box<dyn FnMut(EventBatch) -> Result<(), ()>>;

    /// Drive a batcher to completion and return `(flattened events,
    /// per-batch sizes)`. The shared `Mutex` cells let the sink closure
    /// (which the batcher owns) write back into the test's collectors.
    fn collect(
        batch_size: usize,
        push: impl FnOnce(&mut EventBatcher<TestSink, ()>) -> Result<(), ()>,
    ) -> (Vec<StreamEvent>, Vec<usize>) {
        let flat: Arc<std::sync::Mutex<Vec<StreamEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let sizes: Arc<std::sync::Mutex<Vec<usize>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let flat_sink = Arc::clone(&flat);
        let sizes_sink = Arc::clone(&sizes);
        let sink: TestSink = Box::new(move |batch: EventBatch| {
            sizes_sink.lock().unwrap().push(batch.len());
            flat_sink.lock().unwrap().extend(batch.into_events());
            Ok(())
        });
        let mut batcher = EventBatcher::new(batch_size, sink);
        push(&mut batcher).unwrap();
        batcher.finish().unwrap();
        let flat_out = Arc::try_unwrap(flat).unwrap().into_inner().unwrap();
        let sizes_out = Arc::try_unwrap(sizes).unwrap().into_inner().unwrap();
        (flat_out, sizes_out)
    }

    #[test]
    fn flushes_full_batches_and_a_partial_tail() {
        let (flat, sizes) = collect(2, |b| {
            for i in 0..5 {
                b.push_record(rec(i), i as u64)?;
            }
            Ok(())
        });
        // 5 records at batch_size 2 → batches of 2, 2, 1.
        assert_eq!(sizes, vec![2, 2, 1]);
        assert_eq!(flat.len(), 5);
        for (i, ev) in flat.iter().enumerate() {
            match ev {
                StreamEvent::Record(_, rn) => assert_eq!(rn.ordinal(), i as u64),
                StreamEvent::Punctuation(_) => panic!("expected only records"),
            }
        }
    }

    #[test]
    fn trailing_close_stays_after_last_record_across_a_split() {
        let ctx = synthetic_document_context();
        let doc_id = ctx.id();
        // Open, three records, close — at batch_size 2 the document's
        // records split across batches and the close lands in a later
        // batch than the open and the first records.
        let (flat, sizes) = collect(2, |b| {
            b.push_punctuation(Punctuation::document_open(ctx.clone()))?;
            for i in 0..3 {
                b.push_record(rec(i), i as u64)?;
            }
            b.push_punctuation(Punctuation::document_close(ctx.clone()))?;
            Ok(())
        });
        // open, r0 | r1, r2 | close  → batches of 2, 2, 1.
        assert_eq!(sizes, vec![2, 2, 1]);

        // The open precedes every record; the close trails every record.
        let open_pos = flat
            .iter()
            .position(|e| matches!(e, StreamEvent::Punctuation(p) if p.kind() == PunctuationKind::DocumentOpen))
            .unwrap();
        let close_pos = flat
            .iter()
            .position(|e| matches!(e, StreamEvent::Punctuation(p) if p.kind() == PunctuationKind::DocumentClose))
            .unwrap();
        let record_positions: Vec<usize> = flat
            .iter()
            .enumerate()
            .filter_map(|(i, e)| e.is_record().then_some(i))
            .collect();
        assert!(record_positions.iter().all(|&p| open_pos < p));
        assert!(record_positions.iter().all(|&p| close_pos > p));

        // Both punctuations carry the same document identity.
        for e in &flat {
            if let StreamEvent::Punctuation(p) = e {
                assert_eq!(p.doc_id(), doc_id);
            }
        }
    }

    #[test]
    fn two_documents_keep_their_boundaries_separate_across_splits() {
        let doc_a = distinct_doc();
        let doc_b = distinct_doc();
        let (flat, _sizes) = collect(2, |b| {
            b.push_punctuation(Punctuation::document_open(doc_a.clone()))?;
            b.push_record(rec(0), 0)?;
            b.push_record(rec(1), 1)?;
            b.push_punctuation(Punctuation::document_close(doc_a.clone()))?;
            b.push_punctuation(Punctuation::document_open(doc_b.clone()))?;
            b.push_record(rec(2), 2)?;
            b.push_punctuation(Punctuation::document_close(doc_b.clone()))?;
            Ok(())
        });
        // doc_a's close must precede doc_b's open (no interleaving of
        // boundaries across documents).
        let a_close = flat
            .iter()
            .position(|e| {
                matches!(e, StreamEvent::Punctuation(p)
                if p.kind() == PunctuationKind::DocumentClose && p.doc_id() == doc_a.id())
            })
            .unwrap();
        let b_open = flat
            .iter()
            .position(|e| {
                matches!(e, StreamEvent::Punctuation(p)
                if p.kind() == PunctuationKind::DocumentOpen && p.doc_id() == doc_b.id())
            })
            .unwrap();
        assert!(a_close < b_open);
    }

    #[test]
    fn empty_batcher_finish_emits_nothing() {
        let (flat, sizes) = collect(4, |_b| Ok(()));
        assert!(flat.is_empty());
        assert!(sizes.is_empty());
    }

    #[test]
    fn zero_batch_size_clamps_to_one() {
        let (flat, sizes) = collect(0, |b| {
            b.push_record(rec(0), 0)?;
            b.push_record(rec(1), 1)?;
            Ok(())
        });
        // Clamped to 1 → one record per batch.
        assert_eq!(sizes, vec![1, 1]);
        assert_eq!(flat.len(), 2);
    }

    #[test]
    fn estimated_bytes_counts_records_only() {
        let ctx = synthetic_document_context();
        let mut batch = EventBatch::with_capacity(4);
        // Empty batch and punctuation-only batch both cost zero.
        assert_eq!(EventBatch::with_capacity(0).estimated_bytes(), 0);
        batch.push_punctuation(Punctuation::document_open(ctx.clone()));
        assert_eq!(batch.estimated_bytes(), 0, "puncts contribute zero");
        batch.push_record(rec(1), 1);
        batch.push_record(rec(2), 2);
        // Two records of a 1-column schema; punctuations still contribute 0.
        let per_row = crate::executor::node_buffer::record_byte_cost(1);
        assert_eq!(batch.estimated_bytes(), per_row * 2);
    }

    /// The streaming per-batch spill path round-trips a batch's records
    /// through disk while preserving the full records-and-punctuations
    /// interleaving — the leading `DocumentOpen` still precedes the run it
    /// frames and the trailing `DocumentClose` still follows it — and
    /// records the spill bytes against the arbitrator. Forces a
    /// deterministic soft-threshold trip by pinning the arbitrator's limit
    /// and peak RSS so `should_spill()` fires on every platform regardless
    /// of `rss_bytes()` availability.
    #[test]
    fn charge_handle_spills_a_batch_preserving_order_and_close() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};

        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            64 * 1024 * 1024,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        // Force the soft threshold to trip unconditionally. The streaming
        // charge path polls `should_spill_self` (no pausing round), so the
        // precondition asserts that variant.
        arbitrator.set_limit(1).unwrap();
        arbitrator.set_peak_rss_for_test(u64::MAX);
        assert!(arbitrator.should_spill_self());

        let spill_dir = tempfile::tempdir().expect("temp dir");
        let spill_root: Arc<std::path::Path> = Arc::from(spill_dir.path());
        let handle = ConsumerHandle::new();
        let charge = StreamingChargeHandle::new(
            handle.clone(),
            Arc::clone(&arbitrator),
            spill_root,
            "stream-spill-test".to_string(),
            true,
            clinker_plan::config::CompressMode::On,
            2,
            legacy_test_resources(),
        );

        // One document: open, three records, close — at this batch size
        // the whole document is one batch, so the close trails its records
        // even after the records round-trip through the spill file.
        let ctx = synthetic_document_context();
        let mut batch = EventBatch::with_capacity(8);
        batch.push_punctuation(Punctuation::document_open(ctx.clone()));
        for i in 0..3 {
            batch.push_record(rec(i), i as u64);
        }
        batch.push_punctuation(Punctuation::document_close(ctx.clone()));

        let routed: Arc<std::sync::Mutex<Vec<StreamEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let routed_sink = Arc::clone(&routed);
        charge
            .charge_and_route(batch, move |event| {
                routed_sink.lock().unwrap().push(event);
                Ok(())
            })
            .expect("charge_and_route on the spill path");

        let out = Arc::try_unwrap(routed).unwrap().into_inner().unwrap();
        // The spill path emits the same arrival order the in-memory path
        // would: leading open, then the three records (re-read from the
        // spill file in order), then the trailing close. The open frames
        // the run it precedes and the close trails it — the substrate's
        // strict-arrival-order invariant, preserved across the disk
        // round-trip rather than collapsed to records-then-punctuations.
        let record_rns: Vec<u64> = out
            .iter()
            .filter_map(|e| match e {
                StreamEvent::Record(_, rn) => Some(rn.ordinal()),
                StreamEvent::Punctuation(_) => None,
            })
            .collect();
        assert_eq!(
            record_rns,
            vec![0, 1, 2],
            "spill round-trip preserved order"
        );
        let open_pos = out
            .iter()
            .position(|e| matches!(e, StreamEvent::Punctuation(p) if p.kind() == PunctuationKind::DocumentOpen))
            .expect("leading open forwarded at its arrival offset");
        let close_pos = out
            .iter()
            .position(|e| matches!(e, StreamEvent::Punctuation(p) if p.kind() == PunctuationKind::DocumentClose))
            .expect("close forwarded after the spilled records");
        let first_record_pos = out
            .iter()
            .position(|e| e.is_record())
            .expect("records present");
        let last_record_pos = out
            .iter()
            .rposition(|e| e.is_record())
            .expect("records present");
        assert!(
            open_pos < first_record_pos,
            "DocumentOpen must precede the records it frames across the spill round-trip"
        );
        assert!(
            close_pos > last_record_pos,
            "DocumentClose must trail every record across the spill round-trip"
        );

        // The arbitrator recorded the batch's spill to disk.
        assert!(
            arbitrator.cumulative_spill_bytes() > 0,
            "the spilled batch must have recorded spill bytes"
        );
    }

    /// A batch whose records are split into two runs by an interior
    /// `Close`/`Open` pair re-emits each run in place across the spill
    /// path: the two documents stay distinct and every event keeps its
    /// arrival offset, proving the spill path spills record *runs* rather
    /// than collapsing the batch to all-records-then-all-punctuations.
    #[test]
    fn charge_handle_spill_preserves_two_interleaved_document_runs() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};

        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            64 * 1024 * 1024,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        arbitrator.set_limit(1).unwrap();
        arbitrator.set_peak_rss_for_test(u64::MAX);
        assert!(arbitrator.should_spill_self());

        let spill_dir = tempfile::tempdir().expect("temp dir");
        let spill_root: Arc<std::path::Path> = Arc::from(spill_dir.path());
        let handle = ConsumerHandle::new();
        let charge = StreamingChargeHandle::new(
            handle.clone(),
            Arc::clone(&arbitrator),
            spill_root,
            "stream-spill-two-docs".to_string(),
            true,
            clinker_plan::config::CompressMode::On,
            2,
            legacy_test_resources(),
        );

        // [Open(A), r0, r1, Close(A), Open(B), r2, Close(B)] — two record
        // runs separated by the A-close / B-open boundary.
        let doc_a = distinct_doc();
        let doc_b = distinct_doc();
        let mut batch = EventBatch::with_capacity(8);
        batch.push_punctuation(Punctuation::document_open(doc_a.clone()));
        batch.push_record(rec(0), 0);
        batch.push_record(rec(1), 1);
        batch.push_punctuation(Punctuation::document_close(doc_a.clone()));
        batch.push_punctuation(Punctuation::document_open(doc_b.clone()));
        batch.push_record(rec(2), 2);
        batch.push_punctuation(Punctuation::document_close(doc_b.clone()));

        let routed: Arc<std::sync::Mutex<Vec<StreamEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let routed_sink = Arc::clone(&routed);
        charge
            .charge_and_route(batch, move |event| {
                routed_sink.lock().unwrap().push(event);
                Ok(())
            })
            .expect("charge_and_route on the spill path");

        let out = Arc::try_unwrap(routed).unwrap().into_inner().unwrap();
        // Flatten to a comparable shape: punctuations as (kind, doc_id),
        // records as their row number, then assert the exact arrival order.
        #[derive(Debug, PartialEq)]
        enum Shape {
            Open(DocumentId),
            Close(DocumentId),
            Rec(u64),
        }
        let shapes: Vec<Shape> = out
            .iter()
            .map(|e| match e {
                StreamEvent::Record(_, rn) => Shape::Rec(rn.ordinal()),
                StreamEvent::Punctuation(p) => match p.kind() {
                    PunctuationKind::DocumentOpen => Shape::Open(p.doc_id()),
                    PunctuationKind::DocumentClose => Shape::Close(p.doc_id()),
                },
            })
            .collect();
        assert_eq!(
            shapes,
            vec![
                Shape::Open(doc_a.id()),
                Shape::Rec(0),
                Shape::Rec(1),
                Shape::Close(doc_a.id()),
                Shape::Open(doc_b.id()),
                Shape::Rec(2),
                Shape::Close(doc_b.id()),
            ],
            "both record runs re-emit in place with their framing punctuations"
        );
    }

    /// A streaming spill whose file crosses the arbitrator's disk quota
    /// aborts the run with the structured `SpillCapExceeded` (E320) surface
    /// instead of continuing to fill the disk. Pins the soft threshold so the
    /// batch takes the spill path, then sets the disk cap to one byte so the
    /// spilled run's file overflows on its first write. The overflow is a
    /// disk-cap abort, never masqueraded as an out-of-memory E310.
    #[test]
    fn streaming_spill_past_disk_cap_aborts_with_e320() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy, assert_spill_cap_overflow};

        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            64 * 1024 * 1024,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        // Force the streaming charge path onto the spill branch (the charge
        // path polls `should_spill_self`, no pausing round)...
        arbitrator.set_limit(1).unwrap();
        arbitrator.set_peak_rss_for_test(u64::MAX);
        assert!(arbitrator.should_spill_self());
        // ...then choke the disk quota so the first spilled run overflows.
        arbitrator.set_max_spill_bytes(1).unwrap();

        let spill_dir = tempfile::tempdir().expect("temp dir");
        let spill_root: Arc<std::path::Path> = Arc::from(spill_dir.path());
        let handle = ConsumerHandle::new();
        let charge = StreamingChargeHandle::new(
            handle.clone(),
            Arc::clone(&arbitrator),
            spill_root,
            "stream-spill-cap".to_string(),
            true,
            clinker_plan::config::CompressMode::On,
            2,
            legacy_test_resources(),
        );

        // A records-only batch spills as a single run; that run's file
        // crosses the one-byte cap.
        let mut batch = EventBatch::with_capacity(4);
        for i in 0..3 {
            batch.push_record(rec(i), i as u64);
        }

        let routed: Arc<std::sync::Mutex<Vec<StreamEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let routed_sink = Arc::clone(&routed);
        let result = charge.charge_and_route(batch, move |event| {
            routed_sink.lock().unwrap().push(event);
            Ok(())
        });

        assert_spill_cap_overflow(result, &arbitrator, "stream-spill-cap", 1, spill_dir.path());

        // The aborting run forwards nothing downstream: the overflow fires
        // before any spilled record is re-read from disk and sent.
        let out = Arc::try_unwrap(routed).unwrap().into_inner().unwrap();
        assert!(
            out.is_empty(),
            "no records escape downstream once the disk cap trips"
        );
    }

    #[test]
    fn batch_len_counts_records_and_punctuations() {
        let ctx = synthetic_document_context();
        let mut batch = EventBatch::with_capacity(4);
        batch.push_punctuation(Punctuation::document_open(ctx.clone()));
        batch.push_record(rec(0), 0);
        batch.push_record(rec(1), 1);
        batch.push_punctuation(Punctuation::document_close(ctx));
        assert_eq!(batch.len(), 4);
        let events = batch.into_events();
        assert_eq!(events.iter().filter(|e| e.is_record()).count(), 2);
    }
}
