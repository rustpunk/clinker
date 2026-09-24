//! Per-physical-file source-order verification and repair.
//!
//! A declared source order is a promise about records inside each physical
//! file. This barrier holds the complete file behind its outer document
//! boundary, records the first adjacent inversion, and releases only after the
//! file is verified or stably repaired. The record spool is the shared
//! [`SortBuffer`]; forced spill drains through the shared
//! [`SortedRunMerger`]. No source-local replay is involved.

use clinker_record::owned_storage::{AllocationResources, SharedStorage};
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clinker_plan::config::{OnUnsorted, SortField, SortableEventShape};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::PlanNodeId;
use clinker_plan::plan::execution::{CompiledSourceOrder, OrderScope, ResolvedSortField};
use clinker_record::{DocumentContext, Record, Value};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::executor::source_stream::{
    AttemptPopulationDelta, AttemptPopulationId, SourceAttemptEvent, SourceStreamError,
    SourceStreamEvent,
};
use crate::executor::stream_event::{Punctuation, PunctuationKind, SourceRowId};
use crate::pipeline::memory::{ConsumerHandle, MemoryArbitrator};
use crate::pipeline::sort_buffer::{HeapBytes, SortBuffer, SortedOutput};
use crate::pipeline::sort_key::compare_authored_values_with_nulls;
use crate::pipeline::spill::SpillFile;
use crate::pipeline::spill::SpillWriter;
use crate::pipeline::spill_merge::{MergeBudget, SortedRunMerger};

/// Runtime-ready order declaration for one Source.
#[derive(Debug, Clone)]
pub(crate) struct SourceOrderConfig {
    source_id: PlanNodeId,
    source_name: Arc<str>,
    fields: Vec<ResolvedSortField>,
    sort_fields: Vec<SortField>,
    on_unsorted: OnUnsorted,
    shape: SortableEventShape,
}

impl SourceOrderConfig {
    /// Consume a finalized compiled source-order proof and fail closed if its
    /// stable identity or typed field positions no longer match the bound
    /// source schema. This validation runs before the ingest thread starts.
    pub(crate) fn from_compiled(
        order: &CompiledSourceOrder,
        expected_source_id: PlanNodeId,
        expected_source_name: &str,
        schema: &clinker_format::SourceSchema,
    ) -> Result<Self, PipelineError> {
        let invariant = |detail: String| PipelineError::Internal {
            op: "source-order-contract",
            node: expected_source_name.to_string(),
            detail,
        };
        if order.source_id != expected_source_id || order.source_name != expected_source_name {
            return Err(invariant(format!(
                "compiled source order names id {:?} / source '{}', but ingest resolved id {:?} / source '{}'",
                order.source_id, order.source_name, expected_source_id, expected_source_name,
            )));
        }
        if order.scope != OrderScope::PerPhysicalFile {
            return Err(invariant(format!(
                "compiled source order has unsupported scope {:?}; source verification requires per-physical-file scope",
                order.scope,
            )));
        }
        if order.fields.is_empty() {
            return Err(invariant(
                "compiled source order contains no authored fields".to_string(),
            ));
        }
        let columns = schema.bound_columns().ok_or_else(|| {
            invariant("compiled source order has no concrete bound source schema".to_string())
        })?;
        for field in &order.fields {
            let Some(column) = columns.get(field.field_index) else {
                return Err(invariant(format!(
                    "compiled order field '{}' points to index {}, but the bound schema has {} columns",
                    field.field,
                    field.field_index,
                    columns.len(),
                )));
            };
            let bound_type = column.bound_type();
            if column.name != field.field || bound_type != field.value_type {
                return Err(invariant(format!(
                    "compiled order field '{}' at index {} has type {:?}, but the bound schema contains '{}' with type {:?}",
                    field.field, field.field_index, field.value_type, column.name, bound_type,
                )));
            }
        }

        let sort_fields = order
            .fields
            .iter()
            .map(|field| SortField {
                field: field.field.clone(),
                order: field.order,
                null_order: Some(field.null_order),
            })
            .collect();
        Ok(Self {
            source_id: order.source_id,
            source_name: Arc::from(order.source_name.as_str()),
            fields: order.fields.clone(),
            sort_fields,
            on_unsorted: order.on_unsorted,
            shape: order.shape,
        })
    }
}

/// First adjacent authored-key inversion in one physical file.
#[derive(Debug, Clone)]
pub(crate) struct FirstInversion {
    previous_row: u64,
    current_row: u64,
    previous_key: String,
    current_key: String,
}

/// Spill payload for a rejected attempt. The original record travels through
/// the shared spill envelope; this sidecar retains the remaining attribution
/// without consulting authored sort fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagedSourceRejection {
    source_row: SourceRowId,
    row: u64,
    source_name: String,
    source_file: String,
    kind: crate::executor::SourceRejectionKind,
    message: String,
    triggering_field: Box<str>,
    triggering_value: Value,
    /// The reader's failure stamp, split so the spill payload needs no
    /// UUID serializer: a rejection held or spilled here still reports the
    /// moment the reader rejected it.
    failed_at_id: u128,
    failed_at: chrono::DateTime<chrono::Utc>,
}

impl StagedSourceRejection {
    fn from_event(event: crate::executor::SourceRejectionEvent) -> (Record, Self) {
        let crate::executor::SourceRejectionEvent {
            source_row,
            source_name,
            source_file,
            row,
            kind,
            message,
            original_record,
            triggering_field,
            triggering_value,
            failed_at,
        } = event;
        (
            original_record,
            Self {
                source_row,
                row,
                source_name: source_name.to_string(),
                source_file: source_file.to_string(),
                kind,
                message,
                triggering_field,
                triggering_value,
                failed_at_id: failed_at.id().as_u128(),
                failed_at: failed_at.at(),
            },
        )
    }

    fn into_event(self, original_record: Record) -> crate::executor::SourceRejectionEvent {
        crate::executor::SourceRejectionEvent {
            source_row: self.source_row,
            source_name: Arc::from(self.source_name),
            source_file: Arc::from(self.source_file),
            row: self.row,
            kind: self.kind,
            message: self.message,
            original_record,
            triggering_field: self.triggering_field,
            triggering_value: self.triggering_value,
            failed_at: crate::executor::DlqFailureStamp::from_parts(
                uuid::Uuid::from_u128(self.failed_at_id),
                self.failed_at,
            ),
        }
    }
}

impl PartialEq for StagedSourceRejection {
    fn eq(&self, other: &Self) -> bool {
        self.source_row == other.source_row
    }
}

impl Eq for StagedSourceRejection {}

impl PartialOrd for StagedSourceRejection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StagedSourceRejection {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source_row.cmp(&other.source_row)
    }
}

impl HeapBytes for StagedSourceRejection {
    fn unaccounted_heap_bytes(
        &self,
        resources: &clinker_record::owned_storage::AllocationResources,
    ) -> usize {
        self.source_name
            .len()
            .saturating_add(self.source_file.len())
            .saturating_add(self.message.len())
            .saturating_add(self.triggering_field.len())
            .saturating_add(self.triggering_value.unaccounted_heap_size(resources))
    }

    fn heap_bytes(&self) -> usize {
        self.source_name
            .len()
            .saturating_add(self.source_file.len())
            .saturating_add(self.message.len())
            .saturating_add(self.triggering_field.len())
            .saturating_add(self.triggering_value.heap_size())
    }
}

enum PreparedOutput<P> {
    Empty,
    InMemory(Vec<(Record, P)>),
    Spilled { file: SpillFile<P>, bytes: u64 },
}

/// Adjacent-comparison state while a file is staged.
#[derive(Debug)]
pub(crate) enum OrderVerificationState {
    SortedSoFar,
    Unsorted(FirstInversion),
}

/// Bounded event representation for one physical file.
///
/// Punctuation is statically bounded by [`SortableEventShape`]. Records live
/// in the shared spillable sorter, never in an unarbitrated side collection.
pub(crate) struct SourceFileEventSpool {
    population_id: AttemptPopulationId,
    file: Arc<str>,
    leading: Vec<Punctuation>,
    trailing: Vec<Punctuation>,
    records: Option<SortBuffer<SourceRowId>>,
    errors: Option<SortBuffer<StagedSourceRejection>>,
    previous: Option<Record>,
    previous_row: Option<u64>,
    original_doc_ctx: Option<SharedStorage<DocumentContext>>,
    verification: OrderVerificationState,
    row_count: u64,
    attempted_count: u64,
    rejected_count: u64,
    depth: usize,
    nested_opens: usize,
    nested_closes: usize,
}

impl SourceFileEventSpool {
    fn new(open: Punctuation, source: PlanNodeId) -> Self {
        Self {
            population_id: AttemptPopulationId {
                source,
                document: open.doc_ctx().id(),
            },
            file: Arc::clone(open.source_file()),
            leading: vec![open],
            trailing: Vec::with_capacity(2),
            records: None,
            errors: None,
            previous: None,
            previous_row: None,
            original_doc_ctx: None,
            verification: OrderVerificationState::SortedSoFar,
            row_count: 0,
            attempted_count: 0,
            rejected_count: 0,
            depth: 1,
            nested_opens: 0,
            nested_closes: 0,
        }
    }
}

/// Verdict metadata retained for metrics/tests without retaining file rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrderRepairOutcome {
    pub(crate) repaired: bool,
    pub(crate) rows: u64,
    pub(crate) spilled: bool,
}

/// Run-scoped per-source barrier; at most one physical file is open for a
/// synchronous `RecordSource`, while different sources own independent state.
pub(crate) struct SourceFileOrderBarrier {
    config: SourceOrderConfig,
    allocation_resources: clinker_record::owned_storage::AllocationResources,
    state: Option<SourceFileEventSpool>,
    tx: crossbeam_channel::Sender<SourceStreamEvent>,
    consumer_handle: Arc<ConsumerHandle>,
    memory: Arc<MemoryArbitrator>,
    spill_dir: PathBuf,
    spill_compress: bool,
    record_stage: String,
    error_stage: String,
    queued_bytes_ewma: u64,
    /// Full physical record/payload estimate for independent spill decoding.
    reload_bytes_ewma: u64,
    /// Resident records being transferred from an in-memory sorted spool into
    /// the bounded channel. Decrements only after ownership moves to `tx`.
    releasing_memory_bytes: u64,
    /// Fixed bounded buffers held by spill readers/merge cursors/writers while
    /// a spilled file is validated and released.
    fixed_memory_bytes: u64,
    #[cfg(test)]
    emitted_warnings: u64,
}

impl SourceFileOrderBarrier {
    #[cfg(test)]
    pub(crate) fn allocation_identity(&self) -> usize {
        self.allocation_resources.identity()
    }

    pub(crate) fn new(
        config: SourceOrderConfig,
        tx: crossbeam_channel::Sender<SourceStreamEvent>,
        consumer_handle: Arc<ConsumerHandle>,
        memory: Arc<MemoryArbitrator>,
        spill_dir: PathBuf,
        spill_compress: bool,
        allocation_resources: clinker_record::owned_storage::AllocationResources,
    ) -> Self {
        let record_stage = format!("source-order:{}:records", config.source_name);
        let error_stage = format!("source-order:{}:errors", config.source_name);
        Self {
            config,
            allocation_resources,
            state: None,
            tx,
            consumer_handle,
            memory,
            spill_dir,
            spill_compress,
            record_stage,
            error_stage,
            queued_bytes_ewma: 0,
            reload_bytes_ewma: 0,
            releasing_memory_bytes: 0,
            fixed_memory_bytes: 0,
            #[cfg(test)]
            emitted_warnings: 0,
        }
    }

    /// Begin one physical file at its unchanged outer `DocumentOpen`.
    pub(crate) fn begin_physical_file(
        &mut self,
        open: Punctuation,
    ) -> Result<(), SourceStreamError> {
        if self.state.is_some() {
            return Err(self.shape_error(
                open.source_file(),
                "a second physical file opened before the prior file closed",
            ));
        }
        self.state = Some(SourceFileEventSpool::new(open, self.config.source_id));
        self.update_staged_charge();
        Ok(())
    }

    /// Stage one decoded attempt. Invalid values never enter the authored-key
    /// comparator; their sidecar spool orders only by [`SourceRowId`].
    pub(crate) fn observe_attempt(
        &mut self,
        event: SourceAttemptEvent,
    ) -> Result<(), SourceStreamError> {
        match event {
            SourceAttemptEvent::Record(record, row_id) => self.observe_typed_event(record, row_id),
            SourceAttemptEvent::Rejection(event) => self.observe_rejection(*event),
        }
    }

    /// Stage one strictly-coerced record and its already-minted identity.
    pub(crate) fn observe_typed_event(
        &mut self,
        record: Record,
        row_id: SourceRowId,
    ) -> Result<(), SourceStreamError> {
        if row_id.source() != self.config.source_id {
            return Err(SourceStreamError::OrderViolation(Box::new(
                PipelineError::Internal {
                    op: "source-order-contract",
                    node: self.config.source_name.to_string(),
                    detail: format!(
                        "source-order barrier for {:?} received row identity from {:?}",
                        self.config.source_id,
                        row_id.source(),
                    ),
                },
            )));
        }
        let record_bytes = record_pair_bytes(&record);
        self.reload_bytes_ewma = ewma_step(self.reload_bytes_ewma, record_bytes);
        self.queued_bytes_ewma = ewma_step(
            self.queued_bytes_ewma,
            unaccounted_record_pair_bytes(&record, &self.allocation_resources),
        );
        let Some(state) = self.state.as_mut() else {
            return Err(self.shape_error_for_file(
                "<unknown>",
                "a record arrived outside a physical-file open/close pair",
            ));
        };
        if !state.trailing.is_empty() {
            let file = Arc::clone(&state.file);
            return Err(self.shape_error(
                &file,
                "a record arrived after the file's sortable frame closed",
            ));
        }

        let current_row = row_id.ordinal();
        if let Some(previous) = state.previous.as_ref()
            && compare_compiled_keys(previous, &record, &self.config.fields) == Ordering::Greater
            && matches!(state.verification, OrderVerificationState::SortedSoFar)
        {
            let inversion = FirstInversion {
                previous_row: state.previous_row.unwrap_or(current_row),
                current_row,
                previous_key: render_key(previous, &self.config.fields),
                current_key: render_key(&record, &self.config.fields),
            };
            if self.config.on_unsorted == OnUnsorted::Error {
                let file = Arc::clone(&state.file);
                let error = self.order_error(&file, &inversion);
                self.abort_file_barrier();
                return Err(error);
            }
            state.verification = OrderVerificationState::Unsorted(inversion);
        }

        match state.original_doc_ctx.as_ref() {
            None => state.original_doc_ctx = Some(record.doc_ctx().clone()),
            Some(original) if !SharedStorage::ptr_eq(original, record.doc_ctx()) => {
                let file = Arc::clone(&state.file);
                self.abort_file_barrier();
                return Err(self.shape_error(
                    &file,
                    "records inside one sortable frame carried different document contexts",
                ));
            }
            Some(_) => {}
        }

        if state.records.is_none() {
            let threshold = usize::try_from(self.memory.spill_threshold_bytes())
                .unwrap_or(usize::MAX)
                .max(1);
            state.records = Some(SortBuffer::new(
                self.config.sort_fields.clone(),
                threshold,
                Some(self.spill_dir.clone()),
                self.spill_compress,
                record.schema().clone(),
                self.allocation_resources.clone(),
            ));
        }
        state.previous = Some(record.clone());
        state.previous_row = Some(current_row);
        state.row_count = state.row_count.saturating_add(1);
        state.attempted_count = state.attempted_count.saturating_add(1);
        state
            .records
            .as_mut()
            .expect("record buffer initialized above")
            .push(record, row_id);
        let buffer_should_spill = state.records.as_ref().is_some_and(SortBuffer::should_spill)
            || state.errors.as_ref().is_some_and(SortBuffer::should_spill);
        self.update_staged_charge();

        let should_spill = buffer_should_spill
            || self.physical_staged_bytes() >= self.memory.spill_threshold_bytes()
            || self.memory.should_spill_self()
            || self.consumer_handle.take_spill_request();
        if should_spill {
            self.spill_resident_attempts()?;
        }
        Ok(())
    }

    fn observe_rejection(
        &mut self,
        event: crate::executor::SourceRejectionEvent,
    ) -> Result<(), SourceStreamError> {
        if event.source_row.source() != self.config.source_id {
            return Err(SourceStreamError::OrderViolation(Box::new(
                PipelineError::Internal {
                    op: "source-order-contract",
                    node: self.config.source_name.to_string(),
                    detail: format!(
                        "source-order barrier for {:?} received rejected identity from {:?}",
                        self.config.source_id,
                        event.source_row.source(),
                    ),
                },
            )));
        }
        let (record, payload) = StagedSourceRejection::from_event(event);
        let record_bytes = rejection_pair_bytes(&record, &payload);
        self.reload_bytes_ewma = ewma_step(self.reload_bytes_ewma, record_bytes);
        self.queued_bytes_ewma = ewma_step(
            self.queued_bytes_ewma,
            unaccounted_rejection_pair_bytes(&record, &payload, &self.allocation_resources),
        );
        let Some(state) = self.state.as_mut() else {
            return Err(self.shape_error_for_file(
                "<unknown>",
                "a rejected attempt arrived outside a physical-file open/close pair",
            ));
        };
        if !state.trailing.is_empty() {
            let file = Arc::clone(&state.file);
            return Err(self.shape_error(
                &file,
                "a rejected attempt arrived after the file's sortable frame closed",
            ));
        }
        match state.original_doc_ctx.as_ref() {
            None => state.original_doc_ctx = Some(record.doc_ctx().clone()),
            Some(original) if !SharedStorage::ptr_eq(original, record.doc_ctx()) => {
                let file = Arc::clone(&state.file);
                self.abort_file_barrier();
                return Err(self.shape_error(
                    &file,
                    "attempts inside one sortable frame carried different document contexts",
                ));
            }
            Some(_) => {}
        }
        if state.errors.is_none() {
            let threshold = usize::try_from(self.memory.spill_threshold_bytes())
                .unwrap_or(usize::MAX)
                .max(1);
            state.errors = Some(SortBuffer::new_payload_ordered(
                threshold,
                Some(self.spill_dir.clone()),
                self.spill_compress,
                record.schema().clone(),
                self.allocation_resources.clone(),
            ));
        }
        state.attempted_count = state.attempted_count.saturating_add(1);
        if payload.kind.counts_as_type_error() {
            state.rejected_count = state.rejected_count.saturating_add(1);
        }
        state
            .errors
            .as_mut()
            .expect("error buffer initialized above")
            .push(record, payload);
        let buffer_should_spill = state.errors.as_ref().is_some_and(SortBuffer::should_spill)
            || state.records.as_ref().is_some_and(SortBuffer::should_spill);
        self.update_staged_charge();
        if buffer_should_spill
            || self.physical_staged_bytes() >= self.memory.spill_threshold_bytes()
            || self.memory.should_spill_self()
            || self.consumer_handle.take_spill_request()
        {
            self.spill_resident_attempts()?;
        }
        Ok(())
    }

    /// Observe one unchanged punctuation. The outer close completes the file
    /// and triggers verified release; all other boundaries remain staged.
    pub(crate) fn observe_punctuation(
        &mut self,
        punct: Punctuation,
    ) -> Result<Option<OrderRepairOutcome>, SourceStreamError> {
        match punct.kind() {
            PunctuationKind::DocumentOpen => {
                if self.state.is_none() {
                    self.begin_physical_file(punct)?;
                    return Ok(None);
                }
                let state = self.state.as_mut().expect("checked above");
                if state.attempted_count != 0 || !state.trailing.is_empty() {
                    let file = Arc::clone(&state.file);
                    self.abort_file_barrier();
                    return Err(self
                        .shape_error(&file, "a nested frame opened after sortable records began"));
                }
                state.depth += 1;
                state.nested_opens += 1;
                if state.nested_opens > 1 {
                    let file = Arc::clone(&state.file);
                    self.abort_file_barrier();
                    return Err(self.shape_error(
                        &file,
                        "more than one nested frame opened in a physical file",
                    ));
                }
                state.leading.push(punct);
                Ok(None)
            }
            PunctuationKind::DocumentClose => {
                let Some(state) = self.state.as_mut() else {
                    return Err(self.shape_error(
                        punct.source_file(),
                        "a document close arrived without a matching physical-file open",
                    ));
                };
                if state.depth == 0 {
                    let file = Arc::clone(&state.file);
                    self.abort_file_barrier();
                    return Err(self.shape_error(&file, "document framing closed more than once"));
                }
                if state.depth > 1 {
                    state.nested_closes += 1;
                }
                state.trailing.push(punct);
                state.depth -= 1;
                if state.depth == 0 {
                    self.finish_verified_file().map(Some)
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Finish a fully staged file, validating its shape before any release.
    pub(crate) fn finish_verified_file(&mut self) -> Result<OrderRepairOutcome, SourceStreamError> {
        let result = self.finish_verified_file_inner();
        if result.is_err() {
            // `finish_verified_file_inner` takes ownership of the staged
            // state before it performs fallible merge, validation, and
            // downstream sends. Balance both accounting domains here so
            // every error path has the same cleanup boundary.
            self.cleanup_after_failure();
        }
        result
    }

    fn finish_verified_file_inner(&mut self) -> Result<OrderRepairOutcome, SourceStreamError> {
        let Some(mut state) = self.state.take() else {
            return Err(self.shape_error_for_file(
                "<unknown>",
                "physical-file close had no active barrier state",
            ));
        };
        if let Err(detail) = validate_runtime_shape(&state, self.config.shape) {
            let file = Arc::clone(&state.file);
            return Err(self.shape_error(&file, detail));
        }

        let repaired = matches!(state.verification, OrderVerificationState::Unsorted(_));
        let inversion = match &state.verification {
            OrderVerificationState::SortedSoFar => None,
            OrderVerificationState::Unsorted(inversion) => Some(inversion.clone()),
        };
        let rows = state.attempted_count;
        state.previous = None;
        state.previous_row = None;
        // `state` is detached from `self`, but both sorters remain resident
        // during preparation. Carry their contribution across fallible merges.
        self.releasing_memory_bytes = state
            .records
            .as_ref()
            .map_or(0, |buffer| buffer.unaccounted_bytes_used() as u64)
            .saturating_add(
                state
                    .errors
                    .as_ref()
                    .map_or(0, |buffer| buffer.unaccounted_bytes_used() as u64),
            );
        self.update_runtime_charge();
        let records = self.prepare_records(state.records.take())?;
        let errors = self.prepare_errors(state.errors.take())?;
        let spilled = matches!(&records, PreparedOutput::Spilled { .. })
            || matches!(&errors, PreparedOutput::Spilled { .. });
        self.releasing_memory_bytes = resident_record_bytes(&records, &self.allocation_resources)
            .saturating_add(resident_error_bytes(&errors, &self.allocation_resources));
        self.update_runtime_charge();

        if let Some(inversion) = inversion.as_ref() {
            self.emit_unsorted_warning(&state.file, inversion);
        }
        self.emit_population(AttemptPopulationDelta {
            id: state.population_id,
            source_name: Arc::clone(&self.config.source_name),
            attempted: state.attempted_count,
            rejected: state.rejected_count,
        })?;
        // Rejections are released first, in SourceRowId order. FailFast or a
        // missing DLQ therefore fails before any valid record can reach a
        // downstream effect, while successful rows retain authored-key order.
        self.emit_errors(errors, state.population_id, state.original_doc_ctx.as_ref())?;
        self.emit_punctuations(state.leading)?;
        self.emit_records(
            records,
            state.population_id,
            state.original_doc_ctx.as_ref(),
        )?;
        self.emit_punctuations(state.trailing)?;
        self.update_runtime_charge();
        Ok(OrderRepairOutcome {
            repaired,
            rows,
            spilled,
        })
    }

    fn prepare_records(
        &mut self,
        buffer: Option<SortBuffer<SourceRowId>>,
    ) -> Result<PreparedOutput<SourceRowId>, SourceStreamError> {
        let Some(buffer) = buffer else {
            return Ok(PreparedOutput::Empty);
        };
        let expected = buffer.total_rows();
        let resident_bytes = buffer.unaccounted_bytes_used() as u64;
        let (output, residue_bytes) = buffer.finish().map_err(|error| {
            SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
        })?;
        let stage = self.record_stage.clone();
        self.charge_spill_for(&stage, residue_bytes)?;
        match output {
            SortedOutput::InMemory(records) => Ok(PreparedOutput::InMemory(records)),
            SortedOutput::Spilled(files) => {
                self.releasing_memory_bytes =
                    self.releasing_memory_bytes.saturating_sub(resident_bytes);
                self.update_runtime_charge();
                let (file, bytes) = self.consolidate_runs(files, expected, &stage, false)?;
                Ok(PreparedOutput::Spilled { file, bytes })
            }
        }
    }

    fn prepare_errors(
        &mut self,
        buffer: Option<SortBuffer<StagedSourceRejection>>,
    ) -> Result<PreparedOutput<StagedSourceRejection>, SourceStreamError> {
        let Some(buffer) = buffer else {
            return Ok(PreparedOutput::Empty);
        };
        let expected = buffer.total_rows();
        let resident_bytes = buffer.unaccounted_bytes_used() as u64;
        let (output, residue_bytes) = buffer.finish().map_err(|error| {
            SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
        })?;
        let stage = self.error_stage.clone();
        self.charge_spill_for(&stage, residue_bytes)?;
        match output {
            SortedOutput::InMemory(errors) => Ok(PreparedOutput::InMemory(errors)),
            SortedOutput::Spilled(files) => {
                self.releasing_memory_bytes =
                    self.releasing_memory_bytes.saturating_sub(resident_bytes);
                self.update_runtime_charge();
                let (file, bytes) = self.consolidate_runs(files, expected, &stage, true)?;
                Ok(PreparedOutput::Spilled { file, bytes })
            }
        }
    }

    /// Merge spilled runs into one validated, sequentially re-readable spool
    /// before the population decision or any payload is released.
    fn consolidate_runs<P>(
        &mut self,
        files: Vec<SpillFile<P>>,
        expected_rows: usize,
        stage: &str,
        payload_ordered: bool,
    ) -> Result<(SpillFile<P>, u64), SourceStreamError>
    where
        P: Serialize + DeserializeOwned + Ord,
    {
        let schema = files
            .first()
            .map(|file| file.schema().clone())
            .ok_or_else(|| {
                self.shape_error_for_file("<unknown>", "spill-backed attempt spool was empty")
            })?;
        // Opening the merger materializes independent decoded cursors. Their
        // forecast must remain physical even when the original rows were owned.
        self.fixed_memory_bytes = merger_reader_bytes(files.len(), self.reload_bytes_ewma)
            .saturating_add(SPILL_IO_BUFFER_BYTES);
        self.update_runtime_charge();
        let budget = MergeBudget {
            budget: &self.memory,
            node: stage,
            compress: self.spill_compress,
            charge_owner: None,
        };
        let merger = if payload_ordered {
            SortedRunMerger::new_payload_ordered(files, "source attempt barrier", budget)
        } else {
            SortedRunMerger::new(
                files,
                &self.config.sort_fields,
                "source order barrier",
                budget,
            )
        }
        .map_err(|error| SourceStreamError::OrderViolation(Box::new(error)))?;
        self.fixed_memory_bytes =
            merger_reader_bytes(merger.reader_count(), self.reload_bytes_ewma)
                .saturating_add(SPILL_IO_BUFFER_BYTES);
        self.update_runtime_charge();
        let input_charge = self.stage_spill_charge(stage);
        let mut final_writer =
            SpillWriter::<P>::new(schema, Some(&self.spill_dir), self.spill_compress).map_err(
                |error| SourceStreamError::OrderViolation(Box::new(PipelineError::from(error))),
            )?;
        for item in merger {
            let (record, payload) =
                item.map_err(|error| SourceStreamError::OrderViolation(Box::new(error)))?;
            final_writer
                .write_pair(&record, &payload)
                .map_err(|error| {
                    SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
                })?;
        }
        self.fixed_memory_bytes = SPILL_IO_BUFFER_BYTES;
        self.update_runtime_charge();
        let (final_file, final_bytes) = final_writer.finish_with_bytes().map_err(|error| {
            SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
        })?;
        self.fixed_memory_bytes = 0;
        self.update_runtime_charge();
        self.charge_spill_for(stage, final_bytes)?;
        self.memory.release_spill_bytes(stage, input_charge);

        self.fixed_memory_bytes = spill_reader_bytes(self.reload_bytes_ewma);
        self.update_runtime_charge();
        let mut validated_rows = 0usize;
        for item in final_file.reader().map_err(|error| {
            SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
        })? {
            item.map_err(|error| {
                SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
            })?;
            validated_rows += 1;
        }
        self.fixed_memory_bytes = 0;
        self.update_runtime_charge();
        if validated_rows != expected_rows {
            return Err(SourceStreamError::OrderViolation(Box::new(
                PipelineError::Internal {
                    op: "source-order-barrier",
                    node: self.config.source_name.to_string(),
                    detail: format!(
                        "validated attempt spill count changed from {expected_rows} to {validated_rows}"
                    ),
                },
            )));
        }
        Ok((final_file, final_bytes))
    }

    /// Drop all staged evidence and balance every memory/disk charge.
    pub(crate) fn abort_file_barrier(&mut self) {
        self.state.take();
        self.release_all_spill_charge();
        self.releasing_memory_bytes = 0;
        self.fixed_memory_bytes = 0;
        self.update_accounted_charge();
    }

    fn spill_resident_attempts(&mut self) -> Result<(), SourceStreamError> {
        let result = self.spill_resident_attempts_inner();
        if result.is_err() {
            self.abort_file_barrier();
        }
        result
    }

    fn spill_resident_attempts_inner(&mut self) -> Result<(), SourceStreamError> {
        let (record_bytes, error_bytes) = {
            let state = self
                .state
                .as_mut()
                .expect("spill is requested only while a file is staged");
            let record_bytes = state
                .records
                .as_mut()
                .map(SortBuffer::sort_and_spill)
                .transpose()
                .map_err(|error| {
                    SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
                })?
                .unwrap_or(0);
            let error_bytes = state
                .errors
                .as_mut()
                .map(SortBuffer::sort_and_spill)
                .transpose()
                .map_err(|error| {
                    SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
                })?
                .unwrap_or(0);
            (record_bytes, error_bytes)
        };
        let record_stage = self.record_stage.clone();
        let error_stage = self.error_stage.clone();
        self.charge_spill_for(&record_stage, record_bytes)?;
        self.charge_spill_for(&error_stage, error_bytes)?;
        self.update_staged_charge();
        Ok(())
    }

    fn charge_spill_for(&mut self, stage: &str, bytes: u64) -> Result<(), SourceStreamError> {
        if bytes == 0 {
            return Ok(());
        }
        if self.memory.record_spill_bytes(stage, bytes) {
            let cap = self.memory.max_spill_bytes();
            let current = self.memory.cumulative_spill_bytes();
            let error = PipelineError::spill_cap_exceeded(stage, cap, bytes, current);
            self.abort_file_barrier();
            return Err(SourceStreamError::OrderViolation(Box::new(error)));
        }
        Ok(())
    }

    fn emit_population(&self, population: AttemptPopulationDelta) -> Result<(), SourceStreamError> {
        self.tx
            .send(SourceStreamEvent::Population(population))
            .map_err(|_| SourceStreamError::Closed)
    }

    fn emit_punctuations(&self, punctuations: Vec<Punctuation>) -> Result<(), SourceStreamError> {
        for punctuation in punctuations {
            self.tx
                .send(SourceStreamEvent::Punctuation(punctuation))
                .map_err(|_| SourceStreamError::Closed)?;
        }
        Ok(())
    }

    fn emit_records(
        &mut self,
        output: PreparedOutput<SourceRowId>,
        population: AttemptPopulationId,
        original_doc_ctx: Option<&SharedStorage<DocumentContext>>,
    ) -> Result<(), SourceStreamError> {
        match output {
            PreparedOutput::Empty => Ok(()),
            PreparedOutput::InMemory(records) => {
                for (mut record, row_id) in records {
                    reattach_original_doc_ctx(&mut record, original_doc_ctx)?;
                    let retained =
                        unaccounted_record_pair_bytes(&record, &self.allocation_resources);
                    self.emit_record(record, row_id, population, retained)?;
                }
                Ok(())
            }
            PreparedOutput::Spilled { file, bytes } => {
                self.fixed_memory_bytes = spill_reader_bytes(self.reload_bytes_ewma);
                self.update_runtime_charge();
                let reader = file.reader().map_err(|error| {
                    SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
                })?;
                for item in reader {
                    let (mut record, row_id) = item.map_err(|error| {
                        SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
                    })?;
                    reattach_original_doc_ctx(&mut record, original_doc_ctx)?;
                    self.emit_record(record, row_id, population, 0)?;
                }
                self.fixed_memory_bytes = 0;
                self.update_runtime_charge();
                drop(file);
                self.memory.release_spill_bytes(&self.record_stage, bytes);
                Ok(())
            }
        }
    }

    fn emit_errors(
        &mut self,
        output: PreparedOutput<StagedSourceRejection>,
        population: AttemptPopulationId,
        original_doc_ctx: Option<&SharedStorage<DocumentContext>>,
    ) -> Result<(), SourceStreamError> {
        match output {
            PreparedOutput::Empty => Ok(()),
            PreparedOutput::InMemory(errors) => {
                for (mut record, payload) in errors {
                    reattach_original_doc_ctx(&mut record, original_doc_ctx)?;
                    let retained = unaccounted_rejection_pair_bytes(
                        &record,
                        &payload,
                        &self.allocation_resources,
                    );
                    self.emit_rejection(payload.into_event(record), population, retained)?;
                }
                Ok(())
            }
            PreparedOutput::Spilled { file, bytes } => {
                self.fixed_memory_bytes = spill_reader_bytes(self.reload_bytes_ewma);
                self.update_runtime_charge();
                let reader = file.reader().map_err(|error| {
                    SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
                })?;
                for item in reader {
                    let (mut record, payload) = item.map_err(|error| {
                        SourceStreamError::OrderViolation(Box::new(PipelineError::from(error)))
                    })?;
                    reattach_original_doc_ctx(&mut record, original_doc_ctx)?;
                    self.emit_rejection(payload.into_event(record), population, 0)?;
                }
                self.fixed_memory_bytes = 0;
                self.update_runtime_charge();
                drop(file);
                self.memory.release_spill_bytes(&self.error_stage, bytes);
                Ok(())
            }
        }
    }

    fn emit_record(
        &mut self,
        record: Record,
        row_id: SourceRowId,
        population: AttemptPopulationId,
        released_bytes: u64,
    ) -> Result<(), SourceStreamError> {
        let sample = unaccounted_record_pair_bytes(&record, &self.allocation_resources);
        self.queued_bytes_ewma = ewma_step(self.queued_bytes_ewma, sample);
        self.reload_bytes_ewma = ewma_step(self.reload_bytes_ewma, record_pair_bytes(&record));
        self.tx
            .send(SourceStreamEvent::Attempt {
                event: SourceAttemptEvent::Record(record, row_id),
                population: Some(population),
            })
            .map_err(|_| SourceStreamError::Closed)?;
        self.release_resident_bytes(released_bytes)?;
        self.update_runtime_charge();
        Ok(())
    }

    fn emit_rejection(
        &mut self,
        event: crate::executor::SourceRejectionEvent,
        population: AttemptPopulationId,
        released_bytes: u64,
    ) -> Result<(), SourceStreamError> {
        let sample = unaccounted_rejection_event_bytes(&event, &self.allocation_resources);
        self.queued_bytes_ewma = ewma_step(self.queued_bytes_ewma, sample);
        self.reload_bytes_ewma = ewma_step(self.reload_bytes_ewma, rejection_event_bytes(&event));
        self.tx
            .send(SourceStreamEvent::Attempt {
                event: SourceAttemptEvent::Rejection(Box::new(event)),
                population: Some(population),
            })
            .map_err(|_| SourceStreamError::Closed)?;
        self.release_resident_bytes(released_bytes)?;
        self.update_runtime_charge();
        Ok(())
    }

    fn release_resident_bytes(&mut self, bytes: u64) -> Result<(), SourceStreamError> {
        self.releasing_memory_bytes =
            self.releasing_memory_bytes
                .checked_sub(bytes)
                .ok_or_else(|| {
                    SourceStreamError::OrderViolation(Box::new(PipelineError::Internal {
                        op: "source-order-retention",
                        node: self.config.source_name.to_string(),
                        detail: format!(
                            "released {bytes} resident bytes with only {} retained",
                            self.releasing_memory_bytes
                        ),
                    }))
                })?;
        Ok(())
    }

    fn update_staged_charge(&self) {
        self.update_accounted_charge();
    }

    fn update_runtime_charge(&self) {
        self.update_accounted_charge();
    }

    /// Refresh all ordered-source domains after a channel operation returns.
    /// Reloaded rows use this barrier's queue sample, not the original input's.
    pub(crate) fn refresh_accounted_charge(&self) {
        self.update_accounted_charge();
    }

    fn update_accounted_charge(&self) {
        let queued = (self.tx.len() as u64).saturating_mul(self.queued_bytes_ewma);
        self.consumer_handle.set_bytes(
            self.staged_bytes()
                .saturating_add(self.releasing_memory_bytes)
                .saturating_add(self.fixed_memory_bytes)
                .saturating_add(queued),
        );
    }

    fn cleanup_after_failure(&mut self) {
        self.release_all_spill_charge();
        self.releasing_memory_bytes = 0;
        self.fixed_memory_bytes = 0;
        self.update_accounted_charge();
    }

    pub(crate) fn staged_bytes(&self) -> u64 {
        self.state
            .as_ref()
            .map(|state| {
                let sorter = state
                    .records
                    .as_ref()
                    .map_or(0, |records| records.unaccounted_bytes_used() as u64);
                let errors = state
                    .errors
                    .as_ref()
                    .map_or(0, |errors| errors.unaccounted_bytes_used() as u64);
                let adjacent = state.previous.as_ref().map_or(0, |record| {
                    (std::mem::size_of::<Record>()
                        + record.unaccounted_heap_size(&self.allocation_resources))
                        as u64
                });
                sorter.saturating_add(errors).saturating_add(adjacent)
            })
            .unwrap_or(0)
    }

    fn physical_staged_bytes(&self) -> u64 {
        self.state
            .as_ref()
            .map(|state| {
                let sorter = state
                    .records
                    .as_ref()
                    .map_or(0, |records| records.bytes_used() as u64);
                let errors = state
                    .errors
                    .as_ref()
                    .map_or(0, |errors| errors.bytes_used() as u64);
                let adjacent = state.previous.as_ref().map_or(0, |record| {
                    (std::mem::size_of::<Record>() + record.estimated_heap_size()) as u64
                });
                sorter.saturating_add(errors).saturating_add(adjacent)
            })
            .unwrap_or(0)
    }

    fn emit_unsorted_warning(&mut self, file: &Arc<str>, inversion: &FirstInversion) {
        emit_unsorted_warning(&self.config, file, inversion);
        #[cfg(test)]
        {
            self.emitted_warnings = self.emitted_warnings.saturating_add(1);
        }
    }

    #[cfg(test)]
    fn emitted_warning_count(&self) -> u64 {
        self.emitted_warnings
    }

    fn stage_spill_charge(&self, stage: &str) -> u64 {
        self.memory
            .per_stage_spill_bytes()
            .get(stage)
            .copied()
            .unwrap_or(0)
    }

    fn release_all_spill_charge(&self) {
        for stage in [&self.record_stage, &self.error_stage] {
            let bytes = self.stage_spill_charge(stage);
            self.memory.release_spill_bytes(stage, bytes);
        }
    }

    fn order_error(&self, file: &Arc<str>, inversion: &FirstInversion) -> SourceStreamError {
        SourceStreamError::OrderViolation(Box::new(PipelineError::Config(
            clinker_plan::config::ConfigError::Validation(format!(
                "[E366] source '{}' file '{}' violates declared sort_order between rows {} and {}: {} precedes {}; reorder this physical file or set `on_unsorted: warn` to repair it before release",
                self.config.source_name,
                bounded_file_identity(file),
                inversion.previous_row,
                inversion.current_row,
                inversion.previous_key,
                inversion.current_key,
            )),
        )))
    }

    fn shape_error(&self, file: &Arc<str>, detail: &str) -> SourceStreamError {
        self.shape_error_for_file(&bounded_file_identity(file), detail)
    }

    fn shape_error_for_file(&self, file: &str, detail: &str) -> SourceStreamError {
        SourceStreamError::OrderViolation(Box::new(PipelineError::Config(
            clinker_plan::config::ConfigError::Validation(format!(
                "[E366] source '{}' file '{}' cannot preserve its declared sort_order: {detail}; remove `sort_order` or normalize the source to one flat or single-frame physical file",
                self.config.source_name, file,
            )),
        )))
    }
}

impl Drop for SourceFileOrderBarrier {
    fn drop(&mut self) {
        if self.state.is_some()
            || self.stage_spill_charge(&self.record_stage) != 0
            || self.stage_spill_charge(&self.error_stage) != 0
            || self.releasing_memory_bytes != 0
            || self.fixed_memory_bytes != 0
        {
            self.abort_file_barrier();
        }
    }
}

fn validate_runtime_shape(
    state: &SourceFileEventSpool,
    shape: SortableEventShape,
) -> Result<(), &'static str> {
    if state.depth != 0 {
        return Err("the physical file ended with an unmatched document open");
    }
    match shape {
        SortableEventShape::Flat if state.nested_opens == 0 && state.nested_closes == 0 => Ok(()),
        SortableEventShape::SingleFramePerPhysicalFile
            if state.nested_opens == 1 && state.nested_closes == 1 =>
        {
            Ok(())
        }
        SortableEventShape::Flat => Err("a flat source emitted a nested document frame"),
        SortableEventShape::SingleFramePerPhysicalFile => {
            Err("the source did not emit exactly one matching nested frame")
        }
    }
}

fn reattach_original_doc_ctx(
    record: &mut Record,
    original: Option<&SharedStorage<DocumentContext>>,
) -> Result<(), SourceStreamError> {
    let Some(original) = original else {
        return Err(SourceStreamError::OrderViolation(Box::new(
            PipelineError::Internal {
                op: "source-order-barrier",
                node: String::new(),
                detail: String::from("a repaired record has no original document context"),
            },
        )));
    };
    record.set_doc_ctx(original.clone());
    Ok(())
}

fn compare_compiled_keys(left: &Record, right: &Record, fields: &[ResolvedSortField]) -> Ordering {
    for field in fields {
        let ordering = compare_authored_values_with_nulls(
            left.values().get(field.field_index),
            right.values().get(field.field_index),
            field.order,
            field.null_order,
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn render_key(record: &Record, fields: &[ResolvedSortField]) -> String {
    let rendered = fields
        .iter()
        .map(|field| {
            let value = record
                .values()
                .get(field.field_index)
                .map_or_else(|| "<missing>".to_string(), |value| format!("{value:?}"));
            format!("{}={value}", field.field)
        })
        .collect::<Vec<_>>()
        .join(", ");
    truncate_evidence(rendered, 256)
}

fn bounded_file_identity(file: &str) -> String {
    let path = Path::new(file);
    let mut components = path
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let had_prefix = path.is_absolute() || components.len() > 8;
    if components.len() > 8 {
        components.drain(..components.len() - 8);
    }
    let mut rendered = components.join("/");
    if rendered.is_empty() {
        rendered = "<unknown>".to_string();
    } else if had_prefix {
        rendered.insert_str(0, ".../");
    }
    truncate_evidence(rendered, 240)
}

fn truncate_evidence(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes.saturating_sub(3);
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value.push_str("...");
    value
}

/// Emit exactly one structured warning after a successful repair and before
/// the verified file is released.
fn emit_unsorted_warning(config: &SourceOrderConfig, file: &Arc<str>, inversion: &FirstInversion) {
    let fields = config
        .fields
        .iter()
        .map(|field| field.field.as_str())
        .collect::<Vec<_>>()
        .join(",");
    tracing::warn!(
        target: "clinker::source_order",
        code = "W307",
        source = %config.source_name,
        file = %bounded_file_identity(file),
        previous_row = inversion.previous_row,
        current_row = inversion.current_row,
        previous_key = %inversion.previous_key,
        current_key = %inversion.current_key,
        sort_fields = %fields,
        "source file violated declared order and was repaired before release"
    );
}

/// `BufReader`/`BufWriter` use an 8 KiB default buffer. The merge additionally
/// holds one decoded record/payload cursor per open run.
const SPILL_IO_BUFFER_BYTES: u64 = 8 * 1024;

fn record_pair_bytes(record: &Record) -> u64 {
    (std::mem::size_of::<Record>()
        + record.estimated_heap_size()
        + std::mem::size_of::<SourceRowId>()) as u64
}

fn unaccounted_record_pair_bytes(record: &Record, resources: &AllocationResources) -> u64 {
    (std::mem::size_of::<Record>()
        + record.unaccounted_heap_size(resources)
        + std::mem::size_of::<SourceRowId>()) as u64
}

fn rejection_pair_bytes(record: &Record, payload: &StagedSourceRejection) -> u64 {
    (std::mem::size_of::<Record>()
        + record.estimated_heap_size()
        + std::mem::size_of::<StagedSourceRejection>()
        + payload.heap_bytes()) as u64
}

fn unaccounted_rejection_pair_bytes(
    record: &Record,
    payload: &StagedSourceRejection,
    resources: &AllocationResources,
) -> u64 {
    (std::mem::size_of::<Record>()
        + record.unaccounted_heap_size(resources)
        + std::mem::size_of::<StagedSourceRejection>()
        + payload.unaccounted_heap_bytes(resources)) as u64
}

fn rejection_event_bytes(event: &crate::executor::SourceRejectionEvent) -> u64 {
    (std::mem::size_of::<crate::executor::SourceRejectionEvent>() + event.estimated_heap_size())
        as u64
}

fn unaccounted_rejection_event_bytes(
    event: &crate::executor::SourceRejectionEvent,
    resources: &AllocationResources,
) -> u64 {
    (std::mem::size_of::<crate::executor::SourceRejectionEvent>()
        + event.unaccounted_heap_size(resources)) as u64
}

fn resident_record_bytes(
    output: &PreparedOutput<SourceRowId>,
    resources: &AllocationResources,
) -> u64 {
    match output {
        PreparedOutput::InMemory(records) => records
            .iter()
            .map(|(record, _)| unaccounted_record_pair_bytes(record, resources))
            .fold(0, u64::saturating_add),
        PreparedOutput::Empty | PreparedOutput::Spilled { .. } => 0,
    }
}

fn resident_error_bytes(
    output: &PreparedOutput<StagedSourceRejection>,
    resources: &AllocationResources,
) -> u64 {
    match output {
        PreparedOutput::InMemory(errors) => errors
            .iter()
            .map(|(record, payload)| unaccounted_rejection_pair_bytes(record, payload, resources))
            .fold(0, u64::saturating_add),
        PreparedOutput::Empty | PreparedOutput::Spilled { .. } => 0,
    }
}

fn spill_reader_bytes(record_bytes: u64) -> u64 {
    SPILL_IO_BUFFER_BYTES.saturating_add(record_bytes)
}

fn merger_reader_bytes(reader_count: usize, record_bytes: u64) -> u64 {
    (reader_count as u64).saturating_mul(spill_reader_bytes(record_bytes))
}

const fn ewma_step(previous: u64, sample: u64) -> u64 {
    if previous == 0 {
        sample
    } else if sample >= previous {
        previous + (sample - previous) / 8
    } else {
        previous - (previous - sample) / 8
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::pipeline::memory::NoOpPolicy;
    use clinker_format::preparation::MemoryOnlyResources;
    use clinker_plan::config::{CompileContext, NullOrder, PipelineConfig, SortOrder};
    use clinker_plan::plan::{EntityRef, PlanNodeId};
    use clinker_record::{DocumentId, EnvelopeRecord, Schema, Value};
    use clinker_record::{FieldStr, owned_storage::OwnedValues};
    use std::num::NonZeroUsize;

    fn governed_schema() -> SharedStorage<Schema> {
        SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "key".into(),
            "payload".into(),
            "mixed".into(),
            "$source.file".into(),
            "$source.name".into(),
        ])))
    }

    fn governed_record(
        resources: &AllocationResources,
        schema: &SharedStorage<Schema>,
        doc: &SharedStorage<DocumentContext>,
        key: i64,
        leaf: &FieldStr,
    ) -> Record {
        let scope = resources.scope().unwrap();
        let mut values = OwnedValues::try_with_capacity(16, &scope).unwrap();
        for value in [
            Value::Integer(key),
            Value::String(leaf.clone()),
            Value::Array(OwnedValues::from_vec(vec![Value::String(
                "legacy".repeat(32).into(),
            )])),
            Value::String("frame.swift".into()),
            Value::String("rows".into()),
        ] {
            values.try_push(value, &scope).unwrap();
        }
        let mut record = Record::from_owned_values(schema.clone(), values).unwrap();
        record.set_doc_ctx(doc.clone());
        record
    }

    fn rejection(
        record: Record,
        ordinal: u64,
        leaf: &FieldStr,
    ) -> crate::executor::SourceRejectionEvent {
        crate::executor::SourceRejectionEvent {
            source_row: SourceRowId::new(PlanNodeId::new(7), ordinal),
            source_name: Arc::from("rows"),
            source_file: Arc::from("frame.swift"),
            row: ordinal + 10,
            kind: crate::executor::SourceRejectionKind::DeclaredType,
            message: "E126 preserved diagnostic".into(),
            original_record: record,
            triggering_field: "payload".into(),
            triggering_value: Value::String(leaf.clone()),
            failed_at: crate::executor::DlqFailureStamp::now(),
        }
    }

    #[test]
    fn governed_staging_counts_clone_slots_and_preserves_physical_reload_forecast() {
        let (mut barrier, rx, _dir, _) = framed_barrier();
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let resources = provider.resources().allocation().clone();
        barrier.allocation_resources = resources.clone();
        let leaf = FieldStr::try_new(&"owned".repeat(500), &resources.scope().unwrap()).unwrap();
        let schema = governed_schema();
        let doc = document("frame.swift");
        barrier
            .begin_physical_file(Punctuation::document_open(doc.clone()))
            .unwrap();
        let record = governed_record(&resources, &schema, &doc, 1, &leaf);
        let physical = record_pair_bytes(&record);
        let relative = unaccounted_record_pair_bytes(&record, &resources);
        assert!(physical > relative);
        barrier
            .observe_typed_event(record, SourceRowId::new(PlanNodeId::new(7), 1))
            .unwrap();
        let previous = barrier.state.as_ref().unwrap().previous.as_ref().unwrap();
        assert!(!previous.values_are_accounted_by(&resources));
        let previous_relative =
            (std::mem::size_of::<Record>() + previous.unaccounted_heap_size(&resources)) as u64;
        assert_eq!(barrier.staged_bytes(), relative + previous_relative);
        assert_eq!(barrier.consumer_handle.bytes(), barrier.staged_bytes());
        assert_eq!(barrier.reload_bytes_ewma, physical);
        assert_eq!(barrier.queued_bytes_ewma, relative);
        assert!(barrier.physical_staged_bytes() > barrier.staged_bytes());
        assert_eq!(
            spill_reader_bytes(barrier.reload_bytes_ewma),
            SPILL_IO_BUFFER_BYTES + physical
        );
        barrier.abort_file_barrier();
        assert_eq!(barrier.consumer_handle.bytes(), 0);
        assert!(
            provider.used() > 0,
            "escaped leaf remains intrinsically charged"
        );
        drop(rx);
        drop(leaf);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn governed_ordered_spill_matches_resident_records_rejections_and_punctuation() {
        fn run(limit: u64) -> Vec<String> {
            let (mut barrier, rx, _dir, memory) = framed_barrier_with_limit(limit);
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
            let resources = provider.resources().allocation().clone();
            barrier.allocation_resources = resources.clone();
            let leaf =
                FieldStr::try_new(&"owned".repeat(600), &resources.scope().unwrap()).unwrap();
            let schema = governed_schema();
            let outer = document("frame.swift");
            let inner = document("frame.swift");
            barrier
                .observe_punctuation(Punctuation::document_open(outer.clone()))
                .unwrap();
            barrier
                .observe_punctuation(Punctuation::document_open(inner.clone()))
                .unwrap();
            let mut stamps = std::collections::HashMap::new();
            for ordinal in 1..=12 {
                let record = governed_record(
                    &resources,
                    &schema,
                    &inner,
                    (12 - ordinal) as i64 / 2,
                    &leaf,
                );
                if ordinal % 4 == 0 {
                    let event = rejection(record, ordinal, &leaf);
                    stamps.insert(ordinal, event.failed_at);
                    barrier
                        .observe_attempt(SourceAttemptEvent::Rejection(Box::new(event)))
                        .unwrap();
                } else {
                    barrier
                        .observe_typed_event(record, SourceRowId::new(PlanNodeId::new(7), ordinal))
                        .unwrap();
                }
            }
            assert!(barrier.reload_bytes_ewma > barrier.queued_bytes_ewma);
            assert_eq!(barrier.consumer_handle.bytes(), barrier.staged_bytes());
            let forced = limit < 1024 * 1024;
            if forced {
                assert!(memory.cumulative_spill_bytes() > 0);
                assert!(
                    barrier.staged_bytes() > 0,
                    "previous comparison clone remains live"
                );
            }
            barrier
                .observe_punctuation(Punctuation::document_close(inner.clone()))
                .unwrap();
            let outcome = barrier
                .observe_punctuation(Punctuation::document_close(outer.clone()))
                .unwrap()
                .unwrap();
            assert_eq!(outcome.spilled, forced);
            assert_eq!(barrier.releasing_memory_bytes, 0);
            assert_eq!(barrier.fixed_memory_bytes, 0);
            assert_eq!(
                barrier.consumer_handle.bytes(),
                rx.len() as u64 * barrier.queued_bytes_ewma
            );
            assert_eq!(memory.cumulative_spill_bytes(), 0);
            let mut result = Vec::new();
            for event in rx.try_iter() {
                match event {
                    SourceStreamEvent::Attempt { event, .. } => {
                        let (record, row, prefix) = match event {
                            SourceAttemptEvent::Record(record, row) => {
                                (record, row, "row".to_owned())
                            }
                            SourceAttemptEvent::Rejection(event) => {
                                assert_eq!(event.message, "E126 preserved diagnostic");
                                assert_eq!(event.row, event.source_row.ordinal() + 10);
                                assert_eq!(event.triggering_value, Value::String(leaf.clone()));
                                assert_eq!(
                                    event.failed_at,
                                    stamps[&event.source_row.ordinal()],
                                    "the reader's failure stamp survives staging and spill"
                                );
                                let prefix =
                                    format!("rejection:{}:{}", event.row, event.triggering_field);
                                (event.original_record, event.source_row, prefix)
                            }
                        };
                        assert!(SharedStorage::ptr_eq(record.doc_ctx(), &inner));
                        assert!(SharedStorage::ptr_eq(record.schema(), &schema));
                        assert_eq!(record.values_are_accounted_by(&resources), !forced);
                        if forced {
                            assert_eq!(
                                record.unaccounted_heap_size(&resources),
                                record.estimated_heap_size()
                            );
                        }
                        result.push(format!("{prefix}:{}:{:?}", row.ordinal(), record.values()));
                    }
                    SourceStreamEvent::Punctuation(p) => {
                        assert!(
                            SharedStorage::ptr_eq(p.doc_ctx(), &inner)
                                || SharedStorage::ptr_eq(p.doc_ctx(), &outer)
                        );
                        result.push(format!("punct:{:?}:{}", p.kind(), p.source_file()));
                    }
                    SourceStreamEvent::Population(p) => {
                        assert_eq!((p.attempted, p.rejected), (12, 3));
                        result.push(format!("population:{}:{}", p.attempted, p.rejected));
                    }
                }
            }
            barrier.abort_file_barrier();
            assert_eq!(barrier.consumer_handle.bytes(), 0);
            assert!(provider.used() > 0);
            drop(leaf);
            assert_eq!(provider.used(), 0);
            result
        }
        assert_eq!(run(8 * 1024), run(1024 * 1024 * 1024));
    }

    #[test]
    fn rejection_handoff_preserves_other_retained_rows_and_samples_queued_representation() {
        let (mut barrier, rx, _dir, _) = framed_barrier();
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let resources = provider.resources().allocation().clone();
        barrier.allocation_resources = resources.clone();
        let leaf = FieldStr::try_new(&"owned".repeat(100), &resources.scope().unwrap()).unwrap();
        let doc = document("frame.swift");
        let event = rejection(
            governed_record(&resources, &governed_schema(), &doc, 1, &leaf),
            2,
            &leaf,
        );
        let (record, payload) = StagedSourceRejection::from_event(event);
        let retained = unaccounted_rejection_pair_bytes(&record, &payload, &resources);
        let event = payload.into_event(record);
        let queued = unaccounted_rejection_event_bytes(&event, &resources);
        barrier.releasing_memory_bytes = 777 + retained;
        barrier
            .emit_rejection(
                event,
                AttemptPopulationId {
                    source: PlanNodeId::new(7),
                    document: doc.id(),
                },
                retained,
            )
            .unwrap();
        assert_eq!(barrier.releasing_memory_bytes, 777);
        assert_eq!(barrier.queued_bytes_ewma, queued);
        assert_eq!(barrier.consumer_handle.bytes(), 777 + queued);
        drop(rx.recv().unwrap());
        barrier.abort_file_barrier();
        drop(leaf);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn detached_spool_preparation_keeps_the_other_resident_side_charged() {
        let (mut barrier, _rx, _dir, memory) = framed_barrier();
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let resources = provider.resources().allocation().clone();
        barrier.allocation_resources = resources.clone();
        let leaf = FieldStr::try_new(&"owned".repeat(100), &resources.scope().unwrap()).unwrap();
        let doc = document("frame.swift");
        let schema = governed_schema();
        barrier
            .begin_physical_file(Punctuation::document_open(doc.clone()))
            .unwrap();
        barrier
            .observe_typed_event(
                governed_record(&resources, &schema, &doc, 2, &leaf),
                SourceRowId::new(PlanNodeId::new(7), 1),
            )
            .unwrap();
        let spilled = barrier
            .state
            .as_mut()
            .unwrap()
            .records
            .as_mut()
            .unwrap()
            .sort_and_spill()
            .unwrap();
        barrier
            .charge_spill_for(&barrier.record_stage.clone(), spilled)
            .unwrap();
        barrier
            .observe_typed_event(
                governed_record(&resources, &schema, &doc, 1, &leaf),
                SourceRowId::new(PlanNodeId::new(7), 2),
            )
            .unwrap();
        barrier
            .observe_rejection(rejection(
                governed_record(&resources, &schema, &doc, 3, &leaf),
                3,
                &leaf,
            ))
            .unwrap();
        let mut state = barrier.state.take().unwrap();
        state.previous = None;
        let error_bytes = state.errors.as_ref().unwrap().unaccounted_bytes_used() as u64;
        barrier.releasing_memory_bytes =
            error_bytes + state.records.as_ref().unwrap().unaccounted_bytes_used() as u64;
        barrier.update_runtime_charge();
        assert!(barrier.consumer_handle.bytes() > error_bytes);
        let records = barrier.prepare_records(state.records.take()).unwrap();
        assert!(matches!(records, PreparedOutput::Spilled { .. }));
        assert_eq!(barrier.releasing_memory_bytes, error_bytes);
        assert_eq!(barrier.consumer_handle.bytes(), error_bytes);
        let errors = barrier.prepare_errors(state.errors.take()).unwrap();
        assert_eq!(resident_error_bytes(&errors, &resources), error_bytes);
        assert_eq!(barrier.consumer_handle.bytes(), error_bytes);
        assert!(memory.cumulative_spill_bytes() > 0);
        drop((records, errors, state));
        barrier.cleanup_after_failure();
        assert_eq!(barrier.consumer_handle.bytes(), 0);
        assert_eq!(memory.cumulative_spill_bytes(), 0);
        drop(leaf);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn governed_spill_open_and_corruption_failures_release_retained_domains() {
        for corrupt_after_spill in [false, true] {
            let (mut barrier, rx, dir, memory) = framed_barrier_with_limit(1024);
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
            let resources = provider.resources().allocation().clone();
            barrier.allocation_resources = resources.clone();
            let leaf =
                FieldStr::try_new(&"owned".repeat(100), &resources.scope().unwrap()).unwrap();
            let schema = governed_schema();
            let outer = document("frame.swift");
            let inner = document("frame.swift");
            if !corrupt_after_spill {
                barrier.spill_dir = dir.path().join("missing");
            }
            barrier
                .observe_punctuation(Punctuation::document_open(outer.clone()))
                .unwrap();
            barrier
                .observe_punctuation(Punctuation::document_open(inner.clone()))
                .unwrap();
            let first = barrier.observe_typed_event(
                governed_record(&resources, &schema, &inner, 2, &leaf),
                SourceRowId::new(PlanNodeId::new(7), 1),
            );
            let error = if corrupt_after_spill {
                first.unwrap();
                barrier
                    .observe_typed_event(
                        governed_record(&resources, &schema, &inner, 1, &leaf),
                        SourceRowId::new(PlanNodeId::new(7), 2),
                    )
                    .unwrap();
                assert!(barrier.consumer_handle.bytes() > 0);
                assert!(memory.cumulative_spill_bytes() > 0);
                let paths = std::fs::read_dir(dir.path())
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>();
                assert!(paths.len() >= 2, "exercise merging multiple real runs");
                std::fs::write(&paths[0], b"corrupt spill").unwrap();
                barrier
                    .observe_punctuation(Punctuation::document_close(inner))
                    .unwrap();
                barrier
                    .observe_punctuation(Punctuation::document_close(outer))
                    .unwrap_err()
            } else {
                first.unwrap_err()
            };
            assert!(matches!(error, SourceStreamError::OrderViolation(_)));
            assert!(barrier.state.is_none());
            assert_eq!(barrier.staged_bytes(), 0);
            assert_eq!(barrier.releasing_memory_bytes, 0);
            assert_eq!(barrier.fixed_memory_bytes, 0);
            assert_eq!(barrier.consumer_handle.bytes(), 0);
            assert_eq!(memory.cumulative_spill_bytes(), 0);
            assert!(rx.is_empty(), "failed verification releases no evidence");
            assert!(provider.used() > 0);
            drop(leaf);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn governed_spilled_release_failure_after_prefix_keeps_escaped_leaf_alive() {
        let (mut barrier, old_rx, _dir, memory) = framed_barrier_with_limit(1024);
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
        let resources = provider.resources().allocation().clone();
        barrier.allocation_resources = resources.clone();
        let leaf = FieldStr::try_new(&"owned".repeat(100), &resources.scope().unwrap()).unwrap();
        let schema = governed_schema();
        let outer = document("frame.swift");
        let inner = document("frame.swift");
        barrier
            .observe_punctuation(Punctuation::document_open(outer.clone()))
            .unwrap();
        barrier
            .observe_punctuation(Punctuation::document_open(inner.clone()))
            .unwrap();
        for ordinal in 1..=3 {
            barrier
                .observe_typed_event(
                    governed_record(&resources, &schema, &inner, 4 - ordinal as i64, &leaf),
                    SourceRowId::new(PlanNodeId::new(7), ordinal),
                )
                .unwrap();
        }
        assert!(memory.cumulative_spill_bytes() > 0);
        let (tx, rx) = crossbeam_channel::bounded(0);
        barrier.tx = tx;
        drop(old_rx);
        barrier
            .observe_punctuation(Punctuation::document_close(inner.clone()))
            .unwrap();
        std::thread::scope(|scope| {
            let receiver = scope.spawn(move || {
                assert!(matches!(
                    rx.recv().unwrap(),
                    SourceStreamEvent::Population(_)
                ));
                assert!(matches!(
                    rx.recv().unwrap(),
                    SourceStreamEvent::Punctuation(_)
                ));
                assert!(matches!(
                    rx.recv().unwrap(),
                    SourceStreamEvent::Punctuation(_)
                ));
                let SourceStreamEvent::Attempt {
                    event: SourceAttemptEvent::Record(record, row),
                    ..
                } = rx.recv().unwrap()
                else {
                    panic!("first sorted record");
                };
                assert_eq!(record.get("key"), Some(&Value::Integer(1)));
                assert_eq!(row.ordinal(), 3);
                assert!(!record.values_are_accounted_by(&resources));
                assert!(SharedStorage::ptr_eq(record.doc_ctx(), &inner));
                // Receiver drop deterministically interrupts the next send.
            });
            let error = barrier
                .observe_punctuation(Punctuation::document_close(outer))
                .unwrap_err();
            assert!(matches!(error, SourceStreamError::Closed));
            receiver.join().unwrap();
        });
        assert_eq!(barrier.consumer_handle.bytes(), 0);
        assert_eq!(barrier.fixed_memory_bytes, 0);
        assert_eq!(barrier.releasing_memory_bytes, 0);
        assert_eq!(memory.cumulative_spill_bytes(), 0);
        assert!(provider.used() > 0);
        drop(leaf);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn completed_spill_quota_failure_and_early_shutdown_release_governed_owners() {
        for exceed_quota in [false, true] {
            let (mut barrier, rx, _dir, memory) = framed_barrier();
            let handle = barrier.consumer_handle.clone();
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
            let resources = provider.resources().allocation().clone();
            barrier.allocation_resources = resources.clone();
            let leaf =
                FieldStr::try_new(&"owned".repeat(100), &resources.scope().unwrap()).unwrap();
            let doc = document("frame.swift");
            barrier
                .begin_physical_file(Punctuation::document_open(doc.clone()))
                .unwrap();
            barrier
                .observe_typed_event(
                    governed_record(&resources, &governed_schema(), &doc, 1, &leaf),
                    SourceRowId::new(PlanNodeId::new(7), 1),
                )
                .unwrap();
            assert!(handle.bytes() > 0);
            let before = provider.used();
            if exceed_quota {
                memory.set_max_spill_bytes(1).unwrap();
                let error = barrier.spill_resident_attempts().unwrap_err();
                assert!(matches!(error, SourceStreamError::OrderViolation(_)));
                assert!(barrier.state.is_none());
                assert_eq!(barrier.releasing_memory_bytes, 0);
                assert_eq!(barrier.fixed_memory_bytes, 0);
            }
            drop(barrier);
            assert_eq!(handle.bytes(), 0);
            assert_eq!(memory.cumulative_spill_bytes(), 0);
            assert!(rx.is_empty());
            assert!(provider.used() > 0 && provider.used() < before);
            drop(leaf);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn failed_consolidation_write_retains_original_error_and_balances_reader_forecast() {
        #[derive(Eq, PartialEq, Ord, PartialOrd)]
        struct RefuseRewrite(bool);
        impl Serialize for RefuseRewrite {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                if self.0 {
                    return Err(serde::ser::Error::custom(
                        "deliberate rewritten payload failure",
                    ));
                }
                0u8.serialize(serializer)
            }
        }
        impl<'de> Deserialize<'de> for RefuseRewrite {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let _ = u8::deserialize(deserializer)?;
                Ok(Self(true))
            }
        }
        let (mut barrier, rx, dir, memory) = framed_barrier();
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["key".into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(1)]);
        barrier.reload_bytes_ewma = record_pair_bytes(&record);
        barrier.releasing_memory_bytes = 777;
        let mut writer = SpillWriter::new(schema, Some(dir.path()), false).unwrap();
        writer.write_pair(&record, &RefuseRewrite(false)).unwrap();
        let (file, bytes) = writer.finish_with_bytes().unwrap();
        let stage = barrier.record_stage.clone();
        barrier.charge_spill_for(&stage, bytes).unwrap();
        let error = barrier
            .consolidate_runs(vec![file], 1, &stage, true)
            .err()
            .expect("rewrite must fail");
        assert!(matches!(error, SourceStreamError::OrderViolation(_)));
        assert_eq!(barrier.releasing_memory_bytes, 777);
        assert!(barrier.fixed_memory_bytes >= SPILL_IO_BUFFER_BYTES);
        assert!(barrier.consumer_handle.bytes() >= 777 + SPILL_IO_BUFFER_BYTES);
        assert!(memory.cumulative_spill_bytes() > 0);
        barrier.cleanup_after_failure();
        assert_eq!(barrier.consumer_handle.bytes(), 0);
        assert_eq!(barrier.fixed_memory_bytes, 0);
        assert_eq!(memory.cumulative_spill_bytes(), 0);
        assert!(rx.is_empty());
    }

    fn document(file: &str) -> SharedStorage<DocumentContext> {
        SharedStorage::from_arc(Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from(file),
            EnvelopeRecord::empty(),
        )))
    }

    fn compiled_source_order_fixture() -> (
        clinker_plan::plan::execution::CompiledSourceOrder,
        clinker_format::SourceSchema,
    ) {
        let config: PipelineConfig = clinker_plan::yaml::from_str(
            r#"
pipeline:
  name: compiled_order_fixture
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      schema:
        - { name: key, type: int }
        - { name: payload, type: string }
      sort_order: [key]
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
"#,
        )
        .expect("parse compiled order fixture");
        let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
        let order = plan.dag().order_contract().source_orders[0].clone();
        let schema = plan
            .config()
            .source_bodies()
            .next()
            .expect("source body")
            .schema
            .clone();
        (order, schema)
    }

    #[test]
    fn source_order_config_consumes_compiled_typed_proof() {
        let (order, schema) = compiled_source_order_fixture();
        let config = SourceOrderConfig::from_compiled(&order, order.source_id, "rows", &schema)
            .expect("compiled proof must match its bound schema");

        assert_eq!(config.source_id, order.source_id);
        assert_eq!(config.fields, order.fields);
        assert_eq!(config.shape, order.shape);
        assert_eq!(config.on_unsorted, order.on_unsorted);
    }

    #[test]
    fn source_order_config_rejects_identity_and_schema_drift() {
        let (mut order, schema) = compiled_source_order_fixture();
        let wrong_source =
            SourceOrderConfig::from_compiled(&order, PlanNodeId::new(99), "rows", &schema)
                .expect_err("source identity mismatch must fail before ingest");
        assert!(
            matches!(
                wrong_source,
                PipelineError::Internal {
                    op: "source-order-contract",
                    ..
                }
            ),
            "{wrong_source:?}",
        );

        order.fields[0].field_index = 1;
        let wrong_schema =
            SourceOrderConfig::from_compiled(&order, order.source_id, "rows", &schema)
                .expect_err("compiled field index drift must fail before ingest");
        assert!(
            matches!(
                wrong_schema,
                PipelineError::Internal {
                    op: "source-order-contract",
                    ..
                }
            ),
            "{wrong_schema:?}",
        );

        order.fields[0].field_index = 0;
        order.fields[0].value_type = cxl::typecheck::Type::Float;
        let wrong_type = SourceOrderConfig::from_compiled(&order, order.source_id, "rows", &schema)
            .expect_err("compiled field type drift must fail before ingest");
        assert!(
            matches!(
                wrong_type,
                PipelineError::Internal {
                    op: "source-order-contract",
                    ..
                }
            ),
            "{wrong_type:?}",
        );
    }

    fn framed_barrier_with_limit(
        limit: u64,
    ) -> (
        SourceFileOrderBarrier,
        crossbeam_channel::Receiver<SourceStreamEvent>,
        tempfile::TempDir,
        Arc<MemoryArbitrator>,
    ) {
        let (tx, rx) = crossbeam_channel::bounded(512);
        let memory = Arc::new(MemoryArbitrator::with_policy(
            limit,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        let dir = tempfile::tempdir().expect("temporary spill root");
        let barrier = SourceFileOrderBarrier::new(
            SourceOrderConfig {
                source_id: PlanNodeId::new(7),
                source_name: Arc::from("rows"),
                fields: vec![ResolvedSortField {
                    field: "key".to_string(),
                    field_index: 0,
                    value_type: cxl::typecheck::Type::Int,
                    order: SortOrder::Asc,
                    null_order: NullOrder::Last,
                }],
                sort_fields: vec![SortField {
                    field: "key".to_string(),
                    order: SortOrder::Asc,
                    null_order: Some(NullOrder::Last),
                }],
                on_unsorted: OnUnsorted::Warn,
                shape: SortableEventShape::SingleFramePerPhysicalFile,
            },
            tx,
            ConsumerHandle::new(),
            Arc::clone(&memory),
            dir.path().to_path_buf(),
            false,
            clinker_format::preparation::MemoryOnlyResources::new(
                std::num::NonZeroUsize::new(limit as usize).unwrap(),
            )
            .resources()
            .allocation()
            .clone(),
        );
        (barrier, rx, dir, memory)
    }

    fn framed_barrier() -> (
        SourceFileOrderBarrier,
        crossbeam_channel::Receiver<SourceStreamEvent>,
        tempfile::TempDir,
        Arc<MemoryArbitrator>,
    ) {
        framed_barrier_with_limit(1024 * 1024 * 1024)
    }

    fn record(
        schema: &SharedStorage<Schema>,
        doc: &SharedStorage<DocumentContext>,
        key: i64,
    ) -> Record {
        let mut record = Record::new(
            schema.clone(),
            vec![
                Value::Integer(key),
                Value::String("frame.swift".into()),
                Value::String("rows".into()),
            ],
        );
        record.set_doc_ctx(doc.clone());
        record
    }

    #[test]
    fn staged_rejection_preserves_physical_line_distinct_from_attempt_ordinal() {
        let source_row = SourceRowId::new(PlanNodeId::new(7), 2);
        let record = Record::new(
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["record_type".into()]))),
            vec![Value::from("X")],
        );
        let event = crate::executor::SourceRejectionEvent::unknown_record_type(
            source_row,
            Arc::from("rows"),
            Arc::from("rows.csv"),
            3,
            record,
            "X".to_string(),
            "E345 line 3".to_string(),
        );

        let stamp = event.failed_at;
        let (record, staged) = StagedSourceRejection::from_event(event);
        let restored = staged.into_event(record);

        assert_eq!(restored.failed_at, stamp);
        assert_eq!(restored.source_row, source_row);
        assert_eq!(restored.row, 3);
        assert_eq!(restored.diagnostic_message(), "E345 line 3");
    }

    #[test]
    fn repaired_frame_retains_exact_document_arc_identity_and_provenance() {
        let (mut barrier, rx, _dir, _memory) = framed_barrier();
        let outer = document("frame.swift");
        let inner = document("frame.swift");
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "key".into(),
            "$source.file".into(),
            "$source.name".into(),
        ])));
        barrier
            .observe_punctuation(Punctuation::document_open(outer.clone()))
            .unwrap();
        barrier
            .observe_punctuation(Punctuation::document_open(inner.clone()))
            .unwrap();
        let source = PlanNodeId::new(7);
        barrier
            .observe_typed_event(record(&schema, &inner, 2), SourceRowId::new(source, 1))
            .unwrap();
        barrier
            .observe_typed_event(record(&schema, &inner, 1), SourceRowId::new(source, 2))
            .unwrap();
        barrier
            .observe_punctuation(Punctuation::document_close(inner.clone()))
            .unwrap();
        barrier
            .observe_punctuation(Punctuation::document_close(outer.clone()))
            .unwrap();

        assert!(
            barrier.consumer_handle.bytes() > 0,
            "released records must remain charged while queued"
        );
        let next_outer = document("next.swift");
        barrier
            .begin_physical_file(Punctuation::document_open(next_outer))
            .unwrap();
        assert!(
            barrier.consumer_handle.bytes() > 0,
            "opening the next physical file must retain the prior file's queue charge"
        );

        let records = rx
            .try_iter()
            .filter_map(|event| match event {
                SourceStreamEvent::Attempt {
                    event: SourceAttemptEvent::Record(record, row_id),
                    ..
                } => Some((record, row_id)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0.get("key"), Some(&Value::Integer(1)));
        assert_eq!(records[1].0.get("key"), Some(&Value::Integer(2)));
        assert_eq!(records[0].1, SourceRowId::new(source, 2));
        assert_eq!(records[1].1, SourceRowId::new(source, 1));
        for (record, _) in records {
            assert!(SharedStorage::ptr_eq(record.doc_ctx(), &inner));
            assert_eq!(
                record.get("$source.file"),
                Some(&Value::String("frame.swift".into()))
            );
            assert_eq!(
                record.get("$source.name"),
                Some(&Value::String("rows".into()))
            );
        }
        barrier.abort_file_barrier();
        assert_eq!(barrier.consumer_handle.bytes(), 0);
    }

    #[test]
    fn empty_single_frame_releases_the_original_boundary_arcs() {
        let (mut barrier, rx, _dir, _memory) = framed_barrier();
        let outer = document("empty.swift");
        let inner = document("empty.swift");
        barrier
            .observe_punctuation(Punctuation::document_open(outer.clone()))
            .unwrap();
        barrier
            .observe_punctuation(Punctuation::document_open(inner.clone()))
            .unwrap();
        barrier
            .observe_punctuation(Punctuation::document_close(inner.clone()))
            .unwrap();
        barrier
            .observe_punctuation(Punctuation::document_close(outer.clone()))
            .unwrap();

        let events = rx.try_iter().collect::<Vec<_>>();
        let SourceStreamEvent::Population(population) = &events[0] else {
            panic!("empty ordered file must release its complete population first")
        };
        assert_eq!(population.id.source, PlanNodeId::new(7));
        assert_eq!(population.attempted, 0);
        assert_eq!(population.rejected, 0);
        let punctuations = events
            .into_iter()
            .filter_map(|event| match event {
                SourceStreamEvent::Punctuation(punctuation) => Some(punctuation),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(punctuations.len(), 4);
        assert_eq!(punctuations[0].kind(), PunctuationKind::DocumentOpen);
        assert!(SharedStorage::ptr_eq(punctuations[0].doc_ctx(), &outer));
        assert_eq!(punctuations[1].kind(), PunctuationKind::DocumentOpen);
        assert!(SharedStorage::ptr_eq(punctuations[1].doc_ctx(), &inner));
        assert_eq!(punctuations[2].kind(), PunctuationKind::DocumentClose);
        assert!(SharedStorage::ptr_eq(punctuations[2].doc_ctx(), &inner));
        assert_eq!(punctuations[3].kind(), PunctuationKind::DocumentClose);
        assert!(SharedStorage::ptr_eq(punctuations[3].doc_ctx(), &outer));
    }

    #[test]
    fn warn_policy_emits_once_per_repaired_file_and_never_for_sorted_files() {
        let source = PlanNodeId::new(7);
        for (keys, expected_warnings) in [
            (vec![2, 1, 3, 4], 1),
            (vec![1, 3, 2, 4], 1),
            (vec![1, 2, 4, 3], 1),
            (vec![1, 2, 3, 4], 0),
        ] {
            let (mut barrier, _rx, _dir, _memory) = framed_barrier();
            let outer = document("warning.swift");
            let inner = document("warning.swift");
            let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
                "key".into(),
                "$source.file".into(),
                "$source.name".into(),
            ])));
            barrier
                .observe_punctuation(Punctuation::document_open(outer.clone()))
                .unwrap();
            barrier
                .observe_punctuation(Punctuation::document_open(inner.clone()))
                .unwrap();
            for (offset, key) in keys.into_iter().enumerate() {
                barrier
                    .observe_typed_event(
                        record(&schema, &inner, key),
                        SourceRowId::new(source, offset as u64 + 1),
                    )
                    .unwrap();
            }
            barrier
                .observe_punctuation(Punctuation::document_close(inner.clone()))
                .unwrap();
            barrier
                .observe_punctuation(Punctuation::document_close(outer))
                .unwrap();
            assert_eq!(barrier.emitted_warning_count(), expected_warnings);
        }
    }

    #[test]
    fn forced_spill_matches_resident_order_and_restores_exact_document_arc() {
        fn run(
            limit: u64,
        ) -> (
            Vec<(i64, SourceRowId)>,
            bool,
            SharedStorage<DocumentContext>,
            u64,
        ) {
            let (mut barrier, rx, _dir, memory) = framed_barrier_with_limit(limit);
            let outer = document("many.swift");
            let inner = document("many.swift");
            let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
                "key".into(),
                "$source.file".into(),
                "$source.name".into(),
            ])));
            barrier
                .observe_punctuation(Punctuation::document_open(outer.clone()))
                .unwrap();
            barrier
                .observe_punctuation(Punctuation::document_open(inner.clone()))
                .unwrap();
            let source = PlanNodeId::new(7);
            for ordinal in 1..=130u64 {
                // Descending duplicate pairs force repair while also proving
                // that cascaded merge fan-in keeps arrival order for ties.
                let key = (130 - ordinal as i64) / 2;
                barrier
                    .observe_typed_event(
                        record(&schema, &inner, key),
                        SourceRowId::new(source, ordinal),
                    )
                    .unwrap();
            }
            barrier
                .observe_punctuation(Punctuation::document_close(inner.clone()))
                .unwrap();
            let outcome = barrier
                .observe_punctuation(Punctuation::document_close(outer))
                .unwrap()
                .expect("outer close completes the file");
            let records = rx
                .try_iter()
                .filter_map(|event| match event {
                    SourceStreamEvent::Attempt {
                        event: SourceAttemptEvent::Record(record, row_id),
                        ..
                    } => {
                        assert!(SharedStorage::ptr_eq(record.doc_ctx(), &inner));
                        assert_eq!(
                            record.get("$source.file"),
                            Some(&Value::String("frame.swift".into()))
                        );
                        assert_eq!(
                            record.get("$source.name"),
                            Some(&Value::String("rows".into()))
                        );
                        let Value::Integer(key) = record.get("key").unwrap() else {
                            panic!("integer key")
                        };
                        Some((*key, row_id))
                    }
                    _ => None,
                })
                .collect();
            (
                records,
                outcome.spilled,
                inner,
                memory.cumulative_spill_bytes(),
            )
        }

        let (resident, resident_spilled, _resident_doc, resident_disk) = run(1024 * 1024 * 1024);
        let (spilled, forced_spilled, _spilled_doc, forced_disk) = run(1024);
        assert!(!resident_spilled);
        assert!(forced_spilled);
        assert_eq!(spilled, resident);
        assert_eq!(resident_disk, 0);
        assert_eq!(forced_disk, 0, "completed spill state must be released");
    }

    #[test]
    fn interrupted_spill_release_balances_memory_and_disk_without_evidence() {
        let (mut barrier, rx, _dir, memory) = framed_barrier_with_limit(1024);
        let outer = document("interrupted.swift");
        let inner = document("interrupted.swift");
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "key".into(),
            "$source.file".into(),
            "$source.name".into(),
        ])));
        barrier
            .observe_punctuation(Punctuation::document_open(outer.clone()))
            .unwrap();
        barrier
            .observe_punctuation(Punctuation::document_open(inner.clone()))
            .unwrap();
        let source = PlanNodeId::new(7);
        for ordinal in 1..=130u64 {
            barrier
                .observe_typed_event(
                    record(&schema, &inner, 130 - ordinal as i64),
                    SourceRowId::new(source, ordinal),
                )
                .unwrap();
        }
        assert!(memory.cumulative_spill_bytes() > 0);
        assert!(
            barrier.consumer_handle.bytes() > 0,
            "the staged adjacent record remains charged after forced spill"
        );

        drop(rx);
        barrier
            .observe_punctuation(Punctuation::document_close(inner))
            .unwrap();
        let error = barrier
            .observe_punctuation(Punctuation::document_close(outer))
            .expect_err("closed downstream must interrupt release");
        assert!(matches!(error, SourceStreamError::Closed));
        assert_eq!(barrier.consumer_handle.bytes(), 0);
        assert_eq!(memory.cumulative_spill_bytes(), 0);
        assert!(
            memory
                .per_stage_spill_bytes()
                .values()
                .all(|bytes| *bytes == 0)
        );
    }
}
