//! Inter-stage handoff storage for `ExecutorContext::node_buffers`.
//!
//! A single `NodeBuffer` slot can hold:
//!
//! - `Memory`: stream events accumulated entirely in RAM. Holds records
//!   and document-boundary punctuations interleaved in arrival order.
//! - `Spilled`: zero or more on-disk spill files for records, each
//!   paired with its recorded row count. Punctuations never spill —
//!   they live in the `pending_puncts` sidecar.
//! - `Mixed`: a mem tail accumulated after a partial spill.
//! - `ReReadable`: immutable resident or spilled backing shared by sequential
//!   fan-out consumers, each with an independent cursor.
//!
//! Every consumer drains a slot through [`NodeBuffer::drain`], which
//! returns an iterator that streams memory events first, then per-spill
//! records via `SpillReader`, and finally any trailing punctuations
//! that did not spill. Producer-side spill is wired in
//! `executor/node_buffer_spill.rs` and gated on
//! `MemoryArbitrator::should_spill()` at every bulk admission site via
//! `admit_node_buffer`. A slot that stays resident and is later elected
//! as a spill victim under sustained pressure is flushed by the
//! dispatcher's per-node sweep through [`NodeBuffer::spill_resident_memory`].

use std::sync::Arc;
use std::vec::IntoIter as VecIntoIter;

use clinker_record::{Record, Value};

use crate::executor::stream_event::{Punctuation, SourceRowId, StreamEvent};
use crate::pipeline::spill::{SpillFile, SpillReader};
use crate::pipeline::spill_merge::{OwnedMergeBudget, SortedRunMerger};
use clinker_plan::error::PipelineError;

/// Body records paired with the punctuations preserved from a buffer
/// drain. Returned by [`NodeBuffer::drain_split`] and threaded through
/// the per-operator dispatch sites that reshape records while forwarding
/// document boundaries unchanged.
pub(crate) type DrainedEvents = (Vec<(Record, SourceRowId)>, Vec<Punctuation>);

/// Per-record heuristic byte cost for a record of `column_count` columns.
///
/// The single source of truth every memory-accounting surface shares:
/// `NodeBuffer::estimated_memory_bytes` (full-stage `node_buffers`
/// admission), the dispatcher's per-batch `estimate_node_buffer_bytes`,
/// and `EventBatch::estimated_bytes` (streaming per-batch charge). Routing
/// all three through this fn keeps the charged byte total an operator
/// reports to the arbitrator consistent whether its output is admitted as
/// one full slot or streamed batch-by-batch.
///
/// Counts the `Value` slots plus the `(Record, SourceRowId)` pair overhead; it is
/// a fixed-width heuristic that ignores per-`Value` heap (string / list
/// payload), matching the existing admission model the soft-spill
/// threshold is tuned against.
pub(crate) fn record_byte_cost(column_count: usize) -> u64 {
    (std::mem::size_of::<Value>() * column_count + std::mem::size_of::<(Record, SourceRowId)>())
        as u64
}

/// One slot inside `ExecutorContext::node_buffers`.
pub(crate) enum NodeBuffer {
    /// All events live in memory — records and punctuations interleaved
    /// in arrival order.
    Memory(Vec<StreamEvent>),
    /// Every record lives on disk. Each chunk pairs a spill file with
    /// the number of rows that producer wrote to it; the row count
    /// drives `len_hint`'s O(1) total and the per-chunk discharge
    /// logic in the drain iterator. `pending_puncts` carries any
    /// punctuations that arrived before / during the spill — they
    /// drain after the spill chunks at the tail of the document.
    Spilled {
        chunks: Vec<(SpillFile<SourceRowId>, u64)>,
        pending_puncts: Vec<Punctuation>,
    },
    /// A spill followed by a resident mem tail. The sole producer of this
    /// variant is [`Self::push_event`] on an already-`Spilled` slot, which
    /// seeds `mem` with events that arrived *after* the spill — so `mem` is
    /// always the POST-spill tail, never a pre-spill head.
    ///
    /// [`Self::drain`] yields `mem` first, then the spill chunks, then
    /// `pending_puncts` — declaration order, not arrival order. Because
    /// `mem` is the post-spill tail, that drain is arrival-INVERTED for
    /// `Mixed`: the newer resident rows come out ahead of the older spilled
    /// rows. An order-sensitive consumer of a `Mixed` slot must therefore
    /// drain spill-chunks-first itself; the only one that both produces and
    /// consumes `Mixed` is the document-DLQ bucket flush
    /// (`document_dlq::drain_records_in_arrival_order`), which does exactly
    /// that. Inter-stage `node_buffers` slots are only ever built as
    /// `Memory` or `Spilled` and never `push_event`-ed, so they never reach
    /// `Mixed` and their `drain` order is unambiguous.
    Mixed {
        mem: Vec<StreamEvent>,
        spills: Vec<(SpillFile<SourceRowId>, u64)>,
        pending_puncts: Vec<Punctuation>,
    },
    /// Emit-phase payload-ordered output-sort runs adopted whole — never
    /// re-serialized. Drains by lazily k-way-merging the runs on
    /// `(order, driver_idx, build_idx)` and projecting each row to
    /// `(record, order)`. `row_count` is the exact emitted count (O(1)
    /// `len_hint`); `pending_puncts` drain after the merged records. The block-
    /// band IEJoin buffered-spilled path is the sole producer.
    MergeSpilled {
        runs: Vec<SpillFile<(SourceRowId, u64, u64)>>,
        row_count: u64,
        pending_puncts: Vec<Punctuation>,
        /// Charging context for the lazy fold-down: if the adopted runs are too
        /// fragmented, the drain's k-way merge cascades them, and the
        /// intermediate runs charge this node's disk quota through here.
        merge_budget: OwnedMergeBudget,
    },
    /// Immutable backing shared by sequential fan-out readers. Each reader
    /// gets its own cursor; memory events clone one at a time and spill files
    /// open one fresh sequential `SpillReader` at a time. The outer `Arc`
    /// keeps the slot's files alive until the authoritative reader ledger
    /// releases the final reader without pre-forking copies.
    ReReadable(Arc<ReReadableNodeBuffer>),
}

pub(crate) enum ReReadableNodeBuffer {
    Memory(Vec<StreamEvent>),
    Spilled {
        chunks: Vec<(SpillFile<SourceRowId>, u64)>,
        pending_puncts: Vec<Punctuation>,
    },
    Mixed {
        mem: Vec<StreamEvent>,
        spills: Vec<(SpillFile<SourceRowId>, u64)>,
        pending_puncts: Vec<Punctuation>,
    },
}

impl ReReadableNodeBuffer {
    fn len_hint(&self) -> usize {
        match self {
            Self::Memory(events) => events.iter().filter(|event| event.is_record()).count(),
            Self::Spilled { chunks, .. } => chunks.iter().map(|(_, count)| *count as usize).sum(),
            Self::Mixed { mem, spills, .. } => {
                mem.iter().filter(|event| event.is_record()).count()
                    + spills
                        .iter()
                        .map(|(_, count)| *count as usize)
                        .sum::<usize>()
            }
        }
    }

    fn memory_events(&self) -> &[StreamEvent] {
        match self {
            Self::Memory(events) => events,
            Self::Mixed { mem, .. } => mem,
            Self::Spilled { .. } => &[],
        }
    }

    fn spill_chunks(&self) -> &[(SpillFile<SourceRowId>, u64)] {
        match self {
            Self::Memory(_) => &[],
            Self::Spilled { chunks, .. } => chunks,
            Self::Mixed { spills, .. } => spills,
        }
    }

    fn pending_puncts(&self) -> &[Punctuation] {
        match self {
            Self::Memory(_) => &[],
            Self::Spilled { pending_puncts, .. } | Self::Mixed { pending_puncts, .. } => {
                pending_puncts
            }
        }
    }
}

impl NodeBuffer {
    /// Promote a `Vec<(Record, SourceRowId)>` into a `Memory` variant, wrapping
    /// each pair as a [`StreamEvent::Record`]. The dominant existing
    /// pattern at admission sites: producer accumulates records in a
    /// local `Vec`, then publishes the slot via this helper.
    pub(crate) fn memory_from_records<R>(records: Vec<(Record, R)>) -> Self
    where
        R: Into<SourceRowId>,
    {
        Self::Memory(
            records
                .into_iter()
                .map(|(r, rn)| StreamEvent::record(r, rn.into()))
                .collect(),
        )
    }

    /// Adopt the block-band output sort's already-spilled payload-ordered runs
    /// whole into a slot that k-way-merges them lazily at drain — never
    /// re-serializing them to a fresh chunk. `row_count` is the exact emitted
    /// count (the drain's O(1) `len_hint`); `pending_puncts` drain after the
    /// merged records. An empty run set or a zero count carries only the
    /// punctuations, so the drain never opens a merge over nothing.
    pub(crate) fn merge_spilled(
        runs: Vec<SpillFile<(SourceRowId, u64, u64)>>,
        row_count: u64,
        pending_puncts: Vec<Punctuation>,
        merge_budget: OwnedMergeBudget,
    ) -> Self {
        if runs.is_empty() || row_count == 0 {
            return Self::memory_from_records_and_puncts(Vec::new(), pending_puncts);
        }
        Self::MergeSpilled {
            runs,
            row_count,
            pending_puncts,
            merge_budget,
        }
    }

    /// Return a new sequential cursor while retaining this slot for later
    /// readers. The first call atomically converts the slot to immutable
    /// re-readable backing; subsequent calls only clone the backing `Arc`.
    ///
    /// `MergeSpilled` is the one non-repeatable representation. Its first
    /// shared read folds the destructive k-way merge into one ordinary spill,
    /// charging the replacement before releasing the adopted runs. A failed
    /// fold leaves no partially published re-readable slot.
    pub(crate) fn reread(&mut self) -> Result<Self, PipelineError> {
        if let Self::ReReadable(backing) = self {
            return Ok(Self::ReReadable(Arc::clone(backing)));
        }

        let original = std::mem::replace(self, Self::Memory(Vec::new()));
        let backing = match original {
            Self::Memory(events) => ReReadableNodeBuffer::Memory(events),
            Self::Spilled {
                chunks,
                pending_puncts,
            } => ReReadableNodeBuffer::Spilled {
                chunks,
                pending_puncts,
            },
            Self::Mixed {
                mem,
                spills,
                pending_puncts,
            } => ReReadableNodeBuffer::Mixed {
                mem,
                spills,
                pending_puncts,
            },
            Self::MergeSpilled {
                runs,
                row_count,
                pending_puncts,
                merge_budget,
            } => {
                let (file, folded_count) = merge_budget.fold_payload_ordered_runs(
                    runs,
                    row_count,
                    "combine shared node-buffer fold",
                )?;
                ReReadableNodeBuffer::Spilled {
                    chunks: vec![(file, folded_count)],
                    pending_puncts,
                }
            }
            Self::ReReadable(_) => unreachable!("handled before representation replacement"),
        };
        let backing = Arc::new(backing);
        *self = Self::ReReadable(Arc::clone(&backing));
        Ok(Self::ReReadable(backing))
    }

    /// Recover the ordinary owned representation for the authoritative last
    /// reader when no earlier cursor remains live. A still-shared Arc remains
    /// re-readable defensively; synchronous dispatch normally unwraps here.
    pub(crate) fn into_authoritative(self) -> Self {
        let Self::ReReadable(backing) = self else {
            return self;
        };
        match Arc::try_unwrap(backing) {
            Ok(ReReadableNodeBuffer::Memory(events)) => Self::Memory(events),
            Ok(ReReadableNodeBuffer::Spilled {
                chunks,
                pending_puncts,
            }) => Self::Spilled {
                chunks,
                pending_puncts,
            },
            Ok(ReReadableNodeBuffer::Mixed {
                mem,
                spills,
                pending_puncts,
            }) => Self::Mixed {
                mem,
                spills,
                pending_puncts,
            },
            Err(backing) => Self::ReReadable(backing),
        }
    }

    /// Total record count across memory and recorded spill chunks.
    /// Punctuations do not count toward the record total — they are
    /// O(1) per document, not per record.
    ///
    /// Used by consumer call-sites that want a `Vec::with_capacity`
    /// pre-allocation hint without consuming the buffer. Cheap on
    /// every variant — spill chunks carry their row count alongside
    /// the file handle, so no disk scan is required.
    pub(crate) fn len_hint(&self) -> usize {
        match self {
            Self::Memory(v) => v.iter().filter(|e| e.is_record()).count(),
            Self::Spilled { chunks, .. } => chunks.iter().map(|(_, c)| *c as usize).sum(),
            Self::Mixed { mem, spills, .. } => {
                mem.iter().filter(|e| e.is_record()).count()
                    + spills.iter().map(|(_, c)| *c as usize).sum::<usize>()
            }
            Self::MergeSpilled { row_count, .. } => *row_count as usize,
            Self::ReReadable(backing) => backing.len_hint(),
        }
    }

    /// Append a single `(record, row_number)` pair to the in-memory
    /// tail.
    ///
    /// On `Memory` and `Mixed`, the event is pushed onto the existing
    /// mem `Vec`. On `Spilled`, the variant is promoted to `Mixed`
    /// with the new pair as the sole mem tail (its `pending_puncts`
    /// moves with it). Producers that already accumulate a `Vec` and
    /// then insert via `NodeBuffer::memory_from_records(vec)` remain
    /// the dominant pattern; `push` exists so spill-trigger logic can
    /// resume in-memory accumulation after a partial spill.
    pub(crate) fn push<R>(&mut self, record: Record, row_id: R)
    where
        R: Into<SourceRowId>,
    {
        self.push_event(StreamEvent::record(record, row_id.into()));
    }

    /// Append a stream event (record OR punctuation) to the in-memory
    /// tail. Records and puncts interleave in arrival order; spill
    /// triggers filter puncts out of the records-only spill stream
    /// and stash them in the variant's `pending_puncts` sidecar.
    pub(crate) fn push_event(&mut self, event: StreamEvent) {
        match self {
            Self::Memory(v) => v.push(event),
            Self::Mixed { mem, .. } => mem.push(event),
            Self::Spilled { .. } => {
                let (chunks, puncts) = match std::mem::replace(self, Self::Memory(Vec::new())) {
                    Self::Spilled {
                        chunks,
                        pending_puncts,
                    } => (chunks, pending_puncts),
                    _ => unreachable!(),
                };
                *self = Self::Mixed {
                    mem: vec![event],
                    spills: chunks,
                    pending_puncts: puncts,
                };
            }
            Self::MergeSpilled { .. } => panic!(
                "push_event on a MergeSpilled node buffer: block-band output slots \
                 are never push_event-ed, and the (u64, u64, u64) runs are format-\
                 incompatible with Mixed's SpillFile<SourceRowId>, so promotion is impossible"
            ),
            Self::ReReadable(_) => panic!(
                "push_event on a re-readable node buffer: published fan-out slots are immutable"
            ),
        }
    }

    /// Non-consuming borrow of the in-memory rows, materialized as a
    /// `Vec<(&Record, u64)>` filtered to records (punctuations
    /// excluded).
    ///
    /// Returns an empty `Vec` on a pure `Spilled` slot — callers that
    /// need schema-style validation of every row in a spilled buffer
    /// must instead drain through [`Self::drain`]. The schema-check
    /// call-site this is wired into today operates only on memory-
    /// resident rows; spill-aware pre-flight validation is part of
    /// the spill-wiring sub-issue.
    pub(crate) fn peek_mem_records(&self) -> Vec<(&Record, SourceRowId)> {
        let mem_slice = match self {
            Self::Memory(v) => v.as_slice(),
            Self::Mixed { mem, .. } => mem.as_slice(),
            Self::Spilled { .. } | Self::MergeSpilled { .. } => &[],
            Self::ReReadable(backing) => backing.memory_events(),
        };
        mem_slice
            .iter()
            .filter_map(|e| match e {
                StreamEvent::Record(r, rn) => Some((r, *rn)),
                StreamEvent::Punctuation(_) => None,
            })
            .collect()
    }

    /// Column count of the slot's first resident or spill-backed record, or
    /// `0` when the slot holds no records. Cheap — stops at the first record
    /// or reads the first spill chunk's schema without opening it — so spill
    /// and materialization accounting can resolve the slot's row width before
    /// consuming the buffer.
    pub(crate) fn first_record_column_count(&self) -> usize {
        let mem_slice = match self {
            Self::Memory(v) => v.as_slice(),
            Self::Mixed { mem, .. } => mem.as_slice(),
            Self::Spilled { chunks, .. } => {
                return chunks
                    .first()
                    .map(|(file, _)| file.schema().column_count())
                    .unwrap_or(0);
            }
            // Records live on disk; read the width off the adopted run's schema
            // without opening it.
            Self::MergeSpilled { runs, .. } => {
                return runs.first().map(|f| f.schema().column_count()).unwrap_or(0);
            }
            Self::ReReadable(backing) => {
                if let Some(columns) =
                    backing
                        .memory_events()
                        .iter()
                        .find_map(|event| match event {
                            StreamEvent::Record(record, _) => Some(record.schema().column_count()),
                            StreamEvent::Punctuation(_) => None,
                        })
                {
                    return columns;
                }
                return backing
                    .spill_chunks()
                    .first()
                    .map(|(file, _)| file.schema().column_count())
                    .unwrap_or(0);
            }
        };
        mem_slice
            .iter()
            .find_map(|e| match e {
                StreamEvent::Record(r, _) => Some(r.schema().column_count()),
                StreamEvent::Punctuation(_) => None,
            })
            .unwrap_or(0)
    }

    /// Heuristic in-memory footprint of the slot, read by the
    /// `NodeBufferConsumer` wrapper's `current_usage` to drive the
    /// arbitrator's pull-mode attribution and Priority-policy victim
    /// selection. Punctuations contribute 0 to the budget — they are
    /// O(1) per document and never spill.
    ///
    /// Returns `0` on an empty memory tail. Spill-resident chunks are
    /// accounted via `MemoryArbitrator::cumulative_spill_bytes` (the disk
    /// quota), not this counter, so a `Spilled` slot reports `0` here.
    pub(crate) fn estimated_memory_bytes(&self) -> u64 {
        let events = match self {
            Self::Memory(events) => events.as_slice(),
            Self::Mixed { mem, .. } => mem.as_slice(),
            Self::Spilled { .. } | Self::MergeSpilled { .. } => return 0,
            Self::ReReadable(backing) => backing.memory_events(),
        };
        let mut column_count = None;
        let mut record_count = 0u64;
        for event in events {
            if let StreamEvent::Record(record, _) = event {
                column_count.get_or_insert_with(|| record.schema().column_count());
                record_count = record_count.saturating_add(1);
            }
        }
        column_count
            .map(|columns| record_byte_cost(columns).saturating_mul(record_count))
            .unwrap_or(0)
    }

    /// Estimated bytes for collecting every row in one sequential scan at a
    /// caller-supplied schema width. This counts spill-backed rows as well as
    /// the resident tail.
    pub(crate) fn estimated_materialized_bytes_for_columns(&self, column_count: usize) -> u64 {
        record_byte_cost(column_count).saturating_mul(self.len_hint() as u64)
    }

    /// Estimated resident bytes if one sequential scan is collected into a
    /// records vector. Unlike [`Self::estimated_memory_bytes`], this includes
    /// rows currently on disk and is used to reserve a composition port's
    /// unavoidable body-seed materialization before opening the scan.
    pub(crate) fn estimated_materialized_bytes(&self) -> u64 {
        record_byte_cost(self.first_record_column_count()).saturating_mul(self.len_hint() as u64)
    }

    /// Bytes to reserve when a replacement path has already unregistered the
    /// authoritative slot and will collect one complete scan. An owned buffer
    /// transfers or streams its existing storage into that collection. A
    /// still-shared re-readable buffer must additionally keep its resident
    /// backing alive while the scan clones rows from it.
    pub(crate) fn replacement_materialization_bytes_after_unregister(&self) -> u64 {
        let scan_bytes = self.estimated_materialized_bytes();
        if matches!(self, Self::ReReadable(_)) {
            scan_bytes.saturating_add(self.estimated_memory_bytes())
        } else {
            scan_bytes
        }
    }

    /// Additional bytes that overlap this slot's transferred registration
    /// while a consuming scan builds its replacement records vector.
    pub(crate) fn transferred_materialization_overlap_bytes(&self) -> u64 {
        match self {
            // The consuming drain moves resident events out of the sole owner.
            Self::Memory(_) => 0,
            // Disk rows have no resident charge, so the replacement is wholly
            // additional until the representation transition completes.
            Self::Spilled { .. } | Self::MergeSpilled { .. } => self.estimated_materialized_bytes(),
            // Only the disk-backed portion is absent from the existing
            // resident-tail charge.
            Self::Mixed { .. } => self
                .estimated_materialized_bytes()
                .saturating_sub(self.estimated_memory_bytes()),
            // A re-readable cursor clones resident events while the immutable
            // backing remains alive for sibling readers.
            Self::ReReadable(_) => self.estimated_materialized_bytes(),
        }
    }

    pub(crate) fn is_resident_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
            || matches!(self, Self::ReReadable(backing) if matches!(backing.as_ref(), ReReadableNodeBuffer::Memory(_)))
    }

    /// Consume the buffer and partition its events into a records
    /// vector and a punctuations vector. Used by record-processing
    /// operators (Transform, Route, Sort, Combine) that need to
    /// reshape records 1:N while passing punctuations through
    /// unchanged. The caller publishes its output via
    /// [`Self::memory_from_records_and_puncts`], which appends the
    /// preserved punctuations at the tail of the output stream — a
    /// position that preserves the "punctuation trails its document's
    /// records" invariant for any single-document buffer.
    ///
    /// Operators with richer punctuation semantics (Merge dedup,
    /// Aggregate flush-on-close) drain via [`Self::drain`] directly
    /// and pattern-match `StreamEvent` to inject per-document logic
    /// at the boundary.
    pub(crate) fn drain_split(self) -> Result<DrainedEvents, PipelineError> {
        let mut records: Vec<(Record, SourceRowId)> = Vec::with_capacity(self.len_hint());
        let mut puncts: Vec<Punctuation> = Vec::new();
        for event in self.drain() {
            match event? {
                StreamEvent::Record(r, rn) => records.push((r, rn)),
                StreamEvent::Punctuation(p) => puncts.push(p),
            }
        }
        Ok((records, puncts))
    }

    /// Build a `Memory` variant from a records vector and the
    /// punctuations preserved from the input drain. Punctuations are
    /// appended at the tail of the event stream so that document
    /// boundaries continue to trail their document's records — the
    /// streaming-contract invariant that drives Aggregate
    /// flush-on-close and Merge dedup.
    pub(crate) fn memory_from_records_and_puncts(
        records: Vec<(Record, SourceRowId)>,
        puncts: Vec<Punctuation>,
    ) -> Self {
        let mut events: Vec<StreamEvent> = Vec::with_capacity(records.len() + puncts.len());
        for (r, rn) in records {
            events.push(StreamEvent::record(r, rn));
        }
        for p in puncts {
            events.push(StreamEvent::punctuation(p));
        }
        Self::Memory(events)
    }

    /// Convert a resident `Memory` slot into a `Spilled` slot by flushing
    /// its records to a single on-disk chunk, returning the new variant
    /// alongside the chunk's on-disk byte size for the caller's disk-quota
    /// accounting.
    ///
    /// The arbitrator's resident-slot spill sweep
    /// (`dispatch::service_node_buffer_spill_requests`) calls this when it
    /// elects a live `node_buffers` slot as a spill victim: the slot's
    /// records leave RAM for disk and the caller discharges the slot's
    /// in-memory charge. Punctuations never spill — they move to the
    /// `Spilled` variant's `pending_puncts` sidecar and drain after the
    /// spill chunk, preserving the "punctuation trails its document"
    /// order. A slot holding only punctuations (no records) stays `Memory`
    /// (no empty spill file) and reports `0` spilled bytes.
    ///
    /// Only a `Memory` slot spills through this path: a `Spilled` slot is
    /// already on disk and a `Mixed` slot is the document-DLQ-only shape.
    /// Both return unchanged with `0` bytes — the sweep only ever hands
    /// this a `Memory` slot (it filters on the variant before electing a
    /// victim), and the pass-through arm keeps the method total for any
    /// future caller.
    pub(crate) fn spill_resident_memory(
        self,
        spill_dir: Option<&std::path::Path>,
        compress: bool,
    ) -> Result<(Self, u64), PipelineError> {
        let events = match self {
            Self::Memory(events) => events,
            Self::ReReadable(backing) => match Arc::try_unwrap(backing) {
                Ok(ReReadableNodeBuffer::Memory(events)) => events,
                Ok(other) => return Ok((Self::ReReadable(Arc::new(other)), 0)),
                // Dispatch is synchronous, so a completed earlier cursor has
                // dropped before the next spill sweep. If a cursor is still
                // live, retain the shared resident slot and retry later.
                Err(backing) => return Ok((Self::ReReadable(backing), 0)),
            },
            other => return Ok((other, 0)),
        };
        let mut records: Vec<(Record, SourceRowId)> = Vec::with_capacity(events.len());
        let mut puncts: Vec<Punctuation> = Vec::new();
        for event in events {
            match event {
                StreamEvent::Record(r, rn) => records.push((r, rn)),
                StreamEvent::Punctuation(p) => puncts.push(p),
            }
        }
        match crate::executor::node_buffer_spill::spill_node_buffer(records, spill_dir, compress)? {
            Some((file, count)) => {
                let file_bytes = std::fs::metadata(file.path()).map(|m| m.len()).unwrap_or(0);
                Ok((
                    Self::Spilled {
                        chunks: vec![(file, count)],
                        pending_puncts: puncts,
                    },
                    file_bytes,
                ))
            }
            // Punctuation-only slot: nothing to spill, keep the puncts
            // resident so their document boundaries still drain.
            None => Ok((Self::memory_from_records_and_puncts(Vec::new(), puncts), 0)),
        }
    }

    /// Consume the buffer, returning an iterator that yields memory
    /// events first, then per-spill-file records in vector order, and
    /// finally any trailing punctuations that did not spill.
    ///
    /// This is *declaration* order, which equals arrival order for `Memory`
    /// and `Spilled` but is arrival-INVERTED for `Mixed`: a `Mixed` slot's
    /// `mem` is always the post-spill tail (see the `Mixed` variant docs),
    /// so its newer resident rows drain ahead of its older spilled rows. A
    /// consumer that needs arrival order out of a possibly-`Mixed` slot must
    /// drain the spill chunks first itself — the document-DLQ bucket flush is
    /// the only such consumer, via
    /// `document_dlq::drain_records_in_arrival_order`. Inter-stage slots are
    /// never `Mixed`, so this order is unambiguous for them.
    ///
    /// Spill rows stream from disk via `SpillReader<SourceRowId>` without
    /// materializing the spill. Spill-open and per-row decode failures
    /// surface as `PipelineError::Spill` items so the executor's
    /// existing `?`-bubble path applies unchanged.
    pub(crate) fn drain(self) -> NodeBufferDrain {
        let (mem, spills, pending_puncts) = match self {
            Self::Memory(v) => (v, Vec::new(), Vec::new()),
            Self::Spilled {
                chunks,
                pending_puncts,
            } => (Vec::new(), chunks, pending_puncts),
            Self::Mixed {
                mem,
                spills,
                pending_puncts,
            } => (mem, spills, pending_puncts),
            // Adopted runs open their k-way merge lazily on the first record
            // poll (deferring the fallible open into `next`, so `drain` stays
            // infallible), then project each `(order, driver_idx, build_idx)`
            // payload back to the `(record, order)` shape the slot yields.
            Self::MergeSpilled {
                runs,
                row_count: _,
                pending_puncts,
                merge_budget,
            } => {
                return NodeBufferDrain::Merged {
                    runs: Some(runs),
                    merger: None,
                    pending_puncts: pending_puncts.into_iter(),
                    done: false,
                    merge_budget,
                };
            }
            Self::ReReadable(backing) => {
                return NodeBufferDrain::ReReadable {
                    current: None,
                    backing,
                    memory_index: 0,
                    spill_index: 0,
                    punctuation_index: 0,
                };
            }
        };
        NodeBufferDrain::Chunked {
            mem: mem.into_iter(),
            remaining_spills: spills.into_iter(),
            current: None,
            pending_puncts: pending_puncts.into_iter(),
        }
    }
}

/// RAII ownership of one materialized scan's arbitrator registration.
///
/// The registration is removed on every ordinary drop path, including `?`
/// propagation and early returns. Composition body entry consumes the guard
/// with [`Self::into_registration`] and installs the same id and handle in the
/// body-local node-buffer map, avoiding an unregister/register gap or double
/// charge.
#[must_use = "dropping the reservation releases the materialization charge"]
pub(crate) struct TransientNodeBufferReservation {
    budget: std::sync::Arc<crate::pipeline::memory::MemoryArbitrator>,
    consumer_id: crate::pipeline::memory::ConsumerId,
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
    owns_registration: bool,
}

impl TransientNodeBufferReservation {
    /// Reconstitute RAII ownership after a body Source removes a seeded slot
    /// and its registration from the body-local maps without unregistering.
    pub(crate) fn from_registration(
        budget: std::sync::Arc<crate::pipeline::memory::MemoryArbitrator>,
        id: crate::pipeline::memory::ConsumerId,
        handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
    ) -> Self {
        Self {
            budget,
            consumer_id: id,
            handle,
            owns_registration: true,
        }
    }

    /// Reserve an additional temporary allocation while keeping the same
    /// consumer registered. Materializing a sequential scan uses this for the
    /// interval where the immutable backing and its resident output vector
    /// coexist.
    pub(crate) fn reserve_additional(
        &self,
        additional_bytes: u64,
        node: &str,
    ) -> Result<(), PipelineError> {
        if additional_bytes == 0 {
            return Ok(());
        }
        // This preflight accounts the pipeline-owned allocations represented by
        // consumer handles. Adding an allocation estimate to process RSS would
        // double-count tracked state already present in RSS and make a small,
        // intentionally spill-heavy budget fail solely on the host process's
        // fixed baseline.
        let charged_pressure = self.budget.sum_consumer_usage();
        let projected_pressure = charged_pressure.saturating_add(additional_bytes);
        let hard_limit = self.budget.hard_limit();
        if hard_limit != 0 && projected_pressure > hard_limit {
            return Err(PipelineError::MemoryBudgetExceeded {
                node: node.to_string(),
                used: projected_pressure,
                limit: hard_limit,
                source: clinker_plan::BudgetCategory::NodeBuffer,
                detail: Some(format!(
                    "node-buffer materialization overlap projected {projected_pressure} bytes from charged pressure {charged_pressure} plus {additional_bytes} temporary bytes"
                )),
            });
        }
        self.handle.add_bytes(additional_bytes);
        self.budget.sample_peak_consumer_usage();
        Ok(())
    }

    /// Replace the reservation's reported bytes after a representation
    /// transition has completed.
    pub(crate) fn set_bytes(&self, bytes: u64) {
        self.handle.set_bytes(bytes);
    }

    /// Current bytes held by this reservation.
    pub(crate) fn bytes(&self) -> u64 {
        self.handle.bytes()
    }

    /// Move already-charged sibling reservations onto this registration.
    ///
    /// No allocation happens here. The sibling handles are zeroed before this
    /// handle grows, so the arbitrator never observes a transient duplicate
    /// charge while several harvested vectors become one node-buffer slot.
    pub(crate) fn absorb_charges(&self, others: Vec<Self>) {
        let charged_before = self.budget.sum_consumer_usage();
        let mut combined = self.bytes();
        for other in &others {
            debug_assert!(std::sync::Arc::ptr_eq(&self.budget, &other.budget));
            combined = combined.saturating_add(other.bytes());
            other.set_bytes(0);
        }
        drop(others);
        self.set_bytes(combined);
        debug_assert_eq!(
            self.budget.sum_consumer_usage(),
            charged_before,
            "reservation charge consolidation must preserve total usage"
        );
    }

    /// Transfer ownership of the live registration to a node-buffer registry.
    pub(crate) fn into_registration(
        mut self,
    ) -> (
        crate::pipeline::memory::ConsumerId,
        std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
    ) {
        self.owns_registration = false;
        (self.consumer_id, self.handle.clone())
    }
}

impl Drop for TransientNodeBufferReservation {
    fn drop(&mut self) {
        if self.owns_registration {
            self.handle.set_bytes(0);
            self.budget.unregister_consumer(self.consumer_id);
        }
    }
}

/// Reserve a transient materialization before a sequential reader collects its
/// events into a new resident vector. This retains the reservation mechanism
/// used by composition canonicalization without coupling fan-out access to a
/// memory-only buffer clone.
pub(crate) fn reserve_node_buffer_materialization(
    reserved_bytes: u64,
    budget: &std::sync::Arc<crate::pipeline::memory::MemoryArbitrator>,
    node: &str,
) -> Result<TransientNodeBufferReservation, PipelineError> {
    // Use the exact pipeline-owned charge ledger for an allocation preflight.
    // RSS remains the asynchronous spill/abort signal; adding this estimate to
    // RSS here would double-count charged state and include the process's fixed
    // baseline, which a spill-backed scan cannot reclaim.
    let charged_pressure = budget.sum_consumer_usage();
    let projected_pressure = charged_pressure.saturating_add(reserved_bytes);
    let hard_limit = budget.hard_limit();
    if hard_limit != 0 && projected_pressure > hard_limit {
        return Err(PipelineError::MemoryBudgetExceeded {
            node: node.to_string(),
            used: projected_pressure,
            limit: hard_limit,
            source: clinker_plan::BudgetCategory::NodeBuffer,
            detail: Some(format!(
                "transient node-buffer materialization projected {projected_pressure} bytes from charged pressure {charged_pressure} plus {reserved_bytes} reserved bytes"
            )),
        });
    }

    let handle = crate::pipeline::memory::ConsumerHandle::new();
    handle.set_bytes(reserved_bytes);
    let consumer_id = budget.register_consumer(std::sync::Arc::new(
        TransientNodeBufferConsumer::new(handle.clone()),
    ));
    budget.sample_peak_consumer_usage();
    let reservation = TransientNodeBufferReservation {
        budget: std::sync::Arc::clone(budget),
        consumer_id,
        handle,
        owns_registration: true,
    };

    Ok(reservation)
}

/// Arbitrator wrapper for a transient materialization. Unlike a resident
/// `NodeBufferConsumer`, this allocation has no dispatcher-owned slot that can
/// service a spill request, so it advertises neither reclamation nor
/// back-pressure. Its owner releases the charge only when the materialization is dropped
/// or transfers it into a composition body slot.
struct TransientNodeBufferConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl TransientNodeBufferConsumer {
    fn new(handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl crate::pipeline::memory::MemoryConsumer for TransientNodeBufferConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        i32::MAX
    }

    fn try_spill(
        &self,
        target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        Err(crate::pipeline::memory::ConsumerSpillError::BelowTarget {
            target: target_bytes,
            freed: 0,
        })
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

/// Iterator returned by [`NodeBuffer::drain`]. One variant per drainable buffer
/// family; both dispatch statically and are infallible to construct.
///
/// - `Chunked` streams a `Memory` / `Spilled` / `Mixed` slot: its in-memory
///   events first, then each spill chunk's records via
///   `SpillReader<SourceRowId>`,
///   finally the trailing punctuations. It owns the spill chunks so each
///   chunk's `TempPath` stays alive until the iterator advances past it, even
///   if the producer dropped its handle. Fields drop in declaration order: the
///   active reader closes its file handle before the chunk it was opened from
///   is unlinked.
/// - `Merged` streams a `MergeSpilled` slot by lazily k-way-merging the adopted
///   `(SourceRowId, u64, u64)` runs and projecting each row to
///   `(record, order)`, then
///   the trailing punctuations. The merge opens on the first record poll, so a
///   run-open failure surfaces as an `Err` item rather than at construction —
///   mirroring the chunked arm's lazy `file.reader()` open.
pub(crate) enum NodeBufferDrain {
    Chunked {
        mem: VecIntoIter<StreamEvent>,
        remaining_spills: VecIntoIter<(SpillFile<SourceRowId>, u64)>,
        // Boxed so the `Chunked` variant's size does not dwarf `Merged`'s: the
        // active `SpillReader` is bulky and only one is live at a time, and the
        // chunked path already does per-chunk file I/O, so the heap indirection
        // is free here.
        current: Option<Box<ActiveSpill>>,
        pending_puncts: VecIntoIter<Punctuation>,
    },
    Merged {
        /// Taken on the first record poll to open the merge; `None` afterward.
        runs: Option<Vec<SpillFile<(SourceRowId, u64, u64)>>>,
        merger: Option<SortedRunMerger<(SourceRowId, u64, u64)>>,
        pending_puncts: VecIntoIter<Punctuation>,
        /// Latches once a run-open or decode error has surfaced, so the drain
        /// stops rather than falling through to the trailing punctuations over a
        /// broken merge.
        done: bool,
        /// Charging context lent to the merge open so a fragmented adopted-run
        /// set folds down under the disk quota (E320) at drain.
        merge_budget: OwnedMergeBudget,
    },
    ReReadable {
        current: Option<Box<SpillReader<SourceRowId>>>,
        // Drops before `backing`, closing the active handle before the final
        // Arc can unlink its TempPaths on Windows.
        backing: Arc<ReReadableNodeBuffer>,
        memory_index: usize,
        spill_index: usize,
        punctuation_index: usize,
    },
}

pub(crate) struct ActiveSpill {
    reader: SpillReader<SourceRowId>,
    // Holds the file alive while `reader` streams it.
    _file: SpillFile<SourceRowId>,
}

impl Iterator for NodeBufferDrain {
    type Item = Result<StreamEvent, PipelineError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Chunked {
                mem,
                remaining_spills,
                current,
                pending_puncts,
            } => {
                if let Some(event) = mem.next() {
                    return Some(Ok(event));
                }
                loop {
                    if let Some(curr) = current.as_mut() {
                        match curr.reader.next() {
                            Some(Ok((rec, rn))) => return Some(Ok(StreamEvent::record(rec, rn))),
                            Some(Err(e)) => return Some(Err(PipelineError::from(e))),
                            None => *current = None,
                        }
                    }
                    if let Some((file, _count)) = remaining_spills.next() {
                        let reader = match file.reader() {
                            Ok(r) => r,
                            Err(e) => return Some(Err(PipelineError::from(e))),
                        };
                        *current = Some(Box::new(ActiveSpill {
                            reader,
                            _file: file,
                        }));
                        continue;
                    }
                    // Spill chunks exhausted — emit any trailing puncts.
                    return pending_puncts
                        .next()
                        .map(|p| Ok(StreamEvent::punctuation(p)));
                }
            }
            Self::Merged {
                runs,
                merger,
                pending_puncts,
                done,
                merge_budget,
            } => {
                if *done {
                    return None;
                }
                // Open the k-way merge on the first record poll; a failed open
                // is deferred here (keeping `drain` infallible) and latches
                // `done` so the trailing puncts never drain over a broken merge.
                // The merge folds an over-fragmented run set down under the disk
                // quota, charging intermediate runs through the parked budget.
                if merger.is_none()
                    && let Some(files) = runs.take()
                {
                    match SortedRunMerger::new_payload_ordered(
                        files,
                        "combine payload-sorted output merge",
                        merge_budget.as_borrowed(),
                    ) {
                        Ok(m) => *merger = Some(m),
                        Err(e) => {
                            *done = true;
                            return Some(Err(e));
                        }
                    }
                }
                if let Some(m) = merger.as_mut() {
                    match m.next() {
                        Some(Ok((record, (order, _, _)))) => {
                            return Some(Ok(StreamEvent::record(record, order)));
                        }
                        Some(Err(e)) => {
                            *done = true;
                            return Some(Err(e));
                        }
                        // Runs exhausted — fall through to the trailing puncts.
                        None => {}
                    }
                }
                pending_puncts
                    .next()
                    .map(|p| Ok(StreamEvent::punctuation(p)))
            }
            Self::ReReadable {
                backing,
                memory_index,
                spill_index,
                current,
                punctuation_index,
            } => {
                if let Some(event) = backing.memory_events().get(*memory_index) {
                    *memory_index += 1;
                    return Some(Ok(event.clone()));
                }
                loop {
                    if let Some(reader) = current.as_mut() {
                        match reader.next() {
                            Some(Ok((record, row_id))) => {
                                return Some(Ok(StreamEvent::record(record, row_id)));
                            }
                            Some(Err(error)) => return Some(Err(PipelineError::from(error))),
                            None => *current = None,
                        }
                    }
                    let chunks = backing.spill_chunks();
                    if let Some((file, _)) = chunks.get(*spill_index) {
                        *spill_index += 1;
                        match file.reader() {
                            Ok(reader) => *current = Some(Box::new(reader)),
                            Err(error) => return Some(Err(PipelineError::from(error))),
                        }
                        continue;
                    }
                    let puncts = backing.pending_puncts();
                    let punctuation = puncts.get(*punctuation_index)?.clone();
                    *punctuation_index += 1;
                    return Some(Ok(StreamEvent::punctuation(punctuation)));
                }
            }
        }
    }
}

/// `MemoryConsumer` wrapper for one `ctx.node_buffers` slot. Holds an
/// `Arc<ConsumerHandle>` shared with the dispatcher: every producer
/// push updates `handle.bytes` to track `NodeBuffer::estimated_memory_bytes()`;
/// every consumer drain decrements it. `try_spill` flips the handle's
/// spill-request flag but performs no I/O itself; the dispatcher's
/// per-node sweep `dispatch::service_node_buffer_spill_requests` reads
/// the flag via `take_spill_request` at the next `dispatch_plan_node`
/// turn and, for any resident `Memory` slot, spills it through
/// `NodeBuffer::spill_resident_memory` (postcard, optionally LZ4-framed, via
/// `SpillWriter<SourceRowId>`). Shared and producer-port slots remain readable because
/// each consumer opens its own sequential cursor over immutable backing.
///
/// `spill_priority = 0`: cheapest victim. Inter-stage buffers are
/// already row-oriented and write straight through `SpillWriter<SourceRowId>`;
/// no per-group or per-run reconstruction needed on the consumer
/// side. Preferred first victim under `Priority` and
/// `BackPressurePreferred::wrapping(Priority)`.
///
/// `can_back_pressure` is a constant `false`: a node buffer is filled
/// synchronously by the walk thread via `admit_node_buffer`, so there is
/// no separate producer thread to park, and the walk cannot resume a pause
/// it is itself blocked behind. Pressure on a node buffer is relieved by
/// spilling its resident rows (`try_spill`), never by pausing — only a
/// Source, fronted by a real producer thread, can honor a pause.
pub struct NodeBufferConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl NodeBufferConsumer {
    pub fn new(handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl crate::pipeline::memory::MemoryConsumer for NodeBufferConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        0
    }

    fn try_spill(
        &self,
        target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        self.handle.request_spill();
        let bytes = self.handle.bytes();
        if bytes >= target_bytes {
            Ok(bytes)
        } else {
            Err(crate::pipeline::memory::ConsumerSpillError::BelowTarget {
                target: target_bytes,
                freed: bytes,
            })
        }
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use clinker_plan::plan::EntityRef;
    use clinker_record::{Schema, Value, synthetic_document_context};

    use crate::executor::stream_event::{Punctuation, StreamEvent};
    use crate::pipeline::spill::SpillWriter;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["id".into(), "v".into()]))
    }

    fn rec(s: &Arc<Schema>, id: i64, v: &str) -> Record {
        Record::new(
            Arc::clone(s),
            vec![Value::Integer(id), Value::String(v.into())],
        )
    }

    fn rec_event(s: &Arc<Schema>, id: i64, v: &str, rn: u64) -> StreamEvent {
        StreamEvent::record(rec(s, id, v), rn)
    }

    fn spill_chunk<R>(rows: Vec<(Record, R)>) -> (SpillFile<SourceRowId>, u64)
    where
        R: Copy + Into<SourceRowId>,
    {
        let s = if let Some(first) = rows.first() {
            Arc::clone(first.0.schema())
        } else {
            schema()
        };
        let mut w: SpillWriter<SourceRowId> = SpillWriter::new(s, None, true).unwrap();
        let count = rows.len() as u64;
        for (r, rn) in &rows {
            w.write_pair(r, &(*rn).into()).unwrap();
        }
        (w.finish().unwrap(), count)
    }

    fn rec_row_num(e: &StreamEvent) -> u64 {
        match e {
            StreamEvent::Record(_, rn) => rn.ordinal(),
            StreamEvent::Punctuation(_) => panic!("expected Record event"),
        }
    }

    #[test]
    fn memory_push_drain_round_trip() {
        let s = schema();
        let mut nb = NodeBuffer::Memory(Vec::new());
        nb.push(rec(&s, 1, "a"), 10);
        nb.push(rec(&s, 2, "b"), 11);

        assert_eq!(nb.len_hint(), 2);

        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        assert_eq!(drained.len(), 2);
        assert_eq!(rec_row_num(&drained[0]), 10);
        assert_eq!(rec_row_num(&drained[1]), 11);
    }

    #[test]
    fn spilled_drains_records_then_pending_puncts() {
        let s = schema();
        let ctx = synthetic_document_context();
        let chunk_a = spill_chunk(vec![(rec(&s, 1, "a"), 1), (rec(&s, 2, "b"), 2)]);
        let chunk_b = spill_chunk(vec![(rec(&s, 3, "c"), 3)]);
        let nb = NodeBuffer::Spilled {
            chunks: vec![chunk_a, chunk_b],
            pending_puncts: vec![Punctuation::document_close(Arc::clone(&ctx))],
        };

        assert_eq!(nb.len_hint(), 3);

        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        assert_eq!(drained.len(), 4);
        assert_eq!(rec_row_num(&drained[0]), 1);
        assert_eq!(rec_row_num(&drained[1]), 2);
        assert_eq!(rec_row_num(&drained[2]), 3);
        assert!(matches!(drained[3], StreamEvent::Punctuation(_)));
    }

    #[test]
    fn mixed_drains_memory_before_spills_then_puncts() {
        let s = schema();
        let ctx = synthetic_document_context();
        let chunk = spill_chunk(vec![(rec(&s, 100, "spill-row"), 100)]);
        let nb = NodeBuffer::Mixed {
            mem: vec![rec_event(&s, 1, "mem-a", 1), rec_event(&s, 2, "mem-b", 2)],
            spills: vec![chunk],
            pending_puncts: vec![Punctuation::document_close(ctx)],
        };

        assert_eq!(nb.len_hint(), 3);

        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        assert_eq!(drained.len(), 4);
        assert_eq!(rec_row_num(&drained[0]), 1);
        assert_eq!(rec_row_num(&drained[1]), 2);
        assert_eq!(rec_row_num(&drained[2]), 100);
        assert!(matches!(drained[3], StreamEvent::Punctuation(_)));
    }

    #[test]
    fn punctuation_in_mem_interleaves_with_records() {
        let s = schema();
        let ctx = synthetic_document_context();
        let mut nb = NodeBuffer::Memory(Vec::new());
        nb.push(rec(&s, 1, "a"), 1);
        nb.push_event(StreamEvent::punctuation(Punctuation::document_close(ctx)));
        nb.push(rec(&s, 2, "b"), 2);

        // len_hint counts records only
        assert_eq!(nb.len_hint(), 2);

        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        assert_eq!(drained.len(), 3);
        assert!(matches!(drained[0], StreamEvent::Record(..)));
        assert!(matches!(drained[1], StreamEvent::Punctuation(_)));
        assert!(matches!(drained[2], StreamEvent::Record(..)));
    }

    #[test]
    fn push_on_spilled_promotes_to_mixed_preserving_puncts() {
        let s = schema();
        let ctx = synthetic_document_context();
        let mut nb = NodeBuffer::Spilled {
            chunks: vec![spill_chunk(vec![(rec(&s, 100, "s"), 100)])],
            pending_puncts: vec![Punctuation::document_close(ctx)],
        };
        nb.push(rec(&s, 1, "after-spill"), 200);

        assert!(matches!(nb, NodeBuffer::Mixed { .. }));
        assert_eq!(nb.len_hint(), 2);

        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        // mem tail drains first per the documented order, then spill,
        // then puncts.
        assert_eq!(rec_row_num(&drained[0]), 200);
        assert_eq!(rec_row_num(&drained[1]), 100);
        assert!(matches!(drained[2], StreamEvent::Punctuation(_)));
    }

    /// Pins the load-bearing `Mixed` invariant the drain-order docs rest on:
    /// `push_event` is the only producer of `Mixed`, and it seeds `mem` with
    /// the POST-spill tail — so every real `Mixed` has mem row-numbers that
    /// arrived *after* the spilled rows, and `drain` (mem-first) is therefore
    /// arrival-INVERTED. A `Mixed` whose mem is a pre-spill head does not
    /// exist, and inter-stage slots (built only as `Memory` / `Spilled`,
    /// never `push_event`-ed) never reach `Mixed` at all — so only the
    /// document-DLQ flush, which reorders spill-first, must compensate.
    #[test]
    fn mixed_mem_is_post_spill_tail_so_drain_is_arrival_inverted() {
        let s = schema();
        // Spill the older rows (arrival 10, 11), then push a newer row
        // (arrival 20) after the spill — the sole way a `Mixed` is built.
        let mut nb = NodeBuffer::Spilled {
            chunks: vec![spill_chunk(vec![
                (rec(&s, 1, "old-a"), 10),
                (rec(&s, 2, "old-b"), 11),
            ])],
            pending_puncts: Vec::new(),
        };
        nb.push(rec(&s, 3, "new"), 20);

        let NodeBuffer::Mixed { mem, spills, .. } = &nb else {
            panic!("push_event on a Spilled slot must promote to Mixed");
        };
        // The mem tail holds only the post-spill arrival, and its row number
        // is strictly greater than every spilled row's — mem is the tail,
        // never a head.
        let mem_rns: Vec<u64> = mem
            .iter()
            .filter_map(|e| match e {
                StreamEvent::Record(_, rn) => Some(rn.ordinal()),
                StreamEvent::Punctuation(_) => None,
            })
            .collect();
        assert_eq!(mem_rns, vec![20], "mem holds exactly the post-spill tail");
        assert_eq!(spills.len(), 1, "the pre-spill body stays on disk");

        // drain() yields mem (newer) before the spill chunk (older): the
        // documented arrival-INVERSION for every real `Mixed`.
        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        let drained_rns: Vec<u64> = drained.iter().map(rec_row_num).collect();
        assert_eq!(
            drained_rns,
            vec![20, 10, 11],
            "drain is mem-first, so the newer tail precedes the older spilled body"
        );
    }

    fn scan_shape(buffer: NodeBuffer) -> Vec<Option<u64>> {
        buffer
            .drain()
            .map(|event| match event.expect("re-readable scan event") {
                StreamEvent::Record(_, row_number) => Some(row_number.ordinal()),
                StreamEvent::Punctuation(_) => None,
            })
            .collect()
    }

    #[test]
    fn memory_supports_repeatable_sequential_scans() {
        let s = schema();
        let ctx = synthetic_document_context();
        let mut buffer = NodeBuffer::Memory(vec![
            rec_event(&s, 1, "a", 1),
            StreamEvent::punctuation(Punctuation::document_close(ctx)),
            rec_event(&s, 2, "b", 2),
        ]);

        let first = buffer.reread().expect("first memory scan");
        let second = buffer.reread().expect("second memory scan");

        assert_eq!(scan_shape(first), vec![Some(1), None, Some(2)]);
        assert_eq!(scan_shape(second), vec![Some(1), None, Some(2)]);
    }

    #[test]
    fn replacement_reserves_live_rereadable_memory_backing_and_scan() {
        let s = schema();
        let mut slot = NodeBuffer::Memory(vec![rec_event(&s, 1, "a", 1), rec_event(&s, 2, "b", 2)]);
        let live_reader = slot.reread().expect("shared reader");
        let scan_bytes = slot.estimated_materialized_bytes();
        let backing_bytes = slot.estimated_memory_bytes();

        let still_shared = slot.into_authoritative();
        assert!(matches!(still_shared, NodeBuffer::ReReadable(_)));
        assert_eq!(
            still_shared.replacement_materialization_bytes_after_unregister(),
            scan_bytes + backing_bytes
        );

        drop(live_reader);
        let authoritative = still_shared.into_authoritative();
        assert!(matches!(authoritative, NodeBuffer::Memory(_)));
        assert_eq!(
            authoritative.replacement_materialization_bytes_after_unregister(),
            scan_bytes
        );
    }

    #[test]
    fn spilled_supports_repeatable_sequential_scans() {
        let s = schema();
        let ctx = synthetic_document_context();
        let mut buffer = NodeBuffer::Spilled {
            chunks: vec![
                spill_chunk(vec![(rec(&s, 1, "a"), 1), (rec(&s, 2, "b"), 2)]),
                spill_chunk(vec![(rec(&s, 3, "c"), 3)]),
            ],
            pending_puncts: vec![Punctuation::document_close(ctx)],
        };

        let first = buffer.reread().expect("first spilled scan");
        let second = buffer.reread().expect("second spilled scan");

        assert_eq!(scan_shape(first), vec![Some(1), Some(2), Some(3), None]);
        assert_eq!(scan_shape(second), vec![Some(1), Some(2), Some(3), None]);
    }

    #[test]
    fn spilled_reread_holds_only_one_active_chunk_reader() {
        let s = schema();
        let chunks = (0..32)
            .map(|i| spill_chunk(vec![(rec(&s, i, "v"), i as u64)]))
            .collect();
        let mut buffer = NodeBuffer::Spilled {
            chunks,
            pending_puncts: Vec::new(),
        };
        let scan = buffer.reread().expect("spilled scan");
        let mut drain = scan.drain();
        let mut rows = 0;
        let mut max_active_readers = 0;
        while let Some(event) = drain.next() {
            assert!(event.expect("spill row").is_record());
            rows += 1;
            let active_readers = match &drain {
                NodeBufferDrain::ReReadable { current, .. } => usize::from(current.is_some()),
                _ => panic!("reread must use the re-readable drain"),
            };
            max_active_readers = max_active_readers.max(active_readers);
        }

        assert_eq!(rows, 32);
        assert_eq!(
            max_active_readers, 1,
            "a sequential scan opens one spill chunk at a time"
        );
    }

    #[test]
    fn mixed_supports_repeatable_sequential_scans_without_order_drift() {
        let s = schema();
        let ctx = synthetic_document_context();
        let mut buffer = NodeBuffer::Mixed {
            mem: vec![rec_event(&s, 3, "tail", 3)],
            spills: vec![spill_chunk(vec![
                (rec(&s, 1, "a"), 1),
                (rec(&s, 2, "b"), 2),
            ])],
            pending_puncts: vec![Punctuation::document_close(ctx)],
        };

        let first = buffer.reread().expect("first mixed scan");
        let second = buffer.reread().expect("second mixed scan");

        // Preserve NodeBuffer's established declaration order exactly: the
        // resident Mixed tail precedes its spill chunks, then punctuation.
        assert_eq!(scan_shape(first), vec![Some(3), Some(1), Some(2), None]);
        assert_eq!(scan_shape(second), vec![Some(3), Some(1), Some(2), None]);
    }

    #[test]
    fn empty_variants_have_zero_len_hint() {
        let s = schema();
        assert_eq!(NodeBuffer::Memory(Vec::new()).len_hint(), 0);
        assert_eq!(
            NodeBuffer::Spilled {
                chunks: Vec::new(),
                pending_puncts: Vec::new(),
            }
            .len_hint(),
            0
        );
        assert_eq!(
            NodeBuffer::Mixed {
                mem: Vec::new(),
                spills: Vec::new(),
                pending_puncts: Vec::new(),
            }
            .len_hint(),
            0
        );
        assert_eq!(
            NodeBuffer::Memory(vec![rec_event(&s, 1, "a", 1)]).len_hint(),
            1
        );
    }

    #[test]
    fn estimated_memory_bytes_scales_with_record_count_only() {
        let s = schema();
        let ctx = synthetic_document_context();
        let row_bytes_each = std::mem::size_of::<Value>() * s.column_count()
            + std::mem::size_of::<(Record, SourceRowId)>();

        // Memory: record count × per-row formula; puncts don't count.
        let mem = NodeBuffer::Memory(vec![
            rec_event(&s, 1, "a", 1),
            StreamEvent::punctuation(Punctuation::document_close(Arc::clone(&ctx))),
            rec_event(&s, 2, "b", 2),
            rec_event(&s, 3, "c", 3),
        ]);
        assert_eq!(mem.estimated_memory_bytes(), (row_bytes_each * 3) as u64);
        assert_eq!(
            mem.estimated_materialized_bytes_for_columns(s.column_count() + 2),
            record_byte_cost(s.column_count() + 2) * 3
        );

        // Spilled: zero bytes here — the disk surface tracks them
        // separately through `MemoryArbitrator::cumulative_spill_bytes`.
        let spilled = NodeBuffer::Spilled {
            chunks: vec![spill_chunk(vec![(rec(&s, 1, "a"), 1)])],
            pending_puncts: Vec::new(),
        };
        assert_eq!(spilled.estimated_memory_bytes(), 0);
        assert_eq!(
            spilled.estimated_materialized_bytes(),
            record_byte_cost(s.column_count())
        );
        let exact_budget = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            spilled.estimated_materialized_bytes(),
            0.80,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        let reservation = reserve_node_buffer_materialization(
            spilled.estimated_materialized_bytes(),
            &exact_budget,
            "sole_spill_reader",
        )
        .expect("the complete schema-width reservation fits at the exact boundary");
        assert_eq!(
            exact_budget.sum_consumer_usage(),
            record_byte_cost(s.column_count())
        );
        drop(reservation);
        assert_eq!(exact_budget.consumer_count(), 0);
        assert_eq!(
            spilled.estimated_materialized_bytes_for_columns(s.column_count() + 2),
            record_byte_cost(s.column_count() + 2)
        );

        // Empty mem reports zero.
        assert_eq!(NodeBuffer::Memory(Vec::new()).estimated_memory_bytes(), 0);
    }

    #[test]
    fn authoritative_reread_spill_preserves_materialized_row_width() {
        let s = schema();
        let mut slot = NodeBuffer::Spilled {
            chunks: vec![spill_chunk(vec![
                (rec(&s, 1, "a"), 1),
                (rec(&s, 2, "b"), 2),
            ])],
            pending_puncts: Vec::new(),
        };

        let earlier_reader = slot.reread().expect("first shared reader");
        drop(earlier_reader);
        let authoritative = slot.into_authoritative();

        assert!(matches!(authoritative, NodeBuffer::Spilled { .. }));
        assert_eq!(
            authoritative.estimated_materialized_bytes(),
            record_byte_cost(s.column_count()) * 2
        );
        let hard_limit = authoritative
            .estimated_materialized_bytes()
            .saturating_sub(1);
        let budget = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            hard_limit,
            0.80,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        assert!(matches!(
            reserve_node_buffer_materialization(
                authoritative.estimated_materialized_bytes(),
                &budget,
                "last_reader",
            ),
            Err(PipelineError::MemoryBudgetExceeded { .. })
        ));
        assert_eq!(budget.consumer_count(), 0);
    }

    fn roomy_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
        Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            100 * 1024 * 1024 * 1024,
            0.80,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ))
    }

    #[test]
    fn materialization_reservation_rejects_projected_hard_limit_before_registration() {
        let s = schema();
        let buffer = NodeBuffer::Memory(vec![rec_event(&s, 1, "a", 1)]);
        let reserved_bytes = buffer.estimated_memory_bytes();
        let hard_limit = 100 * 1024 * 1024 * 1024;
        let baseline_usage = hard_limit - reserved_bytes + 1;
        let budget = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            hard_limit,
            0.80,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        // A fixed registered footprint makes the charged-pressure projection
        // exact. The requested materialization crosses the limit by one byte.
        let baseline_id = budget.register_consumer(Arc::new(FixedUsageConsumer(baseline_usage)));

        match reserve_node_buffer_materialization(reserved_bytes, &budget, "clone_site") {
            Err(PipelineError::MemoryBudgetExceeded {
                node,
                used,
                limit,
                source,
                ..
            }) => {
                assert_eq!(node, "clone_site");
                assert_eq!(used, hard_limit + 1);
                assert_eq!(limit, hard_limit);
                assert_eq!(source, clinker_plan::BudgetCategory::NodeBuffer);
            }
            Ok(_) => panic!("expected pre-allocation E310 NodeBuffer; reservation succeeded"),
            Err(other) => panic!("expected pre-allocation E310 NodeBuffer; got {other:?}"),
        }
        assert_eq!(budget.consumer_count(), 1);
        assert_eq!(budget.sum_consumer_usage(), baseline_usage);
        budget.unregister_consumer(baseline_id);
        assert_eq!(budget.consumer_count(), 0);
    }

    #[test]
    fn materialization_reservation_covers_allocation_until_drop() {
        let s = schema();
        let buffer = NodeBuffer::Memory(vec![rec_event(&s, 1, "a", 1), rec_event(&s, 2, "b", 2)]);
        let reserved_bytes = buffer.estimated_memory_bytes();
        let budget = roomy_arbitrator();

        let reservation =
            reserve_node_buffer_materialization(reserved_bytes, &budget, "clone_site")
                .expect("reservation fits");
        assert_eq!(budget.consumer_count(), 1);
        assert_eq!(budget.sum_consumer_usage(), reserved_bytes);
        drop(reservation);
        assert_eq!(budget.consumer_count(), 0);
        assert_eq!(budget.sum_consumer_usage(), 0);
    }

    #[test]
    fn materialization_reservation_uses_pipeline_charges_not_process_baseline() {
        let s = schema();
        let buffer = NodeBuffer::Memory(vec![rec_event(&s, 1, "a", 1)]);
        let reserved_bytes = buffer.estimated_memory_bytes();
        let budget = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            reserved_bytes + 1,
            0.80,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));

        let reservation =
            reserve_node_buffer_materialization(reserved_bytes, &budget, "spill_backed_consumer")
                .expect("an empty charge ledger has room for the materialized scan");
        assert_eq!(budget.sum_consumer_usage(), reserved_bytes);
        drop(reservation);
        assert_eq!(budget.sum_consumer_usage(), 0);
    }

    #[test]
    fn materialization_reservation_error_unwind_returns_registry_to_baseline() {
        fn fail_after_reservation(
            reserved_bytes: u64,
            budget: &Arc<crate::pipeline::memory::MemoryArbitrator>,
        ) -> Result<(), PipelineError> {
            let _reservation =
                reserve_node_buffer_materialization(reserved_bytes, budget, "clone_site")?;
            Err(PipelineError::Internal {
                op: "reserved_materialization_test",
                node: "clone_site".to_string(),
                detail: "failure after allocation".to_string(),
            })
        }

        let s = schema();
        let buffer = NodeBuffer::Memory(vec![rec_event(&s, 1, "a", 1)]);
        let budget = roomy_arbitrator();
        let baseline_count = budget.consumer_count();
        let baseline_usage = budget.sum_consumer_usage();

        assert!(fail_after_reservation(buffer.estimated_memory_bytes(), &budget).is_err());
        assert_eq!(budget.consumer_count(), baseline_count);
        assert_eq!(budget.sum_consumer_usage(), baseline_usage);
    }

    #[test]
    fn materialization_reservation_transfer_keeps_one_continuous_charge() {
        let s = schema();
        let buffer = NodeBuffer::Memory(vec![rec_event(&s, 1, "a", 1)]);
        let reserved_bytes = buffer.estimated_memory_bytes();
        let budget = roomy_arbitrator();

        let reservation =
            reserve_node_buffer_materialization(reserved_bytes, &budget, "composition")
                .expect("reservation fits");
        let (consumer_id, handle) = reservation.into_registration();
        assert_eq!(
            budget.consumer_count(),
            1,
            "transfer must not register twice"
        );
        assert_eq!(budget.sum_consumer_usage(), reserved_bytes);

        handle.set_bytes(0);
        budget.unregister_consumer(consumer_id);
        assert_eq!(budget.consumer_count(), 0);
        assert_eq!(budget.sum_consumer_usage(), 0);
    }

    #[test]
    fn materialization_reservation_consolidation_preserves_exact_charge() {
        let budget = roomy_arbitrator();
        let primary = reserve_node_buffer_materialization(111, &budget, "composition")
            .expect("primary reservation fits");
        let second = reserve_node_buffer_materialization(222, &budget, "composition")
            .expect("second reservation fits");
        let third = reserve_node_buffer_materialization(333, &budget, "composition")
            .expect("third reservation fits");
        let charged_before = budget.sum_consumer_usage();
        assert_eq!(charged_before, 666);

        primary.absorb_charges(vec![second, third]);

        assert_eq!(primary.bytes(), 666);
        assert_eq!(budget.consumer_count(), 1);
        assert_eq!(budget.sum_consumer_usage(), charged_before);
        drop(primary);
        assert_eq!(budget.consumer_count(), 0);
        assert_eq!(budget.sum_consumer_usage(), 0);
    }

    #[test]
    fn materialization_reservation_preserves_zero_limit_as_unlimited() {
        let s = schema();
        let buffer = NodeBuffer::Memory(vec![rec_event(&s, 1, "a", 1)]);
        let budget = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            0,
            0.80,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));

        let reservation = reserve_node_buffer_materialization(
            buffer.estimated_memory_bytes(),
            &budget,
            "clone_site",
        )
        .expect("zero hard limit is unlimited");
        drop(reservation);
        assert_eq!(budget.consumer_count(), 0);
    }

    #[test]
    fn zero_byte_authoritative_memory_transition_does_not_false_abort() {
        let s = schema();
        let buffer = NodeBuffer::Memory(vec![rec_event(&s, 1, "a", 1)]);
        assert_eq!(buffer.transferred_materialization_overlap_bytes(), 0);
        let hard = 100 * 1024 * 1024 * 1024;
        let budget = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            hard,
            0.80,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        let reservation = reserve_node_buffer_materialization(
            buffer.estimated_memory_bytes(),
            &budget,
            "plain_memory",
        )
        .expect("initial slot charge fits");
        let baseline = budget.register_consumer(Arc::new(FixedUsageConsumer(hard)));

        reservation
            .reserve_additional(0, "plain_memory")
            .expect("a zero-overlap ownership move is a true no-op");

        budget.unregister_consumer(baseline);
        drop(reservation);
        assert_eq!(budget.consumer_count(), 0);
    }

    #[test]
    fn spilled_drop_unlinks_temp_files() {
        let s = schema();
        let (file, _) = spill_chunk(vec![(rec(&s, 1, "a"), 1)]);
        let path = file.path().to_path_buf();
        assert!(path.exists());

        let nb = NodeBuffer::Spilled {
            chunks: vec![(file, 1)],
            pending_puncts: Vec::new(),
        };
        drop(nb);

        assert!(!path.exists());
    }

    #[test]
    fn spill_backed_reread_opens_independent_sequential_cursors() {
        let s = schema();
        let expected = vec![
            SourceRowId::new(clinker_plan::plan::PlanNodeId::new(7), 1),
            SourceRowId::new(clinker_plan::plan::PlanNodeId::new(7), 2),
        ];
        let (file, count) = spill_chunk(vec![
            (rec(&s, 1, "a"), expected[0]),
            (rec(&s, 2, "b"), expected[1]),
        ]);
        let path = file.path().to_path_buf();
        let mut slot = NodeBuffer::Spilled {
            chunks: vec![(file, count)],
            pending_puncts: Vec::new(),
        };

        let first = slot.reread().expect("first shared cursor");
        let second = slot.reread().expect("second shared cursor");
        let collect_ids = |buffer: NodeBuffer| {
            buffer
                .drain()
                .map(|event| match event.expect("shared spill row decodes") {
                    StreamEvent::Record(_, row_id) => row_id,
                    StreamEvent::Punctuation(_) => panic!("fixture contains only records"),
                })
                .collect::<Vec<_>>()
        };

        assert_eq!(collect_ids(first), expected);
        assert_eq!(collect_ids(second), expected);
        assert!(path.exists(), "authoritative backing keeps the spill alive");

        drop(slot);
        assert!(!path.exists(), "last backing drop unlinks the spill");
    }

    #[test]
    fn spill_backed_reread_preserves_decode_error_and_cleans_up() {
        use std::io::{Seek, SeekFrom, Write};

        let s = schema();
        let (file, count) = spill_chunk(vec![(rec(&s, 1, "a"), 1)]);
        let path = file.path().to_path_buf();
        let mut slot = NodeBuffer::Spilled {
            chunks: vec![(file, count)],
            pending_puncts: Vec::new(),
        };
        let cursor = slot.reread().expect("shared cursor");

        let mut raw = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open spill for corruption fixture");
        raw.seek(SeekFrom::Start(0)).expect("seek to format tag");
        raw.write_all(&[0xff]).expect("corrupt format tag");
        raw.flush().expect("flush corrupt tag");
        drop(raw);

        let err = cursor
            .drain()
            .next()
            .expect("corrupt spill yields one error")
            .expect_err("corrupt spill must not be swallowed");
        assert!(
            matches!(err, PipelineError::Spill(_)),
            "shared cursor must preserve the underlying spill error: {err:?}"
        );
        assert!(
            path.exists(),
            "authoritative backing remains live after error"
        );

        drop(slot);
        assert!(!path.exists(), "error cleanup unlinks the shared spill");
    }

    #[test]
    fn spill_resident_memory_converts_records_to_spilled_preserving_puncts() {
        let s = schema();
        let ctx = synthetic_document_context();
        let mut nb = NodeBuffer::Memory(Vec::new());
        nb.push(rec(&s, 1, "a"), 10);
        nb.push(rec(&s, 2, "b"), 11);
        nb.push_event(StreamEvent::punctuation(Punctuation::document_close(ctx)));

        let (spilled, file_bytes) = nb
            .spill_resident_memory(None, true)
            .expect("resident spill ok");
        assert!(matches!(spilled, NodeBuffer::Spilled { .. }));
        assert!(
            file_bytes > 0,
            "a non-empty record run must report its on-disk byte size"
        );

        // Records stream back from disk in arrival order, then the trailing
        // punctuation — the spill sidecar preserves the document boundary.
        let drained: Vec<_> = spilled.drain().collect::<Result<_, _>>().unwrap();
        assert_eq!(drained.len(), 3);
        assert_eq!(rec_row_num(&drained[0]), 10);
        assert_eq!(rec_row_num(&drained[1]), 11);
        assert!(matches!(drained[2], StreamEvent::Punctuation(_)));
    }

    #[test]
    fn spill_resident_memory_keeps_punct_only_slot_in_memory() {
        let ctx = synthetic_document_context();
        let nb = NodeBuffer::Memory(vec![StreamEvent::punctuation(Punctuation::document_close(
            ctx,
        ))]);

        let (kept, file_bytes) = nb
            .spill_resident_memory(None, true)
            .expect("punct-only spill ok");
        // No records to spill: the slot stays resident with zero spilled
        // bytes so the sweep records nothing against the disk quota and the
        // document boundary still drains.
        assert!(matches!(kept, NodeBuffer::Memory(_)));
        assert_eq!(file_bytes, 0);
        let drained: Vec<_> = kept.drain().collect::<Result<_, _>>().unwrap();
        assert_eq!(drained.len(), 1);
        assert!(matches!(drained[0], StreamEvent::Punctuation(_)));
    }

    #[test]
    fn spill_resident_memory_passes_through_already_spilled_slot() {
        let s = schema();
        let nb = NodeBuffer::Spilled {
            chunks: vec![spill_chunk(vec![(rec(&s, 1, "a"), 1)])],
            pending_puncts: Vec::new(),
        };
        // An already-spilled slot is on disk: the helper leaves it untouched
        // and reports zero fresh spilled bytes (the sweep never hands it one,
        // but the arm keeps the method total).
        let (passed, file_bytes) = nb
            .spill_resident_memory(None, true)
            .expect("pass-through ok");
        assert!(matches!(passed, NodeBuffer::Spilled { .. }));
        assert_eq!(file_bytes, 0);
    }

    /// A registered consumer that reports a fixed charged footprint, so a
    /// test can stage `sum_consumer_usage()` at a chosen value without
    /// standing up a real spilling operator.
    struct FixedUsageConsumer(u64);

    impl crate::pipeline::memory::MemoryConsumer for FixedUsageConsumer {
        fn current_usage(&self) -> u64 {
            self.0
        }
        fn spill_priority(&self) -> i32 {
            0
        }
        fn try_spill(
            &self,
            _target_bytes: u64,
        ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
            Ok(0)
        }
        fn can_back_pressure(&self) -> bool {
            false
        }
    }

    #[test]
    fn node_buffer_consumer_reports_handle_bytes() {
        use crate::pipeline::memory::{ConsumerHandle, MemoryConsumer};
        let handle = ConsumerHandle::new();
        handle.set_bytes(4096);
        let consumer = NodeBufferConsumer::new(handle.clone());
        assert_eq!(consumer.current_usage(), 4096);
        assert_eq!(consumer.spill_priority(), 0);
        assert!(!consumer.can_back_pressure());
    }

    #[test]
    fn node_buffer_consumer_try_spill_flags_handle_and_returns_freed_or_below_target() {
        use crate::pipeline::memory::{ConsumerHandle, ConsumerSpillError, MemoryConsumer};
        let handle = ConsumerHandle::new();
        handle.set_bytes(1024);
        let consumer = NodeBufferConsumer::new(handle.clone());
        // Below-target: handle has 1024, asked for 4096.
        match consumer.try_spill(4096) {
            Err(ConsumerSpillError::BelowTarget { target, freed }) => {
                assert_eq!(target, 4096);
                assert_eq!(freed, 1024);
            }
            other => panic!("expected BelowTarget; got {other:?}"),
        }
        // Spill request flag flips regardless of return value; the
        // dispatcher reads it at the next admission boundary.
        assert!(handle.take_spill_request());
        // Above-target: 4096 ≥ 1024 → Ok.
        handle.set_bytes(8192);
        assert_eq!(consumer.try_spill(4096).unwrap(), 8192);
    }

    /// The block-band buffered-spilled drain adopts the emit-phase sorted runs
    /// whole: [`NodeBuffer::merge_spilled`] holds the `(order, driver_idx,
    /// build_idx)` runs on disk and k-way-merges them lazily at drain, projecting
    /// each payload back to the `(record, order)` shape the slot yields — with NO
    /// second disk write. The arbitrator here exists only so the test can read
    /// `cumulative_spill_bytes` on either side of the drain; the drain takes no
    /// arbitrator, so it structurally cannot charge. Pins: (a) the
    /// no-double-charge invariant, (b) the exact emitted count, (c) the
    /// deterministic `(order, driver_idx, build_idx)` order against a std-sort
    /// oracle and the payload→order projection, (d) the trailing punctuation.
    #[test]
    fn merge_spilled_adopts_runs_without_recharging_disk() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};

        let s = schema();
        let ctx = synthetic_document_context();
        let arb = std::sync::Arc::new(MemoryArbitrator::with_policy(
            1024 * 1024 * 1024,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));

        // (order, driver_idx, build_idx) payloads. `order` repeats (5 and 2
        // thrice) so ties exist; `driver_idx` is unique across every row, so the
        // (order, driver_idx, build_idx) key is a total order with an unambiguous
        // oracle. The record's `id` column mirrors `driver_idx`, making the
        // drained sequence observable.
        let payloads: Vec<(SourceRowId, u64, u64)> = vec![
            (5.into(), 0, 0),
            (2.into(), 1, 0),
            (5.into(), 2, 0),
            (2.into(), 3, 0),
            (9.into(), 4, 0),
            (0.into(), 5, 0),
            (2.into(), 6, 0),
            (5.into(), 7, 0),
        ];
        let row_count = payloads.len() as u64;

        // budget=1 is the spill-everything threshold; explicit flushes between
        // chunks force several individually-sorted runs. Each returned byte count
        // is charged exactly once, as the emit phase charges its runs.
        let mut buf: SortBuffer<(SourceRowId, u64, u64)> =
            SortBuffer::new_payload_ordered(1, None, true, s.clone());
        let push_chunk = |buf: &mut SortBuffer<(SourceRowId, u64, u64)>,
                          chunk: &[(SourceRowId, u64, u64)]| {
            for &(order, driver_idx, build_idx) in chunk {
                buf.push(
                    rec(&s, driver_idx as i64, "x"),
                    (order, driver_idx, build_idx),
                );
            }
        };
        push_chunk(&mut buf, &payloads[0..3]);
        let written = buf.sort_and_spill().unwrap();
        arb.record_spill_bytes("banded", written);
        push_chunk(&mut buf, &payloads[3..6]);
        let written = buf.sort_and_spill().unwrap();
        arb.record_spill_bytes("banded", written);
        // The remaining pair stays resident for finish() to flush as the residue.
        push_chunk(&mut buf, &payloads[6..]);
        let (out, residue) = buf.finish().unwrap();
        arb.record_spill_bytes("banded", residue);
        let SortedOutput::Spilled(files) = out else {
            panic!("expected Spilled after explicit flushes");
        };
        assert!(files.len() >= 2, "forced spill must produce multiple runs");

        // (a) no-double-charge: adopting + draining the runs adds not a
        // single byte to the cumulative spill total.
        let before = arb.cumulative_spill_bytes();
        // Few runs (< the merge fan-in) so the drain's k-way merge is a single
        // pass: it writes no intermediate runs and charges nothing further.
        let nb = NodeBuffer::merge_spilled(
            files,
            row_count,
            vec![Punctuation::document_close(ctx)],
            crate::pipeline::spill_merge::OwnedMergeBudget::new(
                std::sync::Arc::clone(&arb),
                std::sync::Arc::from("banded"),
                true,
            ),
        );
        let (drained, puncts) = nb.drain_split().unwrap();
        assert_eq!(
            arb.cumulative_spill_bytes(),
            before,
            "the merge-on-drain adopt path re-serializes nothing, so it charges no disk"
        );

        // (b) Every emitted row survives exactly once.
        assert_eq!(drained.len() as u64, row_count);

        // (c) Deterministic order: a std sort of the payloads is the oracle. The
        // drained `id` column == driver_idx and the projected order ==
        // payload.order.
        let mut oracle = payloads.clone();
        oracle.sort();
        let expected_ids: Vec<i64> = oracle
            .iter()
            .map(|(_, driver_idx, _)| *driver_idx as i64)
            .collect();
        let expected_orders: Vec<u64> =
            oracle.iter().map(|(order, _, _)| order.ordinal()).collect();
        let drained_ids: Vec<i64> = drained
            .iter()
            .map(|(r, _)| match r.get("id") {
                Some(Value::Integer(n)) => *n,
                other => panic!("expected Integer id, got {other:?}"),
            })
            .collect();
        let drained_orders: Vec<u64> = drained.iter().map(|(_, order)| order.ordinal()).collect();
        assert_eq!(
            drained_ids, expected_ids,
            "drained records order by (order, driver_idx, build_idx)"
        );
        assert_eq!(
            drained_orders, expected_orders,
            "each drained row projects the payload's order back as its (record, order) tag"
        );

        // (d) The trailing punctuation drains after the merged records.
        assert_eq!(puncts.len(), 1);
    }

    fn shared_merge_spilled_fixture(
        arb: &Arc<crate::pipeline::memory::MemoryArbitrator>,
    ) -> (NodeBuffer, Vec<std::path::PathBuf>) {
        use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};

        let s = schema();
        let mut buffer: SortBuffer<(SourceRowId, u64, u64)> =
            SortBuffer::new_payload_ordered(1, None, true, Arc::clone(&s));
        for (record_id, payload) in [(1, (3.into(), 1, 0)), (2, (1.into(), 2, 0))] {
            buffer.push(rec(&s, record_id, "x"), payload);
        }
        let first_bytes = buffer.sort_and_spill().expect("first merge run");
        arb.record_spill_bytes("banded", first_bytes);
        for (record_id, payload) in [(3, (2.into(), 3, 0)), (4, (4.into(), 4, 0))] {
            buffer.push(rec(&s, record_id, "x"), payload);
        }
        let second_bytes = buffer.sort_and_spill().expect("second merge run");
        arb.record_spill_bytes("banded", second_bytes);
        let (output, residue) = buffer.finish().expect("finish merge runs");
        arb.record_spill_bytes("banded", residue);
        let SortedOutput::Spilled(files) = output else {
            panic!("forced merge fixture must spill");
        };
        let paths = files.iter().map(|file| file.path().to_path_buf()).collect();
        (
            NodeBuffer::merge_spilled(
                files,
                4,
                Vec::new(),
                crate::pipeline::spill_merge::OwnedMergeBudget::new(
                    Arc::clone(arb),
                    Arc::from("banded"),
                    true,
                ),
            ),
            paths,
        )
    }

    #[test]
    fn shared_merge_spilled_folds_once_then_repeats_sequentially() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        let arb = Arc::new(MemoryArbitrator::with_policy(
            u64::MAX,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        let (mut buffer, original_paths) = shared_merge_spilled_fixture(&arb);

        let first = buffer.reread().expect("first reader folds merge runs");
        let after_first = arb.cumulative_spill_bytes();
        let second = buffer.reread().expect("second reader reuses folded spill");

        assert_eq!(
            arb.cumulative_spill_bytes(),
            after_first,
            "the second cursor must not fold or charge another replacement"
        );
        assert_eq!(scan_shape(first), vec![Some(1), Some(2), Some(3), Some(4)]);
        assert_eq!(scan_shape(second), vec![Some(1), Some(2), Some(3), Some(4)]);
        assert!(
            original_paths.iter().all(|path| !path.exists()),
            "the successful fold unlinks every destructive input run"
        );
    }

    #[test]
    fn shared_merge_spilled_e320_cleans_its_files_and_preserves_other_stage_charge() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        let arb = Arc::new(MemoryArbitrator::with_policy(
            u64::MAX,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        arb.record_spill_bytes("other", 777);
        let (mut buffer, original_paths) = shared_merge_spilled_fixture(&arb);
        let input_bytes = arb.cumulative_spill_bytes() - 777;
        arb.set_max_spill_bytes(input_bytes + 777);

        match buffer.reread() {
            Err(PipelineError::SpillCapExceeded { node, .. }) => assert_eq!(node, "banded"),
            Ok(_) => panic!("replacement overlap must exceed the exact input-only cap"),
            Err(other) => panic!("expected E320 replacement-overlap failure; got {other:?}"),
        }
        assert_eq!(
            arb.cumulative_spill_bytes(),
            777,
            "failed-fold cleanup releases only the banded stage's removed files"
        );
        assert!(
            original_paths.iter().all(|path| !path.exists()),
            "failed-fold cleanup unlinks every consumed input run"
        );
    }

    #[test]
    fn shared_merge_spilled_decode_error_is_preserved_and_cleans_inputs() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        let arb = Arc::new(MemoryArbitrator::with_policy(
            u64::MAX,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        arb.record_spill_bytes("other", 777);
        let (mut buffer, original_paths) = shared_merge_spilled_fixture(&arb);
        std::fs::write(&original_paths[0], b"corrupt spill run").expect("corrupt one adopted run");

        let error = match buffer.reread() {
            Ok(_) => panic!("the corrupt adopted run must fail the shared fold"),
            Err(error) => error,
        };
        let rendered = error.to_string();
        assert!(
            rendered.contains("spill run open failed")
                || rendered.contains("spill run decode failed"),
            "cleanup must preserve the original run-read failure: {rendered}"
        );
        assert_eq!(
            arb.cumulative_spill_bytes(),
            777,
            "error cleanup must not release another stage's live spill charge"
        );
        assert!(
            original_paths.iter().all(|path| !path.exists()),
            "error cleanup unlinks every adopted input run"
        );
    }
}
