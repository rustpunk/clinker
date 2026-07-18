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

use std::vec::IntoIter as VecIntoIter;

use clinker_record::{Record, Value};

use crate::executor::stream_event::{Punctuation, StreamEvent};
use crate::pipeline::spill::{SpillFile, SpillReader};
use crate::pipeline::spill_merge::{OwnedMergeBudget, SortedRunMerger};
use clinker_plan::error::PipelineError;

/// Body records paired with the punctuations preserved from a buffer
/// drain. Returned by [`NodeBuffer::drain_split`] and threaded through
/// the per-operator dispatch sites that reshape records while forwarding
/// document boundaries unchanged.
pub(crate) type DrainedEvents = (Vec<(Record, u64)>, Vec<Punctuation>);

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
/// Counts the `Value` slots plus the `(Record, u64)` pair overhead; it is
/// a fixed-width heuristic that ignores per-`Value` heap (string / list
/// payload), matching the existing admission model the soft-spill
/// threshold is tuned against.
pub(crate) fn record_byte_cost(column_count: usize) -> u64 {
    (std::mem::size_of::<Value>() * column_count + std::mem::size_of::<(Record, u64)>()) as u64
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
        chunks: Vec<(SpillFile<u64>, u64)>,
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
        spills: Vec<(SpillFile<u64>, u64)>,
        pending_puncts: Vec<Punctuation>,
    },
    /// Emit-phase payload-ordered output-sort runs adopted whole — never
    /// re-serialized. Drains by lazily k-way-merging the runs on
    /// `(order, driver_idx, build_idx)` and projecting each row to
    /// `(record, order)`. `row_count` is the exact emitted count (O(1)
    /// `len_hint`); `pending_puncts` drain after the merged records. The block-
    /// band IEJoin buffered-spilled path is the sole producer.
    MergeSpilled {
        runs: Vec<SpillFile<(u64, u64, u64)>>,
        row_count: u64,
        pending_puncts: Vec<Punctuation>,
        /// Charging context for the lazy fold-down: if the adopted runs are too
        /// fragmented, the drain's k-way merge cascades them, and the
        /// intermediate runs charge this node's disk quota through here.
        merge_budget: OwnedMergeBudget,
    },
}

impl NodeBuffer {
    /// Promote a `Vec<(Record, u64)>` into a `Memory` variant, wrapping
    /// each pair as a [`StreamEvent::Record`]. The dominant existing
    /// pattern at admission sites: producer accumulates records in a
    /// local `Vec`, then publishes the slot via this helper.
    pub(crate) fn memory_from_records(records: Vec<(Record, u64)>) -> Self {
        Self::Memory(
            records
                .into_iter()
                .map(|(r, rn)| StreamEvent::record(r, rn))
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
        runs: Vec<SpillFile<(u64, u64, u64)>>,
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
    pub(crate) fn push(&mut self, record: Record, rn: u64) {
        self.push_event(StreamEvent::record(record, rn));
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
                 incompatible with Mixed's SpillFile<u64>, so promotion is impossible"
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
    pub(crate) fn peek_mem_records(&self) -> Vec<(&Record, u64)> {
        let mem_slice = match self {
            Self::Memory(v) => v.as_slice(),
            Self::Mixed { mem, .. } => mem.as_slice(),
            Self::Spilled { .. } | Self::MergeSpilled { .. } => &[],
        };
        mem_slice
            .iter()
            .filter_map(|e| match e {
                StreamEvent::Record(r, rn) => Some((r, *rn)),
                StreamEvent::Punctuation(_) => None,
            })
            .collect()
    }

    /// Deep-clone the in-memory events for a multi-consumer fan-out site.
    ///
    /// # Panics
    ///
    /// Panics on `Spilled` and `Mixed`. Spill chunks cannot be
    /// cheap-cloned; the only legitimate multi-consumer access for a
    /// spilled buffer is to drain it. The producer-side admission
    /// helper `dispatch::node_buffer_spill_allowed` returns `false`
    /// for any slot whose outgoing topology will route through this
    /// method (multi-consumer fan-out or a composition input-port
    /// edge), so a `Spilled`/`Mixed` slot never reaches a caller of
    /// this method. Lifting that constraint requires sharing
    /// `Arc<SpillFile<u64>>` across readers and is a separate
    /// follow-up under #108.
    pub(crate) fn clone_memory_only(&self) -> Vec<StreamEvent> {
        match self {
            Self::Memory(v) => v.clone(),
            Self::Spilled { .. } | Self::Mixed { .. } | Self::MergeSpilled { .. } => {
                panic!(
                    "NodeBuffer::clone_memory_only called on a spill-backed \
                     variant; spilled rows cannot be cloned for multi-consumer \
                     fanout. Drain through NodeBuffer::drain instead.",
                );
            }
        }
    }

    /// Column count of the slot's first resident record, or `0` when the
    /// slot holds no in-memory record. Cheap — stops at the first record
    /// without allocating — so the spill sweep can resolve the on-disk
    /// compression mode against the slot's schema width before consuming
    /// the buffer.
    pub(crate) fn first_record_column_count(&self) -> usize {
        let mem_slice = match self {
            Self::Memory(v) => v.as_slice(),
            Self::Mixed { mem, .. } => mem.as_slice(),
            Self::Spilled { .. } => &[],
            // Records live on disk; read the width off the adopted run's schema
            // without opening it.
            Self::MergeSpilled { runs, .. } => {
                return runs.first().map(|f| f.schema().column_count()).unwrap_or(0);
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
        let mem_records = self.peek_mem_records();
        let Some((first, _)) = mem_records.first() else {
            return 0;
        };
        record_byte_cost(first.schema().column_count()).saturating_mul(mem_records.len() as u64)
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
        let mut records: Vec<(Record, u64)> = Vec::with_capacity(self.len_hint());
        let mut puncts: Vec<Punctuation> = Vec::new();
        for event in self.drain() {
            match event? {
                StreamEvent::Record(r, rn) => records.push((r, rn)),
                StreamEvent::Punctuation(p) => puncts.push(p),
            }
        }
        Ok((records, puncts))
    }

    /// Like [`Self::drain_split`], but aborts when the growing re-materialized
    /// record vector alone exceeds the entire memory budget, so a spill-backed
    /// slot cannot re-inflate a working set larger than the budget uncharged.
    ///
    /// [`Self::drain_split`] streams a `Spilled`/`Mixed` slot's records off
    /// disk straight into a `Vec` with no arbitrator charge and no hard-limit
    /// poll — the slot's admission charge was discharged when its consumer
    /// unregistered it, so the bytes landing back in memory here are
    /// invisible to the budget. A slot that spilled precisely because it
    /// outgrew the budget would then re-materialize a Vec larger than the
    /// whole budget with no gate. Every ~1024 records this estimates the
    /// resident footprint, adds every other registered consumer's charged
    /// bytes, and when that joint total exceeds the hard limit surfaces a
    /// typed `MemoryBudgetExceeded` (E310, tagged `BudgetCategory::NodeBuffer`)
    /// naming the draining stage.
    ///
    /// The gate sums this vector's footprint with
    /// [`MemoryArbitrator::sum_consumer_usage`] rather than testing the vector
    /// alone: a live consumer already charged near the limit and this growing
    /// vector can jointly breach the budget while each stays under it
    /// individually. That charged-byte sum is RSS-independent, deliberately
    /// not [`MemoryArbitrator::should_abort`]: `should_abort` also polls
    /// whole-process RSS, which for any budget below the process baseline (the
    /// very condition that made the upstream slot spill) is always over — it
    /// would abort every legitimate spill round-trip whose finite working set
    /// still fits the budget. Gating on the charged-byte sum keeps the
    /// "spillable stages complete" guarantee for the common case, catches the
    /// genuinely unaffordable one, and holds identically on targets where
    /// `rss_bytes()` returns `None`.
    ///
    /// Wired into the single-consumer Transform and Aggregate drain sites,
    /// whose owned buffer has no separate budget gate of its own. Sort,
    /// Combine, Cull, and Reshape keep [`Self::drain_split`] because they gate
    /// their own re-materialized working set downstream.
    pub(crate) fn drain_split_metered(
        self,
        budget: &crate::pipeline::memory::MemoryArbitrator,
        node: &str,
    ) -> Result<DrainedEvents, PipelineError> {
        // Only a spill-backed slot re-inflates uncharged; a pure `Memory`
        // slot's bytes were never off the budget, so skip the per-batch poll
        // and reuse the plain drain for it.
        if matches!(self, Self::Memory(_)) {
            return self.drain_split();
        }
        let hard = budget.hard_limit();
        let mut records: Vec<(Record, u64)> = Vec::with_capacity(self.len_hint());
        let mut puncts: Vec<Punctuation> = Vec::new();
        let mut since_check: usize = 0;
        for event in self.drain() {
            match event? {
                StreamEvent::Record(r, rn) => {
                    records.push((r, rn));
                    since_check += 1;
                    if since_check >= 1024 {
                        since_check = 0;
                        let cols = records
                            .last()
                            .map(|(r, _)| r.schema().column_count())
                            .unwrap_or(0);
                        let bytes = record_byte_cost(cols).saturating_mul(records.len() as u64);
                        // Gate on the re-materialized footprint PLUS every other
                        // registered consumer's charged bytes, not this vector
                        // alone: during a drain a consumer already charged near
                        // the limit and this growing vector can jointly breach
                        // the budget while each stays under it individually.
                        // Summing registered consumers is RSS-independent, so it
                        // holds where `rss_bytes()` returns `None` and does not
                        // false-positive on the sub-baseline budgets that made
                        // the upstream slot spill in the first place.
                        // `hard == 0` means "unlimited" (no configured budget),
                        // so never abort in that case.
                        let combined = bytes.saturating_add(budget.sum_consumer_usage());
                        if hard > 0 && combined > hard {
                            return Err(PipelineError::MemoryBudgetExceeded {
                                node: node.to_string(),
                                used: combined,
                                limit: hard,
                                source: clinker_plan::BudgetCategory::NodeBuffer,
                                detail: Some(format!(
                                    "re-materializing spilled node buffer for `{node}` \
                                     exceeded the memory budget"
                                )),
                            });
                        }
                    }
                }
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
        records: Vec<(Record, u64)>,
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
        let Self::Memory(events) = self else {
            return Ok((self, 0));
        };
        let mut records: Vec<(Record, u64)> = Vec::with_capacity(events.len());
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
    /// Spill rows stream from disk via `SpillReader<u64>` without
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
        };
        NodeBufferDrain::Chunked {
            mem: mem.into_iter(),
            remaining_spills: spills.into_iter(),
            current: None,
            pending_puncts: pending_puncts.into_iter(),
        }
    }
}

/// Iterator returned by [`NodeBuffer::drain`]. One variant per drainable buffer
/// family; both dispatch statically and are infallible to construct.
///
/// - `Chunked` streams a `Memory` / `Spilled` / `Mixed` slot: its in-memory
///   events first, then each spill chunk's records via `SpillReader<u64>`,
///   finally the trailing punctuations. It owns the spill chunks so each
///   chunk's `TempPath` stays alive until the iterator advances past it, even
///   if the producer dropped its handle. Fields drop in declaration order: the
///   active reader closes its file handle before the chunk it was opened from
///   is unlinked.
/// - `Merged` streams a `MergeSpilled` slot by lazily k-way-merging the adopted
///   `(u64, u64, u64)` runs and projecting each row to `(record, order)`, then
///   the trailing punctuations. The merge opens on the first record poll, so a
///   run-open failure surfaces as an `Err` item rather than at construction —
///   mirroring the chunked arm's lazy `file.reader()` open.
pub(crate) enum NodeBufferDrain {
    Chunked {
        mem: VecIntoIter<StreamEvent>,
        remaining_spills: VecIntoIter<(SpillFile<u64>, u64)>,
        // Boxed so the `Chunked` variant's size does not dwarf `Merged`'s: the
        // active `SpillReader` is bulky and only one is live at a time, and the
        // chunked path already does per-chunk file I/O, so the heap indirection
        // is free here.
        current: Option<Box<ActiveSpill>>,
        pending_puncts: VecIntoIter<Punctuation>,
    },
    Merged {
        /// Taken on the first record poll to open the merge; `None` afterward.
        runs: Option<Vec<SpillFile<(u64, u64, u64)>>>,
        merger: Option<SortedRunMerger<(u64, u64, u64)>>,
        pending_puncts: VecIntoIter<Punctuation>,
        /// Latches once a run-open or decode error has surfaced, so the drain
        /// stops rather than falling through to the trailing punctuations over a
        /// broken merge.
        done: bool,
        /// Charging context lent to the merge open so a fragmented adopted-run
        /// set folds down under the disk quota (E320) at drain.
        merge_budget: OwnedMergeBudget,
    },
}

pub(crate) struct ActiveSpill {
    reader: SpillReader<u64>,
    // Holds the file alive while `reader` streams it.
    _file: SpillFile<u64>,
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
                        "iejoin block-band output merge",
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
/// turn and, for a resident `Memory` slot with a single drain consumer,
/// spills it through `NodeBuffer::spill_resident_memory` (postcard,
/// optionally LZ4-framed, via `SpillWriter<u64>`). A non-spillable slot
/// (fan-out / composition input-port edge, whose consumer would reach
/// `clone_memory_only`) is skipped by that sweep and hard-gated at
/// admission instead.
///
/// `spill_priority = 0`: cheapest victim. Inter-stage buffers are
/// already row-oriented and write straight through `SpillWriter<u64>`;
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

    fn spill_chunk(rows: Vec<(Record, u64)>) -> (SpillFile<u64>, u64) {
        let s = if let Some(first) = rows.first() {
            Arc::clone(first.0.schema())
        } else {
            schema()
        };
        let mut w: SpillWriter<u64> = SpillWriter::new(s, None, true).unwrap();
        let count = rows.len() as u64;
        for (r, rn) in &rows {
            w.write_pair(r, rn).unwrap();
        }
        (w.finish().unwrap(), count)
    }

    fn rec_row_num(e: &StreamEvent) -> u64 {
        match e {
            StreamEvent::Record(_, rn) => *rn,
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
                StreamEvent::Record(_, rn) => Some(*rn),
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
        let row_bytes_each =
            std::mem::size_of::<Value>() * s.column_count() + std::mem::size_of::<(Record, u64)>();

        // Memory: record count × per-row formula; puncts don't count.
        let mem = NodeBuffer::Memory(vec![
            rec_event(&s, 1, "a", 1),
            StreamEvent::punctuation(Punctuation::document_close(Arc::clone(&ctx))),
            rec_event(&s, 2, "b", 2),
            rec_event(&s, 3, "c", 3),
        ]);
        assert_eq!(mem.estimated_memory_bytes(), (row_bytes_each * 3) as u64);

        // Spilled: zero bytes here — the disk surface tracks them
        // separately through `MemoryArbitrator::cumulative_spill_bytes`.
        let spilled = NodeBuffer::Spilled {
            chunks: vec![spill_chunk(vec![(rec(&s, 1, "a"), 1)])],
            pending_puncts: Vec::new(),
        };
        assert_eq!(spilled.estimated_memory_bytes(), 0);

        // Empty mem reports zero.
        assert_eq!(NodeBuffer::Memory(Vec::new()).estimated_memory_bytes(), 0);
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

    #[test]
    fn drain_split_metered_aborts_when_rematerialization_exceeds_budget() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        let s = schema();
        // >1024 records so the per-batch poll fires at least once. Spilled to
        // disk, so the metered drain re-materializes them off disk uncharged
        // absent the gate.
        let rows: Vec<(Record, u64)> = (0..1100).map(|i| (rec(&s, i, "v"), i as u64)).collect();
        let nb = NodeBuffer::Spilled {
            chunks: vec![spill_chunk(rows)],
            pending_puncts: Vec::new(),
        };
        // A 1-byte hard limit: the growing re-materialized vector crosses it
        // at the first 1024-record poll.
        let arb = MemoryArbitrator::with_policy(1, 0.80, 0.70, Box::new(NoOpPolicy));
        match nb.drain_split_metered(&arb, "spilled_stage") {
            Err(PipelineError::MemoryBudgetExceeded { node, source, .. }) => {
                assert_eq!(node, "spilled_stage", "the error names the draining stage");
                assert_eq!(
                    source,
                    clinker_plan::BudgetCategory::NodeBuffer,
                    "the re-materialized node-buffer drain is tagged NodeBuffer"
                );
            }
            other => panic!("expected E310 NodeBuffer; got: {other:?}"),
        }
    }

    #[test]
    fn drain_split_metered_matches_drain_split_under_ample_budget() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        let s = schema();
        let ctx = synthetic_document_context();
        // >1024 rows so the metered poll fires but a 100 GiB limit keeps it
        // from aborting; the output must match the plain drain exactly.
        let rows: Vec<(Record, u64)> = (0..1100).map(|i| (rec(&s, i, "v"), i as u64)).collect();
        let make = || NodeBuffer::Spilled {
            chunks: vec![spill_chunk(rows.clone())],
            pending_puncts: vec![Punctuation::document_close(Arc::clone(&ctx))],
        };
        let arb = MemoryArbitrator::with_policy(
            100 * 1024 * 1024 * 1024,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        );
        let (metered_recs, metered_puncts) = make()
            .drain_split_metered(&arb, "stage")
            .expect("ample-budget metered drain");
        let (plain_recs, plain_puncts) = make().drain_split().expect("plain drain");
        let metered_rns: Vec<u64> = metered_recs.iter().map(|(_, rn)| *rn).collect();
        let plain_rns: Vec<u64> = plain_recs.iter().map(|(_, rn)| *rn).collect();
        assert_eq!(
            metered_rns, plain_rns,
            "metered drain yields the same records in the same order as the plain drain"
        );
        assert_eq!(
            metered_puncts.len(),
            plain_puncts.len(),
            "metered drain preserves the trailing punctuations"
        );
    }

    /// A registered consumer that reports a fixed charged footprint, so a
    /// test can stage `sum_consumer_usage()` at a chosen value without
    /// standing up a real spilling operator. `try_spill` is unreachable here:
    /// `drain_split_metered` only reads `hard_limit()` and
    /// `sum_consumer_usage()`, never selects a victim.
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
    fn drain_split_metered_aborts_on_joint_consumer_plus_rematerialization_overshoot() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        let s = schema();
        // >1024 records so exactly one per-batch poll fires, at 1024 accumulated
        // records; the re-materialized footprint at that poll is deterministic.
        let rows: Vec<(Record, u64)> = (0..1100).map(|i| (rec(&s, i, "v"), i as u64)).collect();
        let nb = NodeBuffer::Spilled {
            chunks: vec![spill_chunk(rows)],
            pending_puncts: Vec::new(),
        };

        // Footprint the metered drain estimates at its first (and only) poll.
        let poll_footprint = record_byte_cost(s.column_count()) * 1024;
        // A consumer already charged exactly one footprint, and a hard limit
        // halfway between one footprint and two. The re-materialized vector
        // alone stays under the limit and the consumer alone stays under it,
        // but their sum (two footprints) breaches it — the joint-overshoot the
        // sum-aware gate exists to catch, invisible to a bytes-only test.
        let hard = poll_footprint + poll_footprint / 2;
        let arb = MemoryArbitrator::with_policy(hard, 0.80, 0.70, Box::new(NoOpPolicy));
        arb.register_consumer(Arc::new(FixedUsageConsumer(poll_footprint)));

        match nb.drain_split_metered(&arb, "joint_stage") {
            Err(PipelineError::MemoryBudgetExceeded {
                node,
                source,
                used,
                limit,
                ..
            }) => {
                assert_eq!(node, "joint_stage", "the error names the draining stage");
                assert_eq!(
                    source,
                    clinker_plan::BudgetCategory::NodeBuffer,
                    "a re-materialized node-buffer drain is tagged NodeBuffer"
                );
                assert_eq!(limit, hard, "the reported limit is the hard budget");
                assert_eq!(
                    used,
                    poll_footprint * 2,
                    "used is the re-materialized footprint PLUS the charged consumer, \
                     not either side alone"
                );
                assert!(
                    poll_footprint < hard,
                    "precondition: the re-materialized footprint alone is under the limit"
                );
            }
            other => panic!(
                "a joint consumer + re-materialization overshoot must abort E310 NodeBuffer; \
                 got: {other:?}"
            ),
        }
    }

    #[test]
    fn drain_split_metered_completes_when_joint_footprint_fits() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        let s = schema();
        let rows: Vec<(Record, u64)> = (0..1100).map(|i| (rec(&s, i, "v"), i as u64)).collect();
        let nb = NodeBuffer::Spilled {
            chunks: vec![spill_chunk(rows)],
            pending_puncts: Vec::new(),
        };

        let poll_footprint = record_byte_cost(s.column_count()) * 1024;
        // The same charged consumer, but a budget generous enough that the
        // joint footprint (two footprints) still fits: each side individually
        // under AND their sum under → the drain completes and yields every
        // record. Guards against the gate over-firing on a legitimate drain.
        let hard = poll_footprint * 3;
        let arb = MemoryArbitrator::with_policy(hard, 0.80, 0.70, Box::new(NoOpPolicy));
        arb.register_consumer(Arc::new(FixedUsageConsumer(poll_footprint)));

        let (recs, _puncts) = nb
            .drain_split_metered(&arb, "joint_stage")
            .expect("a joint footprint under the budget must complete");
        assert_eq!(
            recs.len(),
            1100,
            "every re-materialized record drains through when the joint footprint fits"
        );
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
        let payloads: Vec<(u64, u64, u64)> = vec![
            (5, 0, 0),
            (2, 1, 0),
            (5, 2, 0),
            (2, 3, 0),
            (9, 4, 0),
            (0, 5, 0),
            (2, 6, 0),
            (5, 7, 0),
        ];
        let row_count = payloads.len() as u64;

        // budget=1 is the spill-everything threshold; explicit flushes between
        // chunks force several individually-sorted runs. Each returned byte count
        // is charged exactly once, as the emit phase charges its runs.
        let mut buf: SortBuffer<(u64, u64, u64)> =
            SortBuffer::new_payload_ordered(1, None, true, s.clone());
        let push_chunk = |buf: &mut SortBuffer<(u64, u64, u64)>, chunk: &[(u64, u64, u64)]| {
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
        let expected_orders: Vec<u64> = oracle.iter().map(|(order, _, _)| *order).collect();
        let drained_ids: Vec<i64> = drained
            .iter()
            .map(|(r, _)| match r.get("id") {
                Some(Value::Integer(n)) => *n,
                other => panic!("expected Integer id, got {other:?}"),
            })
            .collect();
        let drained_orders: Vec<u64> = drained.iter().map(|(_, order)| *order).collect();
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
}
