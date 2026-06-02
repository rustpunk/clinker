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
//! `admit_node_buffer`.

use std::vec::IntoIter as VecIntoIter;

use clinker_record::{Record, Value};

use crate::error::PipelineError;
use crate::executor::stream_event::{Punctuation, StreamEvent};
use crate::pipeline::spill::{SpillFile, SpillReader};

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
    /// A mem tail accumulated after a partial spill. Drain order is
    /// mem events first, then spill chunks, then `pending_puncts`.
    Mixed {
        mem: Vec<StreamEvent>,
        spills: Vec<(SpillFile<u64>, u64)>,
        pending_puncts: Vec<Punctuation>,
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
            Self::Spilled { .. } => &[],
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
            Self::Spilled { .. } | Self::Mixed { .. } => {
                panic!(
                    "NodeBuffer::clone_memory_only called on a spill-backed \
                     variant; spilled rows cannot be cloned for multi-consumer \
                     fanout. Drain through NodeBuffer::drain instead.",
                );
            }
        }
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

    /// Consume the buffer, returning an iterator that yields memory
    /// events first, then per-spill-file records in vector order, and
    /// finally any trailing punctuations that did not spill.
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
        };
        NodeBufferDrain {
            mem: mem.into_iter(),
            remaining_spills: spills.into_iter(),
            current: None,
            pending_puncts: pending_puncts.into_iter(),
        }
    }
}

/// Iterator returned by [`NodeBuffer::drain`].
///
/// Owns the spill chunks so each chunk's `TempPath` stays alive until
/// the iterator advances past it, even if the producer dropped its
/// handle. Fields drop in declaration order: the active reader closes
/// its file handle before the chunk it was opened from is unlinked.
pub(crate) struct NodeBufferDrain {
    mem: VecIntoIter<StreamEvent>,
    remaining_spills: VecIntoIter<(SpillFile<u64>, u64)>,
    current: Option<ActiveSpill>,
    pending_puncts: VecIntoIter<Punctuation>,
}

struct ActiveSpill {
    reader: SpillReader<u64>,
    // Holds the file alive while `reader` streams it.
    _file: SpillFile<u64>,
}

impl Iterator for NodeBufferDrain {
    type Item = Result<StreamEvent, PipelineError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(event) = self.mem.next() {
            return Some(Ok(event));
        }
        loop {
            if let Some(curr) = self.current.as_mut() {
                match curr.reader.next() {
                    Some(Ok((rec, rn))) => return Some(Ok(StreamEvent::record(rec, rn))),
                    Some(Err(e)) => return Some(Err(PipelineError::from(e))),
                    None => self.current = None,
                }
            }
            if let Some((file, _count)) = self.remaining_spills.next() {
                let reader = match file.reader() {
                    Ok(r) => r,
                    Err(e) => return Some(Err(PipelineError::from(e))),
                };
                self.current = Some(ActiveSpill {
                    reader,
                    _file: file,
                });
                continue;
            }
            // Spill chunks exhausted — emit any trailing puncts.
            return self
                .pending_puncts
                .next()
                .map(|p| Ok(StreamEvent::punctuation(p)));
        }
    }
}

/// `MemoryConsumer` wrapper for one `ctx.node_buffers` slot. Holds an
/// `Arc<ConsumerHandle>` shared with the dispatcher: every producer
/// push updates `handle.bytes` to track `NodeBuffer::estimated_memory_bytes()`;
/// every consumer drain decrements it. `try_spill` flips the handle's
/// spill-request flag; the dispatcher reads it at the next admission
/// boundary and routes through the existing `spill_node_buffer`
/// (postcard + LZ4 via `SpillWriter<u64>`) path.
///
/// `spill_priority = 0`: cheapest victim. Inter-stage buffers are
/// already row-oriented and write straight through `SpillWriter<u64>`;
/// no per-group or per-run reconstruction needed on the consumer
/// side. Preferred first victim under `Priority` and
/// `BackPressurePreferred::wrapping(Priority)`.
///
/// `can_back_pressure` is dynamic: `true` when the slot's producer
/// chain terminates at a pauseable Source (no blocking operator
/// between), `false` otherwise. Stored on construction; the
/// dispatcher classifies the upstream topology at registration time
/// since the DAG is static.
pub struct NodeBufferConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
    back_pressureable: bool,
}

impl NodeBufferConsumer {
    pub fn new(
        handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
        back_pressureable: bool,
    ) -> Self {
        Self {
            handle,
            back_pressureable,
        }
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
        self.back_pressureable
    }

    fn pause(&self) {
        self.handle.pause();
    }

    fn resume(&self) {
        self.handle.resume();
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
        let mut w: SpillWriter<u64> = SpillWriter::new(s, None).unwrap();
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
    fn node_buffer_consumer_reports_handle_bytes() {
        use crate::pipeline::memory::{ConsumerHandle, MemoryConsumer};
        let handle = ConsumerHandle::new();
        handle.set_bytes(4096);
        let consumer = NodeBufferConsumer::new(handle.clone(), false);
        assert_eq!(consumer.current_usage(), 4096);
        assert_eq!(consumer.spill_priority(), 0);
        assert!(!consumer.can_back_pressure());
    }

    #[test]
    fn node_buffer_consumer_back_pressureable_routes_pause_to_handle() {
        use crate::pipeline::memory::{ConsumerHandle, MemoryConsumer};
        let handle = ConsumerHandle::new();
        let consumer = NodeBufferConsumer::new(handle.clone(), true);
        assert!(consumer.can_back_pressure());
        assert!(!handle.is_paused());
        consumer.pause();
        assert!(handle.is_paused());
        consumer.resume();
        assert!(!handle.is_paused());
    }

    #[test]
    fn node_buffer_consumer_try_spill_flags_handle_and_returns_freed_or_below_target() {
        use crate::pipeline::memory::{ConsumerHandle, ConsumerSpillError, MemoryConsumer};
        let handle = ConsumerHandle::new();
        handle.set_bytes(1024);
        let consumer = NodeBufferConsumer::new(handle.clone(), false);
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
}
