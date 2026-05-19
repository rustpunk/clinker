//! Inter-stage handoff storage for `ExecutorContext::node_buffers`.
//!
//! A single `NodeBuffer` slot can hold:
//!
//! - `Memory`: rows accumulated entirely in RAM.
//! - `Spilled`: zero or more on-disk spill files, each paired with its
//!   recorded row count.
//! - `Mixed`: a mem tail accumulated after a partial spill.
//!
//! Every consumer drains a slot through [`NodeBuffer::drain`], which
//! returns an iterator that streams memory rows first, then per-spill
//! rows via `SpillReader` without materializing the spill into RAM.
//! Spill construction is gated behind a future sub-issue of #108; at
//! present no runtime producer emits `Spilled` or `Mixed`, but the
//! type's shape is already the one the spill-trigger logic will use.

use std::vec::IntoIter as VecIntoIter;

use clinker_record::Record;

use crate::error::PipelineError;
use crate::pipeline::spill::{SpillFile, SpillReader};

/// One slot inside `ExecutorContext::node_buffers`.
pub(crate) enum NodeBuffer {
    /// All rows live in memory.
    Memory(Vec<(Record, u64)>),
    /// Every row lives on disk. Each chunk pairs a spill file with the
    /// number of rows that producer wrote to it (used by `len_hint`
    /// and by the memory-charge accounting layer that the spill-wiring
    /// sub-issue introduces). No runtime path constructs this variant
    /// yet; tests cover the drain, promotion, and Drop semantics.
    #[allow(dead_code)]
    Spilled(Vec<(SpillFile<u64>, u64)>),
    /// A mem tail accumulated after a partial spill. Drain order is
    /// memory rows first, then spill chunks in vector order.
    Mixed {
        mem: Vec<(Record, u64)>,
        spills: Vec<(SpillFile<u64>, u64)>,
    },
}

impl NodeBuffer {
    /// Total record count across memory and recorded spill chunks.
    ///
    /// Used by consumer call-sites that want a `Vec::with_capacity`
    /// pre-allocation hint without consuming the buffer. Cheap on
    /// every variant — spill chunks carry their row count alongside
    /// the file handle, so no disk scan is required.
    pub(crate) fn len_hint(&self) -> usize {
        match self {
            Self::Memory(v) => v.len(),
            Self::Spilled(chunks) => chunks.iter().map(|(_, c)| *c as usize).sum(),
            Self::Mixed { mem, spills } => {
                mem.len() + spills.iter().map(|(_, c)| *c as usize).sum::<usize>()
            }
        }
    }

    /// Append a single `(record, row_number)` pair to the in-memory tail.
    ///
    /// On `Memory` and `Mixed`, the pair is pushed onto the existing
    /// mem `Vec`. On `Spilled`, the variant is promoted to `Mixed`
    /// with the new pair as the sole mem tail. Producers that already
    /// accumulate a `Vec` and then insert via `NodeBuffer::Memory(vec)`
    /// remain the dominant pattern; `push` exists so spill-trigger
    /// logic can resume in-memory accumulation after a partial spill.
    pub(crate) fn push(&mut self, record: Record, rn: u64) {
        match self {
            Self::Memory(v) => v.push((record, rn)),
            Self::Mixed { mem, .. } => mem.push((record, rn)),
            Self::Spilled(_) => {
                let chunks = match std::mem::replace(self, Self::Memory(Vec::new())) {
                    Self::Spilled(c) => c,
                    _ => unreachable!(),
                };
                *self = Self::Mixed {
                    mem: vec![(record, rn)],
                    spills: chunks,
                };
            }
        }
    }

    /// Non-consuming borrow of the in-memory rows.
    ///
    /// Returns `&[]` on a pure `Spilled` slot — callers that need
    /// schema-style validation of every row in a spilled buffer must
    /// instead drain through [`Self::drain`]. The schema-check
    /// call-site this is wired into today operates only on memory-
    /// resident rows; spill-aware pre-flight validation is part of
    /// the spill-wiring sub-issue.
    pub(crate) fn peek_mem(&self) -> &[(Record, u64)] {
        match self {
            Self::Memory(v) => v.as_slice(),
            Self::Mixed { mem, .. } => mem.as_slice(),
            Self::Spilled(_) => &[],
        }
    }

    /// Deep-clone the in-memory rows for a multi-consumer fan-out site.
    ///
    /// # Panics
    ///
    /// Panics on `Spilled` and `Mixed`. Spill chunks cannot be
    /// cheap-cloned; the only legitimate multi-consumer access for a
    /// spilled buffer is to drain it. The producer-side fan-out arm
    /// in `dispatch.rs` uses this when there is at least one
    /// remaining downstream consumer; today every reachable
    /// `NodeBuffer` is `Memory`, so the panic path is unreachable at
    /// runtime.
    pub(crate) fn clone_memory_only(&self) -> Vec<(Record, u64)> {
        match self {
            Self::Memory(v) => v.clone(),
            Self::Spilled(_) | Self::Mixed { .. } => {
                panic!(
                    "NodeBuffer::clone_memory_only called on a spill-backed \
                     variant; spilled rows cannot be cloned for multi-consumer \
                     fanout. Drain through NodeBuffer::drain instead.",
                );
            }
        }
    }

    /// Consume the buffer, returning an iterator that yields memory
    /// rows first and then per-spill-file rows in vector order.
    ///
    /// Spill rows stream from disk via `SpillReader<u64>` without
    /// materializing the spill. Spill-open and per-row decode failures
    /// surface as `PipelineError::Spill` items so the executor's
    /// existing `?`-bubble path applies unchanged.
    pub(crate) fn drain(self) -> NodeBufferDrain {
        let (mem, spills) = match self {
            Self::Memory(v) => (v, Vec::new()),
            Self::Spilled(s) => (Vec::new(), s),
            Self::Mixed { mem, spills } => (mem, spills),
        };
        NodeBufferDrain {
            mem: mem.into_iter(),
            remaining_spills: spills.into_iter(),
            current: None,
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
    mem: VecIntoIter<(Record, u64)>,
    remaining_spills: VecIntoIter<(SpillFile<u64>, u64)>,
    current: Option<ActiveSpill>,
}

struct ActiveSpill {
    reader: SpillReader<u64>,
    // Holds the file alive while `reader` streams it.
    _file: SpillFile<u64>,
}

impl Iterator for NodeBufferDrain {
    type Item = Result<(Record, u64), PipelineError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(pair) = self.mem.next() {
            return Some(Ok(pair));
        }
        loop {
            if let Some(curr) = self.current.as_mut() {
                match curr.reader.next() {
                    Some(Ok(pair)) => return Some(Ok(pair)),
                    Some(Err(e)) => return Some(Err(PipelineError::from(e))),
                    None => self.current = None,
                }
            }
            let (file, _count) = self.remaining_spills.next()?;
            let reader = match file.reader() {
                Ok(r) => r,
                Err(e) => return Some(Err(PipelineError::from(e))),
            };
            self.current = Some(ActiveSpill {
                reader,
                _file: file,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use clinker_record::{Schema, Value};

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

    #[test]
    fn memory_push_drain_round_trip() {
        let s = schema();
        let mut nb = NodeBuffer::Memory(Vec::new());
        nb.push(rec(&s, 1, "a"), 10);
        nb.push(rec(&s, 2, "b"), 11);

        assert_eq!(nb.len_hint(), 2);

        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].1, 10);
        assert_eq!(drained[1].1, 11);
    }

    #[test]
    fn spilled_drains_in_file_order() {
        let s = schema();
        let chunk_a = spill_chunk(vec![(rec(&s, 1, "a"), 1), (rec(&s, 2, "b"), 2)]);
        let chunk_b = spill_chunk(vec![(rec(&s, 3, "c"), 3)]);
        let nb = NodeBuffer::Spilled(vec![chunk_a, chunk_b]);

        assert_eq!(nb.len_hint(), 3);

        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].1, 1);
        assert_eq!(drained[1].1, 2);
        assert_eq!(drained[2].1, 3);
    }

    #[test]
    fn mixed_drains_memory_before_spills() {
        let s = schema();
        let chunk = spill_chunk(vec![(rec(&s, 100, "spill-row"), 100)]);
        let nb = NodeBuffer::Mixed {
            mem: vec![(rec(&s, 1, "mem-a"), 1), (rec(&s, 2, "mem-b"), 2)],
            spills: vec![chunk],
        };

        assert_eq!(nb.len_hint(), 3);

        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].1, 1);
        assert_eq!(drained[1].1, 2);
        assert_eq!(drained[2].1, 100);
    }

    #[test]
    fn len_hint_matches_drained_count_on_each_variant() {
        let s = schema();
        let mem = NodeBuffer::Memory(vec![
            (rec(&s, 1, "a"), 1),
            (rec(&s, 2, "b"), 2),
            (rec(&s, 3, "c"), 3),
        ]);
        assert_eq!(mem.len_hint(), 3);
        assert_eq!(mem.drain().count(), 3);

        let spilled = NodeBuffer::Spilled(vec![spill_chunk(vec![
            (rec(&s, 1, "a"), 1),
            (rec(&s, 2, "b"), 2),
        ])]);
        assert_eq!(spilled.len_hint(), 2);
        assert_eq!(spilled.drain().count(), 2);

        let mixed = NodeBuffer::Mixed {
            mem: vec![(rec(&s, 1, "m"), 1)],
            spills: vec![spill_chunk(vec![(rec(&s, 2, "s"), 2)])],
        };
        assert_eq!(mixed.len_hint(), 2);
        assert_eq!(mixed.drain().count(), 2);
    }

    #[test]
    fn peek_mem_returns_mem_slice_or_empty() {
        let s = schema();
        let mem = NodeBuffer::Memory(vec![(rec(&s, 1, "a"), 1)]);
        assert_eq!(mem.peek_mem().len(), 1);

        let spilled = NodeBuffer::Spilled(vec![spill_chunk(vec![(rec(&s, 1, "a"), 1)])]);
        assert!(spilled.peek_mem().is_empty());

        let mixed = NodeBuffer::Mixed {
            mem: vec![(rec(&s, 1, "m"), 1)],
            spills: vec![spill_chunk(vec![(rec(&s, 2, "s"), 2)])],
        };
        assert_eq!(mixed.peek_mem().len(), 1);
        assert_eq!(mixed.peek_mem()[0].1, 1);
    }

    #[test]
    fn clone_memory_only_returns_memory_clone() {
        let s = schema();
        let nb = NodeBuffer::Memory(vec![(rec(&s, 1, "a"), 1), (rec(&s, 2, "b"), 2)]);
        let cloned = nb.clone_memory_only();
        assert_eq!(cloned.len(), 2);
        assert_eq!(cloned[1].1, 2);
        // Original still drains its rows.
        assert_eq!(nb.drain().count(), 2);
    }

    #[test]
    #[should_panic(expected = "spill-backed variant")]
    fn clone_memory_only_panics_on_spilled() {
        let s = schema();
        let nb = NodeBuffer::Spilled(vec![spill_chunk(vec![(rec(&s, 1, "a"), 1)])]);
        let _ = nb.clone_memory_only();
    }

    #[test]
    #[should_panic(expected = "spill-backed variant")]
    fn clone_memory_only_panics_on_mixed() {
        let s = schema();
        let nb = NodeBuffer::Mixed {
            mem: vec![(rec(&s, 1, "m"), 1)],
            spills: vec![spill_chunk(vec![(rec(&s, 2, "s"), 2)])],
        };
        let _ = nb.clone_memory_only();
    }

    #[test]
    fn push_on_spilled_promotes_to_mixed() {
        let s = schema();
        let mut nb = NodeBuffer::Spilled(vec![spill_chunk(vec![(rec(&s, 100, "s"), 100)])]);
        nb.push(rec(&s, 1, "after-spill"), 200);

        assert!(matches!(nb, NodeBuffer::Mixed { .. }));
        assert_eq!(nb.len_hint(), 2);

        let drained: Vec<_> = nb.drain().collect::<Result<_, _>>().unwrap();
        // mem tail drains first per the documented order.
        assert_eq!(drained[0].1, 200);
        assert_eq!(drained[1].1, 100);
    }

    #[test]
    fn spilled_drop_unlinks_temp_files() {
        let s = schema();
        let (file, _) = spill_chunk(vec![(rec(&s, 1, "a"), 1)]);
        let path = file.path().to_path_buf();
        assert!(path.exists());

        let nb = NodeBuffer::Spilled(vec![(file, 1)]);
        drop(nb);

        assert!(!path.exists());
    }

    #[test]
    fn empty_variants_have_zero_len_hint() {
        let s = schema();
        assert_eq!(NodeBuffer::Memory(Vec::new()).len_hint(), 0);
        assert_eq!(NodeBuffer::Spilled(Vec::new()).len_hint(), 0);
        assert_eq!(
            NodeBuffer::Mixed {
                mem: Vec::new(),
                spills: Vec::new(),
            }
            .len_hint(),
            0
        );
        assert_eq!(NodeBuffer::Memory(vec![(rec(&s, 1, "a"), 1)]).len_hint(), 1);
    }
}
