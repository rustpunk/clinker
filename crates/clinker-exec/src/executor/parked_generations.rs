//! Rows parked for a deferred (relaxed-key) consumer until the commit.
//!
//! When a relaxed-key pipeline defers part of its DAG to the commit, a node
//! outside a deferred region that feeds a member of it cannot hand its rows
//! over on the forward pass: the member runs only at the commit, and possibly
//! several times there, once per retraction iteration. The producer's rows for
//! each such crossing edge are parked here instead, and every iteration of the
//! commit reads them again through a fresh cursor.
//!
//! Each crossing edge keeps its rows as an ordered list of [`NodeBuffer`]
//! segments, one per park, so arrival order survives any mix of resident and
//! spilled segments. The edge's resident rows are charged to the run's ledger
//! through one consumer registered under the producer's name; a park grows
//! that charge before its rows become resident, which on the walk reclaims
//! other state first, and a park that still does not fit spills the edge's
//! own resident segments and then, if need be, writes its rows straight to
//! disk. A spill is recorded against the producer once, when it is written;
//! reading it again on a later iteration charges nothing more.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use clinker_plan::config::CompressMode;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::CompositionBodyId;
use clinker_plan::runtime_error::{ConsumerLabel, MemorySurface};
use clinker_record::Record;
use petgraph::graph::EdgeIndex;

use crate::executor::node_buffer::{NodeBuffer, NodeBufferConsumer};
use crate::executor::stream_event::SourceRowId;
use crate::pipeline::memory::{ConsumerHandle, ConsumerId, MemoryArbitrator};

/// A crossing edge: the composition body whose graph the edge belongs to
/// (`None` at the top level, where edge ids have their own namespace), and
/// the edge.
pub(crate) type ParkedKey = (Option<CompositionBodyId>, EdgeIndex);

/// The run's parked cross-region rows, by crossing edge.
///
/// Run-scoped and walk-only. Every edge's consumer is unregistered and every
/// spill file removed when the store is released ([`Self::release_all`]) or
/// dropped, so no registration and no file outlives the run on any exit.
pub(crate) struct ParkedGenerations {
    arbitrator: Arc<MemoryArbitrator>,
    spill_root: Arc<Path>,
    spill_compress: CompressMode,
    batch_size: usize,
    forward: HashMap<ParkedKey, ParkedEdge>,
}

/// One crossing edge's parked rows and the consumer that charges them.
struct ParkedEdge {
    consumer: ConsumerId,
    handle: Arc<ConsumerHandle>,
    /// The producer's name: the edge's consumer is registered, and its spill
    /// recorded, under it.
    producer: Box<str>,
    /// In the order they were parked.
    segments: Vec<ParkedSegment>,
}

/// One run of parked rows.
struct ParkedSegment {
    buffer: NodeBuffer,
    /// What this segment has charged to the edge's handle while resident; 0
    /// once it is on disk.
    charged: u64,
    /// Spill-file bytes recorded against the producer for this segment,
    /// released from the disk quota when the segment is.
    file_bytes: u64,
}

impl ParkedEdge {
    /// Record, on the edge's handle, what a spill of the edge would free now.
    fn refresh_reclaimable(&self) {
        self.handle
            .set_reclaimable(self.segments.iter().map(|segment| segment.charged).sum());
    }
}

impl ParkedGenerations {
    /// An empty store charging `arbitrator` and spilling under `spill_root`.
    pub(crate) fn new(
        arbitrator: Arc<MemoryArbitrator>,
        spill_root: Arc<Path>,
        spill_compress: CompressMode,
        batch_size: usize,
    ) -> Self {
        Self {
            arbitrator,
            spill_root,
            spill_compress,
            batch_size,
            forward: HashMap::new(),
        }
    }

    /// Park a copy of `rows` for the crossing edge `key`, from the node named
    /// `from` into the deferred consumer named `to`.
    ///
    /// The first park on an edge registers its consumer, even for no rows, so
    /// the commit reads an empty input rather than a missing one. The copy's
    /// resident size is grown on the edge's handle with no borrow of the
    /// store held, so a reclaim that growth starts on the walk can spill any
    /// parked edge, this one included. When it still does not fit, the edge's
    /// own resident segments spill and the growth is retried once; if that
    /// falls short too, the rows are written straight to disk. Parking is
    /// never refused for memory; past the spill cap it fails with E320.
    pub(crate) fn park(
        store: &Rc<RefCell<Self>>,
        key: ParkedKey,
        rows: &[(Record, SourceRowId)],
        from: &str,
        to: &str,
    ) -> Result<(), PipelineError> {
        let handle = store.borrow_mut().edge_handle(key, from, to);
        if rows.is_empty() {
            return Ok(());
        }
        let segment = NodeBuffer::memory_from_records(
            rows.iter()
                .map(|(record, row)| (record.clone(), *row))
                .collect::<Vec<_>>(),
        );
        // A clone copies every value into storage no other consumer charges,
        // so the copy's resident size is its slots plus its whole payload.
        let bytes = segment.reclaimable_bytes();
        let charged = if handle.try_grow(bytes).is_ok() {
            bytes
        } else {
            store.borrow_mut().spill_edge(&key)?;
            if handle.try_grow(bytes).is_ok() {
                bytes
            } else {
                0
            }
        };
        store.borrow_mut().append(key, segment, charged)
    }

    /// The consumer handle of edge `key`, registering the edge on first use.
    fn edge_handle(&mut self, key: ParkedKey, from: &str, to: &str) -> Arc<ConsumerHandle> {
        if let Some(edge) = self.forward.get(&key) {
            return Arc::clone(&edge.handle);
        }
        let handle = ConsumerHandle::new();
        let consumer = self.arbitrator.register_node_consumer(
            Arc::new(NodeBufferConsumer::new(Arc::clone(&handle))),
            Arc::clone(&handle),
            ConsumerLabel {
                node: from.to_string(),
                surface: MemorySurface::ParkedCrossRegionRows {
                    from: from.to_string(),
                    to: to.to_string(),
                },
            },
        );
        self.forward.insert(
            key,
            ParkedEdge {
                consumer,
                handle: Arc::clone(&handle),
                producer: Box::from(from),
                segments: Vec::new(),
            },
        );
        handle
    }

    /// Add `segment`, whose resident size `charged` bytes the edge's handle
    /// already holds, after edge `key`'s other segments. A segment nothing
    /// was charged for is written to disk first.
    fn append(
        &mut self,
        key: ParkedKey,
        segment: NodeBuffer,
        charged: u64,
    ) -> Result<(), PipelineError> {
        let (segment, file_bytes) = if charged == 0 {
            self.write_to_disk(segment)?
        } else {
            (segment, 0)
        };
        let producer = self
            .forward
            .get(&key)
            .map(|edge| edge.producer.clone())
            .ok_or_else(|| unregistered_edge(&key))?;
        // The file is kept with its segment even past the spill cap, so its
        // quota bytes are released with the segment like any other's.
        let recorded = self.record_spill(&producer, file_bytes);
        let edge = self
            .forward
            .get_mut(&key)
            .ok_or_else(|| unregistered_edge(&key))?;
        match (edge.segments.last_mut(), segment) {
            // Rows parked again while the last segment is still resident and
            // unread join it, keeping one run per resident stretch.
            (
                Some(ParkedSegment {
                    buffer: NodeBuffer::Memory(resident),
                    charged: last_charged,
                    ..
                }),
                NodeBuffer::Memory(events),
            ) if charged > 0 => {
                resident.extend(events);
                *last_charged += charged;
            }
            (_, buffer) => edge.segments.push(ParkedSegment {
                buffer,
                charged,
                file_bytes,
            }),
        }
        edge.refresh_reclaimable();
        recorded
    }

    /// Spill every resident segment of edge `key` that no cursor is reading,
    /// releasing its charge; returns the bytes released. A segment a live
    /// cursor shares stays resident: writing it out would free nothing.
    pub(crate) fn spill_edge(&mut self, key: &ParkedKey) -> Result<u64, PipelineError> {
        let (root, compress, batch_size) = (
            Arc::clone(&self.spill_root),
            self.spill_compress,
            self.batch_size,
        );
        let Some(edge) = self.forward.get_mut(key) else {
            return Ok(0);
        };
        let mut freed = 0u64;
        let mut written = 0u64;
        let mut failure = None;
        for segment in &mut edge.segments {
            if segment.charged == 0 {
                continue;
            }
            let buffer = std::mem::replace(&mut segment.buffer, NodeBuffer::Memory(Vec::new()));
            let compress =
                compress.resolve_for_schema(buffer.first_record_column_count(), batch_size as u64);
            match buffer.spill_resident_memory(Some(root.as_ref()), compress) {
                Ok((spilled, file_bytes)) => {
                    let on_disk = matches!(spilled, NodeBuffer::Spilled { .. });
                    segment.buffer = spilled;
                    if on_disk {
                        edge.handle.shrink(segment.charged);
                        freed += segment.charged;
                        segment.charged = 0;
                        segment.file_bytes += file_bytes;
                        written += file_bytes;
                    }
                }
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        edge.refresh_reclaimable();
        let producer = edge.producer.clone();
        let recorded = self.record_spill(&producer, written);
        match failure {
            Some(error) => Err(error),
            None => recorded.map(|()| freed),
        }
    }

    /// Write `segment` to one spill file; returns the spilled segment and
    /// the file's bytes.
    fn write_to_disk(&self, segment: NodeBuffer) -> Result<(NodeBuffer, u64), PipelineError> {
        let compress = self
            .spill_compress
            .resolve_for_schema(segment.first_record_column_count(), self.batch_size as u64);
        segment.spill_resident_memory(Some(self.spill_root.as_ref()), compress)
    }

    /// Charge `bytes` written for `producer` to the disk quota, failing with
    /// the spill-cap error (E320) past it.
    fn record_spill(&self, producer: &str, bytes: u64) -> Result<(), PipelineError> {
        if bytes > 0 && self.arbitrator.record_spill_bytes(producer, bytes) {
            return Err(PipelineError::spill_cap_exceeded(
                producer,
                self.arbitrator.max_spill_bytes(),
                bytes,
                self.arbitrator.cumulative_spill_bytes(),
            ));
        }
        Ok(())
    }

    /// A cursor over every row parked for edge `key`, segment by segment in
    /// the order they were parked; `None` when nothing was ever parked for
    /// it. The segments stay here, so every later call reads the same rows
    /// again. The cursor shares the segments' backing: it adds no charge, and
    /// while it lives a spill of the edge cannot free the segments it reads.
    pub(crate) fn publish_view(
        &mut self,
        key: &ParkedKey,
    ) -> Result<Option<NodeBuffer>, PipelineError> {
        let Some(edge) = self.forward.get_mut(key) else {
            return Ok(None);
        };
        let mut parts = Vec::with_capacity(edge.segments.len());
        for segment in &mut edge.segments {
            parts.push(segment.buffer.reread_backing()?);
        }
        edge.refresh_reclaimable();
        Ok(Some(NodeBuffer::chained(parts)))
    }

    /// Edge `key`'s consumer and the handle it charges.
    #[cfg(test)]
    pub(crate) fn edge_consumer(
        &self,
        key: &ParkedKey,
    ) -> Option<(ConsumerId, Arc<ConsumerHandle>)> {
        self.forward
            .get(key)
            .map(|edge| (edge.consumer, Arc::clone(&edge.handle)))
    }

    /// Release every parked edge: drop its rows and spill files, release its
    /// charge and its disk-quota bytes, and unregister its consumer.
    pub(crate) fn release_all(&mut self) {
        for (_, edge) in std::mem::take(&mut self.forward) {
            release_edge(&self.arbitrator, edge);
        }
    }
}

impl Drop for ParkedGenerations {
    fn drop(&mut self) {
        self.release_all();
    }
}

/// Drop `edge`'s segments, then release what it held and unregister it.
fn release_edge(arbitrator: &MemoryArbitrator, edge: ParkedEdge) {
    let ParkedEdge {
        consumer,
        handle,
        producer,
        segments,
    } = edge;
    let file_bytes: u64 = segments.iter().map(|segment| segment.file_bytes).sum();
    drop(segments);
    arbitrator.release_spill_bytes(&producer, file_bytes);
    handle.shrink(handle.bytes());
    arbitrator.unregister_consumer(consumer);
}

#[cold]
fn unregistered_edge(key: &ParkedKey) -> PipelineError {
    PipelineError::Internal {
        op: "executor",
        node: format!("edge-{}", key.1.index()),
        detail: "parked cross-region rows were added to an edge that was never registered"
            .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::Priority;
    use crate::pipeline::memory::ledger::{PassKind, Requester};
    use crate::pipeline::memory::walk::{WalkContextGuard, WalkReclaimSet, WalkSpillSettings};
    use clinker_plan::plan::EntityRef;
    use clinker_record::owned_storage::SharedStorage;
    use clinker_record::{Schema, Value};

    fn rows(first: u64, count: u64) -> Vec<(Record, SourceRowId)> {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "payload".into()])));
        (first..first + count)
            .map(|row| {
                (
                    Record::new(
                        schema.clone(),
                        vec![
                            Value::Integer(row as i64),
                            Value::String(format!("parked-{row:06}").repeat(8).into()),
                        ],
                    ),
                    SourceRowId::new(clinker_plan::plan::PlanNodeId::new(3), row),
                )
            })
            .collect()
    }

    /// The bytes a park of `rows` charges: every row's slots and payload.
    fn resident_bytes(rows: &[(Record, SourceRowId)]) -> u64 {
        NodeBuffer::memory_from_records(rows.to_vec()).reclaimable_bytes()
    }

    /// One edge parks twice with a cursor over its first segment still open
    /// in between. The edge ranks by, and a pass electing it frees, only the
    /// segment no cursor shares; once the cursor closes the first segment
    /// counts again.
    #[test]
    fn reclaimable_excludes_segments_under_a_live_view() {
        let root = tempfile::tempdir().expect("spill root");
        let first = rows(0, 32);
        let second = rows(32, 16);
        let (first_bytes, second_bytes) = (resident_bytes(&first), resident_bytes(&second));
        let limit = first_bytes + second_bytes + 1024;
        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            limit,
            0.80,
            0.70,
            Box::new(Priority),
        ));
        let set = Rc::new(RefCell::new(WalkReclaimSet::new(WalkSpillSettings {
            spill_root: Arc::from(root.path()),
            spill_compress: CompressMode::Auto,
            batch_size: 1024,
        })));
        let store = Rc::new(RefCell::new(ParkedGenerations::new(
            Arc::clone(&arbitrator),
            Arc::from(root.path()),
            CompressMode::Auto,
            1024,
        )));
        set.borrow_mut().set_parked_generations(Rc::clone(&store));
        let _walk = WalkContextGuard::install(&arbitrator, Rc::clone(&set));
        let consumers_before = arbitrator.consumer_count();

        let key: ParkedKey = (None, EdgeIndex::new(4));
        ParkedGenerations::park(&store, key, &first, "lookup", "enriched").expect("first park");
        let view = store
            .borrow_mut()
            .publish_view(&key)
            .expect("view")
            .expect("the edge has rows");
        ParkedGenerations::park(&store, key, &second, "lookup", "enriched").expect("second park");
        let (consumer, handle) = store.borrow().edge_consumer(&key).expect("registered");
        assert_eq!(handle.bytes(), first_bytes + second_bytes);
        let registered = arbitrator
            .registered_consumer(consumer)
            .expect("the edge's consumer is registered");
        assert_eq!(
            registered.reclaimable_bytes(),
            second_bytes,
            "a segment an open cursor shares cannot be freed by a spill, so it does not rank"
        );

        let outcome = arbitrator
            .reclaim_pass(
                second_bytes,
                Requester::governed(),
                &mut *set.borrow_mut(),
                PassKind::Ordinary,
            )
            .expect("the pass spills");
        assert_eq!(
            outcome.freed, second_bytes,
            "the pass frees exactly the unviewed segment"
        );
        assert_eq!(
            handle.bytes(),
            first_bytes,
            "the viewed segment stays charged"
        );
        assert!(arbitrator.per_stage_spill_bytes_written()["lookup"] > 0);
        assert_eq!(registered.reclaimable_bytes(), 0);

        drop(view);
        assert_eq!(
            registered.reclaimable_bytes(),
            first_bytes,
            "once the cursor closes, the first segment can be spilled again"
        );
        let reread: Vec<u64> = store
            .borrow_mut()
            .publish_view(&key)
            .expect("view")
            .expect("rows")
            .drain()
            .map(|event| match event.expect("read") {
                crate::executor::stream_event::StreamEvent::Record(_, row) => row.ordinal(),
                crate::executor::stream_event::StreamEvent::Punctuation(_) => u64::MAX,
            })
            .collect();
        assert_eq!(
            reread,
            (0..48).collect::<Vec<u64>>(),
            "both segments read back whole, in parking order"
        );

        store.borrow_mut().release_all();
        assert_eq!(arbitrator.consumer_count(), consumers_before);
        assert_eq!(
            arbitrator.cumulative_spill_bytes(),
            0,
            "released files leave the quota"
        );
    }
}
