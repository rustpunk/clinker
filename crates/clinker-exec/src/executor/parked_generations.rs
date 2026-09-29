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
//! disk. Any reclaim pass on the walk can spill an edge the arbitrator
//! elects. A spill is recorded against the producer once, when it is
//! written; reading it again on a later iteration charges nothing more.
//!
//! Rows parked on the forward pass are read by every iteration of the
//! commit. Rows a region member parks during the commit pass itself, for a
//! member of another region, belong to that iteration only: they are kept
//! in a generation of their own, which the next iteration discards before it
//! parks its own.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};

use clinker_plan::config::CompressMode;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::CompositionBodyId;
use clinker_plan::runtime_error::{ConsumerLabel, MemorySurface};
use clinker_record::Record;
use petgraph::graph::EdgeIndex;

use crate::executor::node_buffer::{NodeBuffer, ReReadableNodeBuffer};
use crate::executor::stream_event::SourceRowId;
use crate::pipeline::memory::walk::{WalkOwnedRegistration, WalkOwnedSpill, register_walk_owned};
use crate::pipeline::memory::{ConsumerHandle, ConsumerId, ConsumerSpillError, MemoryArbitrator};

/// A crossing edge: the composition body whose graph the edge belongs to
/// (`None` at the top level, where edge ids have their own namespace), and
/// the edge.
pub(crate) type ParkedKey = (Option<CompositionBodyId>, EdgeIndex);

/// Which pass parked a crossing edge's rows, and so how long they live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Generation {
    /// Parked on the forward pass: read by every iteration of the commit.
    Forward,
    /// Parked by a region member during one iteration of the commit: read by
    /// that iteration only.
    CommitPass,
}

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
    /// Rows parked on the forward pass.
    forward: HashMap<ParkedKey, ParkedEdge>,
    /// Rows parked during the running iteration of the commit.
    commit_pass: HashMap<ParkedKey, ParkedEdge>,
}

/// One crossing edge's parked rows and the consumer that charges them.
struct ParkedEdge {
    consumer: ConsumerId,
    handle: Arc<ConsumerHandle>,
    /// What the edge's consumer ranks by, kept in step with `segments`.
    reclaim: Arc<ParkedEdgeConsumer>,
    /// The producer's name: the edge's consumer is registered, and its spill
    /// recorded, under it.
    producer: Box<str>,
    /// In the order they were parked.
    segments: Vec<ParkedSegment>,
    /// The store's entry in the walk reclaim set under this edge's consumer,
    /// made right after the consumer registers at the edge's first park; it
    /// leaves the set when the edge is released.
    walk_entry: Option<WalkOwnedRegistration>,
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
    /// Tell the edge's consumer which resident bytes a spill could free: every
    /// segment still charged, each with the backing a re-read cursor may
    /// share.
    fn publish_reclaimable(&self) {
        let resident = self
            .segments
            .iter()
            .filter(|segment| segment.charged > 0)
            .map(|segment| ResidentSegment {
                bytes: segment.charged,
                shared: match &segment.buffer {
                    NodeBuffer::ReReadable(backing) => Some(Arc::downgrade(backing)),
                    _ => None,
                },
            })
            .collect();
        *self
            .reclaim
            .resident
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = resident;
    }
}

/// The memory consumer of one parked edge: rows held between a producer and
/// the deferred consumer that reads them at the commit.
///
/// Charged through the edge's handle, which holds the resident segments'
/// bytes. Ranks by the resident segments no open re-read cursor shares, read
/// when the arbitrator asks, since a cursor closing frees nothing the store
/// is told about: a shared segment cannot be freed by a spill. A reclaim pass
/// on the walk spills the edge itself; an election anywhere else raises the
/// edge's spill request, which its next park answers.
///
/// `spill_priority = 0` and `can_back_pressure = false`, as for any
/// inter-stage buffer: the rows are parked synchronously on the walk, so
/// there is no producer thread to pause.
pub(crate) struct ParkedEdgeConsumer {
    handle: Arc<ConsumerHandle>,
    resident: Mutex<Vec<ResidentSegment>>,
}

/// A resident segment's charge, and the backing an open cursor may share.
struct ResidentSegment {
    bytes: u64,
    shared: Option<Weak<ReReadableNodeBuffer>>,
}

impl crate::pipeline::memory::MemoryConsumer for ParkedEdgeConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    /// The resident segments no open cursor shares: the store holds the only
    /// reference to their backing, so a spill drops them.
    fn reclaimable_bytes(&self) -> u64 {
        self.resident
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|segment| {
                segment
                    .shared
                    .as_ref()
                    .is_none_or(|backing| backing.strong_count() <= 1)
            })
            .map(|segment| segment.bytes)
            .sum()
    }

    fn peak_charged_bytes(&self) -> Option<u64> {
        Some(self.handle.peak_bytes())
    }

    fn spill_priority(&self) -> i32 {
        0
    }

    fn try_spill(&self, target_bytes: u64) -> Result<u64, ConsumerSpillError> {
        self.handle.request_spill();
        let bytes = self.handle.bytes();
        if bytes >= target_bytes {
            Ok(bytes)
        } else {
            Err(ConsumerSpillError::BelowTarget {
                target: target_bytes,
                freed: bytes,
            })
        }
    }

    fn can_back_pressure(&self) -> bool {
        false
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
            commit_pass: HashMap::new(),
        }
    }

    /// Park a copy of `rows` for the crossing edge `key`, from the node named
    /// `from` into the deferred consumer named `to`, in `generation`.
    ///
    /// The first park on an edge registers its consumer, even for no rows, so
    /// the commit reads an empty input rather than a missing one, and
    /// registers the store under that consumer in the walk reclaim set
    /// ([`register_walk_owned`]), so any reclaim pass on the walk can spill
    /// the edge. A spill
    /// request raised on the edge since its last park is answered first. The
    /// copy's resident size is grown on the edge's handle with no borrow of
    /// the store held, so a reclaim that growth starts on the walk can spill
    /// any parked edge, this one included. When it still does not fit, the
    /// edge's own resident segments spill and the growth is retried once; if
    /// that falls short too, the rows are written straight to disk. Parking
    /// is never refused for memory; past the spill cap it fails with E320.
    pub(crate) fn park(
        store: &Rc<RefCell<Self>>,
        generation: Generation,
        key: ParkedKey,
        rows: &[(Record, SourceRowId)],
        from: &str,
        to: &str,
    ) -> Result<(), PipelineError> {
        let (handle, registered) = store.borrow_mut().edge_handle(generation, key, from, to);
        if let Some(consumer) = registered {
            let arbitrator = Arc::clone(&store.borrow().arbitrator);
            let walk_entry = register_walk_owned(&arbitrator, consumer, &handle, store)?;
            store
                .borrow_mut()
                .keep_walk_entry(generation, &key, walk_entry)?;
        }
        if rows.is_empty() {
            return Ok(());
        }
        if handle.take_spill_request() {
            store.borrow_mut().spill_edge(generation, &key)?;
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
            store.borrow_mut().spill_edge(generation, &key)?;
            if handle.try_grow(bytes).is_ok() {
                bytes
            } else {
                0
            }
        };
        store.borrow_mut().append(generation, key, segment, charged)
    }

    /// The edges parked in `generation`.
    fn edges(&self, generation: Generation) -> &HashMap<ParkedKey, ParkedEdge> {
        match generation {
            Generation::Forward => &self.forward,
            Generation::CommitPass => &self.commit_pass,
        }
    }

    fn edges_mut(&mut self, generation: Generation) -> &mut HashMap<ParkedKey, ParkedEdge> {
        match generation {
            Generation::Forward => &mut self.forward,
            Generation::CommitPass => &mut self.commit_pass,
        }
    }

    /// The consumer handle of edge `key` in `generation`, registering the
    /// edge on first use; the consumer is returned beside it when this call
    /// registered it.
    fn edge_handle(
        &mut self,
        generation: Generation,
        key: ParkedKey,
        from: &str,
        to: &str,
    ) -> (Arc<ConsumerHandle>, Option<ConsumerId>) {
        if let Some(edge) = self.edges(generation).get(&key) {
            return (Arc::clone(&edge.handle), None);
        }
        let handle = ConsumerHandle::new();
        let reclaim = Arc::new(ParkedEdgeConsumer {
            handle: Arc::clone(&handle),
            resident: Mutex::new(Vec::new()),
        });
        let consumer = self.arbitrator.register_node_consumer(
            Arc::clone(&reclaim) as Arc<dyn crate::pipeline::memory::MemoryConsumer>,
            Arc::clone(&handle),
            ConsumerLabel {
                node: from.to_string(),
                surface: MemorySurface::ParkedCrossRegionRows {
                    from: from.to_string(),
                    to: to.to_string(),
                },
            },
        );
        self.edges_mut(generation).insert(
            key,
            ParkedEdge {
                consumer,
                handle: Arc::clone(&handle),
                reclaim,
                producer: Box::from(from),
                segments: Vec::new(),
                walk_entry: None,
            },
        );
        (handle, Some(consumer))
    }

    /// Keep edge `key`'s walk reclaim registration with the edge.
    fn keep_walk_entry(
        &mut self,
        generation: Generation,
        key: &ParkedKey,
        walk_entry: WalkOwnedRegistration,
    ) -> Result<(), PipelineError> {
        let edge = self
            .edges_mut(generation)
            .get_mut(key)
            .ok_or_else(|| unregistered_edge(key))?;
        edge.walk_entry = Some(walk_entry);
        Ok(())
    }

    /// Add `segment`, whose resident size `charged` bytes the edge's handle
    /// already holds, after edge `key`'s other segments. A segment nothing
    /// was charged for is written to disk first.
    fn append(
        &mut self,
        generation: Generation,
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
            .edges(generation)
            .get(&key)
            .map(|edge| edge.producer.clone())
            .ok_or_else(|| unregistered_edge(&key))?;
        // The file is kept with its segment even past the spill cap, so its
        // quota bytes are released with the segment like any other's.
        let recorded = self.record_spill(&producer, file_bytes);
        let edge = self
            .edges_mut(generation)
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
        edge.publish_reclaimable();
        recorded
    }

    /// Spill the edge registered as consumer `id`, when it is one of this
    /// store's; `None` when it is not. A reclaim pass on the walk calls it
    /// for an elected edge.
    pub(crate) fn spill_consumer(&mut self, id: ConsumerId) -> Result<Option<u64>, PipelineError> {
        for generation in [Generation::Forward, Generation::CommitPass] {
            let found = self
                .edges(generation)
                .iter()
                .find(|(_, edge)| edge.consumer == id)
                .map(|(key, _)| *key);
            if let Some(key) = found {
                return self.spill_edge(generation, &key).map(Some);
            }
        }
        Ok(None)
    }

    /// Spill every resident segment of edge `key` that no cursor is reading,
    /// releasing its charge; returns the bytes released. A segment a live
    /// cursor shares stays resident: writing it out would free nothing.
    pub(crate) fn spill_edge(
        &mut self,
        generation: Generation,
        key: &ParkedKey,
    ) -> Result<u64, PipelineError> {
        let (root, compress, batch_size) = (
            Arc::clone(&self.spill_root),
            self.spill_compress,
            self.batch_size,
        );
        let Some(edge) = self.edges_mut(generation).get_mut(key) else {
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
        edge.publish_reclaimable();
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
    /// the order they were parked, the forward pass's before the running
    /// iteration's; `None` when nothing was parked for it in either. The
    /// segments stay here, so every later call reads the same rows again. The
    /// cursor shares the segments' backing: it adds no charge, and while it
    /// lives a spill of the edge cannot free the segments it reads.
    pub(crate) fn publish_view(
        &mut self,
        key: &ParkedKey,
    ) -> Result<Option<NodeBuffer>, PipelineError> {
        let mut parts = Vec::new();
        let mut parked = false;
        for generation in [Generation::Forward, Generation::CommitPass] {
            let Some(edge) = self.edges_mut(generation).get_mut(key) else {
                continue;
            };
            parked = true;
            for segment in &mut edge.segments {
                parts.push(segment.buffer.reread_backing()?);
            }
            edge.publish_reclaimable();
        }
        Ok(parked.then(|| NodeBuffer::chained(parts)))
    }

    /// Begin a retraction iteration of the commit: release the rows the
    /// previous iteration's commit pass parked, which this iteration parks
    /// afresh.
    pub(crate) fn start_iteration(&mut self) {
        for (_, edge) in std::mem::take(&mut self.commit_pass) {
            self.release_edge(edge);
        }
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

    /// Every parked edge, in either generation.
    #[cfg(test)]
    pub(crate) fn edge_count(&self) -> usize {
        self.forward.len() + self.commit_pass.len()
    }

    /// Every parked edge's consumer, in either generation.
    #[cfg(test)]
    pub(crate) fn consumer_ids(&self) -> Vec<ConsumerId> {
        self.forward
            .values()
            .chain(self.commit_pass.values())
            .map(|edge| edge.consumer)
            .collect()
    }

    /// Release every parked edge of both generations: drop its rows and
    /// spill files, release its charge and its disk-quota bytes, and
    /// unregister its consumer. The store stays usable.
    pub(crate) fn release_all(&mut self) {
        self.start_iteration();
        for (_, edge) in std::mem::take(&mut self.forward) {
            self.release_edge(edge);
        }
    }

    /// Drop `edge`'s segments, then release what it held, take it out of the
    /// walk reclaim set and unregister it.
    fn release_edge(&self, edge: ParkedEdge) {
        let ParkedEdge {
            consumer,
            handle,
            producer,
            segments,
            walk_entry,
            ..
        } = edge;
        let file_bytes: u64 = segments.iter().map(|segment| segment.file_bytes).sum();
        drop(segments);
        self.arbitrator.release_spill_bytes(&producer, file_bytes);
        handle.shrink(handle.bytes());
        drop(walk_entry);
        self.arbitrator.unregister_consumer(consumer);
    }
}

impl WalkOwnedSpill for ParkedGenerations {
    /// A pass that elects a parked edge's consumer spills that edge's
    /// resident segments ([`ParkedGenerations::spill_consumer`]); a consumer
    /// whose edge was released is not held here.
    fn spill_owned(
        &mut self,
        id: ConsumerId,
        _arbitrator: &MemoryArbitrator,
    ) -> Result<bool, PipelineError> {
        Ok(self.spill_consumer(id)?.is_some())
    }
}

impl Drop for ParkedGenerations {
    fn drop(&mut self) {
        self.release_all();
    }
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
        let _walk = WalkContextGuard::install(&arbitrator, Rc::clone(&set));
        let consumers_before = arbitrator.consumer_count();

        let key: ParkedKey = (None, EdgeIndex::new(4));
        ParkedGenerations::park(
            &store,
            Generation::Forward,
            key,
            &first,
            "lookup",
            "enriched",
        )
        .expect("first park");
        let view = store
            .borrow_mut()
            .publish_view(&key)
            .expect("view")
            .expect("the edge has rows");
        ParkedGenerations::park(
            &store,
            Generation::Forward,
            key,
            &second,
            "lookup",
            "enriched",
        )
        .expect("second park");
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

    /// A walk over an arbitrator of `limit` bytes whose reclaim set reaches
    /// a parked-row store spilling under `root`.
    struct ParkedWalk {
        arbitrator: Arc<MemoryArbitrator>,
        store: Rc<RefCell<ParkedGenerations>>,
        _walk: WalkContextGuard,
    }

    fn parked_walk(limit: u64, root: &Path) -> ParkedWalk {
        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            limit,
            0.80,
            0.70,
            Box::new(Priority),
        ));
        let set = Rc::new(RefCell::new(WalkReclaimSet::new(WalkSpillSettings {
            spill_root: Arc::from(root),
            spill_compress: CompressMode::Auto,
            batch_size: 1024,
        })));
        let store = Rc::new(RefCell::new(ParkedGenerations::new(
            Arc::clone(&arbitrator),
            Arc::from(root),
            CompressMode::Auto,
            1024,
        )));
        let walk = WalkContextGuard::install(&arbitrator, set);
        ParkedWalk {
            arbitrator,
            store,
            _walk: walk,
        }
    }

    /// Four parks on one edge: three of 64 rows each, then one of 96.
    fn chunks() -> Vec<Vec<(Record, SourceRowId)>> {
        vec![rows(0, 64), rows(64, 64), rows(128, 64), rows(192, 96)]
    }

    /// Every row ordinal a fresh cursor over edge `key` reads, in order.
    fn read_back(store: &Rc<RefCell<ParkedGenerations>>, key: &ParkedKey) -> Vec<u64> {
        store
            .borrow_mut()
            .publish_view(key)
            .expect("view")
            .expect("rows")
            .drain()
            .map(|event| match event.expect("read") {
                crate::executor::stream_event::StreamEvent::Record(_, row) => row.ordinal(),
                crate::executor::stream_event::StreamEvent::Punctuation(_) => u64::MAX,
            })
            .collect()
    }

    /// Under a limit a third of the parked rows' size, each park that does
    /// not fit spills the edge's resident rows through the reclaim its growth
    /// starts on the walk, and the last park, larger than the limit on its
    /// own, is written straight to disk. Every later read returns every row
    /// in parking order, and a spill is recorded once, however often the
    /// rows are read again.
    #[test]
    fn parked_edge_spills_under_a_low_limit_and_reads_back_whole() {
        let root = tempfile::tempdir().expect("spill root");
        let chunks = chunks();
        let chunk_bytes: Vec<u64> = chunks.iter().map(|rows| resident_bytes(rows)).collect();
        let parked_bytes: u64 = chunk_bytes.iter().sum();
        // Each of the first three parks fits alone, two do not fit together,
        // and the last is larger than the limit.
        let limit = chunk_bytes[0] + chunk_bytes[0] / 4;
        assert!(parked_bytes >= 3 * limit && chunk_bytes[3] > limit);
        let walk = parked_walk(limit, root.path());
        let consumers_before = walk.arbitrator.consumer_count();
        let key: ParkedKey = (None, EdgeIndex::new(1));
        for rows in &chunks {
            ParkedGenerations::park(
                &walk.store,
                Generation::Forward,
                key,
                rows,
                "lookup",
                "enriched",
            )
            .expect("a park is never refused for memory");
        }
        let (_, handle) = walk.store.borrow().edge_consumer(&key).expect("edge");
        assert_eq!(handle.bytes(), 0, "every parked row is on disk");
        assert!(
            handle.peak_bytes() > 0 && handle.peak_bytes() <= limit,
            "the edge never held more than the limit charged (peak {})",
            handle.peak_bytes()
        );
        let written = walk.arbitrator.per_stage_spill_bytes_written()["lookup"];
        assert!(written > 0);

        let expected: Vec<u64> = (0..288).collect();
        assert_eq!(read_back(&walk.store, &key), expected, "first iteration");
        assert_eq!(read_back(&walk.store, &key), expected, "second iteration");
        assert_eq!(
            walk.arbitrator.per_stage_spill_bytes_written()["lookup"],
            written,
            "reading spilled rows again writes and records nothing more"
        );

        walk.store.borrow_mut().release_all();
        assert_eq!(walk.arbitrator.consumer_count(), consumers_before);
        assert_eq!(walk.arbitrator.cumulative_spill_bytes(), 0);
        assert!(
            std::fs::read_dir(root.path())
                .expect("spill root")
                .next()
                .is_none(),
            "the released edge's spill files are gone"
        );
    }

    /// With ample memory the same parks stay resident: nothing is written,
    /// and the edge's charge covers every parked byte.
    #[test]
    fn parked_edge_stays_resident_with_ample_memory() {
        let root = tempfile::tempdir().expect("spill root");
        let chunks = chunks();
        let parked_bytes: u64 = chunks.iter().map(|rows| resident_bytes(rows)).sum();
        let walk = parked_walk(64 * 1024 * 1024, root.path());
        let key: ParkedKey = (None, EdgeIndex::new(1));
        for rows in &chunks {
            ParkedGenerations::park(
                &walk.store,
                Generation::Forward,
                key,
                rows,
                "lookup",
                "enriched",
            )
            .expect("park");
        }
        let (_, handle) = walk.store.borrow().edge_consumer(&key).expect("edge");
        assert_eq!(handle.bytes(), parked_bytes);
        assert!(handle.peak_bytes() >= parked_bytes);
        let expected: Vec<u64> = (0..288).collect();
        assert_eq!(read_back(&walk.store, &key), expected);
        assert_eq!(read_back(&walk.store, &key), expected);
        assert!(walk.arbitrator.per_stage_spill_bytes_written().is_empty());
    }
}
