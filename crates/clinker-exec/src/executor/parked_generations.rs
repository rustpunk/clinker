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
use clinker_record::owned_storage::AllocationResources;
use petgraph::graph::EdgeIndex;

use crate::executor::node_buffer::{NodeBuffer, ReReadableNodeBuffer};
use crate::executor::node_buffer_spill::spill_borrowed_rows;
use crate::executor::stream_event::SourceRowId;
use crate::pipeline::memory::walk::{
    OwnedSpillResult, WalkOwnedRegistration, WalkOwnedSpill, register_walk_owned,
};
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
    /// The run's allocation resources, over the ledger `arbitrator` keeps.
    resources: AllocationResources,
    spill_root: Arc<Path>,
    spill_compress: CompressMode,
    batch_size: usize,
    /// Rows parked on the forward pass.
    forward: HashMap<ParkedKey, ParkedEdge>,
    /// Rows parked during the running iteration of the commit.
    commit_pass: HashMap<ParkedKey, ParkedEdge>,
    /// Every resident copy a park made, and the edge's charge when the
    /// first was made.
    #[cfg(test)]
    copies: ParkCopies,
}

/// The resident copies parks made, kept for tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ParkCopies {
    /// Rows copied into resident segments.
    pub(crate) rows: u64,
    /// The edge handle's charge at the moment the first row was copied.
    pub(crate) charge_at_first_copy: Option<u64>,
}

/// What parking `rows` charges in the run whose allocation `resources` are
/// given: the bytes a copy of them allocates or alone may keep alive with no
/// other charge in that run. Each row counts the `(Record, SourceRowId)` pair
/// its copy occupies plus [`Record::clone_allocation_bytes`], which counts
/// the value slots once and leaves out only governed shared text the run
/// itself admitted. Reads the borrowed rows only, so the charge can be taken
/// before the copy is made.
fn parked_copy_bytes(rows: &[(Record, SourceRowId)], resources: &AllocationResources) -> u64 {
    rows.iter()
        .map(|(record, _)| {
            (std::mem::size_of::<(Record, SourceRowId)>()
                + record.clone_allocation_bytes(resources)) as u64
        })
        .sum()
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
    /// `resources` are the run's allocation resources over that same ledger:
    /// a park leaves out of its charge only text whose admission they hold.
    pub(crate) fn new(
        arbitrator: Arc<MemoryArbitrator>,
        resources: AllocationResources,
        spill_root: Arc<Path>,
        spill_compress: CompressMode,
        batch_size: usize,
    ) -> Self {
        Self {
            arbitrator,
            resources,
            spill_root,
            spill_compress,
            batch_size,
            forward: HashMap::new(),
            commit_pass: HashMap::new(),
            #[cfg(test)]
            copies: ParkCopies::default(),
        }
    }

    /// Note that `rows` rows are being copied into a resident segment while
    /// the edge is charged `charge`.
    #[cfg(test)]
    fn note_copy(&mut self, rows: usize, charge: u64) {
        self.copies.charge_at_first_copy.get_or_insert(charge);
        self.copies.rows += rows as u64;
    }

    /// The resident copies parks have made.
    #[cfg(test)]
    pub(crate) fn park_copies(&self) -> ParkCopies {
        self.copies
    }

    /// For each segment of edge `key` parked on the forward pass, in order,
    /// whether it is on disk.
    #[cfg(test)]
    pub(crate) fn segments_on_disk(&self, key: &ParkedKey) -> Vec<bool> {
        self.forward.get(key).map_or_else(Vec::new, |edge| {
            edge.segments
                .iter()
                .map(|segment| matches!(segment.buffer, NodeBuffer::Spilled { .. }))
                .collect()
        })
    }

    /// Park a copy of `rows` for the crossing edge `key`, from the node named
    /// `from` into the deferred consumer named `to`, in `generation`.
    ///
    /// The first park on an edge registers its consumer, even for no rows, so
    /// the commit reads an empty input rather than a missing one, and
    /// registers the store under that consumer in the walk reclaim set
    /// ([`register_walk_owned`]), so any reclaim pass on the walk can spill
    /// the edge. A spill
    /// request raised on the edge since its last park is answered first.
    ///
    /// The rows are charged what their copy allocates or alone may keep
    /// alive with no other charge in this run (the store's run resources
    /// decide which governed text the run already holds charged), computed
    /// from the borrowed rows before any copy is made. The
    /// charge is grown on the edge's handle with no borrow of the store held,
    /// so a reclaim that growth starts on the walk can spill any parked edge,
    /// this one included. When it still does not fit, the edge's own resident
    /// segments spill and the growth is retried once; if that falls short
    /// too, the borrowed rows are written straight to disk and no resident
    /// copy is made. Parking is never refused for memory; past the spill cap
    /// it fails with E320.
    pub(crate) fn park(
        store: &Rc<RefCell<Self>>,
        generation: Generation,
        key: ParkedKey,
        rows: &[(Record, SourceRowId)],
        from: &str,
        to: &str,
    ) -> Result<(), PipelineError> {
        let (handle, registered) = store.borrow_mut().edge_handle(generation, key, from, to)?;
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
        // Charged from the borrowed rows, before any copy exists, so no copy
        // is ever held uncharged. The figure is what the copy allocates or
        // alone may keep alive (`Record::clone_allocation_bytes`): governed
        // shared text this run admitted is left out, its admission covering
        // every alias; text another authority admitted and ungoverned shared
        // text stay in, since the copy may outlive the original whose holder
        // carried the only charge, or none in this run.
        let bytes = parked_copy_bytes(rows, &store.borrow().resources);
        let mut granted = handle.try_grow(bytes).is_ok();
        if !granted {
            store.borrow_mut().spill_edge(generation, &key)?;
            granted = handle.try_grow(bytes).is_ok();
        }
        if granted {
            #[cfg(test)]
            store.borrow_mut().note_copy(rows.len(), handle.bytes());
            let segment = NodeBuffer::memory_from_records(rows.to_vec());
            return store
                .borrow_mut()
                .append(generation, key, segment, bytes, 0);
        }
        // Still short: the borrowed rows go straight to disk, and no
        // resident copy is built.
        let (segment, file_bytes) = store.borrow().write_rows_to_disk(rows)?;
        store
            .borrow_mut()
            .append(generation, key, segment, 0, file_bytes)
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
    /// registered it. A consumer that cannot be registered is an internal
    /// error and leaves the edge unregistered.
    fn edge_handle(
        &mut self,
        generation: Generation,
        key: ParkedKey,
        from: &str,
        to: &str,
    ) -> Result<(Arc<ConsumerHandle>, Option<ConsumerId>), PipelineError> {
        if let Some(edge) = self.edges(generation).get(&key) {
            return Ok((Arc::clone(&edge.handle), None));
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
        )?;
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
        Ok((handle, Some(consumer)))
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

    /// Add `segment` after edge `key`'s other segments: a resident one whose
    /// `charged` bytes the edge's handle already holds, or one already on
    /// disk (`charged` 0) whose file is `file_bytes` long.
    fn append(
        &mut self,
        generation: Generation,
        key: ParkedKey,
        segment: NodeBuffer,
        charged: u64,
        file_bytes: u64,
    ) -> Result<(), PipelineError> {
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

    /// Write the borrowed `rows` to one spill file without copying them;
    /// returns the spilled segment and the file's bytes.
    fn write_rows_to_disk(
        &self,
        rows: &[(Record, SourceRowId)],
    ) -> Result<(NodeBuffer, u64), PipelineError> {
        let column_count = rows
            .first()
            .map_or(0, |(record, _)| record.schema().column_count());
        let compress = self
            .spill_compress
            .resolve_for_schema(column_count, self.batch_size as u64);
        match spill_borrowed_rows(rows, Some(self.spill_root.as_ref()), compress)? {
            Some((file, count)) => {
                let file_bytes = std::fs::metadata(file.path()).map_or(0, |meta| meta.len());
                Ok((
                    NodeBuffer::Spilled {
                        chunks: vec![(file, count)],
                        pending_puncts: Vec::new(),
                    },
                    file_bytes,
                ))
            }
            None => Ok((NodeBuffer::Memory(Vec::new()), 0)),
        }
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
    /// whose edge was released is not held here. It wrote when the edge's
    /// charge fell; an edge whose segments are all on disk, or all shared
    /// with an open cursor, wrote nothing.
    fn spill_owned(
        &mut self,
        id: ConsumerId,
        _arbitrator: &MemoryArbitrator,
    ) -> Result<OwnedSpillResult, PipelineError> {
        Ok(match self.spill_consumer(id)? {
            None => OwnedSpillResult::NotHeld,
            Some(0) => OwnedSpillResult::NothingToWrite,
            Some(_) => OwnedSpillResult::Wrote,
        })
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

    /// The bytes a park of `rows` charges: what their copy allocates or
    /// alone may keep alive, the figure the park itself computes. The
    /// fixtures this sizes hold no governed text, so the figure is the same
    /// whichever run's resources it is read under.
    fn resident_bytes(rows: &[(Record, SourceRowId)]) -> u64 {
        let standalone =
            clinker_format::preparation::MemoryOnlyResources::new(std::num::NonZeroUsize::MIN);
        parked_copy_bytes(rows, standalone.resources().allocation())
    }

    /// The allocation provider of a run over `arbitrator`, as the executor
    /// builds it: text admitted through its allocation resources is charged
    /// to `arbitrator`'s ledger.
    fn run_provider(
        arbitrator: &Arc<MemoryArbitrator>,
    ) -> crate::executor::preparation::ExecutorResources {
        crate::executor::preparation::ExecutorResources::new(
            Arc::clone(arbitrator),
            crate::pipeline::shutdown::ShutdownToken::detached(),
            None,
            std::num::NonZeroUsize::MIN,
            None,
        )
        .expect("a run provider")
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
        let provider = run_provider(&arbitrator);
        let store = Rc::new(RefCell::new(ParkedGenerations::new(
            Arc::clone(&arbitrator),
            provider.allocation(),
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
        /// The run's allocation resources, which the store also holds.
        resources: AllocationResources,
        store: Rc<RefCell<ParkedGenerations>>,
        _walk: WalkContextGuard,
        _provider: crate::executor::preparation::ExecutorResources,
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
        let provider = run_provider(&arbitrator);
        let resources = provider.allocation();
        let store = Rc::new(RefCell::new(ParkedGenerations::new(
            Arc::clone(&arbitrator),
            resources.clone(),
            Arc::from(root),
            CompressMode::Auto,
            1024,
        )));
        let walk = WalkContextGuard::install(&arbitrator, set);
        ParkedWalk {
            arbitrator,
            resources,
            store,
            _walk: walk,
            _provider: provider,
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

    /// Rows `first..first + count` of the `rows` fixture's shape, whose
    /// payload column holds what `payload` builds for each row.
    fn rows_with_payload(
        first: u64,
        count: u64,
        columns: &[&str],
        payload: impl Fn(u64) -> Vec<Value>,
    ) -> Vec<(Record, SourceRowId)> {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(
            columns.iter().map(|&column| column.into()).collect(),
        )));
        (first..first + count)
            .map(|row| {
                // Exactly as many slots as columns, like the `rows` fixture.
                let payload = payload(row);
                let mut values = Vec::with_capacity(1 + payload.len());
                values.push(Value::Integer(row as i64));
                values.extend(payload);
                (
                    Record::new(schema.clone(), values),
                    SourceRowId::new(clinker_plan::plan::PlanNodeId::new(3), row),
                )
            })
            .collect()
    }

    /// Park `rows` on edge `key` of `walk`'s store, on the forward pass.
    fn park_rows(walk: &ParkedWalk, key: ParkedKey, rows: &[(Record, SourceRowId)]) {
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

    /// With ample memory a park charges the edge exactly what the copy of
    /// its rows allocates or alone may keep alive, and that charge is in
    /// place before the first row is copied.
    #[test]
    fn a_park_charges_the_rows_before_it_copies_them() {
        let root = tempfile::tempdir().expect("spill root");
        let rows = rows(0, 32);
        let walk = parked_walk(64 * 1024 * 1024, root.path());
        let figure = parked_copy_bytes(&rows, &walk.resources);
        let key: ParkedKey = (None, EdgeIndex::new(2));
        park_rows(&walk, key, &rows);

        let (_, handle) = walk.store.borrow().edge_consumer(&key).expect("edge");
        assert_eq!(handle.bytes(), figure);
        assert_eq!(
            walk.store.borrow().park_copies(),
            ParkCopies {
                rows: 32,
                charge_at_first_copy: Some(figure),
            },
            "the rows are charged before any of them is copied"
        );
        assert_eq!(read_back(&walk.store, &key), (0..32).collect::<Vec<u64>>());
    }

    /// Below the rows' figure, with nothing to reclaim, a park writes the
    /// borrowed rows to one spill file and never builds a resident copy;
    /// the file reads back whole and its bytes count against the producer.
    #[test]
    fn a_refused_park_writes_the_rows_without_a_resident_copy() {
        let root = tempfile::tempdir().expect("spill root");
        let rows = rows(0, 32);
        let walk = parked_walk(resident_bytes(&rows) / 2, root.path());
        let key: ParkedKey = (None, EdgeIndex::new(3));
        park_rows(&walk, key, &rows);

        assert_eq!(
            walk.store.borrow().park_copies(),
            ParkCopies::default(),
            "a refused park copies no row"
        );
        assert_eq!(walk.store.borrow().segments_on_disk(&key), vec![true]);
        let (_, handle) = walk.store.borrow().edge_consumer(&key).expect("edge");
        assert_eq!(handle.bytes(), 0);
        assert_eq!(handle.peak_bytes(), 0, "nothing was ever held charged");
        assert_eq!(read_back(&walk.store, &key), (0..32).collect::<Vec<u64>>());
        let written = walk.arbitrator.per_stage_spill_bytes_written()["lookup"];
        assert!(written > 0);
        assert_eq!(walk.arbitrator.cumulative_spill_bytes(), written);

        walk.store.borrow_mut().release_all();
        assert_eq!(walk.arbitrator.cumulative_spill_bytes(), 0);
    }

    /// Rows whose long text is ungoverned and shared are parked, then the
    /// originals are dropped: the parked copy alone now keeps that text
    /// alive, so the edge keeps charging it. The same rows with inline text
    /// set the baseline.
    #[test]
    fn a_park_keeps_charging_payload_only_its_copy_keeps_alive() {
        let root = tempfile::tempdir().expect("spill root");
        let long = rows(0, 32);
        let payload = format!("parked-{:06}", 0).repeat(8).len() as u64;
        let inline = rows_with_payload(0, 32, &["id", "payload"], |row| {
            vec![Value::String(format!("short-{row:06}").into())]
        });
        let walk = parked_walk(64 * 1024 * 1024, root.path());
        let figure = parked_copy_bytes(&long, &walk.resources);
        let (long_key, inline_key): (ParkedKey, ParkedKey) =
            ((None, EdgeIndex::new(5)), (None, EdgeIndex::new(6)));
        park_rows(&walk, long_key, &long);
        park_rows(&walk, inline_key, &inline);
        drop(long);
        drop(inline);

        let (_, long_handle) = walk.store.borrow().edge_consumer(&long_key).expect("edge");
        let (_, inline_handle) = walk
            .store
            .borrow()
            .edge_consumer(&inline_key)
            .expect("edge");
        assert_eq!(long_handle.bytes(), figure);
        assert_eq!(
            long_handle.bytes() - inline_handle.bytes(),
            32 * payload,
            "the parked copy is the text's only holder now, so its charge covers it"
        );
    }

    /// Length of the governed shared text in [`governed_rows`].
    const GOVERNED_SHARED_LEN: usize = 257;
    /// Length of the governed unique text in [`governed_rows`].
    const GOVERNED_UNIQUE_LEN: usize = 513;

    /// Rows as a park borrows them.
    type ParkRows = Vec<(Record, SourceRowId)>;

    /// 32 rows of an id, a governed shared string and a governed unique
    /// string, their text admitted under `scope` as a Source reader admits
    /// it; and the same rows with inline text, as the baseline.
    fn governed_rows(
        scope: &clinker_record::owned_storage::AllocationScope,
    ) -> (ParkRows, ParkRows) {
        use clinker_record::FieldStr;

        let columns = ["id", "shared", "unique"];
        let governed = rows_with_payload(0, 32, &columns, |_| {
            vec![
                Value::String(
                    FieldStr::try_new(&"s".repeat(GOVERNED_SHARED_LEN), scope).expect("admitted"),
                ),
                Value::String(
                    FieldStr::try_new_unique(&"u".repeat(GOVERNED_UNIQUE_LEN), scope)
                        .expect("admitted"),
                ),
            ]
        });
        let inline = rows_with_payload(0, 32, &columns, |_| {
            vec![Value::String("s".into()), Value::String("u".into())]
        });
        (governed, inline)
    }

    /// The edge charges of parking `governed` and `inline` on two edges of
    /// `walk`'s store.
    fn park_pair(
        walk: &ParkedWalk,
        governed: &[(Record, SourceRowId)],
        inline: &[(Record, SourceRowId)],
    ) -> (u64, u64) {
        let (governed_key, inline_key): (ParkedKey, ParkedKey) =
            ((None, EdgeIndex::new(7)), (None, EdgeIndex::new(8)));
        park_rows(walk, governed_key, governed);
        park_rows(walk, inline_key, inline);
        let store = walk.store.borrow();
        let (_, governed_handle) = store.edge_consumer(&governed_key).expect("edge");
        let (_, inline_handle) = store.edge_consumer(&inline_key).expect("edge");
        (governed_handle.bytes(), inline_handle.bytes())
    }

    /// The admitted size of the text in column `column` of `row`.
    fn admitted_text(row: &(Record, SourceRowId), column: usize) -> u64 {
        match &row.0.values()[column] {
            Value::String(text) => text.heap_size() as u64,
            other => panic!("column {column} holds {other:?}, not text"),
        }
    }

    /// Rows whose long text this run's Source reader admitted: the copy of a
    /// governed unique string is a fresh allocation nothing admitted, so the
    /// edge charges it; a governed shared string's admission in this run's
    /// ledger covers every alias, so the edge does not charge it again. The
    /// same rows with inline text set the baseline.
    #[test]
    fn a_park_charges_a_governed_unique_copy_and_not_a_governed_shared_alias() {
        let root = tempfile::tempdir().expect("spill root");
        let walk = parked_walk(64 * 1024 * 1024, root.path());
        let scope = walk.resources.scope().expect("a run scope");
        let (governed, inline) = governed_rows(&scope);
        let figure = parked_copy_bytes(&governed, &walk.resources);
        let (governed_bytes, inline_bytes) = park_pair(&walk, &governed, &inline);

        assert_eq!(governed_bytes, figure);
        assert_eq!(
            governed_bytes - inline_bytes,
            32 * GOVERNED_UNIQUE_LEN as u64,
            "each governed unique copy is charged its text and no governed shared alias is"
        );
    }

    /// Rows whose governed text another allocation authority admitted, as a
    /// custom Source may supply it: that admission is no charge in this
    /// run's ledger, and the parked copy keeps the shared text alive, so the
    /// edge charges the shared text at its admitted size as well as the
    /// unique copy.
    #[test]
    fn a_park_charges_governed_text_another_authority_admitted() {
        use clinker_format::preparation::MemoryOnlyResources;
        use std::num::NonZeroUsize;

        let other = MemoryOnlyResources::new(NonZeroUsize::new(1 << 20).expect("non-zero"));
        let scope = other
            .resources()
            .allocation()
            .scope()
            .expect("another authority's scope");
        let (governed, inline) = governed_rows(&scope);
        let shared_admitted = admitted_text(&governed[0], 1);
        assert!(shared_admitted >= GOVERNED_SHARED_LEN as u64);
        let root = tempfile::tempdir().expect("spill root");
        let walk = parked_walk(64 * 1024 * 1024, root.path());
        let figure = parked_copy_bytes(&governed, &walk.resources);
        let (governed_bytes, inline_bytes) = park_pair(&walk, &governed, &inline);

        assert_eq!(governed_bytes, figure);
        assert_eq!(
            governed_bytes - inline_bytes,
            32 * (shared_admitted + GOVERNED_UNIQUE_LEN as u64),
            "text another authority admitted is no charge in this run, so the edge charges it"
        );
    }

    /// A note this run's reader admitted is charged once, by the reader's
    /// admission. Parking the rows adds only the copy's own bytes; the note
    /// stays charged while the parked copy alone keeps it alive, and the
    /// last copy's release returns every byte.
    #[test]
    fn a_parked_alias_keeps_its_note_charged_once_until_the_last_copy_drops() {
        use clinker_record::FieldStr;

        const NOTE: usize = 512;
        let root = tempfile::tempdir().expect("spill root");
        let walk = parked_walk(64 * 1024 * 1024, root.path());
        let scope = walk.resources.scope().expect("a run scope");
        let before = walk.arbitrator.charged_bytes();
        let rows = rows_with_payload(0, 32, &["id", "note"], |_| {
            vec![Value::String(
                FieldStr::try_new(&"n".repeat(NOTE), &scope).expect("admitted"),
            )]
        });
        let note_layouts: u64 = rows.iter().map(|row| admitted_text(row, 1)).sum();
        assert!(note_layouts >= 32 * NOTE as u64);
        let admitted = walk.arbitrator.charged_bytes();
        assert_eq!(
            admitted,
            before + note_layouts,
            "the reader's admission charges each note once"
        );

        let parked = parked_copy_bytes(&rows, &walk.resources);
        park_rows(&walk, (None, EdgeIndex::new(9)), &rows);
        assert_eq!(
            walk.arbitrator.charged_bytes(),
            admitted + parked,
            "the park charges its copy, never the notes again"
        );

        drop(rows);
        assert_eq!(
            walk.arbitrator.charged_bytes(),
            before + note_layouts + parked,
            "the notes stay charged while only the parked copy keeps them alive"
        );

        walk.store.borrow_mut().release_all();
        assert_eq!(
            walk.arbitrator.charged_bytes(),
            before,
            "the last copy's release returns the notes and the copy"
        );
    }
}
