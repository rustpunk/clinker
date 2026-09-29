//! Walk-thread identity, and the walk-owned state a reclaim can reach.
//!
//! One thread walks a run's DAG, and it alone owns the node-buffer slots a
//! memory reclaim may spill. A run installs a [`WalkContextGuard`] on that
//! thread for the whole walk. Code that holds only the run's
//! [`MemoryArbitrator`] can then ask [`thread_role`] whether it is on that
//! walk, on a rayon kernel worker or on some other thread, and on the walk
//! reach the run's [`WalkReclaimSet`] through [`walk_reclaim_set`] without a
//! `&mut` path to the executor context.
//!
//! The set holds one frame of node-buffer slots per dispatch scope on the
//! walk: the top level's, and one above it for each composition body running
//! inside it. A body reads and writes only its own frame, but a reclaim can
//! spill a resident slot of any frame, so a body that falls short still
//! reaches the state its callers are holding.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Weak};

use clinker_plan::config::CompressMode;
use clinker_plan::error::PipelineError;

use super::reservation::ReservationState;
use super::{ConsumerHandle, ConsumerId, MemoryArbitrator};
use crate::executor::dispatch::{NodeBufferKey, NodeBufferReaderLedger, ResidentSlotSpill};
use crate::executor::node_buffer::NodeBuffer;

/// Where the calling thread stands relative to one run's walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThreadRole {
    /// The thread walking the run's DAG, with the run's walk frame installed.
    Walk,
    /// A rayon pool worker running a kernel fan-out; never the walk itself.
    RayonWorker,
    /// Any other thread: Source ingest, streaming writers, probe and ingest
    /// workers, or a thread walking a different run.
    OffWalk,
}

/// A node-buffer slot's consumer registration: the id to unregister after its
/// last reader, and the handle partial discharges and spills charge.
pub(crate) type SlotRegistration = (ConsumerId, Arc<ConsumerHandle>);

/// What the node-buffer spill sweep decides for a slot from the DAG it was
/// published in, captured when the slot's consumer registers, so a reclaim
/// can spill the slot without that DAG.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SlotSpill {
    /// Whether the slot's compiled classification lets it spill.
    pub(crate) spill_allowed: bool,
    /// The keyed node's name, which a spill reports its file bytes under.
    pub(crate) node_name: Box<str>,
}

/// One dispatch scope's node-buffer slots: the buffers, their consumer
/// registrations, what a spill of each registered slot needs, and the
/// remaining-reader ledger.
///
/// Whenever a registered slot's buffer is published here, whichever of the
/// two comes second records the buffer's reclaimable bytes on the slot's
/// handle, so the slot ranks as a reclaim victim by what its spill frees.
///
/// A composition body walks its own scope, pushed as a frame above the
/// parent's, so equal body-local and parent `NodeIndex` values never collide.
/// A slot's
/// registration and its spill facts change together, through
/// [`Self::register`], [`Self::remove_registration`] and
/// [`Self::take_registrations`] only.
#[derive(Default)]
pub(crate) struct NodeBufferSlots {
    buffers: HashMap<NodeBufferKey, NodeBuffer>,
    registrations: HashMap<NodeBufferKey, SlotRegistration>,
    spill: HashMap<NodeBufferKey, SlotSpill>,
    readers: NodeBufferReaderLedger,
}

impl NodeBufferSlots {
    /// Every published buffer in this scope, by slot.
    pub(crate) fn buffers(&self) -> &HashMap<NodeBufferKey, NodeBuffer> {
        &self.buffers
    }

    pub(crate) fn buffer(&self, key: &NodeBufferKey) -> Option<&NodeBuffer> {
        self.buffers.get(key)
    }

    pub(crate) fn contains_buffer(&self, key: &NodeBufferKey) -> bool {
        self.buffers.contains_key(key)
    }

    /// Publish `buffer` at `key`, returning the buffer it replaced.
    pub(crate) fn insert_buffer(
        &mut self,
        key: NodeBufferKey,
        buffer: NodeBuffer,
    ) -> Option<NodeBuffer> {
        if let Some((_, handle)) = self.registrations.get(&key) {
            handle.set_reclaimable(buffer.reclaimable_bytes());
        }
        self.buffers.insert(key, buffer)
    }

    pub(crate) fn remove_buffer(&mut self, key: &NodeBufferKey) -> Option<NodeBuffer> {
        self.buffers.remove(key)
    }

    /// Every registered slot's consumer registration.
    pub(crate) fn registrations(&self) -> &HashMap<NodeBufferKey, SlotRegistration> {
        &self.registrations
    }

    pub(crate) fn is_registered(&self, key: &NodeBufferKey) -> bool {
        self.registrations.contains_key(key)
    }

    /// Record `key`'s consumer registration and its spill facts, returning a
    /// registration it replaced.
    pub(crate) fn register(
        &mut self,
        key: NodeBufferKey,
        registration: SlotRegistration,
        spill: SlotSpill,
    ) -> Option<SlotRegistration> {
        if let Some(buffer) = self.buffers.get(&key) {
            registration.1.set_reclaimable(buffer.reclaimable_bytes());
        }
        self.spill.insert(key.clone(), spill);
        self.registrations.insert(key, registration)
    }

    /// Remove `key`'s registration and its spill facts. The caller
    /// unregisters the returned consumer or hands it to a new owner.
    pub(crate) fn remove_registration(&mut self, key: &NodeBufferKey) -> Option<SlotRegistration> {
        self.spill.remove(key);
        self.registrations.remove(key)
    }

    /// Remove every registration and its spill facts, leaving the buffers.
    pub(crate) fn take_registrations(&mut self) -> HashMap<NodeBufferKey, SlotRegistration> {
        self.spill.clear();
        std::mem::take(&mut self.registrations)
    }

    /// The spill facts captured when `key`'s consumer registered.
    pub(crate) fn slot_spill(&self, key: &NodeBufferKey) -> Option<&SlotSpill> {
        self.spill.get(key)
    }

    pub(crate) fn readers(&self) -> &NodeBufferReaderLedger {
        &self.readers
    }

    pub(crate) fn readers_mut(&mut self) -> &mut NodeBufferReaderLedger {
        &mut self.readers
    }

    /// Discard a scope that will not be read again: drop every buffer while
    /// its wrapper is still registered, then zero and unregister every
    /// remaining registration.
    pub(crate) fn release_residue(mut self, arbitrator: &MemoryArbitrator) {
        drop(std::mem::take(&mut self.buffers));
        for (_, (id, handle)) in self.take_registrations() {
            handle.shrink(handle.bytes());
            arbitrator.unregister_consumer(id);
        }
    }

    /// Spill the slot registered here under consumer `id`, if this scope
    /// registered it; `None` when it did not.
    ///
    /// A resident slot spills through the same core as the walk's
    /// spill-request sweep (`service_pending_node_buffer_spills`). A
    /// registered slot whose buffer is out of the scope is held by a running
    /// arm: its spill request is raised and it is `Busy`. A slot its compiled
    /// classification keeps in memory is `NotOwned`.
    fn spill_registered(
        &mut self,
        id: ConsumerId,
        arbitrator: &MemoryArbitrator,
        spill_settings: &WalkSpillSettings,
    ) -> Result<Option<VictimOutcome>, PipelineError> {
        let Some((key, handle)) = self
            .registrations
            .iter()
            .find(|(_, (registered, _))| *registered == id)
            .map(|(key, (_, handle))| (key.clone(), Arc::clone(handle)))
        else {
            return Ok(None);
        };
        let Some(spill) = self.spill.get(&key) else {
            return Ok(Some(VictimOutcome::NotOwned));
        };
        if !spill.spill_allowed {
            return Ok(Some(VictimOutcome::NotOwned));
        }
        if !self.buffers.contains_key(&key) {
            handle.request_spill();
            return Ok(Some(VictimOutcome::Busy));
        }
        let node_name = spill.node_name.clone();
        ResidentSlotSpill {
            arbitrator,
            spill_root: spill_settings.spill_root.as_ref(),
            spill_compress: spill_settings.spill_compress,
            batch_size: spill_settings.batch_size,
        }
        .spill_slot(&mut self.buffers, &key, &handle, &node_name)?;
        Ok(Some(VictimOutcome::Spilled))
    }
}

/// The run's spill settings a node-buffer spill needs, copied from the run
/// when the walk starts.
pub(crate) struct WalkSpillSettings {
    pub(crate) spill_root: Arc<Path>,
    pub(crate) spill_compress: CompressMode,
    pub(crate) batch_size: usize,
}

/// The walk-owned state a reclaim on the walk may spill: a stack of frames of
/// node-buffer slots, one per dispatch scope the walk is inside, and the
/// settings their spill needs.
///
/// The top frame is the running scope's. Every slot operation
/// ([`Self::slots`], [`Self::slots_mut`], [`Self::spill_sweep_parts`]) acts
/// on the top frame only, so a composition body can never read, replace or
/// remove a slot of the scope that called it. Only a reclaim
/// ([`WalkReclaim::spill_victim`]) searches every frame: a body's shortfall
/// may spill a resident slot its callers hold.
///
/// Walk-only (`!Send`, reached through an `Rc<RefCell<_>>`). A borrow of it is
/// short and never held across a governed allocation, a `reserve`, a channel
/// wait or a call into another dispatch arm.
pub(crate) struct WalkReclaimSet {
    /// The top frame: the running dispatch scope's slots.
    slots: NodeBufferSlots,
    /// The frames beneath the top, innermost last: each calling scope's
    /// slots, kept while a composition body it entered runs.
    parents: Vec<NodeBufferSlots>,
    spill_settings: WalkSpillSettings,
}

impl WalkReclaimSet {
    /// An empty set for a walk spilling under `spill_settings`.
    pub(crate) fn new(spill_settings: WalkSpillSettings) -> Self {
        Self {
            slots: NodeBufferSlots::default(),
            parents: Vec::new(),
            spill_settings,
        }
    }

    /// The running dispatch scope's node-buffer slots: the top frame.
    pub(crate) fn slots(&self) -> &NodeBufferSlots {
        &self.slots
    }

    pub(crate) fn slots_mut(&mut self) -> &mut NodeBufferSlots {
        &mut self.slots
    }

    /// Make `slots` the current scope, returning the scope it replaces.
    pub(crate) fn replace_slots(&mut self, slots: NodeBufferSlots) -> NodeBufferSlots {
        std::mem::replace(&mut self.slots, slots)
    }

    /// Take the top frame's slots, leaving an empty one. The run's teardown
    /// takes the top level's slots this way once every body frame is gone.
    pub(crate) fn take_slots(&mut self) -> NodeBufferSlots {
        debug_assert!(
            self.parents.is_empty(),
            "the walk's teardown runs with no composition body frame pushed"
        );
        std::mem::take(&mut self.slots)
    }

    /// Make `frame` the top frame, keeping the running scope's slots beneath
    /// it, until the returned guard pops it.
    ///
    /// The guard pops on every exit: [`FrameGuard::pop`] hands the body's
    /// frame back so its caller releases it in its own order; a guard dropped
    /// without that (an error returned early, or an unwind) restores the
    /// caller's frame and releases the body frame's residue against
    /// `arbitrator`. Borrows the set only while it pushes.
    pub(crate) fn push_frame(
        set: &Rc<RefCell<Self>>,
        frame: NodeBufferSlots,
        arbitrator: &Arc<MemoryArbitrator>,
    ) -> FrameGuard {
        let depth = {
            let mut this = set.borrow_mut();
            let parent = std::mem::replace(&mut this.slots, frame);
            this.parents.push(parent);
            this.depth()
        };
        FrameGuard {
            set: Rc::clone(set),
            arbitrator: Arc::clone(arbitrator),
            depth,
            pushed: true,
        }
    }

    /// Pop the top frame, making the frame beneath it the top again. `None`
    /// at the bottom frame, which is never popped.
    pub(crate) fn pop_frame(&mut self) -> Option<NodeBufferSlots> {
        let parent = self.parents.pop()?;
        Some(std::mem::replace(&mut self.slots, parent))
    }

    /// How many frames the set holds, the running scope's included.
    fn depth(&self) -> usize {
        self.parents.len() + 1
    }

    #[cfg(test)]
    pub(crate) fn frame_depth(&self) -> usize {
        self.depth()
    }

    /// The top frame's buffers, mutably, beside its registrations and the
    /// spill settings: what the node-buffer spill sweep works on.
    pub(crate) fn spill_sweep_parts(
        &mut self,
    ) -> (
        &mut HashMap<NodeBufferKey, NodeBuffer>,
        &HashMap<NodeBufferKey, SlotRegistration>,
        &WalkSpillSettings,
    ) {
        (
            &mut self.slots.buffers,
            &self.slots.registrations,
            &self.spill_settings,
        )
    }
}

/// Keeps a composition body's frame on top of the walk reclaim set and pops
/// it on every exit.
///
/// Holds the set by `Rc`, not through the executor context, so the body's
/// dispatch keeps its `&mut` context while the guard lives. `!Send`.
#[must_use = "the body's frame is popped as soon as the guard drops"]
pub(crate) struct FrameGuard {
    set: Rc<RefCell<WalkReclaimSet>>,
    arbitrator: Arc<MemoryArbitrator>,
    /// The set's depth with this guard's frame on top.
    depth: usize,
    /// Cleared once [`Self::pop`] popped the frame, so the drop does not.
    pushed: bool,
}

impl FrameGuard {
    /// Pop the body's frame, restoring the caller's as the top, and return
    /// the body's slots for the caller to release in its own order.
    pub(crate) fn pop(mut self) -> NodeBufferSlots {
        self.pushed = false;
        let mut set = self.set.borrow_mut();
        debug_assert_eq!(
            set.depth(),
            self.depth,
            "a body's frame is popped only while it is the top frame"
        );
        let frame = set.pop_frame();
        debug_assert!(frame.is_some(), "a pushed frame is above the bottom");
        frame.unwrap_or_default()
    }
}

impl Drop for FrameGuard {
    fn drop(&mut self) {
        let _ = (&self.set, &self.arbitrator, self.depth, self.pushed);
    }
}

/// What a reclaim pass got from asking the walk to spill one elected
/// consumer's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VictimOutcome {
    /// The walk owns the consumer's state and spilled whatever of it was
    /// resident, now, on the walk.
    Spilled,
    /// The walk holds no spillable state for the consumer: another thread
    /// owns it, or it is not the kind of state a pass spills. Skipped; its
    /// owner is never asked to act.
    NotOwned,
    /// The walk owns the consumer's state but cannot spill it now: the
    /// running dispatch arm holds it, or the reclaim set itself is borrowed.
    /// Frees nothing this pass. When the set could tell which state it is,
    /// that state's own spill request is raised, so the walk spills it at
    /// its next safe point.
    Busy,
}

/// The walk-owned state a reclaim pass can spill, by consumer.
///
/// Implemented by [`WalkReclaimSet`]; a pass reaches it only on the walk,
/// only while nothing else borrows the set, and never with the ledger lock
/// held.
pub(crate) trait WalkReclaim {
    /// Spill the state the walk holds for consumer `id` now, charging any
    /// spill file to `arbitrator`'s disk quota. Blocks on the spill's I/O;
    /// never reserves memory and never waits on another thread. An error is
    /// a failed spill (I/O, or the disk quota passed), which ends the pass.
    fn spill_victim(
        &mut self,
        id: ConsumerId,
        arbitrator: &MemoryArbitrator,
    ) -> Result<VictimOutcome, PipelineError>;
}

impl WalkReclaim for WalkReclaimSet {
    fn spill_victim(
        &mut self,
        id: ConsumerId,
        arbitrator: &MemoryArbitrator,
    ) -> Result<VictimOutcome, PipelineError> {
        Ok(self
            .slots
            .spill_registered(id, arbitrator, &self.spill_settings)?
            .unwrap_or(VictimOutcome::NotOwned))
    }
}

/// The stand-in a pass uses when the reclaim set is already borrowed (the
/// walk is part-way through changing it): it cannot tell which consumers the
/// walk owns, so every candidate is `Busy` and nothing is flagged.
pub(crate) struct BorrowedReclaimSet;

impl WalkReclaim for BorrowedReclaimSet {
    fn spill_victim(
        &mut self,
        _id: ConsumerId,
        _arbitrator: &MemoryArbitrator,
    ) -> Result<VictimOutcome, PipelineError> {
        Ok(VictimOutcome::Busy)
    }
}

/// One installed walk: whose run it is and the state that walk owns.
struct WalkFrame {
    /// The run's arbitrator, compared by address. Weak so the frame never
    /// keeps the arbitrator alive; holding it still keeps the allocation, so
    /// no other arbitrator can take the same address while the frame is
    /// installed.
    arbitrator: Weak<MemoryArbitrator>,
    reclaim: Rc<RefCell<WalkReclaimSet>>,
}

impl WalkFrame {
    fn is_for(&self, arbitrator: &MemoryArbitrator) -> bool {
        std::ptr::eq(self.arbitrator.as_ptr(), arbitrator)
    }
}

thread_local! {
    static WALK_FRAME: RefCell<Option<WalkFrame>> = const { RefCell::new(None) };
}

/// Installs a run's walk frame on the current thread and puts back whatever
/// frame was there before when dropped, on return, error or unwind alike.
///
/// Holds an `Rc`, so it is `!Send`: the frame is uninstalled on the thread
/// that installed it.
#[must_use = "the walk frame is uninstalled as soon as the guard drops"]
pub(crate) struct WalkContextGuard {
    previous: Option<WalkFrame>,
}

impl WalkContextGuard {
    /// Make the current thread `arbitrator`'s walk, owning `reclaim`, until
    /// the guard drops.
    pub(crate) fn install(
        arbitrator: &Arc<MemoryArbitrator>,
        reclaim: Rc<RefCell<WalkReclaimSet>>,
    ) -> Self {
        let frame = WalkFrame {
            arbitrator: Arc::downgrade(arbitrator),
            reclaim,
        };
        Self {
            previous: WALK_FRAME.with_borrow_mut(|slot| slot.replace(frame)),
        }
    }
}

impl Drop for WalkContextGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        // The uninstalled frame may hold the last reference to its set. It
        // drops after the slot is released, so nothing the set's contents do
        // on drop can meet a borrowed slot.
        let uninstalled = WALK_FRAME.with_borrow_mut(|slot| std::mem::replace(slot, previous));
        drop(uninstalled);
    }
}

/// Classify the calling thread against `arbitrator`'s run.
///
/// `Walk` only on the thread holding that run's installed frame. A rayon
/// worker is never the walk, even while the walk blocks in the pool's
/// `install` waiting on it.
pub(crate) fn thread_role(arbitrator: &MemoryArbitrator) -> ThreadRole {
    let on_walk = WALK_FRAME
        .try_with(|slot| {
            slot.borrow()
                .as_ref()
                .is_some_and(|frame| frame.is_for(arbitrator))
        })
        .unwrap_or(false);
    if on_walk {
        ThreadRole::Walk
    } else if rayon::current_thread_index().is_some() {
        ThreadRole::RayonWorker
    } else {
        ThreadRole::OffWalk
    }
}

/// The reclaim set of `arbitrator`'s walk when the calling thread is that
/// walk; `None` on any other thread.
///
/// A caller borrows the set only for a short scope, never across a governed
/// allocation, a `reserve`, a channel wait or a call into another dispatch
/// arm.
pub(crate) fn walk_reclaim_set(
    arbitrator: &MemoryArbitrator,
) -> Option<Rc<RefCell<WalkReclaimSet>>> {
    WALK_FRAME
        .try_with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|frame| frame.is_for(arbitrator))
                .map(|frame| Rc::clone(&frame.reclaim))
        })
        .ok()
        .flatten()
}

#[cfg(test)]
thread_local! {
    static TEST_RECLAIM: RefCell<Option<Rc<RefCell<dyn WalkReclaim>>>> = const { RefCell::new(None) };
}

/// Run `body` with `reclaim` standing in for the walk's reclaim set in every
/// pass this thread runs, so a unit test can script what each victim frees.
#[cfg(test)]
pub(crate) fn with_test_reclaim<R>(
    reclaim: Rc<RefCell<dyn WalkReclaim>>,
    body: impl FnOnce() -> R,
) -> R {
    let previous = TEST_RECLAIM.with_borrow_mut(|slot| slot.replace(reclaim));
    let result = body();
    TEST_RECLAIM.with_borrow_mut(|slot| *slot = previous);
    result
}

/// The stand-in [`with_test_reclaim`] installed, if any.
#[cfg(test)]
pub(crate) fn test_reclaim() -> Option<Rc<RefCell<dyn WalkReclaim>>> {
    TEST_RECLAIM.with_borrow(Clone::clone)
}

/// The arbitrator whose ledger is `state`, when the calling thread is that
/// run's walk; `None` on any other thread or once the run's arbitrator is
/// gone. How a charge made through a handle or a grant, which hold only the
/// ledger, finds the run it may reclaim in.
pub(crate) fn walk_arbitrator(state: &Arc<ReservationState>) -> Option<Arc<MemoryArbitrator>> {
    WALK_FRAME
        .try_with(|slot| {
            slot.borrow()
                .as_ref()
                .and_then(|frame| frame.arbitrator.upgrade())
        })
        .ok()
        .flatten()
        .filter(|arbitrator| Arc::ptr_eq(&arbitrator.admission, state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::NoOpPolicy;

    fn arbitrator() -> Arc<MemoryArbitrator> {
        Arc::new(MemoryArbitrator::with_policy(
            64 * 1024 * 1024,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ))
    }

    fn reclaim_set() -> Rc<RefCell<WalkReclaimSet>> {
        Rc::new(RefCell::new(WalkReclaimSet::new(WalkSpillSettings {
            spill_root: Arc::from(std::env::temp_dir().as_path()),
            spill_compress: CompressMode::Auto,
            batch_size: 1024,
        })))
    }

    fn owns(arbitrator: &MemoryArbitrator, set: &Rc<RefCell<WalkReclaimSet>>) -> bool {
        walk_reclaim_set(arbitrator).is_some_and(|installed| Rc::ptr_eq(&installed, set))
    }

    #[test]
    fn walk_context_guard_restores_the_previous_frame() {
        let outer_run = arbitrator();
        let inner_run = arbitrator();
        let outer_set = reclaim_set();
        let inner_set = reclaim_set();

        let outer = WalkContextGuard::install(&outer_run, Rc::clone(&outer_set));
        assert_eq!(thread_role(&outer_run), ThreadRole::Walk);
        assert!(owns(&outer_run, &outer_set));

        {
            let _inner = WalkContextGuard::install(&inner_run, Rc::clone(&inner_set));
            assert_eq!(thread_role(&inner_run), ThreadRole::Walk);
            assert_eq!(thread_role(&outer_run), ThreadRole::OffWalk);
            assert!(owns(&inner_run, &inner_set));
            assert!(walk_reclaim_set(&outer_run).is_none());
        }
        assert_eq!(thread_role(&outer_run), ThreadRole::Walk);
        assert_eq!(thread_role(&inner_run), ThreadRole::OffWalk);
        assert!(owns(&outer_run, &outer_set));

        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _inner = WalkContextGuard::install(&inner_run, Rc::clone(&inner_set));
            assert_eq!(thread_role(&inner_run), ThreadRole::Walk);
            panic!("unwinding out of a nested walk frame");
        }));
        assert!(unwound.is_err());
        assert_eq!(thread_role(&outer_run), ThreadRole::Walk);
        assert_eq!(thread_role(&inner_run), ThreadRole::OffWalk);
        assert!(owns(&outer_run, &outer_set));

        drop(outer);
        assert_eq!(thread_role(&outer_run), ThreadRole::OffWalk);
        assert!(walk_reclaim_set(&outer_run).is_none());
        // Only the test's own handles remain: the frame kept no set alive.
        assert_eq!(Rc::strong_count(&outer_set), 1);
        assert_eq!(Rc::strong_count(&inner_set), 1);
    }

    #[test]
    fn thread_role_classifies_walk_rayon_and_off_walk() {
        let run = arbitrator();
        let other_run = arbitrator();
        assert_eq!(thread_role(&run), ThreadRole::OffWalk);

        let _walk = WalkContextGuard::install(&run, reclaim_set());
        assert_eq!(thread_role(&run), ThreadRole::Walk);
        assert_eq!(thread_role(&other_run), ThreadRole::OffWalk);

        let spawned = std::thread::scope(|scope| scope.spawn(|| thread_role(&run)).join());
        assert_eq!(spawned.expect("spawned thread"), ThreadRole::OffWalk);

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("rayon pool");
        assert_eq!(pool.install(|| thread_role(&run)), ThreadRole::RayonWorker);
        assert_eq!(thread_role(&run), ThreadRole::Walk);
    }
}

#[cfg(test)]
mod frame_tests {
    use super::*;
    use crate::executor::node_buffer::NodeBufferConsumer;
    use crate::pipeline::memory::Priority;
    use crate::pipeline::memory::ledger::Requester;
    use clinker_plan::runtime_error::{ConsumerLabel, MemorySurface};
    use clinker_record::owned_storage::SharedStorage;
    use clinker_record::{Record, Schema, Value};
    use petgraph::graph::NodeIndex;

    const KIB: u64 = 1024;

    fn arbitrator(limit: u64) -> Arc<MemoryArbitrator> {
        Arc::new(MemoryArbitrator::with_policy(
            limit,
            0.80,
            0.70,
            Box::new(Priority),
        ))
    }

    fn reclaim_set(spill_root: &Path) -> Rc<RefCell<WalkReclaimSet>> {
        Rc::new(RefCell::new(WalkReclaimSet::new(WalkSpillSettings {
            spill_root: Arc::from(spill_root),
            spill_compress: CompressMode::Auto,
            batch_size: 1024,
        })))
    }

    /// Publish a resident slot of `rows` records at node 0 of `slots`, its
    /// `NodeBufferConsumer` registered under `node` and charged `charge`
    /// bytes.
    fn publish_slot(
        arbitrator: &MemoryArbitrator,
        slots: &mut NodeBufferSlots,
        node: &str,
        rows: usize,
        charge: u64,
    ) -> (NodeBufferKey, ConsumerId, Arc<ConsumerHandle>) {
        let handle = ConsumerHandle::new();
        let id = arbitrator.register_node_consumer(
            Arc::new(NodeBufferConsumer::new(Arc::clone(&handle))),
            Arc::clone(&handle),
            ConsumerLabel {
                node: node.to_string(),
                surface: MemorySurface::BufferedRows {
                    from: node.to_string(),
                    to: "next".to_string(),
                },
            },
        );
        handle.set_bytes(charge);
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "id".into(),
            "payload".into(),
        ])));
        let records: Vec<(Record, u64)> = (0..rows)
            .map(|row| {
                (
                    Record::new(
                        schema.clone(),
                        vec![
                            Value::Integer(row as i64),
                            Value::String(format!("{node}-{row:05}").into()),
                        ],
                    ),
                    row as u64,
                )
            })
            .collect();
        let key = NodeBufferKey::from(NodeIndex::new(0));
        slots.register(
            key.clone(),
            (id, Arc::clone(&handle)),
            SlotSpill {
                spill_allowed: true,
                node_name: Box::from(node),
            },
        );
        slots.insert_buffer(key.clone(), NodeBuffer::memory_from_records(records));
        (key, id, handle)
    }

    fn is_resident(slots: &NodeBufferSlots, key: &NodeBufferKey) -> bool {
        matches!(slots.buffer(key), Some(NodeBuffer::Memory(_)))
    }

    fn body_failure() -> Result<(), PipelineError> {
        Err(PipelineError::Internal {
            op: "test",
            node: "body".to_string(),
            detail: "a body arm failed".to_string(),
        })
    }

    /// A body scope that pushes a frame holding one registered slot of its
    /// own and then leaves through `?`.
    fn body_scope_returning_err(
        set: &Rc<RefCell<WalkReclaimSet>>,
        arbitrator: &Arc<MemoryArbitrator>,
    ) -> Result<ConsumerId, PipelineError> {
        let mut body = NodeBufferSlots::default();
        let (_, body_id, _) = publish_slot(arbitrator, &mut body, "body_rows", 4, KIB);
        let _frame = WalkReclaimSet::push_frame(set, body, arbitrator);
        assert_eq!(set.borrow().frame_depth(), 2);
        body_failure()?;
        Ok(body_id)
    }

    #[test]
    fn body_error_restores_the_parent_frame() {
        let root = tempfile::tempdir().expect("spill root");
        let arbitrator = arbitrator(64 * 1024 * KIB);
        let set = reclaim_set(root.path());
        let (parent_key, parent_id, parent_handle) = {
            let mut guard = set.borrow_mut();
            publish_slot(&arbitrator, guard.slots_mut(), "parent_rows", 8, 2 * KIB)
        };
        let parent_intact = |set: &Rc<RefCell<WalkReclaimSet>>| {
            let set = set.borrow();
            set.frame_depth() == 1
                && is_resident(set.slots(), &parent_key)
                && set
                    .slots()
                    .registrations()
                    .get(&parent_key)
                    .is_some_and(|(id, _)| *id == parent_id)
        };

        let consumers_before = arbitrator.consumer_count();
        let returned = body_scope_returning_err(&set, &arbitrator);
        assert!(returned.is_err(), "the body's error reaches its caller");
        assert!(
            parent_intact(&set),
            "an error leaving the body restores the parent's frame with its slot"
        );
        assert_eq!(
            arbitrator.consumer_count(),
            consumers_before,
            "the body frame's residue is released: its slot consumer is unregistered"
        );

        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut body = NodeBufferSlots::default();
            publish_slot(&arbitrator, &mut body, "body_rows", 4, KIB);
            let _frame = WalkReclaimSet::push_frame(&set, body, &arbitrator);
            assert_eq!(set.borrow().frame_depth(), 2);
            panic!("a body arm panicked");
        }));
        assert!(unwound.is_err());
        assert!(
            parent_intact(&set),
            "an unwind out of the body restores the parent's frame with its slot"
        );
        assert_eq!(arbitrator.consumer_count(), consumers_before);
        assert_eq!(parent_handle.bytes(), 2 * KIB, "the parent's charge stands");
    }

    /// While a body's frame is on top, the body sees none of its parent's
    /// slots, yet a shortfall inside it spills the parent's resident slot
    /// before it is refused.
    #[test]
    fn a_body_shortfall_spills_a_parent_slot_it_cannot_otherwise_reach() {
        let root = tempfile::tempdir().expect("spill root");
        let arbitrator = arbitrator(256 * KIB);
        let set = reclaim_set(root.path());
        let _walk = WalkContextGuard::install(&arbitrator, Rc::clone(&set));
        let (parent_key, parent_id, parent_handle) = {
            let mut guard = set.borrow_mut();
            publish_slot(&arbitrator, guard.slots_mut(), "parent_rows", 64, 128 * KIB)
        };

        let frame = WalkReclaimSet::push_frame(&set, NodeBufferSlots::default(), &arbitrator);
        {
            let set = set.borrow();
            assert!(
                !set.slots().contains_buffer(&parent_key)
                    && !set.slots().is_registered(&parent_key),
                "a body's slot operations act on its own frame only"
            );
        }
        let _filler = arbitrator
            .reserve(64 * KIB, Requester::governed())
            .expect("the filler fits beside the parent slot");
        let grant = arbitrator
            .reserve(128 * KIB, Requester::governed())
            .expect("the body's request spills the parent slot and is granted");
        assert_eq!(grant.bytes(), 128 * KIB);
        assert_eq!(
            parent_handle.bytes(),
            0,
            "the parent slot's charge left with its rows"
        );
        drop(frame.pop());

        let set = set.borrow();
        assert_eq!(set.frame_depth(), 1);
        assert!(
            matches!(set.slots().buffer(&parent_key), Some(NodeBuffer::Spilled { .. })),
            "the parent's slot is on disk once its frame is back on top"
        );
        assert!(set.slots().is_registered(&parent_key));
        assert!(arbitrator.cumulative_spill_bytes() > 0);
        assert!(arbitrator.unregister_consumer(parent_id).is_some());
    }
}
