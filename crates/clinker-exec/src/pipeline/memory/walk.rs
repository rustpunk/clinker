//! Walk-thread identity, and the walk-owned state a reclaim can reach.
//!
//! One thread walks a run's DAG, and it alone owns the node-buffer slots a
//! memory reclaim may spill. A run installs a [`WalkContextGuard`] on that
//! thread for the whole walk. Code that holds only the run's
//! [`MemoryArbitrator`] can then ask [`thread_role`] whether it is on that
//! walk, on a rayon kernel worker or on some other thread, and on the walk
//! reach the run's [`WalkReclaimSet`] through [`walk_reclaim_set`] without a
//! `&mut` path to the executor context.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Weak};

use clinker_plan::config::CompressMode;

use super::{ConsumerHandle, ConsumerId, MemoryArbitrator};
use crate::executor::dispatch::{NodeBufferKey, NodeBufferReaderLedger};
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
/// A composition body walks its own scope, swapped in for the parent's, so
/// equal body-local and parent `NodeIndex` values never collide. A slot's
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
            handle.set_bytes(0);
            arbitrator.unregister_consumer(id);
        }
    }
}

/// The run's spill settings a node-buffer spill needs, copied from the run
/// when the walk starts.
pub(crate) struct WalkSpillSettings {
    pub(crate) spill_root: Arc<Path>,
    pub(crate) spill_compress: CompressMode,
    pub(crate) batch_size: usize,
}

/// The walk-owned state a reclaim on the walk may spill: the current dispatch
/// scope's node-buffer slots and the settings their spill needs.
///
/// Walk-only (`!Send`, reached through an `Rc<RefCell<_>>`). A borrow of it is
/// short and never held across a governed allocation, a `reserve`, a channel
/// wait or a call into another dispatch arm.
pub(crate) struct WalkReclaimSet {
    slots: NodeBufferSlots,
    spill_settings: WalkSpillSettings,
}

impl WalkReclaimSet {
    /// An empty set for a walk spilling under `spill_settings`.
    pub(crate) fn new(spill_settings: WalkSpillSettings) -> Self {
        Self {
            slots: NodeBufferSlots::default(),
            spill_settings,
        }
    }

    /// The current dispatch scope's node-buffer slots.
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

    /// Take the current scope, leaving an empty one.
    pub(crate) fn take_slots(&mut self) -> NodeBufferSlots {
        std::mem::take(&mut self.slots)
    }

    /// The current scope's buffers, mutably, beside its registrations and
    /// the spill settings: what the node-buffer spill sweep works on.
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
