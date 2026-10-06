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
//! reaches the state its callers are holding. Outside the frames the set
//! also reaches every walk-owned state registered through
//! [`register_walk_owned`] (the document dead-letter state's held rows, an
//! Output's per-document buckets, the rows parked for a deferred consumer,
//! a Cull's or Reshape's group buffers, a grace-hash join's build partitions
//! and a hash Aggregate's group tables), which any pass can spill in place.
//! No sort registers: a sort spills on a threshold of its own.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::{Rc, Weak as RcWeak};
use std::sync::{Arc, Weak};

use clinker_plan::config::CompressMode;
use clinker_plan::error::PipelineError;

use super::reservation::ReservationState;
use super::{ConsumerHandle, ConsumerId, MemoryArbitrator};
use crate::executor::dispatch::{
    NodeBufferKey, NodeBufferReaderLedger, ResidentSlotSpill, SlotSpillResult,
};
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
/// A slot's registration and its spill facts change together, through
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
    /// arm: its spill request is raised and it is `Busy`. A slot whose rows a
    /// live cursor or view still shares is `Busy` too: the spill writes
    /// nothing and its rows stay where the reader holds them, and its spill
    /// request is raised. The walk's sweep at the next node dispatch clears
    /// that request and writes nothing while the reader still shares the
    /// rows; the next pass that elects the slot raises it again. A slot its
    /// compiled classification keeps in memory is `NotOwned`.
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
        let spilled = ResidentSlotSpill {
            arbitrator,
            spill_root: spill_settings.spill_root.as_ref(),
            spill_compress: spill_settings.spill_compress,
            batch_size: spill_settings.batch_size,
        }
        .spill_slot(&mut self.buffers, &key, &handle, &node_name)?;
        if spilled == SlotSpillResult::StillShared {
            handle.request_spill();
            return Ok(Some(VictimOutcome::Busy));
        }
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
/// Beside the frames, outside every scope, the set holds a registry of the
/// walk-owned state that lives in cells of its own ([`register_walk_owned`]):
/// an owner's borrow and the set's are independent, so a request an owner
/// makes while it holds its own cell can still spill every other victim, and
/// a pass any other request starts can spill the owner's state.
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
    /// The walk-owned cells registered through [`register_walk_owned`], by
    /// the consumer each charges. Run-scoped, outside every frame, so a
    /// composition body's shortfall reaches the state its callers own.
    owned: HashMap<ConsumerId, Vec<OwnedEntry>>,
    /// The serial the next registration's entry takes, so a registration
    /// removes only its own entry among several under one consumer.
    next_owned_serial: u64,
}

/// One registered walk-owned cell: a `Weak`, so a registration never keeps
/// its state alive and a dropped owner is never reached, and the handle of
/// the consumer it charges, on which a pass that finds the cell borrowed
/// raises the spill request.
struct OwnedEntry {
    serial: u64,
    cell: RcWeak<RefCell<dyn WalkOwnedSpill>>,
    handle: Arc<ConsumerHandle>,
}

impl WalkReclaimSet {
    /// An empty set for a walk spilling under `spill_settings`.
    pub(crate) fn new(spill_settings: WalkSpillSettings) -> Self {
        Self {
            slots: NodeBufferSlots::default(),
            parents: Vec::new(),
            spill_settings,
            owned: HashMap::new(),
            next_owned_serial: 0,
        }
    }

    /// Enter `cell` under consumer `id`, dropping any entry for `id` whose
    /// owner is gone; returns the entry's serial.
    fn enter_owned(
        &mut self,
        id: ConsumerId,
        cell: RcWeak<RefCell<dyn WalkOwnedSpill>>,
        handle: Arc<ConsumerHandle>,
    ) -> u64 {
        let serial = self.next_owned_serial;
        self.next_owned_serial += 1;
        let entries = self.owned.entry(id).or_default();
        entries.retain(|entry| entry.cell.strong_count() > 0);
        entries.push(OwnedEntry {
            serial,
            cell,
            handle,
        });
        serial
    }

    /// Remove the entry `serial` made under consumer `id`, if a pass has not
    /// already pruned it.
    fn leave_owned(&mut self, id: ConsumerId, serial: u64) {
        if let Some(entries) = self.owned.get_mut(&id) {
            entries.retain(|entry| entry.serial != serial);
            if entries.is_empty() {
                self.owned.remove(&id);
            }
        }
    }

    /// How many walk-owned cells are entered under consumer `id`, live or
    /// not yet pruned.
    #[cfg(test)]
    pub(crate) fn owned_cell_count(&self, id: ConsumerId) -> usize {
        self.owned.get(&id).map_or(0, Vec::len)
    }

    /// Spill the walk-owned state registered under consumer `id`.
    ///
    /// Every live cell entered under `id` that is free spills what `id`
    /// charges, synchronously ([`WalkOwnedSpill::spill_owned`]); the victim
    /// is `Spilled` only when at least one of them wrote. A cell that holds
    /// state for `id` but had nothing it could write, and a cell that is
    /// borrowed (its owner is mid-mutation, or is the requester), free
    /// nothing now: the consumer's spill request is raised, which the owner
    /// answers at its next boundary, and the victim is `Busy` when no cell
    /// wrote. With no live cell, or none that still holds state for `id`,
    /// the entry is dropped and the victim is `NotOwned`. A spill never
    /// reserves memory; past the spill cap it fails with E320.
    fn spill_owned_victim(
        &mut self,
        id: ConsumerId,
        arbitrator: &MemoryArbitrator,
    ) -> Result<VictimOutcome, PipelineError> {
        let Some(entries) = self.owned.get_mut(&id) else {
            return Ok(VictimOutcome::NotOwned);
        };
        entries.retain(|entry| entry.cell.strong_count() > 0);
        let live: Vec<_> = entries
            .iter()
            .filter_map(|entry| {
                entry
                    .cell
                    .upgrade()
                    .map(|cell| (cell, Arc::clone(&entry.handle)))
            })
            .collect();
        let mut wrote = false;
        let mut unwritten = Vec::new();
        for (cell, handle) in &live {
            match cell.try_borrow_mut() {
                Ok(mut owner) => match owner.spill_owned(id, arbitrator)? {
                    OwnedSpillResult::Wrote => wrote = true,
                    OwnedSpillResult::NothingToWrite => unwritten.push(handle),
                    OwnedSpillResult::NotHeld => {}
                },
                Err(_) => unwritten.push(handle),
            }
        }
        for handle in &unwritten {
            handle.request_spill();
        }
        if wrote {
            Ok(VictimOutcome::Spilled)
        } else if !unwritten.is_empty() {
            Ok(VictimOutcome::Busy)
        } else {
            self.owned.remove(&id);
            Ok(VictimOutcome::NotOwned)
        }
    }

    /// The running dispatch scope's node-buffer slots: the top frame.
    pub(crate) fn slots(&self) -> &NodeBufferSlots {
        &self.slots
    }

    pub(crate) fn slots_mut(&mut self) -> &mut NodeBufferSlots {
        &mut self.slots
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
    /// An exit that did not pop the frame: an error returned early or an
    /// unwind. The caller's frame is restored, then the body's residue is
    /// released with no borrow of the set held.
    fn drop(&mut self) {
        if !self.pushed {
            return;
        }
        // Every borrow of the set ends before any call that could return or
        // unwind to here, so the set is free; were one still held, a second
        // borrow would panic inside an unwind, so the frame is left instead.
        let Ok(mut set) = self.set.try_borrow_mut() else {
            return;
        };
        debug_assert_eq!(
            set.depth(),
            self.depth,
            "a body's frame is popped only while it is the top frame"
        );
        let frame = set.pop_frame();
        drop(set);
        if let Some(frame) = frame {
            frame.release_residue(&self.arbitrator);
        }
    }
}

/// What a reclaim pass got from asking the walk to spill one elected
/// consumer's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VictimOutcome {
    /// The walk owns the consumer's state and wrote resident state of it to
    /// disk, now, on the walk.
    Spilled,
    /// The walk holds no spillable state for the consumer: another thread
    /// owns it, or it is not the kind of state a pass spills. Skipped; its
    /// owner is never asked to act.
    NotOwned,
    /// The walk owns the consumer's state but wrote none of it now: the
    /// running dispatch arm holds it, the reclaim set itself is borrowed,
    /// or nothing of it could be written (it is already on disk, taken out
    /// for use, or shared with a reader). Frees nothing this pass. When the
    /// set could tell which state it is, that state's own spill request is
    /// raised, so the walk spills it at its next safe point.
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
    /// The frame that registered consumer `id` spills its slot. The running
    /// scope's frame is searched first, then each calling scope's outwards,
    /// so a composition body's shortfall reaches the resident slots its
    /// callers hold. After the frames, the walk-owned state registered under
    /// `id` through [`register_walk_owned`] spills in place. Any other
    /// consumer is `NotOwned`.
    fn spill_victim(
        &mut self,
        id: ConsumerId,
        arbitrator: &MemoryArbitrator,
    ) -> Result<VictimOutcome, PipelineError> {
        let spill_settings = &self.spill_settings;
        for frame in std::iter::once(&mut self.slots).chain(self.parents.iter_mut().rev()) {
            if let Some(outcome) = frame.spill_registered(id, arbitrator, spill_settings)? {
                return Ok(outcome);
            }
        }
        self.spill_owned_victim(id, arbitrator)
    }
}

/// State the walk owns that a reclaim pass spills in place, reached through
/// the cell [`register_walk_owned`] entered in the walk reclaim set.
///
/// The owner keeps its state in an `Rc<RefCell<_>>` and borrows it only for
/// one operation of its own, never across a call that can charge another
/// consumer (a `reserve`, a `try_grow`, a checked admission), a channel wait
/// or a call into another dispatch arm. A pass that another consumer's
/// request starts can then spill the state whenever the owner is between
/// operations; one that finds the cell borrowed (the owner is mid-mutation,
/// or is itself the requester) frees nothing from it and raises the
/// consumer's spill request, which the owner answers through
/// [`ConsumerHandle::take_spill_request`] at its next push, yield or batch
/// boundary.
pub(crate) trait WalkOwnedSpill {
    /// Spill, now and on the walk, every resident spillable byte this owner
    /// charges to consumer `id`, releasing that charge from the consumer's
    /// handle and charging any spill file to `arbitrator`'s disk quota.
    /// Runs inside a reclaim pass with the walk reclaim set borrowed.
    ///
    /// Returns what the spill did ([`OwnedSpillResult`]): `Wrote` only when
    /// it wrote state to disk and released that state's charge, never for a
    /// call that found nothing it could write. A pass counts the victim
    /// spilled from that answer alone. One cell may serve several consumers
    /// and spills only what `id` charges.
    ///
    /// Never reserves memory, never blocks on another thread and never
    /// touches the walk reclaim set; blocks only on its own spill I/O.
    ///
    /// # Errors
    ///
    /// A failed spill, including E320 past the spill cap, which ends the
    /// pass.
    fn spill_owned(
        &mut self,
        id: ConsumerId,
        arbitrator: &MemoryArbitrator,
    ) -> Result<OwnedSpillResult, PipelineError>;
}

/// What one walk-owned cell's spill did for the consumer a pass elected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnedSpillResult {
    /// It wrote state it holds for the consumer to disk, releasing that
    /// state's charge.
    Wrote,
    /// It holds state for the consumer but had nothing it could write now:
    /// every part of it is already on disk, taken out for use, or shared
    /// with a reader that keeps it resident.
    NothingToWrite,
    /// It holds no state for the consumer: the state left the owner before
    /// its registration dropped, or the cell never held any for it.
    NotHeld,
}

/// Keeps a walk-owned cell entered in the walk reclaim set, under the
/// consumer it charges, until it drops.
///
/// Dropping it removes the entry when the set is free; when the set is
/// borrowed right then, the entry stays with a `Weak` that the next lookup
/// for that consumer prunes once the owner is gone, so a dropped owner is
/// never reached either way. Inert (it entered nothing) when it was made on
/// a thread with no walk frame for the run. Holds an `Rc` `Weak`, so it is
/// `!Send`.
#[must_use = "the walk-owned cell leaves the walk reclaim set as soon as its registration drops"]
pub(crate) struct WalkOwnedRegistration {
    entry: Option<RegisteredEntry>,
}

/// Where a registration's entry is: the walk's set, and the consumer and
/// serial the entry was made under.
struct RegisteredEntry {
    set: RcWeak<RefCell<WalkReclaimSet>>,
    id: ConsumerId,
    serial: u64,
}

impl WalkOwnedRegistration {
    /// A registration that entered nothing, for a test fixture that builds
    /// an owner's state without registering it.
    #[cfg(test)]
    pub(crate) fn inert() -> Self {
        Self { entry: None }
    }
}

impl Drop for WalkOwnedRegistration {
    fn drop(&mut self) {
        let Some(entry) = self.entry.take() else {
            return;
        };
        if let Some(set) = entry.set.upgrade()
            && let Ok(mut set) = set.try_borrow_mut()
        {
            set.leave_owned(entry.id, entry.serial);
        }
    }
}

/// Make the walk-owned state in `cell`, charged to consumer `id` through
/// `handle`, a victim every reclaim pass on `arbitrator`'s walk can spill,
/// until the returned registration drops.
///
/// The set keeps a `Weak` to the cell, never the cell itself: the owner
/// keeps the registration beside its state (in the same struct, or a local
/// declared after the state), so both drop together on every exit, `?` and
/// unwind included. Several cells may register under one consumer (a role
/// that charges several structures); a pass spills every one it can
/// borrow. The entry is run-scoped, outside every composition body's
/// frame.
///
/// The owner's contract is [`WalkOwnedSpill`]'s: it borrows its cell only
/// for its own operation, never across a call that can charge another
/// consumer, a channel wait or a call into another dispatch arm, answers a
/// raised spill request at its next push, yield or batch boundary, and its
/// spill never reserves.
///
/// Call it on the walk after the run's walk frame is installed, and never
/// from inside a pass. On a thread with no walk frame for `arbitrator` (a
/// unit test building an operator without a run) it registers nothing and
/// returns an inert registration; such a thread is never a rayon worker.
///
/// # Errors
///
/// [`PipelineError::Internal`] when the walk reclaim set is borrowed right
/// now: registration takes one short borrow of it, and a borrow held here
/// means a caller broke the rule that none is held across an owner's
/// operation. Nothing is registered.
pub(crate) fn register_walk_owned<S: WalkOwnedSpill + 'static>(
    arbitrator: &MemoryArbitrator,
    id: ConsumerId,
    handle: &Arc<ConsumerHandle>,
    cell: &Rc<RefCell<S>>,
) -> Result<WalkOwnedRegistration, PipelineError> {
    let Some(set) = walk_reclaim_set(arbitrator) else {
        debug_assert!(
            rayon::current_thread_index().is_none(),
            "walk-owned state is registered on the walk, never on a rayon worker"
        );
        return Ok(WalkOwnedRegistration { entry: None });
    };
    let Ok(mut borrowed) = set.try_borrow_mut() else {
        return Err(PipelineError::Internal {
            op: "memory reclaim",
            node: consumer_node(arbitrator, id),
            detail: "walk-owned state was registered while the walk reclaim set was borrowed"
                .to_string(),
        });
    };
    let owned: RcWeak<RefCell<S>> = Rc::downgrade(cell);
    let owned: RcWeak<RefCell<dyn WalkOwnedSpill>> = owned;
    let serial = borrowed.enter_owned(id, owned, Arc::clone(handle));
    drop(borrowed);
    Ok(WalkOwnedRegistration {
        entry: Some(RegisteredEntry {
            set: Rc::downgrade(&set),
            id,
            serial,
        }),
    })
}

/// The node consumer `id` is registered under, for a diagnostic; empty when
/// the ledger holds no label for it.
fn consumer_node(arbitrator: &MemoryArbitrator, id: ConsumerId) -> String {
    arbitrator
        .admission
        .ledger
        .lock()
        .label(id.0)
        .map(|label| label.node.clone())
        .unwrap_or_default()
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
                    to: vec!["next".to_string()],
                },
            },
        );
        handle.set_bytes(charge);
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "payload".into()])));
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
            matches!(
                set.slots().buffer(&parent_key),
                Some(NodeBuffer::Spilled { .. })
            ),
            "the parent's slot is on disk once its frame is back on top"
        );
        assert!(set.slots().is_registered(&parent_key));
        assert!(arbitrator.cumulative_spill_bytes() > 0);
        assert!(arbitrator.unregister_consumer(parent_id).is_some());
    }
}

/// What a per-site test needs to show that a pass another consumer starts
/// reaches walk-owned state: a walk frame with a fresh reclaim set, a
/// request made on the walk by a consumer that owns nothing, and a
/// registered stand-in owner.
#[cfg(test)]
pub(crate) mod walk_test_support {
    use super::*;
    use crate::executor::node_buffer::NodeBufferConsumer;
    use crate::pipeline::memory::ledger::{Grant, Requester, Shortfall};
    use crate::pipeline::memory::{ConsumerSpillError, MemoryConsumer};
    use clinker_plan::runtime_error::{ConsumerLabel, MemorySurface};

    /// Run `body` as `arbitrator`'s walk, with a fresh walk reclaim set
    /// installed for its duration and node-buffer spills going to a
    /// temporary directory removed afterwards.
    pub(crate) fn with_test_walk_frame<R>(
        arbitrator: &Arc<MemoryArbitrator>,
        body: impl FnOnce() -> R,
    ) -> R {
        let spill_root = tempfile::tempdir().expect("walk spill root");
        let set = Rc::new(RefCell::new(WalkReclaimSet::new(WalkSpillSettings {
            spill_root: Arc::from(spill_root.path()),
            spill_compress: CompressMode::Auto,
            batch_size: 1024,
        })));
        let _walk = WalkContextGuard::install(arbitrator, set);
        body()
    }

    /// Request `bytes` on the calling thread through `reserve` in the name
    /// of a consumer that holds nothing a pass could spill, so a request
    /// that does not fit runs the walk's real reserve loop and passes, and
    /// every byte a pass frees comes from some other consumer's state.
    ///
    /// The probe consumer is unregistered before this returns; a granted
    /// request's bytes stay charged until the grant drops.
    pub(crate) fn foreign_walk_request(
        arbitrator: &MemoryArbitrator,
        bytes: u64,
    ) -> Result<Grant, Shortfall> {
        let handle = ConsumerHandle::new();
        let probe = arbitrator.register_consumer(
            Arc::new(ForeignProbe {
                handle: Arc::clone(&handle),
            }),
            handle,
            ConsumerLabel {
                node: "foreign request".to_string(),
                surface: MemorySurface::ScanMaterialization,
            },
        );
        let result = arbitrator.reserve(bytes, Requester::for_consumer(probe));
        arbitrator.unregister_consumer(probe);
        result
    }

    /// A consumer id no arbitrator issued, for an operator a test builds
    /// with no run: with no walk frame on the thread, nothing registers
    /// under it.
    pub(crate) fn unregistered_consumer_id() -> ConsumerId {
        ConsumerId(u32::MAX)
    }

    /// The consumer a foreign request is made in the name of: charged only
    /// through its grants, with nothing a spill could free.
    struct ForeignProbe {
        handle: Arc<ConsumerHandle>,
    }

    impl MemoryConsumer for ForeignProbe {
        fn current_usage(&self) -> u64 {
            self.handle.bytes()
        }

        fn reclaimable_bytes(&self) -> u64 {
            0
        }

        fn spill_priority(&self) -> i32 {
            i32::MAX
        }

        fn try_spill(&self, _target_bytes: u64) -> Result<u64, ConsumerSpillError> {
            Ok(0)
        }

        fn can_back_pressure(&self) -> bool {
            false
        }
    }

    /// A stand-in walk-owned state: values held resident, charged a fixed
    /// figure, that its spill moves to a stand-in disk.
    pub(crate) struct TestOwnedState {
        consumer: ConsumerId,
        handle: Arc<ConsumerHandle>,
        resident: Vec<u64>,
        on_disk: Vec<u64>,
        /// What the resident values charge the consumer's handle.
        charged: u64,
        spills: usize,
    }

    impl TestOwnedState {
        /// A state charging `bytes` for `values` to consumer `consumer`
        /// through `handle`, grown there now.
        pub(crate) fn charged(
            consumer: ConsumerId,
            handle: &Arc<ConsumerHandle>,
            values: Vec<u64>,
            bytes: u64,
        ) -> Rc<RefCell<Self>> {
            handle
                .try_grow(bytes)
                .expect("a test owner's state fits when it is built");
            handle.set_reclaimable(handle.reclaimable() + bytes);
            Rc::new(RefCell::new(Self {
                consumer,
                handle: Arc::clone(handle),
                resident: values,
                on_disk: Vec::new(),
                charged: bytes,
                spills: 0,
            }))
        }

        /// Move every resident value to the stand-in disk and release its
        /// charge.
        pub(crate) fn spill(&mut self) {
            self.on_disk.append(&mut self.resident);
            self.handle.shrink(self.charged);
            self.handle
                .set_reclaimable(self.handle.reclaimable().saturating_sub(self.charged));
            self.charged = 0;
            self.spills += 1;
        }

        /// Every value held, the spilled ones first, in the order each was
        /// added.
        pub(crate) fn read_back(&self) -> Vec<u64> {
            self.on_disk.iter().chain(&self.resident).copied().collect()
        }

        pub(crate) fn is_resident(&self) -> bool {
            self.charged > 0
        }

        /// How many spills ran.
        pub(crate) fn spills(&self) -> usize {
            self.spills
        }
    }

    impl WalkOwnedSpill for TestOwnedState {
        fn spill_owned(
            &mut self,
            id: ConsumerId,
            _arbitrator: &MemoryArbitrator,
        ) -> Result<OwnedSpillResult, PipelineError> {
            if id != self.consumer {
                return Ok(OwnedSpillResult::NotHeld);
            }
            // Report what the spill did: it writes only resident values, so
            // state already on disk has nothing to write, and a cell that
            // holds no values holds no state.
            if self.is_resident() {
                self.spill();
                Ok(OwnedSpillResult::Wrote)
            } else if !self.on_disk.is_empty() {
                Ok(OwnedSpillResult::NothingToWrite)
            } else {
                Ok(OwnedSpillResult::NotHeld)
            }
        }
    }

    /// A registered stand-in owner: a node consumer and one cell of state
    /// charged to it, entered in the walk reclaim set.
    pub(crate) struct TestWalkOwned {
        pub(crate) id: ConsumerId,
        pub(crate) handle: Arc<ConsumerHandle>,
        pub(crate) cell: Rc<RefCell<TestOwnedState>>,
        pub(crate) registration: WalkOwnedRegistration,
    }

    impl TestWalkOwned {
        /// Register a node consumer named `node`, charge it `bytes` for
        /// `values` and enter the state's cell through
        /// [`register_walk_owned`].
        pub(crate) fn register(
            arbitrator: &MemoryArbitrator,
            node: &str,
            values: Vec<u64>,
            bytes: u64,
        ) -> Self {
            let handle = ConsumerHandle::new();
            let id = arbitrator.register_node_consumer(
                Arc::new(NodeBufferConsumer::new(Arc::clone(&handle))),
                Arc::clone(&handle),
                ConsumerLabel {
                    node: node.to_string(),
                    surface: MemorySurface::SortBuffer,
                },
            );
            let cell = TestOwnedState::charged(id, &handle, values, bytes);
            let registration =
                register_walk_owned(arbitrator, id, &handle, &cell).expect("registered");
            Self {
                id,
                handle,
                cell,
                registration,
            }
        }

        /// The owner's next boundary: answer a raised spill request by
        /// spilling. Returns whether one was raised.
        pub(crate) fn answer_spill_request(&self) -> bool {
            let requested = self.handle.take_spill_request();
            if requested {
                self.cell.borrow_mut().spill();
            }
            requested
        }
    }
}

#[cfg(test)]
mod walk_owned_tests {
    use super::walk_test_support::{
        TestOwnedState, TestWalkOwned, foreign_walk_request, with_test_walk_frame,
    };
    use super::*;
    use crate::executor::node_buffer::NodeBufferConsumer;
    use crate::pipeline::memory::Priority;
    use clinker_plan::runtime_error::{ConsumerLabel, MemorySurface};

    const KIB: u64 = 1024;
    /// What the stand-in owner charges.
    const RESIDENT: u64 = 64 * KIB;
    /// What is free beside it.
    const FREE: u64 = 16 * KIB;

    /// An arbitrator whose capacity is the owner's charge plus `FREE`.
    fn arbitrator() -> Arc<MemoryArbitrator> {
        Arc::new(MemoryArbitrator::with_policy(
            RESIDENT + FREE,
            0.80,
            0.70,
            Box::new(Priority),
        ))
    }

    fn values() -> Vec<u64> {
        (0..64).collect()
    }

    /// A node consumer named `sorted` over a fresh handle.
    fn sort_consumer(arbitrator: &MemoryArbitrator) -> (ConsumerId, Arc<ConsumerHandle>) {
        let handle = ConsumerHandle::new();
        let id = arbitrator.register_node_consumer(
            Arc::new(NodeBufferConsumer::new(Arc::clone(&handle))),
            Arc::clone(&handle),
            ConsumerLabel {
                node: "sorted".to_string(),
                surface: MemorySurface::SortBuffer,
            },
        );
        (id, handle)
    }

    fn spill_victim(arbitrator: &MemoryArbitrator, id: ConsumerId) -> VictimOutcome {
        walk_reclaim_set(arbitrator)
            .expect("on the walk")
            .borrow_mut()
            .spill_victim(id, arbitrator)
            .expect("spill")
    }

    fn owned_cells(arbitrator: &MemoryArbitrator, id: ConsumerId) -> usize {
        walk_reclaim_set(arbitrator)
            .expect("on the walk")
            .borrow()
            .owned_cell_count(id)
    }

    /// A request another consumer makes on the walk, more than is free but
    /// less than is free once the owner's state is on disk, is granted by
    /// spilling that state; the owner's charge falls by all of it and its
    /// values read back whole.
    ///
    /// Capacity: `RESIDENT + FREE`; the request is `FREE + RESIDENT / 2`.
    #[test]
    fn walk_owned_state_is_spilled_by_a_pass_another_request_starts() {
        let arbitrator = arbitrator();
        with_test_walk_frame(&arbitrator, || {
            let owner = TestWalkOwned::register(&arbitrator, "sorted", values(), RESIDENT);
            assert_eq!(owner.handle.bytes(), RESIDENT);

            let grant = foreign_walk_request(&arbitrator, FREE + RESIDENT / 2)
                .expect("the pass spills the owner's state and the request fits");
            assert_eq!(grant.bytes(), FREE + RESIDENT / 2);
            assert_eq!(
                owner.handle.bytes(),
                0,
                "the owner's charge fell by all it held"
            );
            assert_eq!(
                owner.cell.borrow().spills(),
                1,
                "the pass spilled the state once"
            );
            assert!(!owner.cell.borrow().is_resident());
            assert_eq!(
                owner.cell.borrow().read_back(),
                values(),
                "the spilled values read back as they were held"
            );
            assert_eq!(
                spill_victim(&arbitrator, owner.id),
                VictimOutcome::Busy,
                "the owner still holds its consumer's state, now on disk, so a \
                 second spill writes nothing"
            );
            drop(grant);
            arbitrator.unregister_consumer(owner.id);
        });
    }

    /// While the owner holds its cell (mid-mutation, or itself the
    /// requester), the pass frees nothing from it and raises its spill
    /// request instead, which the owner's next boundary answers.
    #[test]
    fn walk_owned_state_held_by_its_owner_is_busy_and_answers_at_its_next_boundary() {
        let arbitrator = arbitrator();
        with_test_walk_frame(&arbitrator, || {
            let owner = TestWalkOwned::register(&arbitrator, "sorted", values(), RESIDENT);
            let held = owner.cell.borrow_mut();
            assert!(
                foreign_walk_request(&arbitrator, FREE + RESIDENT / 2).is_err(),
                "with the owner's cell held the pass frees nothing from it"
            );
            drop(held);
            assert_eq!(
                owner.handle.bytes(),
                RESIDENT,
                "nothing left the busy state"
            );
            assert!(owner.cell.borrow().is_resident());
            assert!(
                owner.answer_spill_request(),
                "the busy state's spill request is raised for its next boundary"
            );
            assert_eq!(owner.handle.bytes(), 0, "its spill frees all it held");
            assert_eq!(owner.cell.borrow().read_back(), values());
            arbitrator.unregister_consumer(owner.id);
        });
    }

    /// An owner that is gone is never reached: its entry is pruned at the
    /// next lookup, and a dropped registration takes its entry with it.
    #[test]
    fn dropped_walk_owned_state_is_not_owned() {
        let arbitrator = arbitrator();
        with_test_walk_frame(&arbitrator, || {
            let TestWalkOwned {
                id,
                handle,
                cell,
                registration,
            } = TestWalkOwned::register(&arbitrator, "sorted", values(), RESIDENT);
            assert_eq!(owned_cells(&arbitrator, id), 1);
            drop(cell);
            assert_eq!(spill_victim(&arbitrator, id), VictimOutcome::NotOwned);
            assert_eq!(
                owned_cells(&arbitrator, id),
                0,
                "the lookup pruned the dead owner's entry"
            );
            drop(registration);
            handle.shrink(handle.bytes());
            arbitrator.unregister_consumer(id);

            let TestWalkOwned {
                id,
                cell,
                registration,
                ..
            } = TestWalkOwned::register(&arbitrator, "sorted", values(), RESIDENT / 2);
            assert_eq!(owned_cells(&arbitrator, id), 1);
            drop(registration);
            assert_eq!(
                owned_cells(&arbitrator, id),
                0,
                "a dropped registration removes its entry"
            );
            assert_eq!(spill_victim(&arbitrator, id), VictimOutcome::NotOwned);
            assert!(cell.borrow().is_resident(), "nothing reached the state");
            arbitrator.unregister_consumer(id);
        });
    }

    /// A stand-in owner whose spill does nothing but report `result` for
    /// its consumer, counting how often it was asked.
    struct ReportingOwner {
        consumer: ConsumerId,
        result: OwnedSpillResult,
        asked: usize,
    }

    impl WalkOwnedSpill for ReportingOwner {
        fn spill_owned(
            &mut self,
            id: ConsumerId,
            _arbitrator: &MemoryArbitrator,
        ) -> Result<OwnedSpillResult, PipelineError> {
            self.asked += 1;
            Ok(if id == self.consumer {
                self.result
            } else {
                OwnedSpillResult::NotHeld
            })
        }
    }

    /// A pass counts an owned victim spilled only when one of its cells
    /// wrote. A cell that holds state for the consumer but wrote nothing is
    /// `Busy`, with the consumer's spill request raised; a cell that wrote
    /// is `Spilled`, also beside one that wrote nothing; a cell that holds no
    /// state is `NotOwned`, and its entry is pruned.
    #[test]
    fn an_owned_victim_that_writes_nothing_is_busy() {
        use OwnedSpillResult::{NotHeld, NothingToWrite, Wrote};
        let arbitrator = arbitrator();
        with_test_walk_frame(&arbitrator, || {
            for (results, expected) in [
                (vec![NothingToWrite], VictimOutcome::Busy),
                (vec![Wrote], VictimOutcome::Spilled),
                (vec![NothingToWrite, Wrote], VictimOutcome::Spilled),
                (vec![NotHeld], VictimOutcome::NotOwned),
            ] {
                let (id, handle) = sort_consumer(&arbitrator);
                let owners: Vec<_> = results
                    .iter()
                    .map(|&result| {
                        let cell = Rc::new(RefCell::new(ReportingOwner {
                            consumer: id,
                            result,
                            asked: 0,
                        }));
                        let registration = register_walk_owned(&arbitrator, id, &handle, &cell)
                            .expect("registered");
                        (cell, registration)
                    })
                    .collect();

                assert_eq!(
                    spill_victim(&arbitrator, id),
                    expected,
                    "cells reporting {results:?}"
                );
                for (cell, _) in &owners {
                    assert_eq!(cell.borrow().asked, 1, "every free cell is asked once");
                }
                if expected == VictimOutcome::Busy {
                    assert!(
                        handle.take_spill_request(),
                        "a cell that wrote nothing has its spill request raised"
                    );
                }
                let entries = if expected == VictimOutcome::NotOwned {
                    0
                } else {
                    results.len()
                };
                assert_eq!(
                    owned_cells(&arbitrator, id),
                    entries,
                    "only a victim that holds no state loses its entries"
                );
                drop(owners);
                arbitrator.unregister_consumer(id);
            }
        });
    }

    /// Two cells that charge one consumer both spill when a pass elects it.
    #[test]
    fn cells_sharing_a_consumer_id_all_spill() {
        let arbitrator = arbitrator();
        with_test_walk_frame(&arbitrator, || {
            let owner = TestWalkOwned::register(&arbitrator, "sorted", values(), RESIDENT / 2);
            let second =
                TestOwnedState::charged(owner.id, &owner.handle, (64..96).collect(), RESIDENT / 4);
            let _second_registration =
                register_walk_owned(&arbitrator, owner.id, &owner.handle, &second)
                    .expect("registered");
            assert_eq!(owned_cells(&arbitrator, owner.id), 2);
            assert_eq!(owner.handle.bytes(), RESIDENT / 2 + RESIDENT / 4);

            assert_eq!(spill_victim(&arbitrator, owner.id), VictimOutcome::Spilled);
            assert_eq!(owner.cell.borrow().spills(), 1);
            assert_eq!(second.borrow().spills(), 1, "the second cell spilled too");
            assert_eq!(owner.handle.bytes(), 0, "both cells' charges left");
            arbitrator.unregister_consumer(owner.id);
        });
    }

    /// With no walk frame installed the registration enters nothing, so a
    /// frame installed later does not reach the state.
    #[test]
    fn registration_without_a_walk_frame_is_inert() {
        let arbitrator = arbitrator();
        let (id, handle) = sort_consumer(&arbitrator);
        let cell = TestOwnedState::charged(id, &handle, values(), RESIDENT);
        let _registration = register_walk_owned(&arbitrator, id, &handle, &cell)
            .expect("a thread with no walk frame registers nothing and does not fail");
        with_test_walk_frame(&arbitrator, || {
            assert_eq!(owned_cells(&arbitrator, id), 0);
            assert_eq!(spill_victim(&arbitrator, id), VictimOutcome::NotOwned);
        });
        assert!(cell.borrow().is_resident());
        arbitrator.unregister_consumer(id);
    }

    /// Registering while the walk reclaim set is borrowed is refused, names
    /// the consumer's node and enters nothing.
    #[test]
    fn registration_while_the_set_is_borrowed_is_refused() {
        let arbitrator = arbitrator();
        with_test_walk_frame(&arbitrator, || {
            let (id, handle) = sort_consumer(&arbitrator);
            let cell = TestOwnedState::charged(id, &handle, values(), RESIDENT);
            let set = walk_reclaim_set(&arbitrator).expect("on the walk");
            let borrowed = set.borrow_mut();
            let refused = register_walk_owned(&arbitrator, id, &handle, &cell);
            drop(borrowed);
            assert!(
                matches!(&refused, Err(PipelineError::Internal { node, .. }) if node == "sorted"),
                "a borrowed set refuses the registration"
            );
            assert_eq!(owned_cells(&arbitrator, id), 0);
            arbitrator.unregister_consumer(id);
        });
    }
}
