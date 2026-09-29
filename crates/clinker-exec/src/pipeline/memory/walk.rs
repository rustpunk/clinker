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
use std::rc::Rc;
use std::sync::{Arc, Weak};

use super::MemoryArbitrator;

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

/// The walk-owned state a reclaim on the walk may spill.
pub(crate) struct WalkReclaimSet {}

impl WalkReclaimSet {
    pub(crate) fn new() -> Self {
        Self {}
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
        Rc::new(RefCell::new(WalkReclaimSet::new()))
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
