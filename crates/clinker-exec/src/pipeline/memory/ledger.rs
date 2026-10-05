//! The one memory ledger's public surface: [`MemoryArbitrator::reserve`], the
//! [`Grant`] it returns and the [`Shortfall`] it refuses with.
//!
//! The ledger holds charged byte counts, a release epoch and, per consumer,
//! the bytes charged in its name and their high-water mark. It never holds
//! records, RSS readings or cleanup callbacks, and nothing that performs I/O
//! runs under its lock. Its synchronized state is [`super::protocol`]; this
//! module turns raw consumer ids into [`ConsumerId`]s and refusals into
//! [`Shortfall`]s.

use super::protocol::Refusal;
use super::reservation::{LockedLedger, ReservationState};
use super::walk::{self, BorrowedReclaimSet, ThreadRole, VictimOutcome, WalkReclaim};
use super::{ConsumerId, MemoryArbitrator, MemoryConsumer, NO_WALK_REQUESTER, ReclaimCandidate};
use clinker_format::FormatError;
use clinker_format::preparation::{ResourceError, ResourceErrorKind};
use clinker_plan::error::PipelineError;
use clinker_plan::runtime_error::{
    ConsumerLabel, EnforcedLimit, HolderReport, HolderState, LimitReading, MemoryShortfallReport,
    MemorySurface, ReclaimReport, suggested_limit_floor,
};
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Whose request a [`MemoryArbitrator::reserve`] is.
///
/// A request made for a consumer is attributed to it: the granted bytes count
/// toward that consumer's charged figure and high-water mark until the grant
/// releases them. A governed request belongs to the run and is attributed to
/// no consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Requester {
    consumer: Option<ConsumerId>,
}

impl Requester {
    /// A request made in `consumer`'s name.
    pub fn for_consumer(consumer: ConsumerId) -> Self {
        Self {
            consumer: Some(consumer),
        }
    }

    /// A run-scoped request attributed to no consumer.
    pub fn governed() -> Self {
        Self { consumer: None }
    }

    /// The consumer the request is made for, if any.
    pub(crate) fn consumer(&self) -> Option<ConsumerId> {
        self.consumer
    }
}

/// Bytes charged to the ledger and not yet released.
///
/// The grant keeps the attribution it was made with: growing, shrinking and
/// dropping it adjust the same consumer's figures whatever requester is
/// current. Dropping it releases its bytes and advances the ledger's release
/// epoch. It holds only the ledger's synchronized state, never the
/// arbitrator, so a grant that outlives the run still settles the ledger.
pub struct Grant {
    state: Arc<ReservationState>,
    bytes: u64,
    attribution: Option<ConsumerId>,
}

impl Grant {
    /// Bytes this grant currently holds charged.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Charge `n` more bytes to this grant, attributed as the grant was.
    ///
    /// Each check and charge is one step under the ledger lock: on a
    /// shortfall neither the grant nor the ledger changes. On the run's walk
    /// a shortfall first runs reclaim passes, as [`MemoryArbitrator::reserve`]
    /// does, and the growth is refused only by their failure rule.
    pub fn try_grow(&mut self, n: u64) -> Result<(), Shortfall> {
        let first = match self.grow_now(n) {
            Ok(()) => return Ok(()),
            Err(shortfall) => shortfall,
        };
        match walk::walk_arbitrator(&self.state) {
            Some(arbitrator) => arbitrator.reclaim_until_granted(
                n,
                Requester {
                    consumer: self.attribution,
                },
                first,
                || self.grow_now(n),
            ),
            None => Err(first),
        }
    }

    fn grow_now(&mut self, n: u64) -> Result<(), Shortfall> {
        let mut ledger = self.state.ledger.lock();
        if let Err(refusal) = ledger.try_charge(n, self.attribution.map(|id| id.0)) {
            return Err(shortfall(&ledger, n, self.attribution, refusal));
        }
        drop(ledger);
        // The ledger's total covers this grant's bytes and just admitted `n`
        // more without overflowing, so their sum fits too.
        self.bytes += n;
        Ok(())
    }

    /// Release `n` of this grant's bytes (all of them when `n` exceeds what it
    /// holds), keeping the rest charged. A nonzero release advances the
    /// ledger's release epoch.
    pub fn shrink(&mut self, n: u64) {
        let n = n.min(self.bytes);
        if n == 0 {
            return;
        }
        self.state.release_memory(n, self.attribution);
        self.bytes -= n;
    }

    /// Hand this grant's bytes to a caller that releases them itself, through
    /// [`ReservationState::release_writer_memory`] with the same attribution.
    /// The grant then releases nothing when it drops.
    pub(crate) fn detach(mut self) -> u64 {
        std::mem::take(&mut self.bytes)
    }
}

impl Drop for Grant {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.state.release_memory(self.bytes, self.attribution);
        }
    }
}

impl std::fmt::Debug for Grant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grant")
            .field("bytes", &self.bytes)
            .field("attribution", &self.attribution)
            .finish()
    }
}

/// A [`MemoryArbitrator::reserve`] or [`Grant::try_grow`] that did not fit.
///
/// Nothing was charged. The snapshot is the ledger as the refusal saw it,
/// taken under the same lock as the check.
#[derive(Clone, Debug)]
pub struct Shortfall {
    /// Bytes the request asked for.
    pub requested: u64,
    /// Bytes the request could have been granted: the limit less the
    /// charged total.
    pub available: u64,
    /// The request exceeds the whole limit (or cannot be represented), so no
    /// release could ever make it fit.
    pub oversized: bool,
    /// The ledger at the refusal.
    pub snapshot: LedgerSnapshot,
    /// The ledger was closed to new charges when the request arrived.
    closed: bool,
    /// A test's armed forced shortfall refused the request.
    forced: bool,
    /// What the walk's reclaim round did before refusing; `None` when the
    /// request was refused without one. Boxed: most refusals are retried or
    /// recovered from, and carry none.
    round: Option<Box<RoundRecord>>,
}

impl Shortfall {
    /// Whether the refusal is because the ledger no longer admits charges
    /// rather than because the request did not fit.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    /// Whether a test's armed forced shortfall
    /// (`crate::executor::ForcedShortfall`) refused the request rather than
    /// a real shortage. Always false in a build without the `test-utils`
    /// feature or `cfg(test)`, where nothing can arm one.
    pub fn forced(&self) -> bool {
        self.forced
    }

    /// Record the reclaim round that ran before this refusal, if one did.
    fn after_round(mut self, round: Option<RoundRecord>) -> Self {
        if let Some(round) = round {
            self.round = Some(Box::new(round));
        }
        self
    }

    /// The E310 report for this refusal.
    ///
    /// Every byte figure (charged total, holders, the memory no single node
    /// holds, the suggested limit) is this shortfall's own snapshot, taken
    /// under the ledger lock at the refusal; nothing re-reads the ledger's
    /// figures. What the round asked and freed is the round's own record.
    /// Read after the snapshot, not under its lock: each listed holder's
    /// state (whether it can spill, whether it is a paused Source), which
    /// Sources are paused, their names, and the process's private memory. A
    /// state therefore describes the holder when the report is built, which
    /// for the walk that builds it is still the refusal: the walk's own
    /// borrows cannot change in between.
    ///
    /// Holder states, first match wins: the request's own consumer is
    /// [`HolderState::Requester`]; a Source (a consumer that can be paused)
    /// is [`HolderState::PausedSource`] when paused and
    /// [`HolderState::ActiveSource`] otherwise, and its bytes never count as
    /// state that cannot spill; a holder the engine had no way to spill for
    /// the request (it reports nothing reclaimable, is no longer registered,
    /// or the round elected it and found the walk does not own it) is
    /// [`HolderState::CannotSpill`], and only those holders' bytes, with the
    /// memory no single node holds, count as state that cannot spill; a
    /// spillable holder the round asked to spill and did not find in use is
    /// [`HolderState::AtFloor`] (it spilled what it could); any other
    /// spillable holder is [`HolderState::InUse`]: the round found it in use,
    /// or did not run at all (the thread that asked cannot spill the walk's
    /// state).
    pub fn into_report(self, arbitrator: &MemoryArbitrator) -> Box<MemoryShortfallReport> {
        let mut snapshot = self.snapshot;
        let requester = snapshot.requester_label.take().map(|label| *label);
        build_report(
            arbitrator,
            snapshot,
            requester,
            self.round.map(|round| *round),
            self.oversized,
        )
    }
}

/// The E310 report for a refusal whose ledger reading is `snapshot`, naming
/// `requester` as the node that asked, with the reclaim round that preceded
/// it when there was one. `oversized` is the ledger's own verdict that no
/// release could make the request fit; the report also calls a request
/// oversized when it does not fit beside what cannot spill.
///
/// A holder is the requester when it is the snapshot's requesting consumer
/// or, for a refusal made in no consumer's name, when its label is
/// `requester`. See [`Shortfall::into_report`] for the other holder states
/// and what is read after the snapshot.
fn build_report(
    arbitrator: &MemoryArbitrator,
    snapshot: LedgerSnapshot,
    requester: Option<ConsumerLabel>,
    round: Option<RoundRecord>,
    oversized: bool,
) -> Box<MemoryShortfallReport> {
    let registered = arbitrator.consumers.load();
    let consumer = |id: ConsumerId| {
        registered
            .iter()
            .find(|(candidate, _)| *candidate == id)
            .map(|(_, consumer)| consumer)
    };
    let spilled_what_it_could = |id: ConsumerId| {
        round.as_ref().is_some_and(|round| {
            round
                .asked
                .iter()
                .any(|victim| victim.consumer == id && !victim.busy)
        })
    };
    let is_requester = |holder: &HolderSnapshot| match snapshot.requester {
        Some(id) => holder.consumer == id,
        None => requester.as_ref() == Some(&holder.label),
    };

    // Whether the engine had no way to spill this holder's memory for the
    // request: it is no longer registered, a spill would free nothing from it
    // now, or the round elected it and found it out of the walk's reach. This
    // one answer decides both the holder's state and whether its bytes count
    // as state that cannot spill, so the two never disagree. A Source is never
    // such a holder: its bytes are the rows it has read, which spilling the
    // steps that hold them, or a higher limit, relieves.
    let no_spill_could_free =
        |id: ConsumerId, registered: Option<&dyn MemoryConsumer>| match registered {
            None => true,
            Some(consumer) if consumer.can_back_pressure() => false,
            Some(consumer) => {
                consumer.reclaimable_bytes() == 0
                    || round
                        .as_ref()
                        .is_some_and(|round| round.found_not_owned(id))
            }
        };

    let mut unspillable_bytes = snapshot.unattributed;
    let mut holders = Vec::with_capacity(snapshot.holders.len());
    for holder in &snapshot.holders {
        let registered = consumer(holder.consumer).map(|consumer| consumer.as_ref());
        let cannot_spill = no_spill_could_free(holder.consumer, registered);
        if cannot_spill {
            unspillable_bytes = unspillable_bytes.saturating_add(holder.charged);
        }
        let source = registered.filter(|consumer| consumer.can_back_pressure());
        let state = if is_requester(holder) {
            HolderState::Requester
        } else if let Some(source) = source {
            if source.is_paused() {
                HolderState::PausedSource
            } else {
                HolderState::ActiveSource
            }
        } else if cannot_spill {
            HolderState::CannotSpill
        } else if spilled_what_it_could(holder.consumer) {
            HolderState::AtFloor
        } else {
            HolderState::InUse
        };
        holders.push(HolderReport {
            node: holder.label.node.clone(),
            surface: holder.label.surface.clone(),
            bytes: holder.charged,
            state,
        });
    }
    let others = holders.split_off(holders.len().min(MemoryShortfallReport::LISTED_HOLDERS));
    let other_holders_bytes = others
        .iter()
        .fold(0u64, |sum, holder| sum.saturating_add(holder.bytes));

    let reclaim = round.map(|round| {
        let paused: Vec<ConsumerId> = registered
            .iter()
            .filter(|(_, consumer)| consumer.can_back_pressure() && consumer.is_paused())
            .map(|(id, _)| *id)
            .collect();
        let sources_paused = if paused.is_empty() {
            Vec::new()
        } else {
            // Labels never change for a consumer id, so this second
            // lock reads names only, never a figure.
            let ledger = arbitrator.admission.ledger.lock();
            paused
                .iter()
                .filter_map(|id| ledger.label(id.0).map(|label| label.node.clone()))
                .collect()
        };
        ReclaimReport {
            holders_asked: round
                .asked
                .into_iter()
                .filter_map(|victim| victim.node)
                .collect(),
            bytes_freed: round.freed,
            sources_paused,
        }
    });

    let requested = snapshot.requested;
    Box::new(MemoryShortfallReport {
        requester,
        group_first_row: None,
        join_partition_distinct_keys: None,
        reading: LimitReading::Charged,
        requested_bytes: requested,
        limit: snapshot.limit,
        charged_bytes: snapshot.charged,
        private_bytes: crate::pipeline::sysstats::private_memory_bytes(),
        holders,
        other_holders_count: u32::try_from(others.len()).unwrap_or(u32::MAX),
        other_holders_bytes,
        unattributed_bytes: snapshot.unattributed,
        unspillable_bytes,
        reclaim,
        suggested_limit_bytes: suggested_limit_floor(snapshot.charged, requested),
        oversized: oversized
            || requested.saturating_add(unspillable_bytes) > snapshot.limit.bytes(),
    })
}

impl std::fmt::Display for Shortfall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.closed {
            return write!(
                f,
                "memory request of {} bytes refused: the run no longer admits new charges",
                self.requested
            );
        }
        write!(
            f,
            "memory request of {} bytes does not fit: {} bytes free of the {}-byte limit ({} charged)",
            self.requested,
            self.available,
            self.snapshot.limit.bytes(),
            self.snapshot.charged
        )?;
        if self.oversized {
            f.write_str("; the request is larger than the whole limit")?;
        }
        Ok(())
    }
}

impl std::error::Error for Shortfall {}

/// The ledger's charges at one instant, read under its lock.
///
/// Every charged byte is either held by one of `holders` or counted in
/// `unattributed`, so the holders' figures plus `unattributed` equal
/// `charged` exactly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerSnapshot {
    /// The limit charges are admitted against, and whether it is
    /// `memory.limit` or a test capacity, read together under the lock.
    pub limit: EnforcedLimit,
    /// Bytes charged to the ledger.
    pub charged: u64,
    /// Bytes the request that took the snapshot asked for.
    pub requested: u64,
    /// The consumer the request was made for, if any.
    pub requester: Option<ConsumerId>,
    /// The label the requesting consumer is recorded under, if it has one.
    /// Boxed: most snapshots are dropped unread.
    pub requester_label: Option<Box<ConsumerLabel>>,
    /// Labelled consumers holding charged bytes, largest first.
    pub holders: Vec<HolderSnapshot>,
    /// Charged bytes no labelled consumer holds: grants made in no
    /// consumer's name, and grants attributed to a consumer the ledger has
    /// no label for.
    pub unattributed: u64,
}

/// One labelled consumer's charged bytes in a [`LedgerSnapshot`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HolderSnapshot {
    pub consumer: ConsumerId,
    pub label: ConsumerLabel,
    /// Its handle's charged bytes plus the bytes currently granted in its
    /// name. The current figure, not its high-water mark.
    pub charged: u64,
}

/// Read `ledger` into a snapshot for a request of `requested` bytes by
/// `requester`. Called with the ledger locked; copies the holders' labels.
fn snapshot(
    ledger: &LockedLedger,
    requested: u64,
    requester: Option<ConsumerId>,
) -> LedgerSnapshot {
    let (holders, unattributed) = ledger.holders();
    LedgerSnapshot {
        limit: if ledger.attachment.test_capacity {
            EnforcedLimit::TestCapacity(ledger.limit())
        } else {
            EnforcedLimit::MemoryLimit(ledger.limit())
        },
        charged: ledger.charged(),
        requested,
        requester,
        requester_label: requester.and_then(|id| ledger.label(id.0).cloned().map(Box::new)),
        holders: holders
            .into_iter()
            .map(|(id, label, charged)| HolderSnapshot {
                consumer: ConsumerId(id),
                label: label.clone(),
                charged,
            })
            .collect(),
        unattributed,
    }
}

pub(super) fn shortfall(
    ledger: &LockedLedger,
    requested: u64,
    requester: Option<ConsumerId>,
    refusal: Refusal,
) -> Shortfall {
    let (available, oversized, closed, forced) = match refusal {
        Refusal::Closed => (0, false, true, false),
        Refusal::Short {
            available,
            oversized,
        } => (available, oversized, false, false),
        // Nothing reported available, so the caller's pass targets the
        // whole request, as it would for a real shortage.
        Refusal::Forced => (0, false, false, true),
    };
    Shortfall {
        requested,
        available,
        oversized,
        snapshot: snapshot(ledger, requested, requester),
        closed,
        forced,
        round: None,
    }
}

/// What one reclaim pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PassOutcome {
    /// Bytes the pass's victims released: the sum of each victim's own
    /// charge decrease, measured while it spilled on the walk. Never a
    /// spill's reported figure and never the change in the ledger's total,
    /// which other threads' charges and releases move.
    pub(crate) freed: u64,
    /// Whether any release that was not a victim's progress happened while
    /// the pass ran: another thread's, or the walk's own between victims.
    pub(crate) released_during: bool,
    /// Victims the walk spilled (it owned their state and it was not held).
    pub(crate) victims_spilled: u32,
    /// The victims the pass asked to spill, in the order it asked: those it
    /// spilled and those it found in use. A victim the walk does not own was
    /// never asked and is not here.
    pub(crate) asked: Vec<AskedVictim>,
    /// The victims the pass elected and found the walk does not own, in the
    /// order it elected them. Never asked to act, so a report never names
    /// them as asked; it lists them as unable to spill.
    pub(crate) not_owned: Vec<ConsumerId>,
}

/// A victim a reclaim pass asked to spill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AskedVictim {
    pub(crate) consumer: ConsumerId,
    /// The node its label names, read when it was asked.
    pub(crate) node: Option<String>,
    /// It was in use, so the pass only raised its spill request.
    pub(crate) busy: bool,
}

/// What a walk's reclaim round (every pass one request ran) did, kept for the
/// report of a refusal that follows it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RoundRecord {
    /// Each victim asked in any pass, once, in the order first asked; `busy`
    /// is what the last pass that asked it found.
    asked: Vec<AskedVictim>,
    /// Each victim the last pass to elect it found the walk does not own,
    /// once: the round's evidence that no spill it could make would free
    /// that consumer's bytes.
    not_owned: Vec<ConsumerId>,
    /// Bytes the round's victims released themselves, over every pass.
    freed: u64,
}

impl RoundRecord {
    fn absorb(&mut self, pass: PassOutcome) {
        self.freed = self.freed.saturating_add(pass.freed);
        for victim in pass.asked {
            self.not_owned.retain(|id| *id != victim.consumer);
            match self
                .asked
                .iter_mut()
                .find(|seen| seen.consumer == victim.consumer)
            {
                Some(seen) => seen.busy = victim.busy,
                None => self.asked.push(victim),
            }
        }
        for id in pass.not_owned {
            if !self.not_owned.contains(&id) {
                self.not_owned.push(id);
            }
        }
    }

    /// Whether the last pass of the round to elect `id` found the walk does
    /// not own it.
    fn found_not_owned(&self, id: ConsumerId) -> bool {
        self.not_owned.contains(&id)
    }
}

impl PassOutcome {
    /// Whether the request that ran the pass may retry by the per-pass
    /// rule: the pass freed bytes, or a release happened while it ran.
    fn earns_a_retry(&self) -> bool {
        self.freed > 0 || self.released_during
    }
}

/// What a reclaim pass aims to free.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PassAim {
    /// Room for a request of this many bytes: the larger of its shortfall
    /// and what brings the ledger, with the request charged, down to the
    /// resume watermark.
    Request(u64),
    /// This many bytes, whatever the ledger holds.
    Shed(u64),
}

/// Which consumers a reclaim pass may elect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PassKind {
    /// Every walk-owned candidate in policy order, the requester last.
    Ordinary,
    /// The pass run before a refusal, when an ordinary pass freed nothing
    /// and nothing was released during it. Elects as an ordinary pass does;
    /// it is the pass that consumers held back by a floor are elected in.
    Final,
    /// The pass a test's forced shortfall starts: only the requester is a
    /// candidate, whatever any other consumer holds. It never leads to a
    /// refusal, and it elects as a final pass does.
    Forced,
}

impl MemoryArbitrator {
    /// Charge `bytes` to the ledger for `requester`, or refuse without
    /// charging anything.
    ///
    /// Each check and charge happens under the one ledger lock, against the
    /// same total every registered consumer's handle charges, so concurrent
    /// requesters and handle growths can never together pass the limit. A
    /// zero-byte request is granted empty and never falls short. A request
    /// larger than the limit is refused as oversized.
    ///
    /// On the run's walk a request that does not fit runs reclaim passes
    /// before it is refused: each pass spills walk-owned state, the
    /// requester's own last, and the request retries after it. It is refused
    /// only when a pass freed nothing with no release during it and a final
    /// pass then freed nothing too. Any other thread's request (a rayon
    /// worker's, a Source's, a writer's) is checked once and never spills.
    /// The walk blocks only on the ledger lock and on the spills it runs
    /// itself; it never waits on another thread.
    pub fn reserve(&self, bytes: u64, requester: Requester) -> Result<Grant, Shortfall> {
        let first = match self.reserve_now(bytes, requester) {
            Ok(grant) => return Ok(grant),
            Err(shortfall) => shortfall,
        };
        if walk::thread_role(self) != ThreadRole::Walk {
            return Err(first);
        }
        self.reclaim_until_granted(bytes, requester, first, || {
            self.reserve_now(bytes, requester)
        })
    }

    /// Charge `bytes` for `requester` only if they fit now, for an optional
    /// over-allocation the caller can do without (a growing buffer's spare
    /// capacity), or refuse at once.
    ///
    /// On any thread, the walk included, a refusal runs no reclaim pass,
    /// never waits, takes no place in any queue of waiting requests and is
    /// recorded nowhere: the ledger is exactly as it was. The caller falls
    /// back to the size it needs through [`Self::reserve`].
    pub fn reserve_if_free(&self, bytes: u64, requester: Requester) -> Result<Grant, Shortfall> {
        self.reserve_now(bytes, requester)
    }

    /// Make room for `projected` bytes that are not yet a consumer charge,
    /// for a hard-limit backstop that would otherwise abort, and charge
    /// nothing.
    ///
    /// `Ok` at once when they fit beside the ledger's charges. On the run's
    /// walk a projection that does not fit runs the same reclaim loop as
    /// [`Self::reserve`] with `projected` as the request: passes spill
    /// walk-owned state, the requester's own last, and the check retries
    /// after each, refusing only when a pass freed nothing with no release
    /// during it and a final pass then freed nothing too. Any other thread
    /// gets the shortfall at once and never spills; an off-walk requester
    /// that must grow takes a grant it can wait for instead.
    pub fn reclaim_before_abort(
        &self,
        requester: Requester,
        projected: u64,
    ) -> Result<(), Shortfall> {
        let first = match self.projection_fits(projected, requester) {
            Ok(()) => return Ok(()),
            Err(shortfall) => shortfall,
        };
        if walk::thread_role(self) != ThreadRole::Walk {
            return Err(first);
        }
        self.reclaim_until_granted(projected, requester, first, || {
            self.projection_fits(projected, requester)
        })
    }

    /// Shed up to `target_bytes` of reclaimable state now, in one reclaim
    /// round, and return the bytes the round's victims released themselves.
    /// A zero target runs nothing.
    ///
    /// On the run's walk the round is a reclaim pass aimed at the target: it
    /// spills walk-owned victims in the policy's order, ranked by what their
    /// spill frees, until their own charge decreases cover it. With no walk
    /// frame for this run on the calling thread the round is
    /// [`Self::frameless_pass`], which only raises spill requests and frees
    /// nothing itself. Best effort either way: a partial or zero result is
    /// not an error, and a spill failure on the walk is kept for the walk's
    /// next dispatch boundary.
    pub fn spill_reclaimable(&self, target_bytes: u64) -> u64 {
        if target_bytes == 0 {
            return 0;
        }
        let requester = Requester::governed();
        if walk::thread_role(self) != ThreadRole::Walk {
            return self.frameless_pass(target_bytes, requester);
        }
        self.pass_on_walk(|reclaim| {
            self.run_pass(
                PassAim::Shed(target_bytes),
                requester,
                reclaim,
                PassKind::Ordinary,
            )
        })
        .map_or(0, |pass| pass.freed)
    }

    /// A reclaim round started where no walk frame for this run is
    /// installed, so nothing can spill walk-owned state synchronously.
    ///
    /// It ranks candidates exactly as a reclaim pass does (never a
    /// back-pressureable consumer, never one whose reclaimable bytes are 0,
    /// the requester last) and raises the cooperative spill request on each
    /// in that order, through the consumer's `try_spill`, until the chosen
    /// candidates' reclaimable bytes cover `target`; the figure `try_spill`
    /// returns is ignored. It returns the chosen candidates' own charge
    /// decreases across their calls, which are 0 until their owners service
    /// the requests at their next safe point. It never spills, never waits
    /// on anyone and never fails. Only tests call the entry points without a
    /// frame: production runs every round on the walk.
    fn frameless_pass(&self, target: u64, requester: Requester) -> u64 {
        self.reclaim_rounds.fetch_add(1, Ordering::Relaxed);
        let registered = self.consumers.load();
        let mut covered = 0u64;
        let mut freed = 0u64;
        for id in self.pass_candidates(requester.consumer, PassKind::Ordinary) {
            if covered >= target {
                break;
            }
            let Some((_, consumer)) = registered.iter().find(|(candidate, _)| *candidate == id)
            else {
                continue;
            };
            covered = covered.saturating_add(consumer.reclaimable_bytes());
            let before = self.admission.ledger.lock().consumer_charged(id.0);
            let _ = consumer.try_spill(target);
            let after = self.admission.ledger.lock().consumer_charged(id.0);
            freed = freed.saturating_add(before.saturating_sub(after));
        }
        freed
    }

    /// Whether `projected` more bytes fit now, checked under the ledger lock
    /// as a charge would be, without charging them.
    fn projection_fits(&self, projected: u64, requester: Requester) -> Result<(), Shortfall> {
        let ledger = self.admission.ledger.lock();
        let refusal = if ledger.closed {
            Refusal::Closed
        } else if projected <= ledger.available() {
            return Ok(());
        } else {
            Refusal::Short {
                available: ledger.available(),
                oversized: projected > ledger.limit(),
            }
        };
        Err(shortfall(&ledger, projected, requester.consumer, refusal))
    }

    /// One locked check-and-charge, with no reclaim.
    fn reserve_now(&self, bytes: u64, requester: Requester) -> Result<Grant, Shortfall> {
        let mut ledger = self.admission.ledger.lock();
        if let Err(refusal) = ledger.try_charge(bytes, requester.consumer.map(|id| id.0)) {
            return Err(shortfall(&ledger, bytes, requester.consumer, refusal));
        }
        drop(ledger);
        Ok(Grant {
            state: Arc::clone(&self.admission),
            bytes,
            attribution: requester.consumer,
        })
    }

    /// Bytes charged to the ledger now.
    pub fn charged_bytes(&self) -> u64 {
        self.admission.ledger.lock().charged()
    }

    /// Highest [`Self::charged_bytes`] the ledger has held this run.
    pub fn peak_charged_bytes(&self) -> u64 {
        self.admission.ledger.lock().peak_charged()
    }

    /// Read the ledger's charges under its lock, as a shortfall for a request
    /// of `requested` bytes by `requester` would report them.
    pub fn ledger_snapshot(&self, requested: u64, requester: Requester) -> LedgerSnapshot {
        snapshot(&self.admission.ledger.lock(), requested, requester.consumer)
    }

    /// The E310 report for a refusal decided outside the ledger: `node`
    /// needed `requested` more bytes for `surface` and the site that checked
    /// is refusing them.
    ///
    /// The figures are one ledger reading taken now, as a shortfall's would
    /// be; `requested` is the bytes the site was about to add (for a
    /// backstop that fires after the fact, what it found over the limit).
    /// No reclaim round preceded the refusal, so the report says none was
    /// attempted. The request is oversized when it is larger than the limit
    /// on its own, or than what the limit leaves beside what cannot spill.
    pub fn refusal_report(
        &self,
        node: &str,
        surface: MemorySurface,
        requested: u64,
    ) -> Box<MemoryShortfallReport> {
        let snapshot = self.ledger_snapshot(requested, Requester::governed());
        self.off_ledger_report(snapshot, node, surface)
    }

    /// The report for a refusal decided outside the ledger, from `snapshot`,
    /// naming `node` and `surface` as the requester: no reclaim round, and
    /// oversized when the snapshot's request alone is larger than the limit.
    fn off_ledger_report(
        &self,
        snapshot: LedgerSnapshot,
        node: &str,
        surface: MemorySurface,
    ) -> Box<MemoryShortfallReport> {
        let oversized = snapshot.requested > snapshot.limit.bytes();
        build_report(
            self,
            snapshot,
            Some(ConsumerLabel {
                node: node.to_string(),
                surface,
            }),
            None,
            oversized,
        )
    }

    /// [`Self::refusal_report`] as the run-ending E310.
    pub fn refusal(&self, node: &str, surface: MemorySurface, requested: u64) -> PipelineError {
        PipelineError::MemoryBudgetExceeded {
            report: self.refusal_report(node, surface, requested),
        }
    }

    /// The E310 report for a backstop that found the run already past its
    /// limit ([`Self::should_abort`] true) while `node` held `surface`.
    ///
    /// The report says which reading was over the limit, from one ledger
    /// snapshot and the process's peak resident reading taken now. When the
    /// charged total is over, it is the ledger form and its request is how
    /// far over the charged total stands. When only the process's memory is
    /// over, it is the process-memory form: the request is how far over the
    /// peak stands, the suggested limit is the peak rounded up (a limit that
    /// peak would not have passed), and nothing is oversized, because no one
    /// request was measured. A charged total that has fallen back under the
    /// limit since the backstop checked, with no process reading over it,
    /// reports the ledger form with a request of 0.
    pub fn backstop_report(
        &self,
        node: &str,
        surface: MemorySurface,
    ) -> Box<MemoryShortfallReport> {
        let limit = self.hard_limit();
        let mut snapshot = self.ledger_snapshot(0, Requester::governed());
        let charged_over_by = snapshot.charged.saturating_sub(limit);
        match self.peak_rss().filter(|peak| *peak > limit) {
            Some(peak) if charged_over_by == 0 => {
                snapshot.requested = peak - limit;
                let mut report = self.off_ledger_report(snapshot, node, surface);
                report.reading = LimitReading::ProcessMemory {
                    peak_resident_bytes: peak,
                };
                report.suggested_limit_bytes = suggested_limit_floor(peak, 0);
                report.oversized = false;
                report
            }
            _ => {
                snapshot.requested = charged_over_by;
                self.off_ledger_report(snapshot, node, surface)
            }
        }
    }

    /// [`Self::backstop_report`] as the run-ending E310.
    pub fn backstop_refusal(&self, node: &str, surface: MemorySurface) -> PipelineError {
        PipelineError::MemoryBudgetExceeded {
            report: self.backstop_report(node, surface),
        }
    }

    /// High-water mark of `id`'s handle bytes plus the bytes granted in its
    /// name, raised by every charge to it and never lowered by a release.
    /// `None` when the ledger holds no entry for `id`. A charge in the name of
    /// a consumer that has already unregistered recreates an unlabelled entry
    /// for it, and this then returns that entry's partial mark rather than
    /// `None`; only tests read the figure today.
    pub fn consumer_peak_charged_bytes(&self, id: ConsumerId) -> Option<u64> {
        self.admission.ledger.lock().consumer_mark(id.0)
    }

    /// Number of nonzero releases the ledger has seen: every grant shrink or
    /// drop, handle shrink, and unregistration of a consumer still holding
    /// bytes advances it. An unchanged value across a span proves nothing
    /// was released in it.
    pub fn release_epoch(&self) -> u64 {
        self.admission.ledger.lock().release_epoch()
    }

    /// Reclaim passes this run has run, of every kind.
    pub fn reclaim_rounds(&self) -> u64 {
        self.reclaim_rounds.load(Ordering::Relaxed)
    }

    /// Name `requester` as the consumer governed allocations made on the walk
    /// are charged to from here on (`None`: charged to no consumer), and
    /// return the one it replaces. Called on the walk only.
    ///
    /// Each such allocation is a grant in that consumer's name: it raises the
    /// consumer's charged figure and mark, and it releases against that same
    /// consumer whatever the walk requester is when it drops. A reclaim pass
    /// the allocation starts elects that consumer last.
    pub(crate) fn set_walk_requester(&self, requester: Option<ConsumerId>) -> Option<ConsumerId> {
        let raw = requester.map_or(NO_WALK_REQUESTER, |id| u64::from(id.0));
        match self.walk_requester.swap(raw, Ordering::Relaxed) {
            NO_WALK_REQUESTER => None,
            previous => Some(ConsumerId(previous as u32)),
        }
    }

    /// The consumer a governed allocation made by the calling thread is
    /// charged to: the walk requester on this run's walk, and no consumer on
    /// any other thread.
    pub(crate) fn walk_requester(&self) -> Option<ConsumerId> {
        if walk::thread_role(self) != ThreadRole::Walk {
            return None;
        }
        match self.walk_requester.load(Ordering::Relaxed) {
            NO_WALK_REQUESTER => None,
            raw => Some(ConsumerId(raw as u32)),
        }
    }

    /// Whether every thread other than the walk is passive (parked, paused,
    /// or blocked on the walk), so that a refusal after a pass that freed
    /// nothing is final. No thread other than the walk ever parks on the
    /// ledger or registers its activity yet, so the walk is always alone in
    /// deciding and this is true.
    pub(crate) fn off_walk_quiescent(&self) -> bool {
        true
    }

    /// Take the first spill failure a reclaim pass met, if any. The walk
    /// calls it at its dispatch boundaries and fails the run with it.
    pub(crate) fn take_reclaim_failure(&self) -> Option<PipelineError> {
        self.reclaim_failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    /// Keep `error` for [`Self::take_reclaim_failure`] unless an earlier
    /// failure is already kept: the first is the cause.
    fn record_reclaim_failure(&self, error: PipelineError) {
        let mut kept = self
            .reclaim_failure
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if kept.is_none() {
            *kept = Some(error);
        }
    }

    /// The walk's side of a request that fell short with `shortfall`: run
    /// reclaim passes, retrying `attempt` after each, until it is granted or
    /// the per-pass failure rule refuses it. Called on the walk only.
    ///
    /// - A closed ledger or an oversized request is refused at once: no pass
    ///   can make it fit.
    /// - A forced refusal (a test's armed shortfall) runs one forced pass,
    ///   which elects only the requester, then retries whatever it freed;
    ///   it never leads to a refusal.
    /// - Otherwise an ordinary pass runs. The request retries after it; if
    ///   it still does not fit and the pass freed bytes or a release
    ///   happened during it, the walk loops. If not, and every other thread
    ///   is passive, a final pass runs; the request is refused only when
    ///   that too freed nothing with no release during it, with the
    ///   snapshot of the retry that followed it.
    ///
    /// Progress and releases are judged per pass, never since the request
    /// was first made, so a pass that freed something followed by one that
    /// freed nothing ends in a decision rather than another pass. A spill
    /// failure inside a pass is kept for the walk's next dispatch boundary
    /// and the request is refused with its last shortfall.
    pub(crate) fn reclaim_until_granted<T>(
        &self,
        need: u64,
        requester: Requester,
        mut shortfall: Shortfall,
        mut attempt: impl FnMut() -> Result<T, Shortfall>,
    ) -> Result<T, Shortfall> {
        // Every pass this request runs, kept for the report of its refusal.
        let mut round: Option<RoundRecord> = None;
        loop {
            if shortfall.is_closed() || shortfall.oversized {
                return Err(shortfall.after_round(round));
            }
            if shortfall.forced() {
                let pass = self.pass_on_walk(|reclaim| {
                    self.reclaim_pass(need, requester, reclaim, PassKind::Forced)
                });
                let record = round.get_or_insert_with(RoundRecord::default);
                let Some(pass) = pass else {
                    return Err(shortfall.after_round(round));
                };
                record.absorb(pass);
                match attempt() {
                    Ok(granted) => return Ok(granted),
                    Err(next) => {
                        shortfall = next;
                        continue;
                    }
                }
            }
            let pass = self.pass_on_walk(|reclaim| {
                self.reclaim_pass(need, requester, reclaim, PassKind::Ordinary)
            });
            let record = round.get_or_insert_with(RoundRecord::default);
            let Some(pass) = pass else {
                return Err(shortfall.after_round(round));
            };
            let pass_earns_a_retry = pass.earns_a_retry();
            record.absorb(pass);
            match attempt() {
                Ok(granted) => return Ok(granted),
                Err(next) => shortfall = next,
            }
            if shortfall.forced() || pass_earns_a_retry || !self.off_walk_quiescent() {
                continue;
            }
            let last = self.pass_on_walk(|reclaim| {
                self.reclaim_pass(need, requester, reclaim, PassKind::Final)
            });
            let Some(last) = last else {
                return Err(shortfall.after_round(round));
            };
            let last_earns_a_retry = last.earns_a_retry();
            if let Some(record) = round.as_mut() {
                record.absorb(last);
            }
            match attempt() {
                Ok(granted) => return Ok(granted),
                Err(next) => shortfall = next,
            }
            if shortfall.forced() || last_earns_a_retry {
                continue;
            }
            return Err(shortfall.after_round(round));
        }
    }

    /// Run `pass` over the walk's reclaim set, or over the busy stand-in when
    /// the set is already borrowed. `None` when the pass met a spill
    /// failure, which is kept for the walk.
    fn pass_on_walk(
        &self,
        pass: impl FnOnce(&mut dyn WalkReclaim) -> Result<PassOutcome, PipelineError>,
    ) -> Option<PassOutcome> {
        #[cfg(test)]
        if let Some(stand_in) = walk::test_reclaim() {
            let outcome = match stand_in.try_borrow_mut() {
                Ok(mut stand_in) => pass(&mut *stand_in),
                Err(_) => pass(&mut BorrowedReclaimSet),
            };
            return self.kept_on_failure(outcome);
        }
        let set = walk::walk_reclaim_set(self);
        let outcome = match set.as_ref().map(|set| set.try_borrow_mut()) {
            Some(Ok(mut set)) => pass(&mut *set),
            Some(Err(_)) | None => pass(&mut BorrowedReclaimSet),
        };
        self.kept_on_failure(outcome)
    }

    /// The pass's outcome, or `None` after keeping its spill failure for the
    /// walk's next dispatch boundary.
    fn kept_on_failure(&self, outcome: Result<PassOutcome, PipelineError>) -> Option<PassOutcome> {
        match outcome {
            Ok(outcome) => Some(outcome),
            Err(error) => {
                self.record_reclaim_failure(error);
                None
            }
        }
    }

    /// Run one reclaim pass for a request of `need` bytes by `requester`,
    /// spilling through `reclaim`.
    ///
    /// Runs on the walk only. Holds no lock while a victim spills and never
    /// waits on another thread; the only blocking is the spills' own I/O.
    /// It measures only its own victims: each victim's progress is what the
    /// walk released while that victim spilled, less what it charged then,
    /// recorded by the ledger under its lock, so a release on another thread
    /// is never counted as progress and only marks the pass as having seen a
    /// release.
    ///
    /// It aims to free the larger of the shortfall and what brings the
    /// ledger down to the resume watermark with the request charged,
    /// reclaiming on demand only. Candidates are the registered consumers
    /// that cannot be paused and that a spill would free bytes from now
    /// ([`super::MemoryConsumer::reclaimable_bytes`] above 0), ordered by
    /// the run's policy, which ranks them by those bytes, with ties to the
    /// older (lower) id, the requester after all of them; a consumer whose
    /// charge no spill can free is never a candidate, however much it holds. A
    /// forced pass's only candidate is the requester. A victim the walk does
    /// not own is skipped and never asked to act; one it owns but cannot
    /// spill now is `Busy` and frees nothing.
    pub(crate) fn reclaim_pass(
        &self,
        need: u64,
        requester: Requester,
        reclaim: &mut dyn WalkReclaim,
        kind: PassKind,
    ) -> Result<PassOutcome, PipelineError> {
        self.run_pass(PassAim::Request(need), requester, reclaim, kind)
    }

    /// [`Self::reclaim_pass`] with what it aims to free given by `aim`.
    fn run_pass(
        &self,
        aim: PassAim,
        requester: Requester,
        reclaim: &mut dyn WalkReclaim,
        kind: PassKind,
    ) -> Result<PassOutcome, PipelineError> {
        self.reclaim_rounds.fetch_add(1, Ordering::Relaxed);
        let target = {
            let mut ledger = self.admission.ledger.lock();
            if let Some(walk) = super::sync::current_thread() {
                ledger.begin_pass(walk);
            }
            match aim {
                PassAim::Request(need) => {
                    let short = need.saturating_sub(ledger.available());
                    let to_watermark = ledger
                        .charged()
                        .saturating_add(need)
                        .saturating_sub(self.resume_limit());
                    short.max(to_watermark)
                }
                PassAim::Shed(target) => target,
            }
        };
        let mut pass = OpenPass {
            arbitrator: self,
            outcome: PassOutcome::default(),
            ended: false,
        };
        for id in self.pass_candidates(requester.consumer, kind) {
            if kind != PassKind::Forced && pass.outcome.freed >= target {
                break;
            }
            let node = {
                let mut ledger = self.admission.ledger.lock();
                ledger.open_victim();
                ledger.label(id.0).map(|label| label.node.clone())
            };
            let spilled = reclaim.spill_victim(id, self);
            let freed = self.admission.ledger.lock().close_victim();
            pass.outcome.freed = pass.outcome.freed.saturating_add(freed);
            let spilled = spilled?;
            if spilled == VictimOutcome::Spilled {
                pass.outcome.victims_spilled += 1;
            }
            if spilled == VictimOutcome::NotOwned {
                pass.outcome.not_owned.push(id);
            } else {
                pass.outcome.asked.push(AskedVictim {
                    consumer: id,
                    node,
                    busy: spilled == VictimOutcome::Busy,
                });
            }
        }
        Ok(pass.end())
    }

    /// The consumers a pass of `kind` asks to spill, in order: every
    /// registered consumer that cannot back-pressure and has reclaimable
    /// bytes, in the policy's order, and the requester last when it is one.
    ///
    /// Reads each consumer's figures once, here, and orders them with what
    /// was read ([`super::ArbitrationPolicy::order_candidates`]): a figure
    /// can sit behind a lock, so the cost is one read per consumer and one
    /// sort, not a read per consumer for every pick. The figures are taken
    /// to hold for the pass.
    fn pass_candidates(&self, requester: Option<ConsumerId>, kind: PassKind) -> Vec<ConsumerId> {
        if kind == PassKind::Forced {
            return requester.into_iter().collect();
        }
        let registered = self.consumers.load();
        let mut others: Vec<(ReclaimCandidate, &dyn MemoryConsumer)> =
            Vec::with_capacity(registered.len());
        let mut requester_can_spill = false;
        for (id, consumer) in registered.iter() {
            let can_back_pressure = consumer.can_back_pressure();
            if can_back_pressure {
                continue;
            }
            let reclaimable_bytes = consumer.reclaimable_bytes();
            if reclaimable_bytes == 0 {
                continue;
            }
            if Some(*id) == requester {
                requester_can_spill = true;
                continue;
            }
            let candidate = ReclaimCandidate {
                id: *id,
                reclaimable_bytes,
                spill_priority: consumer.spill_priority(),
                can_back_pressure,
            };
            others.push((candidate, consumer.as_ref()));
        }
        others.sort_by_key(|(candidate, _)| candidate.id.0);
        let (candidates, consumers): (
            Vec<ReclaimCandidate>,
            Vec<(ConsumerId, &dyn MemoryConsumer)>,
        ) = others
            .into_iter()
            .map(|(candidate, consumer)| (candidate, (candidate.id, consumer)))
            .unzip();
        let mut order = self.policy.order_candidates(&candidates, &consumers);
        if requester_can_spill && let Some(requester) = requester {
            order.push(requester);
        }
        order
    }
}

/// A pass in flight: ends the ledger's tracking of it exactly once, on
/// return or on unwind, so a failed pass never leaves the ledger counting
/// releases for it.
struct OpenPass<'a> {
    arbitrator: &'a MemoryArbitrator,
    outcome: PassOutcome,
    ended: bool,
}

impl OpenPass<'_> {
    fn end(mut self) -> PassOutcome {
        self.outcome.released_during = self.arbitrator.admission.ledger.lock().end_pass();
        self.ended = true;
        std::mem::take(&mut self.outcome)
    }
}

impl Drop for OpenPass<'_> {
    fn drop(&mut self) {
        if !self.ended {
            let mut ledger = self.arbitrator.admission.ledger.lock();
            ledger.close_victim();
            ledger.end_pass();
        }
    }
}

/// A [`crate::executor::ForcedShortfall`] armed on the ledger: the matching
/// charges it has counted, the count its next firing is due at and the
/// firings it has left. Held in the ledger's attachment, so counting and
/// firing are one step with the admission they decide.
#[cfg(any(test, feature = "test-utils"))]
pub(super) struct ArmedShortfall {
    shortfall: crate::executor::ForcedShortfall,
    seen: u32,
    due: u32,
    left: u32,
}

#[cfg(any(test, feature = "test-utils"))]
impl ArmedShortfall {
    fn new(shortfall: crate::executor::ForcedShortfall) -> Self {
        Self {
            due: shortfall.nth(),
            left: shortfall.firings(),
            seen: 0,
            shortfall,
        }
    }

    /// Count a charge by the consumer registered under `label`, holding
    /// `resident` bytes, and say whether it fires. Only a charge the matcher
    /// accepts counts. A firing that is due waits for a charge whose
    /// requester holds bytes, so the refusal always leaves it something to
    /// free; each firing is recorded on the shared counter and sets the next
    /// one [`crate::executor::ForcedShortfall::every`] counts later.
    ///
    /// The matcher runs under the ledger lock, so it must be a pure
    /// predicate on the label.
    pub(super) fn fires(&mut self, label: &ConsumerLabel, resident: u64) -> bool {
        if !self.shortfall.accepts(label) {
            return false;
        }
        self.seen = self.seen.saturating_add(1);
        if self.seen < self.due || resident == 0 {
            return false;
        }
        self.left -= 1;
        self.due = self.seen.saturating_add(self.shortfall.spacing());
        self.shortfall.record_firing();
        true
    }

    /// Whether every firing has happened, so the arm can be dropped.
    pub(super) fn spent(&self) -> bool {
        self.left == 0
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl MemoryArbitrator {
    /// Make the `nth` (from 1) charge whose requester's label `matcher`
    /// accepts fall short once, as if nothing were available, or the first
    /// matching charge after it at which the requester holds resident bytes.
    /// Every other charge, including the caller's retry, takes the real
    /// path. Arming again replaces an arm that has not fired. The charge may
    /// be a [`Self::reserve`], a [`Grant::try_grow`] or a consumer handle's
    /// `try_grow` / `try_resize`.
    ///
    /// Arming an arbitrator the test builds itself is the supported form in
    /// this build. Armed for a whole run, the lever reaches only a Source's
    /// record allocations, because node state charges its handles unchecked,
    /// and nothing answers that refusal (see
    /// [`crate::executor::ForcedShortfall`]).
    ///
    /// For the two kinds of test [`crate::executor::ForcedShortfall`]
    /// permits: a test that spills a whole unit and then reloads it, where no
    /// ledger capacity both forces the spill and admits the reload; and a
    /// spill-path-equivalence test, run twice at the same ample limit (unarmed
    /// with no spill bytes written, armed with the arm fired and the named
    /// node's written spill bytes above 0). Each use records its reason in its
    /// test's doc comment. Proving that the arbitrator spills under real
    /// pressure stays with the two-direction pairs on a derived capacity.
    /// `matcher` runs under the ledger lock and must not call back into the
    /// arbitrator.
    ///
    /// # Panics
    ///
    /// When `nth` is 0.
    pub fn force_shortfall_once(
        &self,
        matcher: impl Fn(&ConsumerLabel) -> bool + Send + Sync + 'static,
        nth: u32,
    ) {
        self.arm_forced_shortfall(crate::executor::ForcedShortfall::at(matcher, nth));
    }

    /// Arm `shortfall` on the ledger, replacing any arm that has not fired
    /// its last time. Use it for an arm built with
    /// [`crate::executor::ForcedShortfall::times`] or
    /// [`crate::executor::ForcedShortfall::every`], or whose
    /// [`crate::executor::ForcedShortfall::fired`] counter the test reads.
    pub fn arm_forced_shortfall(&self, shortfall: crate::executor::ForcedShortfall) {
        self.admission.ledger.lock().attachment.forced_shortfall =
            Some(ArmedShortfall::new(shortfall));
    }
}

/// The report of the last governed allocation the ledger refused on this
/// thread, with the figures its admission error carried.
struct RecordedRefusal {
    requested: usize,
    available: usize,
    report: Box<MemoryShortfallReport>,
}

thread_local! {
    /// One per thread: a refusal overwrites it and the thread's next granted
    /// governed allocation clears it, so it never describes a refusal the
    /// thread recovered from, and never another thread's.
    static LAST_REFUSAL: RefCell<Option<RecordedRefusal>> = const { RefCell::new(None) };
}

/// Keep `report` as this thread's last refused governed allocation, which
/// failed with an admission error carrying `requested` and `available`.
pub(crate) fn record_governed_refusal(
    requested: usize,
    available: usize,
    report: Box<MemoryShortfallReport>,
) {
    LAST_REFUSAL.with(|slot| {
        *slot.borrow_mut() = Some(RecordedRefusal {
            requested,
            available,
            report,
        });
    });
}

/// Forget this thread's last refused governed allocation: one was granted
/// since, so any earlier refusal was recovered from.
pub(crate) fn clear_governed_refusal() {
    LAST_REFUSAL.with(|slot| {
        if slot.borrow().is_some() {
            *slot.borrow_mut() = None;
        }
    });
}

/// This thread's report for the governed refusal `error` describes, taken
/// out of the thread's slot: returned only when the slot's figures are
/// `error`'s own, so an admission error raised anywhere else (a different
/// request, another thread's refusal) never borrows it.
pub(crate) fn take_refusal_report_for(error: &ResourceError) -> Option<Box<MemoryShortfallReport>> {
    if error.kind != ResourceErrorKind::Budget {
        return None;
    }
    LAST_REFUSAL.with(|slot| {
        let mut slot = slot.borrow_mut();
        let matches = slot.as_ref().is_some_and(|recorded| {
            recorded.requested == error.requested && recorded.available == error.available
        });
        if matches {
            slot.take().map(|recorded| recorded.report)
        } else {
            None
        }
    })
}

/// `error` as the E310 it stands for when it is a governed allocation the
/// ledger refused on this thread; otherwise `error` unchanged.
///
/// A reader, writer or worker that met the refusal propagates it as an
/// admission error (`Format(Resource(Budget))`); this recovers the report
/// the refusal recorded. `requester` names the node and surface of the
/// thread's work, stamped onto a report whose refusal named none (a request
/// made for the run as a whole); a report that already names its requester
/// keeps it. Called on the thread that was refused, where its work returns.
pub(crate) fn governed_refusal_error(
    error: PipelineError,
    requester: Option<(&str, MemorySurface)>,
) -> PipelineError {
    let PipelineError::Format(FormatError::Resource(resource)) = &error else {
        return error;
    };
    let Some(mut report) = take_refusal_report_for(resource) else {
        return error;
    };
    if let Some((node, surface)) = requester {
        report.attribute_if_unnamed(node, surface);
    }
    PipelineError::MemoryBudgetExceeded { report }
}

/// [`governed_refusal_error`] over a thread's result, naming `node` and
/// `surface` as the requester of an unnamed refusal. Every thread wrapper
/// that runs a node's work off the walk calls it where the work returns.
pub(crate) fn convert_governed_refusal<T>(
    result: Result<T, PipelineError>,
    node: &str,
    surface: MemorySurface,
) -> Result<T, PipelineError> {
    result.map_err(|error| governed_refusal_error(error, Some((node, surface))))
}

/// Labelled holders placed directly on the ledger, beside governed grants,
/// without registering a consumer.
#[cfg(test)]
impl MemoryArbitrator {
    /// Charge `bytes` to `id`'s handle and record its label, unchecked.
    pub(crate) fn charge_labelled_handle(&self, id: ConsumerId, label: ConsumerLabel, bytes: u64) {
        self.admission.ledger.lock().bind_handle(id.0, label, bytes);
    }

    /// Release `bytes` of `id`'s handle charge.
    pub(crate) fn release_labelled_handle(&self, id: ConsumerId, bytes: u64) {
        let mut ledger = self.admission.ledger.lock();
        let held = ledger.handle_bytes(id.0);
        ledger.set_handle(id.0, held.saturating_sub(bytes));
    }

    /// Remove `id`'s entry as unregistration does, returning its mark.
    pub(crate) fn forget_consumer_entry(&self, id: ConsumerId) -> Option<u64> {
        self.admission.ledger.lock().remove_consumer(id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::{ConsumerHandle, ConsumerSpillError, MemoryConsumer, NoOpPolicy};
    use clinker_plan::runtime_error::MemorySurface;
    use std::sync::Barrier;

    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;

    fn arbitrator(limit: u64) -> MemoryArbitrator {
        MemoryArbitrator::with_policy(limit, 0.80, 0.70, Box::new(NoOpPolicy))
    }

    fn governed() -> Requester {
        Requester::governed()
    }

    fn label(node: &str, surface: MemorySurface) -> ConsumerLabel {
        ConsumerLabel {
            node: node.to_string(),
            surface,
        }
    }

    /// `id`'s figure in a fresh snapshot, zero when it is not listed.
    fn holder_bytes(arbitrator: &MemoryArbitrator, id: ConsumerId) -> u64 {
        arbitrator
            .ledger_snapshot(0, governed())
            .holders
            .iter()
            .find(|holder| holder.consumer == id)
            .map_or(0, |holder| holder.charged)
    }

    fn assert_holders_cover_charged(snapshot: &LedgerSnapshot) {
        let held: u64 = snapshot.holders.iter().map(|holder| holder.charged).sum();
        assert_eq!(
            held + snapshot.unattributed,
            snapshot.charged,
            "holders plus the unattributed remainder must equal the charged total: {snapshot:?}"
        );
    }

    /// Two threads released by one barrier each reserve `bytes`; returns how
    /// many were granted. Both grants stay alive until both requests are
    /// answered, so the second request always sees the first one's charge.
    fn race(arbitrator: &MemoryArbitrator, bytes: u64) -> usize {
        let barrier = Barrier::new(2);
        let results: Vec<Result<Grant, Shortfall>> = std::thread::scope(|scope| {
            let requesters: Vec<_> = (0..2)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        arbitrator.reserve(bytes, governed())
                    })
                })
                .collect();
            requesters
                .into_iter()
                .map(|requester| requester.join().expect("requester thread"))
                .collect()
        });
        let granted = results.iter().filter(|result| result.is_ok()).count();
        assert_eq!(
            arbitrator.charged_bytes(),
            granted as u64 * bytes,
            "the ledger charges exactly the granted requests"
        );
        granted
    }

    /// Every surface, built through an exhaustive match so a new variant
    /// cannot be left out of the vocabulary check.
    fn every_surface() -> Vec<MemorySurface> {
        let surfaces = vec![
            MemorySurface::RowsRead,
            MemorySurface::BufferedRows {
                from: "orders".to_string(),
                to: "totals".to_string(),
            },
            MemorySurface::GroupState,
            MemorySurface::SortBuffer,
            MemorySurface::JoinBuildSide,
            MemorySurface::JoinState,
            MemorySurface::HeldFailingRows,
            MemorySurface::OpenDocumentRows,
            MemorySurface::DeadLetteredRowSet,
            MemorySurface::DecisionState,
            MemorySurface::ReshapeGroups,
            MemorySurface::CullGroups,
            MemorySurface::WindowIndex,
            MemorySurface::ScanMaterialization,
            MemorySurface::OutputStaging,
            MemorySurface::CredentialRegistry,
            MemorySurface::CorrelationGroups,
            MemorySurface::ParkedCrossRegionRows {
                from: "intake".to_string(),
                to: "archive".to_string(),
            },
        ];
        for surface in &surfaces {
            match surface {
                MemorySurface::RowsRead
                | MemorySurface::BufferedRows { .. }
                | MemorySurface::GroupState
                | MemorySurface::SortBuffer
                | MemorySurface::JoinBuildSide
                | MemorySurface::JoinState
                | MemorySurface::HeldFailingRows
                | MemorySurface::OpenDocumentRows
                | MemorySurface::DeadLetteredRowSet
                | MemorySurface::DecisionState
                | MemorySurface::ReshapeGroups
                | MemorySurface::CullGroups
                | MemorySurface::WindowIndex
                | MemorySurface::ScanMaterialization
                | MemorySurface::OutputStaging
                | MemorySurface::CredentialRegistry
                | MemorySurface::CorrelationGroups
                | MemorySurface::ParkedCrossRegionRows { .. } => {}
            }
        }
        surfaces
    }

    /// `error` as the admission error a reader or writer propagates.
    fn admission_error(error: ResourceError) -> PipelineError {
        PipelineError::Format(FormatError::Resource(error))
    }

    /// The report `error` converts to on this thread, if it converts.
    fn converted(error: ResourceError) -> Option<Box<MemoryShortfallReport>> {
        match governed_refusal_error(admission_error(error), None) {
            PipelineError::MemoryBudgetExceeded { report } => Some(report),
            _ => None,
        }
    }

    #[test]
    fn recovered_then_fatal_refusal_reports_the_fatal_one() {
        let arbitrator = arbitrator(64 * KIB);
        // Refused, then recovered from: a smaller retry is granted.
        let recovered = arbitrator
            .admit_writer_memory((100 * KIB) as usize, governed())
            .expect_err("100 KiB does not fit a 64 KiB limit");
        arbitrator
            .admit_writer_memory(KIB as usize, governed())
            .expect("the smaller retry fits");
        // A later request of another size is refused and ends the work.
        let fatal = arbitrator
            .admit_writer_memory((200 * KIB) as usize, governed())
            .expect_err("200 KiB does not fit a 64 KiB limit");

        assert!(
            converted(recovered).is_none(),
            "a refusal the thread recovered from is never reported"
        );
        let report = converted(fatal).expect("the fatal refusal converts to its E310");
        assert_eq!(report.requested_bytes, 200 * KIB);
        assert_eq!(
            report.charged_bytes, KIB,
            "the report is the fatal refusal's reading"
        );
        assert!(
            converted(fatal).is_none(),
            "a report is taken once, by the wrapper the refusal ends"
        );
        arbitrator
            .admission
            .release_writer_memory(KIB as usize, None);
    }

    #[test]
    fn refusal_on_one_thread_is_not_reported_for_another() {
        let arbitrator = Arc::new(arbitrator(64 * KIB));
        let refused_here = arbitrator
            .admit_writer_memory((100 * KIB) as usize, governed())
            .expect_err("100 KiB does not fit a 64 KiB limit");
        // Another thread records a later refusal of its own, and cannot take
        // this thread's report.
        let other = Arc::clone(&arbitrator);
        let there = std::thread::spawn(move || {
            let refused_there = other
                .admit_writer_memory((80 * KIB) as usize, governed())
                .expect_err("80 KiB does not fit a 64 KiB limit");
            let stolen = converted(refused_here).is_some();
            let own = converted(refused_there).map(|report| report.requested_bytes);
            (stolen, own)
        })
        .join()
        .expect("the other thread finishes");
        assert_eq!(
            there,
            (false, Some(80 * KIB)),
            "the other thread reports only its own refusal"
        );

        let report =
            converted(refused_here).expect("this thread's refusal converts with its own report");
        assert_eq!(report.requested_bytes, 100 * KIB);
    }

    #[test]
    fn ledger_grants_exactly_to_the_limit() {
        let arbitrator = arbitrator(MIB);
        let full = arbitrator
            .reserve(MIB, governed())
            .expect("the whole limit is free");
        assert_eq!(full.bytes(), MIB);
        assert_eq!(arbitrator.charged_bytes(), MIB);
        let short = arbitrator
            .reserve(1, governed())
            .expect_err("one byte past a full ledger must fall short");
        assert_eq!(
            (short.requested, short.available, short.oversized),
            (1, 0, false)
        );
        assert_eq!(
            arbitrator.charged_bytes(),
            MIB,
            "a shortfall charges nothing"
        );
        drop(full);

        // With C charged, exactly L - C more fits and one byte more does not.
        let charged = 300 * KIB;
        let held = arbitrator.reserve(charged, governed()).expect("fits");
        let over = arbitrator
            .reserve(MIB - charged + 1, governed())
            .expect_err("one byte past the free capacity");
        assert_eq!(over.available, MIB - charged);
        assert!(
            !over.oversized,
            "a request within the limit is not oversized"
        );
        let rest = arbitrator
            .reserve(MIB - charged, governed())
            .expect("exactly the free capacity fits");
        assert_eq!(arbitrator.charged_bytes(), MIB);
        drop(rest);
        drop(held);
        assert_eq!(arbitrator.charged_bytes(), 0);
    }

    #[test]
    fn two_requesters_cannot_both_pass_the_limit() {
        let limit = MIB;
        let arbitrator = arbitrator(limit);
        for iteration in 0..1_000 {
            assert_eq!(
                race(&arbitrator, limit / 2 + 1),
                1,
                "iteration {iteration}: two requests one byte past the limit together \
                 must leave exactly one of them short"
            );
            assert_eq!(arbitrator.charged_bytes(), 0);
            assert_eq!(
                race(&arbitrator, limit / 2),
                2,
                "iteration {iteration}: two requests summing to the free capacity must both fit"
            );
            assert_eq!(arbitrator.charged_bytes(), 0);
        }
    }

    #[test]
    fn zero_byte_reserve_is_free() {
        let arbitrator = arbitrator(MIB);
        let full = arbitrator.reserve(MIB, governed()).expect("fits");
        let epoch = arbitrator.release_epoch();
        let empty = arbitrator
            .reserve(0, governed())
            .expect("a zero-byte request never falls short");
        assert_eq!(empty.bytes(), 0);
        assert_eq!(arbitrator.charged_bytes(), MIB);
        drop(empty);
        assert_eq!(arbitrator.charged_bytes(), MIB);
        assert_eq!(
            arbitrator.release_epoch(),
            epoch,
            "an empty grant releases nothing when it drops"
        );
        drop(full);
    }

    #[test]
    fn oversized_request_never_wraps() {
        let arbitrator = arbitrator(MIB);
        let held = arbitrator.reserve(KIB, governed()).expect("fits");
        for request in [u64::MAX, MIB + 1] {
            let short = arbitrator
                .reserve(request, governed())
                .expect_err("a request larger than the limit must fall short");
            assert!(short.oversized, "{request} bytes exceeds the whole limit");
            assert_eq!(short.requested, request);
            assert_eq!(short.available, MIB - KIB);
            assert_eq!(arbitrator.charged_bytes(), KIB, "nothing is charged");
        }
        let short = arbitrator
            .reserve(MIB, governed())
            .expect_err("the whole limit does not fit beside a held grant");
        assert!(
            !short.oversized,
            "a request within the limit is not oversized"
        );
        drop(held);
        assert_eq!(arbitrator.charged_bytes(), 0);
    }

    #[test]
    fn dropping_a_grant_releases_and_bumps_release_epoch() {
        let arbitrator = arbitrator(MIB);
        let held = arbitrator.reserve(KIB, governed()).expect("fits");
        let charged = arbitrator.charged_bytes();
        let epoch = arbitrator.release_epoch();
        let grant = arbitrator.reserve(4 * KIB, governed()).expect("fits");
        assert_eq!(arbitrator.charged_bytes(), charged + 4 * KIB);
        assert_eq!(
            arbitrator.release_epoch(),
            epoch,
            "a charge is not a release"
        );
        drop(grant);
        assert_eq!(arbitrator.charged_bytes(), charged);
        assert_eq!(arbitrator.release_epoch(), epoch + 1);
        assert_eq!(
            arbitrator.peak_charged_bytes(),
            5 * KIB,
            "a release never lowers the peak"
        );
        drop(held);
    }

    #[test]
    fn grant_try_grow_is_atomic() {
        let arbitrator = arbitrator(MIB);
        let mut grant = arbitrator.reserve(512 * KIB, governed()).expect("fits");
        let other = arbitrator.reserve(256 * KIB, governed()).expect("fits");
        let short = grant
            .try_grow(256 * KIB + 1)
            .expect_err("one byte past the free capacity");
        assert_eq!(
            (short.requested, short.available, short.oversized),
            (256 * KIB + 1, 256 * KIB, false)
        );
        assert_eq!(grant.bytes(), 512 * KIB, "a failed grow leaves the grant");
        assert_eq!(arbitrator.charged_bytes(), 768 * KIB, "and the ledger");
        let short = grant
            .try_grow(u64::MAX)
            .expect_err("a grow past the whole limit");
        assert!(short.oversized);
        assert_eq!(grant.bytes(), 512 * KIB);
        assert_eq!(arbitrator.charged_bytes(), 768 * KIB);

        grant
            .try_grow(256 * KIB)
            .expect("exactly the free capacity fits");
        assert_eq!(grant.bytes(), 768 * KIB);
        assert_eq!(arbitrator.charged_bytes(), MIB);

        let epoch = arbitrator.release_epoch();
        grant.shrink(512 * KIB);
        assert_eq!(grant.bytes(), 256 * KIB);
        assert_eq!(arbitrator.charged_bytes(), 512 * KIB);
        assert_eq!(
            arbitrator.release_epoch(),
            epoch + 1,
            "a shrink is a release"
        );
        drop(grant);
        drop(other);
        assert_eq!(arbitrator.charged_bytes(), 0);
    }

    #[test]
    fn shortfall_snapshot_is_labelled() {
        let arbitrator = arbitrator(64 * KIB);
        let sort = ConsumerId(1);
        let groups = ConsumerId(2);
        arbitrator.charge_labelled_handle(
            groups,
            label("totals", MemorySurface::GroupState),
            8 * KIB,
        );
        arbitrator.charge_labelled_handle(
            sort,
            label("by_region", MemorySurface::SortBuffer),
            24 * KIB,
        );
        let short = arbitrator
            .reserve(40 * KIB, Requester::for_consumer(groups))
            .expect_err("40 KiB does not fit beside 32 KiB under 64 KiB");
        let snapshot = &short.snapshot;
        assert_eq!(snapshot.limit.bytes(), 64 * KIB);
        assert_eq!(snapshot.charged, 32 * KIB);
        assert_eq!(snapshot.requested, 40 * KIB);
        assert_eq!(snapshot.requester, Some(groups));
        assert_eq!(
            snapshot.holders,
            vec![
                HolderSnapshot {
                    consumer: sort,
                    label: label("by_region", MemorySurface::SortBuffer),
                    charged: 24 * KIB,
                },
                HolderSnapshot {
                    consumer: groups,
                    label: label("totals", MemorySurface::GroupState),
                    charged: 8 * KIB,
                },
            ],
            "holders carry their labels, largest first"
        );
        assert_eq!(snapshot.unattributed, 0);
        assert_holders_cover_charged(snapshot);

        assert_eq!(MemorySurface::GroupState.to_string(), "group state");
        assert_eq!(
            MemorySurface::BufferedRows {
                from: "orders".to_string(),
                to: "totals".to_string(),
            }
            .to_string(),
            "rows buffered between \"orders\" and \"totals\""
        );
        for surface in every_surface() {
            let text = surface.to_string();
            for engine_word in ["arena", "node_buffer", "arbitrator", "consumer", "ledger"] {
                assert!(
                    !text.contains(engine_word),
                    "{surface:?} renders {text:?}, which names the engine's {engine_word:?}"
                );
            }
        }
    }

    #[test]
    fn governed_grant_is_attributed_to_its_requester() {
        let arbitrator = arbitrator(MIB);
        let reader = ConsumerId(7);
        assert_eq!(arbitrator.consumer_peak_charged_bytes(reader), None);
        arbitrator.charge_labelled_handle(reader, label("orders", MemorySurface::RowsRead), 0);

        let first = arbitrator
            .reserve(4 * KIB, Requester::for_consumer(reader))
            .expect("fits");
        assert_eq!(holder_bytes(&arbitrator, reader), 4 * KIB);
        assert_eq!(
            arbitrator.consumer_peak_charged_bytes(reader),
            Some(4 * KIB)
        );
        drop(first);
        assert_eq!(
            holder_bytes(&arbitrator, reader),
            0,
            "a drop returns the attribution"
        );
        assert_eq!(
            arbitrator.consumer_peak_charged_bytes(reader),
            Some(4 * KIB),
            "a release never lowers the mark"
        );

        let mut second = arbitrator
            .reserve(2 * KIB, Requester::for_consumer(reader))
            .expect("fits");
        assert_eq!(holder_bytes(&arbitrator, reader), 2 * KIB);
        assert_eq!(
            arbitrator.consumer_peak_charged_bytes(reader),
            Some(4 * KIB)
        );
        second.try_grow(4 * KIB).expect("fits");
        assert_eq!(holder_bytes(&arbitrator, reader), 6 * KIB);
        assert_eq!(
            arbitrator.consumer_peak_charged_bytes(reader),
            Some(6 * KIB),
            "a grow keeps its grant's attribution and raises the mark"
        );
        second.shrink(KIB);
        assert_eq!(holder_bytes(&arbitrator, reader), 5 * KIB);
        drop(second);
        assert_eq!(holder_bytes(&arbitrator, reader), 0);
        assert_eq!(
            arbitrator.consumer_peak_charged_bytes(reader),
            Some(6 * KIB)
        );
        assert_eq!(arbitrator.charged_bytes(), 0);
    }

    #[test]
    fn unattributed_grant_raises_no_consumer_mark() {
        let arbitrator = arbitrator(MIB);
        let sort = ConsumerId(3);
        arbitrator.charge_labelled_handle(sort, label("by_region", MemorySurface::SortBuffer), KIB);
        let run = arbitrator.reserve(8 * KIB, governed()).expect("fits");
        assert_eq!(arbitrator.charged_bytes(), 9 * KIB);
        assert_eq!(arbitrator.consumer_peak_charged_bytes(sort), Some(KIB));
        let snapshot = arbitrator.ledger_snapshot(0, governed());
        assert_eq!(snapshot.unattributed, 8 * KIB);
        assert_holders_cover_charged(&snapshot);

        // A governed release through the lease path names no consumer, so it
        // lowers only the run total.
        arbitrator
            .admit_writer_memory(2 * KIB as usize, governed())
            .expect("the admission path charges through the same ledger");
        assert_eq!(arbitrator.charged_bytes(), 11 * KIB);
        arbitrator
            .writer_reservation_state()
            .release_writer_memory(2 * KIB as usize, None);
        assert_eq!(arbitrator.charged_bytes(), 9 * KIB);
        assert_eq!(holder_bytes(&arbitrator, sort), KIB);
        assert_eq!(arbitrator.consumer_peak_charged_bytes(sort), Some(KIB));

        drop(run);
        assert_eq!(arbitrator.charged_bytes(), KIB);
        assert_eq!(arbitrator.consumer_peak_charged_bytes(sort), Some(KIB));
        assert_eq!(
            arbitrator.consumer_peak_charged_bytes(ConsumerId(99)),
            None,
            "a governed grant creates no consumer entry"
        );
    }

    #[test]
    fn consumer_mark_covers_handle_and_attributed_bytes() {
        let arbitrator = arbitrator(MIB);
        let both = ConsumerId(1);
        arbitrator.charge_labelled_handle(both, label("orders", MemorySurface::RowsRead), 3 * KIB);
        let grant = arbitrator
            .reserve(5 * KIB, Requester::for_consumer(both))
            .expect("fits");
        assert_eq!(
            arbitrator.consumer_peak_charged_bytes(both),
            Some(8 * KIB),
            "handle and attributed bytes held at once add up in the mark"
        );
        arbitrator.release_labelled_handle(both, 3 * KIB);
        drop(grant);
        assert_eq!(arbitrator.consumer_peak_charged_bytes(both), Some(8 * KIB));

        // Held one after the other, the two charges never add up.
        let apart = ConsumerId(2);
        arbitrator.charge_labelled_handle(
            apart,
            label("totals", MemorySurface::GroupState),
            3 * KIB,
        );
        arbitrator.release_labelled_handle(apart, 3 * KIB);
        let grant = arbitrator
            .reserve(5 * KIB, Requester::for_consumer(apart))
            .expect("fits");
        assert_eq!(
            arbitrator.consumer_peak_charged_bytes(apart),
            Some(5 * KIB),
            "the mark is the largest sum at one instant, not a sum of separate peaks"
        );
        drop(grant);

        let fresh = ConsumerId(3);
        let grant = arbitrator
            .reserve(5 * KIB, Requester::for_consumer(fresh))
            .expect("fits");
        assert_eq!(arbitrator.consumer_peak_charged_bytes(fresh), Some(5 * KIB));
        drop(grant);
        assert_eq!(arbitrator.consumer_peak_charged_bytes(both), Some(8 * KIB));
        assert_eq!(arbitrator.charged_bytes(), 0);
    }

    #[test]
    fn holders_and_remainder_sum_to_the_charged_total() {
        let arbitrator = arbitrator(64 * KIB);
        let a = ConsumerId(1);
        let b = ConsumerId(2);
        arbitrator.charge_labelled_handle(a, label("orders", MemorySurface::RowsRead), 3 * KIB);
        arbitrator.charge_labelled_handle(b, label("totals", MemorySurface::GroupState), 0);
        let a_grant = arbitrator
            .reserve(5 * KIB, Requester::for_consumer(a))
            .expect("fits");
        let b_grant = arbitrator
            .reserve(2 * KIB, Requester::for_consumer(b))
            .expect("fits");
        let run = arbitrator.reserve(4 * KIB, governed()).expect("fits");

        let short = arbitrator
            .reserve(64 * KIB, governed())
            .expect_err("the whole limit does not fit beside 14 KiB");
        let snapshot = &short.snapshot;
        assert_eq!(
            snapshot
                .holders
                .iter()
                .map(|holder| (holder.consumer, holder.charged))
                .collect::<Vec<_>>(),
            vec![(a, 8 * KIB), (b, 2 * KIB)],
            "a holder's figure is its handle bytes plus its attributed bytes"
        );
        assert_eq!(snapshot.unattributed, 4 * KIB);
        assert_eq!(snapshot.charged, 14 * KIB);
        assert_holders_cover_charged(snapshot);

        drop(a_grant);
        let snapshot = arbitrator.ledger_snapshot(0, governed());
        assert_eq!(
            snapshot
                .holders
                .iter()
                .map(|holder| (holder.consumer, holder.charged))
                .collect::<Vec<_>>(),
            vec![(a, 3 * KIB), (b, 2 * KIB)],
            "a holder's figure is current, not its mark"
        );
        assert_eq!(snapshot.charged, 9 * KIB);
        assert_holders_cover_charged(&snapshot);
        drop(b_grant);
        drop(run);
    }

    /// A consumer whose charge is its handle's, as every production consumer's
    /// is.
    struct HandleConsumer(Arc<ConsumerHandle>);

    impl MemoryConsumer for HandleConsumer {
        fn current_usage(&self) -> u64 {
            self.0.bytes()
        }
        fn peak_charged_bytes(&self) -> Option<u64> {
            Some(self.0.peak_bytes())
        }
        fn spill_priority(&self) -> i32 {
            0
        }
        fn try_spill(&self, _: u64) -> Result<u64, ConsumerSpillError> {
            Ok(0)
        }
        fn can_back_pressure(&self) -> bool {
            false
        }
    }

    /// Register a node consumer charging through a fresh handle.
    fn register_node(
        arbitrator: &MemoryArbitrator,
        node: &str,
    ) -> (Arc<ConsumerHandle>, ConsumerId) {
        let handle = ConsumerHandle::new();
        let id = arbitrator.register_node_consumer(
            Arc::new(HandleConsumer(Arc::clone(&handle))),
            Arc::clone(&handle),
            label(node, MemorySurface::GroupState),
        );
        (handle, id)
    }

    #[test]
    fn a_backstop_tripped_by_process_memory_reports_the_process_reading() {
        let limit = 64 * MIB;
        let arbitrator = arbitrator(limit);
        let (handle, id) = register_node(&arbitrator, "enrich");
        handle.try_grow(MIB).expect("1 MiB fits a 64 MiB limit");
        // The process stood 32 MiB over the limit while only 1 MiB was
        // charged: the backstop fired on the process's memory.
        let peak = 96 * MIB;
        arbitrator.set_peak_rss_for_test(peak);

        let PipelineError::MemoryBudgetExceeded { report } =
            arbitrator.backstop_refusal("enrich", MemorySurface::JoinBuildSide)
        else {
            panic!("a backstop refusal is an E310");
        };
        assert_eq!(
            report.reading,
            LimitReading::ProcessMemory {
                peak_resident_bytes: peak
            },
            "{report:?}"
        );
        assert_eq!(report.charged_bytes, MIB);
        assert_eq!(report.limit.bytes(), limit);
        assert_eq!(
            report.requested_bytes,
            peak - limit,
            "how far over the limit"
        );
        assert_eq!(
            report.suggested_limit_bytes, peak,
            "a limit at the process reading would not have tripped the backstop"
        );
        assert!(!report.oversized, "no single request was measured");
        let rendered = report.to_string();
        assert!(!rendered.contains("fully held"), "{rendered}");
        assert!(
            rendered.starts_with(
                "E310 \"enrich\": process memory peaked at 96.0 MiB resident, over memory.limit \
                 64.0 MiB"
            ),
            "{rendered}"
        );
        handle.shrink(MIB);
        arbitrator.unregister_consumer(id);
    }

    #[test]
    fn handle_and_governed_charges_share_one_limit() {
        let limit = MIB;
        let arbitrator = arbitrator(limit);
        let (handle, id) = register_node(&arbitrator, "totals");
        for iteration in 0..1_000 {
            for (bytes, expected) in [(limit / 2 + 1, 1), (limit / 2, 2)] {
                let barrier = Barrier::new(2);
                let (grown, granted) = std::thread::scope(|scope| {
                    let grower = scope.spawn(|| {
                        barrier.wait();
                        handle.try_grow(bytes).is_ok()
                    });
                    let requester = scope.spawn(|| {
                        barrier.wait();
                        arbitrator.reserve(bytes, governed())
                    });
                    (
                        grower.join().expect("grower thread"),
                        requester.join().expect("requester thread"),
                    )
                });
                let succeeded = usize::from(grown) + usize::from(granted.is_ok());
                assert_eq!(
                    succeeded, expected,
                    "iteration {iteration}: a handle growth and a governed request of {bytes} \
                     bytes each under a {limit}-byte limit"
                );
                assert_eq!(
                    arbitrator.charged_bytes(),
                    succeeded as u64 * bytes,
                    "the ledger charges exactly what was granted"
                );
                if grown {
                    handle.shrink(bytes);
                }
                drop(granted);
                assert_eq!(arbitrator.charged_bytes(), 0);
            }
        }
        arbitrator.unregister_consumer(id);
    }

    #[test]
    fn unregister_releases_its_charge() {
        let arbitrator = arbitrator(MIB);
        let run = arbitrator.reserve(KIB, governed()).expect("fits");
        let before = arbitrator.charged_bytes();
        let (handle, id) = register_node(&arbitrator, "totals");
        handle.try_grow(8 * KIB).expect("fits");
        assert_eq!(arbitrator.charged_bytes(), before + 8 * KIB);
        let epoch = arbitrator.release_epoch();
        assert!(arbitrator.unregister_consumer(id).is_some());
        assert_eq!(
            arbitrator.charged_bytes(),
            before,
            "unregistering returns its handle's charge"
        );
        assert_eq!(
            arbitrator.release_epoch(),
            epoch + 1,
            "returning the charge is one release"
        );
        drop(run);
        assert_eq!(arbitrator.charged_bytes(), 0);
    }

    #[test]
    fn bound_handle_peak_rises_on_try_grow_and_try_resize() {
        let arbitrator = arbitrator(MIB);

        let (sorted, sorted_id) = register_node(&arbitrator, "dept_totals");
        sorted.try_grow(4 * KIB).expect("fits");
        sorted.try_resize(12 * KIB).expect("fits");
        assert_eq!(sorted.peak_bytes(), 12 * KIB);
        sorted.shrink(8 * KIB);
        assert_eq!(sorted.bytes(), 4 * KIB);
        assert_eq!(
            sorted.peak_bytes(),
            12 * KIB,
            "a shrink never lowers the mark"
        );
        arbitrator.unregister_consumer(sorted_id);
        assert_eq!(
            sorted.peak_bytes(),
            12 * KIB,
            "unregistration never lowers the mark"
        );
        assert_eq!(
            arbitrator.per_node_peak_charged_bytes().get("dept_totals"),
            Some(&(12 * KIB)),
            "the node reports its consumer's mark under its label's node"
        );

        // The mark covers the handle's charge and the grants made in the
        // consumer's name at one instant.
        let (reader, reader_id) = register_node(&arbitrator, "by_region");
        reader.try_resize(12 * KIB).expect("fits");
        reader.shrink(8 * KIB);
        let mut grant = arbitrator
            .reserve(6 * KIB, Requester::for_consumer(reader_id))
            .expect("fits");
        assert_eq!(
            reader.peak_bytes(),
            12 * KIB,
            "4 KiB held plus 6 KiB granted stays under the 12 KiB mark"
        );
        grant.try_grow(2 * KIB).expect("fits");
        assert_eq!(
            reader.peak_bytes(),
            12 * KIB,
            "reaching the mark is not passing it"
        );
        grant.try_grow(2 * KIB).expect("fits");
        assert_eq!(
            reader.peak_bytes(),
            14 * KIB,
            "4 KiB held plus 10 KiB granted passes the mark"
        );
        drop(grant);
        arbitrator.unregister_consumer(reader_id);
        assert_eq!(reader.peak_bytes(), 14 * KIB);
        assert_eq!(
            arbitrator.per_node_peak_charged_bytes().get("by_region"),
            Some(&(14 * KIB))
        );
        assert_eq!(arbitrator.charged_bytes(), 0);
    }

    #[test]
    fn an_ownership_hand_off_is_charged_once() {
        let arbitrator = arbitrator(MIB);
        let (producer, producer_id) = register_node(&arbitrator, "sorted");
        let (slot, slot_id) = register_node(&arbitrator, "sorted");
        producer.try_grow(8 * KIB + 7).expect("fits");
        let peak = arbitrator.peak_charged_bytes();
        let epoch = arbitrator.release_epoch();

        // The rows' new owner charges a different figure for them than the
        // producer did; the total moves by the difference only.
        slot.take_over(&producer, 8 * KIB, 6 * KIB);
        assert_eq!(producer.bytes(), 7);
        assert_eq!(slot.bytes(), 6 * KIB);
        assert_eq!(arbitrator.charged_bytes(), 6 * KIB + 7);
        assert_eq!(
            arbitrator.peak_charged_bytes(),
            peak,
            "the moved bytes are never charged to both owners at once"
        );
        assert_eq!(
            arbitrator.release_epoch(),
            epoch + 1,
            "a net fall is a release"
        );
        assert_eq!(slot.peak_bytes(), 6 * KIB);

        // A hand-off to a larger figure raises the total by the difference.
        producer.take_over(&slot, 6 * KIB, 9 * KIB);
        assert_eq!(arbitrator.charged_bytes(), 9 * KIB + 7);
        assert_eq!(arbitrator.peak_charged_bytes(), 9 * KIB + 7);
        assert_eq!(slot.bytes(), 0);

        arbitrator.unregister_consumer(slot_id);
        arbitrator.unregister_consumer(producer_id);
        assert_eq!(arbitrator.charged_bytes(), 0);
    }

    #[test]
    fn release_after_the_entry_is_removed_is_unattributed() {
        let arbitrator = arbitrator(MIB);
        let gone = ConsumerId(4);
        arbitrator.charge_labelled_handle(gone, label("orders", MemorySurface::RowsRead), 0);
        let grant = arbitrator
            .reserve(4 * KIB, Requester::for_consumer(gone))
            .expect("fits");
        assert_eq!(arbitrator.forget_consumer_entry(gone), Some(4 * KIB));
        assert_eq!(arbitrator.consumer_peak_charged_bytes(gone), None);
        let snapshot = arbitrator.ledger_snapshot(0, governed());
        assert!(snapshot.holders.is_empty());
        assert_eq!(
            snapshot.unattributed,
            4 * KIB,
            "its live grant is unattributed"
        );
        assert_holders_cover_charged(&snapshot);

        let epoch = arbitrator.release_epoch();
        drop(grant);
        assert_eq!(arbitrator.charged_bytes(), 0);
        assert_eq!(arbitrator.release_epoch(), epoch + 1);
        assert_eq!(
            arbitrator.consumer_peak_charged_bytes(gone),
            None,
            "the release leaves the removed consumer's figures alone"
        );
    }
}

#[cfg(test)]
mod walk_pass_tests {
    use super::*;
    use crate::executor::ForcedShortfall;
    use crate::executor::dispatch::NodeBufferKey;
    use crate::pipeline::memory::walk::{
        SlotSpill, WalkContextGuard, WalkReclaimSet, WalkSpillSettings, with_test_reclaim,
    };
    use crate::pipeline::memory::{
        ArbitrationPolicy, BackPressurePreferred, ConsumerHandle, ConsumerSpillError, LargestFirst,
        MemoryConsumer, Priority,
    };
    use clinker_plan::config::CompressMode;
    use clinker_plan::runtime_error::MemorySurface;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::sync::atomic::Ordering;

    pub(super) const KIB: u64 = 1024;
    pub(super) const MIB: u64 = 1024 * KIB;

    /// A registered consumer with a fixed priority whose bytes are its
    /// handle's charge.
    struct Held {
        handle: Arc<ConsumerHandle>,
        priority: i32,
        pausable: bool,
    }

    impl MemoryConsumer for Held {
        fn current_usage(&self) -> u64 {
            self.handle.bytes()
        }
        fn peak_charged_bytes(&self) -> Option<u64> {
            Some(self.handle.peak_bytes())
        }
        fn spill_priority(&self) -> i32 {
            self.priority
        }
        fn try_spill(&self, _: u64) -> Result<u64, ConsumerSpillError> {
            Ok(0)
        }
        fn can_back_pressure(&self) -> bool {
            self.pausable
        }
    }

    pub(super) fn run(limit: u64, policy: Box<dyn ArbitrationPolicy>) -> Arc<MemoryArbitrator> {
        Arc::new(MemoryArbitrator::with_policy(limit, 0.80, 0.70, policy))
    }

    pub(super) fn empty_set() -> Rc<RefCell<WalkReclaimSet>> {
        Rc::new(RefCell::new(WalkReclaimSet::new(WalkSpillSettings {
            spill_root: Arc::from(std::env::temp_dir().as_path()),
            spill_compress: CompressMode::Auto,
            batch_size: 1024,
        })))
    }

    /// Make the calling thread `arbitrator`'s walk, owning `set`.
    pub(super) fn walk(
        arbitrator: &Arc<MemoryArbitrator>,
        set: &Rc<RefCell<WalkReclaimSet>>,
    ) -> WalkContextGuard {
        WalkContextGuard::install(arbitrator, Rc::clone(set))
    }

    pub(super) fn register(
        arbitrator: &MemoryArbitrator,
        node: &str,
        priority: i32,
        bytes: u64,
    ) -> (ConsumerId, Arc<ConsumerHandle>) {
        register_as(arbitrator, node, priority, bytes, false)
    }

    fn register_as(
        arbitrator: &MemoryArbitrator,
        node: &str,
        priority: i32,
        bytes: u64,
        pausable: bool,
    ) -> (ConsumerId, Arc<ConsumerHandle>) {
        let handle = ConsumerHandle::new();
        let id = arbitrator.register_node_consumer(
            Arc::new(Held {
                handle: Arc::clone(&handle),
                priority,
                pausable,
            }),
            Arc::clone(&handle),
            ConsumerLabel {
                node: node.to_string(),
                surface: MemorySurface::BufferedRows {
                    from: node.to_string(),
                    to: "next".to_string(),
                },
            },
        );
        handle.set_bytes(bytes);
        (id, handle)
    }

    /// A reclaim set scripted per consumer: resident victims give up their
    /// handle charge and the grants they hold; held victims are busy and have
    /// their spill request raised; everything else is not owned. Every
    /// consumer a pass elects is recorded, owned or not.
    #[derive(Default)]
    pub(super) struct Scripted {
        resident: HashMap<ConsumerId, (Arc<ConsumerHandle>, Vec<Grant>)>,
        held: HashMap<ConsumerId, Arc<ConsumerHandle>>,
        pub(super) spilled: Vec<ConsumerId>,
        elected: Vec<ConsumerId>,
        during_spill: Option<Box<dyn FnOnce()>>,
    }

    impl Scripted {
        pub(super) fn resident(self, id: ConsumerId, handle: &Arc<ConsumerHandle>) -> Self {
            self.resident_with(id, handle, Vec::new())
        }
        fn resident_with(
            mut self,
            id: ConsumerId,
            handle: &Arc<ConsumerHandle>,
            grants: Vec<Grant>,
        ) -> Self {
            self.resident.insert(id, (Arc::clone(handle), grants));
            self
        }
        fn held(mut self, id: ConsumerId, handle: &Arc<ConsumerHandle>) -> Self {
            self.held.insert(id, Arc::clone(handle));
            self
        }
        fn during_spill(mut self, during: impl FnOnce() + 'static) -> Self {
            self.during_spill = Some(Box::new(during));
            self
        }
        pub(super) fn shared(self) -> Rc<RefCell<Scripted>> {
            Rc::new(RefCell::new(self))
        }
    }

    impl WalkReclaim for Scripted {
        fn spill_victim(
            &mut self,
            id: ConsumerId,
            _arbitrator: &MemoryArbitrator,
        ) -> Result<VictimOutcome, PipelineError> {
            self.elected.push(id);
            if let Some(during) = self.during_spill.take() {
                during();
            }
            if let Some((handle, grants)) = self.resident.remove(&id) {
                handle.shrink(handle.bytes());
                drop(grants);
                self.spilled.push(id);
                return Ok(VictimOutcome::Spilled);
            }
            if let Some(handle) = self.held.get(&id) {
                handle.request_spill();
                return Ok(VictimOutcome::Busy);
            }
            Ok(VictimOutcome::NotOwned)
        }
    }

    pub(super) fn scripted<R>(set: &Rc<RefCell<Scripted>>, body: impl FnOnce() -> R) -> R {
        let stand_in: Rc<RefCell<dyn WalkReclaim>> = set.clone();
        with_test_reclaim(stand_in, body)
    }

    pub(super) fn governed() -> Requester {
        Requester::governed()
    }

    fn holder_bytes(arbitrator: &MemoryArbitrator, id: ConsumerId) -> u64 {
        arbitrator
            .ledger_snapshot(0, governed())
            .holders
            .iter()
            .find(|holder| holder.consumer == id)
            .map_or(0, |holder| holder.charged)
    }

    #[test]
    fn round_spills_walk_victims_before_refusing() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (a, a_handle) = register(&arbitrator, "a", 0, 400 * KIB);
        let (b, b_handle) = register(&arbitrator, "b", 0, 400 * KIB);
        let _filler = arbitrator
            .reserve(100 * KIB, governed())
            .expect("filler fits");
        assert_eq!(arbitrator.charged_bytes(), 900 * KIB);

        // Off the walk the same request is checked once and refused.
        let off_walk = std::thread::scope(|scope| {
            scope
                .spawn(|| arbitrator.reserve(200 * KIB, governed()).is_err())
                .join()
                .expect("off-walk requester")
        });
        assert!(off_walk, "a thread that is not the walk never reclaims");

        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let victims = Scripted::default()
            .resident(a, &a_handle)
            .resident(b, &b_handle)
            .shared();
        let grant = scripted(&victims, || arbitrator.reserve(200 * KIB, governed()))
            .expect("the walk spills a victim, then the request fits");
        assert_eq!(grant.bytes(), 200 * KIB);
        assert_eq!(victims.borrow().spilled, vec![a], "one victim was enough");
        assert_eq!(
            b_handle.bytes(),
            400 * KIB,
            "the other victim stays resident"
        );
        assert_eq!(arbitrator.charged_bytes(), 700 * KIB);
    }

    #[test]
    fn requester_is_elected_last() {
        let arbitrator = run(MIB, Box::new(Priority));
        // The requester would be the policy's first choice: priority 0 and
        // the most bytes.
        let (requester, requester_handle) = register(&arbitrator, "requester", 0, 400 * KIB);
        let (other, other_handle) = register(&arbitrator, "other", 10, 200 * KIB);
        let _filler = arbitrator
            .reserve(300 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let victims = Scripted::default()
            .resident(requester, &requester_handle)
            .resident(other, &other_handle)
            .shared();
        scripted(&victims, || requester_handle.try_grow(300 * KIB))
            .expect("the growth fits once both are spilled");
        assert_eq!(
            victims.borrow().spilled,
            vec![other, requester],
            "every other candidate goes before the requester's own state"
        );
        assert_eq!(requester_handle.bytes(), 300 * KIB);
    }

    #[test]
    fn round_reclaims_to_the_resume_watermark() {
        let arbitrator = run(MIB, Box::new(Priority));
        let victims: Vec<_> = (0..4)
            .map(|index| register(&arbitrator, &format!("v{index}"), 0, 200 * KIB))
            .collect();
        let _filler = arbitrator
            .reserve(100 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let script = victims
            .iter()
            .fold(Scripted::default(), |script, (id, handle)| {
                script.resident(*id, handle)
            })
            .shared();
        let _grant = scripted(&script, || arbitrator.reserve(150 * KIB, governed()))
            .expect("the request fits after the pass");
        // 26 KiB was short, which one victim would have covered. The pass
        // kept going until the ledger, with the request charged, sat at or
        // below the resume watermark.
        assert_eq!(script.borrow().spilled.len(), 2);
        assert!(
            arbitrator.charged_bytes() <= arbitrator.resume_limit(),
            "charged {} must be at or below the resume watermark {}",
            arbitrator.charged_bytes(),
            arbitrator.resume_limit()
        );
    }

    #[test]
    fn round_without_candidates_fails_at_once() {
        let arbitrator = run(MIB, Box::new(Priority));
        let _full = arbitrator
            .reserve(MIB, governed())
            .expect("the whole limit");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let before = arbitrator.reclaim_rounds();
        let shortfall = arbitrator
            .reserve(KIB, governed())
            .expect_err("nothing can be spilled, so the request is refused");
        assert_eq!(shortfall.requested, KIB);
        assert_eq!(
            arbitrator.reclaim_rounds() - before,
            2,
            "one pass that freed nothing, then the final pass, then the refusal"
        );
    }

    #[test]
    fn equal_victims_are_elected_in_id_order() {
        let policies: Vec<(&str, Box<dyn ArbitrationPolicy>)> = vec![
            ("priority", Box::new(Priority)),
            ("largest first", Box::new(LargestFirst)),
            (
                "pause, then priority",
                Box::new(BackPressurePreferred::wrapping(Priority)),
            ),
        ];
        for (name, policy) in policies {
            let arbitrator = run(MIB, policy);
            let victims: Vec<_> = ["a", "b", "c"]
                .into_iter()
                .map(|node| register(&arbitrator, node, 0, 250 * KIB))
                .collect();
            let _filler = arbitrator
                .reserve(250 * KIB, governed())
                .expect("filler fits");
            let set = empty_set();
            let _walk = walk(&arbitrator, &set);
            let script = victims
                .iter()
                .fold(Scripted::default(), |script, (id, handle)| {
                    script.resident(*id, handle)
                })
                .shared();
            let _grant = scripted(&script, || arbitrator.reserve(50 * KIB, governed()))
                .expect("two victims make room");
            assert_eq!(
                script.borrow().spilled,
                vec![victims[0].0, victims[1].0],
                "{name}: equal victims go in ascending id order"
            );
        }
    }

    #[test]
    fn busy_reclaim_set_frees_nothing_without_panicking() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (_victim, victim_handle) = register(&arbitrator, "victim", 0, 400 * KIB);
        let _filler = arbitrator
            .reserve(624 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let before = arbitrator.reclaim_rounds();
        let held = set.borrow_mut();
        let shortfall = arbitrator
            .reserve(KIB, governed())
            .expect_err("a borrowed set can spill nothing");
        drop(held);
        assert_eq!(shortfall.requested, KIB);
        assert_eq!(arbitrator.reclaim_rounds() - before, 2);
        assert_eq!(victim_handle.bytes(), 400 * KIB);
        assert!(
            !victim_handle.take_spill_request(),
            "a borrowed set cannot tell whose state it holds, so it flags nothing"
        );
    }

    #[test]
    fn progress_is_the_victims_own_charge_change() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (victim, victim_handle) = register(&arbitrator, "victim", 0, 100 * KIB);
        let _filler = arbitrator
            .reserve(700 * KIB, governed())
            .expect("filler fits");
        let old = arbitrator
            .reserve(128 * KIB, governed())
            .expect("old grant fits");
        let kept: Rc<RefCell<Option<Grant>>> = Rc::default();
        let other_thread = Arc::clone(&arbitrator);
        let keep = Rc::clone(&kept);
        let script = Scripted::default()
            .held(victim, &victim_handle)
            .during_spill(move || {
                let grant = std::thread::spawn(move || {
                    let grant = other_thread
                        .reserve(64 * KIB, Requester::governed())
                        .expect("another thread charges");
                    drop(old);
                    grant
                })
                .join()
                .expect("helper thread");
                *keep.borrow_mut() = Some(grant);
            })
            .shared();
        let charged_before = arbitrator.charged_bytes();
        let outcome = arbitrator
            .reclaim_pass(
                200 * KIB,
                governed(),
                &mut *script.borrow_mut(),
                PassKind::Ordinary,
            )
            .expect("the pass runs");
        assert_eq!(outcome.freed, 0, "the victim itself freed nothing");
        assert!(
            outcome.released_during,
            "the other thread's release is seen"
        );
        assert_eq!(
            charged_before - arbitrator.charged_bytes(),
            64 * KIB,
            "the ledger fell by 64 KiB, none of it the victim's"
        );
        assert!(kept.borrow().is_some());
    }

    #[test]
    fn slot_payload_released_by_its_spill_counts_as_the_victims_progress() {
        let arbitrator = run(512 * KIB, Box::new(Priority));
        // The payload was allocated by the Source's ingest thread in the
        // Source's name; the slot's own handle holds only the residue.
        let (source, _source_handle) = register_as(&arbitrator, "source", 0, 0, true);
        let reader = Arc::clone(&arbitrator);
        let payload: Vec<Grant> = std::thread::spawn(move || {
            (0..10)
                .map(|_| {
                    reader
                        .reserve(16 * KIB, Requester::for_consumer(source))
                        .expect("payload fits")
                })
                .collect()
        })
        .join()
        .expect("ingest thread");
        let (slot, slot_handle) = register(&arbitrator, "slot", 0, 16 * KIB);
        let (other, other_handle) = register(&arbitrator, "other", 0, 10 * KIB);
        let _filler = arbitrator
            .reserve(184 * KIB, governed())
            .expect("filler fits");
        let script = Scripted::default()
            .resident_with(slot, &slot_handle, payload)
            .resident(other, &other_handle)
            .shared();
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let outcome = arbitrator
            .reclaim_pass(
                150 * KIB,
                governed(),
                &mut *script.borrow_mut(),
                PassKind::Ordinary,
            )
            .expect("the pass runs");
        assert_eq!(
            outcome.freed,
            176 * KIB,
            "residue plus the payload its spill dropped"
        );
        assert_eq!(outcome.victims_spilled, 1);
        assert_eq!(script.borrow().spilled, vec![slot]);
        assert_eq!(holder_bytes(&arbitrator, source), 0);
    }

    #[test]
    fn other_threads_releases_are_not_a_victims_progress() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (victim, victim_handle) = register(&arbitrator, "victim", 0, 16 * KIB);
        let theirs = arbitrator
            .reserve(64 * KIB, governed())
            .expect("theirs fits");
        let _filler = arbitrator
            .reserve(800 * KIB, governed())
            .expect("filler fits");
        let script = Scripted::default()
            .resident(victim, &victim_handle)
            .during_spill(move || {
                std::thread::spawn(move || drop(theirs))
                    .join()
                    .expect("helper thread");
            })
            .shared();
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let outcome = arbitrator
            .reclaim_pass(
                200 * KIB,
                governed(),
                &mut *script.borrow_mut(),
                PassKind::Ordinary,
            )
            .expect("the pass runs");
        assert_eq!(outcome.freed, 16 * KIB, "only the victim's own residue");
        assert!(outcome.released_during);
    }

    #[test]
    fn partial_round_then_zero_round_fails() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (partial, partial_handle) = register(&arbitrator, "partial", 0, 100 * KIB);
        let _filler = arbitrator
            .reserve(924 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let script = Scripted::default()
            .resident(partial, &partial_handle)
            .shared();
        let before = arbitrator.reclaim_rounds();
        let shortfall = scripted(&script, || arbitrator.reserve(300 * KIB, governed()))
            .expect_err("100 KiB of a 300 KiB shortfall, then nothing");
        assert_eq!(
            arbitrator.reclaim_rounds() - before,
            3,
            "a pass that freed some, a pass that freed none, the final pass; no fourth"
        );
        assert_eq!(shortfall.requested, 300 * KIB);
        assert_eq!(
            shortfall.snapshot.charged,
            924 * KIB,
            "the refusal reports the ledger as the final pass left it"
        );
    }

    #[test]
    fn release_during_pass_retries_once_not_forever() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (held, held_handle) = register(&arbitrator, "held", 0, 100 * KIB);
        let theirs = arbitrator
            .reserve(24 * KIB, governed())
            .expect("theirs fits");
        let _filler = arbitrator
            .reserve(900 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let script = Scripted::default()
            .held(held, &held_handle)
            .during_spill(move || {
                std::thread::spawn(move || drop(theirs))
                    .join()
                    .expect("helper thread");
            })
            .shared();
        let before = arbitrator.reclaim_rounds();
        scripted(&script, || arbitrator.reserve(300 * KIB, governed()))
            .expect_err("the release does not make the request fit");
        assert_eq!(
            arbitrator.reclaim_rounds() - before,
            3,
            "the release earns one more pass; with none after it the walk decides"
        );
    }

    #[test]
    fn unowned_victim_is_skipped_never_flagged() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (_foreign, foreign_handle) = register(&arbitrator, "foreign", 0, 400 * KIB);
        let _filler = arbitrator
            .reserve(624 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let before = arbitrator.reclaim_rounds();
        arbitrator
            .reserve(KIB, governed())
            .expect_err("the only candidate is not the walk's to spill");
        assert_eq!(arbitrator.reclaim_rounds() - before, 2);
        assert_eq!(foreign_handle.bytes(), 400 * KIB);
        assert!(
            !foreign_handle.take_spill_request(),
            "an owner the walk does not hold is never asked to act"
        );
    }

    /// Register an Aggregate's table, held and reclaimable at `bytes`, that
    /// the walk does not own: its owner is another thread.
    fn table_off_the_walk(
        arbitrator: &MemoryArbitrator,
        node: &str,
        bytes: u64,
    ) -> (ConsumerId, Arc<ConsumerHandle>) {
        let handle = ConsumerHandle::new();
        let id = arbitrator.register_node_consumer(
            Arc::new(crate::aggregation::AggregateConsumer::new(Arc::clone(
                &handle,
            ))),
            Arc::clone(&handle),
            ConsumerLabel {
                node: node.to_string(),
                surface: MemorySurface::GroupState,
            },
        );
        handle.set_bytes(bytes);
        handle.set_reclaimable(bytes);
        (id, handle)
    }

    /// A table the round elected but could not reach was something the
    /// engine had no way to spill for the request: it is listed as unable
    /// to spill and its bytes count as state that cannot spill, though the
    /// reclaim line never names it as asked.
    #[test]
    fn a_holder_the_round_could_not_reach_cannot_spill_and_is_counted() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (_table, _table_handle) = table_off_the_walk(&arbitrator, "totals", 400 * KIB);
        let _filler = arbitrator
            .reserve(500 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let report = arbitrator
            .reserve(200 * KIB, governed())
            .expect_err("the only candidate is not the walk's to spill")
            .into_report(&arbitrator);

        let round = report.reclaim.as_ref().expect("the walk ran a round");
        assert!(round.holders_asked.is_empty(), "{round:?}");
        let states: Vec<(&str, HolderState)> = report
            .holders
            .iter()
            .map(|holder| (holder.node.as_str(), holder.state))
            .collect();
        assert_eq!(states, vec![("totals", HolderState::CannotSpill)]);
        assert_eq!(
            report.unspillable_bytes,
            900 * KIB,
            "the table's bytes and the memory no single node holds"
        );
        assert!(report.oversized, "no spill could have made room: {report}");
    }

    /// With no round there is no evidence that a holder was out of reach,
    /// so a table another thread owns, whose figure says a spill would free
    /// it, still reads `in use` and is not counted as state that cannot
    /// spill.
    #[test]
    fn with_no_round_a_table_off_the_walk_reads_in_use() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (_table, _table_handle) = table_off_the_walk(&arbitrator, "totals", 400 * KIB);
        let _filler = arbitrator
            .reserve(500 * KIB, governed())
            .expect("filler fits");
        let report = arbitrator
            .reserve(200 * KIB, governed())
            .expect_err("off the walk the request is checked once")
            .into_report(&arbitrator);

        assert!(report.reclaim.is_none(), "no round runs off the walk");
        assert_eq!(report.holders.len(), 1);
        assert_eq!(report.holders[0].state, HolderState::InUse);
        assert_eq!(report.unspillable_bytes, 500 * KIB);
        assert!(!report.oversized);
    }

    /// No pass can reach the sort-merge or range join kernels, which spill
    /// on thresholds of their own: their figure is 0, so no pass elects
    /// them, and a refused request lists them as unable to spill and counts
    /// their bytes as state that cannot spill, with or without a round.
    #[test]
    fn a_join_kernel_is_listed_as_unable_to_spill() {
        let arbitrator = run(4 * MIB, Box::new(Priority));
        let merge = ConsumerHandle::new();
        let merge_consumer = Arc::new(crate::pipeline::sort_merge_join::SortMergeConsumer::new(
            Arc::clone(&merge),
        ));
        arbitrator.register_node_consumer(
            merge_consumer.clone(),
            Arc::clone(&merge),
            ConsumerLabel {
                node: "matched".to_string(),
                surface: MemorySurface::JoinState,
            },
        );
        merge.set_bytes(1536 * KIB);
        let band = ConsumerHandle::new();
        let band_consumer = Arc::new(crate::pipeline::sort_buffer::SortConsumer::new(Arc::clone(
            &band,
        )));
        arbitrator.register_node_consumer(
            band_consumer.clone(),
            Arc::clone(&band),
            ConsumerLabel {
                node: "banded".to_string(),
                surface: MemorySurface::JoinState,
            },
        );
        band.set_bytes(MIB);
        assert_eq!(merge_consumer.reclaimable_bytes(), 0);
        assert_eq!(band_consumer.reclaimable_bytes(), 0);

        let off_walk = arbitrator
            .reserve(2 * MIB, governed())
            .expect_err("the kernels leave too little room")
            .into_report(&arbitrator);
        assert!(off_walk.reclaim.is_none());

        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let on_walk = arbitrator
            .reserve(2 * MIB, governed())
            .expect_err("no pass can free a kernel's state")
            .into_report(&arbitrator);
        let round = on_walk.reclaim.as_ref().expect("the walk ran a round");
        assert!(round.holders_asked.is_empty(), "{round:?}");

        for report in [&off_walk, &on_walk] {
            let states: Vec<(&str, HolderState)> = report
                .holders
                .iter()
                .map(|holder| (holder.node.as_str(), holder.state))
                .collect();
            assert_eq!(
                states,
                vec![
                    ("matched", HolderState::CannotSpill),
                    ("banded", HolderState::CannotSpill)
                ]
            );
            assert_eq!(report.unspillable_bytes, 2560 * KIB);
        }
    }

    #[test]
    fn forced_shortfall_spills_the_requester_even_with_a_priority_0_slot_resident() {
        for armed in [true, false] {
            let arbitrator = run(64 * MIB, Box::new(Priority));
            let (resident, resident_handle) = register(&arbitrator, "resident", 0, 400 * KIB);
            let (requester, requester_handle) = register(&arbitrator, "requester", 5, 40 * KIB);
            let lever = ForcedShortfall::at(|label| label.node == "requester", 1);
            let fired = lever.fired();
            if armed {
                arbitrator.arm_forced_shortfall(lever);
            }
            let set = empty_set();
            let _walk = walk(&arbitrator, &set);
            let script = Scripted::default()
                .resident(resident, &resident_handle)
                .resident(requester, &requester_handle)
                .shared();
            let before = arbitrator.reclaim_rounds();
            scripted(&script, || requester_handle.try_grow(4 * KIB))
                .expect("the growth is granted either way");
            if armed {
                assert_eq!(fired.load(Ordering::Relaxed), 1);
                assert_eq!(
                    script.borrow().spilled,
                    vec![requester],
                    "a forced pass elects only the requester"
                );
                assert_eq!(arbitrator.reclaim_rounds() - before, 1);
                assert_eq!(
                    requester_handle.bytes(),
                    4 * KIB,
                    "the retry grew by the real path"
                );
            } else {
                assert_eq!(fired.load(Ordering::Relaxed), 0);
                assert!(script.borrow().spilled.is_empty(), "nothing spills unarmed");
                assert_eq!(arbitrator.reclaim_rounds(), before);
                assert_eq!(requester_handle.bytes(), 44 * KIB);
            }
            assert_eq!(
                resident_handle.bytes(),
                400 * KIB,
                "the priority-0 slot stays resident"
            );
        }
    }

    #[test]
    fn forced_shortfall_on_a_busy_requester_flags_it_and_retries() {
        let arbitrator = run(64 * MIB, Box::new(Priority));
        let (_resident, resident_handle) = register(&arbitrator, "resident", 0, 400 * KIB);
        let (requester, requester_handle) = register(&arbitrator, "requester", 5, 40 * KIB);
        let set = empty_set();
        // The requester's slot is registered in the walk's set, but its
        // running arm has taken the buffer out: it is held.
        set.borrow_mut().slots_mut().register(
            NodeBufferKey::from(petgraph::graph::NodeIndex::new(1)),
            (requester, Arc::clone(&requester_handle)),
            SlotSpill {
                spill_allowed: true,
                node_name: Box::from("requester"),
            },
        );
        let lever = ForcedShortfall::at(|label| label.node == "requester", 1);
        let fired = lever.fired();
        arbitrator.arm_forced_shortfall(lever);
        let _walk = walk(&arbitrator, &set);
        let before = arbitrator.reclaim_rounds();
        requester_handle
            .try_grow(4 * KIB)
            .expect("a forced pass never ends in a refusal");
        assert_eq!(fired.load(Ordering::Relaxed), 1);
        assert_eq!(arbitrator.reclaim_rounds() - before, 1, "no final pass");
        assert_eq!(requester_handle.bytes(), 44 * KIB);
        assert_eq!(resident_handle.bytes(), 400 * KIB);
        assert!(
            requester_handle.take_spill_request(),
            "the held requester spills at its next batch boundary"
        );
    }

    #[test]
    fn walk_allocation_is_attributed_to_the_walk_requester() {
        let arbitrator = run(64 * MIB, Box::new(Priority));
        let provider = crate::executor::preparation::ExecutorResources::new(
            Arc::clone(&arbitrator),
            crate::pipeline::shutdown::ShutdownToken::detached(),
            None,
            std::num::NonZeroUsize::MIN,
            None,
        )
        .expect("provider");
        let scope = provider.allocation().scope().expect("scope");
        let layout = std::alloc::Layout::from_size_align(4096, 8).expect("layout");
        let (consumer, _handle) = register(&arbitrator, "requester", 0, 0);
        let set = empty_set();
        let walk_frame = walk(&arbitrator, &set);

        arbitrator.set_walk_requester(Some(consumer));
        let lease = scope.reserve(layout).expect("fits");
        assert_eq!(holder_bytes(&arbitrator, consumer), 4096);
        assert!(
            arbitrator
                .consumer_peak_charged_bytes(consumer)
                .unwrap_or(0)
                >= 4096
        );

        // Off the walk the requester names no one.
        let off_walk = std::thread::scope(|threads| {
            threads
                .spawn(|| {
                    let lease = scope.reserve(layout).expect("fits");
                    let attributed = holder_bytes(&arbitrator, consumer);
                    drop(lease);
                    attributed
                })
                .join()
                .expect("off-walk allocator")
        });
        assert_eq!(
            off_walk, 4096,
            "an off-walk allocation is charged to no consumer"
        );

        // The release follows the grant, not the requester current at drop.
        arbitrator.set_walk_requester(None);
        drop(lease);
        assert_eq!(holder_bytes(&arbitrator, consumer), 0);

        let unattributed = scope.reserve(layout).expect("fits");
        assert_eq!(holder_bytes(&arbitrator, consumer), 0);
        assert!(arbitrator.ledger_snapshot(0, governed()).unattributed >= 4096);
        drop(unattributed);
        drop(walk_frame);
    }

    #[test]
    fn optional_growth_never_reclaims() {
        use clinker_format::reserved::ReservedVec;
        let arbitrator = run(MIB, Box::new(Priority));
        let provider = crate::executor::preparation::ExecutorResources::new(
            Arc::clone(&arbitrator),
            crate::pipeline::shutdown::ShutdownToken::detached(),
            None,
            std::num::NonZeroUsize::MIN,
            None,
        )
        .expect("provider");
        let (victim, victim_handle) = register(&arbitrator, "victim", 0, 64 * KIB);
        let mut buffer: ReservedVec<u8> =
            ReservedVec::new(provider.allocation().scope().expect("scope"));
        buffer.reserve_exact(2048).expect("initial capacity");
        for _ in 0..2048 {
            buffer.push(1).expect("within capacity");
        }
        // Leave exactly 3 KiB free: the next push needs 2049 bytes and
        // prefers 4096.
        let _filler = arbitrator
            .reserve(MIB - arbitrator.charged_bytes() - 3 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let script = Scripted::default()
            .resident(victim, &victim_handle)
            .shared();

        let before = arbitrator.reclaim_rounds();
        scripted(&script, || buffer.push(1)).expect("the needed size fits");
        assert_eq!(
            buffer.capacity(),
            2049,
            "only the needed size was taken, not the doubled one"
        );
        assert_eq!(
            arbitrator.reclaim_rounds(),
            before,
            "spare capacity never runs a reclaim pass"
        );
        assert_eq!(victim_handle.bytes(), 64 * KIB, "the victim stays resident");
        assert!(script.borrow().spilled.is_empty());

        // With room for less than even the needed size, the needed size
        // takes the ordinary path, which spills the victim.
        let free = MIB - arbitrator.charged_bytes();
        let _tighter = arbitrator
            .reserve(free - KIB, governed())
            .expect("second filler fits");
        scripted(&script, || buffer.push(1)).expect("the pass makes room");
        assert_eq!(buffer.capacity(), 2050);
        assert_eq!(arbitrator.reclaim_rounds() - before, 1);
        assert_eq!(script.borrow().spilled, vec![victim]);
    }

    #[test]
    fn if_free_refusal_leaves_no_trace() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (victim, victim_handle) = register(&arbitrator, "victim", 0, 400 * KIB);
        let _filler = arbitrator
            .reserve(620 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let script = Scripted::default()
            .resident(victim, &victim_handle)
            .shared();
        let snapshot = arbitrator.ledger_snapshot(0, governed());
        let figures = (
            arbitrator.charged_bytes(),
            arbitrator.peak_charged_bytes(),
            arbitrator.release_epoch(),
            arbitrator.reclaim_rounds(),
        );
        let refused = scripted(&script, || arbitrator.reserve_if_free(10 * KIB, governed()))
            .expect_err("10 KiB does not fit in the 4 KiB free");
        assert_eq!(refused.requested, 10 * KIB);
        assert_eq!(
            (
                arbitrator.charged_bytes(),
                arbitrator.peak_charged_bytes(),
                arbitrator.release_epoch(),
                arbitrator.reclaim_rounds(),
            ),
            figures,
            "a refused optional request changes no ledger figure and runs no pass"
        );
        assert_eq!(arbitrator.ledger_snapshot(0, governed()), snapshot);
        assert!(script.borrow().spilled.is_empty());
        assert_eq!(victim_handle.bytes(), 400 * KIB);
    }

    /// The inline hash join's build side is approved charged-only: a spill
    /// cannot free it. A pass elects the spillable slot beside it and never
    /// the build side, however much more the build side holds, and with the
    /// slot gone the request is refused without the build side ever being
    /// asked to act. Its bytes stay charged throughout.
    #[test]
    fn charged_only_consumers_are_never_elected() {
        let arbitrator = run(11 * MIB, Box::new(Priority));
        let build_handle = ConsumerHandle::new();
        let build = arbitrator.register_node_consumer(
            Arc::new(crate::pipeline::combine::CombineHashConsumer::new(
                Arc::clone(&build_handle),
            )),
            Arc::clone(&build_handle),
            ConsumerLabel {
                node: "join".to_string(),
                surface: MemorySurface::JoinBuildSide,
            },
        );
        build_handle.set_bytes(10 * MIB);
        let (slot, slot_handle) = register(&arbitrator, "slot", 0, MIB);
        assert_eq!(arbitrator.charged_bytes(), 11 * MIB, "the ledger is full");

        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let script = Scripted::default().resident(slot, &slot_handle).shared();
        let _grant = scripted(&script, || arbitrator.reserve(512 * KIB, governed()))
            .expect("the slot's spill makes room");
        assert_eq!(
            script.borrow().elected,
            vec![slot],
            "only the slot is elected, though the build side holds ten times more"
        );

        let free = 11 * MIB - arbitrator.charged_bytes();
        let refused = scripted(&script, || arbitrator.reserve(free + KIB, governed()))
            .expect_err("nothing a spill can free is left");
        assert_eq!(refused.requested, free + KIB);
        assert_eq!(
            script.borrow().elected,
            vec![slot],
            "the refusing passes elect no one"
        );
        assert!(
            !build_handle.take_spill_request(),
            "the build side is never asked to spill"
        );
        assert_eq!(
            holder_bytes(&arbitrator, build),
            10 * MIB,
            "its bytes still count toward the ledger"
        );
    }

    /// A Source's handle charges the heap its queued events hold, which no
    /// spill can free: a Source is relieved by pausing and by the walk
    /// draining its channel. It reports nothing reclaimable, and a pass at a
    /// full ledger elects the slot beside it and never the Source.
    #[test]
    fn source_queue_charge_is_never_elected() {
        use crate::executor::source_stream::SourceConsumer;
        let arbitrator = run(MIB + 256 * KIB, Box::new(Priority));
        let source_handle = ConsumerHandle::new();
        let source_consumer = Arc::new(SourceConsumer::new(Arc::clone(&source_handle)));
        let source = arbitrator.register_consumer(
            source_consumer.clone(),
            Arc::clone(&source_handle),
            ConsumerLabel {
                node: "orders".to_string(),
                surface: MemorySurface::RowsRead,
            },
        );
        source_handle.set_bytes(256 * KIB);
        let (slot, slot_handle) = register(&arbitrator, "slot", 0, MIB);
        assert_eq!(
            arbitrator.charged_bytes(),
            MIB + 256 * KIB,
            "the ledger is full"
        );

        assert_eq!(source_consumer.reclaimable_bytes(), 0);
        assert_eq!(source_consumer.current_usage(), 262_144);
        let script = Scripted::default().resident(slot, &slot_handle).shared();
        arbitrator
            .reclaim_pass(
                MIB,
                governed(),
                &mut *script.borrow_mut(),
                PassKind::Ordinary,
            )
            .expect("the pass runs");
        assert_eq!(
            script.borrow().elected,
            vec![slot],
            "only the slot is elected, though the pass aims past its bytes"
        );
        assert_eq!(
            holder_bytes(&arbitrator, source),
            256 * KIB,
            "the queue charge stays"
        );
    }

    #[test]
    fn refused_round_reports_what_it_asked_and_freed() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (spills, spills_handle) = register(&arbitrator, "spills", 0, 200 * KIB);
        let (busy, busy_handle) = register(&arbitrator, "busy", 0, 300 * KIB);
        let (_unreached, _unreached_handle) = register(&arbitrator, "unreached", 0, 20 * KIB);
        let _filler = arbitrator
            .reserve(300 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        // `spills` gives up its rows; `busy` is in use in every pass;
        // `unreached` is not the walk's to spill.
        let script = Scripted::default()
            .resident(spills, &spills_handle)
            .held(busy, &busy_handle)
            .shared();
        let shortfall = scripted(&script, || arbitrator.reserve(700 * KIB, governed()))
            .expect_err("spilling 200 KiB leaves too little room");
        let report = shortfall.into_report(&arbitrator);

        let round = report.reclaim.as_ref().expect("the walk ran a round");
        let mut asked = round.holders_asked.clone();
        asked.sort();
        assert_eq!(
            asked,
            vec!["busy".to_string(), "spills".to_string()],
            "each victim the round asked is named once, and the one it could not reach is not"
        );
        assert_eq!(round.bytes_freed, 200 * KIB);
        assert!(round.sources_paused.is_empty());
        let states: Vec<(&str, HolderState)> = report
            .holders
            .iter()
            .map(|holder| (holder.node.as_str(), holder.state))
            .collect();
        assert_eq!(
            states,
            vec![
                ("busy", HolderState::InUse),
                ("unreached", HolderState::CannotSpill)
            ],
            "the spilled victim holds nothing and is no longer listed"
        );
        assert_eq!(report.unattributed_bytes, 300 * KIB);
        let text = report.to_string();
        assert!(
            text.contains("\n  reclaim: asked 2 holders to spill ("),
            "{text}"
        );
        assert!(
            text.contains("), freed 200.0 KiB; paused 0 sources\n"),
            "{text}"
        );
    }

    #[test]
    fn a_holder_that_spilled_what_it_could_is_at_its_floor() {
        /// Spills every victim it is asked to and frees nothing: what each
        /// holds is already its working minimum.
        struct FreesNothing;
        impl WalkReclaim for FreesNothing {
            fn spill_victim(
                &mut self,
                _: ConsumerId,
                _: &MemoryArbitrator,
            ) -> Result<VictimOutcome, PipelineError> {
                Ok(VictimOutcome::Spilled)
            }
        }

        let arbitrator = run(MIB, Box::new(Priority));
        let (_sticky, _sticky_handle) = register(&arbitrator, "sticky", 0, 200 * KIB);
        let _filler = arbitrator
            .reserve(600 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let stand_in: Rc<RefCell<dyn WalkReclaim>> = Rc::new(RefCell::new(FreesNothing));
        let shortfall = with_test_reclaim(stand_in, || arbitrator.reserve(500 * KIB, governed()))
            .expect_err("the victim frees nothing");
        let report = shortfall.into_report(&arbitrator);
        let round = report.reclaim.as_ref().expect("the walk ran a round");
        assert_eq!(round.holders_asked, vec!["sticky".to_string()]);
        assert_eq!(round.bytes_freed, 0);
        assert_eq!(report.holders.len(), 1);
        assert_eq!(report.holders[0].state, HolderState::AtFloor);
        let text = report.to_string();
        assert!(
            text.contains(
                "\n  remedy: \"sticky\"'s rows buffered between \"sticky\" and \"next\" holds \
                 200.0 KiB and could not be spilled further; see \
                 \"Rows buffered between two steps\" in clinker explain --code E310"
            ),
            "{text}"
        );
    }
}

#[cfg(test)]
mod reclaim_entry_tests {
    use super::walk_pass_tests::{
        KIB, MIB, Scripted, empty_set, governed, register, run, scripted, walk,
    };
    use super::*;
    use crate::pipeline::memory::{ConsumerHandle, ConsumerSpillError, MemoryConsumer, Priority};
    use clinker_plan::runtime_error::MemorySurface;
    use std::sync::Mutex;

    /// A registered consumer that records every `try_spill` call and answers
    /// it only by raising its spill request, reporting a figure that frees
    /// nothing now.
    struct Flagged {
        name: &'static str,
        handle: Arc<ConsumerHandle>,
        priority: i32,
        reclaimable: u64,
        pausable: bool,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl MemoryConsumer for Flagged {
        fn current_usage(&self) -> u64 {
            self.handle.bytes()
        }
        fn reclaimable_bytes(&self) -> u64 {
            self.reclaimable
        }
        fn spill_priority(&self) -> i32 {
            self.priority
        }
        fn try_spill(&self, target_bytes: u64) -> Result<u64, ConsumerSpillError> {
            self.calls.lock().expect("call log").push(self.name);
            self.handle.request_spill();
            // Claims the whole target: a caller that trusted this figure
            // would stop after the first candidate.
            Ok(target_bytes)
        }
        fn can_back_pressure(&self) -> bool {
            self.pausable
        }
    }

    fn flagged(
        arbitrator: &MemoryArbitrator,
        calls: &Arc<Mutex<Vec<&'static str>>>,
        name: &'static str,
        priority: i32,
        (charged, reclaimable): (u64, u64),
        pausable: bool,
    ) -> Arc<ConsumerHandle> {
        let handle = ConsumerHandle::new();
        arbitrator.register_node_consumer(
            Arc::new(Flagged {
                name,
                handle: Arc::clone(&handle),
                priority,
                reclaimable,
                pausable,
                calls: Arc::clone(calls),
            }),
            Arc::clone(&handle),
            ConsumerLabel {
                node: name.to_string(),
                surface: MemorySurface::GroupState,
            },
        );
        handle.set_bytes(charged);
        handle
    }

    /// On the walk, `spill_reclaimable` is one reclaim pass aimed at its
    /// target: it spills walk-owned victims in policy order until their own
    /// charge decreases cover the target, and reports those decreases.
    #[test]
    fn spill_reclaimable_runs_a_pass() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (a, a_handle) = register(&arbitrator, "a", 0, 300 * KIB);
        let (b, b_handle) = register(&arbitrator, "b", 5, 200 * KIB);
        let (c, c_handle) = register(&arbitrator, "c", 10, 100 * KIB);
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let script = Scripted::default()
            .resident(a, &a_handle)
            .resident(b, &b_handle)
            .resident(c, &c_handle)
            .shared();

        let before = arbitrator.reclaim_rounds();
        assert_eq!(
            scripted(&script, || arbitrator.spill_reclaimable(0)),
            0,
            "a zero target runs no pass"
        );
        assert_eq!(arbitrator.reclaim_rounds(), before);

        let freed = scripted(&script, || arbitrator.spill_reclaimable(400 * KIB));
        assert_eq!(
            script.borrow().spilled,
            vec![a, b],
            "victims in policy order until the target is covered"
        );
        assert_eq!(freed, 500 * KIB, "the victims' own charge decreases");
        assert_eq!(arbitrator.reclaim_rounds() - before, 1, "one pass");
        assert_eq!(arbitrator.charged_bytes(), 100 * KIB);
        assert_eq!(
            c_handle.bytes(),
            100 * KIB,
            "the last victim stays resident"
        );
    }

    /// A hard-limit backstop's projected growth is not charged; on the walk
    /// it runs the same loop a charge does, retrying after a productive pass
    /// and refusing only after a pass that freed nothing and a final pass.
    #[test]
    fn reclaim_before_abort_runs_the_walk_loop() {
        let arbitrator = run(MIB, Box::new(Priority));
        let (partial, partial_handle) = register(&arbitrator, "partial", 0, 100 * KIB);
        let (other, other_handle) = register(&arbitrator, "other", 5, 200 * KIB);
        let _filler = arbitrator
            .reserve(700 * KIB, governed())
            .expect("filler fits");
        let before = arbitrator.reclaim_rounds();
        arbitrator
            .reclaim_before_abort(governed(), 24 * KIB)
            .expect("what is free covers the projection");
        assert_eq!(arbitrator.reclaim_rounds(), before, "no pass when it fits");

        let off_walk = std::thread::scope(|threads| {
            threads
                .spawn(|| arbitrator.reclaim_before_abort(governed(), 200 * KIB))
                .join()
                .expect("off-walk requester")
        });
        let refused = off_walk.expect_err("off the walk the projection is checked once");
        assert_eq!(refused.requested, 200 * KIB);
        assert_eq!(arbitrator.reclaim_rounds(), before, "no pass off the walk");

        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let script = Scripted::default()
            .resident(partial, &partial_handle)
            .resident(other, &other_handle)
            .shared();
        let charged = arbitrator.charged_bytes();
        scripted(&script, || {
            arbitrator.reclaim_before_abort(governed(), 200 * KIB)
        })
        .expect("the pass makes room for the projection");
        assert_eq!(script.borrow().spilled, vec![partial, other]);
        assert_eq!(arbitrator.reclaim_rounds() - before, 1);
        assert_eq!(
            arbitrator.charged_bytes(),
            charged - 300 * KIB,
            "the projection itself is never charged"
        );

        let arbitrator = run(MIB, Box::new(Priority));
        let (partial, partial_handle) = register(&arbitrator, "partial", 0, 100 * KIB);
        let _filler = arbitrator
            .reserve(924 * KIB, governed())
            .expect("filler fits");
        let set = empty_set();
        let _walk = walk(&arbitrator, &set);
        let script = Scripted::default()
            .resident(partial, &partial_handle)
            .shared();
        let before = arbitrator.reclaim_rounds();
        let refused = scripted(&script, || {
            arbitrator.reclaim_before_abort(governed(), 300 * KIB)
        })
        .expect_err("100 KiB of a 300 KiB projection, then nothing");
        assert_eq!(refused.requested, 300 * KIB);
        assert_eq!(
            arbitrator.reclaim_rounds() - before,
            3,
            "a pass that freed some earns a retry; then a pass that freed none and the final pass"
        );
        assert_eq!(script.borrow().spilled, vec![partial]);
    }

    /// With no walk frame installed a pass spills nothing itself: it raises
    /// the spill request of spillable candidates in ranking order until their
    /// reclaimable bytes cover the target, ignoring what `try_spill` claims,
    /// and never touches a back-pressureable or charged-only consumer.
    #[test]
    fn spill_reclaimable_without_a_frame_flags_walk_candidates_in_rank_order() {
        let arbitrator = run(MIB, Box::new(Priority));
        let calls = Arc::default();
        let a = flagged(&arbitrator, &calls, "a", 0, (100 * KIB, 100 * KIB), false);
        let charged_only = flagged(
            &arbitrator,
            &calls,
            "charged_only",
            5,
            (500 * KIB, 0),
            false,
        );
        let b = flagged(&arbitrator, &calls, "b", 10, (100 * KIB, 100 * KIB), false);
        let source = flagged(&arbitrator, &calls, "source", 0, (50 * KIB, 50 * KIB), true);
        let c = flagged(&arbitrator, &calls, "c", 20, (100 * KIB, 100 * KIB), false);
        let charged = arbitrator.charged_bytes();
        let before = arbitrator.reclaim_rounds();

        let freed = arbitrator.spill_reclaimable(150 * KIB);
        assert_eq!(
            *calls.lock().expect("call log"),
            vec!["a", "b"],
            "rank order, until their reclaimable bytes cover the target"
        );
        assert_eq!(
            freed, 0,
            "a raised request frees nothing until its owner acts"
        );
        assert!(a.take_spill_request());
        assert!(b.take_spill_request());
        assert!(!charged_only.take_spill_request());
        assert!(!source.take_spill_request());
        assert!(!c.take_spill_request());
        assert_eq!(arbitrator.charged_bytes(), charged, "nothing spilled");
        assert_eq!(arbitrator.reclaim_rounds() - before, 1);
    }
}

#[cfg(test)]
mod candidate_order_tests {
    use super::walk_pass_tests::{MIB, run};
    use super::*;
    use crate::pipeline::memory::{
        ArbitrationPolicy, BackPressurePreferred, ConsumerHandle, ConsumerSpillError, LargestFirst,
        MemoryConsumer, Priority,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    /// How often a pass read each of one consumer's figures.
    #[derive(Default)]
    struct Reads {
        reclaimable: AtomicU64,
        priority: AtomicU64,
        back_pressure: AtomicU64,
    }

    /// A consumer with fixed figures that counts every read of them.
    struct Counted {
        reclaimable: u64,
        priority: i32,
        pausable: bool,
        reads: Arc<Reads>,
    }

    impl MemoryConsumer for Counted {
        fn current_usage(&self) -> u64 {
            self.reclaimable
        }
        fn reclaimable_bytes(&self) -> u64 {
            self.reads.reclaimable.fetch_add(1, Ordering::Relaxed);
            self.reclaimable
        }
        fn spill_priority(&self) -> i32 {
            self.reads.priority.fetch_add(1, Ordering::Relaxed);
            self.priority
        }
        fn try_spill(&self, _: u64) -> Result<u64, ConsumerSpillError> {
            Ok(0)
        }
        fn can_back_pressure(&self) -> bool {
            self.reads.back_pressure.fetch_add(1, Ordering::Relaxed);
            self.pausable
        }
    }

    /// One registered consumer of a population and its read counts.
    struct Member {
        id: ConsumerId,
        reads: Arc<Reads>,
    }

    /// `n` consumers with heavily tied figures: priorities 0 to 2,
    /// reclaimable bytes 0 to 4, and one in eight able to back-pressure.
    fn population(arbitrator: &MemoryArbitrator, n: usize) -> Vec<Member> {
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..n)
            .map(|index| {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let reads = Arc::new(Reads::default());
                let id = arbitrator.register_node_consumer(
                    Arc::new(Counted {
                        reclaimable: (seed >> 33) % 5,
                        priority: ((seed >> 20) % 3) as i32,
                        pausable: (seed >> 50).is_multiple_of(8),
                        reads: Arc::clone(&reads),
                    }),
                    ConsumerHandle::new(),
                    ConsumerLabel {
                        node: format!("c{index}"),
                        surface: MemorySurface::SortBuffer,
                    },
                );
                Member { id, reads }
            })
            .collect()
    }

    /// Builds one fresh instance of a policy.
    type MakePolicy = fn() -> Box<dyn ArbitrationPolicy>;

    /// The shipped policies and the back-pressure wrapper over each, by name.
    fn shipped_policies() -> Vec<(&'static str, MakePolicy)> {
        vec![
            ("Priority", || Box::new(Priority)),
            ("LargestFirst", || Box::new(LargestFirst)),
            ("BackPressurePreferred -> Priority", || {
                Box::new(BackPressurePreferred::wrapping(Priority))
            }),
            ("BackPressurePreferred -> LargestFirst", || {
                Box::new(BackPressurePreferred::wrapping(LargestFirst))
            }),
        ]
    }

    /// The order a pass asked victims in before candidates were read once:
    /// the policy asked to select over the remaining candidates in id order
    /// and in reverse, the lower id taken on a tie, until none is left; the
    /// requester last when it has reclaimable bytes.
    fn selection_order(
        arbitrator: &MemoryArbitrator,
        policy: &dyn ArbitrationPolicy,
        requester: Option<ConsumerId>,
    ) -> Vec<ConsumerId> {
        let registered = arbitrator.consumers.load();
        let mut others: Vec<(ConsumerId, &dyn MemoryConsumer)> = registered
            .iter()
            .filter(|(id, consumer)| {
                Some(*id) != requester
                    && !consumer.can_back_pressure()
                    && consumer.reclaimable_bytes() > 0
            })
            .map(|(id, consumer)| (*id, consumer.as_ref()))
            .collect();
        others.sort_by_key(|(id, _)| id.0);
        let mut order = Vec::with_capacity(others.len() + 1);
        while !others.is_empty() {
            let forward = policy.select_victim(&others, 0);
            let reversed: Vec<(ConsumerId, &dyn MemoryConsumer)> =
                others.iter().rev().copied().collect();
            let backward = policy.select_victim(&reversed, 0);
            let pick = match (forward, backward) {
                (Some(a), Some(b)) => {
                    if a.0 <= b.0 {
                        a
                    } else {
                        b
                    }
                }
                (Some(only), None) | (None, Some(only)) => only,
                (None, None) => break,
            };
            order.push(pick);
            others.retain(|(id, _)| *id != pick);
        }
        if let Some(requester) = requester
            && let Some((_, consumer)) = registered.iter().find(|(id, _)| *id == requester)
            && !consumer.can_back_pressure()
            && consumer.reclaimable_bytes() > 0
        {
            order.push(requester);
        }
        order
    }

    #[test]
    fn pass_candidates_read_each_figure_once() {
        for (name, policy) in shipped_policies() {
            let arbitrator = run(64 * MIB, policy());
            let members = population(&arbitrator, 64);
            let requester = members.get(32).map(|member| member.id);

            let order = arbitrator.pass_candidates(requester, PassKind::Ordinary);

            assert!(!order.is_empty(), "{name}: the population has candidates");
            let total: u64 = members
                .iter()
                .map(|member| member.reads.reclaimable.load(Ordering::Relaxed))
                .sum();
            for member in &members {
                for (figure, reads) in [
                    ("reclaimable bytes", &member.reads.reclaimable),
                    ("spill priority", &member.reads.priority),
                    ("back-pressure", &member.reads.back_pressure),
                ] {
                    let reads = reads.load(Ordering::Relaxed);
                    assert!(
                        reads <= 1,
                        "{name}: one pass read consumer {}'s {figure} {reads} times \
                         ({total} reclaimable-byte reads over {} consumers)",
                        member.id.0,
                        members.len()
                    );
                }
            }
        }
    }

    #[test]
    fn pass_candidates_order_matches_the_policy_selection() {
        for n in [16usize, 64, 256] {
            for (name, policy) in shipped_policies() {
                let arbitrator = run(64 * MIB, policy());
                let members = population(&arbitrator, n);
                for requester in [None, members.get(n / 2).map(|member| member.id)] {
                    let order = arbitrator.pass_candidates(requester, PassKind::Ordinary);
                    let oracle = policy();
                    let expected = selection_order(&arbitrator, oracle.as_ref(), requester);
                    assert!(
                        expected.len() > n / 2,
                        "{name}, n = {n}: the population has many candidates"
                    );
                    assert_eq!(
                        order, expected,
                        "{name}, n = {n}, requester {requester:?}: the pass asks in the order \
                         the policy's selection gives"
                    );
                }
            }
        }
    }
}
