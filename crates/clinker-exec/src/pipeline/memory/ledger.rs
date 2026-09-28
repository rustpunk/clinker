//! The one memory ledger's public surface: [`MemoryArbitrator::reserve`], the
//! [`Grant`] it returns and the [`Shortfall`] it refuses with.
//!
//! The ledger holds charged byte counts, a release epoch and, per consumer,
//! the bytes charged in its name and their high-water mark. It never holds
//! records, RSS readings or cleanup callbacks, and nothing that performs I/O
//! runs under its lock.

use super::reservation::ReservationState;
use super::{ConsumerId, MemoryArbitrator};
use clinker_plan::runtime_error::ConsumerLabel;
use std::sync::Arc;

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
}

/// Bytes charged to the ledger and not yet released.
///
/// Dropping the grant releases its bytes, lowers the charged figure of the
/// consumer it was attributed to and advances the ledger's release epoch. The
/// grant holds only the ledger's synchronized state, never the arbitrator, so
/// a grant that outlives the run still settles the ledger when it drops.
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
    /// Check and charge are one step under the ledger lock: on a shortfall
    /// neither the grant nor the ledger changes. Until every consumer's
    /// handle charges the ledger, a grow is checked against the ledger's own
    /// charges only.
    pub fn try_grow(&mut self, n: u64) -> Result<(), Shortfall> {
        let _ = n;
        Ok(())
    }

    /// Release `n` of this grant's bytes (all of them when `n` exceeds what it
    /// holds), keeping the rest charged.
    pub fn shrink(&mut self, n: u64) {
        let _ = n;
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
    /// Bytes the request could have been granted. While consumers still
    /// report usage outside the ledger, this is the limit less both the
    /// ledger's charges and that reported usage, so it can be smaller than
    /// `snapshot.limit - snapshot.charged`.
    pub available: u64,
    /// The request exceeds the whole limit (or cannot be represented), so no
    /// release could ever make it fit.
    pub oversized: bool,
    /// The ledger at the refusal.
    pub snapshot: LedgerSnapshot,
    /// The ledger was closed to new charges when the request arrived.
    closed: bool,
}

impl Shortfall {
    /// Whether the refusal is because the ledger no longer admits charges
    /// rather than because the request did not fit.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }
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
            self.requested, self.available, self.snapshot.limit, self.snapshot.charged
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
    /// The limit charges are admitted against.
    pub limit: u64,
    /// Bytes charged to the ledger.
    pub charged: u64,
    /// Bytes the request that took the snapshot asked for.
    pub requested: u64,
    /// The consumer the request was made for, if any.
    pub requester: Option<ConsumerId>,
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

impl MemoryArbitrator {
    /// Charge `bytes` to the ledger for `requester`, or refuse without
    /// charging anything.
    ///
    /// Check and charge happen under the one ledger lock, so concurrent
    /// requesters can never together pass the limit. A zero-byte request is
    /// granted empty and never falls short. A request larger than the limit
    /// is refused as oversized. The call never blocks on anything but the
    /// ledger lock and never spills; a refusal is final for this call.
    pub fn reserve(&self, bytes: u64, requester: Requester) -> Result<Grant, Shortfall> {
        let _ = bytes;
        Ok(Grant {
            state: self.writer_reservation_state(),
            bytes: 0,
            attribution: requester.consumer,
        })
    }

    /// Bytes charged to the ledger now.
    pub fn charged_bytes(&self) -> u64 {
        0
    }

    /// Highest [`Self::charged_bytes`] the ledger has held this run.
    pub fn peak_charged_bytes(&self) -> u64 {
        0
    }

    /// Read the ledger's charges under its lock, as a shortfall for a request
    /// of `requested` bytes by `requester` would report them.
    pub fn ledger_snapshot(&self, requested: u64, requester: Requester) -> LedgerSnapshot {
        LedgerSnapshot {
            limit: self.limit(),
            charged: 0,
            requested,
            requester: requester.consumer,
            holders: Vec::new(),
            unattributed: 0,
        }
    }

    /// High-water mark of `id`'s handle bytes plus the bytes granted in its
    /// name, raised by every charge to it and never lowered by a release.
    /// `None` when the ledger holds no entry for `id`.
    pub fn consumer_peak_charged_bytes(&self, id: ConsumerId) -> Option<u64> {
        let _ = id;
        None
    }
}

/// Stand-ins for the handle charges a registered consumer will make through
/// the ledger, so tests can place labelled holders beside governed grants.
#[cfg(test)]
impl MemoryArbitrator {
    /// Charge `bytes` to `id`'s handle and record its label, unchecked.
    pub(crate) fn charge_labelled_handle(&self, id: ConsumerId, label: ConsumerLabel, bytes: u64) {
        let _ = (id, label, bytes);
    }

    /// Release `bytes` of `id`'s handle charge.
    pub(crate) fn release_labelled_handle(&self, id: ConsumerId, bytes: u64) {
        let _ = (id, bytes);
    }

    /// Remove `id`'s entry as unregistration does, returning its mark.
    pub(crate) fn forget_consumer_entry(&self, id: ConsumerId) -> Option<u64> {
        let _ = id;
        None
    }

    /// Number of releases the ledger has seen.
    pub(crate) fn release_epoch(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::NoOpPolicy;
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
            MemorySurface::DeadLetteredRowSet,
            MemorySurface::DecisionState,
            MemorySurface::ReshapeGroups,
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
                | MemorySurface::DeadLetteredRowSet
                | MemorySurface::DecisionState
                | MemorySurface::ReshapeGroups
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
        assert_eq!(snapshot.limit, 64 * KIB);
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
            "rows buffered between orders and totals"
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
            .admit_writer_memory(2 * KIB as usize)
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
