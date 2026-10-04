//! The memory ledger's synchronized core: the charged total and its peak, the
//! bytes charged in each consumer's name with their high-water mark, the
//! release epoch, and the disk and descriptor counts that share its lock.
//!
//! It holds byte counts, an epoch and holder labels, never records, RSS
//! readings or cleanup callbacks. Nothing here performs I/O, logs, emits
//! telemetry or calls into a consumer, so every critical section is a few
//! arithmetic steps (a snapshot also copies the holders' labels).
//!
//! The file locks only through `super::sync` and names no other crate type:
//! consumer ids are raw `u32`s and the holder label is a type parameter. That
//! keeps it compilable against a model checker's `sync` module unchanged.

use super::sync::{Mutex, MutexGuard};
use std::collections::BTreeMap;

/// The ledger state behind its one mutex.
///
/// `L` is the label a holder is reported under; `A` is caller-owned state
/// that must change under the same lock as the charges it reflects.
pub(crate) struct LedgerCore<L, A = ()> {
    state: Mutex<LedgerState<L, A>>,
}

impl<L, A> LedgerCore<L, A> {
    /// An open ledger admitting charges up to `limit` bytes.
    pub(crate) fn new(limit: u64, attachment: A) -> Self {
        Self {
            state: Mutex::new(LedgerState {
                limit,
                charged: 0,
                peak_charged: 0,
                granted: 0,
                peak_granted: 0,
                release_epoch: 0,
                consumers: BTreeMap::new(),
                closed: false,
                disk: 0,
                descriptors: 0,
                attachment,
            }),
        }
    }

    /// Lock the ledger. A panic while it was held leaves only counts behind,
    /// so a poisoned lock is recovered rather than propagated.
    pub(crate) fn lock(&self) -> MutexGuard<'_, LedgerState<L, A>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Why a charge was refused. Nothing was charged in any case.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    /// The ledger no longer admits charges.
    Closed,
    /// The request does not fit. `available` is what could have been
    /// granted; `oversized` means no release could ever make it fit.
    Short { available: u64, oversized: bool },
    /// The attachment's [`AdmissionGate`] refused a request that could
    /// otherwise have been checked against the limit.
    Forced,
}

/// A refusal the ledger's attachment may make at admission, before the
/// request is checked against what is available.
///
/// Every charge reaches it: [`LedgerState::try_charge`] with its attribution
/// and [`LedgerState::try_charge_handle`] with its handle's id. It is
/// consulted only when [`Self::CAN_REFUSE`] is true, and only for a request
/// made in a labelled consumer's name that the ledger would otherwise weigh:
/// the ledger is open and the request is nonzero and within the limit. It
/// runs under the ledger lock, so whatever it counts changes in the same
/// step as the charge it decides.
pub(crate) trait AdmissionGate<L> {
    /// Whether this gate can ever refuse. A gate that cannot leaves it
    /// false, and admission skips the requester lookup entirely.
    const CAN_REFUSE: bool = false;

    /// Refuse the request by the consumer registered under `label`, which
    /// holds `resident` bytes now (its handle plus what grants made in its
    /// name hold).
    fn force_refusal(&mut self, _label: &L, _resident: u64) -> bool {
        false
    }
}

impl<L> AdmissionGate<L> for () {}

/// Bytes charged in one consumer's name.
///
/// `handle` is what the consumer's own handle has charged and `attributed`
/// what grants made for it hold now; `mark` is the largest their sum has
/// reached. An entry is created by the first charge to its id and removed
/// only when its consumer unregisters.
struct ConsumerEntry<L> {
    handle: u64,
    attributed: u64,
    mark: u64,
    label: Option<L>,
}

impl<L> ConsumerEntry<L> {
    fn empty() -> Self {
        Self {
            handle: 0,
            attributed: 0,
            mark: 0,
            label: None,
        }
    }

    fn current(&self) -> u64 {
        self.handle.saturating_add(self.attributed)
    }

    fn raise_mark(&mut self) {
        self.mark = self.mark.max(self.current());
    }
}

/// The locked ledger. Reached only through [`LedgerCore::lock`].
pub(crate) struct LedgerState<L, A> {
    limit: u64,
    charged: u64,
    peak_charged: u64,
    /// The part of `charged` that grants hold (governed allocations, in a
    /// consumer's name or none), as opposed to consumer handles.
    granted: u64,
    peak_granted: u64,
    /// Advanced by every release of a nonzero byte count and by nothing
    /// else, so an unchanged epoch across a span proves no byte was released
    /// in it.
    release_epoch: u64,
    consumers: BTreeMap<u32, ConsumerEntry<L>>,
    pub(crate) closed: bool,
    /// Writer disk quota currently granted.
    pub(crate) disk: u64,
    /// Writer file descriptors currently granted.
    pub(crate) descriptors: usize,
    pub(crate) attachment: A,
}

impl<L, A> LedgerState<L, A> {
    /// The limit charges are admitted against.
    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }

    /// Replace the limit, refusing one below the bytes already charged.
    /// On refusal the charged total is returned and the limit is unchanged.
    pub(crate) fn set_limit(&mut self, limit: u64) -> Result<(), u64> {
        if limit < self.charged {
            return Err(self.charged);
        }
        self.limit = limit;
        Ok(())
    }

    /// Bytes charged now.
    pub(crate) fn charged(&self) -> u64 {
        self.charged
    }

    /// Highest charged total so far; a release never lowers it.
    pub(crate) fn peak_charged(&self) -> u64 {
        self.peak_charged
    }

    /// Bytes grants hold now: the charged total less every consumer handle's
    /// charge.
    pub(crate) fn granted(&self) -> u64 {
        self.granted
    }

    /// Highest [`Self::granted`] so far; a release never lowers it.
    pub(crate) fn peak_granted(&self) -> u64 {
        self.peak_granted
    }

    /// Bytes a request could be granted now.
    pub(crate) fn available(&self) -> u64 {
        self.limit.saturating_sub(self.charged)
    }

    /// Check and charge `bytes` in one step, attributed to consumer
    /// `attribution` when given.
    ///
    /// A closed ledger refuses everything; otherwise a zero-byte request is
    /// granted without charging. A request larger than the limit, or one the
    /// charged total could not represent, is refused as oversized; one the
    /// attachment's [`AdmissionGate`] refuses is refused as forced; one
    /// larger than [`Self::available`] is refused as short. A grant raises
    /// the peak and, when attributed, the consumer's attributed bytes and
    /// mark.
    ///
    /// A charge attributed to a consumer the ledger holds no entry for (one
    /// already unregistered, reached through a later lease from its view or
    /// a grant's growth) creates an unlabelled entry for it, which nothing
    /// removes until the run ends. The entry is never reported as a labelled
    /// holder, but its consumer's peak mark reads from it again.
    pub(crate) fn try_charge(&mut self, bytes: u64, attribution: Option<u32>) -> Result<(), Refusal>
    where
        A: AdmissionGate<L>,
    {
        self.admit(bytes, attribution)?;
        self.granted = self.granted.saturating_add(bytes);
        self.peak_granted = self.peak_granted.max(self.granted);
        if let Some(id) = attribution
            && bytes > 0
        {
            let entry = self
                .consumers
                .entry(id)
                .or_insert_with(ConsumerEntry::empty);
            entry.attributed = entry.attributed.saturating_add(bytes);
            entry.raise_mark();
        }
        Ok(())
    }

    /// Check and charge `bytes` to consumer `id`'s handle in one step, with
    /// [`Self::try_charge`]'s refusals. A grant raises the peak and the
    /// consumer's mark.
    pub(crate) fn try_charge_handle(&mut self, id: u32, bytes: u64) -> Result<(), Refusal>
    where
        A: AdmissionGate<L>,
    {
        self.admit(bytes, Some(id))?;
        let entry = self
            .consumers
            .entry(id)
            .or_insert_with(ConsumerEntry::empty);
        entry.handle = entry.handle.saturating_add(bytes);
        entry.raise_mark();
        Ok(())
    }

    /// The check both charge kinds share: refuse, or add `bytes` to the
    /// charged total and raise the peak. `requester` is the consumer the
    /// charge is made in the name of, if any; only the gate reads it.
    fn admit(&mut self, bytes: u64, requester: Option<u32>) -> Result<(), Refusal>
    where
        A: AdmissionGate<L>,
    {
        if self.closed {
            return Err(Refusal::Closed);
        }
        if bytes == 0 {
            return Ok(());
        }
        let available = self.available();
        if bytes > self.limit {
            return Err(Refusal::Short {
                available,
                oversized: true,
            });
        }
        if A::CAN_REFUSE
            && let Some(entry) = requester.and_then(|id| self.consumers.get(&id))
            && let Some(label) = &entry.label
            && self.attachment.force_refusal(label, entry.current())
        {
            return Err(Refusal::Forced);
        }
        if bytes > available {
            return Err(Refusal::Short {
                available,
                oversized: false,
            });
        }
        let Some(charged) = self.charged.checked_add(bytes) else {
            return Err(Refusal::Short {
                available,
                oversized: true,
            });
        };
        self.charged = charged;
        self.peak_charged = self.peak_charged.max(charged);
        Ok(())
    }

    /// Release `bytes` charged with attribution `attribution`.
    ///
    /// Lowers the charged total and, whenever the attributed consumer has an
    /// entry, its attributed bytes. That entry may be one a later charge
    /// recreated after the consumer unregistered (see [`Self::try_charge`]),
    /// and the release then lowers it too. Only when no entry exists (its
    /// consumer unregistered while a grant in its name was still live, and
    /// nothing charged in its name since) are the per-consumer figures
    /// untouched: those bytes were unattributed from the removal on.
    /// Advances the release epoch.
    pub(crate) fn release(&mut self, bytes: u64, attribution: Option<u32>) {
        if bytes == 0 {
            return;
        }
        self.discharge(bytes);
        self.granted = self.granted.saturating_sub(bytes);
        if let Some(id) = attribution
            && let Some(entry) = self.consumers.get_mut(&id)
        {
            entry.attributed = entry.attributed.saturating_sub(bytes);
        }
    }

    fn discharge(&mut self, bytes: u64) {
        debug_assert!(
            bytes <= self.charged,
            "released {bytes} bytes with only {} charged",
            self.charged
        );
        self.charged = self.charged.saturating_sub(bytes);
        self.release_epoch = self.release_epoch.wrapping_add(1);
    }

    /// Number of nonzero releases so far. An unchanged epoch across a span
    /// proves nothing was released in it.
    pub(crate) fn release_epoch(&self) -> u64 {
        self.release_epoch
    }

    /// Record consumer `id` under `label` and charge the `bytes` its handle
    /// already holds, unchecked: those bytes are resident before the
    /// consumer registers, so refusing them would not free them.
    pub(crate) fn bind_handle(&mut self, id: u32, label: L, bytes: u64) {
        self.charged = self.charged.saturating_add(bytes);
        self.peak_charged = self.peak_charged.max(self.charged);
        let entry = self
            .consumers
            .entry(id)
            .or_insert_with(ConsumerEntry::empty);
        entry.label = Some(label);
        entry.handle = entry.handle.saturating_add(bytes);
        entry.raise_mark();
    }

    /// Set consumer `id`'s handle charge to `bytes`, unchecked, and return the
    /// charge it replaced. A rise raises the peak and the mark; a fall is a
    /// release.
    pub(crate) fn set_handle(&mut self, id: u32, bytes: u64) -> u64 {
        let entry = self
            .consumers
            .entry(id)
            .or_insert_with(ConsumerEntry::empty);
        let previous = entry.handle;
        entry.handle = bytes;
        entry.raise_mark();
        if bytes >= previous {
            self.charged = self.charged.saturating_add(bytes - previous);
            self.peak_charged = self.peak_charged.max(self.charged);
        } else {
            self.discharge(previous - bytes);
        }
        previous
    }

    /// Set consumer `from`'s handle charge to `from_bytes` and consumer
    /// `to`'s to `to_bytes` in one step, unchecked, so bytes changing owner
    /// are never charged twice or to nobody. The charged total moves only by
    /// the net difference: a rise raises the peak, a fall is a release.
    pub(crate) fn move_handle_charge(
        &mut self,
        from: u32,
        from_bytes: u64,
        to: u32,
        to_bytes: u64,
    ) {
        let before = self
            .handle_bytes(from)
            .saturating_add(self.handle_bytes(to));
        for (id, bytes) in [(from, from_bytes), (to, to_bytes)] {
            let entry = self
                .consumers
                .entry(id)
                .or_insert_with(ConsumerEntry::empty);
            entry.handle = bytes;
            entry.raise_mark();
        }
        let after = from_bytes.saturating_add(to_bytes);
        if after >= before {
            self.charged = self.charged.saturating_add(after - before);
            self.peak_charged = self.peak_charged.max(self.charged);
        } else {
            self.discharge(before - after);
        }
    }

    /// Consumer `id`'s handle charge now.
    pub(crate) fn handle_bytes(&self, id: u32) -> u64 {
        self.consumers.get(&id).map_or(0, |entry| entry.handle)
    }

    /// Remove consumer `id`'s entry, releasing its remaining handle charge,
    /// and return its mark. Bytes still granted in its name stay charged and
    /// are unattributed from here on.
    pub(crate) fn remove_consumer(&mut self, id: u32) -> Option<u64> {
        let entry = self.consumers.remove(&id)?;
        if entry.handle > 0 {
            self.discharge(entry.handle);
        }
        Some(entry.mark)
    }

    /// `id`'s high-water mark of handle plus attributed bytes, or `None`
    /// when the ledger holds no entry for it.
    pub(crate) fn consumer_mark(&self, id: u32) -> Option<u64> {
        self.consumers.get(&id).map(|entry| entry.mark)
    }

    /// Every labelled consumer holding bytes now, with its current handle
    /// plus attributed bytes, largest first (ties by id); and the charged
    /// bytes none of them holds.
    ///
    /// The remainder is what grants made in no consumer's name hold, plus
    /// what consumers without a label hold, so the holders' figures and the
    /// remainder add up to the charged total.
    pub(crate) fn holders(&self) -> (Vec<(u32, &L, u64)>, u64) {
        let mut holders: Vec<(u32, &L, u64)> = self
            .consumers
            .iter()
            .filter_map(|(&id, entry)| {
                let held = entry.current();
                match &entry.label {
                    Some(label) if held > 0 => Some((id, label, held)),
                    _ => None,
                }
            })
            .collect();
        holders.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)));
        let held = holders
            .iter()
            .fold(0u64, |sum, holder| sum.saturating_add(holder.2));
        (holders, self.charged.saturating_sub(held))
    }
}
