//! Provider-neutral capabilities admitted before a compiled run may start.
//!
//! The executor owns this boundary so runtime code never imports a CLI
//! provider, profile, credential, or secret type. Admission validates a
//! complete sealed activation inventory before invoking any capacity
//! reservation. The resulting bundle is move-only and releases reservations,
//! unopened factories, and opened sessions in reverse order on every exit.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use clinker_plan::credentials::CredentialRequirementName;
use clinker_plan::plan::execution::{
    CompiledSourceInstanceId, SourceActivationCapacity, SourceActivationGroupId,
    SourceActivationPlan,
};

/// An opaque provider-owned session opened for one compiled Source instance.
///
/// The executor never serializes or formats this value. Implementations use
/// `Drop` to release local and provider-side state.
pub trait CapabilitySession: Send {}

/// A provider-neutral, single-use session factory.
///
/// Implementations retain any credential lease internally; they expose no
/// secret accessor and return only a closed failure.
pub trait CapabilityOpener: Send {
    /// Consume this factory and open one opaque session.
    fn open(self: Box<Self>) -> Result<Box<dyn CapabilitySession>, CapabilityOpenError>;
}

/// One all-or-none capacity reservation retained for an activation group.
///
/// The guard releases its reservation through `Drop`. It deliberately exposes
/// no capacity mutation or provider-specific state.
pub trait GroupCapacityLease: Send {}

/// A deferred group-capacity reservation.
///
/// Admission first validates the complete compiled inventory, then invokes
/// these reservations in dependency-stable group order. A later failure drops
/// already-acquired guards in reverse order.
pub trait GroupCapacityReservation: Send {
    /// Reserve the exact capacity declared by the surrounding group request.
    fn reserve(self: Box<Self>) -> Result<Box<dyn GroupCapacityLease>, CapabilityReservationError>;
}

/// Closed failure returned by an opaque opener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityOpenError {
    /// The provider could not open the admitted session.
    Unavailable,
}

impl fmt::Display for CapabilityOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an admitted capability session could not be opened")
    }
}

impl std::error::Error for CapabilityOpenError {}

/// Closed failure returned by a group-capacity reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityReservationError {
    /// The complete group capacity is unavailable.
    Unavailable,
}

impl fmt::Display for CapabilityReservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("activation-group capacity could not be reserved")
    }
}

impl std::error::Error for CapabilityReservationError {}

/// Stable closed classification for admitted-capability failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunCapabilityErrorKind {
    /// The recursive activation inventory was not sealed.
    UnsealedActivation,
    /// Credential-bearing activation is unsupported at this admission edge.
    UnsupportedCredentialRequirements,
    /// Group identities were missing, duplicated, or unexpected.
    GroupInventoryMismatch,
    /// A group did not carry its exact checked capacity.
    CapacityMismatch,
    /// A group did not carry the exact compiled Source identities.
    SourceInventoryMismatch,
    /// A group did not carry the exact logical credential requirement set.
    CredentialInventoryMismatch,
    /// A deferred all-or-none reservation failed.
    ReservationFailed,
    /// The bundle belongs to a different compiled activation inventory.
    ActivationPlanMismatch,
    /// The requested group was absent or already transferred.
    GroupUnavailable,
    /// The requested Source was absent or its opener was already consumed.
    SourceUnavailable,
    /// An admitted opener failed without exposing provider detail.
    OpenFailed,
}

/// Sanitized admitted-capability failure.
///
/// The error retains no group ID, Source identity, logical credential name,
/// provider text, or opaque payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunCapabilityError {
    kind: RunCapabilityErrorKind,
}

impl RunCapabilityError {
    fn new(kind: RunCapabilityErrorKind) -> Self {
        Self { kind }
    }

    /// Return the fixed-cardinality failure classification.
    pub fn kind(self) -> RunCapabilityErrorKind {
        self.kind
    }
}

impl fmt::Display for RunCapabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            RunCapabilityErrorKind::UnsealedActivation => {
                "runtime capabilities require a sealed activation inventory"
            }
            RunCapabilityErrorKind::UnsupportedCredentialRequirements => {
                "credential-bearing activation requires an explicit supported profile preflight"
            }
            RunCapabilityErrorKind::GroupInventoryMismatch => {
                "admitted activation groups do not match the compiled inventory"
            }
            RunCapabilityErrorKind::CapacityMismatch => {
                "admitted group capacity does not match the compiled requirement"
            }
            RunCapabilityErrorKind::SourceInventoryMismatch => {
                "admitted Source capabilities do not match the compiled group"
            }
            RunCapabilityErrorKind::CredentialInventoryMismatch => {
                "admitted credential requirements do not match the compiled group"
            }
            RunCapabilityErrorKind::ReservationFailed => {
                "complete activation-group capacity is unavailable"
            }
            RunCapabilityErrorKind::ActivationPlanMismatch => {
                "admitted runtime capabilities do not belong to this compiled plan"
            }
            RunCapabilityErrorKind::GroupUnavailable => {
                "the activation-group capability was already consumed or is unavailable"
            }
            RunCapabilityErrorKind::SourceUnavailable => {
                "the Source capability was already consumed or is unavailable"
            }
            RunCapabilityErrorKind::OpenFailed => {
                "an admitted Source capability could not be opened"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RunCapabilityError {}

/// One compiled Source identity paired with its opaque, single-use opener.
pub struct AdmittedSourceOpener {
    instance: CompiledSourceInstanceId,
    opener: Option<Box<dyn CapabilityOpener>>,
}

impl AdmittedSourceOpener {
    /// Pair one compiled Source identity with its admitted opener.
    pub fn new(instance: CompiledSourceInstanceId, opener: Box<dyn CapabilityOpener>) -> Self {
        Self {
            instance,
            opener: Some(opener),
        }
    }
}

/// Unsealed input for one complete activation group.
///
/// Constructing this value performs no reservation or open operation. Only
/// [`AdmittedRunCapabilities::admit`] can validate and seal it.
pub struct AdmittedActivationGroup {
    id: SourceActivationGroupId,
    capacity: SourceActivationCapacity,
    credential_requirement_ids: Box<[CredentialRequirementName]>,
    sources: Vec<AdmittedSourceOpener>,
    reservation: Option<Box<dyn GroupCapacityReservation>>,
}

impl AdmittedActivationGroup {
    /// Describe one group without performing its capacity reservation.
    pub fn new(
        id: SourceActivationGroupId,
        capacity: SourceActivationCapacity,
        sources: Vec<AdmittedSourceOpener>,
        reservation: Box<dyn GroupCapacityReservation>,
    ) -> Self {
        Self::with_credentials(id, capacity, Vec::new(), sources, reservation)
    }

    /// Describe a credential-bearing group without exposing provider state.
    pub fn with_credentials(
        id: SourceActivationGroupId,
        capacity: SourceActivationCapacity,
        credential_requirement_ids: Vec<CredentialRequirementName>,
        sources: Vec<AdmittedSourceOpener>,
        reservation: Box<dyn GroupCapacityReservation>,
    ) -> Self {
        Self {
            id,
            capacity,
            credential_requirement_ids: credential_requirement_ids.into_boxed_slice(),
            sources,
            reservation: Some(reservation),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct GroupContract {
    id: SourceActivationGroupId,
    capacity: SourceActivationCapacity,
    members: Box<[CompiledSourceInstanceId]>,
    credential_requirement_ids: Box<[CredentialRequirementName]>,
}

impl GroupContract {
    fn from_plan(plan: &SourceActivationPlan) -> Box<[Self]> {
        plan.groups()
            .iter()
            .map(|group| Self {
                id: group.id(),
                capacity: group.capacity(),
                members: group.members().into(),
                credential_requirement_ids: group.credential_requirement_ids().into(),
            })
            .collect()
    }
}

struct OwnedActivationGroup {
    contract: GroupContract,
    sources: Vec<AdmittedSourceOpener>,
    capacity_lease: Option<Box<dyn GroupCapacityLease>>,
}

impl Drop for OwnedActivationGroup {
    fn drop(&mut self) {
        while let Some(source) = self.sources.pop() {
            drop(source);
        }
        drop(self.capacity_lease.take());
    }
}

impl OwnedActivationGroup {
    fn into_active(mut self) -> ActiveActivationGroup {
        ActiveActivationGroup {
            contract: self.contract.clone(),
            sources: std::mem::take(&mut self.sources),
            sessions: Vec::new(),
            capacity_lease: self.capacity_lease.take(),
        }
    }
}

struct GroupSlot {
    contract: GroupContract,
    owned: Option<OwnedActivationGroup>,
}

/// Sealed, move-only capabilities admitted for exactly one compiled plan.
///
/// This value is intentionally neither `Clone` nor serializable. Its `Debug`
/// implementation renders counts only, never opaque factories, sessions,
/// logical requirements, or compiled identities. It retains a fixed amount of
/// configuration-derived state per compiled Source and activation group.
pub struct AdmittedRunCapabilities {
    contract: Box<[GroupContract]>,
    groups: Vec<GroupSlot>,
}

impl fmt::Debug for AdmittedRunCapabilities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmittedRunCapabilities")
            .field("group_count", &self.group_count())
            .field("remaining_group_count", &self.remaining_group_count())
            .finish()
    }
}

impl AdmittedRunCapabilities {
    /// Validate a complete inventory, then reserve every group atomically.
    ///
    /// Inventory or capacity mismatches invoke no reservation and no opener.
    /// Reservation failure drops all earlier guards in reverse order. On
    /// success this bundle becomes the sole owner of every guard and opener.
    pub fn admit(
        activation: &SourceActivationPlan,
        groups: Vec<AdmittedActivationGroup>,
    ) -> Result<Self, RunCapabilityError> {
        if !activation.is_sealed() {
            return Err(RunCapabilityError::new(
                RunCapabilityErrorKind::UnsealedActivation,
            ));
        }

        let contract = GroupContract::from_plan(activation);
        if groups.len() != contract.len() {
            return Err(RunCapabilityError::new(
                RunCapabilityErrorKind::GroupInventoryMismatch,
            ));
        }

        let mut by_id = BTreeMap::new();
        for group in groups {
            let id = group.id;
            if by_id.insert(id, group).is_some() {
                return Err(RunCapabilityError::new(
                    RunCapabilityErrorKind::GroupInventoryMismatch,
                ));
            }
        }

        for expected in &contract {
            let Some(group) = by_id.get(&expected.id) else {
                return Err(RunCapabilityError::new(
                    RunCapabilityErrorKind::GroupInventoryMismatch,
                ));
            };
            if group.capacity != expected.capacity {
                return Err(RunCapabilityError::new(
                    RunCapabilityErrorKind::CapacityMismatch,
                ));
            }
            let members: Box<[_]> = group.sources.iter().map(|source| source.instance).collect();
            if members != expected.members {
                return Err(RunCapabilityError::new(
                    RunCapabilityErrorKind::SourceInventoryMismatch,
                ));
            }
            let unique_members: BTreeSet<_> = members.iter().copied().collect();
            if unique_members.len() != members.len() {
                return Err(RunCapabilityError::new(
                    RunCapabilityErrorKind::SourceInventoryMismatch,
                ));
            }
            if group.credential_requirement_ids.as_ref()
                != expected.credential_requirement_ids.as_ref()
            {
                return Err(RunCapabilityError::new(
                    RunCapabilityErrorKind::CredentialInventoryMismatch,
                ));
            }
        }

        let mut owned = Vec::with_capacity(contract.len());
        for expected in &contract {
            let mut group = by_id
                .remove(&expected.id)
                .expect("complete inventory was validated above");
            let reservation = group
                .reservation
                .take()
                .expect("unsealed group always retains its reservation");
            let capacity_lease = match reservation.reserve() {
                Ok(lease) => lease,
                Err(_) => {
                    while let Some(prior) = owned.pop() {
                        drop(prior);
                    }
                    return Err(RunCapabilityError::new(
                        RunCapabilityErrorKind::ReservationFailed,
                    ));
                }
            };
            owned.push(OwnedActivationGroup {
                contract: expected.clone(),
                sources: group.sources,
                capacity_lease: Some(capacity_lease),
            });
        }

        let groups = contract
            .iter()
            .cloned()
            .zip(owned)
            .map(|(contract, owned)| GroupSlot {
                contract,
                owned: Some(owned),
            })
            .collect();
        Ok(Self { contract, groups })
    }

    /// Admit the current direct-source runtime when no credentials are needed.
    ///
    /// This is a real fail-closed admission path, not a credential selector.
    /// Any logical credential requirement or credential-handle capacity rejects
    /// the run before a reservation or opener can occur.
    pub fn uncredentialed(activation: &SourceActivationPlan) -> Result<Self, RunCapabilityError> {
        if !activation.is_sealed() {
            return Err(RunCapabilityError::new(
                RunCapabilityErrorKind::UnsealedActivation,
            ));
        }
        if !activation.credential_requirement_ids().is_empty()
            || activation.groups().iter().any(|group| {
                !group.credential_requirement_ids().is_empty()
                    || group.capacity().credential_handle_units() != 0
            })
        {
            return Err(RunCapabilityError::new(
                RunCapabilityErrorKind::UnsupportedCredentialRequirements,
            ));
        }

        let groups = activation
            .groups()
            .iter()
            .map(|group| {
                AdmittedActivationGroup::new(
                    group.id(),
                    group.capacity(),
                    group
                        .members()
                        .iter()
                        .copied()
                        .map(|member| {
                            AdmittedSourceOpener::new(member, Box::new(UncredentialedOpener))
                        })
                        .collect(),
                    Box::new(UncredentialedReservation),
                )
            })
            .collect();
        Self::admit(activation, groups)
    }

    /// Total number of sealed activation groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Number of group leases not yet transferred to an active group.
    pub fn remaining_group_count(&self) -> usize {
        self.groups
            .iter()
            .filter(|slot| slot.owned.is_some())
            .count()
    }

    /// Transfer one group lease and its Source factories exactly once.
    pub fn take_group(
        &mut self,
        id: SourceActivationGroupId,
    ) -> Result<ActiveActivationGroup, RunCapabilityError> {
        let slot = self
            .groups
            .iter_mut()
            .find(|slot| slot.contract.id == id)
            .ok_or_else(|| RunCapabilityError::new(RunCapabilityErrorKind::GroupUnavailable))?;
        let owned = slot
            .owned
            .take()
            .ok_or_else(|| RunCapabilityError::new(RunCapabilityErrorKind::GroupUnavailable))?;
        Ok(owned.into_active())
    }

    pub(crate) fn ensure_matches(
        &self,
        activation: &SourceActivationPlan,
    ) -> Result<(), RunCapabilityError> {
        if activation.is_sealed() && self.contract == GroupContract::from_plan(activation) {
            Ok(())
        } else {
            Err(RunCapabilityError::new(
                RunCapabilityErrorKind::ActivationPlanMismatch,
            ))
        }
    }
}

impl Drop for AdmittedRunCapabilities {
    fn drop(&mut self) {
        while let Some(mut slot) = self.groups.pop() {
            drop(slot.owned.take());
        }
    }
}

/// One transferred activation group and every session opened beneath it.
///
/// The value is move-only. Dropping it closes sessions and unopened factories
/// in reverse order, then releases the group-capacity lease.
pub struct ActiveActivationGroup {
    contract: GroupContract,
    sources: Vec<AdmittedSourceOpener>,
    sessions: Vec<Box<dyn CapabilitySession>>,
    capacity_lease: Option<Box<dyn GroupCapacityLease>>,
}

impl fmt::Debug for ActiveActivationGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveActivationGroup")
            .field("source_count", &self.contract.members.len())
            .field("opened_session_count", &self.sessions.len())
            .finish()
    }
}

impl ActiveActivationGroup {
    /// Return the exact checked capacity retained by this group lease.
    pub fn capacity(&self) -> SourceActivationCapacity {
        self.contract.capacity
    }

    /// Consume one Source opener and retain its opaque session in this group.
    pub fn open(&mut self, instance: CompiledSourceInstanceId) -> Result<(), RunCapabilityError> {
        let source = self
            .sources
            .iter_mut()
            .find(|source| source.instance == instance)
            .ok_or_else(|| RunCapabilityError::new(RunCapabilityErrorKind::SourceUnavailable))?;
        let opener = source
            .opener
            .take()
            .ok_or_else(|| RunCapabilityError::new(RunCapabilityErrorKind::SourceUnavailable))?;
        let session = opener
            .open()
            .map_err(|_| RunCapabilityError::new(RunCapabilityErrorKind::OpenFailed))?;
        self.sessions.push(session);
        Ok(())
    }
}

impl Drop for ActiveActivationGroup {
    fn drop(&mut self) {
        while let Some(session) = self.sessions.pop() {
            drop(session);
        }
        while let Some(source) = self.sources.pop() {
            drop(source);
        }
        drop(self.capacity_lease.take());
    }
}

struct UncredentialedReservation;

impl GroupCapacityReservation for UncredentialedReservation {
    fn reserve(self: Box<Self>) -> Result<Box<dyn GroupCapacityLease>, CapabilityReservationError> {
        Ok(Box::new(UncredentialedCapacityLease))
    }
}

struct UncredentialedCapacityLease;

impl GroupCapacityLease for UncredentialedCapacityLease {}

struct UncredentialedOpener;

impl CapabilityOpener for UncredentialedOpener {
    fn open(self: Box<Self>) -> Result<Box<dyn CapabilitySession>, CapabilityOpenError> {
        Ok(Box::new(UncredentialedSession))
    }
}

struct UncredentialedSession;

impl CapabilitySession for UncredentialedSession {}
