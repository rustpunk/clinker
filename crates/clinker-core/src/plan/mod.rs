pub mod bind_schema;
pub mod combine;
pub mod compiled;
pub mod composition_body;
pub mod deferred_region;
pub mod execution;
pub mod explain_provenance;
pub mod extraction;
pub mod index;
pub mod properties;
pub mod row_type;

pub use compiled::{ChannelIdentity, CompiledPlan};
pub use composition_body::{BoundBody, CompositionBodyId};
pub use row_type::{ColumnLookup, QualifiedField, Row, RowTail, TailVarId};

pub use properties::{
    NodeProperties, Ordering, OrderingProvenance, Partitioning, PartitioningKind,
    PartitioningProvenance,
};

#[cfg(test)]
mod tests;
