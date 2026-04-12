pub mod bind_schema;
pub mod bound_schemas;
pub mod compiled;
pub mod composition_body;
pub mod execution;
pub mod explain_provenance;
pub mod extraction;
pub mod index;
pub mod properties;
pub mod row_type;

pub use compiled::{ChannelIdentity, CompiledPlan};
pub use composition_body::{BoundBody, CompositionBodyId};
pub use row_type::{ColumnLookup, Row, RowTail, TailVarId};

pub use properties::{
    NodeProperties, Ordering, OrderingProvenance, Partitioning, PartitioningKind,
    PartitioningProvenance,
};

#[cfg(test)]
mod tests;
