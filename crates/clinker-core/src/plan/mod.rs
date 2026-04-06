pub mod execution;
pub mod index;
pub mod properties;

pub use properties::{
    NodeProperties, Ordering, OrderingProvenance, Partitioning, PartitioningKind,
    PartitioningProvenance,
};

#[cfg(test)]
mod tests;
