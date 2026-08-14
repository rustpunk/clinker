//! OpenLineage emission, present or absent.
//!
//! The `lineage` feature decides which half of this module the run gets.
//! [`emit`] is the real thing; [`absent`] is the shape it leaves behind, so
//! that a build without it still reads a live output the same way rather than
//! carrying a `#[cfg]` at every place a terminal is emitted or a sink closed.

#[cfg(not(feature = "lineage"))]
mod absent;
#[cfg(feature = "lineage")]
mod emit;

#[cfg(not(feature = "lineage"))]
pub(crate) use absent::*;
#[cfg(feature = "lineage")]
pub(crate) use emit::*;
