//! Provenance tracking for resolved configuration values (Phase 16c.3).
//!
//! Each config value in a compiled pipeline carries a [`ResolvedValue`] wrapper
//! that records which configuration layer (composition default, channel default,
//! channel fixed, inspector edit) contributed the winning value. The provenance
//! chain is stored in a side-table [`ProvenanceDb`], separate from
//! [`CompiledPlan`](crate::plan::compiled::CompiledPlan).
