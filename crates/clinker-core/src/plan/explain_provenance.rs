//! Field-level provenance rendering for `clinker explain --field` (Phase 16c.6).
//!
//! Given a dotted path like `node_name.param_name`, walks the [`ProvenanceDb`]
//! to produce a human-readable provenance chain showing which configuration
//! layer won and which were shadowed.
