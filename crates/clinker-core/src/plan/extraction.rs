//! Extract-as-composition boundary analysis and YAML serialization (Phase 16c.6).
//!
//! Provides [`analyze_extraction_boundary`] to identify crossing edges when
//! extracting a subgraph into a composition, and [`write_extracted_composition`]
//! to serialize the result as a `.comp.yaml` file.
