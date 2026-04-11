//! Unit tests for the composition type system and Phase 1 loader.
//!
//! Scaffolded in Task 16c.1.0. Tests land in subsequent tasks:
//! - 16c.1.1: `test_composition_signature_is_indexmap_keyed`, `test_param_decl_required_field_present`
//! - 16c.1.2: `test_composition_file_deserializes_well_formed` and OutputAlias/ParamType tests
//! - 16c.1.4: `test_open_tail_schema_*` tests
//!
//! The integration test file `tests/composition_loader_test.rs` hosts fixture-driven
//! tests that need on-disk `.comp.yaml` files.
