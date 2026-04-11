//! Unit tests for the composition type system and Phase 1 loader.
//!
//! 16c.1.1 tests live here; 16c.1.2 / 16c.1.4 add more as those tasks land.
//! The integration test file `tests/composition_loader_test.rs` hosts
//! fixture-driven tests that need on-disk `.comp.yaml` files.

use super::*;
use crate::span::{FileId, Span};
use indexmap::IndexMap;
use std::num::NonZeroU32;
use std::path::PathBuf;

fn dummy_span() -> Span {
    Span {
        file: FileId::new(NonZeroU32::new(1).unwrap()),
        start: 0,
        len: 0,
    }
}

fn dummy_signature(name: &str) -> CompositionSignature {
    CompositionSignature {
        name: name.to_string(),
        inputs: IndexMap::new(),
        outputs: IndexMap::new(),
        config_schema: IndexMap::new(),
        resources_schema: IndexMap::new(),
        source_path: PathBuf::from(format!("{name}.comp.yaml")),
        source_spans: IndexMap::new(),
    }
}

/// Gate test for 16c.1.2 unlock: `CompositionSymbolTable` is keyed by
/// `PathBuf` and supports insert / retrieve.
#[test]
fn test_composition_signature_is_indexmap_keyed() {
    let mut table: CompositionSymbolTable = IndexMap::new();
    let key = PathBuf::from("compositions/customer_enrich.comp.yaml");
    let sig = dummy_signature("customer_enrich");

    table.insert(key.clone(), sig);

    let retrieved = table
        .get(&key)
        .expect("entry must be retrievable by PathBuf");
    assert_eq!(retrieved.name, "customer_enrich");
    assert_eq!(table.len(), 1);
}

/// Gate test for 16c.1.2 unlock: `ParamDecl` has a readable `required: bool`
/// field. Deliberately constructs the struct with `required: true` to lock
/// in the field name and type.
#[test]
fn test_param_decl_required_field_present() {
    let decl = ParamDecl {
        param_type: ParamType::Float,
        required: true,
        default: Some(serde_json::json!(0.85)),
        enum_values: None,
        range: Some((0.0, 1.0)),
        description: Some("fuzzy match threshold".into()),
        span: dummy_span(),
    };

    assert!(decl.required);
    assert_eq!(decl.param_type, ParamType::Float);
    assert_eq!(decl.range, Some((0.0, 1.0)));
}
