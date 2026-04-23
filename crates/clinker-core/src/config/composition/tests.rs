//! Unit tests for the composition type system and loader.
//!
//! The integration test file `tests/composition_loader_test.rs` hosts
//! fixture-driven tests that need on-disk `.comp.yaml` files.

use super::*;
use crate::span::{FileId, Span};
use crate::yaml;
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

/// Verifies `CompositionSymbolTable` is keyed by
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

/// Verifies `ParamDecl` has a readable `required: bool`
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

// ---------------------------------------------------------------------
// CompositionFile deserialization tests
// ---------------------------------------------------------------------

fn dummy_file_id() -> FileId {
    FileId::new(NonZeroU32::new(1).unwrap())
}

/// Minimal `.comp.yaml` that exercises every deserialization branch.
const WELL_FORMED_YAML: &str = "\
_compose:
  name: customer_enrich
  inputs:
    customers:
      schema:
        - { name: customer_id, type: string }
        - { name: email, type: string }
      description: 'raw customer rows'
      required: true
    addresses:
      required: true
  outputs:
    enriched: final_stage.out
    rejected:
      ref: addr_filter.reject
      description: 'rows dropped by the address filter'
  config_schema:
    fuzzy_threshold:
      type: float
      default: 0.85
      range: [0.0, 1.0]
      description: 'fuzzy match threshold'
    mode:
      type: string
      required: true
      enum: [strict, lenient]
  resources_schema:
    lookup_table:
      kind: file
      required: true

nodes: []
";

#[test]
fn test_composition_file_deserializes_well_formed() {
    let parsed = CompositionFile::parse(
        WELL_FORMED_YAML,
        dummy_file_id(),
        PathBuf::from("customer_enrich.comp.yaml"),
    )
    .expect("well-formed .comp.yaml must parse");

    let sig = &parsed.signature;
    assert_eq!(sig.name, "customer_enrich");
    assert_eq!(sig.inputs.len(), 2);
    assert_eq!(sig.outputs.len(), 2);
    assert_eq!(sig.config_schema.len(), 2);
    assert_eq!(sig.resources_schema.len(), 1);

    let customers = sig.inputs.get("customers").expect("customers input");
    assert!(customers.required);
    let schema = customers
        .schema
        .as_ref()
        .expect("customers schema should parse");
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "customer_id");
    assert_eq!(schema.columns[1].name, "email");

    let addresses = sig.inputs.get("addresses").expect("addresses input");
    assert!(addresses.schema.is_none(), "absent schema = accept-any");

    let fuzzy = sig
        .config_schema
        .get("fuzzy_threshold")
        .expect("fuzzy_threshold param");
    assert_eq!(fuzzy.param_type, ParamType::Float);
    assert!(!fuzzy.required);
    assert_eq!(fuzzy.default, Some(serde_json::json!(0.85)));
    assert_eq!(fuzzy.range, Some((0.0, 1.0)));

    let mode = sig.config_schema.get("mode").expect("mode param");
    assert_eq!(mode.param_type, ParamType::String);
    assert!(mode.required);
    assert_eq!(
        mode.enum_values,
        Some(vec![
            serde_json::json!("strict"),
            serde_json::json!("lenient")
        ])
    );

    let lookup = sig
        .resources_schema
        .get("lookup_table")
        .expect("lookup_table resource");
    assert_eq!(lookup.kind, ResourceKind::File);
    assert!(lookup.required);

    assert!(parsed.nodes.is_empty(), "nodes: [] should round-trip");
}

#[test]
fn test_composition_file_rejects_unknown_param_type() {
    let yaml = "\
_compose:
  name: bad
  config_schema:
    x:
      type: unknown_type
nodes: []
";
    let err = CompositionFile::parse(yaml, dummy_file_id(), PathBuf::from("bad.comp.yaml"))
        .expect_err("unknown param type must be rejected");
    // serde-saphyr's unknown-variant error surfaces the name; verify it
    // mentions the offending token so the diagnostic is useful.
    let msg = err.to_string();
    assert!(
        msg.contains("unknown_type") || msg.contains("unknown variant"),
        "error should mention the unknown variant; got: {msg}"
    );
}

#[test]
fn test_output_alias_short_form_parses() {
    let yaml = "\
_compose:
  name: short_form
  outputs:
    enriched: final_stage.out
nodes: []
";
    let parsed = CompositionFile::parse(yaml, dummy_file_id(), PathBuf::from("s.comp.yaml"))
        .expect("short-form output alias must parse");

    let alias = parsed
        .signature
        .outputs
        .get("enriched")
        .expect("enriched output");
    assert_eq!(alias.internal_ref.value, "final_stage.out");
    assert!(alias.description.is_none());
}

#[test]
fn test_output_alias_long_form_parses() {
    let yaml = "\
_compose:
  name: long_form
  outputs:
    enriched:
      ref: final_stage.out
      description: 'final output after enrichment'
nodes: []
";
    let parsed = CompositionFile::parse(yaml, dummy_file_id(), PathBuf::from("l.comp.yaml"))
        .expect("long-form output alias must parse");

    let alias = parsed
        .signature
        .outputs
        .get("enriched")
        .expect("enriched output");
    assert_eq!(alias.internal_ref.value, "final_stage.out");
    assert_eq!(
        alias.description.as_deref(),
        Some("final output after enrichment")
    );
}

// ---------------------------------------------------------------------
// Resource enum tests
// ---------------------------------------------------------------------

#[test]
fn test_resource_file_variant_deserializes() {
    let yaml = "kind: file\npath: data/a.csv\n";
    let resource: Resource = yaml::from_str(yaml).expect("Resource::File must deserialize");
    match &resource {
        Resource::File { path, .. } => {
            assert_eq!(path, &PathBuf::from("data/a.csv"));
        }
    }
}

/// Compile-time gate: exhaustive match on both `Resource` and `ResourceKind`
/// with no wildcard arm. If a variant is added to one but not the other,
/// this test fails to compile.
#[test]
fn test_resource_kind_covers_all_resource_variants() {
    let resource = Resource::File {
        path: PathBuf::from("x.csv"),
        span: dummy_span(),
    };

    // Exhaustive match on Resource → derive the expected ResourceKind.
    let expected_kind = match &resource {
        Resource::File { .. } => ResourceKind::File,
    };

    // Exhaustive match on ResourceKind → verify round-trip.
    match expected_kind {
        ResourceKind::File => {}
    }

    // Also verify the .kind() helper agrees.
    assert_eq!(resource.kind(), expected_kind);
}
