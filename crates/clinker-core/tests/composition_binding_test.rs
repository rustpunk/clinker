use clinker_core::CompositionBodyId;
use clinker_core::plan::bind_schema::CompileArtifacts;

#[test]
fn scaffold_compiles() {
    // Verifies the composition_binding_test integration test module is
    // discoverable and the plan::composition_body module exists.
    let _ = std::any::type_name::<clinker_core::plan::compiled::CompiledPlan>();
}

#[test]
fn test_compile_artifacts_fresh_body_id_monotonic() {
    let mut artifacts = CompileArtifacts::default();
    let id0 = artifacts.fresh_body_id();
    let id1 = artifacts.fresh_body_id();
    assert_eq!(id0, CompositionBodyId(0));
    assert_eq!(id1, CompositionBodyId(1));
    assert_ne!(id0, id1);
}

#[test]
fn test_compile_artifacts_insert_body_and_lookup() {
    use clinker_core::BoundBody;
    use indexmap::IndexMap;

    let mut artifacts = CompileArtifacts::default();
    let id = artifacts.fresh_body_id();

    let body = BoundBody {
        signature_path: "compositions/test.comp.yaml".into(),
        nodes: vec![],
        bound_schemas: Default::default(),
        output_port_rows: IndexMap::new(),
        input_port_rows: IndexMap::new(),
        nested_body_ids: vec![],
    };

    artifacts.insert_body(id, body.clone());

    let looked_up = artifacts.body_of(id).expect("body should be present");
    assert_eq!(looked_up.signature_path, body.signature_path);
}

#[test]
fn test_compile_artifacts_body_of_unknown_returns_none() {
    let artifacts = CompileArtifacts::default();
    assert!(artifacts.body_of(CompositionBodyId(999)).is_none());
}

#[test]
fn test_pipeline_node_composition_serde_skips_body_field() {
    // A `.yaml` `type: composition` block deserializes without a `body:`
    // key present — serde-default sentinel is used.
    let yaml = r#"
- name: enrich
  type: composition
  input: source_node
  use: ./enrich.comp.yaml
  inputs:
    data: source_node
"#;
    let nodes: Vec<clinker_core::yaml::Spanned<clinker_core::config::pipeline_node::PipelineNode>> =
        serde_saphyr::from_str(yaml).expect("should deserialize");

    assert_eq!(nodes.len(), 1);
    match &nodes[0].value {
        clinker_core::config::pipeline_node::PipelineNode::Composition { body, .. } => {
            // Sentinel default — bind_composition has not run.
            assert_eq!(*body, CompositionBodyId::SENTINEL);
        }
        other => panic!("expected Composition, got {:?}", other.type_tag()),
    }
}
