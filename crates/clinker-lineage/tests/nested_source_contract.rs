//! Plan-time counterpart of the executed native multi-file cardinality fixture.
//! Structural path selection reads element names, not a column predicate.

use clinker_lineage::{TransformationType, column_lineage_local_diagnostic_paths};
use clinker_plan::{CompileContext, config::parse_config};

#[test]
fn nested_source_selection_preserves_direct_dataset_and_field_mapping() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for format in ["json", "xml"] {
        let selection = if format == "json" {
            "items"
        } else {
            "Root/items/row"
        };
        let yaml = format!(
            r#"
pipeline:
  name: selected_native_rows
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: {format}
      path: input.{format}
      options: {{ record_path: {selection} }}
      schema:
        - {{ name: id, type: {{ nullable: int }} }}
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: json
      path: output.json
      options: {{ format: ndjson }}
"#
        );
        let plan = parse_config(&yaml)
            .unwrap()
            .compile(&CompileContext::new(root))
            .unwrap();
        let lineage = column_lineage_local_diagnostic_paths(&plan, root);
        assert_eq!(lineage.inputs.len(), 1);
        assert_eq!(lineage.inputs[0].namespace, "file");
        assert!(
            lineage.inputs[0]
                .name
                .ends_with(&format!("/input.{format}"))
        );
        assert_eq!(lineage.outputs.len(), 1);
        let output = &lineage.outputs[0];
        assert_eq!(output.dataset.namespace, "file");
        assert!(output.dataset.name.ends_with("/output.json"));
        assert!(
            output.facet.dataset.is_empty(),
            "structural element selection reads no field value"
        );
        assert_eq!(output.facet.fields.len(), 1);
        let inputs = &output.facet.fields["id"].input_fields;
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].namespace, lineage.inputs[0].namespace);
        assert_eq!(inputs[0].name, lineage.inputs[0].name);
        assert_eq!(inputs[0].field, "id");
        assert!(!inputs[0].transformations.is_empty());
        assert!(
            inputs[0]
                .transformations
                .iter()
                .all(|t| t.transformation_type == TransformationType::Direct)
        );
    }
}
