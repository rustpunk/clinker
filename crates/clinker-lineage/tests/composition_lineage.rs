//! Column lineage traces precisely through composition boundaries.
//!
//! Compositions are referenced by an on-disk `use:` path, so these tests compile
//! inline parent pipelines against fixture `.comp.yaml` bodies under
//! `tests/fixtures/compositions/`. Like the unit tests in `builder.rs`, no source
//! data is read — lineage is derived statically from the compiled plan — so the
//! `data/src.csv` paths need not exist.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clinker_lineage::{
    DatasetId, FieldLineage, InputField, OutputColumnLineage, PlanColumnLineage, Transformation,
    TransformationSubtype, TransformationType, column_lineage,
};
use clinker_plan::CompileContext;
use clinker_plan::config::parse_config;
use clinker_plan::plan::CompiledPlan;

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

/// Compile an inline parent pipeline, resolving composition `use:` paths against
/// `tests/fixtures/pipelines/` (so `../compositions/x.comp.yaml` lands in
/// `tests/fixtures/compositions/`).
fn compile_fixture(yaml: &str) -> CompiledPlan {
    parse_config(yaml)
        .expect("parse_config")
        .compile(&CompileContext::with_pipeline_dir(
            fixtures_root(),
            "pipelines",
        ))
        .expect("compile should succeed")
}

fn lineage_of(yaml: &str) -> PlanColumnLineage {
    column_lineage(&compile_fixture(yaml), &fixtures_root())
}

/// The deterministic `file:` terminal name a source `path: <rel>` resolves to
/// under `fixtures_root()`, mirroring `dataset::absolutize`.
fn file_dataset(rel: &str) -> String {
    fixtures_root()
        .join(rel)
        .to_string_lossy()
        .replace('\\', "/")
        .trim_end_matches('/')
        .to_string()
}

fn src_name() -> String {
    file_dataset("data/src.csv")
}

fn direct(name: &str, field: &str, subtype: TransformationSubtype) -> InputField {
    InputField {
        namespace: "file".to_string(),
        name: name.to_string(),
        field: field.to_string(),
        transformations: vec![Transformation {
            transformation_type: TransformationType::Direct,
            subtype: Some(subtype),
            description: None,
            masking: None,
        }],
    }
}

fn indirect(name: &str, field: &str, subtypes: &[TransformationSubtype]) -> InputField {
    InputField {
        namespace: "file".to_string(),
        name: name.to_string(),
        field: field.to_string(),
        transformations: subtypes
            .iter()
            .map(|s| Transformation {
                transformation_type: TransformationType::Indirect,
                subtype: Some(*s),
                description: None,
                masking: None,
            })
            .collect(),
    }
}

fn only_output(lineage: &PlanColumnLineage) -> &OutputColumnLineage {
    assert_eq!(
        lineage.outputs.len(),
        1,
        "expected exactly one output dataset"
    );
    &lineage.outputs[0]
}

fn assert_field(fields: &BTreeMap<String, FieldLineage>, col: &str, expected: &[InputField]) {
    let actual = fields
        .get(col)
        .unwrap_or_else(|| panic!("column {col:?} missing from lineage"));
    assert_eq!(actual.input_fields, expected, "lineage for column {col:?}");
}

/// A single-boundary composition whose body renames a port column. The renamed
/// output column must resolve to the TRUE source column (not the coarse
/// all-to-all fan-out the opaque approximation produced), and the placeholder
/// input-port Source must not leak as a phantom input.
#[test]
fn lineage_traces_through_a_composition_to_the_true_source_column() {
    let yaml = r#"
pipeline: { name: rename_test }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/src.csv
      schema:
        - { name: customer_id, type: string }
        - { name: name, type: string }
  - type: composition
    name: comp
    input: src
    use: ../compositions/rename_id.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: comp
    config: { name: out, type: csv, path: out/rename.csv }
"#;
    let lineage = lineage_of(yaml);
    let src = src_name();
    let out = only_output(&lineage);
    let fields = &out.facet.fields;

    use TransformationSubtype::Identity;
    // The rename resolves to its true source column — exact match rejects any
    // fan-out to the sibling `name` column.
    assert_field(fields, "x", &[direct(&src, "customer_id", Identity)]);
    // Open-row passthrough through the port keeps each column's own source.
    assert_field(
        fields,
        "customer_id",
        &[direct(&src, "customer_id", Identity)],
    );
    assert_field(fields, "name", &[direct(&src, "name", Identity)]);
    assert_eq!(
        fields.len(),
        3,
        "no extra/omitted columns across the boundary"
    );

    // No filter/join/group inside the body → no INDIRECT influence.
    assert!(
        out.facet.dataset.is_empty(),
        "no INDIRECT influence expected, got {:?}",
        out.facet.dataset
    );
    // The bound input-port Source is seeded and skipped, so it never becomes a
    // phantom `clinker:<port>` input — only the real source is reported.
    assert_eq!(
        lineage.inputs,
        vec![DatasetId {
            namespace: "file".to_string(),
            name: src.clone(),
        }],
        "only the real source should be an input"
    );
}

/// A composition that calls another composition: lineage must resolve across
/// both stitched boundaries back to the real source column.
#[test]
fn lineage_traces_through_nested_compositions() {
    let yaml = r#"
pipeline: { name: nested_test }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/src.csv
      schema:
        - { name: customer_id, type: string }
  - type: composition
    name: top
    input: src
    use: ../compositions/outer.comp.yaml
    inputs:
      o_in: src
  - type: output
    name: out
    input: top
    config: { name: out, type: csv, path: out/nested.csv }
"#;
    let lineage = lineage_of(yaml);
    let src = src_name();
    let out = only_output(&lineage);
    let fields = &out.facet.fields;

    use TransformationSubtype::Identity;
    // `y` (outer) = `z` (inner) = `customer_id` (source), through two boundaries.
    assert_field(fields, "y", &[direct(&src, "customer_id", Identity)]);
    assert_field(fields, "z", &[direct(&src, "customer_id", Identity)]);
    // Open-row passthrough carries the source column across both boundaries.
    assert_field(
        fields,
        "customer_id",
        &[direct(&src, "customer_id", Identity)],
    );
    assert_eq!(
        fields.len(),
        3,
        "no extra/dropped columns across two boundaries"
    );
    // The novel double-recursion seed loop must not leak a phantom
    // `clinker:o_in`/`clinker:i_in` input at either boundary.
    assert_eq!(
        lineage.inputs,
        vec![DatasetId {
            namespace: "file".to_string(),
            name: src.clone(),
        }],
        "only the real source should be an input across nested boundaries"
    );
    assert!(
        out.facet.dataset.is_empty(),
        "no INDIRECT influence expected"
    );
}

/// An aggregate inside a composition body: the DIRECT subtypes cross the boundary
/// (sum -> AGGREGATION, group key -> IDENTITY) and the in-body GROUP BY surfaces
/// as INDIRECT (GROUP_BY) influence on the composition output.
#[test]
fn in_body_group_by_surfaces_as_indirect_influence() {
    let yaml = r#"
pipeline: { name: agg_test }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/src.csv
      schema:
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: composition
    name: comp
    input: src
    use: ../compositions/agg_in_body.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: comp
    config: { name: out, type: csv, path: out/agg.csv }
"#;
    let lineage = lineage_of(yaml);
    let src = src_name();
    let out = only_output(&lineage);
    let fields = &out.facet.fields;

    use TransformationSubtype::{Aggregation, GroupBy, Identity};
    assert_field(
        fields,
        "department",
        &[direct(&src, "department", Identity)],
    );
    assert_field(fields, "total", &[direct(&src, "amount", Aggregation)]);

    assert!(
        out.facet
            .dataset
            .contains(&indirect(&src, "department", &[GroupBy])),
        "expected in-body group-by to surface as INDIRECT influence, got {:?}",
        out.facet.dataset
    );
}

/// A body with TWO input ports fed by TWO different parents, joined inside the
/// body. Exercises the multi-port seeding loop: each port's synthetic Source must
/// be seeded from its OWN parent (no cross-wiring), each output column must
/// resolve to the correct side, both real sources must appear as inputs (no
/// phantom port inputs), and the in-body join keys must surface as INDIRECT.
#[test]
fn lineage_traces_each_input_port_to_its_own_parent() {
    let yaml = r#"
pipeline: { name: join_ports_test }
nodes:
  - type: source
    name: orders_src
    config:
      name: orders_src
      type: csv
      path: data/orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: quantity, type: int }
  - type: source
    name: products_src
    config:
      name: products_src
      type: csv
      path: data/products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: price, type: float }
  - type: composition
    name: comp
    input: orders_src
    use: ../compositions/join_ports.comp.yaml
    inputs:
      orders: orders_src
      products: products_src
  - type: output
    name: out
    input: comp
    config: { name: out, type: csv, path: out/join.csv }
"#;
    let lineage = lineage_of(yaml);
    let orders = file_dataset("data/orders.csv");
    let products = file_dataset("data/products.csv");
    let out = only_output(&lineage);
    let fields = &out.facet.fields;

    use TransformationSubtype::{Identity, Transformation};
    // Each output column resolves to the column on its OWN side of the join.
    assert_field(fields, "order_id", &[direct(&orders, "order_id", Identity)]);
    assert_field(
        fields,
        "product_name",
        &[direct(&products, "name", Identity)],
    );
    // A cross-side computed column draws from both parents (terminals ordered by
    // namespace/name/field: orders.csv < products.csv).
    assert_field(
        fields,
        "total",
        &[
            direct(&orders, "quantity", Transformation),
            direct(&products, "price", Transformation),
        ],
    );

    // Both real sources are inputs and nothing else — no phantom
    // `clinker:orders`/`clinker:products` port inputs. Order-independent: the
    // topological order of two independent roots is not contractually fixed.
    assert_eq!(lineage.inputs.len(), 2, "exactly two real source inputs");
    for name in [&orders, &products] {
        assert!(
            lineage.inputs.contains(&DatasetId {
                namespace: "file".to_string(),
                name: name.clone(),
            }),
            "missing real source input {name:?}; got {:?}",
            lineage.inputs
        );
    }
    // The in-body join key on each side surfaces as INDIRECT (JOIN) influence.
    use TransformationSubtype::Join;
    assert!(
        out.facet
            .dataset
            .contains(&indirect(&orders, "product_id", &[Join])),
        "expected orders join-key as INDIRECT JOIN, got {:?}",
        out.facet.dataset
    );
    assert!(
        out.facet
            .dataset
            .contains(&indirect(&products, "product_id", &[Join])),
        "expected products join-key as INDIRECT JOIN, got {:?}",
        out.facet.dataset
    );
}

/// A `$doc` envelope read inside a composition body must attribute to the REAL
/// upstream source feeding the port — the source whose envelope declares the
/// section — not the synthetic port Source. Exercises the doc-source seeding of
/// the body's input-port Source from the parent producer.
#[test]
fn doc_read_inside_a_composition_body_attributes_to_the_feeding_source() {
    let yaml = r#"
pipeline: { name: doc_comp_test }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: xml
      glob: data/*.xml
      options: { record_path: doc/records/record }
      envelope:
        sections:
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
            fields:
              batch_id: string
      schema:
        - { name: amount, type: int }
  - type: composition
    name: comp
    input: src
    use: ../compositions/doc_read.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: comp
    config: { name: out, type: csv, path: out/doc_comp.csv }
"#;
    let lineage = lineage_of(yaml);
    // The glob `data/*.xml` resolves to the directory dataset `<root>/data`.
    let src = file_dataset("data");
    let out = only_output(&lineage);
    let fields = &out.facet.fields;

    use TransformationSubtype::Identity;
    // The in-body `$doc` read resolves across the port boundary to the real
    // source, carrying the rendered envelope path as its `field`.
    assert_field(
        fields,
        "batch",
        &[direct(&src, "$doc.BatchInfo.batch_id", Identity)],
    );
    // Open-row passthrough of the port column keeps its own source.
    assert_field(fields, "amount", &[direct(&src, "amount", Identity)]);

    // Only the real source is an input — the seeded port Source is skipped, so
    // no phantom `clinker:inp` input leaks.
    assert_eq!(
        lineage.inputs,
        vec![DatasetId {
            namespace: "file".to_string(),
            name: src.clone(),
        }],
        "only the real envelope source should be an input"
    );
}
