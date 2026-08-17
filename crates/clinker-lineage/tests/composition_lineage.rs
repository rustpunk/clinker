//! Column lineage traces precisely through composition boundaries.
//!
//! Compositions are referenced by an on-disk `use:` path, so these tests compile
//! inline parent pipelines against fixture `.comp.yaml` bodies under
//! `tests/fixtures/compositions/`. Like the unit tests in `builder.rs`, no source
//! data is read — lineage is derived statically from the compiled plan. Direct
//! source paths need not exist; catalog-backed body Source descriptors do,
//! because catalog admission validates their workspace targets.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clinker_lineage::{
    DatasetId, FieldLineage, InputField, OutputColumnLineage, PlanColumnLineage, Transformation,
    TransformationSubtype, TransformationType, column_lineage_local_diagnostic_paths,
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
    column_lineage_local_diagnostic_paths(&compile_fixture(yaml), &fixtures_root())
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

fn resource_direct(name: &str, field: &str, subtype: TransformationSubtype) -> InputField {
    let mut field = direct(name, field, subtype);
    field.namespace = "clinker-resource:file".to_string();
    field
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

#[test]
fn nested_value_and_structural_reads_keep_distinct_lineage_roles() {
    let yaml = r#"
pipeline: { name: nested_roles }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/src.csv
      schema:
        - { name: value_field, type: string }
        - { name: key_field, type: string }
        - { name: shared, type: string }
        - { name: predicate_field, type: bool }
  - type: transform
    name: construct
    input: src
    config:
      cxl: |
        emit payload = {
          static: value_field,
          [key_field]: [entry for entry in [shared] if predicate_field],
        }
  - type: output
    name: out
    input: construct
    config: { name: out, type: json, path: out/nested-roles.json, include_unmapped: false }
"#;
    let lineage = lineage_of(yaml);
    let src = src_name();
    let out = only_output(&lineage);

    use TransformationSubtype::{Conditional, Filter, Transformation};
    assert_field(
        &out.facet.fields,
        "payload",
        &[
            direct(&src, "shared", Transformation),
            direct(&src, "value_field", Transformation),
        ],
    );
    assert_eq!(
        out.facet.dataset,
        vec![
            indirect(&src, "key_field", &[Conditional]),
            indirect(&src, "predicate_field", &[Filter]),
            indirect(&src, "shared", &[Filter]),
        ],
        "computed keys and comprehension selection affect structure/membership without becoming value edges",
    );
}

#[test]
fn nested_static_keys_add_no_dependency_and_literal_items_stay_direct() {
    let yaml = r#"
pipeline: { name: nested_static_keys }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/src.csv
      schema:
        - { name: first_value, type: string }
        - { name: second_value, type: string }
  - type: transform
    name: construct
    input: src
    config:
      cxl: 'emit payload = [first_value, {static: second_value}]'
  - type: output
    name: out
    input: construct
    config: { name: out, type: json, path: out/nested-static.json, include_unmapped: false }
"#;
    let lineage = lineage_of(yaml);
    let src = src_name();
    let out = only_output(&lineage);

    use TransformationSubtype::Transformation;
    assert_field(
        &out.facet.fields,
        "payload",
        &[
            direct(&src, "first_value", Transformation),
            direct(&src, "second_value", Transformation),
        ],
    );
    assert!(
        out.facet.dataset.is_empty(),
        "authored static key text carries no source-field dependency",
    );
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

/// A composition body may declare its own source. It reads a file nothing at
/// the call site feeds, so it is a logical dataset in its own right — not one
/// the call site's identity could stand in for — and under external identity
/// mode the author has to bind it like any other.
///
/// It is bound by its call-site path, `<composition>.<source>`, because a body
/// carries its own node-name space: the pipeline below has a top-level `ref`
/// and a body `ref`, and a bare key would hand both the same identity and
/// attribute one's columns to the other. That path is also what a channel
/// `sources:` patch uses to reach the same source.
mod body_declared_source {
    use super::*;

    use clinker_lineage::column_lineage_external;
    use clinker_lineage::logical_identity::{
        ExternalDatasetIdentity, LineageIdentityContext, LineageIdentityError, LineageNodeBinding,
    };

    const PIPELINE: &str = r#"
pipeline: { name: body_source_identity }
nodes:
  - type: source
    name: ref
    config:
      name: ref
      type: csv
      path: data/top.csv
      schema: [{ name: x, type: int }]
  - type: composition
    name: enrich
    input: ref
    use: ../compositions/own_source.comp.yaml
    inputs:
      driver: ref
    resources: { reference: reference_codes }
  - type: output
    name: out
    input: enrich
    config: { name: out, type: csv, path: out/out.csv }
"#;

    fn binding(node: &str, name: &str) -> LineageNodeBinding {
        LineageNodeBinding::new(
            node,
            ExternalDatasetIdentity::catalog("analytics", name).expect("catalog identity"),
        )
    }

    /// Preflight enumerates the body, so an unbound body source is refused
    /// before the run starts and the diagnostic names the key that fixes it.
    #[test]
    fn an_unbound_body_source_is_named_at_plan_time() {
        let identities = LineageIdentityContext::external([
            binding("ref", "top_customers"),
            binding("out", "enriched"),
        ])
        .expect("complete top-level context");

        let err = column_lineage_external(&compile_fixture(PIPELINE), &identities)
            .expect_err("a body-declared source needs its own identity");
        assert_eq!(
            err,
            LineageIdentityError::MissingNode {
                node: "enrich.ref".to_string()
            },
            "the diagnostic must name the call-site path, which the author can bind"
        );
    }

    /// Binding that key resolves it, and the body source stays a distinct
    /// dataset from the identically-named top-level node.
    #[test]
    fn a_bound_body_source_is_its_own_dataset() {
        let identities = LineageIdentityContext::external([
            binding("ref", "top_customers"),
            binding("enrich.ref", "reference_codes"),
            binding("out", "enriched"),
        ])
        .expect("complete context");

        let lineage = column_lineage_external(&compile_fixture(PIPELINE), &identities)
            .expect("every dataset node is bound");

        let names: Vec<&str> = lineage.inputs.iter().map(|id| id.name.as_str()).collect();
        assert!(
            names.contains(&"top_customers") && names.contains(&"reference_codes"),
            "both sources are declared inputs, under their own identities: {names:?}"
        );
        assert_eq!(
            lineage.outputs[0].dataset.name, "enriched",
            "the sink keeps its own binding"
        );

        // The output column the body produces derives from the body source's
        // column, so the edge lands on the body source's identity.
        let label = lineage.outputs[0]
            .facet
            .fields
            .get("label")
            .expect("the body's emitted column reaches the sink");
        assert_eq!(
            label
                .input_fields
                .iter()
                .map(|f| (f.name.as_str(), f.field.as_str()))
                .collect::<Vec<_>>(),
            vec![("reference_codes", "code")]
        );
    }

    /// A top-level node named `enrich.ref` once compiled: `.` was reserved in a
    /// transform, aggregate, and route name, but not in a source, output, or
    /// composition one. Joining a call site to a body node with a bare `.`
    /// therefore gave that node and the body source `ref` under composition
    /// `enrich` one key. Nothing detected it — the required keys are collected
    /// into a set, and the duplicate check guards bindings rather than nodes —
    /// so one binding answered for both, and the run published a single dataset
    /// as both an input and an output, each carrying the other's column edges.
    ///
    /// Two changes closed it, and this test pins the outer one: the key join
    /// escapes a name's own `.`, which makes the collision unrepresentable, and
    /// the planner now refuses a `.` in any node name, which makes it
    /// unreachable. Lineage therefore never receives this pipeline at all. The
    /// escape stays covered where it can still be exercised — directly, in
    /// `builder`'s `distinct_node_paths_get_distinct_identity_keys` — so it
    /// remains a safety net rather than the only thing standing between the
    /// engine and a silently merged dataset. Should the naming rule ever be
    /// relaxed, this test fails first and points at the escape.
    #[test]
    fn a_dotted_node_name_does_not_collide_with_a_call_site_path() {
        const COLLIDING: &str = r#"
pipeline: { name: dotted_output_name }
nodes:
  - type: source
    name: drive
    config:
      name: drive
      type: csv
      path: data/top.csv
      schema: [{ name: x, type: int }]
  - type: composition
    name: enrich
    input: drive
    use: ../compositions/own_source.comp.yaml
    inputs:
      driver: drive
    resources: { reference: reference_codes }
  - type: output
    name: enrich.ref
    input: enrich
    config: { name: enrich.ref, type: csv, path: out/out.csv }
"#;

        let diags = parse_config(COLLIDING)
            .expect("the colliding pipeline is well-formed YAML")
            .compile(&CompileContext::with_pipeline_dir(
                fixtures_root(),
                "pipelines",
            ))
            .expect_err("a node named for a call-site path must not compile");

        let refusal = diags
            .iter()
            .find(|d| d.code == "E010")
            .unwrap_or_else(|| panic!("expected E010 for the dotted output; got: {diags:?}"));
        assert!(
            refusal.message.contains("enrich.ref") && refusal.message.contains("enrich_ref"),
            "the refusal names the output and the name it should take instead: {:?}",
            refusal.message
        );
    }

    /// A binding key is capped at 128 bytes, and the cap is applied to a key
    /// composed from names that carry no cap of their own. A long call-site
    /// name over a body node name therefore produced a key no binding could
    /// spell — and the run reported it as a *missing* binding, telling the
    /// author to add one that both the identity context and the configuration
    /// boundary refuse. The diagnostic asked for something the tool would not
    /// accept.
    ///
    /// The cap stays, because it is the same bound the configuration boundary
    /// applies and a binding key has to fit through both. What changes is the
    /// diagnostic: it names the constraint and leaves the author the correction
    /// that works — rename the pipeline nodes the key is composed from.
    #[test]
    fn a_composed_key_over_the_cap_names_a_correction_the_author_can_make() {
        let call_site = "e".repeat(130);
        let pipeline = format!(
            r#"
pipeline: {{ name: long_call_site }}
nodes:
  - type: source
    name: drive
    config:
      name: drive
      type: csv
      path: data/top.csv
      schema: [{{ name: x, type: int }}]
  - type: composition
    name: {call_site}
    input: drive
    use: ../compositions/own_source.comp.yaml
    inputs:
      driver: drive
    resources: {{ reference: reference_codes }}
  - type: output
    name: out
    input: {call_site}
    config: {{ name: out, type: csv, path: out/out.csv }}
"#
        );

        let identities = LineageIdentityContext::external([
            binding("drive", "driver_rows"),
            binding("out", "published_report"),
        ])
        .expect("both bindable nodes are bound");

        let err = column_lineage_external(&compile_fixture(&pipeline), &identities)
            .expect_err("the body source's key cannot be bound");
        let expected_key = format!("{call_site}.ref");
        assert_eq!(
            err,
            LineageIdentityError::UnbindableNode {
                node: expected_key.clone(),
                bytes: expected_key.len(),
            }
        );

        // The key the author would have to write is refused by the same bound,
        // which is why asking for it would have been a dead end.
        assert_eq!(
            LineageIdentityContext::external([binding(&expected_key, "reference_codes")])
                .expect_err("a key over the cap is not a binding key"),
            LineageIdentityError::InvalidNode {
                node: expected_key.clone()
            }
        );

        let diagnostic = err.to_string();
        assert!(
            diagnostic.contains("rename those pipeline nodes"),
            "the correction has to be one the author can carry out: {diagnostic}"
        );
        assert!(
            !diagnostic.contains("add one canonical datasource"),
            "and must not be the one that asks for a refused binding: {diagnostic}"
        );
    }

    /// The same pipeline under local diagnostic paths — the mode that already
    /// walked body sources — resolves both to distinct datasets using the
    /// direct top-level path and compiled catalog identity respectively.
    /// External mode now reaches the same node set rather than aborting.
    #[test]
    fn local_diagnostic_paths_resolve_the_same_node_set() {
        let lineage = lineage_of(PIPELINE);
        let names: Vec<&str> = lineage.inputs.iter().map(|id| id.name.as_str()).collect();
        assert!(
            names.contains(&file_dataset("data/top.csv").as_str())
                && names.contains(&"reference_codes"),
            "both sources are inputs under their compiled identities: {names:?}"
        );
    }
}

/// Whether a source splits per record type is decided from the resolved
/// schemas the plan retains, and that table is built from the top-level
/// `nodes:` list alone. A body source shares the identity key space with every
/// other lookup in the walk, so it never reads a top-level entry — and a body
/// source of its own has no entry to read.
mod multi_record_schemas_are_a_top_level_table {
    use super::*;

    /// The declaration a top-level `ref` carries: two record types, one of
    /// which declares `code` — the column the body's own `ref` declares too.
    const TOP_LEVEL_MULTI_RECORD: &str = r#"
pipeline: { name: shadowed_multi_record }
nodes:
  - type: source
    name: ref
    config:
      name: ref
      type: fixed_width
      path: data/payments.txt
      schema:
        discriminator: { start: 0, width: 1 }
        records:
          - id: header
            tag: H
            columns:
              - { name: batch_id, type: string, start: 1, width: 9 }
          - id: detail
            tag: D
            columns:
              - { name: code, type: string, start: 1, width: 4 }
  - type: transform
    name: drive
    input: ref
    config:
      cxl: |
        emit x = 1
  - type: composition
    name: enrich
    input: drive
    use: ../compositions/own_source.comp.yaml
    inputs:
      driver: drive
    resources: { reference: reference_codes }
  - type: output
    name: out
    input: enrich
    config: { name: out, type: csv, path: out/out.csv }
"#;

    /// A body source named the same as a top-level multi-record source is a
    /// plain single-schema CSV. Reading the top-level declaration for it would
    /// split the body's dataset by record types it does not have and attribute
    /// its `code` column to `<body dataset>#detail` — a dataset no source in
    /// the plan declares, and one that would be read as a record type of the
    /// top-level file.
    #[test]
    fn a_body_source_does_not_inherit_a_top_level_declaration() {
        let lineage = lineage_of(TOP_LEVEL_MULTI_RECORD);
        let names: Vec<&str> = lineage.inputs.iter().map(|id| id.name.as_str()).collect();

        let body = "reference_codes".to_string();
        assert!(
            names.contains(&body.as_str()),
            "the body source is an input under its catalog identity: {names:?}"
        );
        assert!(
            !names
                .iter()
                .any(|name| name.starts_with(&body) && *name != body),
            "the body source declares one flat schema, so it has no record-type \
             datasets: {names:?}"
        );

        // The top-level source keeps its own split, so the fix is a key
        // correction rather than a disabling of the split.
        let top = file_dataset("data/payments.txt");
        assert!(
            names.contains(&format!("{top}#header").as_str())
                && names.contains(&format!("{top}#detail").as_str()),
            "the top-level multi-record source still splits: {names:?}"
        );

        assert_field(
            &only_output(&lineage).facet.fields,
            "label",
            &[resource_direct(
                &body,
                "code",
                TransformationSubtype::Identity,
            )],
        );
    }

    /// A multi-record source declared inside a body keeps every column on its
    /// catalog resource's container dataset. The lineage schema lookup is
    /// top-level-only, so there is nothing to split by — the builder's
    /// documented limitation. The columns still land on a dataset the run
    /// declares as an input, which is what a consumer resolves a column edge
    /// against.
    #[test]
    fn a_body_multi_record_source_is_attributed_to_its_container() {
        let lineage = lineage_of(
            r#"
pipeline: { name: body_multi_record }
nodes:
  - type: source
    name: drv
    config:
      name: drv
      type: csv
      path: data/src.csv
      schema: [{ name: x, type: int }]
  - type: composition
    name: enrich
    input: drv
    use: ../compositions/own_multi_record.comp.yaml
    inputs:
      driver: drv
    resources: { ledger: body_ledger }
  - type: output
    name: out
    input: enrich
    config: { name: out, type: csv, path: out/out.csv }
"#,
        );

        let ledger = "body_ledger".to_string();
        let names: Vec<&str> = lineage.inputs.iter().map(|id| id.name.as_str()).collect();
        assert!(
            names.contains(&ledger.as_str()),
            "the body source is an input: {names:?}"
        );
        assert!(
            !names
                .iter()
                .any(|name| name.starts_with(&ledger) && *name != ledger),
            "no record-type dataset is derived for a body source: {names:?}"
        );

        let fields = &only_output(&lineage).facet.fields;
        assert_field(
            fields,
            "batch",
            &[resource_direct(
                &ledger,
                "batch_id",
                TransformationSubtype::Identity,
            )],
        );
        assert_field(
            fields,
            "total",
            &[resource_direct(
                &ledger,
                "amount",
                TransformationSubtype::Identity,
            )],
        );
    }
}
