use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use clinker_lineage::dataset::resource_dataset_identity;
use clinker_lineage::logical_identity::{
    DatasetIdentifierType, DatasetSubset, ExternalDatasetIdentity, LineageIdentityContext,
    LineageIdentityError, LineageNodeBinding, SymlinkIdentifier,
};
use clinker_lineage::{
    Job, RunLifecycleFacts, RunLifecycleStartFacts, RunLifecycleTerminalFacts, RunStats, Terminal,
    column_lineage_external, run_events,
};
use clinker_plan::CompileContext;
use clinker_plan::config::parse_config;
use clinker_plan::resources::{
    CatalogConfig, CatalogResourceConfig, FileResourceAccess, LogicalResourceId, WorkspaceCatalog,
};

static NEXT_WORKSPACE: AtomicU64 = AtomicU64::new(0);

struct TestWorkspace {
    path: PathBuf,
}

impl TestWorkspace {
    fn new(label: &str) -> Self {
        let sequence = NEXT_WORKSPACE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "clinker-lineage-{label}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("unique test workspace");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).expect("remove test workspace");
    }
}

fn lifecycle_facts() -> RunLifecycleFacts {
    RunLifecycleFacts {
        start: RunLifecycleStartFacts {
            batch_id: "catalog-export".to_owned(),
            execution_id: "019c8e3e-7029-75a0-bc68-b60c36b7ef42".to_owned(),
            plan_fingerprint_algorithm: "blake3".to_owned(),
            plan_fingerprint_version: 1,
            plan_fingerprint_digest: "00".repeat(32),
            event_time: "2026-08-06T12:00:00Z".to_owned(),
        },
        terminal: RunLifecycleTerminalFacts {
            event_time: "2026-08-06T12:00:00Z".to_owned(),
            outcome: Terminal::Complete,
            stats: Some(RunStats::default()),
        },
    }
}

#[test]
fn resource_kind_file_has_stable_logical_dataset_identity() {
    let workspace = TestWorkspace::new("stable-resource");
    std::fs::write(workspace.path().join("orders.csv"), "id\n1\n").expect("resource file");
    let config = CatalogConfig {
        resources: std::collections::BTreeMap::from([(
            "shared.orders".to_string(),
            CatalogResourceConfig::File {
                path: "orders.csv".into(),
                access: FileResourceAccess::Read,
            },
        )]),
        ..CatalogConfig::default()
    };
    let catalog = WorkspaceCatalog::load(workspace.path(), &config).expect("catalog");
    let id = LogicalResourceId::parse("shared.orders").expect("logical id");
    let dataset = resource_dataset_identity(catalog.resolve_resource(&id).expect("resource"))
        .expect("file resource is external");
    assert_eq!(dataset.namespace, "clinker-resource:file");
    assert_eq!(dataset.name, "shared.orders");
}

#[test]
fn resource_kind_identity_omits_descriptor_and_secret_material() {
    let first = TestWorkspace::new("first-resource");
    let second = TestWorkspace::new("second-resource");
    std::fs::write(first.path().join("first.csv"), "id\n1\n").expect("first file");
    std::fs::write(second.path().join("second.csv"), "id\n2\n").expect("second file");
    let build = |root: &std::path::Path, path: &str| {
        let config = CatalogConfig {
            resources: std::collections::BTreeMap::from([(
                "shared.orders".to_string(),
                CatalogResourceConfig::File {
                    path: path.into(),
                    access: FileResourceAccess::Read,
                },
            )]),
            ..CatalogConfig::default()
        };
        let catalog = WorkspaceCatalog::load(root, &config).expect("catalog");
        let id = LogicalResourceId::parse("shared.orders").expect("logical id");
        resource_dataset_identity(catalog.resolve_resource(&id).expect("resource"))
            .expect("external identity")
    };
    let first = build(first.path(), "first.csv");
    let second = build(second.path(), "second.csv");
    assert_eq!(first, second, "physical relocation is not identity");
    let rendered = format!("{first:?}");
    assert!(!rendered.contains("first.csv"));
    assert!(!rendered.contains("credential"));
}

#[test]
fn canonical_catalog_subset_and_symlinks() {
    let canonical = ExternalDatasetIdentity::canonical("s3://warehouse/customers")
        .expect("canonical datasource identity");
    assert_eq!(canonical.dataset_id().namespace, "s3://warehouse");
    assert_eq!(canonical.dataset_id().name, "customers");

    let catalog =
        ExternalDatasetIdentity::catalog("analytics", "customers_clean").expect("catalog identity");
    assert_eq!(catalog.dataset_id().namespace, "analytics");
    assert_eq!(catalog.dataset_id().name, "customers_clean");

    let subset =
        DatasetSubset::input("partition=2026-08-06").expect("stable logical partition identifier");
    let alias = SymlinkIdentifier::new(
        "snowflake://account/database",
        "PUBLIC.CUSTOMERS",
        DatasetIdentifierType::Table,
    )
    .expect("authorized catalog alias");
    let context = LineageIdentityContext::external([
        LineageNodeBinding::new("source_customers", canonical)
            .with_subset(subset.clone())
            .with_symlink(alias.clone()),
        LineageNodeBinding::new("output_customers", catalog),
    ])
    .expect("complete unambiguous context");

    let source = context
        .require("source_customers")
        .expect("source identity is present");
    assert_eq!(source.subsets(), std::slice::from_ref(&subset));
    assert_eq!(source.symlinks(), std::slice::from_ref(&alias));

    let compiled = parse_config(
        r#"
pipeline: { name: stable_identity }
nodes:
  - type: source
    name: source_customers
    config:
      name: source_customers
      type: csv
      glob: incoming/customers/*.csv
      schema: [{ name: id, type: int }]
  - type: sink
    name: output_customers
    input: source_customers
    config: { name: output_customers, type: csv, path: out/customers.csv }
"#,
    )
    .unwrap()
    .compile(&CompileContext::default())
    .unwrap();
    let lineage = column_lineage_external(&compiled, &context).unwrap();
    assert_eq!(lineage.inputs, vec![source.dataset_id().clone()]);
    assert_eq!(
        lineage.outputs[0].dataset,
        context
            .require("output_customers")
            .unwrap()
            .dataset_id()
            .clone()
    );
    let input_facets = lineage
        .input_identity_facets
        .get(source.dataset_id())
        .expect("authorized input facts follow the stable dataset");
    assert_eq!(input_facets.subsets(), std::slice::from_ref(&subset));
    assert_eq!(input_facets.symlinks(), std::slice::from_ref(&alias));
    assert!(lineage.outputs[0].identity_facets.symlinks().is_empty());

    let relocated = LineageIdentityContext::external([
        LineageNodeBinding::new(
            "source_customers",
            ExternalDatasetIdentity::canonical("s3://warehouse/customers").unwrap(),
        )
        .with_subset(DatasetSubset::input("partition=2026-08-06").unwrap())
        .with_symlink(
            SymlinkIdentifier::new(
                "snowflake://account/database",
                "PUBLIC.CUSTOMERS",
                DatasetIdentifierType::Table,
            )
            .unwrap(),
        ),
        LineageNodeBinding::new(
            "output_customers",
            ExternalDatasetIdentity::catalog("analytics", "customers_clean").unwrap(),
        ),
    ])
    .unwrap();
    assert_eq!(
        context, relocated,
        "physical relocation cannot affect identity"
    );

    let duplicate = LineageIdentityContext::external([
        LineageNodeBinding::new(
            "source_customers",
            ExternalDatasetIdentity::catalog("analytics", "customers").unwrap(),
        ),
        LineageNodeBinding::new(
            "source_customers",
            ExternalDatasetIdentity::catalog("analytics", "customers_copy").unwrap(),
        ),
    ])
    .expect_err("duplicate logical node binding must fail");
    assert!(matches!(
        duplicate,
        LineageIdentityError::DuplicateNode { .. }
    ));

    let missing = context
        .validate_required(["source_customers", "output_customers", "audit"])
        .expect_err("missing binding must fail");
    assert!(matches!(missing, LineageIdentityError::MissingNode { .. }));

    assert!(ExternalDatasetIdentity::canonical("warehouse-customers").is_err());
    assert!(ExternalDatasetIdentity::catalog("analytics", "").is_err());
    assert!(DatasetSubset::input("/worker-17/tmp/customers.csv").is_err());
    assert!(DatasetSubset::output("attempt-42/customers.csv").is_err());
}

#[test]
fn serialized_standard_facets() {
    let source_alias = SymlinkIdentifier::new(
        "snowflake://account/database",
        "PUBLIC.CUSTOMERS",
        DatasetIdentifierType::Table,
    )
    .expect("authorized source alias");
    let output_alias = SymlinkIdentifier::new(
        "s3://published",
        "customers/current",
        DatasetIdentifierType::Location,
    )
    .expect("authorized output alias");
    let identities = LineageIdentityContext::external([
        LineageNodeBinding::new(
            "source_customers",
            ExternalDatasetIdentity::canonical("s3://warehouse/customers").unwrap(),
        )
        .with_subset(DatasetSubset::input("partition=2026-08-06").unwrap())
        .with_subset(DatasetSubset::input("partition=2026-08-05").unwrap())
        .with_symlink(source_alias),
        LineageNodeBinding::new(
            "output_customers",
            ExternalDatasetIdentity::catalog("analytics", "customers_clean").unwrap(),
        )
        .with_subset(DatasetSubset::output("release=current").unwrap())
        .with_symlink(output_alias),
    ])
    .expect("complete external identities");

    let compiled = parse_config(
        r#"
pipeline: { name: serialized_identity }
nodes:
  - type: source
    name: source_customers
    config:
      name: source_customers
      type: csv
      path: /worker-a/incoming/customers.csv
      schema: [{ name: id, type: int }]
  - type: sink
    name: output_customers
    input: source_customers
    config: { name: output_customers, type: csv, path: /worker-a/out/customers.csv }
"#,
    )
    .unwrap()
    .compile(&CompileContext {
        allow_absolute_paths: true,
        ..CompileContext::default()
    })
    .unwrap();
    let lineage = column_lineage_external(&compiled, &identities).unwrap();
    let events = run_events(
        &lineage,
        Job::for_pipeline("serialized_identity", "00".repeat(32)),
        &lifecycle_facts(),
    );
    let complete = serde_json::to_value(&events[1]).expect("serialize COMPLETE event");

    // The subset facet's schema type is an `InputDatasetFacet`, which the core
    // spec admits only under `inputFacets`; symlinks is a plain dataset facet
    // and belongs to the dataset itself, in either position.
    assert_eq!(
        complete["inputs"][0],
        serde_json::json!({
            "namespace": "s3://warehouse",
            "name": "customers",
            "inputFacets": {
                "subset": {
                    "_producer": "https://github.com/rustpunk/clinker",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/InputSubsetInputDatasetFacet",
                    "inputCondition": {
                        "type": "location",
                        "locations": ["partition=2026-08-05", "partition=2026-08-06"]
                    }
                }
            },
            "facets": {
                "symlinks": {
                    "_producer": "https://github.com/rustpunk/clinker",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json",
                    "identifiers": [{
                        "namespace": "snowflake://account/database",
                        "name": "PUBLIC.CUSTOMERS",
                        "type": "TABLE"
                    }]
                }
            }
        })
    );
    assert_eq!(
        complete["outputs"][0]["namespace"],
        serde_json::json!("analytics")
    );
    assert_eq!(
        complete["outputs"][0]["name"],
        serde_json::json!("customers_clean")
    );
    assert_eq!(
        complete["outputs"][0]["outputFacets"]["subset"],
        serde_json::json!({
            "_producer": "https://github.com/rustpunk/clinker",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/OutputSubsetOutputDatasetFacet",
            "outputCondition": {
                "type": "location",
                "locations": ["release=current"]
            }
        })
    );
    assert!(
        complete["outputs"][0]["facets"].get("subset").is_none(),
        "an output-position facet must not ride in the dataset's own facets"
    );
    assert_eq!(
        complete["outputs"][0]["facets"]["symlinks"]["identifiers"],
        serde_json::json!([{
            "namespace": "s3://published",
            "name": "customers/current",
            "type": "LOCATION"
        }])
    );
    assert!(!complete.to_string().contains("/worker-a/"));

    let relocated = parse_config(
        r#"
pipeline: { name: serialized_identity }
nodes:
  - type: source
    name: source_customers
    config:
      name: source_customers
      type: csv
      path: /different-worker/input.csv
      schema: [{ name: id, type: int }]
  - type: sink
    name: output_customers
    input: source_customers
    config: { name: output_customers, type: csv, path: /different-worker/output.csv }
"#,
    )
    .unwrap()
    .compile(&CompileContext {
        allow_absolute_paths: true,
        ..CompileContext::default()
    })
    .unwrap();
    let relocated = column_lineage_external(&relocated, &identities).unwrap();
    let relocated_events = run_events(
        &relocated,
        Job::for_pipeline("serialized_identity", "00".repeat(32)),
        &lifecycle_facts(),
    );
    assert_eq!(
        serde_json::to_value(&events).unwrap(),
        serde_json::to_value(&relocated_events).unwrap(),
        "physical relocation cannot alter external lineage bytes"
    );
}

/// A multi-record flat file splits into one logical dataset per record type,
/// and those datasets inherit the source's *bound* identity — so an externally
/// bound source names `{canonical-namespace}/{canonical-name}#<id>` and no
/// worker path reaches the wire. Record types differ in their columns, so they
/// are distinct datasets rather than subsets of the container; the standard
/// subset facet selects rows from one fixed schema and cannot express this.
#[test]
fn multi_record_types_are_datasets_on_the_bound_canonical_identity() {
    let identities = LineageIdentityContext::external([
        LineageNodeBinding::new(
            "payments",
            ExternalDatasetIdentity::canonical("s3://payments-lake/raw/payments").unwrap(),
        ),
        LineageNodeBinding::new(
            "out",
            ExternalDatasetIdentity::catalog("analytics", "payments_flat").unwrap(),
        ),
    ])
    .expect("complete context");

    let compiled = parse_config(
        r#"
pipeline: { name: mr_external }
nodes:
  - type: source
    name: payments
    config:
      name: payments
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
              - { name: amount, type: int, start: 1, width: 4 }
  - type: transform
    name: project
    input: payments
    config:
      cxl: |
        emit kind = record_type
        emit batch_id = batch_id
        emit amount = amount
  - type: sink
    name: out
    input: project
    config: { name: out, type: csv, path: out/out.csv }
"#,
    )
    .unwrap()
    .compile(&CompileContext::default())
    .unwrap();

    let lineage = column_lineage_external(&compiled, &identities).unwrap();

    let base = identities.require("payments").unwrap().dataset_id().clone();
    let names: Vec<(&str, &str)> = lineage
        .inputs
        .iter()
        .map(|id| (id.namespace.as_str(), id.name.as_str()))
        .collect();
    assert_eq!(
        names,
        vec![
            ("s3://payments-lake", "raw/payments"),
            ("s3://payments-lake", "raw/payments#header"),
            ("s3://payments-lake", "raw/payments#detail"),
        ],
        "base collection then each record type, in declaration order"
    );

    // The source path `data/payments.txt` must not survive anywhere in the
    // emitted identities: that leakage is what canonical binding exists to stop.
    for id in &lineage.inputs {
        assert!(
            !id.name.contains("payments.txt") && !id.name.starts_with('/'),
            "worker path leaked into identity: {}:{}",
            id.namespace,
            id.name
        );
    }

    let fields = &lineage.outputs[0].facet.fields;
    let input_of = |col: &str| -> Vec<(String, String)> {
        fields
            .get(col)
            .unwrap_or_else(|| panic!("column {col:?} missing"))
            .input_fields
            .iter()
            .map(|f| (f.name.clone(), f.field.clone()))
            .collect()
    };
    assert_eq!(
        input_of("batch_id"),
        vec![("raw/payments#header".to_owned(), "batch_id".to_owned())]
    );
    assert_eq!(
        input_of("amount"),
        vec![("raw/payments#detail".to_owned(), "amount".to_owned())]
    );
    // The discriminator lead belongs to the container, not a record type.
    assert_eq!(
        input_of("kind"),
        vec![(base.name.clone(), "record_type".to_owned())]
    );
}

/// A pipeline node name carries no grammar: configuration checks node names
/// only for duplication, so a source named with a space and a transform named
/// with non-ASCII letters both compile and run. Binding is what makes those
/// pipelines emit lineage at all, so the binding key space has to admit every
/// name the engine does — otherwise a pipeline that runs cannot be exported,
/// and the author has nothing to correct.
#[test]
fn a_node_name_the_engine_accepts_can_be_bound() {
    let identities = LineageIdentityContext::external([
        LineageNodeBinding::new(
            "normalize orders",
            ExternalDatasetIdentity::canonical("s3://warehouse/orders").unwrap(),
        ),
        LineageNodeBinding::new(
            "récapitulatif",
            ExternalDatasetIdentity::catalog("analytics", "orders_summary").unwrap(),
        ),
    ])
    .expect("a space and a non-ASCII letter are both legal in a pipeline node name");

    let compiled = parse_config(
        r#"
pipeline: { name: odd_node_names }
nodes:
  - type: source
    name: normalize orders
    config:
      name: normalize orders
      type: csv
      path: data/orders.csv
      schema: [{ name: id, type: int }]
  - type: transform
    name: shape
    input: normalize orders
    config:
      cxl: |
        emit id = id
  - type: sink
    name: récapitulatif
    input: shape
    config: { name: récapitulatif, type: csv, path: out/summary.csv }
"#,
    )
    .unwrap()
    .compile(&CompileContext::default())
    .unwrap();

    let lineage = column_lineage_external(&compiled, &identities).unwrap();
    assert_eq!(
        lineage
            .inputs
            .iter()
            .map(|id| (id.namespace.as_str(), id.name.as_str()))
            .collect::<Vec<_>>(),
        vec![("s3://warehouse", "orders")]
    );
    assert_eq!(lineage.outputs[0].dataset.name, "orders_summary");
    assert_eq!(
        lineage.outputs[0]
            .facet
            .fields
            .get("id")
            .expect("the projected column reaches the sink")
            .input_fields
            .iter()
            .map(|f| (f.name.as_str(), f.field.as_str()))
            .collect::<Vec<_>>(),
        vec![("orders", "id")]
    );

    // The bound that stays is the one the configuration boundary applies, so a
    // key configuration would reject is still refused here — and the diagnostic
    // names the offending key rather than describing a grammar.
    for rejected in ["", " padded ", "line\nbreak", &"n".repeat(129)] {
        let err = LineageIdentityContext::external([LineageNodeBinding::new(
            rejected,
            ExternalDatasetIdentity::catalog("analytics", "orders").unwrap(),
        )])
        .expect_err("a key outside the retained bound must be refused");
        assert_eq!(
            err,
            LineageIdentityError::InvalidNode {
                node: rejected.to_owned()
            }
        );
    }
    assert!(
        LineageIdentityError::InvalidNode {
            node: " padded ".to_owned()
        }
        .to_string()
        .contains("\" padded \""),
        "the diagnostic must name the offending key"
    );
}

/// A per-record-type dataset name is its base name with the record type id
/// concatenated on, so an authored name carrying the separator can produce the
/// exact `{namespace, name}` pair one of those record types already occupies.
/// The two logical datasets would then merge in the catalogue and the column
/// edges of one would be read as the other's — a wrong attribution, which is
/// worse than a missing one because nothing in the event signals it.
///
/// The separator is therefore reserved in an authored *name*. Only the name:
/// a record type keeps its base's namespace and appends to the name alone, so a
/// separator in a namespace cannot compose into the ambiguous form.
#[test]
fn a_reserved_separator_in_an_authored_name_is_refused() {
    // The pair the rejected name would have produced is a real record-type
    // dataset of the pipeline below, bound canonically to `payments`.
    let identities = LineageIdentityContext::external([
        LineageNodeBinding::new(
            "payments",
            ExternalDatasetIdentity::catalog("analytics", "payments").unwrap(),
        ),
        LineageNodeBinding::new(
            "out",
            ExternalDatasetIdentity::catalog("analytics", "payments_flat").unwrap(),
        ),
    ])
    .expect("complete context");

    let compiled = parse_config(
        r#"
pipeline: { name: separator_collision }
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: fixed_width
      path: data/payments.txt
      schema:
        discriminator: { start: 0, width: 1 }
        records:
          - id: detail
            tag: D
            columns:
              - { name: amount, type: int, start: 1, width: 4 }
  - type: transform
    name: project
    input: payments
    config:
      cxl: |
        emit amount = amount
  - type: sink
    name: out
    input: project
    config: { name: out, type: csv, path: out/out.csv }
"#,
    )
    .unwrap()
    .compile(&CompileContext::default())
    .unwrap();
    let lineage = column_lineage_external(&compiled, &identities).unwrap();
    let record_type = lineage
        .inputs
        .iter()
        .find(|id| id.name.ends_with("detail"))
        .expect("the detail record type is its own dataset");
    assert_eq!(record_type.namespace, "analytics");
    assert_eq!(record_type.name, "payments#detail");

    // Authoring that same pair as a catalog identity is refused, so no second
    // node can be bound onto the record type's dataset.
    let collision = ExternalDatasetIdentity::catalog("analytics", "payments#detail")
        .expect_err("an authored name may not carry the reserved separator");
    assert_eq!(
        collision,
        LineageIdentityError::ReservedRecordTypeSeparator {
            field: "catalog_name"
        }
    );
    assert!(
        collision.to_string().contains("payments_detail"),
        "the diagnostic must offer a corrected form: {collision}"
    );

    // The same reservation applies to the name half of a canonical datasource.
    assert_eq!(
        ExternalDatasetIdentity::canonical("s3://payments-lake/raw/payments#detail")
            .expect_err("a canonical dataset name may not carry it either"),
        LineageIdentityError::ReservedRecordTypeSeparator {
            field: "canonical_datasource"
        }
    );

    // A namespace is not composed onto, so it is left alone — refusing it would
    // be a restriction with no collision behind it.
    assert!(ExternalDatasetIdentity::catalog("analytics#eu", "payments").is_ok());
    assert!(
        ExternalDatasetIdentity::canonical("s3://payments-lake/raw/payments").is_ok(),
        "a name without the separator is unaffected"
    );
}

/// Two nodes may name one external dataset — a binding is per node, and nothing
/// in configuration or in [`LineageIdentityContext::external`] requires two nodes
/// to carry different identities. The emitted document has one entry per dataset,
/// so that entry has to carry what every contributing node authorized: two
/// writers each producing one partition of a table produced both, and two shards
/// read from one collection were both read. Keeping the first contribution and
/// discarding the rest silently narrowed the run's claim to one node's members.
#[test]
fn nodes_sharing_one_dataset_contribute_every_authorized_fact() {
    let shard_a = LineageNodeBinding::new(
        "orders_eu",
        ExternalDatasetIdentity::catalog("analytics", "orders").unwrap(),
    )
    .with_subset(DatasetSubset::input("region=eu").unwrap())
    .with_symlink(
        SymlinkIdentifier::new("hive://cluster", "db.orders", DatasetIdentifierType::Table)
            .unwrap(),
    );
    // Deliberately overlapping: `db.orders` is authorized by both shards, and a
    // repeated identifier in the emitted facet would be a malformed document.
    let shard_b = LineageNodeBinding::new(
        "orders_us",
        ExternalDatasetIdentity::catalog("analytics", "orders").unwrap(),
    )
    .with_subset(DatasetSubset::input("region=us").unwrap())
    .with_symlink(
        SymlinkIdentifier::new("hive://cluster", "db.orders", DatasetIdentifierType::Table)
            .unwrap(),
    )
    .with_symlink(
        SymlinkIdentifier::new("s3://lake", "orders/", DatasetIdentifierType::Location).unwrap(),
    );
    let write_a = LineageNodeBinding::new(
        "daily_eu",
        ExternalDatasetIdentity::catalog("analytics", "orders_daily").unwrap(),
    )
    .with_subset(DatasetSubset::output("dt=2026-08-12/region=eu").unwrap())
    .with_symlink(
        SymlinkIdentifier::new(
            "hive://cluster",
            "db.orders_daily",
            DatasetIdentifierType::Table,
        )
        .unwrap(),
    );
    let write_b = LineageNodeBinding::new(
        "daily_us",
        ExternalDatasetIdentity::catalog("analytics", "orders_daily").unwrap(),
    )
    .with_subset(DatasetSubset::output("dt=2026-08-12/region=us").unwrap());

    let identities =
        LineageIdentityContext::external([shard_a, shard_b, write_a, write_b]).unwrap();

    let compiled = parse_config(
        r#"
pipeline: { name: shared_dataset_identity }
nodes:
  - type: source
    name: orders_eu
    config:
      name: orders_eu
      type: csv
      glob: incoming/eu/*.csv
      schema: [{ name: id, type: int }]
  - type: source
    name: orders_us
    config:
      name: orders_us
      type: csv
      glob: incoming/us/*.csv
      schema: [{ name: id, type: int }]
  - type: sink
    name: daily_eu
    input: orders_eu
    config: { name: daily_eu, type: csv, path: out/eu.csv }
  - type: sink
    name: daily_us
    input: orders_us
    config: { name: daily_us, type: csv, path: out/us.csv }
"#,
    )
    .unwrap()
    .compile(&CompileContext::default())
    .unwrap();

    let lineage = column_lineage_external(&compiled, &identities).unwrap();

    // One collection dataset, one sink dataset — the merge is what is under test,
    // not a second entry appearing.
    assert_eq!(lineage.inputs.len(), 1, "{:?}", lineage.inputs);
    assert_eq!(lineage.outputs.len(), 1);

    let input_facets = lineage
        .input_identity_facets
        .get(&lineage.inputs[0])
        .expect("the shared collection carries both shards' facts");
    assert_eq!(
        input_facets
            .subsets()
            .iter()
            .map(DatasetSubset::identifier)
            .collect::<Vec<_>>(),
        ["region=eu", "region=us"],
        "both shards were read, and the sorted order is the walk-independent one"
    );
    assert_eq!(
        input_facets
            .symlinks()
            .iter()
            .map(|alias| (alias.namespace(), alias.name()))
            .collect::<Vec<_>>(),
        [("hive://cluster", "db.orders"), ("s3://lake", "orders/")],
        "an alias both shards authorize appears once"
    );

    let output_facets = &lineage.outputs[0].identity_facets;
    assert_eq!(
        output_facets
            .subsets()
            .iter()
            .map(DatasetSubset::identifier)
            .collect::<Vec<_>>(),
        ["dt=2026-08-12/region=eu", "dt=2026-08-12/region=us"],
        "the table was written by both writers, so both partitions were produced"
    );
    assert_eq!(
        output_facets
            .symlinks()
            .iter()
            .map(SymlinkIdentifier::name)
            .collect::<Vec<_>>(),
        ["db.orders_daily"],
        "an alias only one writer authorized is still true of the dataset"
    );

    // The facets reach the wire in both positions, and only the role-matching
    // direction is serialized.
    let events = run_events(
        &lineage,
        Job {
            namespace: "clinker".to_owned(),
            name: "shared_dataset_identity".to_owned(),
            facets: None,
        },
        &lifecycle_facts(),
    );
    let complete = serde_json::to_value(&events[1]).expect("serialize COMPLETE event");
    assert_eq!(
        complete["inputs"][0]["inputFacets"]["subset"]["inputCondition"]["locations"],
        serde_json::json!(["region=eu", "region=us"])
    );
    assert_eq!(
        complete["outputs"][0]["outputFacets"]["subset"]["outputCondition"]["locations"],
        serde_json::json!(["dt=2026-08-12/region=eu", "dt=2026-08-12/region=us"])
    );

    // Byte-stability: the merged collections are sorted, so neither the walk
    // order nor a hash seed can reorder what is emitted.
    let bytes = serde_json::to_vec(&events).expect("serialize events");
    for _ in 0..4 {
        let rebuilt = column_lineage_external(&compiled, &identities).unwrap();
        let again = run_events(
            &rebuilt,
            Job {
                namespace: "clinker".to_owned(),
                name: "shared_dataset_identity".to_owned(),
                facets: None,
            },
            &lifecycle_facts(),
        );
        assert_eq!(
            bytes,
            serde_json::to_vec(&again).expect("serialize events"),
            "the emitted document must be byte-identical across builds"
        );
    }
}
