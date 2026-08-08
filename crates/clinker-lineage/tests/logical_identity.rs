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
  - type: output
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
  - type: output
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

    assert_eq!(
        complete["inputs"][0],
        serde_json::json!({
            "namespace": "s3://warehouse",
            "name": "customers",
            "facets": {
                "subset": {
                    "_producer": "https://github.com/rustpunk/clinker",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/InputSubsetInputDatasetFacet",
                    "inputCondition": {
                        "type": "location",
                        "locations": ["partition=2026-08-05", "partition=2026-08-06"]
                    }
                },
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
        complete["outputs"][0]["facets"]["subset"],
        serde_json::json!({
            "_producer": "https://github.com/rustpunk/clinker",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/BaseSubsetDatasetFacet.json#/$defs/OutputSubsetOutputDatasetFacet",
            "outputCondition": {
                "type": "location",
                "locations": ["release=current"]
            }
        })
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
  - type: output
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
  - type: output
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
