use clinker_lineage::column_lineage_external;
use clinker_lineage::logical_identity::{
    DatasetIdentifierType, DatasetSubset, ExternalDatasetIdentity, LineageIdentityContext,
    LineageIdentityError, LineageNodeBinding, SymlinkIdentifier,
};
use clinker_plan::CompileContext;
use clinker_plan::config::parse_config;

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
    assert_eq!(source.subsets(), &[subset.clone()]);
    assert_eq!(source.symlinks(), &[alias.clone()]);

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
    assert_eq!(input_facets.subsets(), &[subset.clone()]);
    assert_eq!(input_facets.symlinks(), &[alias.clone()]);
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
