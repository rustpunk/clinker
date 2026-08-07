use clinker_lineage::logical_identity::{
    DatasetIdentifierType, DatasetSubset, ExternalDatasetIdentity, LineageIdentityContext,
    LineageIdentityError, LineageNodeBinding, SymlinkIdentifier,
};

#[test]
fn canonical_catalog_subset_and_symlinks() {
    let canonical = ExternalDatasetIdentity::canonical("s3://warehouse/customers")
        .expect("canonical datasource identity");
    assert_eq!(canonical.dataset_id().namespace, "s3://warehouse");
    assert_eq!(canonical.dataset_id().name, "customers");

    let catalog = ExternalDatasetIdentity::catalog("analytics", "customers_clean")
        .expect("catalog identity");
    assert_eq!(catalog.dataset_id().namespace, "analytics");
    assert_eq!(catalog.dataset_id().name, "customers_clean");

    let subset = DatasetSubset::input("partition=2026-08-06")
        .expect("stable logical partition identifier");
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
    assert_eq!(source.subsets(), &[subset]);
    assert_eq!(source.symlinks(), &[alias]);

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
    assert_eq!(context, relocated, "physical relocation cannot affect identity");

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
    assert!(matches!(duplicate, LineageIdentityError::DuplicateNode { .. }));

    let missing = context
        .validate_required(["source_customers", "output_customers", "audit"])
        .expect_err("missing binding must fail");
    assert!(matches!(missing, LineageIdentityError::MissingNode { .. }));

    assert!(ExternalDatasetIdentity::canonical("warehouse-customers").is_err());
    assert!(ExternalDatasetIdentity::catalog("analytics", "").is_err());
    assert!(DatasetSubset::input("/worker-17/tmp/customers.csv").is_err());
    assert!(DatasetSubset::output("attempt-42/customers.csv").is_err());
}
