//! Integration tests for lookup enrichment.
//!
//! Exercises the full pipeline: YAML config → plan → load lookup table →
//! evaluate transforms with LookupResolver → output.

use super::*;

/// Helper: run executor with multiple in-memory CSV sources.
fn run_lookup_test(
    yaml: &str,
    sources: Vec<(&str, &str)>, // (source_name, csv_data)
) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
    let config = crate::config::parse_config(yaml).unwrap();
    let output_buf = crate::test_helpers::SharedBuffer::new();

    let mut readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::new();
    for (name, data) in sources {
        readers.insert(
            name.to_string(),
            Box::new(std::io::Cursor::new(data.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        );
    }

    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(output_buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(|v| crate::config::convert_pipeline_vars(v))
        .unwrap_or_default();
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars,
        shutdown_token: None,
    };

    let report = PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)?;

    let output = output_buf.as_string();
    Ok((report.counters, report.dlq_entries, output))
}

/// Basic equality lookup — replaces old analytic_window pattern.
#[test]
fn test_lookup_equality_join() {
    let yaml = r#"
pipeline:
  name: lookup_equality

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: quantity, type: int }

  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: product_name, type: string }

  - type: transform
    name: enrich
    input: orders
    config:
      lookup:
        source: products
        where: "product_id == products.product_id"
      cxl: |
        emit order_id = order_id
        emit product_name = products.product_name
        emit quantity = quantity

  - type: output
    name: result
    input: enrich
    config:
      name: result
      type: csv
      path: output.csv
"#;

    let orders = "order_id,product_id,quantity\nORD-1,PROD-A,5\nORD-2,PROD-B,3\n";
    let products = "product_id,product_name\nPROD-A,Widget\nPROD-B,Gadget\n";

    let (counters, dlq, output) = run_lookup_test(
        yaml,
        vec![("orders", orders), ("products", products)],
    )
    .unwrap();

    assert_eq!(counters.total_count, 2);
    assert_eq!(counters.ok_count, 2);
    assert!(dlq.is_empty());
    assert!(output.contains("Widget"), "output: {output}");
    assert!(output.contains("Gadget"), "output: {output}");
}

/// Range-predicate lookup — the key new capability.
#[test]
fn test_lookup_range_predicate() {
    let yaml = r#"
pipeline:
  name: lookup_range

nodes:
  - type: source
    name: employees
    config:
      name: employees
      type: csv
      path: employees.csv
      schema:
        - { name: employee_id, type: string }
        - { name: ee_group, type: string }
        - { name: pay, type: int }

  - type: source
    name: rate_bands
    config:
      name: rate_bands
      type: csv
      path: rate_bands.csv
      schema:
        - { name: ee_group, type: string }
        - { name: min_pay, type: int }
        - { name: max_pay, type: int }
        - { name: rate_class, type: string }

  - type: transform
    name: classify
    input: employees
    config:
      lookup:
        source: rate_bands
        where: |
          ee_group == rate_bands.ee_group
          and pay >= rate_bands.min_pay
          and pay <= rate_bands.max_pay
      cxl: |
        emit employee_id = employee_id
        emit rate_class = rate_bands.rate_class

  - type: output
    name: result
    input: classify
    config:
      name: result
      type: csv
      path: output.csv
"#;

    let employees = "employee_id,ee_group,pay\nE001,exempt,75000\nE002,hourly,35000\nE003,exempt,120000\n";
    let rate_bands = "ee_group,min_pay,max_pay,rate_class\nexempt,50000,80000,tier_1\nexempt,80001,150000,tier_2\nhourly,20000,50000,tier_3\n";

    let (counters, dlq, output) = run_lookup_test(
        yaml,
        vec![("employees", employees), ("rate_bands", rate_bands)],
    )
    .unwrap();

    assert_eq!(counters.total_count, 3);
    assert_eq!(counters.ok_count, 3);
    assert!(dlq.is_empty());
    assert!(output.contains("tier_1"), "E001 should be tier_1: {output}");
    assert!(output.contains("tier_3"), "E002 should be tier_3: {output}");
    assert!(output.contains("tier_2"), "E003 should be tier_2: {output}");
}

/// on_miss: null_fields — unmatched records get null lookup fields.
#[test]
fn test_lookup_on_miss_null_fields() {
    let yaml = r#"
pipeline:
  name: lookup_miss

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }

  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: product_name, type: string }

  - type: transform
    name: enrich
    input: orders
    config:
      lookup:
        source: products
        where: "product_id == products.product_id"
        on_miss: null_fields
      cxl: |
        emit order_id = order_id
        emit product_name = products.product_name

  - type: output
    name: result
    input: enrich
    config:
      name: result
      type: csv
      path: output.csv
"#;

    let orders = "order_id,product_id\nORD-1,PROD-A\nORD-2,PROD-MISSING\n";
    let products = "product_id,product_name\nPROD-A,Widget\n";

    let (counters, _dlq, output) = run_lookup_test(
        yaml,
        vec![("orders", orders), ("products", products)],
    )
    .unwrap();

    // Both records should be emitted — one with product_name, one with null
    assert_eq!(counters.total_count, 2);
    assert_eq!(counters.ok_count, 2);
    assert!(output.contains("Widget"), "matched: {output}");
}

/// on_miss: skip — unmatched records are dropped.
#[test]
fn test_lookup_on_miss_skip() {
    let yaml = r#"
pipeline:
  name: lookup_skip

nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }

  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: product_name, type: string }

  - type: transform
    name: enrich
    input: orders
    config:
      lookup:
        source: products
        where: "product_id == products.product_id"
        on_miss: skip
      cxl: |
        emit order_id = order_id
        emit product_name = products.product_name

  - type: output
    name: result
    input: enrich
    config:
      name: result
      type: csv
      path: output.csv
"#;

    let orders = "order_id,product_id\nORD-1,PROD-A\nORD-2,PROD-MISSING\nORD-3,PROD-A\n";
    let products = "product_id,product_name\nPROD-A,Widget\n";

    let (counters, _dlq, output) = run_lookup_test(
        yaml,
        vec![("orders", orders), ("products", products)],
    )
    .unwrap();

    // Only 2 of 3 records should be emitted (PROD-MISSING is skipped)
    assert_eq!(counters.total_count, 3);
    assert_eq!(counters.ok_count, 2);
    assert_eq!(counters.filtered_count, 1);
    assert!(!output.contains("PROD-MISSING"), "skipped: {output}");
}
