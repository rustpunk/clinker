/// Default pipeline YAML loaded when the app starts (no file selected).
///
/// Includes `_notes` metadata to exercise the Notes drawer.
#[allow(dead_code)]
pub const DEFAULT_YAML: &str = r#"pipeline:
  name: customer_etl

_notes:
  pipeline: |
    Customer ETL pipeline for the marketing analytics team.
    Runs nightly via cron. Output feeds the Looker dashboard.

nodes:
  - type: source
    name: customers
    description: Source data from the CRM export.
    config:
      name: customers
      type: csv
      path: ./data/customers.csv
      options:
        has_header: true

  - type: transform
    name: active_only
    description: Filter to active customers
    input: customers
    config:
      cxl: |
        emit status_ok = status == "active"

  - type: transform
    name: enrich
    description: Compute derived fields
    input: active_only
    config:
      cxl: |
        emit full_name = first_name + " " + last_name
        emit email_lower = lower(email)

  - type: output
    name: results
    input: enrich
    config:
      name: results
      type: csv
      path: ./output/customers.csv
      include_unmapped: true

error_handling:
  strategy: fail_fast
"#;
