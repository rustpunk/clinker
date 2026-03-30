/// Default pipeline YAML loaded when the app starts (no file selected).
///
/// This is a realistic `PipelineConfig`-compatible YAML that exercises the
/// full config schema: inputs, transforms with CXL expressions, outputs,
/// and error handling.
pub const DEFAULT_YAML: &str = r#"pipeline:
  name: customer_etl

inputs:
  - name: customers
    type: csv
    path: ./data/customers.csv
    options:
      has_header: true

transformations:
  - name: active_only
    description: Filter to active customers
    cxl: |
      emit status_ok = status == "active"

  - name: enrich
    description: Compute derived fields
    cxl: |
      emit full_name = first_name + " " + last_name
      emit email_lower = lower(email)

outputs:
  - name: results
    type: csv
    path: ./output/customers.csv
    include_unmapped: true

error_handling:
  strategy: fail_fast
"#;
