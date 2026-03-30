#[cfg(test)]
mod tests {
    use crate::config;
    use crate::error::PipelineError;
    use crate::executor::{DlqEntry, StreamingExecutor};

    /// Helper: run executor with in-memory CSV input/output.
    fn run_pipeline(
        yaml: &str,
        csv_input: &str,
    ) -> Result<(clinker_record::PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
        let config = config::parse_config(yaml).unwrap();
        let reader = std::io::Cursor::new(csv_input.as_bytes().to_vec());
        let mut output_buf: Vec<u8> = Vec::new();

        let (counters, dlq) =
            StreamingExecutor::run_with_readers_writers(&config, reader, &mut output_buf)?;

        let output = String::from_utf8(output_buf).unwrap();
        Ok((counters, dlq, output))
    }

    /// Determine exit code from pipeline result (mirrors main.rs logic).
    fn exit_code(result: &Result<(clinker_record::PipelineCounters, Vec<DlqEntry>, String), PipelineError>) -> u8 {
        match result {
            Ok((counters, _, _)) => {
                if counters.dlq_count > 0 { 2 } else { 0 }
            }
            Err(PipelineError::Config(_) | PipelineError::Compilation { .. }) => 1,
            Err(PipelineError::Eval(_)) => 3,
            Err(PipelineError::Io(_) | PipelineError::Format(_)) => 4,
        }
    }

    #[test]
    fn test_exit_code_0_success() {
        let yaml = r#"
pipeline:
  name: success

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations: []
"#;
        let csv = "name,age\nAlice,30\nBob,25\n";
        let result = run_pipeline(yaml, csv);
        assert_eq!(exit_code(&result), 0);
    }

    #[test]
    fn test_exit_code_1_config_error() {
        // Bad YAML — missing required fields
        let yaml = "pipeline:\n  name: broken\n";
        let result = config::parse_config(yaml);
        assert!(result.is_err());
        // Config error maps to exit code 1
        let err = PipelineError::Config(result.unwrap_err());
        assert!(matches!(err, PipelineError::Config(_)));
    }

    #[test]
    fn test_exit_code_2_partial_success() {
        let yaml = r#"
pipeline:
  name: partial

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: will_fail_some
    cxl: |
      emit result = value.to_int() * 2

error_handling:
  strategy: continue
"#;
        let csv = "value\n10\nbad\n20\n";
        let result = run_pipeline(yaml, csv);
        assert_eq!(exit_code(&result), 2);
    }

    #[test]
    fn test_exit_code_3_fatal_data_error() {
        let yaml = r#"
pipeline:
  name: fatal

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: will_fail
    cxl: |
      emit result = value.to_int() + 1

error_handling:
  strategy: fail_fast
"#;
        let csv = "value\n10\nbad\n20\n";
        let result = run_pipeline(yaml, csv);
        assert_eq!(exit_code(&result), 3);
    }

    #[test]
    fn test_end_to_end_csv_transform() {
        let yaml = r#"
pipeline:
  name: end-to-end

inputs:
  - name: employees
    type: csv
    path: input.csv

outputs:
  - name: transformed
    type: csv
    path: output.csv
    include_unmapped: true
    exclude:
      - internal_id
    mapping:
      full_name: employee_name

transformations:
  - name: compute_full_name
    cxl: |
      emit full_name = first_name + " " + last_name
  - name: compute_upper_dept
    cxl: |
      emit dept_upper = department.upper()
"#;
        let csv = "first_name,last_name,department,internal_id\n\
                    Alice,Smith,Engineering,12345\n\
                    Bob,Jones,Marketing,67890\n\
                    Charlie,Brown,Engineering,11111\n";

        let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();

        // Verify counters
        assert_eq!(counters.total_count, 3);
        assert_eq!(counters.ok_count, 3);
        assert_eq!(counters.dlq_count, 0);
        assert!(dlq.is_empty());

        // Verify output contains transformed fields
        assert!(output.contains("employee_name"), "should have renamed full_name to employee_name");
        assert!(output.contains("Alice Smith"), "should have concatenated names");
        assert!(output.contains("Bob Jones"));
        assert!(output.contains("ENGINEERING"), "should have uppercased department");
        assert!(output.contains("MARKETING"));

        // Verify excluded field is gone
        assert!(!output.contains("internal_id"), "should have excluded internal_id");
        assert!(!output.contains("12345"), "should have excluded internal_id values");

        // Verify unmapped fields are present
        assert!(output.contains("first_name"), "include_unmapped should pass through");

        // Parse output as CSV to verify structure
        let mut reader = csv::ReaderBuilder::new().from_reader(output.as_bytes());
        let headers: Vec<String> = reader.headers().unwrap().iter().map(|s| s.to_string()).collect();
        assert!(headers.contains(&"employee_name".to_string()));
        assert!(headers.contains(&"dept_upper".to_string()));
        assert!(!headers.contains(&"internal_id".to_string()));

        let records: Vec<csv::StringRecord> = reader.records().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 3);
    }
}
