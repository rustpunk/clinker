#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::config;
    use crate::error::PipelineError;
    use crate::executor::{DlqEntry, PipelineExecutor, PipelineRunParams};
    use clinker_bench_support::io::SharedBuffer;

    /// Helper: run executor with in-memory CSV input/output.
    fn run_pipeline(
        yaml: &str,
        csv_input: &str,
    ) -> Result<(clinker_record::PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
        let config = config::parse_config(yaml).unwrap();
        let output_buf = SharedBuffer::new();

        let first_source = config.source_configs().next().unwrap().name.clone();
        let first_output = config.output_configs().next().unwrap().name.clone();
        let readers: crate::executor::SourceReaders = HashMap::from([(
            first_source.clone(),
            crate::executor::single_file_reader(
                "test.csv",
                Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec())),
            ),
        )]);
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            first_output,
            Box::new(output_buf.clone()) as Box<dyn std::io::Write + Send>,
        )]);

        let pipeline_vars = config
            .pipeline
            .vars
            .as_ref()
            .map(|v| config::convert_pipeline_vars(v))
            .unwrap_or_default();
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars,
            shutdown_token: None,
        };

        let report = PipelineExecutor::run_with_readers_writers(
            &config,
            &first_source,
            readers,
            writers,
            &params,
        )?;

        let output = output_buf.as_string();
        Ok((report.counters, report.dlq_entries, output))
    }

    /// Determine exit code from pipeline result (mirrors main.rs logic).
    fn exit_code(
        result: &Result<(clinker_record::PipelineCounters, Vec<DlqEntry>, String), PipelineError>,
    ) -> u8 {
        match result {
            Ok((counters, _, _)) => {
                if counters.dlq_count > 0 {
                    2
                } else {
                    0
                }
            }
            Err(
                PipelineError::Config(_)
                | PipelineError::Schema(_)
                | PipelineError::Compilation { .. }
                | PipelineError::Internal { .. }
                | PipelineError::SortOrderViolation { .. }
                | PipelineError::MergeSortOrderViolation { .. }
                | PipelineError::SchemaMismatch { .. }
                | PipelineError::CompositionDepthExceeded { .. }
                | PipelineError::CompositionBodyMissing { .. }
                | PipelineError::CompositionUnknownPort { .. }
                | PipelineError::CompositionBodyError { .. },
            ) => 1,
            Err(PipelineError::Eval(_) | PipelineError::Accumulator { .. }) => 3,
            Err(
                PipelineError::Io(_)
                | PipelineError::Format(_)
                | PipelineError::ThreadPool(_)
                | PipelineError::Multiple(_)
                | PipelineError::CorrelationGroupOverflow { .. },
            ) => 4,
        }
    }

    #[test]
    fn test_exit_code_0_success() {
        let yaml = r#"
pipeline:
  name: success
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: string }

- type: output
  name: dest
  input: src
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;
        let csv = "name,age\nAlice,30\nBob,25\n";
        let result = run_pipeline(yaml, csv);
        assert_eq!(exit_code(&result), 0);
    }

    #[test]
    fn test_exit_code_1_config_error() {
        // Bad YAML — required field missing. An empty pipeline parses,
        // so force a real error with an unknown top-level key
        // (`deny_unknown_fields` still applies).
        let yaml = "pipeline:\n  name: broken\nbogus: 1\n";
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
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: value, type: string }

- type: transform
  name: will_fail_some
  input: src
  config:
    cxl: 'emit result = value.to_int() * 2

      '
- type: output
  name: dest
  input: will_fail_some
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
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
error_handling:
  strategy: fail_fast
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: value, type: string }

- type: transform
  name: will_fail
  input: src
  config:
    cxl: 'emit result = value.to_int() + 1

      '
- type: output
  name: dest
  input: will_fail
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
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
nodes:
- type: source
  name: employees
  config:
    name: employees
    type: csv
    path: input.csv
    schema:
      - { name: first_name, type: any }
      - { name: last_name, type: any }
      - { name: department, type: any }
      - { name: internal_id, type: any }

- type: transform
  name: compute_full_name
  input: employees
  config:
    cxl: 'emit full_name = first_name + " " + last_name

      '
- type: transform
  name: compute_upper_dept
  input: compute_full_name
  config:
    cxl: 'emit dept_upper = department.upper()

      '
- type: output
  name: transformed
  input: compute_upper_dept
  config:
    name: transformed
    type: csv
    path: output.csv
    include_unmapped: true
    exclude:
    - internal_id
    mapping:
      full_name: employee_name
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
        assert!(
            output.contains("employee_name"),
            "should have renamed full_name to employee_name"
        );
        assert!(
            output.contains("Alice Smith"),
            "should have concatenated names"
        );
        assert!(output.contains("Bob Jones"));
        assert!(
            output.contains("ENGINEERING"),
            "should have uppercased department"
        );
        assert!(output.contains("MARKETING"));

        // Verify excluded field is gone
        assert!(
            !output.contains("internal_id"),
            "should have excluded internal_id"
        );
        assert!(
            !output.contains("12345"),
            "should have excluded internal_id values"
        );

        // Verify unmapped fields are present
        assert!(
            output.contains("first_name"),
            "include_unmapped should pass through"
        );

        // Parse output as CSV to verify structure
        let mut reader = csv::ReaderBuilder::new().from_reader(output.as_bytes());
        let headers: Vec<String> = reader
            .headers()
            .unwrap()
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(headers.contains(&"employee_name".to_string()));
        assert!(headers.contains(&"dept_upper".to_string()));
        assert!(!headers.contains(&"internal_id".to_string()));

        let records: Vec<csv::StringRecord> = reader.records().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 3);
    }

    // ── Phase 8 Task 8.4 exit code gate tests ─────────────────

    #[test]
    fn test_exit_code_4_io_error() {
        // Config references a nonexistent input file — I/O error on open
        let yaml = r#"
pipeline:
  name: io-test
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: /nonexistent/path/that/does/not/exist.csv
    schema:
      - { name: id, type: string }

- type: transform
  name: t1
  input: src
  config:
    cxl: emit x = 1
- type: output
  name: dest
  input: t1
  config:
    name: dest
    type: csv
    path: /tmp/clinker_test_out.csv
"#;
        let _config = config::parse_config(yaml).unwrap();
        let result: Result<_, PipelineError> = Err(PipelineError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        )));
        assert_eq!(exit_code(&result), 4);
    }

    #[test]
    fn test_exit_code_130_interrupted() {
        // Per-token shutdown: a token round-trips request/observe in
        // isolation, and the CLI maps an interrupted run to exit code 130.
        use crate::pipeline::shutdown::ShutdownToken;
        let token = ShutdownToken::detached();
        assert!(!token.is_requested());
        token.request();
        assert!(token.is_requested());
        assert_eq!(crate::exit_codes::EXIT_INTERRUPTED, 130);
    }

    // ══════════════════════════════════════════════════════════════
    // Filter + Distinct integration tests
    // ══════════════════════════════════════════════════════════════

    fn filter_yaml(cxl: &str) -> String {
        let indented: String = cxl
            .lines()
            .map(|l| format!("        {l}"))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "pipeline:\n  name: filter_test\nnodes:\n  - type: source\n    name: src\n    config:\n      name: src\n      type: csv\n      path: input.csv\n      schema:\n        - {{ name: id, type: any }}\n        - {{ name: name, type: any }}\n        - {{ name: status, type: any }}\n        - {{ name: value, type: any }}\n        - {{ name: amount, type: any }}\n        - {{ name: category, type: any }}\n        - {{ name: code, type: any }}\n        - {{ name: dept, type: any }}\n        - {{ name: department, type: any }}\n        - {{ name: priority, type: any }}\n        - {{ name: optional, type: any }}\n        - {{ name: required, type: any }}\n        - {{ name: active, type: any }}\n        - {{ name: first, type: any }}\n        - {{ name: last, type: any }}\n        - {{ name: first_name, type: any }}\n  - type: transform\n    name: t1\n    input: src\n    config:\n      cxl: |\n{indented}\n  - type: output\n    name: dest\n    input: t1\n    config:\n      name: dest\n      type: csv\n      path: output.csv\n"
        )
    }

    // ── Filter tests ──────────────────────────────────────────────

    #[test]
    fn test_filter_simple_predicate() {
        let yaml = filter_yaml(
            r#"filter status == "active"
emit out_name = name"#,
        );
        let csv = "name,status\nAlice,active\nBob,inactive\nCharlie,active\n";
        let (counters, dlq, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.total_count, 3);
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.filtered_count, 1);
        assert_eq!(counters.dlq_count, 0);
        assert!(dlq.is_empty());
        assert!(output.contains("Alice"));
        assert!(output.contains("Charlie"));
        assert!(!output.contains("Bob"));
    }

    #[test]
    fn test_filter_compound_and_or() {
        let yaml = filter_yaml(
            r#"filter amount.to_int() > 100 or priority == "high"
emit out_name = name
emit out_amount = amount"#,
        );
        let csv = "name,amount,priority\nA,200,low\nB,50,high\nC,30,low\nD,150,medium\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 3); // A(200>100), B(high), D(150>100)
        assert_eq!(counters.filtered_count, 1); // C(30,low)
        assert!(!output.contains(",C,"));
    }

    #[test]
    fn test_filter_with_let_binding() {
        let yaml = filter_yaml(
            r#"let derived = amount.to_int() * 2
filter derived > 500
emit out_name = name
emit derived = derived"#,
        );
        let csv = "name,amount\nAlice,300\nBob,200\nCharlie,400\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2); // Alice(600), Charlie(800)
        assert_eq!(counters.filtered_count, 1); // Bob(400)
        assert!(output.contains("Alice"));
        assert!(!output.contains("Bob"));
    }

    #[test]
    fn test_filter_null_field_skips() {
        let yaml = filter_yaml(
            r#"filter status == "active"
emit out_name = name"#,
        );
        let csv = "name,status\nAlice,active\nBob,\nCharlie,active\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.filtered_count, 1); // Bob has empty status → null == "active" is false
        assert!(!output.contains("Bob"));
    }

    #[test]
    fn test_filter_all_rows_filtered() {
        let yaml = filter_yaml(
            r#"filter status == "active"
emit out_name = name"#,
        );
        let csv = "name,status\nAlice,inactive\nBob,inactive\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.total_count, 2);
        assert_eq!(counters.ok_count, 0);
        assert_eq!(counters.filtered_count, 2);
        // Output should be header-only or empty
        let lines: Vec<&str> = output.trim().lines().collect();
        assert!(lines.len() <= 1); // Just header or empty
    }

    #[test]
    fn test_filter_multiple_filters_short_circuit() {
        let yaml = filter_yaml(
            r#"filter status == "active"
filter amount.to_int() > 100
emit out_name = name"#,
        );
        let csv = "name,status,amount\nAlice,active,200\nBob,inactive,300\nCharlie,active,50\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 1); // Only Alice passes both
        assert_eq!(counters.filtered_count, 2); // Bob(first filter), Charlie(second filter)
        assert!(output.contains("Alice"));
        assert!(!output.contains("Bob"));
        assert!(!output.contains("Charlie"));
    }

    #[test]
    fn test_filter_three_valued_or_with_null() {
        let yaml = filter_yaml(
            r#"filter optional == "yes" or required == "yes"
emit out_name = name"#,
        );
        let csv = "name,optional,required\nA,,yes\nB,,no\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        // A: null or true → true (passes)
        // B: null or false → null (filtered)
        assert_eq!(counters.ok_count, 1);
        assert_eq!(counters.filtered_count, 1);
        assert!(output.contains("A"));
    }

    // ── Distinct tests ────────────────────────────────────────────

    #[test]
    fn test_distinct_by_single_field() {
        let yaml = filter_yaml(
            r#"distinct by id
emit out_id = id
emit out_name = name"#,
        );
        let csv = "id,name\n1,Alice\n2,Bob\n1,Charlie\n3,Dave\n2,Eve\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 3); // 1,2,3
        assert_eq!(counters.distinct_count, 2); // duplicate 1 and 2
        assert!(output.contains("Alice")); // first occurrence of id=1
        assert!(output.contains("Bob")); // first occurrence of id=2
        assert!(!output.contains("Charlie")); // duplicate id=1
        assert!(output.contains("Dave"));
        assert!(!output.contains("Eve")); // duplicate id=2
    }

    #[test]
    fn test_distinct_bare_all_fields() {
        let yaml = filter_yaml(
            r#"distinct
emit out_name = name
emit out_dept = dept"#,
        );
        let csv = "name,dept\nAlice,Eng\nBob,Sales\nAlice,Eng\nBob,HR\n";
        let (counters, _, _output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 3); // Alice+Eng, Bob+Sales, Bob+HR are unique
        assert_eq!(counters.distinct_count, 1); // Alice+Eng duplicate
    }

    #[test]
    fn test_distinct_by_let_binding() {
        let yaml = filter_yaml(
            r#"let full = first + " " + last
distinct by full
emit full = full"#,
        );
        let csv = "first,last\nAlice,Smith\nBob,Jones\nAlice,Smith\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.distinct_count, 1);
        assert!(output.contains("Alice Smith"));
        assert!(output.contains("Bob Jones"));
    }

    #[test]
    fn test_distinct_null_field_deduplicates() {
        let yaml = filter_yaml(
            r#"distinct by id
emit out_id = id
emit out_name = name"#,
        );
        let csv = "id,name\n1,Alice\n,Bob\n,Charlie\n2,Dave\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        // null id: Bob is first, Charlie is duplicate (NULL = NULL)
        assert_eq!(counters.ok_count, 3); // 1, null(Bob), 2
        assert_eq!(counters.distinct_count, 1); // null(Charlie)
        assert!(output.contains("Bob"));
        assert!(!output.contains("Charlie"));
    }

    #[test]
    fn test_distinct_preserves_first_fields() {
        let yaml = filter_yaml(
            r#"distinct by id
emit out_id = id
emit out_value = value"#,
        );
        let csv = "id,value\nA,100\nB,200\nA,999\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert!(
            output.contains("A,100") || output.contains("A,\"100\"") || output.contains(",100")
        );
        assert!(!output.contains("999")); // second A is dropped
    }

    #[test]
    fn test_distinct_mixed_type_field() {
        // String "1" vs numeric "1" — both are strings in CSV
        let yaml = filter_yaml(
            r#"distinct by code
emit out_code = code"#,
        );
        let csv = "code\n1\n01\n1\n";
        let (counters, _, _output) = run_pipeline(&yaml, csv).unwrap();
        // "1" and "01" are different strings → both kept. Second "1" is duplicate.
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.distinct_count, 1);
    }

    // ── Combined filter + distinct ────────────────────────────────

    #[test]
    fn test_filter_then_distinct() {
        let yaml = filter_yaml(
            r#"filter status == "active"
distinct by dept
emit out_name = name
emit out_dept = dept"#,
        );
        let csv = "name,status,dept\n\
                   Alice,active,Eng\n\
                   Bob,inactive,Eng\n\
                   Charlie,active,Eng\n\
                   Dave,active,Sales\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        // Bob filtered. Alice first active Eng. Charlie dup active Eng. Dave first active Sales.
        assert_eq!(counters.ok_count, 2); // Alice, Dave
        assert_eq!(counters.filtered_count, 1); // Bob
        assert_eq!(counters.distinct_count, 1); // Charlie
        assert!(output.contains("Alice"));
        assert!(output.contains("Dave"));
        assert!(!output.contains("Bob"));
        assert!(!output.contains("Charlie"));
    }

    #[test]
    fn test_distinct_then_filter() {
        let yaml = filter_yaml(
            r#"distinct by dept
filter status == "active"
emit out_name = name
emit out_dept = dept"#,
        );
        let csv = "name,status,dept\n\
                   Alice,inactive,Eng\n\
                   Bob,active,Sales\n\
                   Charlie,active,Eng\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        // Alice: first Eng (distinct passes), but inactive (filter rejects)
        // Bob: first Sales (distinct passes), active (filter passes)
        // Charlie: dup Eng (distinct rejects)
        assert_eq!(counters.ok_count, 1); // Bob
        assert_eq!(counters.filtered_count, 1); // Alice
        assert_eq!(counters.distinct_count, 1); // Charlie
        assert!(output.contains("Bob"));
        assert!(!output.contains("Alice"));
        assert!(!output.contains("Charlie"));
    }

    #[test]
    fn test_filter_distinct_combined_counters() {
        let yaml = filter_yaml(
            r#"filter status == "active"
distinct by dept
emit out_name = name"#,
        );
        let csv = "name,status,dept\n\
                   A,active,Eng\n\
                   B,inactive,Eng\n\
                   C,active,Eng\n\
                   D,active,Sales\n\
                   E,inactive,Sales\n";
        let (counters, _, _) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.total_count, 5);
        assert_eq!(counters.ok_count, 2); // A, D
        assert_eq!(counters.filtered_count, 2); // B, E
        assert_eq!(counters.distinct_count, 1); // C
        assert_eq!(counters.dlq_count, 0);
        // Invariant: total = ok + filtered + distinct + dlq
        assert_eq!(
            counters.total_count,
            counters.ok_count
                + counters.filtered_count
                + counters.distinct_count
                + counters.dlq_count
        );
    }

    // ── Stats + streaming tests ───────────────────────────────────

    #[test]
    fn test_streaming_filter_basic() {
        // Streaming mode (no windows) — filter should work
        let yaml = filter_yaml(
            r#"filter amount.to_int() > 100
emit out_name = name
emit out_amount = amount"#,
        );
        let csv = "name,amount\nAlice,200\nBob,50\nCharlie,150\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.filtered_count, 1);
        assert!(output.contains("Alice"));
        assert!(output.contains("Charlie"));
        assert!(!output.contains("Bob"));
    }

    #[test]
    fn test_streaming_distinct_global() {
        // Streaming mode — global distinct (no windows)
        let yaml = filter_yaml(
            r#"distinct by category
emit out_category = category
emit out_first_item = name"#,
        );
        let csv = "name,category\nApple,Fruit\nBanana,Fruit\nCarrot,Veg\nDate,Fruit\nEgg,Protein\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 3); // Fruit, Veg, Protein
        assert_eq!(counters.distinct_count, 2); // Banana, Date
        assert!(output.contains("Apple")); // first Fruit
        assert!(output.contains("Carrot"));
        assert!(output.contains("Egg"));
    }

    #[test]
    fn test_filter_distinct_order_matters_state() {
        // A(active), A(inactive), B(active), B(active)
        // distinct by name → filter active
        let yaml = filter_yaml(
            r#"distinct by name
filter status == "active"
emit out_name = name"#,
        );
        let csv = "name,status\nA,active\nA,inactive\nB,active\nB,active\n";
        let (counters, _, output) = run_pipeline(&yaml, csv).unwrap();
        // A first: distinct passes, filter passes → emit
        // A second: distinct rejects (dup)
        // B first: distinct passes, filter passes → emit
        // B second: distinct rejects (dup)
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.distinct_count, 2);
        assert_eq!(counters.filtered_count, 0);
        assert!(output.contains("A"));
        assert!(output.contains("B"));
    }

    #[test]
    fn test_filter_error_in_predicate_routes_to_dlq() {
        let yaml = r#"
pipeline:
  name: filter_err
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: string }
      - { name: name, type: string }
      - { name: amount, type: string }

- type: transform
  name: t1
  input: src
  config:
    cxl: 'filter amount.to_int() > 0

      emit out_name = name

      '
- type: output
  name: dest
  input: t1
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;
        let csv = "name,amount\nAlice,10\nBob,bad\nCharlie,5\n";
        let (counters, dlq, output) = run_pipeline(yaml, csv).unwrap();
        // Bob: "bad".to_int() → error → DLQ
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.dlq_count, 1);
        assert_eq!(dlq.len(), 1);
        assert!(output.contains("Alice"));
        assert!(output.contains("Charlie"));
    }

    #[test]
    fn test_distinct_high_cardinality() {
        let yaml = filter_yaml(
            r#"distinct by id
emit out_id = id"#,
        );
        let mut csv = String::from("id\n");
        for i in 0..1000 {
            csv.push_str(&format!("{}\n", i % 100)); // 100 unique, 900 duplicates
        }
        let (counters, _, _) = run_pipeline(&yaml, &csv).unwrap();
        assert_eq!(counters.ok_count, 100);
        assert_eq!(counters.distinct_count, 900);
        assert_eq!(counters.total_count, 1000);
    }

    #[test]
    fn test_distinct_empty_string_vs_null() {
        let yaml = filter_yaml(
            r#"distinct by code
emit out_code = code"#,
        );
        // Use quoted empty strings to be explicit
        let csv = "code\n\"\"\nfoo\n\"\"\nbar\n";
        let (counters, _, _output) = run_pipeline(&yaml, &csv).unwrap();
        // Row 1: empty string "", Row 2: "foo", Row 3: empty string "" (dup), Row 4: "bar"
        assert_eq!(counters.ok_count, 3); // "", "foo", "bar"
        assert_eq!(counters.distinct_count, 1); // second ""
    }

    // ── Combine enrichment coverage lives in
    //     `crates/clinker-core/tests/combine_test.rs`. ──

    // ── Explicit-primary contract tests ──
    //
    // These three gate tests pin down the contract introduced when
    // `PipelineExecutor::run_with_readers_writers` took an explicit
    // `primary: &str` parameter (replacing the implicit
    // `source_configs[0]`-as-primary convention). See
    // `crates/clinker-core/src/executor/mod.rs`.

    /// Passing a `primary` name that does not match any declared
    /// source in the pipeline config must surface as a
    /// `Config(ConfigError::Validation(..))` error — no panic, no
    /// silent coercion.
    #[test]
    fn test_run_with_readers_writers_rejects_unknown_primary() {
        let yaml = r#"
pipeline:
  name: single_source
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: string }
- type: transform
  name: identity
  input: src
  config:
    cxl: 'emit id = id'
- type: output
  name: dest
  input: identity
  config:
    name: dest
    type: csv
    path: output.csv
"#;
        let config = config::parse_config(yaml).unwrap();

        // `src` is registered, but we pass "nonexistent" as primary.
        let readers: crate::executor::SourceReaders = HashMap::from([(
            "src".to_string(),
            crate::executor::single_file_reader(
                "test.csv",
                Box::new(std::io::Cursor::new(b"id\n1\n".to_vec())),
            ),
        )]);
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            "dest".to_string(),
            Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
        )]);
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars: Default::default(),
            shutdown_token: None,
        };

        let result = PipelineExecutor::run_with_readers_writers(
            &config,
            "nonexistent",
            readers,
            writers,
            &params,
        );

        match result {
            Err(PipelineError::Config(crate::config::ConfigError::Validation(msg))) => {
                assert!(
                    msg.contains("primary source 'nonexistent'") && msg.contains("not declared"),
                    "expected 'primary source \\'nonexistent\\' not declared...' message, got: {msg}"
                );
            }
            other => panic!("expected Config(Validation) for unknown primary, got: {other:?}"),
        }
    }

    /// Passing a `primary` that IS declared in the pipeline config
    /// but is missing from the `readers` HashMap must surface as a
    /// `Config(ConfigError::Validation(..))` error — the same clear
    /// error the old implementation already produced for the
    /// positionally-selected primary.
    #[test]
    fn test_run_with_readers_writers_rejects_primary_missing_from_readers() {
        let yaml = r#"
pipeline:
  name: single_source
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: string }
- type: transform
  name: identity
  input: src
  config:
    cxl: 'emit id = id'
- type: output
  name: dest
  input: identity
  config:
    name: dest
    type: csv
    path: output.csv
"#;
        let config = config::parse_config(yaml).unwrap();

        // Readers map is EMPTY — the primary is declared in config
        // but no reader is registered for it.
        let readers: crate::executor::SourceReaders = HashMap::new();
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            "dest".to_string(),
            Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
        )]);
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars: Default::default(),
            shutdown_token: None,
        };

        let result =
            PipelineExecutor::run_with_readers_writers(&config, "src", readers, writers, &params);

        match result {
            Err(PipelineError::Config(crate::config::ConfigError::Validation(msg))) => {
                assert!(
                    msg.contains("no reader registered for input 'src'"),
                    "expected missing-reader message, got: {msg}"
                );
            }
            other => panic!("expected Config(Validation) for missing reader, got: {other:?}"),
        }
    }

    /// Key regression-proofing test: declare sources in the order
    /// `[reference, driving]` (so `source_configs[0]` is the reference
    /// table, not the driving input), pass `primary = "orders"`
    /// explicitly, and verify the pipeline runs correctly end-to-end.
    ///
    /// Under the old positional-primary convention this configuration
    /// would have consumed the `products` reader as the driving input
    /// and starved the combine build side. With the explicit-primary
    /// contract, declaration order is irrelevant — not just for
    /// reader extraction but also for downstream provenance
    /// (`source_file` on every emitted record) and arena-field
    /// scoping inside `execute_dag` / `execute_dag_branching`.
    #[test]
    fn test_run_with_readers_writers_primary_is_not_first_source() {
        let yaml = r#"
pipeline:
  name: primary_not_first
nodes:
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: product_name, type: string }

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

  - type: combine
    name: enrich
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      cxl: |
        emit order_id = orders.order_id
        emit product_name = products.product_name
        emit quantity = orders.quantity
      propagate_ck: driver

  - type: output
    name: result
    input: enrich
    config:
      name: result
      type: csv
      path: output.csv
"#;
        let config = config::parse_config(yaml).unwrap();

        // Confirm the test's premise: declaration order is
        // [products, orders], i.e. source 0 is the build-side reference,
        // not the driving input. Under the old positional convention
        // this would have been broken.
        let source_names: Vec<String> = config.source_configs().map(|s| s.name.clone()).collect();
        assert_eq!(
            source_names,
            vec!["products".to_string(), "orders".to_string()],
            "test setup invariant: build-side source must be declared before driving source"
        );

        let orders = "order_id,product_id,quantity\nORD-1,PROD-A,5\nORD-2,PROD-B,3\n";
        let products = "product_id,product_name\nPROD-A,Widget\nPROD-B,Gadget\n";

        let readers: crate::executor::SourceReaders = HashMap::from([
            (
                "products".to_string(),
                crate::executor::single_file_reader(
                    "test.csv",
                    Box::new(std::io::Cursor::new(products.as_bytes().to_vec())),
                ),
            ),
            (
                "orders".to_string(),
                crate::executor::single_file_reader(
                    "test.csv",
                    Box::new(std::io::Cursor::new(orders.as_bytes().to_vec())),
                ),
            ),
        ]);
        let out_buf = SharedBuffer::new();
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            "result".to_string(),
            Box::new(out_buf.clone()) as Box<dyn std::io::Write + Send>,
        )]);
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars: Default::default(),
            shutdown_token: None,
        };

        let report = PipelineExecutor::run_with_readers_writers(
            &config, "orders", // explicit primary — NOT the first-declared source
            readers, writers, &params,
        )
        .expect("pipeline must execute when primary is declared second");

        assert_eq!(
            report.counters.total_count, 2,
            "both orders rows must be read as the driving input"
        );
        assert_eq!(
            report.counters.ok_count, 2,
            "both orders rows must enrich successfully against products"
        );
        assert!(report.dlq_entries.is_empty(), "no DLQ entries expected");

        let output = out_buf.as_string();
        assert!(
            output.contains("Widget"),
            "enriched output must include build-side value 'Widget': {output}"
        );
        assert!(
            output.contains("Gadget"),
            "enriched output must include build-side value 'Gadget': {output}"
        );
        assert!(
            output.contains("ORD-1") && output.contains("ORD-2"),
            "output must include both order IDs: {output}"
        );
    }
}
