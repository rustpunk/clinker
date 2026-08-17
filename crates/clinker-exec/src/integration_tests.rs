#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::executor::{DlqEntry, PipelineExecutor, PipelineRunParams};
    use clinker_bench_support::io::SharedBuffer;
    use clinker_plan::config;
    use clinker_plan::error::PipelineError;

    /// Helper: run executor with in-memory CSV input/output.
    fn run_pipeline(
        yaml: &str,
        csv_input: &str,
    ) -> Result<(clinker_record::PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
        let run = run_pipeline_reporting(yaml, csv_input)?;
        Ok((run.counters, run.dlq, run.output))
    }

    /// What one in-memory run produced.
    struct RunOutcome {
        counters: clinker_record::PipelineCounters,
        dlq: Vec<DlqEntry>,
        output: String,
        /// The run's advisory end-of-stream findings — the `mapping:` report.
        advisories: Vec<String>,
    }

    /// [`run_pipeline`] plus the run's advisory findings, for the tests that
    /// assert on the end-of-run `mapping:` report.
    fn run_pipeline_reporting(yaml: &str, csv_input: &str) -> Result<RunOutcome, PipelineError> {
        let config = config::parse_config(yaml).unwrap();
        let output_buf = SharedBuffer::new();

        let first_source = config.source_configs().next().unwrap().name.clone();
        let first_output = config.sink_configs().next().unwrap().name.clone();
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

        let pipeline_vars = indexmap::IndexMap::new();
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars,
            shutdown_token: None,
            ..Default::default()
        };

        let report =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params)?;

        Ok(RunOutcome {
            counters: report.counters,
            dlq: report.dlq_entries,
            output: output_buf.as_string(),
            advisories: report.advisories,
        })
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
                | PipelineError::PlanDiagnostics { .. }
                | PipelineError::OverlayDiagnostics(_)
                | PipelineError::Compilation { .. }
                | PipelineError::Internal { .. }
                | PipelineError::DispatchMismatch { .. }
                | PipelineError::SortOrderViolation { .. }
                | PipelineError::MergeSortOrderViolation { .. }
                | PipelineError::SchemaMismatch { .. }
                | PipelineError::CompositionDepthExceeded { .. }
                | PipelineError::CompositionBodyMissing { .. }
                | PipelineError::CompositionUnknownPort { .. }
                | PipelineError::CompositionBodyError { .. }
                | PipelineError::MemoryBudgetExceeded { .. }
                | PipelineError::UnsatisfiableMemoryBudget { .. }
                | PipelineError::CombineMissingMatch { .. }
                | PipelineError::CombineOutputCapExceeded { .. }
                | PipelineError::EnvelopeMultiHeaderConflict { .. }
                | PipelineError::EnvelopeHeaderGrainUnmatched { .. }
                | PipelineError::EnvelopeHeaderMultipleForGrain { .. },
            ) => 1,
            Err(
                PipelineError::Eval(_)
                | PipelineError::Accumulator { .. }
                | PipelineError::CombineRangeKeyOutOfRange { .. }
                | PipelineError::DlqRateExceeded { .. }
                | PipelineError::TypeErrorThresholdExceeded { .. },
            ) => 3,
            Err(
                PipelineError::Io(_)
                | PipelineError::Spill(_)
                | PipelineError::SpillCapExceeded { .. }
                | PipelineError::Format(_)
                | PipelineError::ThreadPool(_)
                | PipelineError::Multiple(_)
                | PipelineError::CorrelationGroupOverflow { .. },
            ) => 4,
            // A SIGINT-interrupted run reports the conventional 128+SIGINT
            // status so a shell sees the same code as an uncaught signal.
            Err(PipelineError::Interrupted) => 130,
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
    # Keep the success fixture aligned with the physical CSV. Missing
    # non-nullable declarations are source-type failures by contract.
    schema:
      - { name: name, type: string }
      - { name: age, type: int }

- type: sink
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
- type: sink
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
- type: sink
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
- type: sink
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
    - employee_name: full_name
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

    /// The written header line for a `mapping:` block, asserted as a whole
    /// rather than by `contains`. Column order is the block's declaration
    /// order, deliberately different from the upstream column order, and the
    /// one rename is spelled `output_name: source_column`.
    fn mapping_header(mapping_block: &str, include_unmapped: bool) -> String {
        let yaml = format!(
            r#"
pipeline:
  name: mapping_header
nodes:
- type: source
  name: employees
  config:
    name: employees
    type: csv
    path: test.csv
    options:
      has_header: true
    schema:
    - {{ name: first_name, type: string }}
    - {{ name: last_name, type: string }}
    - {{ name: department, type: string }}
- type: sink
  name: out
  input: employees
  config:
    name: out
    type: csv
    path: output.csv
    include_unmapped: {include_unmapped}
    mapping:
{mapping_block}
"#
        );
        let csv = "first_name,last_name,department\nAlice,Smith,Engineering\n";
        let (_, _, output) = run_pipeline(&yaml, csv).expect("pipeline must run");
        output
            .lines()
            .next()
            .expect("output must carry a header")
            .to_string()
    }

    /// Declaration order is the output column order — including when it
    /// disagrees with the order the columns arrive in.
    #[test]
    fn mapping_declaration_order_is_the_output_column_order() {
        let header = mapping_header(
            "    - department\n    - surname: last_name\n    - first_name\n",
            false,
        );
        assert_eq!(header, "department,surname,first_name");
    }

    /// `include_unmapped: false` against a partial mapping: only the listed
    /// columns are written.
    #[test]
    fn mapping_without_include_unmapped_writes_only_the_listed_columns() {
        let header = mapping_header("    - surname: last_name\n", false);
        assert_eq!(header, "surname");
    }

    /// `include_unmapped: true` against the same partial mapping: the listed
    /// column comes first, then everything the block did not claim, in its
    /// existing relative order.
    #[test]
    fn mapping_with_include_unmapped_appends_the_unlisted_columns() {
        let header = mapping_header("    - surname: last_name\n", true);
        assert_eq!(header, "surname,first_name,department");
    }

    /// One upstream column may feed two output columns. Uniqueness is required
    /// on the output side, which the map form could not express in this
    /// direction at all.
    #[test]
    fn one_source_column_may_be_written_twice_under_two_names() {
        let header = mapping_header("    - department\n    - dept: department\n", false);
        assert_eq!(header, "department,dept");
    }

    /// The shape the sequence form exists for: a wide output that renames one
    /// column. Twelve bare scalars and one pair, and the rename is the only
    /// item carrying a colon.
    #[test]
    fn a_wide_mapping_renaming_one_column_writes_the_declared_header() {
        let columns = [
            "order_id",
            "order_date",
            "customer_id",
            "channel",
            "sku",
            "quantity",
            "unit_price",
            "discount_pct",
            "gross_amount",
            "line_total",
            "ship_country",
            "status",
        ];
        let schema: String = columns
            .iter()
            .map(|c| format!("    - {{ name: {c}, type: string }}\n"))
            .collect();
        // Identity for every column but `customer_id`, which is written as
        // `sold_to`. Declared in upstream order so the assertion isolates the
        // rename rather than re-testing reordering.
        let mapping: String = columns
            .iter()
            .map(|c| {
                if *c == "customer_id" {
                    "    - sold_to: customer_id\n".to_string()
                } else {
                    format!("    - {c}\n")
                }
            })
            .collect();
        let yaml = format!(
            r#"
pipeline:
  name: wide_mapping
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: test.csv
    options:
      has_header: true
    schema:
{schema}- type: sink
  name: out
  input: orders
  config:
    name: out
    type: csv
    path: output.csv
    include_unmapped: false
    mapping:
{mapping}"#
        );
        let header_row = columns.join(",");
        let csv = format!("{header_row}\n{}\n", vec!["x"; columns.len()].join(","));

        let (_, _, output) = run_pipeline(&yaml, &csv).expect("pipeline must run");
        assert_eq!(
            output.lines().next().expect("header"),
            "order_id,order_date,sold_to,channel,sku,quantity,unit_price,discount_pct,\
             gross_amount,line_total,ship_country,status"
        );
    }

    /// A mapping entry no record resolves, end to end. The compile gate stands
    /// down on purpose — the source reserves the `auto_widen` sidecar and
    /// `include_unmapped: true` expands it, so `goes_by` might genuinely arrive,
    /// and `goes_by` is not a near-miss of any declared column.
    ///
    /// The run completes and writes the declared column, empty; the end-of-run
    /// report names the entry. Aborting instead would kill a run whose sibling
    /// Outputs have already flushed, over a fault that is visible in the file.
    #[test]
    fn a_mapping_column_no_record_supplies_writes_empty_and_is_reported() {
        let yaml = r#"
pipeline:
  name: mapping_runtime_report
nodes:
- type: source
  name: people
  config:
    name: people
    type: csv
    path: test.csv
    options:
      has_header: true
    schema:
    - { name: first_name, type: string }
- type: sink
  name: out
  input: people
  config:
    name: out
    type: csv
    path: output.csv
    include_unmapped: true
    mapping:
    - first_name
    - nickname: goes_by
"#;
        let csv = "first_name\nAlice\n";
        let run = run_pipeline_reporting(yaml, csv).expect("the run completes");
        let advisories = &run.advisories;

        assert_eq!(run.counters.records_written, 1);
        assert_eq!(
            run.output, "first_name,nickname\nAlice,\n",
            "the declared column is written, empty — the file's shape follows the block, \
             not the data"
        );
        assert_eq!(advisories.len(), 1, "{advisories:?}");
        assert!(advisories[0].contains("W365"), "{}", advisories[0]);
        assert!(
            advisories[0].contains("'goes_by'"),
            "the report names the source column the author must correct: {}",
            advisories[0]
        );
    }

    /// A record in a correlation group is not part of the delivered stream
    /// when any peer in that group fails. Its fields therefore must not count
    /// as evidence that a mapped column was populated in the written file.
    #[test]
    fn a_rejected_correlation_group_cannot_suppress_an_empty_column_report() {
        let yaml = r#"
pipeline:
  name: mapping_correlation_report
error_handling:
  strategy: continue
nodes:
- type: source
  name: people
  config:
    name: people
    type: json
    path: test.json
    correlation_key: employee_id
    schema:
    - { name: employee_id, type: string }
    - { name: value, type: string }
- type: transform
  name: parse_value
  input: people
  config:
    cxl: |
      emit employee_id = employee_id
      emit parsed_value = value.to_int()
- type: sink
  name: out
  input: parse_value
  config:
    name: out
    type: csv
    path: output.csv
    include_unmapped: true
    mapping:
    - employee_id
    - optional_copy: optional
    - displaced: employee_id
"#;
        let input = r#"[
          {"employee_id":"A","value":"100","optional":"present-only-in-rejected-group","displaced":"rejected-only"},
          {"employee_id":"A","value":"bad"},
          {"employee_id":"B","value":"200"}
        ]"#;

        let run = run_pipeline_reporting(yaml, input).expect("the run completes");
        assert_eq!(run.counters.records_written, 1);
        assert!(run.output.contains("B,"), "{}", run.output);
        assert!(!run.output.contains("A,"), "{}", run.output);
        assert_eq!(run.advisories.len(), 1, "{:?}", run.advisories);
        assert!(run.advisories[0].contains("W365"), "{}", run.advisories[0]);
        assert!(
            run.advisories[0].contains("'optional'"),
            "the only record carrying `optional` was rejected with its group: {}",
            run.advisories[0]
        );
        assert!(
            !run.advisories
                .iter()
                .any(|warning| warning.contains("W366")),
            "the only colliding passthrough was rejected with its group: {:?}",
            run.advisories
        );
    }

    /// An oversized correlation group is rejected before commit just like a
    /// dirty group. Its fields cannot resolve or collide in the written file's
    /// mapping report.
    #[test]
    fn an_overflowed_correlation_group_cannot_affect_mapping_advisories() {
        let yaml = r#"
pipeline:
  name: mapping_correlation_overflow_report
error_handling:
  strategy: continue
  max_group_buffer: 1
nodes:
- type: source
  name: people
  config:
    name: people
    type: json
    path: test.json
    correlation_key: employee_id
    schema:
    - { name: employee_id, type: string }
    - { name: value, type: string }
- type: transform
  name: passthrough
  input: people
  config:
    cxl: |
      emit employee_id = employee_id
      emit value = value
- type: sink
  name: out
  input: passthrough
  config:
    name: out
    type: csv
    path: output.csv
    include_unmapped: true
    mapping:
    - employee_id
    - optional_copy: optional
    - displaced: employee_id
"#;
        let input = r#"[
          {"employee_id":"A","value":"one","optional":"overflow-only","displaced":"overflow-only"},
          {"employee_id":"A","value":"two"},
          {"employee_id":"B","value":"clean"}
        ]"#;

        let run = run_pipeline_reporting(yaml, input).expect("the run completes");
        assert_eq!(run.counters.records_written, 1);
        assert!(run.output.contains("B,"), "{}", run.output);
        assert!(!run.output.contains("A,"), "{}", run.output);
        assert_eq!(run.advisories.len(), 1, "{:?}", run.advisories);
        assert!(run.advisories[0].contains("W365"), "{}", run.advisories[0]);
        assert!(
            run.advisories[0].contains("'optional'"),
            "the only record carrying `optional` was rejected on overflow: {}",
            run.advisories[0]
        );
        assert!(
            !run.advisories
                .iter()
                .any(|warning| warning.contains("W366")),
            "the only colliding passthrough was rejected on overflow: {:?}",
            run.advisories
        );
    }

    /// A genuinely heterogeneous stream, and the property the whole redesign
    /// exists for: the file's column set follows the `mapping:` block, not
    /// whichever record happened to arrive first.
    ///
    /// A JSON source because a CSV file's header fixes every record's column
    /// set, so it cannot express a record that lacks a column. `goes_by` is
    /// undeclared, so it reaches the sink through the `auto_widen` sidecar — on
    /// one record and not the other. A CSV sink because its header IS the
    /// projected column set, so the assertion reads it directly.
    ///
    /// Run twice, once in each record order. Under the previous skip-the-column
    /// behaviour the sparse-first order lost `nickname` for every record, since
    /// the header is derived from the first record's projection. The two runs
    /// must now agree.
    #[test]
    fn a_heterogeneous_stream_null_fills_whatever_order_records_arrive_in() {
        let yaml = r#"
pipeline:
  name: mapping_heterogeneous
nodes:
- type: source
  name: people
  config:
    name: people
    type: json
    path: test.json
    schema:
    - { name: first_name, type: string }
- type: sink
  name: out
  input: people
  config:
    name: out
    type: csv
    path: output.csv
    include_unmapped: true
    mapping:
    - first_name
    - nickname: goes_by
"#;

        let dense_first = run_pipeline_reporting(
            yaml,
            r#"[{"first_name":"Alice","goes_by":"Al"},{"first_name":"Bob"}]"#,
        )
        .expect("the run completes");
        assert_eq!(dense_first.counters.records_written, 2);
        assert_eq!(
            dense_first.output, "first_name,nickname\nAlice,Al\nBob,\n",
            "the record without the column still carries it, empty"
        );
        assert!(
            dense_first.advisories.is_empty(),
            "a column some record carried is sparse, not a typo: {:?}",
            dense_first.advisories
        );

        let sparse_first = run_pipeline_reporting(
            yaml,
            r#"[{"first_name":"Bob"},{"first_name":"Alice","goes_by":"Al"}]"#,
        )
        .expect("the run completes");
        assert_eq!(
            sparse_first.output, "first_name,nickname\nBob,\nAlice,Al\n",
            "the declared column survives a first record that does not carry it"
        );
        assert!(
            sparse_first.advisories.is_empty(),
            "{:?}",
            sparse_first.advisories
        );
    }

    /// W366 end to end. `sold_to` is not in the source's `schema:`, so it
    /// reaches the sink through the `auto_widen` sidecar and the plan gate
    /// cannot see the collision with the mapping's output name. The mapped value
    /// wins — that is deterministic and documented — and the displaced upstream
    /// column is named rather than dropped in silence.
    #[test]
    fn a_passthrough_a_mapping_output_name_displaced_is_reported() {
        let yaml = r#"
pipeline:
  name: mapping_collision_report
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: test.csv
    options:
      has_header: true
    schema:
    - { name: order_id, type: string }
    - { name: customer_id, type: string }
- type: sink
  name: out
  input: orders
  config:
    name: out
    type: csv
    path: output.csv
    include_unmapped: true
    mapping:
    - order_id
    - sold_to: customer_id
"#;
        let csv = "order_id,customer_id,sold_to\nA-1,C-9,stale\n";
        let run = run_pipeline_reporting(yaml, csv).expect("the run completes");
        let advisories = &run.advisories;

        assert_eq!(
            run.output, "order_id,sold_to\nA-1,C-9\n",
            "one `sold_to` column, carrying the MAPPED value"
        );
        assert_eq!(advisories.len(), 1, "{advisories:?}");
        assert!(advisories[0].contains("W366"), "{}", advisories[0]);
        assert!(
            advisories[0].contains("'sold_to'"),
            "the report names the displaced upstream column: {}",
            advisories[0]
        );
    }

    /// The narrower contrast, on the CSV path where every record shares one
    /// column set: a column present but EMPTY on a record still resolves. An
    /// empty value is not an absent column, so the report stays quiet.
    #[test]
    fn a_column_some_record_supplies_is_not_reported() {
        let yaml = r#"
pipeline:
  name: mapping_sparse
nodes:
- type: source
  name: people
  config:
    name: people
    type: csv
    path: test.csv
    options:
      has_header: true
    on_unmapped:
      mode: drop
    schema:
    - { name: first_name, type: string }
    - { name: nickname, type: string }
- type: sink
  name: out
  input: people
  config:
    name: out
    type: csv
    path: output.csv
    include_unmapped: false
    mapping:
    - first_name
    - goes_by: nickname
"#;
        let csv = "first_name,nickname\nAlice,Al\nBob,\n";
        let run = run_pipeline_reporting(yaml, csv).expect("the run completes");
        assert_eq!(run.output, "first_name,goes_by\nAlice,Al\nBob,\n");
        assert!(
            run.advisories.is_empty(),
            "an empty value is not an absent column: {:?}",
            run.advisories
        );
    }

    /// Over-rejection guard for the same gate: a mapping every record can
    /// satisfy runs clean and writes the declared header.
    #[test]
    fn a_mapping_the_stream_satisfies_runs_clean() {
        let header = mapping_header("    - surname: last_name\n    - first_name\n", false);
        assert_eq!(header, "surname,first_name");
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
- type: sink
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

    /// SIGINT latency: a long-running pipeline must observe a tripped
    /// shutdown token and unwind promptly, not run to natural completion.
    ///
    /// A clean run never trips shutdown, so the example-pipeline golden
    /// diff cannot exercise this path — it is gated here instead.
    ///
    /// The source pages ~6000 rows behind a per-row delay so an
    /// uninterrupted run would take many seconds. The executor runs on a
    /// worker thread; the main thread trips the token after a short head
    /// start and asserts the run unwinds well inside the documented
    /// shutdown bound (the dispatch loop polls the token at chunk
    /// boundaries, so latency is bounded by one chunk's processing time).
    /// The report flags the run interrupted, which the CLI maps to exit
    /// code 130.
    #[test]
    fn interrupted_long_run_terminates_within_bound() {
        use crate::executor::{
            ExecutionReport, PipelineExecutor, PipelineRunParams, SourceReaders,
        };
        use crate::pipeline::shutdown::ShutdownToken;
        use clinker_bench_support::io::{SharedBuffer, slow_reader};
        use std::time::Duration;

        let yaml = r#"
pipeline:
  name: slow_run
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: int }
- type: transform
  name: t1
  input: src
  config:
    cxl: emit id = id
- type: sink
  name: dest
  input: t1
  config:
    name: dest
    type: csv
    path: output.csv
"#;
        let config = config::parse_config(yaml).unwrap();

        let mut csv = String::from("id\n");
        for i in 0..6000 {
            csv.push_str(&format!("{i}\n"));
        }

        // 1 ms per row over 6000 rows ⇒ a clean run takes ~6 s; the
        // interrupted run must return far sooner.
        let readers: SourceReaders = HashMap::from([(
            "src".to_string(),
            crate::executor::single_file_reader(
                "input.csv",
                slow_reader(&csv, Duration::from_millis(1)),
            ),
        )]);
        let output_buf = SharedBuffer::new();
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            "dest".to_string(),
            Box::new(output_buf.clone()) as Box<dyn std::io::Write + Send>,
        )]);

        let token = ShutdownToken::detached();
        let token_for_run = token.clone();

        let (done_tx, done_rx) = std::sync::mpsc::channel::<ExecutionReport>();
        let worker = std::thread::spawn(move || {
            let params = PipelineRunParams {
                execution_id: "sigint-exec".to_string(),
                batch_id: "sigint-batch".to_string(),
                pipeline_vars: indexmap::IndexMap::new(),
                shutdown_token: Some(token_for_run),
                ..Default::default()
            };
            let report = PipelineExecutor::run_with_readers_writers(
                &config,
                readers,
                writers.into(),
                &params,
            )
            .expect("an interrupted run drains gracefully and returns Ok");
            let _ = done_tx.send(report);
        });

        // Let the run page a few hundred rows, then signal shutdown.
        std::thread::sleep(Duration::from_millis(300));
        token.request();

        // The recv_timeout is only an anti-hang guard, generous because one
        // batch's drain is gated by per-row `thread::sleep` granularity and so
        // runs several-fold slower on macOS than Linux.
        let report = done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("interrupted run must terminate within the shutdown bound");
        worker.join().expect("executor thread did not panic");

        assert!(
            report.interrupted,
            "report must flag the run interrupted so the CLI maps it to exit 130"
        );
        // Promptness and early-stop are asserted by row count, which is
        // identical across platforms — unlike wall-clock, where the per-row
        // sleep is several-fold slower on macOS. A run that honors the token
        // stops at the next batch boundary, draining at most a couple of
        // in-flight batches past the few hundred rows it had paged, well short
        // of the full 6000-row input; one that ignores it ingests all 6000.
        assert!(
            report.counters.total_count < 5000,
            "interrupted run ingested {} of 6000 rows; expected to stop within \
             a couple of batches of the shutdown signal",
            report.counters.total_count
        );
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
            "pipeline:\n  name: filter_test\nnodes:\n  - type: source\n    name: src\n    config:\n      name: src\n      type: csv\n      path: input.csv\n      schema:\n        - {{ name: id, type: any }}\n        - {{ name: name, type: any }}\n        - {{ name: status, type: any }}\n        - {{ name: value, type: any }}\n        - {{ name: amount, type: any }}\n        - {{ name: category, type: any }}\n        - {{ name: code, type: any }}\n        - {{ name: dept, type: any }}\n        - {{ name: department, type: any }}\n        - {{ name: priority, type: any }}\n        - {{ name: optional, type: any }}\n        - {{ name: required, type: any }}\n        - {{ name: active, type: any }}\n        - {{ name: first, type: any }}\n        - {{ name: last, type: any }}\n        - {{ name: first_name, type: any }}\n  - type: transform\n    name: t1\n    input: src\n    config:\n      cxl: |\n{indented}\n  - type: sink\n    name: dest\n    input: t1\n    config:\n      name: dest\n      type: csv\n      path: output.csv\n"
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
  # Continue may retain a rejected row only through an explicit DLQ.
  dlq:
    path: rejected.csv
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    # Declare only physical fields so this fixture reaches the predicate
    # failure it is intended to exercise.
    schema:
      - { name: name, type: string }
      - { name: amount, type: string }

- type: transform
  name: t1
  input: src
  config:
    cxl: 'filter amount.to_int() > 0

      emit out_name = name

      '
- type: sink
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
        let (counters, _, _output) = run_pipeline(&yaml, csv).unwrap();
        // Row 1: empty string "", Row 2: "foo", Row 3: empty string "" (dup), Row 4: "bar"
        assert_eq!(counters.ok_count, 3); // "", "foo", "bar"
        assert_eq!(counters.distinct_count, 1); // second ""
    }

    // ── Combine enrichment coverage lives in
    //     `crates/clinker-exec/tests/combine_test.rs`. ──

    // ── Source-ingest contract tests ──
    //
    // These gate tests pin down the unified-ingest contract: every
    // declared source must have a registered reader, and declaration
    // order is irrelevant (no "primary" asymmetry).

    /// A declared source missing from the `readers` HashMap must
    /// surface as `Config(ConfigError::Validation(..))` — silent skip
    /// would be a regression surface (the previous non-primary preload
    /// passes returned `Ok(None)` on missing readers; with one
    /// unified ingest pass that's no longer a defensible default).
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
- type: sink
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
            ..Default::default()
        };

        let result =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params);

        match result {
            Err(PipelineError::Config(clinker_plan::config::ConfigError::Validation(msg))) => {
                assert!(
                    msg.contains("no reader registered for source 'src'"),
                    "expected missing-reader message, got: {msg}"
                );
            }
            other => panic!("expected Config(Validation) for missing reader, got: {other:?}"),
        }
    }

    /// Regression-proofing test: declare sources in the order
    /// `[reference, driving]` (so `source_configs[0]` is the reference
    /// table, not the driving input), and verify the pipeline runs
    /// correctly end-to-end.
    ///
    /// Under the old positional-primary convention this configuration
    /// would have consumed `products` as the primary driving reader
    /// and starved the combine build side. With unified ingest there
    /// is no "primary" — every source is ingested through its own
    /// `SourceIngestChannel` and dispatch order follows the plan
    /// topology, so declaration order is irrelevant.
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

  - type: sink
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
            ..Default::default()
        };

        let report =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params)
                .expect("pipeline must execute regardless of source declaration order");

        assert_eq!(
            report.counters.total_count, 4,
            "every ingested record (2 orders + 2 products) contributes to total_count"
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
