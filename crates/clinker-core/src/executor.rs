use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;

use clinker_record::{PipelineCounters, Record, Schema, Value};
use indexmap::IndexMap;

use crate::config::{ErrorStrategy, OutputConfig, PipelineConfig, TransformConfig};
use crate::error::PipelineError;
use crate::projection::project_output;
use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
use clinker_format::csv::writer::{CsvWriter, CsvWriterConfig};
use clinker_format::traits::{FormatReader, FormatWriter};
use cxl::eval::{EvalContext, WallClock};
use cxl::typecheck::{Type, TypedProgram};

/// Compiled transform: CXL source compiled once, evaluated per record.
struct CompiledTransform {
    #[allow(dead_code)]
    name: String,
    typed: Arc<TypedProgram>,
}

/// Record that failed evaluation, queued for DLQ output.
pub struct DlqEntry {
    pub source_row: u64,
    pub error_message: String,
    pub original_record: Record,
}

/// Single-pass streaming executor. CSV in → CXL eval → CSV out.
pub struct StreamingExecutor;

impl StreamingExecutor {
    /// Run with explicit reader/writer sources.
    pub fn run_with_readers_writers<R: Read + Send, W: Write + Send>(
        config: &PipelineConfig,
        reader: R,
        writer: W,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>), PipelineError> {
        // Build CSV reader config from input options
        let input = &config.inputs[0];
        let reader_config = build_reader_config(input);
        let mut csv_reader = CsvReader::from_reader(reader, reader_config);

        // Get schema from reader
        let schema = csv_reader.schema()?;

        // Compile CXL transforms against the schema
        let transforms = Self::compile_transforms(&config.transformations, &schema)?;

        // Build CSV writer config from output options
        let output = &config.outputs[0];
        let writer_config = build_writer_config(output);

        // Read the first record to determine output schema
        let first_record = csv_reader.next_record()?;
        let first_record = match first_record {
            Some(r) => r,
            None => {
                // Empty input — write nothing
                return Ok((PipelineCounters::default(), Vec::new()));
            }
        };

        // Process first record to determine output schema
        let ctx = build_eval_context(config, &input.path, 1);
        let first_result = evaluate_record(&first_record, &transforms, &ctx);

        let (output_schema, first_projected) = match &first_result {
            Ok(emitted) => {
                let projected = project_output(&first_record, emitted, output);
                let schema = Arc::clone(projected.schema());
                (schema, Some(projected))
            }
            Err(_) => {
                // If first record fails, use input schema for output
                if config.error_handling.strategy == ErrorStrategy::FailFast {
                    return Err(first_result.unwrap_err().into());
                }
                (Arc::clone(&schema), None)
            }
        };

        let mut csv_writer = CsvWriter::new(writer, Arc::clone(&output_schema), writer_config);
        let mut counters = PipelineCounters::default();
        let mut dlq_entries = Vec::new();
        let strategy = config.error_handling.strategy;

        // Process first record
        counters.total_count += 1;
        match first_result {
            Ok(_emitted) => {
                let projected = first_projected.unwrap();
                csv_writer.write_record(&projected)?;
                counters.ok_count += 1;
            }
            Err(eval_err) => {
                handle_error(
                    strategy,
                    &first_record,
                    1,
                    &eval_err,
                    &mut counters,
                    &mut dlq_entries,
                    output,
                    &output_schema,
                    &mut csv_writer,
                )?;
            }
        }

        // Stream remaining records
        let mut row_num: u64 = 2;
        while let Some(record) = csv_reader.next_record()? {
            counters.total_count += 1;
            let ctx = build_eval_context(config, &input.path, row_num);

            match evaluate_record(&record, &transforms, &ctx) {
                Ok(emitted) => {
                    let projected = project_output(&record, &emitted, output);
                    csv_writer.write_record(&projected)?;
                    counters.ok_count += 1;
                }
                Err(eval_err) => {
                    handle_error(
                        strategy,
                        &record,
                        row_num,
                        &eval_err,
                        &mut counters,
                        &mut dlq_entries,
                        output,
                        &output_schema,
                        &mut csv_writer,
                    )?;
                }
            }
            row_num += 1;
        }

        csv_writer.flush()?;
        Ok((counters, dlq_entries))
    }

    /// Compile all CXL transform blocks against the input schema.
    fn compile_transforms(
        transforms: &[TransformConfig],
        schema: &Arc<Schema>,
    ) -> Result<Vec<CompiledTransform>, PipelineError> {
        let fields: Vec<&str> = schema.columns().iter().map(|c| c.as_ref()).collect();
        // CSV fields are all strings at the data level, but CXL allows
        // polymorphic usage (e.g. `+` for concat, `.to_int()` for coercion).
        // Declare as Any so the type checker doesn't reject valid CXL.
        let type_schema: HashMap<String, Type> = fields
            .iter()
            .map(|f| (f.to_string(), Type::Any))
            .collect();

        let mut compiled = Vec::with_capacity(transforms.len());
        for t in transforms {
            let parse_result = cxl::parser::Parser::parse(&t.cxl);
            if !parse_result.errors.is_empty() {
                let messages: Vec<String> = parse_result
                    .errors
                    .iter()
                    .map(|e| e.message.clone())
                    .collect();
                return Err(PipelineError::Compilation {
                    transform_name: t.name.clone(),
                    messages,
                });
            }

            let resolved = cxl::resolve::resolve_program(
                parse_result.ast,
                &fields,
                parse_result.node_count,
            )
            .map_err(|diags| PipelineError::Compilation {
                transform_name: t.name.clone(),
                messages: diags.into_iter().map(|d| d.message).collect(),
            })?;

            let typed = cxl::typecheck::type_check(resolved, &type_schema).map_err(|diags| {
                // Filter to errors only (not warnings)
                let errors: Vec<String> = diags
                    .iter()
                    .filter(|d| !d.is_warning)
                    .map(|d| d.message.clone())
                    .collect();
                if errors.is_empty() {
                    // Only warnings — this shouldn't happen but be safe
                    return PipelineError::Compilation {
                        transform_name: t.name.clone(),
                        messages: diags.into_iter().map(|d| d.message).collect(),
                    };
                }
                PipelineError::Compilation {
                    transform_name: t.name.clone(),
                    messages: errors,
                }
            })?;

            compiled.push(CompiledTransform {
                name: t.name.clone(),
                typed: Arc::new(typed),
            });
        }
        Ok(compiled)
    }
}

/// Build a CsvReaderConfig from the input config options.
fn build_reader_config(input: &crate::config::InputConfig) -> CsvReaderConfig {
    let mut config = CsvReaderConfig::default();
    if let Some(ref opts) = input.options {
        if let Some(ref d) = opts.delimiter {
            if let Some(b) = d.as_bytes().first() {
                config.delimiter = *b;
            }
        }
        if let Some(ref q) = opts.quote_char {
            if let Some(b) = q.as_bytes().first() {
                config.quote_char = *b;
            }
        }
        if let Some(h) = opts.has_header {
            config.has_header = h;
        }
    }
    config
}

/// Build a CsvWriterConfig from the output config options.
fn build_writer_config(output: &OutputConfig) -> CsvWriterConfig {
    let mut config = CsvWriterConfig::default();
    if let Some(h) = output.include_header {
        config.include_header = h;
    }
    if let Some(ref opts) = output.options {
        if let Some(ref d) = opts.delimiter {
            if let Some(b) = d.as_bytes().first() {
                config.delimiter = *b;
            }
        }
    }
    config
}

/// Build an EvalContext for a given record.
fn build_eval_context(config: &PipelineConfig, source_file: &str, source_row: u64) -> EvalContext {
    EvalContext {
        clock: Box::new(WallClock),
        pipeline_start_time: chrono::Local::now().naive_local(),
        pipeline_name: config.pipeline.name.clone(),
        pipeline_execution_id: String::new(),
        pipeline_counters: PipelineCounters::default(),
        source_file: source_file.into(),
        source_row,
        pipeline_vars: IndexMap::new(),
    }
}

/// Evaluate all transforms against a single record, accumulating emitted fields.
fn evaluate_record(
    record: &Record,
    transforms: &[CompiledTransform],
    ctx: &EvalContext,
) -> Result<IndexMap<String, Value>, cxl::eval::EvalError> {
    let mut all_emitted = IndexMap::new();
    for t in transforms {
        let emitted = cxl::eval::eval_program(&t.typed, ctx, record, None)?;
        all_emitted.extend(emitted);
    }
    Ok(all_emitted)
}

/// Handle an evaluation error according to the configured strategy.
#[allow(clippy::too_many_arguments)]
fn handle_error<W: Write + Send>(
    strategy: ErrorStrategy,
    record: &Record,
    row_num: u64,
    eval_err: &cxl::eval::EvalError,
    counters: &mut PipelineCounters,
    dlq_entries: &mut Vec<DlqEntry>,
    output: &OutputConfig,
    _output_schema: &Arc<Schema>,
    writer: &mut CsvWriter<W>,
) -> Result<(), PipelineError> {
    match strategy {
        ErrorStrategy::FailFast => {
            return Err(PipelineError::Eval(cxl::eval::EvalError {
                span: eval_err.span,
                kind: eval_err.kind.clone(),
            }));
        }
        ErrorStrategy::Continue => {
            counters.dlq_count += 1;
            dlq_entries.push(DlqEntry {
                source_row: row_num,
                error_message: eval_err.to_string(),
                original_record: record.clone(),
            });
            // Skip this record — don't write to output
        }
        ErrorStrategy::BestEffort => {
            counters.dlq_count += 1;
            dlq_entries.push(DlqEntry {
                source_row: row_num,
                error_message: eval_err.to_string(),
                original_record: record.clone(),
            });
            // Write original record unchanged
            let emitted = IndexMap::new();
            let projected = project_output(record, &emitted, output);
            writer.write_record(&projected)?;
            counters.ok_count += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: run executor with in-memory CSV input/output.
    fn run_test(
        yaml: &str,
        csv_input: &str,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
        let config = crate::config::parse_config(yaml).unwrap();
        let reader = std::io::Cursor::new(csv_input.as_bytes().to_vec());
        let mut output_buf: Vec<u8> = Vec::new();

        let (counters, dlq) =
            StreamingExecutor::run_with_readers_writers(&config, reader, &mut output_buf)?;

        let output = String::from_utf8(output_buf).unwrap();
        Ok((counters, dlq, output))
    }

    #[test]
    fn test_executor_identity_passthrough() {
        let yaml = r#"
pipeline:
  name: identity

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
        let (counters, dlq, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.total_count, 2);
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.dlq_count, 0);
        assert!(dlq.is_empty());
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }

    #[test]
    fn test_executor_cxl_string_concat() {
        let yaml = r#"
pipeline:
  name: concat

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
  - name: concat_name
    cxl: |
      emit full_name = first_name + " " + last_name
"#;
        let csv = "first_name,last_name\nAlice,Smith\nBob,Jones\n";
        let (counters, _, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert!(output.contains("Alice Smith"));
        assert!(output.contains("Bob Jones"));
    }

    #[test]
    fn test_executor_cxl_arithmetic() {
        let yaml = r#"
pipeline:
  name: arithmetic

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
  - name: calc_total
    cxl: |
      emit total = price.to_float() * quantity.to_float()
"#;
        let csv = "price,quantity\n10.5,3\n20.0,2\n";
        let (counters, _, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert!(output.contains("31.5"));
        assert!(output.contains("40"));
    }

    #[test]
    fn test_executor_cxl_conditional() {
        let yaml = r#"
pipeline:
  name: conditional

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
  - name: categorize
    cxl: |
      emit category = if age.to_int() >= 18 then "adult" else "minor"
"#;
        let csv = "name,age\nAlice,30\nBob,15\n";
        let (counters, _, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert!(output.contains("adult"));
        assert!(output.contains("minor"));
    }

    #[test]
    fn test_executor_field_mapping() {
        let yaml = r#"
pipeline:
  name: mapping

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
    mapping:
      name: employee_name

transformations: []
"#;
        let csv = "name,age\nAlice,30\n";
        let (_, _, output) = run_test(yaml, csv).unwrap();
        assert!(output.contains("employee_name"));
        assert!(!output.contains("\nname"));  // header should be renamed
    }

    #[test]
    fn test_executor_exclude_fields() {
        let yaml = r#"
pipeline:
  name: exclude

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
    exclude:
      - secret

transformations: []
"#;
        let csv = "name,secret,age\nAlice,password,30\n";
        let (_, _, output) = run_test(yaml, csv).unwrap();
        assert!(output.contains("name"));
        assert!(output.contains("age"));
        assert!(!output.contains("secret"));
        assert!(!output.contains("password"));
    }

    #[test]
    fn test_executor_projection_order() {
        // Gather (include_unmapped) → exclude → mapping
        let yaml = r#"
pipeline:
  name: projection

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
    exclude:
      - secret
    mapping:
      name: full_name

transformations: []
"#;
        let csv = "name,secret,age\nAlice,password,30\n";
        let (_, _, output) = run_test(yaml, csv).unwrap();
        // name renamed to full_name, secret excluded, age kept
        assert!(output.contains("full_name"));
        assert!(output.contains("age"));
        assert!(!output.contains("secret"));
        assert!(!output.contains("password"));
    }

    #[test]
    fn test_executor_fail_fast_aborts() {
        let yaml = r#"
pipeline:
  name: failfast

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
        // "bad" can't be converted to int
        let csv = "value\n10\nbad\n20\n";
        let result = run_test(yaml, csv);
        assert!(result.is_err());
    }

    #[test]
    fn test_executor_continue_skips() {
        let yaml = r#"
pipeline:
  name: continue_skip

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
      emit result = value.to_int() + 1

error_handling:
  strategy: continue
"#;
        let csv = "value\n10\nbad\n20\n";
        let (counters, dlq, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.total_count, 3);
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.dlq_count, 1);
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].source_row, 2);
        // Output should have rows 1 and 3 but not row 2
        assert!(output.contains("11"));
        assert!(output.contains("21"));
        assert!(!output.contains("bad"));
    }

    #[test]
    fn test_executor_best_effort_preserves() {
        let yaml = r#"
pipeline:
  name: best_effort

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
      emit result = value.to_int() + 1

error_handling:
  strategy: best_effort
"#;
        let csv = "value\n10\nbad\n20\n";
        let (counters, dlq, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.total_count, 3);
        // best_effort: ok_count includes error rows (original values preserved)
        assert_eq!(counters.ok_count, 3);
        assert_eq!(counters.dlq_count, 1);
        assert_eq!(dlq.len(), 1);
        // "bad" row should still appear in output with original value
        assert!(output.contains("bad"));
    }

    #[test]
    fn test_executor_pipeline_counters() {
        let yaml = r#"
pipeline:
  name: counters

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
        // 10 rows, rows 3 and 7 have "bad" values
        let mut rows = Vec::new();
        rows.push("value".to_string());
        for i in 1..=10 {
            if i == 3 || i == 7 {
                rows.push("bad".to_string());
            } else {
                rows.push(i.to_string());
            }
        }
        let csv = rows.join("\n") + "\n";

        let (counters, dlq, _) = run_test(yaml, &csv).unwrap();
        assert_eq!(counters.total_count, 10);
        assert_eq!(counters.ok_count, 8);
        assert_eq!(counters.dlq_count, 2);
        assert_eq!(dlq.len(), 2);
    }
}
