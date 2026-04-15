//! Multi-record dispatcher — reads one file, produces N arenas.
//!
//! For files containing heterogeneous record types (HEADER, DETAIL, TRAILER),
//! the discriminator field identifies the record type per line, routing
//! records to per-type arenas with independent schemas.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::schema_def::{Discriminator, FieldDef, RecordTypeDef};
use clinker_record::{MinimalRecord, Schema, Value};

use crate::error::PipelineError;
use crate::pipeline::arena::Arena;
use crate::schema::SchemaError;

/// Multi-record dispatcher — reads one file, produces N arenas.
pub struct MultiRecordDispatcher {
    discriminator: Discriminator,
    record_types: Vec<RecordTypeDef>,
    fail_fast: bool,
}

/// Result of dispatching: per-record-type arena.
#[derive(Debug)]
pub struct DispatchResult {
    pub arenas: HashMap<String, Arena>,
    pub warnings: Vec<String>,
}

impl MultiRecordDispatcher {
    pub fn new(
        discriminator: Discriminator,
        record_types: Vec<RecordTypeDef>,
        fail_fast: bool,
    ) -> Self {
        Self {
            discriminator,
            record_types,
            fail_fast,
        }
    }

    /// Dispatch fixed-width lines into per-type arenas.
    /// Each line is classified by the discriminator byte range.
    pub fn dispatch_lines(&self, lines: &[Vec<u8>]) -> Result<DispatchResult, PipelineError> {
        // Build tag → record type index map
        let tag_map: HashMap<&str, usize> = self
            .record_types
            .iter()
            .enumerate()
            .map(|(i, rt)| (rt.tag.as_str(), i))
            .collect();

        // Accumulate raw lines per record type
        let mut buckets: Vec<Vec<&[u8]>> = vec![Vec::new(); self.record_types.len()];
        let mut warnings = Vec::new();

        for (line_num, line) in lines.iter().enumerate() {
            let tag = self.extract_tag(line, line_num)?;

            if let Some(&idx) = tag_map.get(tag.as_str()) {
                buckets[idx].push(line);
            } else if self.fail_fast {
                return Err(PipelineError::Schema(SchemaError::Validation(format!(
                    "line {}: unmatched discriminator tag '{}' — fail_fast is enabled",
                    line_num + 1,
                    tag
                ))));
            } else {
                warnings.push(format!(
                    "line {}: unmatched discriminator tag '{}', skipping",
                    line_num + 1,
                    tag
                ));
            }
        }

        // Build per-type arenas from accumulated records
        let mut arenas = HashMap::new();
        for (i, rt) in self.record_types.iter().enumerate() {
            let arena = self.build_arena_from_lines(&rt.fields, &buckets[i])?;
            arenas.insert(rt.id.clone(), arena);
        }

        Ok(DispatchResult { arenas, warnings })
    }

    /// Dispatch CSV records (pre-parsed) into per-type arenas.
    /// Discriminator uses a column value to classify records.
    pub fn dispatch_records(
        &self,
        records: &[(HashMap<String, Value>, Vec<u8>)],
    ) -> Result<DispatchResult, PipelineError> {
        let column = self.discriminator.field.as_deref().ok_or_else(|| {
            PipelineError::Schema(SchemaError::Validation(
                "CSV discriminator requires 'field' property".into(),
            ))
        })?;

        let tag_map: HashMap<&str, usize> = self
            .record_types
            .iter()
            .enumerate()
            .map(|(i, rt)| (rt.tag.as_str(), i))
            .collect();

        let mut buckets: Vec<Vec<HashMap<String, Value>>> =
            vec![Vec::new(); self.record_types.len()];
        let mut warnings = Vec::new();

        for (line_num, (record, _raw)) in records.iter().enumerate() {
            let tag_value = record
                .get(column)
                .map(|v| match v {
                    Value::String(s) => s.trim().to_string(),
                    other => format!("{:?}", other),
                })
                .unwrap_or_default();

            if let Some(&idx) = tag_map.get(tag_value.as_str()) {
                buckets[idx].push(record.clone());
            } else if self.fail_fast {
                return Err(PipelineError::Schema(SchemaError::Validation(format!(
                    "line {}: unmatched discriminator tag '{}' — fail_fast is enabled",
                    line_num + 1,
                    tag_value
                ))));
            } else {
                warnings.push(format!(
                    "line {}: unmatched discriminator tag '{}', skipping",
                    line_num + 1,
                    tag_value
                ));
            }
        }

        let mut arenas = HashMap::new();
        for (i, rt) in self.record_types.iter().enumerate() {
            let arena = self.build_arena_from_records(&rt.fields, &buckets[i])?;
            arenas.insert(rt.id.clone(), arena);
        }

        Ok(DispatchResult { arenas, warnings })
    }

    /// Extract discriminator tag from a raw line (fixed-width byte range).
    fn extract_tag(&self, line: &[u8], line_num: usize) -> Result<String, PipelineError> {
        let start = self.discriminator.start.ok_or_else(|| {
            PipelineError::Schema(SchemaError::Validation(
                "fixed-width discriminator requires 'start' property".into(),
            ))
        })?;
        let width = self.discriminator.width.unwrap_or(1);
        let end = start + width;

        let slice = line.get(start..end).ok_or_else(|| {
            PipelineError::Schema(SchemaError::Validation(format!(
                "line {}: too short for discriminator: need {} bytes, got {}",
                line_num + 1,
                end,
                line.len()
            )))
        })?;

        let tag = std::str::from_utf8(slice).map_err(|_| {
            PipelineError::Schema(SchemaError::Validation(format!(
                "line {}: invalid UTF-8 in discriminator range",
                line_num + 1
            )))
        })?;

        Ok(tag.trim().to_string())
    }

    /// Build an Arena from raw fixed-width lines and field definitions.
    fn build_arena_from_lines(
        &self,
        fields: &[FieldDef],
        lines: &[&[u8]],
    ) -> Result<Arena, PipelineError> {
        let columns: Vec<Box<str>> = fields.iter().map(|f| f.name.as_str().into()).collect();
        let schema = Arc::new(Schema::new(columns));

        let mut records = Vec::with_capacity(lines.len());
        for line in lines {
            let mut values = Vec::with_capacity(fields.len());
            for field in fields {
                let start = field.start.unwrap_or(0);
                let width = field
                    .width
                    .or_else(|| field.end.map(|e| e.saturating_sub(start)))
                    .unwrap_or(0);
                let end = start + width;

                let raw = if end <= line.len() {
                    &line[start..end]
                } else if start < line.len() {
                    &line[start..]
                } else {
                    b""
                };

                let s = std::str::from_utf8(raw)
                    .map_err(|e| {
                        PipelineError::Schema(SchemaError::Validation(format!(
                            "field '{}': invalid UTF-8: {e}",
                            field.name
                        )))
                    })?
                    .trim();

                if s.is_empty() {
                    values.push(Value::Null);
                } else {
                    values.push(Value::String(s.into()));
                }
            }
            records.push(MinimalRecord::new(values));
        }

        Ok(Arena::from_parts(schema, records))
    }

    /// Build an Arena from pre-parsed CSV records and field definitions.
    fn build_arena_from_records(
        &self,
        fields: &[FieldDef],
        records: &[HashMap<String, Value>],
    ) -> Result<Arena, PipelineError> {
        let columns: Vec<Box<str>> = fields.iter().map(|f| f.name.as_str().into()).collect();
        let schema = Arc::new(Schema::new(columns));

        let mut arena_records = Vec::with_capacity(records.len());
        for record in records {
            let mut values = Vec::with_capacity(fields.len());
            for field in fields {
                let value = record.get(&field.name).cloned().unwrap_or(Value::Null);
                values.push(value);
            }
            arena_records.push(MinimalRecord::new(values));
        }

        Ok(Arena::from_parts(schema, arena_records))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::schema_def::{Discriminator, FieldDef, RecordTypeDef};

    fn field(name: &str) -> FieldDef {
        FieldDef {
            name: name.into(),
            field_type: None,
            required: None,
            format: None,
            coerce: None,
            default: None,
            allowed_values: None,
            alias: None,
            inherits: None,
            start: None,
            width: None,
            end: None,
            justify: None,
            pad: None,
            trim: None,
            truncation: None,
            precision: None,
            scale: None,
            path: None,
            drop: None,
            record: None,
        }
    }

    fn make_record_types() -> Vec<RecordTypeDef> {
        vec![
            RecordTypeDef {
                id: "HEADER".into(),
                tag: "H".into(),
                description: None,
                fields: vec![{
                    let mut f = field("title");
                    f.start = Some(1);
                    f.width = Some(9);
                    f
                }],
                parent: None,
                join_key: None,
            },
            RecordTypeDef {
                id: "DETAIL".into(),
                tag: "D".into(),
                description: None,
                fields: vec![
                    {
                        let mut f = field("id");
                        f.start = Some(1);
                        f.width = Some(5);
                        f
                    },
                    {
                        let mut f = field("amount");
                        f.start = Some(6);
                        f.width = Some(4);
                        f
                    },
                ],
                parent: None,
                join_key: None,
            },
            RecordTypeDef {
                id: "TRAILER".into(),
                tag: "T".into(),
                description: None,
                fields: vec![{
                    let mut f = field("count");
                    f.start = Some(1);
                    f.width = Some(5);
                    f
                }],
                parent: None,
                join_key: None,
            },
        ]
    }

    fn fw_discriminator() -> Discriminator {
        Discriminator {
            start: Some(0),
            width: Some(1),
            field: None,
        }
    }

    #[test]
    fn test_dispatch_fixedwidth_discriminator() {
        let dispatcher = MultiRecordDispatcher::new(fw_discriminator(), make_record_types(), false);
        let lines: Vec<Vec<u8>> = vec![
            b"HREPORT   ".to_vec(),
            b"D00001 100".to_vec(),
            b"D00002 200".to_vec(),
            b"T00002    ".to_vec(),
        ];

        let result = dispatcher.dispatch_lines(&lines).unwrap();
        assert_eq!(result.arenas["HEADER"].len(), 1);
        assert_eq!(result.arenas["DETAIL"].len(), 2);
        assert_eq!(result.arenas["TRAILER"].len(), 1);
    }

    #[test]
    fn test_dispatch_csv_discriminator() {
        let record_types = vec![
            RecordTypeDef {
                id: "HEADER".into(),
                tag: "header".into(),
                description: None,
                fields: vec![field("title")],
                parent: None,
                join_key: None,
            },
            RecordTypeDef {
                id: "DETAIL".into(),
                tag: "detail".into(),
                description: None,
                fields: vec![field("id"), field("amount")],
                parent: None,
                join_key: None,
            },
        ];
        let discriminator = Discriminator {
            start: None,
            width: None,
            field: Some("rec_type".into()),
        };
        let dispatcher = MultiRecordDispatcher::new(discriminator, record_types, false);

        let records: Vec<(HashMap<String, Value>, Vec<u8>)> = vec![
            (
                {
                    let mut m = HashMap::new();
                    m.insert("rec_type".into(), Value::String("header".into()));
                    m.insert("title".into(), Value::String("Report".into()));
                    m
                },
                vec![],
            ),
            (
                {
                    let mut m = HashMap::new();
                    m.insert("rec_type".into(), Value::String("detail".into()));
                    m.insert("id".into(), Value::String("1".into()));
                    m.insert("amount".into(), Value::String("100".into()));
                    m
                },
                vec![],
            ),
        ];

        let result = dispatcher.dispatch_records(&records).unwrap();
        assert_eq!(result.arenas["HEADER"].len(), 1);
        assert_eq!(result.arenas["DETAIL"].len(), 1);
    }

    #[test]
    fn test_dispatch_trim_tag() {
        let _dispatcher =
            MultiRecordDispatcher::new(fw_discriminator(), make_record_types(), false);
        // Tag "D " (with trailing space) should match "D" after trim
        let lines: Vec<Vec<u8>> = vec![b"D 00001100".to_vec()];
        // Discriminator is start=0, width=1, so it extracts just "D" (1 byte)
        // For trim test, use width=2 to include the space
        let disc = Discriminator {
            start: Some(0),
            width: Some(2),
            field: None,
        };
        let dispatcher = MultiRecordDispatcher::new(disc, make_record_types(), false);
        let result = dispatcher.dispatch_lines(&lines).unwrap();
        assert_eq!(result.arenas["DETAIL"].len(), 1);
    }

    #[test]
    fn test_dispatch_separate_arenas() {
        let dispatcher = MultiRecordDispatcher::new(fw_discriminator(), make_record_types(), false);
        let lines: Vec<Vec<u8>> = vec![b"HREPORT   ".to_vec(), b"D00001 100".to_vec()];
        let result = dispatcher.dispatch_lines(&lines).unwrap();

        // Each record type has its own arena with its own schema
        let header = &result.arenas["HEADER"];
        let detail = &result.arenas["DETAIL"];
        assert_eq!(header.len(), 1);
        assert_eq!(detail.len(), 1);

        // Schemas are different
        assert_ne!(
            header.schema().columns().len(),
            detail.schema().columns().len()
        );
    }

    #[test]
    fn test_dispatch_unmatched_skip_warning() {
        let dispatcher = MultiRecordDispatcher::new(fw_discriminator(), make_record_types(), false);
        let lines: Vec<Vec<u8>> = vec![
            b"D00001 100".to_vec(),
            b"X00002 200".to_vec(), // "X" doesn't match any tag
        ];
        let result = dispatcher.dispatch_lines(&lines).unwrap();
        assert_eq!(result.arenas["DETAIL"].len(), 1);
        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("X"));
    }

    #[test]
    fn test_dispatch_unmatched_fail_fast() {
        let dispatcher = MultiRecordDispatcher::new(fw_discriminator(), make_record_types(), true);
        let lines: Vec<Vec<u8>> = vec![
            b"D00001 100".to_vec(),
            b"X00002 200".to_vec(), // "X" doesn't match — fail_fast
        ];
        let err = dispatcher.dispatch_lines(&lines);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("fail_fast"),
            "error should mention fail_fast: {msg}"
        );
    }

    #[test]
    fn test_dispatch_cxl_three_part_path() {
        // Test that the CXL evaluator resolves three-part paths
        // benefits.EEID.employee_id → resolve_qualified("benefits.EEID", "employee_id")
        use clinker_record::FieldResolver;
        use cxl::eval;
        use cxl::parser::Parser;

        struct TestResolver;
        impl FieldResolver for TestResolver {
            fn resolve(&self, _name: &str) -> Option<Value> {
                None
            }
            fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value> {
                if source == "benefits.EEID" && field == "employee_id" {
                    Some(Value::String("EMP001".into()))
                } else {
                    None
                }
            }
            fn available_fields(&self) -> Vec<&str> {
                vec![]
            }
            fn iter_fields(&self) -> Vec<(String, Value)> {
                vec![]
            }
        }

        let source = "emit x = benefits.EEID.employee_id";
        let parsed = Parser::parse(source);
        assert!(
            parsed.errors.is_empty(),
            "parse errors: {:?}",
            parsed.errors
        );

        let fields: &[&str] = &[];
        let resolved =
            cxl::resolve::pass::resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
        let schema =
            cxl::typecheck::Row::closed(indexmap::IndexMap::new(), cxl::lexer::Span::new(0, 0));
        let typed = cxl::typecheck::pass::type_check(resolved, &schema).unwrap();

        let resolver = TestResolver;
        let stable = eval::StableEvalContext::test_default();
        let ctx = eval::EvalContext::test_default_borrowed(&stable);
        let empty_schema = std::sync::Arc::new(clinker_record::Schema::new(Vec::<Box<str>>::new()));
        let empty_input = clinker_record::Record::new(empty_schema, Vec::new());
        let layout = std::sync::Arc::clone(&typed.output_layout);
        let mut evaluator = eval::ProgramEvaluator::new(std::sync::Arc::new(typed), false);
        let result = evaluator
            .eval_record::<crate::pipeline::arena::Arena>(&ctx, &empty_input, &resolver, None)
            .unwrap();
        let values = match result {
            eval::EvalResult::Emit { values, .. } => values,
            eval::EvalResult::Skip(_) => panic!("expected Emit"),
        };
        let slot = layout.schema.index("x").expect("x slot");
        assert_eq!(
            values[slot],
            Value::String("EMP001".into()),
            "three-part path should resolve to EMP001"
        );
    }

    #[test]
    fn test_dispatch_parent_join_key_stored() {
        let rt = RecordTypeDef {
            id: "DETAIL".into(),
            tag: "D".into(),
            description: None,
            fields: vec![field("id")],
            parent: Some("HEADER".into()),
            join_key: Some("header_id".into()),
        };
        assert_eq!(rt.parent.as_deref(), Some("HEADER"));
        assert_eq!(rt.join_key.as_deref(), Some("header_id"));
    }

    #[test]
    fn test_dispatch_mixed_record_counts() {
        let dispatcher = MultiRecordDispatcher::new(fw_discriminator(), make_record_types(), false);
        let mut lines: Vec<Vec<u8>> = Vec::new();

        // 10 HEADER records
        for _ in 0..10 {
            lines.push(b"HREPORT   ".to_vec());
        }
        // 90 DETAIL records
        for i in 0..90 {
            lines.push(format!("D{:05} {:03}", i, i * 10).into_bytes());
        }
        // 1 TRAILER record
        lines.push(b"T00090    ".to_vec());

        let result = dispatcher.dispatch_lines(&lines).unwrap();
        assert_eq!(result.arenas["HEADER"].len(), 10);
        assert_eq!(result.arenas["DETAIL"].len(), 90);
        assert_eq!(result.arenas["TRAILER"].len(), 1);
    }

    #[test]
    fn test_dispatch_empty_record_type() {
        let dispatcher = MultiRecordDispatcher::new(fw_discriminator(), make_record_types(), false);
        // Only DETAIL records — HEADER and TRAILER have zero matches
        let lines: Vec<Vec<u8>> = vec![b"D00001 100".to_vec()];
        let result = dispatcher.dispatch_lines(&lines).unwrap();

        assert_eq!(result.arenas["HEADER"].len(), 0);
        assert_eq!(result.arenas["DETAIL"].len(), 1);
        assert_eq!(result.arenas["TRAILER"].len(), 0);
    }

    #[test]
    fn test_dispatch_discriminator_short_line() {
        let dispatcher = MultiRecordDispatcher::new(fw_discriminator(), make_record_types(), false);
        // Empty line — too short for discriminator (need 1 byte, got 0)
        let lines: Vec<Vec<u8>> = vec![vec![]];
        let err = dispatcher.dispatch_lines(&lines);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("too short") || msg.contains("need"),
            "error should mention line being too short: {msg}"
        );
    }
}
