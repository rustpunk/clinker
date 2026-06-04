use clinker_record::Value;
use indexmap::IndexMap;

/// Context available during log template resolution.
pub struct LogTemplateContext<'a> {
    /// Record field values (from the current record).
    pub record_fields: &'a IndexMap<String, Value>,
    /// Transform name.
    pub transform_name: &'a str,
    /// Transform duration in milliseconds (only available in after_transform).
    pub transform_duration_ms: Option<u64>,
    /// Source file path.
    pub source_file: &'a str,
    /// Source row number.
    pub source_row: u64,
    /// Pipeline counters.
    pub pipeline_ok_count: u64,
    pub pipeline_dlq_count: u64,
    pub pipeline_total_count: u64,
    /// Pipeline name.
    pub pipeline_name: &'a str,
    /// Pipeline execution ID.
    pub pipeline_execution_id: &'a str,
    /// DLQ error category (only available in on_error).
    pub dlq_error_category: Option<&'a str>,
    /// DLQ error detail (only available in on_error).
    pub dlq_error_detail: Option<&'a str>,
}

/// Resolve `{variable}` placeholders in a log message template.
///
/// Supported variables:
/// - `{field_name}` — record field value
/// - `{_source_file}`, `{_source_row}` — provenance
/// - `{_pipeline_ok_count}`, `{_pipeline_dlq_count}`, `{_pipeline_total_count}` — counters
/// - `{_transform_name}`, `{_transform_duration_ms}` — transform metadata
/// - `{pipeline.name}`, `{pipeline.execution_id}` — pipeline metadata
/// - `{_cxl_dlq_error_category}`, `{_cxl_dlq_error_detail}` — DLQ context
///
/// Unknown variables resolve to the literal `{var_name}` (no error).
pub fn resolve_template(template: &str, ctx: &LogTemplateContext<'_>) -> String {
    let mut result = String::with_capacity(template.len());
    let mut chars = template.char_indices().peekable();

    while let Some((i, ch)) = chars.next() {
        if ch == '{' {
            // Find closing brace
            let start = i + 1;
            let mut end = start;
            let mut found = false;
            for (j, c) in chars.by_ref() {
                if c == '}' {
                    end = j;
                    found = true;
                    break;
                }
            }
            if found {
                let var = &template[start..end];
                resolve_var(var, ctx, &mut result);
            } else {
                // No closing brace — emit as literal
                result.push('{');
                result.push_str(&template[start..]);
            }
        } else {
            result.push(ch);
        }
    }

    result
}

fn resolve_var(var: &str, ctx: &LogTemplateContext<'_>, out: &mut String) {
    match var {
        "_source_file" => out.push_str(ctx.source_file),
        "_source_row" => out.push_str(&ctx.source_row.to_string()),
        "_source_count" => out.push_str(&ctx.pipeline_total_count.to_string()),
        "_pipeline_ok_count" => out.push_str(&ctx.pipeline_ok_count.to_string()),
        "_pipeline_dlq_count" => out.push_str(&ctx.pipeline_dlq_count.to_string()),
        "_pipeline_total_count" => out.push_str(&ctx.pipeline_total_count.to_string()),
        "_transform_name" => out.push_str(ctx.transform_name),
        "_transform_duration_ms" => {
            if let Some(ms) = ctx.transform_duration_ms {
                out.push_str(&ms.to_string());
            } else {
                out.push_str("null");
            }
        }
        "pipeline.name" => out.push_str(ctx.pipeline_name),
        "pipeline.execution_id" => out.push_str(ctx.pipeline_execution_id),
        "_cxl_dlq_error_category" => {
            out.push_str(ctx.dlq_error_category.unwrap_or("null"));
        }
        "_cxl_dlq_error_detail" => {
            out.push_str(ctx.dlq_error_detail.unwrap_or("null"));
        }
        _ => {
            // Try record fields
            if let Some(val) = ctx.record_fields.get(var) {
                format_value(val, out);
            } else {
                // Unknown — emit literal placeholder
                out.push('{');
                out.push_str(var);
                out.push('}');
            }
        }
    }
}

fn format_value(val: &Value, out: &mut String) {
    match val {
        Value::String(s) => out.push_str(s),
        Value::Integer(i) => out.push_str(&i.to_string()),
        Value::Float(f) => out.push_str(&f.to_string()),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Null => out.push_str("null"),
        Value::Date(d) => out.push_str(&d.to_string()),
        Value::DateTime(dt) => out.push_str(&dt.to_string()),
        Value::Array(arr) => {
            out.push('[');
            for (i, v) in arr.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                format_value(v, out);
            }
            out.push(']');
        }
        Value::Map(m) => {
            out.push('{');
            for (i, (k, v)) in m.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(k);
                out.push_str(": ");
                format_value(v, out);
            }
            out.push('}');
        }
    }
}

/// Extract structured key-value fields from a record for log emission.
pub fn extract_fields(
    field_names: &[String],
    record: &IndexMap<String, Value>,
) -> Vec<(String, String)> {
    field_names
        .iter()
        .filter_map(|name| {
            record.get(name).map(|val| {
                let mut s = String::new();
                format_value(val, &mut s);
                (name.clone(), s)
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx<'a>(fields: &'a IndexMap<String, Value>) -> LogTemplateContext<'a> {
        LogTemplateContext {
            record_fields: fields,
            transform_name: "test_transform",
            transform_duration_ms: Some(42),
            source_file: "input.csv",
            source_row: 10,
            pipeline_ok_count: 95,
            pipeline_dlq_count: 5,
            pipeline_total_count: 100,
            pipeline_name: "daily_orders",
            pipeline_execution_id: "abc-123",
            dlq_error_category: None,
            dlq_error_detail: None,
        }
    }

    #[test]
    fn test_log_template_provenance_vars() {
        let fields = IndexMap::new();
        let ctx = test_ctx(&fields);
        let result = resolve_template("{_source_file}:{_source_row}", &ctx);
        assert_eq!(result, "input.csv:10");
    }

    #[test]
    fn test_log_template_pipeline_counters() {
        let fields = IndexMap::new();
        let ctx = test_ctx(&fields);
        let result = resolve_template("ok={_pipeline_ok_count} dlq={_pipeline_dlq_count}", &ctx);
        assert_eq!(result, "ok=95 dlq=5");
    }

    #[test]
    fn test_log_template_transform_meta() {
        let fields = IndexMap::new();
        let ctx = test_ctx(&fields);
        let result = resolve_template("{_transform_name} took {_transform_duration_ms}ms", &ctx);
        assert_eq!(result, "test_transform took 42ms");
    }

    #[test]
    fn test_log_template_record_field() {
        let mut fields = IndexMap::new();
        fields.insert("Name".to_string(), Value::String("Alice".into()));
        fields.insert("Amount".to_string(), Value::Integer(500));
        let ctx = test_ctx(&fields);
        let result = resolve_template("processed {Name} for {Amount}", &ctx);
        assert_eq!(result, "processed Alice for 500");
    }

    #[test]
    fn test_log_template_unknown_var() {
        let fields = IndexMap::new();
        let ctx = test_ctx(&fields);
        let result = resolve_template("hello {unknown_var}", &ctx);
        assert_eq!(result, "hello {unknown_var}");
    }

    #[test]
    fn test_log_template_duration_null() {
        let fields = IndexMap::new();
        let mut ctx = test_ctx(&fields);
        ctx.transform_duration_ms = None;
        let result = resolve_template("{_transform_duration_ms}", &ctx);
        assert_eq!(result, "null");
    }

    #[test]
    fn test_log_template_pipeline_metadata() {
        let fields = IndexMap::new();
        let ctx = test_ctx(&fields);
        let result = resolve_template("{pipeline.name}/{pipeline.execution_id}", &ctx);
        assert_eq!(result, "daily_orders/abc-123");
    }

    #[test]
    fn test_log_template_dlq_context() {
        let fields = IndexMap::new();
        let mut ctx = test_ctx(&fields);
        ctx.dlq_error_category = Some("validation_failure");
        ctx.dlq_error_detail = Some("invalid email");
        let result = resolve_template("{_cxl_dlq_error_category}: {_cxl_dlq_error_detail}", &ctx);
        assert_eq!(result, "validation_failure: invalid email");
    }
}
