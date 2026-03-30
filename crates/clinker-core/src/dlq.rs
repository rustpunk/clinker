use std::io::Write;
use std::sync::Arc;

use clinker_record::{Schema, Value};

use crate::executor::DlqEntry;

/// Structured DLQ error categories per spec SS10.4.
#[derive(Debug, Clone, Copy)]
pub enum DlqErrorCategory {
    TypeCoercionFailure,
    EvaluationError,
}

impl DlqErrorCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TypeCoercionFailure => "type_coercion_failure",
            Self::EvaluationError => "evaluation_error",
        }
    }
}

/// DLQ column names per spec SS10.4.
pub const DLQ_COLUMNS: &[&str] = &[
    "_cxl_dlq_id",
    "_cxl_dlq_timestamp",
    "_cxl_dlq_source_file",
    "_cxl_dlq_source_row",
    "_cxl_dlq_error_category",
    "_cxl_dlq_error_detail",
];

/// Write DLQ entries to a CSV writer.
pub fn write_dlq<W: Write>(
    writer: W,
    entries: &[DlqEntry],
    source_schema: &Arc<Schema>,
    source_file: &str,
) -> Result<(), clinker_format::FormatError> {
    let mut csv_writer = csv::WriterBuilder::new().from_writer(writer);

    // Build header: DLQ columns + source field names
    let mut header: Vec<&str> = DLQ_COLUMNS.to_vec();
    for col in source_schema.columns() {
        header.push(col.as_ref());
    }
    csv_writer.write_record(&header)?;

    for entry in entries {
        let id = uuid::Uuid::now_v7().to_string();
        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        let category = classify_error(&entry.error_message);

        let mut row: Vec<String> = vec![
            id,
            timestamp,
            source_file.to_string(),
            entry.source_row.to_string(),
            category.as_str().to_string(),
            entry.error_message.clone(),
        ];

        // Append original source fields in schema order
        for col in source_schema.columns() {
            let value = entry.original_record.get(col).unwrap_or(&Value::Null);
            row.push(value_to_string(value));
        }

        csv_writer.write_record(&row)?;
    }

    csv_writer.flush()?;
    Ok(())
}

/// Classify an error message into a DLQ error category.
fn classify_error(message: &str) -> DlqErrorCategory {
    let lower = message.to_lowercase();
    if lower.contains("coercion") || lower.contains("convert") || lower.contains("parse") {
        DlqErrorCategory::TypeCoercionFailure
    } else {
        DlqErrorCategory::EvaluationError
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::DateTime(dt) => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
        Value::Array(arr) => serde_json::to_string(arr).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["name".into(), "value".into()]))
    }

    fn make_dlq_entry(row: u64, error: &str, name: &str, value: &str) -> DlqEntry {
        let schema = make_schema();
        let record = Record::new(
            schema,
            vec![Value::String(name.into()), Value::String(value.into())],
        );
        DlqEntry {
            source_row: row,
            error_message: error.to_string(),
            original_record: record,
        }
    }

    #[test]
    fn test_dlq_columns_match_spec() {
        let entries = vec![make_dlq_entry(2, "eval error", "Alice", "bad")];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv").unwrap();
        let output = String::from_utf8(buf).unwrap();

        let header_line = output.lines().next().unwrap();
        let columns: Vec<&str> = header_line.split(',').collect();
        // DLQ columns first
        assert_eq!(columns[0], "_cxl_dlq_id");
        assert_eq!(columns[1], "_cxl_dlq_timestamp");
        assert_eq!(columns[2], "_cxl_dlq_source_file");
        assert_eq!(columns[3], "_cxl_dlq_source_row");
        assert_eq!(columns[4], "_cxl_dlq_error_category");
        assert_eq!(columns[5], "_cxl_dlq_error_detail");
        // Source fields after
        assert_eq!(columns[6], "name");
        assert_eq!(columns[7], "value");
    }

    #[test]
    fn test_dlq_preserves_original_fields() {
        let entries = vec![make_dlq_entry(3, "type error", "Bob", "not_a_number")];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "data.csv").unwrap();
        let output = String::from_utf8(buf).unwrap();

        let data_line = output.lines().nth(1).unwrap();
        // Original fields should be present
        assert!(data_line.contains("Bob"));
        assert!(data_line.contains("not_a_number"));
        // Source file and row
        assert!(data_line.contains("data.csv"));
        assert!(data_line.contains(",3,"));
    }

    #[test]
    fn test_dlq_uuid_v7_format() {
        let entries = vec![make_dlq_entry(1, "error", "test", "val")];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv").unwrap();
        let output = String::from_utf8(buf).unwrap();

        let data_line = output.lines().nth(1).unwrap();
        let id_field = data_line.split(',').next().unwrap();
        // Parse as UUID and verify it's valid
        let uuid = uuid::Uuid::parse_str(id_field).expect("should be valid UUID");
        assert_eq!(uuid.get_version(), Some(uuid::Version::SortRand));
    }
}
