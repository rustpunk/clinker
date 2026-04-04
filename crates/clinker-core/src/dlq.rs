use std::io::Write;
use std::sync::Arc;

use clinker_record::{Schema, Value};

use crate::executor::DlqEntry;

/// All 6 DLQ error categories per spec §10.4.
///
/// Passed from the error site — no string matching. Each error path
/// constructs the correct variant at the point of failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqErrorCategory {
    MissingRequiredField,
    TypeCoercionFailure,
    RequiredFieldConversionFailure,
    NanInOutputField,
    AggregateTypeError,
    ValidationFailure,
}

impl DlqErrorCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingRequiredField => "missing_required_field",
            Self::TypeCoercionFailure => "type_coercion_failure",
            Self::RequiredFieldConversionFailure => "required_field_conversion_failure",
            Self::NanInOutputField => "nan_in_output_field",
            Self::AggregateTypeError => "aggregate_type_error",
            Self::ValidationFailure => "validation_failure",
        }
    }
}

/// DLQ column names per spec §10.4.
pub const DLQ_COLUMNS: &[&str] = &[
    "_cxl_dlq_id",
    "_cxl_dlq_timestamp",
    "_cxl_dlq_source_file",
    "_cxl_dlq_source_row",
    "_cxl_dlq_error_category",
    "_cxl_dlq_error_detail",
    "_cxl_dlq_stage",
    "_cxl_dlq_route",
    "_cxl_dlq_trigger",
];

/// Write DLQ entries to a CSV writer (DLQ is always CSV per spec §10.4).
///
/// Column layout:
/// - Always: _cxl_dlq_id, _cxl_dlq_timestamp, _cxl_dlq_source_file, _cxl_dlq_source_row
/// - If include_reason: _cxl_dlq_error_category, _cxl_dlq_error_detail
/// - If include_source_row: all original source fields in schema order
pub fn write_dlq<W: Write>(
    writer: W,
    entries: &[DlqEntry],
    source_schema: &Arc<Schema>,
    source_file: &str,
    include_reason: bool,
    include_source_row: bool,
) -> Result<(), clinker_format::FormatError> {
    let mut csv_writer = csv::WriterBuilder::new().from_writer(writer);

    // Build header
    let mut header: Vec<&str> = vec![
        "_cxl_dlq_id",
        "_cxl_dlq_timestamp",
        "_cxl_dlq_source_file",
        "_cxl_dlq_source_row",
    ];
    if include_reason {
        header.push("_cxl_dlq_error_category");
        header.push("_cxl_dlq_error_detail");
    }
    // Stage, route, and trigger columns always present (nullable)
    header.push("_cxl_dlq_stage");
    header.push("_cxl_dlq_route");
    header.push("_cxl_dlq_trigger");
    if include_source_row {
        for col in source_schema.columns() {
            header.push(col.as_ref());
        }
    }
    csv_writer.write_record(&header)?;

    for entry in entries {
        let id = uuid::Uuid::now_v7().to_string();
        let timestamp = chrono::Utc::now().to_rfc3339();

        let mut row: Vec<String> = vec![
            id,
            timestamp,
            source_file.to_string(),
            entry.source_row.to_string(),
        ];

        if include_reason {
            row.push(entry.category.as_str().to_string());
            row.push(entry.error_message.clone());
        }

        // Stage, route, and trigger
        row.push(entry.stage.clone().unwrap_or_default());
        row.push(entry.route.clone().unwrap_or_default());
        row.push(entry.trigger.to_string());

        if include_source_row {
            for col in source_schema.columns() {
                let value = entry.original_record.get(col).unwrap_or(&Value::Null);
                row.push(value_to_string(value));
            }
        }

        csv_writer.write_record(&row)?;
    }

    csv_writer.flush()?;
    Ok(())
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
        Value::Map(m) => serde_json::to_string(m.as_ref()).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Record;

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["name".into(), "value".into()]))
    }

    fn make_dlq_entry(
        row: u64,
        category: DlqErrorCategory,
        error: &str,
        name: &str,
        value: &str,
    ) -> DlqEntry {
        let schema = make_schema();
        let record = Record::new(
            schema,
            vec![Value::String(name.into()), Value::String(value.into())],
        );
        DlqEntry {
            source_row: row,
            category,
            error_message: error.to_string(),
            original_record: record,
            stage: None,
            route: None,
            trigger: true,
        }
    }

    #[test]
    fn test_dlq_all_columns_present() {
        let entries = vec![make_dlq_entry(
            2,
            DlqErrorCategory::TypeCoercionFailure,
            "eval error",
            "Alice",
            "bad",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        let header_line = output.lines().next().unwrap();
        let columns: Vec<&str> = header_line.split(',').collect();
        assert_eq!(columns[0], "_cxl_dlq_id");
        assert_eq!(columns[1], "_cxl_dlq_timestamp");
        assert_eq!(columns[2], "_cxl_dlq_source_file");
        assert_eq!(columns[3], "_cxl_dlq_source_row");
        assert_eq!(columns[4], "_cxl_dlq_error_category");
        assert_eq!(columns[5], "_cxl_dlq_error_detail");
        assert_eq!(columns[6], "_cxl_dlq_stage");
        assert_eq!(columns[7], "_cxl_dlq_route");
        assert_eq!(columns[8], "_cxl_dlq_trigger");
        assert_eq!(columns[9], "name");
        assert_eq!(columns[10], "value");
    }

    #[test]
    fn test_dlq_uuid_v7_time_ordered() {
        let entries = vec![
            make_dlq_entry(1, DlqErrorCategory::TypeCoercionFailure, "err1", "a", "1"),
            make_dlq_entry(2, DlqErrorCategory::TypeCoercionFailure, "err2", "b", "2"),
        ];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = output.lines().collect();

        let id1 = lines[1].split(',').next().unwrap();
        let id2 = lines[2].split(',').next().unwrap();
        let uuid1 = uuid::Uuid::parse_str(id1).unwrap();
        let uuid2 = uuid::Uuid::parse_str(id2).unwrap();
        assert!(uuid1 < uuid2, "UUIDs should be monotonically increasing");
    }

    #[test]
    fn test_dlq_uuid_v7_unique() {
        let entries: Vec<_> = (0..1000)
            .map(|i| {
                make_dlq_entry(
                    i,
                    DlqErrorCategory::TypeCoercionFailure,
                    &format!("err{i}"),
                    "n",
                    "v",
                )
            })
            .collect();
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let ids: Vec<&str> = output
            .lines()
            .skip(1)
            .map(|l| l.split(',').next().unwrap())
            .collect();
        let mut unique = ids.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(ids.len(), unique.len(), "all UUIDs should be distinct");
    }

    #[test]
    fn test_dlq_error_category_missing_required() {
        let entries = vec![make_dlq_entry(
            1,
            DlqErrorCategory::MissingRequiredField,
            "field X is required",
            "a",
            "1",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let data_line = output.lines().nth(1).unwrap();
        assert!(data_line.contains("missing_required_field"));
    }

    #[test]
    fn test_dlq_error_category_type_coercion() {
        let entries = vec![make_dlq_entry(
            1,
            DlqErrorCategory::TypeCoercionFailure,
            "cannot convert",
            "a",
            "1",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let data_line = output.lines().nth(1).unwrap();
        assert!(data_line.contains("type_coercion_failure"));
    }

    #[test]
    fn test_dlq_error_category_nan() {
        let entries = vec![make_dlq_entry(
            1,
            DlqErrorCategory::NanInOutputField,
            "NaN in field X",
            "a",
            "1",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let data_line = output.lines().nth(1).unwrap();
        assert!(data_line.contains("nan_in_output_field"));
    }

    #[test]
    fn test_dlq_error_category_validation() {
        let entries = vec![make_dlq_entry(
            1,
            DlqErrorCategory::ValidationFailure,
            "check failed",
            "a",
            "1",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let data_line = output.lines().nth(1).unwrap();
        assert!(data_line.contains("validation_failure"));
    }

    #[test]
    fn test_dlq_error_category_aggregate() {
        let entries = vec![make_dlq_entry(
            1,
            DlqErrorCategory::AggregateTypeError,
            "sum got non-numeric",
            "a",
            "1",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let data_line = output.lines().nth(1).unwrap();
        assert!(data_line.contains("aggregate_type_error"));
    }

    #[test]
    fn test_dlq_error_category_required_conversion() {
        let entries = vec![make_dlq_entry(
            1,
            DlqErrorCategory::RequiredFieldConversionFailure,
            "required conversion failed",
            "a",
            "1",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let data_line = output.lines().nth(1).unwrap();
        assert!(data_line.contains("required_field_conversion_failure"));
    }

    #[test]
    fn test_dlq_include_reason_false() {
        let entries = vec![make_dlq_entry(
            1,
            DlqErrorCategory::TypeCoercionFailure,
            "error",
            "a",
            "1",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", false, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        let header_line = output.lines().next().unwrap();
        assert!(!header_line.contains("_cxl_dlq_error_category"));
        assert!(!header_line.contains("_cxl_dlq_error_detail"));
        // But source fields should still be present
        assert!(header_line.contains("name"));
    }

    #[test]
    fn test_dlq_include_source_row_false() {
        let entries = vec![make_dlq_entry(
            1,
            DlqErrorCategory::TypeCoercionFailure,
            "error",
            "Alice",
            "1",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, false).unwrap();
        let output = String::from_utf8(buf).unwrap();

        let header_line = output.lines().next().unwrap();
        // Error columns should be present
        assert!(header_line.contains("_cxl_dlq_error_category"));
        // Source fields should NOT be present
        assert!(!header_line.contains(",name"));
        let data_line = output.lines().nth(1).unwrap();
        assert!(!data_line.contains("Alice"));
    }

    #[test]
    fn test_dlq_source_fields_schema_order() {
        let schema = Arc::new(Schema::new(vec![
            "zulu".into(),
            "alpha".into(),
            "mike".into(),
        ]));
        let record = Record::new(
            schema.clone(),
            vec![
                Value::String("Z".into()),
                Value::String("A".into()),
                Value::String("M".into()),
            ],
        );
        let entries = vec![DlqEntry {
            source_row: 1,
            category: DlqErrorCategory::TypeCoercionFailure,
            error_message: "err".to_string(),
            original_record: record,
            stage: None,
            route: None,
            trigger: true,
        }];
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        let header_line = output.lines().next().unwrap();
        let columns: Vec<&str> = header_line.split(',').collect();
        // Source fields in schema order (zulu, alpha, mike), not alphabetical
        // Columns 6-8 are _cxl_dlq_stage, _cxl_dlq_route, _cxl_dlq_trigger
        assert_eq!(columns[9], "zulu");
        assert_eq!(columns[10], "alpha");
        assert_eq!(columns[11], "mike");
    }

    #[test]
    fn test_dlq_timestamp_iso8601() {
        let entries = vec![make_dlq_entry(
            1,
            DlqErrorCategory::TypeCoercionFailure,
            "error",
            "a",
            "1",
        )];
        let schema = make_schema();
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        let data_line = output.lines().nth(1).unwrap();
        let timestamp = data_line.split(',').nth(1).unwrap();
        // RFC 3339 / ISO 8601 must parse
        chrono::DateTime::parse_from_rfc3339(timestamp)
            .expect("timestamp should be valid RFC 3339");
    }

    #[test]
    fn test_dlq_category_as_str_all_variants() {
        assert_eq!(
            DlqErrorCategory::MissingRequiredField.as_str(),
            "missing_required_field"
        );
        assert_eq!(
            DlqErrorCategory::TypeCoercionFailure.as_str(),
            "type_coercion_failure"
        );
        assert_eq!(
            DlqErrorCategory::RequiredFieldConversionFailure.as_str(),
            "required_field_conversion_failure"
        );
        assert_eq!(
            DlqErrorCategory::NanInOutputField.as_str(),
            "nan_in_output_field"
        );
        assert_eq!(
            DlqErrorCategory::AggregateTypeError.as_str(),
            "aggregate_type_error"
        );
        assert_eq!(
            DlqErrorCategory::ValidationFailure.as_str(),
            "validation_failure"
        );
    }
}
