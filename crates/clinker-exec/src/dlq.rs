//! DLQ output: serialize dead-letter-queue entries to per-source CSV sinks.
//!
//! The DLQ category vocabulary and stage-label helpers live in
//! [`clinker_core_types::dlq`]; this module owns the writers that turn
//! [`DlqEntry`] records into the on-disk CSV shape, which couples to the
//! pipeline's config and executor types.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};

use clinker_record::{FieldMetadata, Schema, Value};

use crate::executor::DlqEntry;
use clinker_plan::config::DlqConfig;

/// Write DLQ entries to a CSV writer (DLQ is always CSV per spec §10.4).
///
/// Column layout:
/// - Always: _cxl_dlq_id, _cxl_dlq_timestamp, _cxl_dlq_source_file, _cxl_dlq_source_row
/// - If include_reason: _cxl_dlq_error_category, _cxl_dlq_error_detail
/// - If include_source_row: all *user-declared* source fields plus
///   correlation-lattice columns (`$ck.<field>`,
///   `$ck.aggregate.<name>`) in schema order. Engine-stamped
///   sidecar columns (`$widened`) are filtered out — they carry
///   `Value::Map` payloads which would JSON-encode into a single
///   CSV cell and hide routing bugs the same way the regular
///   non-JSON writers' silent map-degrade did before commit
///   `f5ae145`. Users who need the auto_widen sidecar surfaced in
///   their normal output can opt into `include_unmapped: true` on
///   the relevant Output node; the DLQ keeps a stable user-shape
///   schema regardless.
pub fn write_dlq<W: Write>(
    writer: W,
    entries: &[DlqEntry],
    include_reason: bool,
    include_source_row: bool,
) -> Result<(), clinker_format::FormatError> {
    let mut csv_writer = csv::WriterBuilder::new().from_writer(writer);

    // Build header
    let user_columns = stable_user_columns(entries);
    let mut header: Vec<String> = vec![
        "_cxl_dlq_id".into(),
        "_cxl_dlq_timestamp".into(),
        "_cxl_dlq_source_file".into(),
        "_cxl_dlq_source_name".into(),
        "_cxl_dlq_source_row".into(),
        "_cxl_dlq_triggering_field".into(),
        "_cxl_dlq_triggering_value".into(),
    ];
    if include_reason {
        header.push("_cxl_dlq_error_category".into());
        header.push("_cxl_dlq_error_detail".into());
    }
    // Stage, route, and trigger columns always present (nullable)
    header.push("_cxl_dlq_stage".into());
    header.push("_cxl_dlq_route".into());
    header.push("_cxl_dlq_trigger".into());
    if include_source_row {
        header.extend(user_columns.iter().cloned());
    }
    csv_writer.write_record(&header)?;

    for entry in entries {
        let id = uuid::Uuid::now_v7().to_string();
        let timestamp = chrono::Utc::now().to_rfc3339();

        let mut row: Vec<String> = vec![
            id,
            timestamp,
            source_file_of(entry).to_owned(),
            entry.source_name.as_ref().to_string(),
            entry.source_row.to_string(),
            entry.triggering_field.as_deref().unwrap_or("").to_string(),
            entry
                .triggering_value
                .as_ref()
                .map(value_to_string)
                .unwrap_or_default(),
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
            for name in &user_columns {
                let value = entry.original_record.get(name).unwrap_or(&Value::Null);
                row.push(value_to_string(value));
            }
        }

        csv_writer.write_record(&row)?;
    }

    csv_writer.flush()?;
    Ok(())
}

fn stable_user_columns(entries: &[DlqEntry]) -> Vec<String> {
    let mut shared_layout: Option<Vec<String>> = None;
    for entry in entries {
        let layout = dlq_user_columns(entry.original_record.schema())
            .map(|(_, name)| name.to_owned())
            .collect::<Vec<_>>();
        match shared_layout.as_ref() {
            None => shared_layout = Some(layout),
            Some(columns) if columns == &layout => {}
            Some(_) => {
                return entries
                    .iter()
                    .flat_map(|entry| {
                        dlq_user_columns(entry.original_record.schema()).map(|(_, name)| name)
                    })
                    .map(str::to_owned)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
            }
        }
    }
    shared_layout.unwrap_or_default()
}

fn source_file_of(entry: &DlqEntry) -> &str {
    let schema = entry.original_record.schema();
    for index in 0..schema.column_count() {
        if matches!(
            schema.field_metadata(index),
            Some(FieldMetadata::SourceFile)
        ) && let Some(Value::String(path)) = entry.original_record.values().get(index)
        {
            return path;
        }
    }
    "<merged>"
}

/// Partition DLQ entries by configured per-source `path:` override.
///
/// Returns `(path, entries)` pairs. Entries whose `source_name` has a
/// per-source `path` override land in their own bucket; the remainder
/// fall through to `DlqConfig.path` (the pipeline-wide sidecar). When
/// `dlq_config.path` is `None` and no per-source override matches, an
/// entry has no destination and is dropped from the result — the CLI
/// emits nothing for it. Insertion order is preserved within each
/// bucket; bucket ordering is deterministic via the BTreeMap traversal
/// over `per_source`.
pub fn partition_dlq_entries<'a>(
    entries: &'a [DlqEntry],
    dlq_config: &DlqConfig,
) -> Vec<(PathBuf, Vec<&'a DlqEntry>)> {
    let mut per_source_paths: std::collections::BTreeMap<&str, PathBuf> =
        std::collections::BTreeMap::new();
    for (src, per) in &dlq_config.per_source {
        if let Some(p) = per.path.as_deref() {
            per_source_paths.insert(src.as_str(), PathBuf::from(p));
        }
    }

    // Bucket identity is the path's *collision key*, not its raw bytes. On a
    // case-insensitive output filesystem (macOS APFS / Windows NTFS default)
    // `errors.csv` and `Errors.csv` name one physical file; keying buckets on
    // the raw `PathBuf` would open two writers onto it and let the per-source
    // and pipeline-wide records overwrite each other. Folding is conditional on
    // the actual target filesystem (the same `collision_key` the config-time
    // check uses), so case-sensitive Linux still keeps distinct files distinct.
    // The bucket's display `PathBuf` is the first path that claimed the key —
    // pipeline-wide wins, matching the static check's first-insertion-wins.
    let mut buckets: Vec<(PathBuf, Vec<&'a DlqEntry>)> = Vec::new();
    let mut index_of: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    // The same identity the plan-time DLQ check uses, so a partition the
    // planner treated as one file is one bucket here rather than two writers
    // on it. Resolved once per configured path while the buckets are built:
    // it consults the filesystem, so asking again per record would both cost
    // a walk per record and let the answer move under a run that is still
    // creating its destination directories -- and an entry whose key no
    // longer matched any bucket had nowhere to go.
    let key_of = |p: &Path| clinker_plan::config::destination_identity(p);
    let mut bucket_of_source: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
    let mut pipeline_bucket = None;
    if let Some(p) = dlq_config.path.as_deref() {
        let pb = PathBuf::from(p);
        index_of.insert(key_of(&pb), 0);
        buckets.push((pb, Vec::new()));
        pipeline_bucket = Some(0);
    }
    for (source, path) in per_source_paths {
        let index = *index_of.entry(key_of(path.as_path())).or_insert_with(|| {
            buckets.push((path.clone(), Vec::new()));
            buckets.len() - 1
        });
        bucket_of_source.insert(source.as_ref(), index);
    }

    for entry in entries {
        // By the name the entry carries, not by re-deriving its path's
        // identity: every destination an entry can have was keyed above, so
        // this lookup answers from what was decided then.
        if let Some(&i) = bucket_of_source
            .get(entry.source_name.as_ref())
            .or(pipeline_bucket.as_ref())
        {
            buckets[i].1.push(entry);
        }
    }

    buckets
}

/// Iterator over schema columns that should appear in DLQ output:
/// user-declared columns plus correlation-lattice columns
/// (`$ck.<field>`, `$ck.aggregate.<name>`). The `$widened`
/// `auto_widen` sidecar absorber is filtered out — its
/// `Value::Map` payload has no canonical scalar serialization and
/// would silently JSON-encode into a single CSV cell, hiding
/// routing bugs.
fn dlq_user_columns(schema: &Schema) -> impl Iterator<Item = (usize, &str)> {
    schema
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, c)| match schema.field_metadata(i) {
            Some(FieldMetadata::WidenedSidecar)
            | Some(FieldMetadata::SourceFile)
            | Some(FieldMetadata::SourceName)
            | Some(FieldMetadata::SourceEventTime)
            | Some(FieldMetadata::ReshapeAudit) => None,
            Some(FieldMetadata::SourceCorrelation { .. })
            | Some(FieldMetadata::AggregateGroupIndex { .. })
            | None => Some((i, c.as_ref())),
        })
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::DateTime(dt) => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
        Value::Array(arr) => serde_json::to_string(arr).unwrap_or_default(),
        // `Value::Map` only reaches the DLQ row builder if it lives at a
        // non-`$widened` column slot — i.e. the user explicitly emitted a
        // map at a regular column. The `dlq_user_columns` filter above
        // already drops `$widened`, so a Map here is intentional user
        // output. JSON-encode for shape consistency with the
        // `Value::Array` case (regular non-JSON writers raise
        // `UnserializableMapValue` for the same situation; DLQ is
        // best-effort capture, not user-serializable output, so it
        // accepts the JSON-string degrade rather than failing).
        Value::Map(m) => serde_json::to_string(m.as_ref()).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_core_types::dlq::DlqErrorCategory;
    use clinker_record::Record;
    use std::sync::Arc;

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
            source_row: row.into(),
            category,
            error_message: error.to_string(),
            original_record: record,
            stage: None,
            route: None,
            trigger: true,
            source_name: Arc::from("test_source"),
            triggering_field: None,
            triggering_value: None,
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        let header_line = output.lines().next().unwrap();
        let columns: Vec<&str> = header_line.split(',').collect();
        assert_eq!(columns[0], "_cxl_dlq_id");
        assert_eq!(columns[1], "_cxl_dlq_timestamp");
        assert_eq!(columns[2], "_cxl_dlq_source_file");
        assert_eq!(columns[3], "_cxl_dlq_source_name");
        assert_eq!(columns[4], "_cxl_dlq_source_row");
        assert_eq!(columns[5], "_cxl_dlq_triggering_field");
        assert_eq!(columns[6], "_cxl_dlq_triggering_value");
        assert_eq!(columns[7], "_cxl_dlq_error_category");
        assert_eq!(columns[8], "_cxl_dlq_error_detail");
        assert_eq!(columns[9], "_cxl_dlq_stage");
        assert_eq!(columns[10], "_cxl_dlq_route");
        assert_eq!(columns[11], "_cxl_dlq_trigger");
        assert_eq!(columns[12], "name");
        assert_eq!(columns[13], "value");
    }

    #[test]
    fn test_dlq_uuid_v7_time_ordered() {
        let entries = vec![
            make_dlq_entry(1, DlqErrorCategory::TypeCoercionFailure, "err1", "a", "1"),
            make_dlq_entry(2, DlqErrorCategory::TypeCoercionFailure, "err2", "b", "2"),
        ];
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, false, true).unwrap();
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, false).unwrap();
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
    fn test_dlq_source_fields_keep_shared_schema_order() {
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
            source_row: 1.into(),
            category: DlqErrorCategory::TypeCoercionFailure,
            error_message: "err".to_string(),
            original_record: record,
            stage: None,
            route: None,
            trigger: true,
            source_name: Arc::from("test_source"),
            triggering_field: None,
            triggering_value: None,
        }];
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        let header_line = output.lines().next().unwrap();
        let columns: Vec<&str> = header_line.split(',').collect();
        // A single shared schema keeps the user-declared order. Only genuinely
        // heterogeneous layouts need the lexical union fallback.
        // Columns 0-4 are the identity prelude; 5-6 are triggering_field/
        // triggering_value; 7-8 are error category/detail; 9-11 are
        // stage/route/trigger.
        assert_eq!(columns[12], "zulu");
        assert_eq!(columns[13], "alpha");
        assert_eq!(columns[14], "mike");
    }

    #[test]
    fn heterogeneous_records_use_union_columns_and_per_record_source_file() {
        use clinker_record::SchemaBuilder;

        let alpha_schema = SchemaBuilder::new()
            .with_field("alpha")
            .with_field_meta("$source.file", FieldMetadata::SourceFile)
            .build();
        let beta_schema = SchemaBuilder::new()
            .with_field("beta")
            .with_field_meta("$source.file", FieldMetadata::SourceFile)
            .build();
        let entry = |record, source_name: &'static str| DlqEntry {
            source_row: 1.into(),
            category: DlqErrorCategory::TypeCoercionFailure,
            error_message: "bad value".to_owned(),
            original_record: record,
            stage: None,
            route: None,
            trigger: true,
            source_name: Arc::from(source_name),
            triggering_field: None,
            triggering_value: None,
        };
        let entries = vec![
            entry(
                Record::new(
                    beta_schema,
                    vec![
                        Value::String("B".into()),
                        Value::String("inputs/beta.csv".into()),
                    ],
                ),
                "beta-source",
            ),
            entry(
                Record::new(
                    alpha_schema,
                    vec![
                        Value::String("A".into()),
                        Value::String("inputs/alpha.csv".into()),
                    ],
                ),
                "alpha-source",
            ),
        ];

        let mut bytes = Vec::new();
        write_dlq(&mut bytes, &entries, true, true).expect("write heterogeneous DLQ");
        let mut reader = csv::Reader::from_reader(bytes.as_slice());
        let header = reader.headers().expect("DLQ header").clone();
        let alpha = header.iter().position(|name| name == "alpha").unwrap();
        let beta = header.iter().position(|name| name == "beta").unwrap();
        assert!(alpha < beta, "union columns must have stable lexical order");
        assert!(!header.iter().any(|name| name == "$source.file"));

        let rows = reader
            .records()
            .collect::<Result<Vec<_>, _>>()
            .expect("DLQ rows");
        assert_eq!(&rows[0][2], "inputs/beta.csv");
        assert_eq!(&rows[0][alpha], "");
        assert_eq!(&rows[0][beta], "B");
        assert_eq!(&rows[1][2], "inputs/alpha.csv");
        assert_eq!(&rows[1][alpha], "A");
        assert_eq!(&rows[1][beta], "");
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
        let mut buf = Vec::new();
        write_dlq(&mut buf, &entries, true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        let data_line = output.lines().nth(1).unwrap();
        let timestamp = data_line.split(',').nth(1).unwrap();
        // RFC 3339 / ISO 8601 must parse
        chrono::DateTime::parse_from_rfc3339(timestamp)
            .expect("timestamp should be valid RFC 3339");
    }

    /// `write_dlq` filters the engine-stamped `$widened` sidecar
    /// column from both the header and the body. The `$widened`
    /// column carries `Value::Map` payloads which would otherwise
    /// JSON-encode into a single CSV cell — the same silent-degrade
    /// pattern commit `f5ae145` rejected in the regular non-JSON
    /// writers. Correlation-lattice columns (`$ck.<field>`) are
    /// retained for collateral DLQ debugging.
    #[test]
    fn test_dlq_filters_widened_sidecar_column() {
        use clinker_record::SchemaBuilder;
        // Schema mirrors what an `auto_widen` source produces:
        // user-declared columns + `$ck.<field>` shadow + `$widened`.
        let schema = Arc::new(
            SchemaBuilder::new()
                .with_field("employee_id")
                .with_field("salary")
                .with_field_meta(
                    "$ck.employee_id",
                    FieldMetadata::source_correlation("employee_id"),
                )
                .with_field_meta("$widened", FieldMetadata::widened_sidecar())
                .build(),
        );
        let mut sidecar = indexmap::IndexMap::new();
        sidecar.insert("region".into(), Value::String("US".into()));
        sidecar.insert("dept".into(), Value::String("eng".into()));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("E001".into()),
                Value::Integer(50000),
                Value::String("E001".into()),
                Value::Map(Box::new(sidecar)),
            ],
        );
        let entry = DlqEntry {
            source_row: 1.into(),
            category: DlqErrorCategory::TypeCoercionFailure,
            error_message: "test".to_string(),
            original_record: record,
            stage: None,
            route: None,
            trigger: true,
            source_name: Arc::from("test_source"),
            triggering_field: None,
            triggering_value: None,
        };
        let mut buf = Vec::new();
        write_dlq(&mut buf, &[entry], true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let header = output.lines().next().expect("header line");
        let columns: Vec<&str> = header.split(',').collect();

        assert!(
            !columns.contains(&"$widened"),
            "DLQ header must not contain `$widened` (silent JSON-blob degrade); \
             got header: {header}"
        );
        assert!(
            columns.contains(&"$ck.employee_id"),
            "DLQ header must retain `$ck.<field>` correlation lineage for \
             collateral DLQ debugging; got header: {header}"
        );
        assert!(
            columns.contains(&"employee_id") && columns.contains(&"salary"),
            "DLQ header must retain user-declared columns; got header: {header}"
        );

        // Body row has the same column count as the header — no
        // trailing JSON-blob cell from a leaked `$widened` payload.
        let body = output.lines().nth(1).expect("body line");
        let body_cells: Vec<&str> = body.split(',').collect();
        assert_eq!(
            body_cells.len(),
            columns.len(),
            "body row column count must match header; got body: {body}"
        );
        assert!(
            !body.contains("region"),
            "sidecar map's `region` key must not appear in any DLQ body cell; \
             got body: {body}"
        );
    }

    #[test]
    fn test_dlq_triggering_field_and_value_columns() {
        let schema = make_schema();
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::String("Alice".into()), Value::String("oops".into())],
        );
        let entry = DlqEntry {
            source_row: 7.into(),
            category: DlqErrorCategory::TypeCoercionFailure,
            error_message: "cannot convert".to_string(),
            original_record: record,
            stage: None,
            route: None,
            trigger: true,
            source_name: Arc::from("test_source"),
            triggering_field: Some(Arc::from("amount")),
            triggering_value: Some(Value::String("not-a-number".into())),
        };
        let mut buf = Vec::new();
        write_dlq(&mut buf, &[entry], true, true).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let body = output.lines().nth(1).expect("body line");
        let cells: Vec<&str> = body.split(',').collect();
        assert_eq!(cells[5], "amount");
        assert_eq!(cells[6], "not-a-number");
    }

    #[test]
    fn test_partition_dlq_entries_splits_by_source_path() {
        let schema = make_schema();
        let mk = |src: &str| {
            let rec = Record::new(
                Arc::clone(&schema),
                vec![Value::String("n".into()), Value::String("v".into())],
            );
            DlqEntry {
                source_row: 0.into(),
                category: DlqErrorCategory::TypeCoercionFailure,
                error_message: String::new(),
                original_record: rec,
                stage: None,
                route: None,
                trigger: true,
                source_name: Arc::from(src),
                triggering_field: None,
                triggering_value: None,
            }
        };
        let entries = vec![mk("src_a"), mk("src_b"), mk("src_a"), mk("src_c")];

        let mut per_source = std::collections::BTreeMap::new();
        per_source.insert(
            "src_b".to_string(),
            clinker_plan::config::DlqPerSourceConfig {
                path: Some("dlq_b.csv".to_string()),
                max_rate: None,
                min_records: None,
            },
        );
        let cfg = clinker_plan::config::DlqConfig {
            path: Some("dlq.csv".to_string()),
            include_reason: None,
            include_source_row: None,
            max_rate: None,
            min_records: None,
            per_source,
        };

        let buckets = partition_dlq_entries(&entries, &cfg);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].0, PathBuf::from("dlq.csv"));
        assert_eq!(
            buckets[0].1.len(),
            3,
            "src_a + src_a + src_c land in default"
        );
        assert_eq!(buckets[1].0, PathBuf::from("dlq_b.csv"));
        assert_eq!(buckets[1].1.len(), 1);
        assert_eq!(buckets[1].1[0].source_name.as_ref(), "src_b");
    }

    /// A per-source DLQ path that differs from the pipeline-wide path only in
    /// case must share *one* writer bucket on a case-insensitive filesystem
    /// (they name one physical file), while remaining two distinct buckets on a
    /// case-sensitive one. The expectation is conditioned on an isolated temp
    /// directory's actual case-sensitivity — probed the same way the partitioner
    /// does — so the assertion is deterministic on every CI runner regardless of
    /// the process-global working directory other parallel tests may change.
    #[test]
    fn test_partition_collapses_case_variant_paths_only_when_filesystem_folds() {
        let schema = make_schema();
        let mk = |src: &str| {
            let rec = Record::new(
                Arc::clone(&schema),
                vec![Value::String("n".into()), Value::String("v".into())],
            );
            DlqEntry {
                source_row: 0.into(),
                category: DlqErrorCategory::TypeCoercionFailure,
                error_message: String::new(),
                original_record: rec,
                stage: None,
                route: None,
                trigger: true,
                source_name: Arc::from(src),
                triggering_field: None,
                triggering_value: None,
            }
        };
        // `src_b` routes to a case-variant of the pipeline-wide path.
        let entries = vec![mk("src_a"), mk("src_b")];

        // Absolute paths inside an owned temp directory, not bare filenames. A
        // bare filename resolves the case-sensitivity probe against the
        // process-global current directory, which sibling tests running on other
        // threads can mutate or contend on — making this test's probe race with
        // the partitioner's own probe and flake. A fresh temp dir is stable and
        // private to this test, so both probes observe one filesystem and the
        // verdict is deterministic under parallel execution.
        let dir = tempfile::tempdir().unwrap();
        let lower = dir.path().join("errors.csv");
        let upper = dir.path().join("Errors.csv");

        let mut per_source = std::collections::BTreeMap::new();
        per_source.insert(
            "src_b".to_string(),
            clinker_plan::config::DlqPerSourceConfig {
                path: Some(upper.to_string_lossy().into_owned()),
                max_rate: None,
                min_records: None,
            },
        );
        let cfg = clinker_plan::config::DlqConfig {
            path: Some(lower.to_string_lossy().into_owned()),
            include_reason: None,
            include_source_row: None,
            max_rate: None,
            min_records: None,
            per_source,
        };

        let case_sensitive = clinker_plan::config::case_sensitive_dir(&lower).unwrap_or(true);
        let buckets = partition_dlq_entries(&entries, &cfg);

        if case_sensitive {
            // Two distinct files: separate buckets, one entry each.
            assert_eq!(buckets.len(), 2);
            assert_eq!(buckets[0].0, lower);
            assert_eq!(buckets[1].0, upper);
            assert_eq!(buckets[0].1.len(), 1);
            assert_eq!(buckets[1].1.len(), 1);
        } else {
            // One physical file: both entries collapse into the single
            // pipeline-wide bucket (first claimant of the key).
            assert_eq!(buckets.len(), 1);
            assert_eq!(buckets[0].0, lower);
            assert_eq!(
                buckets[0].1.len(),
                2,
                "case-variant per-source path must reuse the pipeline-wide writer"
            );
        }
    }
}
