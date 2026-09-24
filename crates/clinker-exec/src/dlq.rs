//! DLQ output: encode dead-letter-queue entries as CSV rows under the header
//! the compiled plan fixed for each dead-letter file.
//!
//! The DLQ category vocabulary and stage-label helpers live in
//! [`clinker_core_types::dlq`]; the buckets, the rule that routes a row to
//! one, and each bucket's header live in
//! [`clinker_plan::plan::dlq_layout::DlqLayout`]. This module owns the row
//! encoder that turns one [`DlqEntry`] into the on-disk CSV shape.

use clinker_record::owned_storage::{OwnedMap, SharedStorage};
use std::cell::Cell;
use std::io::Write;

use clinker_record::{FieldMetadata, Schema, Value};

use crate::executor::DlqEntry;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::dlq_layout::{DlqBucket, DlqLayout, dlq_user_columns};

/// Encodes dead-letter output one row at a time under the header a compiled
/// [`DlqLayout`] fixed for each bucket.
///
/// Holds one reusable CSV writer and output buffer, so encoding a row
/// allocates nothing beyond the cell text, plus a per-bucket cache mapping
/// each record schema seen to the header positions of its user columns. The
/// cache is bounded by the plan: one entry per distinct schema that reaches a
/// bucket. Returned bytes are valid until the next call. One encoder serves
/// one layout.
pub struct DlqRowEncoder {
    writer: csv::Writer<RowBytes>,
    out: Vec<u8>,
    positions: Vec<Vec<SchemaPositions>>,
}

/// Where each of a bucket's user columns sits in one record schema. The
/// schema handle is kept so its identity cannot be reused by another schema
/// while the cache holds it.
struct SchemaPositions {
    schema: SharedStorage<Schema>,
    columns: Vec<Option<usize>>,
}

/// The CSV writer's sink. The encoder hands its buffer in before a call and
/// takes it back after the writer flushes, so no allocation moves per row.
#[derive(Default)]
struct RowBytes(Cell<Vec<u8>>);

impl Write for RowBytes {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.get_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Default for DlqRowEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl DlqRowEncoder {
    /// A fresh encoder with an empty schema cache.
    pub fn new() -> Self {
        Self {
            // Headers of different buckets differ in length, so the writer
            // must not hold every record to the first record's field count.
            writer: csv::WriterBuilder::new()
                .flexible(true)
                .from_writer(RowBytes::default()),
            out: Vec::new(),
            positions: Vec::new(),
        }
    }

    /// The CSV header line of `bucket`, terminator included.
    pub fn header(&mut self, bucket: &DlqBucket) -> Result<&[u8], PipelineError> {
        self.begin();
        self.writer
            .write_record(bucket.header())
            .map_err(csv_error)?;
        self.finish()
    }

    /// One CSV row for `entry` under `bucket`'s header, terminator included.
    ///
    /// The engine columns carry the entry's identity and reason; each user
    /// column is placed at its header position, and a header column the
    /// record lacks is an empty cell. A user column of the record that the
    /// bucket's compiled header does not admit is a planner defect: it returns
    /// [`PipelineError::Internal`] and writes nothing. Under
    /// `include_source_row: false` no record column is looked up or formatted.
    pub fn row(
        &mut self,
        layout: &DlqLayout,
        bucket: &DlqBucket,
        entry: &DlqEntry,
    ) -> Result<&[u8], PipelineError> {
        let Self {
            writer,
            out,
            positions,
        } = self;
        // Resolve and check the record's columns before any byte is written,
        // so a refused row leaves the writer at a record boundary.
        let columns = if layout.include_source_row() {
            Some(record_positions(positions, layout, bucket, entry)?)
        } else {
            None
        };
        let mut buffer = std::mem::take(out);
        buffer.clear();
        writer.get_ref().0.set(buffer);

        let record = &entry.original_record;
        let id = uuid::Uuid::now_v7().to_string();
        let timestamp = chrono::Utc::now().to_rfc3339();
        writer.write_field(id).map_err(csv_error)?;
        writer.write_field(timestamp).map_err(csv_error)?;
        writer
            .write_field(source_file_of(entry))
            .map_err(csv_error)?;
        writer
            .write_field(entry.source_name.as_bytes())
            .map_err(csv_error)?;
        writer
            .write_field(entry.source_row.to_string())
            .map_err(csv_error)?;
        writer
            .write_field(entry.triggering_field.as_deref().unwrap_or(""))
            .map_err(csv_error)?;
        writer
            .write_field(
                entry
                    .triggering_value
                    .as_ref()
                    .map(value_to_string)
                    .unwrap_or_default(),
            )
            .map_err(csv_error)?;
        if layout.include_reason() {
            writer
                .write_field(entry.category.as_str())
                .map_err(csv_error)?;
            writer
                .write_field(entry.error_message.as_bytes())
                .map_err(csv_error)?;
        }
        writer
            .write_field(entry.stage.as_deref().unwrap_or(""))
            .map_err(csv_error)?;
        writer
            .write_field(entry.route.as_deref().unwrap_or(""))
            .map_err(csv_error)?;
        writer
            .write_field(if entry.trigger { "true" } else { "false" })
            .map_err(csv_error)?;
        if let Some(columns) = columns {
            for column in columns {
                match column.and_then(|index| record.values().get(index)) {
                    Some(value) => writer
                        .write_field(value_to_string(value))
                        .map_err(csv_error)?,
                    None => writer.write_field("").map_err(csv_error)?,
                }
            }
        }
        writer.write_record(None::<&[u8]>).map_err(csv_error)?;
        writer.flush().map_err(PipelineError::Io)?;
        *out = writer.get_ref().0.take();
        Ok(out)
    }

    fn begin(&mut self) {
        let mut buffer = std::mem::take(&mut self.out);
        buffer.clear();
        self.writer.get_ref().0.set(buffer);
    }

    fn finish(&mut self) -> Result<&[u8], PipelineError> {
        self.writer.flush().map_err(PipelineError::Io)?;
        self.out = self.writer.get_ref().0.take();
        Ok(&self.out)
    }
}

/// The header positions of `entry`'s record columns under `bucket`, from the
/// cache or resolved and checked once for a schema not seen before.
fn record_positions<'c>(
    cache: &'c mut Vec<Vec<SchemaPositions>>,
    layout: &DlqLayout,
    bucket: &DlqBucket,
    entry: &DlqEntry,
) -> Result<&'c [Option<usize>], PipelineError> {
    let node = || {
        entry
            .stage
            .clone()
            .unwrap_or_else(|| entry.source_name.as_ref().to_owned())
    };
    let Some(bucket_index) = layout
        .buckets()
        .iter()
        .position(|candidate| std::ptr::eq(candidate, bucket))
    else {
        return Err(PipelineError::Internal {
            op: "dead-letter",
            node: node(),
            detail: format!(
                "bucket {} is not part of the dead-letter layout it was encoded against",
                bucket.path().display()
            ),
        });
    };
    if cache.len() < layout.buckets().len() {
        cache.resize_with(layout.buckets().len(), Vec::new);
    }
    let schema = entry.original_record.schema();
    let per_bucket = &mut cache[bucket_index];
    if let Some(found) = per_bucket
        .iter()
        .position(|cached| SharedStorage::ptr_eq(&cached.schema, schema))
    {
        return Ok(&per_bucket[found].columns);
    }
    let user_columns = bucket.user_columns();
    let mut columns = vec![None; user_columns.len()];
    for (index, name) in dlq_user_columns(schema) {
        let Some(slot) = user_columns.iter().position(|column| column == name) else {
            return Err(PipelineError::Internal {
                op: "dead-letter",
                node: node(),
                detail: format!(
                    "column {name:?} of a dead-lettered record is outside the compiled dead-letter header of {}",
                    bucket.path().display()
                ),
            });
        };
        columns[slot] = Some(index);
    }
    let slot = per_bucket.len();
    per_bucket.push(SchemaPositions {
        schema: schema.clone(),
        columns,
    });
    Ok(&per_bucket[slot].columns)
}

fn csv_error(error: csv::Error) -> PipelineError {
    PipelineError::Format(error.into())
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
        Value::Array(arr) => serde_json::to_string(arr.as_slice()).unwrap_or_default(),
        // `Value::Map` only reaches the DLQ row builder if it lives at a
        // non-`$widened` column slot — i.e. the user explicitly emitted a
        // map at a regular column. The `dlq_user_columns` filter
        // already drops `$widened`, so a Map here is intentional user
        // output. JSON-encode for shape consistency with the
        // `Value::Array` case (regular non-JSON writers raise
        // `UnserializableMapValue` for the same situation; DLQ is
        // best-effort capture, not user-serializable output, so it
        // accepts the JSON-string degrade rather than failing).
        Value::Map(m) => serde_json::to_string(&BorrowedMap(m)).unwrap_or_default(),
    }
}

// Serialize the borrowed map as the same JSON object without copying its entries.
struct BorrowedMap<'a>(&'a OwnedMap);

impl serde::Serialize for BorrowedMap<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.iter().map(|(key, value)| (key.as_ref(), value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_core_types::dlq::DlqErrorCategory;
    use clinker_plan::config::{CompileContext, parse_config};
    use clinker_record::owned_storage::OwnedValues;
    use clinker_record::{Record, SchemaBuilder};
    use std::sync::Arc;

    /// The dead-letter layout the compiler derives for `yaml`. Every header
    /// these tests encode under comes from here, never from the entries.
    fn compiled_layout(yaml: &str) -> DlqLayout {
        compiled_plan(yaml)
            .dlq_layout()
            .expect("a DLQ block yields a layout")
            .clone()
    }

    fn compiled_plan(yaml: &str) -> clinker_plan::plan::CompiledPlan {
        parse_config(yaml)
            .expect("pipeline parses")
            .compile(&CompileContext::default())
            .expect("pipeline compiles")
    }

    /// A `continue` pipeline with one CSV Source `src` declaring `columns`
    /// as strings, feeding one plain CSV Sink. `dlq` is the body of the
    /// `error_handling.dlq` block.
    fn one_source_pipeline(dlq: &str, columns: &[&str]) -> String {
        let schema: String = columns
            .iter()
            .map(|c| format!("      - {{ name: {c}, type: string }}\n"))
            .collect();
        format!(
            "pipeline:\n  name: dlq_encoder\nerror_handling:\n  strategy: continue\n  dlq:\n{dlq}\
nodes:\n- type: source\n  name: src\n  config:\n    name: src\n    type: csv\n    path: in.csv\n    schema:\n{schema}\
- type: sink\n  name: out\n  input: src\n  config:\n    name: out\n    type: csv\n    path: out.csv\n"
        )
    }

    fn entry(record: Record, source: &str) -> DlqEntry {
        DlqEntry {
            source_row: 1.into(),
            category: DlqErrorCategory::TypeCoercionFailure,
            error_message: "bad value".to_owned(),
            original_record: record,
            stage: None,
            route: None,
            trigger: true,
            source_name: Arc::from(source),
            triggering_field: None,
            triggering_value: None,
        }
    }

    fn name_value_entry(row: u64, category: DlqErrorCategory, error: &str) -> DlqEntry {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["name".into(), "value".into()])));
        let record = Record::new(
            schema,
            vec![Value::String("Alice".into()), Value::String("bad".into())],
        );
        DlqEntry {
            source_row: row.into(),
            category,
            error_message: error.to_owned(),
            ..entry(record, "src")
        }
    }

    /// Encode `entries` into the bucket each routes to under `layout`,
    /// asserting they all route to one bucket, and parse the bytes back as
    /// CSV: the header line, then one row per entry.
    fn encode(layout: &DlqLayout, entries: &[DlqEntry]) -> (Vec<String>, Vec<Vec<String>>) {
        let ids: Vec<_> = entries
            .iter()
            .map(|e| {
                layout
                    .bucket_for_source(&e.source_name)
                    .expect("entry has a bucket")
            })
            .collect();
        assert!(
            ids.windows(2).all(|w| w[0] == w[1]),
            "entries of one test share a bucket"
        );
        let bucket = layout.bucket(ids[0]);
        let mut encoder = DlqRowEncoder::new();
        let mut bytes = encoder.header(bucket).expect("header").to_vec();
        for e in entries {
            bytes.extend_from_slice(encoder.row(layout, bucket, e).expect("row"));
        }
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(bytes.as_slice());
        let mut lines = reader
            .records()
            .map(|r| r.expect("csv line").iter().map(str::to_owned).collect())
            .collect::<Vec<Vec<String>>>()
            .into_iter();
        let header = lines.next().expect("header line");
        (header, lines.collect())
    }

    const ENGINE: [&str; 12] = [
        "_cxl_dlq_id",
        "_cxl_dlq_timestamp",
        "_cxl_dlq_source_file",
        "_cxl_dlq_source_name",
        "_cxl_dlq_source_row",
        "_cxl_dlq_triggering_field",
        "_cxl_dlq_triggering_value",
        "_cxl_dlq_error_category",
        "_cxl_dlq_error_detail",
        "_cxl_dlq_stage",
        "_cxl_dlq_route",
        "_cxl_dlq_trigger",
    ];

    #[test]
    fn nested_container_cells_preserve_json_shape_and_order() {
        let value = Value::map([
            ("z", Value::Integer(7)),
            ("a", Value::map([("child", Value::Bool(true))])),
        ]);
        assert_eq!(
            value_to_string(&value),
            r#"{"z":{"Integer":7},"a":{"Map":[["child",{"Bool":true}]]}}"#
        );
        let array = Value::Array(OwnedValues::from_vec(vec![value, Value::Null]));
        assert_eq!(
            value_to_string(&array),
            r#"[{"Map":[["z",{"Integer":7}],["a",{"Map":[["child",{"Bool":true}]]}]]},"Null"]"#
        );
    }

    /// One schema reaches the bucket, so its user columns keep the declared
    /// order (not a lexical one), and the encoder writes each value at its
    /// header position.
    #[test]
    fn plan_header_orders_single_schema_naturally() {
        let layout = compiled_layout(&one_source_pipeline(
            "    path: rejects.csv\n",
            &["zulu", "alpha", "mike"],
        ));
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "zulu".into(),
            "alpha".into(),
            "mike".into(),
        ])));
        let record = Record::new(
            schema,
            vec![
                Value::String("Z".into()),
                Value::String("A".into()),
                Value::String("M".into()),
            ],
        );
        let (header, rows) = encode(&layout, &[entry(record, "src")]);
        assert_eq!(header[..12], ENGINE);
        assert_eq!(
            header[12..],
            ["zulu", "alpha", "mike", "_cxl_dlq_source_record"]
        );
        assert_eq!(rows[0][12..], ["Z", "A", "M", ""]);
    }

    /// Two Sources with different schemas reach one pipeline-wide bucket.
    /// The header is their first-seen union in plan order, a shared column
    /// appears once, and each row leaves the other schema's columns empty.
    #[test]
    fn plan_header_first_seen_union_leaves_missing_cells_empty() {
        let plan = compiled_plan(
            r#"
pipeline:
  name: dlq_union
error_handling:
  strategy: continue
  dlq:
    path: rejects.csv
nodes:
- type: source
  name: alpha_src
  config:
    name: alpha_src
    type: csv
    path: alpha.csv
    schema:
      - { name: id, type: string }
      - { name: alpha, type: string }
- type: source
  name: beta_src
  config:
    name: beta_src
    type: csv
    path: beta.csv
    schema:
      - { name: id, type: string }
      - { name: beta, type: string }
- type: sink
  name: out_alpha
  input: alpha_src
  config:
    name: out_alpha
    type: csv
    path: out_alpha.csv
- type: sink
  name: out_beta
  input: beta_src
  config:
    name: out_beta
    type: csv
    path: out_beta.csv
"#,
        );
        let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
        // Sites are visited in plan topological order, which need not be
        // declaration order: the Source visited first contributes first.
        let dag = plan.dag();
        let first = dag
            .topo_order
            .iter()
            .map(|&idx| dag.graph[idx].name())
            .find(|name| *name == "alpha_src" || *name == "beta_src")
            .expect("both Sources are in the plan");
        let (first_own, second_own) = if first == "alpha_src" {
            ("alpha", "beta")
        } else {
            ("beta", "alpha")
        };
        let schema = |own: &str| {
            SchemaBuilder::new()
                .with_field("id")
                .with_field(own)
                .with_field_meta("$source.file", FieldMetadata::SourceFile)
                .build()
        };
        let beta = entry(
            Record::new(
                schema("beta"),
                vec![
                    Value::String("2".into()),
                    Value::String("B".into()),
                    Value::String("inputs/beta.csv".into()),
                ],
            ),
            "beta_src",
        );
        let alpha = entry(
            Record::new(
                schema("alpha"),
                vec![
                    Value::String("1".into()),
                    Value::String("A".into()),
                    Value::String("inputs/alpha.csv".into()),
                ],
            ),
            "alpha_src",
        );
        let (header, rows) = encode(layout, &[beta, alpha]);
        assert_eq!(
            header[12..],
            ["id", first_own, "_cxl_dlq_source_record", second_own],
            "first-seen union over sites in plan order; `id` appears once"
        );
        assert!(!header.iter().any(|c| c == "$source.file"));
        let col = |name: &str| header.iter().position(|c| c == name).unwrap();
        assert_eq!(rows[0][2], "inputs/beta.csv");
        assert_eq!(rows[0][col("id")], "2");
        assert_eq!(rows[0][col("alpha")], "");
        assert_eq!(rows[0][col("beta")], "B");
        assert_eq!(rows[1][2], "inputs/alpha.csv");
        assert_eq!(rows[1][col("id")], "1");
        assert_eq!(rows[1][col("alpha")], "A");
        assert_eq!(rows[1][col("beta")], "");
        assert!(rows.iter().all(|row| row.len() == header.len()));
    }

    /// A record column the compiled header does not admit is a planner
    /// defect: the row is refused as `Internal` and nothing is written, and
    /// the encoder's next row is still well formed.
    #[test]
    fn user_column_outside_bucket_header_is_internal() {
        let layout = compiled_layout(&one_source_pipeline(
            "    path: rejects.csv\n",
            &["name", "value"],
        ));
        let bucket = layout.bucket(layout.bucket_for_source("src").unwrap());
        let intruder = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "name".into(),
            "intruder".into(),
        ])));
        let refused = entry(
            Record::new(
                intruder,
                vec![
                    Value::String("Mallory".into()),
                    Value::String("smuggled".into()),
                ],
            ),
            "src",
        );
        let mut encoder = DlqRowEncoder::new();
        match encoder.row(&layout, bucket, &refused) {
            Err(PipelineError::Internal { op, detail, .. }) => {
                assert_eq!(op, "dead-letter");
                assert!(
                    detail.contains("\"intruder\"")
                        && detail.contains("outside the compiled dead-letter header"),
                    "{detail}"
                );
            }
            other => panic!("expected Internal, got {:?}", other.map(<[u8]>::to_vec)),
        }
        let good = name_value_entry(1, DlqErrorCategory::TypeCoercionFailure, "bad");
        let text = String::from_utf8(
            encoder
                .row(&layout, bucket, &good)
                .expect("next row")
                .to_vec(),
        )
        .expect("utf-8 row");
        assert_eq!(text.lines().count(), 1, "exactly one row: {text}");
        assert!(
            !text.contains("Mallory") && !text.contains("smuggled"),
            "{text}"
        );
        assert!(text.contains("Alice"), "{text}");
    }

    /// Under `include_source_row: false` the header is the engine columns
    /// alone and no record value is formatted, not even one outside every
    /// header.
    #[test]
    fn include_source_row_false_formats_no_user_column() {
        let layout = compiled_layout(&one_source_pipeline(
            "    path: rejects.csv\n    include_source_row: false\n",
            &["name", "value"],
        ));
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "name".into(),
            "unadmitted".into(),
        ])));
        let record = Record::new(
            schema,
            vec![
                Value::String("Alice".into()),
                Value::String("secret".into()),
            ],
        );
        let (header, rows) = encode(&layout, &[entry(record, "src")]);
        assert_eq!(header, ENGINE);
        assert_eq!(rows[0].len(), ENGINE.len());
        assert!(
            rows[0]
                .iter()
                .all(|cell| cell != "Alice" && cell != "secret"),
            "no user value reaches the row: {:?}",
            rows[0]
        );
    }

    #[test]
    fn engine_columns_carry_category_stage_route_and_trigger() {
        let layout = compiled_layout(&one_source_pipeline(
            "    path: rejects.csv\n",
            &["name", "value"],
        ));
        let categories = [
            DlqErrorCategory::MissingRequiredField,
            DlqErrorCategory::TypeCoercionFailure,
            DlqErrorCategory::RequiredFieldConversionFailure,
            DlqErrorCategory::NanInOutputField,
            DlqErrorCategory::AggregateTypeError,
            DlqErrorCategory::ValidationFailure,
        ];
        let mut entries: Vec<DlqEntry> = categories
            .iter()
            .map(|c| name_value_entry(3, *c, "why it failed"))
            .collect();
        entries[0].stage = Some("transform:calc".to_owned());
        entries[0].route = Some("high".to_owned());
        entries[0].trigger = false;
        let (header, rows) = encode(&layout, &entries);
        assert_eq!(header[..12], ENGINE);
        for (row, category) in rows.iter().zip(categories) {
            assert_eq!(row[3], "src");
            assert_eq!(row[4], "3");
            assert_eq!(row[7], category.as_str());
            assert_eq!(row[8], "why it failed");
        }
        assert_eq!(rows[0][9..12], ["transform:calc", "high", "false"]);
        assert_eq!(rows[1][9..12], ["", "", "true"]);
        assert_eq!(rows[0][2], "<merged>", "no `$source.file` stamp");
    }

    #[test]
    fn uuid_v7_ids_are_time_ordered_and_unique() {
        let layout = compiled_layout(&one_source_pipeline(
            "    path: rejects.csv\n",
            &["name", "value"],
        ));
        let entries: Vec<DlqEntry> = (0..1000)
            .map(|i| name_value_entry(i, DlqErrorCategory::TypeCoercionFailure, "err"))
            .collect();
        let (_, rows) = encode(&layout, &entries);
        let ids: Vec<uuid::Uuid> = rows
            .iter()
            .map(|row| uuid::Uuid::parse_str(&row[0]).expect("uuid"))
            .collect();
        assert!(ids.iter().all(|id| id.get_version_num() == 7));
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]),
            "ids increase in write order, so they are also distinct"
        );
    }

    #[test]
    fn timestamp_is_rfc3339() {
        let layout = compiled_layout(&one_source_pipeline(
            "    path: rejects.csv\n",
            &["name", "value"],
        ));
        let (_, rows) = encode(
            &layout,
            &[name_value_entry(
                1,
                DlqErrorCategory::TypeCoercionFailure,
                "e",
            )],
        );
        chrono::DateTime::parse_from_rfc3339(&rows[0][1])
            .expect("timestamp should be valid RFC 3339");
    }

    #[test]
    fn triggering_field_and_value_columns() {
        let layout = compiled_layout(&one_source_pipeline(
            "    path: rejects.csv\n",
            &["name", "value"],
        ));
        let mut e = name_value_entry(7, DlqErrorCategory::TypeCoercionFailure, "cannot");
        e.triggering_field = Some(Arc::from("amount"));
        e.triggering_value = Some(Value::String("not-a-number".into()));
        let (_, rows) = encode(&layout, &[e]);
        assert_eq!(rows[0][5], "amount");
        assert_eq!(rows[0][6], "not-a-number");
    }

    #[test]
    fn include_reason_false_drops_category_and_detail() {
        let layout = compiled_layout(&one_source_pipeline(
            "    path: rejects.csv\n    include_reason: false\n",
            &["name", "value"],
        ));
        let (header, rows) = encode(
            &layout,
            &[name_value_entry(
                1,
                DlqErrorCategory::TypeCoercionFailure,
                "e",
            )],
        );
        assert!(!header.iter().any(|c| c == "_cxl_dlq_error_category"));
        assert!(!header.iter().any(|c| c == "_cxl_dlq_error_detail"));
        assert_eq!(
            header[7..10],
            ["_cxl_dlq_stage", "_cxl_dlq_route", "_cxl_dlq_trigger"]
        );
        assert_eq!(header[10..12], ["name", "value"]);
        assert_eq!(rows[0][10..12], ["Alice", "bad"]);
        assert_eq!(rows[0].len(), header.len());
    }

    /// The `auto_widen` `$widened` sidecar is never a header column and never
    /// reaches a cell; the correlation shadow `$ck.<field>` is kept.
    #[test]
    fn widened_sidecar_is_filtered_and_correlation_shadow_kept() {
        let layout = compiled_layout(
            r#"
pipeline:
  name: dlq_widened
error_handling:
  strategy: continue
  dlq:
    path: rejects.csv
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: in.csv
    correlation_key: employee_id
    schema:
      - { name: employee_id, type: string }
      - { name: salary, type: int }
- type: transform
  name: validate
  input: src
  config:
    cxl: |
      emit emp_id = employee_id
      emit pay = salary
- type: sink
  name: out
  input: validate
  config:
    name: out
    type: csv
    path: out.csv
"#,
        );
        let schema = SchemaBuilder::new()
            .with_field("employee_id")
            .with_field("salary")
            .with_field_meta(
                "$ck.employee_id",
                FieldMetadata::source_correlation("employee_id"),
            )
            .with_field_meta("$widened", FieldMetadata::widened_sidecar())
            .build();
        let mut sidecar = indexmap::IndexMap::new();
        sidecar.insert("region".into(), Value::String("US".into()));
        let record = Record::new(
            schema,
            vec![
                Value::String("E001".into()),
                Value::Integer(50000),
                Value::String("E001".into()),
                Value::Map(OwnedMap::from_map(sidecar)),
            ],
        );
        let (header, rows) = encode(&layout, &[entry(record, "src")]);
        assert!(!header.iter().any(|c| c == "$widened"), "{header:?}");
        assert!(header.iter().any(|c| c == "$ck.employee_id"), "{header:?}");
        let col = |name: &str| header.iter().position(|c| c == name).unwrap();
        assert_eq!(rows[0][col("employee_id")], "E001");
        assert_eq!(rows[0][col("salary")], "50000");
        assert_eq!(rows[0][col("$ck.employee_id")], "E001");
        assert_eq!(rows[0].len(), header.len());
        assert!(rows[0].iter().all(|cell| !cell.contains("region")));
    }
}
