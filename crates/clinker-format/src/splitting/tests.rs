//! Splitting tests — `SplittingWriter` format-agnostic rotation.
//!
//! Tests: record count splitting, byte size splitting, key-group preservation,
//! oversize group policies, CSV header repetition, file naming, byte counting accuracy.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use clinker_record::{Record, Schema, Value};

use super::*;
use crate::counting::{CountingWriter, SharedByteCounter};
use crate::csv::writer::{CsvWriter, CsvWriterConfig};
use crate::traits::FormatWriter;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Shared in-memory buffer for capturing writer output.
#[derive(Clone)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }

    fn byte_len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Registry of all files created by the factory.
struct FileRegistry {
    files: Arc<Mutex<Vec<SharedBuffer>>>,
}

impl FileRegistry {
    fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn file_factory(&self) -> FileFactory {
        let files = Arc::clone(&self.files);
        Box::new(move |_seq: u32| -> io::Result<Box<dyn Write + Send>> {
            let buf = SharedBuffer::new();
            files.lock().unwrap().push(buf.clone());
            Ok(Box::new(buf))
        })
    }

    fn file_count(&self) -> usize {
        self.files.lock().unwrap().len()
    }

    fn file_contents(&self) -> Vec<String> {
        self.files
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.contents())
            .collect()
    }
}

fn make_schema(cols: &[&str]) -> Arc<Schema> {
    Arc::new(Schema::new(cols.iter().map(|c| (*c).into()).collect()))
}

fn make_record(schema: &Arc<Schema>, values: Vec<Value>) -> Record {
    Record::new(Arc::clone(schema), values)
}

/// Build a CSV writer factory with header capture support.
///
/// Creates `HeaderCapturingCsvWriter` on first call (captures header from
/// first record into shared state). Subsequent calls create a `CsvWriter`
/// and replay the captured header via `write_preset_header()`.
fn csv_writer_factory(config: CsvWriterConfig, repeat_header: bool) -> WriterFactory {
    let shared_header: Arc<Mutex<Option<Vec<Box<str>>>>> = Arc::new(Mutex::new(None));
    let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

    Box::new(
        move |counting: CountingWriter<Box<dyn Write + Send>>, schema: Arc<Schema>| {
            let seq = call_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if seq == 0 {
                // First file: use HeaderCapturingCsvWriter to capture header
                use crate::csv::writer::HeaderCapturingCsvWriter;
                let csv = CsvWriter::new(counting, Arc::clone(&schema), config.clone());
                Ok(Box::new(HeaderCapturingCsvWriter::new(
                    csv,
                    Arc::clone(&schema),
                    Arc::clone(&shared_header),
                )) as Box<dyn FormatWriter>)
            } else {
                // Subsequent files: replay captured header
                let mut csv = CsvWriter::new(counting, Arc::clone(&schema), config.clone());
                if repeat_header {
                    if let Some(ref header) = *shared_header.lock().unwrap() {
                        csv.write_preset_header(header)?;
                    }
                }
                Ok(Box::new(csv) as Box<dyn FormatWriter>)
            }
        },
    )
}

/// Build a simple CSV writer factory (no header capture — for tests with `include_header: false`).
fn csv_writer_factory_simple(config: CsvWriterConfig) -> WriterFactory {
    Box::new(
        move |counting: CountingWriter<Box<dyn Write + Send>>, schema: Arc<Schema>| {
            let csv = CsvWriter::new(counting, schema, config.clone());
            Ok(Box::new(csv) as Box<dyn FormatWriter>)
        },
    )
}

fn count_csv_data_rows(csv_text: &str) -> usize {
    let lines: Vec<&str> = csv_text.lines().collect();
    if lines.is_empty() {
        return 0;
    }
    // First line is header, rest are data
    lines.len() - 1
}

// ---------------------------------------------------------------------------
// CountingWriter tests
// ---------------------------------------------------------------------------

#[test]
fn test_counting_writer_accuracy() {
    let counter = SharedByteCounter::new();
    let mut buf = Vec::new();
    let mut cw = CountingWriter::new(&mut buf, counter.clone());
    let data1 = b"hello";
    let data2 = b" world";
    cw.write_all(data1).unwrap();
    cw.write_all(data2).unwrap();
    assert_eq!(cw.bytes_written(), 11);
    assert_eq!(counter.bytes_written(), 11);
    assert_eq!(buf.len(), 11);
}

#[test]
fn test_counting_writer_flush_on_rotation() {
    let counter = SharedByteCounter::new();
    let buf = SharedBuffer::new();
    let mut cw = CountingWriter::new(buf.clone(), counter.clone());
    cw.write_all(b"data before flush").unwrap();
    cw.flush().unwrap();
    // After flush, all data should be available
    assert_eq!(buf.byte_len(), 17);
    assert_eq!(cw.bytes_written(), 17);
    assert_eq!(counter.bytes_written(), 17);
}

// ---------------------------------------------------------------------------
// SplittingWriter tests
// ---------------------------------------------------------------------------

#[test]
fn test_split_by_record_count() {
    // 100 records, max_records=30 → 4 files (30+30+30+10)
    let schema = make_schema(&["id", "name"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(30),
        max_bytes: None,
        group_key: None,
        repeat_header: true,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory(CsvWriterConfig::default(), true),
        Arc::clone(&schema),
        policy,
    );

    for i in 0..100 {
        let record = make_record(
            &schema,
            vec![Value::Integer(i), Value::String(format!("name_{i}").into())],
        );
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    assert_eq!(registry.file_count(), 4);
    let contents = registry.file_contents();
    assert_eq!(count_csv_data_rows(&contents[0]), 30);
    assert_eq!(count_csv_data_rows(&contents[1]), 30);
    assert_eq!(count_csv_data_rows(&contents[2]), 30);
    assert_eq!(count_csv_data_rows(&contents[3]), 10);
}

#[test]
fn test_split_by_byte_size() {
    // Each record is ~10-15 bytes. Set threshold low to force multiple files.
    let schema = make_schema(&["x"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: None,
        max_bytes: Some(50),
        group_key: None,
        repeat_header: false,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory_simple(CsvWriterConfig {
            include_header: false,
            ..CsvWriterConfig::default()
        }),
        Arc::clone(&schema),
        policy,
    );

    // Write 20 records of "12345\n" = 6 bytes each = 120 bytes total
    // At 50-byte threshold: should get multiple files
    for _ in 0..20 {
        let record = make_record(&schema, vec![Value::String("12345".into())]);
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    assert!(
        registry.file_count() > 1,
        "should have split into multiple files"
    );
    // Total data across all files should be 20 records
    let total_rows: usize = registry
        .file_contents()
        .iter()
        .map(|c| c.lines().count())
        .sum();
    assert_eq!(total_rows, 20);
}

#[test]
fn test_split_preserves_key_groups() {
    // Group key "dept", 10 records per dept, max_records=15
    // dept=A (10 records) should stay together, dept=B (10 records) should start a new file
    let schema = make_schema(&["dept", "id"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(15),
        max_bytes: None,
        group_key: Some("dept".into()),
        repeat_header: true,
        oversize_group: OversizeGroupPolicy::Allow,
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory(CsvWriterConfig::default(), true),
        Arc::clone(&schema),
        policy,
    );

    // Write 10 records for dept A, then 10 for dept B, then 10 for dept C
    for dept in &["A", "B", "C"] {
        for i in 0..10 {
            let record = make_record(
                &schema,
                vec![Value::String((*dept).into()), Value::Integer(i)],
            );
            writer.write_record(&record).unwrap();
        }
    }
    writer.flush().unwrap();

    // File 1: dept A (10 records, under limit 15)
    // When B starts, limit not reached (10 < 15), so B stays in file 1
    // At 15 records (5 B records in), limit reached. Next key change triggers rotation.
    // When all B records done (20 total in file 1), next C record triggers rotation.
    // File 2: dept C (10 records)
    assert_eq!(registry.file_count(), 2);
    let contents = registry.file_contents();
    // File 1: A (10) + B (10) = 20 data rows (group boundary respected)
    assert_eq!(count_csv_data_rows(&contents[0]), 20);
    // File 2: C (10) = 10 data rows
    assert_eq!(count_csv_data_rows(&contents[1]), 10);
}

#[test]
fn test_split_oversize_group_warn() {
    // Single group > limit with warn policy → one file with warning (no error)
    let schema = make_schema(&["dept", "id"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(5),
        max_bytes: None,
        group_key: Some("dept".into()),
        repeat_header: true,
        oversize_group: OversizeGroupPolicy::Warn,
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory(CsvWriterConfig::default(), true),
        Arc::clone(&schema),
        policy,
    );

    // Write 10 records for the same dept — exceeds limit of 5
    for i in 0..10 {
        let record = make_record(
            &schema,
            vec![Value::String("SINGLE".into()), Value::Integer(i)],
        );
        writer.write_record(&record).unwrap(); // should not error
    }
    writer.flush().unwrap();

    // All records in one file (can't split mid-group)
    assert_eq!(registry.file_count(), 1);
    assert_eq!(count_csv_data_rows(&registry.file_contents()[0]), 10);
}

#[test]
fn test_split_oversize_group_error() {
    // Single group > limit with error policy → PipelineError
    let schema = make_schema(&["dept", "id"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(3),
        max_bytes: None,
        group_key: Some("dept".into()),
        repeat_header: true,
        oversize_group: OversizeGroupPolicy::Error,
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory(CsvWriterConfig::default(), true),
        Arc::clone(&schema),
        policy,
    );

    // Write records for same dept until error
    let mut errored = false;
    for i in 0..10 {
        let record = make_record(
            &schema,
            vec![Value::String("SINGLE".into()), Value::Integer(i)],
        );
        if let Err(e) = writer.write_record(&record) {
            assert!(
                format!("{e}").contains("oversize group"),
                "error should mention oversize group: {e}"
            );
            errored = true;
            break;
        }
    }
    assert!(errored, "should have errored on oversize group");
}

#[test]
fn test_split_csv_repeat_header() {
    // Each split CSV file starts with header row
    let schema = make_schema(&["name", "age"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(2),
        max_bytes: None,
        group_key: None,
        repeat_header: true,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory(CsvWriterConfig::default(), true),
        Arc::clone(&schema),
        policy,
    );

    for i in 0..6 {
        let record = make_record(
            &schema,
            vec![
                Value::String(format!("person_{i}").into()),
                Value::Integer(20 + i),
            ],
        );
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    assert_eq!(registry.file_count(), 3);
    for (idx, content) in registry.file_contents().iter().enumerate() {
        let first_line = content.lines().next().unwrap();
        assert_eq!(
            first_line, "name,age",
            "file {idx} should start with header"
        );
    }
}

#[test]
fn test_split_csv_header_consistent() {
    // All split files share an identical header captured from the
    // schema (not from the first record's overflow — that storage no
    // longer exists).
    let schema = make_schema(&["id", "extra"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(2),
        max_bytes: None,
        group_key: None,
        repeat_header: true,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory(CsvWriterConfig::default(), true),
        Arc::clone(&schema),
        policy,
    );

    for i in 0..4 {
        let r = make_record(
            &schema,
            vec![Value::Integer(i), Value::String(format!("val_{i}").into())],
        );
        writer.write_record(&r).unwrap();
    }
    writer.flush().unwrap();

    let contents = registry.file_contents();
    assert_eq!(contents.len(), 2);
    let header1 = contents[0].lines().next().unwrap();
    let header2 = contents[1].lines().next().unwrap();
    assert_eq!(header1, "id,extra");
    assert_eq!(
        header2, "id,extra",
        "rotated file should have same header as first"
    );
}

#[test]
fn test_split_csv_header_excluded_from_count() {
    // Header row not counted in records_written
    let schema = make_schema(&["x"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(3),
        max_bytes: None,
        group_key: None,
        repeat_header: true,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory(CsvWriterConfig::default(), true),
        Arc::clone(&schema),
        policy,
    );

    // Write exactly 6 records → should be 2 files of 3 each
    for i in 0..6 {
        let record = make_record(&schema, vec![Value::Integer(i)]);
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    assert_eq!(registry.file_count(), 2);
    // Each file: 1 header + 3 data = 4 lines
    for content in &registry.file_contents() {
        assert_eq!(content.lines().count(), 4, "header + 3 data rows");
    }
}

#[test]
fn test_split_naming_pattern() {
    // Test that apply_split_naming produces correct names
    let name = crate::splitting::tests::apply_split_naming_wrapper(
        "output/results.csv",
        "{stem}_{seq:04}.{ext}",
        1,
    );
    assert_eq!(name, "output/results_0001.csv");

    let name2 = crate::splitting::tests::apply_split_naming_wrapper(
        "data/export.json",
        "{stem}_{seq:04}.{ext}",
        42,
    );
    assert_eq!(name2, "data/export_0042.json");
}

#[test]
fn test_split_both_limits_either_triggers() {
    // max_records + max_bytes → rotate on whichever fires first
    let schema = make_schema(&["data"]);
    let registry = FileRegistry::new();
    // Very low byte limit — should trigger before record count
    let policy = SplitPolicy {
        max_records: Some(1000), // high record limit
        max_bytes: Some(30),     // low byte limit
        group_key: None,
        repeat_header: false,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory_simple(CsvWriterConfig {
            include_header: false,
            ..CsvWriterConfig::default()
        }),
        Arc::clone(&schema),
        policy,
    );

    // Each "longvalue" record is ~10 bytes. At 30-byte limit, should split after ~3 records.
    for _ in 0..9 {
        let record = make_record(&schema, vec![Value::String("longvalue".into())]);
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    assert!(
        registry.file_count() > 1,
        "byte limit should have triggered splitting"
    );
}

#[test]
fn test_split_no_group_key_mechanical() {
    // Without group_key → exact split at limit
    let schema = make_schema(&["id"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(5),
        max_bytes: None,
        group_key: None, // no group key
        repeat_header: false,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory_simple(CsvWriterConfig {
            include_header: false,
            ..CsvWriterConfig::default()
        }),
        Arc::clone(&schema),
        policy,
    );

    for i in 0..13 {
        let record = make_record(&schema, vec![Value::Integer(i)]);
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    // 13 records / 5 per file = 3 files (5+5+3)
    assert_eq!(registry.file_count(), 3);
    let contents = registry.file_contents();
    assert_eq!(contents[0].lines().count(), 5);
    assert_eq!(contents[1].lines().count(), 5);
    assert_eq!(contents[2].lines().count(), 3);
}

#[test]
fn test_split_zero_records_no_file() {
    // Zero input records → no output files created
    let schema = make_schema(&["x"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(10),
        max_bytes: None,
        group_key: None,
        repeat_header: true,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory(CsvWriterConfig::default(), true),
        Arc::clone(&schema),
        policy,
    );

    writer.flush().unwrap();
    assert_eq!(
        registry.file_count(),
        0,
        "zero records should produce no files"
    );
}

#[test]
fn test_split_null_key_treated_as_group() {
    // Null key → distinct group that stays together
    let schema = make_schema(&["dept", "id"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(3),
        max_bytes: None,
        group_key: Some("dept".into()),
        repeat_header: false,
        oversize_group: OversizeGroupPolicy::Allow,
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory_simple(CsvWriterConfig {
            include_header: false,
            ..CsvWriterConfig::default()
        }),
        Arc::clone(&schema),
        policy,
    );

    // 5 records with null dept — should all stay in one file (oversize allowed)
    for i in 0..5 {
        let record = make_record(&schema, vec![Value::Null, Value::Integer(i)]);
        writer.write_record(&record).unwrap();
    }
    // Then 2 records with dept "A" — triggers rotation (key changed + limit reached)
    for i in 0..2 {
        let record = make_record(&schema, vec![Value::String("A".into()), Value::Integer(i)]);
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    assert_eq!(registry.file_count(), 2);
    let contents = registry.file_contents();
    // File 1: 5 null-key records
    assert_eq!(contents[0].lines().count(), 5);
    // File 2: 2 "A" records
    assert_eq!(contents[1].lines().count(), 2);
}

#[test]
fn test_split_by_byte_size_only() {
    // max_bytes only (no max_records)
    let schema = make_schema(&["val"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: None,
        max_bytes: Some(20),
        group_key: None,
        repeat_header: false,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory_simple(CsvWriterConfig {
            include_header: false,
            ..CsvWriterConfig::default()
        }),
        Arc::clone(&schema),
        policy,
    );

    // Each record "abcdef\n" = 7 bytes. At 20 byte limit: ~3 records per file.
    for _ in 0..9 {
        let record = make_record(&schema, vec![Value::String("abcdef".into())]);
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    assert!(
        registry.file_count() >= 3,
        "should split into at least 3 files"
    );
    let total_rows: usize = registry
        .file_contents()
        .iter()
        .map(|c| c.lines().count())
        .sum();
    assert_eq!(total_rows, 9);
}

#[test]
fn test_split_single_record_one_file() {
    // One record → one file
    let schema = make_schema(&["x"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(10),
        max_bytes: None,
        group_key: None,
        repeat_header: true,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        csv_writer_factory(CsvWriterConfig::default(), true),
        Arc::clone(&schema),
        policy,
    );

    let record = make_record(&schema, vec![Value::Integer(42)]);
    writer.write_record(&record).unwrap();
    writer.flush().unwrap();

    assert_eq!(registry.file_count(), 1);
    assert_eq!(count_csv_data_rows(&registry.file_contents()[0]), 1);
}

/// SplittingWriter with JSON NDJSON format produces valid JSON on each split file.
/// Each file should be independently valid NDJSON (one JSON object per line).
#[test]
fn test_splitting_writer_json_produces_valid_files() {
    use crate::json::writer::{JsonOutputMode, JsonWriter, JsonWriterConfig};

    let schema = make_schema(&["id", "name"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(3),
        max_bytes: None,
        group_key: None,
        repeat_header: false,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let json_config = JsonWriterConfig {
        format: JsonOutputMode::Ndjson,
        pretty: false,
        preserve_nulls: false,
    };

    let json_factory: WriterFactory = Box::new(
        move |counting: CountingWriter<Box<dyn Write + Send>>, schema: Arc<Schema>| {
            Ok(
                Box::new(JsonWriter::new(counting, schema, json_config.clone()))
                    as Box<dyn FormatWriter>,
            )
        },
    );

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        json_factory,
        Arc::clone(&schema),
        policy,
    );

    // Write 7 records → should produce 3 files (3+3+1)
    for i in 0..7 {
        let record = make_record(
            &schema,
            vec![Value::Integer(i), Value::String(format!("name_{i}").into())],
        );
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    assert_eq!(registry.file_count(), 3, "expected 3 split files");
    let contents = registry.file_contents();

    // Each file should contain valid NDJSON lines
    let mut total_lines = 0;
    for (idx, content) in contents.iter().enumerate() {
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert!(!lines.is_empty(), "file {idx} should not be empty");
        for (li, line) in lines.iter().enumerate() {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
            assert!(parsed.is_ok(), "file {idx} line {li} invalid JSON: {line}");
            let obj = parsed.unwrap();
            assert!(obj.is_object(), "file {idx} line {li} not an object");
            assert!(obj.get("id").is_some(), "missing 'id' field");
            assert!(obj.get("name").is_some(), "missing 'name' field");
        }
        total_lines += lines.len();
    }
    assert_eq!(total_lines, 7, "total records across all files");
}

/// SplittingWriter with XML format produces valid XML files on rotation.
/// Each split file must have proper root element open/close tags.
#[test]
fn test_splitting_writer_xml_produces_valid_files() {
    use crate::xml::writer::{XmlWriter, XmlWriterConfig};

    let schema = make_schema(&["id", "val"]);
    let registry = FileRegistry::new();
    let policy = SplitPolicy {
        max_records: Some(3),
        max_bytes: None,
        group_key: None,
        repeat_header: false,
        oversize_group: OversizeGroupPolicy::default(),
    };

    let xml_config = XmlWriterConfig {
        root_element: "items".into(),
        record_element: "item".into(),
        preserve_nulls: false,
    };

    let xml_factory: WriterFactory = Box::new(
        move |counting: CountingWriter<Box<dyn Write + Send>>, schema: Arc<Schema>| {
            Ok(
                Box::new(XmlWriter::new(counting, schema, xml_config.clone()))
                    as Box<dyn FormatWriter>,
            )
        },
    );

    let mut writer = SplittingWriter::new(
        registry.file_factory(),
        xml_factory,
        Arc::clone(&schema),
        policy,
    );

    // Write 7 records → 3 files (3+3+1)
    for i in 0..7 {
        let record = make_record(
            &schema,
            vec![Value::Integer(i), Value::String(format!("v{i}").into())],
        );
        writer.write_record(&record).unwrap();
    }
    writer.flush().unwrap();

    assert_eq!(registry.file_count(), 3, "expected 3 split files");
    let contents = registry.file_contents();

    for (idx, content) in contents.iter().enumerate() {
        assert!(
            content.contains("<items>"),
            "file {idx} missing root open tag: {content}"
        );
        assert!(
            content.contains("</items>"),
            "file {idx} missing root close tag: {content}"
        );
        assert!(
            content.contains("<item>"),
            "file {idx} missing record element: {content}"
        );
    }

    // Verify record counts: file 0 and 1 have 3 <item> each, file 2 has 1
    let count_items = |s: &str| s.matches("<item>").count();
    assert_eq!(count_items(&contents[0]), 3);
    assert_eq!(count_items(&contents[1]), 3);
    assert_eq!(count_items(&contents[2]), 1);
}

// Helper for test_split_naming_pattern — wraps the executor's apply_split_naming logic
// (we duplicate it here since the original is in clinker-core and we're in clinker-format)
pub(crate) fn apply_split_naming_wrapper(base_path: &str, naming: &str, seq: u32) -> String {
    let path = std::path::Path::new(base_path);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("dat");
    let parent = path.parent().unwrap_or(std::path::Path::new(""));

    let filename = naming
        .replace("{stem}", stem)
        .replace("{ext}", ext)
        .replace("{seq:04}", &format!("{seq:04}"));

    parent.join(filename).to_string_lossy().into_owned()
}
