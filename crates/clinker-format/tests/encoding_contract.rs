use clinker_format::FormatReader;
use clinker_format::FormatWriter;
use clinker_format::charset::Charset;
use clinker_format::csv::writer::{
    CsvEncoder, CsvEncoderConfig, CsvEncoderOptions, CsvWriterConfig,
};
use clinker_format::csv::{CsvReader, CsvReaderConfig};
use clinker_format::preparation::{MemoryOnlyResources, PreparedWriter};
use clinker_record::Value;
use clinker_record::owned_storage::{OwnedValues, SharedStorage};
use clinker_record::{Record, Schema};
use std::num::NonZeroUsize;
use std::sync::Arc;

use clinker_format::preparation::{DecodeWorkspace, TextStorage};

#[derive(Default)]
struct DecodeAuthority {
    used: std::sync::atomic::AtomicUsize,
    calls: std::sync::atomic::AtomicUsize,
    fail_at: std::sync::atomic::AtomicUsize,
    cancelled: std::sync::atomic::AtomicBool,
}

impl clinker_record::owned_storage::AllocationAuthority for DecodeAuthority {
    fn try_reserve(
        self: Arc<Self>,
        owner: clinker_record::owned_storage::OwnerId,
        layout: std::alloc::Layout,
    ) -> Result<
        clinker_record::owned_storage::AllocationLease,
        clinker_record::owned_storage::ResourceError,
    > {
        use clinker_record::owned_storage::{AllocationLease, ResourceError, ResourceErrorKind};
        use std::sync::atomic::Ordering::SeqCst;
        self.check_cancelled()?;
        let call = self.calls.fetch_add(1, SeqCst) + 1;
        if self.fail_at.load(SeqCst) == call {
            return Err(ResourceError::new(
                ResourceErrorKind::Budget,
                layout.size(),
                0,
            ));
        }
        let used = self.used.load(SeqCst);
        if layout.size() > 1_048_576 - used {
            return Err(ResourceError::new(
                ResourceErrorKind::Budget,
                layout.size(),
                1_048_576 - used,
            ));
        }
        self.used.fetch_add(layout.size(), SeqCst);
        AllocationLease::admitted(self, owner, layout.size())
    }

    fn release(&self, _: clinker_record::owned_storage::OwnerId, bytes: usize) {
        self.used
            .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
    }

    fn check_cancelled(&self) -> Result<(), clinker_record::owned_storage::ResourceError> {
        use clinker_record::owned_storage::{ResourceError, ResourceErrorKind};
        if self.cancelled.load(std::sync::atomic::Ordering::SeqCst) {
            Err(ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
        } else {
            Ok(())
        }
    }
}

fn decode_workspace(authority: &Arc<DecodeAuthority>) -> DecodeWorkspace {
    DecodeWorkspace::new(clinker_record::owned_storage::AllocationResources::new(
        authority.clone(),
    ))
    .unwrap()
}

fn multi_record_spec(tag: &str) -> clinker_format::multi_record::MultiRecordSpec {
    use clinker_format::schema::{Column, Discriminator, RecordType, StructureConstraint};
    use cxl::typecheck::Type;
    let record = |id: &str, tag: &str, columns| RecordType {
        id: id.into(),
        tag: tag.into(),
        description: None,
        parent: None,
        join_key: None,
        columns,
    };
    clinker_format::multi_record::MultiRecordSpec {
        discriminator: Discriminator {
            start: None,
            width: None,
            field: Some("kind".into()),
        },
        record_types: vec![
            record(
                "batch",
                "H",
                vec![
                    Column::bare("kind", Type::String),
                    Column::bare("label", Type::String),
                ],
            ),
            record(
                "detail",
                tag,
                vec![
                    Column::bare("kind", Type::String),
                    Column::bare("text", Type::String),
                    Column::bare("number", Type::Numeric),
                ],
            ),
            record(
                "end",
                "T",
                vec![
                    Column::bare("kind", Type::String),
                    Column::bare("count", Type::Int),
                ],
            ),
        ],
        structure: vec![StructureConstraint {
            record: "end".into(),
            count: "count".into(),
        }],
        header_tags: vec!["H".into()],
    }
}

fn multi_record_envelope() -> clinker_format::envelope::EnvelopeConfig {
    use clinker_format::envelope::{
        EnvelopeConfig, EnvelopeExtract, EnvelopeFieldType, EnvelopeSection,
    };
    EnvelopeConfig {
        sections: indexmap::IndexMap::from([(
            "customer_metadata".into(),
            EnvelopeSection {
                extract: EnvelopeExtract::RecordType("H".into()),
                fields: indexmap::IndexMap::from([("label".into(), EnvelopeFieldType::String)]),
            },
        )]),
    }
}

fn multi_record_dialect(has_header: bool) -> clinker_format::multi_record::CsvDialect {
    clinker_format::multi_record::CsvDialect {
        delimiter: b',',
        quote_char: b'"',
        has_header,
    }
}

#[test]
fn multi_record_charset_routes_high_tags_and_preserves_retained_owners() {
    use clinker_format::multi_record::MultiRecordReader;
    for charset in [Charset::Utf8, Charset::Latin1] {
        for has_header in [false, true] {
            for storage in [TextStorage::Shared, TextStorage::Unique] {
                let authority = Arc::new(DecodeAuthority::default());
                let label = "caf\u{e9} ".repeat(20);
                let value = "na\u{ef}ve,quoted\nline;still one scalar ".repeat(10);
                let prefix = if has_header {
                    "kind,t\u{e9}xt,numeric\n"
                } else {
                    ""
                };
                let input = format!("{prefix}\u{c0},{label}\n\u{e9},\"{value}\",1.25\nT,1\n");
                let bytes = match charset {
                    Charset::Utf8 => input.into_bytes(),
                    Charset::Latin1 => input
                        .chars()
                        .map(|ch| u8::try_from(ch as u32).unwrap())
                        .collect(),
                };
                let resources =
                    clinker_record::owned_storage::AllocationResources::new(authority.clone());
                let mut spec = multi_record_spec("\u{e9}");
                spec.record_types[0].tag = "\u{c0}".into();
                spec.header_tags[0] = "\u{c0}".into();
                let mut envelope = multi_record_envelope();
                envelope.sections["customer_metadata"].extract =
                    clinker_format::envelope::EnvelopeExtract::RecordType("\u{c0}".into());
                let mut reader = MultiRecordReader::new_csv_admitted(
                    bytes.as_slice(),
                    spec,
                    multi_record_dialect(has_header),
                    charset,
                    decode_workspace(&authority),
                    storage,
                )
                .unwrap();
                let sections = reader.prepare_document(&envelope).unwrap();
                let Value::Map(section) = &sections["customer_metadata"] else {
                    panic!("section map");
                };
                assert_eq!(section["label"], Value::from(label.trim()));
                assert_eq!(sections.unaccounted_heap_size(&resources), 0);
                let record = reader.next_record().unwrap().unwrap();
                assert_eq!(record.get("record_type"), Some(&Value::from("detail")));
                assert_eq!(record.get("text"), Some(&Value::from(value.trim())));
                assert_eq!(record.get("number"), Some(&Value::Float(1.25)));
                assert!(record.values_are_governed());
                assert_eq!(record.legacy_estimated_heap_size(), 0);
                assert!(reader.next_record().unwrap().is_none());
                drop(reader);
                assert!(authority.used.load(std::sync::atomic::Ordering::SeqCst) > 0);
                drop(sections);
                assert_eq!(record.get("text"), Some(&Value::from(value.trim())));
                drop(record);
                assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
            }
        }
    }
}

#[test]
fn multi_record_bom_policy_is_applied_once_before_csv_grammar() {
    use clinker_format::multi_record::MultiRecordReader;
    for charset in [Charset::Utf8, Charset::Latin1] {
        for has_header in [false, true] {
            let authority = Arc::new(DecodeAuthority::default());
            let mut spec = multi_record_spec(if charset == Charset::Latin1 && !has_header {
                "\u{ef}\u{bb}\u{bf}D"
            } else {
                "D"
            });
            spec.header_tags.clear();
            let input: &[u8] = if has_header {
                b"\xef\xbb\xbfkind,text,number\r\nD,\"quoted, body\",2\r\nT,1\r\n"
            } else {
                b"\xef\xbb\xbfD,\"quoted, body\",2\r\nT,1\r\n"
            };
            let mut reader = MultiRecordReader::new_csv_admitted(
                input,
                spec,
                multi_record_dialect(has_header),
                charset,
                decode_workspace(&authority),
                TextStorage::Shared,
            )
            .unwrap();
            let record = reader.next_record().unwrap().unwrap();
            assert_eq!(record.get("text"), Some(&Value::from("quoted, body")));
            assert_eq!(record.get("number"), Some(&Value::Integer(2)));
            assert!(reader.next_record().unwrap().is_none());
            drop(record);
            drop(reader);
            assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn multi_record_utf8_is_strict_in_skipped_headers_and_late_cells() {
    use clinker_format::multi_record::MultiRecordReader;
    for (input, header, accepted) in [
        (&b"kind,\xff,number\nD,ok,1\nT,1\n"[..], true, false),
        (&b"D,ok,1\nD,\xff,2\nT,2\n"[..], false, true),
        (&b"H,\xff\nD,ok,1\nT,1\n"[..], false, false),
    ] {
        let authority = Arc::new(DecodeAuthority::default());
        let mut reader = MultiRecordReader::new_csv_admitted(
            input,
            multi_record_spec("D"),
            multi_record_dialect(header),
            Charset::Utf8,
            decode_workspace(&authority),
            TextStorage::Shared,
        )
        .unwrap();
        if accepted {
            assert!(reader.next_record().unwrap().is_some());
        }
        assert!(matches!(
            reader.next_record(),
            Err(clinker_format::FormatError::Charset(_))
        ));
        drop(reader);
        assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}

#[test]
fn multi_record_unknown_latin1_tag_retains_admitted_exact_rejected_cells() {
    use clinker_format::multi_record::MultiRecordReader;
    let authority = Arc::new(DecodeAuthority::default());
    let resources = clinker_record::owned_storage::AllocationResources::new(authority.clone());
    let mut reader = MultiRecordReader::new_csv_admitted(
        &b"\xff,\"quoted, caf\xe9\",\nD,ok,2\nT,1\n"[..],
        multi_record_spec("D"),
        multi_record_dialect(false),
        Charset::Latin1,
        decode_workspace(&authority),
        TextStorage::Shared,
    )
    .unwrap();
    let error = reader.next_record().unwrap_err();
    let clinker_format::FormatError::UnknownRecordType(failure) = &error else {
        panic!("{error:?}");
    };
    assert_eq!(failure.row, 1);
    assert_eq!(failure.discriminator, "\u{ff}");
    assert_eq!(
        failure.raw_record,
        Value::from("[\"\u{ff}\",\"quoted, caf\u{e9}\",\"\"]")
    );
    assert_eq!(failure.raw_record.unaccounted_heap_size(&resources), 0);
    assert!(reader.next_record().unwrap().is_some());
    assert!(reader.next_record().unwrap().is_none());
    drop(reader);
    assert!(authority.used.load(std::sync::atomic::Ordering::SeqCst) > 0);
    drop(error);
    assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn multi_record_every_admission_refusal_is_typed_and_releases_ownership() {
    use clinker_format::multi_record::MultiRecordReader;
    use std::sync::atomic::Ordering::SeqCst;
    let text = "\u{e9}".repeat(100);
    let input: Vec<u8> = format!("H,{text}\nD,{text},1\nT,1\n")
        .chars()
        .map(|ch| u8::try_from(ch as u32).unwrap())
        .collect();
    let run = |authority: &Arc<DecodeAuthority>| -> Result<(), clinker_format::FormatError> {
        let mut reader = MultiRecordReader::new_csv_admitted(
            input.as_slice(),
            multi_record_spec("D"),
            multi_record_dialect(false),
            Charset::Latin1,
            decode_workspace(authority),
            TextStorage::Shared,
        )?;
        let sections = reader.prepare_document(&multi_record_envelope())?;
        let record = reader.next_record()?.unwrap();
        assert!(reader.next_record()?.is_none());
        drop(reader);
        drop(record);
        drop(sections);
        Ok(())
    };
    let baseline = Arc::new(DecodeAuthority::default());
    run(&baseline).unwrap();
    let calls = baseline.calls.load(SeqCst);
    assert!(
        calls > 20,
        "must exercise retained metadata, headers, body, trailer and sections"
    );
    assert_eq!(baseline.used.load(SeqCst), 0);
    for fail_at in 1..=calls {
        let authority = Arc::new(DecodeAuthority::default());
        authority.fail_at.store(fail_at, SeqCst);
        let error = run(&authority).unwrap_err();
        assert!(
            matches!(error, clinker_format::FormatError::Resource(ref error) if error.kind == clinker_record::owned_storage::ResourceErrorKind::Budget),
            "refusal {fail_at}: {error:?}"
        );
        assert!(!error.is_document_structural());
        drop(error);
        assert_eq!(authority.used.load(SeqCst), 0, "refusal {fail_at}");
    }
}

#[test]
fn multi_record_cancellation_after_document_prescan_is_not_structural() {
    use clinker_format::multi_record::MultiRecordReader;
    use std::sync::atomic::Ordering::SeqCst;
    let authority = Arc::new(DecodeAuthority::default());
    let mut reader = MultiRecordReader::new_csv_admitted(
        &b"H,label\nD,ok,1\nT,1\n"[..],
        multi_record_spec("D"),
        multi_record_dialect(false),
        Charset::Utf8,
        decode_workspace(&authority),
        TextStorage::Shared,
    )
    .unwrap();
    let sections = reader.prepare_document(&multi_record_envelope()).unwrap();
    authority.cancelled.store(true, SeqCst);
    let error = reader.next_record().unwrap_err();
    assert!(
        matches!(error, clinker_format::FormatError::Resource(ref error) if error.kind == clinker_record::owned_storage::ResourceErrorKind::Cancelled)
    );
    assert!(!error.is_document_structural());
    drop(reader);
    drop(sections);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn multi_record_numeric_evidence_matches_nonexecuting_legacy_reader() {
    use clinker_format::multi_record::MultiRecordReader;
    use clinker_format::numeric_observation::NumericObserver;
    use std::sync::Mutex;
    let input = b"H,1.5\nD,first,1.25\nD,second,42\nT,2\n";
    let spec = || {
        let mut spec = multi_record_spec("D");
        spec.record_types[0].columns[1].ty = cxl::typecheck::Type::Numeric;
        spec.record_types[2].columns[1].ty = cxl::typecheck::Type::Numeric;
        spec
    };
    let capture = |rows: &Arc<Mutex<Vec<_>>>| {
        let rows = rows.clone();
        NumericObserver::new_scoped(move |scope, observation| {
            rows.lock().unwrap().push((
                scope.record().map(str::to_owned),
                scope.field().to_owned(),
                observation,
            ));
        })
    };
    let legacy_evidence = Arc::new(Mutex::new(Vec::new()));
    let runtime_evidence = Arc::new(Mutex::new(Vec::new()));
    let authority = Arc::new(DecodeAuthority::default());
    let mut legacy = MultiRecordReader::new_csv(
        &input[..],
        spec(),
        multi_record_dialect(false),
        Charset::Utf8,
    )
    .unwrap()
    .with_numeric_observer(capture(&legacy_evidence));
    let mut runtime = MultiRecordReader::new_csv_admitted(
        &input[..],
        spec(),
        multi_record_dialect(false),
        Charset::Utf8,
        decode_workspace(&authority),
        TextStorage::Shared,
    )
    .unwrap()
    .with_numeric_observer(capture(&runtime_evidence));
    assert_eq!(
        legacy.prepare_document(&multi_record_envelope()).unwrap(),
        runtime.prepare_document(&multi_record_envelope()).unwrap()
    );
    loop {
        let left = legacy.next_record().unwrap();
        let right = runtime.next_record().unwrap();
        match (left, right) {
            (Some(left), Some(right)) => assert_eq!(left.values(), right.values()),
            (None, None) => break,
            _ => panic!("row counts differ"),
        }
    }
    assert_eq!(
        *legacy_evidence.lock().unwrap(),
        *runtime_evidence.lock().unwrap()
    );
    assert_eq!(runtime_evidence.lock().unwrap().len(), 4);
    drop(runtime);
    assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn multi_record_prescan_keeps_first_header_and_pending_body_with_padding_policy() {
    use clinker_format::multi_record::MultiRecordReader;
    use clinker_record::schema_def::Justify;
    let authority = Arc::new(DecodeAuthority::default());
    let mut spec = multi_record_spec("D");
    let mut absent = spec.record_types[0].clone();
    absent.id = "other_batch".into();
    absent.tag = "Z".into();
    spec.record_types.push(absent);
    spec.header_tags.push("Z".into());
    spec.record_types[1].columns[1].trim = Some(false);
    spec.record_types[1].columns[2].pad = Some("0".into());
    spec.record_types[1].columns[2].justify = Some(Justify::Right);
    let mut reader = MultiRecordReader::new_csv_admitted(
        &b"H,first\nH,second\nD,  exact text  ,00042\nT,1\n"[..],
        spec,
        multi_record_dialect(false),
        Charset::Utf8,
        decode_workspace(&authority),
        TextStorage::Unique,
    )
    .unwrap();
    let sections = reader.prepare_document(&multi_record_envelope()).unwrap();
    let Value::Map(section) = &sections["customer_metadata"] else {
        panic!("section");
    };
    assert_eq!(section["label"], Value::from("first"));
    let row = reader.next_record().unwrap().unwrap();
    assert_eq!(row.get("text"), Some(&Value::from("  exact text  ")));
    assert_eq!(row.get("number"), Some(&Value::Integer(42)));
    assert!(reader.next_record().unwrap().is_none());
    drop(reader);
    drop(row);
    drop(sections);
    assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn multi_record_duplicate_header_requests_preserve_scoped_numeric_evidence() {
    use clinker_format::multi_record::MultiRecordReader;
    use clinker_format::numeric_observation::{NumericObserver, NumericParserOutcome};
    use std::sync::Mutex;

    let input = b"H,1.5\nH,1e999\nD,first,1.25\nT,1\n";
    let spec = || {
        let mut spec = multi_record_spec("D");
        spec.record_types[0].columns[1].ty = cxl::typecheck::Type::Numeric;
        spec.header_tags.push("H".into());
        spec
    };
    let mut envelope = multi_record_envelope();
    envelope.sections.insert(
        "dispatch_metadata".into(),
        envelope.sections["customer_metadata"].clone(),
    );
    let capture = |rows: &Arc<Mutex<Vec<_>>>| {
        let rows = rows.clone();
        NumericObserver::new_scoped(move |scope, observation| {
            rows.lock().unwrap().push((
                scope.record().map(str::to_owned),
                scope.field().to_owned(),
                observation,
            ));
        })
    };
    let legacy_evidence = Arc::new(Mutex::new(Vec::new()));
    let runtime_evidence = Arc::new(Mutex::new(Vec::new()));
    let authority = Arc::new(DecodeAuthority::default());
    let mut legacy = MultiRecordReader::new_csv(
        &input[..],
        spec(),
        multi_record_dialect(false),
        Charset::Utf8,
    )
    .unwrap()
    .with_numeric_observer(capture(&legacy_evidence));
    let mut runtime = MultiRecordReader::new_csv_admitted(
        &input[..],
        spec(),
        multi_record_dialect(false),
        Charset::Utf8,
        decode_workspace(&authority),
        TextStorage::Shared,
    )
    .unwrap()
    .with_numeric_observer(capture(&runtime_evidence));
    let legacy_sections = legacy.prepare_document(&envelope).unwrap();
    let runtime_sections = runtime.prepare_document(&envelope).unwrap();
    assert_eq!(legacy_sections, runtime_sections);
    for name in ["customer_metadata", "dispatch_metadata"] {
        let Value::Map(section) = &runtime_sections[name] else {
            panic!("expected section {name}");
        };
        assert_eq!(section["label"], Value::from("1.5"));
    }
    let left = legacy.next_record().unwrap().unwrap();
    let right = runtime.next_record().unwrap().unwrap();
    assert_eq!(left.values(), right.values());
    assert_eq!(right.get("number"), Some(&Value::Float(1.25)));
    assert!(legacy.next_record().unwrap().is_none());
    assert!(runtime.next_record().unwrap().is_none());
    {
        let evidence = legacy_evidence.lock().unwrap();
        assert_eq!(evidence.len(), 2);
        assert_eq!(evidence[0].0.as_deref(), Some("batch"));
        assert_eq!(evidence[0].1, "label");
        assert_eq!(
            evidence[0].2.parser_outcome(),
            &NumericParserOutcome::Float(1.5)
        );
        assert_eq!(evidence[1].0.as_deref(), Some("detail"));
        assert_eq!(evidence[1].1, "number");
        assert_eq!(
            evidence[1].2.parser_outcome(),
            &NumericParserOutcome::Float(1.25)
        );
        assert_eq!(*evidence, *runtime_evidence.lock().unwrap());
    }
    drop(runtime);
    drop(right);
    drop(runtime_sections);
    assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
}

fn assert_multi_record_duplicate_trailer_count(input: &str, expected_error: Option<&str>) {
    use clinker_format::multi_record::MultiRecordReader;
    use clinker_format::schema::Column;
    use cxl::typecheck::Type;

    let spec = || {
        let mut spec = multi_record_spec("D");
        spec.record_types[2]
            .columns
            .push(Column::bare("count", Type::Int));
        spec
    };
    let authority = Arc::new(DecodeAuthority::default());
    let mut legacy = MultiRecordReader::new_csv(
        input.as_bytes(),
        spec(),
        multi_record_dialect(false),
        Charset::Utf8,
    )
    .unwrap();
    let mut runtime = MultiRecordReader::new_csv_admitted(
        input.as_bytes(),
        spec(),
        multi_record_dialect(false),
        Charset::Utf8,
        decode_workspace(&authority),
        TextStorage::Shared,
    )
    .unwrap();
    let left = legacy.next_record().unwrap().unwrap();
    let right = runtime.next_record().unwrap().unwrap();
    assert_eq!(left.values(), right.values());
    assert_eq!(right.get("text"), Some(&Value::from("ok")));
    match expected_error {
        Some(message) => {
            let left = legacy.next_record().unwrap_err();
            assert!(left.is_document_structural());
            assert!(left.is_structural_count());
            assert!(left.to_string().contains(message), "{left}");
            let right = runtime.next_record().unwrap_err();
            assert!(right.is_document_structural());
            assert!(right.is_structural_count());
            assert_eq!(left.to_string(), right.to_string());
        }
        None => {
            assert!(legacy.next_record().unwrap().is_none());
            assert!(runtime.next_record().unwrap().is_none());
        }
    }
    drop(runtime);
    drop(right);
    assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn multi_record_duplicate_trailer_count_rejects_last_mismatch() {
    assert_multi_record_duplicate_trailer_count("D,ok,1\nT,1,999\n", Some("declares count 999"));
}

#[test]
fn multi_record_duplicate_trailer_count_accepts_last_match() {
    assert_multi_record_duplicate_trailer_count("D,ok,1\nT,999,1\n", None);
}

#[test]
fn multi_record_duplicate_trailer_count_rejects_empty_last_field() {
    assert_multi_record_duplicate_trailer_count(
        "D,ok,1\nT,1,\n",
        Some("carries no value for count field 'count'"),
    );
}

#[test]
fn multi_record_typed_sections_preserve_legacy_scalar_semantics() {
    use clinker_format::envelope::EnvelopeFieldType;
    use clinker_format::multi_record::MultiRecordReader;
    for (ty, text) in [
        (EnvelopeFieldType::String, "string field"),
        (EnvelopeFieldType::Int, "42"),
        (EnvelopeFieldType::Float, "1.25"),
        (EnvelopeFieldType::Bool, "true"),
        (EnvelopeFieldType::Date, "2024-01-15"),
        (EnvelopeFieldType::DateTime, "2024-01-15T10:30:00"),
        (EnvelopeFieldType::Int, "invalid"),
        (EnvelopeFieldType::Int, ""),
    ] {
        let authority = Arc::new(DecodeAuthority::default());
        let input = format!("H,{text}\nD,ok,1\nT,1\n");
        let mut config = multi_record_envelope();
        config.sections["customer_metadata"].fields["label"] = ty;
        let mut legacy = MultiRecordReader::new_csv(
            input.as_bytes(),
            multi_record_spec("D"),
            multi_record_dialect(false),
            Charset::Utf8,
        )
        .unwrap();
        let mut runtime = MultiRecordReader::new_csv_admitted(
            input.as_bytes(),
            multi_record_spec("D"),
            multi_record_dialect(false),
            Charset::Utf8,
            decode_workspace(&authority),
            TextStorage::Unique,
        )
        .unwrap();
        match (
            legacy.prepare_document(&config),
            runtime.prepare_document(&config),
        ) {
            (Ok(left), Ok(right)) => assert_eq!(left, right),
            (Err(left), Err(right)) => assert_eq!(left.to_string(), right.to_string()),
            outcomes => panic!("section parity: {outcomes:?}"),
        }
        drop(runtime);
        assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}

#[test]
fn multi_record_structural_failures_preserve_legacy_classification() {
    use clinker_format::multi_record::MultiRecordReader;
    for input in [
        "D,ok,1\n",
        "D,ok,1\nT,2\n",
        "D,ok,1\nT,\n",
        "D,ok,1\nT,1\nD,late,2\n",
    ] {
        let authority = Arc::new(DecodeAuthority::default());
        let mut legacy = MultiRecordReader::new_csv(
            input.as_bytes(),
            multi_record_spec("D"),
            multi_record_dialect(false),
            Charset::Utf8,
        )
        .unwrap();
        let mut runtime = MultiRecordReader::new_csv_admitted(
            input.as_bytes(),
            multi_record_spec("D"),
            multi_record_dialect(false),
            Charset::Utf8,
            decode_workspace(&authority),
            TextStorage::Shared,
        )
        .unwrap();
        loop {
            match (legacy.next_record(), runtime.next_record()) {
                (Ok(Some(left)), Ok(Some(right))) => assert_eq!(left.values(), right.values()),
                (Err(left), Err(right)) => {
                    assert!(right.is_document_structural());
                    assert_eq!(left.is_structural_count(), right.is_structural_count());
                    assert_eq!(left.to_string(), right.to_string());
                    break;
                }
                outcomes => panic!("structural parity: {outcomes:?}"),
            }
        }
        drop(runtime);
        assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}

#[test]
fn multi_record_finite_budget_refuses_oversized_header_without_owner_leak() {
    use clinker_format::multi_record::MultiRecordReader;
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(65_536).unwrap());
    let input = format!("H,{}\nD,ok,1\nT,1\n", "x".repeat(100_000));
    let mut reader = MultiRecordReader::new_csv_admitted(
        input.as_bytes(),
        multi_record_spec("D"),
        multi_record_dialect(false),
        Charset::Utf8,
        DecodeWorkspace::new(provider.resources().allocation().clone()).unwrap(),
        TextStorage::Shared,
    )
    .unwrap();
    let error = reader
        .prepare_document(&multi_record_envelope())
        .unwrap_err();
    assert!(
        matches!(error, clinker_format::FormatError::Resource(ref error) if error.kind == clinker_record::owned_storage::ResourceErrorKind::Budget)
    );
    assert!(!error.is_document_structural());
    drop(reader);
    assert_eq!(provider.used(), 0);
}

#[test]
fn decode_json_numeric_fallback_preserves_legacy_shape_and_errors() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(65536).unwrap());
    for cell in [
        "[1e9999,-1e9999,1e-9999,18446744073709551616,0,1.5,true,null]",
        "[{\"x\":18446744073709551615}]",
        "{}",
        "[",
    ] {
        let input = format!("items\n\"{}\"\n", cell.replace('"', "\"\""));
        let config = || CsvReaderConfig {
            split_values: vec![clinker_format::multi_value::SplitValues {
                field: "items".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            }],
            ..Default::default()
        };
        let mut legacy = CsvReader::from_reader(input.as_bytes(), config());
        let mut admitted = CsvReader::from_reader_admitted(
            input.as_bytes(),
            config(),
            DecodeWorkspace::new(provider.resources().allocation().clone()).unwrap(),
            TextStorage::Shared,
        )
        .unwrap();
        match (legacy.next_record(), admitted.next_record()) {
            (Ok(Some(expected)), Ok(Some(actual))) => {
                assert_eq!(actual.values(), expected.values())
            }
            (Err(expected), Err(actual)) => assert_eq!(actual.to_string(), expected.to_string()),
            outcomes => panic!("reader parity: {outcomes:?}"),
        }
    }
    assert_eq!(provider.used(), 0);
}

#[test]
fn decode_no_header_schema_refusal_retry_preserves_pending_first_row() {
    use std::sync::atomic::Ordering::SeqCst;
    let authority = Arc::new(DecodeAuthority::default());
    authority.fail_at.store(1, SeqCst);
    let mut reader = CsvReader::from_reader_admitted(
        &b"first\nsecond\n"[..],
        CsvReaderConfig {
            has_header: false,
            ..Default::default()
        },
        decode_workspace(&authority),
        TextStorage::Shared,
    )
    .unwrap();
    assert!(matches!(
        reader.schema(),
        Err(clinker_format::FormatError::Resource(_))
    ));
    assert_eq!(authority.used.load(SeqCst), 0);
    authority.fail_at.store(0, SeqCst);
    assert_eq!(
        reader.next_record().unwrap().unwrap().get("col_0"),
        Some(&Value::String("first".into()))
    );
    assert_eq!(
        reader.next_record().unwrap().unwrap().get("col_0"),
        Some(&Value::String("second".into()))
    );
    assert!(reader.next_record().unwrap().is_none());
    drop(reader);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn decode_cancel_before_headers_and_between_records_preserves_identity() {
    use clinker_record::owned_storage::ResourceErrorKind;
    use std::sync::atomic::Ordering::SeqCst;
    struct CountReads<'a> {
        bytes: &'a [u8],
        calls: &'a std::sync::atomic::AtomicUsize,
    }
    impl std::io::Read for CountReads<'_> {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            self.calls.fetch_add(1, SeqCst);
            self.bytes.read(out)
        }
    }
    let authority = Arc::new(DecodeAuthority::default());
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let mut reader = CsvReader::from_reader_admitted(
        CountReads {
            bytes: b"name\nfirst\nsecond\n",
            calls: &calls,
        },
        CsvReaderConfig::default(),
        decode_workspace(&authority),
        TextStorage::Shared,
    )
    .unwrap();
    authority.cancelled.store(true, SeqCst);
    assert!(
        matches!(reader.schema(), Err(clinker_format::FormatError::Resource(e)) if e.kind == ResourceErrorKind::Cancelled)
    );
    assert_eq!(calls.load(SeqCst), 0);
    authority.cancelled.store(false, SeqCst);
    let first = reader.next_record().unwrap().unwrap();
    let used = authority.used.load(SeqCst);
    authority.cancelled.store(true, SeqCst);
    assert!(
        matches!(reader.next_record(), Err(clinker_format::FormatError::Resource(e)) if e.kind == ResourceErrorKind::Cancelled)
    );
    assert_eq!(authority.used.load(SeqCst), used);
    drop(first);
    drop(reader);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn decode_shared_and_unique_text_use_distinct_clone_policies() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(65536).unwrap());
    let workspace = DecodeWorkspace::new(provider.resources().allocation().clone()).unwrap();
    for storage in [TextStorage::Shared, TextStorage::Unique] {
        let value = workspace
            .decode_text(
                b"a_long_string_whose_storage_is_not_inline",
                Charset::Utf8,
                storage,
            )
            .unwrap();
        let alias = value.clone();
        assert_eq!(value.as_str(), alias.as_str());
        assert_eq!(
            value.as_str().as_ptr() == alias.as_str().as_ptr(),
            storage == TextStorage::Shared
        );
        drop(value);
        assert_eq!(provider.used() != 0, storage == TextStorage::Shared);
        drop(alias);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn decode_latin1_scratch_overlaps_final_text_and_utf8_needs_no_scratch() {
    let bytes = [0xff; 128];
    let expanded = "\u{ff}".repeat(128);
    let generous = MemoryOnlyResources::new(NonZeroUsize::new(65536).unwrap());
    let workspace = DecodeWorkspace::new(generous.resources().allocation().clone()).unwrap();
    let value = workspace
        .decode_text(&bytes, Charset::Latin1, TextStorage::Shared)
        .unwrap();
    assert_eq!(value.as_str(), expanded);
    let final_bytes = generous.used();
    drop(value);
    assert_eq!(generous.used(), 0);
    let tight =
        MemoryOnlyResources::new(NonZeroUsize::new(final_bytes + expanded.len() - 1).unwrap());
    let workspace = DecodeWorkspace::new(tight.resources().allocation().clone()).unwrap();
    assert!(
        matches!(workspace.decode_text(&bytes, Charset::Latin1, TextStorage::Shared), Err(clinker_format::FormatError::Resource(e)) if e.kind == clinker_record::owned_storage::ResourceErrorKind::Budget)
    );
    assert_eq!(tight.used(), 0);
    let exact = MemoryOnlyResources::new(NonZeroUsize::new(final_bytes).unwrap());
    let workspace = DecodeWorkspace::new(exact.resources().allocation().clone()).unwrap();
    let value = workspace
        .decode_text(expanded.as_bytes(), Charset::Utf8, TextStorage::Shared)
        .unwrap();
    assert_eq!(exact.used(), final_bytes);
    drop(value);
    assert_eq!(exact.used(), 0);
}

#[test]
fn decode_nested_leaf_outlives_container_workspace_and_resource_handles() {
    use std::sync::atomic::Ordering::SeqCst;
    let authority = Arc::new(DecodeAuthority::default());
    let workspace = decode_workspace(&authority);
    let parsed =
        serde_json::json!([{"key": ["long_text_retained_independently_of_every_container"]}]);
    let value = workspace
        .decode_json_value(&parsed, TextStorage::Shared)
        .unwrap();
    let Value::Array(outer) = &value else {
        panic!("array")
    };
    let Value::Map(map) = &outer[0] else {
        panic!("map")
    };
    let Value::Array(inner) = &map["key"] else {
        panic!("inner array")
    };
    let leaf = inner[0].clone();
    let all_bytes = authority.used.load(SeqCst);
    drop(value);
    drop(workspace);
    drop(parsed);
    let leaf_bytes = authority.used.load(SeqCst);
    assert!(leaf_bytes > 0 && leaf_bytes < all_bytes);
    let weak = Arc::downgrade(&authority);
    drop(authority);
    assert!(
        weak.upgrade().is_some(),
        "leaf must keep release authority alive"
    );
    drop(leaf);
    assert!(weak.upgrade().is_none());
}

#[test]
fn decode_split_grammar_and_header_modes_match_legacy() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(65536).unwrap());
    for (delimiter, escape) in [
        (";", ""),
        (";", "\\"),
        ("::", ""),
        ("", "\\"),
        ("\u{a7}", "\u{b5}"),
        (";", ";"),
    ] {
        for cell in [
            "",
            ";",
            "::x::",
            "a;;b;",
            "a\\;b;tail\\",
            "a\\q;b",
            "\u{e9}\u{b5}\u{a7}x\u{a7}\u{b5}",
        ] {
            for has_header in [true, false] {
                let body = format!("\"{}\"\n", cell.replace('"', "\"\""));
                let input = if has_header {
                    format!("items\n{body}")
                } else {
                    body
                };
                let config = || CsvReaderConfig {
                    has_header,
                    split_values: vec![clinker_format::multi_value::SplitValues {
                        field: if has_header { "items" } else { "col_0" }.into(),
                        delimiter: delimiter.into(),
                        escape: escape.into(),
                        json: false,
                    }],
                    ..Default::default()
                };
                let mut legacy = CsvReader::from_reader(input.as_bytes(), config());
                let mut admitted = CsvReader::from_reader_admitted(
                    input.as_bytes(),
                    config(),
                    DecodeWorkspace::new(provider.resources().allocation().clone()).unwrap(),
                    TextStorage::Shared,
                )
                .unwrap();
                assert_eq!(
                    admitted.schema().unwrap().columns(),
                    legacy.schema().unwrap().columns()
                );
                let expected = legacy.next_record().unwrap().unwrap();
                let actual = admitted.next_record().unwrap().unwrap();
                assert_eq!(
                    actual.values(),
                    expected.values(),
                    "delimiter={delimiter:?}, escape={escape:?}, cell={cell:?}, header={has_header}"
                );
                assert!(admitted.next_record().unwrap().is_none());
            }
        }
    }
    for has_header in [true, false] {
        for input in [
            &b""[..],
            &b"same,same\n1,2\n"[..],
            &b"\xff\n"[..],
            &b"name\n\xff\n"[..],
        ] {
            let config = || CsvReaderConfig {
                has_header,
                ..Default::default()
            };
            let mut legacy = CsvReader::from_reader(input, config());
            let mut admitted = CsvReader::from_reader_admitted(
                input,
                config(),
                DecodeWorkspace::new(provider.resources().allocation().clone()).unwrap(),
                TextStorage::Shared,
            )
            .unwrap();
            match (legacy.next_record(), admitted.next_record()) {
                (Ok(Some(expected)), Ok(Some(actual))) => {
                    assert_eq!(actual.values(), expected.values());
                    assert_eq!(actual.schema().columns(), expected.schema().columns());
                }
                (Ok(None), Ok(None)) => {}
                (Err(expected), Err(actual)) => {
                    assert_eq!(actual.to_string(), expected.to_string())
                }
                outcomes => panic!("header parity: {outcomes:?}"),
            }
        }
    }
    assert_eq!(provider.used(), 0);
}

#[test]
fn decode_refuses_each_schema_split_json_allocation_without_leaks() {
    use std::sync::atomic::Ordering::SeqCst;
    let authority = Arc::new(DecodeAuthority::default());
    let input = b"parts,nested\nlong_long_long_long_long\\;part;tail,\"[{\"\"long_long_long_long_key\"\":[null,\"\"long_long_long_long_long_value\"\"]}]\"\n";
    let config = || CsvReaderConfig {
        charset: Charset::Latin1,
        split_values: vec![
            clinker_format::multi_value::SplitValues {
                field: "parts".into(),
                delimiter: ";".into(),
                escape: "\\".into(),
                json: false,
            },
            clinker_format::multi_value::SplitValues {
                field: "nested".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            },
        ],
        ..Default::default()
    };
    let run = || {
        let mut reader = CsvReader::from_reader_admitted(
            input.as_slice(),
            config(),
            decode_workspace(&authority),
            TextStorage::Shared,
        )
        .unwrap();
        reader.next_record()
    };
    drop(run().unwrap());
    let allocations = authority.calls.load(SeqCst);
    assert!(
        allocations > 20,
        "must exercise headers, schema, slots, scratch, and nested owners"
    );
    for fail_at in 1..=allocations {
        authority.calls.store(0, SeqCst);
        authority.fail_at.store(fail_at, SeqCst);
        let outcome = run();
        // ReservedVec may retry its exact minimum after a preferred-growth
        // refusal. Both successful fallback and typed rejection release fully.
        if let Err(error) = &outcome {
            assert!(
                matches!(error, clinker_format::FormatError::Resource(e) if e.kind == clinker_record::owned_storage::ResourceErrorKind::Budget)
            );
        }
        drop(outcome);
        assert_eq!(
            authority.used.load(SeqCst),
            0,
            "leaked at admission {fail_at}"
        );
    }
}

#[test]
fn decode_headers_body_and_detached_alias_keep_their_owners() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(65536).unwrap());
    let workspace = DecodeWorkspace::new(provider.resources().allocation().clone()).unwrap();
    let mut reader = CsvReader::from_reader_admitted(
        &b"long_header_name_that_is_heap_backed\nlong_body_value_that_is_heap_backed\n"[..],
        CsvReaderConfig::default(),
        workspace,
        TextStorage::Shared,
    )
    .unwrap();
    let schema = reader.schema().unwrap();
    let schema_bytes = provider.used();
    assert!(schema_bytes > 0);
    let record = reader.next_record().unwrap().unwrap();
    let value = record.values()[0].clone();
    assert!(provider.used() > schema_bytes);
    drop(record);
    drop(reader);
    assert!(provider.used() > schema_bytes);
    drop(schema);
    assert!(provider.used() > 0);
    assert_eq!(
        value,
        Value::String("long_body_value_that_is_heap_backed".into())
    );
    drop(value);
    assert_eq!(provider.used(), 0);
}

#[test]
fn decode_latin1_split_json_nested_and_null_are_admitted() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(65536).unwrap());
    let workspace = DecodeWorkspace::new(provider.resources().allocation().clone()).unwrap();
    let config = CsvReaderConfig {
        charset: Charset::Latin1,
        split_values: vec![
            clinker_format::multi_value::SplitValues {
                field: "parts".into(),
                delimiter: ";".into(),
                escape: "\\".into(),
                json: false,
            },
            clinker_format::multi_value::SplitValues {
                field: "nested".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            },
        ],
        ..Default::default()
    };
    let mut reader = CsvReader::from_reader_admitted(
        &b"parts,nested\ncaf\xe9\\;x;tail\\,\"[{\"\"key\"\":[null,\"\"caf\xe9\"\"]}]\"\n"[..],
        config,
        workspace,
        TextStorage::Shared,
    )
    .unwrap();
    let record = reader.next_record().unwrap().unwrap();
    assert_eq!(
        record.get("parts").unwrap(),
        &Value::Array(OwnedValues::from_vec(vec![
            Value::String("caf\u{e9};x".into()),
            Value::String("tail\\".into())
        ]))
    );
    let Value::Array(items) = record.get("nested").unwrap() else {
        panic!("array")
    };
    let Value::Map(map) = &items[0] else {
        panic!("map")
    };
    assert_eq!(
        map["key"],
        Value::Array(OwnedValues::from_vec(vec![
            Value::Null,
            Value::String("caf\u{e9}".into())
        ]))
    );
    assert!(provider.used() > 0);
    drop(reader);
    drop(record);
    assert_eq!(provider.used(), 0);
}

#[test]
fn decode_refusal_overflow_and_strict_utf8_release_every_grant() {
    use clinker_record::owned_storage::{OwnedValues, ResourceErrorKind};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1).unwrap());
    let workspace = DecodeWorkspace::new(provider.resources().allocation().clone()).unwrap();
    assert!(
        matches!(workspace.decode_text(b"long_heap_backed_value_requiring_admission", Charset::Utf8, TextStorage::Shared), Err(clinker_format::FormatError::Resource(e)) if e.kind == ResourceErrorKind::Budget)
    );
    assert!(matches!(
        workspace.decode_text(b"\xff", Charset::Utf8, TextStorage::Shared),
        Err(clinker_format::FormatError::Charset(_))
    ));
    assert_eq!(
        OwnedValues::try_with_capacity(usize::MAX, workspace.scope())
            .unwrap_err()
            .kind,
        ResourceErrorKind::Layout
    );
    let mut reader = CsvReader::from_reader_admitted(
        &b"name\nbody\n"[..],
        CsvReaderConfig::default(),
        workspace,
        TextStorage::Shared,
    )
    .unwrap();
    assert!(
        matches!(reader.schema(), Err(clinker_format::FormatError::Resource(e)) if e.kind == ResourceErrorKind::Budget)
    );
    drop(reader);
    assert_eq!(provider.used(), 0);
}

fn encoder(
    schema: SharedStorage<Schema>,
    config: &CsvWriterConfig,
    charset: Charset,
    provider: &MemoryOnlyResources,
) -> CsvEncoder {
    let mut options = CsvEncoderOptions::from(config);
    options.charset = charset;
    let config = CsvEncoderConfig::new(options, &provider.resources()).unwrap();
    CsvEncoder::from_config(schema, config, provider.resources()).unwrap()
}

fn encoded(
    charset: Charset,
    names: &[&str],
    rows: Vec<Vec<Value>>,
    include_header: bool,
) -> Vec<u8> {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let schema = SharedStorage::from_arc(Arc::new(Schema::new(
        names.iter().map(|name| (*name).into()).collect(),
    )));
    let config = CsvWriterConfig {
        include_header,
        ..Default::default()
    };
    let mut output = Vec::new();
    {
        let mut writer = PreparedWriter::new(
            &mut output,
            encoder(schema.clone(), &config, charset, &provider),
            provider.resources(),
        )
        .unwrap();
        for values in rows {
            writer
                .write_record(&Record::new(schema.clone(), values))
                .unwrap();
        }
        writer.flush().unwrap();
    }
    assert_eq!(provider.used(), 0);
    output
}

#[test]
fn csv_utf8_and_latin1_have_exact_header_multiline_and_adjacent_bytes() {
    for charset in [Charset::Utf8, Charset::Latin1] {
        let rows = vec![
            vec![
                Value::String("caf\u{e9},\"line\nnext\r\n\u{85}".into()),
                Value::Null,
            ],
            vec![Value::String("adjacent".into()), Value::String("".into())],
        ];
        let bytes = encoded(charset, &["caf\u{e9}", "other"], rows, true);
        let expected = match charset {
            Charset::Utf8 => {
                "caf\u{e9},other\n\"caf\u{e9},\"\"line\nnext\r\n\u{85}\",\nadjacent,\n".as_bytes()
            }
            Charset::Latin1 => {
                &b"caf\xe9,other\n\"caf\xe9,\"\"line\nnext\r\n\x85\",\nadjacent,\n"[..]
            }
        };
        assert_eq!(bytes, expected);
        let mut reader = CsvReader::from_reader(
            bytes.as_slice(),
            CsvReaderConfig {
                charset,
                ..Default::default()
            },
        );
        assert_eq!(reader.schema().unwrap().columns()[0].as_ref(), "caf\u{e9}");
        let first = reader.next_record().unwrap().unwrap();
        assert_eq!(
            first.get("caf\u{e9}"),
            Some(&Value::String("caf\u{e9},\"line\nnext\r\n\u{85}".into()))
        );
        assert_eq!(first.get("other"), Some(&Value::String("".into())));
        assert_eq!(
            reader.next_record().unwrap().unwrap().get("caf\u{e9}"),
            Some(&Value::String("adjacent".into()))
        );
        assert!(reader.next_record().unwrap().is_none());
    }
}

#[test]
fn csv_empty_stream_single_empty_null_and_header_only_keep_native_shapes() {
    for charset in [Charset::Utf8, Charset::Latin1] {
        assert!(encoded(charset, &["value"], vec![], true).is_empty());
        assert_eq!(
            encoded(
                charset,
                &["value"],
                vec![vec![Value::Null], vec![Value::String("".into())]],
                false
            ),
            b"\"\"\n\"\"\n"
        );
        assert_eq!(
            encoded(
                charset,
                &["a", "b"],
                vec![vec![Value::Null, Value::Null]],
                true
            ),
            b"a,b\n,\n"
        );
        let mut reader = CsvReader::from_reader(
            &b"value\n"[..],
            CsvReaderConfig {
                charset,
                ..Default::default()
            },
        );
        assert_eq!(reader.schema().unwrap().columns().len(), 1);
        assert!(reader.next_record().unwrap().is_none());
    }
}

#[test]
fn csv_utf8_rejects_malformed_header_body_and_late_multiline_bytes() {
    for bytes in [
        &b"bad\xff\nvalue\n"[..],
        &b"value\nbad\xff\n"[..],
        &b"value\n\"line\nlate\xff\"\n"[..],
    ] {
        let mut reader = CsvReader::from_reader(bytes, CsvReaderConfig::default());
        let error = reader.next_record().unwrap_err();
        assert!(
            matches!(error, clinker_format::FormatError::Charset(_)),
            "{error}"
        );
        assert!(error.to_string().contains("UTF-8"));
    }
    let mut reader = CsvReader::from_reader(&b"a,b\n1,2\n3\n"[..], CsvReaderConfig::default());
    assert!(reader.next_record().unwrap().is_some());
    assert!(reader.next_record().is_err());
}

#[test]
fn csv_latin1_maps_every_high_byte_without_replacement() {
    let mut bytes = b"value\n".to_vec();
    bytes.extend(128..=255);
    bytes.push(b'\n');
    let mut reader = CsvReader::from_reader(
        bytes.as_slice(),
        CsvReaderConfig {
            charset: Charset::Latin1,
            ..Default::default()
        },
    );
    let row = reader.next_record().unwrap().unwrap();
    let expected: String = (128..=255).map(char::from).collect();
    assert_eq!(row.get("value"), Some(&Value::String(expected.into())));
    assert_eq!(
        encoded(
            Charset::Latin1,
            &["value"],
            vec![row.values().to_vec()],
            true
        ),
        bytes
    );
}

#[test]
fn csv_late_unrepresentable_cell_keeps_output_header_and_resources_unchanged() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["value".into()])));
    let mut writer = PreparedWriter::new(
        Vec::new(),
        encoder(
            schema.clone(),
            &CsvWriterConfig::default(),
            Charset::Latin1,
            &provider,
        ),
        provider.resources(),
    )
    .unwrap();
    let bad = Record::new(
        schema.clone(),
        vec![Value::String(
            format!("{}\u{20ac}", "x".repeat(100_000)).into(),
        )],
    );
    let retained = provider.used();
    let error = writer.write_record(&bad).unwrap_err();
    assert!(writer.destination().is_empty());
    assert_eq!(provider.used(), retained);
    assert!(error.to_string().len() < 512);
    let good = Record::new(schema, vec![Value::String("caf\u{e9}".into())]);
    writer.write_record(&good).unwrap();
    let prefix = writer.destination().clone();
    assert!(writer.write_record(&bad).is_err());
    assert_eq!(writer.destination(), &prefix);
    writer.write_record(&good).unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.destination(), b"value\ncaf\xe9\ncaf\xe9\n");
    drop(writer);
    assert_eq!(provider.used(), 0);
}

#[test]
fn csv_unrepresentable_header_or_preset_delivers_no_bytes() {
    for preset in [false, true] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            if preset { "value" } else { "\u{20ac}" }.into(),
        ])));
        let mut encoder = encoder(
            schema.clone(),
            &CsvWriterConfig::default(),
            Charset::Latin1,
            &provider,
        );
        if preset {
            encoder.set_preset_header(&["\u{20ac}".into()]).unwrap();
        }
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        let row = Record::new(schema, vec![Value::String("ok".into())]);
        assert!(writer.write_record(&row).is_err());
        assert!(writer.destination().is_empty());
        drop(writer);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn csv_joined_and_json_cells_round_trip_in_both_charsets() {
    use clinker_format::multi_value::{JoinValues, OnConflict, SplitValues};
    for charset in [Charset::Utf8, Charset::Latin1] {
        for json in [false, true] {
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
            let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["tags".into()])));
            let config = CsvWriterConfig {
                declared_multiple: ["tags".into()].into(),
                join_values: vec![JoinValues {
                    field: "tags".into(),
                    delimiter: ";".into(),
                    escape: "\\".into(),
                    on_conflict: if json {
                        OnConflict::EncodeJson
                    } else {
                        OnConflict::Escape
                    },
                    repeat_as: None,
                    wrap_in: None,
                }],
                ..Default::default()
            };
            let mut writer = PreparedWriter::new(
                Vec::new(),
                encoder(schema.clone(), &config, charset, &provider),
                provider.resources(),
            )
            .unwrap();
            let values = [
                vec![],
                vec![Value::String("caf\u{e9}".into())],
                vec![
                    Value::String("caf\u{e9};x".into()),
                    Value::String("\u{85}\n\"".into()),
                ],
            ];
            for value in &values {
                writer
                    .write_record(&Record::new(
                        schema.clone(),
                        vec![Value::Array(OwnedValues::from_vec(value.clone()))],
                    ))
                    .unwrap();
            }
            writer.flush().unwrap();
            let expected = match (charset, json) {
                (Charset::Utf8, false) => "tags\n\"\"\ncaf\u{e9}\n\"caf\u{e9}\\;x;\u{85}\n\"\"\"\n".as_bytes(),
                (Charset::Latin1, false) => &b"tags\n\"\"\ncaf\xe9\n\"caf\xe9\\;x;\x85\n\"\"\"\n"[..],
                (Charset::Utf8, true) => "tags\n[]\n\"[\"\"caf\u{e9}\"\"]\"\n\"[\"\"caf\u{e9};x\"\",\"\"\u{85}\\n\\\"\"\"\"]\"\n".as_bytes(),
                (Charset::Latin1, true) => &b"tags\n[]\n\"[\"\"caf\xe9\"\"]\"\n\"[\"\"caf\xe9;x\"\",\"\"\x85\\n\\\"\"\"\"]\"\n"[..],
            };
            assert_eq!(writer.destination(), expected);
            let mut reader = CsvReader::from_reader(
                writer.destination().as_slice(),
                CsvReaderConfig {
                    charset,
                    split_values: vec![SplitValues {
                        field: "tags".into(),
                        delimiter: ";".into(),
                        escape: "\\".into(),
                        json,
                    }],
                    ..Default::default()
                },
            );
            for expected in values {
                assert_eq!(
                    reader.next_record().unwrap().unwrap().get("tags"),
                    Some(&Value::Array(OwnedValues::from_vec(expected)))
                );
            }
            assert!(reader.next_record().unwrap().is_none());
            drop(reader);
            drop(writer);
            assert_eq!(provider.used(), 0);
        }
    }
}

#[test]
fn csv_latin1_cell_admission_failure_is_atomic_and_releases_workspace() {
    // Covers the provider's fixed staging/replay overlap plus CSV's 8 KiB
    // library buffer, while remaining below the 100,000-byte encoded cell.
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(64 * 1024).unwrap());
    let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["value".into()])));
    let mut writer = PreparedWriter::new(
        Vec::new(),
        encoder(
            schema.clone(),
            &CsvWriterConfig::default(),
            Charset::Latin1,
            &provider,
        ),
        provider.resources(),
    )
    .unwrap();
    let retained = provider.used();
    let oversized = Record::new(
        schema.clone(),
        vec![Value::String("\u{e9}".repeat(100_000).into())],
    );
    assert!(matches!(
        writer.write_record(&oversized),
        Err(clinker_format::FormatError::Resource(_))
    ));
    assert!(writer.destination().is_empty());
    assert_eq!(provider.used(), retained);
    writer
        .write_record(&Record::new(schema, vec![Value::String("ok".into())]))
        .unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.destination(), b"value\nok\n");
    drop(writer);
    assert_eq!(provider.used(), 0);
}

#[test]
fn csv_latin1_preserves_a_utf8_bom_shaped_prefix_as_data() {
    for has_header in [false, true] {
        let mut reader = CsvReader::from_reader(
            &b"\xef\xbb\xbfvalue\n\xef\xbb\xbfbody\n"[..],
            CsvReaderConfig {
                charset: Charset::Latin1,
                has_header,
                ..Default::default()
            },
        );
        let schema = reader.schema().unwrap();
        if has_header {
            assert_eq!(schema.columns()[0].as_ref(), "\u{ef}\u{bb}\u{bf}value");
        }
        let row = reader.next_record().unwrap().unwrap();
        let expected = if has_header {
            "\u{ef}\u{bb}\u{bf}body"
        } else {
            "\u{ef}\u{bb}\u{bf}value"
        };
        assert_eq!(
            row.get(schema.columns()[0].as_ref()),
            Some(&Value::String(expected.into()))
        );
    }
}

struct DecodeChunks<'a> {
    remaining: &'a [u8],
    pattern: &'a [usize],
    reads: usize,
}

impl std::io::Read for DecodeChunks<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let count = output
            .len()
            .min(self.remaining.len())
            .min(self.pattern[self.reads % self.pattern.len()]);
        self.reads += 1;
        output[..count].copy_from_slice(&self.remaining[..count]);
        self.remaining = &self.remaining[count..];
        Ok(count)
    }
}

#[test]
fn decode_chunked_prefix_headers_and_cells_obey_charset_policy() {
    for pattern in [&[1][..], &[2, 17, 1, 4096][..]] {
        for charset in [Charset::Utf8, Charset::Latin1] {
            for header in [true, false] {
                for (input, latin1, utf8) in [
                    (b"\xef".as_slice(), "ï", None),
                    (b"\xef\xbb".as_slice(), "ï»", None),
                    (b"\xef\xbb\xbfplain\n".as_slice(), "ï»¿plain", Some("plain")),
                    (
                        b"\xef\xbb\xbf\"adjacent\"\n".as_slice(),
                        "ï»¿\"adjacent\"",
                        Some("adjacent"),
                    ),
                    (
                        b"\"\xef\xbb\xbfline\nnext\"\n".as_slice(),
                        "ï»¿line\nnext",
                        Some("\u{feff}line\nnext"),
                    ),
                ] {
                    let authority = Arc::new(DecodeAuthority::default());
                    let mut reader = CsvReader::from_reader_admitted(
                        DecodeChunks {
                            remaining: input,
                            pattern,
                            reads: 0,
                        },
                        CsvReaderConfig {
                            charset,
                            has_header: header,
                            ..Default::default()
                        },
                        decode_workspace(&authority),
                        TextStorage::Shared,
                    )
                    .unwrap();
                    let expected = if charset == Charset::Latin1 {
                        Some(latin1)
                    } else {
                        utf8
                    };
                    match expected {
                        Some(expected) if header => {
                            assert_eq!(reader.schema().unwrap().columns()[0].as_ref(), expected);
                            assert!(reader.next_record().unwrap().is_none());
                        }
                        Some(expected) => {
                            let record = reader.next_record().unwrap().unwrap();
                            assert_eq!(record.get("col_0"), Some(&Value::from(expected)));
                            assert!(reader.next_record().unwrap().is_none());
                        }
                        None => assert!(matches!(
                            reader.next_record(),
                            Err(clinker_format::FormatError::Charset(_))
                        )),
                    }
                    drop(reader);
                    assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
                }
            }
        }
    }
}

#[test]
fn multi_record_chunked_prefix_discriminator_and_late_error_keep_row_numbers() {
    use clinker_format::multi_record::MultiRecordReader;
    for pattern in [&[1][..], &[2, 17, 1, 4096][..]] {
        for charset in [Charset::Utf8, Charset::Latin1] {
            let authority = Arc::new(DecodeAuthority::default());
            let tag = if charset == Charset::Latin1 {
                "ï»¿D"
            } else {
                "D"
            };
            let mut spec = multi_record_spec(tag);
            spec.header_tags.clear();
            let input = b"\xef\xbb\xbfD,\"first\nline\",1\nX,\"second\nline\",2\n";
            let mut reader = MultiRecordReader::new_csv_admitted(
                DecodeChunks {
                    remaining: input,
                    pattern,
                    reads: 0,
                },
                spec,
                multi_record_dialect(false),
                charset,
                decode_workspace(&authority),
                TextStorage::Shared,
            )
            .unwrap();
            let first = reader.next_record().unwrap().unwrap();
            assert_eq!(first.get("kind"), Some(&Value::from(tag)));
            assert_eq!(first.get("text"), Some(&Value::from("first\nline")));
            let error = reader.next_record().unwrap_err();
            let clinker_format::FormatError::UnknownRecordType(failure) = &error else {
                panic!("{error:?}")
            };
            assert_eq!(
                failure.row, 2,
                "logical rows, despite quoted physical newlines"
            );
            assert_eq!(failure.discriminator, "X");
            assert_eq!(
                failure.raw_record,
                Value::from("[\"X\",\"second\\nline\",\"2\"]")
            );
            drop(first);
            drop(error);
            drop(reader);
            assert_eq!(authority.used.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn multi_record_pending_cancel_precedes_late_malformed_utf8() {
    use clinker_format::multi_record::MultiRecordReader;
    use std::sync::atomic::Ordering::SeqCst;
    let authority = Arc::new(DecodeAuthority::default());
    let text = "retained-pending-body-".repeat(20);
    let mut bytes = format!("H,label\nD,{text},1\n").into_bytes();
    bytes.extend_from_slice(b"D,\xff,2\n");
    let mut spec = multi_record_spec("D");
    spec.header_tags.push("H".into());
    let mut reader = MultiRecordReader::new_csv_admitted(
        bytes.as_slice(),
        spec,
        multi_record_dialect(false),
        Charset::Utf8,
        decode_workspace(&authority),
        TextStorage::Shared,
    )
    .unwrap();
    let sections = reader.prepare_document(&multi_record_envelope()).unwrap();
    let before_cancel = authority.used.load(SeqCst);
    authority.cancelled.store(true, SeqCst);
    let error = reader.next_record().unwrap_err();
    assert!(
        matches!(error, clinker_format::FormatError::Resource(error) if error.kind == clinker_record::owned_storage::ResourceErrorKind::Cancelled)
    );
    assert_eq!(
        authority.used.load(SeqCst),
        before_cancel,
        "cancel must not consume or release pending ownership"
    );
    authority.cancelled.store(false, SeqCst);
    let first = reader.next_record().unwrap().unwrap();
    assert_eq!(first.get("text"), Some(&Value::from(text.as_str())));
    assert!(matches!(
        reader.next_record(),
        Err(clinker_format::FormatError::Charset(_))
    ));
    drop(reader);
    drop(sections);
    assert!(authority.used.load(SeqCst) > 0);
    assert_eq!(first.get("text"), Some(&Value::from(text.as_str())));
    drop(first);
    assert_eq!(authority.used.load(SeqCst), 0);
}
