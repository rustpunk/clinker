use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues, SharedStorage};
use std::io::Write;
use std::num::NonZeroUsize;

use clinker_format::FormatError;
use clinker_format::preparation::MemoryOnlyResources;
use clinker_format::preparation::{FormatEncoder, OutputOperation, PreparedWriter, WriterScope};
use clinker_format::preparation::{ResourceError, ResourceErrorKind, StageStorage, StorageStage};
use clinker_format::reserved::ReservedBuffer;
use clinker_format::reserved::ReservedVec;

#[test]
fn fixed_width_late_structured_envelope_rejects_without_delivery() {
    use clinker_format::FormatWriter;
    use clinker_format::envelope_writer::OutputEnvelopeSpec;
    use clinker_format::fixed_width::writer::{FixedWidthEncoder, FixedWidthWriterConfig};
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Value};
    use std::sync::Arc;

    for footer in [false, true] {
        for structured in [
            Value::Array(OwnedValues::from_vec(vec![Value::Integer(1)])),
            Value::Map(OwnedMap::from_map(Default::default())),
        ] {
            let section = Value::Map(OwnedMap::from_map(
                [
                    (OwnedKey::from("valid"), Value::String("prefix".into())),
                    (OwnedKey::from("unsupported"), structured),
                ]
                .into(),
            ));
            let doc = DocumentContext::new(
                DocumentId::next(),
                Arc::from("input.txt"),
                EnvelopeRecord::from_sections([(OwnedKey::from("authored"), section)]),
            );
            let mut bytes = Vec::new();
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
            let encoder = FixedWidthEncoder::new(
                &[],
                &FixedWidthWriterConfig {
                    envelope: Some(OutputEnvelopeSpec {
                        header_from_doc: (!footer).then(|| "authored".into()),
                        footer_from_doc: footer.then(|| "authored".into()),
                        footer_record_count_field: None,
                    }),
                    ..Default::default()
                },
                provider.resources(),
            )
            .unwrap();
            let mut writer =
                PreparedWriter::new(&mut bytes, encoder, provider.resources()).unwrap();
            let result = if footer {
                writer.end_document(&doc)
            } else {
                writer.begin_document(&doc)
            };
            assert!(
                result.is_err(),
                "structured sections have no scalar representation"
            );
            drop(writer);
            assert!(
                bytes.is_empty(),
                "the complete rejected section must stay private"
            );
        }
    }
}

mod fixed_width_prepared {
    use super::*;
    use clinker_format::FormatWriter;
    use clinker_format::envelope_writer::OutputEnvelopeSpec;
    use clinker_format::fixed_width::writer::{
        FixedWidthEncoder, FixedWidthEncoderConfig, FixedWidthWriterConfig,
    };
    use clinker_format::preparation::{
        AllocationAuthority, AllocationLease, MemoryStorage, OperationStage, OwnerId,
        ResourceAuthority, WriterResources,
    };
    use clinker_format::{
        Column, FixedWidthCountField, FixedWidthFill, FixedWidthOccurs, FixedWidthOverflow,
        FixedWidthTruncateKeep,
    };
    use clinker_record::schema_def::{Justify, LineSeparator, TruncationPolicy};
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Schema, Value};
    use cxl::typecheck::Type;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn field(name: &str, width: usize) -> Column {
        Column {
            width: Some(width),
            ..Column::bare(name, Type::String)
        }
    }
    fn record(values: &[(&str, Value)]) -> Record {
        Record::new(
            SharedStorage::from_arc(Arc::new(Schema::new(
                values.iter().map(|(n, _)| (*n).into()).collect(),
            ))),
            values.iter().map(|(_, v)| v.clone()).collect(),
        )
    }
    fn doc(sections: &[(&str, &[(&str, Value)])]) -> DocumentContext {
        DocumentContext::new(
            DocumentId::next(),
            Arc::from("data.txt"),
            EnvelopeRecord::from_sections(sections.iter().map(|(n, fields)| {
                (
                    OwnedKey::from(*n),
                    Value::Map(OwnedMap::from_map(
                        fields
                            .iter()
                            .map(|(n, v)| (OwnedKey::from(*n), v.clone()))
                            .collect(),
                    )),
                )
            })),
        )
    }
    fn config(separator: LineSeparator) -> FixedWidthWriterConfig {
        FixedWidthWriterConfig {
            line_separator: separator,
            envelope: Some(OutputEnvelopeSpec {
                header_from_doc: Some("opening authored".into()),
                footer_from_doc: Some("closing authored".into()),
                footer_record_count_field: None,
            }),
        }
    }
    fn provider() -> MemoryOnlyResources {
        MemoryOnlyResources::new(NonZeroUsize::new(2 * 1024 * 1024).unwrap())
    }

    #[test]
    fn warning_history_keeps_existing_text_backing_across_successful_append() {
        for text in ["x".repeat(4096), "好é".repeat(819)] {
            let provider = provider();
            let encoder = FixedWidthEncoder::new(
                &[field("value", 2)],
                &FixedWidthWriterConfig::default(),
                provider.resources(),
            )
            .unwrap();
            let mut writer =
                PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
            let record = record(&[("value", Value::String(text.into()))]);
            writer.write_record(&record).unwrap();
            let pointer = writer.encoder().truncation_warnings()[0].as_str().as_ptr();
            writer.write_record(&record).unwrap();
            assert_eq!(writer.encoder().truncation_warnings().len(), 2);
            assert_eq!(
                writer.encoder().truncation_warnings()[0].as_str().as_ptr(),
                pointer,
                "successful append must move the old message owner without copying its bytes"
            );
            drop(writer);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn complete_warning_slice_survives_many_tiny_records() {
        let provider = provider();
        let encoder = FixedWidthEncoder::new(
            &[field("v", 1)],
            &FixedWidthWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        let mut expected = Vec::new();
        let mut pointers = Vec::new();
        for index in 0..1025 {
            let text = format!("x{index}");
            writer
                .write_record(&record(&[("v", Value::String(text.clone().into()))]))
                .unwrap();
            expected.push(format!("field 'v': value '{text}' truncated to 1 bytes"));
            let history: &[String] = writer.encoder().truncation_warnings();
            assert_eq!(history, expected);
            assert!(
                history
                    .iter()
                    .zip(&pointers)
                    .all(|(text, pointer)| text.as_ptr() == *pointer)
            );
            pointers.push(history.last().unwrap().as_ptr());
        }
        assert_eq!(writer.destination(), &b"x\n".repeat(1025));
        assert_eq!(writer.encoder().record_count(), 1025);
        drop(writer);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn pending_warning_overlap_and_delivery_commit_never_allocate() {
        for text in ["A".repeat(32_768), "好é".repeat(8192)] {
            let provider = provider();
            let resources = provider.resources();
            let scope = resources.scope().unwrap();
            let mut encoder = FixedWidthEncoder::new(
                &[field("v", 1)],
                &FixedWidthWriterConfig::default(),
                resources,
            )
            .unwrap();
            let record = record(&[("v", Value::String(text.clone().into()))]);
            let expected = format!("field 'v': value '{text}' truncated to 1 bytes");
            let mut destination = Vec::new();
            for count in 1..=9 {
                let retained = provider.used();
                let mut stage = scope.stage().unwrap();
                let pending = encoder
                    .prepare(OutputOperation::Record(&record), &mut stage, &scope)
                    .unwrap();
                assert!(
                    provider.used() > retained + expected.len(),
                    "new complete message and pending storage overlap all old owners"
                );
                assert_eq!(encoder.truncation_warnings().len(), count - 1);
                stage.finish().unwrap().deliver(&mut destination).unwrap();
                let (_, allocations) = allocation_probe(true, || encoder.commit(pending));
                assert_eq!(
                    allocations, 0,
                    "successful delivery commit cannot allocate even under allocator refusal"
                );
                let warnings: &[String] = encoder.truncation_warnings();
                assert_eq!(warnings.len(), count);
                assert!(warnings.iter().all(|w| w == &expected));
            }
            assert_eq!(destination.len(), 18);
            drop(encoder);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn message_backing_is_freed_before_its_admission_lease() {
        for value in ["ascii".repeat(1024), "é好".repeat(1024)] {
            let provider = provider();
            let encoder = FixedWidthEncoder::new(
                &[field("v", 1)],
                &FixedWidthWriterConfig::default(),
                provider.resources(),
            )
            .unwrap();
            let config_charge = provider.used();
            let mut writer =
                PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
            let record = record(&[("v", Value::String(value.into()))]);
            writer.write_record(&record).unwrap();
            let pointer = writer.encoder().truncation_warnings()[0].as_ptr() as *mut u8;
            let capacity = writer.encoder().truncation_warnings()[0].capacity();
            BACKING_WATCH.with(|watch| {
                watch.set(Some(BackingWatch {
                    provider: &provider,
                    pointer,
                    bytes: capacity,
                    live_at_deallocation: None,
                    live_at_allocation: None,
                    layout: Some(std::alloc::Layout::array::<u8>(capacity).unwrap()),
                    deallocations: 0,
                }))
            });
            for _ in 0..8 {
                writer.write_record(&record).unwrap();
            }
            assert!(
                BACKING_WATCH.with(|watch| watch.get().unwrap().live_at_deallocation.is_none()),
                "growing metadata must preserve the message backing"
            );
            let warning_charge = provider.used() - config_charge;
            drop(writer);
            let watched = BACKING_WATCH.with(|watch| watch.take().unwrap());
            assert_eq!(watched.deallocations, 1);
            assert_eq!(
                watched.live_at_deallocation,
                Some(warning_charge),
                "all message grants remain until their Strings and metadata are freed"
            );
            assert!(warning_charge >= capacity);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn warning_message_and_metadata_refusal_at_every_growth_preserves_history() {
        let authority = Authority::new();
        let resources = WriterResources::new(authority.clone());
        let scope = resources.scope().unwrap();
        let encoder = FixedWidthEncoder::new(
            &[field("v", 1)],
            &FixedWidthWriterConfig::default(),
            resources.clone(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, resources).unwrap();
        let record = record(&[("v", Value::String("é好".repeat(64).into()))]);
        for count in 0..9 {
            let before = authority.memory.used();
            let previous = writer.encoder().truncation_warnings().to_vec();
            let pointers: Vec<_> = writer
                .encoder()
                .truncation_warnings()
                .iter()
                .map(|text| text.as_ptr())
                .collect();
            authority.reset();
            let (pending, allocations) = allocation_probe(false, || {
                writer.encoder().prepare(
                    OutputOperation::Record(&record),
                    &mut std::io::sink(),
                    &scope,
                )
            });
            let grants = authority.attempts.load(Ordering::SeqCst);
            drop(pending.unwrap());
            assert_eq!(authority.memory.used(), before);
            assert!(grants > 0 && allocations > 0);
            for fail_allocator in [false, true] {
                for stop in 0..if fail_allocator { allocations } else { grants } {
                    authority.reset();
                    let (result, _) = allocation_probe(false, || {
                        if fail_allocator {
                            ALLOCATIONS_LEFT.with(|remaining| remaining.set(Some(stop)));
                        } else {
                            authority.refuse.store(stop, Ordering::SeqCst);
                        }
                        writer.encoder().prepare(
                            OutputOperation::Record(&record),
                            &mut std::io::sink(),
                            &scope,
                        )
                    });
                    assert!(
                        matches!(result, Err(FormatError::Resource(error)) if error.kind == if fail_allocator { ResourceErrorKind::Allocation } else { ResourceErrorKind::Budget }),
                        "growth {count}, failure {stop}"
                    );
                    assert_eq!(authority.memory.used(), before);
                    assert_eq!(writer.encoder().truncation_warnings(), previous);
                    assert!(
                        writer
                            .encoder()
                            .truncation_warnings()
                            .iter()
                            .zip(&pointers)
                            .all(|(text, pointer)| text.as_ptr() == *pointer)
                    );
                    assert_eq!(writer.encoder().record_count(), count);
                    assert_eq!(writer.destination(), &b" \n".repeat(count as usize));
                }
            }
            authority.reset();
            writer.write_record(&record).unwrap();
        }
        drop(writer);
        assert_eq!(authority.memory.used(), 0);
    }

    #[test]
    fn cancelled_sealed_or_final_delivery_preserves_warning_owners_and_poison() {
        use super::cancellation_harness::{Authority as CancelAuthority, CancelOnWrite};
        use std::sync::atomic::AtomicBool;
        for after_seal in [false, true] {
            let cancelled = Arc::new(AtomicBool::new(false));
            let authority = Arc::new(CancelAuthority {
                memory: provider(),
                cancelled: cancelled.clone(),
                cancel_after_seal: after_seal,
            });
            let resources = authority.memory.resources();
            let scope = resources.scope().unwrap();
            let mut encoder = FixedWidthEncoder::new(
                &[field("v", 1)],
                &FixedWidthWriterConfig::default(),
                resources,
            )
            .unwrap();
            let record = record(&[("v", Value::String("warning".into()))]);
            let mut stage = scope.stage().unwrap();
            let pending = encoder
                .prepare(OutputOperation::Record(&record), &mut stage, &scope)
                .unwrap();
            let mut bytes = Vec::new();
            stage.finish().unwrap().deliver(&mut bytes).unwrap();
            encoder.commit(pending);
            let pointer = encoder.truncation_warnings()[0].as_ptr();
            let retained = authority.memory.used();
            let mut writer = PreparedWriter::new(
                CancelOnWrite {
                    cancelled,
                    bytes,
                    attempts: 0,
                },
                encoder,
                WriterResources::new(authority.clone()),
            )
            .unwrap();
            assert!(
                matches!(writer.write_record(&record), Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Cancelled)
            );
            assert_eq!(writer.encoder().record_count(), 1);
            assert_eq!(writer.encoder().truncation_warnings().len(), 1);
            assert_eq!(writer.encoder().truncation_warnings()[0].as_ptr(), pointer);
            assert_eq!(
                writer.destination().bytes,
                if after_seal {
                    b"w\n".as_slice()
                } else {
                    b"w\nw\n".as_slice()
                }
            );
            assert_eq!(authority.memory.used(), retained);
            let attempts = writer.destination().attempts;
            assert!(writer.write_record(&record).is_err());
            assert!(writer.flush().is_err());
            assert_eq!(writer.destination().attempts, attempts);
            drop(writer);
            assert_eq!(authority.memory.used(), 0);
        }
    }

    #[test]
    fn scalar_sections_order_presence_separators_and_multiple_documents() {
        let values = [
            ("text", Value::String("hé".into())),
            ("null", Value::Null),
            ("integer", Value::Integer(-17)),
            ("float", Value::Float(1.25)),
            ("decimal", Value::Decimal("12.30".parse().unwrap())),
            ("bool", Value::Bool(true)),
            (
                "date",
                Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()),
            ),
            (
                "datetime",
                Value::DateTime(
                    chrono::NaiveDate::from_ymd_opt(2024, 2, 29)
                        .unwrap()
                        .and_hms_opt(13, 14, 15)
                        .unwrap(),
                ),
            ),
        ];
        let full = doc(&[
            ("opening authored", &values),
            (
                "closing authored",
                &[("last", Value::String("tail".into()))],
            ),
        ]);
        let empty = doc(&[("opening authored", &[]), ("closing authored", &[])]);
        let missing = doc(&[]);
        for (sep, literal) in [
            (LineSeparator::Lf, "\n"),
            (LineSeparator::CrLf, "\r\n"),
            (LineSeparator::None, ""),
        ] {
            let provider = provider();
            let encoder =
                FixedWidthEncoder::new(&[field("body", 2)], &config(sep), provider.resources())
                    .unwrap();
            let mut writer =
                PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
            writer.begin_document(&full).unwrap();
            writer
                .write_record(&record(&[("body", Value::String("B".into()))]))
                .unwrap();
            assert_eq!(writer.encoder().record_count(), 1);
            writer.end_document(&full).unwrap();
            assert!(!writer.encoder().document_open());
            writer.begin_document(&missing).unwrap();
            assert_eq!(writer.encoder().record_count(), 0);
            writer.end_document(&missing).unwrap();
            writer.begin_document(&empty).unwrap();
            writer.end_document(&empty).unwrap();
            writer.begin_document(&full).unwrap();
            writer.end_document(&full).unwrap();
            writer.flush().unwrap();
            let line = "hé-171.2512.30true2024022920240229131415";
            let expected = format!(
                "{line}{literal}B {literal}tail{literal}{literal}{literal}{line}{literal}tail{literal}"
            );
            assert_eq!(writer.destination(), expected.as_bytes());
            drop(writer);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn scalar_body_byte_width_padding_order_and_narrow_borrowed_text() {
        let values = [
            ("text", Value::String("é好".into())),
            ("null", Value::Null),
            ("integer", Value::Integer(-17)),
            ("float", Value::Float(1.25)),
            ("decimal", Value::Decimal("12.30".parse().unwrap())),
            ("bool", Value::Bool(false)),
            (
                "date",
                Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()),
            ),
            (
                "datetime",
                Value::DateTime(
                    chrono::NaiveDate::from_ymd_opt(2024, 2, 29)
                        .unwrap()
                        .and_hms_opt(13, 14, 15)
                        .unwrap(),
                ),
            ),
        ];
        let mut fields: Vec<_> = values.iter().map(|(name, _)| field(name, 16)).collect();
        fields[0].width = Some(4);
        fields[0].truncation = Some(TruncationPolicy::Silent);
        fields[2].justify = Some(Justify::Right);
        fields[2].pad = Some("0".into());
        let provider = provider();
        let encoder = FixedWidthEncoder::new(
            &fields,
            &FixedWidthWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        writer.write_record(&record(&values)).unwrap();
        assert_eq!(writer.destination(), "é                  0000000000000-171.25            12.30           false           20240229        20240229131415  \n".as_bytes());
        drop(writer);
        let mut narrow = field("value", 3);
        narrow.truncation = Some(TruncationPolicy::Silent);
        let encoder = FixedWidthEncoder::new(
            &[narrow],
            &FixedWidthWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        for text in ["a".repeat(1024 * 1024), "好".repeat(400_000)] {
            writer
                .write_record(&record(&[("value", Value::String(text.into()))]))
                .unwrap();
        }
        assert_eq!(writer.destination(), "aaa\n好\n".as_bytes());
        drop(writer);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn large_ascii_and_multibyte_sections_stream_exactly() {
        for text in ["x".repeat(100_000), "好é".repeat(20_000)] {
            let provider = provider();
            let doc = doc(&[
                (
                    "opening authored",
                    &[("text", Value::String(text.clone().into()))],
                ),
                (
                    "closing authored",
                    &[("text", Value::String(text.clone().into()))],
                ),
            ]);
            let encoder =
                FixedWidthEncoder::new(&[], &config(LineSeparator::CrLf), provider.resources())
                    .unwrap();
            let retained = provider.used();
            let mut writer =
                PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
            writer.begin_document(&doc).unwrap();
            writer.end_document(&doc).unwrap();
            assert_eq!(
                writer.destination(),
                format!("{text}\r\n{text}\r\n").as_bytes()
            );
            assert_eq!(
                provider.used(),
                retained,
                "sections are borrowed and never retained"
            );
            drop(writer);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn late_record_and_section_failure_preserves_committed_state() {
        let provider = provider();
        let encoder = FixedWidthEncoder::new(
            &[field("a", 2), field("b", 2)],
            &config(LineSeparator::Lf),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        let empty = doc(&[]);
        writer.begin_document(&empty).unwrap();
        let good = record(&[
            ("a", Value::String("long".into())),
            ("b", Value::Integer(1)),
        ]);
        writer.write_record(&good).unwrap();
        let before = provider.used();
        let warning = writer.encoder().truncation_warnings()[0]
            .as_str()
            .to_owned();
        let bad = record(&[
            ("a", Value::String("another warning".into())),
            ("b", Value::Map(OwnedMap::from_map(Default::default()))),
        ]);
        assert!(writer.write_record(&bad).is_err());
        assert_eq!(provider.used(), before);
        let bad_doc = doc(&[
            (
                "opening authored",
                &[
                    ("valid", Value::Integer(5)),
                    ("bad", Value::Array(OwnedValues::from_vec(vec![]))),
                ],
            ),
            (
                "closing authored",
                &[("bad", Value::Map(OwnedMap::from_map(Default::default())))],
            ),
        ]);
        assert!(writer.begin_document(&bad_doc).is_err());
        assert!(writer.end_document(&bad_doc).is_err());
        assert_eq!(writer.encoder().record_count(), 1);
        assert!(writer.encoder().document_open());
        assert_eq!(writer.encoder().truncation_warnings().len(), 1);
        assert_eq!(writer.encoder().truncation_warnings()[0].as_str(), warning);
        assert_eq!(writer.destination(), b"lo1 \n");
        assert_eq!(provider.used(), before);
        writer.end_document(&empty).unwrap();
        drop(writer);
        assert_eq!(provider.used(), 0);
    }

    fn group(fill: FixedWidthFill, count: bool) -> Column {
        Column {
            start: Some(2),
            multiple: Some(true),
            fields: Some(vec![field("code", 2)]),
            occurs: Some(FixedWidthOccurs {
                min: 0,
                max: 2,
                fill,
                on_overflow: FixedWidthOverflow::Error,
                keep: None,
            }),
            count_field: count.then(|| FixedWidthCountField {
                name: "count".into(),
                width: 1,
            }),
            ..Column::bare("items", Type::Map)
        }
    }
    fn items(values: &[&str]) -> Value {
        Value::Array(OwnedValues::from_vec(
            values
                .iter()
                .map(|text| {
                    Value::Map(OwnedMap::from_map(
                        [(OwnedKey::from("code"), Value::String((*text).into()))].into(),
                    ))
                })
                .collect(),
        ))
    }
    #[test]
    fn group_bounds_count_overflow_gaps_and_shift_preserve_bytes() {
        for (fill, expected) in [
            (FixedWidthFill::Pad, b"  1AB    Z \n".as_slice()),
            (FixedWidthFill::Shift, b"  1AB  Z \n".as_slice()),
        ] {
            let provider = provider();
            let mut last = field("last", 2);
            last.start = Some(9);
            let mut group = group(fill, true);
            let encoder = FixedWidthEncoder::new(
                &[last, group.clone()],
                &FixedWidthWriterConfig::default(),
                provider.resources(),
            )
            .unwrap();
            let mut writer =
                PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
            writer
                .write_record(&record(&[
                    ("items", items(&["AB"])),
                    ("last", Value::String("Z".into())),
                ]))
                .unwrap();
            assert_eq!(writer.destination(), expected);
            assert!(
                writer
                    .write_record(&record(&[("items", items(&["AA", "BB", "CC"]))]))
                    .is_err()
            );
            drop(writer);
            for (keep, bytes) in [
                (FixedWidthTruncateKeep::First, b"  2AABB\n"),
                (FixedWidthTruncateKeep::Last, b"  2BBCC\n"),
            ] {
                let occurs = group.occurs.as_mut().unwrap();
                occurs.on_overflow = FixedWidthOverflow::Truncate;
                occurs.keep = Some(keep);
                let encoder = FixedWidthEncoder::new(
                    &[group.clone()],
                    &FixedWidthWriterConfig::default(),
                    provider.resources(),
                )
                .unwrap();
                let mut writer =
                    PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
                writer
                    .write_record(&record(&[("items", items(&["AA", "BB", "CC"]))]))
                    .unwrap();
                assert_eq!(writer.destination(), bytes);
                drop(writer);
            }
            assert_eq!(provider.used(), 0);
        }
    }
    #[test]
    fn group_blank_minimum_null_and_late_occurrence_validation() {
        let provider = provider();
        let mut group = group(FixedWidthFill::Pad, false);
        let encoder = FixedWidthEncoder::new(
            &[group.clone()],
            &FixedWidthWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        writer
            .write_record(&record(&[("items", Value::Null)]))
            .unwrap();
        assert_eq!(writer.destination(), b"      \n");
        assert!(
            writer
                .write_record(&record(&[("items", items(&["  "]))]))
                .is_err()
        );
        assert!(
            writer
                .write_record(&record(&[(
                    "items",
                    Value::Array(OwnedValues::from_vec(vec![
                        Value::Map(OwnedMap::from_map(
                            [(OwnedKey::from("code"), Value::String("AB".into()))].into()
                        )),
                        Value::Integer(1)
                    ]))
                )]))
                .is_err()
        );
        assert_eq!(writer.destination(), b"      \n");
        drop(writer);
        group.occurs.as_mut().unwrap().min = 1;
        let encoder = FixedWidthEncoder::new(
            &[group],
            &FixedWidthWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        assert!(
            writer
                .write_record(&record(&[("items", items(&[]))]))
                .is_err()
        );
        assert!(writer.destination().is_empty());
        drop(writer);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn virtual_padding_width_cannot_delay_blank_rejection() {
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let provider = provider();
            let mut group = group(FixedWidthFill::Pad, false);
            group.start = Some(0);
            group.occurs.as_mut().unwrap().max = 1;
            group.fields.as_mut().unwrap()[0].width = Some(usize::MAX - 1);
            let encoder = FixedWidthEncoder::new(
                &[group],
                &FixedWidthWriterConfig::default(),
                provider.resources(),
            )
            .unwrap();
            let mut writer =
                PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
            let value = Value::Array(OwnedValues::from_vec(vec![Value::Map(OwnedMap::from_map(
                [(OwnedKey::from("code"), Value::Null)].into(),
            ))]));
            assert!(matches!(
                writer.write_record(&record(&[("items", value)])),
                Err(FormatError::OutputEncoding {
                    kind: clinker_format::error::OutputEncodingKind::FixedWidthBlankOccurrence,
                    ..
                })
            ));
            assert!(writer.destination().is_empty());
            drop(writer);
            assert_eq!(provider.used(), 0);
            send.send(()).unwrap();
        });
        receive
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("virtual padding must not require a scan of the declared width");
        worker.join().unwrap();
    }

    #[test]
    fn shared_overlap_diagnostic_names_actual_group_in_both_declaration_orders() {
        let mut group = group(FixedWidthFill::Pad, false);
        group.start = Some(0);
        let mut tail = field("tail", 1);
        tail.start = Some(2);
        for fields in [vec![group.clone(), tail.clone()], vec![tail, group.clone()]] {
            let error = clinker_format::fixed_width::field::validate_write_layout(&fields)
                .unwrap_err()
                .to_string();
            assert!(error.contains("group 'items'"), "{error}");
            assert!(!error.contains("group 'tail'"), "{error}");
            assert!(
                error.contains("range 2..3 overlaps field 'items' (0..4)"),
                "{error}"
            );
            assert!(error.contains("count, payload"), "{error}");
        }
        let mut second = field("other", 1);
        second.start = Some(1);
        group.fields.as_mut().unwrap().push(second);
        let error = clinker_format::fixed_width::field::validate_write_layout(&[group])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("child 'other' range 1..2 overlaps child 'code' range 0..2"),
            "{error}"
        );
    }

    struct Authority {
        memory: MemoryOnlyResources,
        attempts: AtomicUsize,
        refuse: AtomicUsize,
        cancel_checks: AtomicUsize,
        cancel_at: AtomicUsize,
    }
    impl Authority {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                memory: provider(),
                attempts: AtomicUsize::new(0),
                refuse: AtomicUsize::new(usize::MAX),
                cancel_checks: AtomicUsize::new(0),
                cancel_at: AtomicUsize::new(usize::MAX),
            })
        }
        fn reset(&self) {
            self.attempts.store(0, Ordering::SeqCst);
            self.refuse.store(usize::MAX, Ordering::SeqCst);
            self.cancel_checks.store(0, Ordering::SeqCst);
            self.cancel_at.store(usize::MAX, Ordering::SeqCst);
        }
    }
    impl AllocationAuthority for Authority {
        fn identity(&self) -> usize {
            self.memory.resources().allocation().identity()
        }
        fn try_reserve(
            self: Arc<Self>,
            owner: OwnerId,
            layout: std::alloc::Layout,
        ) -> Result<AllocationLease, ResourceError> {
            self.check_cancelled()?;
            if self.attempts.fetch_add(1, Ordering::SeqCst) >= self.refuse.load(Ordering::SeqCst) {
                return Err(ResourceError::new(
                    ResourceErrorKind::Budget,
                    layout.size(),
                    0,
                ));
            }
            self.memory.resources().allocation().reserve(owner, layout)
        }
        fn release(&self, _: OwnerId, _: usize) {
            unreachable!("delegated grants");
        }
        fn check_cancelled(&self) -> Result<(), ResourceError> {
            if self.cancel_checks.fetch_add(1, Ordering::SeqCst)
                >= self.cancel_at.load(Ordering::SeqCst)
            {
                Err(ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
            } else {
                Ok(())
            }
        }
    }
    impl ResourceAuthority for Authority {
        fn create_stage(
            self: Arc<Self>,
            scope: WriterScope,
        ) -> Result<OperationStage, FormatError> {
            StorageStage::create(scope.clone(), MemoryStorage::new(scope))
        }
    }
    #[test]
    fn construction_admits_every_backing_and_retains_shared_config() {
        let authority = Authority::new();
        let resources = WriterResources::new(authority.clone());
        let fields = [group(FixedWidthFill::Pad, true), field("tail", 2)];
        let policy =
            FixedWidthEncoderConfig::new(&fields, &config(LineSeparator::Lf), &resources).unwrap();
        let attempts = authority.attempts.load(Ordering::SeqCst);
        assert!(attempts >= 6);
        let retained = authority.memory.used();
        let (alias, allocations) = allocation_probe(false, || policy.clone());
        assert_eq!(allocations, 0);
        drop(policy);
        assert_eq!(authority.memory.used(), retained);
        drop(alias);
        assert_eq!(authority.memory.used(), 0);
        for stop in 0..attempts {
            authority.reset();
            authority.refuse.store(stop, Ordering::SeqCst);
            assert!(matches!(
                FixedWidthEncoderConfig::new(&fields, &config(LineSeparator::Lf), &resources),
                Err(FormatError::Resource(_))
            ));
            assert_eq!(authority.memory.used(), 0, "constructor refusal {stop}");
        }
    }
    #[test]
    fn preparation_denial_and_cancellation_restore_all_committed_owners() {
        let authority = Authority::new();
        let resources = WriterResources::new(authority.clone());
        let encoder = FixedWidthEncoder::new(
            &[field("v", 2)],
            &config(LineSeparator::Lf),
            resources.clone(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, resources.clone()).unwrap();
        let good = doc(&[]);
        writer.begin_document(&good).unwrap();
        let record = record(&[("v", Value::String("warning".into()))]);
        writer.write_record(&record).unwrap();
        let baseline = authority.memory.used();
        let scope = resources.scope().unwrap();
        let section = doc(&[
            (
                "opening authored",
                &[("v", Value::String("x".repeat(20_000).into()))],
            ),
            ("closing authored", &[("v", Value::Integer(9))]),
        ]);
        for operation in [
            OutputOperation::Record(&record),
            OutputOperation::BeginDocument(&section),
            OutputOperation::EndDocument(&section),
        ] {
            // First measure bounded cancellation points through immutable preparation.
            authority.reset();
            let measured = match &operation {
                OutputOperation::Record(r) => OutputOperation::Record(r),
                OutputOperation::BeginDocument(d) => OutputOperation::BeginDocument(d),
                OutputOperation::EndDocument(d) => OutputOperation::EndDocument(d),
                _ => unreachable!(),
            };
            let pending = writer
                .encoder()
                .prepare(measured, &mut std::io::sink(), &scope)
                .unwrap();
            let checks = authority.cancel_checks.load(Ordering::SeqCst);
            let allocations = authority.attempts.load(Ordering::SeqCst);
            drop(pending);
            assert_eq!(authority.memory.used(), baseline);
            for cancel in [false, true] {
                for stop in 0..if cancel { checks } else { allocations } {
                    authority.reset();
                    if cancel {
                        authority.cancel_at.store(stop, Ordering::SeqCst);
                    } else {
                        authority.refuse.store(stop, Ordering::SeqCst);
                    }
                    let operation = match &operation {
                        OutputOperation::Record(r) => OutputOperation::Record(r),
                        OutputOperation::BeginDocument(d) => OutputOperation::BeginDocument(d),
                        OutputOperation::EndDocument(d) => OutputOperation::EndDocument(d),
                        _ => unreachable!(),
                    };
                    assert!(
                        writer
                            .encoder()
                            .prepare(operation, &mut std::io::sink(), &scope)
                            .is_err()
                    );
                    assert_eq!(authority.memory.used(), baseline);
                    assert_eq!(writer.encoder().record_count(), 1);
                    assert!(writer.encoder().document_open());
                    assert_eq!(writer.encoder().truncation_warnings().len(), 1);
                    assert_eq!(writer.destination(), b"wa\n");
                }
            }
        }
        authority.reset();
        drop(writer);
        assert_eq!(authority.memory.used(), 0);
    }

    struct Destination {
        bytes: Vec<u8>,
        limit: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
        flushes: Arc<AtomicUsize>,
    }
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let remaining = self
                .limit
                .load(Ordering::SeqCst)
                .saturating_sub(self.bytes.len());
            if remaining == 0 {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            let n = bytes.len().min(2).min(remaining);
            self.bytes.extend_from_slice(&bytes[..n]);
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    #[test]
    fn short_delivery_completes_and_partial_delivery_poison_never_retries() {
        for operation in 0..3 {
            let provider = provider();
            let encoder = FixedWidthEncoder::new(
                &[field("v", 3)],
                &config(LineSeparator::Lf),
                provider.resources(),
            )
            .unwrap();
            let limit = Arc::new(AtomicUsize::new(usize::MAX));
            let calls = Arc::new(AtomicUsize::new(0));
            let flushes = Arc::new(AtomicUsize::new(0));
            let destination = Destination {
                bytes: Vec::new(),
                limit: limit.clone(),
                calls: calls.clone(),
                flushes: flushes.clone(),
            };
            let mut writer =
                PreparedWriter::new(destination, encoder, provider.resources()).unwrap();
            let empty = doc(&[]);
            writer.begin_document(&empty).unwrap();
            let r = record(&[("v", Value::String("warning".into()))]);
            writer.write_record(&r).unwrap();
            assert_eq!(writer.destination().bytes, b"war\n");
            let retained = provider.used();
            limit.store(6, Ordering::SeqCst);
            let doc = doc(&[
                ("opening authored", &[("x", Value::String("header".into()))]),
                ("closing authored", &[("x", Value::String("footer".into()))]),
            ]);
            let result = match operation {
                0 => writer.write_record(&r),
                1 => writer.begin_document(&doc),
                _ => writer.end_document(&doc),
            };
            assert!(matches!(result, Err(FormatError::Io(_))));
            assert_eq!(
                &writer.destination().bytes,
                match operation {
                    0 => b"war\nwa",
                    1 => b"war\nhe",
                    _ => b"war\nfo",
                }
            );
            assert_eq!(writer.encoder().record_count(), 1);
            assert!(writer.encoder().document_open());
            assert_eq!(writer.encoder().truncation_warnings().len(), 1);
            assert_eq!(provider.used(), retained);
            let attempts = calls.load(Ordering::SeqCst);
            assert!(writer.write_record(&r).is_err());
            assert!(writer.flush().is_err());
            assert!(writer.flush_bytes().is_err());
            drop(writer);
            assert_eq!(calls.load(Ordering::SeqCst), attempts);
            assert_eq!(flushes.load(Ordering::SeqCst), 0);
            assert_eq!(provider.used(), 0);
        }
    }
}

#[test]
fn allocation_only_scope_and_writer_stage_share_one_finite_ledger() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let resources = provider.resources();
    let allocation = resources.allocation().scope().unwrap();
    let writer = resources.scope().unwrap();
    let mut lease = allocation
        .reserve(std::alloc::Layout::new::<[u8; 64]>())
        .unwrap();
    let identity = lease.allocation_id();
    lease.transfer(writer.allocation()).unwrap();
    assert_eq!(lease.owner(), writer.owner());
    assert_eq!(lease.allocation_id(), identity);
    let other = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    assert_eq!(
        lease
            .transfer(other.resources().scope().unwrap().allocation())
            .unwrap_err()
            .kind,
        ResourceErrorKind::Authority
    );
    assert_eq!(provider.used(), 64);
    assert_eq!(other.used(), 0);

    let mut stage = writer.stage().unwrap();
    stage.write_all(b"same ledger").unwrap();
    let prepared = stage.finish().unwrap();
    assert!(provider.used() > 64);
    drop(writer);
    drop(resources);
    let mut destination = Vec::new();
    prepared.deliver(&mut destination).unwrap();
    assert_eq!(destination, b"same ledger");
    assert_eq!(provider.used(), 64);
    lease.transfer(&allocation).unwrap();
    drop(lease);
    assert_eq!(provider.used(), 0);
    let mut buffer = ReservedBuffer::new(allocation);
    buffer.extend_from_slice(b"allocation only").unwrap();
    assert_eq!(provider.used(), b"allocation only".len());
    drop(buffer);
    assert_eq!(provider.used(), 0);
}

fn csv_schema() -> SharedStorage<clinker_record::Schema> {
    SharedStorage::from_arc(std::sync::Arc::new(clinker_record::Schema::new(vec![
        "first".into(),
        "last".into(),
    ])))
}

#[test]
fn xml_native_complete_operations_keep_literal_bytes() {
    use clinker_format::FormatWriter;
    use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
    use clinker_record::{Record, Schema, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec!["payload".into()])));
    let record = Record::new(
        schema.clone(),
        vec![Value::Map(OwnedMap::from_map(
            [
                (OwnedKey::from("@id"), Value::Integer(7)),
                (OwnedKey::from("#text"), Value::String("A&B".into())),
                (
                    OwnedKey::from("item"),
                    Value::Array(OwnedValues::from_vec(vec![
                        Value::Bool(true),
                        Value::Integer(2),
                    ])),
                ),
            ]
            .into(),
        ))],
    );
    let encoder =
        XmlEncoder::new(schema, &XmlWriterConfig::default(), provider.resources()).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    writer.write_record(&record).unwrap();
    writer.flush_bytes().unwrap();
    assert_eq!(writer.destination(), b"<Root><Record><payload id=\"7\">A&amp;B<item>true</item><item>2</item></payload></Record>");
    writer.write_record(&record).unwrap();
    writer.flush().unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.destination(), b"<Root><Record><payload id=\"7\">A&amp;B<item>true</item><item>2</item></payload></Record><Record><payload id=\"7\">A&amp;B<item>true</item><item>2</item></payload></Record></Root>");
    drop(writer);
    assert_eq!(provider.used(), 0);
}

#[test]
fn json_identity_tracer_borrowed_nested_exact_bytes() {
    use clinker_format::FormatWriter;
    use clinker_format::json::writer::{JsonEncoder, JsonWriterConfig};
    use clinker_record::{Record, Schema, Value};

    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let resources = provider.resources();
    let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec![
        "address.city".into(),
        "items".into(),
    ])));
    let record = Record::new(
        schema.clone(),
        vec![
            Value::String("Montréal".into()),
            Value::Array(OwnedValues::from_vec(vec![
                Value::Integer(7),
                Value::Bool(true),
                Value::Null,
            ])),
        ],
    );
    let encoder =
        JsonEncoder::new(schema, &JsonWriterConfig::default(), resources.clone()).unwrap();
    let mut output = Vec::new();
    let mut writer = PreparedWriter::new(&mut output, encoder, resources).unwrap();
    writer.write_record(&record).unwrap();
    drop(record);
    writer.flush().unwrap();
    drop(writer);
    assert_eq!(
        output,
        "[\n{\"address\":{\"city\":\"Montréal\"},\"items\":[7,true,null]}\n]\n".as_bytes()
    );
    assert_eq!(provider.used(), 0);
}

#[test]
fn json_identity_tracer_refusal_preserves_cache_framing_and_destination() {
    use clinker_format::FormatWriter;
    use clinker_format::json::writer::{JsonEncoder, JsonWriterConfig};
    use clinker_record::{Record, Schema, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let resources = provider.resources();
    let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec!["a.b".into()])));
    let record = Record::new(schema.clone(), vec![Value::Integer(7)]);
    let encoder =
        JsonEncoder::new(schema, &JsonWriterConfig::default(), resources.clone()).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, resources.clone()).unwrap();
    writer.write_record(&record).unwrap();
    let committed = provider.used();
    let other = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec![
        "changed.path".into(),
    ])));
    let changed = Record::new(other, vec![Value::Integer(9)]);
    let scope = resources.scope().unwrap();
    let pending = writer
        .encoder()
        .prepare(
            OutputOperation::Record(&changed),
            &mut std::io::sink(),
            &scope,
        )
        .unwrap();
    assert!(
        provider.used() > committed,
        "old and replacement caches overlap until delivery"
    );
    drop(pending);
    assert_eq!(provider.used(), committed);
    let pressure = scope
        .reserve(std::alloc::Layout::array::<u8>(128 * 1024 - committed).unwrap())
        .unwrap();
    let mut private = Vec::new();
    assert!(matches!(
        writer
            .encoder()
            .prepare(OutputOperation::Record(&changed), &mut private, &scope),
        Err(FormatError::Resource(ResourceError {
            kind: ResourceErrorKind::Budget,
            ..
        }))
    ));
    assert!(
        private.is_empty(),
        "cache refusal precedes even private framing"
    );
    assert!(matches!(
        writer.write_record(&changed),
        Err(FormatError::Resource(ResourceError {
            kind: ResourceErrorKind::Budget,
            ..
        }))
    ));
    assert_eq!(writer.destination(), b"[\n{\"a\":{\"b\":7}}");
    drop(pressure);
    assert_eq!(provider.used(), committed);
    let (failure, _) = allocation_probe(true, || writer.write_record(&changed));
    assert!(matches!(
        failure,
        Err(FormatError::Resource(ResourceError {
            kind: ResourceErrorKind::Allocation,
            ..
        }))
    ));
    assert_eq!(provider.used(), committed);
    assert_eq!(writer.destination(), b"[\n{\"a\":{\"b\":7}}");
    writer.write_record(&record).unwrap();
    writer.write_record(&changed).unwrap();
    writer.flush().unwrap();
    assert_eq!(
        writer.destination(),
        b"[\n{\"a\":{\"b\":7}},\n{\"a\":{\"b\":7}},\n{\"changed\":{\"path\":9}}\n]\n"
    );
    drop(writer);
    assert_eq!(provider.used(), 0);
}

#[test]
fn json_identity_tracer_late_validation_keeps_initial_framing_private() {
    use clinker_format::FormatWriter;
    use clinker_format::json::writer::{JsonEncoder, JsonWriterConfig};
    use clinker_record::{Record, Schema, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let resources = provider.resources();
    let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec!["a.b".into()])));
    let encoder = JsonEncoder::new(
        schema.clone(),
        &JsonWriterConfig::default(),
        resources.clone(),
    )
    .unwrap();
    let baseline = provider.used();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, resources).unwrap();
    let record = Record::new(
        schema.clone(),
        vec![Value::Array(OwnedValues::from_vec(vec![
            Value::Integer(1),
            Value::Float(f64::NAN),
        ]))],
    );
    assert!(matches!(
        writer.write_record(&record),
        Err(FormatError::OutputEncoding {
            format: "JSON",
            field: 1,
            ..
        })
    ));
    assert!(writer.destination().is_empty());
    assert_eq!(provider.used(), baseline);
    writer
        .write_record(&Record::new(schema, vec![Value::Integer(2)]))
        .unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.destination(), b"[\n{\"a\":{\"b\":2}}\n]\n");
    drop(writer);
    assert_eq!(provider.used(), 0);
}

#[test]
fn json_identity_tracer_config_alias_keeps_actual_backing_charged() {
    use clinker_format::json::writer::{JsonEncoderConfig, JsonWriterConfig};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    BACKING_WATCH.with(|watch| {
        watch.set(Some(BackingWatch {
            provider: &provider,
            pointer: std::ptr::null_mut(),
            bytes: 0,
            live_at_deallocation: None,
            live_at_allocation: None,
            layout: None,
            deallocations: 0,
        }))
    });
    let config =
        JsonEncoderConfig::new(&JsonWriterConfig::default(), &provider.resources()).unwrap();
    let charge = provider.used();
    let (alias, allocations) = allocation_probe(false, || config.clone());
    assert_eq!(allocations, 0);
    drop(config);
    assert_eq!(provider.used(), charge);
    drop(alias);
    let watched = BACKING_WATCH.with(|watch| watch.replace(None).unwrap());
    assert_eq!(watched.bytes, charge);
    assert_eq!(watched.live_at_allocation, Some(charge));
    assert_eq!(watched.live_at_deallocation, Some(charge));
    assert_eq!(watched.deallocations, 1);
    assert_eq!(provider.used(), 0);
}

#[test]
fn header_capture_replay_tracer() {
    use clinker_format::FormatWriter;
    use clinker_format::csv::writer::{
        CsvEncoder, CsvEncoderConfig, CsvHeaderCapture, CsvWriterConfig,
    };
    use clinker_format::splitting::{OversizeGroupPolicy, SplitPolicy, SplittingWriter};
    use clinker_record::{Record, Value};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Destination(Arc<Mutex<Vec<u8>>>);
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
    let resources = provider.resources();
    let capture = CsvHeaderCapture::new(&resources).unwrap();
    let capture_charge = provider.used();
    let schema = csv_schema();
    // Reject a differently named first body; later split output must use its
    // own header rather than any names from this unsuccessful attempt.
    let rejected_schema = SharedStorage::from_arc(Arc::new(clinker_record::Schema::new(vec![
        "rejected".into(),
        "names".into(),
    ])));
    let encoder = CsvEncoder::new(
        rejected_schema.clone(),
        &CsvWriterConfig::default(),
        resources.clone(),
    )
    .unwrap()
    .with_header_capture(capture.clone());
    let mut rejected = PreparedWriter::new(Vec::new(), encoder, resources.clone()).unwrap();
    let before = provider.used();
    assert!(
        rejected
            .write_record(&Record::new(
                rejected_schema,
                vec![Value::Null, Value::Array(OwnedValues::from_vec(vec![]))]
            ))
            .is_err()
    );
    assert!(rejected.destination().is_empty());
    assert_eq!(provider.used(), before);
    drop(rejected);
    assert_eq!(provider.used(), capture_charge);

    let destinations = [Destination::default(), Destination::default()];
    let files = destinations.clone();
    let policy = CsvEncoderConfig::new((&CsvWriterConfig::default()).into(), &resources).unwrap();
    let shared = capture.clone();
    let factory_scope = resources.scope().unwrap();
    let mut split = SplittingWriter::new(
        Box::new(move |sequence| Ok(Box::new(files[sequence as usize - 1].clone()))),
        clinker_format::splitting::WriterFactory::try_new(
            move |destination, schema| {
                CsvEncoder::from_config(schema, policy.clone(), resources.clone())?
                    .with_header_capture(shared.clone())
                    .into_boxed_writer(destination, resources.clone())
            },
            factory_scope.allocation(),
        )
        .unwrap(),
        schema.clone(),
        SplitPolicy {
            max_records: Some(1),
            max_bytes: None,
            group_key: None,
            oversize_group: OversizeGroupPolicy::Error,
        },
    );
    split
        .write_record(&Record::new(
            schema.clone(),
            vec![Value::Integer(1), Value::String("a,b".into())],
        ))
        .unwrap();
    split
        .write_record(&Record::new(
            schema,
            vec![Value::Integer(2), Value::String("c".into())],
        ))
        .unwrap();
    split.flush_bytes().unwrap();
    assert_eq!(
        *destinations[0].0.lock().unwrap(),
        b"first,last\n1,\"a,b\"\n"
    );
    assert_eq!(*destinations[1].0.lock().unwrap(), b"first,last\n2,c\n");
    drop(split);
    assert!(
        provider.used() > capture_charge,
        "published text survives every split writer"
    );
    let retained = provider.used();
    struct RefuseReplay;
    impl Write for RefuseReplay {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("failed replay must not reach flush")
        }
    }
    let encoder = CsvEncoder::new(
        csv_schema(),
        &CsvWriterConfig::default(),
        provider.resources(),
    )
    .unwrap()
    .with_header_capture(capture.clone());
    let mut failed_replay =
        PreparedWriter::new(RefuseReplay, encoder, provider.resources()).unwrap();
    let record = Record::new(csv_schema(), vec![Value::Integer(3), Value::Integer(4)]);
    assert!(failed_replay.write_record(&record).is_err());
    assert!(
        matches!(failed_replay.write_record(&record), Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::DeliveryPoisoned)
    );
    drop(failed_replay);
    assert_eq!(
        provider.used(),
        retained,
        "failed replay keeps only the published header"
    );
    let alias = capture.clone();
    assert_eq!(
        provider.used(),
        retained,
        "replay aliases share one allocation"
    );
    drop(capture);
    assert_eq!(provider.used(), retained);
    drop(alias);
    assert_eq!(provider.used(), 0);

    // Isolate the real allocator-null probe: an infallible shared owner aborts
    // the child instead of disguising that failure as a recoverable error.
    for test in [
        "header_capture_shared_backing_refusal",
        "header_config_shared_backing_refusal",
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{test}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn header_capture_shared_backing_refusal() {
    use clinker_format::csv::writer::CsvHeaderCapture;
    for allocation_failure in [false, true] {
        let provider = MemoryOnlyResources::new(
            NonZeroUsize::new(if allocation_failure { 128 * 1024 } else { 1 }).unwrap(),
        );
        let (result, allocations) = allocation_probe(allocation_failure, || {
            CsvHeaderCapture::new(&provider.resources())
        });
        let FormatError::Resource(error) = result.err().expect("shared owner must refuse") else {
            panic!("typed refusal required")
        };
        assert_eq!(
            error.kind,
            if allocation_failure {
                ResourceErrorKind::Allocation
            } else {
                ResourceErrorKind::Budget
            }
        );
        assert_eq!(allocations, usize::from(allocation_failure));
        assert!(error.requested > 0);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn header_config_shared_backing_refusal() {
    use clinker_format::csv::writer::{CsvEncoderConfig, CsvWriterConfig};
    for allocation_failure in [false, true] {
        let provider = MemoryOnlyResources::new(
            NonZeroUsize::new(if allocation_failure { 128 * 1024 } else { 1 }).unwrap(),
        );
        let config = CsvWriterConfig::default();
        let (result, allocations) = allocation_probe(allocation_failure, || {
            CsvEncoderConfig::new((&config).into(), &provider.resources())
        });
        let FormatError::Resource(error) = result.err().expect("shared owner must refuse") else {
            panic!("typed refusal required")
        };
        assert_eq!(
            error.kind,
            if allocation_failure {
                ResourceErrorKind::Allocation
            } else {
                ResourceErrorKind::Budget
            }
        );
        assert_eq!(allocations, usize::from(allocation_failure));
        assert!(error.requested > 0);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn header_config_policy_children_release_on_each_allocation_refusal() {
    use clinker_format::csv::writer::{CsvEncoderConfig, CsvWriterConfig};
    use clinker_format::multi_value::{JoinValues, OnConflict};
    let config = CsvWriterConfig {
        join_values: vec![JoinValues {
            field: "tags".into(),
            delimiter: "||".into(),
            escape: "\\".into(),
            on_conflict: OnConflict::Escape,
            repeat_as: None,
            wrap_in: None,
        }],
        declared_multiple: ["tags".into()].into(),
        envelope: Some(clinker_format::OutputEnvelopeSpec {
            header_from_doc: Some("opening".into()),
            footer_from_doc: Some("closing".into()),
            footer_record_count_field: Some("count".into()),
        }),
        ..Default::default()
    };
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
    let (policy, count) = allocation_probe(false, || {
        CsvEncoderConfig::new((&config).into(), &provider.resources()).unwrap()
    });
    assert!(
        count > 1,
        "both child and shared backing allocations must execute"
    );
    let retained = provider.used();
    let alias = policy.clone();
    assert_eq!(provider.used(), retained);
    drop(policy);
    assert_eq!(provider.used(), retained);
    drop(alias);
    assert_eq!(provider.used(), 0);
    for allowed in 0..count {
        let (result, attempts) = allocation_probe(false, || {
            ALLOCATIONS_LEFT.with(|remaining| remaining.set(Some(allowed)));
            CsvEncoderConfig::new((&config).into(), &provider.resources())
        });
        assert!(
            matches!(result, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Allocation)
        );
        assert_eq!(attempts, allowed + 1);
        assert_eq!(
            provider.used(),
            0,
            "refused shared backing consumes and releases all policy children"
        );
    }
}

#[test]
fn header_capture_replays_the_committed_preset_names() {
    use clinker_format::csv::writer::{CsvEncoder, CsvHeaderCapture, CsvWriterConfig};
    use clinker_record::{Record, Value};
    for include_header in [true, false] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
        let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
        for preset in [true, false] {
            let mut encoder = CsvEncoder::new(
                csv_schema(),
                &CsvWriterConfig {
                    include_header,
                    ..Default::default()
                },
                provider.resources(),
            )
            .unwrap()
            .with_header_capture(capture.clone());
            if preset {
                encoder
                    .set_preset_header(&["chosen,first".into(), "chosen last".into()])
                    .unwrap();
            }
            let mut writer =
                PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
            let bad = Record::new(
                csv_schema(),
                vec![
                    Value::Integer(1),
                    Value::Array(OwnedValues::from_vec(vec![])),
                ],
            );
            let retained = provider.used();
            assert!(
                writer
                    .write_operation(OutputOperation::Record(&bad))
                    .is_err()
            );
            assert!(writer.destination().is_empty());
            assert_eq!(provider.used(), retained);
            let good = Record::new(csv_schema(), vec![Value::Integer(1), Value::Integer(2)]);
            writer
                .write_operation(OutputOperation::Record(&good))
                .unwrap();
            assert_eq!(writer.destination(), b"\"chosen,first\",chosen last\n1,2\n");
        }
        drop(capture);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn header_capture_does_not_invent_suppressed_headers() {
    use clinker_format::csv::writer::{CsvEncoder, CsvHeaderCapture, CsvWriterConfig};
    use clinker_record::{Record, Value};
    for (include_header, envelope, preset) in [
        (false, false, false),
        (true, true, false),
        (false, true, true),
    ] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
        let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
        let capture_charge = provider.used();
        let config = CsvWriterConfig {
            include_header,
            envelope: envelope.then(|| clinker_format::OutputEnvelopeSpec {
                header_from_doc: Some("opening".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        for first in [true, false] {
            let mut encoder = CsvEncoder::new(csv_schema(), &config, provider.resources())
                .unwrap()
                .with_header_capture(capture.clone());
            if first && preset {
                encoder
                    .set_preset_header(&["preset one".into(), "preset two".into()])
                    .unwrap();
            }
            let mut writer =
                PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
            writer
                .write_operation(OutputOperation::Record(&Record::new(
                    csv_schema(),
                    vec![Value::Integer(1), Value::Integer(2)],
                )))
                .unwrap();
            assert_eq!(writer.destination(), b"1,2\n");
            drop(writer);
            assert_eq!(
                provider.used(),
                capture_charge,
                "a suppressed header must not become retained replay state"
            );
        }
        drop(capture);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn csv_invalid_body_does_not_deliver_automatic_header() {
    use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
    use clinker_record::{Record, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
    let schema = csv_schema();
    let encoder = CsvEncoder::new(
        schema.clone(),
        &CsvWriterConfig::default(),
        provider.resources(),
    )
    .unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    let bad = Record::new(
        schema.clone(),
        vec![
            Value::String("valid".into()),
            Value::Array(OwnedValues::from_vec(vec![])),
        ],
    );
    assert!(
        writer
            .write_operation(OutputOperation::Record(&bad))
            .is_err()
    );
    assert!(writer.destination().is_empty());
    let good = Record::new(
        schema,
        vec![Value::String("yes".into()), Value::String("ok".into())],
    );
    writer
        .write_operation(OutputOperation::Record(&good))
        .unwrap();
    assert_eq!(writer.destination(), b"first,last\nyes,ok\n");
    drop(writer);
    assert_eq!(provider.used(), 0);
}

#[test]
fn csv_late_quoting_and_rotating_columns_release_workspace() {
    use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
    use clinker_record::{Record, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
    let schema = csv_schema();
    let encoder = CsvEncoder::new(
        schema.clone(),
        &CsvWriterConfig::default(),
        provider.resources(),
    )
    .unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    let retained = provider.used();
    for column in [0, 1, 0, 1] {
        let wide = format!("{},\"\n", "a".repeat(40_000));
        let mut fields = vec![Value::String("tiny".into()); 2];
        fields[column] = Value::String(wide.into());
        writer
            .write_operation(OutputOperation::Record(&Record::new(
                schema.clone(),
                fields,
            )))
            .unwrap();
        assert_eq!(provider.used(), retained);
    }
    let mut reader = csv::Reader::from_reader(writer.destination().as_slice());
    let mut rows = 0;
    for (row, record) in reader.records().enumerate() {
        let record = record.unwrap();
        assert_eq!(record[row % 2].len(), 40_003);
        assert!(record[row % 2].ends_with(",\"\n"));
        rows += 1;
    }
    assert_eq!(rows, 4);
}

#[test]
fn csv_joined_and_embedded_json_deny_before_delivery() {
    use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
    use clinker_format::multi_value::{JoinValues, OnConflict};
    use clinker_record::{Record, Value};
    for policy in [OnConflict::Escape, OnConflict::EncodeJson] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(64 * 1024).unwrap());
        let schema = csv_schema();
        let config = CsvWriterConfig {
            declared_multiple: ["last".to_string()].into(),
            join_values: vec![JoinValues {
                field: "last".into(),
                delimiter: ";".into(),
                on_conflict: policy,
                escape: "\\".into(),
                repeat_as: None,
                wrap_in: None,
            }],
            ..Default::default()
        };
        let encoder = CsvEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        let retained = provider.used();
        let record = Record::new(
            schema,
            vec![
                Value::Null,
                Value::Array(OwnedValues::from_vec(vec![Value::String(
                    ";".repeat(100_000).into(),
                )])),
            ],
        );
        assert!(matches!(
            writer.write_operation(OutputOperation::Record(&record)),
            Err(FormatError::Resource(_))
        ));
        assert!(writer.destination().is_empty());
        assert_eq!(provider.used(), retained);
    }
}

#[test]
fn csv_many_small_columns_admit_projection_and_pending_header_slots() {
    use clinker_format::csv::writer::{
        CsvEncoder, CsvEncoderConfig, CsvHeaderCapture, CsvWriterConfig,
    };
    use clinker_format::reserved::ReservedText;
    use clinker_record::{Record, Schema, Value};
    use std::alloc::Layout;
    use std::sync::Arc;

    const COLUMNS: usize = 512;
    let names: Vec<_> = (0..COLUMNS).map(|index| format!("c{index:03}")).collect();
    let schema = SharedStorage::from_arc(Arc::new(Schema::new(
        names.iter().map(|name| name.as_str().into()).collect(),
    )));
    let reordered = SharedStorage::from_arc(Arc::new(Schema::new(
        names
            .iter()
            .rev()
            .map(|name| name.as_str().into())
            .collect(),
    )));
    let record = Record::new(
        reordered,
        (0..COLUMNS)
            .rev()
            .map(|index| Value::Integer(index as i64))
            .collect(),
    );
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(512 * 1024).unwrap());
    let resources = provider.resources();
    let config = CsvEncoderConfig::new((&CsvWriterConfig::default()).into(), &resources).unwrap();
    let config_charge = provider.used();
    let encoder = CsvEncoder::from_config(schema.clone(), config, resources.clone()).unwrap();
    // from_config allocates only the column table; measure its private layout
    // rather than copying PreparedColumn's representation into this test.
    let projection_charge = provider.used() - config_charge;
    assert!(projection_charge >= COLUMNS * size_of::<usize>());
    let capture = CsvHeaderCapture::new(&resources).unwrap();
    let capture_charge = provider.used() - config_charge - projection_charge;
    let mut writer = PreparedWriter::new(
        Vec::new(),
        encoder.with_header_capture(capture.clone()),
        resources,
    )
    .unwrap();
    let baseline = provider.used();
    writer
        .write_operation(OutputOperation::Record(&record))
        .unwrap();
    let header_charge = Layout::array::<ReservedText>(COLUMNS).unwrap().size()
        + names.iter().map(String::len).sum::<usize>();
    assert_eq!(provider.used(), baseline + header_charge);
    writer
        .write_operation(OutputOperation::Record(&record))
        .unwrap();
    assert_eq!(provider.used(), baseline + header_charge);
    let row = (0..COLUMNS)
        .map(|index| index.to_string())
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(
        writer.destination(),
        format!("{}\n{row}\n{row}\n", names.join(",")).as_bytes()
    );
    assert_eq!(
        csv::Reader::from_reader(writer.destination().as_slice())
            .records()
            .count(),
        2
    );
    drop(writer);
    assert_eq!(provider.used(), capture_charge + header_charge);
    drop(capture);
    assert_eq!(provider.used(), 0);

    let provider =
        MemoryOnlyResources::new(NonZeroUsize::new(config_charge + projection_charge - 1).unwrap());
    let config =
        CsvEncoderConfig::new((&CsvWriterConfig::default()).into(), &provider.resources()).unwrap();
    let error = CsvEncoder::from_config(schema.clone(), config, provider.resources())
        .err()
        .unwrap();
    assert!(matches!(error, FormatError::Resource(error)
        if error == ResourceError::new(ResourceErrorKind::Budget, projection_charge, projection_charge - 1)));
    assert_eq!(provider.used(), 0);

    // Isolate the capture slot allocation from stage storage. The fixed CSV
    // buffer/error grants fit; the very next allocation is the name-slot table.
    let slots = Layout::array::<ReservedText>(COLUMNS).unwrap().size();
    let provider = MemoryOnlyResources::new(
        NonZeroUsize::new(baseline + 8192 + size_of::<csv::ErrorKind>() + slots - 1).unwrap(),
    );
    let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
    let encoder = CsvEncoder::new(schema, &CsvWriterConfig::default(), provider.resources())
        .unwrap()
        .with_header_capture(capture.clone());
    assert_eq!(provider.used(), baseline);
    let mut output = Vec::new();
    let error = encoder
        .prepare(
            OutputOperation::Record(&record),
            &mut output,
            &provider.resources().scope().unwrap(),
        )
        .err()
        .unwrap();
    assert!(matches!(error, FormatError::Resource(error)
        if error == ResourceError::new(ResourceErrorKind::Budget, slots, slots - 1)));
    assert!(output.is_empty());
    assert_eq!(provider.used(), baseline);
    drop(encoder);
    assert_eq!(provider.used(), capture_charge);
    drop(capture);
    assert_eq!(provider.used(), 0);
}

#[test]
fn csv_expansion_alone_exceeds_cell_budget_and_repeated_success_releases_workspace() {
    use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
    use clinker_format::multi_value::{JoinValues, OnConflict};
    use clinker_record::{Record, Value};

    const LENGTH: usize = 16_000;
    const ROWS: usize = 8;
    for policy in [OnConflict::Escape, OnConflict::EncodeJson] {
        let config = CsvWriterConfig {
            declared_multiple: ["last".to_string()].into(),
            join_values: vec![JoinValues {
                field: "last".into(),
                delimiter: ";".into(),
                on_conflict: policy,
                escape: "\\".into(),
                repeat_as: None,
                wrap_in: None,
            }],
            ..Default::default()
        };
        let (input, expanded, expected_row) = match policy {
            OnConflict::Escape => (
                ";".repeat(LENGTH),
                LENGTH * 2,
                format!("7,{}\n", "\\;".repeat(LENGTH)),
            ),
            OnConflict::EncodeJson => (
                "\\".repeat(LENGTH),
                LENGTH * 2 + 4,
                format!("7,\"[\"\"{}\"\"]\"\n", "\\\\".repeat(LENGTH)),
            ),
            OnConflict::Error => unreachable!(),
        };
        let expected_scalar = format!("first,last\n7,{input}\n");
        let scalar = Record::new(
            csv_schema(),
            vec![Value::Integer(7), Value::String(input.clone().into())],
        );
        let array = Record::new(
            csv_schema(),
            vec![
                Value::Integer(7),
                Value::Array(OwnedValues::from_vec(vec![Value::String(input.into())])),
            ],
        );
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(512 * 1024).unwrap());
        let encoder = CsvEncoder::new(csv_schema(), &config, provider.resources()).unwrap();
        let baseline = provider.used();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        for _ in 0..ROWS {
            writer
                .write_operation(OutputOperation::Record(&array))
                .unwrap();
            assert_eq!(provider.used(), baseline);
        }
        assert_eq!(
            writer.destination(),
            format!("first,last\n{}", expected_row.repeat(ROWS)).as_bytes()
        );
        assert_eq!(
            csv::Reader::from_reader(writer.destination().as_slice())
                .records()
                .count(),
            ROWS
        );
        drop(writer);
        assert_eq!(provider.used(), 0);

        // Direct preparation removes stage growth from this refusal oracle:
        // leave room for the unexpanded input, but not the rendered cell.
        let provider = MemoryOnlyResources::new(
            NonZeroUsize::new(baseline + 8192 + size_of::<csv::ErrorKind>() + expanded - 1)
                .unwrap(),
        );
        let encoder = CsvEncoder::new(csv_schema(), &config, provider.resources()).unwrap();
        let scope = provider.resources().scope().unwrap();
        let mut unexpanded = Vec::new();
        let pending = encoder
            .prepare(OutputOperation::Record(&scalar), &mut unexpanded, &scope)
            .unwrap();
        assert_eq!(
            unexpanded,
            expected_scalar.as_bytes(),
            "the same unexpanded input fits"
        );
        drop(pending);
        assert_eq!(provider.used(), baseline);
        let mut refused = Vec::new();
        let error = encoder
            .prepare(OutputOperation::Record(&array), &mut refused, &scope)
            .err()
            .unwrap();
        assert!(matches!(error, FormatError::Resource(error)
            if error.kind == ResourceErrorKind::Budget
                && error.requested == expanded
                && (LENGTH..expanded).contains(&error.available)));
        assert_eq!(provider.used(), baseline);
        drop(encoder);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn csv_single_empty_field_and_preset_header_keep_library_bytes() {
    use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
    use clinker_record::{Record, Schema, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec!["value".into()])));
    let mut encoder = CsvEncoder::new(
        schema.clone(),
        &CsvWriterConfig::default(),
        provider.resources(),
    )
    .unwrap();
    encoder.set_preset_header(&["captured".into()]).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    let bad = Record::new(
        schema.clone(),
        vec![Value::Array(OwnedValues::from_vec(vec![]))],
    );
    assert!(
        writer
            .write_operation(OutputOperation::Record(&bad))
            .is_err()
    );
    assert!(writer.destination().is_empty());
    writer
        .write_operation(OutputOperation::Record(&Record::new(
            schema,
            vec![Value::Null],
        )))
        .unwrap();
    assert_eq!(writer.destination(), b"captured\n\"\"\n");
}

#[test]
fn csv_envelope_failed_body_preserves_committed_count() {
    use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let schema = csv_schema();
    let config = CsvWriterConfig {
        envelope: Some(clinker_format::OutputEnvelopeSpec {
            header_from_doc: Some("open".into()),
            footer_from_doc: Some("close".into()),
            footer_record_count_field: Some("rows".into()),
        }),
        ..Default::default()
    };
    let section = |text: &str| {
        Value::Map(OwnedMap::from_map(
            [(OwnedKey::from("text"), Value::String(text.into()))].into(),
        ))
    };
    let doc = DocumentContext::new(
        DocumentId::next(),
        std::sync::Arc::from("fixture.csv"),
        EnvelopeRecord::from_sections([
            (OwnedKey::from("open"), section("BEGIN")),
            (OwnedKey::from("close"), section("END")),
        ]),
    );
    let encoder = CsvEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    writer
        .write_operation(OutputOperation::BeginDocument(&doc))
        .unwrap();
    let bad = Record::new(
        schema.clone(),
        vec![Value::Null, Value::Array(OwnedValues::from_vec(vec![]))],
    );
    assert!(
        writer
            .write_operation(OutputOperation::Record(&bad))
            .is_err()
    );
    assert_eq!(writer.destination(), b"BEGIN\n");
    let good = Record::new(schema, vec![Value::Integer(1), Value::Integer(2)]);
    writer
        .write_operation(OutputOperation::Record(&good))
        .unwrap();
    writer
        .write_operation(OutputOperation::EndDocument(&doc))
        .unwrap();
    writer.flush().unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.destination(), b"BEGIN\n1,2\nEND,1\n");
}

#[test]
fn csv_delivery_prefix_failure_poisoned_without_retry() {
    use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
    struct Prefix {
        calls: usize,
    }
    impl Write for Prefix {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            if self.calls == 1 {
                Ok(1)
            } else {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("must not retry poisoned delivery")
        }
    }
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let schema = csv_schema();
    let encoder = CsvEncoder::new(
        schema.clone(),
        &CsvWriterConfig::default(),
        provider.resources(),
    )
    .unwrap();
    let mut writer =
        PreparedWriter::new(Prefix { calls: 0 }, encoder, provider.resources()).unwrap();
    let record = clinker_record::Record::new(schema, vec![clinker_record::Value::Null; 2]);
    assert!(
        writer
            .write_operation(OutputOperation::Record(&record))
            .is_err()
    );
    assert!(
        writer
            .write_operation(OutputOperation::Record(&record))
            .is_err()
    );
    assert!(writer.flush().is_err());
    assert_eq!(writer.destination().calls, 2);
}

#[test]
fn csv_embedded_json_and_scalars_match_existing_representation() {
    use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
    use clinker_format::multi_value::{JoinValues, OnConflict};
    use clinker_record::{Record, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
    let schema = csv_schema();
    let config = CsvWriterConfig {
        declared_multiple: ["last".to_string()].into(),
        join_values: vec![JoinValues {
            field: "last".into(),
            delimiter: ";".into(),
            on_conflict: OnConflict::EncodeJson,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }],
        ..Default::default()
    };
    let values = vec![
        Value::String("quoted \" , \n".into()),
        Value::Float(1.25),
        Value::Decimal("1.2300".parse().unwrap()),
        Value::Date("2026-09-12".parse().unwrap()),
        Value::DateTime("2026-09-12T01:02:03.123456".parse().unwrap()),
        Value::Map(OwnedMap::from_map(
            [(OwnedKey::from("\\@id"), Value::Integer(4))].into(),
        )),
    ];
    let encoder = CsvEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    for scalar in &values[..5] {
        let record = Record::new(
            schema.clone(),
            vec![
                scalar.clone(),
                Value::Array(OwnedValues::from_vec(values.clone())),
            ],
        );
        writer
            .write_operation(OutputOperation::Record(&record))
            .unwrap();
    }
    // Literal predecessor bytes: CSV doubles quotes; embedded JSON escapes the
    // string's quote/newline, preserves decimal scale, and renders datetime with
    // a space while the scalar column uses T. The nested map key is unescaped.
    let expected = concat!(
        "first,last\n\"quoted \"\" , \n\",",
        r#""[""quoted \"" , \n"",1.25,""1.2300"",""2026-09-12"",""2026-09-12 01:02:03.123456"",{""@id"":4}]"
1.25,"[""quoted \"" , \n"",1.25,""1.2300"",""2026-09-12"",""2026-09-12 01:02:03.123456"",{""@id"":4}]"
1.2300,"[""quoted \"" , \n"",1.25,""1.2300"",""2026-09-12"",""2026-09-12 01:02:03.123456"",{""@id"":4}]"
2026-09-12,"[""quoted \"" , \n"",1.25,""1.2300"",""2026-09-12"",""2026-09-12 01:02:03.123456"",{""@id"":4}]"
2026-09-12T01:02:03.123456,"[""quoted \"" , \n"",1.25,""1.2300"",""2026-09-12"",""2026-09-12 01:02:03.123456"",{""@id"":4}]"
"#,
    );
    assert_eq!(writer.destination(), expected.as_bytes());
}

#[test]
fn csv_charset_sink_rejects_with_bounded_field_offset() {
    let mut output = Vec::new();
    let error = clinker_format::charset::Charset::Latin1
        .encode_to(&format!("{}€", "x".repeat(20_000)), 7, &mut output)
        .unwrap_err();
    assert!(matches!(
        error,
        FormatError::OutputEncoding {
            field: 7,
            offset: 20_000,
            ..
        }
    ));
    assert!(error.to_string().len() < 256);
}

#[test]
fn csv_public_writer_entrypoints_require_finite_resources() {
    use clinker_format::csv::CsvWriterConfig;
    use clinker_format::csv::writer::{CsvEncoder, CsvEncoderConfig, CsvHeaderCapture};
    use clinker_record::{Record, Value};

    fn denied<T>(result: Result<T, FormatError>) {
        assert!(matches!(
            result,
            Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget
        ));
    }

    // Independently enumerated public constructor/capture inventory. Keep the
    // resource-free API absence checks in the CSV module's compile-fail docs.
    let paths = [
        "CsvEncoderConfig::new",
        "CsvHeaderCapture::new",
        "CsvEncoder::new",
        "CsvEncoder::from_config",
        "CsvEncoder::into_boxed_writer",
        "CsvEncoder::set_preset_header",
        "PreparedWriter::new<CsvEncoder>",
        "CsvEncoder::with_header_capture",
    ];
    for path in paths {
        println!("finite-denial probe: {path}");
        let denied_provider = MemoryOnlyResources::new(NonZeroUsize::new(1).unwrap());
        let limit = 256 * 1024;
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(limit).unwrap());
        let resources = provider.resources();
        let config = CsvWriterConfig::default();
        let schema = csv_schema();
        match path {
            "CsvEncoderConfig::new" => denied(CsvEncoderConfig::new(
                (&config).into(),
                &denied_provider.resources(),
            )),
            "CsvHeaderCapture::new" => {
                denied(CsvHeaderCapture::new(&denied_provider.resources()));
            }
            "CsvEncoder::new" => {
                denied(CsvEncoder::new(
                    schema,
                    &config,
                    denied_provider.resources(),
                ));
            }
            "CsvEncoder::from_config" => {
                let policy = CsvEncoderConfig::new((&config).into(), &resources).unwrap();
                denied(CsvEncoder::from_config(
                    schema,
                    policy,
                    denied_provider.resources(),
                ));
            }
            "CsvEncoder::into_boxed_writer" => {
                let encoder = CsvEncoder::new(schema, &config, resources.clone()).unwrap();
                denied(encoder.into_boxed_writer(Vec::new(), denied_provider.resources()));
            }
            "CsvEncoder::set_preset_header" => {
                let mut encoder =
                    CsvEncoder::new(schema.clone(), &config, resources.clone()).unwrap();
                let baseline = provider.used();
                let held = resources
                    .scope()
                    .unwrap()
                    .reserve(std::alloc::Layout::array::<u8>(limit - baseline).unwrap())
                    .unwrap();
                denied(encoder.set_preset_header(&["renamed".into(), "last".into()]));
                assert_eq!(provider.used(), limit);
                drop(held);
                assert_eq!(provider.used(), baseline);
                // A denied preset leaves the original schema header intact.
                let record = Record::new(schema, vec![Value::Integer(1), Value::Integer(2)]);
                let mut writer =
                    PreparedWriter::new(Vec::new(), encoder, resources.clone()).unwrap();
                writer
                    .write_operation(OutputOperation::Record(&record))
                    .unwrap();
                assert_eq!(writer.destination(), b"first,last\n1,2\n");
            }
            "PreparedWriter::new<CsvEncoder>" => {
                let encoder = CsvEncoder::new(schema.clone(), &config, resources.clone()).unwrap();
                let mut writer =
                    PreparedWriter::new(Vec::new(), encoder, denied_provider.resources()).unwrap();
                let record = Record::new(schema, vec![Value::Integer(1), Value::Integer(2)]);
                denied(writer.write_operation(OutputOperation::Record(&record)));
                assert!(writer.destination().is_empty());
            }
            "CsvEncoder::with_header_capture" => {
                let capture = CsvHeaderCapture::new(&resources).unwrap();
                let encoder = CsvEncoder::new(schema.clone(), &config, resources.clone())
                    .unwrap()
                    .with_header_capture(capture.clone());
                let record =
                    Record::new(schema.clone(), vec![Value::Integer(1), Value::Integer(2)]);
                let mut writer =
                    PreparedWriter::new(Vec::new(), encoder, resources.clone()).unwrap();
                let held = resources
                    .scope()
                    .unwrap()
                    .reserve(std::alloc::Layout::array::<u8>(limit - provider.used()).unwrap())
                    .unwrap();
                denied(writer.write_operation(OutputOperation::Record(&record)));
                assert!(writer.destination().is_empty());
                drop(held);
                drop(writer);
                // With automatic headers disabled, any header here would prove
                // that the rejected operation incorrectly published capture.
                let config = CsvWriterConfig {
                    include_header: false,
                    ..config
                };
                let encoder = CsvEncoder::new(schema, &config, resources.clone())
                    .unwrap()
                    .with_header_capture(capture);
                let mut writer =
                    PreparedWriter::new(Vec::new(), encoder, resources.clone()).unwrap();
                writer
                    .write_operation(OutputOperation::Record(&record))
                    .unwrap();
                assert_eq!(writer.destination(), b"1,2\n");
            }
            _ => panic!("unprobed public CSV entrypoint: {path}"),
        }
        assert_eq!(provider.used(), 0, "{path} retained an allocation");
        assert_eq!(
            denied_provider.used(),
            0,
            "{path} retained a denied allocation"
        );
    }
}

#[test]
fn csv_shared_header_capture_commits_only_after_success() {
    use clinker_format::csv::writer::{CsvEncoder, CsvHeaderCapture, CsvWriterConfig};
    use clinker_record::{Record, Schema, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
    let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
    let schema = csv_schema();
    let encoder = CsvEncoder::new(
        schema.clone(),
        &CsvWriterConfig::default(),
        provider.resources(),
    )
    .unwrap()
    .with_header_capture(capture.clone());
    let mut failed = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    let bad = Record::new(
        schema,
        vec![Value::Null, Value::Array(OwnedValues::from_vec(vec![]))],
    );
    let retained = provider.used();
    assert!(
        failed
            .write_operation(OutputOperation::Record(&bad))
            .is_err()
    );
    assert_eq!(provider.used(), retained);
    drop(failed);
    for names in [["a", "b"], ["renamed", "other"]] {
        let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(
            names.map(OwnedKey::from).into(),
        )));
        let encoder = CsvEncoder::new(
            schema.clone(),
            &CsvWriterConfig::default(),
            provider.resources(),
        )
        .unwrap()
        .with_header_capture(capture.clone());
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        writer
            .write_operation(OutputOperation::Record(&Record::new(
                schema,
                vec![Value::Integer(1), Value::Integer(2)],
            )))
            .unwrap();
        assert_eq!(writer.destination(), b"a,b\n1,2\n");
    }
    drop(capture);
    assert_eq!(provider.used(), 0);
}

#[test]
fn csv_collision_evidence_uses_projected_order_and_array_position() {
    use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
    use clinker_record::{Record, Schema, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let pinned = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec![
        "id".into(),
        "tags".into(),
    ])));
    let reordered = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec![
        "tags".into(),
        "id".into(),
    ])));
    let config = CsvWriterConfig {
        declared_multiple: ["tags".into()].into(),
        ..Default::default()
    };
    let encoder = CsvEncoder::new(pinned, &config, provider.resources()).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    let record = Record::new(
        reordered,
        vec![
            Value::Array(OwnedValues::from_vec(vec![
                Value::String("ok".into()),
                Value::String("bad;value".into()),
            ])),
            Value::Integer(3),
        ],
    );
    let error = writer
        .write_operation(OutputOperation::Record(&record))
        .unwrap_err();
    assert!(
        matches!(error, FormatError::OutputEncoding { field: 1, element: Some(element), offset: 3, .. } if element.get() == 2)
    );
    assert!(error.is_join_collision());
    assert!(error.to_string().contains("tags"));
    assert!(writer.destination().is_empty());
}

struct FaultAllocator;
thread_local! {
    static ALLOCATIONS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static FAIL_NEXT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static ALLOCATIONS_LEFT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static BACKING_WATCH: std::cell::Cell<Option<BackingWatch>> = const { std::cell::Cell::new(None) };
}

#[derive(Clone, Copy)]
struct BackingWatch {
    provider: *const MemoryOnlyResources,
    pointer: *mut u8,
    bytes: usize,
    live_at_deallocation: Option<usize>,
    live_at_allocation: Option<usize>,
    layout: Option<std::alloc::Layout>,
    deallocations: usize,
}
// SAFETY: successful allocations and all deallocations use System with the
// original layouts. Failure returns null, as GlobalAlloc permits. Thread-local
// scalar tracking neither allocates nor affects other test threads.
unsafe impl std::alloc::GlobalAlloc for FaultAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| {
            if let Some(n) = count.get() {
                count.set(Some(n + 1));
            }
        });
        if FAIL_NEXT
            .try_with(|fail| fail.replace(false))
            .unwrap_or(false)
        {
            return std::ptr::null_mut();
        }
        if ALLOCATIONS_LEFT
            .try_with(|remaining| match remaining.get() {
                Some(0) => true,
                Some(count) => {
                    remaining.set(Some(count - 1));
                    false
                }
                None => false,
            })
            .unwrap_or(false)
        {
            return std::ptr::null_mut();
        }
        // SAFETY: the caller supplies a valid allocation layout.
        let pointer = unsafe { std::alloc::System.alloc(layout) };
        let _ = BACKING_WATCH.try_with(|watch| {
            if let Some(mut state) = watch.get()
                && state.pointer.is_null()
                && state.layout.is_none_or(|expected| expected == layout)
            {
                state.pointer = pointer;
                state.bytes = layout.size();
                // SAFETY: the observation guard keeps the provider alive; its
                // fixed, warmed mutex does not allocate or enclose allocation.
                state.live_at_allocation = Some(unsafe { &*state.provider }.used());
                watch.set(Some(state));
            }
        });
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        let _ = BACKING_WATCH.try_with(|watch| {
            if let Some(mut state) = watch.get()
                && state.pointer == pointer
                && state.live_at_deallocation.is_none()
            {
                // SAFETY: the test keeps the provider alive until the watch is
                // removed. The provider's fixed mutex was initialized before
                // observation; used() allocates nothing. Stop after the first
                // deallocation so later pointer reuse cannot replace evidence.
                state.live_at_deallocation = Some(unsafe { &*state.provider }.used());
                state.deallocations += 1;
                watch.set(Some(state));
            }
        });
        // SAFETY: every non-null allocation above came from System.
        unsafe { std::alloc::System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: FaultAllocator = FaultAllocator;

#[test]
fn header_shared_backings_remain_charged_through_final_alias_deallocation() {
    use clinker_format::csv::writer::{CsvEncoderConfig, CsvHeaderCapture, CsvWriterConfig};
    fn check<T: Clone>(make: impl FnOnce(&MemoryOnlyResources) -> T) {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
        BACKING_WATCH.with(|watch| {
            watch.set(Some(BackingWatch {
                provider: &provider,
                pointer: std::ptr::null_mut(),
                bytes: 0,
                live_at_deallocation: None,
                live_at_allocation: None,
                layout: None,
                deallocations: 0,
            }))
        });
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                BACKING_WATCH.with(|watch| watch.set(None));
            }
        }
        let reset = Reset;
        let owner = make(&provider);
        let charge = provider.used();
        assert!(charge > 0);
        let (alias, allocations) = allocation_probe(false, || owner.clone());
        assert_eq!(allocations, 0);
        assert_eq!(provider.used(), charge);
        drop(owner);
        assert_eq!(provider.used(), charge);
        assert!(BACKING_WATCH.with(|watch| watch.get().unwrap().live_at_deallocation.is_none()));
        drop(alias);
        let observed = BACKING_WATCH.with(|watch| watch.get().unwrap());
        assert!(observed.bytes > 0);
        assert!(
            observed.live_at_deallocation.unwrap() >= observed.bytes,
            "the actual shared allocation must be freed before its grant"
        );
        assert_eq!(provider.used(), 0);
        drop(reset);
    }
    check(|provider| CsvHeaderCapture::new(&provider.resources()).unwrap());
    check(|provider| {
        CsvEncoderConfig::new((&CsvWriterConfig::default()).into(), &provider.resources()).unwrap()
    });
}

fn allocation_probe<T>(fail_next: bool, operation: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FAIL_NEXT.with(|fail| fail.set(false));
            ALLOCATIONS_LEFT.with(|remaining| remaining.set(None));
            ALLOCATIONS.with(|count| count.set(None));
        }
    }
    let reset = Reset;
    ALLOCATIONS.with(|count| count.set(Some(0)));
    FAIL_NEXT.with(|fail| fail.set(fail_next));
    let result = operation();
    let allocations = ALLOCATIONS.with(|count| count.get().unwrap());
    drop(reset);
    (result, allocations)
}

#[test]
fn memory_resource_refusal_does_not_allocate_an_error() {
    for allocation_failure in [false, true] {
        let limit = if allocation_failure { 128 * 1024 } else { 1 };
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(limit).unwrap());
        let mut storage =
            clinker_format::preparation::MemoryStorage::new(provider.resources().scope().unwrap());
        let (result, allocations) = allocation_probe(allocation_failure, || storage.write(b"x"));
        assert!(result.is_err());
        assert_eq!(allocations, usize::from(allocation_failure));
        assert!(result.unwrap_err().get_ref().is_none());
        assert_eq!(storage.len(), 0);
        assert_eq!(provider.used(), 0);
        let expected = ResourceError::new(
            if allocation_failure {
                ResourceErrorKind::Allocation
            } else {
                ResourceErrorKind::Budget
            },
            clinker_format::preparation::STAGE_CHUNK_BYTES,
            usize::from(!allocation_failure),
        );
        assert_eq!(storage.failure(), Some(expected));
        let (retry, allocations) = allocation_probe(false, || storage.write(b"retry"));
        assert!(retry.is_err());
        assert_eq!(allocations, 0);
        assert_eq!(storage.seal(), Err(expected));
    }
}

#[test]
fn csv_box_allocation_refusal_releases_consumed_owners_without_effects() {
    use clinker_format::csv::writer::{CsvEncoder, CsvHeaderCapture, CsvWriterConfig};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Destination(Arc<[AtomicUsize; 3]>);
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0[0].fetch_add(1, Ordering::Relaxed);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.0[1].fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    impl Drop for Destination {
        fn drop(&mut self) {
            self.0[2].fetch_add(1, Ordering::Relaxed);
        }
    }

    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
    let mut encoder = CsvEncoder::new(
        csv_schema(),
        &CsvWriterConfig::default(),
        provider.resources(),
    )
    .unwrap()
    .with_header_capture(capture);
    encoder
        .set_preset_header(&["first".into(), "last".into()])
        .unwrap();
    assert!(provider.used() > 0);
    let effects = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let destination = Destination(effects.clone());
    let (result, allocations) = allocation_probe(true, || {
        encoder.into_boxed_writer(destination, provider.resources())
    });
    let FormatError::Resource(error) = result.err().expect("box allocation must be refused") else {
        panic!("allocation failure must retain typed resource evidence");
    };
    assert_eq!(error.kind, ResourceErrorKind::Allocation);
    assert!(error.requested > 0);
    assert_eq!(error.available, 0);
    assert_eq!(
        allocations, 1,
        "only the refused box allocation is attempted"
    );
    assert_eq!(effects[0].load(Ordering::Relaxed), 0, "no writes");
    assert_eq!(effects[1].load(Ordering::Relaxed), 0, "no flush");
    assert_eq!(effects[2].load(Ordering::Relaxed), 1, "destination dropped");
    assert_eq!(provider.used(), 0, "all consumed grants released");
}

#[test]
fn csv_first_capture_operation_has_no_late_mutex_allocation() {
    use clinker_format::csv::writer::{CsvEncoder, CsvHeaderCapture, CsvWriterConfig};
    use clinker_record::{Record, Schema};

    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(Vec::new())));
    let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
    let config = CsvWriterConfig {
        include_header: false,
        ..Default::default()
    };
    let mut encoder = CsvEncoder::new(schema.clone(), &config, provider.resources())
        .unwrap()
        .with_header_capture(capture);
    let record = Record::new(schema, Vec::new());
    let scope = provider.resources().scope().unwrap();
    let retained = provider.used();
    let (result, allocations) = allocation_probe(false, || {
        // The CSV writer creates one fixed 8,192-byte buffer before acquiring
        // the capture lock. Empty columns/header and io::sink need no other
        // allocations, so every later allocation is refused through commit.
        ALLOCATIONS_LEFT.with(|remaining| remaining.set(Some(1)));
        let pending = encoder.prepare(
            OutputOperation::Record(&record),
            &mut std::io::sink(),
            &scope,
        )?;
        encoder.commit(pending);
        Ok::<_, FormatError>(())
    });
    result.unwrap();
    assert_eq!(allocations, 1, "only the fixed CSV buffer is allocated");
    assert_eq!(provider.used(), retained);
    drop(encoder);
    drop(scope);
    assert_eq!(provider.used(), 0);
}

#[test]
fn first_memory_grant_uses_only_startup_allocations() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024).unwrap());
    let scope = provider.resources().scope().unwrap();
    let (grant, allocations) = allocation_probe(false, || {
        scope.reserve(std::alloc::Layout::from_size_align(1, 1).unwrap())
    });
    assert_eq!(allocations, 0);
    assert_eq!(provider.used(), 1);
    drop(grant.unwrap());
    assert_eq!(provider.used(), 0);
}

#[test]
fn storage_recovers_exact_inline_evidence_on_write_flush_and_readback() {
    #[derive(Clone, Copy)]
    enum Boundary {
        Write,
        Flush,
        Read,
    }
    struct FailingStorage {
        boundary: Boundary,
        failed: bool,
        evidence: ResourceError,
    }
    impl Write for FailingStorage {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if matches!(self.boundary, Boundary::Write) {
                self.failed = true;
                Err(std::io::ErrorKind::Other.into())
            } else {
                Ok(bytes.len())
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.failed = true;
            Err(std::io::ErrorKind::Other.into())
        }
    }
    impl std::io::Read for FailingStorage {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            self.failed = true;
            Err(std::io::ErrorKind::Other.into())
        }
    }
    impl StageStorage for FailingStorage {
        fn failure(&self) -> Option<ResourceError> {
            self.failed.then_some(self.evidence)
        }
        fn seal(&mut self) -> Result<u64, ResourceError> {
            Ok(1)
        }
        fn complete(&mut self) -> Result<(), ResourceError> {
            Ok(())
        }
    }
    let evidence = ResourceError {
        kind: ResourceErrorKind::Cancelled,
        requested: 91,
        available: 17,
        field: Some(3),
        offset: Some(41),
    };
    for boundary in [Boundary::Write, Boundary::Flush, Boundary::Read] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
        let mut stage = StorageStage::create(
            provider.resources().scope().unwrap(),
            FailingStorage {
                boundary,
                failed: false,
                evidence,
            },
        )
        .unwrap();
        match boundary {
            Boundary::Write => {
                assert!(stage.write(b"x").is_err());
                assert_eq!(stage.failure(), Some(evidence));
                assert!(
                    matches!(stage.finish(), Err(FormatError::Resource(error)) if error == evidence)
                );
            }
            Boundary::Flush => {
                assert!(stage.flush().is_err());
                assert_eq!(stage.failure(), Some(evidence));
                assert!(
                    matches!(stage.finish(), Err(FormatError::Resource(error)) if error == evidence)
                );
            }
            Boundary::Read => {
                let mut output = Vec::new();
                let (result, allocations) =
                    allocation_probe(false, || stage.finish().unwrap().deliver(&mut output));
                assert_eq!(allocations, 0);
                assert!(matches!(result, Err(FormatError::Resource(error)) if error == evidence));
                assert!(output.is_empty());
            }
        }
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn memory_allocator_failure_reaches_prepared_writer_without_error_allocation() {
    struct FailDuringEncode;
    impl FormatEncoder for FailDuringEncode {
        type Pending = ();
        fn prepare(
            &self,
            _: OutputOperation<'_>,
            stage: &mut dyn Write,
            _: &WriterScope,
        ) -> Result<(), FormatError> {
            let (result, allocations) = allocation_probe(true, || stage.write_all(b"x"));
            assert_eq!(
                allocations, 1,
                "only the refused chunk allocation is attempted"
            );
            result?;
            Ok(())
        }
        fn commit(&mut self, _: ()) {
            panic!("failed preparation cannot commit");
        }
    }
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let mut writer =
        PreparedWriter::new(Vec::new(), FailDuringEncode, provider.resources()).unwrap();
    let error = writer
        .write_operation(OutputOperation::Finalize)
        .unwrap_err();
    assert!(
        matches!(error, FormatError::Resource(error) if error.kind == clinker_format::preparation::ResourceErrorKind::Allocation)
    );
    assert!(writer.destination().is_empty());
    assert_eq!(provider.used(), 0);
}

struct Encoder {
    committed: usize,
    reject: bool,
}

mod cancellation_harness {
    use super::*;
    use clinker_format::preparation::{
        AllocationAuthority, AllocationLease, MemoryStorage, OperationStage, OwnerId,
        ResourceAuthority,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    pub(super) struct Authority {
        pub(super) memory: MemoryOnlyResources,
        pub(super) cancelled: Arc<AtomicBool>,
        pub(super) cancel_after_seal: bool,
    }
    struct CancelAfterSeal {
        storage: MemoryStorage,
        pub(super) cancelled: Arc<AtomicBool>,
    }
    impl Write for CancelAfterSeal {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.storage.write(bytes)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.storage.flush()
        }
    }
    impl std::io::Read for CancelAfterSeal {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.storage.read(bytes)
        }
    }
    impl StageStorage for CancelAfterSeal {
        fn resource_failed(&mut self, error: ResourceError) {
            self.storage.resource_failed(error);
        }
        fn failure(&self) -> Option<ResourceError> {
            self.storage.failure()
        }
        fn seal(&mut self) -> Result<u64, ResourceError> {
            let len = self.storage.seal()?;
            self.cancelled.store(true, Ordering::SeqCst);
            Ok(len)
        }
        fn complete(&mut self) -> Result<(), ResourceError> {
            self.storage.complete()
        }
    }
    impl AllocationAuthority for Authority {
        fn identity(&self) -> usize {
            self.memory.resources().allocation().identity()
        }
        fn try_reserve(
            self: Arc<Self>,
            owner: OwnerId,
            layout: std::alloc::Layout,
        ) -> Result<AllocationLease, ResourceError> {
            self.check_cancelled()?;
            self.memory.resources().allocation().reserve(owner, layout)
        }
        fn release(&self, _: OwnerId, _: usize) {
            unreachable!("grants belong to the delegated memory authority")
        }
        fn check_cancelled(&self) -> Result<(), ResourceError> {
            if self.cancelled.load(Ordering::SeqCst) {
                Err(ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
            } else {
                Ok(())
            }
        }
    }
    impl ResourceAuthority for Authority {
        fn create_stage(
            self: Arc<Self>,
            scope: WriterScope,
        ) -> Result<OperationStage, FormatError> {
            let storage = MemoryStorage::new(scope.clone());
            if self.cancel_after_seal {
                StorageStage::create(
                    scope,
                    CancelAfterSeal {
                        storage,
                        cancelled: self.cancelled.clone(),
                    },
                )
            } else {
                StorageStage::create(scope, storage)
            }
        }
    }
    pub(super) struct CancelOnWrite {
        pub(super) cancelled: Arc<AtomicBool>,
        pub(super) bytes: Vec<u8>,
        pub(super) attempts: usize,
    }
    impl Write for CancelOnWrite {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.attempts += 1;
            self.bytes.extend_from_slice(bytes);
            self.cancelled.store(true, Ordering::SeqCst);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("cancelled delivery must not flush")
        }
    }
}

fn cancelled_delivery_never_commits(cancel_after_seal: bool) {
    use cancellation_harness::{Authority, CancelOnWrite};
    use clinker_format::preparation::WriterResources;
    use std::sync::{Arc, atomic::AtomicBool};

    struct ProbeEncoder {
        empty: bool,
        prepared: std::cell::Cell<usize>,
        committed: usize,
    }
    impl FormatEncoder for ProbeEncoder {
        type Pending = ();
        fn prepare(
            &self,
            _: OutputOperation<'_>,
            stage: &mut dyn Write,
            _: &WriterScope,
        ) -> Result<(), FormatError> {
            self.prepared.set(self.prepared.get() + 1);
            if !self.empty {
                stage.write_all(b"sealed")?;
            }
            Ok(())
        }
        fn commit(&mut self, _: ()) {
            self.committed += 1;
        }
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let authority = Arc::new(Authority {
        memory: MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap()),
        cancelled: cancelled.clone(),
        cancel_after_seal,
    });
    let scope = WriterResources::new(authority.clone()).scope().unwrap();
    let mut lease = scope.reserve(std::alloc::Layout::new::<u64>()).unwrap();
    assert_eq!(lease.owner(), scope.owner());
    lease.transfer(scope.allocation()).unwrap();
    drop(lease);
    let mut writer = PreparedWriter::new(
        CancelOnWrite {
            cancelled,
            bytes: Vec::new(),
            attempts: 0,
        },
        ProbeEncoder {
            empty: cancel_after_seal,
            prepared: std::cell::Cell::new(0),
            committed: 0,
        },
        WriterResources::new(authority.clone()),
    )
    .unwrap();
    assert!(
        matches!(writer.write_operation(OutputOperation::Finalize), Err(FormatError::Resource(error)) if error == ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
    );
    for result in [
        writer.write_operation(OutputOperation::Finalize),
        writer.flush(),
        writer.flush_bytes(),
    ] {
        assert!(
            matches!(result, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::DeliveryPoisoned)
        );
    }
    assert_eq!(writer.encoder().committed, 0);
    assert_eq!(writer.encoder().prepared.get(), 1);
    assert_eq!(
        writer.destination().attempts,
        usize::from(!cancel_after_seal)
    );
    assert_eq!(
        writer.destination().bytes.as_slice(),
        if cancel_after_seal {
            b"".as_slice()
        } else {
            b"sealed".as_slice()
        }
    );
    assert_eq!(authority.memory.used(), 0);
}

#[test]
fn empty_delivery_cancellation_prevents_commit_and_continuation() {
    cancelled_delivery_never_commits(true);
}

#[test]
fn final_write_cancellation_prevents_commit_and_continuation() {
    cancelled_delivery_never_commits(false);
}

fn cancelled_csv_header_remains_unpublished(cancel_after_seal: bool) {
    use cancellation_harness::{Authority, CancelOnWrite};
    use clinker_format::csv::writer::{CsvEncoder, CsvHeaderCapture, CsvWriterConfig};
    use clinker_format::preparation::WriterResources;
    use clinker_format::reserved::ReservedText;
    use clinker_record::{Record, Schema, Value};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    for previously_published in [false, true] {
        let cancelled = Arc::new(AtomicBool::new(false));
        let authority = Arc::new(Authority {
            memory: MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap()),
            cancelled: cancelled.clone(),
            cancel_after_seal,
        });
        // The non-cancelling view uses the identical finite allocation ledger.
        let ordinary = authority.memory.resources();
        let capture = CsvHeaderCapture::new(&ordinary).unwrap();
        let empty_charge = authority.memory.used();
        let alias = capture.clone();
        assert_eq!(authority.memory.used(), empty_charge);
        let record = Record::new(csv_schema(), vec![Value::Integer(1), Value::Integer(2)]);
        if previously_published {
            let encoder =
                CsvEncoder::new(csv_schema(), &CsvWriterConfig::default(), ordinary.clone())
                    .unwrap()
                    .with_header_capture(capture.clone());
            let mut writer = PreparedWriter::new(Vec::new(), encoder, ordinary.clone()).unwrap();
            writer
                .write_operation(OutputOperation::Record(&record))
                .unwrap();
            assert_eq!(writer.destination(), b"first,last\n1,2\n");
        }
        let published_charge = authority.memory.used();
        let names_charge = std::alloc::Layout::array::<ReservedText>(2).unwrap().size()
            + "first".len()
            + "last".len();
        assert_eq!(
            published_charge,
            empty_charge + usize::from(previously_published) * names_charge
        );
        let resources = WriterResources::new(authority.clone());
        let encoder = CsvEncoder::new(csv_schema(), &CsvWriterConfig::default(), resources.clone())
            .unwrap()
            .with_header_capture(capture.clone());
        let mut writer = PreparedWriter::new(
            CancelOnWrite {
                cancelled: cancelled.clone(),
                bytes: Vec::new(),
                attempts: 0,
            },
            encoder,
            resources,
        )
        .unwrap();
        let retained = authority.memory.used();
        assert!(
            matches!(writer.write_operation(OutputOperation::Record(&record)),
            Err(FormatError::Resource(error))
                if error == ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
        );
        assert!(
            cancelled.load(Ordering::SeqCst),
            "the seal/write cancellation point was reached"
        );
        assert_eq!(
            authority.memory.used(),
            retained,
            "pending names and stage must be released"
        );
        for result in [
            writer.write_operation(OutputOperation::Record(&record)),
            writer.flush(),
            writer.flush_bytes(),
        ] {
            assert!(matches!(result, Err(FormatError::Resource(error))
                if error.kind == ResourceErrorKind::DeliveryPoisoned));
        }
        assert_eq!(
            writer.destination().attempts,
            usize::from(!cancel_after_seal)
        );
        assert_eq!(
            writer.destination().bytes.as_slice(),
            if cancel_after_seal {
                b"".as_slice()
            } else {
                b"first,last\n1,2\n".as_slice()
            }
        );
        drop(writer);
        assert_eq!(authority.memory.used(), published_charge);
        drop(capture);
        assert_eq!(
            authority.memory.used(),
            published_charge,
            "the alias retains real capture storage"
        );

        // A fresh encoder must see only committed names, even if the cancelled
        // destination accepted the entire first row and header.
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["new".into(), "names".into()])));
        let encoder = CsvEncoder::new(
            schema.clone(),
            &CsvWriterConfig::default(),
            ordinary.clone(),
        )
        .unwrap()
        .with_header_capture(alias.clone());
        let mut replay = PreparedWriter::new(Vec::new(), encoder, ordinary).unwrap();
        replay
            .write_operation(OutputOperation::Record(&Record::new(
                schema,
                vec![Value::Integer(3), Value::Integer(4)],
            )))
            .unwrap();
        assert_eq!(
            replay.destination().as_slice(),
            if previously_published {
                b"first,last\n3,4\n".as_slice()
            } else {
                b"new,names\n3,4\n".as_slice()
            }
        );
        drop(replay);
        let replay_names = if previously_published {
            "firstlast".len()
        } else {
            "newnames".len()
        };
        assert_eq!(
            authority.memory.used(),
            empty_charge
                + std::alloc::Layout::array::<ReservedText>(2).unwrap().size()
                + replay_names
        );
        drop(alias);
        assert_eq!(authority.memory.used(), 0);
    }
}

#[test]
fn csv_header_cancellation_after_seal_preserves_capture_and_releases_pending_names() {
    cancelled_csv_header_remains_unpublished(true);
}

#[test]
fn csv_header_cancellation_after_final_write_preserves_capture_and_releases_pending_names() {
    cancelled_csv_header_remains_unpublished(false);
}

impl FormatEncoder for Encoder {
    type Pending = usize;
    fn prepare(
        &self,
        _: OutputOperation<'_>,
        stage: &mut dyn Write,
        _: &WriterScope,
    ) -> Result<usize, FormatError> {
        stage.write_all(b"sealed")?;
        if self.reject {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData).into());
        }
        Ok(self.committed + 1)
    }
    fn commit(&mut self, pending: usize) {
        self.committed = pending;
    }
}

#[test]
fn memory_stage_seals_and_delivers_exact_bytes() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let resources = provider.resources();
    let scope = resources.scope().unwrap();
    let mut stage = scope.stage().unwrap();
    stage.write_all(b"one operation\n").unwrap();
    let prepared = stage.finish().unwrap();
    assert_eq!(prepared.len(), 14);
    let mut destination = Vec::new();
    prepared.deliver(&mut destination).unwrap();
    assert_eq!(destination, b"one operation\n");
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_growth_reserves_old_and_new_blocks_together() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(24).unwrap());
    let scope = provider.resources().scope().unwrap();
    let mut bytes = ReservedBuffer::new(scope.allocation().clone());
    bytes.extend_from_slice(&[1; 16]).unwrap();
    assert_eq!(provider.used(), 16);
    assert!(bytes.extend_from_slice(&[2; 16]).is_err());
    assert_eq!(bytes.as_slice(), &[1; 16]);
    assert_eq!(provider.used(), 16);
    drop(bytes);
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_preparation_error_preserves_destination_and_committed_state() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let mut writer = PreparedWriter::new(
        Vec::new(),
        Encoder {
            committed: 0,
            reject: true,
        },
        provider.resources(),
    )
    .unwrap();
    assert!(writer.write_operation(OutputOperation::Finalize).is_err());
    assert!(writer.destination().is_empty());
    assert_eq!(writer.encoder().committed, 0);
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_delivery_failure_poisons_and_never_commits_or_retries() {
    struct Fail {
        attempts: usize,
        bytes: Vec<u8>,
    }
    impl Write for Fail {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.attempts += 1;
            if self.attempts > 1 {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            self.bytes.push(bytes[0]);
            Ok(1)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("poisoned writer must not flush")
        }
    }
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let mut writer = PreparedWriter::new(
        Fail {
            attempts: 0,
            bytes: Vec::new(),
        },
        Encoder {
            committed: 0,
            reject: false,
        },
        provider.resources(),
    )
    .unwrap();
    assert!(writer.write_operation(OutputOperation::Finalize).is_err());
    assert!(writer.flush().is_err());
    assert!(writer.write_operation(OutputOperation::Finalize).is_err());
    assert_eq!(writer.encoder().committed, 0);
    assert_eq!(writer.destination().attempts, 2);
    assert_eq!(writer.destination().bytes, b"s");
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_alignment_partial_initialization_and_destructor_panic_release() {
    #[repr(align(256))]
    struct Aligned;
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(4096).unwrap());
    let scope = provider.resources().scope().unwrap();
    let mut aligned = ReservedVec::new(scope.allocation().clone());
    aligned.push(Aligned).unwrap();
    assert_eq!(aligned.as_slice().as_ptr() as usize % 256, 0);
    assert_eq!(provider.used(), 0, "aligned ZST needs no allocation");
    struct Drops(std::sync::Arc<std::sync::atomic::AtomicUsize>, bool);
    impl Drop for Drops {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(!self.1, "injected destructor panic");
        }
    }
    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut values = ReservedVec::new(scope.allocation().clone());
    values.reserve_exact(8).unwrap();
    values.push(Drops(count.clone(), true)).unwrap();
    values.push(Drops(count.clone(), false)).unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(values))).is_err());
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_failed_stage_cannot_seal_and_grants_move_split_merge() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let scope = provider.resources().scope().unwrap();
    let mut grant = scope
        .reserve(std::alloc::Layout::from_size_align(100, 1).unwrap())
        .unwrap();
    let part = grant.split(40).unwrap();
    assert_eq!(provider.used(), 100);
    grant.merge(part).unwrap();
    drop(grant);
    assert_eq!(provider.used(), 0);
    let mut stage = scope.stage().unwrap();
    assert!(stage.write_all(&[1; 256 * 1024]).is_err());
    assert_eq!(
        stage.failure().unwrap().kind,
        clinker_format::preparation::ResourceErrorKind::Budget
    );
    assert!(stage.finish().is_err());
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_zero_capacity_alignment_and_finite_startup_refusal() {
    #[repr(align(256))]
    struct Aligned(u8);
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(4096).unwrap());
    let mut values = ReservedVec::new(provider.resources().allocation().scope().unwrap());
    assert!(values.as_slice().is_empty());
    values.push(Aligned(7)).unwrap();
    assert_eq!(values.as_slice().as_ptr() as usize % 256, 0);
    assert_eq!(values.as_slice()[0].0, 7);
    assert_eq!(provider.used(), 256);
    drop(values);
    assert!(provider.resources().scope().unwrap().stage().is_err());
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_finalize_commits_once_and_zero_acceptance_also_poisons() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let mut writer = PreparedWriter::new(
        Vec::new(),
        Encoder {
            committed: 0,
            reject: false,
        },
        provider.resources(),
    )
    .unwrap();
    writer.flush().unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.encoder().committed, 1);
    assert_eq!(writer.destination(), b"sealed");
    let mut writer = PreparedWriter::new(
        std::io::Cursor::new([0u8; 0]),
        Encoder {
            committed: 0,
            reject: false,
        },
        provider.resources(),
    )
    .unwrap();
    assert!(writer.flush().is_err());
    assert!(writer.flush().is_err());
    assert_eq!(writer.encoder().committed, 0);
}

#[test]
fn memory_standalone_stage_has_no_hidden_operation_byte_cap() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    let bytes = vec![42; 192 * 1024];
    stage.write_all(&bytes).unwrap();
    let mut output = Vec::new();
    stage.finish().unwrap().deliver(&mut output).unwrap();
    assert_eq!(output, bytes);
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_many_appends_have_geometric_growth_and_exact_fallback() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let mut bytes = ReservedBuffer::new(provider.resources().allocation().scope().unwrap());
    let mut replacements = 0;
    let mut capacity = 0;
    for _ in 0..10000 {
        bytes.extend_from_slice(b"x").unwrap();
        if bytes.capacity() != capacity {
            replacements += 1;
            capacity = bytes.capacity();
        }
    }
    assert!(
        replacements <= 15,
        "linear append workload must not cause linear reallocations"
    );
    let small = MemoryOnlyResources::new(NonZeroUsize::new(9).unwrap());
    let mut bytes = ReservedBuffer::new(small.resources().allocation().scope().unwrap());
    bytes.extend_from_slice(b"1234").unwrap();
    bytes.extend_from_slice(b"5").unwrap();
    assert_eq!(bytes.capacity(), 5);
    assert_eq!(bytes.as_slice(), b"12345");
}

#[test]
fn csv_rejected_cell_never_reaches_destructor_stage_io() {
    use clinker_format::charset::Charset;
    use clinker_format::csv::writer::{
        CsvEncoder, CsvEncoderConfig, CsvEncoderOptions, CsvWriterConfig,
    };
    use clinker_format::error::OutputEncodingKind;
    use clinker_format::preparation::{
        AllocationAuthority, AllocationLease, OperationStage, OwnerId, ResourceAuthority,
        WriterResources,
    };
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Value};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Authority {
        memory: MemoryOnlyResources,
        calls: Arc<AtomicUsize>,
        evidence: ResourceError,
    }
    impl AllocationAuthority for Authority {
        fn identity(&self) -> usize {
            self.memory.resources().allocation().identity()
        }
        fn try_reserve(
            self: Arc<Self>,
            owner: OwnerId,
            layout: std::alloc::Layout,
        ) -> Result<AllocationLease, ResourceError> {
            self.memory.resources().allocation().reserve(owner, layout)
        }
        fn release(&self, _: OwnerId, _: usize) {
            unreachable!("delegated memory owns grants")
        }
        fn check_cancelled(&self) -> Result<(), ResourceError> {
            Ok(())
        }
    }
    struct RefusingStage {
        calls: Arc<AtomicUsize>,
        evidence: ResourceError,
        failed: bool,
    }
    impl Write for RefusingStage {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.failed = true;
            Err(std::io::ErrorKind::Other.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.failed = true;
            Err(std::io::ErrorKind::Other.into())
        }
    }
    impl std::io::Read for RefusingStage {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("rejected encoding or failed stage cannot read back")
        }
    }
    impl StageStorage for RefusingStage {
        fn failure(&self) -> Option<ResourceError> {
            self.failed.then_some(self.evidence)
        }
        fn seal(&mut self) -> Result<u64, ResourceError> {
            panic!("rejected encoding or failed stage cannot finish")
        }
        fn complete(&mut self) -> Result<(), ResourceError> {
            panic!("rejected encoding or failed stage cannot complete")
        }
    }
    impl ResourceAuthority for Authority {
        fn create_stage(
            self: Arc<Self>,
            scope: WriterScope,
        ) -> Result<OperationStage, FormatError> {
            StorageStage::create(
                scope,
                RefusingStage {
                    calls: self.calls.clone(),
                    evidence: self.evidence,
                    failed: false,
                },
            )
        }
    }

    for kind in [
        ResourceErrorKind::Cancelled,
        ResourceErrorKind::Budget,
        ResourceErrorKind::Storage,
    ] {
        for charset_failure in [false, true] {
            for operation in ["record", "headerless", "opening", "closing"] {
                let calls = Arc::new(AtomicUsize::new(0));
                let authority = Arc::new(Authority {
                    memory: MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap()),
                    calls: calls.clone(),
                    evidence: ResourceError::new(kind, 91, 17),
                });
                let resources = WriterResources::new(authority.clone());
                let schema = csv_schema();
                let config = CsvWriterConfig {
                    include_header: operation != "headerless",
                    envelope: matches!(operation, "opening" | "closing").then(|| {
                        clinker_format::OutputEnvelopeSpec {
                            header_from_doc: Some("opening".into()),
                            footer_from_doc: Some("closing".into()),
                            ..Default::default()
                        }
                    }),
                    ..Default::default()
                };
                let mut options = CsvEncoderOptions::from(&config);
                if charset_failure {
                    options.charset = Charset::Latin1;
                }
                let policy = CsvEncoderConfig::new(options, &resources).unwrap();
                let encoder =
                    CsvEncoder::from_config(schema.clone(), policy, resources.clone()).unwrap();
                let mut writer = PreparedWriter::new(Vec::new(), encoder, resources).unwrap();
                let before = authority.memory.used();
                let bad = if charset_failure {
                    Value::String("€".into())
                } else {
                    Value::Array(OwnedValues::from_vec(vec![]))
                };
                let record = Record::new(
                    schema.clone(),
                    vec![Value::String("buffered prefix".into()), bad.clone()],
                );
                let section = || {
                    Value::Map(OwnedMap::from_map(
                        [
                            ("first".into(), Value::String("buffered prefix".into())),
                            ("last".into(), bad.clone()),
                        ]
                        .into(),
                    ))
                };
                let doc = DocumentContext::new(
                    DocumentId::next(),
                    Arc::from("fixture.csv"),
                    EnvelopeRecord::from_sections([
                        ("opening".into(), section()),
                        ("closing".into(), section()),
                    ]),
                );
                let result = writer.write_operation(match operation {
                    "opening" => OutputOperation::BeginDocument(&doc),
                    "closing" => OutputOperation::EndDocument(&doc),
                    _ => OutputOperation::Record(&record),
                });
                assert!(
                    matches!(result, Err(FormatError::OutputEncoding { kind: actual, field: 2, .. })
                    if actual == if charset_failure { OutputEncodingKind::Charset } else { OutputEncodingKind::Array }),
                    "{operation}, {kind:?}, charset={charset_failure}: {result:?}"
                );
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    0,
                    "rejection must not flush a buffered prefix"
                );
                assert!(writer.destination().is_empty());
                assert_eq!(authority.memory.used(), before);

                // An actual stage failure while preparing a valid operation
                // must retain its complete resource evidence.
                let valid = Record::new(schema, vec![Value::Integer(1), Value::Integer(2)]);
                let result = writer.write_operation(OutputOperation::Record(&valid));
                assert!(
                    matches!(result, Err(FormatError::Resource(error)) if error == authority.evidence)
                );
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    1,
                    "no destructor retry after a real stage failure"
                );
                assert!(writer.destination().is_empty());
                assert_eq!(authority.memory.used(), before);
                drop(writer);
                assert_eq!(authority.memory.used(), 0);
            }
        }
    }
}

mod boxed_owner_lifetimes {
    use super::*;
    use clinker_format::splitting::WriterFactory;
    use clinker_format::{FormatWriter, FormatWriterHandle};
    use std::alloc::Layout;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct Watch<'a>(&'a MemoryOnlyResources);
    impl<'a> Watch<'a> {
        fn new(provider: &'a MemoryOnlyResources, layout: Layout) -> Self {
            assert_eq!(
                provider.used(),
                0,
                "no unrelated grants may mask early release"
            );
            BACKING_WATCH.with(|watch| {
                assert!(watch.get().is_none());
                watch.set(Some(BackingWatch {
                    provider,
                    pointer: std::ptr::null_mut(),
                    bytes: 0,
                    live_at_deallocation: None,
                    live_at_allocation: None,
                    layout: Some(layout),
                    deallocations: 0,
                }));
            });
            Self(provider)
        }
        fn with_existing_grants(provider: &'a MemoryOnlyResources, layout: Layout) -> Self {
            Self::arm(provider, layout);
            Self(provider)
        }
        fn deferred(provider: &'a MemoryOnlyResources) -> Self {
            assert!(BACKING_WATCH.with(|watch| watch.get().is_none()));
            Self(provider)
        }
        // A deferred guard must already retain this provider when a real CSV
        // factory arms observation immediately before its final writer box.
        fn arm(provider: &MemoryOnlyResources, layout: Layout) {
            BACKING_WATCH.with(|watch| {
                assert!(watch.get().is_none());
                watch.set(Some(BackingWatch {
                    provider,
                    pointer: std::ptr::null_mut(),
                    bytes: 0,
                    live_at_deallocation: None,
                    live_at_allocation: None,
                    layout: Some(layout),
                    deallocations: 0,
                }));
            });
        }
        fn admitted(&self, layout: Layout, prior_grants: usize) {
            let state = BACKING_WATCH.with(|watch| watch.get().unwrap());
            assert_eq!(state.bytes, layout.size());
            assert_eq!(
                state.live_at_allocation,
                Some(prior_grants + layout.size()),
                "the concrete backing must already be admitted when allocation begins"
            );
        }
        fn deallocated_with(&self, layout: Layout, remaining_grants: usize) {
            let state = BACKING_WATCH.with(|watch| watch.get().unwrap());
            assert_eq!(state.bytes, layout.size());
            assert_eq!(state.deallocations, 1);
            assert_eq!(
                state.live_at_deallocation,
                Some(remaining_grants + layout.size()),
                "surviving aliases or wrappers cannot mask premature backing release"
            );
        }
        fn live(&self, layout: Layout) {
            let state = BACKING_WATCH.with(|watch| watch.get().unwrap());
            assert!(!state.pointer.is_null());
            assert_eq!(state.bytes, layout.size());
            assert_eq!(state.deallocations, 0);
            assert_eq!(state.live_at_deallocation, None);
        }
        fn released(&self, layout: Layout) {
            let state = BACKING_WATCH.with(|watch| watch.get().unwrap());
            assert_eq!(state.bytes, layout.size());
            assert_eq!(state.deallocations, 1);
            assert_eq!(
                state.live_at_deallocation,
                Some(layout.size()),
                "only the watched backing grant must remain when deallocation runs"
            );
            assert_eq!(self.0.used(), 0);
        }
    }
    impl Drop for Watch<'_> {
        fn drop(&mut self) {
            BACKING_WATCH.with(|watch| watch.set(None));
        }
    }

    use clinker_format::counting::{CountedFormatWriter, CountingWriter, SharedByteCounter};
    use clinker_format::csv::writer::{
        CsvEncoder, CsvEncoderConfig, CsvHeaderCapture, CsvWriterConfig,
    };
    use clinker_format::splitting::{OversizeGroupPolicy, SplitPolicy, SplittingWriter};
    use clinker_record::{Record, Value};
    use std::sync::Mutex;

    #[derive(Clone, Default)]
    struct CsvDestination {
        output: Arc<Mutex<(Vec<u8>, usize)>>,
        fail_after: Option<usize>,
    }
    impl Write for CsvDestination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let mut output = self.output.lock().unwrap();
            output.1 += 1;
            let len = match self.fail_after {
                Some(limit) if output.0.len() >= limit => {
                    return Err(std::io::ErrorKind::BrokenPipe.into());
                }
                Some(limit) => bytes.len().min(limit - output.0.len()),
                None => bytes.len(),
            };
            output.0.extend_from_slice(&bytes[..len]);
            Ok(len)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn csv_row(first: i64, last: i64) -> Record {
        Record::new(
            csv_schema(),
            vec![Value::Integer(first), Value::Integer(last)],
        )
    }

    #[test]
    fn concrete_csv_writer_backing_is_charged_through_normal_and_poisoned_drop() {
        for poisoned in [false, true] {
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
            let resources = provider.resources();
            let config =
                CsvEncoderConfig::new((&CsvWriterConfig::default()).into(), &resources).unwrap();
            let capture = CsvHeaderCapture::new(&resources).unwrap();
            let encoder = CsvEncoder::from_config(csv_schema(), config.clone(), resources.clone())
                .unwrap()
                .with_header_capture(capture.clone());
            let destination = CsvDestination {
                fail_after: poisoned.then_some(4),
                ..Default::default()
            };
            let output = destination.output.clone();
            let layout = Layout::new::<PreparedWriter<CsvDestination, CsvEncoder>>();
            let prior = provider.used();
            let watch = Watch::with_existing_grants(&provider, layout);
            let (writer, allocations) = allocation_probe(false, || {
                encoder.into_boxed_writer(destination, resources.clone())
            });
            assert_eq!(allocations, 1);
            let mut writer = writer.unwrap();
            watch.admitted(layout, prior);
            let result = writer.write_record(&csv_row(1, 2));
            if poisoned {
                assert!(result.is_err());
                let attempts = output.lock().unwrap().1;
                assert!(
                    matches!(writer.write_record(&csv_row(3, 4)), Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::DeliveryPoisoned)
                );
                assert_eq!(
                    output.lock().unwrap().1,
                    attempts,
                    "poisoned continuation cannot retry the destination"
                );
                assert_eq!(output.lock().unwrap().0, b"firs");
            } else {
                result.unwrap();
                writer.flush().unwrap();
                assert_eq!(output.lock().unwrap().0, b"first,last\n1,2\n");
            }
            watch.live(layout);
            drop(writer);
            let retained_aliases = provider.used();
            assert!(retained_aliases > 0);
            watch.deallocated_with(layout, retained_aliases);
            drop(watch);
            // These aliases remain usable after the real writer box is gone.
            let replay = CsvEncoder::from_config(csv_schema(), config.clone(), resources.clone())
                .unwrap()
                .with_header_capture(capture.clone());
            let mut replay = PreparedWriter::new(Vec::new(), replay, resources).unwrap();
            replay.write_record(&csv_row(3, 4)).unwrap();
            assert_eq!(replay.destination(), b"first,last\n3,4\n");
            drop(replay);
            drop(config);
            drop(capture);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn concrete_csv_factory_backing_preserves_surviving_config_and_capture_aliases() {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
        let resources = provider.resources();
        let scope = resources.allocation().scope().unwrap();
        let config =
            CsvEncoderConfig::new((&CsvWriterConfig::default()).into(), &resources).unwrap();
        let capture = CsvHeaderCapture::new(&resources).unwrap();
        let policy = config.clone();
        let headers = capture.clone();
        let factory_resources = resources.clone();
        let make = move |destination, schema| {
            CsvEncoder::from_config(schema, policy.clone(), factory_resources.clone())?
                .with_header_capture(headers.clone())
                .into_boxed_writer(destination, factory_resources.clone())
        };
        let layout = Layout::for_value(&make);
        let prior = provider.used();
        let watch = Watch::with_existing_grants(&provider, layout);
        let (factory, allocations) =
            allocation_probe(false, || WriterFactory::try_new(make, &scope));
        assert_eq!(allocations, 1);
        let factory = factory.unwrap();
        watch.admitted(layout, prior);
        let destination = CsvDestination::default();
        let output = destination.output.clone();
        let raw: Box<dyn Write + Send> = Box::new(destination);
        let mut writer = factory
            .create(
                CountingWriter::new(raw, SharedByteCounter::new()),
                csv_schema(),
            )
            .unwrap();
        writer.write_record(&csv_row(1, 2)).unwrap();
        writer.flush().unwrap();
        assert_eq!(output.lock().unwrap().0, b"first,last\n1,2\n");
        drop(writer);
        watch.live(layout);
        drop(factory);
        let retained_aliases = provider.used();
        assert!(
            retained_aliases > prior,
            "published capture names outlive the factory"
        );
        watch.deallocated_with(layout, retained_aliases);
        drop(watch);
        let encoder = CsvEncoder::from_config(csv_schema(), config.clone(), resources.clone())
            .unwrap()
            .with_header_capture(capture.clone());
        let mut writer = PreparedWriter::new(Vec::new(), encoder, resources).unwrap();
        writer.write_record(&csv_row(3, 4)).unwrap();
        assert_eq!(writer.destination(), b"first,last\n3,4\n");
        drop(writer);
        assert_eq!(provider.used(), retained_aliases);
        drop(config);
        drop(capture);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn counted_csv_outer_backing_has_its_own_admission_and_deallocation_charge() {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
        let resources = provider.resources();
        let scope = resources.allocation().scope().unwrap();
        let config =
            CsvEncoderConfig::new((&CsvWriterConfig::default()).into(), &resources).unwrap();
        let capture = CsvHeaderCapture::new(&resources).unwrap();
        let destination = CsvDestination::default();
        let output = destination.output.clone();
        let counter = SharedByteCounter::new();
        let encoder = CsvEncoder::from_config(csv_schema(), config.clone(), resources.clone())
            .unwrap()
            .with_header_capture(capture.clone());
        let inner = encoder
            .into_boxed_writer(CountingWriter::new(destination, counter.clone()), resources)
            .unwrap();
        let value = CountedFormatWriter::new(inner, counter);
        let layout = Layout::new::<CountedFormatWriter>();
        let prior = provider.used();
        let watch = Watch::with_existing_grants(&provider, layout);
        let (owner, allocations) =
            allocation_probe(false, || FormatWriterHandle::try_new(value, &scope));
        assert_eq!(allocations, 1);
        let mut owner = owner.unwrap();
        watch.admitted(layout, prior);
        owner.write_record(&csv_row(1, 2)).unwrap();
        owner.flush().unwrap();
        assert_eq!(owner.bytes_written(), Some(15));
        assert_eq!(output.lock().unwrap().0, b"first,last\n1,2\n");
        drop(owner);
        let retained_aliases = provider.used();
        watch.deallocated_with(layout, retained_aliases);
        drop(watch);
        drop(config);
        drop(capture);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn splitting_csv_outer_backing_and_rotated_writer_release_remain_distinct() {
        for observe_inner in [false, true] {
            let provider = Arc::new(MemoryOnlyResources::new(
                NonZeroUsize::new(512 * 1024).unwrap(),
            ));
            let resources = provider.resources();
            let scope = resources.allocation().scope().unwrap();
            let config =
                CsvEncoderConfig::new((&CsvWriterConfig::default()).into(), &resources).unwrap();
            let capture = CsvHeaderCapture::new(&resources).unwrap();
            let policy = config.clone();
            let headers = capture.clone();
            let encoder_resources = resources.clone();
            let watcher_provider = provider.clone();
            let before_inner = Arc::new(AtomicUsize::new(0));
            let record_before_inner = before_inner.clone();
            let opened = AtomicBool::new(false);
            let inner_layout =
                Layout::new::<PreparedWriter<CountingWriter<Box<dyn Write + Send>>, CsvEncoder>>();
            let make = move |destination, schema| {
                let encoder =
                    CsvEncoder::from_config(schema, policy.clone(), encoder_resources.clone())?
                        .with_header_capture(headers.clone());
                if observe_inner && !opened.swap(true, Ordering::SeqCst) {
                    record_before_inner.store(watcher_provider.used(), Ordering::SeqCst);
                    Watch::arm(&watcher_provider, inner_layout);
                }
                encoder.into_boxed_writer(destination, encoder_resources.clone())
            };
            let factory_layout = Layout::for_value(&make);
            let factory = WriterFactory::try_new(make, &scope).unwrap();
            let outputs = [CsvDestination::default(), CsvDestination::default()];
            let files = outputs.clone();
            let rotation_baseline = Arc::new(AtomicUsize::new(0));
            let record_rotation = rotation_baseline.clone();
            let file_provider = provider.clone();
            let value = SplittingWriter::new(
                Box::new(move |sequence| {
                    if sequence == 2 {
                        // rotate_file has destroyed the old writer, and the
                        // new physical file/writer has not been created yet.
                        record_rotation.store(file_provider.used(), Ordering::SeqCst);
                    }
                    Ok(Box::new(files[sequence as usize - 1].clone()))
                }),
                factory,
                csv_schema(),
                SplitPolicy {
                    max_records: Some(1),
                    max_bytes: None,
                    group_key: None,
                    oversize_group: OversizeGroupPolicy::Error,
                },
            );
            let outer_layout = Layout::new::<SplittingWriter>();
            let prior = provider.used();
            let watch = if observe_inner {
                Watch::deferred(&provider)
            } else {
                Watch::with_existing_grants(&provider, outer_layout)
            };
            let (owner, allocations) =
                allocation_probe(false, || FormatWriterHandle::try_new(value, &scope));
            assert_eq!(allocations, 1);
            let mut owner = owner.unwrap();
            if !observe_inner {
                watch.admitted(outer_layout, prior);
            }
            owner.write_record(&csv_row(1, 2)).unwrap();
            if observe_inner {
                watch.admitted(inner_layout, before_inner.load(Ordering::SeqCst));
                watch.live(inner_layout);
            }
            owner.write_record(&csv_row(3, 4)).unwrap();
            owner.flush().unwrap();
            assert_eq!(outputs[0].output.lock().unwrap().0, b"first,last\n1,2\n");
            assert_eq!(outputs[1].output.lock().unwrap().0, b"first,last\n3,4\n");
            if observe_inner {
                let remaining = rotation_baseline.load(Ordering::SeqCst);
                assert!(remaining >= outer_layout.size() + factory_layout.size());
                watch.deallocated_with(inner_layout, remaining);
            } else {
                watch.live(outer_layout);
            }
            drop(owner);
            if !observe_inner {
                watch.deallocated_with(outer_layout, provider.used());
            }
            drop(watch);
            drop(config);
            drop(capture);
            assert_eq!(provider.used(), 0);
        }
    }

    #[repr(align(64))]
    struct Payload {
        drops: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "intentional payload-drop unwind");
        }
    }
    impl FormatWriter for Payload {
        fn write_record(&mut self, _: &clinker_record::Record) -> Result<(), FormatError> {
            Ok(())
        }
        fn flush(&mut self) -> Result<(), FormatError> {
            Ok(())
        }
    }

    #[test]
    fn writer_box_charge_survives_actual_deallocation_and_unwind() {
        for panic_on_drop in [false, true] {
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
            let scope = provider.resources().allocation().scope().unwrap();
            let drops = Arc::new(AtomicUsize::new(0));
            let payload = Payload {
                drops: drops.clone(),
                panic_on_drop,
            };
            let layout = Layout::new::<Payload>();
            let watch = Watch::new(&provider, layout);
            let (writer, allocations) =
                allocation_probe(false, || FormatWriterHandle::try_new(payload, &scope));
            assert_eq!(allocations, 1);
            let writer = writer.unwrap();
            assert_eq!(provider.used(), layout.size());
            watch.live(layout);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(writer)));
            assert_eq!(outcome.is_err(), panic_on_drop);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            watch.released(layout);
        }
    }

    #[test]
    fn factory_box_charge_uses_concrete_closure_layout_through_deallocation() {
        for panic_on_drop in [false, true] {
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
            let scope = provider.resources().allocation().scope().unwrap();
            let drops = Arc::new(AtomicUsize::new(0));
            let payload = Payload {
                drops: drops.clone(),
                panic_on_drop,
            };
            let factory = move |_, _| {
                std::hint::black_box(&payload);
                Err(FormatError::Resource(ResourceError::new(
                    ResourceErrorKind::Storage,
                    0,
                    0,
                )))
            };
            let layout = Layout::for_value(&factory);
            assert_eq!(layout.align(), 64);
            let watch = Watch::new(&provider, layout);
            let (owner, allocations) =
                allocation_probe(false, || WriterFactory::try_new(factory, &scope));
            assert_eq!(allocations, 1);
            let owner = owner.unwrap();
            assert_eq!(provider.used(), layout.size());
            watch.live(layout);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)));
            assert_eq!(outcome.is_err(), panic_on_drop);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            watch.released(layout);
        }
    }

    #[test]
    fn writer_and_factory_box_refusal_drop_payload_once_without_retained_charge() {
        for allocator_refusal in [false, true] {
            for factory in [false, true] {
                let limit = if allocator_refusal { 128 * 1024 } else { 1 };
                let provider = MemoryOnlyResources::new(NonZeroUsize::new(limit).unwrap());
                let scope = provider.resources().allocation().scope().unwrap();
                let drops = Arc::new(AtomicUsize::new(0));
                let payload = Payload {
                    drops: drops.clone(),
                    panic_on_drop: false,
                };
                let (error, allocations) = allocation_probe(allocator_refusal, || {
                    if factory {
                        let make = move |_, _| {
                            std::hint::black_box(&payload);
                            Err(FormatError::Resource(ResourceError::new(
                                ResourceErrorKind::Storage,
                                0,
                                0,
                            )))
                        };
                        WriterFactory::try_new(make, &scope).err().unwrap()
                    } else {
                        FormatWriterHandle::try_new(payload, &scope).err().unwrap()
                    }
                });
                assert_eq!(
                    error.kind,
                    if allocator_refusal {
                        ResourceErrorKind::Allocation
                    } else {
                        ResourceErrorKind::Budget
                    }
                );
                assert_eq!(allocations, usize::from(allocator_refusal));
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                assert_eq!(provider.used(), 0);
            }
        }
    }
}

#[test]
fn xml_rejected_operations_preserve_root_counts_and_cache() {
    use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
    use clinker_format::{FormatWriter, OutputEnvelopeSpec};
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Schema, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
    let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec!["value".into()])));
    let config = XmlWriterConfig {
        envelope: Some(OutputEnvelopeSpec {
            header_from_doc: Some("opening".into()),
            footer_from_doc: Some("closing".into()),
            footer_record_count_field: Some("rows".into()),
        }),
        ..Default::default()
    };
    let document = |bad| {
        DocumentContext::new(
            DocumentId::next(),
            std::sync::Arc::from("input.xml"),
            EnvelopeRecord::from_sections([
                (
                    "opening".into(),
                    Value::Map(OwnedMap::from_map(
                        [
                            ("@id".into(), Value::Integer(7)),
                            (
                                "text".into(),
                                Value::String(if bad { "bad\u{1}" } else { "begin" }.into()),
                            ),
                        ]
                        .into(),
                    )),
                ),
                (
                    "closing".into(),
                    Value::Map(OwnedMap::from_map(
                        [(
                            "text".into(),
                            Value::String(if bad { "bad\u{1}" } else { "end" }.into()),
                        )]
                        .into(),
                    )),
                ),
            ]),
        )
    };
    let good = document(false);
    let bad = document(true);
    let encoder = XmlEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    let baseline = provider.used();
    assert!(writer.begin_document(&bad).is_err());
    assert!(writer.destination().is_empty());
    assert_eq!(provider.used(), baseline);
    for _ in 0..2 {
        writer.begin_document(&good).unwrap();
        let before = writer.destination().clone();
        assert!(
            writer
                .write_record(&Record::new(
                    schema.clone(),
                    vec![Value::Array(OwnedValues::from_vec(vec![]))]
                ))
                .is_err()
        );
        assert_eq!(writer.destination(), &before);
        writer
            .write_record(&Record::new(schema.clone(), vec![Value::Integer(1)]))
            .unwrap();
        writer.flush_bytes().unwrap();
        let before = writer.destination().clone();
        assert!(writer.end_document(&bad).is_err());
        assert_eq!(writer.destination(), &before);
        writer.end_document(&good).unwrap();
    }
    writer.flush().unwrap();
    let expected = "<Document><header id=\"7\"><text>begin</text></header><Record><value>1</value></Record><footer><text>end</text><rows>1</rows></footer></Document>";
    assert_eq!(
        writer.destination(),
        format!("<Root>{expected}{expected}</Root>").as_bytes()
    );
    assert_eq!(provider.used(), baseline);
    drop(writer);
    assert_eq!(provider.used(), 0);
}

#[test]
fn xml_late_shape_errors_preserve_destination_and_admission() {
    use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
    use clinker_record::{Record, Schema, Value};
    let nested = |key: &str, value| {
        Value::Map(OwnedMap::from_map(
            [("good".into(), Value::Integer(1)), (key.into(), value)].into(),
        ))
    };
    let deep = (0..65).fold(Value::Null, |value, _| nested("next", value));
    let cases = vec![
        (vec!["value"], nested("bad name", Value::Integer(2))),
        (
            vec!["value"],
            nested("last", Value::String("bad\u{1}".into())),
        ),
        (
            vec!["value"],
            nested("@attr", Value::Array(OwnedValues::from_vec(vec![]))),
        ),
        (
            vec!["value"],
            nested("#text", Value::Map(OwnedMap::from_map(Default::default()))),
        ),
        (
            vec!["value"],
            Value::Map(OwnedMap::from_map(
                [
                    ("@id".into(), Value::Integer(1)),
                    (r"\@id".into(), Value::Integer(2)),
                ]
                .into(),
            )),
        ),
        (vec!["value"], nested(r"\invalid", Value::Null)),
        (vec!["value"], deep),
        (vec!["a", "a.b"], Value::Null),
        (vec!["a.b", "a"], Value::Null),
        (vec![r"bad\q"], Value::Null),
        (vec!["a."], Value::Null),
    ];
    for (columns, value) in cases {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
        let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(
            columns.iter().map(|s| (*s).into()).collect(),
        )));
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        let retained = provider.used();
        let error = writer
            .write_operation(OutputOperation::Record(&Record::new(
                schema,
                vec![value; columns.len()],
            )))
            .unwrap_err();
        assert!(
            matches!(error, FormatError::OutputEncoding { .. }),
            "{columns:?}: {error}"
        );
        assert!(writer.destination().is_empty());
        assert_eq!(provider.used(), retained);
        drop(writer);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn xml_cache_replacement_overlap_refusal_and_finalize_release() {
    use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
    use clinker_record::{Record, Schema, Value};
    let limit = 128 * 1024;
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(limit).unwrap());
    let scope = provider.resources().scope().unwrap();
    let schema =
        |name: &str| SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec![name.into()])));
    let first = Record::new(schema("old.value"), vec![Value::Integer(1)]);
    let next = Record::new(schema("new.value"), vec![Value::Integer(2)]);
    let mut encoder = XmlEncoder::new(
        first.schema().clone(),
        &XmlWriterConfig::default(),
        provider.resources(),
    )
    .unwrap();
    let base = provider.used();
    let pending = encoder
        .prepare(OutputOperation::Record(&first), &mut Vec::new(), &scope)
        .unwrap();
    encoder.commit(pending);
    let committed = provider.used();
    assert!(committed > base);
    let pending = encoder
        .prepare(OutputOperation::Record(&next), &mut Vec::new(), &scope)
        .unwrap();
    assert_eq!(provider.used(), committed + (committed - base));
    drop(pending);
    assert_eq!(provider.used(), committed);
    let denied = scope
        .reserve(std::alloc::Layout::array::<u8>(limit - committed).unwrap())
        .unwrap();
    assert!(matches!(
        encoder.prepare(OutputOperation::Record(&next), &mut Vec::new(), &scope),
        Err(FormatError::Resource(_))
    ));
    assert_eq!(provider.used(), limit);
    drop(denied);
    let mut bytes = Vec::new();
    let pending = encoder
        .prepare(OutputOperation::Record(&first), &mut bytes, &scope)
        .unwrap();
    encoder.commit(pending);
    assert_eq!(provider.used(), committed);
    assert_eq!(bytes, b"<Record><old><value>1</value></old></Record>");
    let pending = encoder
        .prepare(OutputOperation::Record(&next), &mut Vec::new(), &scope)
        .unwrap();
    encoder.commit(pending);
    assert_eq!(provider.used(), committed);
    let pending = encoder
        .prepare(OutputOperation::Finalize, &mut Vec::new(), &scope)
        .unwrap();
    encoder.commit(pending);
    assert_eq!(provider.used(), base);
    drop(encoder);
    assert_eq!(provider.used(), 0);
}

#[test]
fn xml_empty_finalize_is_idempotent_and_drop_does_not_finalize() {
    use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
    use clinker_record::Schema;
    for finalize in [false, true] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
        let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec![])));
        let encoder =
            XmlEncoder::new(schema, &XmlWriterConfig::default(), provider.resources()).unwrap();
        let mut bytes = Vec::new();
        let mut writer = PreparedWriter::new(&mut bytes, encoder, provider.resources()).unwrap();
        writer.flush_bytes().unwrap();
        assert!(writer.destination().is_empty());
        if finalize {
            writer.flush().unwrap();
            writer.flush().unwrap();
        }
        drop(writer);
        assert_eq!(
            bytes,
            if finalize {
                b"<Root></Root>".as_slice()
            } else {
                b""
            }
        );
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn xml_repeats_wrappers_nulls_and_configured_prefix_keep_exact_bytes() {
    use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
    use clinker_format::{
        FormatWriter,
        multi_value::{JoinValues, OnConflict},
    };
    use clinker_record::{Record, Schema, Value};
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
    let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec![
        "_id".into(),
        "A.x".into(),
        "A.y".into(),
        "tags".into(),
        r"literal\.dot".into(),
    ])));
    let config = XmlWriterConfig {
        root_element: "Batch".into(),
        record_element: "Row".into(),
        attribute_prefix: "_".into(),
        preserve_nulls: true,
        declared_multiple: ["tags".to_owned()].into(),
        join_values: vec![JoinValues {
            field: "tags".into(),
            delimiter: ",".into(),
            escape: "\\".into(),
            on_conflict: OnConflict::Error,
            repeat_as: Some("tag".into()),
            wrap_in: Some("list".into()),
        }],
        ..Default::default()
    };
    let nested = Value::Map(OwnedMap::from_map(
        [
            ("_code".into(), Value::String("a\t\n\r".into())),
            ("#text".into(), Value::String("text<&>".into())),
            (
                "item".into(),
                Value::Array(OwnedValues::from_vec(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                ])),
            ),
        ]
        .into(),
    ));
    let record = Record::new(
        schema.clone(),
        vec![
            Value::Integer(7),
            nested,
            Value::Null,
            Value::Array(OwnedValues::from_vec(vec![
                Value::String("one".into()),
                Value::String("two".into()),
            ])),
            Value::Bool(true),
        ],
    );
    let encoder = XmlEncoder::new(schema, &config, provider.resources()).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    writer.write_record(&record).unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.destination(), b"<Batch><Row id=\"7\"><A><x code=\"a&#9;&#10;&#13;\">text&lt;&amp;&gt;<item>1</item><item>2</item></x><y/></A><list><tag>one</tag><tag>two</tag></list><literal.dot>true</literal.dot></Row></Batch>");
    drop(writer);
    assert_eq!(provider.used(), 0);
}

#[test]
fn xml_schema_identity_releases_dynamic_columns_and_depth_boundary_is_exact() {
    use clinker_format::{
        FormatWriter,
        xml::writer::{XmlEncoder, XmlWriterConfig},
    };
    use clinker_record::{Record, Schema, Value};
    for depth in [64, 65] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
        let schema = std::sync::Arc::new(Schema::new(vec!["value".into()]));
        let identity = std::sync::Arc::downgrade(&schema);
        let schema = SharedStorage::from_arc(schema);
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        let value = (0..depth).fold(Value::Integer(7), |value, _| {
            Value::Map(OwnedMap::from_map([("x".into(), value)].into()))
        });
        let record = Record::new(schema, vec![value]);
        let result = writer.write_record(&record);
        drop(record);
        assert!(
            identity.upgrade().is_none(),
            "cache must not retain schema columns"
        );
        if depth == 64 {
            result.unwrap();
            writer.flush().unwrap();
            assert_eq!(
                writer.destination(),
                format!(
                    "<Root><Record><value>{}7{}</value></Record></Root>",
                    "<x>".repeat(64),
                    "</x>".repeat(64)
                )
                .as_bytes()
            );
        } else {
            assert!(matches!(result, Err(FormatError::OutputEncoding { .. })));
            assert!(writer.destination().is_empty());
        }
        drop(writer);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn json_complete_framing_matrix_has_literal_byte_oracles() {
    use clinker_format::json::writer::{JsonEncoder, JsonOutputMode, JsonWriterConfig};
    use clinker_format::{FormatWriter, OutputEnvelopeSpec};
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Schema, Value};
    let compact_records = ["", "{\"v\":1}", "{\"v\":1},{\"v\":2}"];
    let pretty_records = ["", "{\n  \"v\": 1\n}", "{\n  \"v\": 1\n},{\n  \"v\": 2\n}"];
    let compact_documents = [
        "{\"header\":{\"tag\":7},\"body\":[],\"footer\":{\"rows\":0}}",
        "{\"header\":{\"tag\":7},\"body\":[{\"v\":1}],\"footer\":{\"rows\":1}}",
        "{\"header\":{\"tag\":7},\"body\":[{\"v\":1},{\"v\":2}],\"footer\":{\"rows\":2}}",
    ];
    let pretty_documents = [
        "{\"header\":{\n  \"tag\": 7\n},\"body\":[],\"footer\":{\n  \"rows\": 0\n}}",
        "{\"header\":{\n  \"tag\": 7\n},\"body\":[{\n  \"v\": 1\n}],\"footer\":{\n  \"rows\": 1\n}}",
        "{\"header\":{\n  \"tag\": 7\n},\"body\":[{\n  \"v\": 1\n},{\n  \"v\": 2\n}],\"footer\":{\n  \"rows\": 2\n}}",
    ];
    let plain_arrays = ["[]\n", "[\n{\"v\":1}\n]\n", "[\n{\"v\":1},\n{\"v\":2}\n]\n"];
    let pretty_arrays = [
        "[]\n",
        "[\n{\n  \"v\": 1\n}\n]\n",
        "[\n{\n  \"v\": 1\n},\n{\n  \"v\": 2\n}\n]\n",
    ];
    let lines = ["", "{\"v\":1}\n", "{\"v\":1}\n{\"v\":2}\n"];
    let doc = DocumentContext::new(
        DocumentId::next(),
        std::sync::Arc::from("source.json"),
        EnvelopeRecord::from_sections([
            (
                "opening".into(),
                Value::Map(OwnedMap::from_map(
                    [("tag".into(), Value::Integer(7))].into(),
                )),
            ),
            (
                "closing".into(),
                Value::Map(OwnedMap::from_map(Default::default())),
            ),
        ]),
    );
    let mut cases = 0;
    for mode in [JsonOutputMode::Array, JsonOutputMode::Ndjson] {
        for pretty in [false, true] {
            for envelope in [false, true] {
                for count in 0..=2 {
                    for documents in 1..=if envelope { 2 } else { 1 } {
                        let provider =
                            MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
                        let schema =
                            SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec![
                                "v".into(),
                            ])));
                        let config = JsonWriterConfig {
                            format: mode,
                            pretty,
                            envelope: envelope.then(|| OutputEnvelopeSpec {
                                header_from_doc: Some("opening".into()),
                                footer_from_doc: Some("closing".into()),
                                footer_record_count_field: Some("rows".into()),
                            }),
                            ..Default::default()
                        };
                        let encoder =
                            JsonEncoder::new(schema.clone(), &config, provider.resources())
                                .unwrap();
                        let mut writer =
                            PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
                        for _ in 0..documents {
                            writer.begin_document(&doc).unwrap();
                            for value in 1..=count {
                                writer
                                    .write_record(&Record::new(
                                        schema.clone(),
                                        vec![Value::Integer(value as i64)],
                                    ))
                                    .unwrap();
                                let before = writer.destination().clone();
                                writer.flush_bytes().unwrap();
                                assert_eq!(writer.destination(), &before, "drain changed syntax");
                            }
                            writer.end_document(&doc).unwrap();
                        }
                        writer.flush().unwrap();
                        writer.flush().unwrap();
                        let expected = if envelope {
                            let document = if pretty {
                                pretty_documents[count]
                            } else {
                                compact_documents[count]
                            };
                            match (mode, documents) {
                                (JsonOutputMode::Array, 1) => format!("[\n{document}\n]\n"),
                                (JsonOutputMode::Array, 2) => {
                                    format!("[\n{document},\n{document}\n]\n")
                                }
                                (JsonOutputMode::Ndjson, 1) => document.to_owned(),
                                (JsonOutputMode::Ndjson, 2) => format!("{document}\n{document}"),
                                _ => unreachable!(),
                            }
                        } else {
                            match mode {
                                JsonOutputMode::Array if pretty => pretty_arrays[count],
                                JsonOutputMode::Array => plain_arrays[count],
                                JsonOutputMode::Ndjson => lines[count],
                            }
                            .to_owned()
                        };
                        assert_eq!(
                            writer.destination(),
                            expected.as_bytes(),
                            "{mode:?} pretty={pretty} envelope={envelope} count={count} documents={documents}"
                        );
                        // A separate literal grammar check keeps the compact and
                        // pretty body examples tied to the envelope byte table.
                        if envelope {
                            let body = if pretty {
                                pretty_records[count]
                            } else {
                                compact_records[count]
                            };
                            assert!(expected.contains(&format!("\"body\":[{body}]")));
                        }
                        drop(writer);
                        assert_eq!(provider.used(), 0);
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 36);
}

#[test]
fn json_late_failure_does_not_commit_separator_or_document_count() {
    use clinker_format::json::writer::{JsonEncoder, JsonOutputMode, JsonWriterConfig};
    use clinker_format::{FormatWriter, OutputEnvelopeSpec};
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Schema, Value};
    let doc = DocumentContext::new(
        DocumentId::next(),
        std::sync::Arc::from("in.json"),
        EnvelopeRecord::from_sections([(
            "closing".into(),
            Value::Map(OwnedMap::from_map(Default::default())),
        )]),
    );
    for envelope in [false, true] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(256 * 1024).unwrap());
        let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec!["v".into()])));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Array,
            envelope: envelope.then(|| OutputEnvelopeSpec {
                header_from_doc: None,
                footer_from_doc: Some("closing".into()),
                footer_record_count_field: Some("rows".into()),
            }),
            ..Default::default()
        };
        let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        writer.begin_document(&doc).unwrap();
        writer
            .write_record(&Record::new(schema.clone(), vec![Value::Integer(1)]))
            .unwrap();
        let before = writer.destination().clone();
        let committed = provider.used();
        let invalid = [
            Value::Array(OwnedValues::from_vec(vec![
                Value::Integer(7),
                Value::Float(f64::INFINITY),
            ])),
            Value::Map(OwnedMap::from_map(
                [
                    ("good".into(), Value::Bool(true)),
                    (r"\bad".into(), Value::Null),
                ]
                .into(),
            )),
            Value::Map(OwnedMap::from_map(
                [
                    ("@id".into(), Value::Integer(1)),
                    (r"\@id".into(), Value::Integer(2)),
                ]
                .into(),
            )),
            (0..65).fold(Value::Null, |value, _| {
                Value::Array(OwnedValues::from_vec(vec![value]))
            }),
        ];
        for value in invalid {
            assert!(matches!(
                writer.write_record(&Record::new(schema.clone(), vec![value])),
                Err(FormatError::OutputEncoding { .. })
            ));
            assert_eq!(writer.destination(), &before);
            assert_eq!(provider.used(), committed);
        }
        writer
            .write_record(&Record::new(schema, vec![Value::Integer(2)]))
            .unwrap();
        writer.end_document(&doc).unwrap();
        writer.flush().unwrap();
        assert_eq!(
            writer.destination(),
            if envelope {
                b"[\n{\"body\":[{\"v\":1},{\"v\":2}],\"footer\":{\"rows\":2}}\n]\n".as_slice()
            } else {
                b"[\n{\"v\":1},\n{\"v\":2}\n]\n".as_slice()
            }
        );
        drop(writer);
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn json_empty_envelope_stream_without_a_document_finalizes_exactly() {
    use clinker_format::{
        OutputEnvelopeSpec,
        json::writer::{JsonEncoder, JsonOutputMode, JsonWriterConfig},
    };
    use clinker_record::Schema;
    for mode in [JsonOutputMode::Array, JsonOutputMode::Ndjson] {
        for pretty in [false, true] {
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
            let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec![])));
            let config = JsonWriterConfig {
                format: mode,
                pretty,
                envelope: Some(OutputEnvelopeSpec {
                    header_from_doc: Some("opening".into()),
                    footer_from_doc: None,
                    footer_record_count_field: None,
                }),
                ..Default::default()
            };
            let encoder = JsonEncoder::new(schema, &config, provider.resources()).unwrap();
            let mut writer =
                PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
            writer.flush_bytes().unwrap();
            assert!(writer.destination().is_empty());
            writer.flush().unwrap();
            writer.flush().unwrap();
            assert_eq!(
                writer.destination(),
                match mode {
                    JsonOutputMode::Array => b"[]\n".as_slice(),
                    JsonOutputMode::Ndjson => b"",
                }
            );
            drop(writer);
            assert_eq!(provider.used(), 0);
        }
    }
}

mod nested_public_boundary {
    use super::*;
    use clinker_format::{FormatWriter, FormatWriterHandle};
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Value};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Destination(Arc<Mutex<Vec<u8>>>);
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn probe<E: FormatEncoder>(
        encoder: E,
        mut owned: FormatWriterHandle,
        destination: Destination,
        provider: &MemoryOnlyResources,
        expected: &[u8],
    ) {
        let record = Record::new(csv_schema(), vec![Value::Integer(1), Value::Integer(2)]);
        let doc = DocumentContext::new(
            DocumentId::next(),
            Arc::from("input"),
            EnvelopeRecord::from_sections([]),
        );
        let mut borrowed = Vec::new();
        let mut writer = PreparedWriter::new(&mut borrowed, encoder, provider.resources()).unwrap();
        // Exhaust the remaining allowance after construction. A first record
        // cannot fall back to an unadmitted buffer.
        let scope = provider.resources().scope().unwrap();
        let held = scope
            .reserve(std::alloc::Layout::array::<u8>(128 * 1024 - provider.used()).unwrap())
            .unwrap();
        assert!(
            matches!(writer.write_operation(OutputOperation::Record(&record)),
            Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
        );
        assert!(writer.destination().is_empty());
        drop(held);
        writer
            .write_operation(OutputOperation::BeginDocument(&doc))
            .unwrap();
        writer
            .write_operation(OutputOperation::Record(&record))
            .unwrap();
        writer.flush_bytes().unwrap();
        writer
            .write_operation(OutputOperation::EndDocument(&doc))
            .unwrap();
        writer.flush().unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.destination().as_slice(), expected);
        assert!(
            matches!(writer.write_operation(OutputOperation::Record(&record)),
            Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Finalized)
        );
        drop(writer);
        assert_eq!(borrowed, expected);

        owned.begin_document(&doc).unwrap();
        owned.write_record(&record).unwrap();
        owned.flush_bytes().unwrap();
        owned.end_document(&doc).unwrap();
        owned.flush().unwrap();
        owned.flush().unwrap();
        assert_eq!(*destination.0.lock().unwrap(), expected);
        assert!(matches!(owned.write_record(&record),
            Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Finalized));
        drop(owned);
    }

    #[test]
    fn nested_public_json_entrypoints_require_finite_resources() {
        use clinker_format::json::writer::{JsonEncoder, JsonEncoderConfig, JsonWriterConfig};
        let config = JsonWriterConfig::default();
        let denied = MemoryOnlyResources::new(NonZeroUsize::new(1).unwrap());
        assert!(
            matches!(JsonEncoder::new(csv_schema(), &config, denied.resources()),
            Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
        );
        assert_eq!(denied.used(), 0);
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
        let policy = JsonEncoderConfig::new(&config, &provider.resources()).unwrap();
        let retained = provider.used();
        let alias = policy.clone();
        assert_eq!(provider.used(), retained);
        let encoder = JsonEncoder::from_config(csv_schema(), policy).unwrap();
        let destination = Destination::default();
        let owned = JsonEncoder::from_config(csv_schema(), alias.clone())
            .unwrap()
            .into_boxed_writer(destination.clone(), provider.resources())
            .unwrap();
        probe(
            encoder,
            owned,
            destination,
            &provider,
            b"[\n{\"first\":1,\"last\":2}\n]\n",
        );
        assert_eq!(
            provider.used(),
            retained,
            "last config alias retains exactly its backing"
        );
        let encoder = JsonEncoder::from_config(csv_schema(), alias.clone()).unwrap();
        assert!(
            matches!(encoder.into_boxed_writer(Destination::default(), denied.resources()),
            Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
        );
        drop(alias);
        assert_eq!(provider.used(), 0);
        assert_eq!(denied.used(), 0);
    }

    #[test]
    fn nested_public_xml_entrypoints_require_finite_resources() {
        use clinker_format::xml::writer::{XmlEncoder, XmlEncoderConfig, XmlWriterConfig};
        let config = XmlWriterConfig::default();
        let denied = MemoryOnlyResources::new(NonZeroUsize::new(1).unwrap());
        assert!(
            matches!(XmlEncoder::new(csv_schema(), &config, denied.resources()),
            Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
        );
        assert_eq!(denied.used(), 0);
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
        let policy = XmlEncoderConfig::new((&config).into(), &provider.resources()).unwrap();
        let retained = provider.used();
        let alias = policy.clone();
        assert_eq!(provider.used(), retained);
        let encoder = XmlEncoder::from_config(csv_schema(), policy).unwrap();
        let destination = Destination::default();
        let owned = XmlEncoder::from_config(csv_schema(), alias.clone())
            .unwrap()
            .into_boxed_writer(destination.clone(), provider.resources())
            .unwrap();
        probe(
            encoder,
            owned,
            destination,
            &provider,
            b"<Root><Record><first>1</first><last>2</last></Record></Root>",
        );
        assert_eq!(
            provider.used(),
            retained,
            "last config alias retains exactly its backing"
        );
        let encoder = XmlEncoder::from_config(csv_schema(), alias.clone()).unwrap();
        assert!(
            matches!(encoder.into_boxed_writer(Destination::default(), denied.resources()),
            Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
        );
        drop(alias);
        assert_eq!(provider.used(), 0);
        assert_eq!(denied.used(), 0);
    }
}

mod nested_fault_boundaries {
    use super::*;
    use clinker_format::preparation::{
        AllocationAuthority, AllocationLease, MemoryStorage, OperationStage, OwnerId,
        ResourceAuthority, WriterResources,
    };
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, Record, Value};
    use std::io::Read;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[repr(usize)]
    enum Fault {
        None,
        Budget,
        Layout,
        Allocation,
        Write,
        Flush,
        Seal,
        BeforeSeal,
        AfterSeal,
        Read,
        ShortRead,
        Destination,
        AfterWrite,
        Complete,
    }
    const FAULTS: [Fault; 13] = [
        Fault::Budget,
        Fault::Layout,
        Fault::Allocation,
        Fault::Write,
        Fault::Flush,
        Fault::Seal,
        Fault::BeforeSeal,
        Fault::AfterSeal,
        Fault::Read,
        Fault::ShortRead,
        Fault::Destination,
        Fault::AfterWrite,
        Fault::Complete,
    ];
    struct Authority {
        memory: MemoryOnlyResources,
        fault: AtomicUsize,
        cancelled: AtomicBool,
        stages: AtomicUsize,
    }
    impl Authority {
        fn is(&self, fault: Fault) -> bool {
            self.fault.load(Ordering::SeqCst) == fault as usize
        }
        fn evidence(&self, kind: ResourceErrorKind) -> ResourceError {
            ResourceError {
                kind,
                requested: 91,
                available: 17,
                field: Some(3),
                offset: Some(41),
            }
        }
        fn reset(&self) {
            self.fault.store(Fault::None as usize, Ordering::SeqCst);
            self.cancelled.store(false, Ordering::SeqCst);
        }
    }
    impl AllocationAuthority for Authority {
        fn identity(&self) -> usize {
            self.memory.resources().allocation().identity()
        }
        fn try_reserve(
            self: Arc<Self>,
            owner: OwnerId,
            layout: std::alloc::Layout,
        ) -> Result<AllocationLease, ResourceError> {
            self.check_cancelled()?;
            for (fault, kind) in [
                (Fault::Budget, ResourceErrorKind::Budget),
                (Fault::Layout, ResourceErrorKind::Layout),
                (Fault::Allocation, ResourceErrorKind::Allocation),
            ] {
                if self.is(fault) {
                    return Err(self.evidence(kind));
                }
            }
            self.memory.resources().allocation().reserve(owner, layout)
        }
        fn release(&self, _: OwnerId, _: usize) {
            unreachable!("delegated memory owns grants")
        }
        fn check_cancelled(&self) -> Result<(), ResourceError> {
            if self.cancelled.load(Ordering::SeqCst) {
                Err(self.evidence(ResourceErrorKind::Cancelled))
            } else {
                Ok(())
            }
        }
    }
    struct Storage {
        memory: MemoryStorage,
        authority: Arc<Authority>,
        failure: Option<ResourceError>,
    }
    impl Storage {
        fn fail(&mut self, kind: ResourceErrorKind) -> std::io::Error {
            self.failure = Some(self.authority.evidence(kind));
            std::io::ErrorKind::Other.into()
        }
    }
    impl Drop for Storage {
        fn drop(&mut self) {
            self.authority.stages.fetch_sub(1, Ordering::SeqCst);
        }
    }
    impl Write for Storage {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.authority.is(Fault::Write) {
                return Err(self.fail(ResourceErrorKind::Storage));
            }
            let n = self.memory.write(bytes)?;
            if self.authority.is(Fault::BeforeSeal) {
                self.authority.cancelled.store(true, Ordering::SeqCst);
            }
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            if self.authority.is(Fault::Flush) {
                return Err(self.fail(ResourceErrorKind::Storage));
            }
            self.memory.flush()
        }
    }
    impl Read for Storage {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            if self.authority.is(Fault::Read) {
                return Err(self.fail(ResourceErrorKind::Readback));
            }
            if self.authority.is(Fault::ShortRead) {
                return Ok(0);
            }
            self.memory.read(bytes)
        }
    }
    impl StageStorage for Storage {
        fn resource_failed(&mut self, error: ResourceError) {
            self.failure.get_or_insert(error);
            self.memory.resource_failed(error);
        }
        fn failure(&self) -> Option<ResourceError> {
            self.failure.or(self.memory.failure())
        }
        fn seal(&mut self) -> Result<u64, ResourceError> {
            if self.authority.is(Fault::Seal) {
                return Err(self.authority.evidence(ResourceErrorKind::Storage));
            }
            self.flush().map_err(|_| self.failure.unwrap())?;
            let len = self.memory.seal()?;
            if self.authority.is(Fault::AfterSeal) {
                self.authority.cancelled.store(true, Ordering::SeqCst);
            }
            Ok(len)
        }
        fn complete(&mut self) -> Result<(), ResourceError> {
            if self.authority.is(Fault::Complete) {
                return Err(self.authority.evidence(ResourceErrorKind::Storage));
            }
            self.memory.complete()
        }
    }
    impl ResourceAuthority for Authority {
        fn create_stage(
            self: Arc<Self>,
            scope: WriterScope,
        ) -> Result<OperationStage, FormatError> {
            self.stages.fetch_add(1, Ordering::SeqCst);
            StorageStage::create(
                scope.clone(),
                Storage {
                    memory: MemoryStorage::new(scope),
                    authority: self,
                    failure: None,
                },
            )
        }
    }
    struct Destination {
        bytes: Vec<u8>,
        authority: Arc<Authority>,
        remaining: std::cell::Cell<usize>,
        calls: usize,
    }
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            if self.authority.is(Fault::Destination) && self.remaining.get() == 0 {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            let n = if self.authority.is(Fault::Destination) {
                bytes.len().min(self.remaining.get())
            } else {
                bytes.len()
            };
            self.bytes.extend_from_slice(&bytes[..n]);
            self.remaining.set(self.remaining.get().saturating_sub(n));
            if self.authority.is(Fault::AfterWrite) {
                self.authority.cancelled.store(true, Ordering::SeqCst);
            }
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn operation<'a>(
        index: usize,
        record: &'a Record,
        doc: &'a DocumentContext,
    ) -> OutputOperation<'a> {
        match index {
            0 | 1 => OutputOperation::BeginDocument(doc),
            2 | 3 => OutputOperation::Record(record),
            4 | 5 => OutputOperation::EndDocument(doc),
            6 | 7 => OutputOperation::Finalize,
            _ => unreachable!(),
        }
    }
    fn literal(xml: bool, index: usize) -> &'static [u8] {
        if xml {
            match index {
                0 => b"<Root><Document><header><v>start</v></header>",
                1 => b"<Document><header><v>start</v></header>",
                2 | 3 => b"<Record><first>1</first><last>2</last></Record>",
                4 => b"<footer><v>end</v><rows>0</rows></footer></Document>",
                5 => b"<footer><v>end</v><rows>1</rows></footer></Document>",
                6 => b"<Root></Root>",
                7 => b"</Root>",
                _ => unreachable!(),
            }
        } else {
            match index {
                0 => b"[\n{\"header\":{\"v\":\"start\"},\"body\":[",
                1 => b",\n{\"header\":{\"v\":\"start\"},\"body\":[",
                2 => b"{\"first\":1,\"last\":2}",
                3 => b",{\"first\":1,\"last\":2}",
                4 => b"],\"footer\":{\"v\":\"end\",\"rows\":0}}",
                5 => b"],\"footer\":{\"v\":\"end\",\"rows\":1}}",
                6 => b"[]\n",
                7 => b"\n]\n",
                _ => unreachable!(),
            }
        }
    }
    fn snapshot<E: FormatEncoder>(
        encoder: &E,
        resources: &WriterResources,
        record: &Record,
        doc: &DocumentContext,
    ) -> Vec<Result<Vec<u8>, String>> {
        (0..4)
            .map(|i| {
                let scope = resources.scope().unwrap();
                let mut stage = scope.stage().unwrap();
                let op = match i {
                    0 => OutputOperation::BeginDocument(doc),
                    1 => OutputOperation::Record(record),
                    2 => OutputOperation::EndDocument(doc),
                    _ => OutputOperation::Finalize,
                };
                match encoder.prepare(op, &mut stage, &scope) {
                    Ok(pending) => {
                        drop(pending);
                        let mut bytes = Vec::new();
                        stage.finish().unwrap().deliver(&mut bytes).unwrap();
                        Ok(bytes)
                    }
                    Err(error) => Err(error.to_string()),
                }
            })
            .collect()
    }
    fn run<E: FormatEncoder>(xml: bool, build: impl Fn(WriterResources) -> E) {
        let record = Record::new(csv_schema(), vec![Value::Integer(1), Value::Integer(2)]);
        let doc = DocumentContext::new(
            DocumentId::next(),
            Arc::from("input"),
            EnvelopeRecord::from_sections([
                (
                    "opening".into(),
                    Value::Map(OwnedMap::from_map(
                        [("v".into(), Value::String("start".into()))].into(),
                    )),
                ),
                (
                    "closing".into(),
                    Value::Map(OwnedMap::from_map(
                        [("v".into(), Value::String("end".into()))].into(),
                    )),
                ),
            ]),
        );
        for index in 0..8 {
            for fault in FAULTS {
                let authority = Arc::new(Authority {
                    memory: MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap()),
                    fault: AtomicUsize::new(0),
                    cancelled: AtomicBool::new(false),
                    stages: AtomicUsize::new(0),
                });
                let resources = WriterResources::new(authority.clone());
                let encoder = build(resources.clone());
                let destination = Destination {
                    bytes: Vec::new(),
                    authority: authority.clone(),
                    remaining: std::cell::Cell::new(usize::MAX),
                    calls: 0,
                };
                let mut writer =
                    PreparedWriter::new(destination, encoder, resources.clone()).unwrap();
                let prior: &[usize] = match index {
                    0 | 6 => &[],
                    1 | 7 => &[0, 2, 5],
                    2 | 4 => &[0],
                    3 | 5 => &[0, 2],
                    _ => unreachable!(),
                };
                let mut expected = Vec::new();
                for &step in prior {
                    writer
                        .write_operation(operation(step, &record, &doc))
                        .unwrap();
                    expected.extend_from_slice(literal(xml, step));
                }
                assert_eq!(writer.destination().bytes, expected);
                let before = snapshot(writer.encoder(), &resources, &record, &doc);
                let retained = authority.memory.used();
                // This target accepts only a two-byte irreversible prefix.
                writer.destination().remaining.set(2);
                authority.fault.store(fault as usize, Ordering::SeqCst);
                let error = writer
                    .write_operation(operation(index, &record, &doc))
                    .unwrap_err();
                let kind = match fault {
                    Fault::Budget => ResourceErrorKind::Budget,
                    Fault::Layout => ResourceErrorKind::Layout,
                    Fault::Allocation => ResourceErrorKind::Allocation,
                    Fault::BeforeSeal | Fault::AfterSeal | Fault::AfterWrite => {
                        ResourceErrorKind::Cancelled
                    }
                    Fault::Read | Fault::ShortRead => ResourceErrorKind::Readback,
                    _ => ResourceErrorKind::Storage,
                };
                match (&error, fault) {
                    (FormatError::Io(error), Fault::Destination) => {
                        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe)
                    }
                    (FormatError::Resource(error), Fault::ShortRead) => {
                        assert_eq!(error.kind, kind);
                        assert_eq!(error.available, 0);
                        assert_eq!(error.requested, literal(xml, index).len());
                    }
                    (FormatError::Resource(error), _) => {
                        assert_eq!(*error, authority.evidence(kind), "{xml}/{index}/{fault:?}")
                    }
                    _ => panic!("{xml}/{index}/{fault:?}: {error:?}"),
                }
                let poisoned = matches!(
                    fault,
                    Fault::AfterSeal
                        | Fault::Read
                        | Fault::ShortRead
                        | Fault::Destination
                        | Fault::AfterWrite
                        | Fault::Complete
                );
                match fault {
                    Fault::Destination => expected.extend_from_slice(&literal(xml, index)[..2]),
                    Fault::AfterWrite | Fault::Complete => {
                        expected.extend_from_slice(literal(xml, index))
                    }
                    _ => {}
                }
                assert_eq!(
                    writer.destination().bytes,
                    expected,
                    "{xml}/{index}/{fault:?}"
                );
                authority.reset();
                assert_eq!(
                    snapshot(writer.encoder(), &resources, &record, &doc),
                    before,
                    "committed state changed: {xml}/{index}/{fault:?}"
                );
                assert_eq!(
                    authority.memory.used(),
                    retained,
                    "pending state leaked: {xml}/{index}/{fault:?}"
                );
                assert_eq!(authority.stages.load(Ordering::SeqCst), 0);
                if poisoned {
                    let calls = writer.destination().calls;
                    for op in [operation(index, &record, &doc), OutputOperation::Finalize] {
                        assert!(
                            matches!(writer.write_operation(op), Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::DeliveryPoisoned)
                        );
                    }
                    assert!(
                        matches!(writer.flush_bytes(), Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::DeliveryPoisoned)
                    );
                    assert_eq!(writer.destination().calls, calls);
                } else {
                    writer
                        .write_operation(operation(index, &record, &doc))
                        .unwrap();
                    expected.extend_from_slice(literal(xml, index));
                    assert_eq!(
                        writer.destination().bytes,
                        expected,
                        "retry bytes: {xml}/{index}/{fault:?}"
                    );
                }
                drop(writer);
                assert_eq!(authority.memory.used(), 0);
                assert_eq!(authority.stages.load(Ordering::SeqCst), 0);
            }
        }
    }
    fn envelope() -> clinker_format::OutputEnvelopeSpec {
        clinker_format::OutputEnvelopeSpec {
            header_from_doc: Some("opening".into()),
            footer_from_doc: Some("closing".into()),
            footer_record_count_field: Some("rows".into()),
        }
    }
    #[test]
    fn nested_fault_json_operations_preserve_state_or_poison_delivery() {
        use clinker_format::json::writer::{JsonEncoder, JsonWriterConfig};
        run(false, |resources| {
            JsonEncoder::new(
                csv_schema(),
                &JsonWriterConfig {
                    envelope: Some(envelope()),
                    ..Default::default()
                },
                resources,
            )
            .unwrap()
        });
    }
    #[test]
    fn nested_fault_xml_operations_preserve_state_or_poison_delivery() {
        use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
        run(true, |resources| {
            XmlEncoder::new(
                csv_schema(),
                &XmlWriterConfig {
                    envelope: Some(envelope()),
                    ..Default::default()
                },
                resources,
            )
            .unwrap()
        });
    }
}

#[test]
fn nested_fault_wide_schema_and_huge_keys_keep_cache_and_bounded_errors() {
    use clinker_format::{
        json::writer::{JsonEncoder, JsonWriterConfig},
        xml::writer::{XmlEncoder, XmlWriterConfig},
    };
    use clinker_record::{Record, Schema, Value};
    fn probe<E: FormatEncoder>(
        encoder: E,
        provider: &MemoryOnlyResources,
        original: &Record,
        xml: bool,
    ) {
        let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
        writer
            .write_operation(OutputOperation::Record(original))
            .unwrap();
        let before = writer.destination().clone();
        let retained = provider.used();
        for columns in [
            vec!["x".repeat(256 * 1024)],
            (0..2048).map(|i| format!("field_{i}")).collect(),
        ] {
            let values = vec![Value::Integer(1); columns.len()];
            let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(
                columns.into_iter().map(Into::into).collect(),
            )));
            let record = Record::new(schema, values);
            let error = writer
                .write_operation(OutputOperation::Record(&record))
                .unwrap_err();
            assert!(
                matches!(error, FormatError::Resource(error) if error.kind == ResourceErrorKind::Budget)
            );
            assert!(error.to_string().len() < 200);
            assert_eq!(writer.destination(), &before);
            assert_eq!(provider.used(), retained);
        }
        let (result, attempts) = allocation_probe(true, || {
            writer.write_operation(OutputOperation::Record(original))
        });
        assert!(
            matches!(result, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Allocation)
        );
        assert_eq!(attempts, 1);
        assert_eq!(writer.destination(), &before);
        assert_eq!(provider.used(), retained);
        writer
            .write_operation(OutputOperation::Record(original))
            .unwrap();
        writer.flush().unwrap();
        assert_eq!(
            writer.destination(),
            if xml {
                b"<Root><Record><value>1</value></Record><Record><value>1</value></Record></Root>"
                    .as_slice()
            } else {
                b"[\n{\"value\":1},\n{\"value\":1}\n]\n".as_slice()
            }
        );
        drop(writer);
        assert_eq!(provider.used(), 0);
    }
    for xml in [false, true] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
        let schema =
            SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec!["value".into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(1)]);
        if xml {
            let config = XmlWriterConfig::default();
            let (result, _) = allocation_probe(true, || {
                XmlEncoder::new(schema.clone(), &config, provider.resources())
            });
            assert!(
                matches!(result, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Allocation)
            );
            assert_eq!(provider.used(), 0);
            let encoder = XmlEncoder::new(schema, &config, provider.resources()).unwrap();
            probe(encoder, &provider, &record, xml);
        } else {
            let config = JsonWriterConfig::default();
            let (result, _) = allocation_probe(true, || {
                JsonEncoder::new(schema.clone(), &config, provider.resources())
            });
            assert!(
                matches!(result, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Allocation)
            );
            assert_eq!(provider.used(), 0);
            let encoder = JsonEncoder::new(schema, &config, provider.resources()).unwrap();
            probe(encoder, &provider, &record, xml);
        }
    }
}
