use std::io::Cursor;

use clinker_bench_support::alloc::{AccountingAlloc, Region};
use clinker_format::json::reader::{JsonMode, JsonReader, JsonReaderConfig};
use clinker_format::multi_value::{SplitToRows, SplitToRowsMode};
use clinker_format::traits::FormatReader;
use clinker_format::xml::reader::{XmlReader, XmlReaderConfig};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

#[global_allocator]
static ALLOC: AccountingAlloc = AccountingAlloc::new();

#[derive(Clone, Copy)]
enum Format {
    Json,
    Xml,
}

impl Format {
    const ALL: [Self; 2] = [Self::Json, Self::Xml];

    const fn label(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Xml => "xml",
        }
    }
}

fn fan_out_entry(field: &str, mode: SplitToRowsMode) -> SplitToRows {
    SplitToRows {
        mode,
        ..SplitToRows::bare(field)
    }
}

fn json_input(occurrences: usize) -> Vec<u8> {
    let left = (0..occurrences)
        .map(|i| format!(r#"{{"left_value":{i}}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let right = (0..occurrences)
        .map(|i| format!(r#"{{"right_value":{i}}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let mut bytes = format!(r#"{{"id":1,"left":[{left}],"right":[{right}]}}"#).into_bytes();
    bytes.push(b'\n');
    bytes
}

fn xml_input(occurrences: usize) -> Vec<u8> {
    let mut xml = String::from("<Root><Row><id>1</id>");
    for i in 0..occurrences {
        xml.push_str(&format!("<left><left_value>{i}</left_value></left>"));
    }
    for i in 0..occurrences {
        xml.push_str(&format!("<right><right_value>{i}</right_value></right>"));
    }
    xml.push_str("</Row></Root>");
    xml.into_bytes()
}

fn drain(format: Format, mode: SplitToRowsMode, bytes: &[u8]) -> u64 {
    let entries = vec![fan_out_entry("left", mode), fan_out_entry("right", mode)];
    let mut reader: Box<dyn FormatReader> = match format {
        Format::Json => Box::new(
            JsonReader::from_reader(
                Cursor::new(bytes.to_vec()),
                JsonReaderConfig {
                    format: Some(JsonMode::Ndjson),
                    split_to_rows: entries,
                    ..Default::default()
                },
            )
            .expect("benchmark JSON is valid"),
        ),
        Format::Xml => Box::new(
            XmlReader::from_reader(
                Cursor::new(bytes.to_vec()),
                XmlReaderConfig {
                    record_path: Some("Root/Row".into()),
                    split_to_rows: entries,
                    ..Default::default()
                },
            )
            .expect("benchmark XML is valid"),
        ),
    };
    reader.schema().expect("benchmark schema is valid");
    let mut rows = 0;
    while reader
        .next_record()
        .expect("benchmark record is valid")
        .is_some()
    {
        rows += 1;
    }
    rows
}

fn profile_allocations(format: Format, mode: SplitToRowsMode, bytes: &[u8], rows: u64) {
    let region = Region::new(&ALLOC);
    assert_eq!(drain(format, mode, bytes), rows);
    let stats = region.change();
    eprintln!(
        "fan_out_alloc format={} mode={} rows={} allocations_per_row={:.3} bytes_per_row={:.3}",
        format.label(),
        match mode {
            SplitToRowsMode::Extract => "extract",
            SplitToRowsMode::Split => "split",
        },
        rows,
        stats.allocs as f64 / rows as f64,
        stats.bytes_alloc as f64 / rows as f64,
    );
}

fn bench_fan_out(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_fan_out");
    for occurrences in [8usize, 32, 64] {
        let rows = (occurrences * occurrences) as u64;
        for format in Format::ALL {
            let bytes = match format {
                Format::Json => json_input(occurrences),
                Format::Xml => xml_input(occurrences),
            };
            for mode in [SplitToRowsMode::Extract, SplitToRowsMode::Split] {
                profile_allocations(format, mode, &bytes, rows);
                group.throughput(Throughput::Elements(rows));
                group.bench_with_input(
                    BenchmarkId::new(
                        format!(
                            "{}_{}",
                            format.label(),
                            match mode {
                                SplitToRowsMode::Extract => "extract",
                                SplitToRowsMode::Split => "split",
                            }
                        ),
                        occurrences,
                    ),
                    &bytes,
                    |b, input| b.iter(|| black_box(drain(format, mode, input))),
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_fan_out);
criterion_main!(benches);
