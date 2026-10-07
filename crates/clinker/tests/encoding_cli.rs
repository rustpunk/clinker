//! Exact-byte text format qualification through the compiled executable.
//!
//! Destination-prefix failure, cancellation, and forced stage spill are tested
//! at the integration-owned writer boundary in `writer_resources` and
//! `sink_surface`; these CLI cases exercise real file publication.

use std::path::Path;
use std::process::{Command, Output};

// Independently spelled octets and Unicode scalars include C1 controls; a
// Windows-1252 mapping cannot satisfy both cross-charset directions.
const HIGH_LATIN1: &[u8] = b"\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff";
const HIGH_UTF8: &[u8] = "\u{80}\u{81}\u{82}\u{83}\u{84}\u{85}\u{86}\u{87}\u{88}\u{89}\u{8a}\u{8b}\u{8c}\u{8d}\u{8e}\u{8f}\u{90}\u{91}\u{92}\u{93}\u{94}\u{95}\u{96}\u{97}\u{98}\u{99}\u{9a}\u{9b}\u{9c}\u{9d}\u{9e}\u{9f}\u{a0}\u{a1}\u{a2}\u{a3}\u{a4}\u{a5}\u{a6}\u{a7}\u{a8}\u{a9}\u{aa}\u{ab}\u{ac}\u{ad}\u{ae}\u{af}\u{b0}\u{b1}\u{b2}\u{b3}\u{b4}\u{b5}\u{b6}\u{b7}\u{b8}\u{b9}\u{ba}\u{bb}\u{bc}\u{bd}\u{be}\u{bf}\u{c0}\u{c1}\u{c2}\u{c3}\u{c4}\u{c5}\u{c6}\u{c7}\u{c8}\u{c9}\u{ca}\u{cb}\u{cc}\u{cd}\u{ce}\u{cf}\u{d0}\u{d1}\u{d2}\u{d3}\u{d4}\u{d5}\u{d6}\u{d7}\u{d8}\u{d9}\u{da}\u{db}\u{dc}\u{dd}\u{de}\u{df}\u{e0}\u{e1}\u{e2}\u{e3}\u{e4}\u{e5}\u{e6}\u{e7}\u{e8}\u{e9}\u{ea}\u{eb}\u{ec}\u{ed}\u{ee}\u{ef}\u{f0}\u{f1}\u{f2}\u{f3}\u{f4}\u{f5}\u{f6}\u{f7}\u{f8}\u{f9}\u{fa}\u{fb}\u{fc}\u{fd}\u{fe}\u{ff}".as_bytes();

const EXPECTED_REPERTOIRE_ROWS: &[&str] = &[
    "utf-8/utf-8/single",
    "utf-8/utf-8/multi",
    "utf-8/iso-8859-1/single",
    "utf-8/iso-8859-1/multi",
    "iso-8859-1/utf-8/single",
    "iso-8859-1/utf-8/multi",
    "iso-8859-1/iso-8859-1/single",
    "iso-8859-1/iso-8859-1/multi",
];

#[test]
fn csv_cli_every_high_byte_has_true_latin1_parity() {
    use std::collections::BTreeSet;
    let mut executed = BTreeSet::new();
    for input in ["utf-8", "iso-8859-1"] {
        for output in ["utf-8", "iso-8859-1"] {
            for multi in [false, true] {
                let root = tempfile::tempdir().unwrap();
                std::fs::write(
                    root.path().join("pipeline.yaml"),
                    matrix_pipeline(input, output, multi, false, "file"),
                )
                .unwrap();
                let prefix: &[u8] = if multi {
                    if input == "utf-8" {
                        "Dé,".as_bytes()
                    } else {
                        b"D\xe9,"
                    }
                } else {
                    b""
                };
                std::fs::write(
                    root.path().join("input.csv"),
                    [
                        prefix,
                        if input == "utf-8" {
                            HIGH_UTF8
                        } else {
                            HIGH_LATIN1
                        },
                        b"\n",
                    ]
                    .concat(),
                )
                .unwrap();
                assert_completed(&run(root.path()), 1, 1, 0);
                let expected = [
                    if output == "utf-8" {
                        HIGH_UTF8
                    } else {
                        HIGH_LATIN1
                    },
                    b"\n",
                ]
                .concat();
                assert_eq!(
                    std::fs::read(root.path().join("output.csv")).unwrap(),
                    expected
                );
                assert!(executed.insert(format!(
                    "{input}/{output}/{}",
                    if multi { "multi" } else { "single" }
                )));
            }
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        EXPECTED_REPERTOIRE_ROWS.iter().copied().collect()
    );
    assert_eq!(executed.len(), 8);
}

#[test]
fn csv_cli_latin1_bom_shaped_data_survives_repeated_runs() {
    for (output, expected) in [
        ("utf-8", "label\nï»¿Café\n".as_bytes()),
        ("iso-8859-1", b"label\n\xef\xbb\xbfCaf\xe9\n".as_slice()),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("pipeline.yaml"),
            simple_pipeline("iso-8859-1", output, false),
        )
        .unwrap();
        std::fs::write(root.path().join("input.csv"), b"\xef\xbb\xbfCaf\xe9\n").unwrap();
        for _ in 0..2 {
            let result = Command::new(env!("CARGO_BIN_EXE_clinker"))
                .current_dir(root.path())
                .args([
                    "run",
                    "pipeline.yaml",
                    "--force",
                    "--machine",
                    "ndjson-v1",
                    "--batch-id",
                    "csv-repeated",
                ])
                .output()
                .unwrap();
            assert_completed(&result, 1, 1, 0);
            assert_eq!(
                std::fs::read(root.path().join("output.csv")).unwrap(),
                expected
            );
        }
    }
}

const EXPECTED_DOCUMENT_ROWS: &[&str] = &[
    "utf-8/utf-8/header/file",
    "utf-8/utf-8/header/files",
    "utf-8/utf-8/no-header/file",
    "utf-8/utf-8/no-header/files",
    "utf-8/iso-8859-1/header/file",
    "utf-8/iso-8859-1/header/files",
    "utf-8/iso-8859-1/no-header/file",
    "utf-8/iso-8859-1/no-header/files",
    "iso-8859-1/utf-8/header/file",
    "iso-8859-1/utf-8/header/files",
    "iso-8859-1/utf-8/no-header/file",
    "iso-8859-1/utf-8/no-header/files",
    "iso-8859-1/iso-8859-1/header/file",
    "iso-8859-1/iso-8859-1/header/files",
    "iso-8859-1/iso-8859-1/no-header/file",
    "iso-8859-1/iso-8859-1/no-header/files",
];

#[test]
fn csv_cli_document_sections_reconstruct_each_physical_document() {
    use std::collections::BTreeSet;
    let mut executed = BTreeSet::new();
    for input in ["utf-8", "iso-8859-1"] {
        for output in ["utf-8", "iso-8859-1"] {
            for header in [true, false] {
                for files in [false, true] {
                    let root = tempfile::tempdir().unwrap();
                    let mut yaml = matrix_pipeline(
                        input,
                        output,
                        true,
                        header,
                        if files { "files" } else { "file" },
                    );
                    yaml = yaml.replace("          - id: detail", "          - id: metadata\n            tag: Hé\n            columns:\n              - { name: marker, type: string }\n              - { name: batch, type: string }\n          - id: detail");
                    yaml = yaml.replace("  - type: sink", "      envelope:\n        sections:\n          manifest:\n            extract: { record_type: Hé }\n            fields:\n              batch: string\n  - type: sink");
                    yaml = yaml.replace(&format!("      options: {{ encoding: {output} }}"), &format!("      reconstruct_envelope: true\n      options:\n        encoding: {output}\n        envelope: {{ header_from_doc: manifest }}"));
                    std::fs::write(root.path().join("pipeline.yaml"), yaml).unwrap();
                    let (a, b): (&[u8], &[u8]) = if input == "utf-8" {
                        (
                            "Hé,Café\nDé,Crème\n".as_bytes(),
                            "Hé,Crème\nDé,Café\n".as_bytes(),
                        )
                    } else {
                        (
                            b"H\xe9,Caf\xe9\nD\xe9,Cr\xe8me\n",
                            b"H\xe9,Cr\xe8me\nD\xe9,Caf\xe9\n",
                        )
                    };
                    for (name, body) in if files {
                        vec![("input-a.csv", a), ("input-b.csv", b)]
                    } else {
                        vec![("input.csv", a)]
                    } {
                        std::fs::write(
                            root.path().join(name),
                            [
                                if header {
                                    b"marker,label\n".as_slice()
                                } else {
                                    b""
                                },
                                body,
                            ]
                            .concat(),
                        )
                        .unwrap();
                    }
                    assert_completed(
                        &run(root.path()),
                        if files { 2 } else { 1 },
                        if files { 2 } else { 1 },
                        0,
                    );
                    let expected: &[u8] = match (output, files) {
                        ("utf-8", false) => "Café\nCrème\n".as_bytes(),
                        ("utf-8", true) => "Café\nCrème\nCrème\nCafé\n".as_bytes(),
                        (_, false) => b"Caf\xe9\nCr\xe8me\n",
                        (_, true) => b"Caf\xe9\nCr\xe8me\nCr\xe8me\nCaf\xe9\n",
                    };
                    assert_eq!(
                        std::fs::read(root.path().join("output.csv")).unwrap(),
                        expected
                    );
                    assert!(executed.insert(format!(
                        "{input}/{output}/{}/{}",
                        if header { "header" } else { "no-header" },
                        if files { "files" } else { "file" }
                    )));
                }
            }
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        EXPECTED_DOCUMENT_ROWS.iter().copied().collect()
    );
    assert_eq!(executed.len(), 16);
}
// Independently declared coverage contract. The runner below must execute every
// row exactly once; adding a runner branch cannot silently redefine coverage.
const EXPECTED_CLI_ROWS: &[&str] = &[
    "utf-8/utf-8/single/header/file",
    "utf-8/utf-8/single/header/files",
    "utf-8/utf-8/single/header/correlation",
    "utf-8/utf-8/single/header/split",
    "utf-8/utf-8/single/header/fan-out",
    "utf-8/utf-8/single/header/split-fan-out",
    "utf-8/utf-8/single/no-header/file",
    "utf-8/utf-8/single/no-header/files",
    "utf-8/utf-8/single/no-header/correlation",
    "utf-8/utf-8/single/no-header/split",
    "utf-8/utf-8/single/no-header/fan-out",
    "utf-8/utf-8/single/no-header/split-fan-out",
    "utf-8/utf-8/multi/header/file",
    "utf-8/utf-8/multi/header/files",
    "utf-8/utf-8/multi/header/correlation",
    "utf-8/utf-8/multi/header/split",
    "utf-8/utf-8/multi/header/fan-out",
    "utf-8/utf-8/multi/header/split-fan-out",
    "utf-8/utf-8/multi/no-header/file",
    "utf-8/utf-8/multi/no-header/files",
    "utf-8/utf-8/multi/no-header/correlation",
    "utf-8/utf-8/multi/no-header/split",
    "utf-8/utf-8/multi/no-header/fan-out",
    "utf-8/utf-8/multi/no-header/split-fan-out",
    "utf-8/iso-8859-1/single/header/file",
    "utf-8/iso-8859-1/single/header/files",
    "utf-8/iso-8859-1/single/header/correlation",
    "utf-8/iso-8859-1/single/header/split",
    "utf-8/iso-8859-1/single/header/fan-out",
    "utf-8/iso-8859-1/single/header/split-fan-out",
    "utf-8/iso-8859-1/single/no-header/file",
    "utf-8/iso-8859-1/single/no-header/files",
    "utf-8/iso-8859-1/single/no-header/correlation",
    "utf-8/iso-8859-1/single/no-header/split",
    "utf-8/iso-8859-1/single/no-header/fan-out",
    "utf-8/iso-8859-1/single/no-header/split-fan-out",
    "utf-8/iso-8859-1/multi/header/file",
    "utf-8/iso-8859-1/multi/header/files",
    "utf-8/iso-8859-1/multi/header/correlation",
    "utf-8/iso-8859-1/multi/header/split",
    "utf-8/iso-8859-1/multi/header/fan-out",
    "utf-8/iso-8859-1/multi/header/split-fan-out",
    "utf-8/iso-8859-1/multi/no-header/file",
    "utf-8/iso-8859-1/multi/no-header/files",
    "utf-8/iso-8859-1/multi/no-header/correlation",
    "utf-8/iso-8859-1/multi/no-header/split",
    "utf-8/iso-8859-1/multi/no-header/fan-out",
    "utf-8/iso-8859-1/multi/no-header/split-fan-out",
    "iso-8859-1/utf-8/single/header/file",
    "iso-8859-1/utf-8/single/header/files",
    "iso-8859-1/utf-8/single/header/correlation",
    "iso-8859-1/utf-8/single/header/split",
    "iso-8859-1/utf-8/single/header/fan-out",
    "iso-8859-1/utf-8/single/header/split-fan-out",
    "iso-8859-1/utf-8/single/no-header/file",
    "iso-8859-1/utf-8/single/no-header/files",
    "iso-8859-1/utf-8/single/no-header/correlation",
    "iso-8859-1/utf-8/single/no-header/split",
    "iso-8859-1/utf-8/single/no-header/fan-out",
    "iso-8859-1/utf-8/single/no-header/split-fan-out",
    "iso-8859-1/utf-8/multi/header/file",
    "iso-8859-1/utf-8/multi/header/files",
    "iso-8859-1/utf-8/multi/header/correlation",
    "iso-8859-1/utf-8/multi/header/split",
    "iso-8859-1/utf-8/multi/header/fan-out",
    "iso-8859-1/utf-8/multi/header/split-fan-out",
    "iso-8859-1/utf-8/multi/no-header/file",
    "iso-8859-1/utf-8/multi/no-header/files",
    "iso-8859-1/utf-8/multi/no-header/correlation",
    "iso-8859-1/utf-8/multi/no-header/split",
    "iso-8859-1/utf-8/multi/no-header/fan-out",
    "iso-8859-1/utf-8/multi/no-header/split-fan-out",
    "iso-8859-1/iso-8859-1/single/header/file",
    "iso-8859-1/iso-8859-1/single/header/files",
    "iso-8859-1/iso-8859-1/single/header/correlation",
    "iso-8859-1/iso-8859-1/single/header/split",
    "iso-8859-1/iso-8859-1/single/header/fan-out",
    "iso-8859-1/iso-8859-1/single/header/split-fan-out",
    "iso-8859-1/iso-8859-1/single/no-header/file",
    "iso-8859-1/iso-8859-1/single/no-header/files",
    "iso-8859-1/iso-8859-1/single/no-header/correlation",
    "iso-8859-1/iso-8859-1/single/no-header/split",
    "iso-8859-1/iso-8859-1/single/no-header/fan-out",
    "iso-8859-1/iso-8859-1/single/no-header/split-fan-out",
    "iso-8859-1/iso-8859-1/multi/header/file",
    "iso-8859-1/iso-8859-1/multi/header/files",
    "iso-8859-1/iso-8859-1/multi/header/correlation",
    "iso-8859-1/iso-8859-1/multi/header/split",
    "iso-8859-1/iso-8859-1/multi/header/fan-out",
    "iso-8859-1/iso-8859-1/multi/header/split-fan-out",
    "iso-8859-1/iso-8859-1/multi/no-header/file",
    "iso-8859-1/iso-8859-1/multi/no-header/files",
    "iso-8859-1/iso-8859-1/multi/no-header/correlation",
    "iso-8859-1/iso-8859-1/multi/no-header/split",
    "iso-8859-1/iso-8859-1/multi/no-header/fan-out",
    "iso-8859-1/iso-8859-1/multi/no-header/split-fan-out",
];

fn matrix_pipeline(input: &str, output: &str, multi: bool, header: bool, route: &str) -> String {
    let mut yaml = simple_pipeline(input, output, header);
    yaml.push_str(&format!(
        "      include_header: {header}\n      mapping: [label]\n      include_unmapped: false\n"
    ));
    if multi {
        let source_name = if header { "label" } else { "col_0" };
        yaml = yaml.replace(
            &format!("        - {{ name: label, source_name: {source_name}, type: string }}"),
            "        discriminator: { field: marker }\n        records:\n          - id: detail\n            tag: Dé\n            columns:\n              - { name: marker, type: string }\n              - { name: label, type: string }",
        );
    }
    if matches!(route, "files" | "fan-out" | "split-fan-out") {
        yaml = yaml.replace("path: input.csv", "glob: input-*.csv");
    }
    if route == "correlation" {
        yaml = yaml.replace(
            "      path: input.csv",
            "      correlation_key: label\n      path: input.csv",
        );
    }
    if matches!(route, "fan-out" | "split-fan-out") {
        yaml = yaml.replace("path: output.csv", "path: output_{source_file}.csv");
    }
    if matches!(route, "split" | "split-fan-out") {
        yaml.push_str("      split: { max_records: 1 }\n");
    }
    yaml
}

fn assert_completed(output: &Output, rows: u64, written: u64, dlq: u64) {
    let outcome = terminal(output, if dlq == 0 { 0 } else { 2 }, "completed");
    assert_eq!(
        outcome["result"],
        if dlq == 0 {
            "success"
        } else {
            "completed_with_dlq"
        }
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!(
            "Pipeline complete: {rows} total, {} ok, {written} written, {dlq} dlq",
            rows - dlq
        )),
        "{stderr}"
    );
}

#[test]
fn csv_cli_expected_rows_equal_executed_rows() {
    use std::collections::BTreeSet;
    let expected: BTreeSet<_> = EXPECTED_CLI_ROWS.iter().copied().collect();
    assert_eq!(expected.len(), 96);
    let mut executed = BTreeSet::new();
    for input_encoding in ["utf-8", "iso-8859-1"] {
        for output_encoding in ["utf-8", "iso-8859-1"] {
            for multi in [false, true] {
                for header in [true, false] {
                    for route in [
                        "file",
                        "files",
                        "correlation",
                        "split",
                        "fan-out",
                        "split-fan-out",
                    ] {
                        let id = format!(
                            "{input_encoding}/{output_encoding}/{}/{}/{route}",
                            if multi { "multi" } else { "single" },
                            if header { "header" } else { "no-header" }
                        );
                        let root = tempfile::tempdir().unwrap();
                        std::fs::write(
                            root.path().join("pipeline.yaml"),
                            matrix_pipeline(input_encoding, output_encoding, multi, header, route),
                        )
                        .unwrap();
                        let (heading, body): (&[u8], &[u8]) = match (input_encoding, multi) {
                            ("utf-8", false) => {
                                (b"\xef\xbb\xbflabel\n", "Café\nCrème\n".as_bytes())
                            }
                            ("utf-8", true) => (
                                b"\xef\xbb\xbfmarker,label\n",
                                "Dé,Café\nDé,Crème\n".as_bytes(),
                            ),
                            (_, false) => (b"label\n", b"Caf\xe9\nCr\xe8me\n"),
                            (_, true) => (b"marker,label\n", b"D\xe9,Caf\xe9\nD\xe9,Cr\xe8me\n"),
                        };
                        let mut bytes = if header {
                            heading.to_vec()
                        } else if input_encoding == "utf-8" {
                            b"\xef\xbb\xbf".to_vec()
                        } else {
                            Vec::new()
                        };
                        bytes.extend_from_slice(body);
                        let multiple_files = matches!(route, "files" | "fan-out" | "split-fan-out");
                        let names: &[&str] = if multiple_files {
                            &["input-a.csv", "input-b.csv"]
                        } else {
                            &["input.csv"]
                        };
                        for name in names {
                            std::fs::write(root.path().join(name), &bytes).unwrap();
                        }
                        let output = run(root.path());
                        assert_completed(
                            &output,
                            if multiple_files { 4 } else { 2 },
                            if multiple_files { 4 } else { 2 },
                            0,
                        );
                        let (first, second): (&[u8], &[u8]) = match (output_encoding, header) {
                            ("utf-8", true) => {
                                ("label\nCafé\n".as_bytes(), "label\nCrème\n".as_bytes())
                            }
                            ("utf-8", false) => ("Café\n".as_bytes(), "Crème\n".as_bytes()),
                            (_, true) => (b"label\nCaf\xe9\n", b"label\nCr\xe8me\n"),
                            (_, false) => (b"Caf\xe9\n", b"Cr\xe8me\n"),
                        };
                        let pair = match (output_encoding, header) {
                            ("utf-8", true) => "label\nCafé\nCrème\n".as_bytes(),
                            ("utf-8", false) => "Café\nCrème\n".as_bytes(),
                            (_, true) => b"label\nCaf\xe9\nCr\xe8me\n",
                            (_, false) => b"Caf\xe9\nCr\xe8me\n",
                        };
                        let outputs: Vec<(String, Vec<u8>)> = match route {
                            "split" => vec![
                                ("output_0001.csv".into(), first.into()),
                                ("output_0002.csv".into(), second.into()),
                            ],
                            "fan-out" => vec![
                                ("output_input-a.csv".into(), pair.into()),
                                ("output_input-b.csv".into(), pair.into()),
                            ],
                            "split-fan-out" => vec![
                                ("output_input-a_0001.csv".into(), first.into()),
                                ("output_input-a_0002.csv".into(), second.into()),
                                ("output_input-b_0001.csv".into(), first.into()),
                                ("output_input-b_0002.csv".into(), second.into()),
                            ],
                            "files" => {
                                let body = match output_encoding {
                                    "utf-8" => "Café\nCrème\n".as_bytes(),
                                    _ => b"Caf\xe9\nCr\xe8me\n",
                                };
                                vec![("output.csv".into(), [pair, body].concat())]
                            }
                            _ => vec![("output.csv".into(), pair.into())],
                        };
                        for (name, bytes) in outputs {
                            assert_eq!(
                                std::fs::read(root.path().join(&name)).unwrap(),
                                bytes,
                                "{id}: {name}"
                            );
                        }
                        assert!(executed.insert(id));
                    }
                }
            }
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        expected
    );
}
fn run(root: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(root)
        .args([
            "run",
            "pipeline.yaml",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "csv-contract",
        ])
        .output()
        .expect("execute compiled CLI")
}

fn terminal(output: &Output, exit: i32, event: &str) -> serde_json::Value {
    assert_eq!(
        output.status.code(),
        Some(exit),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout.clone())
        .expect("machine output is UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("machine event"))
        .collect();
    assert_eq!(
        events
            .iter()
            .filter(|value| matches!(
                value["event"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ))
            .count(),
        1
    );
    let last = events.last().expect("terminal");
    assert_eq!(last["event"], event, "{last}");
    last.clone()
}

fn simple_pipeline(input_encoding: &str, output_encoding: &str, header: bool) -> String {
    let source_name = if header { "label" } else { "col_0" };
    format!(
        r#"pipeline: {{ name: csv_encoding }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      options: {{ encoding: {input_encoding}, has_header: {header} }}
      schema:
        - {{ name: label, source_name: {source_name}, type: string }}
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: output.csv
      options: {{ encoding: {output_encoding} }}
"#
    )
}

#[test]
fn encoding_cli_csv_tracer() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("pipeline.yaml"),
        simple_pipeline("iso-8859-1", "utf-8", true),
    )
    .unwrap();
    std::fs::write(root.path().join("input.csv"), b"label\nCaf\xe9\n").unwrap();
    let outcome = terminal(&run(root.path()), 0, "completed");
    assert_eq!(outcome["result"], "success");
    assert_eq!(
        std::fs::read(root.path().join("output.csv")).unwrap(),
        "label\nCafé\n".as_bytes()
    );
}

const EXPECTED_CELL_ROWS: &[&str] = &[
    "utf-8/empty-stream",
    "utf-8/empty-cell",
    "utf-8/null-cell",
    "utf-8/adjacent-cells",
    "utf-8/multiline",
    "utf-8/joined",
    "utf-8/json",
    "iso-8859-1/empty-stream",
    "iso-8859-1/empty-cell",
    "iso-8859-1/null-cell",
    "iso-8859-1/adjacent-cells",
    "iso-8859-1/multiline",
    "iso-8859-1/joined",
    "iso-8859-1/json",
];

#[test]
fn csv_cli_cells_have_literal_bytes_and_counts() {
    use std::collections::BTreeSet;
    let mut executed = BTreeSet::new();
    for encoding in ["utf-8", "iso-8859-1"] {
        for kind in [
            "empty-stream",
            "empty-cell",
            "null-cell",
            "adjacent-cells",
            "multiline",
            "joined",
            "json",
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut yaml = simple_pipeline("utf-8", encoding, true);
            let (input, utf8, latin1, count): (&[u8], &[u8], &[u8], u64) = match kind {
                "empty-stream" => (b"label\n", b"", b"", 0),
                "empty-cell" => (b"label\n\"\"\n", b"label\n\"\"\n", b"label\n\"\"\n", 1),
                "null-cell" => {
                    yaml = yaml.replace("  - type: sink", "  - type: transform\n    name: clear\n    input: rows\n    config:\n      cxl: 'emit label = null'\n  - type: sink").replace("    input: rows\n    config:\n      name: out", "    input: clear\n    config:\n      name: out");
                    (b"label\nvalue\n", b"label\n\"\"\n", b"label\n\"\"\n", 1)
                }
                "adjacent-cells" => {
                    yaml = yaml.replace(
                        "  - type: sink",
                        "        - { name: other, type: string }\n  - type: sink",
                    );
                    (
                        b"label,other\n,\n",
                        b"label,other\n,\n",
                        b"label,other\n,\n",
                        1,
                    )
                }
                "multiline" => (
                    "label\n\"Café,\"\"line\nnext\r\n\u{85}\"\n".as_bytes(),
                    "label\n\"Café,\"\"line\nnext\r\n\u{85}\"\n".as_bytes(),
                    b"label\n\"Caf\xe9,\"\"line\nnext\r\n\x85\"\n",
                    1,
                ),
                "joined" => {
                    yaml = yaml.replace("      schema:", "      split_values: [{ field: label, delimiter: ';', escape: '\\' }]\n      schema:").replace("type: string }", "type: string, multiple: true }");
                    yaml.push_str("      join_values: [{ field: label, on_conflict: escape, escape: '\\' }]\n");
                    (
                        "label\nCafé\\;x;Crème\n".as_bytes(),
                        "label\nCafé\\;x;Crème\n".as_bytes(),
                        b"label\nCaf\xe9\\;x;Cr\xe8me\n",
                        1,
                    )
                }
                "json" => {
                    yaml = yaml
                        .replace(
                            "      schema:",
                            "      split_values: [{ field: label, json: true }]\n      schema:",
                        )
                        .replace("type: string }", "type: string, multiple: true }");
                    yaml.push_str(
                        "      join_values: [{ field: label, on_conflict: encode_json }]\n",
                    );
                    (
                        "label\n\"[\"\"Café\"\",\"\"\"\",\"\"Crème\"\"]\"\n".as_bytes(),
                        "label\n\"[\"\"Café\"\",\"\"\"\",\"\"Crème\"\"]\"\n".as_bytes(),
                        b"label\n\"[\"\"Caf\xe9\"\",\"\"\"\",\"\"Cr\xe8me\"\"]\"\n",
                        1,
                    )
                }
                _ => unreachable!(),
            };
            std::fs::write(root.path().join("pipeline.yaml"), yaml).unwrap();
            std::fs::write(root.path().join("input.csv"), input).unwrap();
            assert_completed(&run(root.path()), count, count, 0);
            let id = format!("{encoding}/{kind}");
            assert_eq!(
                std::fs::read(root.path().join("output.csv")).unwrap(),
                if encoding == "utf-8" { utf8 } else { latin1 },
                "{id}"
            );
            assert!(executed.insert(id));
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        EXPECTED_CELL_ROWS.iter().copied().collect()
    );
    assert_eq!(executed.len(), 14);
}

const EXPECTED_REJECTION_ROWS: &[&str] = &[
    "single/invalid-header",
    "single/invalid-body",
    "single/invalid-late-multiline",
    "multi/invalid-header",
    "multi/invalid-body",
    "multi/invalid-late-multiline",
    "unsupported-source",
    "unsupported-sink",
    "unrepresentable-header",
    "unrepresentable-body",
    "multi/split-values",
    "multi/multiple",
];

#[test]
fn csv_cli_rejections_are_explicit_and_never_publish_output() {
    use std::collections::BTreeSet;
    let mut executed = BTreeSet::new();
    for multi in [false, true] {
        for (kind, single, multiple) in [
            (
                "invalid-header",
                b"label\xff\nok\n".as_slice(),
                b"marker,label\xff\nD\xc3\xa9,ok\n".as_slice(),
            ),
            (
                "invalid-body",
                b"label\nlate\xff\n".as_slice(),
                b"marker,label\nD\xc3\xa9,late\xff\n".as_slice(),
            ),
            (
                "invalid-late-multiline",
                b"label\n\"first\nlate\xff\"\n".as_slice(),
                b"marker,label\nD\xc3\xa9,\"first\nlate\xff\"\n".as_slice(),
            ),
        ] {
            let id = format!("{}/{kind}", if multi { "multi" } else { "single" });
            // Header discovery and body decoding both consume source bytes;
            // their malformed UTF-8 has the same data-failure classification.
            reject(
                &matrix_pipeline("utf-8", "utf-8", multi, true, "file"),
                if multi { multiple } else { single },
                4,
                "source.data.invalid",
                "UTF-8",
            );
            assert!(executed.insert(id));
        }
    }
    for (id, yaml, input, exit, code, diagnostic) in [
        (
            "unsupported-source",
            simple_pipeline("shift_jis", "utf-8", true),
            b"label\nok\n".as_slice(),
            1,
            "admission.configuration.invalid",
            "shift_jis",
        ),
        (
            "unsupported-sink",
            simple_pipeline("utf-8", "shift_jis", true),
            b"label\nok\n".as_slice(),
            1,
            "admission.configuration.invalid",
            "shift_jis",
        ),
        (
            "unrepresentable-header",
            simple_pipeline("utf-8", "iso-8859-1", true).replace("label", "€"),
            "€\nok\n".as_bytes(),
            4,
            "source.data.invalid",
            "ISO-8859-1",
        ),
        (
            "unrepresentable-body",
            simple_pipeline("utf-8", "iso-8859-1", true),
            "label\n€\n".as_bytes(),
            4,
            "source.data.invalid",
            "ISO-8859-1",
        ),
        (
            "multi/split-values",
            matrix_pipeline("utf-8", "utf-8", true, true, "file").replace(
                "      schema:",
                "      split_values: [{ field: label }]\n      schema:",
            ),
            b"marker,label\n".as_slice(),
            1,
            "admission.configuration.invalid",
            "E358",
        ),
        (
            "multi/multiple",
            matrix_pipeline("utf-8", "utf-8", true, true, "file").replace(
                "name: label, type: string",
                "name: label, type: string, multiple: true",
            ),
            b"marker,label\n".as_slice(),
            1,
            "admission.configuration.invalid",
            "E361",
        ),
    ] {
        reject(&yaml, input, exit, code, diagnostic);
        assert!(executed.insert(id.into()));
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        EXPECTED_REJECTION_ROWS.iter().copied().collect()
    );
    assert_eq!(executed.len(), 12);
}

fn reject(yaml: &str, input: &[u8], exit: i32, code: &str, diagnostic: &str) {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("pipeline.yaml"), yaml).unwrap();
    std::fs::write(root.path().join("input.csv"), input).unwrap();
    let output = run(root.path());
    let outcome = terminal(&output, exit, "failed");
    assert_eq!(outcome["failure"]["code"], code, "{outcome}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(diagnostic),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!root.path().join("output.csv").exists());
    // Charset failure precedes delivery. This is distinct from the irreversible
    // prefix failure injected at the raw destination in writer_resources.
    fn assert_empty_partials(path: &Path) -> usize {
        let mut found = 0;
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                found += assert_empty_partials(&entry.path());
            } else if entry.file_name().to_string_lossy().ends_with(".partial")
                || entry.file_name().to_string_lossy().starts_with("artifact-")
            {
                assert_eq!(entry.metadata().unwrap().len(), 0);
                found += 1;
            }
        }
        found
    }
    let partials = assert_empty_partials(root.path());
    if diagnostic == "ISO-8859-1" {
        assert!(
            partials > 0,
            "encoding failure retains an actual empty staged artifact"
        );
    }
}

const EXPECTED_DLQ_ROWS: &[&str] = &[
    "utf-8/single",
    "utf-8/multi",
    "iso-8859-1/single",
    "iso-8859-1/multi",
];

#[test]
fn csv_cli_continue_reports_exact_rejected_population_and_category() {
    use clinker_format::FormatReader;
    use std::collections::BTreeSet;
    let mut executed = BTreeSet::new();
    for encoding in ["utf-8", "iso-8859-1"] {
        for multi in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut yaml = matrix_pipeline(encoding, "utf-8", multi, true, "file");
            yaml = yaml.replace(
                "nodes:",
                "error_handling:\n  strategy: continue\n  dlq: { path: rejected.csv }\nnodes:",
            );
            yaml = yaml.replace(
                "      schema:",
                "      dlq_granularity: record\n      schema:",
            );
            if !multi {
                yaml = yaml.replace("type: string", "type: int");
            }
            let input: &[u8] = match (encoding, multi) {
                ("utf-8", false) => "label\n1\nrefusé\n2\n".as_bytes(),
                ("utf-8", true) => "marker,label\nDé,Café\nXé,refusé\nDé,Crème\n".as_bytes(),
                (_, false) => b"label\n1\nrefus\xe9\n2\n",
                (_, true) => b"marker,label\nD\xe9,Caf\xe9\nX\xe9,refus\xe9\nD\xe9,Cr\xe8me\n",
            };
            std::fs::write(root.path().join("pipeline.yaml"), yaml).unwrap();
            std::fs::write(root.path().join("input.csv"), input).unwrap();
            assert_completed(&run(root.path()), 3, 2, 1);
            assert_eq!(
                std::fs::read(root.path().join("output.csv")).unwrap(),
                if multi {
                    "label\nCafé\nCrème\n".as_bytes()
                } else {
                    b"label\n1\n2\n"
                }
            );
            // The DLQ has dynamic UUID/timestamp metadata. Parse it with the
            // existing format reader, checking the stable category/population.
            let mut dlq = clinker_format::csv::CsvReader::from_reader(
                std::fs::File::open(root.path().join("rejected.csv")).unwrap(),
                Default::default(),
            );
            let row = dlq.next_record().unwrap().unwrap();
            assert_eq!(
                row.get("_cxl_dlq_source_row"),
                Some(&clinker_record::Value::from("2"))
            );
            assert_eq!(
                row.get("_cxl_dlq_error_category"),
                Some(&clinker_record::Value::from(if multi {
                    "structural_validation"
                } else {
                    "type_coercion_failure"
                }))
            );
            assert!(dlq.next_record().unwrap().is_none());
            assert!(executed.insert(format!(
                "{encoding}/{}",
                if multi { "multi" } else { "single" }
            )));
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        EXPECTED_DLQ_ROWS.iter().copied().collect()
    );
    assert_eq!(executed.len(), 4);
}

const EXPECTED_NESTED_OUTPUT_ROWS: &[&str] = &[
    "json-array/compact/0",
    "json-array/compact/1",
    "json-array/compact/2",
    "json-array/pretty/0",
    "json-array/pretty/1",
    "json-array/pretty/2",
    "ndjson/compact/0",
    "ndjson/compact/1",
    "ndjson/compact/2",
    "ndjson/pretty/0",
    "ndjson/pretty/1",
    "ndjson/pretty/2",
    "json-envelope-array/compact/0",
    "json-envelope-array/compact/1",
    "json-envelope-array/compact/2",
    "json-envelope-array/compact/two-documents",
    "json-envelope-array/pretty/0",
    "json-envelope-array/pretty/1",
    "json-envelope-array/pretty/2",
    "json-envelope-array/pretty/two-documents",
    "json-envelope-ndjson/compact/0",
    "json-envelope-ndjson/compact/1",
    "json-envelope-ndjson/compact/2",
    "json-envelope-ndjson/compact/two-documents",
    "json-envelope-ndjson/pretty/0",
    "json-envelope-ndjson/pretty/1",
    "json-envelope-ndjson/pretty/2",
    "json-envelope-ndjson/pretty/two-documents",
    "xml/compact/0",
    "xml/compact/1",
    "xml/compact/2",
    "xml-envelope/compact/0",
    "xml-envelope/compact/1",
    "xml-envelope/compact/2",
    "xml-envelope/compact/two-documents",
];

#[test]
fn nested_output_framing_rows_equal_literal_files_and_process_counts() {
    use std::collections::BTreeSet;
    let mut executed = BTreeSet::new();
    for codec in [
        "json-array",
        "ndjson",
        "json-envelope-array",
        "json-envelope-ndjson",
        "xml",
        "xml-envelope",
    ] {
        let xml = codec.starts_with("xml");
        let envelope = codec.contains("envelope");
        for pretty in [false, true] {
            if xml && pretty {
                continue;
            }
            for population in ["0", "1", "2", "two-documents"] {
                let documents = if population == "two-documents" { 2 } else { 1 };
                if documents == 2 && !envelope {
                    continue;
                }
                let rows = if documents == 2 {
                    1
                } else {
                    population.parse::<usize>().unwrap()
                };
                let id = format!(
                    "{codec}/{}/{population}",
                    if pretty { "pretty" } else { "compact" }
                );
                let root = tempfile::tempdir().unwrap();
                let source_path = if documents == 2 {
                    "glob: input-*.json"
                } else {
                    "path: input-1.json"
                };
                let source_envelope = if envelope {
                    r#"      options: { record_path: items }
      envelope:
        sections:
          opening:
            extract: { json_pointer: "/opening" }
            fields: { tag: int }
          closing:
            extract: { json_pointer: "/closing" }
            fields: { status: string }
"#
                } else {
                    ""
                };
                let sink_options = if xml {
                    if envelope {
                        r#"      options:
        envelope:
          header_from_doc: opening
          footer_from_doc: closing
          footer_record_count_field: rows
"#
                        .to_owned()
                    } else {
                        String::new()
                    }
                } else {
                    let mode = if codec.contains("ndjson") {
                        "ndjson"
                    } else {
                        "array"
                    };
                    format!(
                        "      options:\n        format: {mode}\n        pretty: {pretty}\n{}",
                        if envelope {
                            "        envelope:\n          header_from_doc: opening\n          footer_from_doc: closing\n          footer_record_count_field: rows\n"
                        } else {
                            ""
                        }
                    )
                };
                let format = if xml { "xml" } else { "json" };
                let yaml = format!(
                    r#"
pipeline:
  name: native_framing
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: json
      {source_path}
{source_envelope}      schema: [{{ name: v, type: int }}]
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: {format}
      path: output.{format}
      reconstruct_envelope: {envelope}
{sink_options}"#
                );
                std::fs::write(root.path().join("pipeline.yaml"), yaml).unwrap();
                for document in 1..=documents {
                    let body = match (rows, document) {
                        (0, _) => "",
                        (1, 1) => r#"{"v":1}"#,
                        (1, 2) => r#"{"v":2}"#,
                        (2, _) => r#"{"v":1},{"v":2}"#,
                        _ => unreachable!(),
                    };
                    let input = if envelope {
                        format!(
                            r#"{{"opening":{{"tag":7}},"items":[{body}],"closing":{{"status":"done"}}}}"#
                        )
                    } else {
                        format!("[{body}]")
                    };
                    std::fs::write(root.path().join(format!("input-{document}.json")), input)
                        .unwrap();
                }
                let result = run(root.path());
                assert_completed(
                    &result,
                    (rows * documents) as u64,
                    (rows * documents) as u64,
                    0,
                );
                // Runtime writers open on the first body row. An empty run
                // publishes the existing empty file; it never constructs an
                // encoder whose explicit Finalize would emit empty framing.
                let expected = if rows == 0 {
                    String::new()
                } else if xml {
                    let body = match rows {
                        0 => "",
                        1 => "<Record><v>1</v></Record>",
                        2 => "<Record><v>1</v></Record><Record><v>2</v></Record>",
                        _ => unreachable!(),
                    };
                    if envelope {
                        let doc = format!(
                            "<Document><header><tag>7</tag></header>{body}<footer><status>done</status><rows>{rows}</rows></footer></Document>"
                        );
                        if documents == 2 {
                            format!(
                                "<Root>{doc}<Document><header><tag>7</tag></header><Record><v>2</v></Record><footer><status>done</status><rows>1</rows></footer></Document></Root>"
                            )
                        } else {
                            format!("<Root>{doc}</Root>")
                        }
                    } else {
                        format!("<Root>{body}</Root>")
                    }
                } else if envelope {
                    let header = if pretty {
                        "{\n  \"tag\": 7\n}"
                    } else {
                        r#"{"tag":7}"#
                    };
                    let body = match (rows, pretty) {
                        (0, _) => "",
                        (1, false) => r#"{"v":1}"#,
                        (1, true) => "{\n  \"v\": 1\n}",
                        (2, false) => r#"{"v":1},{"v":2}"#,
                        (2, true) => "{\n  \"v\": 1\n},{\n  \"v\": 2\n}",
                        _ => unreachable!(),
                    };
                    let footer = if pretty {
                        format!("{{\n  \"status\": \"done\",\n  \"rows\": {rows}\n}}")
                    } else {
                        format!(r#"{{"status":"done","rows":{rows}}}"#)
                    };
                    let first =
                        format!("{{\"header\":{header},\"body\":[{body}],\"footer\":{footer}}}");
                    let doc = if documents == 2 {
                        let body = if pretty {
                            "{\n  \"v\": 2\n}"
                        } else {
                            r#"{"v":2}"#
                        };
                        format!(
                            "{first}{}{{\"header\":{header},\"body\":[{body}],\"footer\":{footer}}}",
                            if codec.ends_with("ndjson") {
                                "\n"
                            } else {
                                ",\n"
                            }
                        )
                    } else {
                        first
                    };
                    if codec.ends_with("ndjson") {
                        doc
                    } else {
                        format!("[\n{doc}\n]\n")
                    }
                } else if codec == "ndjson" {
                    match rows {
                        0 => "",
                        1 => "{\"v\":1}\n",
                        2 => "{\"v\":1}\n{\"v\":2}\n",
                        _ => unreachable!(),
                    }
                    .to_owned()
                } else {
                    match (rows, pretty) {
                        (0, _) => "[]\n",
                        (1, false) => "[\n{\"v\":1}\n]\n",
                        (1, true) => "[\n{\n  \"v\": 1\n}\n]\n",
                        (2, false) => "[\n{\"v\":1},\n{\"v\":2}\n]\n",
                        (2, true) => "[\n{\n  \"v\": 1\n},\n{\n  \"v\": 2\n}\n]\n",
                        _ => unreachable!(),
                    }
                    .to_owned()
                };
                assert_eq!(
                    std::fs::read(root.path().join(format!("output.{format}"))).unwrap(),
                    expected.as_bytes(),
                    "{id}"
                );
                assert!(executed.insert(id));
            }
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        EXPECTED_NESTED_OUTPUT_ROWS.iter().copied().collect()
    );
    assert_eq!(executed.len(), 35);
}

const EXPECTED_NESTED_INPUT_ROWS: &[&str] = &[
    "json-array/utf8",
    "json-array/bom8",
    "json-array/bom16le",
    "json-array/bom16be",
    "json-array/bom32le",
    "json-array/bom32be",
    "json-array/malformed-first",
    "json-array/malformed-late",
    "json-array/two-files",
    "json-array/invalid-second",
    "json-ndjson/utf8",
    "json-ndjson/bom8",
    "json-ndjson/bom16le",
    "json-ndjson/bom16be",
    "json-ndjson/bom32le",
    "json-ndjson/bom32be",
    "json-ndjson/malformed-first",
    "json-ndjson/malformed-late",
    "json-ndjson/two-files",
    "json-ndjson/invalid-second",
    "json-body/utf8",
    "json-body/bom8",
    "json-body/bom16le",
    "json-body/bom16be",
    "json-body/bom32le",
    "json-body/bom32be",
    "json-body/malformed-first",
    "json-body/malformed-late",
    "json-body/two-files",
    "json-body/invalid-second",
    "xml-ordinary/utf8",
    "xml-ordinary/bom8",
    "xml-ordinary/bom16le",
    "xml-ordinary/bom16be",
    "xml-ordinary/bom32le",
    "xml-ordinary/bom32be",
    "xml-ordinary/malformed-first",
    "xml-ordinary/malformed-late",
    "xml-ordinary/two-files",
    "xml-ordinary/invalid-second",
    "xml-ordinary/declaration8",
    "xml-ordinary/declaration16",
    "xml-ordinary/conflicting-declaration",
    "xml-body/utf8",
    "xml-body/bom8",
    "xml-body/bom16le",
    "xml-body/bom16be",
    "xml-body/bom32le",
    "xml-body/bom32be",
    "xml-body/malformed-first",
    "xml-body/malformed-late",
    "xml-body/two-files",
    "xml-body/invalid-second",
    "xml-body/declaration8",
    "xml-body/declaration16",
    "xml-body/conflicting-declaration",
    "xml-envelope/utf8",
    "xml-envelope/bom8",
    "xml-envelope/bom16le",
    "xml-envelope/bom16be",
    "xml-envelope/bom32le",
    "xml-envelope/bom32be",
    "xml-envelope/malformed-first",
    "xml-envelope/malformed-late",
    "xml-envelope/two-files",
    "xml-envelope/invalid-second",
    "xml-envelope/declaration8",
    "xml-envelope/declaration16",
    "xml-envelope/conflicting-declaration",
    "xml-prescan/utf8",
    "xml-prescan/bom8",
    "xml-prescan/bom16le",
    "xml-prescan/bom16be",
    "xml-prescan/bom32le",
    "xml-prescan/bom32be",
    "xml-prescan/malformed-first",
    "xml-prescan/malformed-late",
    "xml-prescan/two-files",
    "xml-prescan/invalid-second",
    "xml-prescan/declaration8",
    "xml-prescan/declaration16",
    "xml-prescan/conflicting-declaration",
];

fn nested_input_yaml(mode: &str, multiple: bool) -> String {
    let xml = mode.starts_with("xml");
    let format = if xml { "xml" } else { "json" };
    let path = if multiple {
        format!("glob: input-*.{format}")
    } else {
        format!("path: input-1.{format}")
    };
    let options = match mode {
        "json-ndjson" => "      options: { format: ndjson }\n",
        "json-body" => "      options: { record_path: items }\n",
        "xml-body" | "xml-envelope" | "xml-prescan" => {
            "      options: { record_path: Root/items/row }\n"
        }
        _ => "",
    };
    let envelope = if matches!(mode, "xml-envelope" | "xml-prescan") {
        r#"      envelope:
        sections:
          manifest:
            extract: { xml_path: "/Root/manifest" }
            fields: { batch: int }
"#
    } else {
        ""
    };
    let transform = if mode == "xml-prescan" {
        r#"  - type: transform
    name: attach
    input: rows
    config:
      cxl: |
        emit id = id
        emit batch = $doc.manifest.batch
"#
    } else {
        ""
    };
    let upstream = if mode == "xml-prescan" {
        "attach"
    } else {
        "rows"
    };
    format!(
        r#"
pipeline:
  name: native_input
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: {format}
      {path}
{options}{envelope}      schema: [{{ name: id, type: int }}]
{transform}  - type: sink
    name: out
    input: {upstream}
    config:
      name: out
      type: json
      path: output.json
      options: {{ format: ndjson }}
"#
    )
}

fn nested_input_bytes(mode: &str, id: u8, malformed: bool) -> Vec<u8> {
    let text = match mode {
        "json-array" => format!(r#"[{{"id":{id}}}]"#),
        "json-ndjson" => format!("{{\"id\":{id}}}\n"),
        "json-body" => {
            format!(r#"{{"metadata":{{"id":99}},"items":[{{"id":{id}}}],"after":{{"id":98}}}}"#)
        }
        "xml-ordinary" => format!("<row><id>{id}</id></row>"),
        "xml-prescan" => format!(
            "<Root><items><row><id>{id}</id></row></items><manifest><batch>7</batch></manifest></Root>"
        ),
        _ => format!(
            "<Root><manifest><batch>7</batch></manifest><items><row><id>{id}</id></row></items><metadata><id>99</id></metadata></Root>"
        ),
    };
    if !malformed {
        return text.into_bytes();
    }
    let marker = if mode.starts_with("json") {
        format!("\"id\":{id}")
    } else {
        format!("<id>{id}</id>")
    };
    let mut bytes = text.into_bytes();
    let start = bytes
        .windows(marker.len())
        .position(|window| window == marker.as_bytes())
        .unwrap();
    let offset = start + if mode.starts_with("json") { 5 } else { 4 };
    bytes[offset] = 0xff;
    bytes
}

fn nested_partial_outputs(root: &Path) -> Vec<Vec<u8>> {
    let mut outputs = Vec::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            outputs.extend(nested_partial_outputs(&entry.path()));
        } else if entry.file_name().to_string_lossy().starts_with("artifact-")
            || entry.file_name().to_string_lossy().ends_with(".partial")
        {
            outputs.push(std::fs::read(entry.path()).unwrap());
        }
    }
    outputs
}

#[test]
fn nested_input_rows_enforce_utf8_and_independent_physical_file_policy() {
    use std::collections::BTreeSet;
    let mut executed = BTreeSet::new();
    for mode in [
        "json-array",
        "json-ndjson",
        "json-body",
        "xml-ordinary",
        "xml-body",
        "xml-envelope",
        "xml-prescan",
    ] {
        for variant in [
            "utf8",
            "bom8",
            "bom16le",
            "bom16be",
            "bom32le",
            "bom32be",
            "malformed-first",
            "malformed-late",
            "two-files",
            "invalid-second",
            "declaration8",
            "declaration16",
            "conflicting-declaration",
        ] {
            let xml = mode.starts_with("xml");
            if !xml && variant.contains("declaration") {
                continue;
            }
            let id = format!("{mode}/{variant}");
            let root = tempfile::tempdir().unwrap();
            let multiple = matches!(variant, "two-files" | "invalid-second");
            std::fs::write(
                root.path().join("pipeline.yaml"),
                nested_input_yaml(mode, multiple),
            )
            .unwrap();
            let mut bytes = nested_input_bytes(mode, 1, variant == "malformed-first");
            if variant == "malformed-late" {
                // Separate the complete first body record from corruption by
                // more than the reader buffer. This tests streaming progress,
                // without depending on where a short read happens to stop.
                let padding = " ".repeat(32 * 1024);
                bytes = match mode {
                    "json-array" => format!("[{{\"id\":1}},{padding}{{\"id\":2}}]").into_bytes(),
                    "json-ndjson" => format!("{{\"id\":1}}\n{padding}{{\"id\":2}}\n").into_bytes(),
                    "json-body" => format!("{{\"items\":[{{\"id\":1}},{padding}{{\"id\":2}}]}}").into_bytes(),
                    "xml-ordinary" => format!("<row><id>1</id>{padding}<tail>2</tail></row>").into_bytes(),
                    _ => format!("<Root><manifest><batch>7</batch></manifest><items><row><id>1</id></row>{padding}<row><id>2</id></row></items></Root>").into_bytes(),
                };
                let offset = bytes.iter().rposition(|byte| *byte == b'2').unwrap();
                bytes[offset] = 0xff;
            }
            let prefix: &[u8] = match variant {
                "bom8" | "two-files" | "invalid-second" => b"\xef\xbb\xbf",
                "bom16le" => b"\xff\xfe",
                "bom16be" => b"\xfe\xff",
                "bom32le" => b"\xff\xfe\0\0",
                "bom32be" => b"\0\0\xfe\xff",
                "declaration8" => b"<?xml version='1.0' encoding='UTF-8'?>",
                "declaration16" => b"<?xml version='1.0' encoding='UTF-16'?>",
                "conflicting-declaration" => {
                    b"<?xml version='1.0' encoding='UTF-8' encoding='UTF-16'?>"
                }
                _ => b"",
            };
            bytes.splice(..0, prefix.iter().copied());
            let format = if xml { "xml" } else { "json" };
            std::fs::write(root.path().join(format!("input-1.{format}")), bytes).unwrap();
            if multiple {
                let second = nested_input_bytes(mode, 2, variant == "invalid-second");
                std::fs::write(
                    root.path().join(format!("input-2.{format}")),
                    [b"\xef\xbb\xbf".as_slice(), &second].concat(),
                )
                .unwrap();
            }
            let result = run(root.path());
            let success = matches!(variant, "utf8" | "bom8" | "two-files" | "declaration8");
            let first = if mode == "xml-prescan" {
                b"{\"id\":1,\"batch\":7}\n".as_slice()
            } else {
                b"{\"id\":1}\n".as_slice()
            };
            if success {
                let rows = if multiple { 2 } else { 1 };
                assert_completed(&result, rows, rows, 0);
                let second = if mode == "xml-prescan" {
                    b"{\"id\":2,\"batch\":7}\n".as_slice()
                } else {
                    b"{\"id\":2}\n".as_slice()
                };
                let expected = [first, if multiple { second } else { b"" }].concat();
                assert_eq!(
                    std::fs::read(root.path().join("output.json")).unwrap(),
                    expected,
                    "{id}"
                );
            } else {
                let outcome = terminal(&result, 4, "failed");
                assert_eq!(
                    outcome["failure"]["code"], "source.data.invalid",
                    "{id}: {outcome}"
                );
                assert!(
                    !root.path().join("output.json").exists(),
                    "{id}: failed run must not publish"
                );
                let partials = nested_partial_outputs(root.path());
                let expected = if variant == "invalid-second"
                    || (variant == "malformed-late"
                        && !matches!(mode, "xml-ordinary" | "xml-prescan"))
                {
                    first
                } else {
                    b""
                };
                assert!(
                    partials.iter().any(|bytes| bytes == expected),
                    "{id}: retained prefix {partials:?}"
                );
                assert!(
                    partials
                        .iter()
                        .all(|bytes| bytes == expected || bytes.is_empty()),
                    "{id}: unexpected retained bytes"
                );
            }
            assert!(executed.insert(id));
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        EXPECTED_NESTED_INPUT_ROWS.iter().copied().collect()
    );
    assert_eq!(executed.len(), 82);
}

const EXPECTED_NESTED_OUTPUT_EDGE_ROWS: &[&str] = &[
    "json/native/omit",
    "json/native/preserve",
    "xml/native/omit",
    "xml/native/preserve",
    "json/depth",
    "xml/depth",
    "json/collision",
    "xml/collision",
    "xml/name",
    "xml/character",
    "json/correlation",
    "xml/correlation",
    "json/split",
    "xml/split",
    "json/fanout",
    "xml/fanout",
    "json/reject-split",
    "xml/reject-split",
    "json/reject-fanout",
    "xml/reject-fanout",
    "json/reject-correlation",
    "xml/reject-correlation",
    "json/reject-document-dlq",
    "xml/reject-document-dlq",
];

fn nested_transform_yaml(format: &str, expression: &str, preserve: bool) -> String {
    let options = if format == "json" {
        "      options: { format: ndjson }\n"
    } else {
        ""
    };
    let cxl = expression
        .lines()
        .map(|line| format!("        {line}\n"))
        .collect::<String>();
    format!(
        r#"
pipeline:
  name: native_values
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema: [{{ name: key, type: string }}]
  - type: transform
    name: construct
    input: rows
    config:
      cxl: |
{cxl}  - type: sink
    name: out
    input: construct
    config:
      name: out
      type: {format}
      path: output.{format}
      include_unmapped: false
      preserve_nulls: {preserve}
{options}"#
    )
}

#[test]
fn nested_output_edges_have_exact_bytes_rejections_and_publication() {
    use std::collections::BTreeSet;
    let mut executed = BTreeSet::new();
    for format in ["json", "xml"] {
        for preserve in [false, true] {
            let id = format!(
                "{format}/native/{}",
                if preserve { "preserve" } else { "omit" }
            );
            let expression = r##"emit payload = {"@kind": "event", "#text": "before", item: [{"@id": 2, "#text": "two"}], tail: "after"}
emit enabled = true
emit count = 2
emit amount = 1.5
emit absent = null"##;
            let root = tempfile::tempdir().unwrap();
            std::fs::write(
                root.path().join("pipeline.yaml"),
                nested_transform_yaml(format, expression, preserve),
            )
            .unwrap();
            std::fs::write(root.path().join("input.csv"), b"key\n@id\n").unwrap();
            assert_completed(&run(root.path()), 1, 1, 0);
            let expected: &[u8] = match (format, preserve) {
                ("json", false) => b"{\"payload\":{\"@kind\":\"event\",\"#text\":\"before\",\"item\":[{\"@id\":2,\"#text\":\"two\"}],\"tail\":\"after\"},\"enabled\":true,\"count\":2,\"amount\":1.5}\n",
                ("json", true) => b"{\"payload\":{\"@kind\":\"event\",\"#text\":\"before\",\"item\":[{\"@id\":2,\"#text\":\"two\"}],\"tail\":\"after\"},\"enabled\":true,\"count\":2,\"amount\":1.5,\"absent\":null}\n",
                (_, false) => b"<Root><Record><payload kind=\"event\">before<item id=\"2\">two</item><tail>after</tail></payload><enabled>true</enabled><count>2</count><amount>1.5</amount></Record></Root>",
                (_, true) => b"<Root><Record><payload kind=\"event\">before<item id=\"2\">two</item><tail>after</tail></payload><enabled>true</enabled><count>2</count><amount>1.5</amount><absent/></Record></Root>",
            };
            assert_eq!(
                std::fs::read(root.path().join(format!("output.{format}"))).unwrap(),
                expected,
                "{id}"
            );
            assert!(executed.insert(id));
        }
        for (kind, expression, exit, diagnostic) in [
            (
                "depth",
                format!("emit payload = {}null{}", "{n: ".repeat(65), "}".repeat(65)),
                3,
                "depth",
            ),
            (
                "collision",
                r#"emit payload = {"@id": 1, [key]: 2}"#.into(),
                3,
                "duplicate map key",
            ),
            (
                "name",
                r#"emit payload = {"1bad": 2}"#.into(),
                4,
                "XML name",
            ),
            ("character", "emit payload = key".into(), 4, "XML"),
        ] {
            if format == "json" && matches!(kind, "name" | "character") {
                continue;
            }
            let id = format!("{format}/{kind}");
            let root = tempfile::tempdir().unwrap();
            std::fs::write(
                root.path().join("pipeline.yaml"),
                nested_transform_yaml(format, &expression, false),
            )
            .unwrap();
            std::fs::write(
                root.path().join("input.csv"),
                if kind == "character" {
                    b"key\n\x01\n".as_slice()
                } else {
                    b"key\n@id\n".as_slice()
                },
            )
            .unwrap();
            let result = run(root.path());
            assert_eq!(
                terminal(&result, exit, "failed")["failure"]["code"],
                "source.data.invalid",
                "{id}"
            );
            let stderr = String::from_utf8_lossy(&result.stderr);
            assert!(stderr.contains(diagnostic), "{id}: {stderr}");
            assert!(stderr.len() < 4096, "{id}: bounded diagnostic");
            assert!(!root.path().join(format!("output.{format}")).exists());
            assert!(
                nested_partial_outputs(root.path())
                    .iter()
                    .all(Vec::is_empty),
                "{id}"
            );
            assert!(executed.insert(id));
        }
        for mode in [
            "correlation",
            "split",
            "fanout",
            "reject-split",
            "reject-fanout",
            "reject-correlation",
            "reject-document-dlq",
        ] {
            let id = format!("{format}/{mode}");
            let root = tempfile::tempdir().unwrap();
            let source = match mode {
                "correlation" | "reject-correlation" => "      correlation_key: id\n",
                "reject-document-dlq" => "      dlq_granularity: document\n",
                _ => "",
            };
            let path = if mode.ends_with("fanout") {
                format!("output_{{source_file}}.{format}")
            } else {
                format!("output.{format}")
            };
            let split = if mode.ends_with("split") {
                "      split: { max_records: 1 }\n"
            } else {
                ""
            };
            let envelope = if mode.starts_with("reject-") {
                "      reconstruct_envelope: true\n      options:\n        envelope:\n          footer_record_count_field: rows\n"
            } else {
                ""
            };
            let yaml = format!(
                r#"
pipeline:
  name: native_routing
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      glob: input-*.csv
{source}      schema: [{{ name: id, type: int }}]
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: {format}
      path: {path}
{split}{envelope}"#
            );
            std::fs::write(root.path().join("pipeline.yaml"), yaml).unwrap();
            std::fs::write(root.path().join("input-a.csv"), b"id\n1\n").unwrap();
            std::fs::write(root.path().join("input-b.csv"), b"id\n2\n").unwrap();
            let result = run(root.path());
            if mode.starts_with("reject-") {
                assert_eq!(
                    terminal(&result, 1, "failed")["failure"]["code"],
                    "admission.configuration.invalid",
                    "{id}"
                );
                assert!(
                    String::from_utf8_lossy(&result.stderr).contains("E347"),
                    "{id}"
                );
                assert!(nested_partial_outputs(root.path()).is_empty());
                assert!(!std::fs::read_dir(root.path()).unwrap().any(|entry| {
                    entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("output")
                }));
            } else {
                assert_completed(&result, 2, 2, 0);
                let expected: Vec<(&str, &[u8])> = match (format, mode) {
                    ("json", "correlation") => {
                        vec![("output.json", b"[\n{\"id\":1},\n{\"id\":2}\n]\n")]
                    }
                    ("xml", "correlation") => vec![(
                        "output.xml",
                        b"<Root><Record><id>1</id></Record><Record><id>2</id></Record></Root>",
                    )],
                    ("json", "split") => vec![
                        ("output_0001.json", b"[\n{\"id\":1}\n]\n"),
                        ("output_0002.json", b"[\n{\"id\":2}\n]\n"),
                    ],
                    ("xml", "split") => vec![
                        (
                            "output_0001.xml",
                            b"<Root><Record><id>1</id></Record></Root>",
                        ),
                        (
                            "output_0002.xml",
                            b"<Root><Record><id>2</id></Record></Root>",
                        ),
                    ],
                    ("json", _) => vec![
                        ("output_input-a.json", b"[\n{\"id\":1}\n]\n"),
                        ("output_input-b.json", b"[\n{\"id\":2}\n]\n"),
                    ],
                    (_, _) => vec![
                        (
                            "output_input-a.xml",
                            b"<Root><Record><id>1</id></Record></Root>",
                        ),
                        (
                            "output_input-b.xml",
                            b"<Root><Record><id>2</id></Record></Root>",
                        ),
                    ],
                };
                for (name, bytes) in expected {
                    assert_eq!(
                        std::fs::read(root.path().join(name)).unwrap(),
                        bytes,
                        "{id}/{name}"
                    );
                }
            }
            assert!(executed.insert(id));
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        EXPECTED_NESTED_OUTPUT_EDGE_ROWS.iter().copied().collect()
    );
    assert_eq!(executed.len(), 24);
}

#[test]
fn nested_input_xml_metadata_never_inflates_selected_body_count() {
    for mode in ["xml-body", "xml-envelope", "xml-prescan"] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("pipeline.yaml"),
            nested_input_yaml(mode, false)
                .replace("name: id, type: int", "name: id, type: { nullable: int }"),
        )
        .unwrap();
        std::fs::write(root.path().join("input-1.xml"), b"<Root><metadata><id>99</id></metadata><items><row><id>1</id></row><row/></items><manifest><batch>7</batch></manifest><items/><items><row><id>2</id></row></items><metadata><id>98</id></metadata></Root>").unwrap();
        assert_completed(&run(root.path()), 3, 3, 0);
        let expected: &[u8] = if mode == "xml-prescan" {
            b"{\"id\":1,\"batch\":7}\n{\"batch\":7}\n{\"id\":2,\"batch\":7}\n"
        } else {
            b"{\"id\":1}\n{}\n{\"id\":2}\n"
        };
        assert_eq!(
            std::fs::read(root.path().join("output.json")).unwrap(),
            expected,
            "{mode}"
        );
    }
}

const EXPECTED_PHYSICAL_ROUTES: &[&str] = &[
    "fixed_width/file",
    "fixed_width/files",
    "fixed_width/fanout",
    "fixed_width/split",
    "fixed_width/split-fanout",
    "fixed_width/empty",
    "swift/file",
    "swift/files",
    "swift/fanout",
    "swift/empty",
    "swift/reconstruct",
    "swift/reject-files",
    "swift/reconstruct-fanout",
    "swift/reject-split",
    "swift/reject-split-fanout",
];

fn assert_physical_completed(output: &Output, rows: u64, artifacts: u64) {
    assert_completed(output, rows, rows, 0);
    let outcome = terminal(output, 0, "completed");
    assert_eq!(outcome["publication"]["complete"], true);
    assert_eq!(outcome["publication"]["cleanup_debt_count"], 0);
    assert_eq!(outcome["publication"]["artifact_count"], artifacts);
    assert_eq!(
        outcome["publication"]["state_counts"]["published"],
        artifacts
    );
}

#[test]
fn physical_cli_fixed_width_parse_failure_is_terminal_under_continue() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("pipeline.yaml"),
        r#"pipeline: { name: physical_data_policy }
error_handling:
  strategy: continue
  dlq: { path: rejected.csv }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: fixed_width
      path: input.dat
      dlq_granularity: record
      schema: [{ name: number, type: int, start: 0, width: 2 }]
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: json
      path: output.json
      options: { format: ndjson }
"#,
    )
    .unwrap();
    std::fs::write(root.path().join("input.dat"), b"01\nXX\n03\n").unwrap();
    let result = run(root.path());
    // A typed physical-reader parse error is terminal, even under continue;
    // it is not a row rejection delivered through the coercion/DLQ path.
    assert_eq!(
        terminal(&result, 4, "failed")["failure"]["code"],
        "source.data.invalid"
    );
    assert!(!root.path().join("output.json").exists());
    let partials = nested_partial_outputs(root.path());
    assert!(partials.iter().any(|bytes| bytes == b"{\"number\":1}\n"));
    assert!(
        partials
            .iter()
            .all(|bytes| bytes == b"{\"number\":1}\n" || bytes.is_empty())
    );
    let diagnostic = String::from_utf8_lossy(&result.stderr);
    assert!(
        diagnostic.contains("row 2") && diagnostic.contains("XX"),
        "{diagnostic}"
    );
}

#[test]
fn physical_cli_malformed_files_preserve_exact_successful_prefixes() {
    for format in ["fixed_width", "swift"] {
        for variant in [
            "utf8",
            "bom",
            "unsupported-bom",
            "first",
            "second",
            "late",
            "invalid-value",
        ] {
            let root = tempfile::tempdir().unwrap();
            let schema = if format == "fixed_width" {
                "[{ name: number, type: int, start: 0, width: 2 }]"
            } else {
                "[{ name: block, type: string }, { name: tag, type: string }, { name: value, type: string }]"
            };
            let yaml = format!(
                r#"pipeline: {{ name: physical_input }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: {format}
      glob: input-*.dat
      schema: {schema}
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: json
      path: output.json
      options: {{ format: ndjson }}
"#
            );
            std::fs::write(root.path().join("pipeline.yaml"), yaml).unwrap();
            let valid: &[u8] = if format == "fixed_width" {
                b"01\n"
            } else {
                b"{4:\n:20:one\n-}"
            };
            let first: &[u8] = if format == "fixed_width" {
                b"{\"number\":1}\n"
            } else {
                b"{\"block\":\"4\",\"tag\":\"20\",\"value\":\"one\"}\n"
            };
            let invalid: &[u8] = match (format, variant) {
                (_, "unsupported-bom") => b"\xff\xfe\0\0",
                ("fixed_width", "invalid-value") => b"XX\n",
                ("fixed_width", _) => b"\xff1\n",
                (_, "invalid-value") => b"{4:\n:20:one\n",
                _ => b"{4:\n:20:\xff\n-}",
            };
            let bytes = if variant == "bom" {
                [b"\xef\xbb\xbf".as_slice(), valid].concat()
            } else if matches!(variant, "utf8" | "second") {
                valid.to_vec()
            } else if variant == "late" {
                if format == "fixed_width" {
                    [valid, invalid].concat()
                } else {
                    [
                        b"{4:\n:20:one\n:21:".as_slice(),
                        &vec![b'x'; 32768],
                        b"\xff\n-}",
                    ]
                    .concat()
                }
            } else {
                invalid.to_vec()
            };
            std::fs::write(root.path().join("input-a.dat"), bytes).unwrap();
            if variant == "second" {
                std::fs::write(root.path().join("input-b.dat"), invalid).unwrap();
            }
            let result = run(root.path());
            if variant == "utf8" || (variant == "bom" && format == "fixed_width") {
                assert_physical_completed(&result, 1, 1);
                assert_eq!(
                    std::fs::read(root.path().join("output.json")).unwrap(),
                    first
                );
            } else {
                let outcome = terminal(&result, 4, "failed");
                assert_eq!(
                    outcome["failure"]["code"], "source.data.invalid",
                    "{format}/{variant}: {outcome}"
                );
                assert!(!root.path().join("output.json").exists());
                let expected =
                    if variant == "second" || (variant == "late" && format == "fixed_width") {
                        first
                    } else {
                        b""
                    };
                let partials = nested_partial_outputs(root.path());
                assert!(
                    partials.iter().any(|bytes| bytes == expected),
                    "{format}/{variant}: {partials:?}"
                );
                assert!(
                    partials
                        .iter()
                        .all(|bytes| bytes == expected || bytes.is_empty()),
                    "{format}/{variant}: {partials:?}"
                );
            }
        }
    }
}

#[test]
fn physical_cli_fixed_width_repetition_keeps_utf8_byte_positions() {
    let root = tempfile::tempdir().unwrap();
    let columns = r#"        - name: items
          type: map
          multiple: true
          start: 0
          count_field: { name: total, width: 1 }
          occurs: { min: 0, max: 2, fill: pad }
          fields:
            - { name: text, type: string, start: 0, width: 2 }
"#;
    let yaml = format!(
        r#"pipeline: {{ name: physical_repetition }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: fixed_width
      path: input.dat
      schema:
{columns}  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: fixed_width
      path: output.dat
      schema:
{columns}"#
    );
    std::fs::write(root.path().join("pipeline.yaml"), yaml).unwrap();
    let bytes = b"2\xc3\xa9\xc3\xb1\n1\xc3\xb1  \n0    \n";
    std::fs::write(root.path().join("input.dat"), bytes).unwrap();
    assert_physical_completed(&run(root.path()), 3, 1);
    assert_eq!(
        std::fs::read(root.path().join("output.dat")).unwrap(),
        bytes
    );
}

#[test]
fn physical_cli_fixed_width_selected_sections_reconstruct_each_file() {
    for variant in ["file", "files", "bad-header", "bad-footer"] {
        let root = tempfile::tempdir().unwrap();
        let yaml = r#"pipeline: { name: physical_sections }
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: fixed_width
      glob: input-*.dat
      schema:
        discriminator: { start: 0, width: 1 }
        records:
          - id: opening
            tag: H
            columns:
              - { name: kind, type: string, start: 0, width: 1 }
              - { name: label, type: string, start: 1, width: 2 }
          - id: detail
            tag: D
            columns:
              - { name: kind, type: string, start: 0, width: 1 }
              - { name: text, type: string, start: 1, width: 2 }
          - id: closing
            tag: F
            columns:
              - { name: kind, type: string, start: 0, width: 1 }
              - { name: count, type: int, start: 1, width: 2 }
      envelope:
        sections:
          manifest:
            extract: { record_type: H }
            fields: { label: string }
          totals:
            extract: { record_type: F }
            fields: { count: int }
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: fixed_width
      path: output.dat
      reconstruct_envelope: true
      schema: [{ name: text, type: string, width: 2 }]
      include_unmapped: false
      mapping: [text]
      options:
        envelope: { header_from_doc: manifest, footer_from_doc: totals }
"#;
        std::fs::write(root.path().join("pipeline.yaml"), yaml).unwrap();
        // Flat-file section extraction captures the leading header region.
        // The selected totals section is rendered as the output footer.
        let input: &[u8] = match variant {
            "bad-header" => b"H\xffx\nF01\nD\xc3\xb1\n",
            "bad-footer" => b"H\xc3\xa9\nF\xff1\nD\xc3\xb1\n",
            _ => b"H\xc3\xa9\nF01\nD\xc3\xb1\n",
        };
        std::fs::write(root.path().join("input-a.dat"), input).unwrap();
        if variant == "files" {
            std::fs::write(root.path().join("input-b.dat"), input).unwrap();
        }
        let result = run(root.path());
        if variant.starts_with("bad-") {
            assert_eq!(
                terminal(&result, 4, "failed")["failure"]["code"],
                "source.data.invalid"
            );
            assert!(!root.path().join("output.dat").exists());
            // Reconstruction waits for the full document's selected sections.
            assert!(
                nested_partial_outputs(root.path())
                    .iter()
                    .all(Vec::is_empty)
            );
        } else {
            let rows = if variant == "files" { 2 } else { 1 };
            assert_physical_completed(&result, rows, 1);
            let document = b"\xc3\xa9\n\xc3\xb1\n1\n";
            let expected = if variant == "files" {
                [document.as_slice(), document].concat()
            } else {
                document.to_vec()
            };
            assert_eq!(
                std::fs::read(root.path().join("output.dat")).unwrap(),
                expected,
                "{variant}"
            );
        }
    }
}

fn physical_pipeline(format: &str, route: &str) -> String {
    let files = route.contains("files") || route.contains("fanout");
    let source_path = if route == "reject-files" {
        "paths: [input-a.dat, input-b.dat]"
    } else if files {
        "glob: input-*.dat"
    } else {
        "path: input-a.dat"
    };
    let output_path = if route.contains("fanout") {
        "output_{source_file}.dat"
    } else {
        "output.dat"
    };
    let schema = if format == "fixed_width" {
        "        - { name: label, type: string, start: 1, width: 2 }\n        - { name: number, type: int, start: 4, width: 2 }\n"
    } else {
        "        - { name: block, type: string }\n        - { name: tag, type: string }\n        - { name: value, type: string }\n"
    };
    let envelope = if route.starts_with("reconstruct") {
        "      envelope:\n        sections:\n          routing:\n            extract: { segment: '1' }\n          signature:\n            extract: { segment: '5' }\n"
    } else {
        ""
    };
    let sink = if format == "fixed_width" {
        "      schema:\n        - { name: label, type: string, width: 2 }\n        - { name: number, type: int, width: 2 }\n"
    } else if route.starts_with("reconstruct") {
        "      options: { basic_header_from_doc: routing, trailer_from_doc: signature }\n"
    } else {
        ""
    };
    let split = if route.contains("split") {
        "      split: { max_records: 1 }\n"
    } else {
        ""
    };
    let consolidate = if format == "swift" && route == "files" {
        "  - type: envelope\n    name: combined\n    body: rows\n    config: { strategy: concat }\n"
    } else {
        ""
    };
    let upstream = if consolidate.is_empty() {
        "rows"
    } else {
        "combined"
    };
    format!(
        r#"pipeline: {{ name: physical_routes }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: {format}
      {source_path}
{envelope}      schema:
{schema}{consolidate}  - type: sink
    name: out
    input: {upstream}
    config:
      name: out
      type: {format}
      path: {output_path}
{sink}{split}"#
    )
}

#[test]
fn physical_cli_routes_preserve_literal_bytes_counts_and_publication() {
    use std::collections::BTreeSet;
    let mut executed = BTreeSet::new();
    for format in ["fixed_width", "swift"] {
        let routes: &[&str] = if format == "fixed_width" {
            &["file", "files", "fanout", "split", "split-fanout", "empty"]
        } else {
            &[
                "file",
                "files",
                "fanout",
                "empty",
                "reconstruct",
                "reject-files",
                "reconstruct-fanout",
                "reject-split",
                "reject-split-fanout",
            ]
        };
        for route in routes {
            let id = format!("{format}/{route}");
            let root = tempfile::tempdir().unwrap();
            std::fs::write(
                root.path().join("pipeline.yaml"),
                physical_pipeline(format, route),
            )
            .unwrap();
            let files = route.contains("files") || route.contains("fanout");
            let input: &[u8] = if *route == "empty" {
                if format == "swift" {
                    b"{1:HDR}{4:\r\n-}{5:TAIL}"
                } else {
                    b""
                }
            } else if format == "fixed_width" {
                // Ignored octets must not shift the selected UTF-8 byte cells.
                b"\xff\xc3\xa9\xfe01ignored\n\xff\xc3\xb1\xfe02tail\n"
            } else {
                b"{1:HDR}{4:\r\n:20:  first  \r\ncontinuation \n\r\n:20:second\r\n-}{5:TAIL}"
            };
            std::fs::write(root.path().join("input-a.dat"), input).unwrap();
            if files {
                std::fs::write(root.path().join("input-b.dat"), input).unwrap();
            }
            let result = run(root.path());
            if route.starts_with("reject-") {
                assert_eq!(
                    terminal(&result, 1, "failed")["failure"]["code"],
                    "admission.configuration.invalid",
                    "{id}"
                );
                let diagnostic = String::from_utf8_lossy(&result.stderr);
                let required = if *route == "reject-files" {
                    ["E355", "out", "swift", "strategy: concat", "source_file"]
                } else {
                    ["E342", "out", "swift", "split", "remove"]
                };
                for text in required {
                    assert!(diagnostic.contains(text), "{id}: {diagnostic}");
                }
                assert!(nested_partial_outputs(root.path()).is_empty(), "{id}");
                assert!(
                    !std::fs::read_dir(root.path()).unwrap().any(|entry| {
                        entry
                            .unwrap()
                            .file_name()
                            .to_string_lossy()
                            .starts_with("output")
                    }),
                    "{id}"
                );
            } else {
                let count = if *route == "empty" {
                    0
                } else if files {
                    4
                } else {
                    2
                };
                let artifacts = if *route == "split-fanout" {
                    4
                } else if route.contains("split") || route.contains("fanout") {
                    2
                } else {
                    1
                };
                assert_physical_completed(&result, count, artifacts);
                let body: &[u8] = if *route == "empty" {
                    b""
                } else if format == "fixed_width" {
                    b"\xc3\xa9 1\n\xc3\xb1 2\n"
                } else if route.starts_with("reconstruct") {
                    b"{1:HDR}{4:\r\n:20:  first  \r\ncontinuation \n\r\n:20:second\r\n-}{5:TAIL}"
                } else {
                    b"{4:\r\n:20:  first  \r\ncontinuation \n\r\n:20:second\r\n-}"
                };
                if route.contains("split") {
                    for stem in if files {
                        vec!["output_input-a", "output_input-b"]
                    } else {
                        vec!["output"]
                    } {
                        for (suffix, expected) in
                            [("0001", b"\xc3\xa9 1\n"), ("0002", b"\xc3\xb1 2\n")]
                        {
                            assert_eq!(
                                std::fs::read(root.path().join(format!("{stem}_{suffix}.dat")))
                                    .unwrap(),
                                expected,
                                "{id}"
                            );
                        }
                    }
                } else if route.contains("fanout") {
                    for name in ["output_input-a.dat", "output_input-b.dat"] {
                        assert_eq!(std::fs::read(root.path().join(name)).unwrap(), body, "{id}");
                    }
                } else {
                    let expected = if files
                        && format == "swift"
                        && !route.starts_with("reconstruct")
                    {
                        b"{4:\r\n:20:  first  \r\ncontinuation \n\r\n:20:second\r\n:20:  first  \r\ncontinuation \n\r\n:20:second\r\n-}".to_vec()
                    } else if files {
                        [body, body].concat()
                    } else {
                        body.to_vec()
                    };
                    assert_eq!(
                        std::fs::read(root.path().join("output.dat")).unwrap(),
                        expected,
                        "{id}"
                    );
                }
            }
            assert!(executed.insert(id));
        }
    }
    assert_eq!(
        executed.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        EXPECTED_PHYSICAL_ROUTES.iter().copied().collect()
    );
}
