//! Exact-byte CSV qualification through the compiled command-line executable.
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
