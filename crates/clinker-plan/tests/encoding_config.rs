use clinker_format::charset::Charset;
use clinker_plan::config::{
    CompileContext, CsvInputOptions, CsvOutputOptions, InputFormat, OutputFormat, PipelineNode,
    X12InputOptions, X12OutputOptions, parse_config,
};

fn pipeline(source: &str, sink: &str) -> String {
    format!(
        "pipeline: {{ name: encoding_identity }}\nnodes:\n  - type: source\n    name: src\n    config:\n      name: src\n      path: input.csv\n      type: csv\n      schema: [{{ name: value, type: string }}]\n{source}  - type: sink\n    name: dest\n    input: src\n    config:\n      name: dest\n      path: output.csv\n      type: csv\n{sink}"
    )
}

#[test]
fn csv_and_x12_admit_the_closed_alias_set() {
    for format in ["csv", "x12"] {
        for name in [
            "utf-8",
            "UTF8",
            "uTf_8",
            "iso-8859-1",
            "ISO8859_1",
            "Latin 1",
            "l1",
        ] {
            let yaml = format!("type: {format}\noptions: {{ encoding: '{name}' }}\n");
            clinker_plan::yaml::from_str::<InputFormat>(&yaml).unwrap();
            clinker_plan::yaml::from_str::<OutputFormat>(&yaml).unwrap();
        }
    }
}

#[test]
fn unsupported_csv_encoding_fails_before_execution() {
    for (source, sink) in [
        ("      options: { encoding: windows-1252 }\n", ""),
        ("", "      options: { encoding: windows-1252 }\n"),
    ] {
        let yaml = pipeline(source, sink);
        let result = parse_config(&yaml)
            .map_err(|e| e.to_string())
            .and_then(|config| {
                config
                    .compile(&CompileContext::default())
                    .map_err(|e| format!("{e:?}"))
            });
        let error = result.unwrap_err();
        assert!(error.contains("windows-1252"), "{error}");
        assert!(error.contains("encoding: utf-8"), "{error}");
        assert!(error.contains("iso-8859-1"), "{error}");
    }
}

#[test]
fn formats_without_authored_encoding_reject_overrides() {
    for format in ["json", "xml", "fixed_width", "swift", "edifact", "hl7"] {
        for name in ["utf-8", "iso-8859-1", "windows-1252"] {
            let yaml = format!("type: {format}\noptions: {{ encoding: '{name}' }}\n");
            assert!(
                clinker_plan::yaml::from_str::<InputFormat>(&yaml).is_err(),
                "input {yaml}"
            );
            assert!(
                clinker_plan::yaml::from_str::<OutputFormat>(&yaml).is_err(),
                "output {yaml}"
            );
        }
    }
}

#[test]
fn encoding_changes_the_semantic_fingerprint() {
    let fingerprint = |source: &str, sink: &str| {
        parse_config(&pipeline(source, sink))
            .unwrap()
            .compile(&CompileContext::default())
            .unwrap()
            .semantic_fingerprint()
            .unwrap()
    };
    let default = fingerprint("", "");
    let input = fingerprint("      options: { encoding: iso-8859-1 }\n", "");
    let output = fingerprint("", "      options: { encoding: iso-8859-1 }\n");
    assert_ne!(default, input);
    assert_ne!(default, output);
    assert_ne!(input, output);
    assert_eq!(
        output,
        fingerprint("", "      options: { encoding: iso-8859-1 }\n")
    );
}

#[test]
fn unknown_encoding_keys_remain_source_located() {
    let error = parse_config(&pipeline("      options: { encodng: utf-8 }\n", ""))
        .unwrap_err()
        .to_string();
    assert!(error.contains("encodng"), "{error}");
    assert!(error.contains("line") || error.contains("10 |"), "{error}");
}

#[test]
fn default_and_alias_encodings_have_one_semantic_identity() {
    for format in ["csv", "x12"] {
        let fingerprint = |source: &str, sink: &str| {
            parse_config(&pipeline(source, sink).replace("type: csv", &format!("type: {format}")))
                .unwrap()
                .compile(&CompileContext::default())
                .unwrap()
                .semantic_fingerprint()
                .unwrap()
        };
        assert_eq!(
            fingerprint("", ""),
            fingerprint(
                "      options: { encoding: UTF_8 }\n",
                "      options: { encoding: utf-8 }\n"
            )
        );
        assert_eq!(
            fingerprint(
                "      options: { encoding: Latin1 }\n",
                "      options: { encoding: l1 }\n"
            ),
            fingerprint(
                "      options: { encoding: iso-8859-1 }\n",
                "      options: { encoding: ISO8859_1 }\n"
            )
        );
    }
}

#[test]
fn typed_options_cannot_bypass_the_compile_gate() {
    for x12 in [false, true] {
        for source in [false, true] {
            let mut config = parse_config(&pipeline("", "")).unwrap();
            for node in &mut config.nodes {
                match &mut node.value {
                    PipelineNode::Source { config, .. } if source => {
                        config.source.format = if x12 {
                            InputFormat::X12(Some(X12InputOptions {
                                encoding: Some("shift_jis".into()),
                                ..Default::default()
                            }))
                        } else {
                            InputFormat::Csv(Some(CsvInputOptions {
                                encoding: Some("shift_jis".into()),
                                ..Default::default()
                            }))
                        };
                    }
                    PipelineNode::Sink { config, .. } if !source => {
                        config.sink.format = if x12 {
                            OutputFormat::X12(Some(X12OutputOptions {
                                encoding: Some("shift_jis".into()),
                                ..Default::default()
                            }))
                        } else {
                            OutputFormat::Csv(Some(CsvOutputOptions {
                                encoding: Some("shift_jis".into()),
                                ..Default::default()
                            }))
                        };
                    }
                    _ => {}
                }
            }
            let errors = config.compile(&CompileContext::default()).unwrap_err();
            assert!(format!("{errors:?}").contains("shift_jis"), "{errors:?}");
        }
    }
}

#[test]
fn every_format_has_an_explicit_default_repertoire_policy() {
    for format in [
        "csv",
        "x12",
        "json",
        "xml",
        "fixed_width",
        "swift",
        "edifact",
        "hl7",
    ] {
        let yaml = format!("type: {format}\n");
        let expected = if matches!(format, "edifact" | "hl7") {
            None
        } else {
            Some(Charset::Utf8)
        };
        assert_eq!(
            clinker_plan::yaml::from_str::<InputFormat>(&yaml)
                .unwrap()
                .resolved_charset()
                .unwrap(),
            expected
        );
        assert_eq!(
            clinker_plan::yaml::from_str::<OutputFormat>(&yaml)
                .unwrap()
                .resolved_charset()
                .unwrap(),
            expected
        );
    }
}
