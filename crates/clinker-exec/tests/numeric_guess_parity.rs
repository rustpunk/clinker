use clinker_exec::executor::build_source_format_reader;
use clinker_exec::pipeline::schema_coerce::{CoercingReader, coerce_numeric_with_observation};
use clinker_format::ReopenableSource;
use clinker_format::edifact::{EdifactReader, EdifactReaderConfig};
use clinker_format::fixed_width::field::{
    coerce_scalar_with_constraints, coerce_scalar_with_constraints_observed,
};
use clinker_format::hl7::{Hl7Reader, Hl7ReaderConfig};
use clinker_format::json::reader::{JsonMode, JsonReader, JsonReaderConfig};
use clinker_format::multi_record::{CsvDialect, MultiRecordReader, MultiRecordSpec};
use clinker_format::numeric_observation::{
    NumericBoundary, NumericIssue, NumericObservation, NumericObserver, NumericParserOutcome,
    NumericVote, observe_json_number, observe_json_value, observe_positional_numeric,
    observe_schema_numeric, observe_xml_scalar,
};
use clinker_format::schema::{Column, Discriminator, RecordType};
use clinker_format::swift::{SwiftReader, SwiftReaderConfig};
use clinker_format::traits::FormatReader;
use clinker_format::x12::{X12Reader, X12ReaderConfig};
use clinker_format::xml::reader::{XmlReader, XmlReaderConfig};
use clinker_plan::config::pipeline_node::OnUnmapped;
use clinker_record::Value;
use clinker_record::schema_def::LineSeparator;
use cxl::typecheck::Type;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

type ObservedEvent = (String, NumericObservation);
type ObservationLog = Arc<Mutex<Vec<ObservedEvent>>>;

fn json_number(lexeme: &str) -> serde_json::Number {
    serde_json::from_str::<serde_json::Value>(lexeme)
        .expect("test lexeme is valid JSON")
        .as_number()
        .expect("test value is a JSON number")
        .clone()
}

#[test]
fn json_exact_numbers_preserve_the_parser_result_and_guess_vote() {
    let cases = [
        ("-9223372036854775808", NumericVote::Int),
        ("9223372036854775807", NumericVote::Int),
        ("1e3", NumericVote::Float),
        ("0.1", NumericVote::Float),
        ("5e-324", NumericVote::Float),
        ("1.7976931348623157e308", NumericVote::Float),
    ];

    for (lexeme, expected_vote) in cases {
        let number = json_number(lexeme);
        let observation = observe_json_number(&number);
        assert_eq!(observation.boundary(), NumericBoundary::Json);
        // serde_json's arbitrary-precision representation preserves every
        // significant digit but canonicalizes exponent spelling (for example
        // `1e3` to `1e+3`). The observer exposes that exact parser-owned form.
        assert_eq!(
            observation.lexeme().complete(),
            Some(number.to_string().as_str())
        );
        assert_eq!(observation.vote(), expected_vote, "lexeme {lexeme}");
    }
}

#[test]
fn json_lossy_or_out_of_range_numbers_remain_unresolved() {
    let cases = [
        ("9223372036854775808", NumericIssue::UnsafeIntegerWidening),
        ("-9223372036854775809", NumericIssue::UnsafeIntegerWidening),
        ("1e999", NumericIssue::NonFinite),
        ("1e-999", NumericIssue::UnderflowToZero),
        ("0.10000000000000001", NumericIssue::PrecisionLoss),
    ];

    for (lexeme, expected_issue) in cases {
        let observation = observe_json_number(&json_number(lexeme));
        assert_eq!(
            observation.vote(),
            NumericVote::Unresolved(expected_issue),
            "lexeme {lexeme}; parser outcome {:?}",
            observation.parser_outcome()
        );
    }
}

#[test]
fn json_signed_zero_and_float_compatibility_are_explicit() {
    // serde_json's parser-owned arbitrary-precision representation
    // canonicalizes integer `-0` to `0`; guess follows that real boundary.
    let json_zero = observe_json_number(&json_number("-0"));
    assert_eq!(json_zero.lexeme().complete(), Some("0"));
    assert_eq!(json_zero.vote(), NumericVote::Int);
    // Text formats retain the spelling and therefore expose the
    // representation-changing conversion.
    assert_eq!(
        observe_positional_numeric("-0").vote(),
        NumericVote::Unresolved(NumericIssue::RepresentationChanged)
    );

    let exact = observe_json_number(&json_number("9007199254740992"));
    assert_eq!(exact.vote(), NumericVote::Int);
    assert!(matches!(
        exact.float_acceptance(),
        clinker_format::NumericAcceptance::Accepted(_)
    ));

    let unsafe_widening = observe_json_number(&json_number("9007199254740993"));
    assert_eq!(unsafe_widening.vote(), NumericVote::Int);
    assert_eq!(
        unsafe_widening.float_acceptance(),
        &clinker_format::NumericAcceptance::Rejected(NumericIssue::UnsafeIntegerWidening)
    );
}

#[test]
fn xml_inference_observation_matches_the_real_inference_order() {
    let integer = observe_xml_scalar("42");
    assert_eq!(integer.boundary(), NumericBoundary::Xml);
    assert_eq!(integer.parser_outcome(), &NumericParserOutcome::Integer(42));
    assert_eq!(integer.vote(), NumericVote::Int);

    let float = observe_xml_scalar("4.2e1");
    assert_eq!(float.parser_outcome(), &NumericParserOutcome::Float(42.0));
    assert_eq!(float.vote(), NumericVote::Float);

    assert_eq!(
        observe_xml_scalar("").parser_outcome(),
        &NumericParserOutcome::NoValue
    );
    assert_eq!(
        observe_xml_scalar("42x").parser_outcome(),
        &NumericParserOutcome::NonNumeric
    );

    let non_finite = observe_xml_scalar("1e999");
    assert!(matches!(
        non_finite.parser_outcome(),
        NumericParserOutcome::Float(value) if value.is_infinite()
    ));
    assert_eq!(
        non_finite.vote(),
        NumericVote::Unresolved(NumericIssue::NonFinite)
    );
}

#[test]
fn null_missing_and_numeric_default_states_stay_distinct() {
    let null = observe_json_value(&serde_json::Value::Null).expect("null observation");
    assert_eq!(null.vote(), NumericVote::NoValue);
    assert_eq!(null.parser_outcome(), &NumericParserOutcome::NoValue);

    // A missing field produces no scalar and therefore no parser observation;
    // the caller applies required/default policy before asking a default to
    // vote through the schema boundary.
    let missing = serde_json::json!({});
    assert!(missing.get("n").and_then(observe_json_value).is_none());

    let default = observe_schema_numeric(&Value::String("7".into()));
    assert_eq!(default.boundary(), NumericBoundary::SchemaCoerce);
    assert_eq!(default.vote(), NumericVote::Int);
}

#[test]
fn positional_observation_uses_int_then_finite_float_order() {
    let integer = observe_positional_numeric("42");
    assert_eq!(integer.boundary(), NumericBoundary::Positional);
    assert_eq!(integer.vote(), NumericVote::Int);

    let float = observe_positional_numeric("42.5");
    assert_eq!(float.vote(), NumericVote::Float);

    let non_finite = observe_positional_numeric("NaN");
    assert_eq!(
        non_finite.vote(),
        NumericVote::Unresolved(NumericIssue::NonFinite)
    );
}

fn observation_collector() -> (NumericObserver, ObservationLog) {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&observations);
    let observer = NumericObserver::new(move |field, observation| {
        sink.lock()
            .expect("observation collector lock")
            .push((field.to_string(), observation));
    });
    (observer, observations)
}

fn assert_record_matches_parser(value: &Value, observation: &NumericObservation) {
    match observation.parsed_value() {
        Some(parsed) => assert_eq!(value, &parsed),
        None => assert_eq!(
            value,
            &Value::String(
                observation
                    .lexeme()
                    .complete()
                    .expect("test lexeme fits evidence cap")
                    .into()
            )
        ),
    }
}

#[test]
fn json_array_and_ndjson_stream_the_same_preconversion_observation() {
    for (mode, input) in [
        (JsonMode::Array, "[{\"n\":0.10000000000000001}]"),
        (JsonMode::Ndjson, "{\"n\":0.10000000000000001}\n"),
    ] {
        let (observer, observations) = observation_collector();
        let mut reader = JsonReader::from_reader_observing(
            input.as_bytes(),
            JsonReaderConfig {
                format: Some(mode),
                ..Default::default()
            },
            observer,
        )
        .expect("JSON reader builds");
        reader.schema().expect("JSON schema");
        let record = reader
            .next_record()
            .expect("JSON record parses")
            .expect("one JSON record");
        let observations = observations.lock().expect("observation collector lock");
        let [(field, observation)] = observations.as_slice() else {
            panic!("expected one numeric observation, got {observations:?}");
        };
        assert_eq!(field, "n");
        assert_eq!(observation.boundary(), NumericBoundary::Json);
        assert_eq!(
            observation.vote(),
            NumericVote::Unresolved(NumericIssue::PrecisionLoss)
        );
        assert_record_matches_parser(record.get("n").expect("n field"), observation);
    }
}

#[test]
fn json_explicit_null_streams_no_value_while_missing_streams_nothing() {
    let (observer, observations) = observation_collector();
    let mut reader = JsonReader::from_reader_observing(
        b"[{\"n\":null},{}]".as_slice(),
        JsonReaderConfig {
            format: Some(JsonMode::Array),
            ..Default::default()
        },
        observer,
    )
    .expect("JSON reader builds");
    reader.schema().expect("JSON schema");
    assert!(reader.next_record().expect("first record").is_some());
    assert!(reader.next_record().expect("second record").is_some());
    assert!(reader.next_record().expect("end of JSON").is_none());

    let observations = observations.lock().expect("observation collector lock");
    let [(field, observation)] = observations.as_slice() else {
        panic!("expected only explicit null evidence, got {observations:?}");
    };
    assert_eq!(field, "n");
    assert_eq!(observation.vote(), NumericVote::NoValue);
}

#[test]
fn xml_reader_streams_the_observation_used_by_infer_value() {
    let (observer, observations) = observation_collector();
    let mut reader = XmlReader::from_reader_observing(
        b"<root><row><n>1e999</n></row></root>".as_slice(),
        XmlReaderConfig {
            record_path: Some("root/row".into()),
            ..Default::default()
        },
        observer,
    )
    .expect("XML reader builds");
    reader.schema().expect("XML schema");
    let record = reader
        .next_record()
        .expect("XML record parses")
        .expect("one XML record");
    let observations = observations.lock().expect("observation collector lock");
    let [(field, observation)] = observations.as_slice() else {
        panic!("expected one numeric observation, got {observations:?}");
    };
    assert_eq!(field, "n");
    assert_eq!(observation.boundary(), NumericBoundary::Xml);
    assert_eq!(
        observation.vote(),
        NumericVote::Unresolved(NumericIssue::NonFinite)
    );
    assert_record_matches_parser(record.get("n").expect("n field"), observation);
}

#[test]
fn positional_observed_result_is_the_canonical_scalar_result() {
    for lexeme in [
        "-9223372036854775808",
        "9223372036854775807",
        "9223372036854775808",
        "0.1",
        "1e-999",
        "0.10000000000000001",
        "NaN",
        "not-a-number",
    ] {
        let observed =
            coerce_scalar_with_constraints_observed(&Type::Numeric, None, None, None, lexeme);
        let canonical = coerce_scalar_with_constraints(&Type::Numeric, None, None, None, lexeme);
        assert_eq!(
            observed.result(),
            canonical.as_ref().map_err(String::as_str)
        );
        assert_eq!(
            observed.numeric_observation().map(NumericObservation::vote),
            Some(observe_positional_numeric(lexeme).vote())
        );
    }
}

#[test]
fn schema_observation_does_not_change_existing_nonlexical_coercions() {
    let boolean = coerce_numeric_with_observation(&Value::Bool(true));
    assert_eq!(boolean.result(), Ok(&Value::Integer(1)));
    assert_eq!(
        boolean.observation().parser_outcome(),
        &NumericParserOutcome::NonNumeric
    );

    let empty = coerce_numeric_with_observation(&Value::String("".into()));
    assert_eq!(empty.result(), Err("value cannot be parsed as Float"));
}

fn record_type(columns: Vec<Column>) -> RecordType {
    RecordType {
        id: "detail".into(),
        tag: "D".into(),
        description: None,
        parent: None,
        join_key: None,
        columns,
    }
}

#[test]
fn multi_record_fixed_and_csv_delegate_numeric_text_to_positional_coercion() {
    let fixed_column = Column {
        start: Some(1),
        width: Some(10),
        ..Column::bare("n", Type::Numeric)
    };
    let fixed_spec = MultiRecordSpec {
        discriminator: Discriminator {
            start: Some(0),
            width: Some(1),
            field: None,
        },
        record_types: vec![record_type(vec![fixed_column])],
        structure: Vec::new(),
        header_tags: Vec::new(),
    };
    let mut fixed = MultiRecordReader::new_fixed_width(
        b"D42.5      \n".as_slice(),
        fixed_spec,
        LineSeparator::Lf,
    )
    .expect("fixed-width multi-record reader builds");
    let fixed_record = fixed
        .next_record()
        .expect("fixed-width multi-record parse")
        .expect("one fixed-width record");

    let csv_spec = MultiRecordSpec {
        discriminator: Discriminator {
            start: None,
            width: None,
            field: Some("kind".into()),
        },
        record_types: vec![record_type(vec![
            Column::bare("kind", Type::String),
            Column::bare("n", Type::Numeric),
        ])],
        structure: Vec::new(),
        header_tags: Vec::new(),
    };
    let mut csv = MultiRecordReader::new_csv(
        b"D,42.5\n".as_slice(),
        csv_spec,
        CsvDialect {
            delimiter: b',',
            quote_char: b'"',
            has_header: false,
        },
    )
    .expect("CSV multi-record reader builds");
    let csv_record = csv
        .next_record()
        .expect("CSV multi-record parse")
        .expect("one CSV record");

    let observed =
        coerce_scalar_with_constraints_observed(&Type::Numeric, None, None, None, "42.5");
    let parsed = observed.result().expect("numeric observation parses");
    assert_eq!(fixed_record.get("n"), Some(parsed));
    assert_eq!(csv_record.get("n"), Some(parsed));
}

#[test]
fn raw_string_reader_families_share_the_schema_coercion_observation() {
    // These readers all deliver raw text to this one runtime boundary. The
    // format name is deliberately not a parsing switch: every row below must
    // produce the same result and exactness vote.
    for family in ["csv", "edifact", "x12", "hl7", "swift"] {
        let value = Value::String("0.10000000000000001".into());
        let observed = coerce_numeric_with_observation(&value);
        assert_eq!(
            observed.observation().boundary(),
            NumericBoundary::SchemaCoerce,
            "family {family}"
        );
        assert_eq!(
            observed.observation().vote(),
            NumericVote::Unresolved(NumericIssue::PrecisionLoss),
            "family {family}"
        );
        assert_eq!(observed.result(), Ok(&Value::Float(0.1)), "family {family}");
    }
}

fn assert_reader_uses_schema_numeric_observation(
    reader: Box<dyn FormatReader>,
    field: &str,
    family: &str,
) {
    let (observer, observations) = observation_collector();
    let mut coercing = CoercingReader::new_observing(
        reader,
        &[Column::bare(field, Type::Nullable(Box::new(Type::Numeric)))],
        OnUnmapped::Drop,
        family,
        false,
        observer,
    )
    .expect("coercing reader builds");

    let mut parsed_value_seen = false;
    while let Some(record) = coercing.next_record().expect("source record coerces") {
        parsed_value_seen |= record.get(field) == Some(&Value::Float(0.1));
    }
    assert!(
        parsed_value_seen,
        "{family} numeric text reached schema coercion"
    );

    let observations = observations.lock().expect("observation collector lock");
    assert!(
        observations.iter().any(|(observed_field, observation)| {
            observed_field == field
                && observation.boundary() == NumericBoundary::SchemaCoerce
                && observation.vote() == NumericVote::Unresolved(NumericIssue::PrecisionLoss)
                && observation.parser_outcome() == &NumericParserOutcome::Float(0.1)
        }),
        "{family} did not publish the expected parser-owned observation: {observations:?}"
    );
}

#[test]
fn every_raw_string_reader_family_reaches_the_shared_schema_boundary() {
    const LEXEME: &str = "0.10000000000000001";
    const X12_ISA: &str = "ISA*00*          *00*          *ZZ*SENDER         \
        *ZZ*RECEIVER       *240101*1200*U*00401*000000001*0*P*:~";

    let csv = clinker_format::csv::reader::CsvReader::from_reader(
        Cursor::new(format!("n\n{LEXEME}\n").into_bytes()),
        clinker_format::csv::reader::CsvReaderConfig::default(),
    );
    assert_reader_uses_schema_numeric_observation(Box::new(csv), "n", "csv");

    let edifact = format!(
        "UNB+UNOA:1+SENDER+RECEIVER+240101:1200+REF1'\
         UNH+M1+ORDERS:D:96A:UN'\
         BGM+220+123+{LEXEME}'\
         UNT+3+M1'\
         UNZ+1+REF1'"
    );
    assert_reader_uses_schema_numeric_observation(
        Box::new(EdifactReader::new(
            Cursor::new(edifact.into_bytes()),
            EdifactReaderConfig::default(),
        )),
        "e03",
        "edifact",
    );

    let x12 = format!(
        "{X12_ISA}\
         GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~\
         ST*850*0001~\
         BEG*00*NE*PO12345**20240101~\
         PO1*1*10*EA*{LEXEME}~\
         SE*4*0001~\
         GE*1*1~\
         IEA*1*000000001~"
    );
    assert_reader_uses_schema_numeric_observation(
        Box::new(X12Reader::new(
            Cursor::new(x12.into_bytes()),
            X12ReaderConfig::default(),
        )),
        "e04",
        "x12",
    );

    let hl7 = format!(
        "MSH|^~\\&|SENDAPP|SENDFAC|RCVAPP|RCVFAC|20240101120000||ADT^A01|MSG001|P|{LEXEME}"
    );
    assert_reader_uses_schema_numeric_observation(
        Box::new(Hl7Reader::new(
            Cursor::new(hl7.into_bytes()),
            Hl7ReaderConfig::default(),
        )),
        "f11",
        "hl7",
    );

    let swift = format!(
        "{{1:F01BANKBEBBAXXX0000000000}}{{2:I103BANKDEFFXXXXN}}\
         {{3:{{108:MSGREF12345}}}}\
         {{4:\r\n:20:{LEXEME}\r\n-}}\
         {{5:{{CHK:1234567890AB}}}}"
    );
    assert_reader_uses_schema_numeric_observation(
        Box::new(SwiftReader::new(
            Cursor::new(swift.into_bytes()),
            SwiftReaderConfig::default(),
        )),
        "value",
        "swift",
    );
}

#[test]
fn coercing_reader_emits_schema_observation_before_accepting_csv_text() {
    use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};

    let reader = CsvReader::from_reader(
        b"n\n42.5\n".as_slice(),
        CsvReaderConfig {
            delimiter: b',',
            quote_char: b'"',
            has_header: true,
            ..Default::default()
        },
    );
    let (observer, observations) = observation_collector();
    let mut coercing = CoercingReader::new_observing(
        Box::new(reader),
        &[Column::bare("n", Type::Numeric)],
        OnUnmapped::Drop,
        "csv",
        false,
        observer,
    )
    .expect("coercing reader builds");
    let record = coercing
        .next_record()
        .expect("coercion succeeds")
        .expect("one CSV record");
    let observations = observations.lock().expect("observation collector lock");
    let [(field, observation)] = observations.as_slice() else {
        panic!("expected one schema observation, got {observations:?}");
    };
    assert_eq!(field, "n");
    assert_eq!(observation.boundary(), NumericBoundary::SchemaCoerce);
    assert_eq!(record.get("n"), observation.parsed_value().as_ref());
}

fn source_body<'a>(
    config: &'a clinker_plan::config::PipelineConfig,
    name: &str,
) -> &'a clinker_plan::config::SourceBody {
    config
        .source_bodies()
        .find(|body| body.source.name == name)
        .unwrap_or_else(|| panic!("source {name} exists"))
}

fn observe_with_shared_source_reader(
    body: &clinker_plan::config::SourceBody,
    input: &str,
) -> (Value, NumericObservation) {
    let (observer, observations) = observation_collector();
    let source = ReopenableSource::buffer(Cursor::new(input.as_bytes().to_vec()))
        .expect("buffer source bytes");
    let mut reader = build_source_format_reader(
        &body.source,
        &body.schema,
        body.on_unmapped.clone(),
        source,
        Some(observer),
    )
    .expect("shared source reader builds");
    let record = reader
        .next_record()
        .expect("source record parses")
        .expect("one source record");
    assert!(reader.next_record().expect("source reaches EOF").is_none());
    let observations = observations.lock().expect("observation collector lock");
    let [(field, observation)] = observations.as_slice() else {
        panic!("expected one numeric observation, got {observations:?}");
    };
    assert_eq!(field, "n");
    (
        record.get("n").expect("n field").clone(),
        observation.clone(),
    )
}

#[test]
fn shared_guess_reader_matches_runtime_csv_json_and_xml_construction() {
    let config = clinker_plan::config::load_config_from_str(
        r#"
pipeline:
  name: shared_reader_parity
nodes:
  - type: source
    name: csv_source
    config:
      name: csv_source
      type: csv
      path: unused.csv
      options:
        delimiter: ";"
      schema:
        - { name: n, type: numeric }
  - type: source
    name: json_source
    config:
      name: json_source
      type: json
      path: unused.json
      options:
        format: array
      schema:
        - { name: n, type: numeric }
  - type: source
    name: xml_source
    config:
      name: xml_source
      type: xml
      path: unused.xml
      options:
        record_path: root/row
      schema:
        - { name: n, type: numeric }
"#,
    )
    .expect("parse reader parity config");

    let (csv_value, csv_observation) = observe_with_shared_source_reader(
        source_body(&config, "csv_source"),
        "n;ignored\n42.5;x\n",
    );
    assert_eq!(csv_value, Value::Float(42.5));
    assert_eq!(csv_observation.boundary(), NumericBoundary::SchemaCoerce);
    assert_eq!(csv_observation.vote(), NumericVote::Float);

    let (json_value, json_observation) = observe_with_shared_source_reader(
        source_body(&config, "json_source"),
        r#"[{"n":0.10000000000000001}]"#,
    );
    assert_eq!(json_value, Value::Float(0.1));
    assert_eq!(json_observation.boundary(), NumericBoundary::Json);
    assert_eq!(
        json_observation.vote(),
        NumericVote::Unresolved(NumericIssue::PrecisionLoss)
    );

    let (xml_value, xml_observation) = observe_with_shared_source_reader(
        source_body(&config, "xml_source"),
        "<root><row><n>42</n></row></root>",
    );
    assert_eq!(xml_value, Value::Integer(42));
    assert_eq!(xml_observation.boundary(), NumericBoundary::Xml);
    assert_eq!(xml_observation.vote(), NumericVote::Int);
}
