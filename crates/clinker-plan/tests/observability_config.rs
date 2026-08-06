use std::time::Duration;

use clinker_plan::config::{
    ClinkerToml, CompileContext, FieldPolicyAction, LineageDatasetIdentity, LineageIdentityMode,
    ObservabilityAuth, ObservabilityDropPolicy, parse_config,
};

const PIPELINE: &str = r#"
pipeline: { name: deployment_policy_identity }
nodes:
  - type: source
    name: src
    config:
      name: src
      path: input.csv
      type: csv
      schema: [{ name: id, type: int }]
  - type: output
    name: out
    input: src
    config: { name: out, path: output.csv, type: csv }
"#;

fn configured_policy(auth: &str) -> String {
    format!(
        r#"
[observability]
arena_bytes = "4MB"
ordinary_lane_bytes = "3MB"
high_severity_lane_bytes = "1MB"
max_batch_bytes = "250KB"
max_attributes_per_event = 20
max_attribute_bytes = "2KB"
drop_policy = "drop-newest"
sample_every = 5
rate_limit_per_second = 120
rate_limit_burst = 240
flush_timeout_ms = 8000

[observability.otlp]
endpoint = " HTTP://Collector.Example.invalid/root?opaque=yes "
connect_timeout_ms = 500
request_timeout_ms = 2000
retry_max_attempts = 3
retry_total_timeout_ms = 7000
max_response_bytes = "64KB"

[observability.otlp.auth]
{auth}

[observability.lineage]
queue_bytes = "2MB"
max_event_bytes = "50KB"
drop_policy = "drop-newest"
flush_timeout_ms = 4000
identity_mode = "external"

[[observability.lineage.dataset]]
node = "src"
canonical_datasource = "s3://warehouse/customers"

[[observability.lineage.dataset]]
node = "out"
catalog_namespace = "analytics"
catalog_name = "customers_clean"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "customer_id"
action = "hash"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "email"
action = "replace"
replacement = "[redacted]"
"#
    )
}

#[test]
fn resolves_complete_policy() {
    let disabled = ClinkerToml::parse("")
        .expect("empty clinker.toml parses")
        .resolve_observability(None)
        .expect("missing observability is disabled");
    assert!(!disabled.is_enabled());
    assert_eq!(disabled.arena_bytes(), 0);
    assert!(disabled.otlp().is_none());
    assert!(disabled.lineage().is_none());

    let raw = configured_policy("mode = \"none\"");
    let policy = ClinkerToml::parse(&raw)
        .expect("complete policy parses")
        .resolve_observability(None)
        .expect("complete policy resolves");

    assert!(policy.is_enabled());
    assert_eq!(policy.arena_bytes(), 4_000_000);
    assert_eq!(policy.ordinary_lane_bytes(), 3_000_000);
    assert_eq!(policy.high_severity_lane_bytes(), 1_000_000);
    assert_eq!(policy.max_batch_bytes(), 250_000);
    assert_eq!(policy.max_attributes_per_event(), 20);
    assert_eq!(policy.max_attribute_bytes(), 2_000);
    assert_eq!(policy.drop_policy(), ObservabilityDropPolicy::DropNewest);
    assert_eq!(policy.sample_every(), 5);
    assert_eq!(policy.rate_limit_per_second(), 120);
    assert_eq!(policy.rate_limit_burst(), 240);
    assert_eq!(policy.flush_timeout(), Duration::from_millis(8_000));

    let otlp = policy.otlp().expect("OTLP policy is retained");
    assert_eq!(
        otlp.raw_endpoint(),
        " HTTP://Collector.Example.invalid/root?opaque=yes "
    );
    assert_eq!(otlp.auth(), &ObservabilityAuth::None);
    assert_eq!(otlp.connect_timeout(), Duration::from_millis(500));
    assert_eq!(otlp.request_timeout(), Duration::from_millis(2_000));
    assert_eq!(otlp.retry_max_attempts().get(), 3);
    assert_eq!(otlp.retry_total_timeout(), Duration::from_millis(7_000));
    assert_eq!(otlp.retry_initial_backoff(), Duration::from_millis(100));
    assert_eq!(otlp.max_response_bytes().get(), 64_000);

    let lineage = policy.lineage().expect("lineage has a separate policy");
    assert_eq!(lineage.queue_bytes().get(), 2_000_000);
    assert_eq!(lineage.max_event_bytes().get(), 50_000);
    assert_eq!(lineage.drop_policy(), ObservabilityDropPolicy::DropNewest);
    assert_eq!(lineage.flush_timeout(), Duration::from_millis(4_000));
    assert_eq!(lineage.identity_mode(), LineageIdentityMode::External);
    assert_eq!(lineage.datasets().len(), 2);
    assert_eq!(lineage.datasets()[0].node(), "out");
    assert_eq!(
        lineage.datasets()[0].identity(),
        &LineageDatasetIdentity::Catalog {
            namespace: "analytics".into(),
            name: "customers_clean".into(),
        }
    );
    assert_eq!(lineage.datasets()[1].node(), "src");
    assert_eq!(
        lineage.datasets()[1].identity(),
        &LineageDatasetIdentity::CanonicalDatasource {
            identifier: "s3://warehouse/customers".into(),
        }
    );

    assert_eq!(policy.field_policies().len(), 2);
    assert_eq!(policy.field_policies()[0].action(), FieldPolicyAction::Hash);
    assert_eq!(
        policy.field_policies()[1].action(),
        FieldPolicyAction::Replace
    );
    assert_eq!(policy.field_policies()[1].replacement(), Some("[redacted]"));

    let reference = ClinkerToml::parse(&configured_policy(
        "mode = \"reference\"\nreference = \"telemetry/production\"",
    ))
    .expect("logical reference parses")
    .resolve_observability(None)
    .expect("logical reference resolves");
    assert_eq!(
        reference.otlp().expect("enabled").auth(),
        &ObservabilityAuth::Reference {
            reference: "telemetry/production".into(),
        }
    );

    let inherited = ClinkerToml::parse("")
        .unwrap()
        .resolve_observability(Some(reference.clone()))
        .expect("an absent table accepts one complete replacement");
    assert_eq!(inherited, reference);

    let conflict = ClinkerToml::parse(&raw)
        .unwrap()
        .resolve_observability(Some(reference))
        .expect_err("workspace and replacement policies never merge");
    assert_eq!(
        conflict.classification().code(),
        "observability.configuration.invalid"
    );

    let fingerprint = || {
        parse_config(PIPELINE)
            .expect("parse semantic fixture")
            .compile(&CompileContext::default())
            .expect("compile semantic fixture")
            .semantic_fingerprint()
            .expect("serialize semantic plan")
    };
    assert_eq!(fingerprint(), fingerprint());
    let serialized = serde_json::to_string(&parse_config(PIPELINE).unwrap()).unwrap();
    for deployment_key in ["observability", "arena_bytes", "field_policy"] {
        assert!(!serialized.contains(deployment_key));
    }
}

fn minimal_policy(auth: &str, datasets: &str) -> String {
    format!(
        r#"
[observability]

[observability.otlp]
endpoint = "https://collector.example.com"

[observability.otlp.auth]
{auth}

[observability.lineage]

{datasets}
"#
    )
}

fn canonical_dataset(node: &str) -> String {
    format!(
        r#"
[[observability.lineage.dataset]]
node = "{node}"
canonical_datasource = "s3://warehouse/{node}"
"#
    )
}

fn reject(text: &str) -> (String, &'static str) {
    match ClinkerToml::parse(text) {
        Ok(document) => {
            let error = document
                .resolve_observability(None)
                .expect_err("configuration must fail before effects");
            let code = error.classification().code();
            (error.to_string(), code)
        }
        Err(error) => {
            let code = error
                .classification()
                .expect("observability parse failures are classified")
                .code();
            (error.to_string(), code)
        }
    }
}

fn assert_rejected(text: &str, field: &str, correction: &str) {
    let (rendered, code) = reject(text);
    assert_eq!(code, "observability.configuration.invalid", "{rendered}");
    assert!(rendered.contains(field), "{rendered}");
    assert!(rendered.contains(correction), "{rendered}");
}

#[test]
fn rejects_before_effects() {
    let sentinel = tempfile::tempdir().expect("create side-effect sentinel directory");
    let canonical = canonical_dataset("src");
    let valid = minimal_policy("mode = \"none\"", &canonical);

    let raw_cases = [
        (
            valid.replace(
                "[observability]\n",
                "[observability]\nqueue_mode = \"blocking\"\n",
            ),
            "observability.queue_mode",
            "remove `queue_mode`",
        ),
        (
            valid.replace(
                "[observability.lineage]\n",
                "[observability.lineage]\narena_bytes = \"1MB\"\n",
            ),
            "observability.lineage.arena_bytes",
            "remove `arena_bytes`",
        ),
        (
            minimal_policy("", &canonical),
            "observability.otlp.auth.mode",
            "mode = \"none\"",
        ),
        (
            minimal_policy("mode = \"none\"\nreference = \"private-token\"", &canonical),
            "observability.otlp.auth.reference",
            "mode = \"none\"",
        ),
        (
            minimal_policy("mode = \"none\"\nheaders = { authorization = \"private-token\" }", &canonical),
            "observability.otlp.auth.headers",
            "mode = \"none\"",
        ),
        (
            minimal_policy(
                "mode = \"none\"\nbearer_token = \"private-token\"",
                &canonical,
            ),
            "observability.otlp.auth.bearer_token",
            "mode = \"none\"",
        ),
        (
            minimal_policy(
                "mode = \"reference\"\nreference = \"telemetry/production\"\nenvironment = \"PRIVATE_TOKEN\"",
                &canonical,
            ),
            "observability.otlp.auth.environment",
            "mode = \"none\"",
        ),
        (
            minimal_policy("mode = \"basic\"\npassword = \"private-token\"", &canonical),
            "observability.otlp.auth.mode",
            "mode = \"none\"",
        ),
        (
            minimal_policy("mode = \"reference\"\nreference = \"\"", &canonical),
            "observability.otlp.auth.reference",
            "telemetry/production",
        ),
        (
            minimal_policy(
                "mode = \"reference\"\nreference = \"Bearer private-token\"",
                &canonical,
            ),
            "observability.otlp.auth.reference",
            "telemetry/production",
        ),
        (
            valid.replace(
                "[observability]\n",
                "[observability]\narena_bytes = 1000\nordinary_lane_bytes = 600\nhigh_severity_lane_bytes = 300\n",
            ),
            "observability.arena_bytes",
            "exact sum",
        ),
        (
            valid.replace(
                "[observability]\n",
                "[observability]\nmax_batch_bytes = 0\n",
            ),
            "observability.max_batch_bytes",
            "256KB",
        ),
        (
            valid.replace(
                "[observability]\n",
                "[observability]\narena_bytes = \"99999999999999999999GB\"\n",
            ),
            "observability.arena_bytes",
            "4MB",
        ),
        (
            valid.replace(
                "[observability.lineage]\n",
                "[observability.lineage]\nqueue_bytes = 0\n",
            ),
            "observability.lineage.queue_bytes",
            "1MB",
        ),
        (
            valid.replace(
                "[observability.lineage]\n",
                "[observability.lineage]\nmax_event_bytes = \"2MB\"\n",
            ),
            "observability.lineage.max_event_bytes",
            "64KB",
        ),
        (
            valid.replace(
                "[observability.lineage]\n",
                "[observability.lineage]\nqueue_bytes = \"99999999999999999999GB\"\n",
            ),
            "observability.lineage.queue_bytes",
            "1MB",
        ),
        (
            valid.replace(
                "[observability.lineage]\n",
                "[observability.lineage]\nidentity_mode = \"paths\"\n",
            ),
            "observability.lineage.identity_mode",
            "local_diagnostic_paths",
        ),
        (
            minimal_policy("mode = \"none\"", ""),
            "observability.lineage.dataset",
            "exactly one canonical or catalog identity",
        ),
        (
            minimal_policy(
                "mode = \"none\"",
                &format!("{canonical}{canonical}"),
            ),
            "observability.lineage.dataset.node",
            "exactly one dataset binding",
        ),
        (
            minimal_policy(
                "mode = \"none\"",
                r#"
[[observability.lineage.dataset]]
node = "src"
catalog_namespace = "analytics"
"#,
            ),
            "observability.lineage.dataset.catalog_name",
            "catalog_name",
        ),
        (
            minimal_policy(
                "mode = \"none\"",
                r#"
[[observability.lineage.dataset]]
node = "src"
canonical_datasource = "s3://warehouse/src"
catalog_namespace = "analytics"
catalog_name = "src"
"#,
            ),
            "observability.lineage.dataset",
            "keep only `canonical_datasource`",
        ),
        (
            format!(
                "{valid}\n[[observability.field_policy]]\nevent = \"run.completed\"\nfield = \"count\"\naction = \"replace\"\n"
            ),
            "observability.field_policy.replacement",
            "[redacted]",
        ),
        (
            format!(
                "{valid}\n[[observability.field_policy]]\nevent = \"run.completed\"\nfield = \"count\"\naction = \"allow\"\n[[observability.field_policy]]\nevent = \"run.completed\"\nfield = \"count\"\naction = \"hash\"\n"
            ),
            "observability.field_policy",
            "exactly one",
        ),
    ];

    for (text, field, correction) in raw_cases {
        assert_rejected(&text, field, correction);
    }

    for endpoint in [
        "http://collector.example.com/path?token=private-token",
        "opaque collector text with spaces",
        "HTTPS://Collector.Example.COM:443/root#fragment",
    ] {
        let text = valid.replace("https://collector.example.com", endpoint);
        let policy = ClinkerToml::parse(&text)
            .expect("raw endpoint text is not interpreted by clinker-plan")
            .resolve_observability(None)
            .expect("endpoint admission belongs to the network boundary");
        assert_eq!(policy.otlp().unwrap().raw_endpoint(), endpoint);
        assert!(!format!("{policy:?}").contains(endpoint));
    }

    let empty = valid.replace("https://collector.example.com", "");
    assert_rejected(
        &empty,
        "observability.otlp.endpoint",
        "https://collector.example.com",
    );
    let oversized = valid.replace("https://collector.example.com", &"x".repeat(2_049));
    assert_rejected(
        &oversized,
        "observability.otlp.endpoint",
        "https://collector.example.com",
    );

    let secret_bearing = minimal_policy(
        "mode = \"none\"\nheaders = { authorization = \"private-token\" }",
        &canonical,
    );
    let (rendered, _) = reject(&secret_bearing);
    assert!(!rendered.contains("private-token"), "{rendered}");
    assert!(!rendered.contains("authorization"), "{rendered}");

    let private_replacement = format!(
        "{valid}\n[[observability.field_policy]]\nevent = \"run.completed\"\nfield = \"customer_id\"\naction = \"replace\"\nreplacement = \"private-token\"\n"
    );
    let private_replacement = ClinkerToml::parse(&private_replacement)
        .unwrap()
        .resolve_observability(None)
        .unwrap();
    assert!(!format!("{private_replacement:?}").contains("private-token"));

    let unrelated = format!("{valid}\n[storage.staging]\nenabled = \"not-a-boolean\"\n");
    let unrelated = ClinkerToml::parse(&unrelated).expect_err("storage type error remains storage");
    assert!(unrelated.classification().is_none());

    let local = minimal_policy("mode = \"none\"", "").replace(
        "[observability.lineage]\n",
        "[observability.lineage]\nidentity_mode = \"local_diagnostic_paths\"\n",
    );
    let local = ClinkerToml::parse(&local)
        .expect("exact local compatibility mode parses")
        .resolve_observability(None)
        .expect("local compatibility mode needs no external binding");
    assert_eq!(
        local.lineage().unwrap().identity_mode(),
        LineageIdentityMode::LocalDiagnosticPaths
    );
    assert!(local.lineage().unwrap().datasets().is_empty());
    assert_eq!(sentinel.path().read_dir().unwrap().count(), 0);
}
