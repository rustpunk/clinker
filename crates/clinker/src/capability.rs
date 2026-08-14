//! What this build can actually do, checked against what a pipeline asks for.
//!
//! The `rest`, `otlp` and `lineage` features are on by default, so these
//! checks pass trivially in the published binary. A deployment that turns one
//! off gets a binary that still parses every construct the full one does —
//! nothing here changes the config grammar — and refuses the ones it cannot
//! carry out, at validation time and by name.
//!
//! Refusing early is the point. A `rest` source discovered mid-run, or an OTLP
//! endpoint admitted after the first records have been staged, is a partial
//! run to unpick. Each check runs before anything is opened or written.

use clinker_plan::error::PipelineError;

/// Reject a pipeline that declares a `rest` source in a build without one.
///
/// Always `Ok` when the `rest` feature is on, which is the default.
#[cfg(feature = "rest")]
pub(crate) const fn check_pipeline(
    _config: &clinker_plan::config::PipelineConfig,
) -> Result<(), PipelineError> {
    Ok(())
}

#[cfg(not(feature = "rest"))]
pub(crate) fn check_pipeline(
    config: &clinker_plan::config::PipelineConfig,
) -> Result<(), PipelineError> {
    for body in config.source_bodies() {
        let source = &body.source;
        if matches!(
            source.transport,
            clinker_plan::config::SourceTransport::Rest(_)
        ) {
            return Err(PipelineError::Config(
                clinker_plan::config::ConfigError::Validation(format!(
                    "[E223] source '{name}' declares a `transport:` block with `kind: rest`, but \
                     this build of clinker was compiled without the `rest` capability, so it has \
                     no HTTP client to pull those pages with. Either run the pipeline with a \
                     build that has it — the released binary does, and a build from source gets \
                     it with `cargo build -p clinker` — or read the data from a file instead, by \
                     replacing the `transport:` block with a `path:`:\n\
                     \x20 - type: source\n\
                     \x20   name: {name}\n\
                     \x20   config:\n\
                     \x20     name: {name}\n\
                     \x20     type: json\n\
                     \x20     path: ./data/{name}.json\n\
                     \x20     schema:\n\
                     \x20       - {{ name: id, type: int }}",
                    name = source.name
                )),
            ));
        }
    }
    Ok(())
}

/// Reject an `observability.otlp` block in a build with no exporter.
///
/// Always `Ok` when the `otlp` feature is on, which is the default.
#[cfg(feature = "otlp")]
pub(crate) const fn check_observability(
    _policy: &clinker_plan::config::ResolvedObservabilityPolicy,
) -> Result<(), PipelineError> {
    Ok(())
}

#[cfg(not(feature = "otlp"))]
pub(crate) fn check_observability(
    policy: &clinker_plan::config::ResolvedObservabilityPolicy,
) -> Result<(), PipelineError> {
    if policy.otlp().is_none() {
        return Ok(());
    }
    Err(crate::observability_configuration_error(
        "clinker.toml sets `observability.otlp.endpoint`, but this build of clinker was compiled \
         without the `otlp` capability, so nothing would be exported to that collector. Either \
         run with a build that has it — the default binary does, and a build from source gets it \
         with `cargo build -p clinker` — or remove the `[observability.otlp]` table. Note that \
         removing it also stops telemetry being recorded: the arena is reserved only for an \
         exporter to drain, so the `--machine ndjson-v1` terminal then carries no \
         `observability` field.",
    ))
}

/// Reject `--lineage` / `--lineage-events` in a build with no lineage emitter.
///
/// Always `Ok` when the `lineage` feature is on, which is the default.
#[cfg(feature = "lineage")]
pub(crate) const fn check_lineage_requested(_requested: bool) -> Result<(), PipelineError> {
    Ok(())
}

#[cfg(not(feature = "lineage"))]
pub(crate) fn check_lineage_requested(requested: bool) -> Result<(), PipelineError> {
    if !requested {
        return Ok(());
    }
    Err(PipelineError::Config(
        clinker_plan::config::ConfigError::Validation(
            "`--lineage` and `--lineage-events` name an OpenLineage destination, but this build \
             of clinker was compiled without the `lineage` capability. Either run with a build \
             that has it — the default binary does, and a build from source gets it with \
             `cargo build -p clinker` — or drop the flag:\n  \
             clinker run pipeline.yaml"
                .to_string(),
        ),
    ))
}

#[cfg(test)]
mod tests {
    /// Resolve deployment TOML the way `run` does, so these assertions are
    /// about the policy the check actually receives.
    #[cfg(not(feature = "otlp"))]
    fn observability_policy(toml: &str) -> clinker_plan::config::ResolvedObservabilityPolicy {
        clinker_plan::config::ClinkerToml::parse(toml)
            .expect("deployment config parses in every build")
            .resolve_observability(None)
            .expect("the policy resolves")
    }

    /// A pipeline with one `rest` source, in the shape the parser accepts.
    #[cfg(not(feature = "rest"))]
    const REST_PIPELINE: &str = r"
pipeline:
  name: capability_test
nodes:
  - type: source
    name: api
    config:
      name: api
      type: json
      transport:
        kind: rest
        url: https://example.com/v1/items
        max_pages: 2
        retries: 1
        timeout_secs: 5
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: api
    config:
      name: out
      type: csv
      path: out.csv
";

    /// A build without the capability must refuse the pipeline rather than run
    /// it as if the source were absent, and must say which source it means.
    #[cfg(not(feature = "rest"))]
    #[test]
    fn a_rest_source_is_refused_by_name_when_the_transport_is_not_compiled_in() {
        let config = clinker_plan::config::parse_config(REST_PIPELINE)
            .expect("the grammar is the same in every build; only the capability differs");
        let error = super::check_pipeline(&config)
            .expect_err("a rest source must not be accepted by a build that cannot fetch it")
            .to_string();
        assert!(error.contains("E223"), "{error}");
        assert!(
            error.contains("'api'"),
            "the diagnostic must name the offending source: {error}"
        );
        assert!(
            error.contains("rest"),
            "the diagnostic must name the rule that was broken: {error}"
        );
    }

    /// The correction a diagnostic offers has to be one the parser accepts. A
    /// suggestion that does not parse sends an author off to fix the fix.
    #[cfg(not(feature = "rest"))]
    #[test]
    fn the_offered_correction_parses() {
        let config =
            clinker_plan::config::parse_config(REST_PIPELINE).expect("fixture pipeline parses");
        let error = super::check_pipeline(&config)
            .expect_err("a rest source is refused")
            .to_string();
        // The suggestion is written at the indentation a `nodes:` list wants,
        // so an author pastes it verbatim. That is what this does.
        let suggestion: String = error
            .lines()
            .skip_while(|line| !line.trim_start().starts_with("- type: source"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            suggestion.contains("path: ./data/api.json"),
            "the suggestion was not found in the message: {error}"
        );
        let corrected = format!(
            "pipeline:\n  name: capability_test\nnodes:\n{suggestion}\n  - type: output\n    \
             name: out\n    input: api\n    config:\n      name: out\n      type: csv\n      \
             path: out.csv\n"
        );
        clinker_plan::config::parse_config(&corrected)
            .expect("the corrected form the diagnostic offers must itself be valid config");
    }

    /// A build with no exporter must not accept a collector endpoint and then
    /// export nothing to it.
    /// The complete deployment policy, which is the only shape the strict
    /// parser takes: a lone `[observability.otlp]` table is rejected as
    /// incomplete before this check would ever see it.
    #[cfg(not(feature = "otlp"))]
    const OBSERVABILITY_WITH_COLLECTOR: &str = r#"[observability]
arena_bytes = "64KB"
ordinary_lane_bytes = "32KB"
high_severity_lane_bytes = "32KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 4
max_attribute_bytes = "256B"
sample_every = 1
rate_limit_per_second = 1000
rate_limit_burst = 1000
flush_timeout_ms = 500

[observability.otlp]
endpoint = "https://collector.example.com"
connect_timeout_ms = 20
request_timeout_ms = 50
retry_max_attempts = 1
retry_total_timeout_ms = 100
max_response_bytes = "4KB"

[observability.otlp.auth]
mode = "none"
"#;

    #[cfg(not(feature = "otlp"))]
    #[test]
    fn an_otlp_endpoint_is_refused_when_no_exporter_is_compiled_in() {
        let policy = observability_policy(OBSERVABILITY_WITH_COLLECTOR);
        let error = super::check_observability(&policy)
            .expect_err("an endpoint nothing would send to must be refused")
            .to_string();
        assert!(
            error.contains("observability.otlp.endpoint"),
            "the diagnostic must name the offending key: {error}"
        );
        assert!(
            error.contains("otlp"),
            "the diagnostic must name the capability: {error}"
        );
    }

    /// Deployment config that asks for nothing optional is accepted whatever
    /// this build has, so turning a feature off does not reject a whole class
    /// of working configuration.
    #[cfg(not(feature = "otlp"))]
    #[test]
    fn deployment_config_without_an_otlp_block_is_accepted() {
        let policy = observability_policy("");
        super::check_observability(&policy)
            .expect("no collector was asked for, so nothing is missing");
    }

    /// The flags name a destination, so a build that cannot write one has to
    /// say so rather than exit zero having emitted nothing.
    #[cfg(not(feature = "lineage"))]
    #[test]
    fn a_lineage_destination_is_refused_when_no_emitter_is_compiled_in() {
        let error = super::check_lineage_requested(true)
            .expect_err("a lineage destination must not be silently ignored")
            .to_string();
        assert!(error.contains("--lineage"), "{error}");
        super::check_lineage_requested(false).expect("a run that asked for none is unaffected");
    }
}
