//! Public admission contract for transform-authored observability events.

use clinker_core_types::Diagnostic;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::plan::execution::PlanNode;

fn pipeline(pipeline_extra: &str, directives: &str) -> String {
    format!(
        r#"
pipeline:
  name: transform_observability
{pipeline_extra}nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - {{ name: customer_id, type: string }}
        - {{ name: amount, type: int }}
  - type: transform
    name: observe
    input: input
    config:
      cxl: |
        emit customer_id = customer_id
        emit amount = amount
      log:
{directives}  - type: output
    name: output
    input: observe
    config:
      name: output
      type: csv
      path: output.csv
"#
    )
}

fn compile(yaml: &str) -> Result<clinker_plan::plan::CompiledPlan, Vec<Diagnostic>> {
    parse_config(yaml)
        .expect("fixture must pass YAML admission")
        .compile(&CompileContext::default())
}

fn admission_error(yaml: &str) -> String {
    match parse_config(yaml) {
        Err(error) => error.to_string(),
        Ok(config) => {
            let diagnostics = config.compile_topology_only(&CompileContext::default());
            assert!(
                diagnostics.iter().any(|diagnostic| diagnostic
                    .primary
                    .span
                    .synthetic_line_number()
                    .is_some()),
                "rejection must retain an authored source line: {diagnostics:?}"
            );
            diagnostics
                .into_iter()
                .map(|diagnostic| {
                    format!(
                        "{}: {} Help: {}",
                        diagnostic.code,
                        diagnostic.message,
                        diagnostic.help.unwrap_or_default()
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
    }
}

#[test]
fn strict_directives_compile_static_named_events() {
    let yaml = pipeline(
        "",
        r#"        - name: transform.starting
          level: debug
          when: before_transform
          message: "Starting transform"
        - name: transform.completed
          level: info
          when: after_transform
          message: "Transform completed"
        - name: transform.customer_seen
          level: info
          when: per_record
          every: 1
          message: "Customer processed"
          fields: [customer_id, amount]
        - name: transform.customer_failed
          level: error
          when: on_error
          message: "Customer processing failed"
          fields: [customer_id]
"#,
    );

    let plan = compile(&yaml).expect("strict directives must compile");
    let payload = plan
        .dag()
        .graph
        .node_weights()
        .find_map(|node| match node {
            PlanNode::Transform {
                resolved: Some(payload),
                ..
            } => Some(payload),
            _ => None,
        })
        .expect("compiled plan must retain the transform payload");

    assert_eq!(payload.log.len(), 4);
    assert_eq!(payload.log[0].name, "transform.starting");
    assert_eq!(payload.log[0].message, "Starting transform");
    assert_eq!(payload.log[2].name, "transform.customer_seen");
    assert_eq!(payload.log[2].every, Some(1));
    assert_eq!(
        payload.log[2].fields.as_deref(),
        Some(["customer_id".to_string(), "amount".to_string()].as_slice())
    );
    assert_eq!(
        payload.log[3].fields.as_deref(),
        Some(["customer_id".to_string()].as_slice())
    );
}

#[test]
fn strict_directives_require_explicit_positive_per_record_cadence() {
    for (directive, expected) in [
        (
            "        - { name: transform.seen, level: info, when: per_record, message: seen }\n",
            "requires explicit `every`",
        ),
        (
            "        - { name: transform.seen, level: info, when: per_record, every: 0, message: seen }\n",
            "at least 1",
        ),
        (
            "        - { name: transform.done, level: info, when: after_transform, every: 2, message: done }\n",
            "only valid with `when: per_record`",
        ),
    ] {
        let message = admission_error(&pipeline("", directive));
        assert!(message.contains(expected), "{message}");
        assert!(
            message.contains("line"),
            "rejection must name its source line: {message}"
        );
    }

    compile(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen }\n",
    ))
    .expect("explicit every: 1 must remain valid");
}

#[test]
fn strict_directives_reject_interpolation_and_retired_policy_with_guidance() {
    let interpolation = admission_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: \"customer {customer_id}\" }\n",
    ));
    assert!(interpolation.contains("static"), "{interpolation}");
    assert!(
        interpolation.contains("fields: [customer_id]"),
        "{interpolation}"
    );
    assert!(interpolation.contains("line"), "{interpolation}");

    let retired = admission_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, log_rule: external }\n",
    ));
    assert!(retired.contains("`log_rule` is retired"), "{retired}");
    assert!(
        retired.contains("name: transform.customer_seen"),
        "{retired}"
    );
    assert!(retired.contains("fields: [customer_id]"), "{retired}");
    assert!(retired.contains("line"), "{retired}");

    let routing = admission_error(&pipeline(
        "  log_rules: {}\n",
        "        - { name: transform.done, level: info, when: after_transform, message: done }\n",
    ));
    assert!(
        routing.contains("pipeline.log_rules is unsupported"),
        "{routing}"
    );
    assert!(
        routing.contains("remove the entire `log_rules:` entry"),
        "{routing}"
    );
}

#[test]
fn retired_surfaces_fail_at_plan_admission() {
    let retired_rule = admission_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, log_rule: external }\n",
    ));
    assert!(retired_rule.contains("`log_rule` is retired"), "{retired_rule}");
    assert!(retired_rule.contains("line"), "{retired_rule}");

    let retired_routing = admission_error(&pipeline(
        "  log_rules: {}\n",
        "        - { name: transform.done, level: info, when: after_transform, message: done }\n",
    ));
    assert!(
        retired_routing.contains("pipeline.log_rules is unsupported"),
        "{retired_routing}"
    );
    assert!(
        retired_routing.contains("remove the entire `log_rules:` entry"),
        "{retired_routing}"
    );

    let interpolation = admission_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: \"customer {customer_id}\" }\n",
    ));
    assert!(interpolation.contains("static"), "{interpolation}");
    assert!(
        interpolation.contains("fields: [customer_id]"),
        "{interpolation}"
    );
}

#[test]
fn strict_directives_enforce_name_message_and_field_bounds() {
    let too_long_name = format!("event.{}", "x".repeat(128));
    let message = admission_error(&pipeline(
        "",
        &format!(
            "        - {{ name: {too_long_name}, level: info, when: after_transform, message: done }}\n"
        ),
    ));
    assert!(message.contains("bounded dotted identifier"), "{message}");

    let message = admission_error(&pipeline(
        "",
        &format!(
            "        - {{ name: transform.done, level: info, when: after_transform, message: \"{}\" }}\n",
            "m".repeat(1_025)
        ),
    ));
    assert!(message.contains("at most 1024 UTF-8 bytes"), "{message}");

    let fields = (0..257)
        .map(|index| format!("field_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let message = admission_error(&pipeline(
        "",
        &format!(
            "        - {{ name: transform.seen, level: info, when: per_record, every: 1, message: seen, fields: [{fields}] }}\n"
        ),
    ));
    assert!(message.contains("at most 256"), "{message}");
}

#[test]
fn strict_directives_reject_duplicate_or_lifecycle_field_requests() {
    let duplicate = admission_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, fields: [customer_id, customer_id] }\n",
    ));
    assert!(duplicate.contains("appears more than once"), "{duplicate}");

    let lifecycle = admission_error(&pipeline(
        "",
        "        - { name: transform.done, level: info, when: after_transform, message: done, fields: [customer_id] }\n",
    ));
    assert!(
        lifecycle.contains("only valid with `when: per_record` or `when: on_error`"),
        "{lifecycle}"
    );
}
