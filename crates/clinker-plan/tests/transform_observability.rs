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
    assert!(
        retired_rule.contains("`log_rule` is retired"),
        "{retired_rule}"
    );
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

/// An unknown key must not be answered with a suggestion the author cannot
/// act on. `log_rule` is recognized only so it can be rejected, so listing it
/// as an alternative routes the author to a second guaranteed failure.
#[test]
fn unknown_directive_key_suggests_only_usable_keys() {
    let message = admission_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, conditon: \"amount > 1000\" }\n",
    ));

    assert!(
        message.contains("unknown log directive key `conditon`"),
        "rejection must name the offending key: {message}"
    );
    assert!(
        !message.contains("log_rule"),
        "rejection must not advertise a key that is itself always rejected: {message}"
    );
    for usable in [
        "name",
        "level",
        "when",
        "message",
        "fields",
        "every",
        "condition",
    ] {
        assert!(
            message.contains(&format!("`{usable}`")),
            "rejection must name the usable key `{usable}`: {message}"
        );
    }
    // Take the directive the diagnostic actually printed and compile it. A
    // suggested form that does not parse is worse than none, and lifting it
    // out of the message rather than restating it keeps this honest if the
    // wording changes.
    let suggested = message
        .split_once("for example `")
        .and_then(|(_, rest)| rest.split_once('`'))
        .map(|(directive, _)| directive.to_string())
        .unwrap_or_else(|| panic!("rejection must offer a pasteable directive: {message}"));
    assert!(
        suggested.starts_with("- { name: transform.customer_seen"),
        "suggested directive must be a complete list entry: {suggested}"
    );
    compile(&pipeline("", &format!("        {suggested}\n")))
        .expect("the directive the diagnostic suggests must itself compile");
}

/// A condition changes which records emit, so it is execution meaning and must
/// reach semantic plan identity. If it did not, two pipelines differing only in
/// their gate would share a fingerprint and plan reuse could apply the wrong
/// predicate.
#[test]
fn condition_enters_semantic_plan_identity() {
    let directive = |condition: &str| {
        format!(
            "        - {{ name: transform.seen, level: info, when: per_record, every: 1, message: seen, condition: \"{condition}\" }}\n"
        )
    };
    let fingerprint = |yaml: &str| {
        compile(yaml)
            .expect("fixture must compile")
            .semantic_fingerprint()
            .expect("fixture must fingerprint")
    };

    let base = fingerprint(&pipeline("", &directive("amount > 1000")));
    let widened = fingerprint(&pipeline("", &directive("amount > 10")));
    assert_ne!(
        base, widened,
        "changing a gate changes which records emit, so identity must change"
    );

    // The same gate must still fingerprint stably.
    assert_eq!(
        base,
        fingerprint(&pipeline("", &directive("amount > 1000"))),
        "an unchanged gate must not perturb identity"
    );

    let ungated = fingerprint(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen }\n",
    ));
    assert_ne!(
        base, ungated,
        "adding a gate must change identity, not be invisible to it"
    );
}

/// The authored spelling pipelines already use. A `condition` must survive
/// admission and reach the plan as a typechecked gate program, positionally
/// paired with its directive.
#[test]
fn per_record_condition_compiles_to_a_typed_gate() {
    let yaml = pipeline(
        "",
        r#"        - name: transform.big_order
          level: info
          when: per_record
          every: 1
          condition: "amount > 1000"
          message: "big order"
          fields: [customer_id]
        - name: transform.every_order
          level: debug
          when: per_record
          every: 1
          message: "order seen"
"#,
    );

    let plan = compile(&yaml).expect("an authored condition must compile");
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

    assert_eq!(payload.log.len(), 2);
    assert_eq!(
        payload.log[0].condition.as_ref().map(|c| c.source.as_str()),
        Some("amount > 1000"),
        "the authored condition text must survive admission"
    );
    assert!(payload.log[1].condition.is_none());

    // The gate slots are parallel to the directives, so index 0 carries a
    // program and index 1 does not.
    assert_eq!(
        payload.log_conditions.len(),
        payload.log.len(),
        "every directive needs a gate slot"
    );
    let gate = payload.log_conditions[0]
        .as_ref()
        .expect("a declared condition must lower to a typed program");
    assert!(
        !gate.program.statements.is_empty(),
        "the gate must carry a compiled predicate"
    );
    assert!(
        payload.log_conditions[1].is_none(),
        "a directive with no condition must stay ungated"
    );
}

/// A gate needs a record to test. Lifecycle events have none, and `on_error`
/// reports a failure the author already selected by routing it.
#[test]
fn condition_is_rejected_on_timings_without_a_record_to_gate() {
    for timing in ["before_transform", "after_transform", "on_error"] {
        let message = admission_error(&pipeline(
            "",
            &format!(
                "        - {{ name: transform.seen, level: info, when: {timing}, message: seen, condition: \"amount > 1000\" }}\n"
            ),
        ));
        assert!(
            message.contains("`condition` is only valid with `when: per_record`"),
            "{timing}: {message}"
        );
        assert!(
            message.contains("when: per_record"),
            "rejection must name the corrected form: {message}"
        );
    }
}

/// Render the diagnostics from a full compile. CXL typechecking runs in
/// `compile()`, not in the topology-only pass `admission_error` uses.
fn compile_error(yaml: &str) -> String {
    let diagnostics = compile(yaml).err().unwrap_or_else(|| {
        panic!("expected the compile to be rejected");
    });
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

/// A gate is CXL, so it is typechecked against the transform's INPUT schema
/// and must resolve to a boolean — not deferred to a runtime surprise.
#[test]
fn condition_is_typechecked_against_the_input_schema() {
    let unknown_field = compile_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, condition: \"nonexistent > 1\" }\n",
    ));
    assert!(
        unknown_field.contains("nonexistent"),
        "an unresolvable gate field must be named: {unknown_field}"
    );

    let not_boolean = compile_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, condition: \"amount\" }\n",
    ));
    assert!(
        not_boolean.contains("E200") || not_boolean.contains("bool"),
        "a non-boolean gate must be a type error: {not_boolean}"
    );
}

/// Pins WHICH schema a gate binds against. Dispatch fires before the
/// transform's own program runs, so a field the transform only *produces* is
/// not in scope for the gate, while the input field it derives from is.
#[test]
fn condition_binds_against_the_input_row_not_the_output_row() {
    let fixture = |condition: &str| {
        format!(
            r#"
pipeline:
  name: gate_scope
nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - {{ name: amount, type: int }}
  - type: transform
    name: observe
    input: input
    config:
      cxl: |
        emit doubled = amount * 2
      log:
        - {{ name: transform.seen, level: info, when: per_record, every: 1, message: seen, condition: "{condition}" }}
  - type: output
    name: output
    input: observe
    config:
      name: output
      type: csv
      path: output.csv
"#
        )
    };

    compile(&fixture("amount > 1000")).expect("an input-row field must resolve in a gate");

    let output_only = compile_error(&fixture("doubled > 1000"));
    assert!(
        output_only.contains("doubled"),
        "a gate must not resolve against the transform's output row: {output_only}"
    );
}

/// A gate runs per record, so its source is bounded like `message` is.
#[test]
fn condition_bounds_are_enforced() {
    let empty = admission_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, condition: \"   \" }\n",
    ));
    assert!(empty.contains("CXL boolean expression"), "{empty}");
    assert!(empty.contains("amount > 1000"), "{empty}");

    let too_long = admission_error(&pipeline(
        "",
        &format!(
            "        - {{ name: transform.seen, level: info, when: per_record, every: 1, message: seen, condition: \"amount > {}\" }}\n",
            "1".repeat(512)
        ),
    ));
    assert!(too_long.contains("at most 512 UTF-8 bytes"), "{too_long}");
}

/// The directive printed in `docs/user/src/nodes/transform.md` must compile.
/// Documentation an author pastes and cannot run is a defect.
#[test]
fn documented_condition_example_compiles() {
    let plan = compile(&pipeline(
        "",
        r#"        - name: transform.large_order
          level: info
          when: per_record
          every: 1
          condition: "amount > 1000"
          message: "large order"
          fields: [customer_id, amount]
"#,
    ))
    .expect("the documented condition example must compile");

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
    assert!(payload.log_conditions[0].is_some());
}

/// A repeated key must be rejected rather than silently taking the last value.
#[test]
fn duplicate_directive_key_is_rejected() {
    let message = admission_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: after_transform, message: first, message: second }\n",
    ));
    assert!(message.contains("message"), "{message}");
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

/// A gate is one predicate.
///
/// The condition is spliced into `filter <source>`, so a source carrying a
/// statement separator compiles into statements the author never wrote into a
/// gate. A `distinct` reached that way lands in an evaluator that was not
/// allocated to run one and aborted the process at the first record, with no
/// classified failure and no terminal event, rather than failing the plan.
#[test]
fn a_condition_carrying_extra_statements_is_refused_at_plan_time() {
    let message = compile_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, condition: \"amount > 1\\ndistinct\" }\n",
    ));
    assert!(
        message.contains("E373"),
        "an injected statement must be refused: {message}"
    );
    assert!(
        message.contains("must be one predicate"),
        "the rejection must name the rule it broke: {message}"
    );
    assert!(
        message.contains("amount > 1 and region == 'eu'"),
        "the rejection must show a corrected form: {message}"
    );
}
