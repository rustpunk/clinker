//! Public admission contract for transform-authored observability events.

use clinker_core_types::Diagnostic;
use clinker_plan::config::{CompileContext, LogDirective, LogLevel, LogTiming, parse_config};
use clinker_plan::plan::execution::PlanNode;
use clinker_plan::yaml::CxlSource;

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

/// A requested field the record cannot carry is refused before the run.
///
/// `fields` is the only channel by which record data reaches an event, so a
/// selector that matches no column publishes the event with the attribute
/// simply absent — which reads exactly like a run where no record had a value
/// for it. The failure mode is a spelling slip, so the rejection names the
/// column the author most likely meant.
#[test]
fn a_field_the_input_row_does_not_carry_is_refused_at_plan_time() {
    let message = compile_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, fields: [customerId] }\n",
    ));
    assert!(
        message.contains("E374"),
        "an unmatched selector must be refused: {message}"
    );
    assert!(
        message.contains("`customerId`"),
        "the rejection must name the offending input: {message}"
    );
    assert!(
        message.contains("`customer_id`, `amount`"),
        "the rejection must state what the input row does carry: {message}"
    );
    assert!(
        message.contains("write `fields: [customer_id]`"),
        "the rejection must offer a pasteable correction: {message}"
    );
}

/// `fields` is legal on `on_error` too, and the check covers it.
///
/// An error event fires on the record that failed — the same input record the
/// per-record path reads — so the selector is decidable in exactly the same
/// way, and leaving `on_error` unchecked would put the empty attribute on the
/// events an operator reads first.
#[test]
fn an_on_error_field_the_input_row_does_not_carry_is_refused_at_plan_time() {
    let message = compile_error(&pipeline(
        "",
        "        - { name: transform.failed, level: error, when: on_error, message: failed, fields: [customer_id, orderId] }\n",
    ));
    assert!(
        message.contains("E374") && message.contains("`orderId`"),
        "an unmatched selector on an error event must be refused: {message}"
    );
    assert_eq!(
        message.matches("E374").count(),
        1,
        "the declared selector beside it must not be reported: {message}"
    );
}

/// Pins WHICH row a selector binds against, exactly as the gate test pins the
/// gate. Dispatch runs before the transform's own program, so a column the
/// transform produces is not something a directive can request.
#[test]
fn fields_bind_against_the_input_row_not_the_output_row() {
    let fixture = |field: &str| {
        format!(
            r#"
pipeline:
  name: field_scope
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
        - {{ name: transform.seen, level: info, when: per_record, every: 1, message: seen, fields: [{field}] }}
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

    compile(&fixture("amount")).expect("an input-row column must resolve in a field request");

    let output_only = compile_error(&fixture("doubled"));
    assert!(
        output_only.contains("E374") && output_only.contains("`doubled`"),
        "a request must not resolve against the transform's output row: {output_only}"
    );
}

/// A dotted selector is a flat column name, not a path into a nested value.
///
/// Dispatch reads it with one flat lookup, so `customer.id` resolves only if
/// the input row declares a column spelled exactly that — which flattening
/// readers do produce. A row without one is refused here rather than emitting
/// a permanently empty attribute.
#[test]
fn a_dotted_selector_naming_no_column_is_refused_at_plan_time() {
    let message = compile_error(&pipeline(
        "",
        "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, fields: [customer.id] }\n",
    ));
    assert!(
        message.contains("E374") && message.contains("`customer.id`"),
        "a dotted selector matching no column must be refused: {message}"
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

/// A gate is one predicate, whatever the extra statement is.
///
/// A `use` an author writes into a gate is refused before the first record like
/// any other spliced statement. It is refused by CXL's own ordering rule rather
/// than by the one-predicate count — the gate is spliced after `filter`, so a
/// declaration can only land somewhere the grammar already rejects — and the
/// rejection still names the offending node and the rule it broke.
#[test]
fn a_condition_carrying_an_authored_use_is_refused_at_plan_time() {
    for condition in [
        "amount > 1\\nuse other.module as m",
        "use other.module as m\\namount > 1",
    ] {
        let message = compile_error(&pipeline(
            "",
            &format!(
                "        - {{ name: transform.seen, level: info, when: per_record, every: 1, message: seen, condition: \"{condition}\" }}\n"
            ),
        ));
        assert!(
            message.contains("observe:log[0]:condition"),
            "the rejection must name the offending gate: {message}"
        );
        assert!(
            message.contains("use must appear before") || message.contains("unexpected token Use"),
            "the rejection must name the rule it broke: {message}"
        );
    }
}

/// A transform's own module aliases still reach its gate.
///
/// The prelude is engine-supplied, so discounting it is what keeps a gate in a
/// module-importing transform legal — the guard counts statements past the
/// prelude, not every non-`use` statement.
#[test]
fn a_gate_in_a_module_importing_transform_still_compiles() {
    let yaml = pipeline("", "        - { name: transform.seen, level: info, when: per_record, every: 1, message: seen, condition: \"amount > 1\" }\n")
        .replace(
            "        emit customer_id = customer_id",
            "        use other.module as m\n        emit customer_id = customer_id",
        );
    compile(&yaml).expect("a gate must survive the transform's module aliases");
}

/// An optional key written with no value is the absent key.
///
/// `fields:` with nothing after it is legal YAML for "absent", and it is what
/// an author gets by commenting out the list's items. Binding the inner type
/// asked serde for a sequence and rejected the null with a raw type error.
#[test]
fn optional_directive_keys_written_with_no_value_are_absent() {
    let yaml = pipeline(
        "",
        r#"        - name: transform.completed
          level: info
          when: after_transform
          message: "Transform completed"
          fields:
          every:
          condition:
"#,
    );

    let plan = compile(&yaml).expect("explicit nulls must read as absent keys");
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

    assert_eq!(payload.log.len(), 1);
    assert!(payload.log[0].fields.is_none());
    assert!(payload.log[0].every.is_none());
    assert!(payload.log[0].condition.is_none());
}

/// A repeated key is still a repeated key when it is written with no value.
#[test]
fn a_repeated_valueless_optional_key_is_still_rejected() {
    let message = admission_error(&pipeline(
        "",
        r#"        - name: transform.completed
          level: info
          when: after_transform
          message: "Transform completed"
          fields:
          fields:
"#,
    ));
    assert!(message.contains("fields"), "{message}");
}

/// A serialized directive parses back through the admission that accepted it.
#[test]
fn a_log_directive_round_trips_through_yaml() {
    for directive in [
        LogDirective {
            name: "transform.customer_seen".to_owned(),
            level: LogLevel::Info,
            when: LogTiming::PerRecord,
            message: "customer processed".to_owned(),
            fields: Some(vec!["customer_id".to_owned()]),
            every: Some(2),
            condition: Some(CxlSource::unspanned("amount > 1000")),
        },
        LogDirective {
            name: "transform.completed".to_owned(),
            level: LogLevel::Info,
            when: LogTiming::AfterTransform,
            message: "transform completed".to_owned(),
            fields: None,
            every: None,
            condition: None,
        },
    ] {
        let rendered = clinker_plan::yaml::to_string(&directive).expect("serialize directive");
        let parsed: LogDirective =
            clinker_plan::yaml::from_str(&rendered).unwrap_or_else(|error| {
                panic!("a serialized directive must parse back: {error}\n{rendered}")
            });
        assert_eq!(parsed.name, directive.name);
        assert_eq!(parsed.message, directive.message);
        assert_eq!(parsed.when, directive.when);
        assert_eq!(parsed.level, directive.level);
        assert_eq!(parsed.fields, directive.fields);
        assert_eq!(parsed.every, directive.every);
        assert_eq!(parsed.condition, directive.condition);
    }
}
