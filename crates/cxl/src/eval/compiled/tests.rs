//! Differential and property tests proving the compiled evaluator
//! produces byte-identical [`EvalResult`]s to the tree-walk.
//!
//! The corpus exercises the full row-level surface both evaluators
//! define: the scalar core, record-level method calls and closure-bearing
//! array builtins, window reads (aggregate / positional-chain / rank /
//! predicate-fold), `distinct` (across a record stream with shared dedup
//! state and partition resets), and `emit each` fan-out. The fidelity
//! assertion compares the field map, the `$record.*` channel, the skip
//! reason, the `EmitMany` fan-out records, and — on the error path — the
//! error kind, span, and boundary provenance fields, byte for byte. Float
//! results are NaN-canonicalized first so an agreed NaN compares equal
//! rather than false-failing. On any divergence the compiled node is the
//! thing to fix; the assertion is never weakened.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{RecordStorage, Value};
use proptest::prelude::*;

use super::{CompiledProgram, EvalState, compile, program_has_distinct};
use crate::eval::context::{EvalContext, StableEvalContext};
use crate::eval::error::EvalError;
use crate::eval::{DEFAULT_MAX_EXPANSION, EvalResult, ProgramEvaluator, SkipReason};
use crate::parser::Parser;
use crate::resolve::HashMapResolver;
use crate::resolve::pass::resolve_program;
use crate::typecheck::pass::{TypedProgram, type_check};
use crate::typecheck::row::Row;

/// Window storage stand-in for the no-window scalar-core programs under
/// test. Every method is unreachable here; the scalar core never reads
/// the window, so the differential harness always passes `None`.
struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u64, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _: u64) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u64 {
        0
    }
}

fn empty_row() -> Row {
    Row::closed(indexmap::IndexMap::new(), crate::lexer::Span::new(0, 0))
}

/// Parse → resolve → typecheck a CXL program against the given input
/// field names, attaching the source text so error spans render
/// identically through both evaluators. Returns `None` when the program
/// fails to parse, resolve, or typecheck — the proptest uses this to
/// discard generated programs that are not well-typed CXL (e.g. a field
/// used as both numeric and string), since neither evaluator is defined
/// on them.
fn try_type_program(src: &str, fields: &[&str]) -> Option<TypedProgram> {
    let parsed = Parser::parse(src);
    if !parsed.errors.is_empty() {
        return None;
    }
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count).ok()?;
    Some(
        type_check(resolved, &empty_row())
            .ok()?
            .with_source(Arc::from(src)),
    )
}

/// Like [`try_type_program`] but panics on any failure. Used by the
/// hand-written corpus, where every program is valid CXL by
/// construction.
fn type_program(src: &str, fields: &[&str]) -> TypedProgram {
    let parsed = Parser::parse(src);
    assert!(
        parsed.errors.is_empty(),
        "parse errors: {:?}",
        parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap_or_else(|d| {
        panic!(
            "resolve errors: {:?}",
            d.iter().map(|e| &e.message).collect::<Vec<_>>()
        )
    });
    type_check(resolved, &empty_row())
        .unwrap_or_else(|d| {
            panic!(
                "type errors: {:?}",
                d.iter().map(|e| &e.message).collect::<Vec<_>>()
            )
        })
        .with_source(Arc::from(src))
}

/// Run the compiled program through the same outermost error boundary
/// the tree-walk's `ProgramEvaluator::eval_record` applies — stamping
/// `source_row` / `source_expr` onto a surfaced error — so the two
/// evaluators are compared on identical boundary provenance.
///
/// `window` and `state` are threaded explicitly so the same helper drives
/// the no-window scalar corpus (`None` window), the window corpus (a
/// concrete `WindowContext`), and the multi-record distinct corpus (a
/// shared `EvalState` across records).
fn compiled_eval_record<S: RecordStorage + 'static>(
    program: &CompiledProgram<S>,
    typed: &TypedProgram,
    ctx: &EvalContext<'_>,
    resolver: &dyn crate::resolve::traits::FieldResolver,
    window: Option<&dyn crate::resolve::traits::WindowContext<'_, S>>,
    state: &mut crate::eval::compiled::EvalState,
) -> Result<EvalResult, EvalError> {
    let source_row = ctx.source_row;
    let source_expr = typed.source.clone();
    program
        .eval_record(ctx, resolver, window, state)
        .map_err(move |mut e| {
            if e.source_row.is_none() {
                e.source_row = Some(source_row);
            }
            if e.source_expr.is_none()
                && let Some(s) = source_expr
            {
                e.source_expr = Some(s);
            }
            e
        })
}

/// Ordered `(key, value)` pairs from an emit channel, captured for
/// equality comparison.
type Pairs = Vec<(String, Value)>;

/// One emitted record's two channels (fields, record-vars) in a
/// comparable form.
type EmitRecordProjection = (Pairs, Pairs);

/// Canonical, comparable projection of an [`EvalResult`] (or the error
/// it surfaced). Reduces both evaluators' outputs to plain owned data so
/// a single `assert_eq!` covers fields, record-vars, skip reason, and —
/// on the error path — the error kind string, span, and boundary
/// provenance fields.
#[derive(Debug, PartialEq)]
enum ResultProjection {
    Emit {
        fields: Pairs,
        record_vars: Pairs,
    },
    EmitMany {
        records: Vec<EmitRecordProjection>,
    },
    Skip(SkipReason),
    Err {
        kind: String,
        span: crate::lexer::Span,
        source_row: Option<u64>,
        source_expr: Option<String>,
        triggering_field: Option<String>,
    },
}

/// Replace every NaN float in `v` (including those nested inside arrays
/// and maps) with one canonical NaN bit pattern.
///
/// `Value`'s own `PartialEq` compares floats by `to_bits()`, so two NaNs
/// that agree only on "is a NaN" but carry different payload bits would
/// compare unequal and false-fail the differential assertion even though
/// the two evaluators agree. Canonicalizing here mirrors the
/// distinct-key path's NaN handling so an agreed NaN compares equal.
fn canonicalize_nan(v: &Value) -> Value {
    match v {
        Value::Float(f) if f.is_nan() => Value::Float(f64::NAN),
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_nan).collect()),
        Value::Map(m) => {
            let mut out: indexmap::IndexMap<Box<str>, Value> = indexmap::IndexMap::new();
            for (k, mv) in m.iter() {
                out.insert(k.clone(), canonicalize_nan(mv));
            }
            Value::Map(Box::new(out))
        }
        other => other.clone(),
    }
}

fn project(result: &Result<EvalResult, EvalError>) -> ResultProjection {
    fn pairs(m: &indexmap::IndexMap<String, Value>) -> Pairs {
        m.iter()
            .map(|(k, v)| (k.clone(), canonicalize_nan(v)))
            .collect()
    }
    match result {
        Ok(EvalResult::Emit {
            fields,
            record_vars,
        }) => ResultProjection::Emit {
            fields: pairs(fields),
            record_vars: pairs(record_vars),
        },
        Ok(EvalResult::EmitMany { records }) => ResultProjection::EmitMany {
            records: records
                .iter()
                .map(|r| (pairs(&r.fields), pairs(&r.record_vars)))
                .collect(),
        },
        Ok(EvalResult::Skip(reason)) => ResultProjection::Skip(*reason),
        Err(e) => ResultProjection::Err {
            // The kind enum is not PartialEq; its Debug form captures
            // every payload field deterministically, so comparing the
            // rendered string is an exact-equality check on the kind.
            kind: format!("{:?}", e.kind),
            span: e.span,
            source_row: e.source_row,
            source_expr: e.source_expr.as_ref().map(|s| s.to_string()),
            triggering_field: e.triggering_field.as_ref().map(|s| s.to_string()),
        },
    }
}

/// Assert the tree-walk and the compiled evaluator agree byte-for-byte
/// on `src` evaluated against `record`. Returns the shared projection so
/// callers can additionally assert the concrete outcome.
///
/// `TypedProgram` is not `Clone` and `ProgramEvaluator` needs an owned
/// `Arc<TypedProgram>`, so the program is type-checked twice from the
/// same source with the same input field set — the two are identical by
/// construction.
fn assert_agree(src: &str, fields: &[&str], record: HashMap<String, Value>) -> ResultProjection {
    let typed = type_program(src, fields);
    let has_distinct = program_has_distinct(&typed);

    let stable = StableEvalContext::test_default();
    let resolver = HashMapResolver::new(record);

    // Tree-walk path through the public, boundary-wrapped entry. The
    // distinct flag is detected the same way the compiled path detects it,
    // so a `distinct` program allocates dedup state on both sides.
    let ctx_tw = EvalContext::test_default_borrowed(&stable);
    let mut evaluator = ProgramEvaluator::new(Arc::new(type_program(src, fields)), has_distinct);
    let tree = evaluator.eval_record::<NullStorage>(&ctx_tw, &resolver, None);

    // Compiled path through the same boundary wrapper, with an `EvalState`
    // allocated under the same distinct flag.
    let ctx_c = EvalContext::test_default_borrowed(&stable);
    let program = compile::<NullStorage>(&typed);
    let mut state = EvalState::new(has_distinct, DEFAULT_MAX_EXPANSION);
    let comp = compiled_eval_record(&program, &typed, &ctx_c, &resolver, None, &mut state);

    let tree_proj = project(&tree);
    let comp_proj = project(&comp);
    assert_eq!(
        tree_proj, comp_proj,
        "tree-walk and compiled diverged on `{src}`",
    );
    tree_proj
}

// ── Differential corpus: the scalar core ───────────────────────────────

/// The full three-valued Kleene truth table for `and` / `or`. Each
/// operand is forced to true / false / null via a let-bound field so the
/// generated program references no method calls.
#[test]
fn kleene_truth_table() {
    let states = [
        ("true", Value::Bool(true)),
        ("false", Value::Bool(false)),
        ("nullv", Value::Null),
    ];
    for (lname, lval) in &states {
        for (rname, rval) in &states {
            for op in ["and", "or"] {
                let src = format!("emit out = a {op} b");
                let record = HashMap::from([
                    ("a".to_string(), lval.clone()),
                    ("b".to_string(), rval.clone()),
                ]);
                assert_agree(&src, &["a", "b"], record);
                let _ = (lname, rname);
            }
        }
    }
}

/// `and` / `or` must short-circuit identically: the right operand is a
/// division-by-zero that errors if evaluated. `false && X` and
/// `true || X` must NOT evaluate `X`, so both paths agree on a clean
/// emit; the other states must evaluate `X` and both must surface the
/// same error.
#[test]
fn kleene_short_circuit_side_effect() {
    // false && (1/0) → false, RHS unread, no error.
    assert_agree(
        "emit out = a and (1 / z)",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Bool(false)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
    // true || (1/0) → true, RHS unread, no error.
    assert_agree(
        "emit out = a or (1 / z)",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Bool(true)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
    // true && (1/0) → RHS evaluated → both must surface the same error.
    assert_agree(
        "emit out = a and (1 / z)",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Bool(true)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
}

/// Arithmetic and comparison null-propagation: a null operand yields
/// null, except equality which never nulls.
#[test]
fn null_propagation() {
    let with_null = |op: &str| {
        assert_agree(
            &format!("emit out = a {op} b"),
            &["a", "b"],
            HashMap::from([
                ("a".to_string(), Value::Null),
                ("b".to_string(), Value::Integer(3)),
            ]),
        )
    };
    for op in ["+", "-", "*", "/", "%", ">", "<", ">=", "<="] {
        let proj = with_null(op);
        assert_eq!(
            proj,
            ResultProjection::Emit {
                fields: vec![("out".to_string(), Value::Null)],
                record_vars: vec![],
            },
        );
    }
}

/// Eq / Neq never null: a null operand still produces a boolean.
#[test]
fn eq_neq_on_null() {
    let proj = assert_agree(
        "emit out = a == b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(3)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(false))],
            record_vars: vec![],
        },
    );
    // null == null → true
    assert_agree(
        "emit out = a == b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Null),
        ]),
    );
    // null != 3 → true
    let proj = assert_agree(
        "emit out = a != b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(3)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
}

/// Coalesce short-circuits on the first non-null operand and never
/// evaluates the RHS in that case.
#[test]
fn coalesce_short_circuit() {
    // a is non-null → RHS (1/0) unread, no error.
    assert_agree(
        "emit out = a ?? (1 / z)",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Integer(7)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
    // a is null → fall through to b.
    let proj = assert_agree(
        "emit out = a ?? b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(9)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(9))],
            record_vars: vec![],
        },
    );
}

/// `if`/`then`/`else` — true branch, false branch, and missing else.
#[test]
fn if_then_else() {
    assert_agree(
        "emit out = if a > 0 then \"pos\" else \"nonpos\"",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
    assert_agree(
        "emit out = if a > 0 then \"pos\" else \"nonpos\"",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(-5))]),
    );
    // Null condition → not true → else branch.
    assert_agree(
        "emit out = if a > 0 then \"pos\" else \"nonpos\"",
        &["a"],
        HashMap::from([("a".to_string(), Value::Null)]),
    );
}

/// Value-form and condition-form `match`, including wildcard fall-through.
#[test]
fn match_forms() {
    // Value form, matching a non-wildcard arm.
    assert_agree(
        "emit out = match status { \"a\" => 1, \"b\" => 2, _ => 0 }",
        &["status"],
        HashMap::from([("status".to_string(), Value::String("b".into()))]),
    );
    // Value form, no non-wildcard arm matches → wildcard fall-through.
    // (The typechecker requires the `_` catch-all, so an unmatched
    // scrutinee always lands on it rather than the implicit null.)
    let proj = assert_agree(
        "emit out = match status { \"a\" => 1, \"b\" => 2, _ => 0 }",
        &["status"],
        HashMap::from([("status".to_string(), Value::String("z".into()))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(0))],
            record_vars: vec![],
        },
    );
    // Condition form.
    assert_agree(
        "emit out = match { a > 10 => \"big\", a > 0 => \"small\", _ => \"neg\" }",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
}

/// Unary negation and logical not, including null-propagation.
#[test]
fn unary_ops() {
    assert_agree(
        "emit out = -a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(4))]),
    );
    assert_agree(
        "emit out = not a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Bool(true))]),
    );
    assert_agree(
        "emit out = not a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Null)]),
    );
}

/// All four emit targets land in the right channel: Field → output map,
/// Record → record_vars, Pipeline / Source → context (neither surfaces
/// in the result), with both evaluators agreeing on the surfaced shape.
#[test]
fn emit_targets() {
    // Field.
    let proj = assert_agree("emit out = 1", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(1))],
            record_vars: vec![],
        },
    );
    // Record.
    let proj = assert_agree("emit $record.tag = 7", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![("tag".to_string(), Value::Integer(7))],
        },
    );
    // Pipeline / Source write through the context — neither appears in
    // the surfaced fields or record_vars. Both evaluators must agree on
    // the empty emit.
    let proj = assert_agree("emit $pipeline.seen = 1", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![],
        },
    );
    let proj = assert_agree("emit $source.seen = 1", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![],
        },
    );
}

/// `filter` short-circuits the record to `Skip(Filtered)` on a non-true
/// predicate (null included), and passes on `true`.
#[test]
fn filter_skip() {
    let proj = assert_agree(
        "filter a > 10\nemit out = a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
    assert_eq!(proj, ResultProjection::Skip(SkipReason::Filtered));

    // Null predicate is not true → skip.
    let proj = assert_agree(
        "filter a > b\nemit out = a",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(1)),
        ]),
    );
    assert_eq!(proj, ResultProjection::Skip(SkipReason::Filtered));

    // Passing predicate → emit.
    assert_agree(
        "filter a > 0\nemit out = a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
}

/// `let` writes the shared env and a later field-ref reads it; field
/// resolution prefers env over the input record.
#[test]
fn let_binding_and_env_shadow() {
    // Env value shadows the input field of the same name.
    let proj = assert_agree(
        "let a = 100\nemit out = a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(1))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(100))],
            record_vars: vec![],
        },
    );
}

/// System-access leaves: `$source.*`, `$pipeline.*`, `now`. Both
/// evaluators resolve them against the same context and agree.
#[test]
fn system_access() {
    assert_agree("emit out = $source.row", &[], HashMap::new());
    assert_agree("emit out = $pipeline.name", &[], HashMap::new());
    assert_agree("emit out = now", &[], HashMap::new());
}

/// Index access on an array-valued field: in-bounds, out-of-bounds
/// (→ null), and negative (→ null). Both evaluators agree, including
/// the OOB null.
#[test]
fn index_access() {
    let nums = || {
        Value::Array(vec![
            Value::Integer(10),
            Value::Integer(20),
            Value::Integer(30),
        ])
    };
    let proj = assert_agree(
        "emit out = nums[1]",
        &["nums"],
        HashMap::from([("nums".to_string(), nums())]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(20))],
            record_vars: vec![],
        },
    );
    // Out of bounds → null.
    assert_agree(
        "emit out = nums[9]",
        &["nums"],
        HashMap::from([("nums".to_string(), nums())]),
    );
    // Negative index → null.
    assert_agree(
        "emit out = nums[0 - 1]",
        &["nums"],
        HashMap::from([("nums".to_string(), nums())]),
    );
}

/// Error path: division by zero must surface the same error kind, span,
/// boundary `source_row`, `source_expr`, and `triggering_field` from
/// both evaluators.
#[test]
fn error_span_fidelity_division_by_zero() {
    let proj = assert_agree(
        "emit out = 1 / z",
        &["z"],
        HashMap::from([("z".to_string(), Value::Integer(0))]),
    );
    match proj {
        ResultProjection::Err {
            kind,
            source_row,
            source_expr,
            triggering_field,
            ..
        } => {
            assert_eq!(kind, "DivisionByZero");
            assert_eq!(source_row, Some(1));
            assert!(source_expr.is_some());
            assert_eq!(triggering_field.as_deref(), Some("out"));
        }
        other => panic!("expected error projection, got {other:?}"),
    }
}

/// Error path: integer overflow on negation surfaces identically.
#[test]
fn error_span_fidelity_overflow() {
    let proj = assert_agree(
        "emit out = -a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(i64::MIN))]),
    );
    match proj {
        ResultProjection::Err { kind, .. } => assert!(
            kind.contains("IntegerOverflow"),
            "expected IntegerOverflow, got {kind}"
        ),
        other => panic!("expected error projection, got {other:?}"),
    }
}

/// Multiple statements and emits accumulate in order; trace / use / expr
/// statements run without altering the field channel.
#[test]
fn multi_statement_ordering() {
    assert_agree(
        "let x = a + 1\nemit p = x\nemit q = x * 2\ntrace \"hi\"\nx + 1",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(4))]),
    );
}

// ── Differential corpus: method calls ──────────────────────────────────

/// String builtins delegate to the shared `dispatch_method`, so both
/// evaluators produce identical results for the common string surface.
#[test]
fn string_methods() {
    let with_s = |src: &str, s: &str| {
        assert_agree(
            src,
            &["s"],
            HashMap::from([("s".to_string(), Value::String(s.into()))]),
        )
    };
    with_s("emit out = s.upper()", "abc");
    with_s("emit out = s.lower()", "ABC");
    with_s("emit out = s.trim()", "  hi  ");
    with_s("emit out = s.length()", "hello");
    with_s("emit out = s.contains(\"ell\")", "hello");
    with_s("emit out = s.starts_with(\"he\")", "hello");
    with_s("emit out = s.replace(\"l\", \"L\")", "hello");
    with_s("emit out = s.substring(1, 3)", "hello");
    // Chained string methods compose identically.
    with_s("emit out = s.trim().upper()", "  hi  ");
}

/// Numeric builtins delegate to the shared `dispatch_method`.
#[test]
fn numeric_methods() {
    let with_n =
        |src: &str, n: Value| assert_agree(src, &["n"], HashMap::from([("n".to_string(), n)]));
    with_n("emit out = n.abs()", Value::Integer(-7));
    with_n("emit out = n.abs()", Value::Float(-2.5));
    with_n("emit out = n.ceil()", Value::Float(2.1));
    with_n("emit out = n.floor()", Value::Float(2.9));
    with_n("emit out = n.round()", Value::Float(2.5));
    with_n("emit out = n.to_float()", Value::Integer(3));
    with_n("emit out = n.to_string()", Value::Integer(42));
    with_n("emit out = n.min(10)", Value::Integer(3));
    with_n("emit out = n.max(10)", Value::Integer(3));
}

/// The null-receiver gate: for every method *except* the four exceptions
/// a null receiver short-circuits to `Null`, identically through both
/// evaluators. The gate lives inside the shared `dispatch_method`, so the
/// compiled node delegates to it rather than re-deriving the rule.
#[test]
fn method_null_receiver_gate() {
    let null_recv =
        |src: &str| assert_agree(src, &["s"], HashMap::from([("s".to_string(), Value::Null)]));
    // Gated methods → Null.
    for src in [
        "emit out = s.upper()",
        "emit out = s.length()",
        "emit out = s.trim()",
        "emit out = s.contains(\"x\")",
    ] {
        let proj = null_recv(src);
        assert_eq!(
            proj,
            ResultProjection::Emit {
                fields: vec![("out".to_string(), Value::Null)],
                record_vars: vec![],
            },
            "null receiver should gate `{src}` to Null",
        );
    }
}

/// The four gate exceptions (`is_null` / `type_of` / `is_empty` /
/// `catch`) run on a null receiver instead of short-circuiting — both
/// evaluators must agree on the concrete (non-null) result.
#[test]
fn method_null_receiver_exceptions() {
    let null_recv =
        |src: &str| assert_agree(src, &["s"], HashMap::from([("s".to_string(), Value::Null)]));

    // is_null → true on a null receiver.
    let proj = null_recv("emit out = s.is_null()");
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    // catch(default) → the default, since the receiver is null.
    let proj = null_recv("emit out = s.catch(99)");
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(99))],
            record_vars: vec![],
        },
    );
    // type_of / is_empty still run on the null receiver.
    null_recv("emit out = s.type_of()");
    null_recv("emit out = s.is_empty()");
}

/// Regex methods (`matches` / `find` / `capture`) use the pre-compiled
/// regex captured by value at lowering. Both evaluators read the same
/// `typed.regexes[node_id]` entry, so results agree.
#[test]
fn regex_methods() {
    let with_s = |src: &str, s: &str| {
        assert_agree(
            src,
            &["s"],
            HashMap::from([("s".to_string(), Value::String(s.into()))]),
        )
    };
    with_s("emit out = s.matches(\"^[a-z]+$\")", "abc");
    with_s("emit out = s.matches(\"^[a-z]+$\")", "abc123");
    with_s("emit out = s.find(\"[0-9]+\")", "abc123");
    with_s("emit out = s.find(\"[0-9]+\")", "abc");
    with_s("emit out = s.capture(\"([a-z]+)([0-9]+)\")", "abc123");
}

// ── Differential corpus: closure-bearing array builtins ────────────────

/// Build an array `Value`, the receiver shape the closure builtins
/// require.
fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

/// Each closure builtin (`filter` / `map` / `find` / `any` / `flat_map`)
/// runs the host loop over a separately-compiled closure body and must
/// agree with the tree-walk on a populated array.
///
/// CXL has no array-literal syntax, so `flat_map`'s array-producing body
/// uses a string element split into an array (`it.split(",")`); the
/// other four use an integer array.
#[test]
fn closure_builtins_populated() {
    let nums = || {
        arr(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])
    };
    let with_nums = |src: &str| {
        assert_agree(
            src,
            &["nums"],
            HashMap::from([("nums".to_string(), nums())]),
        )
    };
    with_nums("emit out = nums.filter(it => it > 1)");
    with_nums("emit out = nums.map(it => it + 10)");
    with_nums("emit out = nums.find(it => it > 1)");
    with_nums("emit out = nums.any(it => it > 5)");
    // find with no match falls through to Null.
    with_nums("emit out = nums.find(it => it > 99)");
    // any with no match is false.
    with_nums("emit out = nums.any(it => it > 99)");

    // flat_map flattens each element's array-valued body result.
    assert_agree(
        "emit out = strs.flat_map(it => it.split(\",\"))",
        &["strs"],
        HashMap::from([(
            "strs".to_string(),
            arr(vec![Value::String("a,b".into()), Value::String("c".into())]),
        )]),
    );
}

/// A null receiver short-circuits every closure builtin to `Null`; the
/// body never runs, matching the tree-walk's null-propagation.
#[test]
fn closure_builtins_null_receiver() {
    let null_recv_nums = |src: &str| {
        let proj = assert_agree(
            src,
            &["nums"],
            HashMap::from([("nums".to_string(), Value::Null)]),
        );
        assert_eq!(
            proj,
            ResultProjection::Emit {
                fields: vec![("out".to_string(), Value::Null)],
                record_vars: vec![],
            },
            "null receiver should gate `{src}` to Null",
        );
    };
    null_recv_nums("emit out = nums.filter(it => it > 1)");
    null_recv_nums("emit out = nums.map(it => it + 10)");
    null_recv_nums("emit out = nums.find(it => it > 1)");
    null_recv_nums("emit out = nums.any(it => it > 1)");

    let proj = assert_agree(
        "emit out = strs.flat_map(it => it.split(\",\"))",
        &["strs"],
        HashMap::from([("strs".to_string(), Value::Null)]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// An empty array runs no iterations: `filter` / `map` / `flat_map`
/// return an empty array, `find` returns `Null`, `any` returns `false`.
/// Both evaluators agree on the zero-iteration outcome.
#[test]
fn closure_builtins_empty_array() {
    let with_empty = |src: &str| {
        assert_agree(
            src,
            &["nums"],
            HashMap::from([("nums".to_string(), arr(vec![]))]),
        )
    };
    let proj = with_empty("emit out = nums.filter(it => it > 1)");
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), arr(vec![]))],
            record_vars: vec![],
        },
    );
    with_empty("emit out = nums.map(it => it + 1)");
    let proj = with_empty("emit out = nums.find(it => it > 1)");
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
    let proj = with_empty("emit out = nums.any(it => it > 1)");
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(false))],
            record_vars: vec![],
        },
    );

    // flat_map on an empty string array → empty array.
    let proj = assert_agree(
        "emit out = strs.flat_map(it => it.split(\",\"))",
        &["strs"],
        HashMap::from([("strs".to_string(), arr(vec![]))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), arr(vec![]))],
            record_vars: vec![],
        },
    );
}

/// A closure body that errors on one element propagates that error
/// identically (kind + span + boundary provenance), and the per-element
/// binding is removed on the error path so the env never leaks. The
/// `find`-style early break is not reached because the error fires first.
#[test]
fn closure_body_error_propagates() {
    let proj = assert_agree(
        "emit out = nums.map(it => 1 / it)",
        &["nums"],
        HashMap::from([(
            "nums".to_string(),
            arr(vec![Value::Integer(1), Value::Integer(0)]),
        )]),
    );
    match proj {
        ResultProjection::Err {
            kind,
            triggering_field,
            ..
        } => {
            assert_eq!(kind, "DivisionByZero");
            assert_eq!(triggering_field.as_deref(), Some("out"));
        }
        other => panic!("expected error projection, got {other:?}"),
    }
}

// ── Differential corpus: hardening — mixed numerics, ordering, Kleene ───

/// Mixed int/float comparison widens the int to float before comparing;
/// both evaluators must agree on the widened ordering.
#[test]
fn mixed_int_float_comparison() {
    // Integer(2) > Float(1.5) → true.
    let proj = assert_agree(
        "emit out = a > b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Integer(2)),
            ("b".to_string(), Value::Float(1.5)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    // Float(1.5) < Integer(2) → true.
    let proj = assert_agree(
        "emit out = a < b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Float(1.5)),
            ("b".to_string(), Value::Integer(2)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    // Integer(2) == Float(2.0) → true (cross-type equality widens).
    let proj = assert_agree(
        "emit out = a == b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Integer(2)),
            ("b".to_string(), Value::Float(2.0)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
}

/// String ordering compares lexicographically; both evaluators agree.
#[test]
fn string_ordering() {
    let proj = assert_agree(
        "emit out = a < b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::String("apple".into())),
            ("b".to_string(), Value::String("banana".into())),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    assert_agree(
        "emit out = a >= b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::String("zoo".into())),
            ("b".to_string(), Value::String("zoo".into())),
        ]),
    );
}

/// The four `null or <bool>` Kleene cells, pinned deterministically:
/// `null or true → true`, `null or false → null`, and the symmetric
/// `null and false → false`, `null and true → null`.
#[test]
fn null_or_kleene_cells() {
    let cell = |op: &str, b: Value| {
        assert_agree(
            &format!("emit out = a {op} b"),
            &["a", "b"],
            HashMap::from([("a".to_string(), Value::Null), ("b".to_string(), b)]),
        )
    };
    // null or true → true.
    assert_eq!(
        cell("or", Value::Bool(true)),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    // null or false → null.
    assert_eq!(
        cell("or", Value::Bool(false)),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
    // null and false → false.
    assert_eq!(
        cell("and", Value::Bool(false)),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(false))],
            record_vars: vec![],
        },
    );
    // null and true → null.
    assert_eq!(
        cell("and", Value::Bool(true)),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// An agreed NaN *result* compares equal through the projection's NaN
/// canonicalization rather than false-failing. The arithmetic must
/// actually reach the float op: `0.0 / 0.0` cannot — the division arm
/// rejects a zero divisor as `DivisionByZero` before any float divide
/// runs — so this feeds a pre-existing `f64::NAN` field through `+` and
/// `*` (and a NaN dividend through a non-zero divisor), all of which
/// produce a NaN `Value::Float` and so exercise `canonicalize_nan`.
#[test]
fn nan_result_agrees() {
    let with_nan = |src: &str| {
        let proj = assert_agree(
            src,
            &["a", "b"],
            HashMap::from([
                ("a".to_string(), Value::Float(f64::NAN)),
                ("b".to_string(), Value::Float(2.0)),
            ]),
        );
        // Both evaluators must agree the result is a (canonicalized) NaN
        // float, not an error — confirming the float op ran.
        match proj {
            ResultProjection::Emit { fields, .. } => {
                assert_eq!(fields.len(), 1);
                match &fields[0].1 {
                    Value::Float(f) => assert!(f.is_nan(), "expected NaN, got {f}"),
                    other => panic!("expected NaN float, got {other:?}"),
                }
            }
            other => panic!("expected NaN emit, got {other:?}"),
        }
    };
    // NaN propagates through addition and multiplication.
    with_nan("emit out = a + b");
    with_nan("emit out = a * b");
    // A NaN dividend with a non-zero divisor reaches the float divide
    // (the divisor guard only rejects zero), yielding NaN rather than an
    // error.
    with_nan("emit out = a / b");
}

/// The `Any`-receiver method shapes the proptest generator emits
/// (`to_string` / `is_null` / `type_of` / `catch`) typecheck on the
/// generated leaf set and agree across both evaluators — pinned here so
/// the property test's method coverage cannot silently collapse to
/// all-rejected without this deterministic case also failing.
#[test]
fn generated_method_shapes_agree() {
    let record = || {
        HashMap::from([
            ("a".to_string(), Value::Integer(5)),
            ("b".to_string(), Value::String("x".into())),
        ])
    };
    assert_agree("emit out = (a).to_string()", &["a", "b"], record());
    assert_agree("emit out = (a).is_null()", &["a", "b"], record());
    assert_agree("emit out = (a).type_of()", &["a", "b"], record());
    assert_agree("emit out = ((a + 1)).to_string()", &["a", "b"], record());
    assert_agree("emit out = (a).catch(b)", &["a", "b"], record());
    assert_agree(
        "emit out = ((a).is_null() or (a > 0))",
        &["a", "b"],
        record(),
    );
}

// ── Carried-over hardening: null-receiver regex, error-surfacing catch ──

/// A regex method on a null receiver is gated to `Null` by the shared
/// `dispatch_method` — the regex is never consulted. Both evaluators must
/// agree on the gated `Null` rather than one of them attempting a match
/// against a null receiver.
#[test]
fn regex_method_null_receiver() {
    let proj = assert_agree(
        "emit out = s.matches(\"^[a-z]+$\")",
        &["s"],
        HashMap::from([("s".to_string(), Value::Null)]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// `catch(default)` is a null-coalesce, not an error swallow: its receiver
/// is evaluated eagerly *before* `dispatch_method` runs, so an error in
/// the receiver expression propagates and `catch` never substitutes its
/// default. The receiver here is non-null-shaped but raises a real error
/// (division by zero); both evaluators must surface that error identically
/// rather than returning the `catch` default.
#[test]
fn catch_does_not_swallow_receiver_error() {
    let proj = assert_agree(
        "emit out = (1 / z).catch(99)",
        &["z"],
        HashMap::from([("z".to_string(), Value::Integer(0))]),
    );
    match proj {
        ResultProjection::Err { kind, .. } => assert_eq!(kind, "DivisionByZero"),
        other => panic!("expected DivisionByZero to propagate through catch, got {other:?}"),
    }
}

// ── Differential corpus: window leaves ─────────────────────────────────

/// In-memory partition storage for the window corpus: a list of records,
/// each a field map. Indexed by record position; the window context reads
/// fields off it by index, exactly as the arena-backed production storage
/// does.
struct VecStorage {
    rows: Vec<indexmap::IndexMap<String, Value>>,
}

impl RecordStorage for VecStorage {
    fn resolve_field(&self, index: u64, name: &str) -> Option<&Value> {
        self.rows.get(index as usize).and_then(|r| r.get(name))
    }
    fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, index: u64) -> Vec<&str> {
        self.rows
            .get(index as usize)
            .map(|r| r.keys().map(String::as_str).collect())
            .unwrap_or_default()
    }
    fn record_count(&self) -> u64 {
        self.rows.len() as u64
    }
}

/// Window over the whole `VecStorage`, with an explicit current-row index
/// so `lag`/`lead` resolve relative to a known position. Aggregate methods
/// compute real sums/averages over the numeric fields of the partition so
/// the differential exercises the trait surface with non-trivial values;
/// both evaluators call the identical methods, so the comparison validates
/// dispatch parity, not the aggregate arithmetic itself.
struct VecWindow<'a> {
    storage: &'a VecStorage,
    current: usize,
}

impl<'a> VecWindow<'a> {
    fn numeric(&self, field: &str) -> Vec<f64> {
        self.storage
            .rows
            .iter()
            .filter_map(|r| match r.get(field) {
                Some(Value::Integer(n)) => Some(*n as f64),
                Some(Value::Float(f)) => Some(*f),
                _ => None,
            })
            .collect()
    }
}

impl<'a> clinker_record::WindowContext<'a, VecStorage> for VecWindow<'a> {
    fn first(&self) -> Option<clinker_record::RecordView<'a, VecStorage>> {
        (!self.storage.rows.is_empty()).then(|| clinker_record::RecordView::new(self.storage, 0))
    }
    fn last(&self) -> Option<clinker_record::RecordView<'a, VecStorage>> {
        self.storage
            .rows
            .len()
            .checked_sub(1)
            .map(|i| clinker_record::RecordView::new(self.storage, i as u64))
    }
    fn lag(&self, offset: usize) -> Option<clinker_record::RecordView<'a, VecStorage>> {
        self.current
            .checked_sub(offset)
            .map(|i| clinker_record::RecordView::new(self.storage, i as u64))
    }
    fn lead(&self, offset: usize) -> Option<clinker_record::RecordView<'a, VecStorage>> {
        let i = self.current + offset;
        (i < self.storage.rows.len())
            .then(|| clinker_record::RecordView::new(self.storage, i as u64))
    }
    fn count(&self) -> i64 {
        self.storage.rows.len() as i64
    }
    fn sum(&self, field: &str) -> Value {
        let v = self.numeric(field);
        if v.is_empty() {
            Value::Null
        } else {
            Value::Float(v.iter().sum())
        }
    }
    fn cumulative_sum(&self, field: &str) -> Value {
        let s: f64 = self.numeric(field).iter().take(self.current + 1).sum();
        Value::Float(s)
    }
    fn avg(&self, field: &str) -> Value {
        let v = self.numeric(field);
        if v.is_empty() {
            Value::Null
        } else {
            Value::Float(v.iter().sum::<f64>() / v.len() as f64)
        }
    }
    fn min(&self, field: &str) -> Value {
        self.numeric(field)
            .into_iter()
            .reduce(f64::min)
            .map_or(Value::Null, Value::Float)
    }
    fn max(&self, field: &str) -> Value {
        self.numeric(field)
            .into_iter()
            .reduce(f64::max)
            .map_or(Value::Null, Value::Float)
    }
    fn partition_len(&self) -> usize {
        self.storage.rows.len()
    }
    fn partition_record(&self, index: usize) -> clinker_record::RecordView<'a, VecStorage> {
        clinker_record::RecordView::new(self.storage, index as u64)
    }
    fn collect(&self, field: &str) -> Value {
        Value::Array(
            self.storage
                .rows
                .iter()
                .filter_map(|r| r.get(field).cloned())
                .collect(),
        )
    }
    fn distinct(&self, field: &str) -> Value {
        let mut seen = Vec::new();
        for r in &self.storage.rows {
            if let Some(v) = r.get(field)
                && !seen.contains(v)
            {
                seen.push(v.clone());
            }
        }
        Value::Array(seen)
    }
    fn row_number(&self) -> i64 {
        (self.current + 1) as i64
    }
    fn rank(&self) -> i64 {
        1
    }
    fn dense_rank(&self) -> i64 {
        1
    }
    fn first_value(&self, field: &str) -> Value {
        self.storage
            .rows
            .first()
            .and_then(|r| r.get(field).cloned())
            .unwrap_or(Value::Null)
    }
    fn last_value(&self, field: &str) -> Value {
        self.storage
            .rows
            .last()
            .and_then(|r| r.get(field).cloned())
            .unwrap_or(Value::Null)
    }
}

/// Type-check a window program against an explicit field-type row (window
/// programs reference fields whose types the typecheck pass needs to see).
fn type_program_with_row(
    src: &str,
    fields: &[(&str, crate::typecheck::types::Type)],
) -> TypedProgram {
    use crate::typecheck::row::QualifiedField;
    let parsed = Parser::parse(src);
    assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
    let names: Vec<&str> = fields.iter().map(|(n, _)| *n).collect();
    let resolved = resolve_program(parsed.ast, &names, parsed.node_count)
        .unwrap_or_else(|d| panic!("resolve: {:?}", d));
    let declared: indexmap::IndexMap<QualifiedField, crate::typecheck::types::Type> = fields
        .iter()
        .map(|(n, t)| (QualifiedField::bare(*n), t.clone()))
        .collect();
    let row = Row::closed(declared, crate::lexer::Span::new(0, 0));
    type_check(resolved, &row)
        .unwrap_or_else(|d| panic!("type: {:?}", d))
        .with_source(Arc::from(src))
}

/// Assert the tree-walk and the compiled evaluator agree on a window
/// program, evaluated with the same `VecWindow` over `partition` and the
/// same current-row index. The transform's own input record is empty (the
/// scalar core reads nothing here); every read flows through the window.
fn assert_agree_window(
    src: &str,
    fields: &[(&str, crate::typecheck::types::Type)],
    partition: Vec<indexmap::IndexMap<String, Value>>,
    current: usize,
) -> ResultProjection {
    let typed = type_program_with_row(src, fields);
    let has_distinct = program_has_distinct(&typed);
    let stable = StableEvalContext::test_default();
    let resolver = HashMapResolver::new(HashMap::new());
    let storage = VecStorage { rows: partition };
    let window = VecWindow {
        storage: &storage,
        current,
    };

    let ctx_tw = EvalContext::test_default_borrowed(&stable);
    let mut evaluator =
        ProgramEvaluator::new(Arc::new(type_program_with_row(src, fields)), has_distinct);
    let tree = evaluator.eval_record::<VecStorage>(&ctx_tw, &resolver, Some(&window));

    let ctx_c = EvalContext::test_default_borrowed(&stable);
    let program = compile::<VecStorage>(&typed);
    let mut state = EvalState::new(has_distinct, DEFAULT_MAX_EXPANSION);
    let comp = compiled_eval_record(
        &program,
        &typed,
        &ctx_c,
        &resolver,
        Some(&window),
        &mut state,
    );

    let tree_proj = project(&tree);
    let comp_proj = project(&comp);
    assert_eq!(
        tree_proj, comp_proj,
        "tree-walk and compiled diverged on window program `{src}`",
    );
    tree_proj
}

/// One partition row from `(field, value)` pairs.
fn row(pairs: Vec<(&str, Value)>) -> indexmap::IndexMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// Bare aggregate / cardinality / rank window builtins lower to leaf trait
/// calls; both evaluators dispatch to the same `VecWindow` methods.
#[test]
fn window_bare_calls() {
    use crate::typecheck::types::Type;
    let partition = || {
        vec![
            row(vec![("amount", Value::Integer(10))]),
            row(vec![("amount", Value::Integer(20))]),
            row(vec![("amount", Value::Integer(30))]),
        ]
    };
    let fields: &[(&str, Type)] = &[("amount", Type::Int)];
    assert_agree_window("emit out = $window.count()", fields, partition(), 1);
    assert_agree_window("emit out = $window.sum(amount)", fields, partition(), 1);
    assert_agree_window("emit out = $window.avg(amount)", fields, partition(), 1);
    assert_agree_window("emit out = $window.min(amount)", fields, partition(), 1);
    assert_agree_window("emit out = $window.max(amount)", fields, partition(), 1);
    assert_agree_window(
        "emit out = $window.cumulative_sum(amount)",
        fields,
        partition(),
        1,
    );
    assert_agree_window("emit out = $window.row_number()", fields, partition(), 1);
    assert_agree_window("emit out = $window.rank()", fields, partition(), 1);
    assert_agree_window("emit out = $window.dense_rank()", fields, partition(), 1);
    assert_agree_window("emit out = $window.collect(amount)", fields, partition(), 1);
    assert_agree_window(
        "emit out = $window.first_value(amount)",
        fields,
        partition(),
        1,
    );
    assert_agree_window(
        "emit out = $window.last_value(amount)",
        fields,
        partition(),
        1,
    );
}

/// The `$window.<fn>().<field>` chain resolves a field off a positional
/// window row with no intermediate `Value`; both evaluators read the same
/// `RecordView`. Covers `first`/`last`/`lag`/`lead`, including an
/// out-of-range offset that resolves to `Null`.
#[test]
fn window_chain_field() {
    use crate::typecheck::types::Type;
    let partition = || {
        vec![
            row(vec![("price", Value::Integer(100))]),
            row(vec![("price", Value::Integer(200))]),
            row(vec![("price", Value::Integer(300))]),
        ]
    };
    let fields: &[(&str, Type)] = &[("price", Type::Int)];
    assert_agree_window("emit out = $window.first().price", fields, partition(), 1);
    assert_agree_window("emit out = $window.last().price", fields, partition(), 1);
    // lag(1) from current=2 → row 1.
    assert_agree_window("emit out = $window.lag(1).price", fields, partition(), 2);
    // lead(1) from current=0 → row 1.
    assert_agree_window("emit out = $window.lead(1).price", fields, partition(), 0);
    // lag past the start → Null.
    assert_agree_window("emit out = $window.lag(5).price", fields, partition(), 0);
    // lead past the end → Null.
    assert_agree_window("emit out = $window.lead(5).price", fields, partition(), 2);
}

/// Window reads against an empty partition: aggregates → `Null`, count
/// → 0, the chain forms → `Null` (no positional row). Both evaluators
/// agree on the empty-frame outcome.
#[test]
fn window_empty_partition() {
    use crate::typecheck::types::Type;
    let fields: &[(&str, Type)] = &[("amount", Type::Int)];
    let proj = assert_agree_window("emit out = $window.count()", fields, vec![], 0);
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(0))],
            record_vars: vec![],
        },
    );
    let proj = assert_agree_window("emit out = $window.sum(amount)", fields, vec![], 0);
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
    let proj = assert_agree_window("emit out = $window.first().amount", fields, vec![], 0);
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// A window read with no window context available surfaces the same typed
/// error from both evaluators (rather than one returning `Null`). Driven
/// through the no-window `assert_agree` path, which threads `None`.
#[test]
fn window_missing_context_errors() {
    let proj = assert_agree("emit out = $window.count()", &[], HashMap::new());
    match proj {
        ResultProjection::Err { kind, .. } => assert!(
            kind.contains("TypeMismatch"),
            "expected a window-context TypeMismatch, got {kind}"
        ),
        other => panic!("expected window-context error, got {other:?}"),
    }
}

/// The `$window` predicate folds (`any`/`every`/`exists`/`not_exists`)
/// run a compiled predicate over every partition record with the same
/// three-valued Kleene collapse the tree-walk uses — including the
/// short-circuit, the null-taint, and the iterated-operator identity on
/// an empty fold.
#[test]
fn window_predicate_folds() {
    use crate::typecheck::types::Type;
    let fields: &[(&str, Type)] = &[("flag", Type::Bool)];
    let part = |flags: Vec<Value>| flags.into_iter().map(|f| row(vec![("flag", f)])).collect();
    let b = Value::Bool;
    let nullv = Value::Null;

    // any short-circuits true before a later null is seen.
    assert_agree_window(
        "emit out = $window.any(flag)",
        fields,
        part(vec![b(true), nullv.clone(), b(false)]),
        0,
    );
    // any with a null and no true → null taint.
    assert_agree_window(
        "emit out = $window.any(flag)",
        fields,
        part(vec![b(false), nullv.clone(), b(false)]),
        0,
    );
    // any over all-false → false (iterated-or identity).
    assert_agree_window(
        "emit out = $window.any(flag)",
        fields,
        part(vec![b(false), b(false)]),
        0,
    );
    // every short-circuits false.
    assert_agree_window(
        "emit out = $window.every(flag)",
        fields,
        part(vec![b(true), b(false), b(true)]),
        0,
    );
    // every over all-true → true (iterated-and identity).
    assert_agree_window(
        "emit out = $window.every(flag)",
        fields,
        part(vec![b(true), b(true)]),
        0,
    );
    // exists is an alias of any; not_exists inverts then folds as every.
    assert_agree_window(
        "emit out = $window.exists(flag)",
        fields,
        part(vec![b(false), b(true)]),
        0,
    );
    assert_agree_window(
        "emit out = $window.not_exists(flag)",
        fields,
        part(vec![b(false), b(false)]),
        0,
    );
    // Empty partition → iterated-operator identity, no taint.
    assert_agree_window("emit out = $window.any(flag)", fields, vec![], 0);
    assert_agree_window("emit out = $window.every(flag)", fields, vec![], 0);
}

// ── Differential corpus: distinct ──────────────────────────────────────

/// One record in a distinct stream: an optional partition key to set
/// before evaluating, paired with the record's field map.
type DistinctStreamRecord = (
    Option<Vec<clinker_record::GroupByKey>>,
    HashMap<String, Value>,
);

/// Drive a stream of records through both evaluators with shared dedup
/// state, optionally setting a partition key before each record. Asserts
/// the two evaluators agree on every record's outcome and returns the
/// per-record projections so callers can assert the concrete skip/emit
/// sequence.
fn assert_agree_distinct_stream(
    src: &str,
    fields: &[&str],
    records: Vec<DistinctStreamRecord>,
) -> Vec<ResultProjection> {
    let typed = type_program(src, fields);
    let has_distinct = program_has_distinct(&typed);
    assert!(
        has_distinct,
        "distinct stream helper requires a distinct program"
    );
    let stable = StableEvalContext::test_default();

    let mut tree_eval = ProgramEvaluator::new(Arc::new(type_program(src, fields)), has_distinct);
    let program = compile::<NullStorage>(&typed);
    let mut state = EvalState::new(has_distinct, DEFAULT_MAX_EXPANSION);

    let mut out = Vec::new();
    for (partition_key, record) in records {
        if let Some(key) = &partition_key {
            tree_eval.set_partition(key);
            state.set_partition(key);
        }
        let resolver = HashMapResolver::new(record);

        let ctx_tw = EvalContext::test_default_borrowed(&stable);
        let tree = tree_eval.eval_record::<NullStorage>(&ctx_tw, &resolver, None);

        let ctx_c = EvalContext::test_default_borrowed(&stable);
        let comp = compiled_eval_record(&program, &typed, &ctx_c, &resolver, None, &mut state);

        let tree_proj = project(&tree);
        let comp_proj = project(&comp);
        assert_eq!(
            tree_proj, comp_proj,
            "tree-walk and compiled diverged on distinct stream `{src}`",
        );
        out.push(tree_proj);
    }
    out
}

/// `distinct by <field>`: the first occurrence of a key emits, later
/// duplicates skip with `Skip(Duplicate)`. Both evaluators agree across
/// the stream against the shared dedup set.
#[test]
fn distinct_duplicate_skip() {
    let stream = vec![
        (None, HashMap::from([("k".to_string(), Value::Integer(1))])),
        (None, HashMap::from([("k".to_string(), Value::Integer(2))])),
        (None, HashMap::from([("k".to_string(), Value::Integer(1))])),
        (None, HashMap::from([("k".to_string(), Value::Integer(2))])),
        (None, HashMap::from([("k".to_string(), Value::Integer(3))])),
    ];
    let projs = assert_agree_distinct_stream("distinct by k\nemit out = k", &["k"], stream);
    let kinds: Vec<_> = projs
        .iter()
        .map(|p| matches!(p, ResultProjection::Skip(SkipReason::Duplicate)))
        .collect();
    // First 1, first 2 emit; repeat 1, repeat 2 skip; first 3 emits.
    assert_eq!(kinds, vec![false, false, true, true, false]);
}

/// A partition-key change clears the dedup set (window-scoped distinct):
/// a key seen in the previous partition is *not* a duplicate in the next.
/// Both evaluators reset on the same boundary.
#[test]
fn distinct_partition_reset() {
    let p0 = Some(vec![clinker_record::GroupByKey::Int(0)]);
    let p1 = Some(vec![clinker_record::GroupByKey::Int(1)]);
    let stream = vec![
        (
            p0.clone(),
            HashMap::from([("k".to_string(), Value::Integer(7))]),
        ),
        (
            p0.clone(),
            HashMap::from([("k".to_string(), Value::Integer(7))]),
        ),
        // Partition boundary: the dedup set clears, so 7 is fresh again.
        (
            p1.clone(),
            HashMap::from([("k".to_string(), Value::Integer(7))]),
        ),
        (
            p1.clone(),
            HashMap::from([("k".to_string(), Value::Integer(7))]),
        ),
    ];
    let projs = assert_agree_distinct_stream("distinct by k\nemit out = k", &["k"], stream);
    let dup: Vec<_> = projs
        .iter()
        .map(|p| matches!(p, ResultProjection::Skip(SkipReason::Duplicate)))
        .collect();
    // Within each partition the second 7 is a duplicate; the partition
    // boundary resets, so the first 7 of partition 1 emits.
    assert_eq!(dup, vec![false, true, false, true]);
}

/// A NaN distinct key surfaces the same `GroupKeyError`-derived error from
/// both evaluators (NaN is not hashable as a group key). The error path is
/// compared on kind + span + provenance, identically to a value path.
#[test]
fn distinct_nan_key_errors() {
    let stream = vec![(
        None,
        HashMap::from([("k".to_string(), Value::Float(f64::NAN))]),
    )];
    let projs = assert_agree_distinct_stream("distinct by k\nemit out = k", &["k"], stream);
    match &projs[0] {
        ResultProjection::Err { kind, .. } => assert!(
            kind.contains("TypeMismatch"),
            "expected NaN group-key error, got {kind}"
        ),
        other => panic!("expected NaN distinct error, got {other:?}"),
    }
}

/// A bare `distinct` (no field) keys on every input field through the
/// resolver's `iter_fields`; both evaluators hash the full record
/// identically and dedup in lockstep. A single input field keeps the
/// all-fields key order deterministic (the test resolver iterates a
/// `HashMap`, whose multi-field order is per-instance randomized), so the
/// concrete dedup outcome is stable while still routing through the
/// all-fields path.
#[test]
fn distinct_bare_all_fields() {
    let rec = |a: i64| HashMap::from([("a".to_string(), Value::Integer(a))]);
    let stream = vec![
        (None, rec(1)),
        (None, rec(2)),
        (None, rec(1)),
        (None, rec(2)),
    ];
    let projs = assert_agree_distinct_stream("distinct\nemit out = a", &["a"], stream);
    let dup: Vec<_> = projs
        .iter()
        .map(|p| matches!(p, ResultProjection::Skip(SkipReason::Duplicate)))
        .collect();
    // The repeat 1 and repeat 2 are duplicates of their first occurrences.
    assert_eq!(dup, vec![false, false, true, true]);
}

// ── Differential corpus: emit each ─────────────────────────────────────

/// `emit each` fans an array source into one output record per element;
/// both evaluators produce identical `EmitMany` records. The per-element
/// binding is visible to the body, and a field emit before the block seeds
/// every produced record.
#[test]
fn emit_each_fanout_and_seed() {
    let nums = Value::Array(vec![
        Value::Integer(10),
        Value::Integer(20),
        Value::Integer(30),
    ]);
    // `emit tag = "t"` before the block seeds every fanned record.
    let proj = assert_agree(
        "emit tag = \"t\"\nemit each n in nums {\n  emit val = n + 1\n}",
        &["nums"],
        HashMap::from([("nums".to_string(), nums)]),
    );
    match proj {
        ResultProjection::EmitMany { records } => {
            assert_eq!(records.len(), 3);
            for (i, (fields, _)) in records.iter().enumerate() {
                // Seeded field present on every record.
                assert!(
                    fields
                        .iter()
                        .any(|(k, v)| k == "tag" && matches!(v, Value::String(s) if &**s == "t"))
                );
                let expected = (i as i64 + 1) * 10 + 1;
                assert!(
                    fields
                        .iter()
                        .any(|(k, v)| k == "val" && *v == Value::Integer(expected)),
                    "record {i} missing val={expected}: {fields:?}"
                );
            }
        }
        other => panic!("expected EmitMany, got {other:?}"),
    }
}

/// A null `emit each` source fans out zero records — the block is a no-op,
/// and a preceding field emit produces the single non-fanned record. Both
/// evaluators agree.
#[test]
fn emit_each_null_source() {
    let proj = assert_agree(
        "emit base = 1\nemit each n in nums {\n  emit val = n\n}",
        &["nums"],
        HashMap::from([("nums".to_string(), Value::Null)]),
    );
    // No fan-out: the result is a plain Emit carrying the pre-block field.
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("base".to_string(), Value::Integer(1))],
            record_vars: vec![],
        },
    );
}

/// An empty array source runs zero body iterations; both evaluators
/// produce zero fanned records (a plain `Emit` with any pre-block writes).
#[test]
fn emit_each_empty_array() {
    let proj = assert_agree(
        "emit each n in nums {\n  emit val = n\n}",
        &["nums"],
        HashMap::from([("nums".to_string(), Value::Array(vec![]))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![],
        },
    );
}

/// The fan-out ceiling fires *before* the body when the running emit count
/// reaches `max_expansion`. Driven through a dedicated helper that sets a
/// low ceiling on both evaluators so the boundary is observable; both must
/// surface `ExpansionLimitExceeded` at the same element.
#[test]
fn emit_each_ceiling_boundary() {
    let src = "emit each n in nums {\n  emit val = n\n}";
    let typed = type_program(src, &["nums"]);
    let stable = StableEvalContext::test_default();
    let nums = Value::Array(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    let resolver = HashMapResolver::new(HashMap::from([("nums".to_string(), nums)]));

    // Ceiling of 2 over a 3-element array → the third element trips it.
    let ctx_tw = EvalContext::test_default_borrowed(&stable);
    let mut tree_eval =
        ProgramEvaluator::with_max_expansion(Arc::new(type_program(src, &["nums"])), false, 2);
    let tree = tree_eval.eval_record::<NullStorage>(&ctx_tw, &resolver, None);

    let ctx_c = EvalContext::test_default_borrowed(&stable);
    let program = compile::<NullStorage>(&typed);
    let mut state = EvalState::new(false, 2);
    let comp = compiled_eval_record(&program, &typed, &ctx_c, &resolver, None, &mut state);

    let tree_proj = project(&tree);
    assert_eq!(
        tree_proj,
        project(&comp),
        "tree-walk and compiled diverged on emit-each ceiling",
    );
    match tree_proj {
        ResultProjection::Err { kind, .. } => assert!(
            kind.contains("ExpansionLimitExceeded"),
            "expected ExpansionLimitExceeded, got {kind}"
        ),
        other => panic!("expected ceiling error, got {other:?}"),
    }
}

// ── Differential corpus: system-access leaves with resolvable data ─────

/// Assert agreement on a program that references declared scoped vars /
/// sources, with the resolve and typecheck passes threaded the same
/// `ScopedVarsRegistry` so `$vars.<key>` / `$source.<input>.<field>` /
/// qualified field refs resolve. The eval context's `static_vars` /
/// `source_input_arcs` stay empty, so the declared reads resolve to
/// `Null` at run time — identically through both evaluators.
fn assert_agree_with_vars(
    src: &str,
    fields: &[&str],
    qualified_sources: &[&str],
    registry: &crate::resolve::scoped_vars::ScopedVarsRegistry,
    record: HashMap<String, Value>,
) -> ResultProjection {
    use crate::resolve::pass::resolve_program_with_modules_and_vars;
    use crate::typecheck::pass::{AggregateMode, type_check_with_mode_and_vars};
    let build = || {
        let parsed = Parser::parse(src);
        assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
        let resolved = resolve_program_with_modules_and_vars(
            parsed.ast,
            fields,
            parsed.node_count,
            &std::collections::HashMap::new(),
            registry,
        )
        .unwrap_or_else(|d| {
            panic!(
                "resolve: {:?}",
                d.iter().map(|e| &e.message).collect::<Vec<_>>()
            )
        });
        type_check_with_mode_and_vars(resolved, &empty_row(), AggregateMode::Row, registry)
            .unwrap_or_else(|d| {
                panic!(
                    "type: {:?}",
                    d.iter().map(|e| &e.message).collect::<Vec<_>>()
                )
            })
            .with_source(Arc::from(src))
    };
    let typed = build();
    let stable = StableEvalContext::test_default();
    // Declare each qualified-source input name with an empty Arc list so
    // `$source.<input>.<field>` resolves to Null at eval rather than
    // panicking on a missing map entry; the lookup still flows through
    // both evaluators identically.
    let mut input_arcs = std::collections::HashMap::new();
    for s in qualified_sources {
        input_arcs.insert(s.to_string(), Vec::new());
    }
    let stable = StableEvalContext {
        source_input_arcs: Arc::new(input_arcs),
        ..stable
    };
    let resolver = HashMapResolver::new(record);

    let ctx_tw = EvalContext::test_default_borrowed(&stable);
    let mut evaluator = ProgramEvaluator::new(Arc::new(build()), false);
    let tree = evaluator.eval_record::<NullStorage>(&ctx_tw, &resolver, None);

    let ctx_c = EvalContext::test_default_borrowed(&stable);
    let program = compile::<NullStorage>(&typed);
    let mut state = EvalState::new(false, DEFAULT_MAX_EXPANSION);
    let comp = compiled_eval_record(&program, &typed, &ctx_c, &resolver, None, &mut state);

    let tree_proj = project(&tree);
    let comp_proj = project(&comp);
    assert_eq!(
        tree_proj, comp_proj,
        "tree-walk and compiled diverged on `{src}`",
    );
    tree_proj
}

/// `$source.<member>` builtins and `$doc.<section>.<field>` resolve
/// without a declared registry; both evaluators agree, including the
/// `Null` an empty envelope resolves `$doc` to.
#[test]
fn system_access_builtins_and_doc() {
    // `$source.row` is a builtin → Integer(1) under the test context.
    let proj = assert_agree("emit out = $source.row", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(1))],
            record_vars: vec![],
        },
    );
    // `$source.file` is a builtin string.
    assert_agree("emit out = $source.file", &[], HashMap::new());
    // `$doc.<section>.<field>` with an empty envelope → Null on both paths.
    let proj = assert_agree("emit out = $doc.header.title", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// `$vars.<key>` (declared static config) resolves through the shared
/// `static_vars` map; with the map empty at run time both evaluators read
/// `Null`. Pinned because `VarsAccess` otherwise has zero differential
/// coverage.
#[test]
fn vars_access() {
    use crate::resolve::scoped_vars::{ScopedVarType, ScopedVarsRegistry};
    let mut registry = ScopedVarsRegistry::default();
    registry
        .static_vars
        .insert("threshold".to_string(), ScopedVarType::Int);
    let proj = assert_agree_with_vars(
        "emit out = $vars.threshold",
        &[],
        &[],
        &registry,
        HashMap::new(),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// `$source.<input_name>.<field>` (qualified source access) resolves
/// through the plan-time `source_input_arcs` map. The input is declared so
/// the read type-checks; with no source-var writes both evaluators
/// resolve `Null`. Pinned because `QualifiedSourceAccess` otherwise has
/// zero differential coverage.
#[test]
fn qualified_source_access() {
    use crate::resolve::scoped_vars::{ScopedVarType, ScopedVarsRegistry};
    let mut registry = ScopedVarsRegistry::default();
    registry
        .source
        .insert("tag".to_string(), ScopedVarType::String);
    let proj = assert_agree_with_vars(
        "emit out = $source.upstream.tag",
        &[],
        &["upstream"],
        &registry,
        HashMap::new(),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// A 3-part qualified field reference (`source.record_type.field`)
/// resolves through `resolve_qualified` with the first two parts joined as
/// the compound source key. With the qualified entry absent both
/// evaluators resolve `Null`. Pinned because the 3-part form otherwise has
/// zero differential coverage.
#[test]
fn qualified_field_ref_three_part() {
    let resolver = HashMapResolver::new(HashMap::new()).with_qualified(
        "orders.line",
        "amount",
        Value::Integer(42),
    );
    let typed = type_program("emit out = orders.line.amount", &[]);
    let stable = StableEvalContext::test_default();

    let ctx_tw = EvalContext::test_default_borrowed(&stable);
    let mut evaluator = ProgramEvaluator::new(
        Arc::new(type_program("emit out = orders.line.amount", &[])),
        false,
    );
    let tree = evaluator.eval_record::<NullStorage>(&ctx_tw, &resolver, None);

    let ctx_c = EvalContext::test_default_borrowed(&stable);
    let program = compile::<NullStorage>(&typed);
    let mut state = EvalState::new(false, DEFAULT_MAX_EXPANSION);
    let comp = compiled_eval_record(&program, &typed, &ctx_c, &resolver, None, &mut state);

    let tree_proj = project(&tree);
    assert_eq!(
        tree_proj,
        project(&comp),
        "tree-walk and compiled diverged on 3-part qualified ref",
    );
    // The compound `orders.line` + `amount` resolves to the seeded value
    // through both evaluators.
    assert_eq!(
        tree_proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(42))],
            record_vars: vec![],
        },
    );
}

// ── Differential corpus: real example-pipeline transform bodies ────────

/// The row-transform CXL bodies lifted verbatim from `examples/pipelines/`.
/// Any pipeline body that type-checks as a flat row transform must diff
/// identically between the two evaluators now that every node lowers;
/// aggregate / combine bodies (qualified refs, `sum(..)`) are not row
/// transforms and are out of scope for this evaluator, so they are
/// excluded rather than expected to type-check here.
#[test]
fn example_pipeline_transform_bodies() {
    // (src, input field set, record).
    let cases: Vec<(&str, &[&str], HashMap<String, Value>)> = vec![
        (
            "emit is_active = status == \"active\"",
            &["status"],
            HashMap::from([("status".to_string(), Value::String("active".into()))]),
        ),
        (
            "emit tier = if lifetime_value.to_int() > 10000 then \"gold\" else \"standard\"",
            &["lifetime_value"],
            HashMap::from([("lifetime_value".to_string(), Value::String("25000".into()))]),
        ),
        (
            "emit _include = days_since(invoice_date) < 90",
            &["invoice_date"],
            HashMap::from([(
                "invoice_date".to_string(),
                Value::String("2026-05-01".into()),
            )]),
        ),
        (
            "filter priority == \"high\"\nemit p = priority",
            &["priority"],
            HashMap::from([("priority".to_string(), Value::String("high".into()))]),
        ),
        (
            "emit address_upper = shipping_address.upper()\nemit uuid_prefix = ticket_uuid.substring(0, 8)\nemit tagged_comment = priority.upper().concat(\": \", comment)\nemit comment_length = comment.length()",
            &["shipping_address", "ticket_uuid", "priority", "comment"],
            HashMap::from([
                (
                    "shipping_address".to_string(),
                    Value::String("12 main st".into()),
                ),
                (
                    "ticket_uuid".to_string(),
                    Value::String("abcdef0123456789".into()),
                ),
                ("priority".to_string(), Value::String("high".into())),
                (
                    "comment".to_string(),
                    Value::String("please expedite".into()),
                ),
            ]),
        ),
        (
            "emit order_ref = \"ORD-\" + order_id.to_string().pad_left(8, \"0\")\nemit line_total = quantity * unit_price",
            &["order_id", "quantity", "unit_price"],
            HashMap::from([
                ("order_id".to_string(), Value::Integer(42)),
                ("quantity".to_string(), Value::Integer(3)),
                ("unit_price".to_string(), Value::Float(9.99)),
            ]),
        ),
        (
            "emit _route = if priority_level == \"urgent\" or priority_level == \"high\" then \"priority_report\" else \"fulfilled_orders\"",
            &["priority_level"],
            HashMap::from([("priority_level".to_string(), Value::String("urgent".into()))]),
        ),
    ];
    for (src, fields, record) in cases {
        // Skip any body that does not type-check as a flat row transform
        // (e.g. one referencing a builtin this corpus does not feed); the
        // ones that type-check must diff identically.
        if try_type_program(src, fields).is_some() {
            assert_agree(src, fields, record);
        }
    }
}

// ── Property test: random scalar-core expressions ──────────────────────

/// A generated scalar-core expression. Lowers to CXL source text and is
/// evaluated by both paths. The grammar covers literals (including
/// nulls), the three input field refs, nested binary / unary / coalesce
/// / if, with real depth — but no method calls, closures, windows, or
/// other deferred nodes.
#[derive(Debug, Clone)]
enum GenExpr {
    IntLit(i64),
    BoolLit(bool),
    NullLit,
    StrLit(String),
    Field(&'static str),
    Binary(&'static str, Box<GenExpr>, Box<GenExpr>),
    Not(Box<GenExpr>),
    Neg(Box<GenExpr>),
    Coalesce(Box<GenExpr>, Box<GenExpr>),
    If(Box<GenExpr>, Box<GenExpr>, Box<GenExpr>),
    /// A zero-argument method on an arbitrary receiver. Restricted to the
    /// `TypeTag::Any`-receiver builtins (`to_string` / `is_null` /
    /// `type_of`) so the method does not constrain the receiver's type
    /// and over-reject otherwise-valid generated programs. Exercises the
    /// compiled `MethodCall` node and its delegation to `dispatch_method`
    /// — including the null-receiver gate and its exceptions — over the
    /// same nested receivers the scalar core generates.
    Method(&'static str, Box<GenExpr>),
    /// `catch(default)`: a one-argument `Any`-receiver method, exercising
    /// the eager-argument path of the compiled `MethodCall` node.
    Catch(Box<GenExpr>, Box<GenExpr>),
}

impl GenExpr {
    /// Render to CXL source text. Every binary/unary form is fully
    /// parenthesized so the generated precedence is unambiguous and the
    /// parser reconstructs exactly the generated tree.
    fn render(&self) -> String {
        match self {
            GenExpr::IntLit(n) => n.to_string(),
            GenExpr::BoolLit(b) => b.to_string(),
            GenExpr::NullLit => "null".to_string(),
            GenExpr::StrLit(s) => format!("\"{s}\""),
            GenExpr::Field(name) => name.to_string(),
            GenExpr::Binary(op, l, r) => format!("({} {} {})", l.render(), op, r.render()),
            GenExpr::Not(e) => format!("(not {})", e.render()),
            GenExpr::Neg(e) => format!("(-{})", e.render()),
            GenExpr::Coalesce(l, r) => format!("({} ?? {})", l.render(), r.render()),
            GenExpr::If(c, t, e) => {
                format!(
                    "(if {} then {} else {})",
                    c.render(),
                    t.render(),
                    e.render()
                )
            }
            GenExpr::Method(name, recv) => format!("({}).{}()", recv.render(), name),
            GenExpr::Catch(recv, default) => {
                format!("({}).catch({})", recv.render(), default.render())
            }
        }
    }
}

/// Leaf generator: literals (with nulls and strings) and the three input
/// field refs `a` / `b` / `c`.
fn leaf_strategy() -> impl Strategy<Value = GenExpr> {
    prop_oneof![
        (-1000i64..1000).prop_map(GenExpr::IntLit),
        any::<bool>().prop_map(GenExpr::BoolLit),
        Just(GenExpr::NullLit),
        "[a-z]{0,4}".prop_map(GenExpr::StrLit),
        Just(GenExpr::Field("a")),
        Just(GenExpr::Field("b")),
        Just(GenExpr::Field("c")),
    ]
}

/// Recursive expression generator with real nesting: up to 4 levels
/// deep, branching into binary / unary / coalesce / if forms over the
/// full operator set the scalar core supports.
fn expr_strategy() -> impl Strategy<Value = GenExpr> {
    leaf_strategy().prop_recursive(4, 48, 4, |inner| {
        prop_oneof![
            (
                prop_oneof![
                    Just("+"),
                    Just("-"),
                    Just("*"),
                    Just("/"),
                    Just("%"),
                    Just("=="),
                    Just("!="),
                    Just(">"),
                    Just("<"),
                    Just(">="),
                    Just("<="),
                    Just("and"),
                    Just("or"),
                ],
                inner.clone(),
                inner.clone(),
            )
                .prop_map(|(op, l, r)| GenExpr::Binary(op, Box::new(l), Box::new(r))),
            inner.clone().prop_map(|e| GenExpr::Not(Box::new(e))),
            inner.clone().prop_map(|e| GenExpr::Neg(Box::new(e))),
            (inner.clone(), inner.clone())
                .prop_map(|(l, r)| GenExpr::Coalesce(Box::new(l), Box::new(r))),
            (inner.clone(), inner.clone(), inner.clone()).prop_map(|(c, t, e)| GenExpr::If(
                Box::new(c),
                Box::new(t),
                Box::new(e)
            )),
            (
                prop_oneof![Just("to_string"), Just("is_null"), Just("type_of")],
                inner.clone(),
            )
                .prop_map(|(name, recv)| GenExpr::Method(name, Box::new(recv))),
            (inner.clone(), inner.clone())
                .prop_map(|(recv, default)| GenExpr::Catch(Box::new(recv), Box::new(default))),
        ]
    })
}

/// Random input values for the `a` / `b` / `c` fields, spanning every
/// scalar type plus null so generated expressions hit mixed-type and
/// null-propagation paths.
fn value_strategy() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        (-1000i64..1000).prop_map(Value::Integer),
        (-1000.0f64..1000.0).prop_map(Value::Float),
        "[a-z]{0,4}".prop_map(|s| Value::String(s.into())),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// For any generated scalar-core expression and any random a/b/c
    /// inputs, the tree-walk and the compiled evaluator must produce
    /// byte-identical results — value, skip, or error (kind + span +
    /// boundary provenance).
    #[test]
    fn compiled_matches_tree_walk(
        expr in expr_strategy(),
        a in value_strategy(),
        b in value_strategy(),
        c in value_strategy(),
    ) {
        let src = format!("emit out = {}", expr.render());
        // Discard programs that are not well-typed CXL (e.g. a field
        // used as both numeric and string): neither evaluator is defined
        // on them, so they are out of scope for the fidelity contract.
        let typed = match try_type_program(&src, &["a", "b", "c"]) {
            Some(t) => t,
            None => return Err(TestCaseError::reject("not well-typed")),
        };
        let typed_tw = try_type_program(&src, &["a", "b", "c"]).expect("second type-check agrees");
        let record = HashMap::from([
            ("a".to_string(), a),
            ("b".to_string(), b),
            ("c".to_string(), c),
        ]);

        let stable = StableEvalContext::test_default();
        let resolver = HashMapResolver::new(record);

        let ctx_tw = EvalContext::test_default_borrowed(&stable);
        let mut evaluator = ProgramEvaluator::new(Arc::new(typed_tw), false);
        let tree = evaluator.eval_record::<NullStorage>(&ctx_tw, &resolver, None);

        let ctx_c = EvalContext::test_default_borrowed(&stable);
        let program = compile::<NullStorage>(&typed);
        let mut state = EvalState::new(program_has_distinct(&typed), DEFAULT_MAX_EXPANSION);
        let comp = compiled_eval_record(&program, &typed, &ctx_c, &resolver, None, &mut state);

        prop_assert_eq!(project(&tree), project(&comp), "diverged on `{}`", src);
    }
}

// ── Property test: random statement sequences ──────────────────────────

/// One generated statement in a randomized program. Covers the four emit
/// targets (so emit routing is property-checked, not just example-checked)
/// plus `let` bindings, with an optional leading `filter`. `Filter` only
/// appears as the first statement — a mid-program filter would skip the
/// record and make the remaining statements unreachable, which is valid
/// but uninteresting for the routing/ordering property this exercises.
#[derive(Debug, Clone)]
enum GenStmt {
    /// `let v<i> = <expr>` — exercises the env-write path; the binding is
    /// uniquely named so it never shadows an input field.
    Let(usize, GenExpr),
    /// `emit out<i> = <expr>` — Field target → output map.
    EmitField(usize, GenExpr),
    /// `emit $record.r<i> = <expr>` — Record target → record_vars channel.
    EmitRecord(usize, GenExpr),
    /// `emit $pipeline.p<i> = <expr>` — Pipeline target → context, not
    /// surfaced in the result.
    EmitPipeline(usize, GenExpr),
    /// `emit $source.s<i> = <expr>` — Source target → context, not
    /// surfaced in the result.
    EmitSource(usize, GenExpr),
}

impl GenStmt {
    fn render(&self) -> String {
        match self {
            GenStmt::Let(i, e) => format!("let v{i} = {}", e.render()),
            GenStmt::EmitField(i, e) => format!("emit out{i} = {}", e.render()),
            GenStmt::EmitRecord(i, e) => format!("emit $record.r{i} = {}", e.render()),
            GenStmt::EmitPipeline(i, e) => format!("emit $pipeline.p{i} = {}", e.render()),
            GenStmt::EmitSource(i, e) => format!("emit $source.s{i} = {}", e.render()),
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// For any generated statement *sequence* — an optional leading
    /// `filter`, then one-to-six `let` / `emit` statements spanning all
    /// four emit targets — the tree-walk and the compiled evaluator must
    /// agree byte-for-byte on the result. This property-checks emit
    /// routing (which channel each target lands in) and statement ordering
    /// (the order field emits accumulate, and the filter short-circuit),
    /// not just single-expression evaluation.
    #[test]
    fn compiled_matches_tree_walk_statement_seq(
        leading_filter in proptest::option::of(expr_strategy()),
        stmts in proptest::collection::vec(0usize..5, 1..6),
        filler in expr_strategy(),
        a in value_strategy(),
        b in value_strategy(),
        c in value_strategy(),
    ) {
        // Build the program text. The `filler` expr seeds each statement's
        // RHS via `stmt_strategy`-equivalent rendering keyed by index.
        let mut lines: Vec<String> = Vec::new();
        if let Some(f) = &leading_filter {
            lines.push(format!("filter {}", f.render()));
        }
        for (i, kind) in stmts.iter().enumerate() {
            let stmt = match kind {
                0 => GenStmt::Let(i, filler.clone()),
                1 => GenStmt::EmitField(i, filler.clone()),
                2 => GenStmt::EmitRecord(i, filler.clone()),
                3 => GenStmt::EmitPipeline(i, filler.clone()),
                _ => GenStmt::EmitSource(i, filler.clone()),
            };
            lines.push(stmt.render());
        }
        // Ensure at least one field emit so a non-skipped program produces
        // an observable output channel (otherwise every emit could route
        // to pipeline/source and the result is trivially empty).
        lines.push("emit final = a".to_string());
        let src = lines.join("\n");

        let typed = match try_type_program(&src, &["a", "b", "c"]) {
            Some(t) => t,
            None => return Err(TestCaseError::reject("not well-typed")),
        };
        let typed_tw = try_type_program(&src, &["a", "b", "c"]).expect("second type-check agrees");
        let record = HashMap::from([
            ("a".to_string(), a),
            ("b".to_string(), b),
            ("c".to_string(), c),
        ]);

        let stable = StableEvalContext::test_default();
        let resolver = HashMapResolver::new(record);

        let ctx_tw = EvalContext::test_default_borrowed(&stable);
        let mut evaluator = ProgramEvaluator::new(Arc::new(typed_tw), false);
        let tree = evaluator.eval_record::<NullStorage>(&ctx_tw, &resolver, None);

        let ctx_c = EvalContext::test_default_borrowed(&stable);
        let program = compile::<NullStorage>(&typed);
        let mut state = EvalState::new(program_has_distinct(&typed), DEFAULT_MAX_EXPANSION);
        let comp = compiled_eval_record(&program, &typed, &ctx_c, &resolver, None, &mut state);

        prop_assert_eq!(project(&tree), project(&comp), "diverged on `{}`", src);
    }
}
