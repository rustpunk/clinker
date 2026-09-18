use cxl::ast::{Expr, LiteralValue, Statement};
use cxl::parser::Parser;

// Match the small platform main-stack budget. This deliberately constrains a
// test thread; production parsing must not require a larger stack to succeed.
fn on_small_stack(check: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(1024 * 1024)
        .spawn(check)
        .expect("spawn constrained-stack parser check")
        .join()
        .expect("constrained-stack parser check panicked");
}

const WRAPPERS: &[(&str, &str)] = &[
    ("{n: ", "}"),
    ("[", "]"),
    ("(", ")"),
    ("not ", ""),
    ("- ", ""),
    ("0 ?? ", ""),
    ("if ", " then 1 else 2"),
    ("if true then ", " else 2"),
    ("if true then 1 else ", ""),
    ("match ", " { _ => 1 }"),
    ("match { ", " => 1 }"),
    ("match { _ => ", " }"),
    ("items.method(", ")"),
    ("items.map(it => ", ")"),
    ("sum(", ")"),
    ("$window.lag(", ")"),
    ("items[", "]"),
    ("{[", "]: 1}"),
    ("[", " for item in items if true]"),
    ("[item for item in ", " if true]"),
    ("[item for item in items if ", "]"),
];

// These wrappers can nest in any order. A bare coalesce in an if-condition,
// for example, would be invalid at any depth because that context has BP 5.
const MIXED_WRAPPERS: &[(&str, &str)] = &[
    WRAPPERS[0],
    WRAPPERS[1],
    WRAPPERS[2],
    WRAPPERS[3],
    WRAPPERS[12],
    WRAPPERS[13],
    WRAPPERS[15],
    WRAPPERS[16],
    WRAPPERS[17],
];

fn nested(wrappers: &[(&str, &str)], count: usize, leaf: &str) -> String {
    let mut expression = leaf.to_owned();
    for index in 0..count {
        let (prefix, suffix) = wrappers[index % wrappers.len()];
        expression = format!("{prefix}{expression}{suffix}");
    }
    expression
}

#[test]
fn nested_value_boundaries_parse_on_small_stack() {
    on_small_stack(|| {
        for wrappers in [&WRAPPERS[..1], &WRAPPERS[1..2], &WRAPPERS[..2]] {
            for depth in [0, 1, 63, 64, 65] {
                let source = format!("emit payload = {}", nested(wrappers, depth, "null"));
                let parsed = Parser::parse(&source);
                assert!(
                    parsed.errors.is_empty(),
                    "depth {depth}: {:?}",
                    parsed.errors
                );
                assert_eq!(parsed.ast.statements.len(), 1);
                // Drop the completed tree on the same constrained stack.
                drop(parsed);
            }
        }
    });
}

#[test]
fn expression_depth_boundary_covers_every_recursive_syntax_on_small_stack() {
    on_small_stack(|| {
        // The root expression counts as level one: 255 wrappers around a
        // scalar reach the existing 256-call limit, 256 wrappers exceed it.
        for wrappers in WRAPPERS
            .iter()
            .map(std::slice::from_ref)
            .chain([MIXED_WRAPPERS])
        {
            for (count, allowed) in [(254, true), (255, true), (256, false), (300, false)] {
                let source = format!(
                    "let deep = {}\nlet recovered = 42",
                    nested(wrappers, count, "0")
                );
                let parsed = Parser::parse(&source);
                if allowed {
                    assert!(
                        parsed.errors.is_empty(),
                        "wrapper {:?}, count {count}: {:?}",
                        wrappers[0],
                        parsed.errors
                    );
                    assert_eq!(parsed.ast.statements.len(), 2);
                } else {
                    assert_eq!(
                        parsed.errors.len(),
                        1,
                        "wrapper {:?}, count {count}: {:?}",
                        wrappers[0],
                        parsed.errors
                    );
                    assert_eq!(
                        parsed.errors[0].message,
                        "expression nesting too deep (max 256 levels)"
                    );
                    assert_eq!(parsed.ast.statements.len(), 1);
                }
                assert!(matches!(
                    parsed.ast.statements.last(),
                    Some(Statement::Let {
                        expr: Expr::Literal {
                            value: LiteralValue::Int(42),
                            ..
                        },
                        ..
                    })
                ));
                drop(parsed);
            }
        }
    });
}

#[test]
fn malformed_nested_expressions_release_frames_and_reset_depth() {
    on_small_stack(|| {
        for wrappers in WRAPPERS
            .iter()
            .map(std::slice::from_ref)
            .chain([MIXED_WRAPPERS])
        {
            let mut source = String::new();
            for _ in 0..3 {
                source.push_str(&format!("let broken = {}\n", nested(wrappers, 255, ":")));
            }
            source.push_str(&format!("let valid = {}", nested(wrappers, 255, "0")));
            let parsed = Parser::parse(&source);
            assert_eq!(
                parsed.errors.len(),
                3,
                "wrapper {:?}: {:?}",
                wrappers[0],
                parsed.errors
            );
            assert!(
                parsed
                    .errors
                    .iter()
                    .all(|error| error.message.contains("unexpected token"))
            );
            assert_eq!(parsed.ast.statements.len(), 1);
            drop(parsed);
        }
    });
}

#[test]
fn expression_stack_budget_is_independent_of_emit_each_statement_nesting() {
    on_small_stack(|| {
        let mut source = format!("emit payload = {}", nested(&WRAPPERS[..2], 255, "null"));
        for _ in 0..32 {
            source = format!("emit each item in items {{\n{source}\n}}");
        }
        let parsed = Parser::parse(&source);
        assert!(parsed.errors.is_empty(), "{:?}", parsed.errors);
        drop(parsed);
    });
}

#[test]
fn unterminated_match_preserves_error_location_and_node_count() {
    for (suffix, expected, nodes) in [
        ("", "expected '}', got Eof", 2),
        ("\n", "unexpected token Eof", 3),
        ("\n# trailing comment", "unexpected token Eof", 3),
    ] {
        let source = format!("let x = match {{{suffix}");
        let parsed = Parser::parse(&source);
        assert_eq!(parsed.errors.len(), 1);
        assert_eq!(parsed.errors[0].message, expected);
        assert_eq!(parsed.errors[0].span.start as usize, source.len());
        assert_eq!(parsed.node_count, nodes);
    }
}

#[test]
fn empty_containers_and_call_arguments_preserve_depth_accounting() {
    on_small_stack(|| {
        for leaf in [
            "{}",
            "[]",
            "count()",
            "count(*)",
            "items.method()",
            "$window.lag()",
        ] {
            let source = format!("let deep = {}", nested(&WRAPPERS[2..3], 255, leaf));
            let parsed = Parser::parse(&source);
            assert!(parsed.errors.is_empty(), "leaf {leaf}: {:?}", parsed.errors);
        }
        for leaf in [
            "{n: 0}",
            "[0]",
            "count(0)",
            "items.method(0)",
            "$window.lag(0)",
        ] {
            let source = format!("let deep = {}", nested(&WRAPPERS[2..3], 255, leaf));
            let parsed = Parser::parse(&source);
            assert_eq!(parsed.errors.len(), 1, "leaf {leaf}");
            assert_eq!(
                parsed.errors[0].message,
                "expression nesting too deep (max 256 levels)"
            );
        }
    });
}

#[test]
fn module_expressions_keep_depth_budget_after_recovery_on_small_stack() {
    on_small_stack(|| {
        let source = format!(
            "fn bad(x) = {}\nlet good = {}\nfn recovered(x) = {}",
            nested(&WRAPPERS[..2], 256, "0"),
            nested(&WRAPPERS[..2], 255, "0"),
            nested(&WRAPPERS[2..3], 255, "x"),
        );
        let parsed = Parser::parse_module(&source);
        assert_eq!(parsed.errors.len(), 1);
        assert_eq!(
            parsed.errors[0].message,
            "expression nesting too deep (max 256 levels)"
        );
        assert_eq!(parsed.module.constants.len(), 1);
        assert_eq!(parsed.module.functions.len(), 1);
        assert_eq!(parsed.module.functions[0].name.as_ref(), "recovered");
        drop(parsed);
    });
}
