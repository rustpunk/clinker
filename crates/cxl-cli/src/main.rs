use std::collections::HashMap;
use std::process;

use clap::{Parser, Subcommand};
use clinker_record::{RecordStorage, Value};

/// Dummy storage for no-window evaluation.
struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<Value> {
        None
    }
    fn available_fields(&self, _: u32) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u32 {
        0
    }
}
use cxl::eval::{EvalContext, WallClock};
use cxl::resolve::HashMapResolver;

#[derive(Parser)]
#[command(
    name = "cxl",
    version,
    about = "CXL language validator, evaluator, and formatter",
    long_about = "\
Standalone tool for the CXL expression language used by Clinker pipelines. \
Validate .cxl files for correctness, evaluate expressions against sample \
data, or canonically reformat source files.",
    after_long_help = "\
QUICK START:
  cxl check transform.cxl
  cxl eval -e 'emit result = 1 + 2'
  cxl eval transform.cxl --field Price=10.5 --field Qty=3
  cxl fmt transform.cxl

EXIT CODES:
  0  Success (or warnings only)
  1  Parse, resolve, type-check, or evaluation errors
  2  I/O error (file not found, invalid JSON, etc.)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Parse, resolve, and type-check a .cxl file. Exit 0 if valid, 1 if errors.
    #[command(long_about = "\
Parse, resolve, and type-check a .cxl file through the full compilation \
pipeline. Reports parse errors, unresolved references, and type mismatches \
with source locations and fix suggestions. Exits 0 if valid, 1 if errors.")]
    Check {
        /// Path to the .cxl file
        file: String,
    },
    /// Evaluate a CXL expression against provided field values.
    #[command(
        long_about = "\
Evaluate a CXL expression against provided field values and print the \
output as JSON. Provide the expression via a .cxl file or inline with -e. \
Supply input data as a JSON object (--record) or as individual key=value \
pairs (--field) with automatic type inference (integer, float, bool, \
null, or string).",
        after_long_help = "\
EXAMPLES:
  # Evaluate an inline expression
  cxl eval -e 'emit greeting = \"hello\"'

  # Evaluate a file with field values
  cxl eval transform.cxl --field name=Alice --field age=30

  # Evaluate with a JSON record
  cxl eval transform.cxl --record '{\"price\": 10.5, \"qty\": 3}'"
    )]
    Eval {
        /// Path to a .cxl file (required unless -e is provided)
        #[arg(required_unless_present = "expr")]
        file: Option<String>,
        /// Inline CXL expression to evaluate
        #[arg(short = 'e', long)]
        expr: Option<String>,
        /// JSON record to evaluate against (mutually exclusive with --field)
        #[arg(long, conflicts_with = "fields")]
        record: Option<String>,
        /// Field values as name=value pairs with auto-type inference
        #[arg(long = "field")]
        fields: Vec<String>,
    },
    /// Parse and pretty-print a .cxl file (canonical formatting).
    #[command(long_about = "\
Parse a .cxl file and print it in canonical format with normalized \
whitespace and consistent styling. Useful for enforcing a uniform code \
style across CXL source files.")]
    Fmt {
        /// Path to the .cxl file
        file: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Check { file } => cmd_check(&file),
        Command::Eval {
            file,
            expr,
            record,
            fields,
        } => cmd_eval(file.as_deref(), expr.as_deref(), record.as_deref(), &fields),
        Command::Fmt { file } => cmd_fmt(&file),
    }
}

fn cmd_check(file: &str) {
    let source = match std::fs::read_to_string(file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot read '{}': {}", file, e);
            process::exit(2);
        }
    };

    let parsed = cxl::parser::Parser::parse(&source);
    if !parsed.errors.is_empty() {
        for err in &parsed.errors {
            eprintln!(
                "error[parse]: {} (at {}:{})",
                err.message, file, err.span.start
            );
            if !err.how_to_fix.is_empty() {
                eprintln!("  help: {}", err.how_to_fix);
            }
        }
        process::exit(1);
    }

    // Extract field names from the AST (rough: collect FieldRef names)
    let fields = extract_field_names(&parsed.ast);
    let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();

    let resolved = match cxl::resolve::resolve_program(parsed.ast, &field_refs, parsed.node_count) {
        Ok(r) => r,
        Err(diags) => {
            for d in &diags {
                eprintln!(
                    "error[resolve]: {} (at {}:{})",
                    d.message, file, d.span.start
                );
                if let Some(help) = &d.help {
                    eprintln!("  help: {}", help);
                }
            }
            process::exit(1);
        }
    };

    match cxl::typecheck::type_check(resolved, &HashMap::new()) {
        Ok(_) => {
            eprintln!("ok: {} is valid", file);
            process::exit(0);
        }
        Err(diags) => {
            for d in &diags {
                let level = if d.is_warning { "warning" } else { "error" };
                eprintln!(
                    "{}[typecheck]: {} (at {}:{})",
                    level, d.message, file, d.span.start
                );
                if let Some(help) = &d.help {
                    eprintln!("  help: {}", help);
                }
            }
            let has_errors = diags.iter().any(|d| !d.is_warning);
            process::exit(if has_errors { 1 } else { 0 });
        }
    }
}

fn cmd_eval(file: Option<&str>, expr: Option<&str>, record_json: Option<&str>, fields: &[String]) {
    // Get CXL source from file or inline expression
    let source = if let Some(expr) = expr {
        expr.to_string()
    } else if let Some(file) = file {
        match std::fs::read_to_string(file) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: cannot read '{}': {}", file, e);
                process::exit(2);
            }
        }
    } else {
        eprintln!("error: provide a file path or use -e for inline expression");
        process::exit(2);
    };

    let source_name = file.unwrap_or("<inline>");

    // Build field map from --record JSON or --field key=value pairs
    let record_map: HashMap<String, Value> = if let Some(json) = record_json {
        let json_value: serde_json::Value = match serde_json::from_str(json) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("error: invalid JSON record: {}", e);
                process::exit(2);
            }
        };
        match json_value {
            serde_json::Value::Object(map) => map
                .into_iter()
                .map(|(k, v)| (k, json_to_value(v)))
                .collect(),
            _ => {
                eprintln!("error: --record must be a JSON object");
                process::exit(2);
            }
        }
    } else if !fields.is_empty() {
        fields
            .iter()
            .map(|f| {
                let (name, value) = f.split_once('=').unwrap_or_else(|| {
                    eprintln!("error: --field must be name=value, got '{}'", f);
                    process::exit(2);
                });
                (name.to_string(), parse_field_value(value))
            })
            .collect()
    } else {
        HashMap::new()
    };

    let field_names: Vec<String> = record_map.keys().cloned().collect();
    let field_refs: Vec<&str> = field_names.iter().map(|s| s.as_str()).collect();

    let parsed = cxl::parser::Parser::parse(&source);
    if !parsed.errors.is_empty() {
        for err in &parsed.errors {
            eprintln!("error[parse]: {}", err.message);
        }
        process::exit(1);
    }

    let resolved = match cxl::resolve::resolve_program(parsed.ast, &field_refs, parsed.node_count) {
        Ok(r) => r,
        Err(diags) => {
            for d in &diags {
                eprintln!("error[resolve]: {}", d.message);
            }
            process::exit(1);
        }
    };

    let typed = match cxl::typecheck::type_check(resolved, &HashMap::new()) {
        Ok(t) => t,
        Err(diags) => {
            for d in &diags {
                if !d.is_warning {
                    eprintln!("error[typecheck]: {}", d.message);
                }
            }
            process::exit(1);
        }
    };

    let ctx = EvalContext {
        clock: Box::new(WallClock),
        pipeline_start_time: chrono::Utc::now().naive_utc(),
        pipeline_name: "cxl-eval".into(),
        pipeline_execution_id: "00000000-0000-0000-0000-000000000000".into(),
        pipeline_batch_id: "00000000-0000-0000-0000-000000000000".into(),
        pipeline_counters: clinker_record::PipelineCounters::default(),
        source_file: std::sync::Arc::from(source_name),
        source_row: 1,
        pipeline_vars: indexmap::IndexMap::new(),
    };

    let resolver = HashMapResolver::new(record_map);

    match cxl::eval::eval_program::<NullStorage>(&typed, &ctx, &resolver, None) {
        Ok(output) => {
            let json_out: serde_json::Map<String, serde_json::Value> = output
                .into_iter()
                .map(|(k, v)| (k, value_to_json(v)))
                .collect();
            println!("{}", serde_json::to_string_pretty(&json_out).unwrap());
        }
        Err(e) => {
            eprintln!("error[eval]: {}", e);
            process::exit(1);
        }
    }
}

/// Parse a field value string, inferring type: integer → float → bool → null → string.
fn parse_field_value(s: &str) -> Value {
    // Try integer
    if let Ok(n) = s.parse::<i64>() {
        return Value::Integer(n);
    }
    // Try float
    if let Ok(f) = s.parse::<f64>() {
        return Value::Float(f);
    }
    // Try bool
    match s {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }
    // Try null
    if s == "null" {
        return Value::Null;
    }
    // Default: string
    Value::String(s.into())
}

fn cmd_fmt(file: &str) {
    let source = match std::fs::read_to_string(file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot read '{}': {}", file, e);
            process::exit(2);
        }
    };

    let parsed = cxl::parser::Parser::parse(&source);
    if !parsed.errors.is_empty() {
        for err in &parsed.errors {
            eprintln!("error[parse]: {}", err.message);
        }
        process::exit(1);
    }

    // Simple pretty-print: re-emit each statement
    for stmt in &parsed.ast.statements {
        println!("{}", format_statement(stmt));
    }
}

// ── Helpers ──────────────────────────────────────────────────

fn extract_field_names(program: &cxl::ast::Program) -> Vec<String> {
    let mut names = Vec::new();
    for stmt in &program.statements {
        collect_field_refs_stmt(stmt, &mut names);
    }
    names.sort();
    names.dedup();
    names
}

fn collect_field_refs_stmt(stmt: &cxl::ast::Statement, names: &mut Vec<String>) {
    match stmt {
        cxl::ast::Statement::Let { expr, .. }
        | cxl::ast::Statement::Emit { expr, .. }
        | cxl::ast::Statement::ExprStmt { expr, .. } => collect_field_refs_expr(expr, names),
        cxl::ast::Statement::Trace { guard, message, .. } => {
            if let Some(g) = guard {
                collect_field_refs_expr(g, names);
            }
            collect_field_refs_expr(message, names);
        }
        _ => {}
    }
}

fn collect_field_refs_expr(expr: &cxl::ast::Expr, names: &mut Vec<String>) {
    match expr {
        cxl::ast::Expr::FieldRef { name, .. } => {
            if &**name != "it" {
                names.push(name.to_string());
            }
        }
        cxl::ast::Expr::Binary { lhs, rhs, .. } | cxl::ast::Expr::Coalesce { lhs, rhs, .. } => {
            collect_field_refs_expr(lhs, names);
            collect_field_refs_expr(rhs, names);
        }
        cxl::ast::Expr::Unary { operand, .. } => collect_field_refs_expr(operand, names),
        cxl::ast::Expr::MethodCall { receiver, args, .. } => {
            collect_field_refs_expr(receiver, names);
            for a in args {
                collect_field_refs_expr(a, names);
            }
        }
        cxl::ast::Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_field_refs_expr(condition, names);
            collect_field_refs_expr(then_branch, names);
            if let Some(eb) = else_branch {
                collect_field_refs_expr(eb, names);
            }
        }
        cxl::ast::Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                collect_field_refs_expr(s, names);
            }
            for arm in arms {
                collect_field_refs_expr(&arm.pattern, names);
                collect_field_refs_expr(&arm.body, names);
            }
        }
        cxl::ast::Expr::WindowCall { args, .. } => {
            for a in args {
                collect_field_refs_expr(a, names);
            }
        }
        _ => {}
    }
}

fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.into()),
        serde_json::Value::Array(arr) => Value::Array(arr.into_iter().map(json_to_value).collect()),
        serde_json::Value::Object(_) => Value::Null, // Nested objects not supported in v1
    }
}

fn value_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Integer(n) => serde_json::json!(n),
        Value::Float(f) => serde_json::json!(f),
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Date(d) => serde_json::Value::String(d.format("%Y-%m-%d").to_string()),
        Value::DateTime(dt) => {
            serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S").to_string())
        }
        Value::Array(arr) => serde_json::Value::Array(arr.into_iter().map(value_to_json).collect()),
    }
}

fn format_statement(stmt: &cxl::ast::Statement) -> String {
    match stmt {
        cxl::ast::Statement::Let { name, expr, .. } => {
            format!("let {} = {}", name, format_expr(expr))
        }
        cxl::ast::Statement::Emit { name, expr, .. } => {
            format!("emit {} = {}", name, format_expr(expr))
        }
        cxl::ast::Statement::Trace {
            level,
            guard,
            message,
            ..
        } => {
            let mut s = "trace".to_string();
            if let Some(l) = level {
                s.push_str(&format!(" {:?}", l).to_lowercase());
            }
            if let Some(g) = guard {
                s.push_str(&format!(" if {}", format_expr(g)));
            }
            s.push_str(&format!(" {}", format_expr(message)));
            s
        }
        cxl::ast::Statement::UseStmt { path, alias, .. } => {
            let mut s = format!(
                "use {}",
                path.iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join("::")
            );
            if let Some(a) = alias {
                s.push_str(&format!(" as {}", a));
            }
            s
        }
        cxl::ast::Statement::ExprStmt { expr, .. } => format_expr(expr),
        cxl::ast::Statement::Filter { predicate, .. } => {
            format!("filter {}", format_expr(predicate))
        }
        cxl::ast::Statement::Distinct { field, .. } => match field {
            Some(f) => format!("distinct by {}", f),
            None => "distinct".to_string(),
        },
    }
}

fn format_expr(expr: &cxl::ast::Expr) -> String {
    match expr {
        cxl::ast::Expr::Literal { value, .. } => match value {
            cxl::ast::LiteralValue::Int(n) => n.to_string(),
            cxl::ast::LiteralValue::Float(f) => f.to_string(),
            cxl::ast::LiteralValue::String(s) => format!("\"{}\"", s),
            cxl::ast::LiteralValue::Date(d) => format!("#{}", d.format("%Y-%m-%d")),
            cxl::ast::LiteralValue::Bool(b) => b.to_string(),
            cxl::ast::LiteralValue::Null => "null".into(),
        },
        cxl::ast::Expr::FieldRef { name, .. } => name.to_string(),
        cxl::ast::Expr::QualifiedFieldRef { parts, .. } => {
            parts.iter().map(|p| &**p).collect::<Vec<_>>().join(".")
        }
        cxl::ast::Expr::Now { .. } => "now".into(),
        cxl::ast::Expr::Wildcard { .. } => "_".into(),
        cxl::ast::Expr::PipelineAccess { field, .. } => format!("pipeline.{}", field),
        cxl::ast::Expr::Binary { op, lhs, rhs, .. } => {
            let op_str = match op {
                cxl::ast::BinOp::Add => "+",
                cxl::ast::BinOp::Sub => "-",
                cxl::ast::BinOp::Mul => "*",
                cxl::ast::BinOp::Div => "/",
                cxl::ast::BinOp::Mod => "%",
                cxl::ast::BinOp::Eq => "==",
                cxl::ast::BinOp::Neq => "!=",
                cxl::ast::BinOp::Gt => ">",
                cxl::ast::BinOp::Lt => "<",
                cxl::ast::BinOp::Gte => ">=",
                cxl::ast::BinOp::Lte => "<=",
                cxl::ast::BinOp::And => "and",
                cxl::ast::BinOp::Or => "or",
            };
            format!("{} {} {}", format_expr(lhs), op_str, format_expr(rhs))
        }
        cxl::ast::Expr::Unary { op, operand, .. } => {
            let op_str = match op {
                cxl::ast::UnaryOp::Neg => "-",
                cxl::ast::UnaryOp::Not => "not ",
            };
            format!("{}{}", op_str, format_expr(operand))
        }
        cxl::ast::Expr::Coalesce { lhs, rhs, .. } => {
            format!("{} ?? {}", format_expr(lhs), format_expr(rhs))
        }
        cxl::ast::Expr::MethodCall {
            receiver,
            method,
            args,
            ..
        } => {
            let args_str = args.iter().map(format_expr).collect::<Vec<_>>().join(", ");
            format!("{}.{}({})", format_expr(receiver), method, args_str)
        }
        cxl::ast::Expr::WindowCall { function, args, .. } => {
            let args_str = args.iter().map(format_expr).collect::<Vec<_>>().join(", ");
            format!("window.{}({})", function, args_str)
        }
        cxl::ast::Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            let mut s = format!(
                "if {} then {}",
                format_expr(condition),
                format_expr(then_branch)
            );
            if let Some(eb) = else_branch {
                s.push_str(&format!(" else {}", format_expr(eb)));
            }
            s
        }
        cxl::ast::Expr::Match { subject, arms, .. } => {
            let mut s = "match ".to_string();
            if let Some(sub) = subject {
                s.push_str(&format!("{} ", format_expr(sub)));
            }
            s.push_str("{ ");
            for (i, arm) in arms.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(&format!(
                    "{} => {}",
                    format_expr(&arm.pattern),
                    format_expr(&arm.body)
                ));
            }
            s.push_str(" }");
            s
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_field_value_integer() {
        assert_eq!(parse_field_value("42"), Value::Integer(42));
        assert_eq!(parse_field_value("-5"), Value::Integer(-5));
        assert_eq!(parse_field_value("0"), Value::Integer(0));
    }

    #[test]
    fn test_parse_field_value_float() {
        assert_eq!(parse_field_value("3.14"), Value::Float(3.14));
        assert_eq!(parse_field_value("-0.5"), Value::Float(-0.5));
    }

    #[test]
    fn test_parse_field_value_bool() {
        assert_eq!(parse_field_value("true"), Value::Bool(true));
        assert_eq!(parse_field_value("false"), Value::Bool(false));
    }

    #[test]
    fn test_parse_field_value_null() {
        assert_eq!(parse_field_value("null"), Value::Null);
    }

    #[test]
    fn test_parse_field_value_string() {
        assert_eq!(parse_field_value("hello"), Value::String("hello".into()));
        assert_eq!(
            parse_field_value("hello world"),
            Value::String("hello world".into())
        );
    }

    // Note: cmd_check/cmd_eval/cmd_fmt call process::exit() and cannot be tested
    // as unit tests. The logic is tested via direct evaluator calls below.

    #[test]
    fn test_cxl_eval_simple_expr_via_evaluator() {
        // Test the evaluation path directly without process::exit
        let source = "emit result = 1 + 2";
        let parsed = cxl::parser::Parser::parse(source);
        assert!(parsed.errors.is_empty());

        let resolved = cxl::resolve::resolve_program(parsed.ast, &[], parsed.node_count).unwrap();
        let typed = cxl::typecheck::type_check(resolved, &HashMap::new()).unwrap();

        let ctx = EvalContext {
            clock: Box::new(WallClock),
            pipeline_start_time: chrono::Utc::now().naive_utc(),
            pipeline_name: "test".into(),
            pipeline_execution_id: "00000000-0000-0000-0000-000000000000".into(),
            pipeline_batch_id: "00000000-0000-0000-0000-000000000000".into(),
            pipeline_counters: clinker_record::PipelineCounters::default(),
            source_file: std::sync::Arc::from("test"),
            source_row: 1,
            pipeline_vars: indexmap::IndexMap::new(),
        };

        let resolver = HashMapResolver::new(HashMap::new());
        let output = cxl::eval::eval_program::<NullStorage>(&typed, &ctx, &resolver, None).unwrap();
        assert_eq!(output.get("result"), Some(&Value::Integer(3)));
    }

    #[test]
    fn test_cxl_eval_with_fields_via_evaluator() {
        let source = "emit total = Price * Qty";
        let fields: HashMap<String, Value> = [
            ("Price".to_string(), Value::Float(10.5)),
            ("Qty".to_string(), Value::Integer(3)),
        ]
        .into();
        let field_refs: Vec<&str> = fields.keys().map(|s| s.as_str()).collect();

        let parsed = cxl::parser::Parser::parse(source);
        assert!(parsed.errors.is_empty());

        let resolved =
            cxl::resolve::resolve_program(parsed.ast, &field_refs, parsed.node_count).unwrap();
        let typed = cxl::typecheck::type_check(resolved, &HashMap::new()).unwrap();

        let ctx = EvalContext {
            clock: Box::new(WallClock),
            pipeline_start_time: chrono::Utc::now().naive_utc(),
            pipeline_name: "test".into(),
            pipeline_execution_id: "00000000-0000-0000-0000-000000000000".into(),
            pipeline_batch_id: "00000000-0000-0000-0000-000000000000".into(),
            pipeline_counters: clinker_record::PipelineCounters::default(),
            source_file: std::sync::Arc::from("test"),
            source_row: 1,
            pipeline_vars: indexmap::IndexMap::new(),
        };

        let resolver = HashMapResolver::new(fields);
        let output = cxl::eval::eval_program::<NullStorage>(&typed, &ctx, &resolver, None).unwrap();
        assert_eq!(output.get("total"), Some(&Value::Float(31.5)));
    }

    #[test]
    fn test_cxl_fmt_canonical_via_parser() {
        let source = "emit   x  =   1 +  2";
        let parsed = cxl::parser::Parser::parse(source);
        assert!(parsed.errors.is_empty());
        // format_statement should normalize whitespace
        let formatted = format_statement(&parsed.ast.statements[0]);
        assert!(formatted.contains("emit x = 1 + 2"));
    }
}
