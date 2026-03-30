use std::collections::HashMap;
use std::process;

use clap::{Parser, Subcommand};
use clinker_record::Value;
use cxl::eval::{EvalContext, WallClock};
use cxl::resolve::HashMapResolver;

#[derive(Parser)]
#[command(name = "cxl", about = "CXL language validator, evaluator, and formatter")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Parse, resolve, and type-check a .cxl file. Exit 0 if valid, 1 if errors.
    Check {
        /// Path to the .cxl file
        file: String,
    },
    /// Parse, type-check, and evaluate a .cxl file against a JSON record.
    Eval {
        /// Path to the .cxl file
        file: String,
        /// JSON record to evaluate against
        #[arg(long)]
        record: String,
    },
    /// Parse and pretty-print a .cxl file (canonical formatting).
    Fmt {
        /// Path to the .cxl file
        file: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Check { file } => cmd_check(&file),
        Command::Eval { file, record } => cmd_eval(&file, &record),
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
            eprintln!("error[parse]: {} (at {}:{})", err.message, file, err.span.start);
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
                eprintln!("error[resolve]: {} (at {}:{})", d.message, file, d.span.start);
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
                eprintln!("{}[typecheck]: {} (at {}:{})", level, d.message, file, d.span.start);
                if let Some(help) = &d.help {
                    eprintln!("  help: {}", help);
                }
            }
            let has_errors = diags.iter().any(|d| !d.is_warning);
            process::exit(if has_errors { 1 } else { 0 });
        }
    }
}

fn cmd_eval(file: &str, record_json: &str) {
    let source = match std::fs::read_to_string(file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot read '{}': {}", file, e);
            process::exit(2);
        }
    };

    // Parse the JSON record
    let json_value: serde_json::Value = match serde_json::from_str(record_json) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: invalid JSON record: {}", e);
            process::exit(2);
        }
    };

    let record_map = match json_value {
        serde_json::Value::Object(map) => {
            let mut hm = HashMap::new();
            for (k, v) in map {
                hm.insert(k, json_to_value(v));
            }
            hm
        }
        _ => {
            eprintln!("error: --record must be a JSON object");
            process::exit(2);
        }
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
        pipeline_counters: clinker_record::PipelineCounters::default(),
        source_file: std::sync::Arc::from(file),
        source_row: 1,
        pipeline_vars: indexmap::IndexMap::new(),
    };

    let resolver = HashMapResolver::new(record_map);

    match cxl::eval::eval_program(&typed, &ctx, &resolver, None) {
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
            if let Some(g) = guard { collect_field_refs_expr(g, names); }
            collect_field_refs_expr(message, names);
        }
        _ => {}
    }
}

fn collect_field_refs_expr(expr: &cxl::ast::Expr, names: &mut Vec<String>) {
    match expr {
        cxl::ast::Expr::FieldRef { name, .. } => {
            if &**name != "it" { names.push(name.to_string()); }
        }
        cxl::ast::Expr::Binary { lhs, rhs, .. } | cxl::ast::Expr::Coalesce { lhs, rhs, .. } => {
            collect_field_refs_expr(lhs, names);
            collect_field_refs_expr(rhs, names);
        }
        cxl::ast::Expr::Unary { operand, .. } => collect_field_refs_expr(operand, names),
        cxl::ast::Expr::MethodCall { receiver, args, .. } => {
            collect_field_refs_expr(receiver, names);
            for a in args { collect_field_refs_expr(a, names); }
        }
        cxl::ast::Expr::IfThenElse { condition, then_branch, else_branch, .. } => {
            collect_field_refs_expr(condition, names);
            collect_field_refs_expr(then_branch, names);
            if let Some(eb) = else_branch { collect_field_refs_expr(eb, names); }
        }
        cxl::ast::Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject { collect_field_refs_expr(s, names); }
            for arm in arms {
                collect_field_refs_expr(&arm.pattern, names);
                collect_field_refs_expr(&arm.body, names);
            }
        }
        cxl::ast::Expr::WindowCall { args, .. } => {
            for a in args { collect_field_refs_expr(a, names); }
        }
        _ => {}
    }
}

fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() { Value::Integer(i) }
            else if let Some(f) = n.as_f64() { Value::Float(f) }
            else { Value::Null }
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
        Value::DateTime(dt) => serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S").to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.into_iter().map(value_to_json).collect()),
    }
}

fn format_statement(stmt: &cxl::ast::Statement) -> String {
    match stmt {
        cxl::ast::Statement::Let { name, expr, .. } =>
            format!("let {} = {}", name, format_expr(expr)),
        cxl::ast::Statement::Emit { name, expr, .. } =>
            format!("emit {} = {}", name, format_expr(expr)),
        cxl::ast::Statement::Trace { level, guard, message, .. } => {
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
            let mut s = format!("use {}", path.iter().map(|p| p.to_string()).collect::<Vec<_>>().join("::"));
            if let Some(a) = alias {
                s.push_str(&format!(" as {}", a));
            }
            s
        }
        cxl::ast::Statement::ExprStmt { expr, .. } => format_expr(expr),
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
        cxl::ast::Expr::QualifiedFieldRef { source, field, .. } => format!("{}.{}", source, field),
        cxl::ast::Expr::Now { .. } => "now".into(),
        cxl::ast::Expr::Wildcard { .. } => "_".into(),
        cxl::ast::Expr::PipelineAccess { field, .. } => format!("pipeline.{}", field),
        cxl::ast::Expr::Binary { op, lhs, rhs, .. } => {
            let op_str = match op {
                cxl::ast::BinOp::Add => "+", cxl::ast::BinOp::Sub => "-",
                cxl::ast::BinOp::Mul => "*", cxl::ast::BinOp::Div => "/",
                cxl::ast::BinOp::Mod => "%", cxl::ast::BinOp::Eq => "==",
                cxl::ast::BinOp::Neq => "!=", cxl::ast::BinOp::Gt => ">",
                cxl::ast::BinOp::Lt => "<", cxl::ast::BinOp::Gte => ">=",
                cxl::ast::BinOp::Lte => "<=", cxl::ast::BinOp::And => "and",
                cxl::ast::BinOp::Or => "or",
            };
            format!("{} {} {}", format_expr(lhs), op_str, format_expr(rhs))
        }
        cxl::ast::Expr::Unary { op, operand, .. } => {
            let op_str = match op { cxl::ast::UnaryOp::Neg => "-", cxl::ast::UnaryOp::Not => "not " };
            format!("{}{}", op_str, format_expr(operand))
        }
        cxl::ast::Expr::Coalesce { lhs, rhs, .. } => format!("{} ?? {}", format_expr(lhs), format_expr(rhs)),
        cxl::ast::Expr::MethodCall { receiver, method, args, .. } => {
            let args_str = args.iter().map(|a| format_expr(a)).collect::<Vec<_>>().join(", ");
            format!("{}.{}({})", format_expr(receiver), method, args_str)
        }
        cxl::ast::Expr::WindowCall { function, args, .. } => {
            let args_str = args.iter().map(|a| format_expr(a)).collect::<Vec<_>>().join(", ");
            format!("window.{}({})", function, args_str)
        }
        cxl::ast::Expr::IfThenElse { condition, then_branch, else_branch, .. } => {
            let mut s = format!("if {} then {}", format_expr(condition), format_expr(then_branch));
            if let Some(eb) = else_branch { s.push_str(&format!(" else {}", format_expr(eb))); }
            s
        }
        cxl::ast::Expr::Match { subject, arms, .. } => {
            let mut s = "match ".to_string();
            if let Some(sub) = subject { s.push_str(&format!("{} ", format_expr(sub))); }
            s.push_str("{ ");
            for (i, arm) in arms.iter().enumerate() {
                if i > 0 { s.push_str(", "); }
                s.push_str(&format!("{} => {}", format_expr(&arm.pattern), format_expr(&arm.body)));
            }
            s.push_str(" }");
            s
        }
    }
}
