//! `clinker refactor rename-node` — rename a base pipeline node and propagate
//! the rename to every group/channel overlay reference.
//!
//! # Why this exists
//!
//! The channel/group overlay op model addresses base nodes *by name*: an op's
//! `target`, an `add`'s splice anchors (`after`/`before`), a `remove`'s
//! `rewire` map, an injected `alias`, and `config` dotted-path keys
//! (`alias.param`) all reference a base node's name. Renaming a base node
//! therefore breaks every overlay that referenced the old name. This command
//! tames that coupling: it rewrites the base node plus every reference across
//! the workspace's channel/group files in one operation, with a `--dry-run`
//! preview and `channels lint` as the re-validation backstop.
//!
//! # Reference surfaces rewritten
//!
//! - **Base pipeline** (`nodes:`): the renamed node's `name`; every consumer's
//!   `input:` / `inputs:` / `body:`/`header:`/`trailer:` reference; a Combine's
//!   named-input map (qualifier key and/or upstream value); and — when a
//!   Combine draws from the renamed node under a same-named qualifier — the
//!   Combine's `where:`/`cxl:` CXL bodies, rewritten via the CXL parser (not a
//!   blind string replace) so only true source qualifiers are touched.
//! - **Base composition** (`transformations:`): the renamed transformation's
//!   `name` (composition stages reference columns, not node names, so there is
//!   no intra-file reference propagation).
//! - **Group / channel-manifest / per-target overlay files**: op `target`,
//!   `after`, `before`, `alias`, explicit `input`, `rewire` keys+values, an
//!   inline `node`, a `set config.cxl` value's CXL, and top-level `config`
//!   dotted-path prefixes (`old.param` → `new.param`).
//!
//! # Formatting
//!
//! Files are rewritten by parsing to a YAML value tree, mutating it
//! structurally, and re-serializing through the canonical YAML chokepoint.
//! Key order is preserved; comments and incidental scalar styling are
//! normalized. `--dry-run` prints the exact on-disk diff so this is visible
//! before anything is written.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use clinker_plan::config::ClinkerToml;

use crate::{LintArgs, RenameNodeArgs, run_channels_lint};

/// Outcome of the CLI command, mapped to a process exit code by the caller.
type CmdResult = Result<u8, Box<dyn std::error::Error>>;

/// `clinker refactor rename-node <target> <old> <new>`.
pub fn run_rename_node(args: &RenameNodeArgs) -> CmdResult {
    let old = args.old.as_str();
    let new = args.new.as_str();

    if old == new {
        return Err(format!("old and new names are identical (`{old}`); nothing to rename").into());
    }
    validate_node_name(new)?;

    let workspace_root = args
        .base_dir
        .canonicalize()
        .map_err(|e| format!("workspace root {}: {e}", args.base_dir.display()))?;
    let clinker_toml = ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(|e| format!("clinker.toml: {e}"))?;

    // ---- Base target file: parse, guard, rewrite. ----
    let base_text = std::fs::read_to_string(&args.target)
        .map_err(|e| format!("target {}: {e}", args.target.display()))?;
    let base_dom: Value = clinker_plan::yaml::from_str(&base_text)
        .map_err(|e| format!("target {}: {e}", args.target.display()))?;

    let names =
        base_node_names(&base_dom).map_err(|e| format!("target {}: {e}", args.target.display()))?;
    if !names.contains(old) {
        return Err(format!(
            "target {} has no node named `{old}` (found: {})",
            args.target.display(),
            comma_join(&names)
        )
        .into());
    }
    if names.contains(new) {
        return Err(format!(
            "cannot rename `{old}` to `{new}`: target {} already has a node named `{new}`",
            args.target.display()
        )
        .into());
    }

    let mut warnings: Vec<String> = Vec::new();
    let mut planned: Vec<PlannedEdit> = Vec::new();

    // Base file: node/transformation rewrite plus any inline overlay surfaces
    // (a pipeline may carry its own pipeline-default `overrides:`/`config:`).
    {
        let mut dom = base_dom.clone();
        let mut changed = rename_base_nodes(&mut dom, old, new)?;
        changed |= rename_overlay_surfaces(&mut dom, old, new)?;
        if changed {
            let rendered = render_yaml(&dom)?;
            planned.push(PlannedEdit::new(&args.target, base_text.clone(), rendered));
        }
    }

    // ---- Overlay/group/manifest files: propagate references. ----
    // A per-target overlay names its target, so it is only rewritten when it
    // overlays *this* pipeline — otherwise a different pipeline that happens to
    // share the node name `old` would be corrupted. Channel-wide manifest
    // overrides and group files are target-agnostic (they apply to whatever
    // pipeline a channel runs), so their references to `old` are rewritten
    // wherever they appear, with `channels lint` as the backstop.
    let target_stem = file_stem_of(&args.target);
    let overlay_files = collect_overlay_files(&workspace_root, &clinker_toml)?;
    for candidate in overlay_files {
        // The base target is never also an overlay file, but dedupe defensively.
        if same_file(&candidate.path, &args.target) {
            continue;
        }
        let text = match std::fs::read_to_string(&candidate.path) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let mut dom: Value = match clinker_plan::yaml::from_str(&text) {
            Ok(d) => d,
            Err(e) => {
                warnings.push(format!(
                    "skipped {} (parse error: {e})",
                    candidate.path.display()
                ));
                continue;
            }
        };
        if candidate.kind == OverlayFileKind::PerTarget
            && overlay_target_stem(&dom).as_deref() != Some(target_stem.as_str())
        {
            // This overlay targets a different pipeline; leave it untouched.
            continue;
        }
        if rename_overlay_surfaces(&mut dom, old, new)? {
            let rendered = render_yaml(&dom)?;
            planned.push(PlannedEdit::new(&candidate.path, text, rendered));
        }
    }

    // ---- Report / apply. ----
    for w in &warnings {
        eprintln!("warning: {w}");
    }

    if planned.is_empty() {
        // old exists in the base, so the base should always change; reaching
        // here means the rename was a structural no-op (guarded above).
        println!("rename-node: no changes produced for `{old}` -> `{new}`");
        return Ok(0);
    }

    if args.dry_run {
        println!(
            "note: files are rewritten by re-serializing their YAML; key order is preserved, \
             but comments and incidental scalar styling are normalized."
        );
        for edit in &planned {
            let rel = display_path(&edit.path, &workspace_root);
            println!("--- would modify: {rel} ---");
            print!("{}", unified_diff(&edit.before, &edit.after, &rel));
        }
        println!(
            "\nrename-node --dry-run: {} file(s) would change; nothing written",
            planned.len()
        );
        return Ok(0);
    }

    for edit in &planned {
        std::fs::write(&edit.path, &edit.after)
            .map_err(|e| format!("writing {}: {e}", edit.path.display()))?;
        println!("modified: {}", display_path(&edit.path, &workspace_root));
    }
    println!(
        "rename-node: renamed `{old}` -> `{new}` across {} file(s)",
        planned.len()
    );

    // Re-lint: compile every (target × overlay) so an incomplete rename fails
    // loudly rather than at a later run.
    println!("\nre-linting workspace...");
    let lint = run_channels_lint(&LintArgs {
        base_dir: args.base_dir.clone(),
    })?;
    if lint != 0 {
        eprintln!(
            "rename-node: `channels lint` reported failures after the rename; \
             review the changes above"
        );
    }
    Ok(lint)
}

/// A file rewrite that will (or would) be applied.
struct PlannedEdit {
    path: PathBuf,
    before: String,
    after: String,
}

impl PlannedEdit {
    fn new(path: &Path, before: String, after: String) -> Self {
        Self {
            path: path.to_path_buf(),
            before,
            after,
        }
    }
}

// ---------------------------------------------------------------------
// Name validation / discovery
// ---------------------------------------------------------------------

/// A node name is an identifier: non-empty, no dots (dots delimit `node.port`
/// and `alias.param`), and restricted to `[A-Za-z0-9_]` so it can never need
/// quoting or collide with CXL/path syntax.
fn validate_node_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("new node name is empty".to_string());
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!(
            "invalid new node name `{name}`: node names may only contain letters, digits, and `_`"
        ));
    }
    Ok(())
}

/// The set of node names declared by a base file — `nodes:` for a pipeline or
/// `transformations:` for a composition.
fn base_node_names(dom: &Value) -> Result<BTreeSet<String>, String> {
    let obj = dom.as_object().ok_or("file is not a YAML mapping")?;
    let list = obj
        .get("nodes")
        .or_else(|| obj.get("transformations"))
        .and_then(|v| v.as_array())
        .ok_or("file has neither a `nodes:` nor a `transformations:` list")?;
    Ok(list
        .iter()
        .filter_map(|n| n.get("name").and_then(|v| v.as_str()).map(str::to_string))
        .collect())
}

// ---------------------------------------------------------------------
// Base-file node rewrite
// ---------------------------------------------------------------------

/// Rewrite the base file's node list. Pipelines (`nodes:`) get the full
/// reference rewrite; compositions (`transformations:`) get a name-only rewrite
/// (stages reference columns, not names).
fn rename_base_nodes(dom: &mut Value, old: &str, new: &str) -> Result<bool, String> {
    let obj = dom.as_object_mut().ok_or("file is not a YAML mapping")?;
    if let Some(nodes) = obj.get_mut("nodes").and_then(|v| v.as_array_mut()) {
        let mut changed = false;
        for node in nodes.iter_mut() {
            changed |= rename_in_node(node, old, new)?;
        }
        Ok(changed)
    } else if let Some(list) = obj
        .get_mut("transformations")
        .and_then(|v| v.as_array_mut())
    {
        let mut changed = false;
        for t in list.iter_mut() {
            if let Some(o) = t.as_object_mut()
                && o.get("name").and_then(|v| v.as_str()) == Some(old)
            {
                o.insert("name".to_string(), Value::String(new.to_string()));
                changed = true;
            }
        }
        Ok(changed)
    } else {
        // A pipeline may be overrides-only; treat "no node list" as no node
        // rename (overlay-surface rewrite still runs on the same dom).
        Ok(false)
    }
}

/// Rewrite one node object: its own name, its upstream input references, and —
/// for a Combine drawing from the renamed node under a same-named qualifier —
/// its CXL bodies. A CXL body that must be rewritten but fails to parse is a
/// hard error (the caller aborts before writing) so a node is never left half
/// rewritten.
fn rename_in_node(node: &mut Value, old: &str, new: &str) -> Result<bool, String> {
    let Some(obj) = node.as_object_mut() else {
        return Ok(false);
    };
    let mut changed = false;

    // The node's own identity.
    if obj.get("name").and_then(|v| v.as_str()) == Some(old) {
        obj.insert("name".to_string(), Value::String(new.to_string()));
        changed = true;
    }

    // Whether a Combine qualifier equal to `old` (drawing from the renamed
    // node) was renamed — if so, its CXL bodies must follow.
    let mut combine_qualifier_renamed = false;

    if let Some(input) = obj.get_mut("input") {
        match input {
            // Single-input consumer (`input: node` / `input: node.port`).
            Value::String(s) => {
                if let Some(rewritten) = rewrite_ref(s, old, new) {
                    *s = rewritten;
                    changed = true;
                }
            }
            // Combine named-input map (`input: { qualifier: upstream }`).
            Value::Object(map) => {
                let (new_map, map_changed, qual_renamed) = rewrite_combine_inputs(map, old, new)?;
                if map_changed {
                    *input = Value::Object(new_map);
                    changed = true;
                }
                combine_qualifier_renamed = qual_renamed;
            }
            _ => {}
        }
    }

    // Merge inputs list.
    if let Some(Value::Array(list)) = obj.get_mut("inputs") {
        for elem in list.iter_mut() {
            if let Value::String(s) = elem
                && let Some(rewritten) = rewrite_ref(s, old, new)
            {
                *s = rewritten;
                changed = true;
            }
        }
    }

    // Envelope ports.
    for port in ["body", "header", "trailer"] {
        if let Some(Value::String(s)) = obj.get_mut(port)
            && let Some(rewritten) = rewrite_ref(s, old, new)
        {
            *s = rewritten;
            changed = true;
        }
    }

    // Combine CXL: only when a same-named qualifier was renamed (so `old.*`
    // references in the body are true source qualifiers of the renamed node).
    if combine_qualifier_renamed && let Some(Value::Object(cfg)) = obj.get_mut("config") {
        // `drive:` names an input qualifier.
        if cfg.get("drive").and_then(|v| v.as_str()) == Some(old) {
            cfg.insert("drive".to_string(), Value::String(new.to_string()));
            changed = true;
        }
        changed |= rewrite_config_cxl(cfg, "where", old, new, true)?;
        changed |= rewrite_config_cxl(cfg, "cxl", old, new, false)?;
    }

    Ok(changed)
}

/// Rewrite a Combine's named-input map. Returns `(new_map, changed,
/// qualifier_renamed)`. A map entry's upstream *value* is rewritten whenever it
/// references the renamed node; its *qualifier key* is renamed only when the
/// key equals `old` and it draws from the renamed node (`{ old: old }`), the
/// convention where the qualifier mirrors the source it reads. Renaming a
/// qualifier onto an existing one is a hard error.
fn rewrite_combine_inputs(
    map: &Map<String, Value>,
    old: &str,
    new: &str,
) -> Result<(Map<String, Value>, bool, bool), String> {
    let mut out = Map::new();
    let mut changed = false;
    let mut qualifier_renamed = false;

    for (key, value) in map.iter() {
        let value_refers_old = value.as_str().map(|s| ref_node(s) == old).unwrap_or(false);

        // Rewrite the upstream value reference.
        let new_value = match value.as_str() {
            Some(s) => match rewrite_ref(s, old, new) {
                Some(rewritten) => {
                    changed = true;
                    Value::String(rewritten)
                }
                None => value.clone(),
            },
            None => value.clone(),
        };

        // Rename the qualifier only for the mirroring convention `{ old: old }`.
        let new_key = if key == old && value_refers_old {
            qualifier_renamed = true;
            changed = true;
            new.to_string()
        } else {
            key.clone()
        };

        if out.contains_key(&new_key) {
            return Err(format!(
                "renaming Combine qualifier `{old}` to `{new}` collides with an existing \
                 qualifier `{new}`"
            ));
        }
        out.insert(new_key, new_value);
    }

    Ok((out, changed, qualifier_renamed))
}

/// Rewrite a CXL field (`where` predicate or `cxl` statements) in a config map,
/// replacing source qualifiers equal to `old`. Returns whether it changed. A
/// parse failure is a hard error: this field is only rewritten once the
/// Combine's qualifier has already been renamed, so leaving it stale would
/// produce a self-inconsistent node. The workspace should be lint-clean before a
/// rename, so this only fires on already-broken CXL — which the user fixes
/// first.
fn rewrite_config_cxl(
    cfg: &mut Map<String, Value>,
    field: &str,
    old: &str,
    new: &str,
    is_predicate: bool,
) -> Result<bool, String> {
    let Some(src) = cfg.get(field).and_then(|v| v.as_str()) else {
        return Ok(false);
    };
    match rewrite_cxl(src, old, new, is_predicate) {
        CxlRewrite::Changed(rewritten) => {
            cfg.insert(field.to_string(), Value::String(rewritten));
            Ok(true)
        }
        CxlRewrite::Unchanged => Ok(false),
        CxlRewrite::ParseError(msg) => Err(format!(
            "Combine `{field}:` is not valid CXL, so the rename cannot keep it consistent \
             with the renamed qualifier ({msg}); fix the pipeline first"
        )),
    }
}

// ---------------------------------------------------------------------
// Overlay-surface rewrite (config keys + override ops)
// ---------------------------------------------------------------------

/// Rewrite the overlay surfaces of a file: top-level `config:` dotted-path keys
/// and every `overrides:` op. Shared by group files, channel manifests,
/// per-target overlays, and a pipeline's own inline overlay surfaces.
fn rename_overlay_surfaces(dom: &mut Value, old: &str, new: &str) -> Result<bool, String> {
    let Some(obj) = dom.as_object_mut() else {
        return Ok(false);
    };
    let mut changed = false;

    if let Some(Value::Object(cfg)) = obj.get_mut("config") {
        changed |= rename_config_keys(cfg, old, new)?;
    }

    if let Some(Value::Array(ops)) = obj.get_mut("overrides") {
        for op in ops.iter_mut() {
            changed |= rename_in_op(op, old, new)?;
        }
    }

    Ok(changed)
}

/// Rewrite `alias.param` config keys whose alias equals `old`, preserving
/// insertion order. A dot-free key is a pipeline-level parameter (not a node
/// alias) and is left untouched. Renaming a key onto one that already exists
/// (e.g. `config` carries both `old.param` and `new.param`) is a hard error
/// rather than a silent overwrite.
fn rename_config_keys(cfg: &mut Map<String, Value>, old: &str, new: &str) -> Result<bool, String> {
    let mut out = Map::new();
    let mut changed = false;
    for (key, value) in cfg.iter() {
        let new_key = match key.split_once('.') {
            Some((alias, rest)) if alias == old => {
                changed = true;
                format!("{new}.{rest}")
            }
            _ => key.clone(),
        };
        if out.contains_key(&new_key) {
            return Err(format!(
                "renaming config key `{key}` to `{new_key}` collides with an existing key"
            ));
        }
        out.insert(new_key, value.clone());
    }
    if changed {
        *cfg = out;
    }
    Ok(changed)
}

/// Rewrite one override op object.
fn rename_in_op(op: &mut Value, old: &str, new: &str) -> Result<bool, String> {
    let Some(obj) = op.as_object_mut() else {
        return Ok(false);
    };
    let mut changed = false;

    // Scalar node-name fields: op target, splice anchors, injected alias.
    for field in ["target", "after", "before", "alias"] {
        if let Some(Value::String(s)) = obj.get_mut(field)
            && let Some(rewritten) = rewrite_ref(s, old, new)
        {
            *s = rewritten;
            changed = true;
        }
    }

    // Explicit op-level `input:` wiring.
    if let Some(Value::String(s)) = obj.get_mut("input")
        && let Some(rewritten) = rewrite_ref(s, old, new)
    {
        *s = rewritten;
        changed = true;
    }

    // `rewire:` map — keys are `<consumer>.input`, values are upstream refs.
    if let Some(Value::Object(rewire)) = obj.get_mut("rewire") {
        let mut out = Map::new();
        for (key, value) in rewire.iter() {
            let new_key = match key.split_once('.') {
                Some((node, rest)) if node == old => {
                    changed = true;
                    format!("{new}.{rest}")
                }
                _ => key.clone(),
            };
            let new_value = match value.as_str().and_then(|s| rewrite_ref(s, old, new)) {
                Some(rewritten) => {
                    changed = true;
                    Value::String(rewritten)
                }
                None => value.clone(),
            };
            if out.contains_key(&new_key) {
                return Err(format!(
                    "renaming rewire key `{key}` to `{new_key}` collides with an existing key"
                ));
            }
            out.insert(new_key, new_value);
        }
        *rewire = out;
    }

    // Inline `node:` (an `add`/`replace` payload) — a full node definition.
    if let Some(node) = obj.get_mut("node") {
        changed |= rename_in_node(node, old, new)?;
    }

    // `set config.cxl` — the value is a CXL program. The overlay op engine only
    // makes `config.cxl` settable, so that is the only CXL-bearing `set` field.
    // A value that must be rewritten but does not parse is a hard error (the
    // caller aborts before writing).
    if obj.get("field").and_then(|v| v.as_str()) == Some("config.cxl")
        && let Some(src) = obj.get("value").and_then(|v| v.as_str())
    {
        match rewrite_cxl(src, old, new, false) {
            CxlRewrite::Changed(rewritten) => {
                obj.insert("value".to_string(), Value::String(rewritten));
                changed = true;
            }
            CxlRewrite::Unchanged => {}
            CxlRewrite::ParseError(msg) => {
                return Err(format!(
                    "`set config.cxl` value is not valid CXL, so the rename cannot rewrite it \
                     ({msg}); fix the overlay first"
                ));
            }
        }
    }

    Ok(changed)
}

// ---------------------------------------------------------------------
// Node-reference rewriting
// ---------------------------------------------------------------------

/// The node portion of a `node` / `node.port` reference.
fn ref_node(s: &str) -> &str {
    s.split_once('.').map(|(n, _)| n).unwrap_or(s)
}

/// Rewrite a `node` / `node.port` reference when its node portion equals `old`.
/// Returns `None` when it does not refer to `old` (leave untouched).
fn rewrite_ref(s: &str, old: &str, new: &str) -> Option<String> {
    match s.split_once('.') {
        Some((node, port)) if node == old => Some(format!("{new}.{port}")),
        Some(_) => None,
        None if s == old => Some(new.to_string()),
        None => None,
    }
}

// ---------------------------------------------------------------------
// CXL-aware rewriting
// ---------------------------------------------------------------------

/// Result of a CXL rewrite pass.
enum CxlRewrite {
    Unchanged,
    Changed(String),
    ParseError(String),
}

/// Rewrite source qualifiers equal to `old` in a CXL source string. Uses the
/// CXL parser so only true source qualifiers (`old.field`) are rewritten — a
/// method receiver (`old.trim()`) parses to a `MethodCall` and is left alone.
///
/// `is_predicate` wraps a bare boolean (a Combine `where:`) as a `filter`
/// statement so the parser accepts it, then maps spans back to the original
/// source.
fn rewrite_cxl(src: &str, old: &str, new: &str, is_predicate: bool) -> CxlRewrite {
    const FILTER_PREFIX: &str = "filter ";
    let (parse_src, offset) = if is_predicate {
        (format!("{FILTER_PREFIX}{src}"), FILTER_PREFIX.len())
    } else {
        (src.to_string(), 0)
    };

    let result = cxl::parser::Parser::parse(&parse_src);
    if !result.errors.is_empty() {
        let msg = result
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
            .join("; ");
        return CxlRewrite::ParseError(msg);
    }

    // Collect qualifier byte ranges (into `src`) for source qualifiers == old.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for stmt in &result.ast.statements {
        collect_qualifier_ranges_stmt(stmt, old, offset, src.len(), &mut ranges);
    }
    if ranges.is_empty() {
        return CxlRewrite::Unchanged;
    }

    // Apply descending so earlier byte offsets stay valid.
    ranges.sort_unstable();
    ranges.dedup();
    let mut out = src.to_string();
    for (start, end) in ranges.into_iter().rev() {
        out.replace_range(start..end, new);
    }
    CxlRewrite::Changed(out)
}

/// Recurse a statement, collecting qualifier ranges from every contained
/// expression and nested statement body.
fn collect_qualifier_ranges_stmt(
    stmt: &cxl::ast::Statement,
    old: &str,
    offset: usize,
    src_len: usize,
    out: &mut Vec<(usize, usize)>,
) {
    use cxl::ast::Statement;
    match stmt {
        Statement::Let { expr, .. }
        | Statement::Emit { expr, .. }
        | Statement::ExprStmt { expr, .. }
        | Statement::Filter {
            predicate: expr, ..
        } => collect_qualifier_ranges_expr(expr, old, offset, src_len, out),
        Statement::Trace { guard, message, .. } => {
            if let Some(g) = guard {
                collect_qualifier_ranges_expr(g, old, offset, src_len, out);
            }
            collect_qualifier_ranges_expr(message, old, offset, src_len, out);
        }
        Statement::EmitEach { source, body, .. } | Statement::ExplodeOuter { source, body, .. } => {
            collect_qualifier_ranges_expr(source, old, offset, src_len, out);
            for s in body {
                collect_qualifier_ranges_stmt(s, old, offset, src_len, out);
            }
        }
        Statement::Distinct { .. } | Statement::UseStmt { .. } => {}
    }
}

/// Recurse an expression, collecting the byte range of every source qualifier
/// (`parts[0]` of a `QualifiedFieldRef`) that equals `old`.
fn collect_qualifier_ranges_expr(
    expr: &cxl::ast::Expr,
    old: &str,
    offset: usize,
    src_len: usize,
    out: &mut Vec<(usize, usize)>,
) {
    use cxl::ast::Expr;
    match expr {
        Expr::QualifiedFieldRef { parts, span, .. } => {
            if let Some(first) = parts.first()
                && first.as_ref() == old
            {
                // The qualifier is anchored at the node span's start; identifiers
                // are contiguous ASCII, so its byte length is the string length.
                let start = (span.start as usize).saturating_sub(offset);
                let end = start + first.len();
                if end <= src_len {
                    out.push((start, end));
                }
            }
        }
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            collect_qualifier_ranges_expr(lhs, old, offset, src_len, out);
            collect_qualifier_ranges_expr(rhs, old, offset, src_len, out);
        }
        Expr::Unary { operand, .. } => {
            collect_qualifier_ranges_expr(operand, old, offset, src_len, out)
        }
        Expr::MethodCall { receiver, args, .. } => {
            collect_qualifier_ranges_expr(receiver, old, offset, src_len, out);
            for a in args {
                collect_qualifier_ranges_expr(a, old, offset, src_len, out);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                collect_qualifier_ranges_expr(s, old, offset, src_len, out);
            }
            for arm in arms {
                collect_qualifier_ranges_expr(&arm.pattern, old, offset, src_len, out);
                collect_qualifier_ranges_expr(&arm.body, old, offset, src_len, out);
            }
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_qualifier_ranges_expr(condition, old, offset, src_len, out);
            collect_qualifier_ranges_expr(then_branch, old, offset, src_len, out);
            if let Some(e) = else_branch {
                collect_qualifier_ranges_expr(e, old, offset, src_len, out);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                collect_qualifier_ranges_expr(a, old, offset, src_len, out);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            collect_qualifier_ranges_expr(receiver, old, offset, src_len, out);
            collect_qualifier_ranges_expr(index, old, offset, src_len, out);
        }
        Expr::Closure { body, .. } => {
            collect_qualifier_ranges_expr(body, old, offset, src_len, out)
        }
        // Leaves and system-namespace reads carry no nested source qualifier.
        Expr::Literal { .. }
        | Expr::FieldRef { .. }
        | Expr::PipelineAccess { .. }
        | Expr::VarsAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::RecordAccess { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::DocAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }
}

// ---------------------------------------------------------------------
// File discovery
// ---------------------------------------------------------------------

/// What kind of overlay file a candidate is — decides whether reference
/// rewriting is scoped to the renamed pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverlayFileKind {
    /// A per-target overlay (`<target>.channel.yaml` / `.comp.yaml` / bare
    /// `<target>.yaml`) — names its target, so rewriting is scoped to it.
    PerTarget,
    /// A channel manifest (`channel.cfg.yaml`) — channel-wide, target-agnostic.
    Manifest,
    /// A group definition (`*.group.yaml`) — target-agnostic.
    Group,
}

/// A candidate overlay file plus its kind.
struct OverlayCandidate {
    path: PathBuf,
    kind: OverlayFileKind,
}

/// Every candidate overlay file across the workspace: each channel folder's
/// `channel.cfg.yaml` manifest and per-target overlay files, plus every
/// `*.group.yaml` under the group root.
fn collect_overlay_files(
    workspace_root: &Path,
    clinker_toml: &ClinkerToml,
) -> Result<Vec<OverlayCandidate>, String> {
    let mut files = Vec::new();

    let channels =
        clinker_channel::scan_channels(&clinker_toml.channel, workspace_root).map_err(|diags| {
            let msgs: Vec<String> = diags
                .iter()
                .map(|d| format!("[{}] {}", d.code, d.message))
                .collect();
            format!("channel scan failed: {}", msgs.join("; "))
        })?;
    for channel in &channels {
        let manifest = channel.dir.join(clinker_channel::CHANNEL_MANIFEST_FILE);
        if manifest.is_file() {
            files.push(OverlayCandidate {
                path: manifest,
                kind: OverlayFileKind::Manifest,
            });
        }
        for entry in yaml_files_in(&channel.dir) {
            if entry
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n != clinker_channel::CHANNEL_MANIFEST_FILE)
                .unwrap_or(false)
            {
                files.push(OverlayCandidate {
                    path: entry,
                    kind: OverlayFileKind::PerTarget,
                });
            }
        }
    }

    let group_root = resolve_root(&clinker_toml.group.root, workspace_root);
    let mut group_paths = Vec::new();
    collect_group_files(&group_root, 0, &mut group_paths);
    for path in group_paths {
        files.push(OverlayCandidate {
            path,
            kind: OverlayFileKind::Group,
        });
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));
    files.dedup_by(|a, b| a.path == b.path);
    Ok(files)
}

/// The bare target stem of an overlay's authoritative `channel.target:`, if
/// present — used to scope per-target overlay rewriting to the renamed pipeline.
fn overlay_target_stem(dom: &Value) -> Option<String> {
    dom.get("channel")
        .and_then(|c| c.get("target"))
        .and_then(|t| t.as_str())
        .map(|t| file_stem_of(Path::new(t)))
}

/// The bare file stem of a path: its file name with a `.comp.yaml` or `.yaml`
/// suffix stripped (so `order.channel.yaml` → `order.channel`, and
/// `order.yaml`/`order.comp.yaml` → `order`). A per-target overlay's target
/// path and the base pipeline path reduce to the same stem this way.
fn file_stem_of(path: &Path) -> String {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    name.strip_suffix(".comp.yaml")
        .or_else(|| name.strip_suffix(".yaml"))
        .unwrap_or(name)
        .to_string()
}

/// Non-recursive `*.yaml` listing of a directory (symlinks skipped).
fn yaml_files_in(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e == "yaml")
                .unwrap_or(false)
        {
            out.push(path);
        }
    }
    out
}

/// Bounded recursive walk collecting `*.group.yaml` files under `root`.
fn collect_group_files(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
    const MAX_DEPTH: usize = 16;
    if depth > MAX_DEPTH {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_symlink() {
            continue;
        }
        let path = entry.path();
        if ft.is_dir() {
            collect_group_files(&path, depth + 1, out);
        } else if ft.is_file()
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".group.yaml"))
                .unwrap_or(false)
        {
            out.push(path);
        }
    }
}

/// Resolve a layout root against the workspace root (absolute roots verbatim).
fn resolve_root(root: &Path, workspace_root: &Path) -> PathBuf {
    if root.is_absolute() {
        root.to_path_buf()
    } else {
        workspace_root.join(root)
    }
}

// ---------------------------------------------------------------------
// YAML rendering / paths / diff
// ---------------------------------------------------------------------

/// Serialize a value tree back to YAML through the canonical chokepoint.
fn render_yaml(dom: &Value) -> Result<String, String> {
    clinker_plan::yaml::to_string(dom).map_err(|e| format!("YAML serialization failed: {e}"))
}

/// Whether two paths point at the same file (best-effort via canonicalization).
fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(x), Ok(y)) => x == y,
        _ => a == b,
    }
}

/// Display a path relative to the workspace root when possible.
fn display_path(path: &Path, workspace_root: &Path) -> String {
    let canon = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    canon
        .strip_prefix(workspace_root)
        .unwrap_or(&canon)
        .display()
        .to_string()
}

fn comma_join(names: &BTreeSet<String>) -> String {
    names.iter().cloned().collect::<Vec<_>>().join(", ")
}

/// A minimal line-oriented unified diff (LCS-based). Small config files, so the
/// quadratic table is fine. Emits `-`/`+`/` ` lines with three lines of context.
fn unified_diff(before: &str, after: &str, path: &str) -> String {
    let a: Vec<&str> = before.lines().collect();
    let b: Vec<&str> = after.lines().collect();

    // LCS length table.
    let (n, m) = (a.len(), b.len());
    let mut lcs = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }

    // Backtrack into a linear op list.
    #[derive(PartialEq)]
    enum Op {
        Keep,
        Del,
        Add,
    }
    let mut ops: Vec<(Op, String)> = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < n && j < m {
        if a[i] == b[j] {
            ops.push((Op::Keep, a[i].to_string()));
            i += 1;
            j += 1;
        } else if lcs[i + 1][j] >= lcs[i][j + 1] {
            ops.push((Op::Del, a[i].to_string()));
            i += 1;
        } else {
            ops.push((Op::Add, b[j].to_string()));
            j += 1;
        }
    }
    while i < n {
        ops.push((Op::Del, a[i].to_string()));
        i += 1;
    }
    while j < m {
        ops.push((Op::Add, b[j].to_string()));
        j += 1;
    }

    // Emit changed regions with 3 lines of surrounding context.
    const CTX: usize = 3;
    let mut show = vec![false; ops.len()];
    for (idx, (op, _)) in ops.iter().enumerate() {
        if *op != Op::Keep {
            let lo = idx.saturating_sub(CTX);
            let hi = (idx + CTX + 1).min(ops.len());
            for s in show.iter_mut().take(hi).skip(lo) {
                *s = true;
            }
        }
    }

    let mut buf = format!("diff {path}\n");
    let mut any = false;
    let mut gap = false;
    for (idx, (op, line)) in ops.iter().enumerate() {
        if !show[idx] {
            if !gap && any {
                buf.push_str("  ...\n");
                gap = true;
            }
            continue;
        }
        gap = false;
        any = true;
        let sign = match op {
            Op::Keep => ' ',
            Op::Del => '-',
            Op::Add => '+',
        };
        buf.push(sign);
        buf.push(' ');
        buf.push_str(line);
        buf.push('\n');
    }
    buf
}

#[cfg(test)]
mod tests;
