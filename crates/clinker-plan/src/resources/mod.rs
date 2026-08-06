//! Typed workspace resources and planning-time CXL module closure.
//!
//! Resource identities are logical and kind-scoped. Filesystem paths are an
//! implementation detail: they are canonicalized once against the workspace
//! root and never included in user-facing diagnostics.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::num::NonZeroU32;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crate::config::composition::{CompositionFile, CompositionSymbolTable};
use crate::config::{CxlBearingField, CxlFieldScope, PipelineNode};
use crate::yaml::Spanned;
use clinker_core_types::span::FileId;
use cxl::ast::Module;
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_MODULE_BYTES: usize = 1_048_576;
pub const DEFAULT_MAX_MODULES: usize = 64;
pub const DEFAULT_MAX_IMPORT_DEPTH: usize = 32;
pub const DEFAULT_MAX_CLOSURE_BYTES: usize = 16 * 1_048_576;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    #[serde(default)]
    pub rules_root: Option<PathBuf>,
    #[serde(default)]
    pub rules: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub schemas: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub compositions: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub pipelines: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub channels: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CatalogResourceKind {
    Rule,
    Schema,
    Composition,
    Pipeline,
    Channel,
}

impl CatalogResourceKind {
    fn label(self) -> &'static str {
        match self {
            Self::Rule => "rule",
            Self::Schema => "schema",
            Self::Composition => "composition",
            Self::Pipeline => "pipeline",
            Self::Channel => "channel",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogicalResourceId(String);

impl LogicalResourceId {
    pub fn parse(value: &str) -> Result<Self, ResourceError> {
        let valid = !value.is_empty()
            && value.split('.').all(|part| {
                !part.is_empty()
                    && part != "."
                    && part != ".."
                    && part
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            });
        if !valid {
            return Err(ResourceError::new(format!(
                "invalid logical resource identity `{value}`; use dot-separated names such as `shared.dates`"
            )));
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LogicalResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceError {
    message: String,
}

impl ResourceError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ResourceError {}

#[derive(Debug, Clone)]
pub struct WorkspaceCatalog {
    workspace_root: PathBuf,
    rules_root: Option<PathBuf>,
    entries: HashMap<(CatalogResourceKind, LogicalResourceId), PathBuf>,
}

impl WorkspaceCatalog {
    pub fn load(workspace_root: &Path, config: &CatalogConfig) -> Result<Self, ResourceError> {
        let workspace_root = workspace_root
            .canonicalize()
            .map_err(|error| ResourceError::new(format!("cannot open workspace root: {error}")))?;
        let mut entries = HashMap::new();
        let mut physical_ids: HashMap<PathBuf, (CatalogResourceKind, LogicalResourceId)> =
            HashMap::new();

        for (kind, configured) in [
            (CatalogResourceKind::Rule, &config.rules),
            (CatalogResourceKind::Schema, &config.schemas),
            (CatalogResourceKind::Composition, &config.compositions),
            (CatalogResourceKind::Pipeline, &config.pipelines),
            (CatalogResourceKind::Channel, &config.channels),
        ] {
            for (raw_id, raw_path) in configured {
                let id = LogicalResourceId::parse(raw_id)?;
                let target = canonical_workspace_target(&workspace_root, raw_path, kind, &id)?;
                if let Some((previous_kind, previous_id)) = physical_ids.get(&target) {
                    return Err(ResourceError::new(format!(
                        "catalog identities `{}` ({}) and `{}` ({}) resolve to the same canonical target",
                        previous_id,
                        previous_kind.label(),
                        id,
                        kind.label()
                    )));
                }
                physical_ids.insert(target.clone(), (kind, id.clone()));
                entries.insert((kind, id), target);
            }
        }

        Ok(Self {
            workspace_root,
            rules_root: config.rules_root.clone(),
            entries,
        })
    }

    pub fn resolve(
        &self,
        kind: CatalogResourceKind,
        id: &LogicalResourceId,
    ) -> Result<&Path, ResourceError> {
        self.entries
            .get(&(kind, id.clone()))
            .map(PathBuf::as_path)
            .ok_or_else(|| {
                ResourceError::new(format!(
                    "unknown {} resource `{id}`; add it to `[catalog.{}s]`",
                    kind.label(),
                    kind.label()
                ))
            })
    }

    /// Return the logical identity registered for a canonical resource path.
    ///
    /// Paths not represented in the catalog return `None`; callers may still
    /// retain their canonical path for closure checks without inventing a
    /// filename-derived identity.
    pub fn identify(
        &self,
        kind: CatalogResourceKind,
        canonical_path: &Path,
    ) -> Option<&LogicalResourceId> {
        self.entries.iter().find_map(|((entry_kind, id), path)| {
            (*entry_kind == kind && path == canonical_path).then_some(id)
        })
    }

    pub fn select_rules_root(
        &self,
        cli: Option<&Path>,
        pipeline: Option<&Path>,
    ) -> Result<ResolvedRulesRoot, ResourceError> {
        let (raw, origin) = if let Some(path) = cli {
            (path, RulesRootOrigin::Cli)
        } else if let Some(path) = pipeline {
            (path, RulesRootOrigin::Pipeline)
        } else if let Some(path) = self.rules_root.as_deref() {
            (path, RulesRootOrigin::Catalog)
        } else {
            (Path::new("rules"), RulesRootOrigin::Default)
        };
        let path = canonical_rules_root(&self.workspace_root, raw)?;
        Ok(ResolvedRulesRoot { path, origin })
    }

    fn rule_path(
        &self,
        id: &LogicalResourceId,
        root: &ResolvedRulesRoot,
    ) -> Result<PathBuf, ResourceError> {
        if let Some(path) = self.entries.get(&(CatalogResourceKind::Rule, id.clone())) {
            return Ok(path.clone());
        }
        let mut path = root.path.clone();
        for part in id.as_str().split('.') {
            path.push(part);
        }
        path.set_extension("cxl");
        canonical_workspace_target(&self.workspace_root, &path, CatalogResourceKind::Rule, id)
    }
}

fn has_parent_component(path: &Path) -> bool {
    path.components()
        .any(|part| matches!(part, Component::ParentDir))
}

fn canonical_workspace_target(
    workspace_root: &Path,
    raw: &Path,
    kind: CatalogResourceKind,
    id: &LogicalResourceId,
) -> Result<PathBuf, ResourceError> {
    if has_parent_component(raw) {
        return Err(ResourceError::new(format!(
            "{} resource `{id}` escapes the workspace; use a workspace-relative path without `..`",
            kind.label()
        )));
    }
    let candidate = if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        workspace_root.join(raw)
    };
    let canonical = candidate.canonicalize().map_err(|error| {
        ResourceError::new(format!(
            "{} resource `{id}` cannot be opened: {error}",
            kind.label()
        ))
    })?;
    if !canonical.starts_with(workspace_root) {
        return Err(ResourceError::new(format!(
            "{} resource `{id}` resolves outside the workspace",
            kind.label()
        )));
    }
    Ok(canonical)
}

fn canonical_rules_root(workspace_root: &Path, raw: &Path) -> Result<PathBuf, ResourceError> {
    if has_parent_component(raw) {
        return Err(ResourceError::new(
            "rules root escapes the workspace; use a workspace-relative path without `..`",
        ));
    }
    let candidate = if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        workspace_root.join(raw)
    };
    let canonical = candidate
        .canonicalize()
        .map_err(|error| ResourceError::new(format!("rules root cannot be opened: {error}")))?;
    if !canonical.starts_with(workspace_root) {
        return Err(ResourceError::new(
            "rules root resolves outside the workspace",
        ));
    }
    Ok(canonical)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RulesRootOrigin {
    Cli,
    Pipeline,
    Catalog,
    Default,
}

#[derive(Debug, Clone)]
pub struct ResolvedRulesRoot {
    path: PathBuf,
    origin: RulesRootOrigin,
}

impl ResolvedRulesRoot {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn origin(&self) -> RulesRootOrigin {
        self.origin
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ModuleLimits {
    pub max_module_bytes: usize,
    pub max_modules: usize,
    pub max_import_depth: usize,
    pub max_closure_bytes: usize,
}

impl Default for ModuleLimits {
    fn default() -> Self {
        Self {
            max_module_bytes: DEFAULT_MAX_MODULE_BYTES,
            max_modules: DEFAULT_MAX_MODULES,
            max_import_depth: DEFAULT_MAX_IMPORT_DEPTH,
            max_closure_bytes: DEFAULT_MAX_CLOSURE_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompiledCxlModule {
    pub id: LogicalResourceId,
    pub module: Module,
    pub node_count: u32,
    pub imports: BTreeMap<String, LogicalResourceId>,
    /// BLAKE3 of the exact module source bytes read during planning.
    pub content_digest: [u8; 32],
}

#[derive(Debug, Clone, Default)]
pub struct CompiledModuleRegistry {
    modules: HashMap<LogicalResourceId, Arc<CompiledCxlModule>>,
    program_roots: HashSet<LogicalResourceId>,
    declaration_graph: cxl::module_eval::ResolvedModuleDeclarationGraph,
}

impl CompiledModuleRegistry {
    pub fn get(&self, id: &str) -> Option<&Arc<CompiledCxlModule>> {
        self.modules
            .iter()
            .find_map(|(key, value)| (key.as_str() == id).then_some(value))
    }

    pub fn len(&self) -> usize {
        self.modules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }

    /// Closure-wide constant/function dependency graph retained at admission.
    pub fn declaration_graph(&self) -> &cxl::module_eval::ResolvedModuleDeclarationGraph {
        &self.declaration_graph
    }

    /// Whether a module was imported directly by the pipeline program.
    /// Transitive dependencies remain private to their importing module.
    pub fn is_program_visible(&self, id: &str) -> bool {
        self.program_roots.iter().any(|root| root.as_str() == id)
    }

    /// Deterministic semantic identities for every module in the closure.
    pub(crate) fn semantic_identities(&self) -> Vec<ModuleSemanticIdentity<'_>> {
        let mut modules = self.modules.values().collect::<Vec<_>>();
        modules.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        modules
            .into_iter()
            .map(|module| ModuleSemanticIdentity {
                id: module.id.as_str(),
                content_digest: module.content_digest,
                imports: module
                    .imports
                    .iter()
                    .map(|(alias, dependency)| (alias.as_str(), dependency.as_str()))
                    .collect(),
                program_visible: self.program_roots.contains(&module.id),
            })
            .collect()
    }

    /// Resolver-facing export table keyed by logical module identity.
    pub fn module_exports(&self) -> HashMap<String, cxl::resolve::ModuleExports> {
        self.modules
            .iter()
            .map(|(id, module)| {
                let exports = cxl::resolve::ModuleExports {
                    functions: module
                        .module
                        .functions
                        .iter()
                        .map(|function| function.name.to_string())
                        .collect(),
                    constants: module
                        .module
                        .constants
                        .iter()
                        .map(|constant| constant.name.to_string())
                        .collect(),
                };
                (id.to_string(), exports)
            })
            .collect()
    }

    /// Evaluator-facing declaration registry with no filesystem handles.
    pub fn runtime_modules(&self) -> Arc<cxl::module_eval::RuntimeModuleRegistry> {
        let mut registry = cxl::module_eval::RuntimeModuleRegistry::default();
        for (id, module) in &self.modules {
            registry.insert(
                id.to_string(),
                module.module.clone(),
                module
                    .imports
                    .iter()
                    .map(|(alias, target)| (alias.clone(), target.to_string()))
                    .collect(),
            );
        }
        Arc::new(registry)
    }
}

/// Path-independent semantic identity of one admitted CXL module.
pub(crate) struct ModuleSemanticIdentity<'a> {
    pub(crate) id: &'a str,
    pub(crate) content_digest: [u8; 32],
    pub(crate) imports: Vec<(&'a str, &'a str)>,
    pub(crate) program_visible: bool,
}

/// Collect direct `use` declarations from typed executable CXL fields.
///
/// The input type deliberately cannot represent ordinary config strings.
/// Imports retain first-authored order while duplicate roots are admitted
/// only once.
pub fn collect_direct_imports(
    fields: &[CxlBearingField],
) -> Result<Vec<LogicalResourceId>, ResourceError> {
    let mut imports = Vec::new();
    let mut seen = HashSet::new();
    let mut parse_errors = Vec::new();
    for field in fields {
        let parsed = cxl::parser::Parser::parse(&field.source);
        for error in &parsed.errors {
            parse_errors.push(format!(
                "{} node `{}` field `{}` at byte {}: {} (correct the CXL in this field)",
                field.scope.label(),
                field.node_name,
                field.surface,
                field.authored_span.byte_offset().unwrap_or(0),
                error.message
            ));
        }
        for statement in parsed.ast.statements {
            if let cxl::ast::Statement::UseStmt { path, .. } = statement {
                let logical = path
                    .iter()
                    .map(AsRef::as_ref)
                    .collect::<Vec<&str>>()
                    .join(".");
                let id = LogicalResourceId::parse(&logical)?;
                if seen.insert(id.clone()) {
                    imports.push(id);
                }
            }
        }
    }
    if !parse_errors.is_empty() {
        return Err(ResourceError::new(format!(
            "module-root CXL admission failed: {}",
            parse_errors.join("; ")
        )));
    }
    Ok(imports)
}

/// Collect typed CXL fields from the top-level pipeline and its complete,
/// bounded reachable composition-body closure.
///
/// Bodies are loaded through the same workspace signature catalog and
/// relative `use:` resolution used by schema binding. Each canonical body is
/// visited once; cycles, missing bodies, parse failures, and depth overflow
/// fail before module admission.
pub fn collect_cxl_fields_with_compositions(
    nodes: &[Spanned<PipelineNode>],
    workspace_root: &Path,
    pipeline_dir: &Path,
) -> Result<Vec<CxlBearingField>, ResourceError> {
    collect_cxl_fields_with_composition_identities(nodes, workspace_root, pipeline_dir)
        .map(|discovery| discovery.fields)
}

/// CXL-bearing fields and the exact composition-body snapshot that produced
/// them during bounded closure discovery.
#[derive(Debug)]
pub struct CompositionDiscovery {
    pub fields: Vec<CxlBearingField>,
    pub identities: HashMap<PathBuf, [u8; 32]>,
}

/// Collect CXL fields and retain a content identity for every composition body
/// read during closure discovery. The caller passes those identities into the
/// compile context so schema binding can reject a pathname replacement instead
/// of compiling a different filesystem snapshot.
pub fn collect_cxl_fields_with_composition_identities(
    nodes: &[Spanned<PipelineNode>],
    workspace_root: &Path,
    pipeline_dir: &Path,
) -> Result<CompositionDiscovery, ResourceError> {
    let mut fields = Vec::new();
    let mut body_identities = HashMap::new();
    visit_node_fields(nodes, CxlFieldScope::TopLevel, &mut fields);
    if !nodes
        .iter()
        .any(|node| matches!(node.value, PipelineNode::Composition { .. }))
    {
        return Ok(CompositionDiscovery {
            fields,
            identities: body_identities,
        });
    }

    let symbol_table = crate::config::composition::scan_workspace_signatures(workspace_root)
        .map_err(|diagnostics| {
            ResourceError::new(format!(
                "composition discovery failed before module admission: {}",
                diagnostics
                    .iter()
                    .map(|diagnostic| diagnostic.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            ))
        })?;
    let mut loader = CompositionCxlLoader {
        workspace_root,
        symbol_table: &symbol_table,
        loaded: HashSet::new(),
        stack: Vec::new(),
        next_file_id: 1,
        fields: &mut fields,
        body_identities: &mut body_identities,
    };
    loader.visit_composition_calls(nodes, pipeline_dir, 0)?;
    Ok(CompositionDiscovery {
        fields,
        identities: body_identities,
    })
}

fn visit_node_fields(
    nodes: &[Spanned<PipelineNode>],
    scope: CxlFieldScope,
    fields: &mut Vec<CxlBearingField>,
) {
    for node in nodes {
        node.value
            .visit_cxl_fields(scope.clone(), node.referenced.span(), &mut |field| {
                fields.push(field)
            });
    }
}

struct CompositionCxlLoader<'a> {
    workspace_root: &'a Path,
    symbol_table: &'a CompositionSymbolTable,
    loaded: HashSet<PathBuf>,
    stack: Vec<PathBuf>,
    next_file_id: u32,
    fields: &'a mut Vec<CxlBearingField>,
    body_identities: &'a mut HashMap<PathBuf, [u8; 32]>,
}

impl CompositionCxlLoader<'_> {
    fn visit_composition_calls(
        &mut self,
        nodes: &[Spanned<PipelineNode>],
        origin_dir: &Path,
        depth: u32,
    ) -> Result<(), ResourceError> {
        for node in nodes {
            let PipelineNode::Composition { header, r#use, .. } = &node.value else {
                continue;
            };
            let resolved = crate::plan::bind_schema::resolve_use_path(
                r#use,
                origin_dir,
                self.workspace_root,
                self.symbol_table,
            );
            self.visit_body(
                &resolved,
                header.name.as_str(),
                node.referenced.span(),
                depth,
            )?;
        }
        Ok(())
    }

    fn visit_body(
        &mut self,
        resolved: &Path,
        call_name: &str,
        call_span: crate::yaml::Span,
        depth: u32,
    ) -> Result<(), ResourceError> {
        if let Some(cycle_start) = self.stack.iter().position(|path| path == resolved) {
            let mut cycle = self.stack[cycle_start..]
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>();
            cycle.push(resolved.display().to_string());
            return Err(ResourceError::new(format!(
                "composition call `{call_name}` at byte {} forms a `use:` cycle: {}; remove one of these calls",
                call_span.byte_offset().unwrap_or(0),
                cycle.join(" -> ")
            )));
        }
        if self.loaded.contains(resolved) {
            return Ok(());
        }
        if depth > crate::plan::bind_schema::MAX_COMPOSITION_DEPTH {
            return Err(ResourceError::new(format!(
                "composition call `{call_name}` at byte {} exceeds the nesting limit of {}; flatten the call chain",
                call_span.byte_offset().unwrap_or(0),
                crate::plan::bind_schema::MAX_COMPOSITION_DEPTH
            )));
        }
        let signature = self.symbol_table.get(resolved).ok_or_else(|| {
            ResourceError::new(format!(
                "composition call `{call_name}` at byte {} cannot load `{}`; correct `use:` to a workspace `.comp.yaml` path",
                call_span.byte_offset().unwrap_or(0),
                resolved.display()
            ))
        })?;
        let source = fs::read(&signature.source_path).map_err(|error| {
            ResourceError::new(format!(
                "composition call `{call_name}` cannot read body `{}`: {error}",
                resolved.display()
            ))
        })?;
        let identity = *blake3::hash(&source).as_bytes();
        let source = std::str::from_utf8(&source).map_err(|error| {
            ResourceError::new(format!(
                "composition call `{call_name}` body `{}` is not UTF-8: {error}",
                resolved.display()
            ))
        })?;
        let file_id = NonZeroU32::new(self.next_file_id)
            .map(FileId::new)
            .ok_or_else(|| ResourceError::new("composition source identity overflowed"))?;
        self.next_file_id = self
            .next_file_id
            .checked_add(1)
            .ok_or_else(|| ResourceError::new("composition source identity overflowed"))?;
        let body = CompositionFile::parse(source, file_id, signature.source_path.clone()).map_err(
            |error| {
                ResourceError::new(format!(
                    "composition call `{call_name}` body `{}` cannot be parsed: {error}",
                    resolved.display()
                ))
            },
        )?;
        self.body_identities
            .insert(resolved.to_path_buf(), identity);

        self.loaded.insert(resolved.to_path_buf());
        self.stack.push(resolved.to_path_buf());
        visit_node_fields(
            &body.nodes,
            CxlFieldScope::CompositionBody {
                composition: resolved.to_path_buf(),
            },
            self.fields,
        );
        let body_origin = resolved.parent().unwrap_or_else(|| Path::new(""));
        let nested = self.visit_composition_calls(&body.nodes, body_origin, depth + 1);
        self.stack.pop();
        nested
    }
}

pub fn compile_module_closure(
    catalog: &WorkspaceCatalog,
    rules_root: &ResolvedRulesRoot,
    roots: &[LogicalResourceId],
    limits: ModuleLimits,
) -> Result<CompiledModuleRegistry, ResourceError> {
    let mut state = ModuleCompileState {
        limits,
        total_bytes: 0,
        discovered: HashSet::new(),
        registry: CompiledModuleRegistry::default(),
    };
    state.registry.program_roots.extend(roots.iter().cloned());
    for id in roots {
        compile_one(catalog, rules_root, id, 0, &mut Vec::new(), &mut state)?;
    }
    state.registry.resolve_declaration_graph()?;
    Ok(state.registry)
}

impl CompiledModuleRegistry {
    fn resolve_declaration_graph(&mut self) -> Result<(), ResourceError> {
        let mut modules = self.modules.values().collect::<Vec<_>>();
        modules.sort_by(|left, right| left.id.cmp(&right.id));
        let import_maps = modules
            .iter()
            .map(|module| {
                module
                    .imports
                    .iter()
                    .map(|(alias, target)| (alias.clone(), target.to_string()))
                    .collect::<HashMap<_, _>>()
            })
            .collect::<Vec<_>>();
        let sources = modules
            .iter()
            .zip(&import_maps)
            .map(
                |(module, imports)| cxl::module_eval::ModuleDeclarationSource {
                    module_id: module.id.as_str(),
                    module: &module.module,
                    imports,
                },
            )
            .collect::<Vec<_>>();
        self.declaration_graph = cxl::module_eval::validate_module_declaration_closure(&sources)
            .map_err(|error| {
                let locations = error
                    .chain
                    .iter()
                    .map(|site| {
                        format!(
                            "{}.{} ({:?})@{}..{}",
                            site.declaration.module,
                            site.declaration.name,
                            site.declaration.kind,
                            site.span.start,
                            site.span.end
                        )
                    })
                    .collect::<Vec<_>>();
                if locations.is_empty() {
                    ResourceError::new(format!(
                        "{} (authored span {}..{})",
                        error.message, error.span.start, error.span.end
                    ))
                } else {
                    ResourceError::new(format!(
                        "{} (authored declaration spans: {})",
                        error.message,
                        locations.join(" -> ")
                    ))
                }
            })?;
        Ok(())
    }
}

struct ModuleCompileState {
    limits: ModuleLimits,
    total_bytes: usize,
    discovered: HashSet<LogicalResourceId>,
    registry: CompiledModuleRegistry,
}

fn compile_one(
    catalog: &WorkspaceCatalog,
    rules_root: &ResolvedRulesRoot,
    id: &LogicalResourceId,
    depth: usize,
    stack: &mut Vec<LogicalResourceId>,
    state: &mut ModuleCompileState,
) -> Result<(), ResourceError> {
    if let Some(cycle_start) = stack.iter().position(|candidate| candidate == id) {
        let mut cycle = stack[cycle_start..]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        cycle.push(id.to_string());
        return Err(ResourceError::new(format!(
            "module import cycle: {}",
            cycle.join(" -> ")
        )));
    }
    if state.registry.modules.contains_key(id) {
        return Ok(());
    }
    if depth > state.limits.max_import_depth {
        return Err(ResourceError::new(format!(
            "module import depth exceeds {} at `{id}`",
            state.limits.max_import_depth
        )));
    }
    if !state.discovered.contains(id) && state.discovered.len() >= state.limits.max_modules {
        return Err(ResourceError::new(format!(
            "module closure exceeds {} unique modules",
            state.limits.max_modules
        )));
    }
    state.discovered.insert(id.clone());
    stack.push(id.clone());
    let path = catalog.rule_path(id, rules_root)?;
    let bytes = fs::read(&path)
        .map_err(|error| ResourceError::new(format!("module `{id}` cannot be read: {error}")))?;
    if bytes.len() > state.limits.max_module_bytes {
        return Err(ResourceError::new(format!(
            "module `{id}` exceeds the {} byte limit",
            state.limits.max_module_bytes
        )));
    }
    state.total_bytes = state
        .total_bytes
        .checked_add(bytes.len())
        .ok_or_else(|| ResourceError::new("module closure byte count overflowed"))?;
    if state.total_bytes > state.limits.max_closure_bytes {
        return Err(ResourceError::new(format!(
            "module closure exceeds the {} byte aggregate limit",
            state.limits.max_closure_bytes
        )));
    }
    let source = std::str::from_utf8(&bytes)
        .map_err(|_| ResourceError::new(format!("module `{id}` is not valid UTF-8")))?;
    let parsed = cxl::parser::Parser::parse_module(source);
    if !parsed.errors.is_empty() {
        let messages = parsed
            .errors
            .iter()
            .map(|error| error.message.as_str())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(ResourceError::new(format!("module `{id}`: {messages}")));
    }
    let mut imports = BTreeMap::new();
    for import in &parsed.module.imports {
        let dependency = LogicalResourceId::parse(
            &import
                .path
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>()
                .join("."),
        )?;
        let alias = import
            .alias
            .as_deref()
            .unwrap_or_else(|| import.path.last().expect("non-empty import path"));
        if imports
            .insert(alias.to_owned(), dependency.clone())
            .is_some()
        {
            return Err(ResourceError::new(format!(
                "module `{id}` has duplicate import alias `{alias}`"
            )));
        }
        compile_one(catalog, rules_root, &dependency, depth + 1, stack, state)?;
    }
    stack.pop();
    state.registry.modules.insert(
        id.clone(),
        Arc::new(CompiledCxlModule {
            id: id.clone(),
            module: parsed.module,
            node_count: parsed.node_count,
            imports,
            content_digest: *blake3::hash(&bytes).as_bytes(),
        }),
    );
    Ok(())
}
