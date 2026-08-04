use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::ast::{Expr, FnDecl, MatchArm, Module, ModuleConst, NodeId};
use crate::lexer::Span;

const RUNTIME_MODULE_NODE: NodeId = NodeId(u32::MAX);

/// Error from module constant evaluation.
#[derive(Debug)]
pub struct ModuleConstError {
    pub span: Span,
    pub message: String,
}

/// Declaration category used in closure-wide module dependency identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ResolvedDeclarationKind {
    Constant,
    Function,
}

/// Stable identity for one declaration in a compiled module closure.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ResolvedDeclarationId {
    pub module: String,
    pub name: String,
    pub kind: ResolvedDeclarationKind,
}

impl ResolvedDeclarationId {
    pub fn label(&self) -> String {
        format!("{}.{}", self.module, self.name)
    }
}

/// A module and its already-resolved, private import aliases.
pub struct ModuleDeclarationSource<'a> {
    pub module_id: &'a str,
    pub module: &'a Module,
    pub imports: &'a HashMap<String, String>,
}

/// One authored declaration site retained in a dependency diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDeclarationSite {
    pub declaration: ResolvedDeclarationId,
    pub span: Span,
}

/// Error produced while resolving the complete module declaration closure.
#[derive(Debug)]
pub struct ResolvedDeclarationError {
    pub span: Span,
    pub message: String,
    pub chain: Vec<ResolvedDeclarationSite>,
}

/// Deterministic dependency graph for every declaration in an admitted closure.
#[derive(Debug, Clone, Default)]
pub struct ResolvedModuleDeclarationGraph {
    dependencies: BTreeMap<ResolvedDeclarationId, Vec<ResolvedDeclarationId>>,
}

impl ResolvedModuleDeclarationGraph {
    pub fn contains(&self, declaration: &ResolvedDeclarationId) -> bool {
        self.dependencies.contains_key(declaration)
    }

    pub fn dependencies(
        &self,
        declaration: &ResolvedDeclarationId,
    ) -> Option<&[ResolvedDeclarationId]> {
        self.dependencies.get(declaration).map(Vec::as_slice)
    }

    pub fn len(&self) -> usize {
        self.dependencies.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dependencies.is_empty()
    }
}

/// Resolve all constant and function dependencies across a compiled module
/// closure, honoring function parameters and closure binders as lexical names.
pub fn validate_module_declaration_closure(
    sources: &[ModuleDeclarationSource<'_>],
) -> Result<ResolvedModuleDeclarationGraph, ResolvedDeclarationError> {
    let mut ordered_sources = sources.iter().collect::<Vec<_>>();
    ordered_sources.sort_by_key(|source| source.module_id);

    let mut module_ids = HashSet::new();
    let mut spans = BTreeMap::new();
    for source in &ordered_sources {
        if !module_ids.insert(source.module_id) {
            return Err(declaration_error(
                source.module.span,
                format!(
                    "module `{}` appears more than once in the closure",
                    source.module_id
                ),
            ));
        }
        for constant in &source.module.constants {
            insert_declaration(
                &mut spans,
                source.module_id,
                &constant.name,
                ResolvedDeclarationKind::Constant,
                constant.span,
            )?;
        }
        for function in &source.module.functions {
            insert_declaration(
                &mut spans,
                source.module_id,
                &function.name,
                ResolvedDeclarationKind::Function,
                function.span,
            )?;
        }
    }

    let mut dependencies = BTreeMap::new();
    for source in ordered_sources {
        for constant in &source.module.constants {
            let id = resolved_id(
                source.module_id,
                &constant.name,
                ResolvedDeclarationKind::Constant,
            );
            let mut resolved = BTreeSet::new();
            collect_declaration_dependencies(
                &constant.expr,
                &id,
                source.imports,
                &spans,
                &HashSet::new(),
                &mut resolved,
            )?;
            dependencies.insert(id, resolved.into_iter().collect());
        }
        for function in &source.module.functions {
            let id = resolved_id(
                source.module_id,
                &function.name,
                ResolvedDeclarationKind::Function,
            );
            let lexical = function
                .params
                .iter()
                .map(|parameter| parameter.to_string())
                .collect();
            let mut resolved = BTreeSet::new();
            collect_declaration_dependencies(
                &function.body,
                &id,
                source.imports,
                &spans,
                &lexical,
                &mut resolved,
            )?;
            dependencies.insert(id, resolved.into_iter().collect());
        }
    }

    reject_declaration_cycles(&dependencies, &spans)?;
    Ok(ResolvedModuleDeclarationGraph { dependencies })
}

fn resolved_id(module: &str, name: &str, kind: ResolvedDeclarationKind) -> ResolvedDeclarationId {
    ResolvedDeclarationId {
        module: module.to_owned(),
        name: name.to_owned(),
        kind,
    }
}

fn declaration_error(span: Span, message: String) -> ResolvedDeclarationError {
    ResolvedDeclarationError {
        span,
        message,
        chain: Vec::new(),
    }
}

fn insert_declaration(
    spans: &mut BTreeMap<ResolvedDeclarationId, Span>,
    module: &str,
    name: &str,
    kind: ResolvedDeclarationKind,
    span: Span,
) -> Result<(), ResolvedDeclarationError> {
    let id = resolved_id(module, name, kind);
    if spans.insert(id.clone(), span).is_some() {
        let kind = match kind {
            ResolvedDeclarationKind::Constant => "constant",
            ResolvedDeclarationKind::Function => "function",
        };
        return Err(declaration_error(
            span,
            format!("module `{module}` defines {kind} `{name}` more than once"),
        ));
    }
    Ok(())
}

fn find_declaration(
    spans: &BTreeMap<ResolvedDeclarationId, Span>,
    module: &str,
    name: &str,
    kind: ResolvedDeclarationKind,
) -> Option<ResolvedDeclarationId> {
    let id = resolved_id(module, name, kind);
    spans.contains_key(&id).then_some(id)
}

fn collect_declaration_dependencies(
    expr: &Expr,
    owner: &ResolvedDeclarationId,
    imports: &HashMap<String, String>,
    spans: &BTreeMap<ResolvedDeclarationId, Span>,
    lexical: &HashSet<String>,
    dependencies: &mut BTreeSet<ResolvedDeclarationId>,
) -> Result<(), ResolvedDeclarationError> {
    let visit = |child: &Expr,
                 lexical: &HashSet<String>,
                 dependencies: &mut BTreeSet<ResolvedDeclarationId>| {
        collect_declaration_dependencies(child, owner, imports, spans, lexical, dependencies)
    };
    match expr {
        Expr::FieldRef { name, span, .. } => {
            if lexical.contains(name.as_ref()) {
                return Ok(());
            }
            if let Some(dependency) = find_declaration(
                spans,
                &owner.module,
                name,
                ResolvedDeclarationKind::Constant,
            ) {
                dependencies.insert(dependency);
                return Ok(());
            }
            Err(declaration_error(
                *span,
                format!(
                    "module declaration `{}` references unresolved name `{name}`; declare it, pass it as a parameter, or import its module directly",
                    owner.label()
                ),
            ))
        }
        Expr::QualifiedFieldRef { parts, span, .. } => {
            if parts.len() != 2 {
                return Err(declaration_error(
                    *span,
                    format!(
                        "module declaration `{}` has unsupported qualified reference `{}`",
                        owner.label(),
                        parts.join(".")
                    ),
                ));
            }
            let alias = parts[0].as_ref();
            let target = imports.get(alias).ok_or_else(|| {
                declaration_error(
                    *span,
                    format!(
                        "module declaration `{}` cannot access `{}`: alias `{alias}` is not a direct import of module `{}`",
                        owner.label(),
                        parts.join("."),
                        owner.module
                    ),
                )
            })?;
            let dependency = find_declaration(
                spans,
                target,
                &parts[1],
                ResolvedDeclarationKind::Constant,
            )
            .ok_or_else(|| {
                declaration_error(
                    *span,
                    format!(
                        "module declaration `{}` references missing constant `{}.{}` through direct import `{alias}`",
                        owner.label(),
                        target,
                        parts[1]
                    ),
                )
            })?;
            dependencies.insert(dependency);
            Ok(())
        }
        Expr::MethodCall {
            receiver,
            method,
            args,
            span,
            ..
        } => {
            if let Expr::FieldRef { name, .. } = receiver.as_ref() {
                if lexical.contains(name.as_ref()) {
                    // A lexical receiver invokes a built-in method.
                } else if let Some(target) = imports.get(name.as_ref()) {
                    let dependency = find_declaration(
                        spans,
                        target,
                        method,
                        ResolvedDeclarationKind::Function,
                    )
                    .ok_or_else(|| {
                        declaration_error(
                            *span,
                            format!(
                                "module declaration `{}` references missing function `{}.{method}` through direct import `{name}`",
                                owner.label(), target
                            ),
                        )
                    })?;
                    dependencies.insert(dependency);
                } else if let Some(dependency) = find_declaration(
                    spans,
                    &owner.module,
                    name,
                    ResolvedDeclarationKind::Function,
                ) {
                    if method.as_ref() != "call" {
                        return Err(declaration_error(
                            *span,
                            format!(
                                "module declaration `{}` calls local function `{name}` with method `{method}`; use `{name}.call(...)`",
                                owner.label()
                            ),
                        ));
                    }
                    dependencies.insert(dependency);
                } else if let Some(dependency) = find_declaration(
                    spans,
                    &owner.module,
                    name,
                    ResolvedDeclarationKind::Constant,
                ) {
                    dependencies.insert(dependency);
                } else {
                    return Err(declaration_error(
                        *span,
                        format!(
                            "module declaration `{}` uses unresolved receiver `{name}`; pass it as a parameter or import its module directly",
                            owner.label()
                        ),
                    ));
                }
            } else {
                visit(receiver, lexical, dependencies)?;
            }
            for argument in args {
                visit(argument, lexical, dependencies)?;
            }
            Ok(())
        }
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            visit(lhs, lexical, dependencies)?;
            visit(rhs, lexical, dependencies)
        }
        Expr::Unary { operand, .. } => visit(operand, lexical, dependencies),
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            visit(condition, lexical, dependencies)?;
            visit(then_branch, lexical, dependencies)?;
            if let Some(branch) = else_branch {
                visit(branch, lexical, dependencies)?;
            }
            Ok(())
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(subject) = subject {
                visit(subject, lexical, dependencies)?;
            }
            for arm in arms {
                visit(&arm.pattern, lexical, dependencies)?;
                visit(&arm.body, lexical, dependencies)?;
            }
            Ok(())
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for argument in args {
                visit(argument, lexical, dependencies)?;
            }
            Ok(())
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            visit(receiver, lexical, dependencies)?;
            visit(index, lexical, dependencies)
        }
        Expr::Closure { param, body, .. } => {
            let mut nested = lexical.clone();
            nested.insert(param.to_string());
            visit(body, &nested, dependencies)
        }
        Expr::Literal { .. }
        | Expr::PipelineAccess { .. }
        | Expr::VarsAccess { .. }
        | Expr::ConfigAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::RecordAccess { .. }
        | Expr::DocAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => Ok(()),
    }
}

fn reject_declaration_cycles(
    dependencies: &BTreeMap<ResolvedDeclarationId, Vec<ResolvedDeclarationId>>,
    spans: &BTreeMap<ResolvedDeclarationId, Span>,
) -> Result<(), ResolvedDeclarationError> {
    let ids = dependencies.keys().cloned().collect::<Vec<_>>();
    let indexes = ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.clone(), index))
        .collect::<BTreeMap<_, _>>();
    let edges = ids
        .iter()
        .map(|id| {
            dependencies[id]
                .iter()
                .map(|dependency| indexes[dependency])
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let Some(cycle) = find_cycle(&edges) else {
        return Ok(());
    };
    let chain = cycle
        .iter()
        .map(|&index| ResolvedDeclarationSite {
            declaration: ids[index].clone(),
            span: spans[&ids[index]],
        })
        .collect::<Vec<_>>();
    let labels = chain
        .iter()
        .map(|site| site.declaration.label())
        .collect::<Vec<_>>()
        .join(" -> ");
    Err(ResolvedDeclarationError {
        span: chain[0].span,
        message: format!("cyclic module declaration dependency: {labels}"),
        chain,
    })
}

/// Immutable module declarations retained by a compiled CXL program.
///
/// Filesystem discovery belongs to the planner. The evaluator receives this
/// source-free representation and expands referenced constants/functions while
/// lowering the already-typechecked pipeline program.
#[derive(Debug, Clone, Default)]
pub struct RuntimeModuleRegistry {
    modules: HashMap<String, RuntimeModule>,
}

/// Stable signature and retained body for one admitted module function.
#[derive(Debug, Clone)]
pub struct ModuleFunctionSignature {
    pub declaration: ResolvedDeclarationId,
    pub parameters: Vec<Box<str>>,
    pub body: Expr,
    pub declaration_span: Span,
}

#[derive(Debug, Clone)]
struct RuntimeModule {
    module: Module,
    imports: HashMap<String, String>,
}

impl RuntimeModuleRegistry {
    pub fn insert(
        &mut self,
        id: impl Into<String>,
        module: Module,
        imports: HashMap<String, String>,
    ) {
        self.modules
            .insert(id.into(), RuntimeModule { module, imports });
    }

    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }

    pub fn expand_constant(
        &self,
        module_id: &str,
        name: &str,
        call_span: Span,
    ) -> Result<Expr, String> {
        self.expand_constant_inner(module_id, name, &mut Vec::new(), call_span)
    }

    pub fn expand_function(
        &self,
        module_id: &str,
        name: &str,
        call_span: Span,
    ) -> Result<(Vec<Box<str>>, Expr), String> {
        let signature = self.function_signature(module_id, name)?;
        let parameters = signature.parameters;
        let protected = parameters
            .iter()
            .map(|parameter| parameter.to_string())
            .collect();
        let body = self.expand_expr(
            module_id,
            &signature.body,
            &HashMap::new(),
            &protected,
            &mut vec![format!("{module_id}.{name}")],
            call_span,
        )?;
        Ok((parameters, body))
    }

    /// Look up one admitted declaration without consulting the filesystem.
    pub fn function_signature(
        &self,
        module_id: &str,
        name: &str,
    ) -> Result<ModuleFunctionSignature, String> {
        let module = self
            .modules
            .get(module_id)
            .ok_or_else(|| format!("compiled module `{module_id}` is unavailable"))?;
        let function = module
            .module
            .functions
            .iter()
            .find(|function| function.name.as_ref() == name)
            .ok_or_else(|| format!("compiled module `{module_id}` has no function `{name}`"))?;
        Ok(ModuleFunctionSignature {
            declaration: resolved_id(module_id, name, ResolvedDeclarationKind::Function),
            parameters: function.params.clone(),
            body: function.body.as_ref().clone(),
            declaration_span: function.span,
        })
    }

    fn expand_constant_inner(
        &self,
        module_id: &str,
        name: &str,
        stack: &mut Vec<String>,
        call_span: Span,
    ) -> Result<Expr, String> {
        let key = format!("{module_id}.{name}");
        if stack.iter().any(|entry| entry == &key) {
            return Err(format!("compiled module constant cycle reached at `{key}`"));
        }
        let module = self
            .modules
            .get(module_id)
            .ok_or_else(|| format!("compiled module `{module_id}` is unavailable"))?;
        let constant = module
            .module
            .constants
            .iter()
            .find(|constant| constant.name.as_ref() == name)
            .ok_or_else(|| format!("compiled module `{module_id}` has no constant `{name}`"))?;
        stack.push(key);
        let expanded = self.expand_expr(
            module_id,
            &constant.expr,
            &HashMap::new(),
            &HashSet::new(),
            stack,
            call_span,
        );
        stack.pop();
        expanded
    }

    fn expand_function_inline(
        &self,
        module_id: &str,
        name: &str,
        args: Vec<Expr>,
        stack: &mut Vec<String>,
        call_span: Span,
    ) -> Result<Expr, String> {
        let key = format!("{module_id}.{name}");
        if stack.iter().any(|entry| entry == &key) {
            return Err(format!("compiled module function cycle reached at `{key}`"));
        }
        let module = self
            .modules
            .get(module_id)
            .ok_or_else(|| format!("compiled module `{module_id}` is unavailable"))?;
        let function = module
            .module
            .functions
            .iter()
            .find(|function| function.name.as_ref() == name)
            .ok_or_else(|| format!("compiled module `{module_id}` has no function `{name}`"))?;
        if function.params.len() != args.len() {
            let mut chain = stack.clone();
            chain.push(key.clone());
            return Err(format!(
                "compiled module function `{key}` expects {} arguments, got {} in call chain {}",
                function.params.len(),
                args.len(),
                chain.join(" -> ")
            ));
        }
        let substitutions = function
            .params
            .iter()
            .map(|parameter| parameter.to_string())
            .zip(args)
            .collect();
        stack.push(key);
        let expanded = self.expand_expr(
            module_id,
            &function.body,
            &substitutions,
            &HashSet::new(),
            stack,
            call_span,
        );
        stack.pop();
        expanded
    }

    fn expand_expr(
        &self,
        module_id: &str,
        expr: &Expr,
        substitutions: &HashMap<String, Expr>,
        protected: &HashSet<String>,
        stack: &mut Vec<String>,
        call_span: Span,
    ) -> Result<Expr, String> {
        let expand = |child: &Expr, stack: &mut Vec<String>| {
            self.expand_expr(module_id, child, substitutions, protected, stack, call_span)
        };
        Ok(match expr {
            Expr::FieldRef { name, .. } => {
                if let Some(argument) = substitutions.get(name.as_ref()) {
                    argument.clone()
                } else if protected.contains(name.as_ref()) {
                    Expr::FieldRef {
                        node_id: RUNTIME_MODULE_NODE,
                        name: name.clone(),
                        span: call_span,
                    }
                } else if self.modules.get(module_id).is_some_and(|module| {
                    module
                        .module
                        .constants
                        .iter()
                        .any(|constant| constant.name == *name)
                }) {
                    self.expand_constant_inner(module_id, name, stack, call_span)?
                } else {
                    Expr::FieldRef {
                        node_id: RUNTIME_MODULE_NODE,
                        name: name.clone(),
                        span: call_span,
                    }
                }
            }
            Expr::QualifiedFieldRef { parts, .. } => {
                if parts.len() == 2
                    && let Some(target) = self
                        .modules
                        .get(module_id)
                        .and_then(|module| module.imports.get(parts[0].as_ref()))
                    && self.modules.get(target).is_some_and(|module| {
                        module
                            .module
                            .constants
                            .iter()
                            .any(|constant| constant.name == parts[1])
                    })
                {
                    self.expand_constant_inner(target, &parts[1], stack, call_span)?
                } else {
                    Expr::QualifiedFieldRef {
                        node_id: RUNTIME_MODULE_NODE,
                        parts: parts.clone(),
                        span: call_span,
                    }
                }
            }
            Expr::MethodCall {
                receiver,
                method,
                args,
                ..
            } => {
                if let Expr::FieldRef { name: alias, .. } = receiver.as_ref()
                    && !protected.contains(alias.as_ref())
                    && !substitutions.contains_key(alias.as_ref())
                    && let Some(target) = self
                        .modules
                        .get(module_id)
                        .and_then(|module| module.imports.get(alias.as_ref()))
                    && self.modules.get(target).is_some_and(|module| {
                        module
                            .module
                            .functions
                            .iter()
                            .any(|function| function.name == *method)
                    })
                {
                    let args = args
                        .iter()
                        .map(|arg| expand(arg, stack))
                        .collect::<Result<Vec<_>, _>>()?;
                    self.expand_function_inline(target, method, args, stack, call_span)?
                } else if method.as_ref() == "call"
                    && let Expr::FieldRef { name, .. } = receiver.as_ref()
                    && !protected.contains(name.as_ref())
                    && !substitutions.contains_key(name.as_ref())
                    && self.modules.get(module_id).is_some_and(|module| {
                        module
                            .module
                            .functions
                            .iter()
                            .any(|function| function.name == *name)
                    })
                {
                    let args = args
                        .iter()
                        .map(|arg| expand(arg, stack))
                        .collect::<Result<Vec<_>, _>>()?;
                    self.expand_function_inline(module_id, name, args, stack, call_span)?
                } else {
                    Expr::MethodCall {
                        node_id: RUNTIME_MODULE_NODE,
                        receiver: Box::new(expand(receiver, stack)?),
                        method: method.clone(),
                        args: args
                            .iter()
                            .map(|arg| expand(arg, stack))
                            .collect::<Result<Vec<_>, _>>()?,
                        span: call_span,
                    }
                }
            }
            Expr::Binary { op, lhs, rhs, .. } => Expr::Binary {
                node_id: RUNTIME_MODULE_NODE,
                op: *op,
                lhs: Box::new(expand(lhs, stack)?),
                rhs: Box::new(expand(rhs, stack)?),
                span: call_span,
            },
            Expr::Unary { op, operand, .. } => Expr::Unary {
                node_id: RUNTIME_MODULE_NODE,
                op: *op,
                operand: Box::new(expand(operand, stack)?),
                span: call_span,
            },
            Expr::Coalesce { lhs, rhs, .. } => Expr::Coalesce {
                node_id: RUNTIME_MODULE_NODE,
                lhs: Box::new(expand(lhs, stack)?),
                rhs: Box::new(expand(rhs, stack)?),
                span: call_span,
            },
            Expr::IfThenElse {
                condition,
                then_branch,
                else_branch,
                ..
            } => Expr::IfThenElse {
                node_id: RUNTIME_MODULE_NODE,
                condition: Box::new(expand(condition, stack)?),
                then_branch: Box::new(expand(then_branch, stack)?),
                else_branch: else_branch
                    .as_ref()
                    .map(|branch| expand(branch, stack).map(Box::new))
                    .transpose()?,
                span: call_span,
            },
            Expr::Match { subject, arms, .. } => Expr::Match {
                node_id: RUNTIME_MODULE_NODE,
                subject: subject
                    .as_ref()
                    .map(|subject| expand(subject, stack).map(Box::new))
                    .transpose()?,
                arms: arms
                    .iter()
                    .map(|arm| {
                        Ok(MatchArm {
                            node_id: RUNTIME_MODULE_NODE,
                            pattern: expand(&arm.pattern, stack)?,
                            body: expand(&arm.body, stack)?,
                            span: call_span,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                span: call_span,
            },
            Expr::IndexAccess {
                receiver, index, ..
            } => Expr::IndexAccess {
                node_id: RUNTIME_MODULE_NODE,
                receiver: Box::new(expand(receiver, stack)?),
                index: Box::new(expand(index, stack)?),
                span: call_span,
            },
            Expr::Closure { param, body, .. } => {
                let mut closure_substitutions = substitutions.clone();
                closure_substitutions.remove(param.as_ref());
                let mut closure_protected = protected.clone();
                closure_protected.insert(param.to_string());
                Expr::Closure {
                    node_id: RUNTIME_MODULE_NODE,
                    param: param.clone(),
                    body: Box::new(self.expand_expr(
                        module_id,
                        body,
                        &closure_substitutions,
                        &closure_protected,
                        stack,
                        call_span,
                    )?),
                    span: call_span,
                }
            }
            Expr::WindowCall { function, args, .. } => Expr::WindowCall {
                node_id: RUNTIME_MODULE_NODE,
                function: function.clone(),
                args: args
                    .iter()
                    .map(|arg| expand(arg, stack))
                    .collect::<Result<Vec<_>, _>>()?,
                span: call_span,
            },
            Expr::AggCall { name, args, .. } => Expr::AggCall {
                node_id: RUNTIME_MODULE_NODE,
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| expand(arg, stack))
                    .collect::<Result<Vec<_>, _>>()?,
                span: call_span,
            },
            Expr::Literal { value, .. } => Expr::Literal {
                node_id: RUNTIME_MODULE_NODE,
                value: value.clone(),
                span: call_span,
            },
            Expr::PipelineAccess { field, .. } => Expr::PipelineAccess {
                node_id: RUNTIME_MODULE_NODE,
                field: field.clone(),
                span: call_span,
            },
            Expr::VarsAccess { key, .. } => Expr::VarsAccess {
                node_id: RUNTIME_MODULE_NODE,
                key: key.clone(),
                span: call_span,
            },
            Expr::ConfigAccess { param, .. } => Expr::ConfigAccess {
                node_id: RUNTIME_MODULE_NODE,
                param: param.clone(),
                span: call_span,
            },
            Expr::SourceAccess { field, .. } => Expr::SourceAccess {
                node_id: RUNTIME_MODULE_NODE,
                field: field.clone(),
                span: call_span,
            },
            Expr::QualifiedSourceAccess {
                input_name, field, ..
            } => Expr::QualifiedSourceAccess {
                node_id: RUNTIME_MODULE_NODE,
                input_name: input_name.clone(),
                field: field.clone(),
                span: call_span,
            },
            Expr::RecordAccess { field, .. } => Expr::RecordAccess {
                node_id: RUNTIME_MODULE_NODE,
                field: field.clone(),
                span: call_span,
            },
            Expr::DocAccess { section, field, .. } => Expr::DocAccess {
                node_id: RUNTIME_MODULE_NODE,
                section: section.clone(),
                field: field.clone(),
                span: call_span,
            },
            Expr::Now { .. } => Expr::Now {
                node_id: RUNTIME_MODULE_NODE,
                span: call_span,
            },
            Expr::Wildcard { .. } => Expr::Wildcard {
                node_id: RUNTIME_MODULE_NODE,
                span: call_span,
            },
            Expr::AggSlot { slot, .. } => Expr::AggSlot {
                node_id: RUNTIME_MODULE_NODE,
                slot: *slot,
                span: call_span,
            },
            Expr::GroupKey { slot, .. } => Expr::GroupKey {
                node_id: RUNTIME_MODULE_NODE,
                slot: *slot,
                span: call_span,
            },
        })
    }
}

/// Topologically sort module constants by their dependencies (Kahn's algorithm).
/// Returns constants in evaluation order, or an error if there is a cycle,
/// a duplicate name, or a field reference in a constant expression.
pub fn toposort_constants(constants: &[ModuleConst]) -> Result<Vec<usize>, ModuleConstError> {
    let n = constants.len();
    if n == 0 {
        return Ok(vec![]);
    }

    // Build name → index map, checking for duplicates
    let mut name_to_idx: HashMap<&str, usize> = HashMap::with_capacity(n);
    for (i, c) in constants.iter().enumerate() {
        if let Some(prev) = name_to_idx.insert(&c.name, i) {
            let _ = prev;
            return Err(ModuleConstError {
                span: c.span,
                message: format!("duplicate constant name '{}'", c.name),
            });
        }
    }

    // Build adjacency: edges[i] = set of indices that i depends on
    let mut in_degree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![vec![]; n]; // dependents[dep] = list of nodes that depend on dep
    let mut dependencies: Vec<Vec<usize>> = vec![vec![]; n];

    for (i, c) in constants.iter().enumerate() {
        let deps = collect_ident_refs(&c.expr);
        for dep_name in &deps {
            if let Some(&dep_idx) = name_to_idx.get(dep_name.as_str()) {
                // i depends on dep_idx
                dependents[dep_idx].push(i);
                dependencies[i].push(dep_idx);
                in_degree[i] += 1;
            } else if !is_known_builtin(dep_name) {
                // Not a constant and not a builtin — it's a field reference
                return Err(ModuleConstError {
                    span: c.span,
                    message: format!(
                        "module constants cannot reference fields (found '{}' in constant '{}')",
                        dep_name, c.name
                    ),
                });
            }
        }
    }

    // Kahn's algorithm
    let mut queue: Vec<usize> = Vec::new();
    for (i, &deg) in in_degree.iter().enumerate().take(n) {
        if deg == 0 {
            queue.push(i);
        }
    }

    let mut order = Vec::with_capacity(n);
    while let Some(node) = queue.pop() {
        order.push(node);
        for &dep in &dependents[node] {
            in_degree[dep] -= 1;
            if in_degree[dep] == 0 {
                queue.push(dep);
            }
        }
    }

    if order.len() != n {
        let cycle = find_cycle(&dependencies).expect("a failed topological sort has a cycle");
        let cycle_node = cycle[0];
        let chain = cycle
            .iter()
            .map(|&index| constants[index].name.as_ref())
            .collect::<Vec<_>>()
            .join(" -> ");
        return Err(ModuleConstError {
            span: constants[cycle_node].span,
            message: format!("cyclic dependency detected: {chain}"),
        });
    }

    Ok(order)
}

/// Collect all identifier references from an expression (names that could be
/// other constants or field references).
fn collect_ident_refs(expr: &Expr) -> Vec<String> {
    let mut refs = Vec::new();
    walk_expr(expr, &mut refs);
    refs
}

fn walk_expr(expr: &Expr, refs: &mut Vec<String>) {
    match expr {
        Expr::FieldRef { name, .. } => {
            refs.push(name.to_string());
        }
        Expr::Binary { lhs, rhs, .. } => {
            walk_expr(lhs, refs);
            walk_expr(rhs, refs);
        }
        Expr::Unary { operand, .. } => {
            walk_expr(operand, refs);
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            walk_expr(lhs, refs);
            walk_expr(rhs, refs);
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            walk_expr(condition, refs);
            walk_expr(then_branch, refs);
            if let Some(eb) = else_branch {
                walk_expr(eb, refs);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                walk_expr(s, refs);
            }
            for arm in arms {
                walk_expr(&arm.pattern, refs);
                walk_expr(&arm.body, refs);
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            walk_expr(receiver, refs);
            for arg in args {
                walk_expr(arg, refs);
            }
        }
        Expr::Literal { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
        Expr::QualifiedFieldRef { .. } => {
            // Qualified refs like module.CONST are handled separately
        }
        Expr::WindowCall { args, .. } => {
            for arg in args {
                walk_expr(arg, refs);
            }
        }
        Expr::PipelineAccess { .. }
        | Expr::VarsAccess { .. }
        | Expr::ConfigAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::RecordAccess { .. }
        | Expr::DocAccess { .. } => {
            // Per-record / per-source namespaces not allowed in module constants —
            // but we don't reject here; the evaluator will catch it at runtime.
        }
        Expr::AggCall { args, .. } => {
            for arg in args {
                walk_expr(arg, refs);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            walk_expr(receiver, refs);
            walk_expr(index, refs);
        }
        Expr::Closure { body, .. } => {
            walk_expr(body, refs);
        }
    }
}

/// Check if a name is a known builtin (literal keywords that parse as FieldRef).
/// These are NOT constant references and should be ignored during dependency analysis.
fn is_known_builtin(_name: &str) -> bool {
    // Keywords like true/false/null/now/it parse as their own tokens,
    // not FieldRef. So any FieldRef that isn't a constant name is a
    // field reference — which is invalid in module constants.
    false
}

/// Phase C validation: check that no module function calls itself recursively.
/// Since modules have single-expression bodies and no cross-module imports,
/// only direct self-recursion is possible.
pub fn check_recursive_calls(functions: &[FnDecl]) -> Result<(), ModuleConstError> {
    let names = functions
        .iter()
        .enumerate()
        .map(|(index, function)| (function.name.as_ref(), index))
        .collect::<HashMap<_, _>>();
    let mut dependencies = vec![Vec::new(); functions.len()];
    for (index, function) in functions.iter().enumerate() {
        collect_function_refs(&function.body, &names, &mut dependencies[index]);
        dependencies[index].sort_unstable();
        dependencies[index].dedup();
    }
    if let Some(cycle) = find_cycle(&dependencies) {
        let chain = cycle
            .iter()
            .map(|&index| functions[index].name.as_ref())
            .collect::<Vec<_>>()
            .join(" -> ");
        return Err(ModuleConstError {
            span: functions[cycle[0]].span,
            message: format!(
                "recursive calls are not supported: {chain} (cycle begins at function '{}')",
                functions[cycle[0]].name
            ),
        });
    }
    Ok(())
}

fn find_cycle(edges: &[Vec<usize>]) -> Option<Vec<usize>> {
    fn visit(
        node: usize,
        edges: &[Vec<usize>],
        state: &mut [u8],
        stack: &mut Vec<usize>,
    ) -> Option<Vec<usize>> {
        state[node] = 1;
        stack.push(node);
        for &dependency in &edges[node] {
            if state[dependency] == 0 {
                if let Some(cycle) = visit(dependency, edges, state, stack) {
                    return Some(cycle);
                }
            } else if state[dependency] == 1 {
                let start = stack
                    .iter()
                    .position(|&candidate| candidate == dependency)
                    .expect("visiting node is on DFS stack");
                let mut cycle = stack[start..].to_vec();
                cycle.push(dependency);
                return Some(cycle);
            }
        }
        stack.pop();
        state[node] = 2;
        None
    }

    let mut state = vec![0; edges.len()];
    let mut stack = Vec::new();
    for node in 0..edges.len() {
        if state[node] == 0
            && let Some(cycle) = visit(node, edges, &mut state, &mut stack)
        {
            return Some(cycle);
        }
    }
    None
}

fn collect_function_refs(expr: &Expr, names: &HashMap<&str, usize>, refs: &mut Vec<usize>) {
    match expr {
        Expr::MethodCall { receiver, args, .. } => {
            if let Expr::FieldRef { name, .. } = &**receiver
                && let Some(&index) = names.get(name.as_ref())
            {
                refs.push(index);
            }
            collect_function_refs(receiver, names, refs);
            for arg in args {
                collect_function_refs(arg, names, refs);
            }
        }
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            collect_function_refs(lhs, names, refs);
            collect_function_refs(rhs, names, refs);
        }
        Expr::Unary { operand, .. } => collect_function_refs(operand, names, refs),
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_function_refs(condition, names, refs);
            collect_function_refs(then_branch, names, refs);
            if let Some(branch) = else_branch {
                collect_function_refs(branch, names, refs);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(subject) = subject {
                collect_function_refs(subject, names, refs);
            }
            for arm in arms {
                collect_function_refs(&arm.pattern, names, refs);
                collect_function_refs(&arm.body, names, refs);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for arg in args {
                collect_function_refs(arg, names, refs);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            collect_function_refs(receiver, names, refs);
            collect_function_refs(index, names, refs);
        }
        Expr::Closure { body, .. } => collect_function_refs(body, names, refs),
        Expr::Literal { .. }
        | Expr::FieldRef { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::PipelineAccess { .. }
        | Expr::VarsAccess { .. }
        | Expr::ConfigAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::RecordAccess { .. }
        | Expr::DocAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinOp, LiteralValue, NodeId};
    use crate::lexer::Span;

    fn make_const(name: &str, expr: Expr) -> ModuleConst {
        ModuleConst {
            node_id: NodeId(0),
            name: name.into(),
            expr,
            span: Span::new(0, 1),
        }
    }

    fn lit_int(v: i64) -> Expr {
        Expr::Literal {
            node_id: NodeId(0),
            value: LiteralValue::Int(v),
            span: Span::new(0, 1),
        }
    }

    fn field_ref(name: &str) -> Expr {
        Expr::FieldRef {
            node_id: NodeId(0),
            name: name.into(),
            span: Span::new(0, 1),
        }
    }

    fn add(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            node_id: NodeId(0),
            op: BinOp::Add,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            span: Span::new(0, 1),
        }
    }

    #[test]
    fn test_module_const_forward_reference() {
        // let B = A + 1; let A = 5 → order: A first, then B
        let constants = vec![
            make_const("B", add(field_ref("A"), lit_int(1))),
            make_const("A", lit_int(5)),
        ];
        let order = toposort_constants(&constants).unwrap();
        // A (index 1) must come before B (index 0)
        let a_pos = order.iter().position(|&i| i == 1).unwrap();
        let b_pos = order.iter().position(|&i| i == 0).unwrap();
        assert!(a_pos < b_pos, "A must be evaluated before B");
    }

    #[test]
    fn test_module_const_cycle_detection() {
        // let A = B + 1; let B = A + 1 → cycle
        let constants = vec![
            make_const("A", add(field_ref("B"), lit_int(1))),
            make_const("B", add(field_ref("A"), lit_int(1))),
        ];
        let err = toposort_constants(&constants).unwrap_err();
        assert!(err.message.contains("cyclic dependency"));
    }

    #[test]
    fn test_module_const_duplicate_name() {
        let constants = vec![make_const("X", lit_int(1)), make_const("X", lit_int(2))];
        let err = toposort_constants(&constants).unwrap_err();
        assert!(err.message.contains("duplicate constant name 'X'"));
    }

    #[test]
    fn test_module_const_reject_field_reference() {
        // let BAD = Amount → "module constants cannot reference fields"
        let constants = vec![make_const("BAD", field_ref("Amount"))];
        let err = toposort_constants(&constants).unwrap_err();
        assert!(
            err.message
                .contains("module constants cannot reference fields")
        );
        assert!(err.message.contains("Amount"));
    }

    #[test]
    fn test_module_const_no_deps() {
        // Independent constants — any order is valid
        let constants = vec![
            make_const("A", lit_int(1)),
            make_const("B", lit_int(2)),
            make_const("C", lit_int(3)),
        ];
        let order = toposort_constants(&constants).unwrap();
        assert_eq!(order.len(), 3);
    }

    #[test]
    fn test_module_const_empty() {
        let order = toposort_constants(&[]).unwrap();
        assert!(order.is_empty());
    }

    #[test]
    fn test_module_const_chain() {
        // A = 1, B = A + 1, C = B + 1 → must be A, B, C
        let constants = vec![
            make_const("C", add(field_ref("B"), lit_int(1))),
            make_const("B", add(field_ref("A"), lit_int(1))),
            make_const("A", lit_int(1)),
        ];
        let order = toposort_constants(&constants).unwrap();
        let a_pos = order.iter().position(|&i| i == 2).unwrap();
        let b_pos = order.iter().position(|&i| i == 1).unwrap();
        let c_pos = order.iter().position(|&i| i == 0).unwrap();
        assert!(a_pos < b_pos, "A before B");
        assert!(b_pos < c_pos, "B before C");
    }

    // ── Recursive call detection tests ────────────────────────────

    fn make_fn(name: &str, params: &[&str], body: Expr) -> FnDecl {
        FnDecl {
            node_id: NodeId(0),
            name: name.into(),
            params: params.iter().map(|p| (*p).into()).collect(),
            body: Box::new(body),
            span: Span::new(0, 1),
        }
    }

    fn parsed_module(source: &str) -> Module {
        let parsed = crate::parser::Parser::parse_module(source);
        assert!(parsed.errors.is_empty(), "{:?}", parsed.errors);
        parsed.module
    }

    fn declaration_id(
        module: &str,
        name: &str,
        kind: ResolvedDeclarationKind,
    ) -> ResolvedDeclarationId {
        ResolvedDeclarationId {
            module: module.to_owned(),
            name: name.to_owned(),
            kind,
        }
    }

    #[test]
    fn resolved_declaration_graph_has_stable_cross_module_identities() {
        let base = parsed_module("let BASE = 40\nfn add(value) = value + 2\n");
        let root = parsed_module(
            "use shared.base as base\nlet ANSWER = base.BASE\nfn answer(value) = base.add(value)\n",
        );
        let no_imports = HashMap::new();
        let root_imports = HashMap::from([("base".to_owned(), "shared.base".to_owned())]);
        let graph = validate_module_declaration_closure(&[
            ModuleDeclarationSource {
                module_id: "app.main",
                module: &root,
                imports: &root_imports,
            },
            ModuleDeclarationSource {
                module_id: "shared.base",
                module: &base,
                imports: &no_imports,
            },
        ])
        .unwrap();

        let answer = declaration_id("app.main", "ANSWER", ResolvedDeclarationKind::Constant);
        let base = declaration_id("shared.base", "BASE", ResolvedDeclarationKind::Constant);
        let answer_fn = declaration_id("app.main", "answer", ResolvedDeclarationKind::Function);
        let add_fn = declaration_id("shared.base", "add", ResolvedDeclarationKind::Function);
        assert!(graph.contains(&answer));
        assert_eq!(graph.dependencies(&answer), Some(&[base][..]));
        assert_eq!(graph.dependencies(&answer_fn), Some(&[add_fn][..]));
    }

    #[test]
    fn resolved_declaration_graph_honors_parameter_and_closure_shadowing() {
        let mut module =
            parsed_module("let value = 1\nfn shadow(value) = value.map(it => it + 1)\n");
        module.constants.push(make_const("it", lit_int(2)));
        let imports = HashMap::new();
        let graph = validate_module_declaration_closure(&[ModuleDeclarationSource {
            module_id: "shadow",
            module: &module,
            imports: &imports,
        }])
        .unwrap();
        let shadow = declaration_id("shadow", "shadow", ResolvedDeclarationKind::Function);

        assert_eq!(graph.dependencies(&shadow), Some(&[][..]));
    }

    #[test]
    fn resolved_declaration_cycles_cover_constant_function_and_mixed_edges() {
        let imports = HashMap::new();
        for (source, expected) in [
            (
                "let A = B\nlet B = A\n",
                ["cycles.A", "cycles.B", "cycles.A"],
            ),
            (
                "fn first(x) = second.call(x)\nfn second(x) = first.call(x)\n",
                ["cycles.first", "cycles.second", "cycles.first"],
            ),
            (
                "let A = f.call(1)\nfn f(x) = A\n",
                ["cycles.A", "cycles.f", "cycles.A"],
            ),
        ] {
            let module = parsed_module(source);
            let error = validate_module_declaration_closure(&[ModuleDeclarationSource {
                module_id: "cycles",
                module: &module,
                imports: &imports,
            }])
            .unwrap_err();
            let mut cursor = 0;
            for declaration in expected {
                let offset = error.message[cursor..]
                    .find(declaration)
                    .unwrap_or_else(|| panic!("{} missing from {}", declaration, error.message));
                cursor += offset + declaration.len();
            }
        }
    }

    #[test]
    fn resolved_declaration_cycles_follow_import_aliases() {
        let first = parsed_module("let FIRST = second.invoke(1)\n");
        let second = parsed_module("fn invoke(value) = first.FIRST + value\n");
        let first_imports = HashMap::from([("second".to_owned(), "second".to_owned())]);
        let second_imports = HashMap::from([("first".to_owned(), "first".to_owned())]);
        let error = validate_module_declaration_closure(&[
            ModuleDeclarationSource {
                module_id: "first",
                module: &first,
                imports: &first_imports,
            },
            ModuleDeclarationSource {
                module_id: "second",
                module: &second,
                imports: &second_imports,
            },
        ])
        .unwrap_err();

        assert!(error.message.contains("first.FIRST"), "{}", error.message);
        assert!(error.message.contains("second.invoke"), "{}", error.message);
    }

    fn method_call(receiver: Expr, method: &str, args: Vec<Expr>) -> Expr {
        Expr::MethodCall {
            node_id: NodeId(0),
            receiver: Box::new(receiver),
            method: method.into(),
            args,
            span: Span::new(0, 1),
        }
    }

    fn if_then_else(cond: Expr, then_br: Expr, else_br: Expr) -> Expr {
        Expr::IfThenElse {
            node_id: NodeId(0),
            condition: Box::new(cond),
            then_branch: Box::new(then_br),
            else_branch: Some(Box::new(else_br)),
            span: Span::new(0, 1),
        }
    }

    fn gt(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            node_id: NodeId(0),
            op: BinOp::Gt,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            span: Span::new(0, 1),
        }
    }

    fn sub(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            node_id: NodeId(0),
            op: crate::ast::BinOp::Sub,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            span: Span::new(0, 1),
        }
    }

    #[test]
    fn test_module_fn_recursive_call_rejected() {
        // fn f(x) = if x > 0 then f.something(x - 1) else x
        // This simulates self-reference: receiver FieldRef("f") with method call
        let body = if_then_else(
            gt(field_ref("x"), lit_int(0)),
            method_call(
                field_ref("f"),
                "call",
                vec![sub(field_ref("x"), lit_int(1))],
            ),
            field_ref("x"),
        );
        let functions = vec![make_fn("f", &["x"], body)];
        let err = check_recursive_calls(&functions).unwrap_err();
        assert!(err.message.contains("recursive calls are not supported"));
        assert!(err.message.contains("'f'"));
    }

    #[test]
    fn test_module_fn_no_recursion_ok() {
        // fn double(x) = x * 2 — no recursion
        let body = Expr::Binary {
            node_id: NodeId(0),
            op: BinOp::Mul,
            lhs: Box::new(field_ref("x")),
            rhs: Box::new(lit_int(2)),
            span: Span::new(0, 1),
        };
        let functions = vec![make_fn("double", &["x"], body)];
        assert!(check_recursive_calls(&functions).is_ok());
    }

    #[test]
    fn test_module_fn_indirect_cycle_reports_complete_chain() {
        let first = make_fn(
            "first",
            &["x"],
            method_call(field_ref("second"), "call", vec![field_ref("x")]),
        );
        let second = make_fn(
            "second",
            &["x"],
            method_call(field_ref("first"), "call", vec![field_ref("x")]),
        );
        let error = check_recursive_calls(&[first, second]).unwrap_err();
        assert!(error.message.contains("first -> second -> first"));
    }

    #[test]
    fn test_module_fn_different_name_method_ok() {
        // fn clean(val) = val.trim() — method name != fn name, no recursion
        let body = method_call(field_ref("val"), "trim", vec![]);
        let functions = vec![make_fn("clean", &["val"], body)];
        assert!(check_recursive_calls(&functions).is_ok());
    }
}
