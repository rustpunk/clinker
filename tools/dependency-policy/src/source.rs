use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use proc_macro2::{TokenStream, TokenTree};
use syn::ext::IdentExt;
use syn::visit::{self, Visit};
use syn::{
    Attribute, Expr, ExprLit, ForeignItem, Generics, ImplItem, Item, ItemEnum, ItemExternCrate,
    ItemImpl, ItemMod, ItemStruct, ItemTrait, ItemUnion, Lit, Meta, Signature, Token, TraitItem,
    Type, UseTree, Visibility,
};

use crate::manifest::allowed_shared_types;
use crate::{BoundaryError, BoundaryResult, crate_source_root};

type ModulePath = Vec<String>;

const FORBIDDEN_CORE_IDENTIFIERS: [&str; 12] = [
    "CompiledPlan",
    "DatasetIdentity",
    "ExecutionPlanDag",
    "FormatReader",
    "HttpClient",
    "OpenLineage",
    "PhysicalDatasetIdentity",
    "PipelineError",
    "PlanNodeId",
    "RecordSource",
    "SemanticFingerprint",
    "SourceInput",
];

#[derive(Clone)]
struct ParsedFile {
    path: PathBuf,
    syntax: syn::File,
    edges: Vec<ModuleEdge>,
}

#[derive(Clone, Debug)]
struct ModuleEdge {
    target: PathBuf,
    suffix: ModulePath,
    test_only: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PathRef {
    leading_colon: bool,
    segments: Vec<String>,
}

impl PathRef {
    fn from_path(path: &syn::Path) -> Self {
        Self {
            leading_colon: path.leading_colon.is_some(),
            segments: path
                .segments
                .iter()
                .map(|segment| identifier_name(&segment.ident))
                .collect(),
        }
    }
}

#[derive(Clone, Debug)]
enum AliasExpression {
    Path(PathRef),
    Type(Vec<PathRef>),
}

#[derive(Clone, Debug)]
struct AliasDeclaration {
    expression: AliasExpression,
    public: bool,
    label: String,
}

#[derive(Clone)]
struct Surface {
    module: ModulePath,
    paths: Vec<PathRef>,
    attributes: Vec<Attribute>,
    label: String,
    facade: bool,
}

#[derive(Default)]
struct Analysis {
    aliases: BTreeMap<(ModulePath, String), Vec<AliasDeclaration>>,
    semantic_bindings: BTreeMap<(ModulePath, String), Vec<PathRef>>,
    modules: BTreeSet<ModulePath>,
    items: BTreeSet<(ModulePath, String)>,
    public_items: BTreeSet<(ModulePath, String)>,
    shared_reference_seen: bool,
    surfaces: Vec<Surface>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Resolution {
    Shared(String),
    Module(ModulePath),
    Other,
    Unknown,
}

pub(crate) fn check_core_source(root: &Path) -> BoundaryResult<()> {
    let units = production_units(root, "clinker-core-types")?;
    let mut visitor = CoreVisitor::default();
    for unit in &units {
        visitor.visit_file(&unit.syntax);
        if let Some(error) = visitor.error.take() {
            return Err(error.context(unit.path.display().to_string()));
        }
    }
    check_core_failure_exports(root)
}

pub(crate) fn check_consumer_source(root: &Path, crate_name: &str) -> BoundaryResult<()> {
    let units = production_units(root, crate_name)?;
    let mut analysis = Analysis::default();

    for unit in &units {
        collect_item_declarations(&unit.syntax.items, &unit.module, false, &mut analysis);
    }
    for unit in &units {
        let mut direct = DirectReferenceVisitor::new(crate_name, &unit.path);
        direct.visit_file(&unit.syntax);
        if let Some(error) = direct.error.take() {
            return Err(error);
        }
        analysis.shared_reference_seen |= direct.shared_reference_seen;
        analyze_module_items(
            crate_name,
            &unit.path,
            &unit.syntax.items,
            &unit.module,
            false,
            &mut analysis,
        )?;
    }

    if !analysis.shared_reference_seen {
        return Err(BoundaryError::new(format!(
            "{crate_name} normal dependency is unused in production source"
        )));
    }

    let resolver = Resolver::new(
        &analysis.aliases,
        &analysis.semantic_bindings,
        &analysis.modules,
    );
    for ((module, name), declarations) in &analysis.aliases {
        let resolution = resolver.resolve_name(module, name);
        for declaration in declarations.iter().filter(|declaration| declaration.public) {
            if resolution
                .iter()
                .any(|value| matches!(value, Resolution::Shared(_)))
            {
                return Err(BoundaryError::new(format!(
                    "{crate_name} must not re-export the shared taxonomy through {}",
                    declaration.label
                )));
            }
            if resolution.contains(&Resolution::Unknown) {
                return Err(BoundaryError::new(format!(
                    "{crate_name} cannot prove public alias {} is outside the shared taxonomy",
                    declaration.label
                )));
            }
        }
    }

    for surface in &analysis.surfaces {
        let mut shared = BTreeSet::new();
        let mut unknown = false;
        for path in &surface.paths {
            for resolution in resolver.resolve_path(&surface.module, path) {
                match resolution {
                    Resolution::Shared(name) => {
                        shared.insert(name);
                    }
                    Resolution::Unknown => unknown = true,
                    Resolution::Module(_) | Resolution::Other => {}
                }
            }
        }
        if unknown {
            return Err(BoundaryError::new(format!(
                "{crate_name} cannot resolve {} strongly enough to prove the shared-taxonomy boundary",
                surface.label
            )));
        }
        if shared.is_empty() {
            continue;
        }
        if surface.facade {
            return Err(BoundaryError::new(format!(
                "{crate_name} must not expose a public taxonomy facade through {}",
                surface.label
            )));
        }
        if !has_api_classification(&surface.attributes) {
            return Err(BoundaryError::new(format!(
                "{crate_name} public signature {} using {:?} lacks an exact local API classification",
                surface.label, shared
            )));
        }
    }
    Ok(())
}

fn collect_item_declarations(
    items: &[Item],
    module: &ModulePath,
    inherited_test: bool,
    analysis: &mut Analysis,
) {
    for item in items {
        let test_only = inherited_test || exact_cfg_test(item_attrs(item));
        if test_only {
            continue;
        }
        let declaration = match item {
            Item::Enum(item) => Some((&item.ident, &item.vis)),
            Item::Struct(item) => Some((&item.ident, &item.vis)),
            Item::Trait(item) => Some((&item.ident, &item.vis)),
            Item::Type(item) => Some((&item.ident, &item.vis)),
            Item::Union(item) => Some((&item.ident, &item.vis)),
            _ => None,
        };
        if let Some((identifier, visibility)) = declaration {
            let key = (module.clone(), identifier_name(identifier));
            analysis.items.insert(key.clone());
            if visible(visibility) {
                analysis.public_items.insert(key);
            }
        }
        if let Item::Mod(item_mod) = item
            && let Some((_, nested)) = &item_mod.content
        {
            let mut child = module.clone();
            child.push(identifier_name(&item_mod.ident));
            collect_item_declarations(nested, &child, test_only, analysis);
        }
    }
}

#[derive(Clone)]
struct ProductionUnit {
    path: PathBuf,
    module: ModulePath,
    syntax: syn::File,
}

fn production_units(root: &Path, crate_name: &str) -> BoundaryResult<Vec<ProductionUnit>> {
    let source_root = crate_source_root(root, crate_name);
    if !source_root.is_dir() {
        return Err(BoundaryError::new(format!(
            "missing production source directory for {crate_name}"
        )));
    }

    let mut paths = Vec::new();
    collect_source_files(&source_root, &source_root, &mut paths)?;
    paths.sort();
    let mut parsed = HashMap::new();
    let mut incoming = HashSet::new();
    for path in &paths {
        let text = fs::read_to_string(path).map_err(|error| {
            BoundaryError::new(format!("cannot read {}: {error}", path.display()))
        })?;
        let syntax = syn::parse_file(&text).map_err(|error| {
            BoundaryError::new(format!(
                "cannot parse Rust source {}: {error}",
                path.display()
            ))
        })?;
        let mut edges = Vec::new();
        collect_module_edges(path, &syntax.items, &[], false, &mut edges)?;
        incoming.extend(edges.iter().map(|edge| edge.target.clone()));
        parsed.insert(
            path.clone(),
            ParsedFile {
                path: path.clone(),
                syntax,
                edges,
            },
        );
    }

    let roots: Vec<PathBuf> = paths
        .iter()
        .filter(|path| {
            let relative = path.strip_prefix(&source_root).unwrap_or(path);
            matches!(relative.to_str(), Some("lib.rs" | "main.rs"))
                || relative
                    .components()
                    .next()
                    .is_some_and(|part| part.as_os_str() == "bin")
                || !incoming.contains(*path)
        })
        .cloned()
        .collect();

    let candidate_set: HashSet<PathBuf> = paths.iter().cloned().collect();
    let mut pending: Vec<(PathBuf, ModulePath, bool)> = roots
        .into_iter()
        .map(|path| {
            let module = physical_module(&source_root, &path);
            (path, module, false)
        })
        .collect();
    let mut visited = HashSet::new();
    let mut units = Vec::new();
    while let Some((path, module, inherited_test)) = pending.pop() {
        if !visited.insert((path.clone(), module.clone(), inherited_test)) {
            continue;
        }
        let parsed_file = parsed.get(&path).ok_or_else(|| {
            BoundaryError::new(format!(
                "module graph references unparsed source {}",
                path.display()
            ))
        })?;
        let file_test = inherited_test || exact_cfg_test(&parsed_file.syntax.attrs);
        if !file_test {
            units.push(ProductionUnit {
                path: parsed_file.path.clone(),
                module: module.clone(),
                syntax: parsed_file.syntax.clone(),
            });
        }
        for edge in &parsed_file.edges {
            let child_test = file_test || edge.test_only;
            if !candidate_set.contains(&edge.target) {
                if !child_test {
                    return Err(BoundaryError::new(format!(
                        "production module source is missing: {}",
                        edge.target.display()
                    )));
                }
                continue;
            }
            let mut child_module = module.clone();
            child_module.extend(edge.suffix.iter().cloned());
            pending.push((edge.target.clone(), child_module, child_test));
        }
    }
    units.sort_by(|left, right| (&left.path, &left.module).cmp(&(&right.path, &right.module)));
    Ok(units)
}

fn collect_source_files(
    root: &Path,
    directory: &Path,
    paths: &mut Vec<PathBuf>,
) -> BoundaryResult<()> {
    for entry in fs::read_dir(directory).map_err(|error| {
        BoundaryError::new(format!(
            "cannot read source directory {}: {error}",
            directory.display()
        ))
    })? {
        let entry = entry
            .map_err(|error| BoundaryError::new(format!("cannot read source entry: {error}")))?;
        let path = entry.path();
        if path.is_dir() {
            collect_source_files(root, &path, paths)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            paths.push(path);
        } else {
            let relative = path.strip_prefix(root).unwrap_or(&path);
            return Err(BoundaryError::new(format!(
                "non-.rs production source cannot be audited: {}",
                relative.display()
            )));
        }
    }
    Ok(())
}

fn collect_module_edges(
    physical_file: &Path,
    items: &[Item],
    inline_path: &[String],
    inherited_test: bool,
    edges: &mut Vec<ModuleEdge>,
) -> BoundaryResult<()> {
    for item in items {
        let Item::Mod(module) = item else {
            continue;
        };
        let direct_test = exact_cfg_test(&module.attrs);
        let test_only = inherited_test || direct_test;
        let name = identifier_name(&module.ident);
        if let Some((_, nested)) = &module.content {
            let mut nested_path = inline_path.to_vec();
            nested_path.push(name);
            collect_module_edges(physical_file, nested, &nested_path, test_only, edges)?;
            continue;
        }
        let target = external_module_path(physical_file, inline_path, module)?;
        let mut suffix = inline_path.to_vec();
        suffix.push(name);
        edges.push(ModuleEdge {
            target,
            suffix,
            test_only,
        });
    }
    Ok(())
}

fn external_module_path(
    physical_file: &Path,
    inline_path: &[String],
    module: &ItemMod,
) -> BoundaryResult<PathBuf> {
    let path_attribute = module
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("path"))
        .map(attribute_string_value)
        .collect::<BoundaryResult<Vec<_>>>()?;
    if path_attribute.len() > 1 {
        return Err(BoundaryError::new(format!(
            "module {} has more than one #[path] attribute",
            module.ident
        )));
    }
    if let Some(relative) = path_attribute.first() {
        let base = if inline_path.is_empty() {
            physical_file
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        } else {
            module_directory(physical_file).join(inline_path.iter().collect::<PathBuf>())
        };
        return Ok(base.join(relative));
    }

    let base = module_directory(physical_file).join(inline_path.iter().collect::<PathBuf>());
    let module_name = identifier_name(&module.ident);
    let flat = base.join(format!("{module_name}.rs"));
    let nested = base.join(module_name).join("mod.rs");
    match (flat.exists(), nested.exists()) {
        (true, false) => Ok(flat),
        (false, true) => Ok(nested),
        (true, true) => Err(BoundaryError::new(format!(
            "module {} has both {} and {}",
            module.ident,
            flat.display(),
            nested.display()
        ))),
        (false, false) => Ok(flat),
    }
}

fn module_directory(physical_file: &Path) -> PathBuf {
    let parent = physical_file.parent().unwrap_or_else(|| Path::new("."));
    match physical_file.file_name().and_then(|value| value.to_str()) {
        Some("lib.rs" | "main.rs" | "mod.rs") => parent.to_path_buf(),
        _ => parent.join(physical_file.file_stem().unwrap_or_default()),
    }
}

fn attribute_string_value(attribute: &Attribute) -> BoundaryResult<PathBuf> {
    let Meta::NameValue(name_value) = &attribute.meta else {
        return Err(BoundaryError::new(
            "#[path] must be a string name-value attribute",
        ));
    };
    let Expr::Lit(ExprLit {
        lit: Lit::Str(value),
        ..
    }) = &name_value.value
    else {
        return Err(BoundaryError::new("#[path] must contain a string literal"));
    };
    let path = PathBuf::from(value.value());
    if path.extension().and_then(|value| value.to_str()) != Some("rs") {
        return Err(BoundaryError::new(format!(
            "#[path] target must be a .rs source, found {}",
            path.display()
        )));
    }
    Ok(path)
}

fn physical_module(source_root: &Path, path: &Path) -> ModulePath {
    let relative = path.strip_prefix(source_root).unwrap_or(path);
    let mut parts: Vec<String> = relative
        .with_extension("")
        .components()
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .collect();
    if parts
        .last()
        .is_some_and(|part| matches!(part.as_str(), "lib" | "main" | "mod"))
    {
        parts.pop();
    }
    parts
}

fn analyze_module_items(
    crate_name: &str,
    file: &Path,
    items: &[Item],
    module: &ModulePath,
    inherited_test: bool,
    analysis: &mut Analysis,
) -> BoundaryResult<()> {
    analysis.modules.insert(module.clone());
    for item in items {
        let test_only = inherited_test || exact_cfg_test(item_attrs(item));
        if test_only {
            continue;
        }
        match item {
            Item::Use(item_use) => {
                let leaves = expand_use_tree(&item_use.tree, Vec::new())?;
                for leaf in leaves {
                    if leaf.glob {
                        return Err(BoundaryError::new(format!(
                            "{crate_name} production source {} uses a glob import that the dependency policy resolver cannot prove safe",
                            file.display()
                        )));
                    }
                    let name = leaf
                        .alias
                        .clone()
                        .or_else(|| leaf.path.segments.last().cloned())
                        .ok_or_else(|| {
                            BoundaryError::new("use declaration has no local binding name")
                        })?;
                    analysis
                        .aliases
                        .entry((module.clone(), name.clone()))
                        .or_default()
                        .push(AliasDeclaration {
                            expression: AliasExpression::Path(leaf.path),
                            public: visible(&item_use.vis),
                            label: format!("use {name} in {}", file.display()),
                        });
                }
            }
            Item::Type(item_type) => {
                let mut collector = PathCollector::with_generics(&item_type.generics);
                collector.visit_generics(&item_type.generics);
                collector.visit_type(&item_type.ty);
                let paths = collector.paths;
                let name = identifier_name(&item_type.ident);
                analysis
                    .aliases
                    .entry((module.clone(), name.clone()))
                    .or_default()
                    .push(AliasDeclaration {
                        expression: AliasExpression::Type(paths.clone()),
                        public: visible(&item_type.vis),
                        label: format!("type {name} in {}", file.display()),
                    });
                if visible(&item_type.vis) {
                    analysis.surfaces.push(Surface {
                        module: module.clone(),
                        paths,
                        attributes: item_type.attrs.clone(),
                        label: format!("type {name} in {}", file.display()),
                        facade: true,
                    });
                }
            }
            Item::Fn(function) if visible(&function.vis) => {
                push_signature_surface(
                    analysis,
                    module,
                    &function.sig,
                    &function.attrs,
                    format!("function {} in {}", function.sig.ident, file.display()),
                    None,
                    true,
                );
            }
            Item::Struct(item_struct) if visible(&item_struct.vis) => {
                push_struct_surface(analysis, module, item_struct, file);
            }
            Item::Enum(item_enum) if visible(&item_enum.vis) => {
                push_enum_surface(analysis, module, item_enum, file);
            }
            Item::Union(item_union) if visible(&item_union.vis) => {
                push_union_surface(analysis, module, item_union, file);
            }
            Item::Trait(item_trait) if visible(&item_trait.vis) => {
                push_trait_surfaces(crate_name, analysis, module, item_trait, file)?;
            }
            Item::Const(item_const) if visible(&item_const.vis) => {
                analysis.surfaces.push(Surface {
                    module: module.clone(),
                    paths: type_paths(&item_const.ty),
                    attributes: item_const.attrs.clone(),
                    label: format!("const {} in {}", item_const.ident, file.display()),
                    facade: false,
                });
            }
            Item::Static(item_static) if visible(&item_static.vis) => {
                analysis.surfaces.push(Surface {
                    module: module.clone(),
                    paths: type_paths(&item_static.ty),
                    attributes: item_static.attrs.clone(),
                    label: format!("static {} in {}", item_static.ident, file.display()),
                    facade: false,
                });
            }
            Item::Impl(item_impl) => {
                push_impl_surfaces(crate_name, analysis, module, item_impl, file)?;
            }
            Item::ForeignMod(foreign) => {
                push_foreign_surfaces(crate_name, analysis, module, foreign, file)?;
            }
            Item::Mod(item_mod) => {
                let mut child = module.clone();
                child.push(identifier_name(&item_mod.ident));
                analysis.modules.insert(child.clone());
                if let Some((_, nested)) = &item_mod.content {
                    analyze_module_items(crate_name, file, nested, &child, test_only, analysis)?;
                }
            }
            Item::Macro(item_macro) => {
                return Err(BoundaryError::new(format!(
                    "{crate_name} has an item-level macro definition or expansion in {} that the shared-taxonomy checker cannot audit: {:?}",
                    file.display(),
                    item_macro.ident
                )));
            }
            Item::ExternCrate(item) => check_extern_crate(crate_name, file, item)?,
            _ => {}
        }
    }
    Ok(())
}

fn push_signature_surface(
    analysis: &mut Analysis,
    module: &ModulePath,
    signature: &Signature,
    attributes: &[Attribute],
    label: String,
    outer_generics: Option<&Generics>,
    opaque_signature_generics: bool,
) {
    let mut collector = PathCollector::default();
    if let Some(generics) = outer_generics {
        collector.add_generic_bindings(generics, true);
        collector.visit_generics(generics);
    }
    collector.opaque_self_projection = opaque_signature_generics;
    collector.add_generic_bindings(&signature.generics, opaque_signature_generics);
    collector.visit_signature(signature);
    analysis.surfaces.push(Surface {
        module: module.clone(),
        paths: collector.paths,
        attributes: attributes.to_vec(),
        label,
        facade: false,
    });
}

fn push_struct_surface(
    analysis: &mut Analysis,
    module: &ModulePath,
    item: &ItemStruct,
    file: &Path,
) {
    let mut collector = PathCollector::with_generics(&item.generics);
    collector.visit_generics(&item.generics);
    for field in &item.fields {
        if visible(&field.vis) {
            collector.visit_type(&field.ty);
        }
    }
    analysis.surfaces.push(Surface {
        module: module.clone(),
        paths: collector.paths,
        attributes: item.attrs.clone(),
        label: format!("struct {} in {}", item.ident, file.display()),
        facade: false,
    });
}

fn push_enum_surface(analysis: &mut Analysis, module: &ModulePath, item: &ItemEnum, file: &Path) {
    let mut collector = PathCollector::with_generics(&item.generics);
    collector.visit_generics(&item.generics);
    for variant in &item.variants {
        for field in &variant.fields {
            collector.visit_type(&field.ty);
        }
    }
    analysis.surfaces.push(Surface {
        module: module.clone(),
        paths: collector.paths,
        attributes: item.attrs.clone(),
        label: format!("enum {} in {}", item.ident, file.display()),
        facade: false,
    });
}

fn push_union_surface(analysis: &mut Analysis, module: &ModulePath, item: &ItemUnion, file: &Path) {
    let mut collector = PathCollector::with_generics(&item.generics);
    collector.visit_generics(&item.generics);
    for field in &item.fields.named {
        if visible(&field.vis) {
            collector.visit_type(&field.ty);
        }
    }
    analysis.surfaces.push(Surface {
        module: module.clone(),
        paths: collector.paths,
        attributes: item.attrs.clone(),
        label: format!("union {} in {}", item.ident, file.display()),
        facade: false,
    });
}

fn push_trait_surfaces(
    crate_name: &str,
    analysis: &mut Analysis,
    module: &ModulePath,
    item: &ItemTrait,
    file: &Path,
) -> BoundaryResult<()> {
    let mut trait_paths = PathCollector::with_generics(&item.generics);
    trait_paths.visit_generics(&item.generics);
    for bound in &item.supertraits {
        trait_paths.visit_type_param_bound(bound);
    }
    analysis.semantic_bindings.insert(
        (module.clone(), identifier_name(&item.ident)),
        trait_paths.paths.clone(),
    );
    analysis.surfaces.push(Surface {
        module: module.clone(),
        paths: trait_paths.paths,
        attributes: item.attrs.clone(),
        label: format!("trait {} in {}", item.ident, file.display()),
        facade: false,
    });
    for associated in &item.items {
        match associated {
            TraitItem::Fn(function) => push_signature_surface(
                analysis,
                module,
                &function.sig,
                &function.attrs,
                format!(
                    "trait method {}::{} in {}",
                    item.ident,
                    function.sig.ident,
                    file.display()
                ),
                Some(&item.generics),
                true,
            ),
            TraitItem::Type(item_type) => {
                let mut collector = PathCollector::with_generics(&item.generics);
                collector.visit_generics(&item.generics);
                collector.add_generic_bindings(&item_type.generics, true);
                collector.visit_generics(&item_type.generics);
                for bound in &item_type.bounds {
                    collector.visit_type_param_bound(bound);
                }
                if let Some((_, default)) = &item_type.default {
                    collector.visit_type(default);
                }
                analysis.surfaces.push(Surface {
                    module: module.clone(),
                    paths: collector.paths,
                    attributes: item_type.attrs.clone(),
                    label: format!(
                        "trait associated type {}::{} in {}",
                        item.ident,
                        item_type.ident,
                        file.display()
                    ),
                    facade: true,
                });
            }
            TraitItem::Const(item_const) => {
                let mut collector = PathCollector::with_generics(&item.generics);
                collector.visit_generics(&item.generics);
                collector.visit_type(&item_const.ty);
                analysis.surfaces.push(Surface {
                    module: module.clone(),
                    paths: collector.paths,
                    attributes: item_const.attrs.clone(),
                    label: format!(
                        "trait const {}::{} in {}",
                        item.ident,
                        item_const.ident,
                        file.display()
                    ),
                    facade: false,
                });
            }
            TraitItem::Macro(_) => {
                return Err(BoundaryError::new(format!(
                    "{crate_name} has a macro expansion in public trait {} in {} that the shared-taxonomy checker cannot audit",
                    item.ident,
                    file.display()
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

fn push_impl_surfaces(
    crate_name: &str,
    analysis: &mut Analysis,
    module: &ModulePath,
    item: &ItemImpl,
    file: &Path,
) -> BoundaryResult<()> {
    let public_trait_impl = item.trait_.as_ref().is_some_and(|(_, trait_path, _)| {
        impl_surface_is_public(analysis, module, trait_path, &item.self_ty)
    });
    if public_trait_impl {
        let mut collector = PathCollector::with_generics(&item.generics);
        collector.visit_generics(&item.generics);
        if let Some((_, trait_path, _)) = &item.trait_ {
            collector.visit_path(trait_path);
        }
        collector.visit_type(&item.self_ty);
        analysis.surfaces.push(Surface {
            module: module.clone(),
            paths: collector.paths,
            attributes: item.attrs.clone(),
            label: format!("trait impl declaration in {}", file.display()),
            facade: true,
        });
    }
    for associated in &item.items {
        match associated {
            ImplItem::Fn(function) if public_trait_impl || visible(&function.vis) => {
                push_signature_surface(
                    analysis,
                    module,
                    &function.sig,
                    &function.attrs,
                    format!("impl method {} in {}", function.sig.ident, file.display()),
                    Some(&item.generics),
                    !public_trait_impl,
                )
            }
            ImplItem::Const(item_const) if public_trait_impl || visible(&item_const.vis) => {
                let mut collector = PathCollector::with_generics(&item.generics);
                collector.visit_generics(&item.generics);
                collector.visit_type(&item_const.ty);
                analysis.surfaces.push(Surface {
                    module: module.clone(),
                    paths: collector.paths,
                    attributes: item_const.attrs.clone(),
                    label: format!("impl const {} in {}", item_const.ident, file.display()),
                    facade: false,
                });
            }
            ImplItem::Type(item_type) if public_trait_impl || visible(&item_type.vis) => {
                let mut collector = PathCollector::with_generics(&item.generics);
                collector.visit_generics(&item.generics);
                collector.visit_type(&item_type.ty);
                analysis.surfaces.push(Surface {
                    module: module.clone(),
                    paths: collector.paths,
                    attributes: item_type.attrs.clone(),
                    label: format!(
                        "impl associated type {} in {}",
                        item_type.ident,
                        file.display()
                    ),
                    facade: true,
                });
            }
            ImplItem::Macro(_) => {
                return Err(BoundaryError::new(format!(
                    "{crate_name} has a macro expansion in an impl in {} that the shared-taxonomy checker cannot audit",
                    file.display()
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

fn push_foreign_surfaces(
    crate_name: &str,
    analysis: &mut Analysis,
    module: &ModulePath,
    item: &syn::ItemForeignMod,
    file: &Path,
) -> BoundaryResult<()> {
    for foreign in &item.items {
        match foreign {
            ForeignItem::Fn(function) if visible(&function.vis) => push_signature_surface(
                analysis,
                module,
                &function.sig,
                &function.attrs,
                format!(
                    "foreign function {} in {}",
                    function.sig.ident,
                    file.display()
                ),
                None,
                true,
            ),
            ForeignItem::Static(item_static) if visible(&item_static.vis) => {
                analysis.surfaces.push(Surface {
                    module: module.clone(),
                    paths: type_paths(&item_static.ty),
                    attributes: item_static.attrs.clone(),
                    label: format!("foreign static {} in {}", item_static.ident, file.display()),
                    facade: false,
                });
            }
            ForeignItem::Type(item_type) if visible(&item_type.vis) => {
                analysis.surfaces.push(Surface {
                    module: module.clone(),
                    paths: Vec::new(),
                    attributes: item_type.attrs.clone(),
                    label: format!("foreign type {} in {}", item_type.ident, file.display()),
                    facade: true,
                });
            }
            ForeignItem::Macro(_) => {
                return Err(BoundaryError::new(format!(
                    "{crate_name} has a macro expansion in a foreign block in {} that the shared-taxonomy checker cannot audit",
                    file.display()
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

fn impl_surface_is_public(
    analysis: &Analysis,
    module: &ModulePath,
    trait_path: &syn::Path,
    self_type: &Type,
) -> bool {
    let trait_public = local_item_publicity(analysis, module, &PathRef::from_path(trait_path));
    let self_public = match self_type {
        Type::Path(path) if path.qself.is_none() => {
            local_item_publicity(analysis, module, &PathRef::from_path(&path.path))
        }
        _ => None,
    };
    trait_public != Some(false) && self_public != Some(false)
}

fn local_item_publicity(analysis: &Analysis, module: &ModulePath, path: &PathRef) -> Option<bool> {
    let (mut base, remaining) = absolute_base(module, path);
    if remaining.is_empty() {
        return None;
    }
    if !path.leading_colon
        && !matches!(path.segments[0].as_str(), "crate" | "self" | "super")
        && remaining.len() == 1
    {
        base = module.clone();
    } else {
        base.extend_from_slice(&remaining[..remaining.len() - 1]);
    }
    let key = (base, remaining.last()?.clone());
    analysis
        .items
        .contains(&key)
        .then(|| analysis.public_items.contains(&key))
}

fn item_attrs(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        Item::Verbatim(_) => &[],
        _ => &[],
    }
}

fn exact_cfg_test(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.path().is_ident("cfg")
            && attribute
                .parse_args::<syn::Path>()
                .is_ok_and(|path| path.is_ident("test"))
    })
}

fn visible(visibility: &Visibility) -> bool {
    !matches!(visibility, Visibility::Inherited)
}

fn identifier_name(identifier: &syn::Ident) -> String {
    identifier.unraw().to_string()
}

fn path_is_single_ident(path: &syn::Path, expected: &str) -> bool {
    path.leading_colon.is_none()
        && path.segments.len() == 1
        && path
            .segments
            .first()
            .is_some_and(|segment| identifier_name(&segment.ident) == expected)
}

fn token_stream_contains_identifier(tokens: TokenStream, expected: &str) -> bool {
    let mut pending: Vec<TokenTree> = tokens.into_iter().collect();
    while let Some(token) = pending.pop() {
        match token {
            TokenTree::Ident(identifier) if identifier_name(&identifier) == expected => {
                return true;
            }
            TokenTree::Group(group) => pending.extend(group.stream()),
            TokenTree::Ident(_) | TokenTree::Literal(_) | TokenTree::Punct(_) => {}
        }
    }
    false
}

#[derive(Clone, Debug)]
struct UseLeaf {
    path: PathRef,
    alias: Option<String>,
    glob: bool,
}

fn expand_use_tree(tree: &UseTree, prefix: Vec<String>) -> BoundaryResult<Vec<UseLeaf>> {
    match tree {
        UseTree::Path(path) => {
            let mut prefix = prefix;
            prefix.push(identifier_name(&path.ident));
            expand_use_tree(&path.tree, prefix)
        }
        UseTree::Name(name) => {
            let mut segments = prefix;
            if identifier_name(&name.ident) != "self" || segments.is_empty() {
                segments.push(identifier_name(&name.ident));
            }
            Ok(vec![UseLeaf {
                path: PathRef {
                    leading_colon: false,
                    segments,
                },
                alias: None,
                glob: false,
            }])
        }
        UseTree::Rename(rename) => {
            let mut segments = prefix;
            if identifier_name(&rename.ident) != "self" || segments.is_empty() {
                segments.push(identifier_name(&rename.ident));
            }
            Ok(vec![UseLeaf {
                path: PathRef {
                    leading_colon: false,
                    segments,
                },
                alias: Some(identifier_name(&rename.rename)),
                glob: false,
            }])
        }
        UseTree::Glob(_) => Ok(vec![UseLeaf {
            path: PathRef {
                leading_colon: false,
                segments: prefix,
            },
            alias: None,
            glob: true,
        }]),
        UseTree::Group(group) => {
            let mut leaves = Vec::new();
            for nested in &group.items {
                leaves.extend(expand_use_tree(nested, prefix.clone())?);
            }
            Ok(leaves)
        }
    }
}

fn check_extern_crate(crate_name: &str, file: &Path, item: &ItemExternCrate) -> BoundaryResult<()> {
    if identifier_name(&item.ident) == "clinker_core_types"
        && (visible(&item.vis) || item.rename.is_some())
    {
        return Err(BoundaryError::new(format!(
            "{crate_name} must not alias or re-export clinker-core-types in {}",
            file.display()
        )));
    }
    Ok(())
}

#[derive(Default)]
struct PathCollector {
    paths: Vec<PathRef>,
    generic_bindings: BTreeSet<String>,
    opaque_projection_bindings: BTreeSet<String>,
    opaque_self_projection: bool,
}

impl PathCollector {
    fn with_generics(generics: &Generics) -> Self {
        let mut collector = Self::default();
        collector.add_generic_bindings(generics, true);
        collector
    }

    fn add_generic_bindings(&mut self, generics: &Generics, opaque_projections: bool) {
        for parameter in &generics.params {
            let name = match parameter {
                syn::GenericParam::Type(parameter) => Some(identifier_name(&parameter.ident)),
                syn::GenericParam::Const(parameter) => Some(identifier_name(&parameter.ident)),
                syn::GenericParam::Lifetime(_) => None,
            };
            if let Some(name) = name {
                self.generic_bindings.insert(name.clone());
                if opaque_projections {
                    self.opaque_projection_bindings.insert(name);
                }
            }
        }
    }

    fn mark_unknown(&mut self) {
        self.paths.push(PathRef {
            leading_colon: false,
            segments: Vec::new(),
        });
    }
}

impl<'ast> Visit<'ast> for PathCollector {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        let shadowed = path.leading_colon.is_none()
            && path.segments.first().is_some_and(|segment| {
                self.generic_bindings
                    .contains(&identifier_name(&segment.ident))
            });
        let opaque_projection = shadowed
            && path.segments.len() > 1
            && path.segments.first().is_some_and(|segment| {
                self.opaque_projection_bindings
                    .contains(&identifier_name(&segment.ident))
            });
        let opaque_self_projection = self.opaque_self_projection
            && path.segments.len() > 1
            && path
                .segments
                .first()
                .is_some_and(|segment| identifier_name(&segment.ident) == "Self");
        if opaque_projection || opaque_self_projection {
            self.mark_unknown();
        } else if !shadowed {
            self.paths.push(PathRef::from_path(path));
        }
        visit::visit_path(self, path);
    }

    fn visit_type_macro(&mut self, _type_macro: &'ast syn::TypeMacro) {
        self.mark_unknown();
    }

    fn visit_type_path(&mut self, type_path: &'ast syn::TypePath) {
        if type_path.qself.is_some() {
            self.mark_unknown();
        }
        visit::visit_type_path(self, type_path);
    }
}

fn type_paths(value: &Type) -> Vec<PathRef> {
    let mut collector = PathCollector::default();
    collector.visit_type(value);
    collector.paths
}

struct DirectReferenceVisitor<'a> {
    crate_name: &'a str,
    file: &'a Path,
    error: Option<BoundaryError>,
    shared_reference_seen: bool,
}

impl<'a> DirectReferenceVisitor<'a> {
    fn new(crate_name: &'a str, file: &'a Path) -> Self {
        Self {
            crate_name,
            file,
            error: None,
            shared_reference_seen: false,
        }
    }

    fn reject(&mut self, message: impl Into<String>) {
        if self.error.is_none() {
            self.error = Some(BoundaryError::new(message));
        }
    }

    fn inspect_use(&mut self, item: &syn::ItemUse) {
        let leaves = match expand_use_tree(&item.tree, Vec::new()) {
            Ok(leaves) => leaves,
            Err(error) => {
                self.error = Some(error);
                return;
            }
        };
        for leaf in leaves {
            if leaf.path.segments.first().map(String::as_str) != Some("clinker_core_types") {
                continue;
            }
            if visible(&item.vis) {
                self.reject(format!(
                    "{} must not re-export the shared taxonomy in {}",
                    self.crate_name,
                    self.file.display()
                ));
                return;
            }
            if leaf.glob {
                self.reject(format!(
                    "{} must not wildcard-import clinker-core-types in {}",
                    self.crate_name,
                    self.file.display()
                ));
                return;
            }
            if leaf.path.segments.len() == 1
                || leaf.alias.is_some() && leaf.path.segments.len() == 1
            {
                self.reject(format!(
                    "{} must not alias clinker-core-types in {}",
                    self.crate_name,
                    self.file.display()
                ));
                return;
            }
            let item_name = &leaf.path.segments[1];
            if !allowed_shared_types().contains(&item_name.as_str()) {
                self.reject(format!(
                    "{} references unapproved clinker-core-types item {item_name} in {}",
                    self.crate_name,
                    self.file.display()
                ));
                return;
            }
            self.shared_reference_seen = true;
        }
    }
}

impl<'ast> Visit<'ast> for DirectReferenceVisitor<'_> {
    fn visit_item(&mut self, item: &'ast Item) {
        if exact_cfg_test(item_attrs(item)) {
            return;
        }
        visit::visit_item(self, item);
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        self.inspect_use(item);
        if self.error.is_none() {
            visit::visit_item_use(self, item);
        }
    }

    fn visit_item_extern_crate(&mut self, item: &'ast ItemExternCrate) {
        if let Err(error) = check_extern_crate(self.crate_name, self.file, item) {
            self.error = Some(error);
        }
    }

    fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
        if path_is_single_ident(&item.mac.path, "include") {
            self.reject(format!(
                "{} uses include! in production source {}; expanded source cannot be audited",
                self.crate_name,
                self.file.display()
            ));
        } else {
            self.reject(format!(
                "{} has an item-level macro definition or expansion in {} that the shared-taxonomy checker cannot audit: {:?}",
                self.crate_name,
                self.file.display(),
                item.ident
            ));
        }
    }

    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        let name = attribute
            .path()
            .segments
            .first()
            .map(|segment| identifier_name(&segment.ident));
        let allowed = name.as_deref().is_some_and(|name| {
            matches!(
                name,
                "allow"
                    | "cfg"
                    | "cold"
                    | "deny"
                    | "deprecated"
                    | "derive"
                    | "doc"
                    | "expect"
                    | "forbid"
                    | "inline"
                    | "link"
                    | "link_name"
                    | "must_use"
                    | "no_mangle"
                    | "non_exhaustive"
                    | "path"
                    | "repr"
                    | "serde"
                    | "test"
                    | "unsafe"
                    | "warn"
            )
        });
        if !allowed {
            self.reject(format!(
                "{} uses unsupported production attribute {} in {}; macro expansion cannot be audited",
                self.crate_name,
                name.as_deref().unwrap_or("<empty>"),
                self.file.display()
            ));
            return;
        }
        if attribute.path().is_ident("derive") {
            match parse_derive_paths(attribute) {
                Ok(derives)
                    if derives.iter().all(|path| {
                        path.segments.last().is_some_and(|segment| {
                            matches!(
                                identifier_name(&segment.ident).as_str(),
                                "Clone"
                                    | "Copy"
                                    | "Debug"
                                    | "Default"
                                    | "Deserialize"
                                    | "Eq"
                                    | "Hash"
                                    | "Ord"
                                    | "PartialEq"
                                    | "PartialOrd"
                                    | "Serialize"
                            )
                        })
                    }) => {}
                Ok(_) => self.reject(format!(
                    "{} uses an unapproved derive macro in {}; generated API cannot be audited",
                    self.crate_name,
                    self.file.display()
                )),
                Err(error) => self.reject(format!(
                    "{} has an unsupported derive attribute in {}: {error}",
                    self.crate_name,
                    self.file.display()
                )),
            }
        }
        visit::visit_attribute(self, attribute);
    }

    fn visit_trait_item_macro(&mut self, _item: &'ast syn::TraitItemMacro) {
        self.reject(format!(
            "{} has a macro expansion in a trait in {} that the shared-taxonomy checker cannot audit",
            self.crate_name,
            self.file.display()
        ));
    }

    fn visit_impl_item_macro(&mut self, _item: &'ast syn::ImplItemMacro) {
        self.reject(format!(
            "{} has a macro expansion in an impl in {} that the shared-taxonomy checker cannot audit",
            self.crate_name,
            self.file.display()
        ));
    }

    fn visit_foreign_item_macro(&mut self, _item: &'ast syn::ForeignItemMacro) {
        self.reject(format!(
            "{} has a macro expansion in a foreign block in {} that the shared-taxonomy checker cannot audit",
            self.crate_name,
            self.file.display()
        ));
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        if self.error.is_some() {
            return;
        }
        let segments: Vec<String> = path
            .segments
            .iter()
            .map(|segment| identifier_name(&segment.ident))
            .collect();
        if segments.first().map(String::as_str) == Some("clinker_core_types") {
            match segments.get(1) {
                Some(item) if allowed_shared_types().contains(&item.as_str()) => {}
                Some(item) => self.reject(format!(
                    "{} references unapproved clinker-core-types item {item} in {}",
                    self.crate_name,
                    self.file.display()
                )),
                None => self.reject(format!(
                    "{} aliases or exposes clinker-core-types in {}",
                    self.crate_name,
                    self.file.display()
                )),
            }
        }
        if segments.first().map(String::as_str) == Some("clinker_core_types")
            && segments
                .get(1)
                .is_some_and(|item| allowed_shared_types().contains(&item.as_str()))
        {
            self.shared_reference_seen = true;
        }
        visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        if path_is_single_ident(&item.path, "include") {
            self.reject(format!(
                "{} uses include! in production source {}; expanded source cannot be audited",
                self.crate_name,
                self.file.display()
            ));
        }
        visit::visit_macro(self, item);
    }

    fn visit_type_macro(&mut self, _item: &'ast syn::TypeMacro) {
        self.reject(format!(
            "{} uses a macro in a production type position in {}; expanded public types cannot be audited",
            self.crate_name,
            self.file.display()
        ));
    }
}

struct Resolver<'a> {
    aliases: &'a BTreeMap<(ModulePath, String), Vec<AliasDeclaration>>,
    semantic_bindings: &'a BTreeMap<(ModulePath, String), Vec<PathRef>>,
    modules: &'a BTreeSet<ModulePath>,
}

impl<'a> Resolver<'a> {
    fn new(
        aliases: &'a BTreeMap<(ModulePath, String), Vec<AliasDeclaration>>,
        semantic_bindings: &'a BTreeMap<(ModulePath, String), Vec<PathRef>>,
        modules: &'a BTreeSet<ModulePath>,
    ) -> Self {
        Self {
            aliases,
            semantic_bindings,
            modules,
        }
    }

    fn resolve_name(&self, module: &ModulePath, name: &str) -> BTreeSet<Resolution> {
        self.resolve_name_inner(module, name, &mut BTreeSet::new())
    }

    fn resolve_name_inner(
        &self,
        module: &ModulePath,
        name: &str,
        stack: &mut BTreeSet<(ModulePath, String)>,
    ) -> BTreeSet<Resolution> {
        let key = (module.clone(), name.to_owned());
        if !stack.insert(key.clone()) {
            return BTreeSet::from([Resolution::Unknown]);
        }
        let result = match self.aliases.get(&key) {
            Some(declarations) => declarations
                .iter()
                .flat_map(|declaration| {
                    self.resolve_expression(module, &declaration.expression, stack)
                })
                .collect(),
            None if self.semantic_bindings.contains_key(&key) => {
                let mut result = BTreeSet::new();
                for path in &self.semantic_bindings[&key] {
                    result.extend(self.resolve_path_inner(module, path, stack));
                }
                if result
                    .iter()
                    .any(|value| matches!(value, Resolution::Shared(_) | Resolution::Unknown))
                {
                    result.retain(|value| {
                        !matches!(value, Resolution::Other | Resolution::Module(_))
                    });
                    result
                } else {
                    BTreeSet::from([Resolution::Other])
                }
            }
            None => {
                let mut child = module.clone();
                child.push(name.to_owned());
                if self.modules.contains(&child) {
                    BTreeSet::from([Resolution::Module(child)])
                } else {
                    BTreeSet::from([Resolution::Other])
                }
            }
        };
        stack.remove(&key);
        if result.is_empty() {
            BTreeSet::from([Resolution::Unknown])
        } else {
            result
        }
    }

    fn resolve_expression(
        &self,
        module: &ModulePath,
        expression: &AliasExpression,
        stack: &mut BTreeSet<(ModulePath, String)>,
    ) -> BTreeSet<Resolution> {
        match expression {
            AliasExpression::Path(path) => self.resolve_path_inner(module, path, stack),
            AliasExpression::Type(paths) => {
                let mut result = BTreeSet::new();
                for path in paths {
                    result.extend(self.resolve_path_inner(module, path, stack));
                }
                if result
                    .iter()
                    .any(|value| matches!(value, Resolution::Shared(_) | Resolution::Unknown))
                {
                    result.retain(|value| {
                        !matches!(value, Resolution::Other | Resolution::Module(_))
                    });
                    result
                } else {
                    BTreeSet::from([Resolution::Other])
                }
            }
        }
    }

    fn resolve_path(&self, module: &ModulePath, path: &PathRef) -> BTreeSet<Resolution> {
        self.resolve_path_inner(module, path, &mut BTreeSet::new())
    }

    fn resolve_path_inner(
        &self,
        module: &ModulePath,
        path: &PathRef,
        stack: &mut BTreeSet<(ModulePath, String)>,
    ) -> BTreeSet<Resolution> {
        if path.segments.is_empty() {
            return BTreeSet::from([Resolution::Unknown]);
        }
        if path.segments[0] == "clinker_core_types" {
            return match path.segments.get(1) {
                Some(name) if allowed_shared_types().contains(&name.as_str()) => {
                    BTreeSet::from([Resolution::Shared(name.clone())])
                }
                Some(_) => BTreeSet::from([Resolution::Other]),
                None => BTreeSet::from([Resolution::Unknown]),
            };
        }

        let (base, remaining) = absolute_base(module, path);
        if remaining.is_empty() {
            return BTreeSet::from([Resolution::Module(base)]);
        }
        if path.leading_colon || matches!(path.segments[0].as_str(), "crate" | "self" | "super") {
            return self.resolve_from_module(&base, remaining, stack);
        }

        let first = &remaining[0];
        let first_resolutions = self.resolve_name_inner(module, first, stack);
        if remaining.len() == 1 {
            return first_resolutions;
        }
        let mut result = BTreeSet::new();
        for resolution in first_resolutions {
            match resolution {
                Resolution::Module(module) => {
                    result.extend(self.resolve_from_module(&module, &remaining[1..], stack));
                }
                Resolution::Unknown => {
                    result.insert(Resolution::Unknown);
                }
                Resolution::Shared(_) | Resolution::Other => {
                    result.insert(Resolution::Other);
                }
            }
        }
        result
    }

    fn resolve_from_module(
        &self,
        module: &ModulePath,
        segments: &[String],
        stack: &mut BTreeSet<(ModulePath, String)>,
    ) -> BTreeSet<Resolution> {
        if segments.is_empty() {
            return BTreeSet::from([Resolution::Module(module.clone())]);
        }
        let mut cursor = module.clone();
        for (index, segment) in segments.iter().enumerate() {
            let mut child = cursor.clone();
            child.push(segment.clone());
            if self.modules.contains(&child) {
                cursor = child;
                if index + 1 == segments.len() {
                    return BTreeSet::from([Resolution::Module(cursor)]);
                }
                continue;
            }
            if index + 1 == segments.len() {
                return self.resolve_name_inner(&cursor, segment, stack);
            }
            let resolutions = self.resolve_name_inner(&cursor, segment, stack);
            let mut result = BTreeSet::new();
            for resolution in resolutions {
                match resolution {
                    Resolution::Module(module) => {
                        result.extend(self.resolve_from_module(
                            &module,
                            &segments[index + 1..],
                            stack,
                        ));
                    }
                    Resolution::Unknown => {
                        result.insert(Resolution::Unknown);
                    }
                    Resolution::Shared(_) | Resolution::Other => {
                        result.insert(Resolution::Other);
                    }
                }
            }
            return result;
        }
        BTreeSet::from([Resolution::Module(cursor)])
    }
}

fn absolute_base<'a>(module: &ModulePath, path: &'a PathRef) -> (ModulePath, &'a [String]) {
    if path.leading_colon || path.segments.first().map(String::as_str) == Some("crate") {
        let skip = usize::from(path.segments.first().map(String::as_str) == Some("crate"));
        return (Vec::new(), &path.segments[skip..]);
    }
    if path.segments.first().map(String::as_str) == Some("self") {
        return (module.clone(), &path.segments[1..]);
    }
    let mut base = module.clone();
    let mut skip = 0;
    while path.segments.get(skip).map(String::as_str) == Some("super") {
        base.pop();
        skip += 1;
    }
    (base, &path.segments[skip..])
}

fn has_api_classification(attributes: &[Attribute]) -> bool {
    const APPROVED: [&str; 6] = [
        "supported integration api",
        "workspace-internal exposed api",
        "test support",
        "deprecated route",
        "deprecated/cleanup debt",
        "deprecated cleanup debt",
    ];
    doc_lines(attributes).iter().any(|line| {
        let line = line.trim().to_ascii_lowercase();
        APPROVED.iter().any(|classification| {
            line == format!("api classification: {classification}.")
                || line == format!("api classification: {classification}")
        })
    })
}

fn parse_derive_paths(
    attribute: &Attribute,
) -> syn::Result<syn::punctuated::Punctuated<syn::Path, Token![,]>> {
    attribute.parse_args_with(syn::punctuated::Punctuated::<syn::Path, Token![,]>::parse_terminated)
}

fn doc_lines(attributes: &[Attribute]) -> Vec<String> {
    let mut lines = Vec::new();
    for attribute in attributes {
        if !attribute.path().is_ident("doc") {
            continue;
        }
        let Meta::NameValue(name_value) = &attribute.meta else {
            continue;
        };
        let Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) = &name_value.value
        else {
            continue;
        };
        lines.extend(value.value().lines().map(str::to_owned));
    }
    lines
}

#[derive(Default)]
struct CoreVisitor {
    error: Option<BoundaryError>,
}

impl CoreVisitor {
    fn reject(&mut self, message: impl Into<String>) {
        if self.error.is_none() {
            self.error = Some(BoundaryError::new(message));
        }
    }

    fn reject_foreign_identity(&mut self, identifier: &syn::Ident) {
        let name = identifier_name(identifier);
        if FORBIDDEN_CORE_IDENTIFIERS.contains(&name.as_str()) {
            self.reject(format!(
                "clinker-core-types must not own foreign layer or identity type {name}"
            ));
        }
    }

    fn inspect_macro_tokens(&mut self, tokens: TokenStream) {
        let mut pending: Vec<TokenTree> = tokens.into_iter().collect();
        while let Some(token) = pending.pop() {
            match token {
                TokenTree::Ident(identifier) => {
                    let identifier = identifier_name(&identifier);
                    if FORBIDDEN_CORE_IDENTIFIERS.contains(&identifier.as_str())
                        || matches!(
                            identifier.as_str(),
                            "Deserialize"
                                | "Serialize"
                                | "enum"
                                | "serde"
                                | "serde_json"
                                | "struct"
                                | "trait"
                                | "type"
                                | "union"
                        )
                    {
                        self.reject(format!(
                            "clinker-core-types macro tokens contain forbidden identifier {identifier}"
                        ));
                        return;
                    }
                }
                TokenTree::Group(group) => pending.extend(group.stream()),
                TokenTree::Literal(_) | TokenTree::Punct(_) => {}
            }
        }
    }

    fn inspect_macro(&mut self, item: &syn::Macro) {
        if path_is_single_ident(&item.path, "include") {
            self.reject(
                "clinker-core-types uses include! in production source; expanded source cannot be audited",
            );
            return;
        }
        self.inspect_macro_tokens(item.tokens.clone());
    }

    fn inspect_type_path(&mut self, path: &syn::Path) {
        let segments: Vec<String> = path
            .segments
            .iter()
            .map(|segment| identifier_name(&segment.ident))
            .collect();
        if let Some(identifier) = segments
            .iter()
            .find(|identifier| FORBIDDEN_CORE_IDENTIFIERS.contains(&identifier.as_str()))
        {
            self.reject(format!(
                "clinker-core-types references forbidden foreign layer or identity type {identifier}"
            ));
            return;
        }
        let serialization = matches!(
            segments.as_slice(),
            [root, ..] if root == "serde" || root == "serde_json"
        );
        let transport_or_clock = matches!(
            segments.as_slice(),
            [root, subsystem, ..]
                if root == "std" && matches!(subsystem.as_str(), "net" | "time")
        );
        if serialization {
            self.reject("clinker-core-types must remain serialization-neutral");
        } else if transport_or_clock {
            self.reject("clinker-core-types must not own transport or clock types");
        }
    }
}

impl<'ast> Visit<'ast> for CoreVisitor {
    fn visit_item(&mut self, item: &'ast Item) {
        if exact_cfg_test(item_attrs(item)) {
            return;
        }
        visit::visit_item(self, item);
    }

    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        let name = attribute
            .path()
            .segments
            .first()
            .map(|segment| identifier_name(&segment.ident));
        let allowed = name.as_deref().is_some_and(|name| {
            matches!(
                name,
                "allow"
                    | "cfg"
                    | "cold"
                    | "deny"
                    | "deprecated"
                    | "derive"
                    | "doc"
                    | "expect"
                    | "forbid"
                    | "inline"
                    | "must_use"
                    | "non_exhaustive"
                    | "repr"
                    | "test"
                    | "track_caller"
                    | "warn"
            )
        });
        if !allowed {
            self.reject(format!(
                "clinker-core-types uses unsupported production attribute {}; macro expansion cannot be audited",
                name.as_deref().unwrap_or("<empty>")
            ));
            return;
        }
        if attribute.path().is_ident("derive") {
            let derives = parse_derive_paths(attribute);
            match derives {
                Ok(derives)
                    if derives.iter().all(|path| {
                        path.segments.last().is_some_and(|segment| {
                            matches!(
                                identifier_name(&segment.ident).as_str(),
                                "Clone"
                                    | "Copy"
                                    | "Debug"
                                    | "Default"
                                    | "Eq"
                                    | "Hash"
                                    | "Ord"
                                    | "PartialEq"
                                    | "PartialOrd"
                            )
                        })
                    }) => {}
                Ok(_) => self.reject(
                    "clinker-core-types uses an unapproved derive and must remain serialization-neutral",
                ),
                Err(error) => self.reject(format!(
                    "clinker-core-types has an unsupported derive attribute: {error}"
                )),
            }
        }
        visit::visit_attribute(self, attribute);
    }

    fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
        if path_is_single_ident(&item.mac.path, "include") {
            self.inspect_macro(&item.mac);
            return;
        }
        let definition = path_is_single_ident(&item.mac.path, "macro_rules")
            && item.ident.as_ref().is_some_and(|identifier| {
                matches!(
                    identifier_name(identifier).as_str(),
                    "diagnostic_registry" | "failure_registry"
                )
            });
        let invocation = item.ident.is_none()
            && item.mac.path.segments.last().is_some_and(|segment| {
                matches!(
                    identifier_name(&segment.ident).as_str(),
                    "diagnostic_registry" | "failure_registry"
                )
            });
        let failure_registry = item
            .ident
            .as_ref()
            .is_some_and(|identifier| identifier_name(identifier) == "failure_registry")
            || item
                .mac
                .path
                .segments
                .last()
                .is_some_and(|segment| identifier_name(&segment.ident) == "failure_registry");
        if !definition && !invocation {
            self.reject(format!(
                "clinker-core-types has an unapproved item-level macro definition or expansion: {:?}",
                item.ident
            ));
            return;
        }
        if failure_registry && token_stream_contains_identifier(item.mac.tokens.clone(), "pub") {
            self.reject(
                "clinker-core-types failure_registry macro must not generate public surface",
            );
            return;
        }
        self.inspect_macro(&item.mac);
    }

    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        self.inspect_macro(item);
    }

    fn visit_type_path(&mut self, type_path: &'ast syn::TypePath) {
        self.inspect_type_path(&type_path.path);
        if self.error.is_none() {
            visit::visit_type_path(self, type_path);
        }
    }

    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        if let Some((_, trait_path, _)) = &item.trait_ {
            self.inspect_type_path(trait_path);
        }
        if self.error.is_none() {
            visit::visit_item_impl(self, item);
        }
    }

    fn visit_trait_bound(&mut self, bound: &'ast syn::TraitBound) {
        self.inspect_type_path(&bound.path);
        if self.error.is_none() {
            visit::visit_trait_bound(self, bound);
        }
    }

    fn visit_item_enum(&mut self, item: &'ast ItemEnum) {
        self.reject_foreign_identity(&item.ident);
        visit::visit_item_enum(self, item);
    }

    fn visit_item_struct(&mut self, item: &'ast ItemStruct) {
        self.reject_foreign_identity(&item.ident);
        visit::visit_item_struct(self, item);
    }

    fn visit_item_trait(&mut self, item: &'ast ItemTrait) {
        self.reject_foreign_identity(&item.ident);
        visit::visit_item_trait(self, item);
    }

    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        self.reject_foreign_identity(&item.ident);
        visit::visit_item_type(self, item);
    }

    fn visit_item_union(&mut self, item: &'ast ItemUnion) {
        self.reject_foreign_identity(&item.ident);
        visit::visit_item_union(self, item);
    }
}

fn check_core_failure_exports(root: &Path) -> BoundaryResult<()> {
    let path = crate_source_root(root, "clinker-core-types").join("lib.rs");
    let text = fs::read_to_string(&path)
        .map_err(|error| BoundaryError::new(format!("cannot read {}: {error}", path.display())))?;
    let syntax = syn::parse_file(&text)
        .map_err(|error| BoundaryError::new(format!("cannot parse {}: {error}", path.display())))?;
    let mut public_failure_module = false;
    let mut exports = BTreeSet::new();
    for item in syntax.items {
        if exact_cfg_test(item_attrs(&item)) {
            continue;
        }
        match item {
            Item::Mod(module)
                if identifier_name(&module.ident) == "failure" && visible(&module.vis) =>
            {
                if module.content.is_some()
                    || module
                        .attrs
                        .iter()
                        .any(|attribute| attribute.path().is_ident("path"))
                {
                    return Err(BoundaryError::new(
                        "clinker-core-types failure module must retain its approved external source",
                    ));
                }
                public_failure_module = true;
            }
            Item::Use(item_use) if visible(&item_use.vis) => {
                for leaf in expand_use_tree(&item_use.tree, Vec::new())? {
                    if leaf.path.segments.first().map(String::as_str) == Some("failure")
                        && let Some(name) = leaf.path.segments.get(1)
                    {
                        if leaf.alias.as_deref().is_some_and(|alias| alias != name) {
                            return Err(BoundaryError::new(format!(
                                "clinker-core-types must export {name} under its canonical name"
                            )));
                        }
                        exports.insert(name.clone());
                    }
                }
            }
            _ => {}
        }
    }
    if !public_failure_module {
        return Err(BoundaryError::new(
            "clinker-core-types must expose the approved failure module",
        ));
    }
    let expected: BTreeSet<String> = allowed_shared_types()
        .iter()
        .map(|value| (*value).to_owned())
        .collect();
    if exports != expected {
        return Err(BoundaryError::new(format!(
            "clinker-core-types failure export must contain only the three approved shared types; expected={expected:?}, actual={exports:?}"
        )));
    }

    let failure_path = crate_source_root(root, "clinker-core-types").join("failure.rs");
    let failure_text = fs::read_to_string(&failure_path).map_err(|error| {
        BoundaryError::new(format!("cannot read {}: {error}", failure_path.display()))
    })?;
    let failure_syntax = syn::parse_file(&failure_text).map_err(|error| {
        BoundaryError::new(format!("cannot parse {}: {error}", failure_path.display()))
    })?;
    let mut public_types = BTreeSet::new();
    for item in &failure_syntax.items {
        let public_type = match item {
            Item::Enum(item) if visible(&item.vis) => Some(identifier_name(&item.ident)),
            Item::Struct(item) if visible(&item.vis) => Some(identifier_name(&item.ident)),
            Item::Trait(item) if visible(&item.vis) => Some(identifier_name(&item.ident)),
            Item::Type(item) if visible(&item.vis) => Some(identifier_name(&item.ident)),
            Item::Union(item) if visible(&item.vis) => Some(identifier_name(&item.ident)),
            Item::Mod(item) if visible(&item.vis) => {
                return Err(BoundaryError::new(format!(
                    "clinker-core-types failure module must not expose nested module {}",
                    item.ident
                )));
            }
            Item::Use(item) if visible(&item.vis) => {
                return Err(BoundaryError::new(
                    "clinker-core-types failure module must not expose public re-exports",
                ));
            }
            Item::ExternCrate(item) if visible(&item.vis) => {
                return Err(BoundaryError::new(
                    "clinker-core-types failure module must not expose an extern crate",
                ));
            }
            _ => None,
        };
        if let Some(public_type) = public_type {
            public_types.insert(public_type);
        }
    }
    if public_types != expected {
        return Err(BoundaryError::new(format!(
            "clinker-core-types failure module must own exactly the three approved public types; expected={expected:?}, actual={public_types:?}"
        )));
    }
    Ok(())
}
