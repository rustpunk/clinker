//! Channel-driven source-config patches.
//!
//! A channel can override any part of a source node's parsed config through
//! partial, path-addressed patches applied to the typed [`PipelineConfig`]
//! *before* validation and compile. The patched config is exactly what
//! `validate_config` sees and what `compile()` consumes, so a run behaves as
//! if the operator had hand-edited the source YAML.
//!
//! The patch is applied by mutating the already-parsed typed config in place —
//! the span-aware `Spanned<PipelineNode>` structure is preserved, never
//! round-tripped through a span-losing intermediate value.
//!
//! v1 handles the format-agnostic schema-shaping surfaces: the CXL-typed
//! column list (`schema`), nested-array explosion/join (`array_paths`), and
//! scalar per-format input options (`options`). Deeper format-layer layouts
//! (fixed-width/positional field schemas, X12/HL7 nested sections, multi-record
//! discrimination) reuse this same framework as additional handlers.

use super::*;
use crate::config::pipeline_node::{ColumnDecl, PipelineNode, SchemaDecl, SourceBody};
use cxl::typecheck::Type;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::de;

/// Per-source config patch carried by a channel.
///
/// Keyed forms only — leaf-replace, never deep-merge. `schema` ops are keyed
/// by column name, `array_paths` ops by array path, and `options` sets a
/// scalar per-format input option by its key.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SourceConfigPatch {
    /// Column-name-keyed schema ops (`add` / `rename` / `retype` / `remove`).
    pub schema: IndexMap<String, SchemaColumnOp>,
    /// Array-path-keyed ops: set-by-path (add-or-modify) or `remove`.
    pub array_paths: IndexMap<String, ArrayPathOp>,
    /// Scalar per-format input options, keyed by option name. Merged onto the
    /// source's current options and re-validated through the format's option
    /// struct, so an unknown key is rejected exactly as in hand-written config.
    pub options: IndexMap<String, serde_json::Value>,
}

impl SourceConfigPatch {
    /// Whether this patch carries no ops — a no-op the apply pass can skip.
    pub fn is_empty(&self) -> bool {
        self.schema.is_empty() && self.array_paths.is_empty() && self.options.is_empty()
    }
}

/// One column-keyed schema op.
///
/// YAML forms (the map key is the target column name):
///
/// ```yaml
/// amount:      { retype: float }        # change an existing column's type
/// cust_id:     { rename: customer_id }  # rename an existing column
/// order_notes: remove                   # drop an existing column
/// region:      { add: { type: string } }# add a new column (key = new name)
/// ```
#[derive(Debug, Clone)]
pub enum SchemaColumnOp {
    /// Drop the keyed column.
    Remove,
    /// Rename the keyed column to the given name.
    Rename(String),
    /// Change the keyed column's CXL type.
    Retype(Type),
    /// Add a new column named by the map key.
    Add(AddColumnPatch),
}

/// Payload for a schema `add` op: the new column's declared shape.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddColumnPatch {
    /// CXL type of the new column.
    #[serde(rename = "type")]
    pub ty: Type,
    /// Mirrors [`ColumnDecl::long_unique`] — advisory storage hint.
    #[serde(default)]
    pub long_unique: bool,
}

impl<'de> Deserialize<'de> for SchemaColumnOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Two accepted YAML shapes:
        //   remove                         (bare scalar)
        //   { rename | retype | add: ... } (single-key map)
        // The bare scalar is handled explicitly; the map form accepts exactly
        // one of the three op keys.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct OpMap {
            #[serde(default)]
            rename: Option<String>,
            #[serde(default)]
            retype: Option<Type>,
            #[serde(default)]
            add: Option<AddColumnPatch>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Scalar(String),
            Map(OpMap),
        }

        match Either::deserialize(d)? {
            Either::Scalar(s) if s == "remove" => Ok(SchemaColumnOp::Remove),
            Either::Scalar(other) => Err(de::Error::custom(format!(
                "unknown schema op {other:?}; the bare scalar form only accepts `remove` — \
                 use `{{ rename: <name> }}`, `{{ retype: <type> }}`, or `{{ add: {{ type: <type> }} }}`"
            ))),
            Either::Map(m) => {
                let count =
                    m.rename.is_some() as u8 + m.retype.is_some() as u8 + m.add.is_some() as u8;
                match count {
                    0 => Err(de::Error::custom(
                        "empty schema op; provide exactly one of `rename`, `retype`, `add`, \
                         or the bare scalar `remove`",
                    )),
                    1 => {
                        if let Some(to) = m.rename {
                            Ok(SchemaColumnOp::Rename(to))
                        } else if let Some(t) = m.retype {
                            Ok(SchemaColumnOp::Retype(t))
                        } else {
                            Ok(SchemaColumnOp::Add(m.add.expect("count==1 with add set")))
                        }
                    }
                    _ => Err(de::Error::custom(
                        "ambiguous schema op; provide exactly one of `rename`, `retype`, `add`",
                    )),
                }
            }
        }
    }
}

/// One array-path-keyed op.
///
/// YAML forms (the map key is the array path):
///
/// ```yaml
/// items:      { mode: join, separator: ";" }  # add-or-modify the entry
/// line_items: remove                          # drop the entry
/// ```
#[derive(Debug, Clone)]
pub enum ArrayPathOp {
    /// Drop the keyed array-path entry.
    Remove,
    /// Add-or-modify the keyed array-path entry (leaf-replace of the whole
    /// entry — omitted fields take their defaults).
    Set {
        /// Explode (default) or join.
        mode: ArrayMode,
        /// Join separator; ignored for explode mode.
        separator: Option<String>,
    },
}

impl<'de> Deserialize<'de> for ArrayPathOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Two accepted YAML shapes:
        //   remove                       (bare scalar)
        //   { mode?, separator? }        (entry map)
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SetMap {
            #[serde(default)]
            mode: ArrayMode,
            #[serde(default)]
            separator: Option<String>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Scalar(String),
            Map(SetMap),
        }

        match Either::deserialize(d)? {
            Either::Scalar(s) if s == "remove" => Ok(ArrayPathOp::Remove),
            Either::Scalar(other) => Err(de::Error::custom(format!(
                "unknown array_paths op {other:?}; the bare scalar form only accepts `remove` — \
                 use `{{ mode: explode|join, separator: <str> }}` to add or modify an entry"
            ))),
            Either::Map(m) => Ok(ArrayPathOp::Set {
                mode: m.mode,
                separator: m.separator,
            }),
        }
    }
}

/// Apply channel source-config patches to the parsed config in place.
///
/// Runs after parse and before `validate_config`, so the patched config is
/// the one validated and compiled. Each entry names a source node; an unknown
/// source name or an ill-formed op is a config error (E230–E235) reported
/// before the pipeline compiles.
pub fn apply_source_patches(
    config: &mut PipelineConfig,
    patches: &IndexMap<String, SourceConfigPatch>,
) -> Result<(), ConfigError> {
    for (src_name, patch) in patches {
        if patch.is_empty() {
            // Still resolve the source so an empty patch on a typo'd name
            // fails rather than passing silently.
            source_body_mut(config, src_name).ok_or_else(|| unknown_source(src_name))?;
            continue;
        }
        let body = source_body_mut(config, src_name).ok_or_else(|| unknown_source(src_name))?;
        apply_schema_ops(&mut body.schema, &patch.schema, src_name)?;
        apply_array_path_ops(&mut body.source, &patch.array_paths, src_name)?;
        apply_option_ops(&mut body.source, &patch.options, src_name)?;
    }
    Ok(())
}

/// Locate a source node's body by its declared name.
fn source_body_mut<'a>(config: &'a mut PipelineConfig, name: &str) -> Option<&'a mut SourceBody> {
    config
        .nodes
        .iter_mut()
        .find_map(|spanned| match &mut spanned.value {
            PipelineNode::Source { config: body, .. } if body.source.name == name => Some(body),
            _ => None,
        })
}

fn unknown_source(src_name: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E230] channel source patch targets unknown source '{src_name}': \
         no source node by that name in the pipeline"
    ))
}

fn unknown_column(src: &str, col: &str, op: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E231] channel schema patch on source '{src}': {op} of unknown column '{col}'"
    ))
}

fn apply_schema_ops(
    schema: &mut SchemaDecl,
    ops: &IndexMap<String, SchemaColumnOp>,
    src: &str,
) -> Result<(), ConfigError> {
    for (col, op) in ops {
        match op {
            SchemaColumnOp::Remove => {
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == *col)
                    .ok_or_else(|| unknown_column(src, col, "remove"))?;
                schema.columns.remove(idx);
            }
            SchemaColumnOp::Retype(ty) => {
                let column = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name == *col)
                    .ok_or_else(|| unknown_column(src, col, "retype"))?;
                column.ty = ty.clone();
            }
            SchemaColumnOp::Rename(to) => {
                if !schema.columns.iter().any(|c| c.name == *col) {
                    return Err(unknown_column(src, col, "rename"));
                }
                if to != col && schema.columns.iter().any(|c| c.name == *to) {
                    return Err(ConfigError::Validation(format!(
                        "[E233] channel schema patch on source '{src}': rename of '{col}' to \
                         '{to}' collides with an existing column"
                    )));
                }
                let column = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name == *col)
                    .expect("existence checked above");
                column.name = to.clone();
            }
            SchemaColumnOp::Add(add) => {
                if schema.columns.iter().any(|c| c.name == *col) {
                    return Err(ConfigError::Validation(format!(
                        "[E232] channel schema patch on source '{src}': add of column '{col}' \
                         that already exists"
                    )));
                }
                schema.columns.push(ColumnDecl {
                    name: col.clone(),
                    ty: add.ty.clone(),
                    long_unique: add.long_unique,
                });
            }
        }
    }
    Ok(())
}

fn apply_array_path_ops(
    source: &mut SourceConfig,
    ops: &IndexMap<String, ArrayPathOp>,
    src: &str,
) -> Result<(), ConfigError> {
    for (path, op) in ops {
        match op {
            ArrayPathOp::Remove => {
                let Some(entries) = source.array_paths.as_mut() else {
                    return Err(unknown_array_path(src, path));
                };
                let Some(idx) = entries.iter().position(|a| a.path == *path) else {
                    return Err(unknown_array_path(src, path));
                };
                entries.remove(idx);
            }
            ArrayPathOp::Set { mode, separator } => {
                let entries = source.array_paths.get_or_insert_with(Vec::new);
                if let Some(existing) = entries.iter_mut().find(|a| a.path == *path) {
                    existing.mode = mode.clone();
                    existing.separator = separator.clone();
                } else {
                    entries.push(ArrayPathConfig {
                        path: path.clone(),
                        mode: mode.clone(),
                        separator: separator.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

fn unknown_array_path(src: &str, path: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E234] channel array_paths patch on source '{src}': remove of unknown path '{path}'"
    ))
}

/// Merge scalar option overrides onto the source's current per-format options.
///
/// Serializes the current options to a JSON object, sets each override key,
/// then re-deserializes through the format's option struct — reusing its
/// `deny_unknown_fields` so an unknown or mistyped option is rejected (E235)
/// exactly as it would be in hand-written config. The round-trip touches only
/// the leaf option struct, never the span-aware pipeline-node structure.
fn apply_option_ops(
    source: &mut SourceConfig,
    overrides: &IndexMap<String, serde_json::Value>,
    src: &str,
) -> Result<(), ConfigError> {
    if overrides.is_empty() {
        return Ok(());
    }
    let fmt = source.format.format_name();

    macro_rules! patch_options {
        ($cur:expr, $ty:ty, $wrap:path) => {{
            let mut value = match $cur {
                Some(existing) => {
                    serde_json::to_value(existing).map_err(|e| options_internal(src, &e))?
                }
                None => serde_json::Value::Object(serde_json::Map::new()),
            };
            let obj = value
                .as_object_mut()
                .expect("format input options serialize to a JSON object");
            for (key, val) in overrides {
                obj.insert(key.clone(), val.clone());
            }
            let parsed: $ty =
                serde_json::from_value(value).map_err(|e| options_rejected(src, fmt, &e))?;
            $wrap(Some(parsed))
        }};
    }

    let new_format = match &source.format {
        InputFormat::Csv(o) => patch_options!(o, CsvInputOptions, InputFormat::Csv),
        InputFormat::Json(o) => patch_options!(o, JsonInputOptions, InputFormat::Json),
        InputFormat::Xml(o) => patch_options!(o, XmlInputOptions, InputFormat::Xml),
        InputFormat::FixedWidth(o) => {
            patch_options!(o, FixedWidthInputOptions, InputFormat::FixedWidth)
        }
        InputFormat::Edifact(o) => patch_options!(o, EdifactInputOptions, InputFormat::Edifact),
        InputFormat::X12(o) => patch_options!(o, X12InputOptions, InputFormat::X12),
        InputFormat::Hl7(o) => patch_options!(o, Hl7InputOptions, InputFormat::Hl7),
        InputFormat::Swift(o) => patch_options!(o, SwiftInputOptions, InputFormat::Swift),
    };
    source.format = new_format;
    Ok(())
}

fn options_rejected(src: &str, fmt: &str, err: &serde_json::Error) -> ConfigError {
    ConfigError::Validation(format!(
        "[E235] channel options patch on source '{src}' ({fmt} format): {err}"
    ))
}

fn options_internal(src: &str, err: &serde_json::Error) -> ConfigError {
    ConfigError::Validation(format!(
        "[E235] channel options patch on source '{src}': could not read current options: {err}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A CSV source named `src` with three declared columns, feeding one
    /// output. Parsed (not validated), which is all the patch handlers need.
    fn csv_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/in.csv
      schema:
        - { name: amount, type: int }
        - { name: cust_id, type: string }
        - { name: order_notes, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse csv pipeline")
    }

    fn patch_from_yaml(yaml: &str) -> SourceConfigPatch {
        crate::yaml::from_str(yaml).expect("parse patch")
    }

    fn columns(config: &PipelineConfig) -> Vec<(String, Type)> {
        config
            .source_bodies()
            .next()
            .expect("one source")
            .schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.ty.clone()))
            .collect()
    }

    fn apply(
        config: &mut PipelineConfig,
        source: &str,
        patch: SourceConfigPatch,
    ) -> Result<(), ConfigError> {
        let mut patches = IndexMap::new();
        patches.insert(source.to_string(), patch);
        apply_source_patches(config, &patches)
    }

    #[test]
    fn schema_retype_changes_type() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { retype: float }\n"),
        )
        .unwrap();
        assert_eq!(columns(&config)[0], ("amount".to_string(), Type::Float));
    }

    #[test]
    fn schema_rename_renames_column() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  cust_id: { rename: customer_id }\n"),
        )
        .unwrap();
        assert_eq!(columns(&config)[1].0, "customer_id");
    }

    #[test]
    fn schema_remove_drops_column() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  order_notes: remove\n"),
        )
        .unwrap();
        let names: Vec<String> = columns(&config).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["amount".to_string(), "cust_id".to_string()]);
    }

    #[test]
    fn schema_add_appends_column() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  region: { add: { type: string } }\n"),
        )
        .unwrap();
        let cols = columns(&config);
        assert_eq!(cols.last().unwrap(), &("region".to_string(), Type::String));
    }

    #[test]
    fn schema_add_long_unique_flag_flows_through() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  uuid: { add: { type: string, long_unique: true } }\n"),
        )
        .unwrap();
        let body = config.source_bodies().next().unwrap();
        let col = body
            .schema
            .columns
            .iter()
            .find(|c| c.name == "uuid")
            .unwrap();
        assert!(col.long_unique);
    }

    #[test]
    fn schema_combined_ops_apply_in_order() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml(
                "schema:\n  amount: { retype: float }\n  cust_id: { rename: customer_id }\n  order_notes: remove\n  region: { add: { type: string } }\n",
            ),
        )
        .unwrap();
        assert_eq!(
            columns(&config),
            vec![
                ("amount".to_string(), Type::Float),
                ("customer_id".to_string(), Type::String),
                ("region".to_string(), Type::String),
            ]
        );
    }

    #[test]
    fn schema_retype_unknown_column_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  nope: { retype: float }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E231"), "{err}");
    }

    #[test]
    fn schema_rename_unknown_column_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  nope: { rename: x }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E231"), "{err}");
    }

    #[test]
    fn schema_remove_unknown_column_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  nope: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E231"), "{err}");
    }

    #[test]
    fn schema_add_existing_column_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { add: { type: float } }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E232"), "{err}");
    }

    #[test]
    fn schema_rename_collision_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { rename: cust_id }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E233"), "{err}");
    }

    #[test]
    fn schema_rename_to_same_name_is_allowed() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { rename: amount }\n"),
        )
        .unwrap();
        assert_eq!(columns(&config)[0].0, "amount");
    }

    #[test]
    fn unknown_source_name_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "ghost",
            patch_from_yaml("schema:\n  amount: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E230"), "{err}");
    }

    /// A JSON source declaring a `record_path` option and one array-path entry.
    fn json_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: /tmp/in.json
      options:
        record_path: data.rows
      array_paths:
        - { path: items, mode: explode }
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse json pipeline")
    }

    fn array_paths(config: &PipelineConfig) -> Vec<(String, ArrayMode, Option<String>)> {
        config
            .source_configs()
            .next()
            .unwrap()
            .array_paths
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|a| (a.path, a.mode, a.separator))
            .collect()
    }

    #[test]
    fn array_paths_modify_existing_entry() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  items: { mode: join, separator: \";\" }\n"),
        )
        .unwrap();
        let entries = array_paths(&config);
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0].1, ArrayMode::Join));
        assert_eq!(entries[0].2.as_deref(), Some(";"));
    }

    #[test]
    fn array_paths_add_new_entry() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  tags: { mode: explode }\n"),
        )
        .unwrap();
        let entries = array_paths(&config);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|(p, _, _)| p == "tags"));
    }

    #[test]
    fn array_paths_remove_entry() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  items: remove\n"),
        )
        .unwrap();
        assert!(array_paths(&config).is_empty());
    }

    #[test]
    fn array_paths_remove_unknown_errors() {
        let mut config = json_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  ghost: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E234"), "{err}");
    }

    #[test]
    fn options_set_scalar_record_path() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("options:\n  record_path: batch_records\n"),
        )
        .unwrap();
        let source = config.source_configs().next().unwrap();
        match &source.format {
            InputFormat::Json(Some(opts)) => {
                assert_eq!(opts.record_path.as_deref(), Some("batch_records"));
            }
            other => panic!("expected json options, got {other:?}"),
        }
    }

    #[test]
    fn options_unknown_key_errors() {
        let mut config = json_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("options:\n  not_a_key: 5\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E235"), "{err}");
    }

    #[test]
    fn options_set_preserves_existing_keys() {
        // record_path was `data.rows`; overriding an unrelated csv-ish key is
        // rejected, but setting `format` should keep record_path intact.
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("options:\n  format: ndjson\n"),
        )
        .unwrap();
        let source = config.source_configs().next().unwrap();
        match &source.format {
            InputFormat::Json(Some(opts)) => {
                assert_eq!(opts.record_path.as_deref(), Some("data.rows"));
                assert!(matches!(opts.format, Some(JsonFormat::Ndjson)));
            }
            other => panic!("expected json options, got {other:?}"),
        }
    }

    #[test]
    fn options_on_source_without_options_block() {
        // A CSV source with no `options:` starts from an empty option struct;
        // the override materializes it.
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("options:\n  delimiter: \"|\"\n"),
        )
        .unwrap();
        let source = config.source_configs().next().unwrap();
        match &source.format {
            InputFormat::Csv(Some(opts)) => assert_eq!(opts.delimiter.as_deref(), Some("|")),
            other => panic!("expected csv options, got {other:?}"),
        }
    }

    #[test]
    fn empty_patch_on_unknown_source_still_errors() {
        let mut config = csv_pipeline();
        let err = apply(&mut config, "ghost", SourceConfigPatch::default()).unwrap_err();
        assert!(err.to_string().contains("E230"), "{err}");
    }

    #[test]
    fn schema_op_ambiguous_map_rejected_at_parse() {
        let err = crate::yaml::from_str::<SourceConfigPatch>(
            "schema:\n  amount: { rename: x, retype: float }\n",
        )
        .unwrap_err();
        assert!(
            format!("{}", err.0).contains("exactly one")
                || format!("{}", err.0).contains("ambiguous"),
            "{}",
            err.0
        );
    }
}
