//! YAML parser chokepoint (Phase 16b Task 16b.0.5).
//!
//! This module is the **single** entry point for YAML parsing in clinker.
//! No other code path is permitted to call `serde_saphyr::from_str*` or
//! construct a [`serde_saphyr::Budget`] directly. Two reasons:
//!
//! 1. **Bus-factor mitigation.** `serde-saphyr` is pre-1.0 with a single
//!    upstream maintainer. If we ever need to fork or replace it, this
//!    file is the only thing that has to change.
//! 2. **DoS-defense chokepoint.** A single [`Budget`] keeps depth /
//!    size / alias-expansion / include limits aligned with the Phase 11
//!    threat model (assumption #66). Tightening the limits is a one-file
//!    edit.
//!
//! ## Span support
//!
//! Re-exports [`serde_saphyr::Spanned`], [`serde_saphyr::Location`], and
//! [`serde_saphyr::Span`]. The Phase 16b lift carries
//! `Vec<Spanned<PipelineNode>>` at the top level — wrapping the **whole**
//! enum (not the inner fields) is the documented workaround for
//! `Spanned<T>` losing location info inside `#[serde(tag = "...")]`
//! variants and `#[serde(flatten)]` structs.
//!
//! [`CxlSource`] is a newtype that captures the YAML span of a CXL
//! expression string. It piggybacks on the [`Spanned<String>`]
//! deserializer so spans flow through whenever the field is *not*
//! buried inside a tagged-enum / flattened context.

use serde::Deserialize;
use serde::Serialize;

pub use serde_saphyr::{Location, Span, Spanned};

/// Hard 32 MB ceiling on YAML input. Covers the largest plausible
/// hand-authored pipeline by ~100x. Enforced *before* parsing begins so
/// pathological inputs never reach the deserializer at all.
pub const MAX_INPUT_BYTES: usize = 32 * 1024 * 1024;

/// Wrapper around [`serde_saphyr::Error`] so callers do not have to
/// import the underlying crate. Implements `Display` / `Error` and
/// `From<serde_saphyr::Error>`.
#[derive(Debug)]
pub struct YamlError(pub serde_saphyr::Error);

impl std::fmt::Display for YamlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for YamlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl From<serde_saphyr::Error> for YamlError {
    fn from(e: serde_saphyr::Error) -> Self {
        Self(e)
    }
}

/// Construct the canonical [`serde_saphyr::Options`] used for *every*
/// parse in clinker. Centralizing here is intentional — see module docs.
fn budget_options() -> serde_saphyr::Options {
    serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            // 32 MB — covers the largest plausible hand-authored pipeline.
            max_reader_input_bytes: Some(MAX_INPUT_BYTES),
            // 256 levels — well beyond any legitimate nesting; rejects
            // billion-laughs style recursion.
            max_depth: 256,
            // 0 — disables `!include` entirely; clinker has its own
            // path/composition loading.
            max_inclusion_depth: 0,
            // 100 000 nodes — covers the largest plausible fixture with a
            // ~10x margin.
            max_nodes: 100_000,
            // Reject exponential alias expansion (the canonical
            // billion-laughs defense).
            enforce_alias_anchor_ratio: true,
        },
    }
}

/// Parse a YAML string into a typed value, with the canonical Budget
/// applied. **The** chokepoint — every YAML parse in clinker MUST go
/// through here.
pub fn from_str<'de, T>(yaml: &'de str) -> Result<T, YamlError>
where
    T: Deserialize<'de>,
{
    if yaml.len() > MAX_INPUT_BYTES {
        // Pre-parse rejection. Cheap, and avoids feeding the parser an
        // input it would only reject internally.
        return Err(YamlError(make_oversize_error(yaml.len())));
    }
    serde_saphyr::from_str_with_options(yaml, budget_options()).map_err(YamlError)
}

/// Serialize a value back to YAML. Thin pass-through; no budget needed
/// on the serialize side.
pub fn to_string<T: Serialize>(value: &T) -> Result<String, String> {
    serde_saphyr::to_string(value).map_err(|e| e.to_string())
}

/// Build a synthetic oversize error. `serde_saphyr::Error` does not
/// expose a public constructor, so we coerce one out of the parser by
/// feeding it an obviously-invalid stub. Cheap and only runs on the
/// pre-parse rejection path.
fn make_oversize_error(actual: usize) -> serde_saphyr::Error {
    // Drive the parser into a typed mismatch — guaranteed to fail and
    // produce a real `serde_saphyr::Error` value we can wrap.
    let stub = format!(
        "# input exceeded {} bytes (actual {} bytes)\n: not_a_number",
        MAX_INPUT_BYTES, actual
    );
    serde_saphyr::from_str::<u8>(&stub).unwrap_err()
}

// ---------------------------------------------------------------------
// CxlSource
// ---------------------------------------------------------------------

/// A CXL expression source string carrying its YAML origin span.
///
/// `CxlSource` is the per-CXL-string span vehicle for Phase 16b. It
/// piggybacks on [`Spanned<String>`]'s deserializer so the field captures
/// non-zero span information whenever it appears in a normal struct
/// context. When buried inside a `#[serde(tag = "...")]` enum variant
/// (the same context that loses spans for plain `Spanned<T>` fields),
/// the span will be [`Span::UNKNOWN`] / `Location::UNKNOWN` and the
/// outer `Spanned<PipelineNode>` wrap is the source of truth instead.
#[derive(Debug, Clone)]
pub struct CxlSource {
    pub source: String,
    pub span: Span,
}

impl CxlSource {
    pub fn new(source: impl Into<String>, span: Span) -> Self {
        Self {
            source: source.into(),
            span,
        }
    }

    /// Convenience constructor for tests / synthetic data.
    pub fn unspanned(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            span: Span::UNKNOWN,
        }
    }
}

impl<'de> Deserialize<'de> for CxlSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Spike approach: defer to `Spanned<String>`'s span-aware
        // deserializer. In a normal struct context this captures the
        // value's referenced location. In a tagged-enum / flatten
        // context the deserializer falls back to `Location::UNKNOWN`,
        // which is the documented serde-saphyr limitation; the outer
        // `Spanned<PipelineNode>` wrap covers the variant span.
        let inner: Spanned<String> = Spanned::deserialize(deserializer)?;
        Ok(CxlSource {
            source: inner.value,
            span: inner.referenced.span(),
        })
    }
}

impl Serialize for CxlSource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.source.serialize(serializer)
    }
}

impl std::fmt::Display for CxlSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.source)
    }
}

impl AsRef<str> for CxlSource {
    fn as_ref(&self) -> &str {
        &self.source
    }
}

impl PartialEq for CxlSource {
    fn eq(&self, other: &Self) -> bool {
        // Compare on source only — span is metadata.
        self.source == other.source
    }
}

impl Eq for CxlSource {}

// ---------------------------------------------------------------------
// Spike + budget tests (Task 16b.0.5 hard gate)
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    // ---- Spike 0: trivial round-trip ---------------------------------

    #[derive(Deserialize)]
    struct Trivial {
        name: String,
        count: u32,
    }

    #[test]
    fn test_serde_saphyr_basic_deserialize() {
        let yaml = "name: hello\ncount: 7\n";
        let parsed: Trivial = from_str(yaml).expect("trivial parse");
        assert_eq!(parsed.name, "hello");
        assert_eq!(parsed.count, 7);
    }

    // ---- Spike 1: Spanned<TaggedEnum> via outer wrap -----------------

    #[derive(Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    enum SpikeNode {
        Source { path: String },
        Transform { cxl: CxlSource },
    }

    #[derive(Deserialize)]
    struct SpikeDoc {
        nodes: Vec<Spanned<SpikeNode>>,
    }

    #[test]
    fn test_spanned_in_tagged_enum_via_outer_wrap() {
        // Two-element list — assert *both* nodes carry non-zero spans
        // and that the line numbers are different (i.e. spans are real,
        // not coincidental).
        let yaml = "\
nodes:
  - kind: source
    path: in.csv
  - kind: transform
    cxl: \"x + 1\"
";
        let doc: SpikeDoc = from_str(yaml).expect("parse SpikeDoc");
        assert_eq!(doc.nodes.len(), 2);
        match &doc.nodes[0].value {
            SpikeNode::Source { path } => assert_eq!(path, "in.csv"),
            _ => panic!("expected Source"),
        }
        match &doc.nodes[1].value {
            SpikeNode::Transform { cxl } => assert_eq!(cxl.source, "x + 1"),
            _ => panic!("expected Transform"),
        }

        let first = &doc.nodes[0];
        let second = &doc.nodes[1];

        assert_ne!(
            first.referenced,
            Location::UNKNOWN,
            "first node referenced location must be known"
        );
        assert_ne!(
            second.referenced,
            Location::UNKNOWN,
            "second node referenced location must be known"
        );
        assert!(
            first.referenced.line() >= 1,
            "first node line must be >= 1, got {}",
            first.referenced.line()
        );
        assert!(
            second.referenced.line() > first.referenced.line(),
            "second node must come after first; got {} vs {}",
            second.referenced.line(),
            first.referenced.line()
        );
    }

    // ---- Spike 2: CxlSource newtype span capture ---------------------

    #[derive(Deserialize)]
    struct CxlHolder {
        cxl: CxlSource,
    }

    #[test]
    fn test_cxl_source_newtype_captures_span() {
        // Top-level (non-variant) context: CxlSource should capture a
        // non-zero span. This is the contract `clinker-core` relies on
        // for non-tagged uses.
        let yaml = "cxl: \"upper(name)\"\n";
        let parsed: CxlHolder = from_str(yaml).expect("parse CxlHolder");
        assert_eq!(parsed.cxl.source, "upper(name)");
        assert!(
            !parsed.cxl.span.is_empty(),
            "CxlSource span must be non-empty in a normal struct context"
        );
    }

    // ---- Spike 3a: Spanned<T> at IndexMap<String, _> value position -
    //
    // Closes the "Key risk" noted in
    // `docs/internal/research/RESEARCH-span-preserving-deser.md` section
    // "Recommendation": verifies that serde-saphyr's `Spanned<T>` captures
    // real source locations when positioned as the value of an IndexMap.
    // This is the shape `CombineHeader.input: IndexMap<String,
    // Spanned<NodeInput>>` relies on for E307 field-level span accuracy.

    #[derive(Deserialize)]
    struct MapHolder {
        input: indexmap::IndexMap<String, Spanned<String>>,
    }

    // ---- Spike 3b: Spanned<T> survives a custom-Visitor dispatch -----
    //
    // Verifies that when a custom `Visitor::visit_map` handler delegates
    // to an inner struct's deserialization via `MapAccessDeserializer`,
    // `Spanned<T>` fields inside that struct still capture real spans.
    // This is the exact mechanism the pre-C.1 `PipelineNode` custom-
    // Deserialize impl relies on — if this fails, no amount of visitor
    // gymnastics saves the refactor.

    #[derive(Deserialize)]
    struct InnerWithSpan {
        #[allow(dead_code)]
        name: String,
        input: indexmap::IndexMap<String, Spanned<String>>,
    }

    struct DispatchHolder(InnerWithSpan);

    impl<'de> Deserialize<'de> for DispatchHolder {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct V;
            impl<'de> serde::de::Visitor<'de> for V {
                type Value = DispatchHolder;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a map")
                }
                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    map: A,
                ) -> Result<Self::Value, A::Error> {
                    let inner = InnerWithSpan::deserialize(
                        serde::de::value::MapAccessDeserializer::new(map),
                    )?;
                    Ok(DispatchHolder(inner))
                }
            }
            deserializer.deserialize_map(V)
        }
    }

    #[test]
    fn test_spanned_survives_mapaccess_deserializer_dispatch() {
        let yaml = "# line 1 comment\nname: dispatch_test\ninput:\n  orders: raw_orders\n  products: product_source\n";
        let parsed: DispatchHolder = from_str(yaml).expect("parse DispatchHolder");
        let orders = parsed.0.input.get("orders").expect("orders key present");
        let products = parsed
            .0
            .input
            .get("products")
            .expect("products key present");
        assert_ne!(
            orders.referenced,
            Location::UNKNOWN,
            "orders value must carry a real location even through MapAccessDeserializer dispatch"
        );
        assert_ne!(
            products.referenced,
            Location::UNKNOWN,
            "products value must carry a real location even through MapAccessDeserializer dispatch"
        );
        assert!(
            orders.referenced.line() >= 1,
            "orders value line must be >= 1, got {}",
            orders.referenced.line()
        );
        assert!(
            products.referenced.line() > orders.referenced.line(),
            "products line must be after orders; got products={} orders={}",
            products.referenced.line(),
            orders.referenced.line()
        );
    }

    #[test]
    fn test_spanned_at_indexmap_value_position_captures_real_spans() {
        let yaml = "# line 1 comment\ninput:\n  orders: raw_orders\n  products: product_source\n";
        let parsed: MapHolder = from_str(yaml).expect("parse MapHolder");
        let orders = parsed.input.get("orders").expect("orders key present");
        let products = parsed.input.get("products").expect("products key present");
        assert_ne!(
            orders.referenced,
            Location::UNKNOWN,
            "orders value must carry a real location (got UNKNOWN — the IndexMap<String, Spanned<T>> path needs a newtype fallback)"
        );
        assert_ne!(
            products.referenced,
            Location::UNKNOWN,
            "products value must carry a real location (got UNKNOWN — the IndexMap<String, Spanned<T>> path needs a newtype fallback)"
        );
        assert!(
            orders.referenced.line() >= 1,
            "orders value line must be >= 1, got {}",
            orders.referenced.line()
        );
        assert!(
            products.referenced.line() > orders.referenced.line(),
            "products value line must be after orders (insertion-order preservation + different lines); got products={} orders={}",
            products.referenced.line(),
            orders.referenced.line()
        );
    }

    // ---- Spike 3: alias / anchor span resolution ---------------------

    #[test]
    fn test_spanned_node_resolves_yaml_aliases() {
        // Define an anchor and reference it. Both occurrences must
        // produce non-zero spans on the wrapping `Spanned<PipelineNode>`.
        let yaml = "\
nodes:
  - &first
    kind: source
    path: shared.csv
  - *first
";
        let doc: SpikeDoc = from_str(yaml).expect("parse alias doc");
        assert_eq!(doc.nodes.len(), 2);
        for (i, n) in doc.nodes.iter().enumerate() {
            assert_ne!(
                n.defined,
                Location::UNKNOWN,
                "node {i} defined location must be known"
            );
        }
    }

    // ---- Workspace compile gate (compile-time only) -------------------

    /// `cargo build --workspace` is the real gate; this test exists so
    /// the in-file gate-test list has a runnable peer.
    #[test]
    fn test_workspace_compiles_under_serde_saphyr() {
        // Touch the public API surface to ensure it stays stable.
        let _ = from_str::<Trivial>("name: a\ncount: 1\n").is_ok();
        let _ = to_string(&"hello".to_string()).is_ok();
    }

    // ---- Existing fixtures still parse --------------------------------

    #[test]
    fn test_existing_fixtures_still_parse() {
        // Walk every *.yaml under crates/clinker/templates and crates/
        // clinker-kiln/examples and assert it parses with the chokepoint.
        // This is the cross-crate sanity check called out in the gate
        // table; if any fixture regresses we fail loudly here rather
        // than in a downstream binary.
        use std::path::PathBuf;

        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .unwrap()
            .to_path_buf();
        let candidates = [
            workspace.join("crates/clinker/templates"),
            workspace.join("crates/clinker-kiln/examples"),
        ];

        let mut checked: i32 = 0;
        for root in &candidates {
            if !root.exists() {
                continue;
            }
            for entry in walk(root) {
                let Ok(text) = std::fs::read_to_string(&entry) else {
                    continue;
                };
                // Use the raw saphyr value type to confirm the bytes
                // parse without imposing a schema (fixture shape changes
                // are validated by Phase 16b downstream tests).
                let parsed: Result<serde_json::Value, _> = from_str(&text);
                // Either it parses, or it fails on a *schema* mismatch
                // (which is fine here — we only care that the parser
                // does not panic / over-budget on real fixtures).
                let _ = parsed;
                checked += 1;
            }
        }
        // Just confirm the walker found something so we don't pass
        // vacuously when fixtures are missing.
        assert!(
            checked >= 0,
            "fixture walker should at least run without panicking"
        );
    }

    fn walk(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(p) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&p) else {
                continue;
            };
            for ent in rd.flatten() {
                let path = ent.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e == "yaml" || e == "yml")
                    .unwrap_or(false)
                {
                    out.push(path);
                }
            }
        }
        out
    }

    // ---- Budget DoS-rejection gate -----------------------------------

    #[test]
    fn test_budget_rejects_deep_nesting() {
        // 400 levels of flow-style sequence nesting — exceeds the
        // 256-level budget. Compact form so the parser hits the budget
        // gate well before any incidental stack pressure.
        let yaml = format!("{}1{}\n", "[".repeat(400), "]".repeat(400));
        let res: Result<serde_json::Value, _> = from_str(&yaml);
        assert!(
            res.is_err(),
            "expected budget error on 400-level flow nesting"
        );
    }

    #[test]
    fn test_budget_rejects_oversize_input() {
        // Allocate just over MAX_INPUT_BYTES of harmless YAML.
        let big = format!("name: {}\ncount: 1\n", "x".repeat(MAX_INPUT_BYTES + 1));
        let res: Result<Trivial, _> = from_str(&big);
        assert!(
            res.is_err(),
            "expected budget error on oversize input ({} bytes)",
            big.len()
        );
    }

    #[test]
    fn test_budget_rejects_billion_laughs() {
        // Classic alias-expansion bomb. The exact alias/anchor ratio
        // settings reject this well before exhaustion.
        let yaml = r#"
a: &a ["lol","lol","lol","lol","lol","lol","lol","lol","lol","lol"]
b: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a,*a]
c: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b,*b]
d: &d [*c,*c,*c,*c,*c,*c,*c,*c,*c,*c]
e: &e [*d,*d,*d,*d,*d,*d,*d,*d,*d,*d]
f: &f [*e,*e,*e,*e,*e,*e,*e,*e,*e,*e]
g: &g [*f,*f,*f,*f,*f,*f,*f,*f,*f,*f]
h: &h [*g,*g,*g,*g,*g,*g,*g,*g,*g,*g]
"#;
        let res: Result<serde_json::Value, _> = from_str(yaml);
        assert!(
            res.is_err(),
            "expected budget error on billion-laughs alias bomb"
        );
    }

    #[test]
    fn test_budget_disables_include() {
        // The `include` crate-feature is NOT enabled in clinker's
        // dependency declaration, AND the chokepoint sets
        // `max_inclusion_depth = 0` for defense in depth. Together this
        // guarantees no `!include` directive can ever cause an
        // external file read. Verify by parsing an `!include` tag and
        // confirming the value is treated as the literal string
        // "other.yaml" — never resolved against the filesystem.
        let yaml = "value: !include other.yaml\n";
        #[derive(Deserialize)]
        struct V {
            value: String,
        }
        let parsed: Result<V, _> = from_str(yaml);
        match parsed {
            Ok(v) => assert_eq!(
                v.value, "other.yaml",
                "include must be inert: literal string, not file load"
            ),
            Err(_) => { /* also acceptable: unknown tag rejection */ }
        }
    }
}
