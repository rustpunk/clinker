use std::fs;
use std::path::Path;
use std::process::Command;

use crate::source::{check_consumer_source, check_core_source};
use crate::test_support::TempTree;

const CORE_PREAMBLE: &str = r#"
pub mod failure;
pub use failure::{FailureCategory, FailureClassification, RetryAdvice};
"#;

fn core_fixture(label: &str, source: &str) -> TempTree {
    let tree = TempTree::new(label);
    tree.write(
        "crates/clinker-core-types/src/lib.rs",
        &format!("{CORE_PREAMBLE}\n{source}"),
    );
    tree.write(
        "crates/clinker-core-types/src/failure.rs",
        "pub struct FailureCategory;\npub struct FailureClassification;\npub struct RetryAdvice;\n",
    );
    tree
}

fn consumer_fixture(label: &str, files: &[(&str, &str)]) -> TempTree {
    let tree = TempTree::new(label);
    for (relative, contents) in files {
        tree.write(format!("crates/clinker-net/src/{relative}"), contents);
    }
    tree
}

fn reject_consumer(label: &str, files: &[(&str, &str)], expected: &str) {
    let tree = consumer_fixture(label, files);
    let error = check_consumer_source(tree.root(), "clinker-net")
        .expect_err("adversarial consumer fixture must be rejected")
        .to_string();
    assert!(
        error.contains(expected),
        "fixture {label} expected {expected:?}, found {error:?}"
    );
}

fn accept_consumer(label: &str, files: &[(&str, &str)]) {
    let tree = consumer_fixture(label, files);
    check_consumer_source(tree.root(), "clinker-net")
        .unwrap_or_else(|error| panic!("fixture {label} should pass: {error}"));
}

fn compile_rust_fixture(tree: &TempTree, source: &Path, output: &Path, test_mode: bool) {
    let compiler = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let core = build_core_stub(tree, &compiler);

    let mut command = Command::new(compiler);
    command.arg("--edition=2024");
    if test_mode {
        command.arg("--test");
    } else {
        command.arg("--crate-type=lib");
    }
    let result = command
        .arg("--extern")
        .arg(format!("clinker_core_types={}", core.display()))
        .arg(source)
        .arg("-o")
        .arg(output)
        .output()
        .expect("execute rustc for dependency policy adversarial fixture");
    assert!(
        result.status.success(),
        "{} failed to compile: {}",
        source.display(),
        String::from_utf8_lossy(&result.stderr)
    );
}

fn build_core_stub(tree: &TempTree, compiler: &std::ffi::OsStr) -> std::path::PathBuf {
    let core = tree.path("libclinker_core_types.rlib");
    if !core.exists() {
        tree.write(
            "core_stub.rs",
            r#"pub struct FailureClassification;
pub struct FailureCategory;
pub struct RetryAdvice;
pub mod span { pub struct Span; }
pub mod dlq { pub struct DlqErrorCategory; }
"#,
        );
        let result = Command::new(compiler)
            .args([
                "--edition=2024",
                "--crate-name=clinker_core_types",
                "--crate-type=rlib",
            ])
            .arg(tree.path("core_stub.rs"))
            .arg("-o")
            .arg(&core)
            .output()
            .expect("execute rustc for core stub");
        assert!(
            result.status.success(),
            "core stub failed to compile: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    core
}

fn compile_strict_rust_fixture(
    tree: &TempTree,
    source: &Path,
    output: &Path,
    extra_externs: &[(&str, &Path)],
) {
    let compiler = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let core = build_core_stub(tree, &compiler);
    let mut command = Command::new(compiler);
    command
        .args(["--edition=2024", "--crate-type=lib", "-Dwarnings"])
        .arg("--extern")
        .arg(format!("clinker_core_types={}", core.display()));
    for (name, path) in extra_externs {
        command
            .arg("--extern")
            .arg(format!("{name}={}", path.display()));
    }
    let result = command
        .arg(source)
        .arg("-o")
        .arg(output)
        .output()
        .expect("execute rustc for dependency policy adversarial fixture");
    assert!(
        result.status.success(),
        "{} failed to compile: {}",
        source.display(),
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn syn_parsing_covers_the_old_lexical_masker_regressions() {
    let rejected = [
        (
            "identity-after-test-brace",
            r#"#[cfg(test)]
mod tests { const SAMPLE: &str = "}"; }
pub struct SemanticFingerprint;
"#,
            "SemanticFingerprint",
        ),
        (
            "string-suffix-is-not-raw-prefix",
            r#"#[cfg(test)]
mod tests { const THREAD_NAME: &str = "writer"; }
pub struct SemanticFingerprint;
const NEXT: &str = "next";
"#,
            "SemanticFingerprint",
        ),
        (
            "arbitrary-raw-string-hashes",
            r#######"const DECOY: &str = r#####"#[cfg(test)] mod hidden { const CLOSE: &str = "}"; }"#####;
const BYTE_DECOY: &[u8] = br######"#[cfg(test)] mod byte_hidden {}"######;
pub struct SemanticFingerprint;
"#######,
            "SemanticFingerprint",
        ),
        (
            "raw-c-string",
            r#######"const DECOY: &core::ffi::CStr = cr#####"#[cfg(test)] mod hidden {}"#####;
pub struct SemanticFingerprint;
"#######,
            "SemanticFingerprint",
        ),
        (
            "nested-block-comments",
            "/* outer { /* nested } */ still outer } */\npub struct SemanticFingerprint;\n",
            "SemanticFingerprint",
        ),
        (
            "escaped-literals",
            r#"const TEXT: &str = "escaped quote: \" // not a comment";
const BYTE_TEXT: &[u8] = b"escaped quote: \" /* not a comment */";
const CHARACTER: char = '\'';
const BYTE: u8 = b'\\';
pub struct SemanticFingerprint;
"#,
            "SemanticFingerprint",
        ),
        (
            "serialization-derive",
            "#[derive(serde::Serialize)]\npub struct Leaky;\n",
            "serialization-neutral",
        ),
        (
            "foreign-dataset-identity",
            "pub struct DatasetIdentity;\n",
            "DatasetIdentity",
        ),
        (
            "foreign-physical-identity",
            "pub struct PhysicalDatasetIdentity;\n",
            "PhysicalDatasetIdentity",
        ),
    ];

    for (label, source, expected) in rejected {
        let tree = core_fixture(label, source);
        let error = check_core_source(tree.root())
            .expect_err("core lexical bypass must be rejected")
            .to_string();
        assert!(
            error.contains(expected),
            "fixture {label} expected {expected:?}, found {error:?}"
        );
    }

    let tree = core_fixture(
        "test-only-identity",
        "#[cfg(test)]\npub struct SemanticFingerprint;\n",
    );
    check_core_source(tree.root()).expect("exact cfg(test) core items are not production source");

    let tree = core_fixture(
        "identity-shaped-local-binding",
        "fn local_name_is_not_type_ownership() { let DatasetIdentity = 1_u8; let _ = DatasetIdentity; }\n",
    );
    check_core_source(tree.root())
        .expect("only type declarations participate in identity ownership");

    let renamed = TempTree::new("renamed-core-export");
    renamed.write(
        "crates/clinker-core-types/src/lib.rs",
        r#"pub mod failure;
pub use failure::{FailureCategory, FailureClassification as FC, RetryAdvice};
"#,
    );
    renamed.write(
        "crates/clinker-core-types/src/failure.rs",
        "pub struct FailureCategory;\npub struct FailureClassification;\npub struct RetryAdvice;\n",
    );
    let error = check_core_source(renamed.root())
        .expect_err("renamed core export must be rejected")
        .to_string();
    assert!(error.contains("canonical name"), "{error}");
}

#[test]
fn authoritative_adversarial_probes_are_valid_rust_2024() {
    let probes = [
        (
            "cr21_impl_method",
            r#"use clinker_core_types::FailureClassification;
pub struct Api;
impl Api { pub fn exposed(_: FailureClassification) {} }
"#,
        ),
        (
            "cr21b_impl_const",
            r#"use clinker_core_types::FailureClassification;
pub struct Api;
impl Api { pub const SEED: Option<FailureClassification> = None; }
"#,
        ),
        (
            "cr22_cfg_union",
            r#"#[cfg(unix)] use clinker_core_types::FailureClassification as Shared;
#[cfg(windows)] use clinker_core_types::FailureCategory as Shared;
pub fn exposed(_: Shared) {}
"#,
        ),
        (
            "e1_chained_module_alias",
            r#"mod parent { pub type FC = clinker_core_types::FailureClassification; }
use crate::parent as p;
use p as q;
pub type PublicFailure = q::FC;
"#,
        ),
        (
            "e3_nested_rustdoc",
            r#"use clinker_core_types::FailureClassification;
/** API classification: supported integration API. /* nested */ */
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "new1_trait_impl_associated_type",
            r#"use clinker_core_types::FailureClassification;
pub trait PubTrait { type Item; }
pub struct PubType;
impl PubTrait for PubType { type Item = FailureClassification; }
"#,
        ),
        (
            "new2_unrelated_docs",
            r#"use clinker_core_types::FailureClassification;
/// See the test support fixtures.
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "new3_item_macro",
            r#"use clinker_core_types::FailureClassification;
macro_rules! facade { () => { pub type PublicFailure = FailureClassification; }; }
facade!();
"#,
        ),
        (
            "allowlist_function_group",
            "fn bypass() { use clinker_core_types::{span::Span}; let _ = core::mem::size_of::<Span>(); }\n",
        ),
        (
            "allowlist_function_rename",
            "fn bypass() { use clinker_core_types::{dlq::DlqErrorCategory as D}; let _ = core::mem::size_of::<D>(); }\n",
        ),
        (
            "associated_type_projection",
            r#"use clinker_core_types::FailureClassification;
trait Hidden { type Item; }
impl Hidden for () { type Item = FailureClassification; }
#[allow(private_interfaces)]
pub fn exposed(_: <() as Hidden>::Item) {}
"#,
        ),
        (
            "raw_core_crate_path",
            r#"use clinker_core_types::FailureClassification;
pub fn consume(_: FailureClassification) {}
pub fn bypass(_: r#clinker_core_types::span::Span) {}
"#,
        ),
        (
            "generic_shadow",
            r#"use clinker_core_types::FailureClassification;
pub fn consume(_: FailureClassification) {}
pub fn generic<FailureClassification>(_: FailureClassification) {}
"#,
        ),
    ];

    for (label, source) in probes {
        let tree = TempTree::new(label);
        tree.write("probe.rs", source);
        compile_rust_fixture(
            &tree,
            &tree.path("probe.rs"),
            &tree.path("probe.rlib"),
            false,
        );
    }

    let path_probe = TempTree::new("e2-path-routing");
    path_probe.write("lib.rs", "mod outer;\n");
    path_probe.write("outer.rs", "#[path = \"child.rs\"] mod child;\n");
    path_probe.write(
        "child.rs",
        "fn consume(_: clinker_core_types::FailureClassification) {}\n",
    );
    compile_rust_fixture(
        &path_probe,
        &path_probe.path("lib.rs"),
        &path_probe.path("path-probe.rlib"),
        false,
    );

    let cfg_probe = TempTree::new("wr12-inner-cfg-test");
    cfg_probe.write("lib.rs", "mod inline_tests { #![cfg(test)] mod helper; }\n");
    cfg_probe.write(
        "inline_tests/helper.rs",
        "pub type Hidden = clinker_core_types::FailureClassification;\n",
    );
    compile_rust_fixture(
        &cfg_probe,
        &cfg_probe.path("lib.rs"),
        &cfg_probe.path("cfg-probe"),
        true,
    );

    let type_macro = TempTree::new("type-position-macro-provider");
    type_macro.write(
        "macro_provider.rs",
        r#"#[macro_export]
macro_rules! shared_type {
    () => { ::clinker_core_types::FailureClassification };
}
"#,
    );
    let compiler = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let core = build_core_stub(&type_macro, &compiler);
    let provider = type_macro.path("libmacro_provider.rlib");
    let result = Command::new(&compiler)
        .args([
            "--edition=2024",
            "--crate-name=macro_provider",
            "--crate-type=rlib",
            "-Dwarnings",
        ])
        .arg("--extern")
        .arg(format!("clinker_core_types={}", core.display()))
        .arg(type_macro.path("macro_provider.rs"))
        .arg("-o")
        .arg(&provider)
        .output()
        .expect("compile type-macro provider");
    assert!(
        result.status.success(),
        "type-macro provider failed to compile: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    type_macro.write(
        "type_macro_probe.rs",
        "pub fn exposed(_: macro_provider::shared_type!()) {}\n",
    );
    compile_strict_rust_fixture(
        &type_macro,
        &type_macro.path("type_macro_probe.rs"),
        &type_macro.path("type-macro-probe.rlib"),
        &[("macro_provider", &provider)],
    );
}

#[test]
fn allowlist_is_enforced_in_module_and_function_scopes() {
    let cases = [
        (
            "direct-unapproved-item",
            "use clinker_core_types::Span;\n",
            "unapproved clinker-core-types item",
        ),
        (
            "nested-unapproved-item",
            "use clinker_core_types::span::Span;\n",
            "unapproved clinker-core-types item",
        ),
        (
            "function-local-grouped-span",
            r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
fn bypass() { use clinker_core_types::{span::Span}; let _ = Span::SYNTHETIC; }
"#,
            "unapproved clinker-core-types item",
        ),
        (
            "function-local-grouped-dlq-rename",
            r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
fn bypass() { use clinker_core_types::{dlq::DlqErrorCategory as D}; let _ = core::mem::size_of::<D>(); }
"#,
            "unapproved clinker-core-types item",
        ),
        (
            "crate-alias",
            r#"use clinker_core_types as core_types;
use clinker_core_types::FailureCategory;
fn consume(_: FailureCategory) { let _: Option<core_types::span::Span> = None; }
"#,
            "must not alias clinker-core-types",
        ),
        (
            "grouped-root-alias",
            r#"use {clinker_core_types as core_types};
use clinker_core_types::FailureCategory;
fn consume(_: FailureCategory) { let _: Option<core_types::span::Span> = None; }
"#,
            "must not alias clinker-core-types",
        ),
        (
            "wildcard",
            "use clinker_core_types::*;\n",
            "must not wildcard-import",
        ),
        (
            "raw-grouped-crate-path",
            r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
fn bypass() { use r#clinker_core_types::{span::Span}; let _ = core::mem::size_of::<Span>(); }
"#,
            "unapproved clinker-core-types item",
        ),
        (
            "public-extern-crate",
            r#"pub extern crate clinker_core_types;
use clinker_core_types::FailureCategory;
fn consume(_: FailureCategory) {}
"#,
            "alias or re-export",
        ),
        (
            "public-grouped-use",
            "pub use clinker_core_types::{FailureCategory};\n",
            "must not re-export",
        ),
    ];
    for (label, source, expected) in cases {
        reject_consumer(label, &[("lib.rs", source)], expected);
    }
}

#[test]
fn public_facades_are_rejected_even_when_documented_or_indirect() {
    let cases = [
        (
            "direct-reexport",
            "pub use clinker_core_types::FailureClassification;\n",
        ),
        (
            "alias-reexport",
            r#"use clinker_core_types::FailureClassification as FC;
pub use FC as PublicFailure;
"#,
        ),
        (
            "documented-type-facade",
            r#"use clinker_core_types::FailureClassification;
/// API classification: supported integration API.
pub type PublicFailure = FailureClassification;
"#,
        ),
        (
            "reexport-local-type-alias",
            r#"use clinker_core_types::FailureClassification;
type LocalFailure = FailureClassification;
pub use self::LocalFailure as PublicFailure;
"#,
        ),
        (
            "restricted-public-type",
            r#"use clinker_core_types::FailureClassification;
pub(crate) type WorkspaceFailure = FailureClassification;
"#,
        ),
        (
            "generic-default-facade",
            r#"use clinker_core_types::FailureClassification;
pub type PublicFailure<T = FailureClassification> = T;
"#,
        ),
        (
            "nested-semicolons",
            r#"use clinker_core_types::FailureClassification;
pub struct Wrapper<const N: usize, T>(core::marker::PhantomData<T>);
pub type PublicFailure = fn([(); { let marker = (); 1 }])
    -> Wrapper<{ let value = { 1; 2 }; value }, FailureClassification>;
"#,
        ),
        (
            "const-default-comparison",
            r#"use clinker_core_types::FailureClassification;
pub struct Wrapper<const B: bool, T>(core::marker::PhantomData<T>);
pub type PublicFailure<const LESS: bool = { 1 < 2 }> = Wrapper<LESS, FailureClassification>;
"#,
        ),
    ];
    for (label, source) in cases {
        reject_consumer(label, &[("lib.rs", source)], "must not re-export");
    }
}

#[test]
fn every_public_signature_shape_requires_exact_local_classification() {
    let cases = [
        (
            "direct-function",
            r#"use clinker_core_types::FailureClassification;
pub fn exposed() -> FailureClassification { todo!() }
"#,
        ),
        (
            "import-alias",
            r#"use clinker_core_types::FailureClassification as FC;
pub fn exposed() -> FC { todo!() }
"#,
        ),
        (
            "local-alias",
            r#"use clinker_core_types::FailureClassification;
type FC = FailureClassification;
pub fn exposed() -> FC { todo!() }
"#,
        ),
        (
            "private-alias-chain",
            r#"use clinker_core_types::FailureClassification;
type First = FailureClassification;
type Second = Option<First>;
pub fn exposed() -> Second { todo!() }
"#,
        ),
        (
            "raw-identifier-alias",
            r#"use clinker_core_types::FailureClassification as r#shared;
pub fn exposed() -> r#shared { todo!() }
"#,
        ),
        (
            "long-multiline-signature",
            r#"use clinker_core_types::FailureClassification;
pub fn exposed(










) -> FailureClassification { todo!() }
"#,
        ),
        (
            "function-name-containing-use",
            r#"use clinker_core_types::FailureCategory;
pub fn user_facing() -> clinker_core_types::FailureClassification { todo!() }
"#,
        ),
        (
            "public-union-field",
            r#"use clinker_core_types::FailureClassification;
pub union FailureUnion {
    pub shared: core::mem::ManuallyDrop<FailureClassification>,
    pub marker: usize,
}
"#,
        ),
        (
            "inherent-method-cr21",
            r#"use clinker_core_types::FailureClassification;
pub struct Api;
impl Api { pub fn exposed(&self, _: FailureClassification) {} }
"#,
        ),
        (
            "inherent-associated-const-cr21b",
            r#"use clinker_core_types::FailureClassification;
pub struct Api;
impl Api { pub const SEED: Option<FailureClassification> = None; }
"#,
        ),
    ];
    for (label, source) in cases {
        reject_consumer(
            label,
            &[("lib.rs", source)],
            "lacks an exact local API classification",
        );
    }
}

#[test]
fn trait_impl_associated_types_observe_public_reachability() {
    reject_consumer(
        "public-trait-associated-facade-new1",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
pub trait PubTrait { type Item; }
pub struct PubType;
impl PubTrait for PubType { type Item = FailureClassification; }
"#,
        )],
        "public taxonomy facade",
    );

    accept_consumer(
        "private-trait-associated-type",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
trait Carrier { type Item; }
struct Holder;
impl Carrier for Holder { type Item = FailureClassification; }
pub struct Item;
fn consume(value: FailureClassification) { let _ = value; }
"#,
        )],
    );
}

#[test]
fn cfg_union_and_alias_chains_cannot_erase_shared_bindings() {
    reject_consumer(
        "cfg-union-cr22",
        &[(
            "lib.rs",
            r#"#[cfg(unix)]
use clinker_core_types::FailureClassification as Shared;
#[cfg(windows)]
use std::string::String as Shared;
pub fn exposed(_: Shared) {}
"#,
        )],
        "lacks an exact local API classification",
    );

    reject_consumer(
        "chained-module-alias-e1",
        &[
            (
                "lib.rs",
                r#"mod parent;
use crate::parent as p;
use p as q;
pub type PublicFailure = q::FC;
"#,
            ),
            (
                "parent.rs",
                r#"use clinker_core_types::FailureClassification;
pub(crate) type FC = FailureClassification;
"#,
            ),
        ],
        "must not re-export",
    );

    reject_consumer(
        "grouped-self-module-alias",
        &[
            (
                "lib.rs",
                r#"mod parent;
use crate::parent::{self as p};
pub type PublicFailure = p::FC;
"#,
            ),
            (
                "parent.rs",
                r#"use clinker_core_types::FailureClassification;
pub(crate) type FC = FailureClassification;
"#,
            ),
        ],
        "must not re-export",
    );

    reject_consumer(
        "parent-qualified-alias",
        &[
            (
                "lib.rs",
                r#"use clinker_core_types::FailureClassification;
type FC = FailureClassification;
mod child;
"#,
            ),
            ("child.rs", "pub type PublicFailure = super::FC;\n"),
        ],
        "must not re-export",
    );

    reject_consumer(
        "parent-module-alias",
        &[
            (
                "lib.rs",
                r#"use clinker_core_types::FailureClassification;
type FC = FailureClassification;
mod child;
"#,
            ),
            (
                "child.rs",
                "use super as parent;\npub type PublicFailure = parent::FC;\n",
            ),
        ],
        "must not re-export",
    );
}

#[test]
fn module_routing_follows_path_and_cfg_test_ancestry() {
    accept_consumer(
        "path-on-non-mod-rs-e2",
        &[
            ("lib.rs", "mod outer;\n"),
            ("outer.rs", "#[path = \"child.rs\"]\nmod routed;\n"),
            (
                "child.rs",
                "fn consume(_: clinker_core_types::FailureClassification) {}\n",
            ),
        ],
    );

    accept_consumer(
        "inner-cfg-test-ancestry-wr12",
        &[
            (
                "lib.rs",
                r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
mod inline_tests {
    #![cfg(test)]
    mod helper;
}
"#,
            ),
            (
                "inline_tests/helper.rs",
                "pub type Hidden = clinker_core_types::FailureClassification;\n",
            ),
        ],
    );

    accept_consumer(
        "pure-cfg-test-path-module",
        &[
            (
                "lib.rs",
                r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
#[cfg(test)]
#[path = "test_support.rs"]
mod helpers;
"#,
            ),
            (
                "test_support.rs",
                "pub type Hidden = clinker_core_types::FailureClassification;\n",
            ),
        ],
    );

    reject_consumer(
        "test-and-production-share-path",
        &[
            (
                "lib.rs",
                r#"#[cfg(test)]
#[path = "shared.rs"]
mod test_view;
#[path = "shared.rs"]
pub mod production_view;
"#,
            ),
            (
                "shared.rs",
                r#"use clinker_core_types::FailureClassification;
pub type PublicFailure = FailureClassification;
"#,
            ),
        ],
        "must not re-export",
    );

    reject_consumer(
        "missing-module-fails-closed",
        &[(
            "lib.rs",
            "use clinker_core_types::FailureClassification;\nmod missing;\nfn consume(_: FailureClassification) {}\n",
        )],
        "source is missing",
    );
}

#[test]
fn doc_policy_is_parsed_and_anchored_to_the_local_item() {
    let accepted = [
        (
            "doc-attribute",
            r#"use clinker_core_types::FailureClassification;
#[doc = "API classification: supported integration API."]
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "block-rustdoc",
            r#"use clinker_core_types::FailureClassification;
/** API classification: workspace-internal exposed API. */
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "long-contiguous-rustdoc",
            r#"use clinker_core_types::FailureClassification;
/// API classification: test support.
/// Detail 01.
/// Detail 02.
/// Detail 03.
/// Detail 04.
/// Detail 05.
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "raw-approved-shared-type",
            r#"use clinker_core_types::r#FailureClassification;
/// API classification: supported integration API.
pub fn exposed(_: r#FailureClassification) {}
"#,
        ),
    ];
    for (label, source) in accepted {
        accept_consumer(label, &[("lib.rs", source)]);
    }

    let rejected = [
        (
            "unrelated-test-support-prose-new2",
            r#"use clinker_core_types::FailureClassification;
/// See the test support fixtures for examples.
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "nested-block-rustdoc-e3",
            r#"use clinker_core_types::FailureClassification;
/** API classification: supported integration API. /* nested */ */
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "four-slash-comment",
            r#"use clinker_core_types::FailureClassification;
//// API classification: supported integration API.
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "inner-line-doc",
            r#"//! API classification: supported integration API.
use clinker_core_types::FailureClassification;
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "inner-block-doc",
            r#"/*! API classification: supported integration API. */
use clinker_core_types::FailureClassification;
pub fn exposed(_: FailureClassification) {}
"#,
        ),
        (
            "triple-star-comment",
            r#"use clinker_core_types::FailureClassification;
/*** API classification: supported integration API. */
pub fn exposed(_: FailureClassification) {}
"#,
        ),
    ];
    for (label, source) in rejected {
        reject_consumer(
            label,
            &[("lib.rs", source)],
            "lacks an exact local API classification",
        );
    }
}

#[test]
fn macros_non_rust_sources_and_parse_failures_fail_closed() {
    reject_consumer(
        "item-macro-expansion-new3",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
macro_rules! facade { () => { pub type PublicFailure = FailureClassification; }; }
facade!();
"#,
        )],
        "item-level macro",
    );

    reject_consumer(
        "include-rust-source",
        &[
            (
                "lib.rs",
                r#"use clinker_core_types::FailureClassification;
include!("included.rs");
fn consume(_: FailureClassification) {}
"#,
            ),
            ("included.rs", "const INCLUDED: usize = 1;\n"),
        ],
        "uses include!",
    );

    reject_consumer(
        "public-trait-macro",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
pub trait Api { generated!(); }
"#,
        )],
        "macro expansion in a trait",
    );

    reject_consumer(
        "impl-macro",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
pub struct Api;
impl Api { generated!(); }
"#,
        )],
        "macro expansion in an impl",
    );

    reject_consumer(
        "attribute-macro",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
#[generated_api]
pub fn exposed(_: FailureClassification) {}
"#,
        )],
        "unsupported production attribute",
    );

    reject_consumer(
        "derive-macro",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
#[derive(GeneratedApi)]
pub struct Api;
"#,
        )],
        "unapproved derive macro",
    );

    let non_rust = consumer_fixture(
        "non-rust-source",
        &[(
            "lib.rs",
            "use clinker_core_types::FailureClassification;\nfn consume(_: FailureClassification) {}\n",
        )],
    );
    fs::write(
        non_rust.path("crates/clinker-net/src/generated.inc"),
        "pub type Hidden = clinker_core_types::FailureClassification;\n",
    )
    .expect("write non-Rust source probe");
    let error = check_consumer_source(non_rust.root(), "clinker-net")
        .expect_err("non-Rust production source must fail closed")
        .to_string();
    assert!(error.contains("non-.rs production source"), "{error}");

    reject_consumer(
        "syntax-error",
        &[(
            "lib.rs",
            "use clinker_core_types::FailureClassification;\npub fn broken( {\n",
        )],
        "cannot parse Rust source",
    );
}

#[test]
fn macro_projections_raw_identifiers_and_opaque_types_fail_closed() {
    reject_consumer(
        "type-position-macro",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
pub fn exposed(_: macro_provider::shared_type!()) {}
"#,
        )],
        "macro in a production type position",
    );

    reject_consumer(
        "private-associated-type-projection",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
trait Hidden { type Item; }
impl Hidden for () { type Item = FailureClassification; }
#[allow(private_interfaces)]
pub fn exposed(_: <() as Hidden>::Item) {}
"#,
        )],
        "cannot resolve",
    );

    reject_consumer(
        "trait-method-inherits-shared-generic-constraint",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
pub trait Carrier { type Item; }
/// API classification: supported integration API.
pub trait Api<T>
where
    T: Carrier<Item = FailureClassification>,
{
    fn exposed(_: T::Item);
}
"#,
        )],
        "trait method Api::exposed",
    );

    reject_consumer(
        "trait-self-associated-projection",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
pub trait Carrier { type Item; }
/// API classification: supported integration API.
pub trait Api: Carrier<Item = FailureClassification> {
    fn exposed(_: Self::Item);
}
"#,
        )],
        "cannot resolve",
    );

    reject_consumer(
        "impl-const-inherits-shared-generic-constraint",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
pub trait Carrier { type Item; }
pub struct Api<T>(T);
impl<T> Api<T>
where
    T: Carrier<Item = FailureClassification>,
{
    pub const FLAG: usize = 1;
}
"#,
        )],
        "impl const FLAG",
    );

    reject_consumer(
        "generic-associated-type-projection",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
pub trait Carrier { type Item; }
pub struct Api<T>(T);
impl<T> Api<T>
where
    T: Carrier<Item = FailureClassification>,
{
    pub fn exposed(_: T::Item) {}
}
"#,
        )],
        "cannot resolve",
    );

    reject_consumer(
        "transitive-supertrait-associated-projection",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
pub trait Carrier { type Item; }
/// API classification: supported integration API.
pub trait SharedCarrier: Carrier<Item = FailureClassification> {}
pub struct Api<T>(T);
impl<T: SharedCarrier> Api<T> {
    pub fn exposed(_: T::Item) {}
}
"#,
        )],
        "cannot resolve",
    );

    reject_consumer(
        "transitive-associated-constraint-through-impl-trait",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
pub trait Carrier { type Item; }
/// API classification: supported integration API.
pub trait SharedCarrier: Carrier<Item = FailureClassification> {}
pub fn exposed(_: impl SharedCarrier) {}
"#,
        )],
        "lacks an exact local API classification",
    );

    reject_consumer(
        "raw-core-crate-path",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
fn consume(_: FailureClassification) {}
pub fn bypass(_: r#clinker_core_types::span::Span) {}
"#,
        )],
        "unapproved clinker-core-types item",
    );

    accept_consumer(
        "generic-shadows-shared-import",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
/// API classification: supported integration API.
pub fn consume(_: FailureClassification) {}
pub fn generic<FailureClassification>(_: FailureClassification) {}
"#,
        )],
    );
}

#[test]
fn core_macros_includes_and_raw_identity_names_fail_closed() {
    let macro_tree = core_fixture(
        "core-item-macro-leak",
        r#"macro_rules! leak { () => { pub struct SemanticFingerprint; }; }
leak!();
"#,
    );
    let error = check_core_source(macro_tree.root())
        .expect_err("unapproved core item macros must fail closed")
        .to_string();
    assert!(error.contains("unapproved item-level macro"), "{error}");

    let approved_name_macro = core_fixture(
        "core-approved-name-macro-leak",
        r#"macro_rules! failure_registry { () => { pub struct ExtraFailureType; }; }
failure_registry!();
"#,
    );
    let error = check_core_source(approved_name_macro.root())
        .expect_err("approved registry macros must not generate types")
        .to_string();
    assert!(
        error.contains("must not generate public surface"),
        "{error}"
    );

    let include_tree = core_fixture("core-include-leak", "include!(\"../external.rs\");\n");
    include_tree.write(
        "crates/clinker-core-types/external.rs",
        "pub struct DatasetIdentity;\n",
    );
    let error = check_core_source(include_tree.root())
        .expect_err("core include expansions must fail closed")
        .to_string();
    assert!(error.contains("uses include!"), "{error}");

    for identity in [
        "SemanticFingerprint",
        "DatasetIdentity",
        "PhysicalDatasetIdentity",
    ] {
        let tree = core_fixture(
            &format!("raw-{identity}"),
            &format!("pub struct r#{identity};\n"),
        );
        let error = check_core_source(tree.root())
            .expect_err("raw foreign identity declarations must be rejected")
            .to_string();
        assert!(error.contains(identity), "raw {identity}: {error}");
    }

    let extra_failure_type = core_fixture("extra-failure-type", "");
    extra_failure_type.write(
        "crates/clinker-core-types/src/failure.rs",
        "pub struct FailureCategory;\npub struct FailureClassification;\npub struct RetryAdvice;\npub struct ExtraFailureType;\n",
    );
    let error = check_core_source(extra_failure_type.root())
        .expect_err("the failure module must own only the approved public types")
        .to_string();
    assert!(error.contains("exactly the three approved"), "{error}");

    let nested_failure_type = core_fixture("nested-failure-type", "");
    nested_failure_type.write(
        "crates/clinker-core-types/src/failure.rs",
        "pub struct FailureCategory;\npub struct FailureClassification;\npub struct RetryAdvice;\npub mod nested { pub struct ExtraFailureType; }\n",
    );
    let error = check_core_source(nested_failure_type.root())
        .expect_err("public failure submodules must not widen the type surface")
        .to_string();
    assert!(error.contains("must not expose nested module"), "{error}");

    let reexported_failure_type = core_fixture("reexported-failure-type", "");
    reexported_failure_type.write(
        "crates/clinker-core-types/src/failure.rs",
        "pub struct FailureCategory;\npub struct FailureClassification;\npub struct RetryAdvice;\npub use crate::span::Span;\n",
    );
    let error = check_core_source(reexported_failure_type.root())
        .expect_err("public failure re-exports must not widen the type surface")
        .to_string();
    assert!(error.contains("public re-exports"), "{error}");
}

#[test]
fn foreign_signatures_and_public_trait_impl_headers_are_checked() {
    reject_consumer(
        "public-foreign-function",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
unsafe extern "C" { pub fn exposed(value: FailureClassification); }
"#,
        )],
        "lacks an exact local API classification",
    );

    reject_consumer(
        "public-trait-impl-header",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
pub trait Carrier<T> {}
pub struct Holder;
impl Carrier<FailureClassification> for Holder {}
"#,
        )],
        "public taxonomy facade",
    );
}

#[test]
fn exact_cfg_test_items_are_excluded_but_other_cfgs_are_union_analyzed() {
    accept_consumer(
        "exact-cfg-test-items",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
#[cfg(test)]
pub type TestOnlyFacade = FailureClassification;
#[cfg(test)]
mod nested { pub type Hidden = super::FailureClassification; }
/// API classification: supported integration API.
pub fn exposed(_: FailureClassification) { let _ = format!("ok"); }
"#,
        )],
    );

    reject_consumer(
        "non-test-cfg-remains-production",
        &[(
            "lib.rs",
            r#"use clinker_core_types::FailureClassification;
#[cfg(feature = "never")]
pub type PublicFailure = FailureClassification;
"#,
        )],
        "must not re-export",
    );

    accept_consumer(
        "function-local-aliases-do-not-converge-globally",
        &[(
            "lib.rs",
            r#"mod left {}
mod right {}
fn first() { use crate::left as Collision; }
fn second() { use crate::right as Collision; }
fn consume(_: clinker_core_types::FailureClassification) {}
"#,
        )],
    );

    accept_consumer(
        "inline-module-aliases-do-not-collide",
        &[(
            "lib.rs",
            r#"mod left {}
mod right {}
mod first { use crate::left as Collision; }
mod second { use crate::right as Collision; }
pub struct Collision;
fn consume(_: clinker_core_types::FailureClassification) {}
"#,
        )],
    );
}
