use std::fs;

use serde_json::{Value as JsonValue, json};

use crate::Scope;
use crate::manifest::{
    check_consumer, check_core, check_final_crate_map, check_lock_membership, check_metadata,
};
use crate::test_support::TempTree;

fn manifest_fixture(label: &str, crates: &[&str]) -> TempTree {
    let tree = TempTree::new(label);
    for crate_name in crates {
        tree.copy_from_repository(format!("crates/{crate_name}/Cargo.toml"));
    }
    tree
}

fn rewrite(tree: &TempTree, relative: &str, transform: impl FnOnce(String) -> String) {
    let path = tree.path(relative);
    let original = fs::read_to_string(&path).expect("read manifest fixture");
    fs::write(path, transform(original)).expect("rewrite manifest fixture");
}

fn expected_metadata(root: &std::path::Path) -> JsonValue {
    json!({
        "packages": [
            {"name": "clinker-core-types",
             "manifest_path": root.join("crates/clinker-core-types/Cargo.toml"),
             "targets": [library_target(root, "clinker-core-types")],
             "dependencies": [
                metadata_edge("miette", &["fancy"]),
                metadata_edge("petgraph", &[]),
                metadata_edge("serde-saphyr", &["miette"])
            ]},
            {"name": "clinker-net",
             "manifest_path": root.join("crates/clinker-net/Cargo.toml"),
             "targets": [library_target(root, "clinker-net")],
             "dependencies": [
                normal_edge(root, "clinker-core-types", &[], true),
                normal_edge(root, "clinker-exec", &[], true),
                normal_edge(root, "clinker-format", &[], true),
                normal_edge(root, "clinker-plan", &[], true),
                normal_edge(root, "clinker-record", &[], true),
                normal_edge(root, "http", &[], true),
                normal_edge(root, "indexmap", &["serde"], true),
                optional_edge(root, "rustls-graviola", &[], true),
                normal_edge(
                    root,
                    "serde_json",
                    &["arbitrary_precision", "preserve_order"],
                    true,
                ),
                normal_edge(root, "tracing", &[], true),
                optional_edge(root, "ureq", &["rustls-no-provider", "rustls-webpki-roots"], false),
                development_edge(root, "clinker-bench-support", &[]),
                development_edge(root, "clinker-exec", &["test-utils"])
             ]},
            {"name": "clinker-lineage",
             "manifest_path": root.join("crates/clinker-lineage/Cargo.toml"),
             "targets": [library_target(root, "clinker-lineage")],
             "dependencies": [
                normal_edge(root, "clinker-core-types", &[], true),
                normal_edge(root, "clinker-plan", &[], true),
                normal_edge(root, "clinker-record", &[], true),
                normal_edge(root, "cxl", &[], true),
                normal_edge(root, "petgraph", &[], true),
                normal_edge(root, "serde", &["derive", "rc"], true),
                normal_edge(
                    root,
                    "serde_json",
                    &["arbitrary_precision", "preserve_order"],
                    true,
                )
             ]}
        ]
    })
}

fn library_target(root: &std::path::Path, crate_name: &str) -> JsonValue {
    json!({
        "kind": ["lib"],
        "crate_types": ["lib"],
        "src_path": root.join("crates").join(crate_name).join("src/lib.rs")
    })
}

fn metadata_edge(name: &str, features: &[&str]) -> JsonValue {
    json!({
        "name": name,
        "kind": null,
        "rename": null,
        "features": features,
        "optional": false,
        "uses_default_features": true,
        "target": null
    })
}

fn normal_edge(
    root: &std::path::Path,
    name: &str,
    features: &[&str],
    uses_default_features: bool,
) -> JsonValue {
    let path = matches!(
        name,
        "clinker"
            | "clinker-bench-support"
            | "clinker-benchmarks"
            | "clinker-channel"
            | "clinker-core-types"
            | "clinker-exec"
            | "clinker-format"
            | "clinker-lineage"
            | "clinker-net"
            | "clinker-plan"
            | "clinker-record"
            | "clinker-scenarios"
            | "clinker-schema"
            | "cxl"
            | "cxl-cli"
    )
    .then(|| root.join("crates").join(name));
    json!({
        "name": name,
        "kind": null,
        "path": path,
        "source": null,
        "rename": null,
        "features": features,
        "optional": false,
        "uses_default_features": uses_default_features,
        "target": null
    })
}

fn optional_edge(
    root: &std::path::Path,
    name: &str,
    features: &[&str],
    uses_default_features: bool,
) -> JsonValue {
    let mut edge = normal_edge(root, name, features, uses_default_features);
    edge["optional"] = json!(true);
    edge
}

fn development_edge(root: &std::path::Path, name: &str, features: &[&str]) -> JsonValue {
    let mut edge = normal_edge(root, name, features, true);
    edge["kind"] = json!("dev");
    edge
}

#[derive(Clone, Copy)]
enum MetadataMutation {
    MissingEdge,
    DevKind,
    Features,
    Optional,
    DefaultFeatures,
    Target,
}

impl MetadataMutation {
    fn apply(self, dependencies: &mut JsonValue) {
        match self {
            Self::MissingEdge => *dependencies = JsonValue::Array(Vec::new()),
            Self::DevKind => dependencies[0]["kind"] = json!("dev"),
            Self::Features => dependencies[0]["features"] = json!(["serde"]),
            Self::Optional => dependencies[0]["optional"] = json!(true),
            Self::DefaultFeatures => dependencies[0]["uses_default_features"] = json!(false),
            Self::Target => dependencies[0]["target"] = json!("cfg(unix)"),
        }
    }
}

#[test]
fn current_manifests_match_the_approved_dependency_edges() {
    let tree = manifest_fixture(
        "manifest-baseline",
        &["clinker-core-types", "clinker-net", "clinker-lineage"],
    );
    check_core(tree.root()).expect("core manifest contract");
    check_consumer(tree.root(), "clinker-net").expect("network manifest contract");
    check_consumer(tree.root(), "clinker-lineage").expect("lineage manifest contract");
    check_metadata(tree.root(), &expected_metadata(tree.root()), Scope::Final)
        .expect("metadata contract");
}

#[test]
fn core_manifest_rejects_dependency_build_and_feature_expansion() {
    let cases = [
        (
            "extra-external",
            "\nserde = { workspace = true }\n",
            "preapproved dependencies",
        ),
        (
            "internal-back-edge",
            "\nclinker-plan = { workspace = true }\n",
            "internal workspace dependency",
        ),
        (
            "build-dependency",
            "\n[build-dependencies]\nserde = { workspace = true }\n",
            "build-dependencies",
        ),
        (
            "feature-table",
            "\n[features]\ndefault = []\n",
            "preapproved features",
        ),
    ];
    for (label, suffix, expected) in cases {
        let tree = manifest_fixture(label, &["clinker-core-types"]);
        rewrite(&tree, "crates/clinker-core-types/Cargo.toml", |text| {
            format!("{text}{suffix}")
        });
        let error = check_core(tree.root())
            .expect_err("core expansion must be rejected")
            .to_string();
        assert!(error.contains(expected), "fixture {label}: {error}");
    }

    let tree = manifest_fixture("core-build-script", &["clinker-core-types"]);
    rewrite(&tree, "crates/clinker-core-types/Cargo.toml", |text| {
        text.replacen("[package]", "[package]\nbuild = \"build.rs\"", 1)
    });
    let error = check_core(tree.root())
        .expect_err("core build script must be rejected")
        .to_string();
    assert!(error.contains("build script"), "{error}");

    let tree = manifest_fixture("core-implicit-build-script", &["clinker-core-types"]);
    tree.write("crates/clinker-core-types/build.rs", "fn main() {}\n");
    let error = check_core(tree.root())
        .expect_err("implicit core build script must be rejected")
        .to_string();
    assert!(error.contains("build script"), "{error}");

    let tree = manifest_fixture("core-explicit-lib", &["clinker-core-types"]);
    rewrite(&tree, "crates/clinker-core-types/Cargo.toml", |text| {
        format!("{text}\n[lib]\npath = \"alternate.rs\"\n")
    });
    let error = check_core(tree.root())
        .expect_err("explicit core library target must be rejected")
        .to_string();
    assert!(error.contains("explicit lib target"), "{error}");
}

#[test]
fn consumer_core_edge_must_remain_normal_featureless_and_singular() {
    let replacements = [
        (
            "edge-features",
            "clinker-core-types = { workspace = true, features = [\"serde\"] }",
        ),
        (
            "edge-default-features",
            "clinker-core-types = { workspace = true, default-features = false }",
        ),
        (
            "edge-optional",
            "clinker-core-types = { workspace = true, optional = true }",
        ),
    ];
    for (label, replacement) in replacements {
        let tree = manifest_fixture(label, &["clinker-net"]);
        rewrite(&tree, "crates/clinker-net/Cargo.toml", |text| {
            text.replace("clinker-core-types = { workspace = true }", replacement)
        });
        let error = check_consumer(tree.root(), "clinker-net")
            .expect_err("expanded core edge must be rejected")
            .to_string();
        assert!(
            error.contains("declared exactly"),
            "fixture {label}: {error}"
        );
    }

    let tree = manifest_fixture("dev-only-edge", &["clinker-lineage"]);
    rewrite(&tree, "crates/clinker-lineage/Cargo.toml", |text| {
        text.replace("clinker-core-types = { workspace = true }\n", "")
            + "clinker-core-types = { workspace = true }\n"
    });
    let error = check_consumer(tree.root(), "clinker-lineage")
        .expect_err("dev-only edge must be rejected")
        .to_string();
    assert!(error.contains("normal dependency"), "{error}");

    let tree = manifest_fixture("unapproved-existing-edge-feature", &["clinker-net"]);
    rewrite(&tree, "crates/clinker-net/Cargo.toml", |text| {
        text.replace(
            "serde_json = { workspace = true }",
            "serde_json = { workspace = true, features = [\"raw_value\"] }",
        )
    });
    let error = check_consumer(tree.root(), "clinker-net")
        .expect_err("feature expansion on an existing edge must be rejected")
        .to_string();
    assert!(error.contains("exact preapproved"), "{error}");
}

#[test]
fn target_specific_and_unapproved_consumer_dependencies_are_rejected() {
    for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
        let tree = manifest_fixture(section, &["clinker-net"]);
        rewrite(&tree, "crates/clinker-net/Cargo.toml", |text| {
            format!("{text}\n[target.'cfg(unix)'.{section}]\nserde = {{ workspace = true }}\n")
        });
        let error = check_consumer(tree.root(), "clinker-net")
            .expect_err("target-specific dependency must be rejected")
            .to_string();
        assert!(
            error.contains("target-specific"),
            "section {section}: {error}"
        );
    }

    let tree = manifest_fixture("unapproved-consumer-dependency", &["clinker-net"]);
    rewrite(&tree, "crates/clinker-net/Cargo.toml", |text| {
        // Anchored on `[dependencies]` rather than on whatever table follows
        // it: the crate now carries a `[features]` table in between, and
        // inserting ahead of `[dev-dependencies]` put the new key in there
        // instead — testing the feature rule while claiming to test this one.
        text.replacen(
            "[dependencies]\n",
            "[dependencies]\ntempfile = { workspace = true }\n",
            1,
        )
    });
    let error = check_consumer(tree.root(), "clinker-net")
        .expect_err("unapproved dependency must be rejected")
        .to_string();
    assert!(error.contains("preapproved dependencies"), "{error}");
}

#[test]
fn cargo_metadata_rejects_edge_kind_feature_optional_default_and_target_changes() {
    let cases = [
        ("missing-edge", MetadataMutation::MissingEdge),
        ("dev-kind", MetadataMutation::DevKind),
        ("features", MetadataMutation::Features),
        ("optional", MetadataMutation::Optional),
        ("default-features", MetadataMutation::DefaultFeatures),
        ("target", MetadataMutation::Target),
    ];

    for (label, mutate) in cases {
        let tree = TempTree::new(label);
        let mut metadata = expected_metadata(tree.root());
        let dependencies = metadata["packages"][1]
            .get_mut("dependencies")
            .expect("network dependency array");
        mutate.apply(dependencies);
        let error = check_metadata(tree.root(), &metadata, Scope::ClinkerNet)
            .expect_err("metadata edge expansion must be rejected")
            .to_string();
        assert!(
            error.contains("exactly one") || error.contains("expansion"),
            "fixture {label}: {error}"
        );
    }

    let tree = TempTree::new("inherited-consumer-feature-expansion");
    let mut metadata = expected_metadata(tree.root());
    metadata["packages"][1]["dependencies"][8]["features"] =
        json!(["arbitrary_precision", "preserve_order", "raw_value"]);
    let error = check_metadata(tree.root(), &metadata, Scope::ClinkerNet)
        .expect_err("inherited workspace feature expansion must be rejected")
        .to_string();
    assert!(error.contains("clinker-net -> serde_json"), "{error}");

    let tree = TempTree::new("missing-inherited-arbitrary-precision");
    let mut metadata = expected_metadata(tree.root());
    metadata["packages"][1]["dependencies"][8]["features"] = json!(["preserve_order"]);
    let error = check_metadata(tree.root(), &metadata, Scope::ClinkerNet)
        .expect_err("missing approved workspace feature must be rejected")
        .to_string();
    assert!(error.contains("clinker-net -> serde_json"), "{error}");
}

#[test]
fn core_metadata_rejects_transitive_workspace_feature_expansion() {
    let tree = TempTree::new("core-metadata-feature");
    let mut metadata = expected_metadata(tree.root());
    metadata["packages"][0]["dependencies"][1]["features"] = json!(["serde"]);
    let error = check_metadata(tree.root(), &metadata, Scope::Core)
        .expect_err("core dependency feature expansion must be rejected")
        .to_string();
    assert!(error.contains("clinker-core-types -> petgraph"), "{error}");
}

#[test]
fn cargo_metadata_binds_each_audited_package_to_its_approved_source() {
    let tree = TempTree::new("metadata-target-routing");
    let mutations = [
        (
            "manifest",
            "manifest_path",
            json!(tree.path("alternate/Cargo.toml")),
        ),
        ("source", "src_path", json!(tree.path("alternate/lib.rs"))),
        ("crate-type", "crate_types", json!(["cdylib"])),
    ];
    for (label, field, value) in mutations {
        let mut metadata = expected_metadata(tree.root());
        if field == "manifest_path" {
            metadata["packages"][1][field] = value;
        } else {
            metadata["packages"][1]["targets"][0][field] = value;
        }
        let error = check_metadata(tree.root(), &metadata, Scope::ClinkerNet)
            .expect_err("alternate Cargo routing must be rejected")
            .to_string();
        assert!(
            error.contains("unexpected manifest") || error.contains("unapproved source"),
            "fixture {label}: {error}"
        );
    }

    let mut metadata = expected_metadata(tree.root());
    metadata["packages"][1]["targets"]
        .as_array_mut()
        .expect("network target array")
        .push(json!({
            "kind": ["custom-build"],
            "crate_types": ["bin"],
            "src_path": tree.path("crates/clinker-net/build.rs")
        }));
    let error = check_metadata(tree.root(), &metadata, Scope::ClinkerNet)
        .expect_err("custom build metadata target must be rejected")
        .to_string();
    assert!(error.contains("custom-build"), "{error}");

    let mut metadata = expected_metadata(tree.root());
    metadata["packages"][1]["dependencies"][0]["path"] =
        json!(tree.path("alternate/clinker-core-types"));
    let error = check_metadata(tree.root(), &metadata, Scope::ClinkerNet)
        .expect_err("the approved internal edge must not be redirected")
        .to_string();
    assert!(error.contains("unapproved package source"), "{error}");
}

#[test]
fn root_lock_membership_and_crate_map_are_fixed_release_inputs() {
    let tree = TempTree::new("lock-and-crate-map");
    tree.copy_from_repository("Cargo.lock");
    tree.copy_from_repository("docs/ai/20_CRATE_MAP.md");
    check_lock_membership(tree.root()).expect("current root lock membership");
    check_final_crate_map(tree.root()).expect("current crate-map classification");

    rewrite(&tree, "Cargo.lock", |text| {
        format!("{text}\n[[package]]\nname = \"unexpected-package\"\nversion = \"1.0.0\"\n")
    });
    let lock_error = check_lock_membership(tree.root())
        .expect_err("package membership drift must be rejected")
        .to_string();
    assert!(
        lock_error.contains("package membership changed"),
        "{lock_error}"
    );

    rewrite(&tree, "docs/ai/20_CRATE_MAP.md", |text| {
        text.replace("dataset identity remains", "dataset identity stays")
    });
    let docs_error = check_final_crate_map(tree.root())
        .expect_err("crate-map policy drift must be rejected")
        .to_string();
    assert!(
        docs_error.contains("crate map classification"),
        "{docs_error}"
    );
}
