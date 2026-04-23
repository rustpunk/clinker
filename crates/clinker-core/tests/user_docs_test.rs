//! Gate test: verify user-facing rustdoc exists on key modules with
//! required topic coverage.

use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn test_user_docs_files_exist() {
    let root = workspace_root();

    // 1. Composition authoring guide (7 bullets)
    let comp_mod =
        std::fs::read_to_string(root.join("crates/clinker-core/src/config/composition/mod.rs"))
            .expect("composition mod.rs must exist");
    let comp_topics = [
        "Composition Authoring Guide",
        "_compose:",
        "IsolatedFromAbove",
        "Pass-through columns",
        "Port naming",
        "use:",
        "comp.yaml",
    ];
    for topic in &comp_topics {
        assert!(
            comp_mod.contains(topic),
            "composition mod.rs missing topic: {topic}"
        );
    }

    // 2. Channel authoring guide (5 bullets)
    let channel_lib = std::fs::read_to_string(root.join("crates/clinker-channel/src/lib.rs"))
        .expect("clinker-channel lib.rs must exist");
    let channel_topics = [
        "Channel Authoring Guide",
        "_channel:",
        "fixed",
        "default",
        "DottedPath",
    ];
    for topic in &channel_topics {
        assert!(
            channel_lib.contains(topic),
            "clinker-channel lib.rs missing topic: {topic}"
        );
    }

    // 3. Kiln walkthrough (4 sections)
    let kiln_main = std::fs::read_to_string(root.join("crates/clinker-kiln/src/main.rs"))
        .expect("clinker-kiln main.rs must exist");
    let kiln_topics = [
        "Composition browser",
        "drill-in",
        "breadcrumb",
        "Raw/Resolved",
        "Provenance panel",
        "Extract-as-composition",
    ];
    for topic in &kiln_topics {
        assert!(
            kiln_main.contains(topic),
            "clinker-kiln main.rs missing topic: {topic}"
        );
    }
}
