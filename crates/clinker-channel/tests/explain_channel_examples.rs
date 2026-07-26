//! Deserializes the channel-overlay YAML printed in `docs/explain/*.md`.
//!
//! `clinker-plan`'s `explain_examples` harness compiles a page's marked blocks
//! through `parse_config`, which reads a *pipeline* file. The channel-overlay
//! pages document `.channel.yaml` blocks instead, and no amount of marking
//! makes `parse_config` accept one — so those pages had nothing checking them,
//! and E116 shipped a prescribed fix that did not deserialize (it told the
//! author to drop a `type:` that `ScopedVarDecl` requires).
//!
//! This closes that gap at the boundary that owns the parser: every channel
//! block on these pages must deserialize through the same
//! [`OverlayFile::from_yaml_bytes`] a real run uses. Blocks a page labels
//! "rejected" are included deliberately — they are rejected by a plan-time
//! gate, not by serde, so they too must parse.

use std::path::{Path, PathBuf};

use clinker_channel::OverlayFile;

/// Pages whose examples are channel overlays rather than pipelines.
const CHANNEL_PAGES: &[&str] = &["E116.md", "E117.md", "E118.md"];

fn explain_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/explain")
}

/// Every ```yaml block in `page` whose opening comment names a `.channel.yaml`
/// file, paired with that comment for failure messages.
fn channel_blocks(page: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut lines = page.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "```yaml" {
            continue;
        }
        let block: Vec<&str> = lines.by_ref().take_while(|l| l.trim() != "```").collect();
        let label = block.first().copied().unwrap_or("").to_owned();
        if label.contains(".channel.yaml") {
            out.push((label, block.join("\n")));
        }
    }
    out
}

#[test]
fn every_documented_channel_overlay_deserializes() {
    let dir = explain_dir();
    let mut checked = 0usize;

    for page_name in CHANNEL_PAGES {
        let path = dir.join(page_name);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let blocks = channel_blocks(&text);
        assert!(
            !blocks.is_empty(),
            "{page_name} documents no channel overlay — either the page changed \
             shape or the block scanner stopped matching it, and in both cases \
             this test would pass while checking nothing"
        );

        for (label, yaml) in blocks {
            OverlayFile::from_yaml_bytes(yaml.as_bytes(), path.clone()).unwrap_or_else(|e| {
                panic!(
                    "{page_name} prescribes an overlay that does not \
                     deserialize.\n  block: {label}\n  error: {e}\n{yaml}"
                )
            });
            checked += 1;
        }
    }

    assert!(
        checked >= CHANNEL_PAGES.len(),
        "expected one block per page"
    );
}

/// The rule E116's third fix step used to contradict: `type:` is not optional
/// on a `vars:` entry, so "drop `type:` and supply the default alone" produced
/// a config that never reached the check the page is about.
#[test]
fn a_channel_var_entry_without_a_type_is_rejected() {
    let yaml = r#"channel:
  target: ../../pipeline/base.yaml
vars:
  pipeline:
    batch_label:
      default: "globex-spring"
"#;
    let err = OverlayFile::from_yaml_bytes(yaml.as_bytes(), PathBuf::from("base.channel.yaml"))
        .expect_err("a vars entry with no `type:` must not deserialize");
    let msg = err.to_string();
    assert!(
        msg.contains("type"),
        "the failure must name the missing field so the author can act on it; got: {msg}"
    );
}
