//! End-to-end tests for the output collision policy and path templating.
//!
//! Covers each `if_exists` policy through `open_output`, the path-template
//! interaction with `OutputConfig`, and a multi-threaded race test that
//! exercises the `OpenOptions::create_new` retry loop under concurrent
//! contention.

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use clinker_core::config::IfExistsPolicy;
use clinker_core::output::open::{append_suffix_before_ext, open_output};
use clinker_core::output::path_template::{
    PathTemplate, TemplateContext, resolve_output_path_templates_in_place,
};

fn touch(path: &Path) {
    let mut f = File::create(path).unwrap();
    f.write_all(b"x").unwrap();
}

#[test]
fn overwrite_policy_truncates_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out.csv");
    touch(&target);
    let written = std::fs::metadata(&target).unwrap().len();
    assert_eq!(written, 1);

    let (path, _f) = open_output(IfExistsPolicy::Overwrite, false, |n| {
        assert!(n.is_none());
        Ok(target.clone())
    })
    .unwrap();
    assert_eq!(path, target);
    assert_eq!(std::fs::metadata(&target).unwrap().len(), 0);
}

#[test]
fn error_policy_refuses_existing_unless_force() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out.csv");
    touch(&target);

    let result = open_output(IfExistsPolicy::Error, false, |_| Ok(target.clone()));
    assert!(result.is_err(), "policy=Error must refuse existing file");

    let (path, _f) = open_output(IfExistsPolicy::Error, true, |_| Ok(target.clone())).unwrap();
    assert_eq!(path, target, "cli_force should downgrade Error → Overwrite");
}

#[test]
fn unique_suffix_walks_until_free_slot() {
    let dir = tempfile::tempdir().unwrap();
    let bare = dir.path().join("out.csv");
    touch(&bare);
    touch(&dir.path().join("out-1.csv"));
    touch(&dir.path().join("out-2.csv"));

    let (path, _f) = open_output(IfExistsPolicy::UniqueSuffix, false, |n| {
        Ok(match n {
            None => bare.clone(),
            Some(k) => append_suffix_before_ext(&bare, &format!("-{k}")),
        })
    })
    .unwrap();
    assert_eq!(path, dir.path().join("out-3.csv"));
}

#[test]
fn unique_suffix_race_under_concurrency() {
    // Twelve threads racing for the same bare path should each receive
    // a distinct file without anyone clobbering anyone else. This is
    // the property `OpenOptions::create_new` is supposed to give us;
    // the test guards against a regression to plain `File::create` in
    // the policy layer.
    let dir = tempfile::tempdir().unwrap();
    let bare = Arc::new(dir.path().join("race.csv"));
    let started = Arc::new(AtomicUsize::new(0));
    let total = 12;
    let mut handles = Vec::with_capacity(total);

    for _ in 0..total {
        let bare = Arc::clone(&bare);
        let started = Arc::clone(&started);
        handles.push(thread::spawn(move || {
            // Spin briefly so threads enter the critical section in
            // overlap rather than serially.
            started.fetch_add(1, Ordering::SeqCst);
            while started.load(Ordering::SeqCst) < total {
                std::hint::spin_loop();
            }
            let bare_path = (*bare).clone();
            let (path, _f) = open_output(IfExistsPolicy::UniqueSuffix, false, |n| match n {
                None => Ok(bare_path.clone()),
                Some(k) => Ok(append_suffix_before_ext(&bare_path, &format!("-{k}"))),
            })
            .unwrap();
            path
        }));
    }

    let mut results: Vec<PathBuf> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    results.sort();
    results.dedup();
    assert_eq!(
        results.len(),
        total,
        "every concurrent open_output call must yield a distinct path"
    );
}

#[test]
fn template_renders_pipeline_hash_and_source_name() {
    let mut hash = [0u8; 32];
    hash[0] = 0xab;
    hash[1] = 0xcd;
    hash[2] = 0xef;
    hash[3] = 0x12;
    let mut by_node = HashMap::new();
    by_node.insert("primary".to_string(), "customers".to_string());

    let template = PathTemplate::parse("{source_name:primary}-{pipeline_hash}.csv").unwrap();
    let ctx = TemplateContext {
        source_name_default: None,
        source_name_by_node: by_node,
        channel: None,
        pipeline_hash: hash,
        timestamp: None,
        execution_id: None,
        batch_id: None,
        n: None,
        unique_suffix_width: 0,
    };
    assert_eq!(template.render(&ctx).unwrap(), "customers-abcdef12.csv");
}

#[test]
fn conditional_emit_protects_against_empty_channel() {
    let template = PathTemplate::parse("out{channel:-:}.csv").unwrap();

    let with_channel = TemplateContext {
        channel: Some("acme"),
        ..Default::default()
    };
    assert_eq!(template.render(&with_channel).unwrap(), "out-acme.csv");

    let without_channel = TemplateContext::default();
    assert_eq!(
        template.render(&without_channel).unwrap(),
        "out.csv",
        "missing channel must not produce broken `out-.csv`"
    );
}

#[test]
fn resolve_output_path_templates_rejects_ambiguous_source_name() {
    // Two-source merge → bare {source_name} is ambiguous and must error.
    let yaml = r#"
pipeline:
  name: ambiguous_test
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: /tmp/a.csv
      options: { has_header: true }
      schema:
        - { name: x, type: string }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: /tmp/b.csv
      options: { has_header: true }
      schema:
        - { name: x, type: string }
  - type: merge
    name: merged
    inputs: [a, b]
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: /tmp/{source_name}-out.csv
      include_unmapped: true
error_handling:
  strategy: fail_fast
"#;
    let mut config = clinker_core::config::parse_config(yaml).unwrap();
    let ctx = TemplateContext::default();
    let err = resolve_output_path_templates_in_place(&mut config, &ctx).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("ambiguous"),
        "expected ambiguous-source error, got {msg}"
    );
    assert!(
        msg.contains("source_name:NODE"),
        "diagnostic should suggest the disambiguating form"
    );
}

#[test]
fn resolve_output_path_templates_rejects_unknown_node_arg() {
    let yaml = r#"
pipeline:
  name: unknown_node_test
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: /tmp/a.csv
      options: { has_header: true }
      schema:
        - { name: x, type: string }
  - type: output
    name: out
    input: a
    config:
      name: out
      type: csv
      path: /tmp/{source_name:nope}-out.csv
      include_unmapped: true
error_handling:
  strategy: fail_fast
"#;
    let mut config = clinker_core::config::parse_config(yaml).unwrap();
    let ctx = TemplateContext::default();
    let err = resolve_output_path_templates_in_place(&mut config, &ctx).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("not a Source ancestor"),
        "expected unknown-source-node error, got {msg}"
    );
}

#[test]
fn resolve_output_path_templates_renders_into_config() {
    let yaml = r#"
pipeline:
  name: render_test
nodes:
  - type: source
    name: customers
    config:
      name: customers
      type: csv
      path: /tmp/customers.csv
      options: { has_header: true }
      schema:
        - { name: x, type: string }
  - type: output
    name: out
    input: customers
    config:
      name: out
      type: csv
      path: /tmp/{source_name}.csv
      include_unmapped: true
error_handling:
  strategy: fail_fast
"#;
    let mut config = clinker_core::config::parse_config(yaml).unwrap();
    let ctx = TemplateContext::default();
    resolve_output_path_templates_in_place(&mut config, &ctx).unwrap();
    let resolved = config.output_configs().next().unwrap().path.clone();
    assert_eq!(resolved, "/tmp/customers.csv");
}
