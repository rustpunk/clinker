//! Concurrency-safety tests for source staging under the partition-and-run
//! model, where several `clinker` invocations stage the *same* shared source
//! into one staging volume at once.
//!
//! These drive the public [`SourceStager`] API across real OS threads, each
//! thread standing in for an independent `clinker` process (an `fs4` advisory
//! lock contends across separate file handles within one process exactly as it
//! does across processes, so threads are a faithful and deterministic stand-in
//! for the cross-process race). They assert the three collision-safety
//! guarantees:
//!
//! - **No second copy on a cache hit.** When a fresh staged copy already
//!   exists, concurrent reusers copy zero bytes — proven with a sentinel written
//!   over the staged file that any stray copy would clobber.
//! - **Byte-identical concurrent reads.** Threads racing to first-stage a fresh
//!   source all observe identical, correct staged bytes, with exactly one
//!   `.staged` + manifest produced.
//! - **Stale source re-stages.** A source whose size changed invalidates the
//!   prior copy and triggers a re-stage.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;

use clinker_channel::SourceStager;
use clinker_plan::config::{OnExisting, StagingPolicy};

/// How many concurrent "invocations" (threads) contend per race.
const RACERS: usize = 8;

fn reuse_policy(dir: &Path) -> StagingPolicy {
    StagingPolicy {
        enabled: true,
        dir: Some(dir.to_path_buf()),
        patterns: vec!["*.csv".to_string()],
        on_existing: OnExisting::Reuse,
        ..Default::default()
    }
}

/// One racer thread's observation: the staged path it resolved, and the staged
/// file's identity (inode on Unix) as the thread saw it the instant `resolve`
/// returned.
struct RaceObservation {
    staged: PathBuf,
    identity: u128,
}

/// Resolve `source` from `racers` threads at once, released together by a
/// barrier so the staging attempts genuinely overlap. Each thread records the
/// staged-file identity it observed right after its own `resolve` returned.
fn race_resolve(policy: &StagingPolicy, source: &Path, racers: usize) -> Vec<RaceObservation> {
    let barrier = Arc::new(Barrier::new(racers));
    let mut handles = Vec::with_capacity(racers);
    for _ in 0..racers {
        let policy = policy.clone();
        let source = source.to_path_buf();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut stager = SourceStager::new(policy);
            // Line every thread up on the barrier so the resolve calls fire as
            // close to simultaneously as the scheduler allows — the window the
            // per-source lock has to serialize.
            barrier.wait();
            let staged = stager
                .resolve(source.clone())
                .expect("concurrent resolve must succeed")
                .staged
                .expect("a matched source must be staged");
            let identity = staged_identity(&staged);
            RaceObservation { staged, identity }
        }));
    }
    handles.into_iter().map(|h| h.join().unwrap()).collect()
}

fn count_with_suffix(dir: &Path, suffix: &str) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(suffix))
        .count()
}

#[test]
fn concurrent_cache_hit_performs_no_second_copy() {
    // The core guarantee: once a fresh staged copy exists, concurrent reusers
    // must reuse it and copy nothing. A SENTINEL is written over the staged
    // bytes after the initial stage; any thread that wrongly re-copies would
    // atomically replace the staged file and wipe the sentinel. All threads
    // reading back the sentinel proves zero copies happened across the race.
    //
    // Repeated several times because a concurrency bug is probabilistic — a
    // single green run could be luck.
    for attempt in 0..25 {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        // A non-trivial body so a stray copy is a wide, easy-to-detect write.
        let body = b"id,name,amount\n".repeat(20_000);
        std::fs::write(&src, &body).unwrap();

        let policy = reuse_policy(stage_dir.path());

        // Initial stage establishes the fresh cache entry (.staged + manifest).
        let staged = {
            let mut seed = SourceStager::new(policy.clone());
            seed.resolve(src.clone()).unwrap().staged.unwrap()
        };

        // Overwrite the staged bytes with a sentinel of a *different* length, so
        // even a partial re-copy is detectable. Reuse must leave this untouched.
        let sentinel = format!("REUSED-SENTINEL-attempt-{attempt}").into_bytes();
        std::fs::write(&staged, &sentinel).unwrap();

        let results = race_resolve(&policy, &src, RACERS);

        for obs in &results {
            assert_eq!(
                obs.staged, staged,
                "every racer resolves the same stable staged path"
            );
            assert_eq!(
                std::fs::read(&obs.staged).unwrap(),
                sentinel,
                "a cache hit must not re-copy: the sentinel must survive every \
                 concurrent reuse (attempt {attempt})"
            );
        }
        // Exactly one cache entry, no leaked partials.
        assert_eq!(count_with_suffix(stage_dir.path(), ".staged"), 1);
        assert_eq!(count_with_suffix(stage_dir.path(), ".manifest.json"), 1);
        assert_eq!(
            count_with_suffix(stage_dir.path(), ".partial"),
            0,
            "no in-flight partial may leak (attempt {attempt})"
        );
    }
}

#[test]
fn concurrent_first_stage_yields_byte_identical_reads_and_one_copy() {
    // Threads race to first-stage a *fresh* source (no prior cache entry). The
    // per-source lock must funnel them so exactly one copies and the rest reuse
    // its result; all must read byte-identical, correct content. A staged-file
    // inode that is stable across a follow-on reuse round confirms the follow-on
    // resolves recopied nothing.
    for attempt in 0..25 {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("events.csv");
        let body = format!("seq,payload\n{}", "x,1\n".repeat(50_000)).into_bytes();
        std::fs::write(&src, &body).unwrap();

        let policy = reuse_policy(stage_dir.path());

        let results = race_resolve(&policy, &src, RACERS);
        let staged = results[0].staged.clone();
        for obs in &results {
            assert_eq!(obs.staged, staged, "one stable staged path for all");
            assert_eq!(
                std::fs::read(&obs.staged).unwrap(),
                body,
                "every concurrent reader sees byte-identical, correct content \
                 (attempt {attempt})"
            );
        }
        // Every racer observed the SAME staged-file identity the instant its
        // own resolve returned. If the lock had failed and two threads both
        // copied, a second atomic-rename publish would have swapped the staged
        // file's inode out from under an earlier reader, so the observed
        // identities would diverge. One shared identity ⇒ exactly one copy
        // produced the staged file and everyone else reused it.
        let winner = results[0].identity;
        for obs in &results {
            assert_eq!(
                obs.identity, winner,
                "all racers must observe one staged-file identity — proof the \
                 lock funneled them to a single copy (attempt {attempt})"
            );
        }
        // Exactly one copy landed: one `.staged`, one manifest, no partial.
        assert_eq!(
            count_with_suffix(stage_dir.path(), ".staged"),
            1,
            "the race produced exactly one staged copy (attempt {attempt})"
        );
        assert_eq!(count_with_suffix(stage_dir.path(), ".manifest.json"), 1);
        assert_eq!(count_with_suffix(stage_dir.path(), ".partial"), 0);

        // A second concurrent round under the now-warm cache must reuse: the
        // staged file must not be recreated, so its identity is unchanged.
        let second = race_resolve(&policy, &src, RACERS);
        for obs in &second {
            assert_eq!(obs.staged, staged);
        }
        assert_eq!(
            staged_identity(&staged),
            winner,
            "a warm-cache concurrent round must not recreate the staged file \
             (attempt {attempt})"
        );
    }
}

#[test]
fn concurrent_stale_source_triggers_a_restage() {
    // A source whose size changed invalidates the prior copy. Even under a
    // concurrent re-stage race the result must be the *new* bytes, and the lock
    // must keep the result coherent (no torn mix of old and new).
    for attempt in 0..15 {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("ledger.csv");
        std::fs::write(&src, b"v1-short").unwrap();

        let policy = reuse_policy(stage_dir.path());

        {
            let mut seed = SourceStager::new(policy.clone());
            let staged = seed.resolve(src.clone()).unwrap().staged.unwrap();
            assert_eq!(std::fs::read(&staged).unwrap(), b"v1-short");
        }

        // Rewrite with a different size so the freshness check (mtime + size)
        // reads the manifest as stale, forcing a re-stage.
        let v2 = b"v2-this-is-a-distinctly-longer-body".to_vec();
        std::fs::write(&src, &v2).unwrap();

        let results = race_resolve(&policy, &src, RACERS);
        for obs in &results {
            assert_eq!(
                std::fs::read(&obs.staged).unwrap(),
                v2,
                "a stale source must re-stage to the new bytes (attempt {attempt})"
            );
        }
        assert_eq!(count_with_suffix(stage_dir.path(), ".staged"), 1);
        assert_eq!(count_with_suffix(stage_dir.path(), ".partial"), 0);
    }
}

/// A stable identity for a staged file that changes if and only if the file is
/// recreated (atomic-rename publish makes a new inode). On Unix this is the
/// inode number; elsewhere it falls back to (modification time, length), which
/// a republish of identical bytes still perturbs via the fresh mtime.
fn staged_identity(path: &Path) -> u128 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path).unwrap().ino() as u128
    }
    #[cfg(not(unix))]
    {
        let m = std::fs::metadata(path).unwrap();
        let mtime = m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        mtime ^ ((m.len() as u128) << 96)
    }
}
