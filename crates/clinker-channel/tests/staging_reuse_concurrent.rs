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

use clinker_channel::{SourceStager, open_source_file};
use clinker_plan::config::{Cleanup, OnExisting, StagingPolicy};

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

#[test]
fn concurrent_cleanup_never_yanks_a_live_readers_file() {
    // The core #353 guarantee: a run with an open handle on a staged file must
    // never lose its bytes to a concurrent run's cleanup of the same shared
    // source. Run A resolves the source (holding the per-source shared read
    // lock for the run) and opens its handle. Run B resolves the same source
    // under `cleanup = always` and then runs cleanup — which must skip the pair
    // because A's shared lock blocks B's liveness-gated exclusive removal. A's
    // open handle keeps reading the correct, complete bytes; no run errors.
    //
    // Repeated because a concurrency bug is probabilistic.
    for attempt in 0..25 {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orders.csv");
        let body = format!("id,name,amount\n{}", "1,alice,10\n".repeat(10_000)).into_bytes();
        std::fs::write(&src, &body).unwrap();

        // Run A: resolve and open. The stager retains A's shared read lock for
        // its whole lifetime, and the open handle reads the staged copy.
        let mut run_a = SourceStager::new(reuse_policy(stage_dir.path()));
        let staged_a = run_a.resolve(src.clone()).unwrap().staged.unwrap();
        let mut reader_a = open_source_file(&staged_a).unwrap();

        // Run B: resolve the same source under cleanup=always, then clean up.
        // Its exclusive-locked removal must find A's shared lock held and keep
        // the pair rather than yank A's file.
        let mut policy_b = reuse_policy(stage_dir.path());
        policy_b.cleanup = Cleanup::Always;
        let mut run_b = SourceStager::new(policy_b);
        let staged_b = run_b.resolve(src.clone()).unwrap().staged.unwrap();
        assert_eq!(staged_a, staged_b, "both runs share the staged path");
        run_b.cleanup(true);

        // A's handle still reads the complete, correct bytes — the file it
        // opened was never deleted underneath it.
        use std::io::Read;
        let mut read_back = Vec::new();
        reader_a
            .read_to_end(&mut read_back)
            .expect("run A's open handle must keep reading after B's cleanup");
        assert_eq!(
            read_back, body,
            "a live reader's bytes must survive a concurrent cleanup (attempt {attempt})"
        );

        // Now run A finishes (drop its stager, releasing the shared lock), and a
        // fresh purge-style cleanup can finally reap the pair — proving the keep
        // was gated on the live lock, not a permanent leak.
        drop(reader_a);
        run_a.cleanup(true);
        assert!(
            !staged_a.exists(),
            "once the last reader is gone, cleanup reaps the pair (attempt {attempt})"
        );
    }
}

#[test]
fn concurrent_overwrite_waits_for_a_live_reader_and_never_corrupts_it() {
    // A reader holding an open handle while a concurrent run re-stages
    // (overwrite) the same source must keep reading its own complete bytes; the
    // re-stage must *wait* for the reader to finish (its exclusive publish lock
    // blocks on the reader's shared lock), then publish cleanly. Modeled with
    // run A on its own thread (an independent invocation) so run B's blocking
    // exclusive lock genuinely contends rather than self-deadlocking in one
    // thread.
    for attempt in 0..15 {
        let src_dir = tempfile::tempdir().unwrap();
        let stage_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("ledger.csv");
        let v1 = format!("v1\n{}", "old,row\n".repeat(8_000)).into_bytes();
        std::fs::write(&src, &v1).unwrap();

        // Seed a v1 staged copy so run A reuses it (and run B's overwrite has
        // something to replace).
        {
            let mut seed = {
                let mut p = reuse_policy(stage_dir.path());
                p.on_existing = OnExisting::Reuse;
                SourceStager::new(p)
            };
            assert!(seed.resolve(src.clone()).unwrap().staged.unwrap().exists());
        }

        // Two gates: A signals "handle open, reading" so B starts its overwrite;
        // A holds its read open until it has fully read its bytes, then drops.
        let opened = Arc::new(Barrier::new(2));
        let v1_for_a = v1.clone();
        let stage_path = stage_dir.path().to_path_buf();
        let src_a = src.clone();
        let opened_a = Arc::clone(&opened);

        let a = thread::spawn(move || {
            let mut p = reuse_policy(&stage_path);
            p.on_existing = OnExisting::Reuse;
            let mut run_a = SourceStager::new(p);
            let staged_a = run_a.resolve(src_a).unwrap().staged.unwrap();
            let mut reader_a = open_source_file(&staged_a).unwrap();
            // Tell run B the handle is open; B's overwrite now contends with the
            // shared lock run A holds.
            opened_a.wait();
            // Read the full body through the open handle. On all platforms the
            // bytes this handle was opened on stay readable across a concurrent
            // publish (POSIX: unlinked-but-open; Windows: FILE_SHARE_DELETE).
            use std::io::Read;
            let mut got = Vec::new();
            reader_a.read_to_end(&mut got).unwrap();
            // run_a (and its shared lock) drops here, letting B's publish land.
            got
        });

        // Wait until run A has reused the v1 copy and opened its handle, THEN
        // change the source and re-stage. Changing the source only after A's
        // handle is open guarantees A opened the v1 copy (not a v2 re-stage of
        // its own), so the test exercises a reader open across a *later*
        // overwrite rather than racing A's own freshness check.
        opened.wait();
        let v2 = format!("v2\n{}", "new,longer,row\n".repeat(9_000)).into_bytes();
        std::fs::write(&src, &v2).unwrap();
        let mut policy_b = reuse_policy(stage_dir.path());
        policy_b.on_existing = OnExisting::Overwrite;
        let mut run_b = SourceStager::new(policy_b);
        let staged_b = run_b.resolve(src.clone()).unwrap().staged.unwrap();

        let read_back = a.join().unwrap();
        assert_eq!(
            read_back, v1_for_a,
            "run A's open handle read its own complete v1 bytes, never a torn mix (attempt {attempt})"
        );
        // run B's publish landed the new bytes for the next opener.
        assert_eq!(
            std::fs::read(&staged_b).unwrap(),
            v2,
            "the re-stage published the new bytes (attempt {attempt})"
        );
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
