//! Process isolation for tests that exercise process-wide memory limits.

/// Env flag distinguishing the re-exec'd child from the harness-launched
/// parent: absent in the parent (which re-execs), set in the child
/// (which runs the probe body). Its presence also breaks the otherwise
/// infinite re-exec loop.
const MEMPROBE_ISOLATED_ENV: &str = "CLINKER_MEMPROBE_ISOLATED";

/// Marker the child prints to stdout after the probe's assertions pass.
/// The parent requires it in the child's captured stdout, which is the
/// positive proof the probe actually ran: libtest exits 0 when its
/// filter matches no test, so `status.success()` alone would pass even
/// if `test_path` stopped matching the real test name (a future rename,
/// a filter quirk) and the probe never executed. A dedicated sentinel is
/// robust to libtest output-format drift in a way that scraping for
/// "1 passed" is not.
const MEMPROBE_RAN_SENTINEL: &str = "__clinker_memprobe_ran__";

/// Runs `probe` in a child process where it is the sole test, so its
/// memory samples bracket only its own allocation and the process-global
/// RSS readings cannot be moved by a sibling test thread.
///
/// The probes read a process-global counter (Linux `/proc/self/statm`
/// RSS, Windows `PrivateUsage`, macOS `phys_footprint`) and assert a
/// relation between two or more samples. Under `cargo test`'s default
/// multi-threaded harness, sibling test threads in the same binary
/// commit and free large buffers between the samples, moving the global
/// figure independently of this probe's own allocation and tripping the
/// assertion at random (issue #394). `#[serial]` is insufficient: it
/// only orders `#[serial]`-tagged tests, leaving every other test in the
/// binary concurrent. The mechanism is platform-agnostic, so it runs on
/// every first-class target rather than only macOS/Windows.
///
/// On first entry (parent, env flag absent) this re-execs the test
/// binary filtered to `test_path` alone, with the flag set, then
/// requires both that the child exited successfully and that it printed
/// [`MEMPROBE_RAN_SENTINEL`] — the latter is positive proof the probe
/// ran, since libtest exits 0 on a filter that matches no test. A
/// failure surfaces the child's stderr (the real assertion text). On the
/// recursive entry (child, env flag present) it runs `probe`, prints the
/// sentinel, and returns, letting libtest report the result.
pub(crate) fn run_isolated(test_path: &str, probe: impl FnOnce()) {
    if std::env::var_os(MEMPROBE_ISOLATED_ENV).is_some() {
        probe();
        // Reached only when the probe's assertions all passed; a panic
        // unwinds past this and the sentinel is absent from stdout.
        println!("{MEMPROBE_RAN_SENTINEL}");
        return;
    }

    let exe = std::env::current_exe().expect("test binary path must be readable");
    let output = std::process::Command::new(exe)
        .args(["--exact", test_path, "--test-threads=1", "--nocapture"])
        .env(MEMPROBE_ISOLATED_ENV, "1")
        .output()
        .expect("re-exec of the isolated memory probe must spawn");

    assert!(
        output.status.success(),
        "isolated memory probe {test_path} failed in child process:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(MEMPROBE_RAN_SENTINEL),
        "isolated memory probe {test_path} never ran: the child exited 0 \
         but did not print its run sentinel, so the `--exact` filter \
         matched no test (likely a stale test-path literal). \
         child stdout:\n{stdout}"
    );
}
