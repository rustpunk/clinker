//! Per-executor shutdown signaling for graceful SIGINT/SIGTERM handling.
//!
//! Each `PipelineExecutor` run owns a [`ShutdownToken`] that the executor
//! checks at chunk boundaries (Phase 2) and during `Arena::build` (Phase 1).
//! Tokens are cheap `Arc<AtomicBool>` handles — clones share the same flag.
//!
//! `install_signal_handler()` installs a process-wide `ctrlc` handler that
//! broadcasts shutdown to every live token via a `Weak` registry. This keeps
//! production SIGINT handling intact while eliminating the global mutable
//! state that previously caused tests to race against each other.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// A handle to a per-execution shutdown flag. Cheap to clone — clones share
/// the same underlying `AtomicBool`. The token is checked at chunk boundaries
/// and inside `Arena::build`.
#[derive(Clone, Debug)]
pub struct ShutdownToken {
    flag: Arc<AtomicBool>,
}

impl Default for ShutdownToken {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownToken {
    /// Create a fresh, un-signaled token registered with the process-wide
    /// signal handler. SIGINT/SIGTERM (when `install_signal_handler` has been
    /// called) will trip every live token.
    pub fn new() -> Self {
        let flag = Arc::new(AtomicBool::new(false));
        register(&flag);
        Self { flag }
    }

    /// Create a token that is NOT registered with the signal handler. Useful
    /// for tests that want isolation from the process-wide SIGINT broadcast.
    pub fn detached() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Programmatically request shutdown on this token (and any clones).
    pub fn request(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }

    /// Check whether shutdown has been requested on this token.
    pub fn is_requested(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
}

/// Process-wide registry of weak handles to live shutdown flags. The signal
/// handler walks this list and stores `true` into every live entry. Dead
/// `Weak`s are pruned lazily on each insert.
fn registry() -> &'static Mutex<Vec<Weak<AtomicBool>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<AtomicBool>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

fn register(flag: &Arc<AtomicBool>) {
    let mut guard = registry().lock().expect("shutdown registry poisoned");
    guard.retain(|w| w.strong_count() > 0);
    guard.push(Arc::downgrade(flag));
}

/// Install the SIGINT + SIGTERM handler. Safe to call multiple times — only
/// the first call wins. The handler walks the live-token registry and trips
/// every flag.
pub fn install_signal_handler() -> Result<(), String> {
    use std::sync::Once;
    static INIT: Once = Once::new();
    let mut result = Ok(());
    INIT.call_once(|| {
        // ctrlc with "termination" feature handles both SIGINT and SIGTERM.
        if let Err(e) = ctrlc::set_handler(move || {
            let guard = registry().lock().expect("shutdown registry poisoned");
            for weak in guard.iter() {
                if let Some(flag) = weak.upgrade() {
                    flag.store(true, Ordering::SeqCst);
                }
            }
        }) {
            result = Err(e.to_string());
        }
    });
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_flag_round_trip() {
        let t = ShutdownToken::detached();
        assert!(!t.is_requested());
        t.request();
        assert!(t.is_requested());
    }

    #[test]
    fn test_clones_share_flag() {
        let a = ShutdownToken::detached();
        let b = a.clone();
        assert!(!b.is_requested());
        a.request();
        assert!(b.is_requested());
    }

    #[test]
    fn test_independent_tokens_do_not_interfere() {
        let a = ShutdownToken::detached();
        let b = ShutdownToken::detached();
        a.request();
        assert!(a.is_requested());
        assert!(!b.is_requested());
    }

    #[test]
    fn test_exit_codes_documented() {
        use crate::exit_codes::*;
        assert_eq!(EXIT_SUCCESS, 0);
        assert_eq!(EXIT_CONFIG_ERROR, 1);
        assert_eq!(EXIT_PARTIAL_DLQ, 2);
        assert_eq!(EXIT_FATAL_DATA, 3);
        assert_eq!(EXIT_IO_ERROR, 4);
        assert_eq!(EXIT_INTERRUPTED, 130);
    }
}
