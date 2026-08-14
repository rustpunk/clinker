//! The crypto provider behind both transports.
//!
//! Every other test in this crate binds a loopback listener speaking plain
//! HTTP, so none of them performs a TLS handshake — a suite that is entirely
//! green proves the provider compiles and links, not that it works. These two
//! tests cover the difference: one runs offline on every CI machine, the other
//! reaches a real endpoint and is opt-in.
#![cfg(feature = "transport")]

use std::io::Read;
use std::net::TcpListener;

/// The provider is reached, and reaching it does not abort the process.
///
/// ureq is built with ring's feature off, so an agent whose provider was never
/// configured does not fall back — it panics on first use. That makes "did a
/// TLS attempt survive to a network error" a real signal: a panic here means an
/// agent was constructed somewhere without `tls::config()`, which no offline
/// test would otherwise notice until a production endpoint was dialled.
///
/// The listener accepts and drops the connection, so the handshake fails at the
/// peer rather than in setup. The assertion is on which side failed.
#[test]
fn a_tls_attempt_fails_at_the_peer_rather_than_in_provider_setup() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();

    let closer = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            // Read the ClientHello, then hang up without replying.
            let mut scratch = [0u8; 1024];
            let _ = stream.read(&mut scratch);
        }
    });

    let agent: ureq::Agent = ureq::Agent::config_builder()
        .tls_config(clinker_net_test_tls::config())
        .build()
        .into();

    let error = agent
        .get(&format!("https://127.0.0.1:{port}/"))
        .call()
        .expect_err("a peer that hangs up mid-handshake cannot produce a response");

    // Any transport-level error is a pass. The failure this guards against is a
    // panic, which never reaches this line.
    assert!(
        !matches!(error, ureq::Error::StatusCode(_)),
        "expected a transport failure from a peer that never replied, got {error}"
    );

    closer.join().expect("listener thread");
}

/// The provider completes a real handshake, including certificate verification
/// against the compiled-in webpki roots.
///
/// Ignored by default: it needs outbound network, which CI does not grant and a
/// developer offline should not be failed for. Run it when the provider, the
/// ureq feature set, or the root store changes:
///
/// ```text
/// cargo test -p clinker-net --features transport --test tls_provider -- --ignored
/// ```
///
/// Verified on 2026-08-14 against the graviola provider: `200 OK`.
#[test]
#[ignore = "requires outbound network"]
fn the_provider_completes_a_handshake_against_a_public_endpoint() {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .tls_config(clinker_net_test_tls::config())
        .build()
        .into();

    let response = agent
        .get("https://example.com/")
        .call()
        .expect("the configured provider must complete a public TLS handshake");
    assert_eq!(response.status(), 200);
}

/// The crate's own TLS configuration, included rather than duplicated so these
/// tests exercise the definition the transports actually use. A second copy
/// here would pass while the real one was broken.
///
/// Declared at file scope on purpose: a `#[path]` module nested inside another
/// module resolves against that module's notional directory, not the including
/// file's, so `../src/tls.rs` one level down looks for `tests/src/tls.rs`.
#[path = "../src/tls.rs"]
mod clinker_net_test_tls;
