//! Tracing auto-init (the `Session::open` process-entry hook).
//!
//! Unit tests cover the pure env-parsing decision. The PROCESS-level test
//! (`auto_init_fires_on_first_session_open`) mutates process env vars and
//! opens real sessions, so it is `#[ignore]` for ordinary parallel runs and
//! executed by the dedicated single-threaded `aorta_core_trace_runtime_test`
//! Bazel target (its own process, deterministic order, no env races).

use crate::trace::runtime;

#[test]
fn hermetic_test_builds_default_auto_init_off() {
    // `runtime_env` disables env reads in cfg(test) builds unless
    // AORTA_TEST_ALLOW_ENV is set; without a readable opt-out, auto-init must
    // default OFF so session-opening suites never flip the global gate.
    if std::env::var_os("AORTA_TEST_ALLOW_ENV").is_none() {
        assert!(!runtime::auto_init_enabled());
    }
}

#[test]
fn auto_init_enabled_value_parses_env_conventions() {
    // Unset ⇒ enabled (default-on posture).
    assert!(runtime::auto_init_enabled_value(None));
    // Explicit opt-out forms.
    assert!(!runtime::auto_init_enabled_value(Some("0")));
    assert!(!runtime::auto_init_enabled_value(Some("false")));
    assert!(!runtime::auto_init_enabled_value(Some("off")));
    assert!(!runtime::auto_init_enabled_value(Some(" no ")));
    // Explicit opt-in forms.
    assert!(runtime::auto_init_enabled_value(Some("1")));
    assert!(runtime::auto_init_enabled_value(Some("true")));
    // Unparsable ⇒ enabled (fail open, matching logging's auto-init).
    assert!(runtime::auto_init_enabled_value(Some("garbage")));
    assert!(runtime::auto_init_enabled_value(Some("")));
}

/// PROCESS-LEVEL (run via the dedicated Bazel target / `--ignored`):
/// `AORTA_TRACE_AUTO_INIT=0` keeps `Session::open` from initializing tracing;
/// re-enabling it makes the next open initialize. Uses `AORTA_TRACE=off` so
/// init only latches the flag (no sink thread, no gate flip) — the sink path
/// itself is covered by the sink/runtime unit tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn auto_init_fires_on_first_session_open() {
    // Lift the cfg(test) env-read lockout (runtime_env) — this test drives the
    // env vars itself and runs in its own single-threaded process.
    std::env::set_var("AORTA_TEST_ALLOW_ENV", "1");
    std::env::set_var("AORTA_TRACE", "off");

    std::env::set_var("AORTA_TRACE_AUTO_INIT", "0");
    let s = crate::session::Session::open_default()
        .await
        .expect("open session");
    drop(s);
    assert!(
        !runtime::is_initialized(),
        "AORTA_TRACE_AUTO_INIT=0 must keep Session::open from initializing tracing"
    );

    std::env::set_var("AORTA_TRACE_AUTO_INIT", "1");
    let s = crate::session::Session::open_default()
        .await
        .expect("open session");
    drop(s);
    assert!(
        runtime::is_initialized(),
        "with auto-init enabled, the first Session::open must initialize tracing"
    );
}
