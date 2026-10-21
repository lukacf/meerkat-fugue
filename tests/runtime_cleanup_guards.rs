#[test]
fn fugue_does_not_reintroduce_manual_meta_console_facade() {
    let meta_runtime = include_str!("../src/meta_runtime.rs");
    let http = include_str!("../src/http.rs");
    for (file, source) in [("src/meta_runtime.rs", meta_runtime), ("src/http.rs", http)] {
        for banned in [
            "MetaConsoleAggregator::in_memory",
            "console_json_router_with_aggregator",
            "refresh_session_history",
            "console_history_refresh",
            "ConsoleRuntimeRegistration",
            "console_aggregator",
        ] {
            assert!(
                !source.contains(banned),
                "{file} must use Meta reference console/runtime flow, found {banned}"
            );
        }
    }
}

#[test]
fn runtime_edge_calls_are_bounded() {
    let meta_runtime = include_str!("../src/meta_runtime.rs");
    let tracker = include_str!("../src/tracker.rs");

    assert!(
        !meta_runtime.contains("reqwest::Client::new()"),
        "Fugue host self-calls must use a bounded reqwest client"
    );
    assert!(
        meta_runtime.contains("FUGUE_HOST_API_CONNECT_TIMEOUT")
            && meta_runtime.contains("FUGUE_HOST_API_REQUEST_TIMEOUT"),
        "Fugue host self-calls need explicit connect/request timeouts"
    );
    assert!(
        meta_runtime.contains("META_EVENT_QUERY_TIMEOUT")
            && meta_runtime.contains("tokio::time::timeout(\n            META_EVENT_QUERY_TIMEOUT"),
        "Meta event queries must be bounded so one stalled runtime read cannot freeze polling"
    );
    assert!(
        tracker.contains(".connect_timeout(Duration::from_millis(10_000))")
            && tracker.contains(".timeout(Duration::from_millis(30_000))"),
        "Linear polling must have explicit connect and request timeouts"
    );
}

#[test]
fn meerkat_dependency_train_is_exact_and_unmixed() {
    let cargo_toml = include_str!("../Cargo.toml");
    let mut meerkat_versions = Vec::new();
    for line in cargo_toml.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("meerkat") {
            continue;
        }
        assert!(
            !trimmed.contains("path ="),
            "published Fugue builds must not use path/dev Meerkat dependencies: {trimmed}"
        );
        if trimmed.starts_with("meerkat-meta") {
            assert!(
                trimmed.contains("=0.9.3"),
                "meerkat-meta must be pinned to the companion train: {trimmed}"
            );
        } else if trimmed.starts_with("meerkat") {
            assert!(
                trimmed.contains("=0.9.3"),
                "Meerkat crates must be pinned to one train: {trimmed}"
            );
            meerkat_versions.push(trimmed.to_string());
        }
    }
    assert!(
        !meerkat_versions.is_empty(),
        "metadata guard should see at least one Meerkat dependency"
    );
}
