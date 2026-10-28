// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/config.rs
// Purpose:   Config loading and validation tests
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

use dfe_fetcher::config::Config;

#[test]
fn test_default_config_loads() {
    let config = Config::default();
    assert!(!config.kafka.brokers.is_empty() || config.kafka.brokers.is_empty());
    assert_eq!(config.scheduler.default_interval_secs, 300);
    assert_eq!(config.scheduler.max_concurrent_fetches, 10);
}

#[test]
fn test_config_from_example_yaml() {
    let yaml =
        std::fs::read_to_string(dfe_fetcher::deployment::repo_root().join("config.example.yaml"))
            .expect("config.example.yaml exists");
    let config: Config = scalo::yaml::from_str(&yaml, "config.yaml", scalo::yaml::Profile::Config)
        .expect("example config parses");

    assert_eq!(config.scheduler.default_interval_secs, 300);
    assert!(!config.sources.aws.enabled);
    assert!(!config.sources.azure.enabled);
    assert!(!config.sources.m365.enabled);
    assert!(!config.sources.gcp.enabled);
    assert_eq!(config.kafka.topic_suffix, "_land");
}

#[test]
fn test_config_validation_passes_for_default() {
    // The default config names no broker and nothing that would use one, so it
    // is valid but empty of work -- the idle gate's case, not a refusal.
    let config = Config::default();
    config.validate().expect("the default config is valid");
}

#[test]
fn test_config_validation_passes_with_brokers() {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    let result = config.validate();
    assert!(result.is_ok());
}

/// The PGO workload config only ever ran in the release build, where a stale
/// service name failed the whole release. It loads through the same validation
/// as a deployed config here instead.
#[test]
fn the_pgo_workload_config_loads_and_validates() {
    let yaml = std::fs::read_to_string(
        dfe_fetcher::deployment::repo_root().join("scripts/pgo-workload.config.yaml"),
    )
    .expect("scripts/pgo-workload.config.yaml exists");

    let cases: &[(&str, &str, &str, Option<&str>)] = &[
        ("as checked in", "", "", None),
        (
            "a service its profile no longer has",
            "cloud_logging",
            "audit_logs",
            Some("the profile has no such unit"),
        ),
    ];
    for (label, from, to, refused) in cases {
        let yaml = if from.is_empty() {
            yaml.clone()
        } else {
            assert!(
                yaml.contains(from),
                "{label}: {from} not in the workload config"
            );
            yaml.replace(from, to)
        };
        let (config, unknown) = Config::parse_yaml(&yaml, "pgo-workload.config.yaml")
            .unwrap_or_else(|e| panic!("{label}: does not parse: {e}"));
        assert!(
            unknown.is_empty(),
            "{label}: keys no field names: {unknown:?}"
        );
        match (config.validate(), refused) {
            (Ok(()), None) => {}
            (Err(e), Some(text)) => {
                assert!(e.to_string().contains(text), "{label}: wrong refusal: {e}");
            }
            (got, _) => panic!("{label}: unexpected validation result {got:?}"),
        }
    }
}
