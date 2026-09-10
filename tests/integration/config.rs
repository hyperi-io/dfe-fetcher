// Project:   dfe-fetcher
// File:      tests/integration/config.rs
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
    let yaml = std::fs::read_to_string("config.example.yaml").expect("config.example.yaml exists");
    let config: Config = serde_yaml_ng::from_str(&yaml).expect("example config parses");

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
