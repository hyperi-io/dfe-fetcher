// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/container_hygiene.rs
// Purpose:   Pin the test-container naming and labelling convention
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The naming and labelling convention for containers this suite starts.
//!
//! Several runs share a developer machine, so a container has to say what it
//! is, which suite started it, and whose run owns it, or nobody can tell from
//! `docker ps` what is safe to remove. scalo's `shared_service` names and
//! labels them, and these tests pin that this suite is wired to it.

use crate::common;
use crate::common::{SUITE, container_name};

/// Every name starts with the suite prefix, so one `docker ps` filter finds
/// everything this repo's suite started.
#[test]
fn names_share_one_greppable_prefix() {
    let prefix = format!("{}-integration-", SUITE.prefix());
    for name in [
        container_name("some_test", "postgres"),
        container_name("other_test", "mariadb"),
    ] {
        assert!(name.starts_with(&prefix), "{name} lacks {prefix}");
    }
    assert_eq!(SUITE.label(), "dfe-fetcher-integration");
}

/// Docker only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*`, and a Rust test path
/// carries colons, so the name is normalised rather than refused at create.
#[test]
fn container_name_is_a_legal_docker_name() {
    let name = container_name("credentials::test_vault_resolve", "OpenBao");
    let mut chars = name.chars();
    assert!(
        chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')),
        "{name} is not a legal Docker container name"
    );
    assert_ne!(
        container_name("first_test", "postgres"),
        container_name("second_test", "postgres"),
        "two tests holding their own container do not collide on its name"
    );
}

/// A REAL shared container carries the suite's name and labels, and a second
/// acquire in the same run joins it rather than starting another.
#[tokio::test]
async fn a_shared_container_carries_the_suite_name_and_labels() {
    let Some(vault) = common::VaultTestConfig::acquire("container-hygiene").await else {
        eprintln!("skipping: no Docker and no live OpenBao");
        return;
    };
    let Some(container) = vault.container() else {
        eprintln!("skipping: a live OpenBao is configured, so no container was started");
        return;
    };
    let inspect = |field: &str| {
        let out = std::process::Command::new("docker")
            .args(["inspect", &container.name, "--format", field])
            .output()
            .expect("docker inspect");
        assert!(
            out.status.success(),
            "{} is not a container",
            container.name
        );
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    };
    assert!(
        container
            .name
            .starts_with(&format!("{}-integration-openbao", SUITE.prefix())),
        "{}",
        container.name
    );
    assert_eq!(
        inspect(r#"{{index .Config.Labels "io.hyperi.test.suite"}}"#),
        SUITE.label()
    );
    assert_eq!(
        inspect(r#"{{index .Config.Labels "io.hyperi.test.service"}}"#),
        "openbao"
    );
    let again = common::VaultTestConfig::acquire("container-hygiene-again")
        .await
        .expect("the same OpenBao");
    assert_eq!(
        again.container().map(|c| c.name.as_str()),
        Some(container.name.as_str()),
        "a second acquire joins the run's container"
    );
}
