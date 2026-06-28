// Project:   dfe-fetcher
// File:      src/config/shared.rs
// Purpose:   Thread-safe shared configuration with hot-reload support
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared configuration with hot-reload support.
//!
//! Re-exports `scalo::config::shared::SharedConfig<Config>` as
//! `SharedConfig` for backward compatibility. All DFE components share
//! the same generic abstraction from rustlib.

use super::Config;

/// Thread-safe shared configuration with version tracking.
///
/// This is a type alias for the generic `SharedConfig<T>` from rustlib,
/// specialised to dfe-fetcher's `Config` struct.
pub type SharedConfig = scalo::config::shared::SharedConfig<Config>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_config_version_increments() {
        let config = Config::default();
        let shared = SharedConfig::new(config.clone());

        assert_eq!(shared.version(), 0);

        shared.update(config.clone());
        assert_eq!(shared.version(), 1);

        shared.update(config);
        assert_eq!(shared.version(), 2);
    }

    #[tokio::test]
    async fn test_shared_config_subscription() {
        let config = Config::default();
        let shared = SharedConfig::new(config.clone());

        let mut rx = shared.subscribe();

        // Update config
        shared.update(config);

        // Should receive notification
        rx.changed().await.expect("should receive change");
        assert_eq!(*rx.borrow(), 1);
    }

    #[test]
    fn test_shared_config_with_closure() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        let interval = shared.with(|c| c.scheduler.default_interval_secs);
        assert_eq!(interval, 300);
    }

    #[test]
    fn test_shared_config_get_returns_current() {
        let mut config = Config::default();
        config.scheduler.default_interval_secs = 60;
        let shared = SharedConfig::new(config);

        let got = shared.get();
        assert_eq!(got.scheduler.default_interval_secs, 60);
    }
}
