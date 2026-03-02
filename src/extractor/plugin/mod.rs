// Project:   dfe-fetcher
// File:      src/extractor/plugin/mod.rs
// Purpose:   Dynamically loaded Rust plugin extractors (.so)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Plugin-based extractor loading.
//!
//! Dynamically loads Rust `.so` modules that implement the `Source` trait.
//! Same plugin system pattern as dfe-receiver's protocol plugins.
//!
//! ## Plugin Interface
//!
//! Plugins must expose C ABI functions:
//! - `dfe_fetcher_plugin_create(config_json: *const c_char) -> *mut Source`
//! - `dfe_fetcher_plugin_destroy(source: *mut Source)`
//! - `dfe_fetcher_plugin_name() -> *const c_char`
//!
//! ## Configuration
//!
//! ```yaml
//! extractors:
//!   plugins:
//!     directory: "/opt/dfe/plugins"
//!     my_custom_source:
//!       path: "/opt/dfe/plugins/libdfe_fetcher_plugin_custom.so"
//!       topic: "custom_land"
//!       interval_secs: 60
//! ```

use std::collections::HashMap;

use tracing::info;

use crate::error::Result;

/// Plugin registry for loaded extractor plugins.
pub struct PluginRegistry {
    /// Loaded plugins by name.
    plugins: HashMap<String, LoadedPlugin>,
}

/// A loaded plugin with its configuration.
#[allow(dead_code)]
struct LoadedPlugin {
    /// Plugin name.
    name: String,

    /// Path to the .so file.
    path: String,

    /// Plugin-specific configuration (JSON).
    config: serde_json::Value,
}

impl PluginRegistry {
    /// Create a new empty plugin registry.
    pub fn new() -> Self {
        Self {
            plugins: HashMap::new(),
        }
    }

    /// Load plugins from configuration.
    pub fn load_from_config(
        &mut self,
        _directory: Option<&str>,
        _plugins: &HashMap<String, crate::config::PluginEntry>,
    ) -> Result<()> {
        // TODO: Implement dynamic library loading
        // - Scan directory for .so files
        // - Load named plugins from config
        // - Call create function with config JSON
        info!("Plugin loading - not yet implemented");
        Ok(())
    }

    /// Get number of loaded plugins.
    pub fn len(&self) -> usize {
        self.plugins.len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}
