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
//! - `dfe_fetcher_plugin_create(config_json: *const c_char) -> *mut c_void`
//! - `dfe_fetcher_plugin_destroy(ptr: *mut c_void)`
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
//!
//! ## Note
//!
//! Actual dynamic loading requires `unsafe` code. This crate uses
//! `#![forbid(unsafe_code)]`, so plugin loading is currently a stub.
//! To enable real plugin loading, either:
//! 1. Move the plugin loader to a separate crate without `forbid(unsafe_code)`
//! 2. Change `forbid` to `deny` and add `#[allow(unsafe_code)]` on this module

use std::collections::HashMap;
use std::path::Path;

use tracing::{info, warn};

use crate::error::Result;

/// Plugin registry for loaded extractor plugins.
pub struct PluginRegistry {
    plugins: HashMap<String, PluginEntry>,
}

/// Metadata about a loaded (or attempted) plugin.
struct PluginEntry {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    path: String,
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
        directory: Option<&str>,
        plugins: &HashMap<String, crate::config::PluginEntry>,
    ) -> Result<()> {
        // Load explicitly configured plugins
        for (name, entry) in plugins {
            self.register_plugin(name, &entry.path);
            info!(name = name, path = %entry.path, "Plugin registered (loading deferred — requires unsafe)");
        }

        // Scan directory for additional plugins
        if let Some(dir) = directory {
            let dir_path = Path::new(dir);
            if dir_path.is_dir() {
                if let Ok(entries) = std::fs::read_dir(dir_path) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().is_some_and(|ext| ext == "so") {
                            let file_name = path
                                .file_stem()
                                .and_then(|s| s.to_str())
                                .unwrap_or("unknown")
                                .to_string();

                            // Skip if already loaded by name
                            if self.plugins.contains_key(&file_name) {
                                continue;
                            }

                            let path_str = path.to_string_lossy().to_string();
                            self.register_plugin(&file_name, &path_str);
                            info!(name = %file_name, path = %path_str, "Plugin found in directory (loading deferred)");
                        }
                    }
                }
            } else {
                warn!(dir = dir, "Plugin directory does not exist");
            }
        }

        info!(count = self.plugins.len(), "Plugins registered");
        Ok(())
    }

    /// Register a plugin (without actually loading it, since that requires unsafe).
    fn register_plugin(&mut self, name: &str, path: &str) {
        self.plugins.insert(
            name.to_string(),
            PluginEntry {
                name: name.to_string(),
                path: path.to_string(),
            },
        );
    }

    /// Get number of registered plugins.
    pub fn len(&self) -> usize {
        self.plugins.len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    /// Get all registered plugin names.
    pub fn names(&self) -> Vec<&str> {
        self.plugins.keys().map(String::as_str).collect()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_registry() {
        let registry = PluginRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_load_no_plugins() {
        let mut registry = PluginRegistry::new();
        let plugins = HashMap::new();
        let result = registry.load_from_config(None, &plugins);
        assert!(result.is_ok());
        assert!(registry.is_empty());
    }

    #[test]
    fn test_load_nonexistent_directory() {
        let mut registry = PluginRegistry::new();
        let plugins = HashMap::new();
        let result = registry.load_from_config(Some("/nonexistent/path"), &plugins);
        assert!(result.is_ok());
        assert!(registry.is_empty());
    }
}
