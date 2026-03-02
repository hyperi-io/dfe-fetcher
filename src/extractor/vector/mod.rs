// Project:   dfe-fetcher
// File:      src/extractor/vector/mod.rs
// Purpose:   Vector.dev integration for data extraction
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector.dev extractor integration.
//!
//! Runs Vector instances as data extractors, receiving data via the native
//! Vector gRPC sink protocol (supported in hyperi-rustlib).
//!
//! ## Modes
//!
//! 1. **Container mode** — Vector runs as a managed container, configured via
//!    a generated `vector.toml`. Data flows: external source → Vector → gRPC → fetcher.
//!
//! 2. **Sidecar mode** — Vector runs alongside the fetcher (e.g., in same pod),
//!    connecting to the fetcher's gRPC endpoint.
//!
//! ## Configuration
//!
//! ```yaml
//! extractors:
//!   vector:
//!     enabled: true
//!     grpc_bind_address: "0.0.0.0:6000"
//!     instances:
//!       - name: syslog-collector
//!         mode: container
//!         image: timberio/vector:latest-alpine
//!         vector_config: |
//!           [sources.syslog]
//!           type = "syslog"
//!           address = "0.0.0.0:514"
//!           [sinks.dfe]
//!           type = "vector"
//!           address = "host.docker.internal:6000"
//!         topic: syslog_land
//! ```

use tracing::info;

use crate::config::VectorExtractorConfig;
use crate::error::Result;

/// Vector extractor manager.
///
/// Manages Vector instances and receives data via gRPC.
pub struct VectorManager {
    config: VectorExtractorConfig,
}

impl VectorManager {
    /// Create a new Vector manager.
    pub fn new(config: VectorExtractorConfig) -> Self {
        Self { config }
    }

    /// Start the Vector gRPC receiver and managed instances.
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        info!(
            grpc_bind = %self.config.grpc_bind_address,
            instances = self.config.instances.len(),
            "Starting Vector extractor manager"
        );

        // TODO: Start gRPC server using rustlib's Vector protocol support
        // TODO: Start managed container instances
        info!("Vector manager - not yet implemented");

        Ok(())
    }

    /// Stop all managed Vector instances.
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping Vector extractor manager");
        // TODO: Stop gRPC server and managed containers
        Ok(())
    }

    /// Check if Vector manager is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}
