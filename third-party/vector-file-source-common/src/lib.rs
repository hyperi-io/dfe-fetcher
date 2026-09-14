#![deny(warnings)]
#![deny(clippy::all)]

pub mod buffer;
pub mod checkpointer;
mod fingerprinter;
pub mod internal_events;
mod metadata_ext;

// dfe-fetcher: `vector_config::configurable_component` (Vector's config-schema derive) is replaced by plain serde derives below.
use serde::{Deserialize, Serialize};

pub use self::{
    checkpointer::{CHECKPOINT_FILE_NAME, Checkpointer, CheckpointsView},
    fingerprinter::{FileFingerprint, FingerprintStrategy, Fingerprinter},
    internal_events::FileSourceInternalEvents,
    metadata_ext::{AsyncFileInfo, PortableFileExt},
};

pub type FilePosition = u64;

// dfe-fetcher: stands in for `vector_common::constants::GZIP_MAGIC` so the vendored crates carry no Vector dependency.
pub const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];

// dfe-fetcher: stands in for `vector_common::compression::gzip_multiple_decoder` (a multi-member gzip reader on async-compression).
pub fn gzip_multiple_decoder<R: tokio::io::AsyncBufRead>(
    reader: R,
) -> async_compression::tokio::bufread::GzipDecoder<R> {
    let mut decoder = async_compression::tokio::bufread::GzipDecoder::new(reader);
    decoder.multiple_members(true);
    decoder
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ReadFrom {
    #[default]
    Beginning,
    End,
    Checkpoint(FilePosition),
}

/// File position to use when reading a new file.
// dfe-fetcher: was `#[configurable_component]`; the serde half of that derive is what the type needs.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadFromConfig {
    /// Read from the beginning of the file.
    Beginning,

    /// Start reading from the current end of the file.
    End,
}

impl From<ReadFromConfig> for ReadFrom {
    fn from(rfc: ReadFromConfig) -> Self {
        match rfc {
            ReadFromConfig::Beginning => ReadFrom::Beginning,
            ReadFromConfig::End => ReadFrom::End,
        }
    }
}
