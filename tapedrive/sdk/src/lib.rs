//! High-level SDK for tapedrive blob upload/download operations.
//!
//! This crate provides `TapeClient` for uploading and downloading blobs
//! to/from the tapedrive network.

pub mod client;
pub mod communication;
pub mod downloader;
pub mod error;
pub mod uploader;

pub use client::TapeClient;
pub use downloader::ParallelDownloader;
pub use error::{ClientError, DownloadError, UploadError};
pub use uploader::DistributedUploader;
