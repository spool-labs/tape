//! S3-compatible gateway surface.

pub mod accounting;
pub mod admin;
pub mod authz;
pub mod chunked;
pub mod clock;
pub mod error;
pub mod multipart;
pub mod resolve;
pub mod response;
pub mod routes;
pub mod sigv4;
pub mod write;
pub mod xml;
