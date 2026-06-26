use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use rpc::Rpc;
use store::Store;
use tape_core::types::ContentType;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::ObjectMetadataOps;

use crate::http::error::RouteError;
use crate::http::handlers::store_error;
use crate::http::state::AppState;

#[derive(Clone, Debug)]
pub(in crate::http::handlers::object) struct ObjectResponseMetadata {
    pub content_type: ContentType,
    pub filename: Option<Vec<u8>>,
}

pub(in crate::http::handlers::object) fn object_response_metadata<
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<ObjectResponseMetadata, RouteError> {
    let metadata = state
        .context
        .store
        .get_object_metadata(track_addr)
        .map_err(store_error)?;

    let content_type = metadata
        .as_ref()
        .map(|metadata| metadata.content_type)
        .unwrap_or(ContentType::Unknown);
    let filename = metadata
        .and_then(|metadata| (!metadata.name.is_empty()).then_some(metadata.name));

    Ok(ObjectResponseMetadata {
        content_type,
        filename,
    })
}

pub(in crate::http::handlers::object) fn object_response(
    bytes: Vec<u8>,
    metadata: &ObjectResponseMetadata,
    etag: Hash,
) -> Result<Response, RouteError> {
    let headers = object_headers(bytes.len() as u64, metadata, etag)?;
    Ok((StatusCode::OK, headers, bytes).into_response())
}

pub(in crate::http::handlers::object) fn object_headers(
    content_length: u64,
    metadata: &ObjectResponseMetadata,
    etag: Hash,
) -> Result<HeaderMap, RouteError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(metadata.content_type.to_str()),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .map_err(|error| RouteError::Internal(format!("content length header: {error}")))?,
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{etag}\""))
            .map_err(|error| RouteError::Internal(format!("etag header: {error}")))?,
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    if let Some(filename) = metadata.filename.as_deref() {
        headers.insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&content_disposition(filename)).map_err(|error| {
                RouteError::Internal(format!("content disposition header: {error}"))
            })?,
        );
    }

    Ok(headers)
}

fn content_disposition(filename: &[u8]) -> String {
    format!("attachment; filename*=UTF-8''{}", encode_rfc5987(filename))
}

// RFC 5987 encoding for the Content-Disposition `filename*` parameter.
fn encode_rfc5987(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &byte in value {
        match byte {
            b'0'..=b'9'
            | b'A'..=b'Z'
            | b'a'..=b'z'
            | b'!'
            | b'#'
            | b'$'
            | b'&'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~' => out.push(byte as char),
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use axum::http::header;
    use tape_core::types::ContentType;
    use tape_crypto::Hash;

    use super::{ObjectResponseMetadata, object_headers};

    #[test]
    fn object_headers_include_encoded_filename() {
        let metadata = ObjectResponseMetadata {
            content_type: ContentType::TextPlain,
            filename: Some(b"reports/june final.txt".to_vec()),
        };

        let headers = object_headers(42, &metadata, Hash::default()).unwrap();

        assert_eq!(
            headers
                .get(header::CONTENT_DISPOSITION)
                .and_then(|value| value.to_str().ok()),
            Some("attachment; filename*=UTF-8''reports%2Fjune%20final.txt")
        );
    }

    #[test]
    fn object_headers_skip_empty_metadata_filename() {
        let metadata = ObjectResponseMetadata {
            content_type: ContentType::Unknown,
            filename: None,
        };

        let headers = object_headers(42, &metadata, Hash::default()).unwrap();

        assert!(headers.get(header::CONTENT_DISPOSITION).is_none());
    }
}
