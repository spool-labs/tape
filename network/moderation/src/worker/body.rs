//! Response bodies read up to a byte limit

use reqwest::Response;

/// A body cut at the limit when longer
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedBody {
    pub bytes: Vec<u8>,
    pub is_truncated: bool,
}

pub async fn read_bounded(mut response: Response, limit: usize) -> Result<BoundedBody, reqwest::Error> {
    let mut bytes = Vec::new();
    let mut is_truncated = false;
    while let Some(chunk) = response.chunk().await? {
        let room = limit.saturating_sub(bytes.len());
        if chunk.len() > room {
            bytes.extend_from_slice(&chunk[..room]);
            is_truncated = true;
            break;
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(BoundedBody {
        bytes,
        is_truncated,
    })
}
