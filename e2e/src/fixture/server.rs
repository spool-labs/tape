use anyhow::Result;
use tape_node::features::api::ServerHandle;

use crate::harness::node::{start_api, SimNode};

pub fn bind_permission_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("Operation not permitted")
            || message.contains("Permission denied")
            || message.contains("os error 1")
    })
}

pub async fn start_node(node: &SimNode) -> Result<Option<ServerHandle>> {
    match start_api(node).await {
        Ok(handle) => Ok(Some(handle)),
        Err(err) => {
            if bind_permission_error(&err) {
                eprintln!("skipping: socket bind not permitted in this environment");
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}
