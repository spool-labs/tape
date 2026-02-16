use anyhow::Result;
use tape_node::runtime::{start, RuntimeHandle};
use tracing::info;

use crate::harness::log::append_log;
use crate::harness::fixture::server::start_node;
use crate::harness::node::SimNode;

pub struct NodeRun {
    pub api_up: bool,
    run: Option<RuntimeHandle>,
}

impl NodeRun {
    pub async fn stop(self) {
        append_log("sim stop node");
        if let Some(run) = self.run {
            let _ = run.shutdown().await;
        }
        append_log("sim stop node done");
    }
}

pub async fn run_node(node: &SimNode) -> Result<NodeRun> {
    info!(name = %node.ctx.config.name, "sim start node");
    append_log(&format!("sim start node name={}", node.ctx.config.name));
    let server = start_node(node).await?;
    let api_up = server.is_some();
    let run = start(std::sync::Arc::clone(&node.ctx), server);

    if api_up {
        info!(name = %node.ctx.config.name, "sim node api up");
        append_log(&format!("sim node api up name={}", node.ctx.config.name));
    } else {
        info!(name = %node.ctx.config.name, "sim node api skip");
        append_log(&format!("sim node api skip name={}", node.ctx.config.name));
    }

    Ok(NodeRun {
        api_up,
        run: Some(run),
    })
}
