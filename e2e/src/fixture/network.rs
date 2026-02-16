use std::sync::Arc;

use anyhow::Result;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::signature::Keypair;
use tape_node::features::epoch::refresh_state;

use crate::harness::fixture::client::{build_client, owned_spool, seed_authorization};
use crate::harness::fixture::runtime::{run_node, NodeRun};
use crate::harness::log::append_log;
use crate::harness::node::{build_nodes, SimNode};

pub struct SimNet {
    pub nodes: Vec<SimNode>,
    pub rpc: LiteSvmRpc,
    pub payer: Arc<Keypair>,
    pub client: RpcClient<LiteSvmRpc>,
    runs: Vec<Option<NodeRun>>,
    errs: Vec<Option<String>>,
    next_index: usize,
}

impl SimNet {
    pub async fn new(count: usize) -> Result<Self> {
        let nodes = build_nodes(count).await?;
        let rpc = nodes
            .first()
            .map(|node| node.rpc.clone())
            .unwrap_or_default();
        let payer = nodes
            .first()
            .map(|node| Arc::clone(&node.payer))
            .unwrap_or_else(|| Arc::new(Keypair::new()));
        let client = RpcClient::from_rpc(rpc.clone());

        let mut runs = Vec::with_capacity(nodes.len());
        runs.resize_with(nodes.len(), || None);
        let mut errs = Vec::with_capacity(nodes.len());
        errs.resize_with(nodes.len(), || None);

        Ok(Self {
            next_index: nodes.len(),
            nodes,
            rpc,
            payer,
            client,
            runs,
            errs,
        })
    }

    pub fn get_node(&self, index: usize) -> &SimNode {
        &self.nodes[index]
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn next_index(&self) -> usize {
        self.next_index
    }

    pub fn build_client(&self, source: usize, target: usize) -> tape_node_client::NodeClient {
        build_client(&self.nodes[source], &self.nodes[target])
    }

    pub fn seed_authorization(&self, source: usize, target: usize) {
        seed_authorization(&self.nodes[source], &self.nodes[target]);
    }

    pub fn owned_spool(&self, index: usize) -> Option<u16> {
        owned_spool(&self.nodes[index])
    }

    pub async fn refresh_nodes(&self) {
        for node in &self.nodes {
            refresh_state(&node.ctx).await.unwrap();
        }
    }

    pub async fn start_node(&mut self, index: usize) -> bool {
        if let Some(run) = &self.runs[index] {
            return run.api_up;
        }

        append_log(&format!("start node index={index}"));
        match run_node(&self.nodes[index]).await {
            Ok(run) => {
                let api_up = run.api_up;
                self.runs[index] = Some(run);
                self.errs[index] = if api_up {
                    None
                } else {
                    Some("api bind not permitted in this environment".to_string())
                };
                append_log(&format!("start node done index={index} api_up={api_up}"));
                api_up
            }
            Err(err) => {
                self.errs[index] = Some(err.to_string());
                append_log(&format!("start node fail index={index} err={err}"));
                false
            }
        }
    }

    pub fn start_err(&self, index: usize) -> Option<&str> {
        self.errs[index].as_deref()
    }

    pub async fn start_pair(&mut self, left: usize, right: usize) -> bool {
        if !self.start_node(left).await {
            return false;
        }

        if self.start_node(right).await {
            return true;
        }

        if let Some(run) = self.runs[left].take() {
            run.stop().await;
        }
        false
    }

    pub async fn stop_nodes(&mut self) {
        for run in &mut self.runs {
            if let Some(run) = run.take() {
                run.stop().await;
            }
        }
    }

    pub(crate) fn push_nodes(&mut self, mut nodes: Vec<SimNode>) {
        self.next_index += nodes.len();
        self.runs
            .extend(std::iter::repeat_with(|| None).take(nodes.len()));
        self.errs
            .extend(std::iter::repeat_with(|| None).take(nodes.len()));
        self.nodes.append(&mut nodes);
    }
}
