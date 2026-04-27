//! `tape-admin stats` + `tape-admin node list` — read-only views of
//! on-chain cluster state. Return structured results so callers can emit
//! them as text (default) or JSON.

use std::collections::HashSet;

use serde::Serialize;
use tape_cli_common::CliOutput;

use crate::context::Context;
use crate::error::Result;

#[derive(Serialize)]
pub struct ClusterStatus {
    pub rpc: String,
    pub current_epoch: u64,
    pub total_nodes: u64,
    pub committee_prev: usize,
    pub committee: usize,
    pub committee_next: usize,
    pub low_quorum_now: bool,
    pub low_quorum_next: bool,
}

impl CliOutput for ClusterStatus {
    fn print_text(&self) {
        println!("cluster stats");
        println!("  rpc:                {}", self.rpc);
        println!("  current epoch:      {}", self.current_epoch);
        println!("  total nodes:        {}", self.total_nodes);
        println!("  committee (prev):   {}", self.committee_prev);
        println!("  committee (curr):   {}", self.committee);
        println!("  committee (next):   {}", self.committee_next);
        println!("  low quorum now:     {}", yes_no(self.low_quorum_now));
        println!("  low quorum next:    {}", yes_no(self.low_quorum_next));
    }
}

pub async fn cluster(ctx: &Context) -> Result<ClusterStatus> {
    let system = ctx.rpc.get_system().await?;
    let epoch = ctx.rpc.get_epoch().await?;
    Ok(ClusterStatus {
        rpc: ctx.rpc_url.clone(),
        current_epoch: epoch.id.0,
        total_nodes: system.total_nodes,
        committee_prev: system.committee_prev.size(),
        committee: system.committee.size(),
        committee_next: system.committee_next.size(),
        low_quorum_now: system.is_low_quorum(),
        low_quorum_next: system.will_be_low_quorum(),
    })
}

#[derive(Serialize)]
pub struct NodeRow {
    pub id: u64,
    pub authority: String,
    pub name: String,
    pub address: String,
    pub stake_tape: u64,
    pub registered_epoch: u64,
    pub in_committee: bool,
}

#[derive(Serialize)]
pub struct NodeList {
    pub count: usize,
    pub nodes: Vec<NodeRow>,
}

impl CliOutput for NodeList {
    fn print_text(&self) {
        println!(
            "{:>4}  {:<44}  {:<20}  {:<24}  {:>10}  {:>5}  {:<5}",
            "ID", "AUTHORITY", "NAME", "ADDRESS", "STAKE", "REG", "COMM"
        );
        for row in &self.nodes {
            let flag = if row.in_committee { "Y" } else { "-" };
            println!(
                "{:>4}  {:<44}  {:<20}  {:<24}  {:>10}  {:>5}  {:<5}",
                row.id,
                row.authority,
                row.name,
                row.address,
                row.stake_tape,
                row.registered_epoch,
                flag,
            );
        }
    }
}

pub async fn list_nodes(ctx: &Context) -> Result<NodeList> {
    let system = ctx.rpc.get_system().await?;
    let mut nodes = ctx.rpc.get_all_nodes().await?;
    nodes.sort_by_key(|(_, n)| n.id.0);

    let in_committee: HashSet<u64> = system.committee.iter().map(|m| m.id.0).collect();

    let rows = nodes
        .into_iter()
        .map(|(_, node)| {
            let name = name_string(&node.metadata.name);
            let address = node
                .metadata
                .network_address
                .to_socket_addr()
                .map(|sa| sa.to_string())
                .unwrap_or_else(|e| format!("<bad:{e}>"));
            let stake_tape = node.pool.stake.as_u64() / 1_000_000;
            NodeRow {
                id: node.id.0,
                authority: node.authority.to_string(),
                name,
                address,
                stake_tape,
                registered_epoch: node.registered_epoch.0,
                in_committee: in_committee.contains(&node.id.0),
            }
        })
        .collect::<Vec<_>>();

    Ok(NodeList {
        count: rows.len(),
        nodes: rows,
    })
}

fn name_string(bytes: &[u8; 32]) -> String {
    let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

fn yes_no(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}
