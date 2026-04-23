//! `tape-admin status` + `tape-admin node list` — read-only views of
//! on-chain cluster state. Meant for quick debugging when nodes misbehave.

use std::collections::HashSet;

use tape_core::types::NodeId;

use crate::context::Context;
use crate::error::Result;

/// Print a cluster-wide snapshot: epoch, node count, committee sizes,
/// quorum health.
pub async fn cluster(ctx: &Context) -> Result<()> {
    let system = ctx.rpc.get_system().await?;
    let epoch = ctx.rpc.get_epoch().await?;

    println!("cluster status");
    println!("  rpc:                {}", ctx.rpc_url);
    println!("  current epoch:      {}", epoch.id.0);
    println!("  total nodes:        {}", system.total_nodes);
    println!("  committee (prev):   {}", system.committee_prev.size());
    println!("  committee (curr):   {}", system.committee.size());
    println!("  committee (next):   {}", system.committee_next.size());
    println!(
        "  low quorum now:     {}",
        yes_no(system.is_low_quorum())
    );
    println!(
        "  low quorum next:    {}",
        yes_no(system.will_be_low_quorum())
    );
    Ok(())
}

/// Print one row per registered node. Flags membership in the current
/// committee with `Y` in the last column.
pub async fn list_nodes(ctx: &Context) -> Result<()> {
    let system = ctx.rpc.get_system().await?;
    let mut nodes = ctx.rpc.get_all_nodes().await?;
    nodes.sort_by_key(|(_, n)| n.id.0);

    let in_committee: HashSet<u64> = system
        .committee
        .iter()
        .map(|m| m.id.0)
        .collect();

    // Fixed-width columns picked to fit typical outputs (authority and
    // network_address are the widest).
    println!(
        "{:>4}  {:<44}  {:<20}  {:<24}  {:>10}  {:>5}  {:<5}",
        "ID", "AUTHORITY", "NAME", "ADDRESS", "STAKE", "REG", "COMM"
    );
    for (_, node) in nodes {
        let name = name_string(&node.metadata.name);
        let address = node
            .metadata
            .network_address
            .to_socket_addr()
            .map(|sa| sa.to_string())
            .unwrap_or_else(|e| format!("<bad:{e}>"));
        let stake_tape = node.pool.stake.as_u64() / 1_000_000;
        let flag = if in_committee.contains(&node.id.0) {
            "Y"
        } else {
            "-"
        };
        println!(
            "{:>4}  {:<44}  {:<20}  {:<24}  {:>10}  {:>5}  {:<5}",
            node.id.0,
            node.authority,
            name,
            address,
            stake_tape,
            node.registered_epoch.0,
            flag,
        );
    }
    Ok(())
}

fn name_string(bytes: &[u8; 32]) -> String {
    let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

fn yes_no(b: bool) -> &'static str {
    if b {
        "yes"
    } else {
        "no"
    }
}

#[allow(dead_code)] // reserved for a future --id flag on `node list`
fn _kp(n: NodeId) -> u64 {
    n.0
}
