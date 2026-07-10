//! One-off debug dump for the epoch 81 stall (2026-07-07).
//! Usage: cargo run --release -p rpc-client --example epoch_debug

use rpc_client::RpcClient;
use rpc_solana::RpcConfig;
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = RpcConfig {
        endpoints: vec!["https://api.devnet.solana.com".to_string()],
        ..Default::default()
    };
    let client = RpcClient::new(config)?;

    let system = client.get_system().await?;
    println!(
        "system: current_epoch={} committee_size={} target_groups={} live_groups={} min_version={}",
        system.current_epoch.0,
        system.committee_size,
        system.target_group_count,
        system.live_group_count,
        system.min_version.0,
    );

    let nodes = client.get_all_nodes().await?;
    let name_of = |addr: &Address| -> String {
        nodes
            .iter()
            .find(|(a, _)| a == addr)
            .map(|(_, n)| {
                String::from_utf8_lossy(&n.metadata.name)
                    .trim_end_matches('\0')
                    .to_string()
            })
            .unwrap_or_else(|| format!("{addr}"))
    };

    for e in [80u64, 81, 82, 83] {
        let epoch = EpochNumber(e);
        match client.get_epoch(epoch).await {
            Ok(ep) => println!(
                "epoch {}: phase={:?} start_time={} total_groups={} assignment_hash={} snapshot_hash={} duration={}s",
                e,
                ep.state.phase(),
                ep.start_time,
                ep.total_groups,
                ep.has_assignment_hash(),
                ep.has_snapshot_hash(),
                ep.preferences.epoch_duration.0,
            ),
            Err(err) => println!("epoch {e}: <{err}>"),
        }
    }

    for e in [81u64, 82] {
        let epoch = EpochNumber(e);
        match client.get_committee(epoch).await {
            Ok(members) => {
                println!("committee {} ({} members):", e, members.len());
                for m in &members {
                    println!(
                        "  {:<22} stake={:?} spools={} node={}",
                        name_of(&m.node),
                        m.stake,
                        m.spools,
                        m.node,
                    );
                }
            }
            Err(err) => println!("committee {e}: <{err}>"),
        }
    }

    let committee_82 = client
        .get_committee(EpochNumber(82))
        .await
        .unwrap_or_default();
    let committee_81 = client
        .get_committee(EpochNumber(81))
        .await
        .unwrap_or_default();

    println!("staked nodes NOT in committee 82:");
    for (addr, node) in &nodes {
        if node.pool.stake.is_zero() {
            continue;
        }
        if committee_82.iter().any(|m| &m.node == addr) {
            continue;
        }
        let in_81 = committee_81.iter().any(|m| &m.node == addr);
        let name = String::from_utf8_lossy(&node.metadata.name)
            .trim_end_matches('\0')
            .to_string();
        println!(
            "  {:<22} in_committee_81={} latest_advance_epoch={} latest_sync_epoch={} registered={} stake={:?}",
            name,
            in_81,
            node.latest_advance_epoch.0,
            node.latest_sync_epoch.0,
            node.registered_epoch.0,
            node.pool.stake,
        );
    }

    Ok(())
}
