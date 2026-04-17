//! TSV journal of quorum-flow events for post-mortem diffing across nodes.
//!
//! Temporary diagnostic. Each node writes one TSV line per event to
//! `/tmp/tapedrive-quorum/node-{id}.log`. Diff any two nodes' files to spot
//! disagreements in:
//!
//! - the message or value_hash signed for a given `(epoch, group, chunk)`
//! - the peer pubkey a collector used vs the one the peer actually signed with
//! - the per-peer verification result
//! - the aggregated bitmap / sig / submit verdict
//!
//! Format (tab-separated, one line per event):
//!
//!   sign    epoch group chunk my_idx value_hash_hex msg_hex           sig_prefix
//!   peer    epoch group chunk peer_id peer_idx peer_pk_prefix sig_prefix verified(bool|err:<text>)
//!   submit  epoch group chunk bitmap_hex agg_sig_prefix result(ok|err:<text>)
//!
//! Remove this module once the investigation is complete.
//!
//! `chunk` is written as literal `-` for finalize-sig events (no chunk index).

use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::io::Write;

use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber, NodeId, SpoolGroupBitmap};
use tape_crypto::hash::Hash;
use tracing::warn;

const DIR: &str = "/tmp/tapedrive-quorum";
const PREFIX_BYTES: usize = 8;

pub fn sign(
    node_id: NodeId,
    label: &str,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: Option<ChunkNumber>,
    my_idx: usize,
    value_hash: Option<&Hash>,
    message: &[u8],
    sig: &BlsSignature,
) {
    let chunk_col = format_chunk(chunk);
    let value_hash_col = value_hash
        .map(|h| hex_all(h.to_bytes().as_slice()))
        .unwrap_or_else(|| "-".to_string());
    let line = format!(
        "sign\t{label}\t{}\t{}\t{chunk_col}\t{my_idx}\t{value_hash_col}\t{}\t{}\n",
        epoch.0,
        group.0,
        hex_prefix(message),
        hex_prefix(&sig.0.0),
    );
    write_line(node_id, &line);
}

pub fn peer(
    node_id: NodeId,
    label: &str,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: Option<ChunkNumber>,
    peer_id: NodeId,
    peer_idx: usize,
    peer_pubkey: &BlsPubkey,
    sig: Option<&BlsSignature>,
    outcome: PeerOutcome<'_>,
) {
    let chunk_col = format_chunk(chunk);
    let sig_col = sig
        .map(|s| hex_prefix(&s.0.0))
        .unwrap_or_else(|| "-".to_string());
    let outcome_col = match outcome {
        PeerOutcome::Verified => "verified".to_string(),
        PeerOutcome::BadSig => "bad_sig".to_string(),
        PeerOutcome::NodeIdMismatch { expected, got } => {
            format!("id_mismatch(expected={},got={})", expected.0, got.0)
        }
        PeerOutcome::Err(msg) => format!("err:{msg}"),
    };
    let line = format!(
        "peer\t{label}\t{}\t{}\t{chunk_col}\t{}\t{peer_idx}\t{}\t{sig_col}\t{outcome_col}\n",
        epoch.0,
        group.0,
        peer_id.0,
        hex_prefix(&peer_pubkey.0.0),
    );
    write_line(node_id, &line);
}

pub fn submit(
    node_id: NodeId,
    label: &str,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: Option<ChunkNumber>,
    bitmap: &SpoolGroupBitmap,
    agg_sig: &BlsSignature,
    result: Result<(), &str>,
) {
    let chunk_col = format_chunk(chunk);
    let result_col = match result {
        Ok(()) => "ok".to_string(),
        Err(msg) => format!("err:{msg}"),
    };
    let line = format!(
        "submit\t{label}\t{}\t{}\t{chunk_col}\t{}\t{}\t{result_col}\n",
        epoch.0,
        group.0,
        hex_all(bitmap.as_bytes()),
        hex_prefix(&agg_sig.0.0),
    );
    write_line(node_id, &line);
}

pub enum PeerOutcome<'a> {
    Verified,
    BadSig,
    NodeIdMismatch { expected: NodeId, got: NodeId },
    Err(&'a str),
}

fn format_chunk(chunk: Option<ChunkNumber>) -> String {
    match chunk {
        Some(c) => c.0.to_string(),
        None => "-".to_string(),
    }
}

fn hex_prefix(bytes: &[u8]) -> String {
    let n = bytes.len().min(PREFIX_BYTES);
    hex_all(&bytes[..n])
}

fn hex_all(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

fn write_line(node_id: NodeId, line: &str) {
    if let Err(error) = std::fs::create_dir_all(DIR) {
        warn!(?error, dir = DIR, "quorum journal: mkdir failed");
        return;
    }
    let path = format!("{DIR}/node-{}.log", node_id.0);
    let result = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| f.write_all(line.as_bytes()));
    if let Err(error) = result {
        warn!(?error, path, "quorum journal: write failed");
    }
}
