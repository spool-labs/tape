//! Wire types for the live network globe shown on the Colosseum display.
//!
//! Producer is the standalone collector; consumer is the atlas-web kiosk.
//! The privacy boundary lives here: only coordinates and coarse labels ever
//! cross this contract. Raw client IPs are resolved and discarded upstream (in
//! the collector) and never appear in any type below.

pub mod cities;

use serde::{Deserialize, Serialize};

/// The unified live stream: a WebSocket of binary frames, each one a
/// bincode-encoded LiveMsg. One connection carries topology, stats, traffic,
/// and recent objects together; the browser never opens more than this.
pub const LIVE_PATH: &str = "/live";

/// A geographic point in WGS84 degrees. Client points are snapped to a city
/// centroid before they become one of these; a raw address never does.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct GeoPoint {
    pub lat: f32,
    pub lon: f32,
}

impl GeoPoint {
    pub const fn new(lat: f32, lon: f32) -> Self {
        Self { lat, lon }
    }
}

/// A storage site: one or more nodes co-located in a city or datacenter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Site {
    /// Stable identifier, referenced by traffic sources.
    pub id: u32,
    /// Display label, already uppercased, e.g. "HELSINKI, FI".
    pub city: String,
    /// Where the site sits.
    pub at: GeoPoint,
    /// How many nodes run there.
    pub nodes: u32,
    /// How many of them are currently feeding the display; the kiosk calls
    /// out the difference so a quiet site is never mistaken for a healthy one.
    pub reporting: u32,
}

/// Where the network sits: the fixed sites, plus recently active client
/// locations (coarsened, deduped upstream).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Topology {
    pub sites: Vec<Site>,
    pub clients: Vec<GeoPoint>,
}

/// High-level counters for the stat strip.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LiveStats {
    pub bytes_per_sec: u64,
    pub nodes: u32,
    pub sites: u32,
    pub clients: u32,
    pub epoch: u64,
    pub slot: u64,
}

/// What one traffic line means. The renderer maps each variant to a look:
/// `NodeSync` is a braided brand-color bundle, the user variants are a single
/// white line whose draw direction tells upload from fetch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrafficKind {
    /// Node-to-node backbone: sync, repair, or slice pull between sites.
    NodeSync,
    /// A user reading data from a node; the line draws toward the user.
    UserFetch,
    /// A user writing data to a node; the line draws toward the node.
    UserUpload,
}

impl TrafficKind {
    /// Whether this event renders as the multi-strand braided bundle.
    pub fn is_braided(self) -> bool {
        matches!(self, TrafficKind::NodeSync)
    }
}

/// One transfer, geolocated to both ends. Each event becomes one arc (a braid
/// for `NodeSync`, a single white line otherwise).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct TrafficEvent {
    pub kind: TrafficKind,
    pub from: GeoPoint,
    pub to: GeoPoint,
    pub bytes: u64,
}

/// A recently stored object, for the live activity ticker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecentObject {
    /// Coarsened display label, a short key or hash, never a raw filename.
    pub label: String,
    /// Object size in bytes.
    pub size: u64,
    /// Content type or category, e.g. "image/png" or "track".
    pub kind: String,
    /// Unix seconds; the display renders age against its own clock.
    pub ts: u64,
}

/// One framed message on the live stream, bincode-encoded. The collector sends
/// a `Topology` then a `Stats` on connect (replaying recent `Object`s), then
/// streams `Traffic` per transfer, new `Object`s, periodic `Stats`, and a fresh
/// `Topology` whenever placement changes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LiveMsg {
    Topology(Topology),
    Stats(LiveStats),
    Traffic(TrafficEvent),
    Object(RecentObject),
}

#[cfg(test)]
mod tests {
    use super::*;

    // a live message survives the bincode round trip unchanged
    #[test]
    fn round_trip() {
        let msg = LiveMsg::Traffic(TrafficEvent {
            kind: TrafficKind::UserUpload,
            from: GeoPoint::new(1.35, 103.82),
            to: GeoPoint::new(50.48, 12.37),
            bytes: 4096,
        });

        let bytes = bincode::serialize(&msg).expect("serialize");
        let back: LiveMsg = bincode::deserialize(&bytes).expect("deserialize");

        assert_eq!(msg, back);
    }

    // only node sync renders as the braided bundle
    #[test]
    fn braided_kinds() {
        assert!(TrafficKind::NodeSync.is_braided());
        assert!(!TrafficKind::UserFetch.is_braided());
        assert!(!TrafficKind::UserUpload.is_braided());
    }
}
