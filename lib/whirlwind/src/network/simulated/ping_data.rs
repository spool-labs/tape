//! Real-world ping dataset loader (WonderProxy, 2020-07-19..20).
//!
//! The dataset holds average ping measurements between 200+ real cities. The raw
//! file is large and not checked in, so the download script fetches it. This
//! module loads the servers table and the pairwise average round-trip matrix,
//! giving the real inter-city latency used for the network layer of the
//! Whirlwind latency histogram.

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

/// Server ids are dense below this cap; the matrix is sized to it.
const CAP: usize = 300;
const SERVERS_CSV: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data/servers-2020-07-19.csv");
const PINGS_CSV: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data/pings.csv");
/// Mean earth radius in meters (GRS80), matching the geo crate's Haversine.
const MEAN_EARTH_RADIUS_M: f64 = 6_371_008.8;

/// A ping server, i.e. a real city, from the dataset.
#[derive(Clone, Debug, Deserialize)]
pub struct Server {
    pub id: usize,
    #[serde(rename = "name")]
    _name: String,
    #[serde(rename = "title")]
    _title: String,
    pub location: String,
    #[serde(rename = "state")]
    _state: String,
    pub country: String,
    #[serde(rename = "state_abbv")]
    _state_abbv: String,
    #[serde(rename = "continent")]
    _continent: Option<u8>,
    pub latitude: f64,
    pub longitude: f64,
}

/// A ping measurement row from the dataset.
#[derive(Debug, Deserialize)]
struct PingRow {
    source: usize,
    destination: usize,
    #[serde(rename = "timestamp")]
    _timestamp: String,
    #[serde(rename = "min")]
    _min: f64,
    avg: f64,
    #[serde(rename = "max")]
    _max: f64,
    #[serde(rename = "mdev")]
    _mdev: f64,
}

/// Loaded servers and their pairwise average round-trip latency. The latency is
/// a flat matrix of average round-trip milliseconds, zero where no measurement
/// exists.
pub struct PingData {
    servers: Vec<Server>,
    latency: Vec<f64>,
}

impl PingData {
    /// Load from the crate's data directory.
    pub fn load() -> Result<Self> {
        Self::load_from(Path::new(SERVERS_CSV), Path::new(PINGS_CSV))
    }

    pub fn load_from(servers_path: &Path, pings_path: &Path) -> Result<Self> {
        let mut servers = Vec::new();
        let mut server_reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_path(servers_path)
            .with_context(|| format!("open {}", servers_path.display()))?;
        for row in server_reader.deserialize() {
            let server: Server = row.context("parse server row")?;
            if server.id < CAP {
                servers.push(server);
            }
        }
        if servers.is_empty() {
            return Err(anyhow!("no servers loaded from {}", servers_path.display()));
        }

        let mut sums = vec![0.0f64; CAP * CAP];
        let mut counts = vec![0u32; CAP * CAP];
        let mut ping_reader = csv::Reader::from_path(pings_path)
            .with_context(|| format!("open {} (run data/download.sh)", pings_path.display()))?;
        for row in ping_reader.deserialize() {
            let ping: PingRow = row.context("parse ping row")?;
            if ping.source >= CAP || ping.destination >= CAP {
                continue;
            }
            let index = ping.source * CAP + ping.destination;
            sums[index] += ping.avg;
            counts[index] += 1;
        }
        let latency = sums
            .iter()
            .zip(counts.iter())
            .map(|(sum, count)| if *count > 0 { sum / f64::from(*count) } else { 0.0 })
            .collect();

        Ok(Self { servers, latency })
    }

    pub fn servers(&self) -> &[Server] {
        &self.servers
    }

    /// Average measured round-trip ping (ms) between two server ids, if present.
    pub fn ping_rtt_ms(&self, source: usize, destination: usize) -> Option<f64> {
        if source >= CAP || destination >= CAP {
            return None;
        }
        let value = self.latency[source * CAP + destination];
        (value > 0.0).then_some(value)
    }

}

/// Great-circle distance in meters between two (lat, lon) points in degrees.
pub fn great_circle_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();
    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.clamp(0.0, 1.0).sqrt().asin();
    MEAN_EARTH_RADIUS_M * c
}
