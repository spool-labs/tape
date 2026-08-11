//! Simulated network for a Whirlwind spool group, backed by real ping data.
//!
//! A group places n spool owners at real cities and exposes the one-way
//! latency between every pair, measured round trip halved. Where the dataset has
//! no measurement for a pair, a great-circle fiber estimate fills the gap.

pub mod ping_data;

use std::collections::HashSet;

use anyhow::{anyhow, bail, Result};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use self::ping_data::{great_circle_m, PingData, Server};

/// Fiber propagation speed, roughly two thirds of c, in km per millisecond.
const FIBER_KM_PER_MS: f64 = 200.0;
/// Fixed per-hop overhead (switching, last mile) in milliseconds.
const HOP_OVERHEAD_MS: f64 = 2.0;

/// A spool group placed at real cities, with a one-way latency matrix.
pub struct Group {
    pub cities: Vec<String>,
    pub server_ids: Vec<usize>,
    one_way_ms: Vec<f64>,
    size: usize,
}

impl Group {
    /// Sample n distinct servers as spool locations, seeded for reproducibility.
    pub fn sample(data: &PingData, n: usize, seed: u64) -> Self {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut servers: Vec<&Server> = data.servers().iter().collect();
        servers.shuffle(&mut rng);
        let chosen: Vec<&Server> = servers.into_iter().take(n).collect();
        Self::from_servers(data, &chosen)
    }

    /// Place an explicit ordered committee at real cities by server id.
    ///
    /// Unlike sample, this resolves a caller-chosen list so a surviving node
    /// keeps its city across epochs and only a freed slice position takes a fresh
    /// city. Errors rather than panics on an unknown or duplicate id.
    pub fn place(data: &PingData, server_ids: &[usize]) -> Result<Self> {
        let mut chosen: Vec<&Server> = Vec::with_capacity(server_ids.len());
        let mut seen: HashSet<usize> = HashSet::with_capacity(server_ids.len());
        for id in server_ids {
            if !seen.insert(*id) {
                bail!("duplicate server id {id} in committee placement");
            }
            let server = data
                .servers()
                .iter()
                .find(|server| server.id == *id)
                .ok_or_else(|| anyhow!("unknown server id {id} in committee placement"))?;
            chosen.push(server);
        }
        Ok(Self::from_servers(data, &chosen))
    }

    fn from_servers(data: &PingData, chosen: &[&Server]) -> Self {
        let size = chosen.len();
        let mut one_way_ms = vec![0.0; size * size];
        for i in 0..size {
            for j in 0..size {
                if i == j {
                    continue;
                }
                let round_trip = data
                    .ping_rtt_ms(chosen[i].id, chosen[j].id)
                    .or_else(|| data.ping_rtt_ms(chosen[j].id, chosen[i].id))
                    .unwrap_or_else(|| fiber_rtt_ms(chosen[i], chosen[j]));
                one_way_ms[i * size + j] = round_trip / 2.0;
            }
        }
        Self {
            cities: chosen.iter().map(|server| server.location.clone()).collect(),
            server_ids: chosen.iter().map(|server| server.id).collect(),
            one_way_ms,
            size,
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// One-way latency (ms) for a message from spool from to spool to.
    pub fn one_way_ms(&self, from: usize, to: usize) -> f64 {
        self.one_way_ms[from * self.size + to]
    }
}

/// Great-circle fiber round-trip estimate for a pair with no measurement.
fn fiber_rtt_ms(a: &Server, b: &Server) -> f64 {
    let km = great_circle_m(a.latitude, a.longitude, b.latitude, b.longitude) / 1_000.0;
    2.0 * (km / FIBER_KM_PER_MS + HOP_OVERHEAD_MS)
}
