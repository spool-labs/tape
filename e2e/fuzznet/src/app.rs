use ratatui::style::Color;
use tape_store::types::NodeStatus;

pub const NODE_EVENT_HISTORY_EPOCHS: usize = 16;

pub enum Command {
    AddNode,
    RemoveNode,
    UploadBlob,
    ToggleStakeFuzz,
    Quit,
}

#[allow(dead_code)]
#[derive(Clone, Default)]
pub struct NodeSnapshot {
    pub id: usize,
    pub sync_bytes: u64,
    pub repair_bytes: u64,
    pub upload_bytes: u64,
    pub spool_count: usize,
    pub pool_stake: u64,
    pub node_status: Option<NodeStatus>,
    pub event_history: Vec<u64>,
    pub sync_bw_history: Vec<u64>,
}

#[derive(Clone, Copy, Debug)]
pub enum TrackStatus {
    Registered,
    Certified,
    Expired,
    Failed,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub struct TrackSnapshot {
    pub status: TrackStatus,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct PollSnapshot {
    pub slot: u64,
    pub epoch: u64,
    pub tx_count: u64,
    pub runtime_secs: f64,
    pub nodes: Vec<NodeSnapshot>,
    pub spool_owners: [u8; 1000],
    pub node_count: usize,
    pub epoch_duration_history: Vec<u64>,
    pub total_store_history: Vec<u64>,
    pub repair_bw_history: Vec<u64>,
    pub sync_bw_history: Vec<u64>,
    pub upload_bw_history: Vec<u64>,
    pub total_sync_bytes: u64,
    pub total_repair_bytes: u64,
    pub total_upload_bytes: u64,
    pub total_stake: u64,
    pub log: Vec<(String, String, String, u64)>,
    pub stake_fuzz_enabled: bool,
    pub stake_fuzz_succeeded: u64,
    pub stake_fuzz_failed: u64,
    pub uploads_pending: u64,
    pub uploads_certified: u64,
    pub uploads_expired: u64,
    pub uploads_failed: u64,
    pub uploads_retries: u64,
    pub uploads_last_retry_error: Option<String>,
    pub uploads_next_retry_in_ms: Option<u64>,
    pub uploads_retry_in_progress: bool,
    pub tracks: Vec<TrackSnapshot>,
}

impl Default for PollSnapshot {
    fn default() -> Self {
        Self {
            slot: 0,
            epoch: 0,
            tx_count: 0,
            runtime_secs: 0.0,
            nodes: Vec::new(),
            spool_owners: [0u8; 1000],
            node_count: 0,
            epoch_duration_history: Vec::new(),
            total_store_history: Vec::new(),
            repair_bw_history: Vec::new(),
            sync_bw_history: Vec::new(),
            upload_bw_history: Vec::new(),
            total_sync_bytes: 0,
            total_repair_bytes: 0,
            total_upload_bytes: 0,
            total_stake: 0,
            log: Vec::new(),
            stake_fuzz_enabled: false,
            stake_fuzz_succeeded: 0,
            stake_fuzz_failed: 0,
            uploads_pending: 0,
            uploads_certified: 0,
            uploads_expired: 0,
            uploads_failed: 0,
            uploads_retries: 0,
            uploads_last_retry_error: None,
            uploads_next_retry_in_ms: None,
            uploads_retry_in_progress: false,
            tracks: Vec::new(),
        }
    }
}

pub fn node_color(index: usize) -> Color {
    if index == 0 {
        return Color::DarkGray;
    }
    let golden = 0.618033988749895_f64;
    let h = ((index as f64) * golden).fract();
    let s = match index % 3 {
        0 => 0.85,
        1 => 0.65,
        _ => 0.75,
    };
    let l = match (index / 3) % 3 {
        0 => 0.55,
        1 => 0.45,
        _ => 0.65,
    };
    hsl_to_rgb(h, s, l)
}

fn hsl_to_rgb(h: f64, s: f64, l: f64) -> Color {
    let c = (1.0 - (2.0 * l - 1.0).abs()) * s;
    let x = c * (1.0 - ((h * 6.0) % 2.0 - 1.0).abs());
    let m = l - c / 2.0;

    let (r1, g1, b1) = match (h * 6.0) as u32 {
        0 => (c, x, 0.0),
        1 => (x, c, 0.0),
        2 => (0.0, c, x),
        3 => (0.0, x, c),
        4 => (x, 0.0, c),
        _ => (c, 0.0, x),
    };

    Color::Rgb(
        ((r1 + m) * 255.0) as u8,
        ((g1 + m) * 255.0) as u8,
        ((b1 + m) * 255.0) as u8,
    )
}
