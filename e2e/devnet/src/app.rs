use ratatui::style::Color;
use tape_core::system::NodeStatus;

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
    pub is_running: bool,
    pub http_healthy: Option<bool>,
    pub sync_bytes: u64,
    pub repair_bytes: u64,
    pub recovery_bytes: u64,
    pub upload_bytes: u64,
    pub spool_count: usize,
    pub pool_stake: u64,
    pub node_status: Option<NodeStatus>,
    pub event_history: Vec<u64>,
    pub sync_bw_history: Vec<u64>,
}

#[allow(dead_code)]
#[derive(Clone, Default)]
pub struct SpoolSnapshot {
    pub owner: Option<usize>,
    pub available: bool,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct PollSnapshot {
    pub slot: u64,
    pub epoch: u64,
    pub epoch_phase: String,
    pub epoch_phase_weight: Option<u64>,
    pub previous_committee_size: usize,
    pub current_committee_size: usize,
    pub next_committee_size: usize,
    pub target_group_count: u64,
    pub live_group_count: u64,
    pub tx_count: u64,
    pub runtime_secs: f64,
    pub nodes: Vec<NodeSnapshot>,
    pub spools: Vec<SpoolSnapshot>,
    pub node_count: usize,
    pub tracked_node_count: usize,
    pub dead_node_count: usize,
    pub http_unhealthy_count: usize,
    pub epoch_duration_history: Vec<u64>,
    pub total_store_history: Vec<u64>,
    pub repair_bw_history: Vec<u64>,
    pub recovery_bw_history: Vec<u64>,
    pub sync_bw_history: Vec<u64>,
    pub upload_bw_history: Vec<u64>,
    pub total_sync_bytes: u64,
    pub total_repair_bytes: u64,
    pub total_recovery_bytes: u64,
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
    pub uploads_running: u64,
    pub uploads_waiting_retry: u64,
    pub uploads_stalled: u64,
    pub uploads_last_retry_error: Option<String>,
    pub uploads_next_retry_in_ms: Option<u64>,
    pub uploads_retry_in_progress: bool,
}

impl Default for PollSnapshot {
    fn default() -> Self {
        Self {
            slot: 0,
            epoch: 0,
            epoch_phase: String::new(),
            epoch_phase_weight: None,
            previous_committee_size: 0,
            current_committee_size: 0,
            next_committee_size: 0,
            target_group_count: 0,
            live_group_count: 0,
            tx_count: 0,
            runtime_secs: 0.0,
            nodes: Vec::new(),
            spools: Vec::new(),
            node_count: 0,
            tracked_node_count: 0,
            dead_node_count: 0,
            http_unhealthy_count: 0,
            epoch_duration_history: Vec::new(),
            total_store_history: Vec::new(),
            repair_bw_history: Vec::new(),
            recovery_bw_history: Vec::new(),
            sync_bw_history: Vec::new(),
            upload_bw_history: Vec::new(),
            total_sync_bytes: 0,
            total_repair_bytes: 0,
            total_recovery_bytes: 0,
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
            uploads_running: 0,
            uploads_waiting_retry: 0,
            uploads_stalled: 0,
            uploads_last_retry_error: None,
            uploads_next_retry_in_ms: None,
            uploads_retry_in_progress: false,
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
