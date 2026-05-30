use std::collections::BTreeMap;
use std::fmt::{self, Write as _};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use peer_tls::{apply_pinned_tls, install_default_provider};
use tape_core::tape::{tape_namespace, TapeFlags};
use tape_core::system::{NodeStatus, SpoolState};
use tape_core::track::types::CompressedTrack;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::ops::{ObjectInfoOps, SpoolOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, TapeInfo};
use tracing::trace;

use crate::log::{log_path, read_log};
use crate::scenario::SimnetScenario;

const TRACK_SCAN_BATCH: usize = 1024;
const REPORT_SAMPLE_LIMIT: usize = 20;

#[derive(Clone, Debug, PartialEq)]
struct TrackSnapshot {
    metadata: CompressedTrack,
    object_info: Option<ObjectInfo>,
}

#[derive(Clone, Debug)]
struct ReplayStoreSnapshot {
    node_index: usize,
    tapes: BTreeMap<Address, TapeInfo>,
    tracks: BTreeMap<Address, TrackSnapshot>,
}

#[derive(Debug)]
struct LateTrackIssue {
    track: Address,
    parent_tape: Address,
    metadata: CompressedTrack,
    object_info: Option<ObjectInfo>,
    parent_tape_on_baseline: usize,
    parent_tape_info: Option<TapeInfo>,
    baseline_count: usize,
    track_on_baseline: usize,
    baseline_object_info: Option<ObjectInfo>,
    issue: &'static str,
}

#[derive(Debug)]
pub struct ReplayStoreDiff {
    report: String,
    missing_tapes_on_all_baselines: usize,
    missing_tracks_on_all_baselines: usize,
    late_orphan_tracks: usize,
    late_system_tracks_marked_valid: usize,
}

impl ReplayStoreDiff {
    pub fn is_clean(&self) -> bool {
        !self.has_missing_baseline_items()
            && self.late_orphan_tracks == 0
            && self.late_system_tracks_marked_valid == 0
    }

    pub fn has_missing_baseline_items(&self) -> bool {
        self.missing_tapes_on_all_baselines > 0 || self.missing_tracks_on_all_baselines > 0
    }
}

impl fmt::Display for ReplayStoreDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.report)
    }
}

impl SimnetScenario<'_> {
    pub async fn is_node_healthy(&self, index: usize) -> bool {
        install_default_provider();

        let node = match self.harness.node(index) {
            Some(node) => node,
            None => return false,
        };

        let url = format!("{}/v1/health", node.base_url());

        let builder = reqwest::Client::builder().timeout(Duration::from_secs(2));
        let builder = match apply_pinned_tls(builder, node.tls_pubkey()) {
            Ok(b) => b,
            Err(_) => return false,
        };
        let client = match builder.build() {
            Ok(c) => c,
            Err(_) => return false,
        };

        match client.get(url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    pub async fn wait_node_healthy(&self, index: usize, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        trace!(index, timeout_secs = timeout.as_secs(), "wait_node_healthy start");
        while start.elapsed() < timeout {
            if self.is_node_healthy(index).await {
                trace!(index, elapsed_ms = start.elapsed().as_millis(), "wait_node_healthy success");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        bail!("node {index} did not become healthy within {timeout:?}");
    }

    pub async fn wait_nodes_healthy(&self, timeout: Duration) -> Result<()> {
        trace!(
            node_count = self.harness.nodes().len(),
            timeout_secs = timeout.as_secs(),
            "wait_nodes_healthy start"
        );
        for i in 0..self.harness.nodes().len() {
            self.wait_node_healthy(i, timeout).await?;
        }
        trace!(
            node_count = self.harness.nodes().len(),
            "wait_nodes_healthy complete"
        );
        Ok(())
    }

    pub async fn wait_nodes_active(&self, indices: &[usize], timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            if indices.iter().all(|&i| self.node_status(i) == Some(NodeStatus::Active)) {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                let statuses: Vec<_> = indices.iter()
                    .map(|&i| (i, self.node_status(i)))
                    .filter(|(_, s)| *s != Some(NodeStatus::Active))
                    .collect();
                bail!("nodes did not reach Active within {timeout:?}: {statuses:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub fn read_node_log(&self, index: usize) -> Option<String> {
        let node = self.harness.node(index)?;
        let raw = read_log()?;
        let name = node.name();

        let lines: Vec<_> = raw.lines().filter(|line| line.contains(name)).collect();
        if lines.is_empty() {
            Some(raw)
        } else {
            Some(lines.join("\n"))
        }
    }

    pub fn read_runtime_log(&self, index: usize) -> Option<String> {
        let node = self.harness.node(index)?;
        let raw = read_log()?;
        let name = node.name();

        let lines: Vec<_> = raw
            .lines()
            .filter(|line| line.contains("tape_node"))
            .filter(|line| line.contains(name))
            .collect();

        if lines.is_empty() {
            None
        } else {
            Some(lines.join("\n"))
        }
    }

    pub fn node_urls(&self) -> Vec<String> {
        self.harness
            .nodes()
            .iter()
            .map(|node| node.base_url())
            .collect()
    }

    pub fn log_file(&self) -> Option<String> {
        let _ = self.harness.nodes().first()?;
        log_path().map(|p| p.display().to_string())
    }

    pub fn check_node_stores(&self) -> Result<()> {
        for node in self.harness.nodes() {
            let _status = node.context().node_status();
            // Recovery status is no longer tracked in ChainState;
            // node_status() derives Active/Standby from committee membership.
        }
        Ok(())
    }

    pub fn compare_replay_stores(
        &self,
        baseline_indices: &[usize],
        late_index: usize,
    ) -> Result<ReplayStoreDiff> {
        let baselines = baseline_indices
            .iter()
            .copied()
            .map(|index| self.collect_replay_store_snapshot(index))
            .collect::<Result<Vec<_>>>()?;
        let late = self.collect_replay_store_snapshot(late_index)?;

        Ok(build_replay_store_diff(&baselines, &late))
    }

    fn collect_replay_store_snapshot(&self, index: usize) -> Result<ReplayStoreSnapshot> {
        let node = self
            .harness
            .node(index)
            .with_context(|| format!("node {index} missing"))?;
        let ctx = node.context();
        let store = &ctx.store;

        let tapes = store
            .iter_all_tapes()
            .with_context(|| format!("iter_all_tapes node {index}"))?
            .into_iter()
            .collect();

        let mut tracks = BTreeMap::new();
        let mut cursor = None;
        loop {
            let batch = store
                .iter_tracks_from(cursor, TRACK_SCAN_BATCH)
                .with_context(|| format!("iter_tracks_from node {index}"))?;
            if batch.is_empty() {
                break;
            }

            let last = batch.last().map(|(track, _)| *track);
            for (track, metadata) in batch {
                let object_info = store
                    .get_object_info(track)
                    .with_context(|| format!("get_object_info node {index} track {track}"))?;
                tracks.insert(
                    track,
                    TrackSnapshot {
                        metadata,
                        object_info,
                    },
                );
            }
            cursor = last;
        }

        Ok(ReplayStoreSnapshot {
            node_index: index,
            tapes,
            tracks,
        })
    }

    pub fn node_spool_count(&self, index: usize) -> Result<usize> {
        let node = self
            .harness
            .node(index)
            .with_context(|| format!("node {index} missing"))?;
        let spools = node.context().store.iter_all_spools()
            .with_context(|| format!("iter_all_spools node {index}"))?;
        Ok(spools.len())
    }

    pub fn node_spool_statuses(&self, index: usize) -> Result<Vec<(SpoolIndex, SpoolState)>> {
        let node = self
            .harness
            .node(index)
            .with_context(|| format!("node {index} missing"))?;
        node.context().store.iter_all_spools()
            .with_context(|| format!("iter_all_spools node {index}"))
    }

    pub fn total_spool_count(&self, indices: &[usize]) -> Result<usize> {
        let mut total = 0;
        for &i in indices {
            total += self.node_spool_count(i)?;
        }
        Ok(total)
    }
}

fn build_replay_store_diff(
    baselines: &[ReplayStoreSnapshot],
    late: &ReplayStoreSnapshot,
) -> ReplayStoreDiff {
    let baseline_count = baselines.len();
    let mut baseline_tapes = BTreeMap::<Address, BaselinePresence<TapeInfo>>::new();
    let mut baseline_tracks = BTreeMap::<Address, BaselinePresence<TrackSnapshot>>::new();

    for baseline in baselines {
        for (&address, info) in &baseline.tapes {
            baseline_tapes
                .entry(address)
                .or_insert_with(|| BaselinePresence::new(info.clone()))
                .observe(info);
        }
        for (&address, track) in &baseline.tracks {
            baseline_tracks
                .entry(address)
                .or_insert_with(|| BaselinePresence::new(track.clone()))
                .observe(track);
        }
    }

    let missing_tapes_all = baseline_tapes
        .iter()
        .filter(|(address, presence)| {
            presence.count == baseline_count && !late.tapes.contains_key(address)
        })
        .count();
    let missing_tracks_all = baseline_tracks
        .iter()
        .filter(|(address, presence)| {
            presence.count == baseline_count && !late.tracks.contains_key(address)
        })
        .count();

    let mut late_issues = Vec::new();
    for (&track, snapshot) in &late.tracks {
        let parent_tape = snapshot.metadata.tape;
        let parent_tape_on_baseline = baseline_tapes
            .get(&parent_tape)
            .map_or(0, |presence| presence.count);
        let parent_tape_info = baseline_tapes
            .get(&parent_tape)
            .map(|presence| presence.first.clone());
        let track_on_baseline = baseline_tracks
            .get(&track)
            .map_or(0, |presence| presence.count);
        let baseline_object_info = baseline_tracks
            .get(&track)
            .and_then(|presence| presence.first.object_info.clone());

        if !late.tapes.contains_key(&parent_tape) {
            late_issues.push(LateTrackIssue {
                track,
                parent_tape,
                metadata: snapshot.metadata,
                object_info: snapshot.object_info.clone(),
                parent_tape_on_baseline,
                parent_tape_info,
                baseline_count,
                track_on_baseline,
                baseline_object_info,
                issue: "missing parent tape",
            });
            continue;
        }

        let Some(tape) = late.tapes.get(&parent_tape) else {
            continue;
        };
        if TapeFlags::is_system(tape.flags)
            && matches!(snapshot.object_info, Some(ObjectInfo::Valid { .. }))
        {
            late_issues.push(LateTrackIssue {
                track,
                parent_tape,
                metadata: snapshot.metadata,
                object_info: snapshot.object_info.clone(),
                parent_tape_on_baseline,
                parent_tape_info,
                baseline_count,
                track_on_baseline,
                baseline_object_info,
                issue: "system parent classified as valid",
            });
        }
    }

    let late_orphan_tracks = late_issues
        .iter()
        .filter(|issue| issue.issue == "missing parent tape")
        .count();
    let late_system_tracks_marked_valid = late_issues
        .iter()
        .filter(|issue| issue.issue == "system parent classified as valid")
        .count();

    let mut report = String::new();
    writeln!(
        &mut report,
        "replay store comparison: baselines={:?} late=node {}",
        baselines.iter().map(|s| s.node_index).collect::<Vec<_>>(),
        late.node_index
    )
    .expect("write report");
    writeln!(
        &mut report,
        "  late tapes={} tracks={}",
        late.tapes.len(),
        late.tracks.len()
    )
    .expect("write report");
    writeln!(
        &mut report,
        "  baseline union tapes={} tracks={}",
        baseline_tapes.len(),
        baseline_tracks.len()
    )
    .expect("write report");
    writeln!(
        &mut report,
        "  missing on late but present on all baselines: tapes={} tracks={}",
        missing_tapes_all,
        missing_tracks_all
    )
    .expect("write report");
    writeln!(
        &mut report,
        "  late invariant issues: orphan_tracks={} system_tracks_marked_valid={}",
        late_orphan_tracks,
        late_system_tracks_marked_valid
    )
    .expect("write report");

    if !late_issues.is_empty() {
        writeln!(&mut report, "\nlate track issues:").expect("write report");
        for issue in late_issues.iter().take(REPORT_SAMPLE_LIMIT) {
            write_late_track_issue(&mut report, issue);
        }
        if late_issues.len() > REPORT_SAMPLE_LIMIT {
            writeln!(
                &mut report,
                "  ... {} more late track issues omitted",
                late_issues.len() - REPORT_SAMPLE_LIMIT
            )
            .expect("write report");
        }
    }

    write_missing_tape_samples(&mut report, &baseline_tapes, &late.tapes, baseline_count);
    write_missing_track_samples(&mut report, &baseline_tracks, &late.tracks, baseline_count);

    ReplayStoreDiff {
        report,
        missing_tapes_on_all_baselines: missing_tapes_all,
        missing_tracks_on_all_baselines: missing_tracks_all,
        late_orphan_tracks,
        late_system_tracks_marked_valid,
    }
}

#[derive(Debug)]
struct BaselinePresence<T> {
    count: usize,
    first: T,
    mismatched: bool,
}

impl<T: PartialEq> BaselinePresence<T> {
    fn new(first: T) -> Self {
        Self {
            count: 0,
            first,
            mismatched: false,
        }
    }

    fn observe(&mut self, value: &T) {
        self.count += 1;
        self.mismatched |= *value != self.first;
    }
}

fn write_late_track_issue(report: &mut String, issue: &LateTrackIssue) {
    writeln!(
        report,
        "  issue={} track={} parent_tape={} parent_on_baselines={}/{} track_on_baselines={}/{}",
        issue.issue,
        issue.track,
        issue.parent_tape,
        issue.parent_tape_on_baseline,
        issue.baseline_count,
        issue.track_on_baseline,
        issue.baseline_count,
    )
    .expect("write report");
    writeln!(
        report,
        "    late_track={}",
        describe_track(&issue.metadata)
    )
    .expect("write report");
    writeln!(
        report,
        "    late_object_info={}",
        describe_object_info(issue.object_info.as_ref())
    )
    .expect("write report");
    writeln!(
        report,
        "    baseline_parent_tape={}",
        describe_tape_info(issue.parent_tape_info.as_ref())
    )
    .expect("write report");
    writeln!(
        report,
        "    baseline_object_info={}",
        describe_object_info(issue.baseline_object_info.as_ref())
    )
    .expect("write report");
}

fn write_missing_tape_samples(
    report: &mut String,
    baseline_tapes: &BTreeMap<Address, BaselinePresence<TapeInfo>>,
    late_tapes: &BTreeMap<Address, TapeInfo>,
    baseline_count: usize,
) {
    let samples = baseline_tapes
        .iter()
        .filter(|(address, presence)| {
            presence.count == baseline_count && !late_tapes.contains_key(address)
        })
        .take(REPORT_SAMPLE_LIMIT)
        .collect::<Vec<_>>();

    if samples.is_empty() {
        return;
    }

    writeln!(report, "\ntapes present on all baselines and missing on late:").expect("write report");
    for (address, presence) in samples {
        writeln!(
            report,
            "  tape={} {}{}",
            address,
            describe_tape_info(Some(&presence.first)),
            if presence.mismatched {
                " baseline_values_differ=true"
            } else {
                ""
            }
        )
        .expect("write report");
    }
}

fn write_missing_track_samples(
    report: &mut String,
    baseline_tracks: &BTreeMap<Address, BaselinePresence<TrackSnapshot>>,
    late_tracks: &BTreeMap<Address, TrackSnapshot>,
    baseline_count: usize,
) {
    let samples = baseline_tracks
        .iter()
        .filter(|(address, presence)| {
            presence.count == baseline_count && !late_tracks.contains_key(address)
        })
        .take(REPORT_SAMPLE_LIMIT)
        .collect::<Vec<_>>();

    if samples.is_empty() {
        return;
    }

    writeln!(report, "\ntracks present on all baselines and missing on late:")
        .expect("write report");
    for (address, presence) in samples {
        writeln!(
            report,
            "  track={} {} object_info={}{}",
            address,
            describe_track(&presence.first.metadata),
            describe_object_info(presence.first.object_info.as_ref()),
            if presence.mismatched {
                " baseline_values_differ=true"
            } else {
                ""
            }
        )
        .expect("write report");
    }
}

fn describe_tape_info(info: Option<&TapeInfo>) -> String {
    match info {
        Some(info) => {
            let namespace = tape_namespace(info.id)
                .map(|namespace| format!("{namespace:?}"))
                .unwrap_or_else(|| "Unknown".to_string());
            let system = if TapeFlags::is_system(info.flags) {
                " system=true"
            } else {
                " system=false"
            };
            format!(
                "id={} namespace={} flags={}{} end_epoch={} next_track={}",
                info.id, namespace, info.flags, system, info.end_epoch, info.next_track_number
            )
        }
        None => "missing".to_string(),
    }
}

fn describe_track(track: &CompressedTrack) -> String {
    format!(
        "tape={} track_number={} kind={:?}/{} state={:?}/{} size={} group={} value_hash={}",
        track.tape,
        track.track_number,
        track.kind_enum(),
        track.kind,
        track.state_enum(),
        track.state,
        track.size,
        track.group,
        track.value_hash
    )
}

fn describe_object_info(info: Option<&ObjectInfo>) -> String {
    match info {
        Some(ObjectInfo::Valid {
            registered_epoch,
            certified_epoch,
            slot,
            track_address,
        }) => format!(
            "Valid(track={} registered_epoch={} certified_epoch={:?} slot={})",
            track_address, registered_epoch, certified_epoch, slot
        ),
        Some(ObjectInfo::System {
            kind,
            registered_epoch,
            certified_epoch,
            slot,
            track_address,
        }) => format!(
            "System(kind={kind:?} track={} registered_epoch={} certified_epoch={:?} slot={})",
            track_address, registered_epoch, certified_epoch, slot
        ),
        Some(other) => format!("{other:?}"),
        None => "missing".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::spooler::GroupIndex;
    use tape_core::track::types::{TrackKind, TrackState};
    use tape_core::types::{EpochNumber, StorageUnits, TapeNumber, TrackNumber};
    use tape_crypto::Hash;

    fn tape_info() -> TapeInfo {
        TapeInfo {
            id: TapeNumber(1),
            flags: 0,
            end_epoch: EpochNumber(9),
            next_track_number: TrackNumber(1),
        }
    }

    fn track_snapshot(tape: Address) -> TrackSnapshot {
        TrackSnapshot {
            metadata: CompressedTrack {
                tape,
                key: Hash::new_unique(),
                track_number: TrackNumber(0),
                kind: TrackKind::Raw as u64,
                state: TrackState::Certified as u64,
                size: StorageUnits::from_bytes(64),
                group: GroupIndex::from(0),
                value_hash: Hash::new_unique(),
            },
            object_info: None,
        }
    }

    #[test]
    fn missing_baseline_items_make_diff_unclean() {
        let tape = Address::new_unique();
        let track = Address::new_unique();

        let baseline = ReplayStoreSnapshot {
            node_index: 0,
            tapes: BTreeMap::from([(tape, tape_info())]),
            tracks: BTreeMap::from([(track, track_snapshot(tape))]),
        };
        let late = ReplayStoreSnapshot {
            node_index: 1,
            tapes: BTreeMap::new(),
            tracks: BTreeMap::new(),
        };

        let diff = build_replay_store_diff(&[baseline], &late);

        assert!(diff.has_missing_baseline_items());
        assert!(!diff.is_clean());
        assert!(
            diff.to_string()
                .contains("missing on late but present on all baselines: tapes=1 tracks=1")
        );
    }
}
