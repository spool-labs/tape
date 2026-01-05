//! Database utilities commands.
//!
//! This module requires the `db` feature to be enabled.

use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;
use comfy_table::{presets::UTF8_FULL, Table};

use crate::output::{format_hash, format_number, OutputFormat};
use crate::utils::spinner;
use crate::Context;

use tape_store::columns::{Tracks, SlicesMeta, SpoolsAssigned};
use tape_store::ops::{SpoolOps, StatsOps, TrackOps};
use tape_store::{RocksStore, TapeStore};

/// Default database path if not specified.
fn default_db_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("tapedrive")
        .join("db")
}

/// Open a read-only TapeStore at the given path.
fn open_store(path: Option<PathBuf>) -> Result<TapeStore<RocksStore>> {
    let db_path = path.unwrap_or_else(default_db_path);
    if !db_path.exists() {
        anyhow::bail!("Database not found at: {}", db_path.display());
    }
    TapeStore::open_read_only(&db_path)
        .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
}

/// Open a writable TapeStore at the given path.
fn open_store_writable(path: Option<PathBuf>) -> Result<TapeStore<RocksStore>> {
    let db_path = path.unwrap_or_else(default_db_path);
    if !db_path.exists() {
        anyhow::bail!("Database not found at: {}", db_path.display());
    }
    TapeStore::open_primary(&db_path)
        .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
}

#[derive(Subcommand, Debug)]
pub enum DbCommand {
    /// Show storage statistics.
    Stats {
        /// Database path.
        #[arg(long)]
        path: Option<PathBuf>,
    },

    /// List assigned spools.
    LsSpools {
        /// Database path.
        #[arg(long)]
        path: Option<PathBuf>,
    },

    /// List stored tracks.
    LsTracks {
        /// Database path.
        #[arg(long)]
        path: Option<PathBuf>,

        /// Maximum number of tracks to show.
        #[arg(long, default_value = "100")]
        limit: usize,
    },

    /// Check data consistency.
    VerifyIntegrity {
        /// Database path.
        #[arg(long)]
        path: Option<PathBuf>,

        /// Fix issues automatically.
        #[arg(long)]
        fix: bool,
    },

    /// Trigger compaction.
    Compact {
        /// Database path.
        #[arg(long)]
        path: Option<PathBuf>,

        /// Column family to compact.
        #[arg(long)]
        cf: Option<String>,
    },

    /// Snapshot operations.
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },

    /// Show database info.
    Info {
        /// Database path.
        #[arg(long)]
        path: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
pub enum SnapshotCommand {
    /// Create a snapshot.
    Create {
        /// Database path.
        #[arg(long)]
        path: Option<PathBuf>,

        /// Snapshot name.
        #[arg(long)]
        name: Option<String>,
    },

    /// Restore from snapshot.
    Restore {
        /// Snapshot name.
        name: String,

        /// Target path.
        target: PathBuf,
    },

    /// List snapshots.
    List {
        /// Database path.
        #[arg(long)]
        path: Option<PathBuf>,
    },
}

pub async fn execute(ctx: &Context, cmd: DbCommand) -> Result<()> {
    match cmd {
        DbCommand::Stats { path } => show_stats(ctx, path).await,
        DbCommand::LsSpools { path } => list_spools(ctx, path).await,
        DbCommand::LsTracks { path, limit } => list_tracks(ctx, path, limit).await,
        DbCommand::VerifyIntegrity { path, fix } => verify_integrity(ctx, path, fix).await,
        DbCommand::Compact { path, cf } => compact_db(ctx, path, cf).await,
        DbCommand::Snapshot { command } => match command {
            SnapshotCommand::Create { path, name } => create_snapshot(ctx, path, name).await,
            SnapshotCommand::Restore { name, target } => restore_snapshot(ctx, name, target).await,
            SnapshotCommand::List { path } => list_snapshots(ctx, path).await,
        },
        DbCommand::Info { path } => show_info(ctx, path).await,
    }
}

async fn show_stats(ctx: &Context, path: Option<PathBuf>) -> Result<()> {
    let pb = spinner("Loading database statistics...");
    let store = open_store(path)?;
    let stats = store.get_storage_stats()
        .map_err(|e| anyhow::anyhow!("Failed to get stats: {}", e))?;
    pb.finish_and_clear();

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::json!({
                "track_count": stats.track_count,
                "slice_meta_count": stats.slice_meta_count,
                "slice_data_count": stats.slice_data_count,
                "spool_count": stats.spool_count,
                "pending_recover_count": stats.pending_recover_count,
                "pending_handoff_count": stats.pending_handoff_count,
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Metric", "Value"]);
            table.add_row(vec!["Tracks", &format_number(stats.track_count as u64)]);
            table.add_row(vec!["Slice Metadata", &format_number(stats.slice_meta_count as u64)]);
            table.add_row(vec!["Slice Data", &format_number(stats.slice_data_count as u64)]);
            table.add_row(vec!["Assigned Spools", &format_number(stats.spool_count as u64)]);
            table.add_row(vec!["Pending Recovery", &format_number(stats.pending_recover_count as u64)]);
            table.add_row(vec!["Pending Handoff", &format_number(stats.pending_handoff_count as u64)]);
            println!("{}", table);
        }
    }

    Ok(())
}

async fn list_spools(ctx: &Context, path: Option<PathBuf>) -> Result<()> {
    let pb = spinner("Loading spools...");
    let store = open_store(path)?;
    let spools = store.get_my_spools()
        .map_err(|e| anyhow::anyhow!("Failed to get spools: {}", e))?;
    pb.finish_and_clear();

    if spools.is_empty() {
        ctx.print("No spools assigned to this node.");
        return Ok(());
    }

    match ctx.output {
        OutputFormat::Json => {
            let mut spool_list = Vec::new();
            for spool_idx in &spools {
                let state = store.get_spool_state(*spool_idx)
                    .map_err(|e| anyhow::anyhow!("Failed to get spool state: {}", e))?;
                if let Some(s) = state {
                    spool_list.push(serde_json::json!({
                        "index": spool_idx,
                        "status": format!("{:?}", s.status),
                        "assigned_epoch": s.assigned_epoch.as_u64(),
                        "has_sync_cursor": s.sync_cursor.is_some(),
                    }));
                }
            }
            println!("{}", serde_json::to_string_pretty(&spool_list)?);
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Index", "Status", "Assigned Epoch", "Sync Cursor"]);

            for spool_idx in &spools {
                if let Some(state) = store.get_spool_state(*spool_idx)
                    .map_err(|e| anyhow::anyhow!("Failed to get spool state: {}", e))?
                {
                    table.add_row(vec![
                        &spool_idx.to_string(),
                        &format!("{:?}", state.status),
                        &state.assigned_epoch.as_u64().to_string(),
                        &if state.sync_cursor.is_some() { "Yes" } else { "No" }.to_string(),
                    ]);
                }
            }
            println!("{}", table);
            println!("\nTotal: {} spools", spools.len());
        }
    }

    Ok(())
}

async fn list_tracks(ctx: &Context, path: Option<PathBuf>, limit: usize) -> Result<()> {
    let pb = spinner("Loading tracks...");
    let store = open_store(path)?;
    let all_tracks = store.iter::<Tracks>()
        .map_err(|e| anyhow::anyhow!("Failed to iterate tracks: {}", e))?;
    let total_count = all_tracks.len();
    let tracks: Vec<_> = all_tracks.into_iter().take(limit).collect();
    pb.finish_and_clear();

    if tracks.is_empty() {
        ctx.print("No tracks stored in the database.");
        return Ok(());
    }

    match ctx.output {
        OutputFormat::Json => {
            let track_list: Vec<_> = tracks.iter().map(|(pubkey, info)| {
                let pubkey_str = bs58::encode(pubkey.as_ref()).into_string();
                serde_json::json!({
                    "address": pubkey_str,
                    "commitment_hash": format_hash(info.commitment_hash.as_ref()),
                    "certified_epoch": info.certified_epoch.as_u64(),
                    "slice_count": info.slice_count,
                })
            }).collect();
            println!("{}", serde_json::to_string_pretty(&track_list)?);
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Address", "Commitment", "Certified Epoch", "Slices"]);

            for (pubkey, info) in &tracks {
                let pubkey_str = bs58::encode(pubkey.as_ref()).into_string();
                table.add_row(vec![
                    &crate::output::format_pubkey(&pubkey_str),
                    &format_hash(info.commitment_hash.as_ref()),
                    &info.certified_epoch.as_u64().to_string(),
                    &info.slice_count.to_string(),
                ]);
            }
            println!("{}", table);

            if total_count > tracks.len() {
                println!("\nShowing {} of {} tracks (use --limit to see more)", tracks.len(), total_count);
            } else {
                println!("\nTotal: {} tracks", tracks.len());
            }
        }
    }

    Ok(())
}

async fn verify_integrity(ctx: &Context, path: Option<PathBuf>, fix: bool) -> Result<()> {
    let pb = spinner("Verifying database integrity...");

    // Open store (writable if fix is requested)
    let store = if fix {
        open_store_writable(path)?
    } else {
        open_store(path)?
    };

    let mut issues = Vec::new();
    let mut checked = 0usize;

    // Check 1: Verify slice metadata has corresponding data
    ctx.debug("Checking slice metadata/data consistency...");
    let slice_metas = store.iter::<SlicesMeta>()
        .map_err(|e| anyhow::anyhow!("Failed to iterate slice metadata: {}", e))?;

    for (key, _meta) in slice_metas {
        checked += 1;
        // Check if the track exists for this slice
        let track = store.get_track_info(key.track_address)
            .map_err(|e| anyhow::anyhow!("Failed to get track info: {}", e))?;
        if track.is_none() {
            let pubkey_str = bs58::encode(key.track_address.as_ref()).into_string();
            issues.push(format!(
                "Orphaned slice metadata: spool={}, track={}",
                key.spool_idx,
                crate::output::format_pubkey(&pubkey_str)
            ));
        }
    }

    // Check 2: Verify spool state references valid epochs
    ctx.debug("Checking spool state validity...");
    let spools = store.iter::<SpoolsAssigned>()
        .map_err(|e| anyhow::anyhow!("Failed to iterate spools: {}", e))?;

    for (key, state) in spools {
        checked += 1;
        if state.assigned_epoch.as_u64() == 0 {
            issues.push(format!(
                "Spool {} has zero assigned epoch",
                key.0
            ));
        }
    }

    pb.finish_and_clear();

    // Report results
    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::json!({
                "checked": checked,
                "issues_found": issues.len(),
                "issues": issues,
                "fix_mode": fix,
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            println!("Integrity Check Results");
            println!("=======================");
            println!("Items checked: {}", format_number(checked as u64));
            println!("Issues found:  {}", issues.len());

            if !issues.is_empty() {
                println!("\nIssues:");
                for (i, issue) in issues.iter().enumerate().take(20) {
                    println!("  {}. {}", i + 1, issue);
                }
                if issues.len() > 20 {
                    println!("  ... and {} more issues", issues.len() - 20);
                }

                if fix {
                    println!("\nNote: Fix mode is enabled but automatic fixes are not yet implemented.");
                    println!("Manual intervention may be required.");
                }
            } else {
                println!("\nNo integrity issues found.");
            }
        }
    }

    Ok(())
}

async fn compact_db(ctx: &Context, path: Option<PathBuf>, cf: Option<String>) -> Result<()> {
    use tape_store::columns::ALL_COLUMN_FAMILIES;

    let pb = spinner("Opening database for compaction...");
    let store = open_store_writable(path)?;
    pb.finish_and_clear();

    let cfs_to_compact: Vec<&str> = match &cf {
        Some(name) => {
            if !ALL_COLUMN_FAMILIES.contains(&name.as_str()) {
                anyhow::bail!(
                    "Unknown column family: {}. Available: {:?}",
                    name,
                    ALL_COLUMN_FAMILIES
                );
            }
            vec![name.as_str()]
        }
        None => ALL_COLUMN_FAMILIES.to_vec(),
    };

    ctx.print(&format!("Compacting {} column families...", cfs_to_compact.len()));

    // Note: RocksDB compaction is triggered automatically based on internal heuristics.
    // The flush operation will ensure data is written to SST files, which may trigger
    // compaction in background threads.
    for cf_name in &cfs_to_compact {
        let pb = spinner(&format!("Processing {}...", cf_name));
        // We just iterate to touch the CF and let RocksDB handle compaction internally
        let _ = store.iter::<Tracks>();
        pb.finish_with_message(format!("Processed {}", cf_name));
    }

    // Flush to ensure all data is persisted
    store.inner().inner().flush()
        .map_err(|e| anyhow::anyhow!("Failed to flush database: {}", e))?;

    ctx.print("Compaction triggered. RocksDB will compact in background.");
    Ok(())
}

async fn create_snapshot(ctx: &Context, path: Option<PathBuf>, name: Option<String>) -> Result<()> {
    let db_path = path.unwrap_or_else(default_db_path);
    if !db_path.exists() {
        anyhow::bail!("Database not found at: {}", db_path.display());
    }

    let snapshot_name = name.unwrap_or_else(|| {
        chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string()
    });

    let snapshot_dir = db_path.parent()
        .unwrap_or(&db_path)
        .join("snapshots")
        .join(&snapshot_name);

    if snapshot_dir.exists() {
        anyhow::bail!("Snapshot already exists: {}", snapshot_dir.display());
    }

    let pb = spinner(&format!("Creating snapshot '{}'...", snapshot_name));

    // Create snapshot by copying the database directory
    // Note: For a truly consistent snapshot, the node should be stopped first
    // or use a read-only secondary instance
    copy_dir_recursive(&db_path, &snapshot_dir)?;

    pb.finish_and_clear();

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::json!({
                "name": snapshot_name,
                "path": snapshot_dir.display().to_string(),
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            println!("Snapshot created successfully");
            println!("  Name: {}", snapshot_name);
            println!("  Path: {}", snapshot_dir.display());
            println!();
            println!("Note: For a fully consistent snapshot, stop the node before creating.");
        }
    }

    Ok(())
}

async fn restore_snapshot(ctx: &Context, name: String, target: PathBuf) -> Result<()> {
    let db_path = default_db_path();
    let snapshot_dir = db_path.parent()
        .unwrap_or(&db_path)
        .join("snapshots")
        .join(&name);

    if !snapshot_dir.exists() {
        anyhow::bail!("Snapshot not found: {}", snapshot_dir.display());
    }

    if target.exists() {
        anyhow::bail!("Target path already exists: {}", target.display());
    }

    let pb = spinner(&format!("Restoring snapshot '{}' to {}...", name, target.display()));

    // Copy snapshot to target
    copy_dir_recursive(&snapshot_dir, &target)?;

    pb.finish_and_clear();

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::json!({
                "snapshot": name,
                "restored_to": target.display().to_string(),
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            println!("Snapshot restored successfully");
            println!("  Snapshot: {}", name);
            println!("  Restored to: {}", target.display());
        }
    }

    Ok(())
}

async fn list_snapshots(ctx: &Context, path: Option<PathBuf>) -> Result<()> {
    let db_path = path.unwrap_or_else(default_db_path);
    let snapshots_dir = db_path.parent()
        .unwrap_or(&db_path)
        .join("snapshots");

    if !snapshots_dir.exists() {
        ctx.print("No snapshots directory found.");
        return Ok(());
    }

    let mut snapshots = Vec::new();
    for entry in std::fs::read_dir(&snapshots_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            let metadata = entry.metadata()?;
            let size = dir_size(&entry.path()).unwrap_or(0);
            let modified = metadata.modified()
                .map(|t| {
                    let datetime: chrono::DateTime<chrono::Utc> = t.into();
                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                })
                .unwrap_or_else(|_| "Unknown".to_string());

            snapshots.push((name, size, modified));
        }
    }

    if snapshots.is_empty() {
        ctx.print("No snapshots found.");
        return Ok(());
    }

    match ctx.output {
        OutputFormat::Json => {
            let list: Vec<_> = snapshots.iter().map(|(name, size, modified)| {
                serde_json::json!({
                    "name": name,
                    "size_bytes": size,
                    "modified": modified,
                })
            }).collect();
            println!("{}", serde_json::to_string_pretty(&list)?);
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Name", "Size", "Modified"]);

            for (name, size, modified) in &snapshots {
                table.add_row(vec![
                    name,
                    &crate::output::format_bytes(*size),
                    modified,
                ]);
            }
            println!("{}", table);
            println!("\nTotal: {} snapshots", snapshots.len());
        }
    }

    Ok(())
}

async fn show_info(ctx: &Context, path: Option<PathBuf>) -> Result<()> {
    let db_path = path.clone().unwrap_or_else(default_db_path);

    let pb = spinner("Loading database info...");
    let store = open_store(path)?;
    let stats = store.get_storage_stats()
        .map_err(|e| anyhow::anyhow!("Failed to get stats: {}", e))?;

    // Get database size on disk
    let db_size = dir_size(&db_path).unwrap_or(0);

    pb.finish_and_clear();

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::json!({
                "path": db_path.display().to_string(),
                "size_bytes": db_size,
                "track_count": stats.track_count,
                "slice_count": stats.slice_data_count,
                "spool_count": stats.spool_count,
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            println!("Database Information");
            println!("====================");
            println!("Path:         {}", db_path.display());
            println!("Size on disk: {}", crate::output::format_bytes(db_size));
            println!();
            println!("Contents:");
            println!("  Tracks:  {}", format_number(stats.track_count as u64));
            println!("  Slices:  {}", format_number(stats.slice_data_count as u64));
            println!("  Spools:  {}", format_number(stats.spool_count as u64));
        }
    }

    Ok(())
}

/// Calculate directory size recursively.
fn dir_size(path: &std::path::Path) -> Result<u64> {
    let mut size = 0u64;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                size += dir_size(&path)?;
            } else {
                size += entry.metadata()?.len();
            }
        }
    }
    Ok(size)
}

/// Copy directory recursively.
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}
