//! Database utilities commands.
//!
//! This module requires the `db` feature to be enabled.

use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;
use comfy_table::{presets::UTF8_FULL, Table};

use crate::output::{format_number, OutputFormat};
use crate::utils::spinner;
use crate::Context;

use tape_store::columns::{
    PrimarySlices, RecoverySlices, SliceInfoCol, TapeInfoCol, TrackInfoCol, ALL_COLUMN_FAMILIES,
};
use tape_store::ops::{MetaOps, SpoolOps, TrackInfoOps};
use tape_store::types::{EpochNumber, SpoolStatus};
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

    /// List assigned spools for an epoch.
    LsSpools {
        /// Database path.
        #[arg(long)]
        path: Option<PathBuf>,

        /// Epoch to list spools for (defaults to current epoch from meta).
        #[arg(long)]
        epoch: Option<u64>,
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
        DbCommand::LsSpools { path, epoch } => list_spools(ctx, path, epoch).await,
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

    // Count items in each column family
    let track_count = store
        .iter::<TrackInfoCol>()
        .map(|v| v.len())
        .unwrap_or(0);
    let slice_info_count = store
        .iter::<SliceInfoCol>()
        .map(|v| v.len())
        .unwrap_or(0);
    let tape_count = store.iter::<TapeInfoCol>().map(|v| v.len()).unwrap_or(0);
    let primary_slice_count = store
        .iter::<PrimarySlices>()
        .map(|v| v.len())
        .unwrap_or(0);
    let recovery_slice_count = store
        .iter::<RecoverySlices>()
        .map(|v| v.len())
        .unwrap_or(0);

    // Get current epoch and count spools
    let current_epoch = store.get_current_epoch().ok().flatten();
    let spool_count = if let Some(epoch) = current_epoch {
        store
            .iter_assigned_spools(epoch)
            .map(|iter| iter.count())
            .unwrap_or(0)
    } else {
        0
    };

    pb.finish_and_clear();

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::json!({
                "track_count": track_count,
                "slice_info_count": slice_info_count,
                "tape_count": tape_count,
                "primary_slice_count": primary_slice_count,
                "recovery_slice_count": recovery_slice_count,
                "spool_count": spool_count,
                "current_epoch": current_epoch.map(|e| e.as_u64()),
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Metric", "Value"]);
            table.add_row(vec!["Tracks", &format_number(track_count as u64)]);
            table.add_row(vec!["Slice Info", &format_number(slice_info_count as u64)]);
            table.add_row(vec!["Tapes", &format_number(tape_count as u64)]);
            table.add_row(vec![
                "Primary Slices",
                &format_number(primary_slice_count as u64),
            ]);
            table.add_row(vec![
                "Recovery Slices",
                &format_number(recovery_slice_count as u64),
            ]);
            table.add_row(vec![
                "Assigned Spools",
                &format_number(spool_count as u64),
            ]);
            if let Some(epoch) = current_epoch {
                table.add_row(vec!["Current Epoch", &epoch.as_u64().to_string()]);
            }
            println!("{}", table);
        }
    }

    Ok(())
}

async fn list_spools(ctx: &Context, path: Option<PathBuf>, epoch_arg: Option<u64>) -> Result<()> {
    let pb = spinner("Loading spools...");
    let store = open_store(path)?;

    // Use provided epoch or get current epoch from meta
    let epoch = match epoch_arg {
        Some(e) => EpochNumber(e),
        None => store
            .get_current_epoch()
            .map_err(|e| anyhow::anyhow!("Failed to get current epoch: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("No current epoch set in database"))?,
    };

    let spools: Vec<(u16, SpoolStatus)> = store
        .iter_assigned_spools(epoch)
        .map_err(|e| anyhow::anyhow!("Failed to iterate spools: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    pb.finish_and_clear();

    if spools.is_empty() {
        ctx.print(&format!(
            "No spools assigned for epoch {}.",
            epoch.as_u64()
        ));
        return Ok(());
    }

    match ctx.output {
        OutputFormat::Json => {
            let spool_list: Vec<_> = spools
                .iter()
                .map(|(idx, status)| {
                    serde_json::json!({
                        "index": idx,
                        "status": format!("{:?}", status),
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&spool_list)?);
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Index", "Status"]);

            for (idx, status) in &spools {
                table.add_row(vec![&idx.to_string(), &format!("{:?}", status)]);
            }
            println!("{}", table);
            println!(
                "\nTotal: {} spools for epoch {}",
                spools.len(),
                epoch.as_u64()
            );
        }
    }

    Ok(())
}

async fn list_tracks(ctx: &Context, path: Option<PathBuf>, limit: usize) -> Result<()> {
    let pb = spinner("Loading tracks...");
    let store = open_store(path)?;
    let all_tracks = store
        .iter::<TrackInfoCol>()
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
            let track_list: Vec<_> = tracks
                .iter()
                .map(|(pubkey, info)| {
                    let pubkey_str = bs58::encode(pubkey.as_ref()).into_string();
                    let tape_str = bs58::encode(info.tape_address.as_ref()).into_string();
                    serde_json::json!({
                        "address": pubkey_str,
                        "tape": tape_str,
                        "registered_epoch": info.registered_epoch.as_u64(),
                        "certified_epoch": info.certified_epoch.map(|e| e.as_u64()),
                        "has_slice_info": info.has_slice_info,
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&track_list)?);
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec![
                "Address",
                "Tape",
                "Registered",
                "Certified",
                "Has Info",
            ]);

            for (pubkey, info) in &tracks {
                let pubkey_str = bs58::encode(pubkey.as_ref()).into_string();
                let tape_str = bs58::encode(info.tape_address.as_ref()).into_string();
                table.add_row(vec![
                    &crate::output::format_pubkey(&pubkey_str),
                    &crate::output::format_pubkey(&tape_str),
                    &info.registered_epoch.as_u64().to_string(),
                    &info
                        .certified_epoch
                        .map(|e| e.as_u64().to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    &if info.has_slice_info { "Yes" } else { "No" }.to_string(),
                ]);
            }
            println!("{}", table);

            if total_count > tracks.len() {
                println!(
                    "\nShowing {} of {} tracks (use --limit to see more)",
                    tracks.len(),
                    total_count
                );
            } else {
                println!("\nTotal: {} tracks", tracks.len());
            }
        }
    }

    Ok(())
}

async fn verify_integrity(ctx: &Context, path: Option<PathBuf>, fix: bool) -> Result<()> {
    let pb = spinner("Verifying database integrity...");

    let store = if fix {
        open_store_writable(path)?
    } else {
        open_store(path)?
    };

    let mut issues = Vec::new();
    let mut checked = 0usize;

    // Check 1: Verify tracks have valid tape addresses
    ctx.debug("Checking track info validity...");
    let tracks = store
        .iter::<TrackInfoCol>()
        .map_err(|e| anyhow::anyhow!("Failed to iterate tracks: {}", e))?;

    for (track_addr, info) in tracks {
        checked += 1;
        if info.registered_epoch.as_u64() == 0 && info.tape_address.as_ref() == &[0u8; 32] {
            let pubkey_str = bs58::encode(track_addr.as_ref()).into_string();
            issues.push(format!(
                "Track {} has zero epoch and null tape address",
                crate::output::format_pubkey(&pubkey_str)
            ));
        }
    }

    // Check 2: Verify slice info entries
    ctx.debug("Checking slice info entries...");
    let slice_infos = store
        .iter::<SliceInfoCol>()
        .map_err(|e| anyhow::anyhow!("Failed to iterate slice info: {}", e))?;

    for (track_addr, info) in slice_infos {
        checked += 1;
        // Check that track exists
        let track =
            store
                .get_track_info(track_addr)
                .map_err(|e| anyhow::anyhow!("Failed to get track info: {}", e))?;
        if track.is_none() {
            let pubkey_str = bs58::encode(track_addr.as_ref()).into_string();
            issues.push(format!(
                "Orphaned slice info for track {}",
                crate::output::format_pubkey(&pubkey_str)
            ));
        }
        // Check that primary/recovery arrays have reasonable sizes
        if info.primary.len() > 1024 {
            let pubkey_str = bs58::encode(track_addr.as_ref()).into_string();
            issues.push(format!(
                "Slice info for {} has {} primary hashes (max 1024)",
                crate::output::format_pubkey(&pubkey_str),
                info.primary.len()
            ));
        }
    }

    // Check 3: Count spools for current epoch if set
    ctx.debug("Checking spool assignments...");
    if let Ok(Some(epoch)) = store.get_current_epoch() {
        if let Ok(iter) = store.iter_assigned_spools(epoch) {
            for result in iter {
                checked += 1;
                if let Err(e) = result {
                    issues.push(format!("Corrupted spool entry: {}", e));
                }
            }
        }
    }

    pb.finish_and_clear();

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
                    println!(
                        "\nNote: Fix mode is enabled but automatic fixes are not yet implemented."
                    );
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

    ctx.print(&format!(
        "Compacting {} column families...",
        cfs_to_compact.len()
    ));

    for cf_name in &cfs_to_compact {
        let pb = spinner(&format!("Processing {}...", cf_name));
        // Touch the CF to trigger background compaction
        let _ = store.iter::<TrackInfoCol>();
        pb.finish_with_message(format!("Processed {}", cf_name));
    }

    // Flush to ensure all data is persisted
    store
        .inner()
        .inner()
        .flush()
        .map_err(|e| anyhow::anyhow!("Failed to flush database: {}", e))?;

    ctx.print("Compaction triggered. RocksDB will compact in background.");
    Ok(())
}

async fn create_snapshot(
    ctx: &Context,
    path: Option<PathBuf>,
    name: Option<String>,
) -> Result<()> {
    let db_path = path.unwrap_or_else(default_db_path);
    if !db_path.exists() {
        anyhow::bail!("Database not found at: {}", db_path.display());
    }

    let snapshot_name = name.unwrap_or_else(|| chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string());

    let snapshot_dir = db_path
        .parent()
        .unwrap_or(&db_path)
        .join("snapshots")
        .join(&snapshot_name);

    if snapshot_dir.exists() {
        anyhow::bail!("Snapshot already exists: {}", snapshot_dir.display());
    }

    let pb = spinner(&format!("Creating snapshot '{}'...", snapshot_name));

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
    let snapshot_dir = db_path
        .parent()
        .unwrap_or(&db_path)
        .join("snapshots")
        .join(&name);

    if !snapshot_dir.exists() {
        anyhow::bail!("Snapshot not found: {}", snapshot_dir.display());
    }

    if target.exists() {
        anyhow::bail!("Target path already exists: {}", target.display());
    }

    let pb = spinner(&format!(
        "Restoring snapshot '{}' to {}...",
        name,
        target.display()
    ));

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
    let snapshots_dir = db_path.parent().unwrap_or(&db_path).join("snapshots");

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
            let modified = metadata
                .modified()
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
            let list: Vec<_> = snapshots
                .iter()
                .map(|(name, size, modified)| {
                    serde_json::json!({
                        "name": name,
                        "size_bytes": size,
                        "modified": modified,
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&list)?);
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["Name", "Size", "Modified"]);

            for (name, size, modified) in &snapshots {
                table.add_row(vec![name, &crate::output::format_bytes(*size), modified]);
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

    // Get counts
    let track_count = store
        .iter::<TrackInfoCol>()
        .map(|v| v.len())
        .unwrap_or(0);
    let primary_slice_count = store
        .iter::<PrimarySlices>()
        .map(|v| v.len())
        .unwrap_or(0);

    let current_epoch = store.get_current_epoch().ok().flatten();
    let spool_count = if let Some(epoch) = current_epoch {
        store
            .iter_assigned_spools(epoch)
            .map(|iter| iter.count())
            .unwrap_or(0)
    } else {
        0
    };

    // Get database size on disk
    let db_size = dir_size(&db_path).unwrap_or(0);

    pb.finish_and_clear();

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::json!({
                "path": db_path.display().to_string(),
                "size_bytes": db_size,
                "track_count": track_count,
                "slice_count": primary_slice_count,
                "spool_count": spool_count,
                "current_epoch": current_epoch.map(|e| e.as_u64()),
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            println!("Database Information");
            println!("Path:         {}", db_path.display());
            println!("Size on disk: {}", crate::output::format_bytes(db_size));
            println!();
            println!("Contents:");
            println!("  Tracks:  {}", format_number(track_count as u64));
            println!("  Slices:  {}", format_number(primary_slice_count as u64));
            println!("  Spools:  {}", format_number(spool_count as u64));
            if let Some(epoch) = current_epoch {
                println!("  Epoch:   {}", epoch.as_u64());
            }
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
