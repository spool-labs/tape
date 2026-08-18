//! The engines a tape-shaped bench runs its workload against
//!
//! A bench body is written once over `BenchArm` and instantiated per engine, so
//! the rocks row and the reel row are the same code over the same `TapeStore`
//! and differ only in what is underneath it.

use std::path::Path;

use store::Store;
use store_rocks::SplitStore;
use tape_store::TapeStore;

use crate::{bench_config, open_bench_split, MetaBulkStore, ReelBridge};

/// Segment size a reel arm opens with, in MiB, unless the environment names another
const DEFAULT_SEGMENT_MIB: u64 = 256;

/// Environment variable naming the reel arm's segment size in MiB
pub const SEGMENT_MIB_VAR: &str = "TAPE_BENCH_REEL_SEGMENT_MIB";

/// Environment variable dividing every sweep's dataset down from campaign size
pub const SCALE_VAR: &str = "TAPE_BENCH_SCALE";

/// A campaign figure cut down by whatever the environment asked for
///
/// One at campaign size, which is the default; larger on a machine that only has
/// to prove the plumbing runs. Never returns zero, so a scaled-down sweep still
/// writes something.
pub fn scaled(figure: usize) -> usize {
    let divisor = std::env::var(SCALE_VAR)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1);
    (figure / divisor).max(1)
}

/// The reel config every reel-backed arm opens with
fn reel_config() -> reel::ReelConfig {
    let segment_mib = std::env::var(SEGMENT_MIB_VAR)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEGMENT_MIB);
    bench_config(segment_mib * 1024 * 1024)
}

/// One storage engine a bench can be run against
pub trait BenchArm: Store + Sized + 'static {
    /// What a reported row calls this engine
    const NAME: &'static str;

    /// Open a tape store of this engine under the directory
    fn open_bench(root: &Path) -> TapeStore<Self>;

    /// Settle everything written so far, so a read phase measures itself
    ///
    /// A flush for rocks, which would otherwise read its memtables back; the
    /// same call for the reel, which drives its buffered appends to the
    /// filesystem.
    fn settle(store: &TapeStore<Self>);
}

impl BenchArm for SplitStore {
    const NAME: &'static str = "rocks";

    fn open_bench(root: &Path) -> TapeStore<Self> {
        open_bench_split(root).expect("open rocks split store")
    }

    fn settle(store: &TapeStore<Self>) {
        store.inner().inner().flush().expect("flush rocks");
    }
}

impl BenchArm for ReelBridge {
    const NAME: &'static str = "reel";

    fn open_bench(root: &Path) -> TapeStore<Self> {
        TapeStore::new(ReelBridge::open(root, reel_config()).expect("open reel"))
    }

    fn settle(store: &TapeStore<Self>) {
        store.inner().inner().flush().expect("flush reel");
    }
}

impl BenchArm for MetaBulkStore {
    const NAME: &'static str = "split";

    fn open_bench(root: &Path) -> TapeStore<Self> {
        TapeStore::new(MetaBulkStore::open(root, reel_config()).expect("open rocks meta plus reel"))
    }

    fn settle(store: &TapeStore<Self>) {
        store.inner().inner().flush().expect("flush rocks meta plus reel");
    }
}
