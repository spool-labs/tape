//! The machine this process runs on: its cores, its memory and the store's disk

use std::path::Path;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use fs2::statvfs;
use sysinfo::{
    get_current_pid, CpuRefreshKind, MemoryRefreshKind, Pid, ProcessRefreshKind, ProcessesToUpdate,
    RefreshKind, System,
};
use tape_observe_api::HostInfo;

/// The shortest gap between two samples. A CPU figure is the busy share since
/// the sample before, and over a shorter gap it reads as noise.
const MIN_PERIOD: Duration = Duration::from_secs(1);

static SAMPLER: OnceLock<Mutex<Sampler>> = OnceLock::new();

/// The machine as last sampled, sampling again when that is older than
/// [`MIN_PERIOD`]. The disk figures are for the filesystem holding `store_root`.
///
/// The first call only sets the baseline CPU is measured from, and returns
/// zeroes.
pub fn sample(store_root: &Path) -> HostInfo {
    let sampler = SAMPLER.get_or_init(|| Mutex::new(Sampler::new()));
    let mut sampler = sampler.lock().unwrap_or_else(PoisonError::into_inner);
    if sampler.sampled_at.elapsed() >= MIN_PERIOD {
        sampler.refresh(store_root);
    }
    sampler.last.clone()
}

struct Sampler {
    system: System,
    pid: Option<Pid>,
    sampled_at: Instant,
    last: HostInfo,
}

impl Sampler {
    fn new() -> Self {
        let cpu = CpuRefreshKind::nothing().with_cpu_usage();
        let mut system = System::new_with_specifics(RefreshKind::nothing().with_cpu(cpu));
        let pid = get_current_pid().ok();
        if let Some(pid) = pid {
            refresh_process(&mut system, pid);
        }
        Self { system, pid, sampled_at: Instant::now(), last: HostInfo::default() }
    }

    fn refresh(&mut self, store_root: &Path) {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
        let cores = self.system.cpus().len();
        let process_cpu = match self.pid {
            Some(pid) => {
                refresh_process(&mut self.system, pid);
                self.system.process(pid).map(|process| process.cpu_usage()).unwrap_or(0.0)
            }
            None => 0.0,
        };
        let disk = statvfs(store_root);
        let disk_total_bytes = disk
            .as_ref()
            .map(|stats| usable_total(stats.total_space(), stats.free_space(), stats.available_space()))
            .unwrap_or(0);
        let disk_available_bytes = disk
            .as_ref()
            .map(|stats| stats.available_space())
            .unwrap_or(0);
        self.last = HostInfo {
            cores: cores as u64,
            cpu_pct: self.system.global_cpu_usage(),
            // A process's usage counts every core as 100.
            process_cpu_pct: if cores == 0 { 0.0 } else { process_cpu / cores as f32 },
            memory_total_bytes: self.system.total_memory(),
            memory_available_bytes: self.system.available_memory(),
            disk_total_bytes,
            disk_available_bytes,
        };
        self.sampled_at = Instant::now();
    }
}

/// The filesystem's size less the blocks only root can write
fn usable_total(total: u64, free: u64, available: u64) -> u64 {
    total.saturating_sub(free).saturating_add(available)
}

fn refresh_process(system: &mut System, pid: Pid) {
    let cpu = ProcessRefreshKind::nothing().with_cpu();
    system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, cpu);
}

#[cfg(test)]
mod tests {
    use super::*;

    // a sample carries the machine's cores, memory and the disk under a path
    #[test]
    fn samples_the_machine() {
        let dir = tempfile::tempdir().unwrap();
        let mut sampler = Sampler::new();

        sampler.refresh(dir.path());
        let host = sampler.last;

        assert!(host.cores > 0);
        assert!((0.0..=100.0).contains(&host.cpu_pct));
        assert!(host.memory_total_bytes > 0);
        assert!(host.memory_available_bytes <= host.memory_total_bytes);
        assert!(host.disk_total_bytes > 0);
        assert!(host.disk_available_bytes <= host.disk_total_bytes);
    }

    // blocks kept for root count as neither used nor available
    #[test]
    fn reserved_blocks() {
        let (total, free, available) = (1_000, 600, 550);

        let size = usable_total(total, free, available);

        assert_eq!(size - available, total - free);
    }
}
