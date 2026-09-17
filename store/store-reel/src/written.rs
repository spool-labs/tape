//! What the process wrote, as the kernel counted it
//!
//! `wchar` is what the process handed to write syscalls, the engine's own
//! decision about how much to put down. `write_bytes` is what the kernel
//! attributed on the way to storage, accounted a page at a time.
//!
//! The counters are process-wide, so every arm has to run in its own process.

/// The write counters `/proc/self/io` keeps for the whole process
pub struct Written {
    /// Bytes handed to write syscalls
    pub wchar: u64,

    /// Bytes the kernel attributed to this process on their way to storage
    pub write_bytes: u64,

    /// Write syscalls made, which says how the engine split those bytes up
    pub syscw: u64,
}

impl Written {
    pub fn read() -> Written {
        let text = std::fs::read_to_string("/proc/self/io").expect("read /proc/self/io");
        let field = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name)?.trim().parse::<u64>().ok())
                .unwrap_or_else(|| panic!("no {name} in /proc/self/io"))
        };
        Written {
            wchar: field("wchar:"),
            write_bytes: field("write_bytes:"),
            syscw: field("syscw:"),
        }
    }

    pub fn since(&self, earlier: &Written) -> Written {
        Written {
            wchar: self.wchar.saturating_sub(earlier.wchar),
            write_bytes: self.write_bytes.saturating_sub(earlier.write_bytes),
            syscw: self.syscw.saturating_sub(earlier.syscw),
        }
    }
}
