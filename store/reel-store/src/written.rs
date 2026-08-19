//! What the process wrote, as the kernel counted it
//!
//! Two counters, because they answer different questions. `wchar` is what the
//! process passed to write syscalls, so it is the engine's own decision about how
//! much to put down. `write_bytes` is what the kernel attributed to the process on
//! its way to storage, accounted a page at a time, so a record smaller than a page
//! that lands on its own page still costs a page.
//!
//! `/proc/self/io` is process-wide, so an arm that shares a process with another
//! reports the sum of both: every row has to be run in its own process.

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
