use rlimit::{Resource, getrlimit, setrlimit};

const MIN_FDS: u64 = 65536;

pub fn check_fd_limit() {
    let (soft, hard) = getrlimit(Resource::NOFILE).unwrap_or((0, 0));
    if soft < MIN_FDS {
        eprintln!("warning: open file limit is {soft} (hard: {hard}), recommend at least {MIN_FDS}");
        eprintln!("  run: ulimit -n {MIN_FDS}");

        if hard >= MIN_FDS {
            setrlimit(Resource::NOFILE, MIN_FDS, hard)
                .expect("raise NOFILE soft limit");

            eprintln!("  auto-raised to {MIN_FDS}");
        }
    }
}
