use anyhow::Error;

fn msg(error: &Error) -> String {
    error.to_string().to_ascii_lowercase()
}

/// True when join operation is already completed/idempotent.
pub fn join_done(error: &Error) -> bool {
    let m = msg(error);
    m.contains("already in committee")
        || m.contains("already joined")
        || m.contains("already processed")
}

/// True when pool advancement was already processed for current epoch.
pub fn adv_done(error: &Error) -> bool {
    let m = msg(error);
    m.contains("already advanced")
        || m.contains("already processed")
}

/// True when sync operation is already done/idempotent.
pub fn sync_done(error: &Error) -> bool {
    let m = msg(error);
    m.contains("already synced")
        || m.contains("already processed")
}
