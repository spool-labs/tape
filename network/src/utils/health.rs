use log::debug;
use crate::store::TapeStore;

pub fn drift_status(store: &TapeStore, latest_slot: u64, last_processed_slot: u64) {
    // TODO: this function is not working right.

    let drift = latest_slot.saturating_sub(last_processed_slot);

    // Persist updated health (last_processed_slot + drift)
    if let Err(e) = store.update_health(last_processed_slot, drift) {
        eprintln!("ERROR: failed to write health metadata: {e:?}");
    }

    let health_status = if drift < 50 {
        "Healthy"
    } else if drift < 200 {
        "Slightly behind"
    } else {
        "Falling behind"
    };

    debug!(
        "Drift {drift} slots behind tip ({latest_slot}), status: {health_status}"
    );
}

