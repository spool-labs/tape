//! Event log column family for epoch snapshot event storage

use crate::types::keys::EventLogKey;
use store::Column;

/// Event log indexed by compound key (epoch, slot, seq)
///
/// Key: EventLogKey (epoch BE 8B || slot BE 8B || seq BE 4B = 20 bytes)
/// Value: Vec<u8> (wincode-serialized ReplayableEvent)
pub struct EventLogCol;

impl Column for EventLogCol {
    const CF_NAME: &'static str = "event_log";
    type Key = EventLogKey;
    type Value = Vec<u8>;
}
