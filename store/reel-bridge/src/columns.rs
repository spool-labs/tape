//! The column set the public reel is opened with to serve a tape store
//!
//! The reel serves the families it was opened with and refuses every other, so a
//! `TapeStore` on top of it needs every family tape-store declares. The bulk
//! families carry the shapes the node runs in production, taken from the internal
//! engine's own declaration; the rest are declared variable-width and unsharded,
//! since nothing measures their key layout and a wrong fixed width is a runtime
//! refusal rather than a compile error.

use reel::{Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape};
use tape_store::columns::ALL_COLUMN_FAMILIES;

/// Bytes a slice key occupies: the spool big endian, then the track address
const SLICE_KEY_LEN: u16 = 34;

/// Bytes a content address occupies
const ADDRESS_LEN: u16 = 32;

/// Bytes a snapshot artifact key occupies
const SNAPSHOT_KEY_LEN: u16 = 24;

/// A column whose keys are all one width, held in an open-addressed shard
///
/// Point reads first: the shard has no order, so a walk gathers and sorts. Only
/// for a column nothing walks.
const fn open(id: u8, name: &'static str, width: u16, shard_bytes: u8) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(id),
        name,
        key_width: KeyWidth::Fixed(width),
        shard_bytes,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Open,
    }
}

/// A column with nothing declared about its keys beyond that they are keys
const fn plain(id: u8, name: &'static str) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(id),
        name,
        key_width: KeyWidth::Variable,
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    }
}

/// A column whose keys are all one width, sharded by their leading bytes
const fn shaped(id: u8, name: &'static str, width: u16, shard_bytes: u8) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(id),
        name,
        key_width: KeyWidth::Fixed(width),
        shard_bytes,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    }
}

/// Every family a tape store addresses, as the reel needs them declared
///
/// Identifiers are positions in `ALL_COLUMN_FAMILIES` plus one, so zero stays
/// free and the mapping is stable as long as that list does not reorder. The
/// column-set test holds it to that list.
pub const TAPE_COLUMNS: ColumnSet = &[
    plain(1, ALL_COLUMN_FAMILIES[0]),   // meta
    plain(2, ALL_COLUMN_FAMILIES[1]),   // tape
    shaped(3, ALL_COLUMN_FAMILIES[2], ADDRESS_LEN, 1), // track
    plain(4, ALL_COLUMN_FAMILIES[3]),   // track_lookup
    open(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1), // track_data
    plain(6, ALL_COLUMN_FAMILIES[5]),   // object_info
    plain(7, ALL_COLUMN_FAMILIES[6]),   // object_metadata
    plain(8, ALL_COLUMN_FAMILIES[7]),   // object_list
    plain(9, ALL_COLUMN_FAMILIES[8]),   // sync_cursor
    plain(10, ALL_COLUMN_FAMILIES[9]),  // gc
    plain(11, ALL_COLUMN_FAMILIES[10]), // spool_status
    shaped(12, ALL_COLUMN_FAMILIES[11], SLICE_KEY_LEN, 2), // spool_pending_repair
    shaped(13, ALL_COLUMN_FAMILIES[12], SLICE_KEY_LEN, 2), // spool_pending_recovery
    shaped(14, ALL_COLUMN_FAMILIES[13], SLICE_KEY_LEN, 2), // slice
    shaped(15, ALL_COLUMN_FAMILIES[14], SLICE_KEY_LEN, 2), // slice_size
    shaped(16, ALL_COLUMN_FAMILIES[15], SLICE_KEY_LEN, 2), // slice_sidecar
    plain(17, ALL_COLUMN_FAMILIES[16]), // challenge_record
    plain(18, ALL_COLUMN_FAMILIES[17]), // challenge_round
    plain(19, ALL_COLUMN_FAMILIES[18]), // track_sample
    plain(20, ALL_COLUMN_FAMILIES[19]), // spool_sync_cursor
    plain(21, ALL_COLUMN_FAMILIES[20]), // event_log
    plain(22, ALL_COLUMN_FAMILIES[21]), // vote_sig
    shaped(23, ALL_COLUMN_FAMILIES[22], SNAPSHOT_KEY_LEN, 0), // snapshot_artifact
    plain(24, ALL_COLUMN_FAMILIES[23]), // credential
    plain(25, ALL_COLUMN_FAMILIES[24]), // policy_rule
    plain(26, ALL_COLUMN_FAMILIES[25]), // auth_state
    plain(27, ALL_COLUMN_FAMILIES[26]), // audit_log
    plain(28, ALL_COLUMN_FAMILIES[27]), // ledger
    plain(29, ALL_COLUMN_FAMILIES[28]), // ledger_reservation
    plain(30, ALL_COLUMN_FAMILIES[29]), // s3_multipart_upload
    plain(31, ALL_COLUMN_FAMILIES[30]), // s3_multipart_part
    plain(32, ALL_COLUMN_FAMILIES[31]), // s3_multipart_part_data
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_family_is_declared_once() {
        assert_eq!(TAPE_COLUMNS.len(), ALL_COLUMN_FAMILIES.len());
        for (spec, name) in TAPE_COLUMNS.iter().zip(ALL_COLUMN_FAMILIES) {
            assert_eq!(spec.name, *name);
        }
        let mut ids: Vec<u8> = TAPE_COLUMNS.iter().map(|spec| spec.id.as_u8()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), TAPE_COLUMNS.len());
        assert!(!ids.contains(&0));
    }
}
