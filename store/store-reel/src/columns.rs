//! The column set the reel is opened with to serve a tape store
//!
//! The reel refuses any family it was not opened with, so this has to carry
//! every one tape-store declares. The bulk families carry the shapes the node
//! runs in production. The rest are variable-width and unsharded, since nothing
//! measures their key layout and a wrong fixed width is a runtime refusal rather
//! than a compile error.
//!
//! Every column declares lz4. A codec is attempted at admission and not
//! promised, so a payload that does not shrink, a parity slice for one, is
//! stored verbatim with nothing to configure.

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
const fn open(id: u8, name: &'static str, width: u16, shard_bytes: u8, codec: Codec) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(id),
        name,
        key_width: KeyWidth::Fixed(width),
        shard_bytes,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec,
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
        codec: Codec::Lz4,
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
        codec: Codec::Lz4,
        map_shape: MapShape::Tree,
    }
}

/// Families a tape store addresses, with `track_data` declared as asked
///
/// Every identifier is written out rather than derived from a position, because
/// the id is stamped into every record header: a family leaving the set has to
/// leave its number behind rather than hand it to whichever family moved up.
/// Zero stays free.
const fn tape_columns(track_data_codec: Codec) -> [ColumnSpec; ALL_COLUMN_FAMILIES.len()] {
    [
    plain(1, "meta"),
    plain(2, "tape"),
    shaped(3, "track", ADDRESS_LEN, 1),
    plain(4, "track_lookup"),
    open(5, "track_data", ADDRESS_LEN, 1, track_data_codec),
    plain(6, "object_info"),
    plain(7, "object_metadata"),
    plain(8, "object_list"),
    plain(9, "sync_cursor"),
    plain(10, "gc"),
    plain(11, "spool_status"),
    shaped(12, "spool_pending_repair", SLICE_KEY_LEN, 2),
    shaped(13, "spool_pending_recovery", SLICE_KEY_LEN, 2),
    shaped(14, "slice", SLICE_KEY_LEN, 2),
    plain(17, "challenge_record"),
    plain(18, "challenge_round"),
    plain(19, "track_sample"),
    plain(20, "spool_sync_cursor"),
    plain(21, "event_log"),
    plain(22, "vote_sig"),
    shaped(23, "snapshot_artifact", SNAPSHOT_KEY_LEN, 0),
    plain(24, "credential"),
    plain(25, "policy_rule"),
    plain(26, "auth_state"),
    plain(27, "audit_log"),
    plain(28, "ledger"),
    plain(29, "ledger_reservation"),
    plain(30, "s3_multipart_upload"),
    plain(31, "s3_multipart_part"),
    plain(32, "s3_multipart_part_data"),
    plain(33, "s3_pending_write"),
    plain(34, "s3_pending_write_data"),
    ]
}

/// The set the node ships, `track_data` coded
const CODED_TRACK_DATA: [ColumnSpec; ALL_COLUMN_FAMILIES.len()] = tape_columns(Codec::Lz4);

/// The same set with `track_data` stored verbatim
const RAW_TRACK_DATA: [ColumnSpec; ALL_COLUMN_FAMILIES.len()] = tape_columns(Codec::None);

/// Every family a tape store addresses, as the reel needs them declared
pub const TAPE_COLUMNS: ColumnSet = &CODED_TRACK_DATA;

/// The same families with `track_data` uncoded, for a run weighing the codec
pub const RAW_TRACK_DATA_COLUMNS: ColumnSet = &RAW_TRACK_DATA;

#[cfg(test)]
mod tests {
    use super::*;

    // the declared set is tape-store's own family list, with distinct ids
    #[test]
    fn declared_once() {
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

    // the two sets differ in track_data's codec and in nothing else
    #[test]
    fn codec_only() {
        for (coded, raw) in TAPE_COLUMNS.iter().zip(RAW_TRACK_DATA_COLUMNS) {
            if coded.name == "track_data" {
                assert_eq!(coded.codec, Codec::Lz4);
                assert_eq!(raw.codec, Codec::None);
            } else {
                assert_eq!(coded.codec, raw.codec);
            }
            assert_eq!(coded.name, raw.name);
            assert_eq!(coded.id.as_u8(), raw.id.as_u8());
        }
    }
}
