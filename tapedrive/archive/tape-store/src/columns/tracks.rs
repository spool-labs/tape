//! Track column families

use crate::types::{Hash, Pubkey, TapeKey, Track, TrackKey, TrackNumber};
use std::mem::MaybeUninit;
use store::Column;
use wincode::{io::{Reader, Writer}, ReadResult, SchemaRead, SchemaWrite, WriteResult};

/// Tracks indexed by ID
pub struct TracksById;

impl Column for TracksById {
    const CF_NAME: &'static str = "tracks/by_id";
    type Key = TrackKey;
    type Value = Track;
}

/// Tracks indexed by on-chain address
pub struct TracksByAddress;

impl Column for TracksByAddress {
    const CF_NAME: &'static str = "tracks/by_address";
    type Key = Pubkey;
    type Value = TrackNumber;
}

/// Tracks indexed by tape (for listing all tracks on a tape)
/// Key: (TapeKey, TrackKey) - composite key
/// Value: unit (presence indicates track belongs to tape)
pub struct TracksByTape;

impl Column for TracksByTape {
    const CF_NAME: &'static str = "tracks/by_tape";
    type Key = TapeTrackKey;
    type Value = ();
}

/// Tracks indexed by blob key (content hash)
pub struct TracksByBlobKey;

impl Column for TracksByBlobKey {
    const CF_NAME: &'static str = "tracks/by_blob_key";
    type Key = Hash;
    type Value = TrackNumber;
}

/// Composite key for tracks by tape index
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TapeTrackKey {
    pub tape_id: TapeKey,
    pub track_id: TrackKey,
}

impl TapeTrackKey {
    pub fn new(tape_id: TapeKey, track_id: TrackKey) -> Self {
        Self { tape_id, track_id }
    }
}

impl SchemaWrite for TapeTrackKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(16) // 8 bytes tape_id + 8 bytes track_id
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        TapeKey::write(writer, &src.tape_id)?;
        TrackKey::write(writer, &src.track_id)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for TapeTrackKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<TapeTrackKey>) -> ReadResult<()> {
        let mut tape_id = MaybeUninit::uninit();
        let mut track_id = MaybeUninit::uninit();
        TapeKey::read(reader, &mut tape_id)?;
        TrackKey::read(reader, &mut track_id)?;
        dst.write(TapeTrackKey {
            tape_id: unsafe { tape_id.assume_init() },
            track_id: unsafe { track_id.assume_init() },
        });
        Ok(())
    }
}
