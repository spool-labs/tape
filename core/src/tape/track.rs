use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum TrackKind {
    Unknown = 0,
    Blob,   // An immutable blob of const data
    Stream, // A mutable stream of data
}

impl TrackKind {
    #[inline]
    pub fn pack(self) -> [u8; 8] {
        let kind: u64 = self.into();
        kind.to_le_bytes()
    }

    pub fn unpack(bytes: [u8; 8]) -> Result<Self, num_enum::TryFromPrimitiveError<Self>> {
        let kind = u64::from_le_bytes(bytes);
        Self::try_from(kind)
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::TrackKind;

    #[test]
    fn pack_unpack_roundtrip() {
        let cases = [
            TrackKind::Unknown,
            TrackKind::Blob,
            TrackKind::Stream,
        ];

        for &original in &cases {
            let packed = original.pack();
            let unpacked = TrackKind::unpack(packed)
                .expect("valid track kind");

            assert_eq!(original, unpacked);
        }
    }

    #[test]
    fn pack_known_values() {
        assert_eq!(TrackKind::Unknown.pack(), [0u8; 8]);
        assert_eq!(TrackKind::Blob.pack(), [1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(TrackKind::Stream.pack(), [2, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn unpack_invalid_returns_err() {
        let invalid = [3, 0, 0, 0, 0, 0, 0, 0]; // 3 is not a valid variant
        assert!(TrackKind::unpack(invalid).is_err());
    }
}
