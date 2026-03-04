use crate::define_numeric_type;

// Index types
define_numeric_type!(EpochNumber, "epoch");
define_numeric_type!(SlotNumber, "slot");
define_numeric_type!(TapeNumber, "tape");
define_numeric_type!(TrackNumber, "track");
define_numeric_type!(CommitteeNumber, "committee");
define_numeric_type!(NodeId, "node");
define_numeric_type!(VersionId, "version");
define_numeric_type!(ShareAmount, "shares");

// Generic types
define_numeric_type!(BasisPoints, "bps");
define_numeric_type!(StorageUnits, "units");
define_numeric_type!(ChunkIndex, "chunk");

impl BasisPoints {
    pub const MAX: u64 = 10_000;

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.0 <= 10_000
    }

    #[inline]
    pub fn to_percent(&self) -> f64 {
        (self.0 as f64) / 100.0
    }
}

impl StorageUnits {
    /// 1 MB in bytes — billing granularity.
    pub const MB: u64 = 1024 * 1024;

    /// Create from byte count (identity — StorageUnits stores bytes).
    #[inline]
    pub fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Byte count (identity — StorageUnits stores bytes).
    #[inline]
    pub fn to_bytes(&self) -> u64 {
        self.0
    }

    /// Construct from megabytes.
    #[inline]
    pub fn mb(megabytes: u64) -> Self {
        Self(megabytes.saturating_mul(Self::MB))
    }

    /// Convert to MB (ceiling). Used for billing granularity.
    #[inline]
    pub fn to_mb(&self) -> u64 {
        (self.0 + Self::MB - 1) / Self::MB
    }
}

impl CommitteeNumber {
    const CURRENT: u64 = 0;
    const PREVIOUS: u64 = 1;

    #[inline]
    pub fn previous() -> Self {
        Self::new(Self::PREVIOUS)
    }

    #[inline]
    pub fn current() -> Self {
        Self::new(Self::CURRENT)
    }

    #[inline]
    pub fn is_current(&self) -> bool {
        self.0 == Self::CURRENT
    }

    #[inline]
    pub fn is_previous(&self) -> bool {
        self.0 == Self::PREVIOUS
    }
    
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.0 == Self::CURRENT || self.0 == Self::PREVIOUS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basis_points() {
        let bps = BasisPoints::new(2500);
        assert!(bps.is_valid());
        assert_eq!(bps.to_percent(), 25.0);

        let invalid_bps = BasisPoints::new(15000);
        assert!(!invalid_bps.is_valid());
    }

    #[test]
    fn test_storage_units() {
        let storage = StorageUnits::from_bytes(5_242_880); // 5 MB
        assert_eq!(storage.0, 5_242_880);
        assert_eq!(storage.to_bytes(), 5_242_880);
        assert_eq!(storage.to_mb(), 5);

        let storage_mb = StorageUnits::mb(5);
        assert_eq!(storage_mb.0, 5 * StorageUnits::MB);
        assert_eq!(storage_mb.to_mb(), 5);

        // Ceiling division
        let partial = StorageUnits::from_bytes(StorageUnits::MB + 1);
        assert_eq!(partial.to_mb(), 2);
    }
}
