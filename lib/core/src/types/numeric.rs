use crate::define_numeric_type;

// Index types
define_numeric_type!(EpochNumber, "epoch");
define_numeric_type!(SlotNumber, "slot");
define_numeric_type!(TapeNumber, "tape");
define_numeric_type!(TrackNumber, "track");
define_numeric_type!(ChunkNumber, "chunk");
define_numeric_type!(CommitteeNumber, "committee");
define_numeric_type!(NodeId, "node");
define_numeric_type!(VersionId, "version");
define_numeric_type!(ShareAmount, "shares");

// Generic types
define_numeric_type!(BasisPoints, "bps");
define_numeric_type!(StorageUnits, "units");
define_numeric_type!(ChunkIndex, "chunk");
define_numeric_type!(StripeCount, "stripes");

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
    /// 1 MB in bytes.
    pub const MB: u64 = 1024 * 1024;
    /// 1 GB in bytes.
    pub const GB: u64 = 1024 * Self::MB;
    /// 1 TB in bytes.
    pub const TB: u64 = 1024 * Self::GB;

    /// Create from byte count.
    #[inline]
    pub fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Byte count.
    #[inline]
    pub fn to_bytes(&self) -> u64 {
        self.0
    }

    /// Construct from megabytes.
    #[inline]
    pub fn mb(megabytes: u64) -> Self {
        Self(megabytes.saturating_mul(Self::MB))
    }

    /// Construct from gigabytes.
    #[inline]
    pub fn gb(gigabytes: u64) -> Self {
        Self(gigabytes.saturating_mul(Self::GB))
    }

    /// Construct from terabytes.
    #[inline]
    pub fn tb(terabytes: u64) -> Self {
        Self(terabytes.saturating_mul(Self::TB))
    }

    /// Convert to MB (ceiling).
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

        let storage_gb = StorageUnits::gb(2);
        assert_eq!(storage_gb.0, 2 * StorageUnits::GB);
        assert_eq!(storage_gb.to_mb(), 2 * 1024);

        let storage_tb = StorageUnits::tb(3);
        assert_eq!(storage_tb.0, 3 * StorageUnits::TB);
        assert_eq!(storage_tb.to_mb(), 3 * 1024 * 1024);

        // Ceiling division
        let partial = StorageUnits::from_bytes(StorageUnits::MB + 1);
        assert_eq!(partial.to_mb(), 2);
    }

    #[test]
    fn test_stripe_count() {
        let stripe_count = StripeCount(4);
        assert_eq!(stripe_count.pack(), 4u64.to_le_bytes());
        assert_eq!(StripeCount::unpack(7u64.to_le_bytes()), StripeCount(7));
        assert_eq!(stripe_count.as_u64(), 4);
    }
}
