use crate::define_numeric_type;

// Index types
define_numeric_type!(EpochNumber, "epoch");
define_numeric_type!(PoolNumber, "pool");
define_numeric_type!(SpoolNumber, "spool");
define_numeric_type!(NodeId, "node");

// Generic types
define_numeric_type!(BasisPoints, "bps");
define_numeric_type!(StorageUnits, "units");

impl BasisPoints {
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
    const BYTES: u64 = 1024 * 1024;

    #[inline]
    pub fn from_bytes(bytes: u64) -> Self {
        Self::new((bytes + Self::BYTES - 1) / Self::BYTES)
    }

    #[inline]
    pub fn to_bytes(&self) -> u64 {
        self.0 * Self::BYTES
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
        assert_eq!(storage.0, 5);
        assert_eq!(storage.to_bytes(), 5 * StorageUnits::BYTES);

        let storage_exact = StorageUnits::from_bytes(5 * StorageUnits::BYTES);
        assert_eq!(storage_exact.0, 5);
    }
}
