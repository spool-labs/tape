use crate::define_numeric_type;

// Index types
define_numeric_type!(EpochNumber, "epoch");
define_numeric_type!(PoolNumber, "pool");
define_numeric_type!(SpoolNumber, "spool");
define_numeric_type!(NodeId, "node");

// Generic types
define_numeric_type!(BasisPoints, "bps");
define_numeric_type!(StorageUnits, "bytes");
