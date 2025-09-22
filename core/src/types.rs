use crate::define_u64_type;

// Tapedrive specific types
define_u64_type!(ArchiveNumber, "archive");
define_u64_type!(EpochNumber, "epoch");
define_u64_type!(PoolNumber, "pool");
define_u64_type!(SpoolNumber, "spool");
define_u64_type!(NodeId, "node");

// Generic types
define_u64_type!(BasisPoints, "bps");
