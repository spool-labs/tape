use crate::define_u64_type_with_display;

// Tapedrive specific types
define_u64_type_with_display!(ArchiveNumber, "archive");
define_u64_type_with_display!(EpochNumber, "epoch");
define_u64_type_with_display!(PoolNumber, "pool");
define_u64_type_with_display!(SpoolNumber, "spool");
define_u64_type_with_display!(NodeId, "node");

// Generic types
define_u64_type_with_display!(BasisPoints, "bps");
