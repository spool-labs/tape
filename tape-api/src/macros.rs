#[macro_export]
macro_rules! state {
    // $acct_ty is your AccountType enum variant, $data_ty is the struct name
    ($acct_ty:ident, $data_ty:ident) => {
        impl $data_ty {
            /// 8 bytes for the discriminator + the POD struct size
            pub const fn get_size() -> usize {
                8 + core::mem::size_of::<Self>()
            }

            /// Immutably unpack from a raw account data slice
            pub fn unpack(data: &[u8]) -> Result<&Self, ProgramError> {
                bytemuck::try_from_bytes::<Self>(data)
                    .map_err(|_| ProgramError::InvalidAccountData)
            }

            /// Mutably unpack from a raw account data slice
            pub fn unpack_mut(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
                bytemuck::try_from_bytes_mut::<Self>(data)
                    .map_err(|_| ProgramError::InvalidAccountData)
            }

            /// Immutably unpack from a raw account data slice
            pub fn unpack_with_discriminator(data: &[u8]) -> Result<&Self, ProgramError> {
                let data = &data[..Self::get_size()];
                Self::try_from_bytes(data)
            }

            /// Mutably unpack from a raw account data slice
            pub fn unpack_with_discriminator_mut(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
                let data = &mut data[..Self::get_size()];
                Self::try_from_bytes_mut(data)
            }
        }

        // steel account macro
        account!($acct_ty, $data_ty);
    };
}

#[macro_export]
macro_rules! impl_to_bytes {
    ($struct_name:ident, $discriminator_name:ident) => {
        impl $struct_name {
            pub fn to_bytes(&self) -> Vec<u8> {
                let mut discriminator = [0u8; 8];
                discriminator[0] = $discriminator_name::$struct_name as u8;
                [
                    discriminator.to_vec(),
                    bytemuck::bytes_of(self).to_vec(),
                ]
                .concat()
            }
        }
    };
}

#[macro_export]
macro_rules! impl_try_from_bytes {
    ($struct_name:ident, $discriminator_name:ident) => {
        impl $struct_name {
            pub fn try_from_bytes(data: &[u8]) -> std::io::Result<&Self> {
                if data.len() < 8 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Data too short for discriminator",
                    ));
                }
                let discriminator = data[0];
                if discriminator != $discriminator_name::$struct_name as u8 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Invalid discriminator: expected {}, got {}",
                            $discriminator_name::$struct_name as u8,
                            discriminator
                        ),
                    ));
                }
                let struct_size = std::mem::size_of::<Self>();
                if data.len() < 8 + struct_size {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Data too short: expected at least {} bytes, got {}",
                            8 + struct_size,
                            data.len()
                        ),
                    ));
                }
                bytemuck::try_from_bytes::<Self>(&data[8..8 + struct_size]).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })
            }
        }
    };
}

#[macro_export]
macro_rules! event {
    ($discriminator_name:ident, $struct_name:ident) => {
        $crate::impl_to_bytes!($struct_name, $discriminator_name);
        $crate::impl_try_from_bytes!($struct_name, $discriminator_name);

        impl $struct_name {
            const DISCRIMINATOR_SIZE: usize = 8;

            pub fn size_of() -> usize {
                core::mem::size_of::<Self>() + Self::DISCRIMINATOR_SIZE
            }

            pub fn log(&self) {
                solana_program::log::sol_log_data(&[&self.to_bytes()]);
            }
        }
    };
}

/// Helper macro to provide baseline unsigned integer functionality.
/// Assumes the type has a single unsigned integer field.
#[macro_export]
macro_rules! wrapped_uint {
    ($type_name:ident, $inner:ty) => {
        impl From<$inner> for $type_name {
            #[inline]
            fn from(value: $inner) -> Self {
                $type_name(value)
            }
        }

        impl From<$type_name> for $inner {
            #[inline]
            fn from(value: $type_name) -> $inner {
                value.0
            }
        }
    };
}

/// A macro to create distinct index types wrapping a `u16` for type safety.
/// Generates a newtype struct with conversions, Default, and Display implementations.
#[macro_export]
macro_rules! define_u16_type {
    ($type_name:ident, $prefix:literal) => {
        /// A type-safe wrapper around a `u16` index for $type_name.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $type_name(u16);

        unsafe impl bytemuck::Pod for $type_name {}
        unsafe impl bytemuck::Zeroable for $type_name {}

        impl $type_name {
            /// Creates a new $type_name from a u16.
            #[inline]
            pub fn new(value: u16) -> Self {
                $type_name(value)
            }

            /// Returns the inner u16 value.
            #[inline]
            pub fn as_u16(&self) -> u16 {
                self.0
            }

            /// Converts the index to usize.
            #[inline]
            pub fn as_usize(&self) -> usize {
                self.0 as usize
            }

            /// Converts the index to u32.
            #[inline]
            pub fn as_u32(&self) -> u32 {
                self.0 as u32
            }

            /// Converts the index to u64.
            #[inline]
            pub fn as_u64(&self) -> u64 {
                self.0 as u64
            }
        }

        impl Default for $type_name {
            /// Returns a default $type_name with value 0.
            fn default() -> Self {
                $type_name(0)
            }
        }

        impl std::fmt::Display for $type_name {
            /// Formats the $type_name as its inner u16 value with a prefix.
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}:{}", $prefix, self.0)
            }
        }

        $crate::wrapped_uint!($type_name, u16);
    };
}

/// A macro to create distinct index types wrapping a `u64` for type safety.
/// Generates a newtype struct with conversions, Default, and Display implementations.
#[macro_export]
macro_rules! define_u64_type {
    ($type_name:ident, $prefix:literal) => {

        /// A type-safe wrapper around a `u64` index for $type_name.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $type_name(u64);

        unsafe impl bytemuck::Pod for $type_name {}
        unsafe impl bytemuck::Zeroable for $type_name {}

        impl $type_name {
            /// Creates a new $type_name from a u64.
            #[inline]
            pub fn new(value: u64) -> Self {
                $type_name(value)
            }

            /// Returns the inner u64 value.
            #[inline]
            pub fn as_u64(&self) -> u64 {
                self.0
            }

            /// Converts the index to usize.
            #[inline]
            pub fn as_usize(&self) -> usize {
                self.0 as usize
            }

            /// Converts the index to u32.
            #[inline]
            pub fn as_u32(&self) -> u32 {
                self.0 as u32
            }
        }

        impl Default for $type_name {
            /// Returns a default $type_name with value 0.
            fn default() -> Self {
                $type_name(0)
            }
        }

        impl std::fmt::Display for $type_name {
            /// Formats the $type_name as its inner u64 value with a prefix.
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}:{}", $prefix, self.0)
            }
        }

        $crate::wrapped_uint!($type_name, u64);
    };
}



#[cfg(test)]
mod tests {
    define_u16_type!(SegmentIndexU16, "seg16");
    define_u16_type!(SectorIndexU16, "sec16");
    define_u64_type!(SegmentIndexU64, "seg64");
    define_u64_type!(SectorIndexU64, "sec64");

    #[test]
    fn test_segment_index_u16() {
        let seg = SegmentIndexU16::new(42);
        assert_eq!(seg.as_u16(), 42);
        assert_eq!(seg.as_usize(), 42);
        assert_eq!(seg.as_u32(), 42);
        assert_eq!(seg.as_u64(), 42);
        assert_eq!(seg, SegmentIndexU16::from(42));
        assert_eq!(u16::from(seg), 42);
        assert_eq!(format!("{}", seg), "seg16:42");
        assert_eq!(SegmentIndexU16::default(), SegmentIndexU16(0));
    }

    #[test]
    fn test_sector_index_u16() {
        let sec = SectorIndexU16::new(99);
        assert_eq!(sec.as_u16(), 99);
        assert_eq!(sec.as_usize(), 99);
        assert_eq!(sec.as_u32(), 99);
        assert_eq!(sec.as_u64(), 99);
        assert_eq!(sec, SectorIndexU16::from(99));
        assert_eq!(u16::from(sec), 99);
        assert_eq!(format!("{}", sec), "sec16:99");
        assert_eq!(SectorIndexU16::default(), SectorIndexU16(0));
    }

    #[test]
    fn test_segment_index_u64() {
        let seg = SegmentIndexU64::new(42_000);
        assert_eq!(seg.as_u64(), 42_000);
        assert_eq!(seg.as_usize(), 42_000);
        assert_eq!(seg.as_u32(), 42_000);
        assert_eq!(seg, SegmentIndexU64::from(42_000));
        assert_eq!(u64::from(seg), 42_000);
        assert_eq!(format!("{}", seg), "seg64:42000");
        assert_eq!(SegmentIndexU64::default(), SegmentIndexU64(0));
    }

    #[test]
    fn test_sector_index_u64() {
        let sec = SectorIndexU64::new(99_000);
        assert_eq!(sec.as_u64(), 99_000);
        assert_eq!(sec.as_usize(), 99_000);
        assert_eq!(sec.as_u32(), 99_000);
        assert_eq!(sec, SectorIndexU64::from(99_000));
        assert_eq!(u64::from(sec), 99_000);
        assert_eq!(format!("{}", sec), "sec64:99000");
        assert_eq!(SectorIndexU64::default(), SectorIndexU64(0));
    }

    #[test]
    fn test_type_safety() {
        let seg_u16 = SegmentIndexU16::new(42);
        let sec_u16 = SectorIndexU16::new(42);
        let seg_u64 = SegmentIndexU64::new(42);
        let sec_u64 = SectorIndexU64::new(42);

        // Different types cannot be compared directly, ensuring type safety.
        assert_eq!(seg_u16.as_u16(), sec_u16.as_u16());
        assert_eq!(seg_u64.as_u64(), sec_u64.as_u64());
        // The following would cause compile-time errors:
        // let _ = seg_u16 == sec_u16; // Different u16 types.
        // let _ = seg_u64 == sec_u64; // Different u64 types.
        // let _ = seg_u16 == seg_u64; // u16 vs u64 types.
    }
}
