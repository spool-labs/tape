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

/// A macro to create distinct value types wrapping a `u64` for type safety.
/// Generates a newtype struct with conversions, Default implementations.
#[macro_export]
macro_rules! define_u64_type {
    ($type_name:ident) => {

        /// A type-safe wrapper around a `u64` value for $type_name.
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
        #[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
        #[repr(transparent)]
        pub struct $type_name(pub u64);

        unsafe impl bytemuck::Pod for $type_name {}
        unsafe impl bytemuck::Zeroable for $type_name {}

        impl $type_name {
            /// Creates a new $type_name from a u64.
            #[inline]
            pub fn new(value: u64) -> Self {
                $type_name(value)
            }

            /// Zero value for $type_name.
            #[inline]
            pub const fn zero() -> Self {
                $type_name(0)
            }

            /// One value for $type_name.
            #[inline]
            pub const fn one() -> Self {
                $type_name(1)
            }

            /// Pack from a u64 into a [u8; 8] array in little-endian order.
            #[inline]
            pub fn pack(&self) -> [u8; 8] {
                self.0.to_le_bytes()
            }

            /// Unpack from a [u8; 8] array in little-endian order into a $type_name.
            #[inline]
            pub fn unpack(data: [u8; 8]) -> Self {
                $type_name(u64::from_le_bytes(data))
            }

            /// Returns the inner u64 value.
            #[inline]
            pub fn as_u64(&self) -> u64 {
                self.0
            }

            /// Returns the value as a u128
            #[inline]
            pub fn as_u128(&self) -> u128 {
                self.0 as u128
            }

            /// Converts the value to usize.
            #[inline]
            pub fn as_usize(&self) -> usize {
                self.0 as usize
            }

            /// Converts the value to u32.
            #[inline]
            pub fn as_u32(&self) -> u32 {
                self.0 as u32
            }

            /// Checked addition with overflow handling.
            #[inline]
            pub fn checked_add(&self, rhs: Self) -> Option<Self> {
                self.0.checked_add(rhs.0).map($type_name)
            }

            /// Checked subtraction with underflow handling.
            #[inline]
            pub fn checked_sub(&self, rhs: Self) -> Option<Self> {
                self.0.checked_sub(rhs.0).map($type_name)
            }

            /// Checked multiplication with overflow handling.
            #[inline]
            pub fn checked_mul(&self, rhs: Self) -> Option<Self> {
                self.0.checked_mul(rhs.0).map($type_name)
            }

            /// Checked division with division-by-zero handling.
            #[inline]
            pub fn checked_div(&self, rhs: Self) -> Option<Self> {
                self.0.checked_div(rhs.0).map($type_name)
            }

            /// Saturating addition, capping at u64::MAX.
            #[inline]
            pub fn saturating_add(&self, rhs: Self) -> Self {
                $type_name(self.0.saturating_add(rhs.0))
            }

            /// Saturating subtraction, capping at 0.
            #[inline]
            pub fn saturating_sub(&self, rhs: Self) -> Self {
                $type_name(self.0.saturating_sub(rhs.0))
            }

            /// Saturating multiplication, capping at u64::MAX.
            #[inline]
            pub fn saturating_mul(&self, rhs: Self) -> Self {
                $type_name(self.0.saturating_mul(rhs.0))
            }

            /// Increments the by 1, saturating at u64::MAX.
            #[inline]
            pub fn increment(&mut self) {
                self.0 = self.0.saturating_add(1);
            }

            /// Decrements the by 1, saturating at 0.
            #[inline]
            pub fn decrement(&mut self) {
                self.0 = self.0.saturating_sub(1);
            }

            /// Returns true if the value is zero.
            #[inline]
            pub fn is_zero(&self) -> bool {
                self.0 == 0
            }

            /// Returns true if the value is one.
            #[inline]
            pub fn is_one(&self) -> bool {
                self.0 == 1
            }
        }

        impl core::ops::Add for $type_name {
            type Output = Self;

            #[inline]
            fn add(self, rhs: Self) -> Self {
                self.checked_add(rhs).expect("Addition overflow")
            }
        }

        impl core::ops::Sub for $type_name {
            type Output = Self;

            #[inline]
            fn sub(self, rhs: Self) -> Self {
                self.checked_sub(rhs).expect("Subtraction underflow")
            }
        }

        impl core::ops::Mul for $type_name {
            type Output = Self;

            #[inline]
            fn mul(self, rhs: Self) -> Self {
                self.checked_mul(rhs).expect("Multiplication overflow")
            }
        }

        impl core::ops::Div for $type_name {
            type Output = Self;

            #[inline]
            fn div(self, rhs: Self) -> Self {
                self.checked_div(rhs).expect("Division by zero or overflow")
            }
        }

        impl Default for $type_name {
            /// Returns a default $type_name with value 0.
            fn default() -> Self {
                $type_name(0)
            }
        }

        $crate::wrapped_uint!($type_name, u64);
    };
}

/// A macro to create distinct value types wrapping a `u64` for type safety.
/// Generates a newtype struct with conversions, Default, and Display implementations.
#[macro_export]
macro_rules! define_numeric_type {
    ($type_name:ident, $prefix:literal) => {
        $crate::define_u64_type!($type_name);

        impl core::fmt::Debug for $type_name {
            /// Formats the $type_name as its inner u64 value with a prefix.
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, "{}:{}", $prefix, self.0)
            }
        }

        impl core::fmt::Display for $type_name {
            /// Formats the $type_name as its inner u64 value with a prefix.
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, "{}:{}", $prefix, self.0)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    define_u64_type!(BasisPoints);
    define_numeric_type!(TapeNumber, "tape");

    #[test]
    fn test_segment() {
        let v = BasisPoints::new(42_000);
        assert_eq!(v.as_u64(), 42_000);
        assert_eq!(v.as_usize(), 42_000);
        assert_eq!(v.as_u32(), 42_000);
        assert_eq!(u64::from(v), 42_000);
    }

    #[test]
    fn test_tape() {
        let v = TapeNumber::new(99_000);
        assert_eq!(v.as_u64(), 99_000);
        assert_eq!(v.as_usize(), 99_000);
        assert_eq!(v.as_u32(), 99_000);
        assert_eq!(v, TapeNumber::from(99_000));
        assert_eq!(u64::from(v), 99_000);
        assert_eq!(format!("{}", v), "tape:99000");
        assert_eq!(TapeNumber::default(), TapeNumber(0));
    }
}
