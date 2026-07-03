//! Account deserialization traits.

use bytemuck::Pod;
use solana_program::program_error::ProgramError;

/// Trait for types that have a discriminator byte.
pub trait Discriminator {
    /// Returns the discriminator byte for this type.
    fn discriminator() -> u8;
}

/// Trait for deserializing account data.
pub trait AccountDeserialize {
    /// Deserialize from bytes, checking discriminator.
    fn try_from_bytes(data: &[u8]) -> Result<&Self, ProgramError>;
    /// Deserialize from mutable bytes, checking discriminator.
    fn try_from_bytes_mut(data: &mut [u8]) -> Result<&mut Self, ProgramError>;
}

impl<T> AccountDeserialize for T
where
    T: Discriminator + Pod,
{
    fn try_from_bytes(data: &[u8]) -> Result<&Self, ProgramError> {
        if Self::discriminator().ne(&data[0]) {
            return Err(ProgramError::InvalidAccountData);
        }
        bytemuck::try_from_bytes::<Self>(&data[8..]).or(Err(ProgramError::InvalidAccountData))
    }

    fn try_from_bytes_mut(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
        if Self::discriminator().ne(&data[0]) {
            return Err(ProgramError::InvalidAccountData);
        }
        bytemuck::try_from_bytes_mut::<Self>(&mut data[8..])
            .or(Err(ProgramError::InvalidAccountData))
    }
}

/// Account data is sometimes stored via a header and body type,
/// where the former resolves the type of the latter (e.g. merkle trees with a generic size const).
/// This trait parses a header type from the first N bytes of some data, and returns the remaining
/// bytes, which are then available for further processing.
pub trait AccountHeaderDeserialize {
    /// Parse header and return remaining bytes.
    fn try_header_from_bytes(data: &[u8]) -> Result<(&Self, &[u8]), ProgramError>;
    /// Parse header mutably and return remaining bytes.
    fn try_header_from_bytes_mut(data: &mut [u8]) -> Result<(&mut Self, &mut [u8]), ProgramError>;
}

impl<T> AccountHeaderDeserialize for T
where
    T: Discriminator + Pod,
{
    fn try_header_from_bytes(data: &[u8]) -> Result<(&Self, &[u8]), ProgramError> {
        if Self::discriminator().ne(&data[0]) {
            return Err(ProgramError::InvalidAccountData);
        }
        let (prefix, remainder) = data[8..].split_at(std::mem::size_of::<T>());
        Ok((
            bytemuck::try_from_bytes::<Self>(prefix).or(Err(ProgramError::InvalidAccountData))?,
            remainder,
        ))
    }

    fn try_header_from_bytes_mut(data: &mut [u8]) -> Result<(&mut Self, &mut [u8]), ProgramError> {
        if Self::discriminator().ne(&data[0]) {
            return Err(ProgramError::InvalidAccountData);
        }
        let (prefix, remainder) = data[8..].split_at_mut(std::mem::size_of::<T>());
        Ok((
            bytemuck::try_from_bytes_mut::<Self>(prefix)
                .or(Err(ProgramError::InvalidAccountData))?,
            remainder,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::{Pod, Zeroable};

    #[repr(C)]
    #[derive(Copy, Clone, Zeroable, Pod)]
    struct TestType {
        field0: u64,
        field1: u64,
    }

    impl Discriminator for TestType {
        fn discriminator() -> u8 {
            7
        }
    }

    #[test]
    fn account_deserialize() {
        let mut data = [0u8; 24];
        data[0] = 7;
        data[8] = 42;
        data[16] = 43;
        let foo = TestType::try_from_bytes(&data).unwrap();
        assert_eq!(42, foo.field0);
        assert_eq!(43, foo.field1);
    }

    // both header loaders reject data with the wrong discriminator
    #[test]
    fn header_deserialize_checks_discriminator() {
        let mut data = [0u8; 24];
        data[0] = 7;
        data[8] = 42;
        let (header, rest) = TestType::try_header_from_bytes_mut(&mut data).unwrap();
        assert_eq!(42, header.field0);
        assert!(rest.is_empty());

        data[0] = 8;
        assert!(TestType::try_header_from_bytes(&data).is_err());
        assert!(TestType::try_header_from_bytes_mut(&mut data).is_err());
    }
}
