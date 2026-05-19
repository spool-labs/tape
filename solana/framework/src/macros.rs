//! Macro definitions for Solana programs.
//!
//! These macros reduce boilerplate for account definitions, instructions, events, and errors.

/// Similar to declare_id! from solana_program, but doesn't declare everything pub.
#[macro_export]
macro_rules! declare_id {
    ($address:expr) => {
        /// The const program ID.
        pub const ID: Pubkey = Pubkey::from_str_const($address);

        /// Returns the program ID.
        pub const fn id() -> Pubkey {
            ID
        }
    };
}

/// Implements to_bytes() for a struct with discriminator.
#[macro_export]
macro_rules! impl_to_bytes {
    ($struct_name:ident) => {
        impl $struct_name {
            pub fn to_bytes(&self) -> &[u8] {
                bytemuck::bytes_of(self)
            }
        }
    };
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

/// Implements from_bytes() for a struct.
#[macro_export]
macro_rules! impl_from_bytes {
    ($struct_name:ident) => {
        impl $struct_name {
            pub fn from_bytes(data: &[u8]) -> &Self {
                bytemuck::from_bytes::<Self>(data)
            }
        }
    };
}

/// Implements try_from_bytes() for instruction parsing.
#[macro_export]
macro_rules! impl_instruction_from_bytes {
    ($struct_name:ident) => {
        impl $struct_name {
            pub fn try_from_bytes(
                data: &[u8],
            ) -> Result<Self, solana_program::program_error::ProgramError> {
                if data.len() != core::mem::size_of::<Self>() {
                    return Err(solana_program::program_error::ProgramError::InvalidInstructionData);
                }

                let mut value = <Self as bytemuck::Zeroable>::zeroed();
                bytemuck::bytes_of_mut(&mut value).copy_from_slice(data);
                Ok(value)
            }
        }
    };
}

/// Implements try_from_bytes() with discriminator validation.
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

/// Defines an account type with all necessary trait implementations.
///
/// Generates:
/// - `to_bytes()` implementation
/// - `Discriminator` trait impl
/// - `AccountValidation` trait impl with all assertion methods
#[macro_export]
macro_rules! account {
    ($discriminator_name:ident, $struct_name:ident) => {
        $crate::impl_to_bytes!($struct_name);

        impl $crate::Discriminator for $struct_name {
            fn discriminator() -> u8 {
                $discriminator_name::$struct_name.into()
            }
        }

        impl $crate::AccountValidation for $struct_name {
            #[track_caller]
            fn assert<F>(
                &self,
                condition: F,
            ) -> Result<&Self, solana_program::program_error::ProgramError>
            where
                F: Fn(&Self) -> bool,
            {
                if !condition(self) {
                    return Err($crate::trace(
                        "Account data is invalid",
                        solana_program::program_error::ProgramError::InvalidAccountData,
                    ));
                }
                Ok(self)
            }

            #[track_caller]
            fn assert_err<F>(
                &self,
                condition: F,
                err: solana_program::program_error::ProgramError,
            ) -> Result<&Self, solana_program::program_error::ProgramError>
            where
                F: Fn(&Self) -> bool,
            {
                if !condition(self) {
                    return Err($crate::trace("Account data is invalid", err));
                }
                Ok(self)
            }

            #[track_caller]
            fn assert_msg<F>(
                &self,
                condition: F,
                msg: &str,
            ) -> Result<&Self, solana_program::program_error::ProgramError>
            where
                F: Fn(&Self) -> bool,
            {
                if !condition(self) {
                    return Err($crate::trace(
                        format!("Account data is invalid: {}", msg).as_str(),
                        solana_program::program_error::ProgramError::InvalidAccountData,
                    ));
                }
                Ok(self)
            }

            #[track_caller]
            fn assert_mut<F>(
                &mut self,
                condition: F,
            ) -> Result<&mut Self, solana_program::program_error::ProgramError>
            where
                F: Fn(&Self) -> bool,
            {
                if !condition(self) {
                    return Err($crate::trace(
                        "Account data is invalid",
                        solana_program::program_error::ProgramError::InvalidAccountData,
                    ));
                }
                Ok(self)
            }

            #[track_caller]
            fn assert_mut_err<F>(
                &mut self,
                condition: F,
                err: solana_program::program_error::ProgramError,
            ) -> Result<&mut Self, solana_program::program_error::ProgramError>
            where
                F: Fn(&Self) -> bool,
            {
                if !condition(self) {
                    return Err($crate::trace("Account data is invalid", err));
                }
                Ok(self)
            }

            #[track_caller]
            fn assert_mut_msg<F>(
                &mut self,
                condition: F,
                msg: &str,
            ) -> Result<&mut Self, solana_program::program_error::ProgramError>
            where
                F: Fn(&Self) -> bool,
            {
                if !condition(self) {
                    return Err($crate::trace(
                        format!("Account data is invalid: {}", msg).as_str(),
                        solana_program::program_error::ProgramError::InvalidAccountData,
                    ));
                }
                Ok(self)
            }
        }
    };
}

/// Defines an account state type with pack/unpack helpers.
///
/// Generates everything from `account!` plus:
/// - `get_size()` - returns 8 + sizeof::<Self>
/// - `pack()` - serializes with discriminator
/// - `unpack()` / `unpack_mut()` - deserializes without discriminator check
/// - `unpack_with_discriminator()` / `unpack_with_discriminator_mut()` - deserializes with discriminator check
#[macro_export]
macro_rules! state {
    ($acct_ty:ident, $data_ty:ident) => {
        impl $data_ty {
            /// 8 bytes for the discriminator + the POD struct size
            pub const fn get_size() -> usize {
                8 + core::mem::size_of::<Self>()
            }

            pub fn pack(&self) -> Vec<u8> {
                let mut discriminator = [0u8; 8];
                discriminator[0] = $acct_ty::$data_ty as u8;

                let mut data = Vec::with_capacity(8 + self.to_bytes().len());
                data.extend_from_slice(&discriminator);
                data.extend_from_slice(self.to_bytes());

                data
            }

            /// Immutably unpack from a raw account data slice
            pub fn unpack(data: &[u8]) -> Result<&Self, solana_program::program_error::ProgramError> {
                bytemuck::try_from_bytes::<Self>(data)
                    .map_err(|_| solana_program::program_error::ProgramError::InvalidAccountData)
            }

            /// Mutably unpack from a raw account data slice
            pub fn unpack_mut(data: &mut [u8]) -> Result<&mut Self, solana_program::program_error::ProgramError> {
                bytemuck::try_from_bytes_mut::<Self>(data)
                    .map_err(|_| solana_program::program_error::ProgramError::InvalidAccountData)
            }

            /// Immutably unpack from a raw account data slice with discriminator
            pub fn unpack_with_discriminator(data: &[u8]) -> Result<&Self, solana_program::program_error::ProgramError> {
                let data = &data[..Self::get_size()];
                <Self as $crate::AccountDeserialize>::try_from_bytes(data)
            }

            /// Mutably unpack from a raw account data slice with discriminator
            pub fn unpack_with_discriminator_mut(data: &mut [u8]) -> Result<&mut Self, solana_program::program_error::ProgramError> {
                let data = &mut data[..Self::get_size()];
                <Self as $crate::AccountDeserialize>::try_from_bytes_mut(data)
            }
        }

        // Include all the account! macro functionality
        $crate::account!($acct_ty, $data_ty);
    };
}

/// Converts a custom error enum to ProgramError::Custom.
#[macro_export]
macro_rules! error {
    ($struct_name:ident) => {
        impl From<$struct_name> for solana_program::program_error::ProgramError {
            fn from(e: $struct_name) -> Self {
                solana_program::program_error::ProgramError::Custom(e as u32)
            }
        }
    };
}

/// Defines an event type with logging support.
///
/// Generates:
/// - `to_bytes()` with discriminator
/// - `try_from_bytes()` with discriminator validation
/// - `size_of()` - total size including discriminator
/// - `log()` - emits via sol_log_data
#[macro_export]
macro_rules! event {
    ($struct_name:ident) => {
        $crate::impl_to_bytes!($struct_name);
        $crate::impl_from_bytes!($struct_name);

        impl $crate::Loggable for $struct_name {
            fn log(&self) {
                solana_program::log::sol_log_data(&[self.to_bytes()]);
            }

            fn log_return(&self) {
                solana_program::program::set_return_data(self.to_bytes());
            }
        }
    };
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

/// Defines an instruction type.
///
/// Generates:
/// - `try_from_bytes()` for instruction parsing
/// - `Discriminator` trait impl
/// - `to_bytes()` that prepends discriminator
#[macro_export]
macro_rules! instruction {
    ($discriminator_name:ident, $struct_name:ident) => {
        $crate::impl_instruction_from_bytes!($struct_name);

        impl $crate::Discriminator for $struct_name {
            fn discriminator() -> u8 {
                $discriminator_name::$struct_name as u8
            }
        }

        impl $struct_name {
            pub fn to_bytes(&self) -> Vec<u8> {
                [
                    [$discriminator_name::$struct_name as u8].to_vec(),
                    bytemuck::bytes_of(self).to_vec(),
                ]
                .concat()
            }
        }
    };
}
