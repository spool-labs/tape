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


