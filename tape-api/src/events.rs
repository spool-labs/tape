
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
