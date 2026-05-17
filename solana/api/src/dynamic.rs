//! `DynamicState` trait — Pod accounts with dynamic size tail region.
//!
//! Each impl points `tail()` at the [`Tail`] metadata that sizes the
//! trailing slice. The default methods below are the canonical loaders;
//! per-struct wrapper functions are not needed.

use core::mem::size_of;
use tape_solana::*;
use tape_core::types::Tail;
use crate::loaders::FromAccountSlice;

pub trait DynamicState: Pod + Discriminator {
    type Entry: Pod;

    fn tail(&self) -> &Tail<Self::Entry>;
    fn tail_mut(&mut self) -> &mut Tail<Self::Entry>;

    fn pack_with(&self, entries: &[Self::Entry]) -> Vec<u8>
    where Self: Sized,
    {
        let header_size = 8 + size_of::<Self>();
        let mut out = vec![0u8; header_size + self.tail().trailing_size()];
        out[0] = Self::discriminator();
        out[8..header_size].copy_from_slice(bytemuck::bytes_of(self));
        let body = bytemuck::cast_slice(entries);
        out[header_size..header_size + body.len()].copy_from_slice(body);
        out
    }

    fn read<'a>(
        info: &'a AccountInfo<'_>,
        program_id: &Pubkey,
    ) -> Result<(&'a Self, &'a [Self::Entry]), ProgramError>
    where Self: Sized,
    {
        let header: &Self = info.from_slice(program_id, 0, size_of::<Self>())?;
        let cap = header.tail().capacity as usize;
        let body: &[Self::Entry] = info.from_slice_array(program_id, size_of::<Self>(), cap)?;

        Ok((header, body))
    }

    fn read_mut<'a>(
        info: &'a AccountInfo<'_>,
        program_id: &Pubkey,
    ) -> Result<(&'a mut Self, &'a mut [Self::Entry]), ProgramError>
    where Self: Sized,
    {
        let header: &mut Self = info.from_slice_mut(program_id, 0, size_of::<Self>())?;
        let cap = header.tail().capacity as usize;
        let body: &mut [Self::Entry] = info.from_slice_array_mut(program_id, size_of::<Self>(), cap)?;

        Ok((header, body))
    }

    fn header<'a>(
        info: &'a AccountInfo<'_>,
        program_id: &Pubkey,
    ) -> Result<&'a Self, ProgramError>
    where Self: Sized,
    {
        info.from_slice(program_id, 0, size_of::<Self>())
    }

    fn header_mut<'a>(
        info: &'a AccountInfo<'_>,
        program_id: &Pubkey,
    ) -> Result<&'a mut Self, ProgramError>
    where Self: Sized,
    {
        info.from_slice_mut(program_id, 0, size_of::<Self>())
    }
}
