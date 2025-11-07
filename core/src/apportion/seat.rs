use bytemuck::{Pod, Zeroable};
use crate::system::Committee;
use super::assignment::*;
use super::dhondt::*;
use tape_crypto::hash::{hashv, Hash};

pub type SeatMapping = u8;
pub type SeatIndex = u16;
pub type SeatCount = u16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeatAssignmentError {
    CountMismatch,
    MemberLimit,
    TotalMismatch,
    BalanceMismatch,
    InsufficientFree,
    BadIndex,
    NotNext,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Seats<const SEATS: usize> {
    pub seats: [SeatMapping; SEATS],
}

unsafe impl<const SEATS: usize> Zeroable for Seats<SEATS> {}
unsafe impl<const SEATS: usize> Pod for Seats<SEATS> {}

impl <const SEATS: usize> Seats<SEATS> {

    /// Create a new seat mapping.
    pub fn new(seat_map: [SeatMapping; SEATS]) -> Self {
        Self {
            seats: seat_map,
        }
    }

    /// Create a seat map from a slice.
    pub fn try_from(seat_map: &[SeatMapping]) -> Result<Self, SeatAssignmentError> {
        if seat_map.len() != SEATS {
            return Err(SeatAssignmentError::TotalMismatch);
        }

        let mut seats = [0u8; SEATS];
        for i in 0..SEATS {
            seats[i] = seat_map[i];
        }

        Ok(Self {
            seats,
        })
    }

    /// Create an initial seat map from seat counts, assigning seats contiguously.
    pub fn try_from_counts(
        seat_counts: &[SeatCount],
    ) -> Result<Self, SeatAssignmentError> {
        let seat_map = to_seat_map(seat_counts);
        Self::try_from(&seat_map)
    }

    /// Reassign seats from current committee to next committee with minimal disruption.
    pub fn reassign<const N:usize>(
        &mut self,
        current: &Committee<N>,
        next: &Committee<N>,
    ) -> Result<(), SeatAssignmentError> {

        let members_current = current.active_members();
        let members_next    = next.active_members();
        let stakes_next     = next.active_stakes();

        // Figure out how many seats each member should get.
        let seat_counts = dhondt_allocate(
            &stakes_next,
            SEATS as u16,
        );

        // Distribute seats with minimal disruption.
        let seats = reassign_seats(
            &self.seats,
            &members_current,
            &members_next,
            &seat_counts,
        )?;

        // Update seat mapping
        for i in 0..SEATS {
            self.seats[i] = seats[i];
        }

        Ok(())
    }

    /// Returns the voting weight for a given member based on how many seats they hold.
    pub fn weight(&self, member_index: usize) -> u16 {
        debug_assert!(member_index <= u8::MAX as usize);

        let mut count = 0u16;
        for i in 0..SEATS {
            if self.seats[i] as usize == member_index {
                count += 1;
            }
        }
        count
    }

    /// Returns a slice of the seat mappings for a given member.
    pub fn seats_for_member(&self, member_index: usize) -> Vec<SeatIndex> {
        debug_assert!(member_index <= u8::MAX as usize);

        let mut seat_indices = Vec::new();
        for i in 0..SEATS {
            if self.seats[i] as usize == member_index {
                seat_indices.push(i as SeatIndex);
            }
        }
        seat_indices
    }


    /// Returns an iterator over the seat mappings.
    pub fn iter(&self) -> impl Iterator<Item = &SeatMapping> {
        self.seats.iter()
    }
}

pub fn get_seat_hash(seats: &[SeatIndex]) -> Hash {
    let data: &[&[u8]] = &[bytemuck::cast_slice(seats)];
    hashv(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_slice_okay() {
        let s = Seats::<4>::try_from(&[0u8, 1, 1, 0]).unwrap();
        assert_eq!(s.seats, [0, 1, 1, 0]);
    }

    #[test]
    fn from_slice_bad_length() {
        let err = Seats::<3>::try_from(&[0u8, 1, 2, 3]).unwrap_err();
        assert_eq!(err, SeatAssignmentError::TotalMismatch);
    }

    #[test]
    fn from_counts_weight() {
        let counts: &[SeatCount] = &[2, 1, 3];
        let seats = Seats::<6>::try_from_counts(counts).unwrap();

        assert_eq!(seats.seats, [0, 0, 1, 2, 2, 2]);
        assert_eq!(seats.weight(0), 2);
        assert_eq!(seats.weight(1), 1);
        assert_eq!(seats.weight(2), 3);
        assert_eq!(seats.weight(3), 0);
    }

    #[test]
    fn from_counts_bad_length() {
        let counts: &[SeatCount] = &[2, 1, 1]; // total 4
        let res = Seats::<3>::try_from_counts(counts);
        assert_eq!(res.unwrap_err(), SeatAssignmentError::TotalMismatch);
    }

    #[test]
    fn weight_count() {
        let seats = Seats::<5>::new([3, 3, 3, 2, 1]);
        assert_eq!(seats.weight(3), 3);
        assert_eq!(seats.weight(2), 1);
        assert_eq!(seats.weight(1), 1);
        assert_eq!(seats.weight(0), 0);
    }

    #[test]
    fn empty_weight() {
        let seats = Seats::<0>::new([]);
        assert_eq!(seats.weight(0), 0);
        assert_eq!(Seats::<0>::try_from(&[]).unwrap().seats, []);
    }

    #[test]
    fn seats_slice() {
        let seats = Seats::<6>::new([1, 0, 1, 2, 1, 0]);
        let member_seats = seats.seats_for_member(1);
        assert_eq!(member_seats, vec![0, 2, 4]);
    }

    #[test]
    fn seat_hash() {
        let seats: &[SeatIndex] = &[42, 1, 2, 3, 4, 5, 99];
        let hash = get_seat_hash(seats);
        let expected_bytes: [u8; 32] = [
            0x41, 0x03, 0xab, 0xff, 0x9f, 0xac, 0xfc, 0x32,
            0x5a, 0xa0, 0x2c, 0x99, 0x23, 0x6b, 0xfc, 0xc9,
            0xea, 0x56, 0xdc, 0x08, 0x41, 0xf3, 0x04, 0xab,
            0x79, 0xd4, 0x5d, 0x3e, 0xe4, 0x0f, 0xbe, 0xcf,
        ];
        assert_eq!(hash.to_bytes(), expected_bytes);
    }
}
