use crate::define_u64_type;

define_u64_type!(TAPE, "TAPE");
define_u64_type!(SOL, "SOL");

/// A type alias for coin amounts.
pub type Coin<T> = T;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tape() {
        let amount = TAPE::new(1000);
        assert_eq!(amount.as_u64(), 1000);
        assert_eq!(format!("{}", amount), "TAPE:1000");
    }

    #[test]
    fn test_sol() {
        let amount = SOL::new(500);
        assert_eq!(amount.as_u64(), 500);
        assert_eq!(format!("{}", amount), "SOL:500");
    }
}
