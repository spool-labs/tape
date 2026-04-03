//! Shared helper functions for log parsing.

use tape_crypto::address::Address;

/// Check if log indicates program invoke.
pub fn is_program_invoke(log: &str) -> bool {
    log.starts_with("Program ") && log.contains(" invoke ")
}

/// Check if log indicates program success.
pub fn is_program_success(log: &str) -> bool {
    log.starts_with("Program ") && log.contains(" success")
}

/// Check if log indicates program failure.
pub fn is_program_failure(log: &str) -> bool {
    log.starts_with("Program ") && log.contains(" failed")
}

/// Check if log contains program data (event).
pub fn is_program_data(log: &str) -> bool {
    log.starts_with("Program data: ")
}

/// Extract program ID from invoke log.
pub fn get_program_id(log: &str) -> Option<Address> {
    let parts: Vec<&str> = log.split_whitespace().collect();
    if parts.len() >= 3 {
        return parts[1].parse::<Address>().ok();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_program_invoke() {
        assert!(is_program_invoke(
            "Program 11111111111111111111111111111111 invoke [1]"
        ));
        assert!(!is_program_invoke("Program log: Hello"));
    }

    #[test]
    fn test_is_program_data() {
        assert!(is_program_data("Program data: SGVsbG8gV29ybGQ="));
        assert!(!is_program_data("Program log: Hello"));
    }

    #[test]
    fn test_get_program_id() {
        let log = "Program 11111111111111111111111111111111 invoke [1]";
        let pubkey = get_program_id(log).unwrap();
        assert_eq!(pubkey.to_string(), "11111111111111111111111111111111");
    }
}
