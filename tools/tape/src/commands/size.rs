use crate::error::{Error, Result};

/// Parse a size spec: a bare number is bytes; a `k`/`m`/`g` suffix
/// (case-insensitive) uses binary KiB/MiB/GiB.
pub fn parse_size(spec: &str) -> Result<u64> {
    let s = spec.trim();
    if s.is_empty() {
        return Err(Error::Invalid("empty size".into()));
    }

    let (digits, suffix) = match s.chars().last() {
        Some(c) if c.is_ascii_alphabetic() => (&s[..s.len() - 1], c.to_ascii_lowercase()),
        _ => (s, '\0'),
    };

    let n: u64 = digits
        .parse()
        .map_err(|e| Error::Invalid(format!("invalid size `{spec}`: {e}")))?;

    let mul = match suffix {
        '\0' => 1u64,
        'k' => 1 << 10,
        'm' => 1 << 20,
        'g' => 1 << 30,
        other => {
            return Err(Error::Invalid(format!(
                "unknown size suffix `{other}`; use k/m/g or bytes"
            )));
        }
    };

    n.checked_mul(mul)
        .ok_or_else(|| Error::Invalid(format!("size overflow: {spec}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_raw_bytes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
    }

    #[test]
    fn parses_suffixes() {
        assert_eq!(parse_size("1k").unwrap(), 1024);
        assert_eq!(parse_size("5M").unwrap(), 5 * 1024 * 1024);
        assert_eq!(parse_size("2g").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn rejects_unknown_suffix() {
        assert!(parse_size("1x").is_err());
    }
}
