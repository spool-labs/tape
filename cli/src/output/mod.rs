//! Output formatting for CLI commands.

use serde::Serialize;

/// Output format for CLI commands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OutputFormat {
    /// JSON output.
    Json,
    /// Table output (default).
    #[default]
    Table,
    /// Plain text output.
    Plain,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(OutputFormat::Json),
            "table" => Ok(OutputFormat::Table),
            "plain" => Ok(OutputFormat::Plain),
            _ => Err(format!(
                "Invalid output format: '{}'. Use json, table, or plain",
                s
            )),
        }
    }
}

/// Print data in the specified format.
pub fn print_output<T: Serialize + TableDisplay>(data: &T, format: OutputFormat) {
    match format {
        OutputFormat::Json => print_json(data),
        OutputFormat::Table => data.print_table(),
        OutputFormat::Plain => data.print_plain(),
    }
}

/// Print data as JSON.
pub fn print_json<T: Serialize + ?Sized>(data: &T) {
    match serde_json::to_string_pretty(data) {
        Ok(json) => println!("{}", json),
        Err(e) => eprintln!("Error serializing to JSON: {}", e),
    }
}

/// Trait for types that can be displayed as a table.
pub trait TableDisplay: Serialize {
    /// Print as a formatted table.
    fn print_table(&self);

    /// Print as plain text.
    fn print_plain(&self) {
        // Default implementation uses JSON
        print_json(self);
    }
}

/// Format a pubkey for display (truncated).
pub fn format_pubkey(pubkey: &str) -> String {
    if pubkey.len() > 12 {
        format!("{}...{}", &pubkey[..4], &pubkey[pubkey.len()-4..])
    } else {
        pubkey.to_string()
    }
}

/// Format a hash for display (truncated).
pub fn format_hash(hash: &[u8]) -> String {
    let hex = hex::encode(hash);
    if hex.len() > 16 {
        format!("0x{}...{}", &hex[..8], &hex[hex.len()-8..])
    } else {
        format!("0x{}", hex)
    }
}

/// Format bytes as human-readable size.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Format a number with thousands separators.
pub fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Format basis points as percentage.
pub fn format_basis_points(bps: u64) -> String {
    format!("{:.2}%", bps as f64 / 100.0)
}

/// Print raw hex dump of data.
pub fn print_hex_dump(data: &[u8]) {
    for (i, chunk) in data.chunks(16).enumerate() {
        // Offset
        print!("{:08x}  ", i * 16);

        // Hex bytes (first 8)
        for (j, byte) in chunk.iter().enumerate() {
            if j == 8 {
                print!(" ");
            }
            print!("{:02x} ", byte);
        }

        // Padding for incomplete lines
        for j in chunk.len()..16 {
            if j == 8 {
                print!(" ");
            }
            print!("   ");
        }

        // ASCII representation
        print!(" |");
        for byte in chunk {
            if *byte >= 0x20 && *byte < 0x7f {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 bytes");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1000000), "1,000,000");
    }

    #[test]
    fn test_format_basis_points() {
        assert_eq!(format_basis_points(500), "5.00%");
        assert_eq!(format_basis_points(10000), "100.00%");
    }
}
