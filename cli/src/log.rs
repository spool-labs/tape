use colored::Colorize;

/// Prints a bold, prominent title for major sections of output.
pub fn print_title(text: &str) {
    println!("{}", format!("\n{text}").bold());
}

/// Prints a plain informational message.
pub fn print_info(text: &str) {
    println!("{text}");
}

/// Prints an empty line to separate sections of output.
pub fn print_divider() {
    println!();
}

/// Prints a highlighted section header with yellow bold text and surrounding markers.
pub fn print_section_header(text: &str) {
    println!("{}", format!("\n=== {text} ===").yellow().bold());
}

/// Prints an informational message with a cyan arrow prefix for emphasis.
pub fn print_message(text: &str) {
    println!("{}", format!("→ {text}").cyan());
}

/// Prints a count or metric with a blue diamond prefix for quantitative data.
pub fn print_count(text: &str) {
    println!("{}", format!("⟐ {text}").blue());
}

/// Prints an error message with a red cross prefix to indicate failure.
pub fn print_error(text: &str) {
    println!("{}", format!("✗ {text}").red());
}
