//! Output format abstraction. Every command returns a value implementing
//! [`CliOutput`]; [`emit`] prints it either as human-readable text or as
//! pretty JSON depending on `--output`.

use clap::ValueEnum;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
#[clap(rename_all = "lowercase")]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
}

/// A command's result. Must be `Serialize` so JSON mode works, and must
/// provide a human-readable rendering so text mode works.
pub trait CliOutput: Serialize {
    /// Write the human-readable form to stdout. Only called when the user
    /// asked for `--output text` (the default).
    fn print_text(&self);
}

pub fn emit<T: CliOutput>(value: &T, format: OutputFormat) -> anyhow::Result<()> {
    match format {
        OutputFormat::Json => {
            let s = serde_json::to_string_pretty(value)?;
            println!("{s}");
        }
        OutputFormat::Text => {
            value.print_text();
        }
    }
    Ok(())
}

/// Convenience: emit a single key-value pair. Used by commands that don't
/// need a full struct.
#[derive(Serialize)]
pub struct KeyValue<V: Serialize> {
    pub key: String,
    pub value: V,
}

impl<V: Serialize + std::fmt::Display> CliOutput for KeyValue<V> {
    fn print_text(&self) {
        println!("{}: {}", self.key, self.value);
    }
}

/// Convenience: a command that just succeeded with a user-facing message.
/// JSON mode emits `{"ok": true, "message": "..."}` so scripts can parse.
#[derive(Serialize)]
pub struct OkMessage {
    pub ok: bool,
    pub message: String,
}

impl OkMessage {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            ok: true,
            message: message.into(),
        }
    }
}

impl CliOutput for OkMessage {
    fn print_text(&self) {
        println!("{}", self.message);
    }
}
