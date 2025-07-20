use anyhow::Result;
use std::{ fs, io::{self, Write}, path::Path };
use tape_client::MimeType;

use crate::log;
use super::get_extension;

pub fn write_output(output: Option<String>, data: &[u8], mime_type: MimeType) -> Result<()> {
    match output {
        Some(mut filename) => {
            if Path::new(&filename).extension().is_none() {
                if let Some(ext) = get_extension(mime_type) {
                    filename.push('.');
                    filename.push_str(ext);
                }
            }
            fs::write(&filename, data)?;

            log::print_divider();
            log::print_message(&format!("Wrote output to: {}", filename));
        }
        None => {
            io::stdout().write_all(data)?;
            io::stdout().flush()?;
        }
    }

    Ok(())
}

