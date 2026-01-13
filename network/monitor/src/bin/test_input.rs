//! Minimal test for terminal input over SSH.
//!
//! Run with: cargo run --release -p tape-monitor --bin test_input

use std::io::{self, Write};
use crossterm::{
    event::{self, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode},
};

fn main() -> io::Result<()> {
    println!("Testing terminal input. Press keys to see events, 'q' to quit.");
    println!("If you see nothing when pressing keys, there's a terminal/SSH issue.");
    println!();

    enable_raw_mode()?;

    loop {
        // Use blocking read with timeout
        if event::poll(std::time::Duration::from_millis(500))? {
            let evt = event::read()?;

            // Print to stderr (works in raw mode)
            disable_raw_mode()?;
            println!("Event: {:?}", evt);
            io::stdout().flush()?;
            enable_raw_mode()?;

            if let Event::Key(key) = evt {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    disable_raw_mode()?;
    println!("\nExited normally.");
    Ok(())
}
