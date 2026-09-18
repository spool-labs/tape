//! The blacklist format: `tapedrive-blacklist` on the first line of track 0, then one entry per line

use tape_core::system::BlacklistEntry;
use tape_crypto::address::Address;

use crate::entry::{Action, Entry, kind_name};
use crate::error::BlacklistError;

/// First line of a blacklist tape's track 0
pub const HEADER: &str = "tapedrive-blacklist";

const KIND_TRACK: &str = "track";
const KIND_TAPE: &str = "tape";

/// The entries in a tape's track 0, or `None` when the track does not declare a blacklist tape
///
/// A declared blacklist tape whose track 0 entries do not parse is still a
/// blacklist tape; its track 0 just names nothing.
pub fn parse_first(bytes: &[u8]) -> Option<Result<Vec<Entry>, BlacklistError>> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (header, rest) = text.split_once('\n').unwrap_or((text, ""));
    if header.trim() != HEADER {
        return None;
    }
    // The header is line 1.
    Some(parse_lines(rest, 2))
}

/// The entries in any track after track 0
pub fn parse(bytes: &[u8]) -> Result<Vec<Entry>, BlacklistError> {
    let text = std::str::from_utf8(bytes).map_err(|_| BlacklistError::NotText)?;
    parse_lines(text, 1)
}

/// A blacklist tape's track 0: the header, then the entries
pub fn render_first(entries: &[Entry]) -> Result<Vec<u8>, BlacklistError> {
    let mut text = format!("{HEADER}\n");
    for (index, entry) in entries.iter().enumerate() {
        text.push_str(&entry_line(entry, index + 2)?);
    }
    Ok(text.into_bytes())
}

/// A track after track 0 holding the entries
pub fn render(entries: &[Entry]) -> Result<Vec<u8>, BlacklistError> {
    let mut text = String::new();
    for (index, entry) in entries.iter().enumerate() {
        text.push_str(&entry_line(entry, index + 1)?);
    }
    Ok(text.into_bytes())
}

/// Entries packed in order into as few tracks after track 0 as fit `max_bytes` each
pub fn render_tracks(entries: &[Entry], max_bytes: usize) -> Result<Vec<Vec<u8>>, BlacklistError> {
    let mut tracks = Vec::new();
    let mut text = String::new();
    for (index, entry) in entries.iter().enumerate() {
        let line = entry_line(entry, index + 1)?;
        if line.len() > max_bytes {
            return Err(BlacklistError::TrackLimit { max_bytes });
        }
        if text.len() + line.len() > max_bytes {
            tracks.push(std::mem::take(&mut text).into_bytes());
        }
        text.push_str(&line);
    }
    if !text.is_empty() {
        tracks.push(text.into_bytes());
    }
    Ok(tracks)
}

fn parse_lines(text: &str, first_line: usize) -> Result<Vec<Entry>, BlacklistError> {
    let mut entries = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry = parse_entry(line).map_err(|reason| BlacklistError::InvalidEntry {
            line: first_line + index,
            reason,
        })?;
        entries.push(entry);
    }
    Ok(entries)
}

fn entry_line(entry: &Entry, line: usize) -> Result<String, BlacklistError> {
    let invalid = || BlacklistError::InvalidEntry {
        line,
        reason: format!("`{}` does not apply to this target", entry.action.as_str()),
    };
    if !entry.is_valid() {
        return Err(invalid());
    }
    let kind = kind_name(&entry.target).ok_or_else(invalid)?;
    Ok(format!("{} {kind} {}\n", entry.action.as_str(), entry.target.target))
}

fn parse_entry(line: &str) -> Result<Entry, String> {
    let words: Vec<&str> = line.split_whitespace().collect();
    let [action, kind, address] = words.as_slice() else {
        return Err("expected an action, a kind and an address".to_string());
    };

    let action = Action::parse(action).ok_or_else(|| format!("unknown action `{action}`"))?;
    let address: Address = address
        .parse()
        .map_err(|_| format!("`{address}` is not an address"))?;
    let target = match *kind {
        KIND_TRACK => BlacklistEntry::track(address),
        KIND_TAPE => BlacklistEntry::tape(address),
        unknown => return Err(format!("unknown kind `{unknown}`")),
    };

    let entry = Entry { action, target };
    if !entry.is_valid() {
        return Err(format!("`{}` does not apply to a {kind}", action.as_str()));
    }
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    // track 0 and later tracks round-trip their entries
    #[test]
    fn round_trip() {
        let entries = vec![Entry::remove(address(1)), Entry::unlist(address(2))];

        let first = render_first(&entries).expect("render first");
        let later = render(&entries).expect("render");

        assert_eq!(parse_first(&first), Some(Ok(entries.clone())));
        assert_eq!(parse(&later), Ok(entries));
    }

    // the text is the header, then one readable entry per line
    #[test]
    fn readable_text() {
        let bytes = render_first(&[Entry::remove(address(1))]).expect("render");

        let text = String::from_utf8(bytes).expect("utf-8");
        assert_eq!(text, format!("{HEADER}\nremove track {}\n", address(1)));
    }

    // blank lines and crlf endings are tolerated
    #[test]
    fn loose_whitespace() {
        let text = format!("\r\n  unlist tape {}  \r\n\r\n", address(4));

        assert_eq!(parse(text.as_bytes()), Ok(vec![Entry::unlist(address(4))]));
    }

    // only an exact first line declares a blacklist tape
    #[test]
    fn declarations() {
        let header_only = format!("{HEADER}\n");
        let versioned = format!("{HEADER} v2\nremove track {}\n", address(1));
        let not_first = format!("remove track {}\n{HEADER}\n", address(1));
        let bad_entry = format!("{HEADER}\nremove track nope\n");

        assert_eq!(parse_first(header_only.as_bytes()), Some(Ok(Vec::new())));
        assert_eq!(parse_first(versioned.as_bytes()), None);
        assert_eq!(parse_first(not_first.as_bytes()), None);
        assert_eq!(parse_first(&[0xff, 0xfe]), None);
        assert!(matches!(parse_first(bad_entry.as_bytes()), Some(Err(BlacklistError::InvalidEntry { line: 2, .. }))));
    }

    // a header in a later track is not an entry, so that track names nothing
    #[test]
    fn later_header() {
        let text = format!("{HEADER}\nremove track {}\n", address(1));

        assert!(matches!(parse(text.as_bytes()), Err(BlacklistError::InvalidEntry { line: 1, .. })));
        assert_eq!(parse(&[0xff]), Err(BlacklistError::NotText));
    }

    // actions only apply to their kind, restore no longer exists, and malformed lines name their line
    #[test]
    fn invalid_entries() {
        let wrong_kind = format!("remove tape {}\n", address(1));
        let restore = format!("restore track {}\n", address(1));
        let bad_address = "remove track nope\n".to_string();
        let short = "\nremove track\n".to_string();

        assert!(matches!(parse(wrong_kind.as_bytes()), Err(BlacklistError::InvalidEntry { line: 1, .. })));
        assert!(matches!(parse(restore.as_bytes()), Err(BlacklistError::InvalidEntry { line: 1, .. })));
        assert!(matches!(parse(bad_address.as_bytes()), Err(BlacklistError::InvalidEntry { line: 1, .. })));
        assert!(matches!(parse(short.as_bytes()), Err(BlacklistError::InvalidEntry { line: 2, .. })));
    }

    // packing keeps order and never exceeds the byte limit
    #[test]
    fn packed_tracks() {
        let mut entries = Vec::new();
        for byte in 1..=200 {
            entries.push(Entry::remove(address(byte)));
        }
        let max_bytes = 1024;

        let tracks = render_tracks(&entries, max_bytes).expect("render tracks");

        let mut parsed = Vec::new();
        for track in &tracks {
            assert!(track.len() <= max_bytes);
            parsed.extend(parse(track).expect("parse"));
        }
        assert!(tracks.len() > 1);
        assert_eq!(parsed, entries);
    }

    // a limit too small for one entry is refused
    #[test]
    fn tiny_limit() {
        assert_eq!(
            render_tracks(&[Entry::remove(address(1))], 10),
            Err(BlacklistError::TrackLimit { max_bytes: 10 })
        );
    }
}
