//! The rack format: `tapedrive-rack` on the first line of track 0, then one record per line

use tape_crypto::address::Address;

use crate::error::RackError;
use crate::record::{Record, is_valid_key};

/// First line of a rack's track 0
pub const HEADER: &str = "tapedrive-rack";

const KIND_TAPE: &str = "tape";

/// The records in a tape's track 0, or `None` when the track does not declare a rack
///
/// A declared rack whose track 0 records do not parse is still a rack; its track 0
/// just names nothing.
pub fn parse_first(bytes: &[u8]) -> Option<Result<Vec<Record>, RackError>> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (header, rest) = text.split_once('\n').unwrap_or((text, ""));
    if header.trim() != HEADER {
        return None;
    }
    // The header is line 1.
    Some(parse_lines(rest, 2))
}

/// The records in any track after track 0
pub fn parse(bytes: &[u8]) -> Result<Vec<Record>, RackError> {
    let text = std::str::from_utf8(bytes).map_err(|_| RackError::NotText)?;
    parse_lines(text, 1)
}

/// A rack's track 0: the header, then the records
pub fn render_first(records: &[Record]) -> Vec<u8> {
    let mut text = format!("{HEADER}\n");
    for record in records {
        text.push_str(&record_line(record));
    }
    text.into_bytes()
}

/// A track after track 0 holding the records
pub fn render(records: &[Record]) -> Vec<u8> {
    let mut text = String::new();
    for record in records {
        text.push_str(&record_line(record));
    }
    text.into_bytes()
}

/// Records packed in order into as few tracks after track 0 as fit `max_bytes` each
pub fn render_tracks(records: &[Record], max_bytes: usize) -> Result<Vec<Vec<u8>>, RackError> {
    let mut tracks = Vec::new();
    let mut text = String::new();
    for record in records {
        let line = record_line(record);
        if line.len() > max_bytes {
            return Err(RackError::RecordLimit { max_bytes });
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

fn parse_lines(text: &str, first_line: usize) -> Result<Vec<Record>, RackError> {
    let mut records = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let record = parse_record(line).map_err(|reason| RackError::InvalidRecord {
            line: first_line + index,
            reason,
        })?;
        records.push(record);
    }
    Ok(records)
}

fn record_line(record: &Record) -> String {
    let mut line = format!("{KIND_TAPE} {}", record.tape);
    for (key, value) in record.metadata() {
        line.push(' ');
        line.push_str(key);
        line.push('=');
        line.push_str(&encode_value(value));
    }
    line.push('\n');
    line
}

fn parse_record(line: &str) -> Result<Record, String> {
    let mut words = line.split_whitespace();
    let kind = words.next().unwrap_or_default();
    if kind != KIND_TAPE {
        return Err(format!("unknown record kind `{kind}`"));
    }
    let address = words
        .next()
        .ok_or_else(|| "expected a tape address".to_string())?;
    let tape: Address = address
        .parse()
        .map_err(|_| format!("`{address}` is not an address"))?;

    let mut record = Record::new(tape);
    for field in words {
        let (key, value) = field
            .split_once('=')
            .ok_or_else(|| format!("`{field}` is not key=value"))?;
        if !is_valid_key(key) {
            return Err(format!("`{key}` is not a metadata key"));
        }
        let value = decode_value(value).ok_or_else(|| format!("`{value}` is not percent-encoded"))?;
        record = record.try_with(key, &value).map_err(|error| error.to_string())?;
    }
    Ok(record)
}

/// Whether a byte stands for itself in an encoded value
fn is_unreserved(byte: u8) -> bool {
    matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~')
}

/// Bytes as text: unreserved bytes stay, every other byte becomes `%XX`
fn encode_value(value: &[u8]) -> String {
    let mut text = String::with_capacity(value.len());
    for byte in value {
        match is_unreserved(*byte) {
            true => text.push(char::from(*byte)),
            false => text.push_str(&format!("%{byte:02X}")),
        }
    }
    text
}

fn decode_value(text: &str) -> Option<Vec<u8>> {
    let mut value = Vec::with_capacity(text.len());
    let mut bytes = text.bytes();
    while let Some(byte) = bytes.next() {
        if byte != b'%' {
            if !is_unreserved(byte) {
                return None;
            }
            value.push(byte);
            continue;
        }
        let high = char::from(bytes.next()?).to_digit(16)?;
        let low = char::from(bytes.next()?).to_digit(16)?;
        value.push(u8::try_from(high * 16 + low).ok()?);
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    // track 0 and later tracks round-trip their records, metadata bytes included
    #[test]
    fn round_trip() {
        let records = vec![
            Record::new(address(1)),
            Record::new(address(2))
                .try_with("name", "photos/2024 summer".as_bytes())
                .and_then(|record| record.try_with("raw", &[0, 37, 255]))
                .expect("metadata"),
        ];

        let first = parse_first(&render_first(&records));
        let later = parse(&render(&records));

        assert_eq!(first, Some(Ok(records.clone())));
        assert_eq!(later, Ok(records));
    }

    // the text is the header, then one readable record per line
    #[test]
    fn readable_text() {
        let record = Record::new(address(1)).try_with("name", b"a b").expect("metadata");

        let text = String::from_utf8(render_first(&[record])).expect("utf-8");

        assert_eq!(text, format!("{HEADER}\ntape {} name=a%20b\n", address(1)));
    }

    // blank lines, crlf endings and lowercase hex are tolerated
    #[test]
    fn loose_text() {
        let text = format!("{HEADER}\r\n\r\n  tape {}  name=a%2fb \r\n", address(4));

        let records = parse_first(text.as_bytes()).expect("declares a rack").expect("parse");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].get("name"), Some(&b"a/b"[..]));
    }

    // only an exact first line declares a rack
    #[test]
    fn declarations() {
        let header_only = format!("{HEADER}\n");
        let versioned = format!("{HEADER} v2\ntape {}\n", address(1));
        let not_first = format!("tape {}\n{HEADER}\n", address(1));
        let bad_record = format!("{HEADER}\ntape nope\n");

        assert_eq!(parse_first(header_only.as_bytes()), Some(Ok(Vec::new())));
        assert_eq!(parse_first(versioned.as_bytes()), None);
        assert_eq!(parse_first(not_first.as_bytes()), None);
        assert_eq!(parse_first(&[0xff, 0xfe]), None);
        assert!(matches!(parse_first(bad_record.as_bytes()), Some(Err(RackError::InvalidRecord { line: 2, .. }))));
    }

    // a header in a later track is not a record, so that track names nothing
    #[test]
    fn later_header() {
        let text = format!("{HEADER}\ntape {}\n", address(1));

        assert!(matches!(parse(text.as_bytes()), Err(RackError::InvalidRecord { line: 1, .. })));
        assert_eq!(parse(&[0xff]), Err(RackError::NotText));
    }

    // malformed records name their line
    #[test]
    fn invalid_records() {
        let records = [
            format!("folder {}\n", address(1)),
            "tape nope\n".to_string(),
            "tape\n".to_string(),
            format!("tape {} name\n", address(1)),
            format!("tape {} Name=x\n", address(1)),
            format!("tape {} name=a/b\n", address(1)),
            format!("tape {} name=%2\n", address(1)),
            format!("tape {} name=a name=b\n", address(1)),
        ];

        for record in records {
            assert!(
                matches!(parse(record.as_bytes()), Err(RackError::InvalidRecord { line: 1, .. })),
                "{record:?}"
            );
        }
    }

    // packing keeps order and never exceeds the byte limit
    #[test]
    fn packed_tracks() {
        let mut records = Vec::new();
        for byte in 1..=200 {
            records.push(Record::new(address(byte)));
        }
        let max_bytes = 3704;

        let tracks = render_tracks(&records, max_bytes).expect("render tracks");

        let mut parsed = Vec::new();
        for track in &tracks {
            assert!(track.len() <= max_bytes);
            parsed.extend(parse(track).expect("parse"));
        }
        assert!(tracks.len() > 1);
        assert_eq!(parsed, records);
    }

    // a limit too small for one record is refused
    #[test]
    fn tiny_limit() {
        assert_eq!(
            render_tracks(&[Record::new(address(1))], 10),
            Err(RackError::RecordLimit { max_bytes: 10 })
        );
    }
}
