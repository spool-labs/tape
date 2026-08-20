//! The bytes a bench hands an engine, since a codec's answer is a property of them
//!
//! Markdown-shaped prose is the content the product actually holds, repetitive
//! the way English is. Pseudorandom bytes are the control: a codec declines
//! them, so a coded row over them has to land on the uncoded one.

/// Pseudorandom bytes, which lz4 declines and stores verbatim
pub fn random(seed: usize, len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    let mut state = 0x9E37_79B9_7F4A_7C15u64 ^ seed as u64;
    while bytes.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes.truncate(len);
    bytes
}

/// The pool a generated page draws its prose from
///
/// Whole words rather than runs of one byte, which any codec would flatter.
const WORDS: &[&str] = &[
    "storage", "node", "committee", "epoch", "slice", "spool", "tape", "track", "record",
    "segment", "engine", "payload", "commitment", "challenge", "operator", "network", "cluster",
    "request", "response", "the", "a", "an", "and", "or", "of", "for", "with", "without", "each",
    "every", "when", "while", "before", "after", "reads", "writes", "holds", "keeps", "answers",
    "returns", "declares", "refuses", "settles", "verifies", "small", "large", "warm", "cold",
];

/// Headings a generated page picks between
const HEADINGS: &[&str] = &[
    "Overview", "Getting started", "Configuration", "Storage layout", "Reading a track",
    "Writing a track", "Failure modes", "Operational notes", "Reference",
];

/// A markdown-shaped page of about `len` bytes, deterministic in `seed`
pub fn markdown(seed: usize, len: usize) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1Du64 ^ (seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    let mut page = String::with_capacity(len + 64);
    page.push_str("# ");
    page.push_str(HEADINGS[seed % HEADINGS.len()]);
    page.push_str("\n\n");

    while page.len() < len {
        match next() % 8 {
            0 => {
                page.push_str("\n## ");
                page.push_str(HEADINGS[(next() % HEADINGS.len() as u64) as usize]);
                page.push_str("\n\n");
            }
            1 | 2 => {
                for _ in 0..3 + next() % 4 {
                    page.push_str("- ");
                    for _ in 0..2 + next() % 5 {
                        page.push_str(WORDS[(next() % WORDS.len() as u64) as usize]);
                        page.push(' ');
                    }
                    page.push('\n');
                }
                page.push('\n');
            }
            _ => {
                for _ in 0..2 + next() % 4 {
                    for word in 0..6 + next() % 10 {
                        if word > 0 {
                            page.push(' ');
                        }
                        page.push_str(WORDS[(next() % WORDS.len() as u64) as usize]);
                    }
                    page.push_str(". ");
                }
                page.push_str("\n\n");
            }
        }
    }

    let mut bytes = page.into_bytes();
    bytes.truncate(len);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    // the generated page is prose-shaped, not a run of one byte
    #[test]
    fn prose_shaped() {
        let page = markdown(7, 16 * 1024);
        assert_eq!(page.len(), 16 * 1024);

        let shrunk = lz4_flex::block::compress(&page).len();
        assert!(shrunk < page.len() - page.len() / 8, "lz4 declined a page: {shrunk} of {}", page.len());
        assert!(shrunk > page.len() / 16, "a page that shrinks this far is not prose: {shrunk}");

        let distinct = page.iter().collect::<std::collections::BTreeSet<_>>().len();
        assert!(distinct > 20, "a page of {distinct} distinct bytes is not text");
    }

    // random bytes defeat the codec, which is what makes them the control
    #[test]
    fn random_control() {
        let bytes = random(7, 16 * 1024);
        let shrunk = lz4_flex::block::compress(&bytes).len();
        assert!(shrunk > bytes.len() - bytes.len() / 8, "random shrank by an eighth: {shrunk}");
    }
}
