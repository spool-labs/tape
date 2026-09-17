//! Turning read content into moderation input: a whole image or text segments

use base64::engine::general_purpose::STANDARD;
use base64::Engine;

use crate::worker::gateway::ContentRead;

/// The largest image sent whole
pub const IMAGE_MAX_BYTES: u64 = 20 * 1_024 * 1_024;

pub const TEXT_SEGMENT_CHARACTERS: usize = 8_000;

pub const TEXT_SEGMENT_COUNT: usize = 8;

const UTF8_MAX_BYTES_PER_CHARACTER: u64 = 4;

/// Enough bytes for every text segment even at four bytes per character
pub const TEXT_READ_BYTES: u64 =
    (TEXT_SEGMENT_CHARACTERS * TEXT_SEGMENT_COUNT) as u64 * UTF8_MAX_BYTES_PER_CHARACTER;

const UNTYPED_MEDIA_TYPE: &str = "application/octet-stream";

const TEXT_APPLICATION_TYPES: [&str; 2] = ["application/json", "application/xml"];

const PNG_MAGIC: &[u8] = b"\x89PNG\r\n\x1a\n";
const JPEG_MAGIC: &[u8] = b"\xff\xd8\xff";
const GIF87_MAGIC: &[u8] = b"GIF87a";
const GIF89_MAGIC: &[u8] = b"GIF89a";
const RIFF_MAGIC: &[u8] = b"RIFF";
const WEBP_MAGIC: &[u8] = b"WEBP";
const WEBP_MAGIC_OFFSET: usize = 8;

/// The kind of input a moderation category applies to
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum InputType {
    Text,
    Image,
}

/// Image formats the moderation API accepts
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ImageFormat {
    Png,
    Jpeg,
    Gif,
    Webp,
}

/// Content ready to send, or why it cannot be
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedInput {
    Text {
        segments: Vec<String>,
        /// Every character of the content is in the segments
        is_complete: bool,
    },
    Image {
        data_url: String,
    },
    Unsupported {
        reason: String,
    },
}

impl InputType {
    /// The name the API uses in `category_applied_input_types`
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Image => "image",
        }
    }
}

impl ImageFormat {
    pub fn mime_type(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Gif => "image/gif",
            Self::Webp => "image/webp",
        }
    }
}

/// An image format recognized by its magic bytes
pub fn image_format(bytes: &[u8]) -> Option<ImageFormat> {
    let is_webp = bytes.starts_with(RIFF_MAGIC)
        && bytes
            .get(WEBP_MAGIC_OFFSET..WEBP_MAGIC_OFFSET + WEBP_MAGIC.len())
            .is_some_and(|magic| magic == WEBP_MAGIC);
    match () {
        () if bytes.starts_with(PNG_MAGIC) => Some(ImageFormat::Png),
        () if bytes.starts_with(JPEG_MAGIC) => Some(ImageFormat::Jpeg),
        () if bytes.starts_with(GIF87_MAGIC) || bytes.starts_with(GIF89_MAGIC) => Some(ImageFormat::Gif),
        () if is_webp => Some(ImageFormat::Webp),
        () => None,
    }
}

/// Prepare content for the moderation API
pub fn prepare(read: ContentRead) -> PreparedInput {
    if let Some(format) = image_format(&read.bytes) {
        return prepare_image(read, format);
    }
    if !is_textual(read.content_type.as_deref()) {
        let content_type = read.content_type.unwrap_or_default();
        return unsupported(format!("content type `{content_type}` is neither text nor a supported image"));
    }
    prepare_text(read)
}

fn prepare_image(read: ContentRead, format: ImageFormat) -> PreparedInput {
    if !read.is_complete() || read.bytes.len() as u64 > IMAGE_MAX_BYTES {
        return unsupported(format!("image is not readable whole within {IMAGE_MAX_BYTES} bytes"));
    }
    let encoded = STANDARD.encode(&read.bytes);
    PreparedInput::Image {
        data_url: format!("data:{};base64,{encoded}", format.mime_type()),
    }
}

fn prepare_text(read: ContentRead) -> PreparedInput {
    let is_read_whole = read.is_complete();
    let text = match std::str::from_utf8(&read.bytes) {
        Ok(text) => text,
        // A prefix read can end partway through a character
        Err(error) if !is_read_whole && error.error_len().is_none() => {
            std::str::from_utf8(&read.bytes[..error.valid_up_to()]).unwrap_or_default()
        }
        Err(_) => return unsupported("content is not utf-8 text".to_string()),
    };
    if text.is_empty() {
        return unsupported("content is empty".to_string());
    }

    let mut characters = text.chars();
    let mut segments = Vec::new();
    while segments.len() < TEXT_SEGMENT_COUNT {
        let segment: String = characters.by_ref().take(TEXT_SEGMENT_CHARACTERS).collect();
        if segment.is_empty() {
            break;
        }
        segments.push(segment);
    }
    let has_unsent = characters.next().is_some();

    PreparedInput::Text {
        segments,
        is_complete: is_read_whole && !has_unsent,
    }
}

fn is_textual(content_type: Option<&str>) -> bool {
    let Some(content_type) = content_type else {
        return true;
    };
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media_type.is_empty()
        || media_type == UNTYPED_MEDIA_TYPE
        || media_type.starts_with("text/")
        || TEXT_APPLICATION_TYPES.contains(&media_type.as_str())
        || media_type.ends_with("+json")
        || media_type.ends_with("+xml")
}

fn unsupported(reason: String) -> PreparedInput {
    PreparedInput::Unsupported { reason }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(bytes: &[u8], content_type: Option<&str>, total: u64) -> ContentRead {
        ContentRead {
            bytes: bytes.to_vec(),
            content_type: content_type.map(str::to_string),
            total_bytes: Some(total),
        }
    }

    fn image_url(prepared: PreparedInput) -> Option<String> {
        match prepared {
            PreparedInput::Image { data_url } => Some(data_url),
            PreparedInput::Text { .. } | PreparedInput::Unsupported { .. } => None,
        }
    }

    fn text_shape(prepared: PreparedInput) -> Option<(usize, bool)> {
        match prepared {
            PreparedInput::Text { segments, is_complete } => Some((segments.len(), is_complete)),
            PreparedInput::Image { .. } | PreparedInput::Unsupported { .. } => None,
        }
    }

    // each supported format is recognized by its magic bytes alone
    #[test]
    fn magic_bytes() {
        assert_eq!(image_format(b"\x89PNG\r\n\x1a\nrest"), Some(ImageFormat::Png));
        assert_eq!(image_format(b"\xff\xd8\xff\xe0"), Some(ImageFormat::Jpeg));
        assert_eq!(image_format(b"GIF89a..."), Some(ImageFormat::Gif));
        assert_eq!(image_format(b"RIFF\0\0\0\0WEBPVP8 "), Some(ImageFormat::Webp));
        assert_eq!(image_format(b"RIFF\0\0\0\0WAVEfmt "), None);
        assert_eq!(image_format(b"hello"), None);
    }

    // a whole image becomes a data url whatever its declared type
    #[test]
    fn whole_image() {
        let bytes = b"\x89PNG\r\n\x1a\npixels";

        let prepared = prepare(read(bytes, Some("application/octet-stream"), bytes.len() as u64));

        let data_url = image_url(prepared).expect("an image");
        assert!(data_url.starts_with("data:image/png;base64,"));
    }

    // an image read only in part is never sent
    #[test]
    fn partial_image() {
        let bytes = b"\x89PNG\r\n\x1a\npixels";

        let prepared = prepare(read(bytes, None, IMAGE_MAX_BYTES + 1));

        assert!(matches!(prepared, PreparedInput::Unsupported { .. }));
    }

    // text splits into segments and is incomplete past the last one
    #[test]
    fn segments() {
        let short = "a".repeat(TEXT_SEGMENT_CHARACTERS + 1);
        let long = "b".repeat(TEXT_SEGMENT_CHARACTERS * TEXT_SEGMENT_COUNT + 1);

        let short = prepare(read(short.as_bytes(), Some("text/plain"), short.len() as u64));
        let long = prepare(read(long.as_bytes(), None, long.len() as u64));

        assert_eq!(text_shape(short), Some((2, true)));
        assert_eq!(text_shape(long), Some((TEXT_SEGMENT_COUNT, false)));
    }

    // a prefix cut inside a character keeps the valid text and is incomplete
    #[test]
    fn cut_character() {
        let text = "héllo".as_bytes();

        let prepared = prepare(read(&text[..2], Some("text/plain; charset=utf-8"), text.len() as u64));

        assert_eq!(
            prepared,
            PreparedInput::Text {
                segments: vec!["h".to_string()],
                is_complete: false,
            }
        );
    }

    // binary types, invalid utf-8 and empty content are unsupported
    #[test]
    fn unsupported_content() {
        let pdf = prepare(read(b"%PDF-1.7", Some("application/pdf"), 8));
        let binary = prepare(read(&[0xff, 0xfe, 0x00], None, 3));
        let empty = prepare(read(b"", Some("text/plain"), 0));

        for prepared in [pdf, binary, empty] {
            assert!(matches!(prepared, PreparedInput::Unsupported { .. }));
        }
    }

    // json and xml types count as text
    #[test]
    fn textual_types() {
        assert!(is_textual(Some("application/json")));
        assert!(is_textual(Some("application/ld+json; charset=utf-8")));
        assert!(is_textual(Some("image/svg+xml")));
        assert!(!is_textual(Some("video/mp4")));
    }
}
