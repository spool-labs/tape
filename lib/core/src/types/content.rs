//! Typed content-type values.

use num_enum::{IntoPrimitive, TryFromPrimitive};

#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub enum ContentType {
    Unknown = 0,

    // Image formats.
    ImagePng,
    ImageJpeg,
    ImageGif,
    ImageWebp,
    ImageBmp,
    ImageTiff,

    // Document formats.
    ApplicationPdf,
    ApplicationMsword,
    ApplicationDocx,
    ApplicationOdt,

    // Text formats.
    TextPlain,
    TextHtml,
    TextCss,
    TextJavascript,
    TextCsv,
    TextMarkdown,

    // Audio formats.
    AudioMpeg,
    AudioWav,
    AudioOgg,
    AudioFlac,

    // Video formats.
    VideoMp4,
    VideoWebm,
    VideoMpeg,
    VideoAvi,

    // Application formats.
    ApplicationJson,
    ApplicationXml,
    ApplicationZip,
    ApplicationGzip,
    ApplicationTar,

    // Font formats.
    FontWoff,
    FontWoff2,
    FontTtf,
    FontOtf,

    // Miscellaneous formats.
    ApplicationRtf,
    ApplicationSql,
    ApplicationYaml,
}

impl ContentType {
    pub fn from_str(content_type: &str) -> Self {
        let content_type = content_type.split(';').next().unwrap_or("").trim();
        let Some((media_type, subtype)) = content_type.split_once('/') else {
            return Self::Unknown;
        };

        let media_type = media_type.trim().to_ascii_lowercase();
        let subtype = subtype.trim().to_ascii_lowercase();

        match (media_type.as_str(), subtype.as_str()) {
            // Image formats.
            ("image", "png") => Self::ImagePng,
            ("image", "jpeg") | ("image", "jpg") => Self::ImageJpeg,
            ("image", "gif") => Self::ImageGif,
            ("image", "webp") => Self::ImageWebp,
            ("image", "bmp") => Self::ImageBmp,
            ("image", "tiff") | ("image", "tif") => Self::ImageTiff,

            // Document formats.
            ("application", "pdf") => Self::ApplicationPdf,
            ("application", "msword") => Self::ApplicationMsword,
            ("application", "vnd.openxmlformats-officedocument.wordprocessingml.document") => {
                Self::ApplicationDocx
            }
            ("application", "vnd.oasis.opendocument.text") => Self::ApplicationOdt,

            // Text formats.
            ("text", "plain") => Self::TextPlain,
            ("text", "html") => Self::TextHtml,
            ("text", "css") => Self::TextCss,
            ("text", "javascript") | ("application", "javascript") => Self::TextJavascript,
            ("text", "csv") => Self::TextCsv,
            ("text", "markdown") | ("text", "md") => Self::TextMarkdown,

            // Audio formats.
            ("audio", "mpeg") | ("audio", "mp3") => Self::AudioMpeg,
            ("audio", "wav") => Self::AudioWav,
            ("audio", "ogg") => Self::AudioOgg,
            ("audio", "flac") => Self::AudioFlac,

            // Video formats.
            ("video", "mp4") => Self::VideoMp4,
            ("video", "webm") => Self::VideoWebm,
            ("video", "mpeg") => Self::VideoMpeg,
            ("video", "x-msvideo") | ("video", "avi") => Self::VideoAvi,

            // Application formats.
            ("application", "json") => Self::ApplicationJson,
            ("application", "xml") | ("text", "xml") => Self::ApplicationXml,
            ("application", "zip") => Self::ApplicationZip,
            ("application", "gzip") | ("application", "x-gzip") => Self::ApplicationGzip,
            ("application", "x-tar") | ("application", "tar") => Self::ApplicationTar,

            // Font formats.
            ("font", "woff") => Self::FontWoff,
            ("font", "woff2") => Self::FontWoff2,
            ("font", "ttf") | ("application", "font-sfnt") => Self::FontTtf,
            ("font", "otf") => Self::FontOtf,

            // Miscellaneous formats.
            ("application", "rtf") => Self::ApplicationRtf,
            ("application", "sql") => Self::ApplicationSql,
            ("application", "x-yaml") | ("text", "yaml") => Self::ApplicationYaml,
            _ => Self::Unknown,
        }
    }

    pub fn to_str(self) -> &'static str {
        match self {
            // Fallback type.
            Self::Unknown => "application/octet-stream",

            // Image formats.
            Self::ImagePng => "image/png",
            Self::ImageJpeg => "image/jpeg",
            Self::ImageGif => "image/gif",
            Self::ImageWebp => "image/webp",
            Self::ImageBmp => "image/bmp",
            Self::ImageTiff => "image/tiff",

            // Document formats.
            Self::ApplicationPdf => "application/pdf",
            Self::ApplicationMsword => "application/msword",
            Self::ApplicationDocx => {
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            }
            Self::ApplicationOdt => "application/vnd.oasis.opendocument.text",

            // Text formats.
            Self::TextPlain => "text/plain",
            Self::TextHtml => "text/html",
            Self::TextCss => "text/css",
            Self::TextJavascript => "text/javascript",
            Self::TextCsv => "text/csv",
            Self::TextMarkdown => "text/markdown",

            // Audio formats.
            Self::AudioMpeg => "audio/mpeg",
            Self::AudioWav => "audio/wav",
            Self::AudioOgg => "audio/ogg",
            Self::AudioFlac => "audio/flac",

            // Video formats.
            Self::VideoMp4 => "video/mp4",
            Self::VideoWebm => "video/webm",
            Self::VideoMpeg => "video/mpeg",
            Self::VideoAvi => "video/x-msvideo",

            // Application formats.
            Self::ApplicationJson => "application/json",
            Self::ApplicationXml => "application/xml",
            Self::ApplicationZip => "application/zip",
            Self::ApplicationGzip => "application/gzip",
            Self::ApplicationTar => "application/x-tar",

            // Font formats.
            Self::FontWoff => "font/woff",
            Self::FontWoff2 => "font/woff2",
            Self::FontTtf => "font/ttf",
            Self::FontOtf => "font/otf",

            // Miscellaneous formats.
            Self::ApplicationRtf => "application/rtf",
            Self::ApplicationSql => "application/sql",
            Self::ApplicationYaml => "application/x-yaml",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            // Fallback type.
            Self::Unknown => "bin",

            // Image formats.
            Self::ImagePng => "png",
            Self::ImageJpeg => "jpg",
            Self::ImageGif => "gif",
            Self::ImageWebp => "webp",
            Self::ImageBmp => "bmp",
            Self::ImageTiff => "tiff",

            // Document formats.
            Self::ApplicationPdf => "pdf",
            Self::ApplicationMsword => "doc",
            Self::ApplicationDocx => "docx",
            Self::ApplicationOdt => "odt",

            // Text formats.
            Self::TextPlain => "txt",
            Self::TextHtml => "html",
            Self::TextCss => "css",
            Self::TextJavascript => "js",
            Self::TextCsv => "csv",
            Self::TextMarkdown => "md",

            // Audio formats.
            Self::AudioMpeg => "mp3",
            Self::AudioWav => "wav",
            Self::AudioOgg => "ogg",
            Self::AudioFlac => "flac",

            // Video formats.
            Self::VideoMp4 => "mp4",
            Self::VideoWebm => "webm",
            Self::VideoMpeg => "mpeg",
            Self::VideoAvi => "avi",

            // Application formats.
            Self::ApplicationJson => "json",
            Self::ApplicationXml => "xml",
            Self::ApplicationZip => "zip",
            Self::ApplicationGzip => "gz",
            Self::ApplicationTar => "tar",

            // Font formats.
            Self::FontWoff => "woff",
            Self::FontWoff2 => "woff2",
            Self::FontTtf => "ttf",
            Self::FontOtf => "otf",

            // Miscellaneous formats.
            Self::ApplicationRtf => "rtf",
            Self::ApplicationSql => "sql",
            Self::ApplicationYaml => "yaml",
        }
    }
}

impl core::fmt::Display for ContentType {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(self.to_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // known mime strings parse to their typed values
    #[test]
    fn parse_known() {
        assert_eq!(ContentType::from_str("image/png"), ContentType::ImagePng);
        assert_eq!(ContentType::from_str("image/jpg"), ContentType::ImageJpeg);
        assert_eq!(ContentType::from_str("text/javascript"), ContentType::TextJavascript);
        assert_eq!(
            ContentType::from_str("application/javascript"),
            ContentType::TextJavascript
        );
        assert_eq!(ContentType::from_str("text/xml"), ContentType::ApplicationXml);
        assert_eq!(
            ContentType::from_str("application/x-yaml"),
            ContentType::ApplicationYaml
        );
    }

    // parameters and casing are ignored during parsing
    #[test]
    fn parse_normalized() {
        assert_eq!(
            ContentType::from_str("IMAGE/JPEG; charset=utf-8"),
            ContentType::ImageJpeg
        );
    }

    // content types expose their default file extension
    #[test]
    fn extension() {
        assert_eq!(ContentType::ImageJpeg.extension(), "jpg");
        assert_eq!(ContentType::ApplicationGzip.extension(), "gz");
        assert_eq!(ContentType::Unknown.extension(), "bin");
    }
}
