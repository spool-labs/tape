use mime::Mime;
use tape_client::MimeType;

/// Returns the file extension for a given MIME type.
pub fn get_extension(mime_type: MimeType) -> Option<&'static str> {
    match mime_type {
        // Image formats
        MimeType::ImagePng => Some("png"),
        MimeType::ImageJpeg => Some("jpg"),
        MimeType::ImageGif => Some("gif"),
        MimeType::ImageWebp => Some("webp"),
        MimeType::ImageBmp => Some("bmp"),
        MimeType::ImageTiff => Some("tiff"),

        // Document formats
        MimeType::ApplicationPdf => Some("pdf"),
        MimeType::ApplicationMsword => Some("doc"),
        MimeType::ApplicationDocx => Some("docx"),
        MimeType::ApplicationOdt => Some("odt"),

        // Text formats
        MimeType::TextPlain => Some("txt"),
        MimeType::TextHtml => Some("html"),
        MimeType::TextCss => Some("css"),
        MimeType::TextJavascript => Some("js"),
        MimeType::TextCsv => Some("csv"),
        MimeType::TextMarkdown => Some("md"),

        // Audio formats
        MimeType::AudioMpeg => Some("mp3"),
        MimeType::AudioWav => Some("wav"),
        MimeType::AudioOgg => Some("ogg"),
        MimeType::AudioFlac => Some("flac"),

        // Video formats
        MimeType::VideoMp4 => Some("mp4"),
        MimeType::VideoWebm => Some("webm"),
        MimeType::VideoMpeg => Some("mpeg"),
        MimeType::VideoAvi => Some("avi"),

        // Application formats
        MimeType::ApplicationJson => Some("json"),
        MimeType::ApplicationXml => Some("xml"),
        MimeType::ApplicationZip => Some("zip"),
        MimeType::ApplicationGzip => Some("gz"),
        MimeType::ApplicationTar => Some("tar"),

        // Font formats
        MimeType::FontWoff => Some("woff"),
        MimeType::FontWoff2 => Some("woff2"),
        MimeType::FontTtf => Some("ttf"),
        MimeType::FontOtf => Some("otf"),

        // Miscellaneous
        MimeType::ApplicationRtf => Some("rtf"),
        MimeType::ApplicationSql => Some("sql"),
        MimeType::ApplicationYaml => Some("yaml"),

        MimeType::Unknown => Some("bin"),
    }
}

/// Returns default octet-stream MIME type.
pub fn default_octet() -> Mime {
    "application/octet-stream".parse().unwrap()
}

/// Converts a `Mime` type to a `MimeType` enum.
pub fn mime_to_type(mime: &Mime) -> MimeType {
    let t = mime.type_().as_str().to_ascii_lowercase();
    let s = mime.subtype().as_str().to_ascii_lowercase();

    match (t.as_str(), s.as_str()) {
        // Image formats
        ("image", "png") => MimeType::ImagePng,
        ("image", "jpeg") | ("image", "jpg") => MimeType::ImageJpeg,
        ("image", "gif") => MimeType::ImageGif,
        ("image", "webp") => MimeType::ImageWebp,
        ("image", "bmp") => MimeType::ImageBmp,
        ("image", "tiff") | ("image", "tif") => MimeType::ImageTiff,

        // Document formats
        ("application", "pdf") => MimeType::ApplicationPdf,
        ("application", "msword") => MimeType::ApplicationMsword,
        ("application", "vnd.openxmlformats-officedocument.wordprocessingml.document") => {
            MimeType::ApplicationDocx
        }
        ("application", "vnd.oasis.opendocument.text") => MimeType::ApplicationOdt,

        // Text formats
        ("text", "plain") => MimeType::TextPlain,
        ("text", "html") => MimeType::TextHtml,
        ("text", "css") => MimeType::TextCss,
        ("text", "javascript") | ("application", "javascript") => MimeType::TextJavascript,
        ("text", "csv") => MimeType::TextCsv,
        ("text", "markdown") | ("text", "md") => MimeType::TextMarkdown,

        // Audio formats
        ("audio", "mpeg") | ("audio", "mp3") => MimeType::AudioMpeg,
        ("audio", "wav") => MimeType::AudioWav,
        ("audio", "ogg") => MimeType::AudioOgg,
        ("audio", "flac") => MimeType::AudioFlac,

        // Video formats
        ("video", "mp4") => MimeType::VideoMp4,
        ("video", "webm") => MimeType::VideoWebm,
        ("video", "mpeg") => MimeType::VideoMpeg,
        ("video", "x-msvideo") | ("video", "avi") => MimeType::VideoAvi,

        // Application formats
        ("application", "json") => MimeType::ApplicationJson,
        ("application", "xml") | ("text", "xml") => MimeType::ApplicationXml,
        ("application", "zip") => MimeType::ApplicationZip,
        ("application", "gzip") | ("application", "x-gzip") => MimeType::ApplicationGzip,
        ("application", "x-tar") | ("application", "tar") => MimeType::ApplicationTar,

        // Font formats
        ("font", "woff") => MimeType::FontWoff,
        ("font", "woff2") => MimeType::FontWoff2,
        ("font", "ttf") | ("application", "font-sfnt") => MimeType::FontTtf,
        ("font", "otf") => MimeType::FontOtf,

        // Miscellaneous
        ("application", "rtf") => MimeType::ApplicationRtf,
        ("application", "sql") => MimeType::ApplicationSql,
        ("application", "x-yaml") | ("text", "yaml") => MimeType::ApplicationYaml,

        _ => MimeType::Unknown,
    }
}
