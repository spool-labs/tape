//! Request-body readers for streamed (bounded-memory) S3 writes.

use std::io;

use axum::body::Body;
use futures::TryStreamExt;
use tokio::io::{
    copy, duplex, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
    DuplexStream,
};
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio_util::io::StreamReader;

/// In-memory pipe capacity between the body producer and the stream writer.
const PIPE_CAPACITY: usize = 1024 * 1024;

/// Copy buffer size when de-framing or forwarding a chunk's data.
const COPY_BUFFER: usize = 64 * 1024;

/// Cap on one `aws-chunked` header line. A real header
/// (`<hex-size>;chunk-signature=<hex>\r\n`) is well under this, so a longer line
/// is malformed or hostile; bounding the read keeps a never-terminated line from
/// growing the buffer without limit.
const MAX_CHUNK_HEADER: u64 = 8 * 1024;

/// Build an AsyncRead yielding the raw object bytes from a request body,
/// de-framing `aws-chunked` when `is_aws_chunked` is set.
pub fn object_reader(body: Body, is_aws_chunked: bool) -> (DuplexStream, JoinHandle<io::Result<()>>) {
    let framed = StreamReader::new(body.into_data_stream().map_err(io::Error::other));
    let (pipe_reader, pipe_writer) = duplex(PIPE_CAPACITY);

    let handle = spawn(async move {
        if is_aws_chunked {
            copy_dechunked(framed, pipe_writer).await
        } else {
            copy_plain(framed, pipe_writer).await
        }
    });

    (pipe_reader, handle)
}

/// Forward a plain body verbatim into `sink`, then close it.
async fn copy_plain<Reader, Writer>(mut framed: Reader, mut sink: Writer) -> io::Result<()>
where
    Reader: AsyncRead + Unpin,
    Writer: AsyncWrite + Unpin,
{
    copy(&mut framed, &mut sink).await?;
    sink.shutdown().await?;
    Ok(())
}

/// Strip `aws-chunked` framing from `framed`, writing the de-framed bytes into
/// `sink`, then close it.
async fn copy_dechunked<Reader, Writer>(framed: Reader, mut sink: Writer) -> io::Result<()>
where
    Reader: AsyncRead + Unpin,
    Writer: AsyncWrite + Unpin,
{
    let mut reader = BufReader::new(framed);
    let mut header = Vec::new();
    let mut buffer = [0u8; COPY_BUFFER];

    loop {
        header.clear();
        let read = (&mut reader)
            .take(MAX_CHUNK_HEADER)
            .read_until(b'\n', &mut header)
            .await?;
        if read == 0 {
            // A well-formed body ends with an explicit zero-size chunk, so an EOF
            // here means the stream was truncated before its terminator.
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "aws-chunked body ended before its terminating chunk",
            ));
        }
        if !header.ends_with(b"\n") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "aws-chunked header line exceeds the maximum length",
            ));
        }

        let chunk_size = parse_chunk_size(&header)?;
        if chunk_size == 0 {
            break;
        }

        let mut remaining = chunk_size;
        while remaining > 0 {
            let want = remaining.min(buffer.len());
            reader.read_exact(&mut buffer[..want]).await?;
            sink.write_all(&buffer[..want]).await?;
            remaining -= want;
        }

        // Each chunk's data is followed by a CRLF before the next chunk header.
        let mut line_ending = [0u8; 2];
        reader.read_exact(&mut line_ending).await?;
    }

    sink.flush().await?;
    sink.shutdown().await?;
    Ok(())
}

/// Parse the hex chunk size from an `aws-chunked` header line
///
/// The line is `<hex-size>[;chunk-signature=...]\r\n`; the size is the hex digits
/// before the first `;` or CR.
fn parse_chunk_size(header: &[u8]) -> io::Result<usize> {
    let size_field = header
        .split(|&byte| byte == b';' || byte == b'\r' || byte == b'\n')
        .next()
        .unwrap_or(&[]);
    let text = std::str::from_utf8(size_field)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 chunk size"))?;
    usize::from_str_radix(text.trim(), 16)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk size"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // hex chunk sizes parse, with or without a chunk-signature extension
    #[test]
    fn chunk_size() {
        assert_eq!(parse_chunk_size(b"1000\r\n").expect("size"), 0x1000);
        assert_eq!(
            parse_chunk_size(b"a;chunk-signature=deadbeef\r\n").expect("size"),
            10
        );
        assert_eq!(parse_chunk_size(b"0\r\n").expect("size"), 0);
    }

    // a malformed chunk size is rejected
    #[test]
    fn bad_chunk_size() {
        assert!(parse_chunk_size(b"zz\r\n").is_err());
    }

    // aws-chunked framing is stripped back to the original payload
    #[tokio::test]
    async fn dechunk_roundtrip() {
        let framed = b"5;chunk-signature=aaaa\r\nhello\r\n6;chunk-signature=bbbb\r\n world\r\n0;chunk-signature=cccc\r\n\r\n";

        let mut output = Vec::new();
        copy_dechunked(&framed[..], &mut output).await.expect("dechunk");

        assert_eq!(output, b"hello world");
    }

    // a body that ends before its terminating chunk is rejected, not silently accepted
    #[tokio::test]
    async fn truncated_body() {
        let framed = b"5;chunk-signature=aaaa\r\nhello\r\n";

        let mut output = Vec::new();
        let result = copy_dechunked(&framed[..], &mut output).await;

        assert!(result.is_err());
    }

    // a chunk header with no newline within the cap is rejected
    #[tokio::test]
    async fn oversized_header() {
        let framed = vec![b'a'; (MAX_CHUNK_HEADER as usize) + 16];

        let mut output = Vec::new();
        let result = copy_dechunked(&framed[..], &mut output).await;

        assert!(result.is_err());
    }

    // a plain body is forwarded unchanged
    #[tokio::test]
    async fn plain_passthrough() {
        let input = b"raw object bytes";

        let mut output = Vec::new();
        copy_plain(&input[..], &mut output).await.expect("copy");

        assert_eq!(output, input);
    }
}
