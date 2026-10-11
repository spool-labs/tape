//! Request-body readers for streamed (bounded-memory) S3 writes.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::mem::take;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use axum::body::Body;
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use tokio::io::{
    copy, duplex, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
    DuplexStream, ReadBuf,
};
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio_util::io::StreamReader;

use super::sigv4::{constant_time_eq, hmac_sha256, sha256_hex, ChunkVerification, CHUNK_ALGORITHM};

/// In-memory pipe capacity between the body producer and the stream writer.
const PIPE_CAPACITY: usize = 1024 * 1024;

/// Copy buffer size when de-framing or forwarding a chunk's data.
const COPY_BUFFER: usize = 64 * 1024;

/// Cap on one `aws-chunked` header line. A real header
/// (`<hex-size>;chunk-signature=<hex>\r\n`) is well under this, so a longer line
/// is malformed or hostile; bounding the read keeps a never-terminated line from
/// growing the buffer without limit.
const MAX_CHUNK_HEADER: u64 = 8 * 1024;

/// Cap on one signed chunk's payload. A signed chunk is held whole until its
/// signature checks out, and real clients chunk at 64 KiB to a few MiB.
const MAX_SIGNED_CHUNK_BYTES: usize = 32 * 1024 * 1024;

/// Hex SHA-256 of the empty string, the "no extra headers" slot in a chunk's
/// string-to-sign.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// How a request body has to be read to recover the object bytes.
pub enum BodyFraming {
    /// Raw bytes, forwarded verbatim.
    Plain,
    /// `aws-chunked` framing whose chunks carry no signatures, stripped as it
    /// streams. The client declined to sign its body.
    Unsigned,
    /// `aws-chunked` framing with a signature per chunk, checked against the
    /// chain seeded by the request signature.
    Signed(Box<ChunkVerification>),
}

/// Build an AsyncRead yielding the raw object bytes from a request body,
/// de-framing and verifying `aws-chunked` according to `framing`.
pub fn object_reader(body: Body, framing: BodyFraming) -> (DuplexStream, JoinHandle<io::Result<()>>) {
    let framed = StreamReader::new(body.into_data_stream().map_err(io::Error::other));
    let (pipe_reader, pipe_writer) = duplex(PIPE_CAPACITY);

    let handle = spawn(async move {
        match framing {
            BodyFraming::Plain => copy_plain(framed, pipe_writer).await,
            BodyFraming::Unsigned => copy_dechunked(framed, pipe_writer).await,
            BodyFraming::Signed(chain) => copy_verified(framed, pipe_writer, *chain).await,
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

/// The body hashed to a value other than the signed `x-amz-content-sha256`
#[derive(Debug)]
pub struct DigestMismatch;

impl Display for DigestMismatch {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("request body does not match x-amz-content-sha256")
    }
}

impl Error for DigestMismatch {}

/// Whether an I/O error is a [`DigestMismatch`]
pub fn is_digest_mismatch(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|inner| inner.is::<DigestMismatch>())
}

/// A reader that hashes the bytes passing through and fails at end of body
/// when they miss the SHA-256 the request signed
///
/// The stream writer drains the reader before it writes the manifest, so a
/// body that fails here never becomes an object.
pub struct DigestCheck<Reader> {
    inner: Reader,
    /// The signed SHA-256, or `None` when the request signed no body hash
    expected: Option<[u8; 32]>,
    digest: Sha256,
    /// Whether the end of body was reached with a matching digest, once known
    is_match: Option<bool>,
}

impl<Reader> DigestCheck<Reader> {
    pub fn new(inner: Reader, expected: Option<[u8; 32]>) -> Self {
        Self {
            inner,
            expected,
            digest: Sha256::new(),
            is_match: None,
        }
    }
}

impl<Reader: AsyncRead + Unpin> AsyncRead for DigestCheck<Reader> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(expected) = this.expected else {
            return Pin::new(&mut this.inner).poll_read(context, buf);
        };
        if this.is_match == Some(false) {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, DigestMismatch)));
        }

        let start = buf.filled().len();
        let has_room = buf.remaining() > 0;
        ready!(Pin::new(&mut this.inner).poll_read(context, buf))?;
        let read = &buf.filled()[start..];
        if !read.is_empty() {
            this.digest.update(read);
            return Poll::Ready(Ok(()));
        }

        // A read with room that returns nothing is the end of the body.
        if has_room && this.is_match.is_none() {
            let digest: [u8; 32] = take(&mut this.digest).finalize().into();
            this.is_match = Some(digest == expected);
        }
        match this.is_match {
            Some(false) => Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, DigestMismatch))),
            _ => Poll::Ready(Ok(())),
        }
    }
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
        let chunk_size = read_chunk_header(&mut reader, &mut header).await?;
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

/// Strip `aws-chunked` framing from `framed` into `sink`, checking each chunk's
/// signature before any of its bytes are written, then close it.
///
/// The chain starts at the request signature and every chunk signs its
/// predecessor, so a body that fails to verify never reaches the writer.
async fn copy_verified<Reader, Writer>(
    framed: Reader,
    mut sink: Writer,
    chain: ChunkVerification,
) -> io::Result<()>
where
    Reader: AsyncRead + Unpin,
    Writer: AsyncWrite + Unpin,
{
    let mut reader = BufReader::new(framed);
    let mut header = Vec::new();
    let mut previous = chain.seed_signature.clone();

    loop {
        let chunk_size = read_chunk_header(&mut reader, &mut header).await?;
        if chunk_size > MAX_SIGNED_CHUNK_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("aws-chunked chunk exceeds the maximum of {MAX_SIGNED_CHUNK_BYTES} bytes"),
            ));
        }
        let presented = parse_chunk_signature(&header)?;

        // Held whole so a chunk that fails to verify was never forwarded.
        let mut chunk = vec![0u8; chunk_size];
        reader.read_exact(&mut chunk).await?;

        // The chain advances only on a match, so chunks cannot be reordered.
        let expected = chunk_signature(&chain, &previous, &chunk)?;
        if !constant_time_eq(expected.as_bytes(), presented.as_bytes()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "aws-chunked chunk signature does not match",
            ));
        }
        previous = expected;

        if chunk_size == 0 {
            break;
        }
        sink.write_all(&chunk).await?;

        // Each chunk's data is followed by a CRLF before the next chunk header.
        let mut line_ending = [0u8; 2];
        reader.read_exact(&mut line_ending).await?;
    }

    sink.flush().await?;
    sink.shutdown().await?;
    Ok(())
}

/// Read one `aws-chunked` header line into `header` and return its chunk size.
async fn read_chunk_header<Reader>(
    reader: &mut BufReader<Reader>,
    header: &mut Vec<u8>,
) -> io::Result<usize>
where
    Reader: AsyncRead + Unpin,
{
    header.clear();
    let read = reader
        .take(MAX_CHUNK_HEADER)
        .read_until(b'\n', header)
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
    parse_chunk_size(header)
}

/// The `chunk-signature=` value from an `aws-chunked` header line.
fn parse_chunk_signature(header: &[u8]) -> io::Result<String> {
    let line = std::str::from_utf8(header)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 chunk header"))?;
    line.split(';')
        .skip(1)
        .find_map(|extension| extension.trim().strip_prefix("chunk-signature="))
        .map(|signature| signature.trim().to_string())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "signed aws-chunked body has a chunk with no chunk-signature",
            )
        })
}

/// The signature a chunk must carry, given the chain state and its bytes.
fn chunk_signature(
    chain: &ChunkVerification,
    previous: &str,
    chunk: &[u8],
) -> io::Result<String> {
    let string_to_sign = format!(
        "{CHUNK_ALGORITHM}\n{date}\n{scope}\n{previous}\n{EMPTY_SHA256}\n{payload}",
        date = chain.date,
        scope = chain.scope,
        payload = sha256_hex(chunk),
    );
    let signature = hmac_sha256(&chain.signing_key, string_to_sign.as_bytes())
        .map_err(|_| io::Error::other("chunk signature hmac"))?;
    Ok(hex::encode(signature))
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

    fn sha256(data: &[u8]) -> [u8; 32] {
        Sha256::digest(data).into()
    }

    // a body matching the signed hash reads through to a clean end
    #[tokio::test]
    async fn digest_match() {
        let body = vec![7u8; 100_000];
        let mut reader = DigestCheck::new(body.as_slice(), Some(sha256(&body)));
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.expect("matching body");
        assert_eq!(read, body);
    }

    // a body missing the signed hash hands over every byte, then fails at the end
    #[tokio::test]
    async fn digest_mismatch() {
        let body = vec![7u8; 100_000];
        let mut reader = DigestCheck::new(body.as_slice(), Some(sha256(b"other")));
        let mut read = vec![0u8; body.len()];
        reader.read_exact(&mut read).await.expect("bytes before the end");
        assert_eq!(read, body);

        let error = reader.read(&mut [0u8; 1]).await.expect_err("end of body fails");
        assert!(is_digest_mismatch(&error));
        let error = reader.read(&mut [0u8; 1]).await.expect_err("and keeps failing");
        assert!(is_digest_mismatch(&error));
    }

    // a read with no room is no end of body, and no expected hash checks nothing
    #[tokio::test]
    async fn digest_unchecked() {
        let body = b"abc".to_vec();
        let mut reader = DigestCheck::new(body.as_slice(), Some(sha256(b"other")));
        assert_eq!(reader.read(&mut []).await.expect("empty read"), 0);
        let mut read = vec![0u8; body.len()];
        reader.read_exact(&mut read).await.expect("bytes");

        let mut reader = DigestCheck::new(body.as_slice(), None);
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.expect("unsigned body");
        assert_eq!(read, body);
    }

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

    /// A chain whose key is fixed, so a test can sign a body the same way the
    /// verifier will check it.
    fn chain() -> ChunkVerification {
        ChunkVerification {
            signing_key: vec![0x2Au8; 32],
            date: "20260823T000000Z".to_string(),
            scope: "20260823/us-east-1/s3/aws4_request".to_string(),
            seed_signature: "seed".to_string(),
        }
    }

    /// Frame `chunks` the way a compliant client would, signing each one.
    fn signed_body(chain: &ChunkVerification, chunks: &[&[u8]]) -> Vec<u8> {
        let mut previous = chain.seed_signature.clone();
        let mut out = Vec::new();
        for chunk in chunks.iter().chain(std::iter::once(&b"".as_slice())) {
            let signature = chunk_signature(chain, &previous, chunk).expect("sign");
            out.extend_from_slice(
                format!("{:x};chunk-signature={signature}\r\n", chunk.len()).as_bytes(),
            );
            out.extend_from_slice(chunk);
            out.extend_from_slice(b"\r\n");
            previous = signature;
        }
        out
    }

    // the six lines a chunk signature is taken over, spelled out literally
    // because every AWS SDK signs this exact format and a client's body is
    // rejected if it drifts
    #[test]
    fn wire_format() {
        let chain = chain();
        let chunk = b"hello";
        let payload = sha256_hex(chunk);

        let expected_string_to_sign = [
            "AWS4-HMAC-SHA256-PAYLOAD",
            "20260823T000000Z",
            "20260823/us-east-1/s3/aws4_request",
            "seed",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            &payload,
        ]
        .join("\n");

        let expected = hex::encode(
            hmac_sha256(&chain.signing_key, expected_string_to_sign.as_bytes()).expect("hmac"),
        );

        assert_eq!(
            chunk_signature(&chain, &chain.seed_signature, chunk).expect("sign"),
            expected
        );
    }

    // aws-chunked framing is stripped back to the original payload
    #[tokio::test]
    async fn dechunk_roundtrip() {
        let framed = b"5;chunk-signature=aaaa\r\nhello\r\n6;chunk-signature=bbbb\r\n world\r\n0;chunk-signature=cccc\r\n\r\n";

        let mut output = Vec::new();
        copy_dechunked(&framed[..], &mut output).await.expect("dechunk");

        assert_eq!(output, b"hello world");
    }

    // a signed body verifies chunk by chunk and yields the original payload
    #[tokio::test]
    async fn signed_roundtrip() {
        let chain = chain();
        let framed = signed_body(&chain, &[b"hello", b" world"]);

        let mut output = Vec::new();
        copy_verified(&framed[..], &mut output, chain)
            .await
            .expect("dechunk");

        assert_eq!(output, b"hello world");
    }

    // a chunk whose bytes were changed in flight does not verify, and none of
    // its bytes reach the writer
    #[tokio::test]
    async fn tampered_chunk_is_refused() {
        let chain = chain();
        let mut framed = signed_body(&chain, &[b"hello", b" world"]);
        let at = framed
            .windows(5)
            .position(|window| window == b"hello")
            .expect("payload");
        framed[at..at + 5].copy_from_slice(b"HELLO");

        let mut output = Vec::new();
        let result = copy_verified(&framed[..], &mut output, chain).await;

        assert!(result.is_err());
        assert!(output.is_empty(), "unverified bytes must not be forwarded");
    }

    // a body signed with a different key does not verify
    #[tokio::test]
    async fn wrong_key_is_refused() {
        let signer = chain();
        let framed = signed_body(&signer, &[b"hello"]);
        let verifier = ChunkVerification { signing_key: vec![0x99u8; 32], ..chain() };

        let mut output = Vec::new();
        let result = copy_verified(&framed[..], &mut output, verifier).await;

        assert!(result.is_err());
        assert!(output.is_empty());
    }

    // chunks are a chain: replaying a later chunk earlier does not verify
    #[tokio::test]
    async fn reordered_chunks_are_refused() {
        let chain = chain();
        let first = chunk_signature(&chain, &chain.seed_signature, b"hello").expect("sign");
        let second = chunk_signature(&chain, &first, b" world").expect("sign");

        // The second chunk's own signature, presented in the first position.
        let mut framed = Vec::new();
        framed.extend_from_slice(format!("6;chunk-signature={second}\r\n").as_bytes());
        framed.extend_from_slice(b" world\r\n");

        let mut output = Vec::new();
        let result = copy_verified(&framed[..], &mut output, chain).await;

        assert!(result.is_err());
        assert!(output.is_empty());
    }

    // a signed body whose chunk carries no signature at all is refused
    #[tokio::test]
    async fn missing_chunk_signature() {
        let framed = b"5\r\nhello\r\n0\r\n\r\n";

        let mut output = Vec::new();
        let result = copy_verified(&framed[..], &mut output, chain()).await;

        assert!(result.is_err());
    }

    // a chunk larger than the cap is refused before it is read into memory
    #[tokio::test]
    async fn oversized_chunk() {
        let framed = format!("{:x};chunk-signature=aa\r\n", MAX_SIGNED_CHUNK_BYTES + 1);

        let mut output = Vec::new();
        let result = copy_verified(framed.as_bytes(), &mut output, chain()).await;

        assert!(result.is_err());
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
