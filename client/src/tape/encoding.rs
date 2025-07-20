use anyhow::{Result, anyhow};
use log::debug;
use crate::utils::*;
use super::{TapeHeader, TapeFlags, CompressionAlgo};
use tape_api::prelude::*;
use std::collections::HashSet;

/// Encodes data into a tape format, applying compression if specified in the header.
pub fn encode_tape(data: &[u8], header: &mut TapeHeader) -> Result<Vec<u8>> {

    let compression_algo = CompressionAlgo::try_from(header.compression)
        .map_err(|_| anyhow!("Invalid compression algorithm"))?;

    let processed = match compression_algo {
        CompressionAlgo::None => Ok(data.to_vec()),
        CompressionAlgo::Gzip => compress(data),
    }?;

    // Add encryption, etc...
    header.data_len = processed.len() as u64;

    let output = if header.flags & (TapeFlags::Prefixed as u8) != 0 {
        prefix_segments(&processed)
    } else {
        processed
    };

    Ok(output)
}

/// Decodes a tape format into raw data, decompressing if necessary based on the header.
pub fn decode_tape(data: Vec<u8>, header: &TapeHeader) -> Result<Vec<u8>> {
    let processed = if header.flags & (TapeFlags::Prefixed as u8) != 0 {
        unprefix_segments(data, header.data_len as usize)?
    } else {
        data
    };

    // Add decryption, etc...

    let compression_algo = CompressionAlgo::try_from(header.compression)
        .map_err(|_| anyhow!("Invalid compression algorithm"))?;

    let decompressed = match compression_algo {
        CompressionAlgo::None => Ok(processed),
        CompressionAlgo::Gzip => decompress(&processed),
    }?;

    Ok(decompressed)
}

/// Splits data into segments of fixed size, prefixing each segment with its index.
pub fn prefix_segments(data: &[u8]) -> Vec<u8> {
    let chunks : Vec<_> = data
        .chunks(SEGMENT_SIZE - 8)
        .map(|c| c.to_vec())
        .collect();

    let mut output = Vec::with_capacity(data.len() + chunks.len() * 8);

    for (i, chunk) in chunks.iter().enumerate() {
        let seg_num = (i as u64).to_be_bytes();
        output.extend_from_slice(&seg_num);
        output.extend_from_slice(chunk);
    }

    output
}

/// Unprefixes segments from a prefixed data vector, ensuring segments are consecutive and starting
/// from 0.
pub fn unprefix_segments(data: Vec<u8>, data_length: usize) -> Result<Vec<u8>> {
    let mut segments: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut seen = HashSet::new();

    for chunk in data.chunks(SEGMENT_SIZE) {
        if chunk.len() < 8 {
            return Err(anyhow!("Invalid segment size: too small"));
        }

        let seg_num_bytes: [u8; 8] = chunk[0..8].try_into().map_err(|_| anyhow!("Failed to read segment number"))?;
        let seg_num = u64::from_be_bytes(seg_num_bytes);

        let seg_data = chunk[8..].to_vec();

        if seen.insert(seg_num) {
            segments.push((seg_num, seg_data));
        }
        // If duplicate, skip assuming they are identical; no check for differences here
    }

    // sort by segment number
    segments.sort_by_key(|(num, _)| *num);

    // Check for consecutive segments starting from 0 with no gaps
    if !segments.is_empty() {
        if segments[0].0 != 0 {
            return Err(anyhow!("Segments do not start from 0"));
        }
        for i in 0..segments.len() - 1 {
            if segments[i + 1].0 != segments[i].0 + 1 {
                debug!("Segment {} is not consecutive with segment {}", segments[i].0, segments[i + 1].0);
                return Err(anyhow!("Non-consecutive segments detected"));
            }
        }
    }

    // merge segments without prefix
    let mut output = Vec::with_capacity(data_length);
    for (_, seg_data) in segments {
        output.extend(seg_data);
    }

    if output.len() > data_length {
        // TODO: this seems wrong... we should probably find a different solution, the issue is
        // outside the decode_tape function, where we merged the segments into single vec<u8>,
        // perhaps pass the segments array instead of merging and then truncating.

        output.truncate(data_length);
    }

    Ok(output)
}
