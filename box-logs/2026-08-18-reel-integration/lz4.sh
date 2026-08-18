#!/usr/bin/env bash
# One change from the last run: the bridge declares lz4 (slice excepted).
# Both fills, since a codec's answer is entirely a property of the bytes.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
touch store/reel-bridge/src/columns.rs
echo "host=$(hostname -s) $(date -u +%FT%TZ)"
grep -n "codec: Codec::" store/reel-bridge/src/columns.rs
cargo test --release -p tape-store --test track_data_read_bench -- \
  --ignored --nocapture --test-threads=1 2>&1 | grep -E '^\s*(reel|rocks|engine)'
date -u +%FT%TZ
