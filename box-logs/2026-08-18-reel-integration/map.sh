#!/usr/bin/env bash
# One change: map_above MAP_EVERYTHING, so read_framed takes its mapped path
# instead of a door round trip per warm read.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
touch store/reel-bridge/src/lib.rs
echo "host=$(hostname -s) $(date -u +%FT%TZ)"
grep -n "map_above" store/reel-bridge/src/lib.rs
cargo test --release -p tape-store --test track_data_read_bench -- \
  --ignored --nocapture --test-threads=1 2>&1 | grep -E '^\s*(reel|rocks|engine)'
date -u +%FT%TZ
