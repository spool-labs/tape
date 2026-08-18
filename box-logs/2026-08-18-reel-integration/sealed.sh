#!/usr/bin/env bash
# One change: 4 MiB segments, so the fill rolls and seals instead of living in
# one open tail. map_above is already MAP_EVERYTHING, so a sealed segment's
# records can take the mapped path; an open tail never could.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
echo "host=$(hostname -s) $(date -u +%FT%TZ)"
echo "segment_mib=4 map_above=MAP_EVERYTHING"
TAPE_BENCH_REEL_SEGMENT_MIB=4 \
  cargo test --release -p tape-store --test track_data_read_bench -- \
  --ignored --nocapture --test-threads=1 2>&1 | grep -E '^\s*(reel|rocks|engine)'
date -u +%FT%TZ
