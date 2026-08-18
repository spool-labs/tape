#!/usr/bin/env bash
# Build the tape bench targets. RocksDB is the long pole, ~20 min from cold.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
echo "host=$(hostname -s) $(date -u +%FT%TZ)"
cargo build --release -p reel-bridge -p tape-store --tests
echo "build_exit=$?"
date -u +%FT%TZ
