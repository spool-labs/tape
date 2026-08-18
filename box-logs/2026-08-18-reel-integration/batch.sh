#!/usr/bin/env bash
# Batched reads against one at a time, full reel against rocks. No split arm.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
echo "host=$(hostname -s) cores=$(nproc) $(date -u +%FT%TZ)"
cargo test --release -p tape-store --test track_data_read_bench -- \
  --ignored --nocapture --test-threads=1 2>&1 \
  | grep -vE '^\s*(Compiling|Finished|Running|Downloaded|Blocking)'
echo "exit=$?"
date -u +%FT%TZ
