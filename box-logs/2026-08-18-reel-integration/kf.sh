#!/usr/bin/env bash
# Pin the key_footprint table. In-memory only: no volume, no io.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/reel
echo "host=$(hostname -s) cores=$(nproc) ram_gb=$(free -g | awk '/Mem:/{print $2}') $(date -u +%FT%TZ)"
echo "toolchain=$(cargo --version)"
cargo test --release -p reel --test index_shape -- key_footprint --ignored --nocapture
echo "exit=$?"
