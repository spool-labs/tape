#!/usr/bin/env bash
# Step 9: track_data point reads, open shard against tree, everything else equal.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
BENCH="cargo test --release -p tape-store --test track_data_read_bench -- --ignored --nocapture --test-threads=1"
echo "host=$(hostname -s) cores=$(nproc) $(date -u +%FT%TZ)"

echo "=== shape assertion: the declaration must have taken ==="
cargo test --release -p reel-bridge --test shapes 2>&1 | tail -5

echo
echo "=== arm A: track_data on Open32 ==="
grep -n 'ALL_COLUMN_FAMILIES\[4\]' store/reel-bridge/src/columns.rs
$BENCH 2>&1 | grep -vE '^\s+(Compiling|Finished|Running|Downloaded)'

echo
echo "=== arm B: track_data on Tree ==="
sed -i 's/    open(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    shaped(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' store/reel-bridge/src/columns.rs
grep -n 'ALL_COLUMN_FAMILIES\[4\]' store/reel-bridge/src/columns.rs
$BENCH 2>&1 | grep -vE '^\s+(Compiling|Finished|Running|Downloaded)'

# Put the tree back so the checkout matches the commit it came from.
sed -i 's/    shaped(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    open(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' store/reel-bridge/src/columns.rs
echo "restored=$(grep -c 'open(5' store/reel-bridge/src/columns.rs)"
date -u +%FT%TZ
