#!/usr/bin/env bash
# Rocks answers a small value from its own block cache with no syscall. The reel
# has no userspace value cache and preads per key. inline_max is the reel's
# equivalent: hold the value in the resident index and the read never leaves RAM.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
COLUMNS=store/reel-bridge/src/columns.rs
BENCH="cargo test --release -p tape-store --test track_data_read_bench -- --ignored --nocapture --test-threads=1"
echo "host=$(hostname -s) $(date -u +%FT%TZ)"
run() { touch $COLUMNS; grep -n 'ALL_COLUMN_FAMILIES\[4\]' $COLUMNS; $BENCH 2>&1 | grep -E '^\s*(reel|rocks)' | sed "s/^/$1 /"; }

sed -i 's/    open(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    shaped(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS
echo "=== inline 0: a pread per key ==="
run "inline0"
echo
echo "=== inline 200: the value lives in the index ==="
sed -i 's/    shaped(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    inlined(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1, 200)/' $COLUMNS
run "inline200"
sed -i 's/    inlined(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1, 200)/    open(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS
touch $COLUMNS
echo "restored=$(grep -c 'open(5' $COLUMNS)"
