#!/usr/bin/env bash
# Does the reel's blocking read floor come from going to the volume for values
# it could carry? Rocks keeps sub-blob values in the SST and serves them from the
# block cache; the bridge declares row_carry 0 on every column, so every read is
# a device round trip.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
COLUMNS=store/reel-bridge/src/columns.rs
BENCH="cargo test --release -p tape-store --test track_data_read_bench -- --ignored --nocapture --test-threads=1"
echo "host=$(hostname -s) tmp=$(findmnt -no SOURCE,FSTYPE /) $(date -u +%FT%TZ)"

run() {
  touch $COLUMNS
  grep -n 'ALL_COLUMN_FAMILIES\[4\]' $COLUMNS
  $BENCH 2>&1 | grep -E '^\s*(reel|rocks)' | sed "s/^/$1 /"
}

echo "=== carry 0: every value read from the volume ==="
sed -i 's/    carried(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1, [0-9]*)/    shaped(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS
sed -i 's/    open(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    shaped(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS
run "carry0"

echo
echo "=== carry 128: the value rides in the sealed row ==="
sed -i 's/    shaped(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    carried(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1, 200)/' $COLUMNS
run "carry200"

sed -i 's/    carried(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1, 200)/    open(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS
touch $COLUMNS
echo "restored=$(grep -c 'open(5' $COLUMNS)"
date -u +%FT%TZ
