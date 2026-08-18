#!/usr/bin/env bash
# Open32 against Tree at depth. Rocks rides as the incumbent control only.
#
# Every arm touches its source and re-asserts the shape before it measures:
# rsync rolls mtimes backwards, which lets cargo skip a rebuild and serve the
# previous arm's binary, and two arms of the same build agree for no reason.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
COLUMNS=store/reel-bridge/src/columns.rs
BENCH="cargo test --release -p tape-store --test track_data_read_bench -- --ignored --nocapture --test-threads=1"
echo "host=$(hostname -s) cores=$(nproc) tmp=$(findmnt -no SOURCE,FSTYPE /tmp || findmnt -no SOURCE,FSTYPE /) $(date -u +%FT%TZ)"

arm() {
  local label="$1" want="$2"
  echo
  echo "=== arm: track_data on $label ==="
  touch $COLUMNS
  grep -n 'ALL_COLUMN_FAMILIES\[4\]' $COLUMNS
  if ! cargo test --release -p reel-bridge --test shapes 2>&1 | grep -q "^test $want \.\.\. ok"; then
    echo "SHAPE ASSERTION FAILED for $label, no number taken"
    return 1
  fi
  echo "shape asserted: $want"
  $BENCH 2>&1 | grep -E '^\s*(reel|rocks|engine)'
}

sed -i 's/    shaped(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    open(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS
arm "Open32" "track_data_is_open"

sed -i 's/    open(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    shaped(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS
arm "Tree" "declared_shapes_take"

sed -i 's/    shaped(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    open(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS
touch $COLUMNS
echo "restored=$(grep -c 'open(5' $COLUMNS)"
date -u +%FT%TZ
