#!/usr/bin/env bash
# Three rounds, arms alternated, so drift lands on both rather than on one.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
COLUMNS=store/reel-bridge/src/columns.rs
BENCH="cargo test --release -p tape-store --test track_data_read_bench -- --ignored --nocapture --test-threads=1"
echo "host=$(hostname -s) cores=$(nproc) tmp=$(findmnt -no SOURCE,FSTYPE /) $(date -u +%FT%TZ)"

to_open() { sed -i 's/    shaped(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    open(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS; touch $COLUMNS; }
to_tree() { sed -i 's/    open(5, ALL_COLUMN_FAMILIES\[4\], ADDRESS_LEN, 1)/    shaped(5, ALL_COLUMN_FAMILIES[4], ADDRESS_LEN, 1)/' $COLUMNS; touch $COLUMNS; }

arm() {
  local label="$1" want="$2" round="$3"
  if ! cargo test --release -p reel-bridge --test shapes 2>&1 | grep -q "^test $want \.\.\. ok"; then
    echo "round $round $label SHAPE ASSERTION FAILED, no number taken"
    return 1
  fi
  $BENCH 2>&1 | grep -E '^\s*reel' | sed "s/^/round$round $label /"
}

for round in 1 2 3; do
  to_open; arm "open" "track_data_is_open" "$round"
  to_tree; arm "tree" "declared_shapes_take" "$round"
done
to_open
echo "restored=$(grep -c 'open(5' $COLUMNS)"
date -u +%FT%TZ
