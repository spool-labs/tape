#!/usr/bin/env bash
# One change: Chunk against Full. Full pre-writes the whole segment at creation,
# so a mapping taken any time covers it; Chunk grows the file in reservations,
# which is the case Mapping::slice warns can fall past the mapped length.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd ~/tape
LIB=store/reel-bridge/src/lib.rs
echo "host=$(hostname -s) $(date -u +%FT%TZ)"
run() {
  touch $LIB
  grep -n "preallocate:" $LIB
  TAPE_BENCH_REEL_SEGMENT_MIB=4 cargo test --release -p tape-store --test track_data_read_bench -- \
    --ignored --nocapture --test-threads=1 2>&1 | grep -E '^\s*(reel|rocks)' | grep -v packed | sed "s/^/$1 /"
}
sed -i 's/preallocate: Preallocate::Full/preallocate: Preallocate::Chunk/' $LIB
run "chunk"
echo
sed -i 's/preallocate: Preallocate::Chunk/preallocate: Preallocate::Full/' $LIB
run "full"
sed -i 's/preallocate: Preallocate::Full/preallocate: Preallocate::Chunk/' $LIB
touch $LIB
date -u +%FT%TZ
