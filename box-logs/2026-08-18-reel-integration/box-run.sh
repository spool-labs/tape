#!/bin/bash
# x86 verification of the reel simd work: correctness, codegen, and before/after.
set -u
export PATH=$HOME/.cargo/bin:$PATH
cd $HOME

rm -rf simdrun && mkdir -p simdrun/base simdrun/now
tar xzf reel-base.tgz -C simdrun/base
tar xzf reel-simd.tgz -C simdrun/now

echo "=== build base ==="
( cd simdrun/base && cargo build --release -p reel 2>&1 | tail -2 )
echo "=== build now ==="
( cd simdrun/now && cargo build --release -p reel 2>&1 | tail -2 )

echo "=== codegen: site arms on x86 ==="
for tree in base now; do
  rlib=$(ls -t $HOME/simdrun/$tree/target/release/deps/libreel-*.rlib | head -1)
  rm -rf $HOME/simdrun/asm-$tree && mkdir -p $HOME/simdrun/asm-$tree
  ( cd $HOME/simdrun/asm-$tree && ar x "$rlib" && objdump -dr --demangle *.o > all.asm 2>/dev/null )
  echo "--- $tree ---"
  grep -c "pcmpeqb" $HOME/simdrun/asm-$tree/all.asm || true
  grep -oE "reel::index::opentable::[A-Za-z_$<>0-9]*::(site|hash_of)::" $HOME/simdrun/asm-$tree/all.asm | sort | uniq -c | head
done

echo "=== oracles (now) ==="
( cd simdrun/now && cargo test --release -p reel --test index 2>&1 | tail -4 )
echo "=== scan arms oracle (now) ==="
( cd simdrun/now && cargo test --release -p reel --test index_shape -- every_scan_counts_alike 2>&1 | tail -4 )
echo "=== scan arms forced narrow (now) ==="
( cd simdrun/now && REEL_SCAN=avx2 cargo test --release -p reel --test index_shape -- every_scan_counts_alike 2>&1 | tail -3 )
( cd simdrun/now && REEL_SCAN=scalar cargo test --release -p reel --test index_shape -- every_scan_counts_alike 2>&1 | tail -3 )

echo "=== key_footprint BASE ==="
( cd simdrun/base && cargo test --release -p reel --test index_shape -- key_footprint --ignored --nocapture 2>&1 | grep -E "keys|scan backend|262144|1048576" )
echo "=== key_footprint NOW ==="
( cd simdrun/now && cargo test --release -p reel --test index_shape -- key_footprint --ignored --nocapture 2>&1 | grep -E "keys|scan backend|262144|1048576" )

echo "=== full suite (now) ==="
( cd simdrun/now && cargo test --release -p reel --no-fail-fast 2>&1 | grep -E "^test result|^error|FAILED" | tail -30 )
echo "=== DONE ==="
