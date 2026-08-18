#!/usr/bin/env bash
# One flag set: build the scout, prove the arms agree, then run every cell twice
set -u
label="${1:?label}"
flags="${2:-}"
out="$HOME/out/$label"
mkdir -p "$out"
echo "$flags" > "$out/flags.txt"

export PATH="$HOME/.cargo/bin:$PATH"
export RUSTFLAGS="$flags"
export CARGO_TARGET_DIR="$HOME/target-$label"

cd "$HOME/stage/simd" || exit 3
cargo build --release > "$out/build.log" 2>&1
echo "$?" > "$out/rc.build"

bin="$CARGO_TARGET_DIR/release/scout"
if [ ! -x "$bin" ]; then
  echo "missing" > "$out/rc.all"
  exit 4
fi

rustc --version > "$out/rustc.txt" 2>&1

# The arms answer the same slot, or the timings below say nothing
"$bin" agree > "$out/agree.txt" 2>&1
echo "$?" > "$out/rc.agree"

for pass in 1 2; do
  for cell in leads dispatch shape iso table grown; do
    "$bin" "$cell" > "$out/$cell.$pass.txt" 2>&1
    echo "$?" > "$out/rc.$cell.$pass"
  done
done

# Codegen: whether the idiomatic count loop widened under these flags
for sym in probe_scan_scalar probe_scan_sse42 probe_scan_avx2 probe_scan_avx512 dispatched direct_avx512; do
  echo "== $sym" >> "$out/syms.txt"
  objdump -d --no-show-raw-insn "--disassemble=$sym" "$bin" >> "$out/syms.txt" 2>&1
done
echo "$?" > "$out/rc.syms"

echo "done" > "$out/rc.all"
