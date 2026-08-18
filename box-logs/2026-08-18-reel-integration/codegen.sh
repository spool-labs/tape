#!/usr/bin/env bash
# A build kept only for reading: dead code linked in, so the probe symbols survive
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd "$HOME/stage/simd" || exit 3
: > "$HOME/out/rc.codegen"
for label in baseline v3 native; do
  case "$label" in
    baseline) flags="" ;;
    v3) flags="-C target-cpu=x86-64-v3" ;;
    native) flags="-C target-cpu=native" ;;
  esac
  export RUSTFLAGS="$flags -C link-dead-code"
  export CARGO_TARGET_DIR="$HOME/target-cg-$label"
  cargo build --release > "$HOME/out/cg-$label.log" 2>&1
  echo "$label build $?" >> "$HOME/out/rc.codegen"
  bin="$CARGO_TARGET_DIR/release/scout"
  : > "$HOME/out/cg-$label.asm"
  for sym in probe_scan_scalar probe_scan_sse42 probe_scan_avx2 probe_scan_avx512; do
    echo "== $sym" >> "$HOME/out/cg-$label.asm"
    objdump -d --no-show-raw-insn "--disassemble=$sym" "$bin" | sed -n '/>:/,$p' >> "$HOME/out/cg-$label.asm"
  done
  echo "$label dump $?" >> "$HOME/out/rc.codegen"
done
echo "done" >> "$HOME/out/rc.codegen"
