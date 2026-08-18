#!/usr/bin/env bash
# Build only, so a compile error surfaces before any cell is launched
set -u
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/target-baseline"
cd "$HOME/stage/simd" || exit 3
cargo build --release > "$HOME/out/check.log" 2>&1
echo "$?" > "$HOME/out/rc.check"
grep -c "^error" "$HOME/out/check.log" > "$HOME/out/errors.check"
