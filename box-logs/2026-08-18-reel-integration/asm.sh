#!/usr/bin/env bash
# The idiomatic count loop alone, so its codegen under each flag set is readable
set -u
export PATH="$HOME/.cargo/bin:$PATH"
mkdir -p "$HOME/asm"
cat > "$HOME/asm/one.rs" <<'RS'
#[no_mangle]
pub fn count_scalar(leads: &[u64], want: u64) -> usize {
    leads.iter().filter(|held| **held < want).count()
}
RS
: > "$HOME/out/rc.asm"
for label in baseline v3 native; do
  case "$label" in
    baseline) flags="" ;;
    v3) flags="-C target-cpu=x86-64-v3" ;;
    native) flags="-C target-cpu=native" ;;
  esac
  rustc -O --emit asm --crate-type lib $flags -o "$HOME/asm/$label.s" "$HOME/asm/one.rs" 2>> "$HOME/out/asm.log"
  echo "$label rustc $?" >> "$HOME/out/rc.asm"
done
echo done >> "$HOME/out/rc.asm"
