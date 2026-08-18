#!/usr/bin/env bash
# Are the probe symbols in the binary at all, and what did the scalar loop become
set -u
out="$HOME/out"
for label in baseline v3 native; do
  bin="$HOME/target-$label/release/scout"
  echo "== $label" >> "$out/nm.txt"
  nm "$bin" 2>/dev/null | grep -E "probe_scan|dispatched|direct_avx512" >> "$out/nm.txt"
  echo "-- count_below/scans symbols" >> "$out/nm.txt"
  nm "$bin" 2>/dev/null | grep -cE "count_below|scans" >> "$out/nm.txt"
done
echo "$?" > "$out/rc.syms2"
