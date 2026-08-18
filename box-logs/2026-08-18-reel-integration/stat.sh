#!/usr/bin/env bash
# Where the driver is, read off the rc files it writes
set -u
cd "$HOME/out" || exit 3
echo "go:"; cat rc.go 2>/dev/null
for label in baseline v3 native; do
  echo "-- $label"
  ls "$label" 2>/dev/null | tr '\n' ' '
  echo
done
