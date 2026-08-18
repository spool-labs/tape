#!/usr/bin/env bash
# The three builds, one after another so no cell ever runs beside another
set -u
cd "$HOME" || exit 3
: > "$HOME/out/rc.go"

nohup bash -c '
  set -u
  bash "$HOME/run.sh" baseline ""
  echo "baseline $?" >> "$HOME/out/rc.go"
  bash "$HOME/run.sh" v3 "-C target-cpu=x86-64-v3"
  echo "v3 $?" >> "$HOME/out/rc.go"
  bash "$HOME/run.sh" native "-C target-cpu=native"
  echo "native $?" >> "$HOME/out/rc.go"
  echo "all" >> "$HOME/out/rc.go"
' > "$HOME/out/go.log" 2>&1 &

echo "launched $!" > "$HOME/out/rc.launch"
