#!/usr/bin/env bash
# The five probe loops and the four lead scan arms must agree before any timing
set -u
bin="$HOME/target-baseline/release/scout"
"$bin" agree > "$HOME/out/agree.pre.txt" 2>&1
echo "$?" > "$HOME/out/rc.agree.pre"
