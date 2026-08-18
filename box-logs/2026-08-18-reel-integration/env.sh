#!/usr/bin/env bash
# Box facts, gathered once
set -u
out="$HOME/out"
mkdir -p "$out"
lscpu > "$out/lscpu.txt" 2>&1
cat /proc/cpuinfo > "$out/cpuinfo.txt" 2>&1
uname -a > "$out/uname.txt" 2>&1
free -g > "$out/free.txt" 2>&1
uptime > "$out/uptime.txt" 2>&1
echo "ok" > "$out/rc.env"
