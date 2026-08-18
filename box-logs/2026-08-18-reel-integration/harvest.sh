#!/usr/bin/env bash
# Pack everything the cells wrote, for pulling down in one file
set -u
cd "$HOME" || exit 3
tar -czf "$HOME/out.tar.gz" out
echo "$?" > "$HOME/rc.harvest"
