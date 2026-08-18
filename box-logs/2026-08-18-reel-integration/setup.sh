#!/usr/bin/env bash
# T5 box setup: a compiler, a linker, and the scout tree unpacked
set -u
mkdir -p "$HOME/out"
rc="$HOME/out/rc.setup"
: > "$rc"

sudo DEBIAN_FRONTEND=noninteractive apt-get -qq update > "$HOME/out/apt.log" 2>&1
echo "apt-update $?" >> "$rc"
sudo DEBIAN_FRONTEND=noninteractive apt-get -qq -y install build-essential binutils >> "$HOME/out/apt.log" 2>&1
echo "apt-install $?" >> "$rc"

if [ ! -x "$HOME/.cargo/bin/cargo" ]; then
  curl -sSf https://sh.rustup.rs -o "$HOME/rustup.sh"
  echo "rustup-fetch $?" >> "$rc"
  sh "$HOME/rustup.sh" -y --profile minimal --default-toolchain stable > "$HOME/out/rustup.log" 2>&1
  echo "rustup-install $?" >> "$rc"
fi

rm -rf "$HOME/stage"
mkdir -p "$HOME/stage"
tar -xzf "$HOME/stage.tar.gz" -C "$HOME/stage"
echo "untar $?" >> "$rc"

"$HOME/.cargo/bin/rustc" --version >> "$rc" 2>&1
echo "ready" >> "$rc"
