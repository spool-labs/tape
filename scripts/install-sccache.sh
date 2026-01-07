#!/usr/bin/env bash
#
# Install sccache for caching Rust/C++ compilations
# Works on Linux (x86_64, aarch64) and macOS (x86_64, arm64)
#

set -euo pipefail

SCCACHE_VERSION="${SCCACHE_VERSION:-0.8.2}"

# Detect OS and architecture
detect_platform() {
    local os arch

    case "$(uname -s)" in
        Linux)  os="unknown-linux-musl" ;;
        Darwin) os="apple-darwin" ;;
        *)      echo "Unsupported OS: $(uname -s)"; exit 1 ;;
    esac

    case "$(uname -m)" in
        x86_64)  arch="x86_64" ;;
        aarch64) arch="aarch64" ;;
        arm64)   arch="aarch64" ;;
        *)       echo "Unsupported architecture: $(uname -m)"; exit 1 ;;
    esac

    echo "${arch}-${os}"
}

# Check if sccache is already installed and up to date
check_existing() {
    if command -v sccache &> /dev/null; then
        local installed_version
        installed_version=$(sccache --version 2>/dev/null | awk '{print $2}')
        if [[ "$installed_version" == "$SCCACHE_VERSION" ]]; then
            echo "sccache $SCCACHE_VERSION is already installed"
            return 0
        fi
        echo "Upgrading sccache from $installed_version to $SCCACHE_VERSION"
    fi
    return 1
}

# Install via cargo (fallback)
install_via_cargo() {
    echo "Installing sccache via cargo..."
    cargo install sccache --version "$SCCACHE_VERSION"
}

# Install via prebuilt binary (faster)
install_prebuilt() {
    local platform="$1"
    local url="https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${platform}.tar.gz"
    local install_dir="${CARGO_HOME:-$HOME/.cargo}/bin"
    local tmp_dir

    echo "Downloading sccache v${SCCACHE_VERSION} for ${platform}..."

    tmp_dir=$(mktemp -d)
    trap "rm -rf $tmp_dir" EXIT

    if command -v curl &> /dev/null; then
        curl -fsSL "$url" | tar -xz -C "$tmp_dir"
    elif command -v wget &> /dev/null; then
        wget -qO- "$url" | tar -xz -C "$tmp_dir"
    else
        echo "Neither curl nor wget found, falling back to cargo install"
        install_via_cargo
        return
    fi

    mkdir -p "$install_dir"
    mv "$tmp_dir/sccache-v${SCCACHE_VERSION}-${platform}/sccache" "$install_dir/"
    chmod +x "$install_dir/sccache"

    echo "Installed sccache to $install_dir/sccache"
}

# Configure shell profile
configure_shell() {
    local shell_rc=""
    local config_lines='
# sccache - Rust/C++ compilation cache
export RUSTC_WRAPPER=sccache
export SCCACHE_CACHE_SIZE="10G"
'

    # Detect shell config file
    if [[ -n "${ZSH_VERSION:-}" ]] || [[ "$SHELL" == *"zsh"* ]]; then
        shell_rc="$HOME/.zshrc"
    elif [[ -n "${BASH_VERSION:-}" ]] || [[ "$SHELL" == *"bash"* ]]; then
        shell_rc="$HOME/.bashrc"
    fi

    if [[ -n "$shell_rc" ]] && [[ -f "$shell_rc" ]]; then
        if ! grep -q "RUSTC_WRAPPER=sccache" "$shell_rc" 2>/dev/null; then
            echo "Adding sccache config to $shell_rc"
            echo "$config_lines" >> "$shell_rc"
            echo ""
            echo "Run 'source $shell_rc' or start a new terminal to activate sccache"
        else
            echo "sccache already configured in $shell_rc"
        fi
    else
        echo ""
        echo "Add the following to your shell profile:"
        echo "$config_lines"
    fi
}

main() {
    echo "=== sccache installer ==="
    echo ""

    # Check if already installed
    if check_existing; then
        configure_shell
        exit 0
    fi

    # Detect platform
    local platform
    platform=$(detect_platform)
    echo "Detected platform: $platform"

    # Install
    install_prebuilt "$platform" || install_via_cargo

    # Verify installation
    if command -v sccache &> /dev/null; then
        echo ""
        echo "Successfully installed: $(sccache --version)"
        configure_shell
    else
        echo "Installation failed"
        exit 1
    fi
}

main "$@"
