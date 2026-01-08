# Tapedrive Local Development Makefile
#
# Usage:
#   make setup     - First-time setup (installs sccache for build caching)
#   make check     - Check all non-program crates (cargo check)
#   make build     - Build all Solana programs (cargo build-sbf)
#   make test      - Test all Solana programs (cargo test-sbf)
#   make test-all  - Run all workspace tests
#   make validator - Start local validator with all programs
#   make local     - Clean, build, and start validator
#   make metadata  - Download Metaplex metadata program from mainnet
#
# Note: Programs are excluded from default workspace members.
#   - `cargo build` / `cargo check` only builds non-program crates
#   - `cargo build-sbf -p <program>` builds specific programs
#   - `make build` builds all programs via cargo build-sbf
#
# Build Caching:
#   sccache is used to cache C++ compilations (e.g., rocksdb).
#   Run `make setup` to install it, or it will be installed automatically.
#
# Program IDs:
#   tapedrive: tajZ1QndNonM3teK59PdUfiF9ZAQT6xqucipbs8mN8W
#   staking:   taQ4ccnpwKHP9SxPxda76YrwxhDwsCMYg8vjf6KRiNh
#   exchange:  taAfD9hTjxpiVUSjTNx5ezKT6CXW9W2Ya4ky1RMev5f
#   token:     tape9hFAE7jstfKB2QT1ovFNUZKKtDUyGZiGQpnBFdL
#   metadata:  metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s

.PHONY: all clean build test check validator local metadata setup ensure-sccache test-build test-all

# Program IDs
TAPEDRIVE_ID = tajZ1QndNonM3teK59PdUfiF9ZAQT6xqucipbs8mN8W
STAKING_ID   = taQ4ccnpwKHP9SxPxda76YrwxhDwsCMYg8vjf6KRiNh
EXCHANGE_ID  = taAfD9hTjxpiVUSjTNx5ezKT6CXW9W2Ya4ky1RMev5f
TOKEN_ID     = tape9hFAE7jstfKB2QT1ovFNUZKKtDUyGZiGQpnBFdL
METADATA_ID  = metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s

# Build output directory
DEPLOY_DIR = target/deploy

# Pre-built ELFs
METADATA_ELF = test/elfs/mpl_token_metadata.so

# RPC URL for cloning programs
RPC_URL = https://api.mainnet-beta.solana.com

all: build

# First-time setup - install sccache for build caching
setup:
	@./scripts/install-sccache.sh

# Ensure sccache is installed (called automatically by build targets)
ensure-sccache:
	@command -v sccache >/dev/null 2>&1 || (echo "Installing sccache..." && ./scripts/install-sccache.sh)

# Check all non-program crates (uses default-members from Cargo.toml)
check: ensure-sccache
	@echo "Checking non-program crates..."
	@RUSTC_WRAPPER=sccache cargo check

# Pre-build all test binaries (prevents rocksdb rebuilds when switching between test commands)
test-build: ensure-sccache
	@echo "Building all test binaries..."
	@RUSTC_WRAPPER=sccache cargo test --no-run

# Run all tests (builds once, runs all)
test-all: ensure-sccache
	@echo "Running all tests..."
	@RUSTC_WRAPPER=sccache cargo test

# Remove test-ledger directory
clean:
	@rm -rf test-ledger

# Download Metaplex metadata program from mainnet
metadata:
	@mkdir -p test/elfs
	solana program dump --url mainnet-beta $(METADATA_ID) $(METADATA_ELF)

# Build all programs (on-chain, uses cargo build-sbf, no rocksdb)
build:
	@echo "Building tapedrive program..."
	@cd programs/tapedrive && cargo build-sbf
	@echo "Building staking program..."
	@cd programs/staking && cargo build-sbf
	@echo "Building exchange program..."
	@cd programs/exchange && cargo build-sbf
	@echo "Building token program..."
	@cd programs/token && cargo build-sbf

# Run tests for all programs (on-chain, uses cargo test-sbf)
test:
	@echo "Testing tapedrive program..."
	@cd programs/tapedrive && cargo test-sbf
	@echo "Testing staking program..."
	@cd programs/staking && cargo test-sbf
	@echo "Testing exchange program..."
	@cd programs/exchange && cargo test-sbf
	@echo "Testing token program..."
	@cd programs/token && cargo test-sbf

# Start local validator with all programs
validator:
	solana-test-validator \
		--reset \
		--bpf-program $(TAPEDRIVE_ID) $(DEPLOY_DIR)/tapedrive.so \
		--bpf-program $(STAKING_ID) $(DEPLOY_DIR)/staking.so \
		--bpf-program $(EXCHANGE_ID) $(DEPLOY_DIR)/exchange.so \
		--bpf-program $(TOKEN_ID) $(DEPLOY_DIR)/token.so \
		--bpf-program $(METADATA_ID) $(METADATA_ELF) \
		--url $(RPC_URL)

# Clean, build, and start validator
local: clean build validator
