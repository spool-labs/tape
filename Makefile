# Runtime developer shortcuts
#
# Usage:
#   make programs     - Build all Solana programs via solana/programs/Makefile
#   make node         - Build tape-node release binary with metrics enabled
#   make testnet      - Build the testnet orchestrator release binary
#   make reset        - Remove local validator ledger + testnet state
#   make run-solana   - Start the local solana-test-validator with programs loaded
#   make run-testnet  - Run testnet against the local validator
#   make run-testnet-samply - Profile the release testnet orchestrator with samply
#   make run-testnet-upload-file - Upload a large file against the running testnet
#   make run-devnet   - Run the in-process devnet TUI (release build, no profiler)
#   make run-devnet-debug - Run the devnet TUI with debug symbols, for attaching tapedbg
#   make run-devnet-samply - Profile the release devnet binary with samply
#
# Optional overrides:
#   TESTNET_RPC_URL=http://127.0.0.1:8899
#   TESTNET_API_PORT=9000
#   TESTNET_NODES=3

PROGRAMS_DIR := solana/programs
TESTNET_DATA_DIR ?= target/testnet
TESTNET_RPC_URL ?= http://127.0.0.1:8899
TESTNET_API_PORT ?= 9000
TESTNET_NODES ?= 25
TESTNET_NODE_BINARY ?= target/release/tape-node
TESTNET_ADMIN_KEYPAIR ?= $(TESTNET_DATA_DIR)/admin.json
TESTNET_FILE_SIZE_BYTES ?= 1073741824
TESTNET_UPLOAD_EPOCHS ?= 4

.PHONY: programs node testnet reset run-solana run-testnet run-testnet-samply run-testnet-upload-file run-devnet run-devnet-debug run-devnet-samply

programs:
	$(MAKE) -C $(PROGRAMS_DIR) build

node:
	cargo build --release -p tape-node --features metrics

testnet:
	cargo build --release -p tape-e2e-testnet --bin testnet

reset:
	$(MAKE) -C $(PROGRAMS_DIR) reset-ledger
	rm -rf $(TESTNET_DATA_DIR)

run-solana:
	$(MAKE) -C $(PROGRAMS_DIR) local

run-testnet:
	cargo build --release -p tape-node --features metrics
	cargo run --release -p tape-e2e-testnet --bin testnet -- \
		--node-binary $(TESTNET_NODE_BINARY) \
		--rpc-url $(TESTNET_RPC_URL) \
		--api-port $(TESTNET_API_PORT) \
		--init-nodes $(TESTNET_NODES)

run-testnet-samply:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release -p tape-e2e-testnet --bin testnet
	CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release -p tape-node --features metrics
	samply record ./target/release/testnet \
		--node-binary $(TESTNET_NODE_BINARY) \
		--rpc-url $(TESTNET_RPC_URL) \
		--api-port $(TESTNET_API_PORT) \
		--init-nodes $(TESTNET_NODES)

run-testnet-upload-file:
	cargo run --release -p tape-e2e-testnet --bin upload-file -- \
		--rpc-url $(TESTNET_RPC_URL) \
		--admin-keypair $(TESTNET_ADMIN_KEYPAIR) \
		--size-bytes $(TESTNET_FILE_SIZE_BYTES) \
		--epochs $(TESTNET_UPLOAD_EPOCHS)

run-devnet:
	cargo run --release -p tape-e2e-devnet --bin devnet

# Release optimizations + full DWARF debug info so tapedbg can resolve source
# breakpoints and read locals. Uses the `debug-release` profile defined in
# Cargo.toml (separate target dir, won't clobber normal release builds). Once
# the TUI is up, grab the PID with `pgrep -x devnet` and attach from another
# shell: `tapedbg attach --pid <pid>`. See tapedbg/CLAUDE.md for the workflow.
run-devnet-debug:
	cargo run --profile debug-release -p tape-e2e-devnet --bin devnet

run-devnet-samply:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release -p tape-e2e-devnet --bin devnet
	samply record ./target/release/devnet
