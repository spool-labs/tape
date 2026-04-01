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

.PHONY: programs node testnet reset run-solana run-testnet run-testnet-samply run-devnet

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

run-devnet:
	cargo run -p tape-e2e-devnet --bin devnet
