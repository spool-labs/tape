# Runtime developer shortcuts
#
# Usage:
#   make programs     - Build all Solana programs via solana/programs/Makefile
#   make node         - Build tape-node with metrics enabled
#   make testnet      - Build the testnet orchestrator binary
#   make reset        - Remove local validator ledger + testnet state
#   make run-solana   - Start the local solana-test-validator with programs loaded
#   make run-testnet  - Run testnet against the local validator
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

.PHONY: programs node testnet reset run-solana run-testnet run-devnet

programs:
	$(MAKE) -C $(PROGRAMS_DIR) build

node:
	cargo build -p tape-node --features metrics

testnet:
	cargo build -p tape-e2e-testnet --bin testnet

reset:
	$(MAKE) -C $(PROGRAMS_DIR) reset-ledger
	rm -rf $(TESTNET_DATA_DIR)

run-solana:
	$(MAKE) -C $(PROGRAMS_DIR) local

run-testnet:
	cargo run -p tape-e2e-testnet --bin testnet -- \
		--rpc-url $(TESTNET_RPC_URL) \
		--api-port $(TESTNET_API_PORT) \
		--init-nodes $(TESTNET_NODES)

run-devnet:
	cargo run -p tape-e2e-devnet --bin devnet
