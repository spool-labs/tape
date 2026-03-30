# Runtime developer shortcuts
#
# Usage:
#   make programs     - Build all Solana programs via solana/programs/Makefile
#   make node         - Build tape-node2 with metrics enabled
#   make prodnet      - Build the prodnet orchestrator binary
#   make reset        - Remove local validator ledger + prodnet state
#   make run-solana   - Start the local solana-test-validator with programs loaded
#   make run-prodnet  - Run prodnet against the local validator
#
# Optional overrides:
#   PRODNET_RPC_URL=http://127.0.0.1:8899
#   PRODNET_API_PORT=9000
#   PRODNET_NODES=3

PROGRAMS_DIR := solana/programs
PRODNET_DATA_DIR ?= target/prodnet
PRODNET_RPC_URL ?= http://127.0.0.1:8899
PRODNET_API_PORT ?= 9000
PRODNET_NODES ?= 25

.PHONY: programs node prodnet reset run-solana run-prodnet

programs:
	$(MAKE) -C $(PROGRAMS_DIR) build

node:
	cargo build -p tape-node2 --features metrics

prodnet:
	cargo build -p tape-e2e-prodnet --bin prodnet

reset:
	$(MAKE) -C $(PROGRAMS_DIR) reset-ledger
	rm -rf $(PRODNET_DATA_DIR)

run-solana:
	$(MAKE) -C $(PROGRAMS_DIR) local

run-prodnet:
	cargo run -p tape-e2e-prodnet --bin prodnet -- \
		--rpc-url $(PRODNET_RPC_URL) \
		--api-port $(PRODNET_API_PORT) \
		--init-nodes $(PRODNET_NODES)

run-devnet:
	cargo run -p tape-e2e-fuzznet --bin fuzznet
