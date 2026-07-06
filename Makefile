# Runtime developer shortcuts
#
# Usage:
#   make programs     - Build SBPFv3 Solana programs into target/deploy
#   make node         - Build tape-node release binary with metrics enabled
#   make explorer     - Build and test the separate explorer workspace
#   make localnet      - Build the localnet orchestrator release binary
#   make reset        - Remove local validator ledger + localnet state
#   make run-solana   - Start the local solana-test-validator with programs loaded
#   make run-localnet  - Run localnet against the local validator
#   make run-cache    - Run the rpc-cache in front of the local validator (block durability)
#   make run-explorer - Serve the explorer through the rpc-cache (EXPLORER_RPC to override)
#   make run-localnet-samply - Profile the release localnet orchestrator with samply
#   make run-localnet-upload-file - Upload a large file against the running localnet
#   make run-devnet   - Run the in-process devnet TUI (release build, no profiler)
#   make run-devnet-debug - Run the devnet TUI with debug symbols, for attaching tapedbg
#   make run-devnet-samply - Profile the release devnet binary with samply
#
# Remote localnet deployment (see tools/tape-network/README.md):
#   make admin        - Build tape-admin release binary
#   make network      - Build tape-network release binary
#   make tape         - Build tape (end-user) CLI release binary
#   make node-linux   - Build tape-node for x86_64 Linux (native, Linux only; from
#                       macOS use `tape-network build-linux` — the DO builder droplet)
#   make deploy-tools - All of the above plus the Solana programs
#   make install      - `cargo install` tape-node, tape-admin, tape-network, and
#                       tape into ~/.cargo/bin
#   make uninstall    - Remove every installed tape binary
#
# Optional overrides:
#   LOCALNET_RPC_URL=http://127.0.0.1:8899
#   LOCALNET_API_PORT=9000
#   LOCALNET_NODES=3
#   EXPLORER_BIND=127.0.0.1:8080
#   EXPLORER_RPC=http://127.0.0.1:8890?api=local
#   CACHE_BIND=127.0.0.1:8890

PROGRAMS_DIR := solana/programs
LOCALNET_DATA_DIR ?= target/localnet
LOCALNET_RPC_URL ?= http://127.0.0.1:8899
LOCALNET_API_PORT ?= 9000
LOCALNET_NODES ?= 25
LOCALNET_NODE_BINARY ?= target/release/tape-node
LOCALNET_ADMIN_KEYPAIR ?= $(LOCALNET_DATA_DIR)/admin.json
LOCALNET_FILE_SIZE_BYTES ?= 1073741824
LOCALNET_UPLOAD_EPOCHS ?= 4

# rpc-cache sits in front of the validator so readers that fall behind the
# validator's prune window can still fetch old blocks. Listens on its own port
# (the validator owns 8899) and forwards to the validator upstream.
CACHE_BIND ?= 127.0.0.1:8890
CACHE_UPSTREAM ?= $(LOCALNET_RPC_URL)
CACHE_API_KEY ?= local
CACHE_CONFIG ?= $(CURDIR)/solana/rpc-cache/rpc-cache.local.yaml

# Explorer reads through the rpc-cache, not the validator directly, so it can
# tail blocks the validator has already pruned. Override EXPLORER_RPC with
# $(LOCALNET_RPC_URL) to bypass the cache and hit the validator directly.
EXPLORER_BIND ?= 127.0.0.1:8080
EXPLORER_DB ?= $(CURDIR)/target/explorer-local.sqlite3
EXPLORER_RPC ?= http://$(CACHE_BIND)?api=$(CACHE_API_KEY)

UNAME_S := $(shell uname -s)

.PHONY: programs node explorer cache localnet reset run-solana run-localnet run-cache run-explorer run-localnet-samply run-localnet-upload-file run-devnet run-devnet-debug run-devnet-samply admin network tape node-linux deploy-tools install uninstall

programs:
	$(MAKE) -C $(PROGRAMS_DIR) build

node:
	cargo build --release -p tape-node --features metrics

# Separate workspace (own Cargo.lock); build and test from within explorer/.
explorer:
	cd explorer && cargo build
	cd explorer && cargo test

cache:
	cargo build --release -p rpc-cache --bin rpc-cache

localnet:
	cargo build --release -p tape-e2e-localnet --bin localnet

reset:
	$(MAKE) -C $(PROGRAMS_DIR) reset-ledger
	rm -rf $(LOCALNET_DATA_DIR)

run-solana:
	$(MAKE) -C $(PROGRAMS_DIR) local

run-localnet:
	cargo build --release -p tape-node --features metrics
	cargo run --release -p tape-e2e-localnet --bin localnet -- \
		--node-binary $(LOCALNET_NODE_BINARY) \
		--rpc-url $(LOCALNET_RPC_URL) \
		--api-port $(LOCALNET_API_PORT) \
		--init-nodes $(LOCALNET_NODES)

run-localnet-samply:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release -p tape-e2e-localnet --bin localnet
	CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release -p tape-node --features metrics
	samply record ./target/release/localnet \
		--node-binary $(LOCALNET_NODE_BINARY) \
		--rpc-url $(LOCALNET_RPC_URL) \
		--api-port $(LOCALNET_API_PORT) \
		--init-nodes $(LOCALNET_NODES)

run-localnet-upload-file:
	cargo run --release -p tape-e2e-localnet --bin upload-file -- \
		--rpc-url $(LOCALNET_RPC_URL) \
		--admin-keypair $(LOCALNET_ADMIN_KEYPAIR) \
		--size-bytes $(LOCALNET_FILE_SIZE_BYTES) \
		--epochs $(LOCALNET_UPLOAD_EPOCHS)

# Read-through cache in front of the local validator. Start it after the
# validator (and before the explorer) so it tails and retains blocks the
# validator will later prune. CACHE_* override the bind/upstream/api-key.
run-cache:
	cargo build --release -p rpc-cache --bin rpc-cache
	CACHE_BIND="$(CACHE_BIND)" CACHE_UPSTREAM="$(CACHE_UPSTREAM)" CACHE_API_KEY="$(CACHE_API_KEY)" \
		cargo run --release -p rpc-cache --bin rpc-cache -- --config $(CACHE_CONFIG)

# Serve the explorer through the rpc-cache (see EXPLORER_RPC above). Separate
# workspace, so cd in; override EXPLORER_RPC to point at a different chain.
run-explorer:
	cd explorer && cargo run --release -- \
		serve \
		--bind $(EXPLORER_BIND) \
		--db $(EXPLORER_DB) \
		--rpc $(EXPLORER_RPC)

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

# ---------------------------------------------------------------------------
# Remote localnet deployment tooling
# ---------------------------------------------------------------------------

admin:
	cargo build --release -p tape-admin

network:
	cargo build --release -p tape-network

tape:
	cargo build --release -p tape

# Build tape-node for x86_64 Linux. Cross-compiling from macOS does not work (RocksDB +
# vendored openssl choke under a cross cc wrapper), so from macOS use the DigitalOcean
# builder — `tape-network build-linux` — which builds natively on an ephemeral droplet.
# This target is that native build; it runs on the droplet and on Linux dev boxes.
ifeq ($(UNAME_S),Linux)
node-linux:
	cargo build --release --target x86_64-unknown-linux-gnu --features metrics -p tape-node
else
node-linux:
	@echo "local cross-compile is unsupported (RocksDB). Build on Linux instead:" >&2
	@echo "  tape-network build-linux   # provisions an ephemeral DO builder droplet" >&2
	@exit 1
endif

deploy-tools: programs admin network tape node-linux

install:
	cargo install --locked --force --features metrics --path network/node
	cargo install --locked --force --path tools/tape-admin
	cargo install --locked --force --path tools/tape-network
	cargo install --locked --force --path tools/tape

uninstall:
	cargo uninstall tape-node
	cargo uninstall tape-admin
	cargo uninstall tape-network
	cargo uninstall tape
