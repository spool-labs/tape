# Solana Programs Makefile
#
# Programs are excluded from the workspace default-members, so:
#   - `cargo build` from root does NOT build these
#   - `cargo build-sbf` from root builds workspace default members for SBF
#   - `make build` (from here or root) builds all programs
#
# Usage:
#   make build - Build all programs as SBPFv3 (cargo build-sbf --arch v3)
#   make local - Start a local solana-test-validator with all programs loaded
#                (LEDGER_SHREDS=n to change how much history it keeps)
#   make reset-ledger - Remove the local validator ledger directory
#   make test  - Test all programs (cargo test-sbf)
#   make clean - Clean all program build artifacts

PROGRAMS = exchange staking token tapedrive
ROOT_TARGET_DEPLOY = ../../target/deploy
ROOT_TEST_ELFS ?= ../../test/elfs
LEDGER_DIR = test-ledger
SBF_ARCH ?= v3

# Shreds the validator keeps in root slots. Its own default is 10,000, which is
# minutes of history: once it prunes, a node that fell behind can no longer fetch
# the blocks it needs and never catches up. Override for a shorter or longer run.
LEDGER_SHREDS ?= 50000000

# Default target
.PHONY: all
all: build

# Build target: run cargo build-sbf in each program directory as SBPFv3.
# Clear RUSTC_WRAPPER so host-side wrappers do not intercept Solana's SBF rustc.
.PHONY: build
build: $(PROGRAMS)
	@for dir in $(PROGRAMS); do \
		echo "Running cargo build-sbf --arch $(SBF_ARCH) in $$dir..."; \
		(cd $$dir && RUSTC_WRAPPER= cargo build-sbf --arch $(SBF_ARCH)) || exit $$?; \
	done

# Test target: run cargo test-sbf in each program directory as SBPFv3.
# Clear RUSTC_WRAPPER so host-side wrappers do not intercept Solana's SBF rustc.
.PHONY: test
test: $(PROGRAMS)
	@for dir in $(PROGRAMS); do \
		echo "Running cargo test-sbf --arch $(SBF_ARCH) in $$dir..."; \
		(cd $$dir && RUSTC_WRAPPER= cargo test-sbf --arch $(SBF_ARCH)) || exit $$?; \
	done

# Local validator target
.PHONY: local
local:
	solana-test-validator \
		--limit-ledger-size $(LEDGER_SHREDS) \
		--bpf-program 4iG5wyAy1u4gCqHm5jYJNU1hsYw7PtQ1N6dC8rZQKmPB $(ROOT_TARGET_DEPLOY)/tapedrive.so \
		--bpf-program 5TSWmdgb3r3R7c9Uu5HxgyePhmJa31FCEDBDjkvTmvJE $(ROOT_TARGET_DEPLOY)/token.so \
		--bpf-program 82S3gYouhLQi2T9cJ9C9NcdYJuSy5MUCivicr2R8Mb4z $(ROOT_TARGET_DEPLOY)/exchange.so \
		--bpf-program 7MrYjvGuuwjTc8mUUPfcUDbVxBEhnWCS2fFWrbsAV2ZK $(ROOT_TARGET_DEPLOY)/staking.so \
		--bpf-program metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s $(ROOT_TEST_ELFS)/mpl_token_metadata.so

.PHONY: reset-ledger
reset-ledger:
	rm -rf $(LEDGER_DIR)

# Clean target: run cargo clean in each program directory
.PHONY: clean
clean:
	@for dir in $(PROGRAMS); do \
  	echo "Running cargo clean in $$dir..."; \
    (cd $$dir && cargo clean); \
	done
