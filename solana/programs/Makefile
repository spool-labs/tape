# Solana Programs Makefile
#
# Programs are excluded from the workspace default-members, so:
#   - `cargo build` from root does NOT build these
#   - `cargo build-sbf` from root builds workspace default members for SBF
#   - `make build` (from here or root) builds all programs
#
# Usage:
#   make build - Build all programs (cargo build-sbf)
#   make local - Start a local solana-test-validator with all programs loaded
#   make reset-ledger - Remove the local validator ledger directory
#   make test  - Test all programs (cargo test-sbf)
#   make clean - Clean all program build artifacts

PROGRAMS = exchange staking token tapedrive
ROOT_TARGET_DEPLOY = ../../target/deploy
ROOT_TEST_ELFS = ../../test/elfs
LEDGER_DIR = test-ledger

# Default target
.PHONY: all
all: build

# Build target: run cargo build-sbf in each program directory.
# Clear RUSTC_WRAPPER so host-side wrappers do not intercept Solana's SBF rustc.
.PHONY: build
build: $(PROGRAMS)
	@for dir in $(PROGRAMS); do \
		echo "Running cargo build-sbf in $$dir..."; \
		(cd $$dir && RUSTC_WRAPPER= cargo build-sbf) || exit $$?; \
	done

# Test target: run cargo test-sbf in each program directory.
# Clear RUSTC_WRAPPER so host-side wrappers do not intercept Solana's SBF rustc.
.PHONY: test
test: $(PROGRAMS)
	@for dir in $(PROGRAMS); do \
		echo "Running cargo test-sbf in $$dir..."; \
		(cd $$dir && RUSTC_WRAPPER= cargo test-sbf) || exit $$?; \
	done

# Local validator target
.PHONY: local
local:
	solana-test-validator \
		--bpf-program tajZ1QndNonM3teK59PdUfiF9ZAQT6xqucipbs8mN8W $(ROOT_TARGET_DEPLOY)/tapedrive.so \
		--bpf-program tape9hFAE7jstfKB2QT1ovFNUZKKtDUyGZiGQpnBFdL $(ROOT_TARGET_DEPLOY)/token.so \
		--bpf-program taAfD9hTjxpiVUSjTNx5ezKT6CXW9W2Ya4ky1RMev5f $(ROOT_TARGET_DEPLOY)/exchange.so \
		--bpf-program taQ4ccnpwKHP9SxPxda76YrwxhDwsCMYg8vjf6KRiNh $(ROOT_TARGET_DEPLOY)/staking.so \
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
