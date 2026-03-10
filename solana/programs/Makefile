# Solana Programs Makefile
#
# Programs are excluded from the workspace default-members, so:
#   - `cargo build` from root does NOT build these
#   - `cargo build-sbf -p <program>` builds a specific program
#   - `make build` (from here or root) builds all programs
#
# Usage:
#   make build - Build all programs (cargo build-sbf)
#   make test  - Test all programs (cargo test-sbf)
#   make clean - Clean all program build artifacts

PROGRAMS = exchange staking token tapedrive

# Default target
.PHONY: all
all: build

# Build target: run cargo build-sbf in each program directory
.PHONY: build
build: $(PROGRAMS)
	@for dir in $(PROGRAMS); do \
		echo "Running cargo build-sbf in $$dir..."; \
    (cd $$dir && cargo build-sbf) || exit $$?; \
	done

# Test target: run cargo test-sbf in each program directory
.PHONY: test
test: $(PROGRAMS)
	@for dir in $(PROGRAMS); do \
		echo "Running cargo test-sbf in $$dir..."; \
		(cd $$dir && cargo test-sbf) || exit $$?; \
	done

# Clean target: run cargo clean in each program directory
.PHONY: clean
clean:
	@for dir in $(PROGRAMS); do \
  	echo "Running cargo clean in $$dir..."; \
    (cd $$dir && cargo clean); \
	done
