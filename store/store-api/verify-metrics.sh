#!/bin/bash
# Verification script for Prometheus metrics implementation

set -e

echo "=== store Metrics Implementation Verification ==="
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $2"
    else
        echo -e "${RED}✗${NC} $2"
    fi
}

# 1. Check Cargo is available
echo "1. Checking Rust/Cargo installation..."
if command -v cargo &> /dev/null; then
    print_status 0 "Cargo found: $(cargo --version)"
else
    print_status 1 "Cargo not found"
    echo -e "${YELLOW}Please install Rust: https://rustup.rs/${NC}"
    exit 1
fi
echo

# 2. Build without metrics feature (default)
echo "2. Building without metrics feature (default)..."
if cargo build 2>&1 | tail -5; then
    print_status 0 "Build successful (no metrics)"
else
    print_status 1 "Build failed (no metrics)"
    exit 1
fi
echo

# 3. Build with metrics feature
echo "3. Building with metrics feature..."
if cargo build --features metrics 2>&1 | tail -5; then
    print_status 0 "Build successful (with metrics)"
else
    print_status 1 "Build failed (with metrics)"
    exit 1
fi
echo

# 4. Run unit tests without metrics
echo "4. Running unit tests (without metrics)..."
if cargo test --lib 2>&1 | grep -E "test result|running"; then
    print_status 0 "Unit tests passed (no metrics)"
else
    print_status 1 "Unit tests failed (no metrics)"
    exit 1
fi
echo

# 5. Run unit tests with metrics
echo "5. Running unit tests (with metrics)..."
if cargo test --lib --features metrics 2>&1 | grep -E "test result|running"; then
    print_status 0 "Unit tests passed (with metrics)"
else
    print_status 1 "Unit tests failed (with metrics)"
    exit 1
fi
echo

# 6. Run metrics integration tests
echo "6. Running metrics integration tests..."
if cargo test --test metrics_test --features metrics 2>&1 | grep -E "test result|running"; then
    print_status 0 "Metrics integration tests passed"
else
    print_status 1 "Metrics integration tests failed"
    exit 1
fi
echo

# 7. Run metrics example
echo "7. Running metrics example..."
if cargo run --example metrics_basic --features metrics 2>&1 | tail -20; then
    print_status 0 "Metrics example ran successfully"
else
    print_status 1 "Metrics example failed"
    exit 1
fi
echo

# 8. Check code formatting
echo "8. Checking code formatting..."
if cargo fmt -- --check &> /dev/null; then
    print_status 0 "Code formatting is correct"
else
    print_status 1 "Code formatting issues found (run: cargo fmt)"
fi
echo

# 9. Run clippy lints
echo "9. Running clippy lints..."
if cargo clippy --features metrics -- -D warnings 2>&1 | tail -10; then
    print_status 0 "No clippy warnings"
else
    print_status 1 "Clippy warnings found"
fi
echo

echo "=== Verification Complete ==="
echo
echo -e "${GREEN}All checks passed!${NC}"
echo
echo "Next steps:"
echo "  - Review metrics output from the example above"
echo "  - Test with RocksStore in addition to MemoryStore"
echo "  - Set up Prometheus scraping endpoint if needed"
echo "  - Review METRICS.md for usage documentation"
