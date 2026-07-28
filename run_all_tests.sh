#!/bin/sh
# Verify this repository: format, lint, and every test.
#
# This is the single definition of what "verified" means here. The pre-commit
# hook calls it rather than keeping its own copy of the pipeline, so a check
# added here is a check the hook enforces, with no second list to keep in step.
#
# Nothing in it is conditional on which files changed: a docs-only edit runs the
# same steps as a rewrite of the parser. Run it by hand any time; it only reads
# the tree, never rewrites it (`cargo fmt --check` reports, the hook is what
# reformats).
#
# Exits non-zero on the first failure.
set -e

# Put the Rust toolchain on PATH. A hook or cron shell inherits almost no
# environment, and this container keeps the toolchain outside the usual home
# directory, so neither can be assumed present.
ensure_rust_toolchain() {
    if command -v cargo >/dev/null 2>&1; then
        return 0
    fi
    toolchain_bin=/tmp/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin
    if [ ! -x "$toolchain_bin/cargo" ]; then
        echo "run_all_tests: cargo not found on PATH or at $toolchain_bin" >&2
        exit 1
    fi
    PATH="$toolchain_bin:$PATH"
    CARGO_HOME=${CARGO_HOME:-/tmp/.cargo}
    RUSTUP_HOME=${RUSTUP_HOME:-/tmp/.rustup}
    export PATH CARGO_HOME RUSTUP_HOME
}

# Announce a step and time it, so a slow run says which part is slow rather
# than going quiet for minutes.
step() {
    step_name=$1
    shift
    echo
    echo "=== $step_name"
    step_started=$(date +%s)
    "$@"
    echo "--- $step_name ok (`expr $(date +%s) - $step_started`s)"
}

ensure_rust_toolchain
cd "$(dirname "$0")"

# -j 6: each test binary links the whole DataFusion tree, so the default job
# count runs enough linkers at once to thrash a 12-core box into swap.
JOBS=6

step "cargo fmt --check" cargo fmt --check
step "cargo clippy (pedantic)" cargo clippy --all-targets -j $JOBS -- -D warnings -D clippy::pedantic
step "cargo test" cargo test -j $JOBS

echo
echo "run_all_tests: everything passed"
