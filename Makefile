# Dependency upgrade pipeline for the df_subquery_udf crate.
# `make update-deps` upgrades deps, then compiles and runs the test suite.
# It stops at the first failure, so a bad upgrade never lands silently.
# Override the toolchain with `make CARGO=... update-deps`.
CARGO ?= cargo

.PHONY: update-deps deps-upgrade build test

update-deps: deps-upgrade build test
	@echo "update-deps: dependencies upgraded, compiled, and tested OK"

# Bump manifest requirements to the latest (incl. semver-incompatible) and
# refresh the lockfile. Requires cargo-edit (`cargo install cargo-edit`).
deps-upgrade:
	$(CARGO) upgrade --incompatible
	$(CARGO) update

build:
	$(CARGO) build --all-targets

test:
	$(CARGO) test
