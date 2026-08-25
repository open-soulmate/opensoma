# OpenSoma — Makefile for build automation
# Usage: make build, make test, make release, make check, make docker, make install

BINARY_NAME := opensoma
PREFIX ?= /usr/local
CARGO := cargo
DOCKER_IMAGE := opensoma
DOCKER_TAG ?= latest

.PHONY: all build release test check clippy fmt clean install docker docker-push ci audit self-test doctor help

all: check test build

## Build debug binary
build:
	$(CARGO) build

## Build optimized release binary
release:
	$(CARGO) build --release

## Run all tests
test:
	$(CARGO) test

## Run all tests with output
test-verbose:
	$(CARGO) test -- --nocapture

## Check compilation without building
check:
	$(CARGO) check --all-targets

## Run clippy linter
clippy:
	$(CARGO) clippy --all-targets -- -D warnings

## Format code
fmt:
	$(CARGO) fmt

## Check formatting (CI mode)
fmt-check:
	$(CARGO) fmt -- --check

## Remove build artifacts
clean:
	$(CARGO) clean

## Install release binary to $PREFIX/bin
install: release
	install -m 755 target/release/$(BINARY_NAME) $(PREFIX)/bin/$(BINARY_NAME)

## Uninstall binary
uninstall:
	rm -f $(PREFIX)/bin/$(BINARY_NAME)

## Generate default config.toml
init:
	./target/release/$(BINARY_NAME) --init

## Validate config.toml
validate:
	./target/release/$(BINARY_NAME) --validate

## Query running daemon status
status:
	./target/release/$(BINARY_NAME) --status

## Print Prometheus metrics from running daemon
metrics:
	./target/release/$(BINARY_NAME) --metrics

## Build Docker image
docker:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

## Push Docker image
docker-push: docker
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)

## Run cargo bench (if benchmarks exist)
bench:
	$(CARGO) bench 2>/dev/null || echo "No benchmarks configured"

## Count lines of Rust code
loc:
	@echo "Source lines:"
	@find src -name "*.rs" | xargs wc -l | tail -1
	@echo "Test lines:"
	@find tests -name "*.rs" | xargs wc -l | tail -1 2>/dev/null || echo "  (none)"
	@echo "Total tests:"
	@$(CARGO) test 2>&1 | grep "test result" | head -5

## Show test coverage summary (requires cargo-tarpaulin)
coverage:
	$(CARGO) tarpaulin --skip-clean 2>/dev/null || echo "Install cargo-tarpaulin: cargo install cargo-tarpaulin"

## Build for all common targets
cross-build:
	cargo build --release --target x86_64-unknown-linux-gnu 2>/dev/null || true
	cargo build --release --target aarch64-unknown-linux-gnu 2>/dev/null || true

## Print version info
version:
	@./target/release/$(BINARY_NAME) --version 2>/dev/null || $(CARGO) metadata --format-version 1 2>/dev/null | grep '"version"' | head -1

## Full CI pipeline: fmt-check + clippy + test + release build + self-test
ci: fmt-check clippy test release self-test
	@echo "✅ CI pipeline passed"

## Run cargo audit for security vulnerabilities (requires cargo-audit)
audit:
	$(CARGO) audit 2>/dev/null || echo "Install cargo-audit: cargo install cargo-audit"

## Run the built-in self-test (no running daemon needed)
self-test: release
	./target/release/$(BINARY_NAME) --self-test

## Run the built-in doctor diagnostics
doctor: release
	./target/release/$(BINARY_NAME) --doctor

## Show this help
help:
	@echo "OpenSoma Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all           - check + test + build (default)"
	@echo "  build         - debug build"
	@echo "  release       - optimized release build"
	@echo "  test          - run all tests"
	@echo "  test-verbose  - run tests with output"
	@echo "  check         - compile check only"
	@echo "  clippy        - run linter"
	@echo "  fmt           - format code"
	@echo "  fmt-check     - check formatting"
	@echo "  clean         - remove build artifacts"
	@echo "  install       - install to PREFIX/bin"
	@echo "  uninstall     - remove from PREFIX/bin"
	@echo "  init          - generate default config.toml"
	@echo "  validate      - validate config.toml"
	@echo "  status        - query daemon status"
	@echo "  metrics       - print Prometheus metrics"
	@echo "  docker        - build Docker image"
	@echo "  docker-push   - push Docker image"
	@echo "  ci            - full CI pipeline (fmt+clippy+test+release+self-test)"
	@echo "  audit         - security vulnerability audit"
	@echo "  self-test     - run built-in pipeline self-test"
	@echo "  doctor        - run diagnostics"
	@echo "  loc           - count lines of code"
	@echo "  version       - print version"
	@echo "  help          - show this help"
