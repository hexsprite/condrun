# condrun — Makefile wrapper around cargo
#
# Default install prefix is $HOME/.local (user-writable, no sudo, on most PATHs).
# Override with PREFIX=/usr/local for system-wide install (needs sudo).
#
#   make install                 # → ~/.local/bin/condrun
#   make install PREFIX=/usr/local  # → /usr/local/bin/condrun (sudo)
#   make uninstall

PREFIX ?= $(HOME)/.local
BINDIR := $(PREFIX)/bin
CARGO  ?= cargo
BIN    := condrun
TARGET := target/release/$(BIN)

.PHONY: all build release debug install uninstall test check fmt clippy clean help

all: build

build release:
	$(CARGO) build --release

debug:
	$(CARGO) build

$(TARGET): build

install: $(TARGET)
	@mkdir -p "$(BINDIR)"
	install -m 0755 "$(TARGET)" "$(BINDIR)/$(BIN)"
	@echo "installed: $(BINDIR)/$(BIN)"

uninstall:
	@rm -f "$(BINDIR)/$(BIN)"
	@echo "removed: $(BINDIR)/$(BIN)"

test:
	$(CARGO) test

check:
	$(CARGO) check

fmt:
	$(CARGO) fmt

clippy:
	$(CARGO) clippy -- -D warnings

clean:
	$(CARGO) clean

help:
	@echo "Targets:"
	@echo "  build     — cargo build --release (default)"
	@echo "  install   — copy release binary to \$$(PREFIX)/bin (default: ~/.local/bin)"
	@echo "  uninstall — remove installed binary"
	@echo "  test      — cargo test"
	@echo "  check     — cargo check"
	@echo "  fmt       — cargo fmt"
	@echo "  clippy    — cargo clippy with -D warnings"
	@echo "  clean     — cargo clean"
	@echo ""
	@echo "Variables:"
	@echo "  PREFIX=$(PREFIX)"
	@echo "  BINDIR=$(BINDIR)"
