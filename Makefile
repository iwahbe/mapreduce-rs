
.PHONY = test clean cargo docs docs-open
CC = clang
OUT = mapreduce

RUST_DEBUG_LOC = -L./target/debug -lconcurrency_mapreduce
RUST_RELEASE_LOC = -L./target/release -lconcurrency_mapreduce

CARGO_VERSION := $(shell cargo --version 2>/dev/null)
CLANG_VERSION := $(shell clang --version 2>/dev/null | head -1)
CURL_VERSION := $(shell curl --version   2>/dev/null | head -1)

DRIVER = comp_test.c


build: cargo
	cargo build
	${CC} ${DRIVER} ${RUST_DEBUG_LOC} -o ${OUT}

release: cargo
	cargo build --release
	${CC} ${DRIVER} ${RUST_RELEASE_LOC} -o ${OUT}

test: build
	cargo test

clean: cargo
	@ cargo clean
	@ rm -r *.dSYM 2>/dev/null || true
	@ rm *.h.gch 2>/dev/null || true
	@ rm ${OUT} 2>/dev/null || true
	@ rm *.ghc 2>/dev/null || true

pre-req: cargo clang

cargo:
	@echo "${PATH}" | grep ".cargo/bin" > /dev/null || \
		(echo "${HOME}/cargo/bin added to PATH" && export PATH="${PATH}:${HOME}/cargo/bin")
ifdef CARGO_VERSION
	@echo "Found Cargo: ${CARGO_VERSION}"
else
ifdef CURL_VERSION
	@echo "Found curl: ${CURL_VERSION}"
	@echo "Installing rust"
	@curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh && \
	(cargo --version 2>/dev/null || (echo "Cargo still not found!!!" && false))
else
	@echo "Could not find cargo or curl in PATH."
	@echo "Please install rust manually: https://www.rust-lang.org/tools/install"
endif
endif

clang:
ifdef CLANG_VERSION
	@echo "Found clang: ${CLANG_VERSION}"
else
	@echo "Could not find clang in PATH"
	false
endif

docs: cargo
	cargo doc --document-private-items

docs-open: docs
	cargo doc --document-private-items --open
