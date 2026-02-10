#!/bin/bash
set -e

# Install wasm-opt
BINARYEN_VERSION="version_125"
cd /tmp
curl -sSL -o binaryen-${BINARYEN_VERSION}-x86_64-linux.tar.gz https://github.com/WebAssembly/binaryen/releases/download/${BINARYEN_VERSION}/binaryen-${BINARYEN_VERSION}-x86_64-linux.tar.gz
tar -xvzf binaryen-${BINARYEN_VERSION}-x86_64-linux.tar.gz binaryen-${BINARYEN_VERSION}/bin/wasm-opt
cp binaryen-${BINARYEN_VERSION}/bin/wasm-opt /usr/local/bin/
rm -rf binaryen-${BINARYEN_VERSION}*

# Setup Rust for WebAssembly
cd /app
rustup target add wasm32-unknown-unknown

# Compile - Keep in sync with Makefile.toml
mkdir -p ../dist
rm -rf ../dist/native.wasm
cargo build --release --target wasm32-unknown-unknown
cp -a target/wasm32-unknown-unknown/release/platformatic_kafka.wasm target/native-release.wasm
cp -a target/native-release.wasm ../dist/native.wasm

# Optimize the WebAssembly binary
wasm-opt --enable-bulk-memory-opt -O4 --converge -o ../dist/native.wasm ../dist/native.wasm
