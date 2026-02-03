#!/bin/bash

set -e

# Configuration options:
# ENABLE_COMPRESSION=1 - Build with compression support (snappy, zlib, bz2, lz4, zstd)
# PORTABLE=0 - Enable native CPU optimizations (SIMD, AVX, etc.) for better performance
ENABLE_COMPRESSION=${ENABLE_COMPRESSION:-0}
PORTABLE=${PORTABLE:-1}

echo "Installing RocksDB dependencies..."
echo "Compression support: $([ "$ENABLE_COMPRESSION" = "1" ] && echo "ENABLED" || echo "DISABLED")"
echo "Portable build: $([ "$PORTABLE" = "1" ] && echo "YES (cross-CPU compatible)" || echo "NO (native CPU optimizations)")"

# Detect OS
if [ -f /etc/debian_version ]; then
    # Debian/Ubuntu
    echo "Detected Debian/Ubuntu system"
    apt-get update

    # Base dependencies
    apt-get install -y \
        build-essential \
        cmake \
        git \
        libgflags-dev

    # Optional compression dependencies
    if [ "$ENABLE_COMPRESSION" = "1" ]; then
        apt-get install -y \
            libsnappy-dev \
            zlib1g-dev \
            libbz2-dev \
            liblz4-dev \
            libzstd-dev
    fi
else
    echo "Unsupported OS. Please install RocksDB manually."
    exit 1
fi

# Download and build RocksDB
ROCKSDB_VERSION="v9.0.0"
BUILD_DIR="/tmp/rocksdb-build"

echo "Downloading RocksDB ${ROCKSDB_VERSION}..."
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}
cd ${BUILD_DIR}

git clone --depth 1 --branch ${ROCKSDB_VERSION} https://github.com/facebook/rocksdb.git
cd rocksdb

echo "Building RocksDB shared library..."

# Disable compression if not enabled
if [ "$ENABLE_COMPRESSION" != "1" ]; then
    export ROCKSDB_DISABLE_SNAPPY=1
    export ROCKSDB_DISABLE_ZLIB=1
    export ROCKSDB_DISABLE_BZIP=1
    export ROCKSDB_DISABLE_LZ4=1
    export ROCKSDB_DISABLE_ZSTD=1
fi

# Build lean RocksDB shared library
# PORTABLE=1 ensures compatibility across different CPU architectures
# PORTABLE=0 enables native CPU optimizations (SIMD, AVX, etc.)
# DEBUG_LEVEL=0 creates optimized build
# -Os optimizes for size
make shared_lib -j$(nproc) \
    PORTABLE=${PORTABLE} \
    DEBUG_LEVEL=0 \
    EXTRA_CXXFLAGS="-fPIC -Os -mno-avx512f" \
    EXTRA_LDFLAGS="-Wl,--strip-all"

echo "Installing RocksDB..."
cp librocksdb.so* /usr/local/lib/
cp -r include/rocksdb /usr/local/include/

# Update library cache
if [ "$(uname)" != "Darwin" ]; then
    ldconfig
fi

echo "RocksDB installation completed!"
