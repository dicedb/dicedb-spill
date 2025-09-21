#!/bin/bash

set -e

echo "Installing RocksDB dependencies..."

# Detect OS
if [ -f /etc/debian_version ]; then
    # Debian/Ubuntu
    echo "Detected Debian/Ubuntu system"
    sudo apt-get update
    sudo apt-get install -y build-essential libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev git
elif [ -f /etc/redhat-release ]; then
    # RHEL/CentOS/Fedora
    echo "Detected RHEL/CentOS/Fedora system"
    sudo yum install -y gcc-c++ gflags-devel snappy-devel zlib-devel bzip2-devel lz4-devel libzstd-devel git
elif [ "$(uname)" == "Darwin" ]; then
    # macOS
    echo "Detected macOS system"
    brew install gflags snappy lz4 zstd
else
    echo "Unsupported OS. Please install RocksDB manually."
    exit 1
fi

# Download and build RocksDB
ROCKSDB_VERSION="v8.5.3"
BUILD_DIR="/tmp/rocksdb-build"

echo "Downloading RocksDB ${ROCKSDB_VERSION}..."
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}
cd ${BUILD_DIR}

git clone --depth 1 --branch ${ROCKSDB_VERSION} https://github.com/facebook/rocksdb.git
cd rocksdb

echo "Building RocksDB..."
make shared_lib -j$(nproc)

echo "Installing RocksDB..."
sudo make install-shared

# Update library cache
if [ "$(uname)" != "Darwin" ]; then
    sudo ldconfig
fi

echo "RocksDB installation completed!"
echo "You can now compile the infcache module with: make"
