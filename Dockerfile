# syntax=docker/dockerfile:1
# Multi-platform Dockerfile for CGSim
# Builds on both ARM (Mac) and AMD64 (Windows/Linux)


FROM ubuntu:22.04
LABEL org.opencontainers.image.source=https://github.com/REDWOOD24/CGSim
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
    git \
    build-essential \
    cmake \
    sqlite3 \
    libsqlite3-dev \
    libboost-all-dev \
    hdf5-tools \
    libhdf5-dev \
    libspdlog-dev \
    nlohmann-json3-dev && \
    rm -rf /var/lib/apt/lists/*

# ----------------------------
# Build and install SimGrid v3.36
# ----------------------------
RUN git clone https://framagit.org/simgrid/simgrid.git /opt/simgrid && \
    cd /opt/simgrid && \
    git checkout v3.36 && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j$(nproc) && \
    make install

# ----------------------------
# Build and install SimGrid File System Module v0.2
# ----------------------------
RUN git clone https://github.com/simgrid/file-system-module.git /opt/fsmod && \
    cd /opt/fsmod && \
    git checkout v0.2 && \
    find . -type f -name "FileSystem.hpp" -exec sed -i 's/1024/8192/g' {} \; && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j$(nproc) && \
    make install

# ----------------------------
# Clone, build, and install CGSim
# ----------------------------
RUN git clone https://github.com/REDWOOD24/CGSim.git /opt/CGSim && \
    cd /opt/CGSim && \
    mkdir output && \
    git checkout CGSim-Refactor && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j$(nproc) && \
    make install && \
    cd /opt/CGSim/dispatch_plugins/simple-test-plugin && \
    rm -rf build && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make

WORKDIR /opt/CGSim
CMD ["build/cg-sim", "-c", "config-files/config.json"]