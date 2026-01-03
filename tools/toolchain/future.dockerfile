# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Dockerfile to build a toolchain with recent GCC and Clang/LLVM on Fedora Rawhide.
# This can be used to look for regressions or incompatibilities in the toolchain under
# development.
#
# to build:
#   podman build -t future-toolchain -f tools/toolchain/future.dockerfile .
#
# to use:
#   ./tools/toolchain/dbuild --image future-toolchain -it --

FROM registry.fedoraproject.org/fedora:rawhide

RUN echo install_weak_deps=False >> /etc/dnf/dnf.conf

# Install dependencies using bind-mounted source tree (read-only from build context)
RUN --mount=type=bind,source=.,target=/src \
    dnf -y update && \
    cd /src && ./install-dependencies.sh

# Build dependencies for GCC and Clang
RUN dnf -y install git flex texinfo gmp-devel mpfr-devel libmpc-devel zlib-devel \
    python3 libedit-devel libxml2-devel ncurses-devel swig

# Build GCC from git
ARG GCC_VERSION=master
RUN git clone --depth=1 --branch="${GCC_VERSION}" git://gcc.gnu.org/git/gcc.git /tmp/gcc-src && \
    mkdir /tmp/gcc-build && cd /tmp/gcc-build && \
    /tmp/gcc-src/configure \
        --prefix=/usr/local \
        --enable-languages=c,c++ \
        --disable-multilib \
        --disable-bootstrap \
        --disable-libsanitizer \
        --enable-libstdcxx-threads \
        --with-system-zlib && \
    make -j$(nproc) && \
    make install && \
    rm -rf /tmp/gcc-src /tmp/gcc-build

# Update library cache for new libstdc++
RUN echo "/usr/local/lib64" > /etc/ld.so.conf.d/local.conf && ldconfig

# Build Clang/LLVM from git, configured to use GCC's libstdc++ from /usr/local
ARG LLVM_VERSION=main
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then LLVM_TARGET=X86; \
    elif [ "$ARCH" = "aarch64" ]; then LLVM_TARGET=AArch64; \
    else echo "Unsupported architecture: $ARCH" && exit 1; fi && \
    git clone --depth=1 --branch="${LLVM_VERSION}" https://github.com/llvm/llvm-project.git /tmp/llvm-src && \
    mkdir /tmp/llvm-build && cd /tmp/llvm-build && \
    cmake -G Ninja /tmp/llvm-src/llvm \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_C_COMPILER=clang \
        -DCMAKE_CXX_COMPILER=clang++ \
        -DLLVM_ENABLE_PROJECTS="clang;lld;clang-tools-extra" \
        -DLLVM_ENABLE_RUNTIMES="compiler-rt" \
        -DLLVM_TARGETS_TO_BUILD="${LLVM_TARGET};WebAssembly" \
        -DLLVM_USE_LINKER=lld \
        -DCLANG_DEFAULT_CXX_STDLIB=libstdc++ \
        -DCLANG_DEFAULT_RTLIB=libgcc \
        -DCLANG_DEFAULT_LINKER=lld \
        -DLLVM_BUILD_LLVM_DYLIB=ON \
        -DLLVM_LINK_LLVM_DYLIB=ON \
        -DLLVM_INCLUDE_BENCHMARKS=OFF \
        -DLLVM_INCLUDE_EXAMPLES=OFF \
        -DLLVM_INCLUDE_TESTS=OFF && \
    ninja && \
    ninja install && \
    rm -rf /tmp/llvm-src /tmp/llvm-build

# Ensure clang uses the correct GCC toolchain by default
# Config files are searched at <driver-path>/../etc/<driver-name>.cfg
RUN mkdir -p /usr/local/etc && \
    echo "--gcc-toolchain=/usr/local" > /usr/local/etc/clang.cfg && \
    cp /usr/local/etc/clang.cfg /usr/local/etc/clang++.cfg

# Verify clang finds the correct libstdc++ headers and library
RUN echo '#include <string>' > /tmp/test.cpp && \
    /usr/local/bin/clang++ -v -c /tmp/test.cpp -o /tmp/test.o 2>&1 | grep -E "usr/local.*(include|lib)" && \
    rm -f /tmp/test.cpp /tmp/test.o

CMD /bin/bash
