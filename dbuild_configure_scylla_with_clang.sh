#!/bin/bash

./tools/toolchain/dbuild ./configure.py \
    --mode=dev \
    --compiler /opt/compiler-explorer/clang-contracts/clang-ericwf-contracts-trunk/bin/clang++ \
    --cflags="--config=${PWD}/tools/toolchain/contracts_compiler.cfg" \
    --ldflags="-rpath /opt/compiler-explorer/clang-contracts/clang-ericwf-contracts-trunk/lib/x86_64-unknown-linux-gnu/ -L/opt/compiler-explorer/clang-contracts/clang-ericwf-contracts-trunk/lib/x86_64-unknown-linux-gnu/ -lc++" 
