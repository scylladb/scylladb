#!/bin/bash -uex

# Which Scylla branch to train on
SCYLLA_BRANCH=master

# Which LLVM release to build
LLVM_TAG=llvmorg-15.0.7

if [ -d "/optimized_clang" ];
then
	DIR="/optimized_clang"
else
	DIR="${PWD}/optimized_clang"
	mkdir -p "${DIR}"
fi
cd "${DIR}"

# Download the scylla codebase for training.
# Scylla's dependencies are already installed.
git clone https://github.com/scylladb/scylla --branch ${SCYLLA_BRANCH} --depth=1
git -C scylla submodule update --init --depth=1

# Clone, patch and bootstrap the newest Clang and BOLT.
#git clone https://github.com/llvm/llvm-project --branch main --depth=1 stage-0
git clone https://github.com/llvm/llvm-project --branch main --depth=1 stage-0
cd stage-0

cmake -B build -S llvm -DLLVM_ENABLE_PROJECTS='clang;lld;bolt' -DCMAKE_BUILD_TYPE=Release -DLLVM_TARGETS_TO_BUILD=X86 -G Ninja -DLLVM_ENABLE_RUNTIMES='compiler-rt' -DCOMPILER_RT_BUILD_SANITIZERS=Off -DCOMPILER_RT_DEFAULT_TARGET_ONLY=On -DLLVM_INCLUDE_BENCHMARKS=No -DLLVM_INCLUDE_EXAMPLES=No -DLLVM_INCLUDE_TESTS=No
cmake --build build --

USE_NEW_COMPILER=(-DCMAKE_C_COMPILER="$DIR/stage-0/build/bin/clang" -DCMAKE_CXX_COMPILER="$DIR/stage-0/build/bin/clang++" -DLLVM_USE_LINKER="$DIR/stage-0/build/bin/ld.lld")
COMMON_OPTS=(-DLLVM_ENABLE_PROJECTS='clang' -DCMAKE_BUILD_TYPE=Release -DLLVM_TARGETS_TO_BUILD=X86 -G Ninja -DLLVM_ENABLE_LTO=Thin -DLLVM_BUILD_RUNTIME=No -DCLANG_DEFAULT_PIE_ON_LINUX=OFF -DLLVM_INCLUDE_BENCHMARKS=No -DLLVM_INCLUDE_EXAMPLES=No -DLLVM_INCLUDE_TESTS=No)

# Build a PGO-optimized compiler using the boostrapped compiler.
# Choose a version compatible with your system.
# If you change it, remember to change clang-15 in the later part of the script to the appropriate name as well.
git fetch --depth=1 origin tag ${LLVM_TAG}
git worktree add ../stage-1 ${LLVM_TAG}
cd "../stage-1"
cmake -B build -S llvm "${USE_NEW_COMPILER[@]}" "${COMMON_OPTS[@]}" -DLLVM_BUILD_INSTRUMENTED=IR
cmake --build build -- bin/clang

cd ../scylla
rm -rf build build.ninja
./configure.py --mode=release --compiler=$(realpath ../stage-1/build/bin)/clang++ --disable-dpdk
LLVM_PROFILE_FILE=$(realpath ../stage-1)/build/profiles/ir-%p-%m.profraw ninja build/release/scylla

cd ../stage-1
../stage-0/build/bin/llvm-profdata merge build/profiles/ir-*.profraw -output=ir.prof
rm -r build
cmake -B build -S llvm "${USE_NEW_COMPILER[@]}" "${COMMON_OPTS[@]}" -DLLVM_BUILD_INSTRUMENTED=CSIR -DLLVM_PROFDATA_FILE=$(realpath ir.prof)
cmake --build build -- bin/clang

cd ../scylla
rm -rf build build.ninja
./configure.py --mode=release --compiler=$(realpath ../stage-1/build/bin)/clang++ --disable-dpdk
LLVM_PROFILE_FILE=$(realpath ../stage-1)/build/profiles/csir-%p-%m.profraw ninja build/release/scylla

cd ../stage-1
../stage-0/build/bin/llvm-profdata merge build/profiles/csir-*.profraw -output=csir.prof
../stage-0/build/bin/llvm-profdata merge ir.prof csir.prof -output=combined.prof
rm -r build
# -DLLVM_LIBDIR_SUFFIX=64 for Fedora compatibility
cmake -B build -S llvm "${USE_NEW_COMPILER[@]}" "${COMMON_OPTS[@]}" -DLLVM_PROFDATA_FILE=$(realpath combined.prof) -DCMAKE_EXE_LINKER_FLAGS="-Wl,--emit-relocs" -DCMAKE_INSTALL_PREFIX=/usr -DLLVM_LIBDIR_SUFFIX=64
cmake --build build -- bin/clang

mv build/bin/clang-15 build/bin/clang-15.prebolt
mkdir build/profiles
../stage-0/build/bin/llvm-bolt build/bin/clang-15.prebolt -o build/bin/clang-15 -instrument -instrumentation-file=$(realpath build/profiles)/prof -instrumentation-file-append-pid -conservative-instrumentation

cd ../scylla
rm -rf build build.ninja
./configure.py --mode=release --compiler=$(realpath ../stage-1/build/bin)/clang++ --disable-dpdk
ninja build/release/scylla

cd ../stage-1
rm -rf ../scylla
../stage-0/build/bin/merge-fdata build/profiles/*.fdata > prof.fdata
../stage-0/build/bin/llvm-bolt build/bin/clang-15.prebolt -o build/bin/clang-15 -data=prof.fdata -reorder-functions=hfsort -reorder-blocks=ext-tsp -split-functions -split-all-cold -split-eh -dyno-stats

# Then use the below to replace your inferior compiler
sudo mv /usr/bin/clang-15 /usr/bin/clang-15.old
sudo cp $DIR/stage-1/build/bin/clang-15 /usr/bin

cd ../
rm -rf stage-0
