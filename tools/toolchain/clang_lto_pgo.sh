#!/bin/bash -uex

if [ -z "${CLANG_ARCH}" ]; then
    echo "CLANG_ARCH is not defined"
elif [ "${CLANG_ARCH}" != "amd64" ]; then
    CLANG_ARCH=Aarch64
    echo "clang optimization is only supported for x86_64 (amd64) at the moment."
else
    CLANG_ARCH=X86
fi

if [ -z "${CLANG_BUILD}" ]; then
	echo "Skip building optimized clang"
	exit 0
elif [ "${CLANG_BUILD}" = "SKIP" ]; then
        echo "Skip building optimized clang"
        exit 0
elif [ "${CLANG_BUILD}" = "BUILD" ]; then
	echo "build optimized clang"
elif [ "${CLANG_BUILD}" = "INSTALL" ]; then
	echo "build and install optimized clang"
else
	echo "Not sure what to do with ${CLANG_BUILD}"
	exit 1
fi

if [ -d "/optimized_clang" ];
then
        DIR="/optimized_clang"
else
        DIR="${PWD}/optimized_clang"
        mkdir -p "${DIR}"
fi
cd "${DIR}"

STAGE0_BIN=$DIR/stage-0-${CLANG_ARCH}/build/bin
STAGE1_BIN=$DIR/stage-1-${CLANG_ARCH}/build/bin

# Which Scylla branch to train on
#SCYLLA_BRANCH=scylla-5.2.0-rc3
SCYLLA_BRANCH=master

# Which LLVM release to build to compile Scylla
LLVM_SCYLLA_TAG=16.0.1

# Which LLVM release to use to build clang.
LLVM_CLANG_TAG=16.0.1

rpm -qa |grep clang

# Clone, patch and bootstrap the newest Clang and BOLT.
git clone https://github.com/llvm/llvm-project --branch llvmorg-${LLVM_CLANG_TAG} --depth=1 stage-0-${CLANG_ARCH}
cd stage-0-${CLANG_ARCH}

USE_CURRENT_COMPILER=(-DCMAKE_C_COMPILER="/usr/bin/clang" -DCMAKE_CXX_COMPILER"=/usr/bin/clang++" -DLLVM_USE_LINKER="/usr/bin/ld")
COMMON_OPTS=(-DCMAKE_BUILD_TYPE=Release -DLLVM_TARGETS_TO_BUILD=${CLANG_ARCH} -DLLVM_TARGET_ARCH=${CLANG_ARCH} -G Ninja -DLLVM_INCLUDE_BENCHMARKS=OFF -DLLVM_INCLUDE_EXAMPLES=OFF -DLLVM_INCLUDE_TESTS=OFF -DLLVM_ENABLE_BINDINGS=OFF)

echo "stage-0: build the bootstrapped compiler"

cmake -B build -S llvm  "${USE_CURRENT_COMPILER[@]}" "${COMMON_OPTS[@]}" -DLLVM_ENABLE_PROJECTS="clang;lld;bolt" -DLLVM_ENABLE_RUNTIMES='compiler-rt' -DCOMPILER_RT_BUILD_SANITIZERS=OFF -DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON
cmake --build build --

USE_NEW_COMPILER=(-DCMAKE_C_COMPILER="$STAGE0_BIN/clang" -DCMAKE_CXX_COMPILER="$STAGE0_BIN/clang++" -DLLVM_USE_LINKER="$STAGE0_BIN/ld.lld")
NEW_COMMON_OPTS=(-DLLVM_ENABLE_PROJECTS="clang" -DLLVM_ENABLE_LTO=Thin -DCLANG_DEFAULT_PIE_ON_LINUX=OFF -DLLVM_BUILD_TOOLS=OFF)
#  -DLLVM_BUILD_RUNTIME=OFF

echo "stage-1: Build a PGO-optimized compiler using the boostrapped compiler"

# Build a PGO-optimized compiler using the boostrapped compiler.
git fetch --depth=1 origin tag llvmorg-${LLVM_SCYLLA_TAG}
git worktree add ../stage-1-${CLANG_ARCH} llvmorg-${LLVM_SCYLLA_TAG}
cd ../stage-1-${CLANG_ARCH}
cmake -B build -S llvm "${USE_NEW_COMPILER[@]}" "${COMMON_OPTS[@]}" "${NEW_COMMON_OPTS[@]}" -DLLVM_BUILD_INSTRUMENTED=IR
time cmake --build build -- bin/clang

echo "cmake stage-1 output:"
ls -l $DIR/stage-1-${CLANG_ARCH}/build/bin/

cd ../
# Download the scylla codebase for training.
# Scylla's dependencies are already installed.
git clone https://github.com/scylladb/scylla --branch ${SCYLLA_BRANCH} --depth=1 scylla-${CLANG_ARCH}
git -C scylla-${CLANG_ARCH} submodule update --init --depth=1 --recursive -f

#1st ScyllaDB compilation: gather a clang profile for PGO
cd scylla-${CLANG_ARCH}
rm -rf build build.ninja
./configure.py --mode=dev --c-compiler="$STAGE1_BIN/clang" --compiler="$STAGE1_BIN/clang++" --disable-dpdk
time ninja build/dev/scylla

#LLVM_PROFILE_FILE=$($DIR/stage-1-${CLANG_ARCH}/build/profiles/default_%p-%m.profraw ninja build/dev/scylla)

cd ../stage-1-${CLANG_ARCH}
$STAGE0_BIN/llvm-profdata merge $DIR/stage-1-${CLANG_ARCH}/build/profiles/default_*.profraw -output=ir.prof
rm -r build
cmake -B build -S llvm "${USE_NEW_COMPILER[@]}" "${COMMON_OPTS[@]}" "${NEW_COMMON_OPTS[@]}" -DLLVM_BUILD_INSTRUMENTED=CSIR -DLLVM_PROFDATA_FILE=$(realpath ir.prof)
time cmake --build build -- bin/clang

# Second compilation: gathering a clang profile for CSPGO
cd ../scylla-${CLANG_ARCH}
rm -rf build build.ninja
./configure.py --mode=dev --c-compiler="$STAGE1_BIN/clang" --compiler="$STAGE1_BIN/clang++" --disable-dpdk
time ninja build/dev/scylla
#LLVM_PROFILE_FILE=$DIR/stage-1-${CLANG_ARCH}/build/profiles/csir-%p-%m.profraw ninja build/dev/scylla

cd ../stage-1-${CLANG_ARCH}
$STAGE0_BIN/llvm-profdata merge build/csprofiles/default_*.profraw -output=csir.prof
$STAGE0_BIN/llvm-profdata merge ir.prof csir.prof -output=combined.prof
rm -r build
# -DLLVM_LIBDIR_SUFFIX=64 for Fedora compatibility
cmake -B build -S llvm "${USE_NEW_COMPILER[@]}" "${COMMON_OPTS[@]}" "${NEW_COMMON_OPTS[@]}" -DLLVM_PROFDATA_FILE=$(realpath combined.prof) -DCMAKE_EXE_LINKER_FLAGS="-Wl,--emit-relocs" -DCMAKE_INSTALL_PREFIX=/usr -DLLVM_LIBDIR_SUFFIX=64
time cmake --build build -- bin/clang


# BOLT phase - skip for now
#mv build/bin/clang build/bin/clang.prebolt
#mkdir -p build/profiles
#$STAGE0_BIN/llvm-bolt build/bin/clang.prebolt -o build/bin/clang --instrument --instrumentation-file=$DIR/stage-1-${CLANG_ARCH}/build/profiles/prof --instrumentation-file-append-pid --conservative-instrumentation

#3rd ScyllaDB compilation: gathering a clang profile for BOLT
cd ../scylla-${CLANG_ARCH}
rm -rf build build.ninja
./configure.py --mode=dev --c-compiler="$STAGE1_BIN/clang" --compiler="$STAGE1_BIN/clang++" --disable-dpdk
time ninja build/dev/scylla

#cd ../stage-1-${CLANG_ARCH}
#$STAGE0_BIN/merge-fdata build/profiles/*.fdata > prof.fdata
#$STAGE0_BIN/llvm-bolt build/bin/clang.prebolt -o build/bin/clang --data=prof.fdata --reorder-functions=hfsort --reorder-blocks=ext-tsp --split-functions --split-all-cold --split-eh --dyno-stats

rm -rf ../scylla-0-${CLANG_ARCH}

# Then use the below to replace your inferior compiler
if [ "${CLANG_BUILD}" = "INSTALL" ]; then
    sudo mv /usr/bin/clang-16 /usr/bin/clang-16.old || 1
    sudo cp $STAGE1_BIN/clang-16 /usr/bin
    echo "optimizaed clang was copied to /usr/bin"
else
    sudo cp $STAGE1_BIN/clang-16 $DIR/clang-16-${CLANG_ARCH}
    echo "optimized clang was copied"
fi

cd ../
rm -rf $DIR/stage-0-${CLANG_ARCH} $DIR/stage-1-${CLANG_ARCH} $DIR/scylla-${CLANG_ARCH}
