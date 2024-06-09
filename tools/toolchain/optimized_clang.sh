#!/bin/bash -uex

case "${CLANG_BUILD}" in
    "SKIP")
        echo "CLANG_BUILD: ${CLANG_BUILD}"
        exit 0
        ;;
    "INSTALL" | "INSTALL_FROM")
        echo "CLANG_BUILD: ${CLANG_BUILD}"
        if [[ -z "${CLANG_ARCHIVE}" ]]; then
            echo "CLANG_ARCHIVE not specified"
        fi
        ;;
    "")
        echo "CLANG_BUILD not specified"
        exit 1
        ;;
    *)
        echo "Invalid mode specified on CLANG_BUILD: ${CLANG_BUILD}"
        exit 1
        ;;
esac

ARCH="$(arch)"
if [[ "${ARCH}" = "x86_64" ]]; then
    LLVM_TARGET_ARCH=X86
elif [[ "${ARCH}" = "aarch64" ]]; then
    LLVM_TARGET_ARCH=AArch64
else
    echo "Unsupported architecture: ${ARCH}"
    exit 1
fi

SCYLLA_DIR=/mnt
CLANG_ROOT_DIR="${SCYLLA_DIR}"/clang_build
CLANG_CHECKOUT_NAME=llvm-project
CLANG_BUILD_DIR="${CLANG_ROOT_DIR}"/"${CLANG_CHECKOUT_NAME}"

SCYLLA_BUILD_DIR=build_profile
SCYLLA_NINJA_FILE=build_profile.ninja
SCYLLA_BUILD_DIR_FULLPATH="${SCYLLA_DIR}"/"${SCYLLA_BUILD_DIR}"
SCYLLA_NINJA_FILE_FULLPATH="${SCYLLA_DIR}"/"${SCYLLA_NINJA_FILE}"

# Which LLVM release to build in order to compile Scylla
LLVM_CLANG_TAG=18.1.6
CLANG_SUFFIX=18

CLANG_ARCHIVE=$(cd "${SCYLLA_DIR}" && realpath -m "${CLANG_ARCHIVE}")

CLANG_OPTS=(-DCMAKE_C_COMPILER="/usr/bin/clang" -DCMAKE_CXX_COMPILER="/usr/bin/clang++" -DLLVM_USE_LINKER="/usr/bin/ld.lld" -DCMAKE_BUILD_TYPE=Release -DLLVM_TARGETS_TO_BUILD="${LLVM_TARGET_ARCH};WebAssembly" -DLLVM_TARGET_ARCH="${LLVM_TARGET_ARCH}" -G Ninja -DLLVM_INCLUDE_BENCHMARKS=OFF -DLLVM_INCLUDE_EXAMPLES=OFF -DLLVM_INCLUDE_TESTS=OFF -DLLVM_ENABLE_BINDINGS=OFF -DLLVM_ENABLE_PROJECTS="clang;lld" -DLLVM_ENABLE_RUNTIMES="compiler-rt" -DLLVM_ENABLE_LTO=Thin -DCLANG_DEFAULT_PIE_ON_LINUX=OFF -DLLVM_BUILD_TOOLS=OFF -DLLVM_VP_COUNTERS_PER_SITE=6)
SCYLLA_OPTS=(--date-stamp "$(date "+%Y%m%d")" --debuginfo 1 --tests-debuginfo 1 --c-compiler="${CLANG_BUILD_DIR}/build/bin/clang" --compiler="${CLANG_BUILD_DIR}/build/bin/clang++" --build-dir="${SCYLLA_BUILD_DIR}" --out="${SCYLLA_NINJA_FILE}")

if [[ "${CLANG_BUILD}" = "INSTALL" ]]; then
    # Build a PGO-optimized compiler using the boostrapped compiler.
    rm -rf "${CLANG_BUILD_DIR}"
    git clone https://github.com/llvm/llvm-project --branch llvmorg-"${LLVM_CLANG_TAG}" --depth=1 "${CLANG_BUILD_DIR}"
    cd "${CLANG_BUILD_DIR}"
    cmake -B build -S llvm "${CLANG_OPTS[@]}" -DLLVM_BUILD_INSTRUMENTED=IR
    ninja -C build

    rm -rf "${SCYLLA_BUILD_DIR_FULLPATH}" "${SCYLLA_NINJA_FILE_FULLPATH}"
    cd "${SCYLLA_DIR}"
    ./configure.py "${SCYLLA_OPTS[@]}"
    LLVM_PROFILE_FILE="${CLANG_BUILD_DIR}"/build/profiles/default_%p-%m.profraw ninja -f "${SCYLLA_NINJA_FILE}" compiler-training

    cd "${CLANG_BUILD_DIR}"
    llvm-profdata merge "${CLANG_BUILD_DIR}"/build/profiles/default_*.profraw -output=ir.prof
    rm -rf build
    cmake -B build -S llvm "${CLANG_OPTS[@]}" -DLLVM_BUILD_INSTRUMENTED=CSIR -DLLVM_PROFDATA_FILE="$(realpath ir.prof)"
    ninja -C build

    # 2nd compilation: gathering a clang profile for CSPGO
    rm -rf "${SCYLLA_BUILD_DIR_FULLPATH}" "${SCYLLA_NINJA_FILE_FULLPATH}"
    cd "${SCYLLA_DIR}"
    ./configure.py "${SCYLLA_OPTS[@]}"
    LLVM_PROFILE_FILE="${CLANG_BUILD_DIR}"/build/profiles/csir-%p-%m.profraw ninja -f "${SCYLLA_NINJA_FILE}" compiler-training

    cd "${CLANG_BUILD_DIR}"
    llvm-profdata merge build/csprofiles/default_*.profraw -output=csir.prof
    llvm-profdata merge ir.prof csir.prof -output=combined.prof
    rm -rf build
    # -DLLVM_LIBDIR_SUFFIX=64 for Fedora compatibility
    cmake -B build -S llvm "${CLANG_OPTS[@]}" -DLLVM_PROFDATA_FILE="$(realpath combined.prof)" -DCMAKE_EXE_LINKER_FLAGS="-Wl,--emit-relocs" -DCMAKE_INSTALL_PREFIX=/usr/local -DLLVM_LIBDIR_SUFFIX=64
    ninja -C build

    #TODO: skipping BOLT for aarch64 for now, since it causes segfault
    if [[ "${ARCH}" != "aarch64" ]]; then
        # BOLT phase
        mv build/bin/clang-"${CLANG_SUFFIX}" build/bin/clang-"${CLANG_SUFFIX}".prebolt
        mkdir -p build/profiles
        llvm-bolt build/bin/clang-"${CLANG_SUFFIX}".prebolt -o build/bin/clang-"${CLANG_SUFFIX}" --instrument --instrumentation-file="${CLANG_BUILD_DIR}"/build/profiles/prof --instrumentation-file-append-pid --conservative-instrumentation

        # 3rd ScyllaDB compilation: gathering a clang profile for BOLT
        rm -rf "${SCYLLA_BUILD_DIR_FULLPATH}" "${SCYLLA_NINJA_FILE_FULLPATH}"
        cd "${SCYLLA_DIR}"
        ./configure.py "${SCYLLA_OPTS[@]}"
        ninja -f "${SCYLLA_NINJA_FILE}" compiler-training

        cd "${CLANG_BUILD_DIR}"
        merge-fdata build/profiles/*.fdata > prof.fdata
        llvm-bolt build/bin/clang-"${CLANG_SUFFIX}".prebolt -o build/bin/clang-"${CLANG_SUFFIX}" --data=prof.fdata --reorder-functions=hfsort --reorder-blocks=ext-tsp --split-functions --split-all-cold --split-eh --dyno-stats
    fi

    cd "${CLANG_ROOT_DIR}"
    rm -rf "${CLANG_BUILD_DIR}"/{build/profiles,*.prof,prof.fdata}
    tar -cpzf "${CLANG_ARCHIVE}" "${CLANG_CHECKOUT_NAME}"
elif [[ "${CLANG_BUILD}" = "INSTALL_FROM" ]]; then
    mkdir -p "${CLANG_ROOT_DIR}"
    tar -C "${CLANG_ROOT_DIR}" -xpzf "${CLANG_ARCHIVE}"
fi

cd "${CLANG_BUILD_DIR}"
mv /usr/bin/clang-"${CLANG_SUFFIX}" /usr/bin/clang-"${CLANG_SUFFIX}".orig
mv /usr/bin/lld /usr/bin/lld.orig
mv /usr/lib64/libLTO.so."${CLANG_SUFFIX}" /usr/lib64/libLTO.so."${CLANG_SUFFIX}".orig
install -Z -m755 "${CLANG_BUILD_DIR}"/build/bin/clang-"${CLANG_SUFFIX}" /usr/bin/clang-"${CLANG_SUFFIX}"
install -Z -m755 "${CLANG_BUILD_DIR}"/build/bin/lld /usr/bin/lld
install -Z -m755 "${CLANG_BUILD_DIR}"/build/lib64/libLTO.so."${CLANG_SUFFIX}" /usr/lib64/libLTO.so."${CLANG_SUFFIX}"
rm -rf "${CLANG_BUILD_DIR}" "${SCYLLA_BUILD_DIR_FULLPATH}" "${SCYLLA_NINJA_FILE_FULLPATH}"
