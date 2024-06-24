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

CLANG_OPTS=(
    -G Ninja
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_C_COMPILER="/usr/bin/clang"
    -DCMAKE_CXX_COMPILER="/usr/bin/clang++"
    -DLLVM_USE_LINKER="/usr/bin/ld.lld"
    -DLLVM_TARGETS_TO_BUILD="${LLVM_TARGET_ARCH};WebAssembly"
    -DLLVM_TARGET_ARCH="${LLVM_TARGET_ARCH}"
    -DLLVM_INCLUDE_BENCHMARKS=OFF
    -DLLVM_INCLUDE_EXAMPLES=OFF
    -DLLVM_INCLUDE_TESTS=OFF
    -DLLVM_ENABLE_BINDINGS=OFF
    -DLLVM_ENABLE_PROJECTS="clang"
    -DLLVM_ENABLE_RUNTIMES="compiler-rt"
    -DLLVM_ENABLE_LTO=Thin
    -DCLANG_DEFAULT_PIE_ON_LINUX=OFF
    -DLLVM_BUILD_TOOLS=OFF
    -DLLVM_VP_COUNTERS_PER_SITE=6
    -DLLVM_BUILD_LLVM_DYLIB=ON
    -DLLVM_LINK_LLVM_DYLIB=ON
    -DCMAKE_INSTALL_PREFIX="/usr/local"
    -DLLVM_LIBDIR_SUFFIX=64
    -DLLVM_INSTALL_TOOLCHAIN_ONLY=ON
)
SCYLLA_OPTS=(
    --date-stamp "$(date "+%Y%m%d")"
    --debuginfo 1
    --tests-debuginfo 1
    --c-compiler="${CLANG_BUILD_DIR}/build/bin/clang"
    --compiler="${CLANG_BUILD_DIR}/build/bin/clang++"
    --build-dir="${SCYLLA_BUILD_DIR}"
    --out="${SCYLLA_NINJA_FILE}"
)

# Utilizing LLVM_DISTRIBUTION_COMPONENTS to avoid
# installing static libraries; inspired by Gentoo
_get_distribution_components() {
    local target
    ninja -t targets | grep -Po 'install-\K.*(?=-stripped:)' | while read -r target; do
        case $target in
            clang-libraries|distribution)
                continue
                ;;
            clang-tidy-headers)
                continue
                ;;
            clang|clangd|clang-*)
                ;;
            clang*|findAllSymbols)
                continue
                ;;
        esac
        echo $target
    done
}

if [[ "${CLANG_BUILD}" = "INSTALL" ]]; then
    rm -rf "${CLANG_BUILD_DIR}"
    git clone https://github.com/llvm/llvm-project --branch llvmorg-"${LLVM_CLANG_TAG}" --depth=1 "${CLANG_BUILD_DIR}"

    echo "[clang-stage1] build the compiler for collecting PGO profile"
    cd "${CLANG_BUILD_DIR}"
    rm -rf build
    cmake -B build -S llvm "${CLANG_OPTS[@]}" -DLLVM_BUILD_INSTRUMENTED=IR
    DISTRIBUTION_COMPONENTS=$(cd build && _get_distribution_components | paste -sd\;)
    test -n "${DISTRIBUTION_COMPONENTS}"
    CLANG_OPTS+=(-DLLVM_DISTRIBUTION_COMPONENTS="${DISTRIBUTION_COMPONENTS}")
    cmake -B build -S llvm "${CLANG_OPTS[@]}" -DLLVM_BUILD_INSTRUMENTED=IR
    ninja -C build

    echo "[scylla-stage1] gather a clang profile for PGO"
    rm -rf "${SCYLLA_BUILD_DIR_FULLPATH}" "${SCYLLA_NINJA_FILE_FULLPATH}"
    cd "${SCYLLA_DIR}"
    ./configure.py "${SCYLLA_OPTS[@]}"
    LLVM_PROFILE_FILE="${CLANG_BUILD_DIR}"/build/profiles/default_%p-%m.profraw ninja -f "${SCYLLA_NINJA_FILE}" compiler-training

    echo "[clang-stage2] build the compiler applied PGO profile and for collecting CSPGO profile"
    cd "${CLANG_BUILD_DIR}"
    llvm-profdata merge "${CLANG_BUILD_DIR}"/build/profiles/default_*.profraw -output=ir.prof
    rm -rf build
    cmake -B build -S llvm "${CLANG_OPTS[@]}" -DLLVM_BUILD_INSTRUMENTED=CSIR -DLLVM_PROFDATA_FILE="$(realpath ir.prof)"
    ninja -C build

    echo "[scylla-stage2] gathering a clang profile for CSPGO"
    rm -rf "${SCYLLA_BUILD_DIR_FULLPATH}" "${SCYLLA_NINJA_FILE_FULLPATH}"
    cd "${SCYLLA_DIR}"
    ./configure.py "${SCYLLA_OPTS[@]}"
    LLVM_PROFILE_FILE="${CLANG_BUILD_DIR}"/build/profiles/csir-%p-%m.profraw ninja -f "${SCYLLA_NINJA_FILE}" compiler-training

    echo "[clang-stage3] build the compiler applied CSPGO profile"
    cd "${CLANG_BUILD_DIR}"
    llvm-profdata merge build/csprofiles/default_*.profraw -output=csir.prof
    llvm-profdata merge ir.prof csir.prof -output=combined.prof
    rm -rf build
    # linker flags are needed for BOLT
    cmake -B build -S llvm "${CLANG_OPTS[@]}" -DLLVM_PROFDATA_FILE="$(realpath combined.prof)" -DCMAKE_EXE_LINKER_FLAGS="-Wl,--emit-relocs"
    ninja -C build

    cd "${CLANG_ROOT_DIR}"
    rm -rf "${CLANG_BUILD_DIR}"/{build/profiles,*.prof,prof.fdata}
    tar -cpzf "${CLANG_ARCHIVE}" "${CLANG_CHECKOUT_NAME}"
elif [[ "${CLANG_BUILD}" = "INSTALL_FROM" ]]; then
    mkdir -p "${CLANG_ROOT_DIR}"
    tar -C "${CLANG_ROOT_DIR}" -xpzf "${CLANG_ARCHIVE}"
fi

cd "${CLANG_BUILD_DIR}"
ninja -C build install-distribution-stripped
dnf remove -y clang clang-libs
