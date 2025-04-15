#!/bin/bash -ue

trap 'echo "error $? in $0 line $LINENO"' ERR

case "${CLANG_BUILD}" in
    "SKIP")
        echo "CLANG_BUILD: ${CLANG_BUILD}"
        exit 0
        ;;
    "INSTALL" | "INSTALL_FROM")
        echo "CLANG_BUILD: ${CLANG_BUILD}"
        if [[ -z "${CLANG_ARCHIVES}" ]]; then
            echo "CLANG_ARCHIVES not specified"
            exit 1
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

# deserialize CLANG_ARCHIVES from string
declare -A CLANG_ARCHIVES_ARRAY=()
while IFS=":" read -r key val; do
    CLANG_ARCHIVES_ARRAY["$key"]="$val"
done < <(echo "$CLANG_ARCHIVES" | tr ' ' '\n')


CLANG_ARCHIVE="${CLANG_ARCHIVES_ARRAY[${ARCH}]}"
if [[ -z ${CLANG_ARCHIVE} ]]; then
    echo "CLANG_ARCHIVE not detected"
    exit 1
fi
echo "CLANG_ARCHIVE: ${CLANG_ARCHIVE}"
if [[ "${ARCH}" = "x86_64" ]]; then
    LLVM_TARGET_ARCH=X86
    LLVM_CXX_FLAGS="-march=x86-64-v3"
elif [[ "${ARCH}" = "aarch64" ]]; then
    LLVM_TARGET_ARCH=AArch64
    # Based on https://community.arm.com/arm-community-blogs/b/tools-software-ides-blog/posts/compiler-flags-across-architectures-march-mtune-and-mcpu
    # and https://github.com/aws/aws-graviton-getting-started/blob/main/c-c%2B%2B.md
    LLVM_CXX_FLAGS="-march=armv8.2-a+crc+crypto"
else
    echo "Unsupported architecture: ${ARCH}"
    exit 1
fi

SCYLLA_DIR=/mnt
CLANG_ROOT_DIR="${SCYLLA_DIR}"/clang_build
CLANG_CHECKOUT_NAME=llvm-project-"${ARCH}"
CLANG_BUILD_DIR="${CLANG_ROOT_DIR}"/"${CLANG_CHECKOUT_NAME}"
CLANG_SYSROOT_NAME=optimized_clang-"${ARCH}"
CLANG_SYSROOT_DIR="${CLANG_ROOT_DIR}"/"${CLANG_SYSROOT_NAME}"

SCYLLA_BUILD_DIR=build_profile
SCYLLA_NINJA_FILE=build_profile.ninja
SCYLLA_BUILD_DIR_FULLPATH="${SCYLLA_DIR}"/"${SCYLLA_BUILD_DIR}"
SCYLLA_NINJA_FILE_FULLPATH="${SCYLLA_DIR}"/"${SCYLLA_NINJA_FILE}"

# Which LLVM release to build in order to compile Scylla
LLVM_CLANG_TAG=19.1.7
CLANG_SUFFIX=19

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
    -DCMAKE_CXX_FLAGS="${LLVM_CXX_FLAGS}"
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
    rm -rf "${CLANG_SYSROOT_DIR}"
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

    mkdir -p "${CLANG_SYSROOT_DIR}"
    DESTDIR="${CLANG_SYSROOT_DIR}" ninja -C build install-distribution-stripped
    cd "${CLANG_ROOT_DIR}"
    tar -C "${CLANG_SYSROOT_NAME}" -cpzf "${CLANG_ARCHIVE}" .
fi

# make sure it is correct archive, before extracting to /
set +e
tar -tpf "${CLANG_ARCHIVE}" ./usr/local/bin/clang > /dev/null 2>&1
if [[ $? -ne 0 ]]; then
    echo "Unable to detect prebuilt clang on ${CLANG_ARCHIVE}, aborted."
    exit 1
fi
set -e
tar -C / -xpzf "${CLANG_ARCHIVE}"
dnf remove -y clang clang-libs
# above package removal might have removed those symbolic links, which will cause ccache not to work later on. Manually restore them.
ln -sf /usr/bin/ccache /usr/lib64/ccache/clang
ln -sf /usr/bin/ccache /usr/lib64/ccache/clang++
