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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
RUNTIMES_FLAGS=""
if [[ "${ARCH}" = "x86_64" ]]; then
    LLVM_TARGET_ARCH=X86
    LLVM_CXX_FLAGS="-march=x86-64-v3"
elif [[ "${ARCH}" = "aarch64" ]]; then
    LLVM_TARGET_ARCH=AArch64
    # Based on https://community.arm.com/arm-community-blogs/b/tools-software-ides-blog/posts/compiler-flags-across-architectures-march-mtune-and-mcpu
    # and https://github.com/aws/aws-graviton-getting-started/blob/main/c-c%2B%2B.md
    LLVM_CXX_FLAGS="-march=armv8.2-a+crc+crypto"
    # The sanitizers' fast unwinder reads return addresses out of the frame
    # records on the stack. Where the unwound code was built with pointer
    # authentication - as everything Fedora ships for aarch64 is, it builds with
    # -mbranch-protection=standard - those are signed, and have to be stripped
    # before they can be used. compiler-rt does that (StackTrace::UnwindFast
    # calls STRIP_PAC_PC), but only if the runtime itself was built with branch
    # protection: sanitizer_ptrauth.h gates the strip on __ARM_FEATURE_PAC_DEFAULT.
    # So build the runtimes that way, otherwise every backtrace asan records at
    # malloc time is garbage and leak suppressions do not match against it.
    RUNTIMES_FLAGS="-mbranch-protection=standard"
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
LLVM_CLANG_TAG=22.1.8

# Installed libraries, and with them clang's resource directory, go to
# ${CMAKE_INSTALL_PREFIX}/lib${LLVM_LIBDIR_SUFFIX}. This is not architecture
# dependent; the architecture only appears in the per-target subdirectory of the
# resource dir (e.g. lib64/clang/22/lib/aarch64-unknown-linux-gnu).
LLVM_LIBDIR_SUFFIX=64

CLANG_ARCHIVE=$(cd "${SCYLLA_DIR}" && realpath -m "${CLANG_ARCHIVE}")

# CMAKE_CXX_FLAGS below tunes the compiler we are building for the machine it
# will run on; it does not reach the runtimes, which are configured as separate
# CMake projects and are compiled for whoever links against them. Flags for
# those have to be passed explicitly.
BUILTINS_CMAKE_ARGS="-DLLVM_LIBDIR_SUFFIX=${LLVM_LIBDIR_SUFFIX}"
RUNTIMES_CMAKE_ARGS=""
if [[ -n "${RUNTIMES_FLAGS}" ]]; then
    for flag_type in ASM C CXX; do
        BUILTINS_CMAKE_ARGS+=";-DCMAKE_${flag_type}_FLAGS=${RUNTIMES_FLAGS}"
        RUNTIMES_CMAKE_ARGS+="${RUNTIMES_CMAKE_ARGS:+;}-DCMAKE_${flag_type}_FLAGS=${RUNTIMES_FLAGS}"
    done
fi

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
    # clang-tools-extra is here for clangd, which developers need for editor
    # integration against this toolchain (a clangd built from a different LLVM
    # cannot read the module and PCH files this clang produces). It builds a
    # dozen other tools as well; _get_distribution_components below picks which
    # of them are installed.
    -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra"
    -DLLVM_ENABLE_RUNTIMES="compiler-rt"
    -DLLVM_ENABLE_LTO=Thin
    -DCLANG_DEFAULT_PIE_ON_LINUX=OFF
    -DLLVM_BUILD_TOOLS=OFF
    -DLLVM_VP_COUNTERS_PER_SITE=6
    -DLLVM_BUILD_LLVM_DYLIB=ON
    -DLLVM_LINK_LLVM_DYLIB=ON
    -DCMAKE_INSTALL_PREFIX="/usr/local"
    -DLLVM_LIBDIR_SUFFIX="${LLVM_LIBDIR_SUFFIX}"
    # The builtins sub-build of the runtimes is a standalone CMake project which
    # does not load LLVMConfig.cmake, so LLVM_LIBDIR_SUFFIX does not reach it
    # (unlike the sanitizers, which are configured via runtimes/CMakeLists.txt).
    # Without this, libclang_rt.builtins.a is installed into lib/clang/<ver>
    # while clang looks for it in its resource dir, lib${LLVM_LIBDIR_SUFFIX}/clang/<ver>,
    # and linking with --rtlib=compiler-rt fails.
    -DBUILTINS_CMAKE_ARGS="${BUILTINS_CMAKE_ARGS}"
    -DRUNTIMES_CMAKE_ARGS="${RUNTIMES_CMAKE_ARGS}"
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
    --use-profile=""
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
            # The tools from clang-tools-extra that are worth carrying: clangd
            # for editor and agent integration, clang-tidy (plus
            # clang-apply-replacements, which run-clang-tidy needs to apply
            # exported fixes) for linting, clang-include-cleaner for include
            # hygiene over a whole target, and clang-query for developing AST
            # matchers.
            clangd|clang-tidy|clang-apply-replacements|clang-include-cleaner|clang-query)
                ;;
            # The rest of clang-tools-extra. Nothing here is wired into an
            # editor: they are one-shot refactoring tools (clang-move,
            # clang-change-namespace, clang-reorder-fields), a documentation
            # generator (clang-doc), the pre-C++20 header modularization
            # checker (modularize), a preprocessor callback tracer (pp-trace),
            # and the include suggester whose index-file approach clangd's own
            # index superseded (clang-include-fixer and the find-all-symbols
            # tool that builds its index). They are built either way; this only
            # keeps them out of the archive.
            clang-change-namespace|clang-doc|clang-include-fixer|clang-move|clang-reorder-fields)
                continue
                ;;
            # These match none of the patterns below, so they would otherwise
            # reach the echo and be installed.
            find-all-symbols|modularize|pp-trace)
                continue
                ;;
            clang|clang-*)
                ;;
            clang*|findAllSymbols)
                continue
                ;;
        esac
        echo "$target"
    done
}

if [[ "${CLANG_BUILD}" = "INSTALL" ]]; then
    rm -rf "${CLANG_BUILD_DIR}"
    rm -rf "${CLANG_SYSROOT_DIR}"
    git clone https://github.com/llvm/llvm-project --branch llvmorg-"${LLVM_CLANG_TAG}" --depth=1 "${CLANG_BUILD_DIR}"

    # Backport codegen miscompilation fixes carried by Fedora's llvm package
    # (fedora-44, llvm 22.1.8). Only the fixes that affect the code clang
    # generates on x86_64/aarch64 are applied here; Fedora's distro-integration,
    # linker (lld is not built here), and other-target (s390x/ppc64le) patches
    # are intentionally omitted.
    #  - SDAG select-of-load fold (#208683): target-independent, affects x86_64 and aarch64
    #  - X86 EVEX compression for VPMOV*2M + KMOV (#198220): x86_64
    #
    # Also backport a clangd use-after-free that 22.1.8 has and 23.1.0 fixed:
    #  - clangd ModuleCache lifetime (#203952, fixes #203799). Since #164889,
    #    which is in 22.1.8, the preamble's CompilerInstance solely owns the
    #    in-memory buffers of the loaded module files. CapturedASTCtx did not
    #    retain the ModuleCache, so those buffers were freed with the
    #    CompilerInstance while ASTReader still held ArrayRefs into them (the
    #    TU_UPDATE_LEXICAL blobs). Indexing the preamble then walked freed
    #    memory and clangd crashed in ASTReader::FindExternalLexicalDecls. It
    #    only triggers when an imported .pcm contributes translation-unit level
    #    lexical decls, i.e. exactly when Scylla is built with C++20 modules.
    for patch in \
        0001-SDAG-Freeze-condition-in-select-of-load-fold-208683.patch \
        0001-X86-Fix-EVEX-compression-for-VPMOV-2M-KMOV-with-tied.patch \
        0001-clangd-Keep-ModuleCache-alive-for-captured-preamble-.patch
    do
        git -C "${CLANG_BUILD_DIR}" apply "${SCRIPT_DIR}/clang-patches/${patch}"
    done

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
    llvm-profdata merge build/profiles/csir-*.profraw -output=csir.prof
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
tar -tpf "${CLANG_ARCHIVE}" ./usr/local/bin/clang ./usr/local/bin/clangd > /dev/null 2>&1
if [[ $? -ne 0 ]]; then
    echo "Unable to detect prebuilt clang and clangd on ${CLANG_ARCHIVE}, aborted."
    exit 1
fi
set -e
tar -C / -xpzf "${CLANG_ARCHIVE}"

# Scylla links with --rtlib=compiler-rt, so the builtins have to be where clang
# looks for them, i.e. in its resource dir.
CLANG_RT_BUILTINS="$(/usr/local/bin/clang --rtlib=compiler-rt -print-libgcc-file-name)"
if [[ ! -f "${CLANG_RT_BUILTINS}" ]]; then
    echo "Installed clang is missing ${CLANG_RT_BUILTINS}, aborted."
    exit 1
fi
