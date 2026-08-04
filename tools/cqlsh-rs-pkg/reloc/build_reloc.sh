#!/bin/bash -e

# Without pipefail a failed download piped into a succeeding command would go
# unnoticed, and we would package whatever partial output it produced.
set -o pipefail

. /etc/os-release

print_usage() {
    echo "build_reloc.sh --clean"
    echo "  --clean clean build directory"
    echo "  --version V  product-version-release string (overriding SCYLLA-VERSION-GEN)"
    echo "  --verbose more chatty. I am quiet by default"
    exit 1
}

CLEAN=
VERSION_OVERRIDE=
VERBOSE=false
while [ $# -gt 0 ]; do
    case "$1" in
        "--clean")
            CLEAN=yes
            shift 1
            ;;
        "--version")
            VERSION_OVERRIDE="$2"
            shift 2
            ;;
        "--nodeps")
            # Accepted for backward compatibility with cmake/build_submodule.cmake
            shift 1
            ;;
        "--verbose")
            VERBOSE=true
            shift 1
            ;;
            *)
            print_usage
            ;;
    esac
done

VERSION=$(./SCYLLA-VERSION-GEN ${VERSION_OVERRIDE:+ --version "$VERSION_OVERRIDE"})
# the former command should generate build/SCYLLA-PRODUCT-FILE and some other version
# related files
PRODUCT=$(cat build/SCYLLA-PRODUCT-FILE)
DEST="build/$PRODUCT-cqlsh-$VERSION.$(uname -m).tar.gz"

if [ ! -e reloc/build_reloc.sh ]; then
    echo "run build_reloc.sh in top of cqlsh-rs-pkg dir"
    exit 1
fi

if [ "$CLEAN" = "yes" ]; then
    rm -rf build
    rm -rf ../cqlsh-rs/target
fi

if [ -f "$DEST" ]; then
    rm "$DEST"
fi

# Pinned cqlsh-rs release and the digests of its published assets.  Parsed
# rather than sourced, so the pin file stays plain data.
VERSION_FILE=./cqlsh-rs.version

read_pin() {
    sed -n "s/^$1=//p" "$VERSION_FILE" | head -1
}

ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  RUST_TARGET="x86_64-unknown-linux-musl" ;;
    aarch64) RUST_TARGET="aarch64-unknown-linux-musl" ;;
    *)       echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

CQLSH_RS_VERSION=$(read_pin cqlsh_rs_version)
SHA256=$(read_pin "cqlsh_rs_sha256_$ARCH")

if [ -z "$CQLSH_RS_VERSION" ] || [ -z "$SHA256" ]; then
    echo "$VERSION_FILE is missing cqlsh_rs_version or cqlsh_rs_sha256_$ARCH"
    exit 1
fi

# The integration tests in test/cqlsh-rs run against the tools/cqlsh-rs
# submodule, while the package ships this pre-built release.  If the two ever
# drift we would be testing code we do not ship, so refuse to build.  The
# submodule is not needed to build the package itself, so only check when it is
# actually checked out.
submodule_manifest="../cqlsh-rs/Cargo.toml"
if [ -f "$submodule_manifest" ]; then
    submodule_version=$(sed -n '/^\[package\]/,/^\[/s/^version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$submodule_manifest" | head -1)
    if [ "$submodule_version" != "$CQLSH_RS_VERSION" ]; then
        echo "cqlsh-rs version mismatch: cqlsh-rs.version pins $CQLSH_RS_VERSION but the"
        echo "tools/cqlsh-rs submodule is at $submodule_version."
        echo "Run tools/cqlsh-rs-pkg/update-cqlsh-rs.sh to bump both together."
        exit 1
    fi
fi

RELEASE_URL="https://github.com/scylladb/cqlsh-rs/releases/download/v${CQLSH_RS_VERSION}/cqlsh-rs-${CQLSH_RS_VERSION}-${RUST_TARGET}.tar.gz"

mkdir -p bin

if $VERBOSE; then
    echo "Downloading cqlsh-rs v${CQLSH_RS_VERSION} for ${RUST_TARGET}..."
fi

# Download to a file and verify it before unpacking, so a corrupted or
# substituted asset never reaches the package.
tarball=$(mktemp)
trap 'rm -f "$tarball"' EXIT
curl -fSL -o "$tarball" "$RELEASE_URL"
echo "${SHA256}  ${tarball}" | sha256sum --check --quiet
tar xz -C bin --strip-components=1 -f "$tarball"
chmod +x bin/cqlsh-rs

printf "version=%s" $VERSION > build.properties

dist/debian/debian_files_gen.py
scripts/create-relocatable-package.py --version $VERSION --binary bin/cqlsh-rs "$DEST"
