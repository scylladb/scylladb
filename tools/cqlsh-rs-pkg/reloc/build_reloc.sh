#!/bin/bash -e

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

# Pinned cqlsh-rs release version for pre-built binary download
CQLSH_RS_VERSION="0.5.11"

ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  RUST_TARGET="x86_64-unknown-linux-musl" ;;
    aarch64) RUST_TARGET="aarch64-unknown-linux-musl" ;;
    *)       echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

RELEASE_URL="https://github.com/scylladb/cqlsh-rs/releases/download/v${CQLSH_RS_VERSION}/cqlsh-rs-${CQLSH_RS_VERSION}-${RUST_TARGET}.tar.gz"

mkdir -p bin

if $VERBOSE; then
    echo "Downloading cqlsh-rs v${CQLSH_RS_VERSION} for ${RUST_TARGET}..."
fi

curl -sSfL "$RELEASE_URL" | tar xz -C bin --strip-components=1
chmod +x bin/cqlsh-rs

printf "version=%s" $VERSION > build.properties

dist/debian/debian_files_gen.py
scripts/create-relocatable-package.py --version $VERSION --binary bin/cqlsh-rs "$DEST"
