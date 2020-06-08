#!/bin/bash -e

. /etc/os-release

COMMON_FLAGS="--cflags=-ffile-prefix-map=$PWD=."

DEFAULT_MODE="release"

print_usage() {
    echo "Usage: build_reloc.sh [OPTION]..."
    echo ""
    echo "  --configure-flags FLAGS specify extra build flags passed to 'configure.py' (common: '$COMMON_FLAGS')"
    echo "  --mode MODE             specify build mode (default: '$DEFAULT_MODE')"
    echo "  --jobs JOBS             specify number of jobs"
    echo "  --clean                 clean build directory"
    echo "  --compiler PATH         C++ compiler path"
    echo "  --c-compiler PATH       C compiler path"
    exit 1
}

FLAGS=""
MODE="$DEFAULT_MODE"
JOBS=
CLEAN=
COMPILER=
CCOMPILER=
while [ $# -gt 0 ]; do
    case "$1" in
        "--configure-flags")
            FLAGS=$2
            shift 2
            ;;
        "--mode")
            MODE=$2
            shift 2
            ;;
        "--jobs")
            JOBS="-j$2"
            shift 2
            ;;
        "--clean")
            CLEAN=yes
            shift 1
            ;;
        "--compiler")
            COMPILER=$2
            shift 2
            ;;
        "--c-compiler")
            CCOMPILER=$2
            shift 2
            ;;
        "--nodeps")
            shift 1
            ;;
        *)
            print_usage
            ;;
    esac
done

FLAGS="$COMMON_FLAGS $FLAGS"

if [ ! -e reloc/build_reloc.sh ]; then
    echo "run build_reloc.sh in top of scylla dir"
    exit 1
fi

if [ "$CLEAN" = "yes" ]; then
    rm -rf build
fi

if [ -f build/$MODE/scylla-package.tar.gz ]; then
    rm build/$MODE/scylla-package.tar.gz
fi

NINJA=$(which ninja-build) &&:
if [ -z "$NINJA" ]; then
    NINJA=$(which ninja) &&:
fi
if [ -z "$NINJA" ]; then
    echo "ninja not found."
    exit 1
fi

FLAGS="$FLAGS --mode=$MODE"
if [ -n "$COMPILER" ]; then
    FLAGS="$FLAGS --compiler $COMPILER"
fi
if [ -n "$CCOMPILER" ]; then
    FLAGS="$FLAGS --c-compiler $CCOMPILER"
fi
echo "Configuring with flags: '$FLAGS' ..."
./configure.py $FLAGS
python3 -m compileall ./dist/common/scripts/ ./seastar/scripts/perftune.py ./tools/scyllatop
$NINJA $JOBS build/$MODE/scylla-package.tar.gz
BUILD_ID_END=$(readelf -SW build/$MODE/scylla | perl -ne '/.note.gnu.build-id *NOTE *[a-f,0-9]* *([a-f,0-9]*) *([a-f,0-9]*)/ && print ((hex $1) + (hex $2))')
if (($BUILD_ID_END > 4096))
then
    echo "build-id is not in the first page. It ends at offset $BUILD_ID_END"
    exit 1
fi
