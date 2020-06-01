#!/bin/bash -e

. /etc/os-release

DEFAULT_MODE="release"

print_usage() {
    echo "Usage: build_reloc.sh [OPTION]..."
    echo ""
    echo "  --mode MODE             specify build mode (default: '$DEFAULT_MODE')"
    exit 1
}

MODE="$DEFAULT_MODE"
while [ $# -gt 0 ]; do
    case "$1" in
        "--mode")
            MODE=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

if [ ! -e reloc/build_reloc.sh ]; then
    echo "run build_reloc.sh in top of scylla dir"
    exit 1
fi

if [ ! -e build/$MODE/scylla ] || [ ! -e build/$MODE/iotune ]; then
    echo "run ./configure.py && ninja-build before running build_reloc.sh"
    exit 1
fi

BUILD_ID_END=$(readelf -SW build/$MODE/scylla | perl -ne '/.note.gnu.build-id *NOTE *[a-f,0-9]* *([a-f,0-9]*) *([a-f,0-9]*)/ && print ((hex $1) + (hex $2))')
if (($BUILD_ID_END > 4096))
then
    echo "build-id is not in the first page. It ends at offset $BUILD_ID_END"
    exit 1
fi

if [ ! -e build/SCYLLA-RELEASE-FILE ] || [ ! -e build/SCYLLA-VERSION-FILE ]; then
    ./SCYLLA-VERSION-GEN
fi

python3 -m compileall ./dist/common/scripts/ ./seastar/scripts/perftune.py ./tools/scyllatop
./dist/debian/debian_files_gen.py
./scripts/create-relocatable-package.py --mode $MODE build/$MODE/scylla-package.tar.gz
