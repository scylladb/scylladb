#!/bin/bash -e

. /etc/os-release

print_usage() {
    echo "build_reloc.sh --jobs 2"
    echo "  --jobs  specify number of jobs"
    echo "  --clean clean build directory"
    echo "  --compiler  C++ compiler path"
    echo "  --nodeps    skip installing dependencies"
    exit 1
}

JOBS=
CLEAN=
COMPILER=
NODEPS=
while [ $# -gt 0 ]; do
    case "$1" in
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
        "--nodeps")
            NODEPS=yes
            shift 1
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
is_debian_variant() {
    [ -f /etc/debian_version ]
}


if [ ! -e reloc/build_reloc.sh ]; then
    echo "run build_reloc.sh in top of scylla dir"
    exit 1
fi

NINJA=$(which ninja-build) &&:
if [ -z "$NINJA" ]; then
    NINJA=$(which ninja) &&:
fi
if [ -z "$NINJA" ]; then
    echo "ninja not found."
    exit 1
fi

if [ "$CLEAN" = "yes" ]; then
    rm -rf build
fi

if [ -f build/release/scylla-package.tar.gz ]; then
    rm build/release/scylla-package.tar.gz
fi

if [ -z "$NODEPS" ]; then
    sudo ./install-dependencies.sh
fi

FLAGS="--with=scylla --with=iotune --mode=release"
if [ -n "$COMPILER" ]; then
    FLAGS="$FLAGS --compiler $COMPILER"
fi
./configure.py $FLAGS
$NINJA $JOBS build/release/scylla-package.tar.gz
