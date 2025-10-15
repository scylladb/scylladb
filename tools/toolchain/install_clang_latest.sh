#!/bin/bash

# install contracts compiler
COMPILER_DEST_DIR=/opt/compiler-explorer/clang-latest
if [ ! -d ${COMPILER_DEST_DIR} ]; then
    echo "compiler-explorer..."
    TEMP=$(mktemp -d)
    pushd "$TEMP" > /dev/null || exit 1
    INFRADIR=compiler-explorer-infra
    git clone --depth 1 https://github.com/compiler-explorer/infra.git $INFRADIR
    echo compier-explorer-infra cloned to ${TEMP}/$INFRADIR
    cd $INFRADIR || exit 1
    make ce
    ./bin/ce_install --dest ${COMPILER_DEST_DIR} --enable nightly install compilers/c++/nightly/clang trunk
    if [ $? -ne 0 ]; then
        echo "Error: ce_install failed"
        popd > /dev/null || exit 1
        exit 1
    fi
    echo "compiler-explorer latest compiler installed to ${COMPILER_DEST_DIR}"
    rm -rf "$TEMP"
    popd > /dev/null || exit 1
fi

