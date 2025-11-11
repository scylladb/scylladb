#!/bin/bash

# install contracts compiler
CONTRACT_CMPILER_DEST_DIR=/opt/compiler-explorer/clang-contracts
if [ ! -d ${CONTRACT_CMPILER_DEST_DIR} ]; then
    echo "compiler-explorer..."
    TEMP=$(mktemp -d)
    pushd "$TEMP" > /dev/null || exit 1
    INFRADIR=compiler-explorer-infra
    git clone --depth 1 https://github.com/compiler-explorer/infra.git $INFRADIR
    echo compier-explorer-infra cloned to ${TEMP}/$INFRADIR
    cd $INFRADIR || exit 1
    make ce
    ./bin/ce_install --dest ${CONTRACT_CMPILER_DEST_DIR} --enable nightly install compilers/c++/nightly/clang ericwf-contracts-trunk
    if [ $? -ne 0 ]; then
        echo "Error: ce_install failed"
        popd > /dev/null || exit 1
        exit 1
    fi
    echo "compiler-explorer contracts compiler installed to ${CONTRACT_CMPILER_DEST_DIR}"
    rm -rf "$TEMP"
    popd > /dev/null || exit 1
fi

