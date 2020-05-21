#!/bin/bash -e

PRODUCT=$(cat SCYLLA-PRODUCT-FILE)

. /etc/os-release
print_usage() {
    echo "build_deb.sh -target <codename> --dist --rebuild-dep --reloc-pkg build/release/scylla-package.tar.gz"
    echo "  --dist  create a public distribution package"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}

DIST="false"
TARGET=stable
RELOC_PKG=
while [ $# -gt 0 ]; do
    case "$1" in
        "--dist")
            DIST="true"
            shift 1
            ;;
        "--reloc-pkg")
            RELOC_PKG=$2
            shift 2
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
pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y $1
    elif is_debian_variant; then
        sudo apt-get install -y $1
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}

if [ ! -e SCYLLA-RELOCATABLE-FILE ]; then
    echo "do not directly execute build_deb.sh, use reloc/build_deb.sh instead."
    exit 1
fi

if [ "$(arch)" != "x86_64" ]; then
    echo "Unsupported architecture: $(arch)"
    exit 1
fi

if [ -z "$RELOC_PKG" ]; then
    print_usage
    exit 1
fi
if [ ! -f "$RELOC_PKG" ]; then
    echo "$RELOC_PKG is not found."
    exit 1
fi

if [ -e debian ]; then
    rm -rf debian
fi
if is_debian_variant; then
    sudo apt-get -y update
fi
# this hack is needed since some environment installs 'git-core' package, it's
# subset of the git command and doesn't works for our git-archive-all script.
if is_redhat_variant && [ ! -f /usr/libexec/git-core/git-submodule ]; then
    sudo yum install -y git
fi
if [ ! -f /usr/bin/git ]; then
    pkg_install git
fi
if [ ! -f /usr/bin/python ]; then
    pkg_install python
fi
if [ ! -f /usr/bin/debuild ]; then
    pkg_install devscripts
fi
if [ ! -f /usr/bin/dh_testdir ]; then
    pkg_install debhelper
fi
if [ ! -f /usr/bin/fakeroot ]; then
    pkg_install fakeroot
fi
if [ ! -f /usr/bin/pystache ] && [ ! -f /usr/local/bin/pystache ]; then
    if is_redhat_variant; then
        sudo yum install -y /usr/bin/pystache
    elif is_debian_variant; then
        sudo apt-get install -y python-pystache
    fi
fi
if [ ! -f /usr/bin/file ]; then
    pkg_install file
fi
if is_debian_variant && [ ! -f /usr/share/doc/python-pkg-resources/copyright ]; then
    sudo apt-get install -y python-pkg-resources
fi

if [ "$ID" = "ubuntu" ] && [ ! -f /usr/share/keyrings/debian-archive-keyring.gpg ]; then
    sudo apt-get install -y debian-archive-keyring
fi
if [ "$ID" = "debian" ] && [ ! -f /usr/share/keyrings/ubuntu-archive-keyring.gpg ]; then
    sudo apt-get install -y ubuntu-archive-keyring
fi

if [ -z "$TARGET" ]; then
    if is_debian_variant; then
        if [ ! -f /usr/bin/lsb_release ]; then
            pkg_install lsb-release
        fi
        TARGET=`lsb_release -c|awk '{print $2}'`
    else
        echo "Please specify target"
        exit 1
    fi
fi
RELOC_PKG_FULLPATH=$(readlink -f $RELOC_PKG)
RELOC_PKG_BASENAME=$(basename $RELOC_PKG)
SCYLLA_VERSION=$(cat SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/')
SCYLLA_RELEASE=$(cat SCYLLA-RELEASE-FILE)

ln -fv $RELOC_PKG_FULLPATH ../$PRODUCT-server_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz

cp -al dist/debian/debian debian
if [ "$PRODUCT" != "scylla" ]; then
    # rename all 'scylla-' prefixed artifacts in the debian folder to have the 
    # product name as a prefix
    find debian -maxdepth 1 -name "scylla-*" -exec bash -c 'mv $1 ${1/scylla-/$2-}' _ {} "$PRODUCT" \;
fi
ln -fv dist/common/sysconfig/scylla-server debian/$PRODUCT-server.default
REVISION="1"
MUSTACHE_DIST="\"debian\": true, \"product\": \"$PRODUCT\", \"$PRODUCT\": true"
pystache dist/debian/changelog.mustache "{ $MUSTACHE_DIST, \"version\": \"$SCYLLA_VERSION\", \"release\": \"$SCYLLA_RELEASE\", \"revision\": \"$REVISION\", \"codename\": \"$TARGET\" }" > debian/changelog
pystache dist/debian/rules.mustache "{ $MUSTACHE_DIST, \"housekeeping\": $DIST }" > debian/rules
pystache dist/debian/control.mustache "{ $MUSTACHE_DIST }" > debian/control
pystache dist/debian/scylla-conf.preinst.mustache "{ \"version\": \"$SCYLLA_VERSION\" }" > debian/$PRODUCT-conf.preinst
chmod a+rx debian/rules

if [ "$PRODUCT" != "scylla" ]; then
    SERVER_SERVICE_PREFIX="$PRODUCT-server."
fi
ln -fv dist/common/systemd/scylla-server.service debian/${SERVER_SERVICE_PREFIX}scylla-server.service
ln -fv dist/common/systemd/scylla-housekeeping-daily.service debian/$PRODUCT-server.scylla-housekeeping-daily.service
ln -fv dist/common/systemd/scylla-housekeeping-restart.service debian/$PRODUCT-server.scylla-housekeeping-restart.service
ln -fv dist/common/systemd/scylla-fstrim.service debian/$PRODUCT-server.scylla-fstrim.service
ln -fv dist/common/systemd/node-exporter.service debian/$PRODUCT-server.node-exporter.service

debuild -rfakeroot -us -uc
