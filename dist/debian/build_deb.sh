#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_deb.sh -target <codename> --dist --rebuild-dep"
    echo "  --target target distribution codename"
    echo "  --dist  create a public distribution package"
    echo "  --no-clean  don't rebuild pbuilder tgz"
    exit 1
}
install_deps() {
    echo Y | sudo mk-build-deps
    DEB_FILE=`ls *-build-deps*.deb`
    sudo gdebi -n $DEB_FILE
    sudo rm -f $DEB_FILE
    sudo dpkg -P ${DEB_FILE%%_*.deb}
}

DIST=0
TARGET=
NO_CLEAN=0
while [ $# -gt 0 ]; do
    case "$1" in
        "--dist")
            DIST=1
            shift 1
            ;;
        "--target")
            TARGET=$2
            shift 2
            ;;
        "--no-clean")
            NO_CLEAN=1
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

if [ ! -e dist/debian/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi
if [ "$(arch)" != "x86_64" ]; then
    echo "Unsupported architecture: $(arch)"
    exit 1
fi

if [ -e debian ] || [ -e build/release ]; then
    sudo rm -rf debian build
    mkdir build
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
if [ ! -f /usr/sbin/pbuilder ]; then
    pkg_install pbuilder
fi
if [ ! -f /usr/bin/dh_testdir ]; then
    pkg_install debhelper
fi

CODENAME=`lsb_release -c|awk '{print $2}'`

if [ -z "$TARGET" ]; then
    if is_debian_variant; then
        if [ ! -f /usr/bin/lsb_release ]; then
            pkg_install lsb-release
        fi
        TARGET=$CODENAME
    else
        echo "Please specify target"
        exit 1
    fi
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/')
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
echo $VERSION > version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-server ../scylla-server_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz 

cp -a dist/debian/debian debian
cp dist/common/sysconfig/scylla-server debian/scylla-server.default
cp dist/debian/changelog.in debian/changelog
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/changelog
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" debian/changelog
sed -i -e "s/@@CODENAME@@/$TARGET/g" debian/changelog
cp dist/debian/rules.in debian/rules
cp dist/debian/control.in debian/control
cp dist/debian/scylla-server.install.in debian/scylla-server.install
cp dist/debian/scylla-conf.preinst.in debian/scylla-conf.preinst
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/scylla-conf.preinst
if [ "$TARGET" = "jessie" ]; then
    cp dist/debian/scylla-server.cron.d debian/
    sed -i -e "s/@@REVISION@@/1~$TARGET/g" debian/changelog
    sed -i -e "s/@@DH_INSTALLINIT@@//g" debian/rules
    sed -i -e "s#@@COMPILER@@#/opt/scylladb/bin/g++-7#g" debian/rules
    sed -i -e "s/@@BUILD_DEPENDS@@/libsystemd-dev, g++-7-scylla72, libunwind-dev, scylla-antlr35, scylla-libthrift010-dev, scylla-antlr35-c++-dev, libboost-program-options1.63-dev, libboost-filesystem1.63-dev, libboost-system1.63-dev, libboost-thread1.63-dev, libboost-test1.63-dev/g" debian/control
    sed -i -e "s/@@DEPENDS@@//g" debian/control
    sed -i -e "s#@@INSTALL@@##g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_D@@#dist/common/systemd/scylla-housekeeping-daily.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_R@@#dist/common/systemd/scylla-housekeeping-restart.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@FTDOTTIMER@@#dist/common/systemd/scylla-fstrim.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@SYSCTL@@#dist/debian/sysctl.d/99-scylla.conf etc/sysctl.d#g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_SAVE_COREDUMP@@#dist/debian/scripts/scylla_save_coredump usr/lib/scylla#g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_DELAY_FSTRIM@@#dist/debian/scripts/scylla_delay_fstrim usr/lib/scylla#g" debian/scylla-server.install
elif [ "$TARGET" = "stretch" ]; then
    cp dist/debian/scylla-server.cron.d debian/
    sed -i -e "s/@@REVISION@@/1~$TARGET/g" debian/changelog
    sed -i -e "s/@@DH_INSTALLINIT@@//g" debian/rules
    sed -i -e "s#@@COMPILER@@#/opt/scylladb/bin/g++-7#g" debian/rules
    sed -i -e "s/@@BUILD_DEPENDS@@/libsystemd-dev, g++-7-scylla72, libunwind-dev, antlr3, scylla-libthrift010-dev, scylla-antlr35-c++-dev, libboost-program-options1.62-dev, libboost-filesystem1.62-dev, libboost-system1.62-dev, libboost-thread1.62-dev, libboost-test1.62-dev/g" debian/control
    sed -i -e "s/@@DEPENDS@@//g" debian/control
    sed -i -e "s#@@INSTALL@@##g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_D@@#dist/common/systemd/scylla-housekeeping-daily.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_R@@#dist/common/systemd/scylla-housekeeping-restart.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@FTDOTTIMER@@#dist/common/systemd/scylla-fstrim.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@SYSCTL@@#dist/debian/sysctl.d/99-scylla.conf etc/sysctl.d#g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_SAVE_COREDUMP@@#dist/debian/scripts/scylla_save_coredump usr/lib/scylla#g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_DELAY_FSTRIM@@#dist/debian/scripts/scylla_delay_fstrim usr/lib/scylla#g" debian/scylla-server.install
elif [ "$TARGET" = "trusty" ]; then
    cp dist/debian/scylla-server.cron.d debian/
    sed -i -e "s/@@REVISION@@/0ubuntu1~$TARGET/g" debian/changelog
    sed -i -e "s/@@DH_INSTALLINIT@@/--upstart-only/g" debian/rules
    sed -i -e "s/@@COMPILER@@/g++-7/g" debian/rules
    sed -i -e "s/@@BUILD_DEPENDS@@/g++-7, libunwind8-dev, scylla-antlr35, scylla-libthrift010-dev, scylla-antlr35-c++-dev, scylla-libboost-program-options163-dev, scylla-libboost-filesystem163-dev, scylla-libboost-system163-dev, scylla-libboost-thread163-dev, scylla-libboost-test163-dev/g" debian/control
    sed -i -e "s/@@DEPENDS@@/hugepages, num-utils/g" debian/control
    sed -i -e "s#@@INSTALL@@#dist/debian/sudoers.d/scylla etc/sudoers.d#g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_D@@##g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_R@@##g" debian/scylla-server.install
    sed -i -e "s#@@FTDOTTIMER@@##g" debian/scylla-server.install
    sed -i -e "s#@@SYSCTL@@#dist/debian/sysctl.d/99-scylla.conf etc/sysctl.d#g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_SAVE_COREDUMP@@#dist/debian/scripts/scylla_save_coredump usr/lib/scylla#g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_DELAY_FSTRIM@@#dist/debian/scripts/scylla_delay_fstrim usr/lib/scylla#g" debian/scylla-server.install
elif [ "$TARGET" = "xenial" ]; then
    sed -i -e "s/@@REVISION@@/0ubuntu1~$TARGET/g" debian/changelog
    sed -i -e "s/@@DH_INSTALLINIT@@//g" debian/rules
    sed -i -e "s/@@COMPILER@@/g++-7/g" debian/rules
    sed -i -e "s/@@BUILD_DEPENDS@@/libsystemd-dev, g++-7, libunwind-dev, antlr3, scylla-libthrift010-dev, scylla-antlr35-c++-dev, scylla-libboost-program-options163-dev, scylla-libboost-filesystem163-dev, scylla-libboost-system163-dev, scylla-libboost-thread163-dev, scylla-libboost-test163-dev/g" debian/control
    sed -i -e "s/@@DEPENDS@@/hugepages, /g" debian/control
    sed -i -e "s#@@INSTALL@@##g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_D@@#dist/common/systemd/scylla-housekeeping-daily.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_R@@#dist/common/systemd/scylla-housekeeping-restart.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@FTDOTTIMER@@#dist/common/systemd/scylla-fstrim.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@SYSCTL@@##g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_SAVE_COREDUMP@@##g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_DELAY_FSTRIM@@##g" debian/scylla-server.install
elif [ "$TARGET" = "yakkety" ] || [ "$TARGET" = "zesty" ] || [ "$TARGET" = "artful" ]; then
    sed -i -e "s/@@REVISION@@/0ubuntu1~$TARGET/g" debian/changelog
    sed -i -e "s/@@DH_INSTALLINIT@@//g" debian/rules
    sed -i -e "s/@@COMPILER@@/g++-7/g" debian/rules
    sed -i -e "s/@@BUILD_DEPENDS@@/libsystemd-dev, g++-7, libunwind-dev, antlr3, scylla-libthrift010-dev, scylla-antlr35-c++-dev, libboost-program-options-dev, libboost-filesystem-dev, libboost-system-dev, libboost-thread-dev, libboost-test-dev/g" debian/control
    sed -i -e "s/@@DEPENDS@@/hugepages, /g" debian/control
    sed -i -e "s#@@INSTALL@@##g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_D@@#dist/common/systemd/scylla-housekeeping-daily.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@HKDOTTIMER_R@@#dist/common/systemd/scylla-housekeeping-restart.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@FTDOTTIMER@@#dist/common/systemd/scylla-fstrim.timer /lib/systemd/system#g" debian/scylla-server.install
    sed -i -e "s#@@SYSCTL@@##g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_SAVE_COREDUMP@@##g" debian/scylla-server.install
    sed -i -e "s#@@SCRIPTS_DELAY_FSTRIM@@##g" debian/scylla-server.install
else
    echo "Unknown distribution: $TARGET"
fi
if [ $DIST -gt 0 ]; then
    sed -i -e "s#@@ADDHKCFG@@#conf/housekeeping.cfg etc/scylla.d/#g" debian/scylla-server.install
else
    sed -i -e "s#@@ADDHKCFG@@##g" debian/scylla-server.install
fi
cp dist/common/systemd/scylla-server.service.in debian/scylla-server.service
sed -i -e "s#@@SYSCONFDIR@@#/etc/default#g" debian/scylla-server.service
cp dist/common/systemd/scylla-housekeeping-daily.service debian/scylla-server.scylla-housekeeping-daily.service
cp dist/common/systemd/scylla-housekeeping-restart.service debian/scylla-server.scylla-housekeeping-restart.service
cp dist/common/systemd/scylla-fstrim.service debian/scylla-server.scylla-fstrim.service
cp dist/common/systemd/node-exporter.service debian/scylla-server.node-exporter.service

cp ./dist/debian/pbuilderrc ~/.pbuilderrc
if [ $NO_CLEAN -eq 0 ]; then
    sudo rm -fv /var/cache/pbuilder/scylla-server-$TARGET.tgz
    sudo -E DIST=$TARGET /usr/sbin/pbuilder clean
    sudo -E DIST=$TARGET /usr/sbin/pbuilder create
fi
sudo -E DIST=$TARGET /usr/sbin/pbuilder update
sudo -E DIST=$TARGET pdebuild --buildresult build/debs
