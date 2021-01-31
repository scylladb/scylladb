#!/bin/bash -e

releasever=`rpm -q --provides $(rpm -q --whatprovides "system-release(releasever)") | grep "system-release(releasever)"| uniq |  cut -d ' ' -f 3`

# Can ignore error since we only needed when files exists
cp /etc/yum/vars/* build/installroot/etc/yum/vars/ ||:

# run yum in non-root mode using fakeroot
fakeroot yum -y install --downloadonly --releasever="$releasever" --installroot=`pwd`/build/installroot --downloaddir=build/offline_installer scylla sudo chrony net-tools kernel-tools mdadm xfsprogs
