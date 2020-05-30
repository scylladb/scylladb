#!/bin/bash

. /etc/os-release

release_major=$(echo $VERSION_ID|sed -e 's/^\([0-9]*\)[^0-9]*.*/\1/')

if [ ! -f /etc/yum.repos.d/epel.repo ]; then
    yum -y install epel-release || yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-"$release_major".noarch.rpm
fi
if [ ! -f /usr/bin/fakeroot ]; then
    yum -y install fakeroot
fi
