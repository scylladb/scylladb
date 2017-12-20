#!/bin/bash
. /etc/os-release

if [ "$VERSION_ID" = "8" ]; then
    apt-get -y install gnupg-curl ca-certificates
    apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key
elif [ "$VERSION_ID" = "9" ]; then
    apt-get -y install dirmngr curl ca-certificates
    curl -fsSL https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-stretch/Debian_9.0/Release.key | apt-key add -
else
    echo "Unsupported distribution."
    exit 1
fi
apt-get update
