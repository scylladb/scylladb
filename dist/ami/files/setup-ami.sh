#!/bin/sh -e
dnf update -y
dnf install -y /home/fedora/*.rpm
dnf install -y mdadm xfsprogs
cp /home/fedora/coredump.conf /etc/systemd/coredump.conf
mv /home/fedora/scylla-setup.service /usr/lib/systemd/system
mv /home/fedora/scylla-setup.sh /usr/lib/scylla
chmod a+rx /usr/lib/scylla/scylla-setup.sh
mv /home/fedora/scylla-ami /usr/lib/scylla/scylla-ami
chmod a+rx /usr/lib/scylla/scylla-ami/ds2_configure.py
systemctl enable scylla-setup.service
sed -e 's!/var/lib/scylla/data!/data/data!' -e 's!commitlog_directory: /var/lib/scylla/commitlog!commitlog_directory: /data/commitlog!' /var/lib/scylla/conf/scylla.yaml > /tmp/scylla.yaml
mv /tmp/scylla.yaml /var/lib/scylla/conf
grep -v ' - mounts' /etc/cloud/cloud.cfg > /tmp/cloud.cfg
mv /tmp/cloud.cfg /etc/cloud/cloud.cfg
