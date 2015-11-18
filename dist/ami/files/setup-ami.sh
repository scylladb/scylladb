#!/bin/sh -e

setenforce 0
sed -e "s/enforcing/disabled/" /etc/sysconfig/selinux > /tmp/selinux
mv /tmp/selinux /etc/sysconfig/
dnf update -y
mv /home/fedora/scylla.repo /etc/yum.repos.d/
dnf install -y scylla-server scylla-server-debuginfo scylla-jmx scylla-tools
dnf install -y mdadm xfsprogs
cp /home/fedora/coredump.conf /etc/systemd/coredump.conf
mv /home/fedora/scylla-setup.service /usr/lib/systemd/system
mv /home/fedora/scylla-setup.sh /usr/lib/scylla
chmod a+rx /usr/lib/scylla/scylla-setup.sh
mv /home/fedora/scylla-ami /usr/lib/scylla/scylla-ami
chmod a+rx /usr/lib/scylla/scylla-ami/ds2_configure.py
systemctl enable scylla-setup.service
grep -v ' - mounts' /etc/cloud/cloud.cfg > /tmp/cloud.cfg
mv /tmp/cloud.cfg /etc/cloud/cloud.cfg
