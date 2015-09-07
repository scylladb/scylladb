#!/bin/sh -e

if [ -f /dev/md0 ]; then
    echo "RAID already constructed."
    exit 1
fi
mdadm --create --verbose /dev/md0 --level=0 -c256 --raid-devices=2 /dev/xvdb /dev/xvdc 
blockdev --setra 65536 /dev/md0
mkfs.xfs /dev/md0 -f
echo "DEVICE /dev/xvdb /dev/xvdc" > /etc/mdadm.conf
mdadm --detail --scan >> /etc/mdadm.conf 
mkdir /data
echo "/dev/md0 /data xfs noatime 0 0" >> /etc/fstab
mount /data
mkdir -p /data/data
mkdir -p /data/commitlog
chown scylla:scylla /data/*
/usr/lib/scylla/setup_yaml.py
systemctl disable scylla-setup.service
systemctl enable scylla-server.service
systemctl start scylla-server.service
