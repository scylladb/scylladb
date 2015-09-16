#!/bin/sh -e

if [ -f /dev/md0 ]; then
    echo "RAID already constructed."
    exit 1
fi
mdadm --create --verbose --force --run /dev/md0 --level=0 -c256 --raid-devices=2 /dev/xvdb /dev/xvdc
blockdev --setra 65536 /dev/md0
mkfs.xfs /dev/md0 -f
echo "DEVICE /dev/xvdb /dev/xvdc" > /etc/mdadm.conf
mdadm --detail --scan >> /etc/mdadm.conf 
UUID=`blkid /dev/md0 | awk '{print $2}'`
mkdir /data
echo "$UUID /data xfs noatime 0 0" >> /etc/fstab
mount /data
mkdir -p /data/data
mkdir -p /data/commitlog
chown scylla:scylla /data/*
/usr/lib/scylla/ComboAMI/ds2_configure.py
systemctl disable scylla-setup.service
systemctl enable scylla-server.service
systemctl start scylla-server.service
systemctl enable scylla-jmx.service
systemctl start scylla-jmx.service
