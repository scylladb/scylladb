#!/bin/sh -e

if [ -b /dev/md0 ]; then
    echo "RAID already constructed."
    exit 1
fi

dnf update -y

DISKS=""
NR=0
for i in xvd{b..z}; do
    if [ -b /dev/$i ];then
        echo Found disk /dev/$i
        DISKS="$DISKS /dev/$i"
        NR=$((NR+1))
    fi
done

echo Creating RAID0 for scylla using $NR disk\(s\): $DISKS

if [ $NR -ge 1 ]; then
    mdadm --create --verbose --force --run /dev/md0 --level=0 -c256 --raid-devices=$NR $DISKS
    blockdev --setra 65536 /dev/md0
    mkfs.xfs /dev/md0 -f
    echo "DEVICE $DISKS" > /etc/mdadm.conf
    mdadm --detail --scan >> /etc/mdadm.conf
    UUID=`blkid /dev/md0 | awk '{print $2}'`
    mkdir /data
    echo "$UUID /data xfs noatime 0 0" >> /etc/fstab
    mount /data
else
   echo "WARN: Scylla is not using XFS to store data. Perforamnce will suffer." > /home/fedora/WARN_PLEASE_READ.TXT
fi

mkdir -p /data/data
mkdir -p /data/commitlog
chown scylla:scylla /data/*

CPU_NR=`cat /proc/cpuinfo |grep processor|wc -l`
if [ $CPU_NR -ge 8 ]; then
    NR=$((CPU_NR - 1))
    echo SCYLLA_ARGS=\"--cpuset 1-$NR  --smp $NR\" >> /etc/sysconfig/scylla-server
    echo SET_NIC=\"yes\" >> /etc/sysconfig/scylla-server
fi

/usr/lib/scylla/scylla-ami/ds2_configure.py
systemctl disable scylla-setup.service
systemctl enable scylla-server.service
systemctl start scylla-server.service
systemctl enable scylla-jmx.service
systemctl start scylla-jmx.service
