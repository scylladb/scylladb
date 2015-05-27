#!/bin/sh

rm -rf /tmp/seastar
cp -a ../ /tmp/seastar
virt-builder fedora-21 -o seastar-dev.qcow2 --format qcow2 --size 20G --hostname seastar-dev --install "@core" --update --selinux-relabel --copy-in /tmp/seastar:/root/ --firstboot scripts/bootstrap.sh
rm -rf /tmp/seastar
