#!/bin/sh

virt-install --import --noreboot --name seastar-dev --vcpus 4 --ram 4096 --disk path=`realpath seastar-dev.qcow2`,format=qcow2,bus=virtio --accelerate --network=network:default,model=virtio --serial pty --cpu host --rng=/dev/random
