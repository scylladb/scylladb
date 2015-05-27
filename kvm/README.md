# seastar-in-kvm
Create a VM for Seastar development environment

# Why we need this
SeaStar scores muximum performance with DPDK, but it cannot live with existing NIC driver/Linux kernel network stack.
Also it directly accesses NIC device, it's bit hard to try it on remote node.

seastar-in-kvm offers Fedora VM with SeaStar + DPDK without setup, it's easiest way to begin SeaStar application development.

### Prerequire

On Fedora 21:
```
yum install @virtualization
systemctl enable libvirtd
systemctl start libvirtd
yum install libguestfs-tools-c virt-install
```

### How to build & run
```
./build.sh
./register.sh
virsh start seastar-dev && virsh console seastar-dev
(Try login as 'seastar' after firstboot.sh finished, Fedora will ask new password for the user)
```

### Usage of the VM

Wait until finish running setup script on first startup.
Then login as 'seastar', login prompt will ask for entering new password.

After login to seastar, initialize DPDK module by following instruction:
```
sudo su - # entering root user
resize    # extend console to actual terminal window size
export TERM=xterm-256color # set terminal type
cd ~/dpdk
./tools/setup.sh

# input numbers by following order:
(type 9 to re-compile DPDK)
(type 12 to insert IGB UIO module)
(type 15, then input "64" to setup hugepage mappings)
(type 18, then input PCI device id something like "0000:xx:yy.z",
which is shown at 'Network devices using DPDK-compatible driver')
(type 30 to exit)

cd ~/seastar
# httpd example
env LD_LIBRARY_PATH=~/dpdk/x86_64-native-linuxapp-gcc/lib/ \
./build/release/apps/httpd/httpd --network-stack native --dpdk-pmd --csum-offload off
```


