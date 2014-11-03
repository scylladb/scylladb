### Set up a tap device for seastar
tap=tap0
bridge=virbr0
user=`whoami`
sudo tunctl -d $tap
sudo ip tuntap add mode tap dev $tap user $user one_queue vnet_hdr
sudo ifconfig $tap up
sudo brctl addif $bridge $tap
sudo brctl stp $bridge off
sudo modprobe vhost-net
sudo chown $user.$user /dev/vhost-net
sudo brctl show $bridge
sudo ifconfig $bridge
