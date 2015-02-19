#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

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
