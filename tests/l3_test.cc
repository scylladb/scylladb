/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "net/net.hh"
#include "core/reactor.hh"
#include "net/virtio.hh"

using namespace net;

void dump_arp_packets(l3_protocol& proto) {
    proto.receive([&proto] (packet p, ethernet_address from) {
        std::cout << "seen arp packet\n";
        return make_ready_future<>();
    }, [] (forward_hash& out_hash_data, packet& p, size_t off) {return false;});
}

int main(int ac, char** av) {
    boost::program_options::variables_map opts;
    opts.insert(std::make_pair("tap-device", boost::program_options::variable_value(std::string("tap0"), false)));

    auto vnet = create_virtio_net_device(opts);
    interface netif(std::move(vnet));
    l3_protocol arp(&netif, eth_protocol_num::arp, []{ return std::experimental::optional<l3_protocol::l3packet>(); });
    dump_arp_packets(arp);
    engine().run();
    return 0;
}

