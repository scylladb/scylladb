/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#define BOOST_TEST_MODULE raft
#include "test/raft/helpers.hh"
#include "service/raft/discovery.hh"

using namespace raft;

using discovery_network = std::unordered_map<server_id, service::discovery*>;

using service::discovery;

using service::discovery_peer;

void
run_discovery_impl(discovery_network& network) {
    while (true) {
        for (auto e : network) {
            discovery& from = *e.second;
            auto output = from.tick();
            if (std::holds_alternative<discovery::i_am_leader>(output)) {
                return;
            } else if (std::holds_alternative<discovery::pause>(output)) {
                continue;
            }
            auto& msgs = std::get<discovery::request_list>(output);
            for (auto&& m : msgs) {
                auto it = network.find(m.first.id);
                if (it == network.end()) {
                    // The node is not available, drop the message
                    continue;
                }
                discovery& to = *(it->second);
                if (auto peer_list = to.request(m.second)) {
                    from.response(m.first, std::move(*peer_list));
                }
            }
        }
    }
}

template <typename... Args>
void run_discovery(Args&&... args) {
    discovery_network network;
    auto add_node = [&network](discovery& node) -> void {
        network.emplace(node.id(), &node);
    };
    (add_node(args), ...);
    run_discovery_impl(network);
}

BOOST_AUTO_TEST_CASE(test_basic) {

    discovery_peer addr1 = {id(), {}};

    // Must supply an Internet address for self
    BOOST_CHECK_THROW(discovery(addr1, {}), std::logic_error);
    discovery_peer addr2 = {id(), gms::inet_address("192.168.1.2")};
    BOOST_CHECK_NO_THROW(discovery(addr2, {}));
    // Must supply an Internet address for each peer
    BOOST_CHECK_THROW(discovery(addr2, {addr1}), std::logic_error);
    // OK to include self into peers
    BOOST_CHECK_NO_THROW(discovery(addr2, {addr2}));
    // With a single peer, discovery immediately finds a leader
    discovery d(addr2, {});
    BOOST_CHECK(d.is_leader());
    d = discovery(addr2, {addr2});
    BOOST_CHECK(d.is_leader());
}


BOOST_AUTO_TEST_CASE(test_discovery) {

    discovery_peer addr1 = {id(), gms::inet_address("192.168.1.1")};
    discovery_peer addr2 = {id(), gms::inet_address("192.168.1.2")};

    discovery d1(addr1, {addr2});
    discovery d2(addr2, {addr1});
    run_discovery(d1, d2);

    BOOST_CHECK(d1.is_leader() ^ d2.is_leader());
}

BOOST_AUTO_TEST_CASE(test_discovery_fullmesh) {

    discovery_peer addr1 = {id(), gms::inet_address("127.0.0.13")};
    discovery_peer addr2 = {id(), gms::inet_address("127.0.0.19")};
    discovery_peer addr3 = {id(), gms::inet_address("127.0.0.21")};

    auto seeds = std::vector<discovery_peer>({addr1, addr2, addr3});

    discovery d1(addr1, seeds);
    discovery d2(addr2, seeds);
    discovery d3(addr3, seeds);
    run_discovery(d1, d2, d3);

    BOOST_CHECK(d1.is_leader() ^ d2.is_leader() ^ d3.is_leader());
}
