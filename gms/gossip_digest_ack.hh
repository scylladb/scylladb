/*

 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "utils/serialization.hh"
#include "gms/gossip_digest.hh"
#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "utils/chunked_vector.hh"
#include <fmt/core.h>

namespace gms {

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
 * endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
 */
class gossip_digest_ack {
private:
    using inet_address = gms::inet_address;
    utils::chunked_vector<gossip_digest> _digests;
    std::map<inet_address, endpoint_state> _map;
public:
    gossip_digest_ack() {
    }

    gossip_digest_ack(utils::chunked_vector<gossip_digest> d, std::map<inet_address, endpoint_state> m)
        : _digests(std::move(d))
        , _map(std::move(m)) {
    }

    const utils::chunked_vector<gossip_digest>& get_gossip_digest_list() const {
        return _digests;
    }

    std::map<inet_address, endpoint_state>& get_endpoint_state_map() {
        return _map;
    }

    const std::map<inet_address, endpoint_state>& get_endpoint_state_map() const {
        return _map;
    }

    friend fmt::formatter<gossip_digest_ack>;
};

}

template <> struct fmt::formatter<gms::gossip_digest_ack> : fmt::formatter<string_view> {
    auto format(const gms::gossip_digest_ack&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
