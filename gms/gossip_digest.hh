/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include "gms/generation-number.hh"
#include "gms/version_generator.hh"

namespace gms {

/**
 * Contains information about a specified list of Endpoints and the largest version
 * of the state they have generated as known by the local endpoint.
 */
class gossip_digest { // implements Comparable<GossipDigest>
private:
    using inet_address = gms::inet_address;
    inet_address _endpoint;
    generation_type _generation;
    version_type _max_version;
public:
    gossip_digest() = default;

    explicit gossip_digest(inet_address ep, generation_type gen = {}, version_type version = {}) noexcept
        : _endpoint(ep)
        , _generation(gen)
        , _max_version(version) {
    }

    inet_address get_endpoint() const {
        return _endpoint;
    }

    generation_type get_generation() const {
        return _generation;
    }

    version_type get_max_version() const {
        return _max_version;
    }

    friend bool operator<(const gossip_digest& x, const gossip_digest& y) {
        if (x._generation != y._generation) {
            return x._generation < y._generation;
        }
        return x._max_version <  y._max_version;
    }

    friend inline std::ostream& operator<<(std::ostream& os, const gossip_digest& d) {
        fmt::print(os, "{}:{}:{}", d._endpoint, d._generation, d._max_version);
        return os;
    }
}; // class gossip_digest

} // namespace gms
