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
#include "utils/serialization.hh"
#include "gms/inet_address.hh"

namespace gms {

/**
 * Contains information about a specified list of Endpoints and the largest version
 * of the state they have generated as known by the local endpoint.
 */
class gossip_digest { // implements Comparable<GossipDigest>
private:
    using inet_address = gms::inet_address;
    inet_address _endpoint;
    int32_t _generation;
    int32_t _max_version;
public:
    gossip_digest()
        : _endpoint(0)
        , _generation(0)
        , _max_version(0) {
    }

    gossip_digest(inet_address ep, int32_t gen, int32_t version)
        : _endpoint(ep)
        , _generation(gen)
        , _max_version(version) {
    }

    inet_address get_endpoint() const {
        return _endpoint;
    }

    int32_t get_generation() const {
        return _generation;
    }

    int32_t get_max_version() const {
        return _max_version;
    }

    int32_t compare_to(gossip_digest d) const {
        if (_generation != d.get_generation()) {
            return (_generation - d.get_generation());
        }
        return (_max_version - d.get_max_version());
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
