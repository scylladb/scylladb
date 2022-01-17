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
#include "gms/gossip_digest.hh"
#include "utils/chunked_vector.hh"

namespace gms {

/**
 * This is the first message that gets sent out as a start of the Gossip protocol in a
 * round.
 */
class gossip_digest_syn {
private:
    sstring _cluster_id;
    sstring _partioner;
    utils::chunked_vector<gossip_digest> _digests;
public:
    gossip_digest_syn() {
    }

    gossip_digest_syn(sstring id, sstring p, utils::chunked_vector<gossip_digest> digests)
        : _cluster_id(std::move(id))
        , _partioner(std::move(p))
        , _digests(std::move(digests)) {
    }

    sstring cluster_id() const {
        return _cluster_id;
    }

    sstring partioner() const {
        return _partioner;
    }

    sstring get_cluster_id() const {
        return cluster_id();
    }

    sstring get_partioner() const {
        return partioner();
    }

    const utils::chunked_vector<gossip_digest>& get_gossip_digests() const {
        return _digests;
    }

    friend std::ostream& operator<<(std::ostream& os, const gossip_digest_syn& syn);
};

}
