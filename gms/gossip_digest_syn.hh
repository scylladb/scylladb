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
#include <fmt/core.h>
#include "utils/serialization.hh"
#include "gms/gossip_digest.hh"
#include "utils/chunked_vector.hh"
#include "utils/UUID.hh"

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
    utils::UUID _group0_id;
public:
    gossip_digest_syn() {
    }

    gossip_digest_syn(sstring id, sstring p, utils::chunked_vector<gossip_digest> digests, utils::UUID group0_id)
        : _cluster_id(std::move(id))
        , _partioner(std::move(p))
        , _digests(std::move(digests))
        , _group0_id(std::move(group0_id)) {
    }

    sstring cluster_id() const {
        return _cluster_id;
    }

    utils::UUID group0_id() const {
        return _group0_id;
    }

    sstring partioner() const {
        return _partioner;
    }

    sstring get_cluster_id() const {
        return cluster_id();
    }

    utils::UUID get_group0_id() const {
        return group0_id();
    }

    sstring get_partioner() const {
        return partioner();
    }

    const utils::chunked_vector<gossip_digest>& get_gossip_digests() const {
        return _digests;
    }

    friend fmt::formatter<gossip_digest_syn>;
};

}

template <> struct fmt::formatter<gms::gossip_digest_syn> : fmt::formatter<string_view> {
    auto format(const gms::gossip_digest_syn&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
