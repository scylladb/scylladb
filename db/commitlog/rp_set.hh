/*
 * Modified by ScyllaDB
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <unordered_map>

#include "replay_position.hh"

namespace db {

class rp_set {
public:
    typedef std::unordered_map<segment_id_type, uint64_t> usage_map;

    rp_set()
    {}
    rp_set(const replay_position & rp)
    {
        put(rp);
    }
    rp_set(rp_set&&) = default;

    rp_set& operator=(rp_set&&) = default;

    void put(const replay_position& rp) {
        _usage[rp.id]++;
    }
    void put(rp_handle && h) {
        if (h) {
            put(h.rp());
        }
        h.release();
    }

    size_t size() const {
        return _usage.size();
    }
    bool empty() const {
        return _usage.empty();
    }

    const usage_map& usage() const {
        return _usage;
    }
private:
    usage_map _usage;
};

}
