/*
 * Modified by ScyllaDB
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
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

    // Lets a later put() in the same segment skip allocating.
    void reserve(const replay_position& rp) {
        _usage.try_emplace(rp.id, 0);
    }
    void reserve(const rp_handle& h) {
        if (h) {
            reserve(h.rp());
        }
    }
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
