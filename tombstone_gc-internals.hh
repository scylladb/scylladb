// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#pragma once

#include "tombstone_gc.hh"
#include <boost/icl/interval_map.hpp>

/**
 * Holds a repair history entry for a given token range.
 * Timestamp is time of last repair, replay_position is an
 * optional flush mark for the table in question which is
 * also marked in the commitlog cleanup table. I.e. the 
 * lowest known position for which a replay might occur.
 */
struct repair_history_entry {
    gc_clock::time_point timestamp;
    db::replay_position replay_position;

    auto operator<=>(const repair_history_entry&) const noexcept = default;
};

using repair_history_map = boost::icl::interval_map<dht::token, repair_history_entry, boost::icl::partial_absorber, std::less, boost::icl::inplace_max>;

class repair_history_map_ptr {
    lw_shared_ptr<repair_history_map> _ptr;
public:
    repair_history_map_ptr() = default;
    repair_history_map_ptr(lw_shared_ptr<repair_history_map> ptr) : _ptr(std::move(ptr)) {}
    repair_history_map& operator*() const { return _ptr.operator*(); }
    repair_history_map* operator->() const { return _ptr.operator->(); }
    explicit operator bool() const { return _ptr.operator bool(); }
};


