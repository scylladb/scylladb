/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <compare>
#include <cstddef>
#include <limits>

namespace db::view {

/**
 * The view update backlog represents the pending view data that a base replica
 * maintains. It is the maximum of the memory backlog - how much memory pending
 * view updates are consuming out of the their allocated quota - and the disk
 * backlog - how much view hints are consuming. The size of a backlog is relative
 * to its maximum size.
 */
struct update_backlog {
    size_t current;
    size_t max;

    float relative_size() const {
        return float(current) / float(max);
    }

    std::partial_ordering operator<=>(const update_backlog &rhs) const {
        return relative_size() <=> rhs.relative_size();
    }
    bool operator==(const update_backlog& rhs) const {
        return relative_size() == rhs.relative_size();
    }

    static update_backlog no_backlog() {
        return update_backlog{0, std::numeric_limits<size_t>::max()};
    }
};

}
