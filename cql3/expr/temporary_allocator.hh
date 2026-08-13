/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>

namespace cql3::expr {

// Hands out temporary slots.
//
// Temporaries are introduced by several parties - prepare, split_aggregation(),
// selection::add_column_for_post_processing() - at different points in building
// a statement, and they all draw from one index space. Passing the allocator
// along is what keeps their slots disjoint without anyone having to know who
// else is allocating, or in what order.
//
// It moves rather than copies, because two allocators over one index space is
// exactly the thing it exists to prevent: each would hand out slots the other
// thinks are free. Handing it on is therefore a transfer, and anyone who really
// does want a second one has to say so.
class temporary_allocator {
    size_t _nr_allocated = 0;
public:
    temporary_allocator() = default;
    temporary_allocator(temporary_allocator&&) = default;
    temporary_allocator& operator=(temporary_allocator&&) = default;
    temporary_allocator(const temporary_allocator&) = delete;
    temporary_allocator& operator=(const temporary_allocator&) = delete;

    // Index of a fresh slot, never handed out before.
    size_t allocate() { return _nr_allocated++; }
    // How many slots exist, i.e. the size the temporaries vector needs to be.
    size_t nr_allocated() const { return _nr_allocated; }
};

}
