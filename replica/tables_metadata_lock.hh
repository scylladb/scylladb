/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

 #pragma once

#include <vector>
#include <seastar/core/sharded.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/smp.hh>

namespace replica {

class tables_metadata_lock_on_all_shards {
    std::vector<seastar::foreign_ptr<std::unique_ptr<seastar::rwlock::holder>>> _holders;
public:
    tables_metadata_lock_on_all_shards() : _holders(seastar::smp::count) {};
    void assign_lock(seastar::rwlock::holder&& h);
};

} // namespace replica
