/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

 #pragma once

#include <vector>
#include <seastar/core/sharded.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/smp.hh>

namespace replica {

class tables_metadata_lock_on_all_shards {
    std::vector<seastar::foreign_ptr<std::unique_ptr<seastar::rwlock::holder>>> _holders;
    std::vector<seastar::foreign_ptr<std::unique_ptr<seastar::rwlock::holder>>> _schema_change_holders;
public:
    tables_metadata_lock_on_all_shards() : _holders(seastar::this_smp_shard_count()), _schema_change_holders(seastar::this_smp_shard_count()) {};
    void assign_lock(seastar::rwlock::holder&& h);
    void assign_schema_change_lock(seastar::rwlock::holder&& h);
};

} // namespace replica
