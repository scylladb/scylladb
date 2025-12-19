/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "sstables.hh"
#include <memory>
#include <seastar/core/sharded.hh>

namespace sstables {

class global_sstable_list {
    std::vector<foreign_ptr<lw_shared_ptr<sstable_list>>> _lists;
private:
    sstable_list& get_or_insert_list() {
        if (!_lists[this_shard_id()]) {
            _lists[this_shard_id()] = make_foreign(make_lw_shared(sstable_list{}));
        }
        return *_lists[this_shard_id()].get();
    }

    future<> mutate_on_shard(unsigned shard, std::function<void(sstable_list&)> f) {
        return smp::submit_to(shard, [this, f] () {
            f(get_or_insert_list());
            return make_ready_future<>();
        });
    }
public:
    global_sstable_list()
        : _lists(smp::count) {
    }

    future<> mutate(std::function<void(sstable_list&)> f) {
        return mutate_on_shard(this_shard_id(), std::move(f));
    }

    future<> mutate_all(std::function<void(sstable_list&)> f) {
        for (unsigned id = 0; id < smp::count; id++) {
            if (_lists[id]) {
                co_await mutate_on_shard(id, f);
            }
        }
    }
};

}
