/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "dht/i_partitioner_fwd.hh"
#include "dht/token.hh"
#include "sstables/generation_type.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/version.hh"
#include "utils/log.hh"

#include <vector>

namespace replica {
class table;
class database;
} // namespace replica

struct minimal_sst_info {
    shard_id shard;
    sstables::generation_type generation;
    sstables::sstable_version_types version;
    sstables::sstable_format_types format;
};

future<minimal_sst_info> download_sstable(replica::database& db, replica::table& table, sstables::shared_sstable sstable, logging::logger& logger);

template <std::ranges::input_range Range, typename T = std::ranges::range_value_t<Range>>
seastar::future<std::tuple<std::vector<T>, std::vector<T>>> get_sstables_for_tablet(Range&& sstables,
                                                                                                                         const dht::token_range& tablet_range,
                                                                                                                         auto&& get_first, auto&& get_last) {
    std::vector<T> fully_contained;
    std::vector<T> partially_contained;
    for (const auto& sst : sstables) {
        auto sst_first = get_first(sst);
        auto sst_last = get_last(sst);

        // SSTable entirely after tablet -> no further SSTables (larger keys) can overlap
        if (tablet_range.after(sst_first, dht::token_comparator{})) {
            break;
        }
        // SSTable entirely before tablet -> skip and continue scanning later (larger keys)
        if (tablet_range.before(sst_last, dht::token_comparator{})) {
            continue;
        }

        if (tablet_range.contains(dht::token_range{sst_first, sst_last}, dht::token_comparator{})) {
            fully_contained.push_back(sst);
        } else {
            partially_contained.push_back(sst);
        }
        co_await coroutine::maybe_yield();
    }
    co_return std::make_tuple(std::move(fully_contained), std::move(partially_contained));
}
