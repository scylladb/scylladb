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
seastar::future<std::tuple<std::vector<T>, std::vector<T>>> get_sstables_for_tablet(Range&& ranges,
                                                                                                                         const dht::token_range& token_range,
                                                                                                                         auto&& get_first, auto&& get_last) {
    std::vector<T> fully_contained;
    std::vector<T> partially_contained;
    for (const auto& range : ranges) {
        auto first_token = get_first(range);
        auto last_token = get_last(range);

        // Range entirely after token range -> no further ranges (larger keys) can overlap
        if (token_range.after(first_token, dht::token_comparator{})) {
            break;
        }
        // Range entirely before token range -> skip and continue scanning later (larger keys)
        if (token_range.before(last_token, dht::token_comparator{})) {
            continue;
        }

        if (token_range.contains(dht::token_range{first_token, last_token}, dht::token_comparator{})) {
            fully_contained.push_back(range);
        } else {
            partially_contained.push_back(range);
        }
        co_await coroutine::maybe_yield();
    }
    co_return std::make_tuple(std::move(fully_contained), std::move(partially_contained));
}
