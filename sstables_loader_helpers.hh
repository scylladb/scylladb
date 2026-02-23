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
    shard_id _shard;
    sstables::generation_type _generation;
    sstables::sstable_version_types _version;
    sstables::sstable_format_types _format;
};

future<minimal_sst_info> download_sstable(replica::database& db, replica::table& table, sstables::shared_sstable sstable, logging::logger& logger);

template <std::ranges::input_range Range>
future<std::tuple<std::vector<sstables::shared_sstable>, std::vector<sstables::shared_sstable>>> get_sstables_for_tablet(Range&& sstables,
                                                                                                                         const dht::token_range& tablet_range) {
    std::vector<sstables::shared_sstable> fully_contained;
    std::vector<sstables::shared_sstable> partially_contained;
    for (const auto& sst : sstables) {
        auto sst_first = sst->get_first_decorated_key().token();
        auto sst_last = sst->get_last_decorated_key().token();

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
