/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "dht/i_partitioner_fwd.hh"
#include "dht/token.hh"
#include "data_dictionary/storage_options.hh"
#include "sstables/generation_type.hh"
#include "sstables/open_info.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/types.hh"
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
    // The sstable_id of the restored sstable. Set only for a table on object storage.
    // There the sstable_id is the prefix of the object names, so attach_sstable()
    // needs it to open the sstable. For a table on filesystem, the sstable_id comes
    // from the streamed Scylla.db.
    optimized_optional<sstables::sstable_id> sid;
    // The sstable_id of the backup sstable. It is the key of the rows in
    // system_distributed.snapshot_sstables, which a retried restore reads to skip the
    // sstables which are already restored.
    sstables::sstable_id source_sid;
};

// Streams the components of the sstable `remote` into a new sstable described by
// `dst_schema` and `dst_opts`, with the generation `gen`. A backup is never on the
// filesystem of the restored table, so, unlike cloning inside one storage, the
// components go through this node.
future<sstables::entry_descriptor> stream_remote_sstable(sstables::sstable& remote, sstables::generation_type gen,
        schema_ptr dst_schema, data_dictionary::storage_options dst_opts, logging::logger& logger);

future<minimal_sst_info> download_sstable(replica::table& table, sstables::shared_sstable sstable, logging::logger& logger);
future<minimal_sst_info> clone_backup_sstable(replica::table& table, const sstables::shared_sstable& sstable, std::string_view snapshot_tag, logging::logger& logger);

template <std::ranges::input_range Range, typename T = std::ranges::range_value_t<Range>>
seastar::future<std::tuple<std::vector<T>, std::vector<T>>>
get_sstables_for_tablet(Range&& ranges, const dht::token_range& token_range, auto&& get_first, auto&& get_last) {
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
