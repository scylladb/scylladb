/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/test/unit_test.hpp>
#include <fmt/format.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/coroutine/as_future.hh>

#include "db/cache_tracker.hh"
#include "dht/i_partitioner_fwd.hh"
#include "mutation/mutation.hh"
#include "reader_concurrency_semaphore.hh"
#include "readers/mutation_reader.hh"
#include "replica/logstor/cache.hh"
#include "replica/logstor/compaction.hh"
#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/recovery_catalog.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/single_row_partition.hh"
#include "schema/schema_fwd.hh"
#include "utils/updateable_value.hh"

// What a test needs around a logstor instance to write to it and read from it: the cache it shares
// with the row cache of a database, and a compaction group to write into. A test has neither a
// database nor a table, which is what owns those in a running node.
namespace tests::logstor {

struct noop_space_accounting_subscriber final : replica::logstor::space_accounting_subscriber {
    void on_add_record(replica::logstor::record_location) noexcept override {}
    void on_free_record(replica::logstor::record_location) noexcept override {}
};

inline noop_space_accounting_subscriber noop_space_accounting{};

struct shared_logstor_cache {
    ::cache_tracker shared_tracker;
    replica::logstor::cache_tracker logstor_tracker;

    shared_logstor_cache()
        : shared_tracker(utils::updateable_value<double>(1.0), ::cache_tracker::register_metrics::no)
        , logstor_tracker(shared_tracker) {
    }
};

// The defaults are a logstor small enough for a unit test: a disk of four files of 32 segments
// each, created sparse so that formatting them costs neither disk space nor IO. A perf test wants
// a larger pool and real files, and says so.
struct logstor_params {
    size_t segment_size = 128 * 1024;
    size_t file_size = 32 * 128 * 1024;
    size_t disk_size = 4 * 32 * 128 * 1024;
    bool format_on_startup = true;
    bool sparse_files = true;
    bool compaction_enabled = true;
    // The default threshold. A minimum at the threshold keeps the threshold constant above the
    // anchor, so that a small test disk does not reach the minimum threshold one segment above the
    // anchor.
    double compaction_min_utilization_threshold = 0.5;
};

inline replica::logstor::logstor_config make_test_logstor_config(const std::filesystem::path& base_dir, logstor_params params = {}) {
    return replica::logstor::logstor_config{
        .segment_manager_cfg = {
            .base_dir = base_dir,
            .segment_size = params.segment_size,
            .file_size = params.file_size,
            .disk_size = params.disk_size,
            .format_on_startup = params.format_on_startup,
            .sparse_files = params.sparse_files,
            .compaction_enabled = params.compaction_enabled,
            .compaction_min_utilization_threshold = utils::updateable_value<double>(params.compaction_min_utilization_threshold),
            .compaction_sg = seastar::current_scheduling_group(),
            .compaction_static_shares = utils::updateable_value<float>(0.0f),
            .compaction_max_shares = utils::updateable_value<float>(2000.0f),
            .separator_sg = seastar::current_scheduling_group(),
            .split_compaction_sg = seastar::current_scheduling_group(),
        },
        .flush_sg = seastar::current_scheduling_group(),
    };
}

// A compaction group of a table, which is what the separator and compaction work on. Registers
// itself with the compaction manager, so it must be destroyed in a seastar thread.
class test_logstor_group final : public replica::logstor::logstor_group {
    ::table_id _table_id;
    std::unique_ptr<replica::logstor::primary_index> _owned_index;
    replica::logstor::primary_index& _index;
    replica::logstor::compaction_manager& _cm;
public:
    test_logstor_group(schema_ptr schema, replica::logstor::logstor& ls, bool cache_enabled = false)
        : logstor_group(ls.get_segment_manager().get_segment_size(), &ls.get_segment_manager().shard_segment_stats())
        , _table_id(schema->id())
        , _owned_index(ls.make_primary_index(cache_enabled, cache_enabled))
        , _index(*_owned_index)
        , _cm(ls.get_compaction_manager()) {
        _cm.add(*this);
    }

    // A group of a table that already has an index: the index is per table, and all the groups of
    // a table share it. This is what the groups of a split have.
    test_logstor_group(schema_ptr schema, replica::logstor::logstor& ls, replica::logstor::primary_index& index)
        : logstor_group(ls.get_segment_manager().get_segment_size(), &ls.get_segment_manager().shard_segment_stats())
        , _table_id(schema->id())
        , _index(index)
        , _cm(ls.get_compaction_manager()) {
        _cm.add(*this);
    }

    ~test_logstor_group() override {
        _cm.remove(*this).get();
    }

    ::table_id table_id() const noexcept override {
        return _table_id;
    }

    replica::logstor::primary_index& logstor_index() noexcept override {
        return _index;
    }

    const replica::logstor::primary_index& logstor_index() const noexcept override {
        return _index;
    }

protected:
    replica::logstor::compaction_manager& logstor_compaction_manager() noexcept override {
        return _cm;
    }
};

// The partition of a consumed mutation: the row is moved out of it, no cell is copied. Tests build
// a partition from a mutation; logstor itself decodes one from a record.
inline replica::logstor::single_row_partition to_single_row_partition(mutation&& m) {
    return replica::logstor::to_single_row_partition(std::move(m.partition()), *m.schema(), m.key());
}

// Reads the partition of a key the way a query does, through the reader logstor::make_reader()
// makes, and builds the mutation from what the reader emits: nullopt when the index does not hold
// the key.
inline future<mutation_opt> read_mutation(replica::logstor::logstor& ls, schema_ptr s, const replica::logstor::primary_index& index,
        reader_permit permit, const dht::decorated_key& dk, const query::partition_slice& slice) {
    const auto pr = dht::partition_range::make_singular(dk);
    auto reader = ls.make_reader(std::move(s), index, std::move(permit), pr, slice);
    auto m = co_await coroutine::as_future(read_mutation_from_mutation_reader(reader));
    co_await reader.close();
    co_return co_await std::move(m);
}

// Like the above, for a test that has no permit to read with: each read takes one from a semaphore
// of its own. The semaphore is stopped however the read ends, since it holds a permit by then.
inline future<mutation_opt> read_mutation(replica::logstor::logstor& ls, schema_ptr s, const replica::logstor::primary_index& index,
        const dht::decorated_key& dk, const query::partition_slice& slice) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, "logstor_read_mutation",
            reader_concurrency_semaphore::register_metrics::no);
    auto permit = semaphore.make_tracking_only_permit(s, "read_mutation", db::no_timeout, {});
    auto m = co_await coroutine::as_future(read_mutation(ls, std::move(s), index, std::move(permit), dk, slice));
    co_await semaphore.stop();
    co_return co_await std::move(m);
}

// test_recovery_catalog holds the groups that a test adds, each with the token range that the group
// owns. The catalog has a table only if the test added a group of the table, and it gives the index
// of that group as the index of the table.
class test_recovery_catalog final : public replica::logstor::recovery_catalog {
    struct table_groups {
        replica::logstor::primary_index* index;
        std::vector<std::pair<dht::token_range, replica::logstor::logstor_group*>> groups;
    };
    std::map<::table_id, table_groups> _tables;
public:
    void add(replica::logstor::logstor_group& group, dht::token_range range = dht::token_range::make_open_ended_both_sides()) {
        auto& table = _tables[group.table_id()];
        if (table.index && table.index != &group.logstor_index()) {
            throw std::invalid_argument("test_recovery_catalog: the groups of a table must share one index");
        }
        for (const auto& [other_range, other_group] : table.groups) {
            if (other_range.overlaps(range, dht::token_comparator())) {
                throw std::invalid_argument(fmt::format("test_recovery_catalog: the range {} overlaps the range {} of another group", range, other_range));
            }
        }
        table.index = &group.logstor_index();
        table.groups.emplace_back(std::move(range), &group);
    }

    replica::logstor::primary_index* index(::table_id table) override {
        auto it = _tables.find(table);
        return it != _tables.end() ? it->second.index : nullptr;
    }

    target find(::table_id table, dht::token token) override {
        return find(table, token, token);
    }

    target find(::table_id table, dht::token first, dht::token last) override {
        auto it = _tables.find(table);
        if (it == _tables.end()) {
            return {};
        }
        for (auto& [range, group] : it->second.groups) {
            if (range.contains(first, dht::token_comparator()) && range.contains(last, dht::token_comparator())) {
                return {it->second.index, group};
            }
        }
        return {it->second.index, nullptr};
    }

    sstring table_name(::table_id table) override {
        return fmt::format("{}", table);
    }
};

// Space accounting that keeps no account, for an index a test rebuilds from disk next to the live
// one: the records are already charged to their segments through the live index.
struct null_space_accounting final : replica::logstor::space_accounting_subscriber {
    void on_add_record(replica::logstor::record_location) noexcept override {}
    void on_free_record(replica::logstor::record_location) noexcept override {}
};

using index_entries = std::vector<std::pair<replica::logstor::primary_index_key, replica::logstor::index_entry>>;

inline index_entries entries_of(const replica::logstor::primary_index& index) {
    index_entries out;
    out.reserve(index.get_key_count());
    auto scan = index.scan();
    while (auto batch = scan.next_batch(1024)) {
        for (const auto& e : batch->entries) {
            out.emplace_back(e.get().key(), e.get().entry());
        }
    }
    return out;
}

// Rebuilds the index of one table from disk through recovery's own routine. Nothing may be writing,
// separating or compacting while it runs: it reads the segments as they are.
inline std::unique_ptr<replica::logstor::primary_index> rebuild_index_from_disk(replica::logstor::logstor& ls, ::table_id table,
        null_space_accounting& accounting) {
    auto index = std::make_unique<replica::logstor::primary_index>(accounting, nullptr, false);
    ls.get_segment_manager().rebuild_index_from_disk([&] (const replica::logstor::record_header& h) -> replica::logstor::primary_index* {
        return h.table == table ? index.get() : nullptr;
    }).get();
    return index;
}

// Requires that the two indexes hold the same entries. If they do not, the failure message lists
// the entries that only one of the indexes holds, up to five for each index.
inline void require_equal_indexes(const replica::logstor::primary_index& expected, const replica::logstor::primary_index& actual) {
    const auto expected_entries = entries_of(expected);
    const auto actual_entries = entries_of(actual);
    if (expected_entries == actual_entries) {
        return;
    }
    static constexpr size_t max_reported = 5;
    auto describe = [] (const index_entries& only_in, const index_entries& other, std::string_view what) {
        std::string out;
        size_t reported = 0;
        for (const auto& [key, entry] : only_in) {
            if (std::ranges::find(other, std::pair(key, entry)) != other.end()) {
                continue;
            }
            if (reported++ < max_reported) {
                out += fmt::format("\n  {} only: {} at {} ts {}", what, key, entry.location, entry.timestamp);
            }
        }
        if (reported > max_reported) {
            out += fmt::format("\n  ... {} more {} only", reported - max_reported, what);
        }
        return out;
    };
    BOOST_FAIL(fmt::format("the expected index has {} entries, the actual index has {}:{}{}", expected_entries.size(), actual_entries.size(),
            describe(expected_entries, actual_entries, "expected"), describe(actual_entries, expected_entries, "actual")));
}

} // namespace tests::logstor
