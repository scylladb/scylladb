/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <filesystem>
#include <memory>

#include <seastar/core/coroutine.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/coroutine/as_future.hh>

#include "db/cache_tracker.hh"
#include "mutation/mutation.hh"
#include "reader_concurrency_semaphore.hh"
#include "readers/mutation_reader.hh"
#include "replica/logstor/cache.hh"
#include "replica/logstor/compaction.hh"
#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/single_row_partition.hh"
#include "schema/schema_fwd.hh"
#include "utils/updateable_value.hh"

// What a test needs around a logstor instance to write to it and read from it: the cache it shares
// with the row cache of a database, and a compaction group to write into. A test has neither a
// database nor a table, which is what owns those in a running node.
namespace tests::logstor {

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
    size_t max_segments_per_compaction = 8;
    // The default threshold. A minimum at the threshold keeps the threshold constant above the
    // anchor, so that a small test disk does not get the minimum one segment above the anchor.
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
            .max_segments_per_compaction = params.max_segments_per_compaction,
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
        , _owned_index(ls.make_primary_index(cache_enabled))
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

} // namespace tests::logstor
