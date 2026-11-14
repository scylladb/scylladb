/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "sstables/shared_sstable.hh"
#include "exceptions/exceptions.hh"
#include "compaction_strategy_type.hh"
#include "compaction_group_view.hh"
#include "strategy_control.hh"

struct mutation_source_metadata;
extern logging::logger compaction_strategy_logger;

namespace sstables {
class sstable;
class sstable_set;
class storage;
}

namespace compaction {

class compaction_backlog_tracker;
class compaction_strategy_impl;
struct compaction_descriptor;

/// Default value of the min_sstable_size option of the size-tiered-like compaction
/// strategies, used when the expected memtable size isn't known.
static constexpr uint64_t default_min_sstable_size = 50L * 1024L * 1024L;

/// Don't derive a min_sstable_size smaller than this, so that it stays meaningful
/// no matter how little memtable memory a single memtable gets.
static constexpr uint64_t min_sstable_size_lower_bound = 4 * 1024;

/// Provides the default values of compaction strategy options that cannot be plain
/// constants, since they depend on the resources available to the memtable the
/// sstables are flushed from.
///
/// The defaults are used only for options that aren't set in the table's schema.
struct strategy_options_defaults {
    /// The expected size, in bytes, of the memtable the sstables are flushed from,
    /// or zero when it isn't known.
    ///
    /// It is derived by the table from the memtable memory available to the shard
    /// and the number of memtables sharing it, so that the compaction strategies
    /// don't need to know how the table's data is distributed across the shard.
    uint64_t expected_memtable_size = 0;

    /// @return the default value of the min_sstable_size option, in bytes.
    ///
    /// SSTables smaller than min_sstable_size are all put in the same size tier, so
    /// it is meant to capture the sstables that are too small to be tiered by size,
    /// not the ones a full memtable is flushed into.  Half of the expected memtable
    /// size satisfies that, and it is clamped to
    /// [min_sstable_size_lower_bound, default_min_sstable_size].
    ///
    /// Returns default_min_sstable_size when the expected memtable size isn't known.
    uint64_t min_sstable_size() const;
};

class compaction_strategy {
    ::shared_ptr<compaction_strategy_impl> _compaction_strategy_impl;
public:
    compaction_strategy(::shared_ptr<compaction_strategy_impl> impl);

    compaction_strategy();
    ~compaction_strategy();
    compaction_strategy(const compaction_strategy&);
    compaction_strategy(compaction_strategy&&);
    compaction_strategy& operator=(compaction_strategy&&);

    // Return a list of sstables to be compacted after applying the strategy.
    future<compaction_descriptor> get_sstables_for_compaction(compaction_group_view& table_s, strategy_control& control);

    compaction_descriptor get_major_compaction_job(compaction_group_view& table_s, std::vector<sstables::shared_sstable> candidates);

    std::vector<compaction_descriptor> get_cleanup_compaction_jobs(compaction_group_view& table_s, std::vector<sstables::shared_sstable> candidates) const;

    // Some strategies may look at the compacted and resulting sstables to
    // get some useful information for subsequent compactions.
    void notify_completion(compaction_group_view& table_s, const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added);

    // Return if parallel compaction is allowed by strategy.
    bool parallel_compaction() const;

    // Return if optimization to rule out sstables based on clustering key filter should be applied.
    bool use_clustering_key_filter() const;

    // An estimation of number of compaction for strategy to be satisfied.
    future<int64_t> estimated_pending_compactions(compaction_group_view& table_s) const;

    static sstring name(compaction_strategy_type type) {
        switch (type) {
        case compaction_strategy_type::null:
            return "NullCompactionStrategy";
        case compaction_strategy_type::size_tiered:
            return "SizeTieredCompactionStrategy";
        case compaction_strategy_type::leveled:
            return "LeveledCompactionStrategy";
        case compaction_strategy_type::time_window:
            return "TimeWindowCompactionStrategy";
        case compaction_strategy_type::in_memory:
            return "InMemoryCompactionStrategy";
        case compaction_strategy_type::incremental:
            return "IncrementalCompactionStrategy";
        default:
            throw std::runtime_error("Invalid Compaction Strategy");
        }
    }

    static compaction_strategy_type type(const sstring& name) {
        auto pos = name.find("org.apache.cassandra.db.compaction.");
        sstring short_name = (pos == sstring::npos) ? name : name.substr(pos + 35);
        if (short_name == "NullCompactionStrategy") {
            return compaction_strategy_type::null;
        } else if (short_name == "SizeTieredCompactionStrategy") {
            return compaction_strategy_type::size_tiered;
        } else if (short_name == "LeveledCompactionStrategy") {
            return compaction_strategy_type::leveled;
        } else if (short_name == "TimeWindowCompactionStrategy") {
            return compaction_strategy_type::time_window;
        } else if (short_name == "InMemoryCompactionStrategy") {
            return compaction_strategy_type::in_memory;
        } else if (short_name == "IncrementalCompactionStrategy") {
            return compaction_strategy_type::incremental;
        } else {
            throw exceptions::configuration_exception(format("Unable to find compaction strategy class '{}'", name));
        }
    }

    compaction_strategy_type type() const;

    sstring name() const {
        return name(type());
    }

    sstables::sstable_set make_sstable_set(const compaction_group_view& ts) const;

    compaction_backlog_tracker make_backlog_tracker() const;

    uint64_t adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate, schema_ptr) const;

    mutation_reader_consumer make_interposer_consumer(const mutation_source_metadata& ms_meta, mutation_reader_consumer end_consumer) const;

    // Returns whether or not interposer consumer is used by a given strategy.
    bool use_interposer_consumer() const;

    // Informs the caller (usually the compaction manager) about what would it take for this set of
    // SSTables closer to becoming in-strategy. If this returns an empty compaction descriptor, this
    // means that the sstable set is already in-strategy.
    //
    // The caller can specify one of two modes: strict or relaxed. In relaxed mode the tolerance for
    // what is considered offstrategy is higher. It can be used, for instance, for when the system
    // is restarting and previous compactions were likely in-flight. In strict mode, we are less
    // tolerant to invariant breakages.
    //
    // The caller should also pass a maximum number of SSTables which is the maximum amount of
    // SSTables that can be added into a single job.
    compaction_descriptor get_reshaping_job(std::vector<sstables::shared_sstable> input, schema_ptr schema, reshape_config cfg) const;

};

/// Creates a compaction_strategy object from one of the strategies available.
///
/// @param defaults provides the values of the options that aren't set in @param options.
/// It isn't referenced after this call returns. Defaults to the values used when
/// the expected memtable size isn't known.
compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options,
        const strategy_options_defaults& defaults = {});

future<reshape_config> make_reshape_config(const sstables::storage& storage, reshape_mode mode);

}
