/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "sstables/shared_sstable.hh"
#include "exceptions/exceptions.hh"
#include "compaction_strategy_type.hh"
#include "table_state.hh"
#include "strategy_control.hh"

struct mutation_source_metadata;
class compaction_backlog_tracker;

using namespace compaction;

namespace sstables {

class compaction_strategy_impl;
class sstable;
class sstable_set;
struct compaction_descriptor;
class storage;

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
    compaction_descriptor get_sstables_for_compaction(table_state& table_s, strategy_control& control);

    compaction_descriptor get_major_compaction_job(table_state& table_s, std::vector<shared_sstable> candidates);

    std::vector<compaction_descriptor> get_cleanup_compaction_jobs(table_state& table_s, std::vector<shared_sstable> candidates) const;

    // Some strategies may look at the compacted and resulting sstables to
    // get some useful information for subsequent compactions.
    void notify_completion(table_state& table_s, const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added);

    // Return if parallel compaction is allowed by strategy.
    bool parallel_compaction() const;

    // Return if optimization to rule out sstables based on clustering key filter should be applied.
    bool use_clustering_key_filter() const;

    // An estimation of number of compaction for strategy to be satisfied.
    int64_t estimated_pending_compactions(table_state& table_s) const;

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
        } else {
            throw exceptions::configuration_exception(format("Unable to find compaction strategy class '{}'", name));
        }
    }

    compaction_strategy_type type() const;

    sstring name() const {
        return name(type());
    }

    sstable_set make_sstable_set(schema_ptr schema) const;

    compaction_backlog_tracker make_backlog_tracker() const;

    uint64_t adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate, schema_ptr) const;

    reader_consumer_v2 make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer_v2 end_consumer) const;

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
    compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, reshape_config cfg) const;

};

// Creates a compaction_strategy object from one of the strategies available.
compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options);

future<reshape_config> make_reshape_config(const sstables::storage& storage, reshape_mode mode);

}
