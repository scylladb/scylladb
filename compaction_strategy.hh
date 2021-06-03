/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/file.hh>

#include "schema_fwd.hh"
#include "sstables/shared_sstable.hh"
#include "exceptions/exceptions.hh"
#include "sstables/compaction_backlog_manager.hh"
#include "compaction_strategy_type.hh"

class table;
using column_family = table;

class flat_mutation_reader;
struct mutation_source_metadata;

namespace sstables {

class compaction_strategy_impl;
class sstable;
class sstable_set;
struct compaction_descriptor;
struct resharding_descriptor;

using reader_consumer = noncopyable_function<future<> (flat_mutation_reader)>;

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
    compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<shared_sstable> candidates);

    compaction_descriptor get_major_compaction_job(column_family& cf, std::vector<shared_sstable> candidates);

    // Some strategies may look at the compacted and resulting sstables to
    // get some useful information for subsequent compactions.
    void notify_completion(const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added);

    // Return if parallel compaction is allowed by strategy.
    bool parallel_compaction() const;

    // Return if optimization to rule out sstables based on clustering key filter should be applied.
    bool use_clustering_key_filter() const;

    // Return true if compaction strategy doesn't care if a sstable belonging to partial sstable run is compacted.
    bool can_compact_partial_runs() const;

    // An estimation of number of compaction for strategy to be satisfied.
    int64_t estimated_pending_compactions(column_family& cf) const;

    static sstring name(compaction_strategy_type type) {
        switch (type) {
        case compaction_strategy_type::null:
            return "NullCompactionStrategy";
        case compaction_strategy_type::major:
            return "MajorCompactionStrategy";
        case compaction_strategy_type::size_tiered:
            return "SizeTieredCompactionStrategy";
        case compaction_strategy_type::leveled:
            return "LeveledCompactionStrategy";
        case compaction_strategy_type::date_tiered:
            return "DateTieredCompactionStrategy";
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
        } else if (short_name == "MajorCompactionStrategy") {
            return compaction_strategy_type::major;
        } else if (short_name == "SizeTieredCompactionStrategy") {
            return compaction_strategy_type::size_tiered;
        } else if (short_name == "LeveledCompactionStrategy") {
            return compaction_strategy_type::leveled;
        } else if (short_name == "DateTieredCompactionStrategy") {
            return compaction_strategy_type::date_tiered;
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

    compaction_backlog_tracker& get_backlog_tracker();

    uint64_t adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate);

    reader_consumer make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer end_consumer);

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
    compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode);

};

// Creates a compaction_strategy object from one of the strategies available.
compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options);

}
