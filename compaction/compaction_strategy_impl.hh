/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "cql3/statements/property_definitions.hh"
#include "compaction_backlog_manager.hh"
#include "compaction_strategy.hh"
#include "database_fwd.hh"
#include "db_clock.hh"

namespace sstables {

compaction_backlog_tracker& get_unimplemented_backlog_tracker();

class sstable_set_impl;
class compaction_descriptor;
class resharding_descriptor;

class compaction_strategy_impl {
    static constexpr float DEFAULT_TOMBSTONE_THRESHOLD = 0.2f;
    // minimum interval needed to perform tombstone removal compaction in seconds, default 86400 or 1 day.
    static constexpr std::chrono::seconds DEFAULT_TOMBSTONE_COMPACTION_INTERVAL() { return std::chrono::seconds(86400); }
protected:
    const sstring TOMBSTONE_THRESHOLD_OPTION = "tombstone_threshold";
    const sstring TOMBSTONE_COMPACTION_INTERVAL_OPTION = "tombstone_compaction_interval";

    bool _use_clustering_key_filter = false;
    bool _disable_tombstone_compaction = false;
    float _tombstone_threshold = DEFAULT_TOMBSTONE_THRESHOLD;
    db_clock::duration _tombstone_compaction_interval = DEFAULT_TOMBSTONE_COMPACTION_INTERVAL();
public:
    static std::optional<sstring> get_value(const std::map<sstring, sstring>& options, const sstring& name);
protected:
    compaction_strategy_impl() = default;
    explicit compaction_strategy_impl(const std::map<sstring, sstring>& options);
public:
    virtual ~compaction_strategy_impl() {}
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) = 0;
    virtual compaction_descriptor get_major_compaction_job(column_family& cf, std::vector<sstables::shared_sstable> candidates);
    virtual void notify_completion(const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added) { }
    virtual compaction_strategy_type type() const = 0;
    virtual bool parallel_compaction() const {
        return true;
    }
    virtual int64_t estimated_pending_compactions(column_family& cf) const = 0;
    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const;

    bool use_clustering_key_filter() const {
        return _use_clustering_key_filter;
    }

    virtual bool can_compact_partial_runs() const {
        return false;
    }

    // Check if a given sstable is entitled for tombstone compaction based on its
    // droppable tombstone histogram and gc_before.
    bool worth_dropping_tombstones(const shared_sstable& sst, gc_clock::time_point gc_before);

    virtual compaction_backlog_tracker& get_backlog_tracker() = 0;

    virtual uint64_t adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate);

    virtual reader_consumer make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer end_consumer);

    virtual bool use_interposer_consumer() const {
        return false;
    }

    virtual compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode);
};
}
