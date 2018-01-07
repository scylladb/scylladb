/*
 * Copyright (C) 2017 ScyllaDB
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

namespace sstables {

compaction_backlog_tracker& get_unimplemented_backlog_tracker();

class sstable_set_impl;

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
    static stdx::optional<sstring> get_value(const std::map<sstring, sstring>& options, const sstring& name) {
        auto it = options.find(name);
        if (it == options.end()) {
            return stdx::nullopt;
        }
        return it->second;
    }
protected:
    compaction_strategy_impl() = default;
    explicit compaction_strategy_impl(const std::map<sstring, sstring>& options) {
        using namespace cql3::statements;

        auto tmp_value = get_value(options, TOMBSTONE_THRESHOLD_OPTION);
        _tombstone_threshold = property_definitions::to_double(TOMBSTONE_THRESHOLD_OPTION, tmp_value, DEFAULT_TOMBSTONE_THRESHOLD);

        tmp_value = get_value(options, TOMBSTONE_COMPACTION_INTERVAL_OPTION);
        auto interval = property_definitions::to_long(TOMBSTONE_COMPACTION_INTERVAL_OPTION, tmp_value, DEFAULT_TOMBSTONE_COMPACTION_INTERVAL().count());
        _tombstone_compaction_interval = db_clock::duration(std::chrono::seconds(interval));

        // FIXME: validate options.
    }
public:
    virtual ~compaction_strategy_impl() {}
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) = 0;
    virtual std::vector<resharding_descriptor> get_resharding_jobs(column_family& cf, std::vector<sstables::shared_sstable> candidates);
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

    // Check if a given sstable is entitled for tombstone compaction based on its
    // droppable tombstone histogram and gc_before.
    bool worth_dropping_tombstones(const shared_sstable& sst, gc_clock::time_point gc_before) {
        if (_disable_tombstone_compaction) {
            return false;
        }
        // ignore sstables that were created just recently because there's a chance
        // that expired tombstones still cover old data and thus cannot be removed.
        // We want to avoid a compaction loop here on the same data by considering
        // only old enough sstables.
        if (db_clock::now()-_tombstone_compaction_interval < sst->data_file_write_time()) {
            return false;
        }
        return sst->estimate_droppable_tombstone_ratio(gc_before) >= _tombstone_threshold;
    }

    virtual compaction_backlog_tracker& get_backlog_tracker() = 0;
};
}
