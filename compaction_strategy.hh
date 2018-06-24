/*
 * Copyright (C) 2015 ScyllaDB
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

#include "sstables/shared_sstable.hh"
#include "exceptions/exceptions.hh"
#include "sstables/compaction_backlog_manager.hh"

class table;
using column_family = table;
class schema;
using schema_ptr = lw_shared_ptr<const schema>;

namespace sstables {

enum class compaction_strategy_type {
    null,
    major,
    size_tiered,
    leveled,
    date_tiered,
    time_window,
};

class compaction_strategy_impl;
class sstable;
class sstable_set;
struct compaction_descriptor;
struct resharding_descriptor;

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

    std::vector<resharding_descriptor> get_resharding_jobs(column_family& cf, std::vector<shared_sstable> candidates);

    // Some strategies may look at the compacted and resulting sstables to
    // get some useful information for subsequent compactions.
    void notify_completion(const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added);

    // Return if parallel compaction is allowed by strategy.
    bool parallel_compaction() const;

    // Return if optimization to rule out sstables based on clustering key filter should be applied.
    bool use_clustering_key_filter() const;

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
            throw exceptions::configuration_exception(sprint("Unable to find compaction strategy class '%s'", name));
        }
    }

    compaction_strategy_type type() const;

    sstring name() const {
        return name(type());
    }

    sstable_set make_sstable_set(schema_ptr schema) const;

    compaction_backlog_tracker& get_backlog_tracker();
};

// Creates a compaction_strategy object from one of the strategies available.
compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options);

}
