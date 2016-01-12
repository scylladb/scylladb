/*
 * Copyright 2015 Cloudius Systems
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

class column_family;

namespace sstables {

enum class compaction_strategy_type {
    null,
    major,
    size_tiered,
    leveled,
    // FIXME: Add support to DateTiered.
};

class compaction_strategy_impl;
class sstable;
struct compaction_descriptor;

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
    compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<lw_shared_ptr<sstable>> candidates);

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
        } else {
            throw exceptions::configuration_exception(sprint("Unable to find compaction strategy class '%s'", name));
        }
    }

    compaction_strategy_type type() const;

    sstring name() const {
        return name(type());
    }
};

// Creates a compaction_strategy object from one of the strategies available.
compaction_strategy make_compaction_strategy(compaction_strategy_type strategy, const std::map<sstring, sstring>& options);

}
