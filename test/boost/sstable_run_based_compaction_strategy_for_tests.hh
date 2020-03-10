/*
 * Copyright (C) 2018 ScyllaDB
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

#include <vector>
#include <map>
#include <boost/range/numeric.hpp>
#include "sstables/compaction_strategy_impl.hh"
#include "sstables/sstable_set.hh"
#include "sstables/compaction.hh"
#include "database.hh"

namespace sstables {

// Not suitable for production, its sole purpose is testing.

class sstable_run_based_compaction_strategy_for_tests : public compaction_strategy_impl {
    static constexpr size_t static_fragment_size_for_run = 1024*1024;
public:
    sstable_run_based_compaction_strategy_for_tests() = default;

    virtual compaction_descriptor get_sstables_for_compaction(column_family& cf, std::vector<sstables::shared_sstable> uncompacting_sstables) override {
        // Get unique runs from all uncompacting sstables
        std::vector<sstable_run> runs = cf.get_sstable_set().select(uncompacting_sstables);

        // Group similar sized runs into a bucket.
        std::map<uint64_t, std::vector<sstable_run>> similar_sized_runs;
        for (auto& run : runs) {
            bool found = false;
            for (auto& e : similar_sized_runs) {
                if (run.data_size() >= e.first*0.5 && run.data_size() <= e.first*1.5) {
                    e.second.push_back(run);
                    found = true;
                    break;
                }
            }
            if (!found) {
                similar_sized_runs[run.data_size()].push_back(run);
            }
        }
        // Get the most interesting bucket, if any, and return sstables from all its runs.
        for (auto& entry : similar_sized_runs) {
            auto& runs = entry.second;
            if (runs.size() < size_t(cf.schema()->min_compaction_threshold())) {
                continue;
            }

            auto all = boost::accumulate(runs, std::vector<shared_sstable>(), [&] (std::vector<shared_sstable>& v, const sstable_run& run) {
                v.insert(v.end(), run.all().begin(), run.all().end());
                return std::move(v);
            });
            return sstables::compaction_descriptor(std::move(all), 0, static_fragment_size_for_run);
        }
        return sstables::compaction_descriptor();
    }

    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        throw std::runtime_error("unimplemented");
    }

    virtual compaction_strategy_type type() const {
        throw std::runtime_error("unimplemented");
    }

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        throw std::runtime_error("unimplemented");
    }
};

}
