/*
 * Copyright (C) 2018-present ScyllaDB
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
#include "sstables/compaction_strategy_impl.hh"
#include "sstables/sstable_set.hh"
#include "sstables/compaction.hh"
#include "database.hh"

namespace sstables {

// Not suitable for production, its sole purpose is testing.

class sstable_run_based_compaction_strategy_for_tests : public compaction_strategy_impl {
    static constexpr size_t static_fragment_size_for_run = 1024*1024;
public:
    sstable_run_based_compaction_strategy_for_tests();

    virtual compaction_descriptor get_sstables_for_compaction(column_family& cf, std::vector<sstables::shared_sstable> uncompacting_sstables) override;

    virtual int64_t estimated_pending_compactions(column_family& cf) const override;

    virtual compaction_strategy_type type() const;

    virtual compaction_backlog_tracker& get_backlog_tracker() override;
};

}
