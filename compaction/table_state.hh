/*
 * Copyright (C) 2021-present ScyllaDB
 *
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

#include "schema_fwd.hh"
#include "sstables/sstable_set.hh"

class reader_permit;

namespace sstables {
class compaction_strategy;
struct sstable_writer_config;
}

namespace compaction {

class table_state {
public:
    virtual ~table_state() {}
    virtual const schema_ptr& schema() const noexcept = 0;
    // min threshold as defined by table.
    virtual unsigned min_compaction_threshold() const noexcept = 0;
    virtual bool compaction_enforce_min_threshold() const noexcept = 0;
    virtual const sstables::sstable_set& get_sstable_set() const = 0;
    virtual std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables) const = 0;
    virtual const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept = 0;
    virtual sstables::compaction_strategy& get_compaction_strategy() const noexcept = 0;
    virtual reader_permit make_compaction_reader_permit() const = 0;
    virtual sstables::sstable_writer_config configure_writer(sstring origin) const = 0;
};

}

