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

#include "sstables/sstables.hh"
#include "query-request.hh"
#include "mutation_reader.hh"

class sstable_range_wrapping_reader final : public mutation_reader::impl {
    lw_shared_ptr<sstables::sstable> _sst;
    sstables::mutation_reader _smr;
public:
    sstable_range_wrapping_reader(lw_shared_ptr<sstables::sstable> sst,
        schema_ptr s, const query::partition_range& pr, const io_priority_class& pc)
        : _sst(sst)
        , _smr(sst->read_range_rows(std::move(s), pr, pc)) {
    }
    virtual future<mutation_opt> operator()() override {
        return _smr.read();
    }
};
