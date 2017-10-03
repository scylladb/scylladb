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
    sstables::shared_sstable _sst;
    sstables::mutation_reader _smr;
public:
    sstable_range_wrapping_reader(sstables::shared_sstable sst,
            schema_ptr s,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr)
        : _sst(sst)
        , _smr(sst->read_range_rows(std::move(s), pr, slice, pc, fwd, fwd_mr)) {
    }
    virtual future<streamed_mutation_opt> operator()() override {
        return _smr.read();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        return _smr.fast_forward_to(pr);
    }
};
