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

#include "database.hh"
#include "sstables/sstables.hh"

static logging::logger tlogger("table");

flat_mutation_reader
table::make_reader_excluding_sstable(schema_ptr s,
        sstables::shared_sstable sst,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const {
    std::vector<flat_mutation_reader> readers;
    readers.reserve(_memtables->size() + 1);

    for (auto&& mt : *_memtables) {
        readers.emplace_back(mt->make_flat_reader(s, range, slice, pc, trace_state, fwd, fwd_mr));
    }

    auto effective_sstables = ::make_lw_shared<sstables::sstable_set>(*_sstables);
    effective_sstables->erase(sst);

    readers.emplace_back(make_sstable_reader(s, std::move(effective_sstables), range, slice, pc, std::move(trace_state), fwd, fwd_mr));
    return make_combined_reader(s, std::move(readers), fwd, fwd_mr);
}

void table::move_sstable_from_staging_in_thread(sstables::shared_sstable sst) {
    try {
        sst->move_to_new_dir_in_thread(dir(), sst->generation());
    } catch (...) {
        tlogger.warn("Failed to move sstable {} from staging: {}", sst->get_filename(), std::current_exception());
        return;
    }
    _sstables_staging.erase(sst->generation());
    _compaction_strategy.get_backlog_tracker().add_sstable(sst);
}

