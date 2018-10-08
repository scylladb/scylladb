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
