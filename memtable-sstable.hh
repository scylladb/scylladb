/*
 * Copyright (C) 2017 ScyllaDB
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


// Glue logic for writing memtables to sstables

#pragma once

#include "memtable.hh"
#include "sstables/sstables.hh"
#include <seastar/core/future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/thread.hh>

future<>
write_memtable_to_sstable(memtable& mt,
        sstables::shared_sstable sst,
        bool backup = false,
        const io_priority_class& pc = default_priority_class(),
        bool leave_unsealed = false,
        seastar::thread_scheduling_group* tsg = nullptr);

