/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "sstables/shared_sstable.hh"
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>

class memtable;
class flat_mutation_reader;

namespace sstables {
class sstables_manager;
class sstable_writer_config;
class write_monitor;
}

seastar::future<>
write_memtable_to_sstable(flat_mutation_reader reader,
        memtable& mt, sstables::shared_sstable sst,
        sstables::write_monitor& monitor,
        sstables::sstable_writer_config& cfg,
        const seastar::io_priority_class& pc);

seastar::future<>
write_memtable_to_sstable(reader_permit permit,
        memtable& mt,
        sstables::shared_sstable sst,
        sstables::write_monitor& mon,
        sstables::sstable_writer_config& cfg,
        const seastar::io_priority_class& pc = seastar::default_priority_class());

seastar::future<>
write_memtable_to_sstable(memtable& mt,
        sstables::shared_sstable sst,
        sstables::sstable_writer_config cfg);
