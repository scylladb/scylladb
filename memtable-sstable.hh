/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


// Glue logic for writing memtables to sstables

#pragma once

#include "sstables/shared_sstable.hh"
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include "reader_permit.hh"

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
        size_t estimated_partitions,
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
