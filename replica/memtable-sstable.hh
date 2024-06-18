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

class mutation_reader;
class reader_permit;

namespace sstables {
class sstables_manager;
class sstable_writer_config;
class write_monitor;
}

namespace replica {

class memtable;

seastar::future<>
write_memtable_to_sstable(mutation_reader reader,
        memtable& mt, sstables::shared_sstable sst,
        size_t estimated_partitions,
        sstables::write_monitor& monitor,
        sstables::sstable_writer_config& cfg);

seastar::future<>
write_memtable_to_sstable(memtable& mt,
        sstables::shared_sstable sst);

}
