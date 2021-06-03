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

#pragma once

#include "sstables/writer_impl.hh"
#include "sstables/types.hh"
#include "encoding_stats.hh"

namespace sstables {
namespace mc {

std::unique_ptr<sstable_writer::writer_impl> make_writer(sstable& sst,
    const schema& s,
    uint64_t estimated_partitions,
    const sstable_writer_config& cfg,
    encoding_stats enc_stats,
    const io_priority_class& pc,
    shard_id shard);

}
}
