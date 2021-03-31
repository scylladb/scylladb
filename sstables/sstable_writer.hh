/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <memory>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/smp.hh>
#include "schema_fwd.hh"
#include "mutation_fragment.hh"

struct encoding_stats;

namespace sstables {

class sstable;
struct sstable_writer_config;

class sstable_writer {
public:
    class writer_impl;
private:
    std::unique_ptr<writer_impl> _impl;
public:
    sstable_writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
            const sstable_writer_config&, encoding_stats enc_stats,
            const io_priority_class& pc, shard_id shard = this_shard_id());

    sstable_writer(sstable_writer&& o);
    sstable_writer& operator=(sstable_writer&& o);

    ~sstable_writer();

    void consume_new_partition(const dht::decorated_key& dk);
    void consume(tombstone t);
    stop_iteration consume(static_row&& sr);
    stop_iteration consume(clustering_row&& cr);
    stop_iteration consume(range_tombstone&& rt);
    stop_iteration consume_end_of_partition();
    void consume_end_of_stream();
};

} // namespace sstables

