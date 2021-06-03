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

#include "sstables/sstable_set.hh"
#include "streaming/stream_reason.hh"

class database;
namespace db {
class system_distributed_keyspace;
namespace view {
class view_update_generator;
}
}

namespace streaming {

std::function<future<>(flat_mutation_reader)> make_streaming_consumer(sstring origin,
    sharded<database>& db,
    sharded<db::system_distributed_keyspace>& sys_dist_ks,
    sharded<db::view::view_update_generator>& vug,
    uint64_t estimated_partitions,
    stream_reason reason,
    sstables::offstrategy offstrategy);

}
