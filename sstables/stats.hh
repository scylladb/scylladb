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

#pragma once

namespace sstables {

class sstables_stats {
    static thread_local struct stats {
        uint64_t partition_writes = 0;
        uint64_t static_row_writes = 0;
        uint64_t row_writes = 0;
        uint64_t tombstone_writes = 0;
        uint64_t range_tombstone_writes = 0;
    } _shard_stats;

    stats& _stats = _shard_stats;

public:
    static const stats& get_shard_stats() {
        return _shard_stats;
    }

    inline void on_partition_write() {
        ++_stats.partition_writes;
    }

    inline void on_static_row_write() {
        ++_stats.static_row_writes;
    }

    inline void on_row_write() {
        ++_stats.row_writes;
    }

    inline void on_tombstone_write() {
        ++_stats.tombstone_writes;
    }

    inline void on_range_tombstone_write() {
        ++_stats.range_tombstone_writes;
    }
};

}
