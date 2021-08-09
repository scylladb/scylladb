/*
 * Copyright 2021-present ScyllaDB
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

namespace db {

namespace hints {

// Contains per-endpoint and per-shard information about replay positions
// for a particular type of hint queues (regular mutation hints or MV update hints)
struct per_manager_sync_point_v1 {
    std::vector<gms::inet_address> addresses;
    std::vector<db::replay_position> flattened_rps;
};

struct sync_point_v1 {
    utils::UUID host_id;
    uint16_t shard_count;

    // Sync point information for regular mutation hints
    db::hints::per_manager_sync_point_v1 regular_sp;

    // Sync point information for materialized view hints
    db::hints::per_manager_sync_point_v1 mv_sp;
};

}

}
