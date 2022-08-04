/*
 * Copyright 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "gms/inet_address_serializer.hh"
#include "db/hints/sync_point.hh"

#include "idl/replay_position.idl.hh"
#include "idl/uuid.idl.hh"

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
