/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "mutation/mutation.hh"
#include "db/system_keyspace.hh"
#include "timestamp.hh"

namespace db {

namespace view {

// Factory for mutations to `system.view_building_tasks` table.
class view_building_task_mutation_builder {
    api::timestamp_type _ts;
    schema_ptr _s;
    mutation _m;

public:
    view_building_task_mutation_builder(api::timestamp_type ts)
            : _ts(ts)
            , _s(db::system_keyspace::view_building_tasks())
            , _m(_s, partition_key::from_single_value(*_s, data_value("view_building").serialize_nonnull()))
    { }

    static utils::UUID new_id();

    view_building_task_mutation_builder& set_type(utils::UUID id, db::view::view_building_task::task_type type);
    view_building_task_mutation_builder& set_state(utils::UUID id, db::view::view_building_task::task_state state);
    view_building_task_mutation_builder& set_base_id(utils::UUID id, table_id base_id);
    view_building_task_mutation_builder& set_view_id(utils::UUID id, table_id view_id);
    view_building_task_mutation_builder& set_last_token(utils::UUID id, dht::token last_token);
    view_building_task_mutation_builder& set_replica(utils::UUID id, const locator::tablet_replica& replica);

    mutation build() {
        return std::move(_m);
    }

private:
    clustering_key get_ck(utils::UUID id);
};

}

}
