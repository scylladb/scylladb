/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "mutation/mutation.hh"
#include "db/system_keyspace.hh"
#include "mutation/timestamp.hh"

namespace db {

namespace view {

// Factory for mutations to `system.view_building_tasks` table.
class view_building_task_mutation_builder {
    api::timestamp_type _ts;
<<<<<<< HEAD
||||||| parent of 2561cc1546 (db/view/view_building_task_mutation_builder: make uuid generator optional)
    task_uuid_generator _uuid_gen;
=======
    std::optional<task_uuid_generator> _uuid_gen;
>>>>>>> 2561cc1546 (db/view/view_building_task_mutation_builder: make uuid generator optional)
    schema_ptr _s;
    mutation _m;

public:
<<<<<<< HEAD
    view_building_task_mutation_builder(api::timestamp_type ts)
||||||| parent of 2561cc1546 (db/view/view_building_task_mutation_builder: make uuid generator optional)
    view_building_task_mutation_builder(api::timestamp_type ts, task_uuid_generator uuid_gen)
=======
    view_building_task_mutation_builder(api::timestamp_type ts, std::optional<task_uuid_generator> uuid_gen = std::nullopt)
>>>>>>> 2561cc1546 (db/view/view_building_task_mutation_builder: make uuid generator optional)
            : _ts(ts)
            , _s(db::system_keyspace::view_building_tasks())
            , _m(_s, partition_key::from_single_value(*_s, data_value("view_building").serialize_nonnull()))
    { }

    static utils::UUID new_id();

    view_building_task_mutation_builder& set_type(utils::UUID id, db::view::view_building_task::task_type type);
    view_building_task_mutation_builder& set_aborted(utils::UUID id, bool aborted);
    view_building_task_mutation_builder& set_base_id(utils::UUID id, table_id base_id);
    view_building_task_mutation_builder& set_view_id(utils::UUID id, table_id view_id);
    view_building_task_mutation_builder& set_last_token(utils::UUID id, dht::token last_token);
    view_building_task_mutation_builder& set_replica(utils::UUID id, const locator::tablet_replica& replica);
    view_building_task_mutation_builder& del_task(utils::UUID id);
<<<<<<< HEAD
||||||| parent of 4227cab5cb (db/view/view_building_task_mutation_builder: add helper method)
    // Deletes all tasks with clustering key < id using a range tombstone.
    view_building_task_mutation_builder& del_tasks_before(utils::UUID id);
    // Deletes all tasks using a range tombstone covering the entire clustering range.
    view_building_task_mutation_builder& del_all_tasks();
    // Sets the static column min_task_id to `id`.
    view_building_task_mutation_builder& set_min_task_id(utils::UUID id);
=======
    // Deletes all tasks with clustering key < id using a range tombstone.
    view_building_task_mutation_builder& del_tasks_before(utils::UUID id);
    // Deletes all tasks using a range tombstone covering the entire clustering range.
    view_building_task_mutation_builder& del_all_tasks();
    // Sets the static column min_task_id to `id`.
    view_building_task_mutation_builder& set_min_task_id(utils::UUID id);
    view_building_task_mutation_builder& set_task(db::view::view_building_task& task);
>>>>>>> 4227cab5cb (db/view/view_building_task_mutation_builder: add helper method)

    mutation build() {
        return std::move(_m);
    }

private:
    clustering_key get_ck(utils::UUID id);
};

}

}
