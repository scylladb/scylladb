/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "db/view/view_building_task_mutation_builder.hh"
#include "keys/keys.hh"

namespace db {

namespace view {

utils::UUID view_building_task_mutation_builder::new_id() {
    return utils::UUID_gen::get_time_UUID();
}

clustering_key view_building_task_mutation_builder::get_ck(utils::UUID id) {
    return clustering_key::from_single_value(*_s, data_value(id).serialize_nonnull());
}

view_building_task_mutation_builder& view_building_task_mutation_builder::set_type(utils::UUID id, db::view::view_building_task::task_type type) {
    _m.set_clustered_cell(get_ck(id), "type", data_value(task_type_to_sstring(type)), _ts);
    return *this;
}
view_building_task_mutation_builder& view_building_task_mutation_builder::set_state(utils::UUID id, db::view::view_building_task::task_state state) {
    _m.set_clustered_cell(get_ck(id), "state", data_value(task_state_to_sstring(state)), _ts);
    return *this;
}
view_building_task_mutation_builder& view_building_task_mutation_builder::set_base_id(utils::UUID id, table_id base_id) {
    _m.set_clustered_cell(get_ck(id), "base_id", data_value(base_id.uuid()), _ts);
    return *this;
}
view_building_task_mutation_builder& view_building_task_mutation_builder::set_view_id(utils::UUID id, table_id view_id) {
    _m.set_clustered_cell(get_ck(id), "view_id", data_value(view_id.uuid()), _ts);
    return *this;
}
view_building_task_mutation_builder& view_building_task_mutation_builder::set_last_token(utils::UUID id, dht::token last_token) {
    _m.set_clustered_cell(get_ck(id), "last_token", data_value(dht::token::to_int64(last_token)), _ts);
    return *this;
}
view_building_task_mutation_builder& view_building_task_mutation_builder::set_replica(utils::UUID id, const locator::tablet_replica& replica) {
    _m.set_clustered_cell(get_ck(id), "host_id", data_value(replica.host.uuid()), _ts);
    _m.set_clustered_cell(get_ck(id), "shard", data_value(int32_t(replica.shard)), _ts);
    return *this;
}

view_building_task_mutation_builder& view_building_task_mutation_builder::del_task(utils::UUID id) {
    _m.partition().apply_delete(*_s, clustering_key_prefix::from_single_value(*_s, data_value(id).serialize_nonnull()), tombstone{_ts, gc_clock::now()});
    return *this;
}

view_building_task_mutation_builder& view_building_task_mutation_builder::set_task(db::view::view_building_task& task) {
    auto id = task.id;
    set_type(id, task.type);
    set_state(id, task.state);
    set_base_id(id, task.base_id);
    if (task.view_id) {
        set_view_id(id, *task.view_id);
    }
    set_last_token(id, task.last_token);
    set_replica(id, task.replica);
    return *this;
}

}

}
