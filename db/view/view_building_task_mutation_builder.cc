/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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
view_building_task_mutation_builder& view_building_task_mutation_builder::set_aborted(utils::UUID id, bool aborted) {
    _m.set_clustered_cell(get_ck(id), "aborted", data_value(aborted), _ts);
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

<<<<<<< HEAD
||||||| parent of 4227cab5cb (db/view/view_building_task_mutation_builder: add helper method)
view_building_task_mutation_builder& view_building_task_mutation_builder::del_tasks_before(utils::UUID id) {
    auto ck = get_ck(id);
    range_tombstone rt(
        position_in_partition::before_all_clustered_rows(),
        position_in_partition_view(ck, bound_weight::before_all_prefixed),
        tombstone{_ts, gc_clock::now()});
    _m.partition().apply_row_tombstone(*_s, std::move(rt));
    return *this;
}

view_building_task_mutation_builder& view_building_task_mutation_builder::del_all_tasks() {
    range_tombstone rt(
        position_in_partition::before_all_clustered_rows(),
        position_in_partition::after_all_clustered_rows(),
        tombstone{_ts, gc_clock::now()});
    _m.partition().apply_row_tombstone(*_s, std::move(rt));
    return *this;
}

view_building_task_mutation_builder& view_building_task_mutation_builder::set_min_task_id(utils::UUID id) {
    _m.set_static_cell("min_task_id", data_value(id), _ts);
    return *this;
}

=======
view_building_task_mutation_builder& view_building_task_mutation_builder::del_tasks_before(utils::UUID id) {
    auto ck = get_ck(id);
    range_tombstone rt(
        position_in_partition::before_all_clustered_rows(),
        position_in_partition_view(ck, bound_weight::before_all_prefixed),
        tombstone{_ts, gc_clock::now()});
    _m.partition().apply_row_tombstone(*_s, std::move(rt));
    return *this;
}

view_building_task_mutation_builder& view_building_task_mutation_builder::del_all_tasks() {
    range_tombstone rt(
        position_in_partition::before_all_clustered_rows(),
        position_in_partition::after_all_clustered_rows(),
        tombstone{_ts, gc_clock::now()});
    _m.partition().apply_row_tombstone(*_s, std::move(rt));
    return *this;
}

view_building_task_mutation_builder& view_building_task_mutation_builder::set_min_task_id(utils::UUID id) {
    _m.set_static_cell("min_task_id", data_value(id), _ts);
    return *this;
}

view_building_task_mutation_builder& view_building_task_mutation_builder::set_task(db::view::view_building_task& task) {
    auto id = task.id;
    set_type(id, task.type);
    set_aborted(id, task.aborted);
    set_base_id(id, task.base_id);
    if (task.view_id) {
        set_view_id(id, *task.view_id);
    }
    set_last_token(id, task.last_token);
    set_replica(id, task.replica);
    return *this;
}

>>>>>>> 4227cab5cb (db/view/view_building_task_mutation_builder: add helper method)
}

}
