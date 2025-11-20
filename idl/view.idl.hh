/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/uuid.idl.hh"
#include "message/messaging_service.hh"

namespace db {
namespace view {
class update_backlog {
    size_t get_current_bytes();
    size_t get_max_bytes();
};

struct view_task_result {
    enum class command_status: uint8_t {
        success,
        abort,
    };
    db::view::view_task_result::command_status status;
};

}

}
namespace query {

struct rebuild_materialized_view_request {
    dht::partition_range_vector base_pranges;
    query::partition_slice base_slice;

    ::table_id view_id;
};


struct rebuild_materialized_view_result {
    enum class command_status: uint8_t {
        fail,
        success
    };
    query::rebuild_materialized_view_result::command_status status;
};

}

verb [[cancellable]] work_on_view_building_tasks(std::vector<utils::UUID> tasks_ids) -> std::vector<db::view::view_task_result>
verb [[with_timeout]] rebuild_materialized_view(query::rebuild_materialized_view_request cmd [[ref]]) -> query::rebuild_materialized_view_result;
