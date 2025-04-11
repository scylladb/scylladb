/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/uuid.idl.hh"

namespace db {
namespace view {
class update_backlog {
    size_t get_current_bytes();
    size_t get_max_bytes();
};
}
}

namespace service {
namespace view_building {
struct view_task_result {
    enum class command_status: uint8_t {
        fail,
        success,
    };
    service::view_building::view_task_result::command_status status;
};
}
}

verb [[cancellable]] work_on_view_building_tasks(std::vector<utils::UUID> tasks_ids) -> std::vector<service::view_building::view_task_result>
