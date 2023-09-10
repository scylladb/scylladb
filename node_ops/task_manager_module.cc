/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "node_ops/task_manager_module.hh"
#include "service/storage_service.hh"

namespace node_ops {

node_ops_task_impl::node_ops_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id id,
        unsigned sequence_number,
        std::string scope,
        std::string entity,
        tasks::task_id parent_id,
        streaming::stream_reason reason,
        service::storage_service& ss) noexcept
    : tasks::task_manager::task::impl(std::move(module), id, sequence_number,
        std::move(scope), "", "", std::move(entity), parent_id)
    , _reason(reason)
    , _ss(ss)
{
    // FIXME: add progress units
}

std::string node_ops_task_impl::type() const {
    return fmt::format("{}", _reason);
}

}
