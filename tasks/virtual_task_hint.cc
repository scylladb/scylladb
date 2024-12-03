/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "tasks/virtual_task_hint.hh"
#include "tasks/task_manager.hh"

namespace tasks {

locator::tablet_task_type virtual_task_hint::get_task_type() const {
    if (!task_type.has_value()) {
        on_internal_error(tasks::tmlogger, "tablet_virtual_task hint does not contain task type");
    }
    return task_type.value();
}

locator::tablet_id virtual_task_hint::get_tablet_id() const {
    if (!tablet_id.has_value()) {
        on_internal_error(tasks::tmlogger, "tablet_virtual_task hint does not contain tablet_id");
    }
    return tablet_id.value();
}

}
