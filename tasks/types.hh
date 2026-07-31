/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/UUID.hh"

namespace tasks {

using task_id = utils::tagged_uuid<struct task_id_tag>;

// See the comment on the analogous declaration in locator/host_id.hh: this
// suppresses per-TU re-instantiation of task_id::to_sstring(), called from
// several task-manager API translation units. Matching explicit
// instantiation lives in tasks/task_manager.cc.
extern template struct utils::tagged_uuid<task_id_tag>;

struct task_info {
    task_id id;
    unsigned shard;

    task_info() noexcept : id(task_id::create_null_id()), shard(0) {}
    task_info(task_id id, unsigned parent_shard) noexcept : id(id), shard(parent_shard) {}

    operator bool() const noexcept {
        return bool(id);
    }
};

}
