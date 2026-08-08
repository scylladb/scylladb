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

// task_id::to_sstring() is the only non-trivial (non-constexpr) member of
// tagged_uuid<Tag>; called from several task-manager API translation units,
// so suppress its per-TU re-instantiation. Matching explicit instantiation
// lives in tasks/task_manager.cc. Must be named fully-qualified outside
// namespace tasks (see locator/host_id.hh).
extern template struct utils::tagged_uuid<tasks::task_id_tag>;
