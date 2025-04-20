/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
