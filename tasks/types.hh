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

enum class task_kind {
    cluster,
    node,
};

// A handle identifying a prospective parent task.
//
// A handle to a regular (node) parent always carries the parent's sequence
// number, so that a child can inherit it without a cross-shard lookup.
class task_info {
    task_id _id;
    unsigned _shard;
    uint64_t _sequence_number;
    task_kind _kind = task_kind::node;

    task_info(task_id id, unsigned shard, uint64_t sequence_number, task_kind kind) noexcept
        : _id(id)
        , _shard(shard)
        , _sequence_number(sequence_number)
        , _kind(kind)
    {}
public:
    task_id get_id() const noexcept {
        return _id;
    }

    unsigned get_shard() const noexcept {
        return _shard;
    }

    uint64_t get_sequence_number() const noexcept {
        return _sequence_number;
    }

    task_kind get_kind() const noexcept {
        return _kind;
    }

    operator bool() const noexcept {
        return bool(_id);
    }

    friend task_info make_empty_task_info() noexcept;
    friend task_info make_node_task_info(task_id id, unsigned shard, uint64_t sequence_number) noexcept;
    friend task_info make_cluster_task_info(task_id id) noexcept;
};

// A null handle: the task has no parent.
inline task_info make_empty_task_info() noexcept {
    return task_info(task_id::create_null_id(), 0, 0, task_kind::node);
}

// A handle to a regular (node) parent.
inline task_info make_node_task_info(task_id id, unsigned shard, uint64_t sequence_number) noexcept {
    return task_info(id, shard, sequence_number, task_kind::node);
}

// A handle to a virtual (cluster-wide) parent.
inline task_info make_cluster_task_info(task_id id) noexcept {
    return task_info(id, 0, 0, task_kind::cluster);
}

}
