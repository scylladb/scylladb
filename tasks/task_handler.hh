/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "tasks/task_manager.hh"
#include "utils/chunked_vector.hh"

namespace tasks {

struct task_identity {
    gms::inet_address node;
    task_id task_id;
};

struct task_status {
    tasks::task_id task_id;
    std::string type;
    std::string scope;
    task_manager::task_state state;
    is_abortable is_abortable;
    db_clock::time_point start_time;
    db_clock::time_point end_time;
    std::string error;
    tasks::task_id parent_id;
    uint64_t sequence_number;
    unsigned shard;
    std::string keyspace;
    std::string table;
    std::string entity;
    std::string progress_units;
    task_manager::task::progress progress;
    std::vector<tasks::task_id> children;
};

struct task_stats {
    tasks::task_id task_id;
    std::string type;
    task_kind kind;
    std::string scope;
    task_manager::task_state state;
    uint64_t sequence_number;
    std::string keyspace;
    std::string table;
    std::string entity;
};

class task_handler {
private:
    task_manager& _tm;
    task_id _id;
public:
    explicit task_handler(task_manager& tm, task_id id) noexcept
        : _tm(tm)
        , _id(id)
    {}
    future<task_stats> get_stats();
    future<task_status> get_status();
    future<task_status> wait_for_task();
    future<utils::chunked_vector<task_status>> get_status_recursively(bool local);
    future<> abort();
};

}
