/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "repair/repair.hh"
#include "tasks/task_manager.hh"

class repair_task_impl : public tasks::task_manager::task::impl {
public:
    repair_task_impl(tasks::task_manager::module_ptr module, tasks::task_id id, unsigned sequence_number = 0, std::string keyspace = "", std::string table = "", std::string type = "", std::string entity = "", tasks::task_id parent_id = tasks::task_id::create_null_id())
        : tasks::task_manager::task::impl(module, id, sequence_number, std::move(keyspace), std::move(table), std::move(type), std::move(entity), parent_id) {
        _status.progress_units = "ranges";
    }
protected:
    repair_uniq_id get_repair_uniq_id() const noexcept {
        return repair_uniq_id{
            .id = _status.sequence_number,
            .task_data = tasks::task_info(_status.id, _status.shard)
        };
    }

    virtual future<> run() override = 0;
};

class user_requested_repair_task_impl : public repair_task_impl {
private:
    std::vector<sstring> _cfs;
    dht::token_range_vector _ranges;
    std::vector<sstring> _hosts;
    std::vector<sstring> _data_centers;
    std::unordered_set<gms::inet_address> _ignore_nodes;
public:
    user_requested_repair_task_impl(tasks::task_manager::module_ptr module, repair_uniq_id id, std::string keyspace, std::string type, std::string entity, std::vector<sstring> cfs, dht::token_range_vector ranges, std::vector<sstring> hosts, std::vector<sstring> data_centers, std::unordered_set<gms::inet_address> ignore_nodes)
        : repair_task_impl(module, id.uuid(), id.id, std::move(keyspace), "", std::move(type), std::move(entity))
        , _cfs(std::move(cfs))
        , _ranges(std::move(ranges))
        , _hosts(std::move(hosts))
        , _data_centers(std::move(data_centers))
        , _ignore_nodes(std::move(ignore_nodes))
    {}
protected:
    future<> run() override;

    // TODO: implement abort and progress for user-requested repairs
};

class data_sync_repair_task_impl : public repair_task_impl {
private:
    dht::token_range_vector _ranges;
    std::unordered_map<dht::token_range, repair_neighbors> _neighbors;
    streaming::stream_reason _reason;
    std::optional<node_ops_id> _ops_uuid;
public:
    data_sync_repair_task_impl(tasks::task_manager::module_ptr module, repair_uniq_id id, std::string keyspace, std::string type, std::string entity, dht::token_range_vector ranges, std::unordered_map<dht::token_range, repair_neighbors> neighbors, streaming::stream_reason reason, std::optional<node_ops_id> ops_uuid)
        : repair_task_impl(module, id.uuid(), id.id, std::move(keyspace), "", std::move(type), std::move(entity))
        , _ranges(std::move(ranges))
        , _neighbors(std::move(neighbors))
        , _reason(reason)
        , _ops_uuid(ops_uuid) {}
protected:
    future<> run() override;

    // TODO: implement abort and progress for data-sync repairs
};

class repair_module : public tasks::task_manager::module {
private:
    repair_service& _rs;
public:
    repair_module(tasks::task_manager& tm, repair_service& rs) noexcept : module(tm, "repair"), _rs(rs) {}

    repair_service& repair_service() noexcept {
        return _rs;
    }

    repair_uniq_id new_repair_uniq_id() noexcept {
        return repair_uniq_id{
            .id = new_sequence_number(),
            .task_data = tasks::task_info(tasks::task_id::create_random_id(), this_shard_id())
        };
    }
};
