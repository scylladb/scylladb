/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "gms/inet_address.hh"
#include "locator/host_id.hh"
#include "raft/internal.hh"
#include "streaming/stream_reason.hh"
#include "tasks/task_manager.hh"

#include <unordered_set>

namespace db {
class system_distributed_keyspace;
}

namespace locator {
class token_metadata;
class host_id_or_endpoint;
}

namespace raft {
class server;
using server_id = internal::tagged_id<struct server_id_tag>;
}

namespace service {
class storage_service;
class storage_proxy;
}

namespace node_ops {

class node_ops_task_impl : public tasks::task_manager::task::impl {
protected:
    streaming::stream_reason _reason;
    service::storage_service& _ss;
public:
    node_ops_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            streaming::stream_reason reason,
            service::storage_service& ss) noexcept;

    virtual std::string type() const override;
protected:
    future<> prepare_raft_joining(raft::server& raft_server, sharded<db::system_distributed_keyspace>& sys_dist_ks);
    future<> finish_raft_joining(raft::server& raft_server);
    virtual future<> run() override = 0;
};

class bootstrap_node_task_impl : public node_ops_task_impl {
public:
    bootstrap_node_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(std::move(module), id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::bootstrap, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class replace_node_task_impl : public node_ops_task_impl {
public:
    replace_node_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(std::move(module), id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::replace, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class rebuild_node_task_impl : public node_ops_task_impl {
public:
    rebuild_node_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(std::move(module), id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::rebuild, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class decommission_node_task_impl : public node_ops_task_impl {
public:
    decommission_node_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(std::move(module), id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::decommission, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class remove_node_task_impl : public node_ops_task_impl {
public:
    remove_node_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id id,
            unsigned sequence_number,
            std::string scope,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss) noexcept
        : node_ops_task_impl(std::move(module), id, sequence_number, std::move(scope), std::move(entity), parent_id,
            streaming::stream_reason::removenode, ss)
    {}
protected:
    virtual future<> run() override = 0;
};

class join_token_ring_task_impl : public node_ops_task_impl {
private:
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<service::storage_proxy>& _proxy;
    std::unordered_set<gms::inet_address> _initial_contact_nodes;
    std::unordered_set<gms::inet_address> _loaded_endpoints;
    std::unordered_map<gms::inet_address, sstring> _loaded_peer_features;
    std::chrono::milliseconds _delay;
public:
    join_token_ring_task_impl(tasks::task_manager::module_ptr module,
            std::string entity,
            service::storage_service& ss,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<service::storage_proxy>& proxy,
            std::unordered_set<gms::inet_address> initial_contact_nodes,
            std::unordered_set<gms::inet_address> loaded_endpoints,
            std::unordered_map<gms::inet_address, sstring> loaded_peer_features,
            std::chrono::milliseconds delay) noexcept;
private:
    gms::inet_address get_broadcast_address() const;
    bool should_bootstrap();
    bool is_replacing();
    lw_shared_ptr<const locator::token_metadata> get_token_metadata_ptr() const noexcept;
    const locator::token_metadata& get_token_metadata() const noexcept;
protected:
    virtual future<> run() override;
};

class raft_bootstrap_task_impl : public bootstrap_node_task_impl {
private:
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    raft::server& _raft_server;
public:
    raft_bootstrap_task_impl(tasks::task_manager::module_ptr module,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            raft::server& raft_server) noexcept
        : bootstrap_node_task_impl(module, tasks::task_id::create_random_id(), 0, "raft entry", std::move(entity), parent_id, ss)
        , _sys_dist_ks(sys_dist_ks)
        , _raft_server(raft_server)
    {}
protected:
    virtual future<> run() override;
};

class raft_replace_task_impl : public replace_node_task_impl {
private:
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    raft::server& _raft_server;
    gms::inet_address _ip_addr;
    raft::server_id _raft_id;
public:
    raft_replace_task_impl(tasks::task_manager::module_ptr module,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            raft::server& raft_server,
            gms::inet_address ip_addr,
            raft::server_id raft_id) noexcept
        : replace_node_task_impl(module, tasks::task_id::create_random_id(), 0, "raft entry", std::move(entity), parent_id, ss)
        , _sys_dist_ks(sys_dist_ks)
        , _raft_server(raft_server)
        , _ip_addr(ip_addr)
        , _raft_id(raft_id)
    {}
protected:
    virtual future<> run() override;
};

class start_rebuild_task_impl : public rebuild_node_task_impl {
private:
    sstring _source_dc;
public:
    start_rebuild_task_impl(tasks::task_manager::module_ptr module,
            std::string entity,
            service::storage_service& ss,
            sstring source_dc) noexcept;
protected:
    virtual future<> run() override;
};

class raft_rebuild_task_impl : public rebuild_node_task_impl {
private:
    sstring _source_dc;
public:
    raft_rebuild_task_impl(tasks::task_manager::module_ptr module,
            std::string entity,
            tasks::task_id parent_id,
            service::storage_service& ss,
            sstring source_dc) noexcept
        : rebuild_node_task_impl(module, tasks::task_id::create_random_id(), 0, "raft entry", std::move(entity), parent_id, ss)
        , _source_dc(std::move(source_dc))
    {}
protected:
    virtual future<> run() override;
};

class start_decommission_task_impl : public decommission_node_task_impl {
public:
    start_decommission_task_impl(tasks::task_manager::module_ptr module,
            std::string entity,
            service::storage_service& ss) noexcept;
protected:
    virtual future<> run() override;
};

class start_remove_node_task_impl : public remove_node_task_impl {
private:
    locator::host_id _host_id;
    std::list<locator::host_id_or_endpoint> _ignore_nodes_params;
public:
    start_remove_node_task_impl(tasks::task_manager::module_ptr module,
            std::string entity,
            service::storage_service& ss,
            locator::host_id host_id,
            std::list<locator::host_id_or_endpoint> ignore_nodes_params) noexcept;
protected:
    virtual future<> run() override;
};

class task_manager_module : public tasks::task_manager::module {
public:
    task_manager_module(tasks::task_manager& tm) noexcept : tasks::task_manager::module(tm, "node_ops") {}
};

}
