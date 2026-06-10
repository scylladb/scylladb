/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "tasks/task_manager.hh"

namespace locator {
class tablet_id;
enum class tablet_task_type;
class tablet_replica;
}

namespace service {

class status_helper;
class storage_service;

class tablet_virtual_task : public tasks::task_manager::virtual_task::impl {
private:
    storage_service& _ss;
private:
    static constexpr auto TABLE_ID = "table_id";
public:
    tablet_virtual_task(tasks::task_manager::module_ptr module,
            service::storage_service& ss)
        : tasks::task_manager::virtual_task::impl(std::move(module))
        , _ss(ss)
    {}
    virtual tasks::task_manager::task_group get_group() const noexcept override;
    virtual future<std::optional<tasks::virtual_task_hint>> contains(tasks::task_id task_id) const override;
    virtual future<tasks::is_abortable> is_abortable(tasks::virtual_task_hint hint) const override;

    virtual future<std::optional<tasks::task_status>> get_status(tasks::task_id id, tasks::virtual_task_hint hint) override;
    virtual future<std::optional<tasks::task_status>> wait(tasks::task_id id, tasks::virtual_task_hint hint) override;
    virtual future<> abort(tasks::task_id id, tasks::virtual_task_hint hint) noexcept override;
    virtual future<std::vector<tasks::task_stats>> get_stats() override;
private:
    std::vector<table_id> get_table_ids() const;
    future<std::optional<status_helper>> get_status_helper(tasks::task_id id, tasks::virtual_task_hint hint);
};

class task_manager_module : public tasks::task_manager::module {
private:
    service::storage_service& _ss;
public:
    task_manager_module(tasks::task_manager& tm, service::storage_service& ss) noexcept;

    std::set<locator::host_id> get_nodes() const override;
};

namespace topo {

class global_topology_request_virtual_task : public tasks::task_manager::virtual_task::impl {
private:
    service::storage_service& _ss;
public:
    global_topology_request_virtual_task(tasks::task_manager::module_ptr module,
            service::storage_service& ss)
        : tasks::task_manager::virtual_task::impl(std::move(module))
        , _ss(ss)
    {}
    virtual tasks::task_manager::task_group get_group() const noexcept override;
    virtual future<std::optional<tasks::virtual_task_hint>> contains(tasks::task_id task_id) const override;
    virtual future<tasks::is_abortable> is_abortable(tasks::virtual_task_hint hint) const override;

    virtual future<std::optional<tasks::task_status>> get_status(tasks::task_id id, tasks::virtual_task_hint hint) override;
    virtual future<std::optional<tasks::task_status>> wait(tasks::task_id id, tasks::virtual_task_hint hint) override;
    virtual future<> abort(tasks::task_id id, tasks::virtual_task_hint hint) noexcept override;
    virtual future<std::vector<tasks::task_stats>> get_stats() override;
};

class task_manager_module : public tasks::task_manager::module {
public:
    task_manager_module(tasks::task_manager& tm) noexcept;
};

}

namespace vnodes_to_tablets {

class migration_virtual_task : public tasks::task_manager::virtual_task::impl {
private:
    service::storage_service& _ss;
public:
    migration_virtual_task(tasks::task_manager::module_ptr module,
            service::storage_service& ss)
        : tasks::task_manager::virtual_task::impl(std::move(module))
        , _ss(ss)
    {}
    virtual tasks::task_manager::task_group get_group() const noexcept override;
    virtual future<std::optional<tasks::virtual_task_hint>> contains(tasks::task_id task_id) const override;

    virtual future<std::optional<tasks::task_status>> get_status(tasks::task_id id, tasks::virtual_task_hint hint) override;
    virtual future<std::optional<tasks::task_status>> wait(tasks::task_id id, tasks::virtual_task_hint hint) override;
    virtual future<> abort(tasks::task_id id, tasks::virtual_task_hint hint) noexcept override;
    virtual future<std::vector<tasks::task_stats>> get_stats() override;
    static tasks::task_id make_task_id(const sstring& keyspace);
private:
    std::optional<sstring> find_keyspace_for_task_id(tasks::task_id id) const;
    static tasks::task_stats make_task_stats(tasks::task_id id, const sstring& keyspace);
    static tasks::task_status make_task_status(tasks::task_id id, const sstring& keyspace,
            tasks::task_manager::task_state state,
            tasks::task_manager::task::progress progress);
};

class pow2_convergence_virtual_task : public tasks::task_manager::virtual_task::impl {
private:
    service::storage_service& _ss;
public:
    pow2_convergence_virtual_task(tasks::task_manager::module_ptr module,
            service::storage_service& ss)
        : tasks::task_manager::virtual_task::impl(std::move(module))
        , _ss(ss)
    {}
    virtual tasks::task_manager::task_group get_group() const noexcept override;
    virtual future<std::optional<tasks::virtual_task_hint>> contains(tasks::task_id task_id) const override;

    virtual future<std::optional<tasks::task_status>> get_status(tasks::task_id id, tasks::virtual_task_hint hint) override;
    virtual future<std::optional<tasks::task_status>> wait(tasks::task_id id, tasks::virtual_task_hint hint) override;
    virtual future<> abort(tasks::task_id id, tasks::virtual_task_hint hint) noexcept override;
    virtual future<std::vector<tasks::task_stats>> get_stats() override;

    static tasks::task_id make_ks_task_id(const sstring& keyspace);
    static tasks::task_id make_table_task_id(const sstring& keyspace, const sstring& table);
private:
    struct table_match {
        sstring keyspace;
        sstring table;
        table_id id;
    };
    std::optional<table_match> find_converging_table_for_task_id(tasks::task_id id) const;
    std::optional<sstring> find_converging_ks_for_task_id(tasks::task_id id) const;
    static tasks::task_stats make_task_stats(tasks::task_id id, const sstring& keyspace,
            const sstring& table, const sstring& scope, const sstring& entity);
    static tasks::task_status make_task_status(tasks::task_id id, const sstring& keyspace,
            const sstring& table, const sstring& scope,
            tasks::task_manager::task_state state,
            tasks::task_manager::task::progress progress,
            const sstring& progress_units, const sstring& entity,
            tasks::task_id parent_id,
            utils::chunked_vector<tasks::task_identity> children);
};

class task_manager_module : public tasks::task_manager::module {
private:
    service::storage_service& _ss;
public:
    task_manager_module(tasks::task_manager& tm, service::storage_service& ss) noexcept;

    std::set<locator::host_id> get_nodes() const override;
};

}

}
