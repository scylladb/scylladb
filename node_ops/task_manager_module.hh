/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "streaming/stream_reason.hh"
#include "tasks/task_manager.hh"

namespace service {
class storage_service;
enum class topology_request: uint16_t;
}

namespace tasks {
struct virtual_task_hint;
}

namespace node_ops {

class node_ops_virtual_task : public tasks::task_manager::virtual_task::impl {
private:
    service::storage_service& _ss;
public:
    node_ops_virtual_task(tasks::task_manager::module_ptr module,
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
    future<std::optional<tasks::task_status>> get_status_helper(tasks::task_id id, tasks::virtual_task_hint hint) const;
};

class streaming_task_impl : public tasks::task_manager::task::impl {
private:
    streaming::stream_reason _reason;
    std::optional<shared_future<>>& _result;
    std::function<future<>()> _action;
public:
    streaming_task_impl(tasks::task_manager::module_ptr module,
            tasks::task_id parent_id,
            streaming::stream_reason reason,
            std::optional<shared_future<>>& result,
            std::function<future<>()> action) noexcept;

    virtual std::string type() const override;
    virtual tasks::is_internal is_internal() const noexcept override;
protected:
    virtual future<> run() override;
};

class task_manager_module : public tasks::task_manager::module {
private:
    service::storage_service& _ss;
public:
    task_manager_module(tasks::task_manager& tm, service::storage_service& ss) noexcept;

    virtual std::set<locator::host_id> get_nodes() const override;
};

}
