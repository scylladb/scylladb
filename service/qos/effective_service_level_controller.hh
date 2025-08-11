/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>

#include "cql3/description.hh"
#include "service/qos/qos_common.hh"
#include "service/qos/service_level_controller.hh"
#include "utils/atomic_vector.hh"

namespace auth {

class service;

} // namespace auth

namespace qos {

struct effective_service_level_event_subscriber {
    virtual ~effective_service_level_event_subscriber() noexcept = default;

    virtual future<> on_effective_service_levels_cache_reloaded() = 0;
};

/// NOTICE: Dummy implementation for now -- work in progress.
class effective_service_level_controller : public peering_sharded_service<effective_service_level_controller> {
private:
    friend class service_level_controller;

private:
    [[maybe_unused]] service_level_controller& _sl_controller;
    [[maybe_unused]] auth::service& _auth_service;

    /// Mappings `role name` -> `service level options`.
    [[maybe_unused]] std::map<sstring, service_level_options> _mapping_cache;
    [[maybe_unused]] atomic_vector<effective_service_level_event_subscriber*> _subscribers;
    /// This gate is supposed to synchronize `effective_service_level_controller::stop`
    /// with other tasks that this interface performs. Because of that, every coroutine
    /// function of this class should hold it throughout its execution.
    [[maybe_unused]] seastar::named_gate _stop_gate;

public:
    effective_service_level_controller(service_level_controller&, auth::service&);
    ~effective_service_level_controller() noexcept;

    future<> stop();

    /// Find the effective service level for a given role.
    /// If there is no applicable service level for it, `std::nullopt` is returned instead.
    future<std::optional<service_level_options>> find_effective_service_level(const sstring& role_name);
    /// Synchronous version of `find_effective_service_level` that only checks the cache.
    std::optional<service_level_options> find_cached_effective_service_level(const sstring& role_name);

    future<scheduling_group> get_user_scheduling_group(const std::optional<auth::authenticated_user>& usr);

    template <typename Func, typename Ret = std::invoke_result_t<Func>>
        requires std::invocable<Func>
    futurize_t<Ret> with_user_service_level(const std::optional<auth::authenticated_user>& user, Func&& func) {
        if (user && user->name) {
            const std::optional<service_level_options> maybe_sl_opts = co_await find_effective_service_level(*user->name);
            const sstring& sl_name = maybe_sl_opts && maybe_sl_opts->shares_name
                    ? *maybe_sl_opts->shares_name
                    : service_level_controller::default_service_level_name;

            co_return co_await _sl_controller.with_service_level(sl_name, std::forward<Func>(func));
        } else {
            co_return co_await _sl_controller.with_service_level(service_level_controller::default_service_level_name, std::forward<Func>(func));
        }
    }

    future<std::vector<cql3::description>> describe_attached_service_levels();

    /// Must be executed on shard 0.
    future<> reload_cache();

    void register_subscriber(effective_service_level_event_subscriber*);
    future<> unregister_subscriber(effective_service_level_event_subscriber*);

private:
    void clear_cache();

    future<> notify_cache_reloaded();
};

} // namespace qos
