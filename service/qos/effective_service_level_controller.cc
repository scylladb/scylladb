/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/qos/effective_service_level_controller.hh"

#include "service/qos/service_level_controller.hh"

namespace qos {

effective_service_level_controller::effective_service_level_controller(
        service_level_controller& sl_controller, auth::service& auth_service)
    : _sl_controller(sl_controller)
    , _auth_service(auth_service)
    , _stop_gate("effective_service_level_controller_stop_gate")
{
    _sl_controller.register_effective_service_level_controller(*this);
}

effective_service_level_controller::~effective_service_level_controller() noexcept {
    // The corresponding `service_level_controller` should've already have
    // this `effective_service_level_controller` unregistered by now.
    SCYLLA_ASSERT(_sl_controller._esl_controller == nullptr);
}

future<> effective_service_level_controller::stop() {
    SCYLLA_ASSERT(_sl_controller._esl_controller == this);
    _sl_controller.unregister_effective_service_level_controller();

    co_await _stop_gate.close();
}

#if 0

future<std::optional<service_level_options>> service_level_controller::find_effective_service_level(const sstring& role_name) {
    if (_sl_data_accessor->can_use_effective_service_level_cache()) {
        auto effective_sl_it = _effective_service_levels_db.find(role_name);
        co_return effective_sl_it != _effective_service_levels_db.end() 
            ? std::optional<service_level_options>(effective_sl_it->second)
            : std::nullopt;
    } else {
        auto& role_manager = _auth_service.local().underlying_role_manager();
        auto roles = co_await role_manager.query_granted(role_name, auth::recursive_role_query::yes);

        // converts a list of roles into the chosen service level.
        co_return co_await ::map_reduce(roles.begin(), roles.end(), [&role_manager, this] (const sstring& role) {
            return role_manager.get_attribute(role, "service_level").then_wrapped([this, role] (future<std::optional<sstring>> sl_name_fut) -> std::optional<service_level_options> {
                try {
                    std::optional<sstring> sl_name = sl_name_fut.get();
                    if (!sl_name) {
                        return std::nullopt;
                    }
                    auto sl_it = _service_levels_db.find(*sl_name);
                    if ( sl_it == _service_levels_db.end()) {
                        return std::nullopt;
                    }

                    sl_it->second.slo.init_effective_names(*sl_name);
                    auto slo = sl_it->second.slo;
                    slo.shares_name = sl_name;
                    return slo;
                } catch (...) { // when we fail, we act as if the attribute does not exist so the node
                            // will not be brought down.
                    return std::nullopt;
                }
            });
        }, std::optional<service_level_options>{}, [] (std::optional<service_level_options> first, std::optional<service_level_options> second) -> std::optional<service_level_options> {
            if (!second) {
                return first;
            } else if (!first) {
                return second;
            } else {
                return first->merge_with(*second);
            }
        });
    }
}

std::optional<service_level_options> service_level_controller::find_cached_effective_service_level(const sstring& role_name) {
    if (!_sl_data_accessor->is_v2()) {
        return std::nullopt;
    }

    auto effective_sl_it = _effective_service_levels_db.find(role_name);
    return effective_sl_it != _effective_service_levels_db.end() 
        ? std::optional<service_level_options>(effective_sl_it->second)
        : std::nullopt;
}

future<scheduling_group> service_level_controller::get_user_scheduling_group(const std::optional<auth::authenticated_user>& usr) {
    if (usr && usr->name) {
        auto sl_opt = co_await find_effective_service_level(*usr->name);
        auto& sl_name = (sl_opt && sl_opt->shares_name) ? *sl_opt->shares_name : default_service_level_name;
        co_return get_scheduling_group(sl_name);
    }
    else {
        co_return get_default_scheduling_group();
    }
}

future<std::vector<cql3::description>> service_level_controller::describe_attached_service_levels() {
    const auto attached_service_levels = co_await _auth_service.local().underlying_role_manager().query_attribute_for_all("service_level");

    std::vector<cql3::description> result{};
    result.reserve(attached_service_levels.size());

    for (const auto& [role, service_level] : attached_service_levels) {
        const auto formatted_role = cql3::util::maybe_quote(role);
        const auto formatted_sl = cql3::util::maybe_quote(service_level);

        sstring create_statement = seastar::format("ATTACH SERVICE LEVEL {} TO {};", formatted_sl, formatted_role);

        result.push_back(cql3::description {
            // Attaching a service level doesn't belong to any keyspace.
            .keyspace = std::nullopt,
            .type = "service_level_attachment",
            .name = service_level,
            .create_statement = managed_string(create_statement)
        });

        co_await coroutine::maybe_yield();
    }

    std::ranges::sort(result, std::less<>{}, [] (const cql3::description& desc) {
        return std::make_tuple(std::ref(desc.name), std::ref(*desc.create_statement));
    });

    co_return result;
}

future<> service_level_controller::update_effective_service_levels_cache() {
    SCYLLA_ASSERT(this_shard_id() == global_controller);
    
    if (!_auth_service.local_is_initialized()) {
        // Auth service might be not initialized yet.
        co_return;
    }
    if (!_sl_data_accessor || !_sl_data_accessor->can_use_effective_service_level_cache()) {
        // Don't populate the effective service level cache until auth is migrated to raft.
        // Otherwise, executing the code that follows would read roles data
        // from system_auth tables; that would be bad because reading from
        // those tables is prone to timeouts, and `update_effective_service_levels_cache`
        // is called from the group0 context - a timeout like that would render
        // group0 non-functional on the node until restart.
        //
        // See scylladb/scylladb#24963 for more details.
        co_return;
    }
    auto units = co_await get_units(_global_controller_db->notifications_serializer, 1);

    auto& role_manager = _auth_service.local().underlying_role_manager();
    const auto all_roles = co_await role_manager.query_all();
    const auto hierarchy = co_await role_manager.query_all_directly_granted();
    // includes only roles with attached service level
    const auto attributes = co_await role_manager.query_attribute_for_all("service_level");

    std::map<sstring, service_level_options> effective_sl_map;

    auto sorted = co_await utils::topological_sort(all_roles, hierarchy);
    // Roles are sorted from the top of the hierarchy to the bottom. 
    /// `GRANT role1 TO role2` means role2 is higher in the hierarchy than role1, so role2 will be before
    // role1 in `sorted` vector.
    // That's why if we iterate over the vector in reversed order, we will visit the roles from the bottom
    // and we can use already calculated effective service levels for all of the subroles.
    for (auto& role: sorted | std::views::reverse) {
        std::optional<service_level_options> sl_options;

        if (auto sl_name_it = attributes.find(role); sl_name_it != attributes.end()) {
            if (auto sl_it = _service_levels_db.find(sl_name_it->second); sl_it != _service_levels_db.end()) { 
                sl_options = sl_it->second.slo;
                sl_options->init_effective_names(sl_name_it->second);
                sl_options->shares_name = sl_name_it->second;
            } else if (_effectively_dropped_sls.contains(sl_name_it->second)) {
                // service level might be effective dropped, then it's not present in `_service_levels_db`
                sl_logger.warn("Service level {} is effectively dropped and its values are ignored.", sl_name_it->second);
            } else {
                sl_logger.error("Couldn't find service level {} in first level cache", sl_name_it->second);
            }
        }

        auto [it, it_end] = hierarchy.equal_range(role);
        while (it != it_end) {
            auto& subrole = it->second;
            if (auto sub_sl_it = effective_sl_map.find(subrole); sub_sl_it != effective_sl_map.end()) {
                if (sl_options) {
                    sl_options = sl_options->merge_with(sub_sl_it->second);
                } else {
                    sl_options = sub_sl_it->second;
                }
            }

            ++it;
        }

        if (sl_options) {
            effective_sl_map.insert({role, *sl_options});
        }
        co_await coroutine::maybe_yield();
    }

    co_await container().invoke_on_all([effective_sl_map] (service_level_controller& sl_controller) -> future<> {
        sl_controller._effective_service_levels_db = std::move(effective_sl_map);
        co_await sl_controller.notify_effective_service_levels_cache_reloaded();
    });
}

#endif

void effective_service_level_controller::register_subscriber(effective_service_level_event_subscriber* subscriber) {
    _subscribers.add(subscriber);
}

future<> effective_service_level_controller::unregister_subscriber(effective_service_level_event_subscriber* subscriber) {
    const auto _ = seastar::gate::holder{_stop_gate};
    return _subscribers.remove(subscriber);
}

void effective_service_level_controller::clear_cache() {
    _mapping_cache.clear();
}

future<> effective_service_level_controller::notify_cache_reloaded() {
    const auto _ = seastar::gate::holder{_stop_gate};

    co_await _subscribers.for_each([] (effective_service_level_event_subscriber* subscriber) -> future<> {
        return subscriber->on_effective_service_levels_cache_reloaded();
    });
}

} // namespace qos
