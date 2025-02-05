/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/util.hh"
#include "utils/assert.hh"
#include <chrono>

#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/as_future.hh>
#include "cql3/untyped_result_set.hh"
#include "db/config.hh"
#include "db/consistency_level_type.hh"
#include "db/system_keyspace.hh"
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "service/qos/raft_service_level_distributed_data_accessor.hh"
#include "service/qos/standard_service_level_distributed_data_accessor.hh"
#include "service_level_controller.hh"
#include "db/system_distributed_keyspace.hh"
#include "cql3/query_processor.hh"
#include "service/storage_service.hh"
#include "service/topology_state_machine.hh"
#include "utils/sorting.hh"
#include <seastar/core/reactor.hh>

namespace qos {
static logging::logger sl_logger("service_level_controller");

sstring service_level_controller::default_service_level_name = "default";
constexpr const char* scheduling_group_name_pattern = "sl:{}";
constexpr const char* deleted_scheduling_group_name_pattern = "sl_deleted:{}";
constexpr const char* temp_scheduling_group_name_pattern = "sl_temp:{}";

service_level_controller::service_level_controller(sharded<auth::service>& auth_service, locator::shared_token_metadata& tm, abort_source& as, service_level_options default_service_level_config, scheduling_group default_scheduling_group, bool destroy_default_sg_on_drain)
        : _sl_data_accessor(nullptr)
        , _auth_service(auth_service)
        , _token_metadata(tm)
        , _last_successful_config_update(seastar::lowres_clock::now())
        , _logged_intervals(0)
        , _early_abort_subscription(as.subscribe([this] () noexcept { do_abort(); }))
{
    // We can't rename the system default scheduling group so we have to reject it.
    assert(default_scheduling_group != get_default_scheduling_group());
    if (this_shard_id() == global_controller) {
        _global_controller_db = std::make_unique<global_controller_data>();
        _global_controller_db->default_service_level_config = default_service_level_config;
        _global_controller_db->default_sg = default_scheduling_group;
        _global_controller_db->destroy_default_sg = destroy_default_sg_on_drain;
        // since the first thing that is being done is adding the default service level, we only
        // need to throw the given group to the pool of scheduling groups for reuse.
        _global_controller_db->deleted_scheduling_groups.emplace_back(default_scheduling_group);
    }
}

future<> service_level_controller::add_service_level(sstring name, service_level_options slo, bool is_static) {
    return container().invoke_on(global_controller, [=] (service_level_controller &sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller, name, slo, is_static] () {
           return sl_controller.do_add_service_level(name, slo, is_static);
        });
    });
}

future<>  service_level_controller::remove_service_level(sstring name, bool remove_static) {
    return container().invoke_on(global_controller, [=] (service_level_controller &sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller, name, remove_static] () {
           return sl_controller.do_remove_service_level(name, remove_static);
        });
    });
}

future<> service_level_controller::start() {
    if (this_shard_id() != global_controller) {
        return make_ready_future();
    }
    return with_semaphore(_global_controller_db->notifications_serializer, 1, [this] () {
        return do_add_service_level(default_service_level_name, _global_controller_db->default_service_level_config, true).then([this] () {
            return container().invoke_on_all([] (service_level_controller& sl) {
                sl._default_service_level =  sl.get_service_level(default_service_level_name);
            });
        });
    });
}


void service_level_controller::set_distributed_data_accessor(service_level_distributed_data_accessor_ptr sl_data_accessor) {
    // unregistering the accessor is always legal
    if (!sl_data_accessor) {
        _sl_data_accessor = nullptr;
    }

    // Registration of a new accessor can be done only when the _sl_data_accessor is not already set.
    // This behavior is intended to allow to unit testing debug to set this value without having
    // overridden by storage_proxy
    if (!_sl_data_accessor) {
        _sl_data_accessor = sl_data_accessor;
    }
}

future<> service_level_controller::reload_distributed_data_accessor(cql3::query_processor& qp, service::raft_group0_client& g0, db::system_keyspace& sys_ks, db::system_distributed_keyspace& sys_dist_ks) {
    auto accessor = co_await qos::get_service_level_distributed_data_accessor_for_current_version(
            sys_ks,
            sys_dist_ks,
            qp,
            g0);
    set_distributed_data_accessor(std::move(accessor));
}

void service_level_controller::do_abort() noexcept {
    if (this_shard_id() != global_controller) {
        return;
    }

    // abort the loop of the distributed data checking if it is running
    if (!_global_controller_db->dist_data_update_aborter.abort_requested()) {
        _global_controller_db->dist_data_update_aborter.request_abort();
    }

    abort_group0_operations();
}

future<> service_level_controller::stop() {
    if (this_shard_id() != global_controller) {
        co_return;
    }

    // If abort source didn't fire, do it now
    _early_abort_subscription->on_abort(std::nullopt);
    
    _global_controller_db->notifications_serializer.broken();
    try {
        auto f = co_await coroutine::as_future(std::exchange(_global_controller_db->distributed_data_update, make_ready_future<>()));
        // delete all sg's in _service_levels_db, leaving it empty.
        for (auto it = _service_levels_db.begin(); it != _service_levels_db.end(); ) {
            _global_controller_db->deleted_scheduling_groups.emplace_back(it->second.sg);
            it = _service_levels_db.erase(it);
        }
        f.get();
    } catch (const broken_semaphore& ignored) {
    } catch (const sleep_aborted& ignored) {
    } catch (const exceptions::unavailable_exception& ignored) {
    } catch (const exceptions::read_timeout_exception& ignored) {
    }

    // exclude scheduling groups we shouldn't destroy
    std::erase_if(_global_controller_db->deleted_scheduling_groups, [this] (scheduling_group& sg) {
        if (sg == default_scheduling_group()) {
            return true;
        } else if (!_global_controller_db->destroy_default_sg && _global_controller_db->default_sg == sg) {
            return true;
        } else {
            return false;
        }
    });

    // destroy all sg's in _global_controller_db->deleted_scheduling_groups, leaving it empty
    // if any destroy_scheduling_group call fails, return one of the exceptions
    std::deque<scheduling_group> deleted_scheduling_groups = std::move(_global_controller_db->deleted_scheduling_groups);
    std::exception_ptr ex;

    while (!deleted_scheduling_groups.empty()) {
         auto f = co_await coroutine::as_future(destroy_scheduling_group(deleted_scheduling_groups.front()));
         if (f.failed()) {
             auto e = f.get_exception();
             sl_logger.error("Destroying scheduling group \"{}\" on stop failed: {}.  Ignored.", deleted_scheduling_groups.front().name(), e);
             ex = std::move(e);
         }
         deleted_scheduling_groups.pop_front();
    }
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
}

void service_level_controller::abort_group0_operations() {
    // abort group0 operations
    if (!_global_controller_db->group0_aborter.abort_requested()) {
        _global_controller_db->group0_aborter.request_abort();
    }
}

future<> service_level_controller::update_service_levels_cache(qos::query_context ctx) {
    SCYLLA_ASSERT(this_shard_id() == global_controller);

    if (!_sl_data_accessor) {
        return make_ready_future();
    }

    return with_semaphore(_global_controller_db->notifications_serializer, 1, [this, ctx] () {
        return async([this, ctx] () {
            service_levels_info service_levels;
            // The next statement can throw, but that's fine since we would like the caller
            // to be able to agreggate those failures and only report when it is critical or noteworthy.
            // one common reason for failure is because one of the nodes comes down and before this node
            // detects it the scan query done inside this call is failing.
            service_levels = _sl_data_accessor->get_service_levels(ctx).get();

            service_levels_info service_levels_for_update;
            service_levels_info service_levels_for_add;
            service_levels_info service_levels_for_delete;

            auto current_it = _service_levels_db.begin();
            auto new_state_it = service_levels.begin();

            // we want to detect 3 kinds of objects in one pass -
            // 1. new service levels that have been added to the distributed keyspace
            // 2. existing service levels that have changed
            // 3. removed service levels
            // this loop is batching together add/update operation and remove operation
            // then they are all executed together.The reason for this is to allow for
            // firstly delete all that there is to be deleted and only then adding new
            // service levels.
            while (current_it != _service_levels_db.end() && new_state_it != service_levels.end()) {
                if (current_it->first.starts_with('$')) {
                    sl_logger.warn("Service level names starting with '$' are reserved for internal tenants. Rename service level \"{}\" to drop '$' prefix.", current_it->first.c_str());
                }

                if (current_it->first == new_state_it->first) {
                    //the service level exists on both the cureent and new state.
                    if (current_it->second.slo != new_state_it->second) {
                        // The service level configuration is different
                        // in the new state and the old state, meaning it needs to be updated.
                        service_levels_for_update.insert(*new_state_it);
                    }
                    current_it++;
                    new_state_it++;
                } else if (current_it->first < new_state_it->first) {
                    //The service level does not exists in the new state so it needs to be
                    //removed, but only if it is not static since static configurations dont
                    //come from the distributed keyspace but from code.
                    if (!current_it->second.is_static) {
                        service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                    }
                    current_it++;
                } else { /*new_it->first < current_it->first */
                    // The service level exits in the new state but not in the old state
                    // so it needs to be added.
                    service_levels_for_add.insert(*new_state_it);
                    new_state_it++;
                }
            }

            for (; current_it != _service_levels_db.end(); current_it++) {
                if (!current_it->second.is_static) {
                    service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                }
            }
            for (; new_state_it != service_levels.end(); new_state_it++) {
                                service_levels_for_add.emplace(new_state_it->first, new_state_it->second);
            }

            for (auto&& sl : service_levels_for_delete) {
                do_remove_service_level(sl.first, false).get();
                sl_logger.info("service level \"{}\" was deleted.", sl.first.c_str());
            }
            for (auto&& sl : service_levels_for_update) {
                do_add_service_level(sl.first, sl.second).get();
                sl_logger.info("service level \"{}\" was updated. New values: (timeout: {}, workload_type: {}, shares: {})",
                        sl.first, sl.second.timeout, sl.second.workload, sl.second.shares);
            }
            _effective_service_levels_db.clear();
            for (auto&& sl : service_levels_for_add) {
                bool make_room = false;
                std::map<sstring, service_level>::reverse_iterator it;
                try {
                    do_add_service_level(sl.first, sl.second).get();
                    sl_logger.info("service level \"{}\" was added.", sl.first.c_str());
                } catch (service_level_scheduling_groups_exhausted &ex) {
                    it = _service_levels_db.rbegin();
                    if (it->first == default_service_level_name) {
                        it++;
                    }
                    if (it->first.compare(sl.first) > 0) {
                        make_room = true;
                    } else {
                         _effectively_dropped_sls.insert(sl.first);
                         sl_logger.warn("{}", ex.what());
                    }
                }
                if (make_room) {
                    sl_logger.warn("service level \"{}\" will be effectively dropped to make scheduling group available to \"{}\", please consider removing a service level."
                            , it->first, sl.first );
                    do_remove_service_level(it->first, false).get();
                    _effectively_dropped_sls.insert(it->first);
                    do_add_service_level(sl.first, sl.second).get();
                }
            }

        });
    });
}

future<> service_level_controller::update_effective_service_levels_cache() {
    SCYLLA_ASSERT(this_shard_id() == global_controller);
    
    if (!_auth_service.local_is_initialized()) {
        // Because cache update is triggered in `topology_state_load()`, auth service
        // might be not initialized yet.
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

future<> service_level_controller::update_cache(update_both_cache_levels update_both_cache_levels, qos::query_context ctx) {
    SCYLLA_ASSERT(this_shard_id() == global_controller);
    if (update_both_cache_levels) {
        co_await update_service_levels_cache(ctx);
    }
    co_await update_effective_service_levels_cache();
}

void service_level_controller::stop_legacy_update_from_distributed_data() {
    SCYLLA_ASSERT(this_shard_id() == global_controller);

    if (_global_controller_db->dist_data_update_aborter.abort_requested()) {
        return;
    }
    _global_controller_db->dist_data_update_aborter.request_abort();
}

future<std::optional<service_level_options>> service_level_controller::find_effective_service_level(const sstring& role_name) {
    if (_sl_data_accessor->is_v2()) {
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

future<>  service_level_controller::notify_service_level_added(sstring name, service_level sl_data) {
    return seastar::async( [this, name, sl_data] {
        service_level_info sl_info = {
            .name = name,
            .sg = sl_data.sg,
        };
        _subscribers.thread_for_each([name, sl_data, sl_info] (qos_configuration_change_subscriber* subscriber) {
            try {
                subscriber->on_before_service_level_add(sl_data.slo, sl_info).get();
            } catch (...) {
                sl_logger.error("notify_service_level_added: exception occurred in one of the observers callbacks {}", std::current_exception());
            }
        });
        auto result= _service_levels_db.emplace(name, sl_data);
        if (result.second) {
            unsigned sl_idx = internal::scheduling_group_index(sl_data.sg);
            _sl_lookup[sl_idx].first = &(result.first->first);
            _sl_lookup[sl_idx].second = &(result.first->second);
        }
    });

}

future<> service_level_controller::notify_service_level_updated(sstring name, service_level_options slo) {
    auto sl_it = _service_levels_db.find(name);
    future<> f = make_ready_future();
    if (sl_it != _service_levels_db.end()) {
        service_level_options slo_before = sl_it->second.slo;
        return seastar::async( [this,sl_it, name, slo_before, slo] {
            future<> f = make_ready_future();
            service_level_info sl_info = {
                .name = name,
                .sg = sl_it->second.sg,
            };
            _subscribers.thread_for_each([name, slo_before, slo, sl_info] (qos_configuration_change_subscriber* subscriber) {
                try {
                    subscriber->on_before_service_level_change(slo_before, slo, sl_info).get();
                } catch (...) {
                    sl_logger.error("notify_service_level_updated: exception occurred in one of the observers callbacks {}", std::current_exception());
                }
            });
            if (sl_it->second.slo.shares != slo.shares) {
                int32_t new_shares = default_shares;
                if (auto new_shares_p = std::get_if<int32_t>(&slo.shares)) {
                    new_shares = *new_shares_p;
                }
                sl_it->second.sg.set_shares(new_shares);
            }

            sl_it->second.slo = slo;
        });
    }
    return f;
}

future<> service_level_controller::notify_service_level_removed(sstring name) {
    auto sl_it = _service_levels_db.find(name);
    if (sl_it != _service_levels_db.end()) {
        unsigned sl_idx = internal::scheduling_group_index(sl_it->second.sg);
        _sl_lookup[sl_idx].first = nullptr;
        _sl_lookup[sl_idx].second = nullptr;
        if (this_shard_id() == global_controller) {
            _global_controller_db->deleted_scheduling_groups.emplace_back(sl_it->second.sg);
            co_await rename_scheduling_group(sl_it->second.sg, seastar::format(deleted_scheduling_group_name_pattern, sl_it->first));
        }
        service_level_info sl_info = {
            .name = name,
            .sg = sl_it->second.sg,
        };
        _service_levels_db.erase(sl_it);
        co_return co_await seastar::async( [this, name, sl_info] {
            _subscribers.thread_for_each([name, sl_info] (qos_configuration_change_subscriber* subscriber) {
                try {
                    subscriber->on_after_service_level_remove(sl_info).get();
                } catch (...) {
                    sl_logger.error("notify_service_level_removed: exception occurred in one of the observers callbacks {}", std::current_exception());
                }
            });
        });
    }
    co_return;
}

scheduling_group service_level_controller::get_default_scheduling_group() {
    return _default_service_level.sg;
}

scheduling_group service_level_controller::get_scheduling_group(sstring service_level_name) {
    auto service_level_it = _service_levels_db.find(service_level_name);
    if (service_level_it != _service_levels_db.end()) {
        return service_level_it->second.sg;
    } else {
        return get_default_scheduling_group();
    }
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

std::optional<sstring> service_level_controller::get_active_service_level() {
    unsigned sched_idx = internal::scheduling_group_index(current_scheduling_group());
    if (_sl_lookup[sched_idx].first) {
        return sstring(*_sl_lookup[sched_idx].first);
    } else {
        return std::nullopt;
    }
}

future<> service_level_controller::notify_effective_service_levels_cache_reloaded() {
    co_await _subscribers.for_each([] (qos_configuration_change_subscriber* subscriber) -> future<> {
        return subscriber->on_effective_service_levels_cache_reloaded();
    });
}

void service_level_controller::maybe_start_legacy_update_from_distributed_data(std::function<steady_clock_type::duration()> interval_f, service::storage_service& storage_service, service::raft_group0_client& group0_client) {
    if (this_shard_id() != global_controller) {
        throw std::runtime_error(format("Service level updates from distributed data can only be activated on shard {}", global_controller));
    }
    if (storage_service.get_topology_upgrade_state() == service::topology::upgrade_state_type::done && !group0_client.in_recovery()) {
        // Falling into this branch means service levels were migrated to raft and legacy update loop is not needed.
        return;
    }

    if (_global_controller_db->distributed_data_update.available()) {
        sl_logger.info("start_legacy_update_from_distributed_data: starting configuration polling loop");
        _logged_intervals = 0;
        _global_controller_db->distributed_data_update = repeat([this, interval_f = std::move(interval_f), &storage_service] {
            return sleep_abortable<steady_clock_type>(interval_f(),
                    _global_controller_db->dist_data_update_aborter).then_wrapped([this, &storage_service] (future<>&& f) {
                try {
                    f.get();
                    
                    if (storage_service.get_topology_upgrade_state() == service::topology::upgrade_state_type::done) {
                        stop_legacy_update_from_distributed_data();
                        sl_logger.info("update_from_distributed_data: Update loop has been shutdown. Now service levels cache will be updated immediately while applying raft group0 log.");
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }

                    return update_service_levels_cache().then_wrapped([this] (future<>&& f){
                        try {
                            f.get();
                            _last_successful_config_update = seastar::lowres_clock::now();
                            _logged_intervals = 0;
                        } catch (...) {
                            using namespace std::literals::chrono_literals;
                            constexpr auto age_resolution = 90s;
                            constexpr unsigned error_threshold = 10; // Change the logging level to error after 10 age_resolution intervals.
                            unsigned configuration_age = (seastar::lowres_clock::now() - _last_successful_config_update) / age_resolution;
                            if (configuration_age > _logged_intervals) {
                                log_level ll = configuration_age >= error_threshold ? log_level::error : log_level::warn;
                                sl_logger.log(ll, "start_legacy_update_from_distributed_data: failed to update configuration for more than  {} seconds : {}",
                                        (age_resolution*configuration_age).count(), std::current_exception());
                                _logged_intervals++;
                            }
                        }
                        return stop_iteration::no;
                    });
                } catch (const sleep_aborted& e) {
                    sl_logger.info("start_legacy_update_from_distributed_data: configuration polling loop aborted");
                    return make_ready_future<seastar::bool_class<seastar::stop_iteration_tag>>(stop_iteration::yes);
                }
            });
        }).then_wrapped([] (future<>&& f) {
            try {
                f.get();
            } catch (...) {
                sl_logger.error("start_legacy_update_from_distributed_data: polling loop stopped unexpectedly by: {}",
                        std::current_exception());
            }
        });
    }
}

future<> service_level_controller::add_distributed_service_level(sstring name, service_level_options slo, bool if_not_exists, service::group0_batch& mc) {
    set_service_level_op_type add_type = if_not_exists ? set_service_level_op_type::add_if_not_exists : set_service_level_op_type::add;
    return set_distributed_service_level(name, slo, add_type, mc);
}

future<> service_level_controller::alter_distributed_service_level(sstring name, service_level_options slo, service::group0_batch& mc) {
    return set_distributed_service_level(name, slo, set_service_level_op_type::alter, mc);
}

future<> service_level_controller::drop_distributed_service_level(sstring name, bool if_exists, service::group0_batch& mc) {
    auto sl_info = co_await _sl_data_accessor->get_service_levels();
    auto it = sl_info.find(name);
    if (it == sl_info.end()) {
        if (if_exists) {
            co_return;
        } else {
            throw nonexistant_service_level_exception(name);
        }
    }
    
    auto& role_manager = _auth_service.local().underlying_role_manager();
    auto attributes = co_await role_manager.query_attribute_for_all("service_level");

    co_await coroutine::parallel_for_each(attributes, [&role_manager, name, &mc] (auto&& attr) {
        if (attr.second == name) {
            return do_with(attr.first, [&role_manager, &mc] (const sstring& role_name) {
                return role_manager.remove_attribute(role_name, "service_level", mc);
            });
        } else {
            return make_ready_future();
        }
    });

    co_return co_await _sl_data_accessor->drop_service_level(name, mc);
}

future<service_levels_info> service_level_controller::get_distributed_service_levels(qos::query_context ctx) {
    return _sl_data_accessor ? _sl_data_accessor->get_service_levels(ctx) : make_ready_future<service_levels_info>();
}

future<service_levels_info> service_level_controller::get_distributed_service_level(sstring service_level_name) {
    return _sl_data_accessor ? _sl_data_accessor->get_service_level(service_level_name) : make_ready_future<service_levels_info>();
}

future<bool> service_level_controller::validate_before_service_level_add() {
    assert(this_shard_id() == global_controller);
    if (_global_controller_db->deleted_scheduling_groups.size() > 0) {
        return make_ready_future<bool>(true);
    } else if (_global_controller_db->scheduling_groups_exhausted) {
        return make_ready_future<bool>(false);
    } else {
        return create_scheduling_group(seastar::format(temp_scheduling_group_name_pattern, _global_controller_db->unique_group_counter++), 1).then_wrapped([this] (future<scheduling_group> new_sg_f) {
            if (new_sg_f.failed()) {
                new_sg_f.ignore_ready_future();
                _global_controller_db->scheduling_groups_exhausted = true;
                return make_ready_future<bool>(false);
            }
            _global_controller_db->deleted_scheduling_groups.emplace_back(new_sg_f.get());
            return make_ready_future<bool>(true);
        });
    }
}

future<> service_level_controller::set_distributed_service_level(sstring name, service_level_options slo, set_service_level_op_type op_type, service::group0_batch& mc) {
    auto sl_info = co_await _sl_data_accessor->get_service_levels();
    auto it = sl_info.find(name);
    // test for illegal requests or requests that should terminate without any action
    if (it == sl_info.end()) {
        if (op_type == set_service_level_op_type::alter) {
            throw exceptions::invalid_request_exception(format("The service level '{}' doesn't exist.", name));
        }
    } else {
        if (op_type == set_service_level_op_type::add) {
            throw exceptions::invalid_request_exception(format("The service level '{}' already exists.", name));
        } else if (op_type == set_service_level_op_type::add_if_not_exists) {
            co_return;
        }
    }

    if (op_type != set_service_level_op_type::alter) {
        bool validation_result = co_await container().invoke_on(global_controller, &service_level_controller::validate_before_service_level_add);
        if (!validation_result&& !utils::get_local_injector().enter("allow_service_level_over_limit")) {
            throw exceptions::invalid_request_exception("Can't create service level - no more scheduling groups exist");
        }
    }
    co_return co_await _sl_data_accessor->set_service_level(name, slo, mc);
}

future<> service_level_controller::do_add_service_level(sstring name, service_level_options slo, bool is_static) {
    auto service_level_it = _service_levels_db.find(name);
    if (is_static) {
        _global_controller_db->static_configurations[name] = slo;
    }
    if (service_level_it != _service_levels_db.end()) {
        if ((is_static && service_level_it->second.is_static) || !is_static) {
           if ((service_level_it->second.is_static) && (!is_static)) {
               service_level_it->second.is_static = false;
           }
           return container().invoke_on_all(&service_level_controller::notify_service_level_updated, name, slo);
        } else {
            // this means we set static layer when the the service level
            // is running of the non static configuration. so we have nothing
            // else to do since we already saved the static configuration.
            return make_ready_future();
        }
    } else {
        return do_with(service_level(slo, is_static, default_scheduling_group()),
                std::move(name), [this] (service_level& sl, sstring& name) {
            return make_ready_future().then([this, &sl, &name] () mutable {
                int32_t share_count = default_shares;
                if (auto* maybe_shares = std::get_if<int32_t>(&sl.slo.shares)) {
                    share_count = *maybe_shares;
                }

                if (!_global_controller_db->deleted_scheduling_groups.empty()) {
                    auto&& it = std::find_if(_global_controller_db->deleted_scheduling_groups.begin()
                            ,   _global_controller_db->deleted_scheduling_groups.end()
                            , [sg_name_to_find = seastar::format(deleted_scheduling_group_name_pattern, name)] (const scheduling_group& sg) {
                                return (sg.name() == sg_name_to_find);
                            });
                    if (it != _global_controller_db->deleted_scheduling_groups.end()) {
                        sl.sg = *it;
                        _global_controller_db->deleted_scheduling_groups.erase(it);
                    } else {
                        sl.sg = _global_controller_db->deleted_scheduling_groups.front();
                        _global_controller_db->deleted_scheduling_groups.pop_front();
                    }
                    return container().invoke_on_all([&sl, share_count] (service_level_controller& service) {
                        scheduling_group non_const_sg = sl.sg;
                        return non_const_sg.set_shares((float)share_count);
                    }).then([&sl, &name] {
                        return rename_scheduling_group(sl.sg, seastar::format(scheduling_group_name_pattern, name));
                    });
                } else if (_global_controller_db->scheduling_groups_exhausted) {
                    return make_exception_future<>(service_level_scheduling_groups_exhausted(name));
                } else {
                   return create_scheduling_group(seastar::format(scheduling_group_name_pattern, name), share_count).then_wrapped([this, name, &sl] (future<scheduling_group> sg_fut) {
                       if (sg_fut.failed()) {
                           sg_fut.ignore_ready_future();
                           _global_controller_db->scheduling_groups_exhausted = true;
                           return make_exception_future<>(service_level_scheduling_groups_exhausted(name));
                       }
                       sl.sg = sg_fut.get();
                       return make_ready_future<>();
                   });
                }
            }).then([this, &sl, &name] () {
                return container().invoke_on_all(&service_level_controller::notify_service_level_added, name, sl);
            });
        });
    }
    return make_ready_future();
}

bool service_level_controller::is_v2() const {
    return _sl_data_accessor && _sl_data_accessor->is_v2();
}

void service_level_controller::upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client) {
    if (!_sl_data_accessor) {
        return;
    }

    auto v2_data_accessor = _sl_data_accessor->upgrade_to_v2(qp, group0_client);
    if (v2_data_accessor) {
        _sl_data_accessor = v2_data_accessor;
    }
}

future<> service_level_controller::migrate_to_v2(size_t nodes_count, db::system_keyspace& sys_ks, cql3::query_processor& qp, service::raft_group0_client& group0_client, abort_source& as) {
    //TODO:
    //Now we trust the administrator to not make changes to service levels during the migration.
    //Ideally, during the migration we should set migration data accessor(on all nodes, on all shards) that allows to read but forbids writes
    using namespace std::chrono_literals;

    auto schema = qp.db().find_schema(db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::SERVICE_LEVELS);

    const auto t = 5min;
    const timeout_config tc{t, t, t, t, t, t, t};
    service::client_state cs(::service::client_state::internal_tag{}, tc);
    service::query_state qs(cs, empty_service_permit());
    
    // `system_distributed` keyspace has RF=3 and we need to scan it with CL=ALL
    // To support migration on cluster with 1 or 2 nodes, set appropriate CL
    auto cl = db::consistency_level::ALL;
    if (nodes_count == 1) {
        cl = db::consistency_level::ONE;
    } else if (nodes_count == 2) {
        cl = db::consistency_level::TWO;
    }
    
    auto rows = co_await qp.execute_internal(
        format("SELECT * FROM {}.{}", db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::SERVICE_LEVELS),
        cl,
        qs,
        {},
        cql3::query_processor::cache_internal::no);
    if (rows->empty()) {
        co_return;
    }
    

    auto col_names = schema->all_columns() | std::views::transform([] (const auto& col) {return col.name_as_cql_string(); }) | std::ranges::to<std::vector<sstring>>();
    auto col_names_str = fmt::to_string(fmt::join(col_names, ", "));
    sstring val_binders_str = "?";
    for (size_t i = 1; i < col_names.size(); ++i) {
        val_binders_str += ", ?";
    }
    
    auto guard = co_await group0_client.start_operation(as);

    std::vector<mutation> migration_muts;
    for (const auto& row: *rows) {
        std::vector<data_value_or_unset> values;
        for (const auto& col: schema->all_columns()) {
            if (row.has(col.name_as_text())) {
                values.push_back(col.type->deserialize(row.get_blob(col.name_as_text())));
            } else {
                values.push_back(unset_value{});
            }
        }

        auto muts = co_await qp.get_mutations_internal(
            seastar::format("INSERT INTO {}.{} ({}) VALUES ({})",
                db::system_keyspace::NAME,
                db::system_keyspace::SERVICE_LEVELS_V2,
                col_names_str,
                val_binders_str), 
            qos_query_state(),
            guard.write_timestamp(),
            std::move(values));
        if (muts.size() != 1) {
            on_internal_error(sl_logger, format("expecting single insert mutation, got {}", muts.size()));
        }
        migration_muts.push_back(std::move(muts[0]));
    }

    auto status_mut = co_await sys_ks.make_service_levels_version_mutation(2, guard.write_timestamp());
    migration_muts.push_back(std::move(status_mut));

    service::write_mutations change {
        .mutations{migration_muts.begin(), migration_muts.end()},
    };
    auto group0_cmd = group0_client.prepare_command(change, guard, "migrate service levels to v2");
    co_await group0_client.add_entry(std::move(group0_cmd), std::move(guard), as);
}

future<> service_level_controller::do_remove_service_level(sstring name, bool remove_static) {
    auto service_level_it = _service_levels_db.find(name);
    if (service_level_it != _service_levels_db.end()) {
        auto static_conf_it = _global_controller_db->static_configurations.end();
        bool static_exists = false;
        if (remove_static) {
            _global_controller_db->static_configurations.erase(name);
        } else {
            static_conf_it = _global_controller_db->static_configurations.find(name);
            static_exists = static_conf_it != _global_controller_db->static_configurations.end();
        }
        if (remove_static && service_level_it->second.is_static) {
            return container().invoke_on_all(&service_level_controller::notify_service_level_removed, name);
        } else if (!remove_static && !service_level_it->second.is_static) {
            if (static_exists) {
                service_level_it->second.is_static = true;
                return container().invoke_on_all(&service_level_controller::notify_service_level_updated, name, static_conf_it->second);
            } else {
                return container().invoke_on_all(&service_level_controller::notify_service_level_removed, name);
            }
        }
    }
    return make_ready_future();
}

void service_level_controller::on_join_cluster(const gms::inet_address& endpoint) { }

void service_level_controller::on_leave_cluster(const gms::inet_address& endpoint, const locator::host_id& hid) {
    if (this_shard_id() == global_controller && _token_metadata.get()->get_topology().is_me(hid)) {
        _global_controller_db->dist_data_update_aborter.request_abort();
        _global_controller_db->group0_aborter.request_abort();
    }
}

void service_level_controller::on_up(const gms::inet_address& endpoint) { }

void service_level_controller::on_down(const gms::inet_address& endpoint) { }

void service_level_controller::register_subscriber(qos_configuration_change_subscriber* subscriber) {
    _subscribers.add(subscriber);
}

future<> service_level_controller::unregister_subscriber(qos_configuration_change_subscriber* subscriber) {
    return _subscribers.remove(subscriber);
}

static sstring describe_service_level(std::string_view sl_name, const service_level_options& sl_opts) {
    using slo = service_level_options;

    utils::small_vector<sstring, 3> opts{};

    const sstring sl_name_formatted = cql3::util::maybe_quote(sl_name);

    if (auto maybe_timeout = std::get_if<lowres_clock::duration>(&sl_opts.timeout)) {
        // According to the documentation, `TIMEOUT` has to be expressed in milliseconds
        // or seconds. It is therefore safe to use milliseconds here.
        const auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(*maybe_timeout);
        opts.push_back(seastar::format("TIMEOUT = {}", timeout));
    }

    switch (sl_opts.workload) {
        case slo::workload_type::batch:
            opts.push_back("WORKLOAD_TYPE = 'batch'");
            break;
        case slo::workload_type::interactive:
            opts.push_back("WORKLOAD_TYPE = 'interactive'");
            break;
        case slo::workload_type::unspecified:
            break;
        case slo::workload_type::delete_marker:
            // `slo::workload_typ::delete_marker` is only set temporarily. When a service level
            // is actually created, it never has this workload type set anymore.
            on_internal_error(sl_logger, "Unexpected workload type");
    }

    if (auto* maybe_shares = std::get_if<int32_t>(&sl_opts.shares)) {
        opts.push_back(seastar::format("SHARES = {}", *maybe_shares));
    }

    if (opts.size() == 0) {
        return seastar::format("CREATE SERVICE LEVEL {};", sl_name_formatted);
    }

    return seastar::format("CREATE SERVICE LEVEL {} WITH {};", sl_name_formatted, fmt::join(opts, " AND "));
}


future<std::vector<cql3::description>> service_level_controller::describe_created_service_levels() const {

    std::vector<cql3::description> result{};

    // If we use Raft, we can rely on the cache and avoid a query.
    // The cache gets updated when applying Raft log, so it's always up-to-date
    // with the table.
    //
    // If Raft is not used, that means we're performing a rolling upgrade right now
    // or the migration to topology on Raft hasn't started yet. It's highly unlikely
    // anyone will try to make a backup in that situation, so we don't have a branch here.
    //
    // If Raft is not used yet, updating the cache will happen every 10 seconds. We deem it
    // good enough if someone does attempt to make a backup in that state.
    for (const auto& [sl_name, sl] : _service_levels_db) {
        if (sl.is_static) {
            continue;
        }

        result.push_back(cql3::description {
            // Service levels do not belong to any keyspace.
            .keyspace = std::nullopt,
            .type = "service_level",
            .name = sl_name,
            .create_statement = describe_service_level(sl_name, sl.slo)
        });

        co_await coroutine::maybe_yield();
    }

    std::ranges::sort(result, std::less<>{}, std::mem_fn(&cql3::description::name));

    co_return result;
}

future<std::vector<cql3::description>> service_level_controller::describe_attached_service_levels() {
    const auto attached_service_levels = co_await _auth_service.local().underlying_role_manager().query_attribute_for_all("service_level");

    std::vector<cql3::description> result{};
    result.reserve(attached_service_levels.size());

    for (const auto& [role, service_level] : attached_service_levels) {
        const auto formatted_role = cql3::util::maybe_quote(role);
        const auto formatted_sl = cql3::util::maybe_quote(service_level);

        result.push_back(cql3::description {
            // Attaching a service level doesn't belong to any keyspace.
            .keyspace = std::nullopt,
            .type = "service_level_attachment",
            .name = service_level,
            .create_statement = seastar::format("ATTACH SERVICE LEVEL {} TO {};", formatted_sl, formatted_role)
        });

        co_await coroutine::maybe_yield();
    }

    std::ranges::sort(result, std::less<>{}, [] (const cql3::description& desc) noexcept {
        return std::make_tuple(std::ref(desc.name), std::ref(*desc.create_statement));
    });


    co_return result;
}

future<std::vector<cql3::description>> service_level_controller::describe_service_levels() {
    std::vector<cql3::description> created_service_levels_descs = co_await describe_created_service_levels();
    std::vector<cql3::description> attached_service_levels_descs = co_await describe_attached_service_levels();

    created_service_levels_descs.insert(created_service_levels_descs.end(),
            std::make_move_iterator(attached_service_levels_descs.begin()), std::make_move_iterator(attached_service_levels_descs.end()));

    co_return created_service_levels_descs;
}

future<shared_ptr<service_level_controller::service_level_distributed_data_accessor>> 
get_service_level_distributed_data_accessor_for_current_version(
    db::system_keyspace& sys_ks,
    db::system_distributed_keyspace& sys_dist_ks,
    cql3::query_processor& qp, service::raft_group0_client& group0_client
) {
    auto sl_version = co_await sys_ks.get_service_levels_version();

    if (sl_version && *sl_version == 2) {
        co_return static_pointer_cast<qos::service_level_controller::service_level_distributed_data_accessor>(
            make_shared<qos::raft_service_level_distributed_data_accessor>(qp, group0_client));
    } else {
        co_return static_pointer_cast<qos::service_level_controller::service_level_distributed_data_accessor>(
            make_shared<qos::standard_service_level_distributed_data_accessor>(sys_dist_ks));
    }
}

} // namespace qos
