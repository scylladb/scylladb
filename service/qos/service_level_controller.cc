/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "seastar/core/timer.hh"
#include "service_level_controller.hh"
#include "db/system_distributed_keyspace.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"

namespace qos {
static logging::logger sl_logger("service_level_controller");

sstring service_level_controller::default_service_level_name = "default";



service_level_controller::service_level_controller(sharded<auth::service>& auth_service, service_level_options default_service_level_config):
        _sl_data_accessor(nullptr),
        _auth_service(auth_service),
        _last_successful_config_update(seastar::lowres_clock::now()),
        _logged_intervals(0)

{
    if (this_shard_id() == global_controller) {
        _global_controller_db = std::make_unique<global_controller_data>();
        _global_controller_db->default_service_level_config = default_service_level_config;
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

future<> service_level_controller::drain() {
    if (this_shard_id() != global_controller) {
        co_return;
    }
    // abort the loop of the distributed data checking if it is running
    if (!_global_controller_db->dist_data_update_aborter.abort_requested()) {
        _global_controller_db->dist_data_update_aborter.request_abort();
    }
    _global_controller_db->notifications_serializer.broken();
    try {
        co_await std::exchange(_global_controller_db->distributed_data_update, make_ready_future<>());
    } catch (const broken_semaphore& ignored) {
    } catch (const sleep_aborted& ignored) {
    } catch (const exceptions::unavailable_exception& ignored) {
    } catch (const exceptions::read_timeout_exception& ignored) {
    }
}

future<> service_level_controller::stop() {
    return drain();
}

future<> service_level_controller::update_service_levels_from_distributed_data() {

    if (!_sl_data_accessor) {
        return make_ready_future();
    }

    if (this_shard_id() != global_controller) {
        return make_ready_future();
    }

    return with_semaphore(_global_controller_db->notifications_serializer, 1, [this] () {
        return async([this] () {
            service_levels_info service_levels;
            // The next statement can throw, but that's fine since we would like the caller
            // to be able to agreggate those failures and only report when it is critical or noteworthy.
            // one common reason for failure is because one of the nodes comes down and before this node
            // detects it the scan query done inside this call is failing.
            service_levels = _sl_data_accessor->get_service_levels().get();

            service_levels_info service_levels_for_add_or_update;
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
                if (current_it->first == new_state_it->first) {
                    //the service level exists on both the cureent and new state.
                    if (current_it->second.slo != new_state_it->second) {
                        // The service level configuration is different
                        // in the new state and the old state, meaning it needs to be updated.
                        service_levels_for_add_or_update.insert(*new_state_it);
                    }
                    current_it++;
                    new_state_it++;
                } else if (current_it->first < new_state_it->first) {
                    //The service level does not exists in the new state so it needs to be
                    //removed, but only if it is not static since static configurations dont
                    //come from the distributed keyspace but from code.
                    if (!current_it->second.is_static) {
                        sl_logger.info("service level \"{}\" was deleted.", current_it->first.c_str());
                        service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                    }
                    current_it++;
                } else { /*new_it->first < current_it->first */
                    // The service level exits in the new state but not in the old state
                    // so it needs to be added.
                    sl_logger.info("service level \"{}\" was added.", new_state_it->first.c_str());
                    service_levels_for_add_or_update.insert(*new_state_it);
                    new_state_it++;
                }
            }

            for (; current_it != _service_levels_db.end(); current_it++) {
                if (!current_it->second.is_static) {
                    sl_logger.info("service level \"{}\" was deleted.", current_it->first.c_str());
                    service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                }
            }
            for (; new_state_it != service_levels.end(); new_state_it++) {
                sl_logger.info("service level \"{}\" was added.", new_state_it->first.c_str());
                service_levels_for_add_or_update.emplace(new_state_it->first, new_state_it->second);
            }

            for (auto&& sl : service_levels_for_delete) {
                do_remove_service_level(sl.first, false).get();
            }
            for (auto&& sl : service_levels_for_add_or_update) {
                do_add_service_level(sl.first, sl.second).get();
            }
        });
    });
}

future<std::optional<service_level_options>> service_level_controller::find_service_level(auth::role_set roles, include_effective_names include_names) {
    auto& role_manager = _auth_service.local().underlying_role_manager();

    // converts a list of roles into the chosen service level.
    return ::map_reduce(roles.begin(), roles.end(), [&role_manager, include_names, this] (const sstring& role) {
        return role_manager.get_attribute(role, "service_level").then_wrapped([include_names, this, role] (future<std::optional<sstring>> sl_name_fut) -> std::optional<service_level_options> {
            try {
                std::optional<sstring> sl_name = sl_name_fut.get();
                if (!sl_name) {
                    return std::nullopt;
                }
                auto sl_it = _service_levels_db.find(*sl_name);
                if ( sl_it == _service_levels_db.end()) {
                    return std::nullopt;
                }

                if (include_names == include_effective_names::yes) {
                    sl_it->second.slo.init_effective_names(*sl_name);
                }
                return sl_it->second.slo;
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

future<>  service_level_controller::notify_service_level_added(sstring name, service_level sl_data) {
    return seastar::async( [this, name, sl_data] {
        _subscribers.thread_for_each([name, sl_data] (qos_configuration_change_subscriber* subscriber) {
            try {
                subscriber->on_before_service_level_add(sl_data.slo, {name}).get();
            } catch (...) {
                sl_logger.error("notify_service_level_added: exception occurred in one of the observers callbacks {}", std::current_exception());
            }
        });
        _service_levels_db.emplace(name, sl_data);
    });

}

future<> service_level_controller::notify_service_level_updated(sstring name, service_level_options slo) {
    auto sl_it = _service_levels_db.find(name);
    future<> f = make_ready_future();
    if (sl_it != _service_levels_db.end()) {
        service_level_options slo_before = sl_it->second.slo;
        return seastar::async( [this,sl_it, name, slo_before, slo] {
            _subscribers.thread_for_each([name, slo_before, slo] (qos_configuration_change_subscriber* subscriber) {
                try {
                    subscriber->on_before_service_level_change(slo_before, slo, {name}).get();
                } catch (...) {
                    sl_logger.error("notify_service_level_updated: exception occurred in one of the observers callbacks {}", std::current_exception());
                }
            });
            sl_it->second.slo = slo;
        });
    }
    return f;
}

future<> service_level_controller::notify_service_level_removed(sstring name) {
    auto sl_it = _service_levels_db.find(name);
    if (sl_it != _service_levels_db.end()) {
        _service_levels_db.erase(sl_it);
        return seastar::async( [this, name] {
            _subscribers.thread_for_each([name] (qos_configuration_change_subscriber* subscriber) {
                try {
                    subscriber->on_after_service_level_remove({name}).get();
                } catch (...) {
                    sl_logger.error("notify_service_level_removed: exception occurred in one of the observers callbacks {}", std::current_exception());
                }
            });
        });
    }
    return make_ready_future<>();
}

void service_level_controller::update_from_distributed_data(std::function<steady_clock_type::duration()> interval_f) {
    if (this_shard_id() != global_controller) {
        throw std::runtime_error(format("Service level updates from distributed data can only be activated on shard {}", global_controller));
    }
    if (_global_controller_db->distributed_data_update.available()) {
        sl_logger.info("update_from_distributed_data: starting configuration polling loop");
        _logged_intervals = 0;
        _global_controller_db->distributed_data_update = repeat([this, interval_f = std::move(interval_f)] {
            return sleep_abortable<steady_clock_type>(interval_f(),
                    _global_controller_db->dist_data_update_aborter).then_wrapped([this] (future<>&& f) {
                try {
                    f.get();
                    return update_service_levels_from_distributed_data().then_wrapped([this] (future<>&& f){
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
                                sl_logger.log(ll, "update_from_distributed_data: failed to update configuration for more than  {} seconds : {}",
                                        (age_resolution*configuration_age).count(), std::current_exception());
                                _logged_intervals++;
                            }
                        }
                        return stop_iteration::no;
                    });
                } catch (const sleep_aborted& e) {
                    sl_logger.info("update_from_distributed_data: configuration polling loop aborted");
                    return make_ready_future<seastar::bool_class<seastar::stop_iteration_tag>>(stop_iteration::yes);
                }
            });
        }).then_wrapped([] (future<>&& f) {
            try {
                f.get();
            } catch (...) {
                sl_logger.error("update_from_distributed_data: polling loop stopped unexpectedly by: {}",
                        std::current_exception());
            }
        });
    }
}

future<> service_level_controller::add_distributed_service_level(sstring name, service_level_options slo, bool if_not_exists) {
    set_service_level_op_type add_type = if_not_exists ? set_service_level_op_type::add_if_not_exists : set_service_level_op_type::add;
    return set_distributed_service_level(name, slo, add_type);
}

future<> service_level_controller::alter_distributed_service_level(sstring name, service_level_options slo) {
    return set_distributed_service_level(name, slo, set_service_level_op_type::alter);
}

future<> service_level_controller::drop_distributed_service_level(sstring name, bool if_exists) {
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
    co_await coroutine::parallel_for_each(attributes.begin(), attributes.end(), [&role_manager, name] (auto&& attr) {
        if (attr.second == name) {
            return do_with(attr.first, [&role_manager] (const sstring& role_name) {
                return role_manager.remove_attribute(role_name, "service_level");
            });
        } else {
            return make_ready_future();
        }
    });

    co_return co_await _sl_data_accessor->drop_service_level(name);
}

future<service_levels_info> service_level_controller::get_distributed_service_levels() {
    return _sl_data_accessor ? _sl_data_accessor->get_service_levels() : make_ready_future<service_levels_info>();
}

future<service_levels_info> service_level_controller::get_distributed_service_level(sstring service_level_name) {
    return _sl_data_accessor ? _sl_data_accessor->get_service_level(service_level_name) : make_ready_future<service_levels_info>();
}

future<> service_level_controller::set_distributed_service_level(sstring name, service_level_options slo, set_service_level_op_type op_type) {
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
    co_return co_await _sl_data_accessor->set_service_level(name, slo);
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
        return do_with(service_level{.slo = slo, .is_static = is_static}, std::move(name), [this] (service_level& sl, sstring& name) {
            return container().invoke_on_all(&service_level_controller::notify_service_level_added, name, sl);
        });
    }
    return make_ready_future();
}

bool service_level_controller::is_v2() const {
    //FIXME: return actual state (fixed with migration commit)
    return false;
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

void service_level_controller::on_leave_cluster(const gms::inet_address& endpoint) {
    auto my_address = _auth_service.local().query_processor().proxy().local_db().get_token_metadata().get_topology().my_address();
    if (this_shard_id() == global_controller && endpoint == my_address) {
        _global_controller_db->dist_data_update_aborter.request_abort();
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

}
