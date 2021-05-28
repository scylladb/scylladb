/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <algorithm>
#include <chrono>

#include <cassert>

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>
#include <seastar/core/with_scheduling_group.hh>
#include "sync_point_service.hh"
#include "manager.hh"
#include "messages.hh"
#include "message/messaging_service.hh"
#include "utils/UUID_gen.hh"

namespace db {
namespace hints {

static constexpr size_t max_coordinator_sync_points = 32;
static constexpr size_t max_follower_sync_points = 128;

static constexpr auto completed_coordinator_task_ttl = std::chrono::minutes(5);
static constexpr auto follower_task_ttl = std::chrono::seconds(10);
static constexpr auto follower_status_check_interval = std::chrono::seconds(1);

static logging::logger service_logger("hints_sync_point_service");

sync_point_service::sync_point_service(netw::messaging_service& ms, seastar::scheduling_group sg)
        : _messaging(ms)
        , _task_scheduling_group(sg) {

    _managers.reserve(2);
}

sync_point_service::~sync_point_service() {
    assert(!_running);
    assert(_coordinator_tasks.empty());
    assert(_follower_tasks.empty());
}

void sync_point_service::register_manager(manager& mgr) {
    auto it = std::find_if(_managers.begin(), _managers.end(), [&mgr] (const manager& other) {
        return &mgr == &other;
    });
    if (it == _managers.end()) {
        service_logger.debug("Registering hints manager: {}", (void*)&mgr);
        _managers.push_back(mgr);
    }
}

void sync_point_service::unregister_manager(manager& mgr) {
    auto it = std::find_if(_managers.begin(), _managers.end(), [&mgr] (const manager& other) {
        return &mgr == &other;
    });
    if (it != _managers.end()) {
        service_logger.debug("Unregistering hints manager: {}", (void*)&mgr);
        _managers.erase(it);
    }
}

future<> sync_point_service::start() {
    register_rpc_verbs();
    _running = true;
    return make_ready_future<>();
}

future<> sync_point_service::stop() {
    _running = false;
    auto abort_tasks_of_one_kind = [] (task_map& tasks) {
        // Because tasks can delete themselves from the map when
        // `cancel_and_finish` is called, we must copy sync point IDs to a vector
        std::vector<sync_point_id> ids;
        ids.reserve(tasks.size());
        for (auto& p : tasks) {
            ids.push_back(p.first);
        }

        return parallel_for_each(ids, [&tasks] (sync_point_id id) {
            return tasks.at(id).cancel_and_finish();
        });
    };

    return when_all_succeed(
        abort_tasks_of_one_kind(_coordinator_tasks),
        abort_tasks_of_one_kind(_follower_tasks),
        unregister_rpc_verbs()
    ).discard_result();
}

future<sync_point_id> sync_point_service::create_sync_point(
        std::vector<gms::inet_address> source_nodes,
        std::vector<gms::inet_address> target_nodes) {

    return container().invoke_on(0, [source_nodes = std::move(source_nodes), target_nodes = std::move(target_nodes)] (sync_point_service& sps) {
        sps.check_if_is_running();

        if (sps._coordinator_tasks.size() >= max_coordinator_sync_points) {
            throw std::runtime_error(format("Too many coordinator sync points; the limit is {}", max_coordinator_sync_points));
        }
        auto id = utils::UUID_gen::get_time_UUID();
        // There should be no collisions with existing waiting points
        // because get_time_UUID is supposed to generate unique IDs

        auto [it, emplaced] = sps._coordinator_tasks.try_emplace(id, sps._coordinator_tasks, "coordinator", id);
        assert(emplaced);
        auto& task = it->second;
        task.run([&sps, &task, id, &source_nodes, &target_nodes] (abort_source& as) mutable {
            return with_scheduling_group(sps._task_scheduling_group, [&sps, &task, &as, id, source_nodes = std::move(source_nodes), target_nodes = std::move(target_nodes)] () mutable {
                return sps.coordinate_hint_waiting(as, id, std::move(source_nodes), std::move(target_nodes)).finally([&task] {
                    task.schedule_expiry(completed_coordinator_task_ttl);
                });
            });
        });

        return make_ready_future<sync_point_id>(id);
    });
}

future<> sync_point_service::wait_for_sync_point(sync_point_id id, clock_type::time_point timeout) {
    return container().invoke_on(0, [id, timeout] (sync_point_service& sps) {
        sps.check_if_is_running();

        auto it = sps._coordinator_tasks.find(id);
        if (it == sps._coordinator_tasks.end()) {
            throw std::runtime_error(format("Coordinator hint sync point with id {} does not exist", id));
        }
        return it->second.wait_for_result(timeout);
    });
}

future<> sync_point_service::delete_sync_point(sync_point_id id) {
    return container().invoke_on(0, [id] (sync_point_service& sps) {
        sps.check_if_is_running();

        auto it = sps._coordinator_tasks.find(id);
        if (it == sps._coordinator_tasks.end()) {
            throw std::runtime_error(format("Coordinator hint sync point with id {} does not exist", id));
        }
        return it->second.cancel_and_finish();
    });
}

std::vector<sync_point_id> sync_point_service::list_sync_points() const {
    std::vector<sync_point_id> ids;
    ids.reserve(_coordinator_tasks.size());
    for (auto& p : _coordinator_tasks) {
        ids.push_back(p.first);
    }
    return ids;
}

void sync_point_service::register_rpc_verbs() {
    auto& ms = _messaging;

    ms.register_hint_sync_point_create([this] (sync_point_create_request request) -> future<sync_point_create_response> {
        return container().invoke_on(0, [request = std::move(request)] (sync_point_service& sps) mutable {
            sps.check_if_is_running();

            const auto sync_point_id = request.sync_point_id;
            auto target_endpoints = std::move(request.target_endpoints);

            if (sps._follower_tasks.size() >= max_follower_sync_points) {
                throw std::runtime_error(format("Too many follower sync points; the limit is {}", max_coordinator_sync_points));
            }
            if (sps._follower_tasks.contains(sync_point_id)) {
                throw std::runtime_error(format("Follower hint sync point with id {} already exists", sync_point_id));
            }

            auto [it, emplaced] = sps._follower_tasks.try_emplace(sync_point_id, sps._follower_tasks, "follower", sync_point_id);
            assert(emplaced);
            auto& task = it->second;
            task.run([&sps, sync_point_id, &target_endpoints] (abort_source& as) mutable {
                return with_scheduling_group(sps._task_scheduling_group, [&sps, &as, sync_point_id, target_endpoints = std::move(target_endpoints)] () mutable {
                    return sps.wait_for_local_hints(as, sync_point_id, target_endpoints);
                });
            });
            task.schedule_expiry(follower_task_ttl);

            return sync_point_create_response{};
        });
    });
    ms.register_hint_sync_point_check([this] (sync_point_check_request request) -> future<sync_point_check_response> {
        return container().invoke_on(0, [request = std::move(request)] (sync_point_service& sps) {
            sps.check_if_is_running();

            const auto id = request.sync_point_id;

            auto it = sps._follower_tasks.find(id);
            if (it == sps._follower_tasks.end()) {
                throw std::runtime_error(format("Follower hint sync point with id {} does not exist", id));
            }

            // Refresh the TTL on the task
            it->second.schedule_expiry(follower_task_ttl);

            sync_point_check_response ret;
            ret.expired = it->second.check_result();
            return ret;
        });
    });
}

future<> sync_point_service::unregister_rpc_verbs() {
    auto& ms = _messaging;

    return when_all(
        ms.unregister_hint_sync_point_create(),
        ms.unregister_hint_sync_point_check()
    ).discard_result();
}

future<> sync_point_service::coordinate_hint_waiting(
        abort_source& as,
        sync_point_id id,
        std::vector<gms::inet_address> source_nodes,
        std::vector<gms::inet_address> target_nodes) {
    
    assert(this_shard_id() == 0);

    // We want to abort waiting for all follower sync points either if waiting
    // for one of the follower failed, or the operation was aborted.
    // This abort_source will be triggered if either of those situations happen.
    abort_source combined_as;
    auto trigger_on_operation_abort_sub = as.subscribe([&combined_as, id] () noexcept {
        service_logger.debug("Task ({} | coordinator): requested abort from above", id);
        if (!combined_as.abort_requested()) {
            combined_as.request_abort();
        }
    });

    co_await parallel_for_each(source_nodes, [&ms_ = _messaging, &combined_as_ = combined_as, id_ = id, &target_nodes_ = target_nodes] (gms::inet_address follower) -> future<> {
        auto& ms = ms_;
        auto& combined_as = combined_as_;
        const auto id = id_;
        auto& target_nodes = target_nodes_;

        service_logger.debug("Task ({} | coordinator): waiting for hints to be replayed towards {}", id, follower);

        try {
            // Create a hint sync point
            sync_point_create_request request;
            request.sync_point_id = id;
            request.target_endpoints = target_nodes;
            co_await ms.send_hint_sync_point_create(netw::msg_addr{ follower, 0 }, clock_type::time_point::max(), std::move(request));

            // Wait in a loop until the sync point resolves
            while (true) {
                sync_point_check_request request;
                request.sync_point_id = id;
                auto resp = co_await ms.send_hint_sync_point_check(netw::msg_addr{ follower, 0 }, clock_type::time_point::max(), std::move(request));

                if (resp.expired) {
                    // Hints were successfully replayed - return
                    break;
                }

                // Hints are still being replayed on the follower
                // Wait a little before checking again
                // If abort_source was engaged, this will throw - that's what we want
                co_await sleep_abortable(follower_status_check_interval, combined_as);
            }
        } catch (...) {
            // TODO: Print error?
            service_logger.debug("Task ({} | coordinator): got an exception: {}", id, std::current_exception());
            if (!combined_as.abort_requested()) {
                combined_as.request_abort();
            }
            throw;
        }

        service_logger.debug("Task ({} | coordinator): the node {} has successfully replayed its hints", id, follower);
    });
}

future<> sync_point_service::wait_for_local_hints(
        abort_source& as,
        sync_point_id id,
        std::vector<gms::inet_address> target_nodes) {
    
    assert(this_shard_id() == 0);

    // If an exception occurs on one shard, we are no longer interested
    // in waiting for other shards, so we need a sharded abort source.
    // Gate is needed because abort_all_shards spawns discarded futures
    // which needs to be waited on.
    struct abort_source_and_gate {
        seastar::gate gate;
        abort_source as;
    };
    sharded<abort_source_and_gate> sharded_abort_source;
    co_await sharded_abort_source.start();

    auto abort_all_shards = [&sharded_abort_source, id] {
        auto& local = sharded_abort_source.local();
        if (!local.gate.try_enter()) {
            return;
        }

        // Waited on by the shard-local gate
        (void)sharded_abort_source.invoke_on(0, [&sharded_abort_source, id] (auto& local0) {
            // To prevent O(smp::count^2) futures from being generated,
            // first check if we already aborted on shard 0, and if not,
            // abort on all shards
            if (local0.as.abort_requested()) {
                service_logger.debug("Task ({} | follower): abort requested from above", id);
                return make_ready_future<>();
            }
            local0.as.request_abort();
            return sharded_abort_source.invoke_on_others([] (auto& local) {
                if (!local.as.abort_requested()) {
                    local.as.request_abort();
                }
            });
        }).finally([&local] {
            local.gate.leave();
        });
    };

    auto sub = as.subscribe([&abort_all_shards] () noexcept {
        abort_all_shards();
    });

    co_await container().invoke_on_all([&sharded_abort_source, &abort_all_shards, id, &target_nodes] (sync_point_service& sps) {
        sps.check_if_is_running();

        return parallel_for_each(sps._managers, [&sharded_abort_source, &target_nodes, id] (manager& mgr) {
            service_logger.debug("Task ({} | follower | shard {}): going to wait on shard for the following nodes: {}", this_shard_id(), id, target_nodes);
            return mgr.wait_until_current_hints_are_replayed(sharded_abort_source.local().as, target_nodes);
        }).handle_exception([&abort_all_shards, id] (auto ep) {
            service_logger.debug("Task ({} | follower | shard {}): got an exception: {}", id, this_shard_id(), ep);
            abort_all_shards();
            return make_exception_future<>(std::move(ep));
        }).then([id] {
            service_logger.debug("Task ({} | follower): finished replaying hints on shard {}", id, this_shard_id());
        });
    }).finally([&sub, &sharded_abort_source] () -> future<> {
        // Because we are destroying the per-shard abort_source,
        // we need to drop the subscription of top-level abort source
        sub = std::nullopt;
        return sharded_abort_source.invoke_on_all([] (auto& local) {
            return local.gate.close();
        }).then([&sharded_abort_source] {
            return sharded_abort_source.stop();
        });
    });
}

void sync_point_service::check_if_is_running() const {
    if (!_running) {
        // TODO: Dedicated error?
        throw std::runtime_error("hints sync point service is not running");
    }
}

sync_point_service::task::task(task_map& parent, sstring typ, sync_point_id id)
        : _id(id)
        , _typ(std::move(typ))
        , _parent_task_map(parent)
{
    _expiry_timer.set_callback([this] {
        cancel();
    });

    service_logger.debug("Task ({} {}) created", _id, _typ);
}

void sync_point_service::task::run(noncopyable_function<future<> (abort_source&)>&& f) {
    assert(!_result.valid());
    _result = f(_as);
}

bool sync_point_service::task::check_result() {
    if (!_result.available()) {
        return false;
    }
    if (!_result.failed()) {
        return true;
    }
    // Re-throw the exception from the shared future
    _result.get_future().get();
    assert(0 && "should not reach");
    return false;
}

future<> sync_point_service::task::wait_for_result(clock_type::time_point timeout) {
    return _result.get_future(timeout);
}

void sync_point_service::task::cancel() {
    if (_finish.valid()) {
        return;
    }
    service_logger.debug("Task ({} {}) canceled", _id, _typ);
    assert(!_as.abort_requested());
    _as.request_abort();
    _expiry_timer.cancel();

    // Ignore exceptions, just wait for the future to resolve
    // Wait until the task removes itself from the parent map
    _finish = _result.get_future().handle_exception([] (auto ep) {}).finally([this] {
        service_logger.debug("Task ({} {}): erasing", _id, _typ);
        _parent_task_map.erase(_id);
    });
}

future<> sync_point_service::task::cancel_and_finish() {
    cancel();
    return _finish.get_future();
}

void sync_point_service::task::schedule_expiry(clock_type::duration timeout) {
    if (_finish.valid()) {
        return;
    }
    service_logger.trace("Scheduling expiration of task ({} {}): {}s", _id, _typ,
            std::chrono::duration_cast<std::chrono::seconds>(timeout).count());
    _expiry_timer.rearm(clock_type::now() + timeout);
}

}
}
