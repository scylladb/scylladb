/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/hints/manager.hh"

// Seastar features.
#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>

// Boost features.
#include <boost/range/adaptors.hpp>

// Scylla includes.
#include "db/hints/internal/hint_logger.hh"
#include "db/extensions.hh"
#include "db/timeout_clock.hh"
#include "gms/gossiper.hh"
#include "gms/versioned_value.hh"
#include "locator/abstract_replication_strategy.hh"
#include "mutation/mutation_partition_view.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "utils/directories.hh"
#include "utils/disk-error-handler.hh"
#include "utils/div_ceil.hh"
#include "utils/error_injection.hh"
#include "utils/lister.hh"
#include "utils/runtime.hh"
#include "converting_mutation_partition_applier.hh"
#include "seastarx.hh"
#include "service_permit.hh"

// STD.
#include <algorithm>

using namespace std::literals::chrono_literals;

namespace db::hints {

using namespace internal;

class directory_initializer::impl {
private:
    enum class state {
        uninitialized,
        created_and_validated,
        rebalanced
    };

private:
    utils::directories& _dirs;
    sstring _hints_directory;
    state _state = state::uninitialized;
    seastar::named_semaphore _lock = {1, named_semaphore_exception_factory{"hints directory initialization lock"}};

public:
    impl(utils::directories& dirs, sstring hints_directory)
        : _dirs(dirs)
        , _hints_directory(std::move(hints_directory))
    { }

public:
    future<> ensure_created_and_verified() {
        if (_state != state::uninitialized) {
            co_return;
        }

        const auto units = co_await seastar::get_units(_lock, 1);
        
        utils::directories::set dir_set;
        dir_set.add_sharded(_hints_directory);
        
        manager_logger.debug("Creating and validating hint directories: {}", _hints_directory);
        co_await _dirs.create_and_verify(std::move(dir_set));
        
        _state = state::created_and_validated;
    }

    future<> ensure_rebalanced() {
        if (_state == state::uninitialized) {
            throw std::logic_error("hints directory needs to be created and validated before rebalancing");
        }

        if (_state == state::rebalanced) {
            co_return;
        }

        const auto units = co_await seastar::get_units(_lock, 1);
        
        manager_logger.debug("Rebalancing hints in {}", _hints_directory);
        co_await rebalance_hints(fs::path{_hints_directory});

        _state = state::rebalanced;
    }
};

directory_initializer::directory_initializer(std::shared_ptr<directory_initializer::impl> impl)
        : _impl(std::move(impl))
{ }

future<directory_initializer> directory_initializer::make(utils::directories& dirs, sstring hints_directory) {
    return smp::submit_to(0, [&dirs, hints_directory = std::move(hints_directory)] () mutable {
        auto impl = std::make_shared<directory_initializer::impl>(dirs, std::move(hints_directory));
        return make_ready_future<directory_initializer>(directory_initializer(std::move(impl)));
    });
}

future<> directory_initializer::ensure_created_and_verified() {
    if (!_impl) {
        return make_ready_future<>();
    }
    return smp::submit_to(0, [impl = this->_impl] () mutable {
        return impl->ensure_created_and_verified().then([impl] {});
    });
}

future<> directory_initializer::ensure_rebalanced() {
    if (!_impl) {
        return make_ready_future<>();
    }
    return smp::submit_to(0, [impl = this->_impl] () mutable {
        return impl->ensure_rebalanced().then([impl] {});
    });
}

manager::manager(sstring hints_directory, host_filter filter, int64_t max_hint_window_ms, resource_manager& res_manager, distributed<replica::database>& db)
    : _hints_dir(fs::path(hints_directory) / format("{:d}", this_shard_id()))
    , _host_filter(std::move(filter))
    , _max_hint_window_us(max_hint_window_ms * 1000)
    , _local_db(db.local())
    , _resource_manager(res_manager)
{
    if (utils::get_local_injector().enter("decrease_hints_flush_period")) {
        hints_flush_period = std::chrono::seconds{1};
    }
}

manager::~manager() {
    assert(_ep_managers.empty());
}

void manager::register_metrics(const sstring& group_name) {
    namespace sm = seastar::metrics;

    _metrics.add_group(group_name, {
        sm::make_gauge("size_of_hints_in_progress", _stats.size_of_hints_in_progress,
                        sm::description("Size of hinted mutations that are scheduled to be written.")),

        sm::make_counter("written", _stats.written,
                        sm::description("Number of successfully written hints.")),

        sm::make_counter("errors", _stats.errors,
                        sm::description("Number of errors during hints writes.")),

        sm::make_counter("dropped", _stats.dropped,
                        sm::description("Number of dropped hints.")),

        sm::make_counter("sent", _stats.sent,
                        sm::description("Number of sent hints.")),

        sm::make_counter("discarded", _stats.discarded,
                        sm::description("Number of hints that were discarded during sending (too old, schema changed, etc.).")),

        sm::make_counter("send_errors", _stats.send_errors,
            sm::description("Number of unexpected errors during sending, sending will be retried later")),

        sm::make_counter("corrupted_files", _stats.corrupted_files,
                        sm::description("Number of hints files that were discarded during sending because the file was corrupted.")),

        sm::make_gauge("pending_drains", 
                        sm::description("Number of tasks waiting in the queue for draining hints"),
                        [this] { return _drain_lock.waiters(); }),

        sm::make_gauge("pending_sends",
                        sm::description("Number of tasks waiting in the queue for sending a hint"),
                        [this] { return _resource_manager.sending_queue_length(); })
    });
}

future<> manager::start(shared_ptr<service::storage_proxy> proxy_ptr, shared_ptr<gms::gossiper> gossiper_ptr) {
    _proxy_anchor = std::move(proxy_ptr);
    _gossiper_anchor = std::move(gossiper_ptr);
    return lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(), [this] (fs::path datadir, directory_entry de) {
        endpoint_id ep = endpoint_id(de.name);
        if (!check_dc_for(ep)) {
            return make_ready_future<>();
        }
        return get_ep_manager(ep).populate_segments_to_replay();
    }).then([this] {
        return compute_hints_dir_device_id();
    }).then([this] {
        set_started();
    });
}

future<> manager::stop() {
    manager_logger.info("Asked to stop");

  auto f = make_ready_future<>();

  return f.finally([this] {
    set_stopping();

    return _draining_eps_gate.close().finally([this] {
        return parallel_for_each(_ep_managers, [] (auto& pair) {
            return pair.second.stop();
        }).finally([this] {
            _ep_managers.clear();
            manager_logger.info("Stopped");
        }).discard_result();
    });
  });
}

future<> manager::compute_hints_dir_device_id() {
    return get_device_id(_hints_dir.native()).then([this](dev_t device_id) {
        _hints_dir_device_id = device_id;
    }).handle_exception([this](auto ep) {
        manager_logger.warn("Failed to stat directory {} for device id: {}", _hints_dir.native(), ep);
        return make_exception_future<>(ep);
    });
}

void manager::allow_hints() {
    boost::for_each(_ep_managers, [] (auto& pair) { pair.second.allow_hints(); });
}

void manager::forbid_hints() {
    boost::for_each(_ep_managers, [] (auto& pair) { pair.second.forbid_hints(); });
}

void manager::forbid_hints_for_eps_with_pending_hints() {
    manager_logger.trace("space_watchdog: Going to block hints to: {}", _eps_with_pending_hints);
    boost::for_each(_ep_managers, [this] (auto& pair) {
        hint_endpoint_manager& ep_man = pair.second;
        if (has_ep_with_pending_hints(ep_man.end_point_key())) {
            ep_man.forbid_hints();
        } else {
            ep_man.allow_hints();
        }
    });
}

sync_point::shard_rps manager::calculate_current_sync_point(const std::vector<endpoint_id>& target_eps) const {
    sync_point::shard_rps rps;
    for (auto addr : target_eps) {
        auto it = _ep_managers.find(addr);
        if (it != _ep_managers.end()) {
            const hint_endpoint_manager& ep_man = it->second;
            rps[ep_man.end_point_key()] = ep_man.last_written_replay_position();
        }
    }
    return rps;
}

future<> manager::wait_for_sync_point(abort_source& as, const sync_point::shard_rps& rps) {
    abort_source local_as;

    auto sub = as.subscribe([&local_as] () noexcept {
        if (!local_as.abort_requested()) {
            local_as.request_abort();
        }
    });

    if (as.abort_requested()) {
        local_as.request_abort();
    }

    bool was_aborted = false;
    co_await coroutine::parallel_for_each(_ep_managers, [&was_aborted, &rps, &local_as] (auto& p) {
        const auto addr = p.first;
        auto& ep_man = p.second;

        db::replay_position rp;
        auto it = rps.find(addr);
        if (it != rps.end()) {
            rp = it->second;
        }

        return ep_man.wait_until_hints_are_replayed_up_to(local_as, rp).handle_exception([&local_as, &was_aborted] (auto eptr) {
            if (!local_as.abort_requested()) {
                local_as.request_abort();
            }
            try {
                std::rethrow_exception(std::move(eptr));
            } catch (abort_requested_exception&) {
                was_aborted = true;
            } catch (...) {
                return make_exception_future<>(std::current_exception());
            }
            return make_ready_future();
        });
    });

    if (was_aborted) {
        throw abort_requested_exception();
    }

    co_return;
}

hint_endpoint_manager& manager::get_ep_manager(endpoint_id ep) {
    auto it = find_ep_manager(ep);
    if (it == ep_managers_end()) {
        manager_logger.trace("Creating an ep_manager for {}", ep);
        hint_endpoint_manager& ep_man = _ep_managers.emplace(ep, hint_endpoint_manager(ep, *this)).first->second;
        ep_man.start();
        return ep_man;
    }
    return it->second;
}

inline bool manager::have_ep_manager(endpoint_id ep) const noexcept {
    return find_ep_manager(ep) != ep_managers_end();
}

bool manager::store_hint(endpoint_id ep, schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept {
    if (stopping() || draining_all() || !started() || !can_hint_for(ep)) {
        manager_logger.trace("Can't store a hint to {}", ep);
        ++_stats.dropped;
        return false;
    }

    try {
        manager_logger.trace("Going to store a hint to {}", ep);
        tracing::trace(tr_state, "Going to store a hint to {}", ep);

        return get_ep_manager(ep).store_hint(std::move(s), std::move(fm), tr_state);
    } catch (...) {
        manager_logger.trace("Failed to store a hint to {}: {}", ep, std::current_exception());
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", ep, std::current_exception());

        ++_stats.errors;
        return false;
    }
}

bool manager::too_many_in_flight_hints_for(endpoint_id ep) const noexcept {
    // There is no need to check the DC here because if there is an in-flight hint for this end point then this means that
    // its DC has already been checked and found to be ok.
    return _stats.size_of_hints_in_progress > MAX_SIZE_OF_HINTS_IN_PROGRESS && !utils::fb_utilities::is_me(ep) && hints_in_progress_for(ep) > 0 && local_gossiper().get_endpoint_downtime(ep) <= _max_hint_window_us;
}

bool manager::can_hint_for(endpoint_id ep) const noexcept {
    if (utils::fb_utilities::is_me(ep)) {
        return false;
    }

    auto it = find_ep_manager(ep);
    if (it != ep_managers_end() && (it->second.stopping() || !it->second.can_hint())) {
        return false;
    }

    // Don't allow more than one in-flight (to the store) hint to a specific destination when the total size of in-flight
    // hints is more than the maximum allowed value.
    //
    // In the worst case there's going to be (_max_size_of_hints_in_progress + N - 1) in-flight hints, where N is the total number Nodes in the cluster.
    if (_stats.size_of_hints_in_progress > MAX_SIZE_OF_HINTS_IN_PROGRESS && hints_in_progress_for(ep) > 0) {
        manager_logger.trace("size_of_hints_in_progress {} hints_in_progress_for({}) {}", _stats.size_of_hints_in_progress, ep, hints_in_progress_for(ep));
        return false;
    }

    // check that the destination DC is "hintable"
    if (!check_dc_for(ep)) {
        manager_logger.trace("{}'s DC is not hintable", ep);
        return false;
    }

    // check if the end point has been down for too long
    if (local_gossiper().get_endpoint_downtime(ep) > _max_hint_window_us) {
        manager_logger.trace("{} is down for {}, not hinting", ep, local_gossiper().get_endpoint_downtime(ep));
        return false;
    }

    return true;
}

future<> manager::change_host_filter(host_filter filter) {
    if (!started()) {
        return make_exception_future<>(std::logic_error("change_host_filter: called before the hints_manager was started"));
    }

    return with_gate(_draining_eps_gate, [this, filter = std::move(filter)] () mutable {
        return with_semaphore(drain_lock(), 1, [this, filter = std::move(filter)] () mutable {
            if (draining_all()) {
                return make_exception_future<>(std::logic_error("change_host_filter: cannot change the configuration because hints all hints were drained"));
            }

            manager_logger.debug("change_host_filter: changing from {} to {}", _host_filter, filter);

            // Change the host_filter now and save the old one so that we can
            // roll back in case of failure
            std::swap(_host_filter, filter);

            // Iterate over existing hint directories and see if we can enable an endpoint manager
            // for some of them
            return lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(), [this] (fs::path datadir, directory_entry de) {
                const endpoint_id ep = endpoint_id(de.name);
                if (_ep_managers.contains(ep) || !_host_filter.can_hint_for(_proxy_anchor->get_token_metadata_ptr()->get_topology(), ep)) {
                    return make_ready_future<>();
                }
                return get_ep_manager(ep).populate_segments_to_replay();
            }).handle_exception([this, filter = std::move(filter)] (auto ep) mutable {
                // Bring back the old filter. The finally() block will cause us to stop
                // the additional hint_endpoint_managers that we started
                _host_filter = std::move(filter);
            }).finally([this] {
                // Remove endpoint managers which are rejected by the filter
                return parallel_for_each(_ep_managers, [this] (auto& pair) {
                    if (_host_filter.can_hint_for(_proxy_anchor->get_token_metadata_ptr()->get_topology(), pair.first)) {
                        return make_ready_future<>();
                    }
                    return pair.second.stop(drain::no).finally([this, ep = pair.first] {
                        _ep_managers.erase(ep);
                    });
                });
            });
        });
    });
}

bool manager::check_dc_for(endpoint_id ep) const noexcept {
    try {
        // If target's DC is not a "hintable" DCs - don't hint.
        // If there is an end point manager then DC has already been checked and found to be ok.
        return _host_filter.is_enabled_for_all() || have_ep_manager(ep) ||
               _host_filter.can_hint_for(_proxy_anchor->get_token_metadata_ptr()->get_topology(), ep);
    } catch (...) {
        // if we failed to check the DC - block this hint
        return false;
    }
}

void manager::drain_for(endpoint_id endpoint) {
    if (!started() || stopping() || draining_all()) {
        return;
    }

    manager_logger.trace("on_leave_cluster: {} is removed/decommissioned", endpoint);

    // Future is waited on indirectly in `stop()` (via `_draining_eps_gate`).
    (void)with_gate(_draining_eps_gate, [this, endpoint] {
        return with_semaphore(drain_lock(), 1, [this, endpoint] {
            return futurize_invoke([this, endpoint] () {
                if (utils::fb_utilities::is_me(endpoint)) {
                    set_draining_all();
                    return parallel_for_each(_ep_managers, [] (auto& pair) {
                        return pair.second.stop(drain::yes).finally([&pair] {
                            return with_file_update_mutex(pair.second, [&pair] {
                                return remove_file(pair.second.hints_dir().c_str());
                            });
                        });
                    }).finally([this] {
                        _ep_managers.clear();
                    });
                } else {
                    ep_managers_map_type::iterator ep_manager_it = find_ep_manager(endpoint);
                    if (ep_manager_it != ep_managers_end()) {
                        return ep_manager_it->second.stop(drain::yes).finally([this, endpoint, &ep_man = ep_manager_it->second] {
                            return with_file_update_mutex(ep_man, [&ep_man] {
                                return remove_file(ep_man.hints_dir().c_str());
                            }).finally([this, endpoint] {
                                _ep_managers.erase(endpoint);
                            });
                        });
                    }

                    return make_ready_future<>();
                }
            }).handle_exception([endpoint] (auto eptr) {
                manager_logger.error("Exception when draining {}: {}", endpoint, eptr);
            });
        });
    }).finally([endpoint] {
        manager_logger.trace("drain_for: finished draining {}", endpoint);
    });
}

void manager::update_backlog(size_t backlog, size_t max_backlog) {
    if (backlog < max_backlog) {
        allow_hints();
    } else {
        forbid_hints_for_eps_with_pending_hints();
    }
}

} // namespace db::hints
