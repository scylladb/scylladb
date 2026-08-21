/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/hints/manager.hh"

#include <fmt/ranges.h>

// Seastar features.
#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/file.hh>

// Boost features.

// Scylla includes.
#include "db/hints/internal/hint_logger.hh"
#include "gms/application_state.hh"
#include "gms/endpoint_state.hh"
#include "gms/feature_service.hh"
#include "gms/gossiper.hh"
#include "gms/inet_address.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/host_id.hh"
#include "locator/token_metadata.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "utils/directories.hh"
#include "utils/disk-error-handler.hh"
#include "utils/error_injection.hh"
#include "utils/lister.hh"
#include "seastarx.hh"

// STD.
#include <algorithm>
#include <exception>
#include <variant>

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

manager::manager(service::storage_proxy& proxy, sstring hints_directory, host_filter filter, int64_t max_hint_window_ms,
        resource_manager& res_manager, sharded<replica::database>& db, scheduling_group sg)
    : _hints_dir(fs::path(hints_directory) / fmt::to_string(this_shard_id()))
    , _host_filter(std::move(filter))
    , _proxy(proxy)
    , _max_hint_window_us(max_hint_window_ms * 1000)
    , _local_db(db.local())
    , _draining_eps_gate(seastar::format("hints::manager::{}", _hints_dir.native()))
    , _resource_manager(res_manager)
    , _hints_sending_sched_group(sg)
{
    if (utils::get_local_injector().enter("decrease_hints_flush_period")) {
        hints_flush_period = std::chrono::seconds{1};
    }
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

        sm::make_counter("sent_total", _stats.sent_total,
                        sm::description("Number of sent hints.")),

        sm::make_counter("sent_bytes_total", _stats.sent_hints_bytes_total,
                        sm::description("The total size of the sent hints (in bytes)")),

        sm::make_counter("discarded", _stats.discarded,
                        sm::description("Number of hints that were discarded during sending (too old, schema changed, etc.).")),

        sm::make_counter("send_errors", _stats.send_errors,
            sm::description("Number of unexpected errors during sending, sending will be retried later")),

        sm::make_counter("corrupted_files", _stats.corrupted_files,
                        sm::description("Number of hints files that were discarded during sending because the file was corrupted.")).set_skip_when_empty(),

        sm::make_gauge("pending_drains",
                        sm::description("Number of tasks waiting in the queue for draining hints"),
                        [this] { return _drain_lock.waiters(); }),

        sm::make_gauge("pending_sends",
                        sm::description("Number of tasks waiting in the queue for sending a hint"),
                        [this] { return _resource_manager.sending_queue_length(); })
    });
}

future<> manager::start(shared_ptr<const gms::gossiper> gossiper_ptr) {
    _gossiper_anchor = std::move(gossiper_ptr);

    co_await initialize_endpoint_managers();

    co_await compute_hints_dir_device_id();
    set_started();
}

future<> manager::stop() {
    manager_logger.info("Asked to stop a shard hint manager");

    set_stopping();

    const auto& node = *_proxy.get_token_metadata_ptr()->get_topology().this_node();
    const bool leaving = node.is_leaving() || node.left();

    // We want to stop the manager as soon as possible if it's not leaving the cluster.
    // Because of that, we need to cancel all ongoing drains (since that can take quite a bit of time),
    // but we also need to ensure that no new drains will be started in the meantime.
    if (!leaving) {
        for (auto& [_, ep_man] : _ep_managers) {
            ep_man.cancel_draining();
        }
    }
    return _draining_eps_gate.close().finally([this] {
        return parallel_for_each(_ep_managers | std::views::values, [] (hint_endpoint_manager& ep_man) {
            return ep_man.stop(drain::no);
        }).finally([this] {
            _ep_managers.clear();
            manager_logger.info("Shard hint manager has stopped");
        });
    });
}

future<> manager::compute_hints_dir_device_id() {
    try {
        _hints_dir_device_id = co_await get_device_id(_hints_dir.native());
    } catch (...) {
        manager_logger.warn("Failed to stat directory {} for device id: {}",
                _hints_dir.native(), std::current_exception());
        throw;
    }
}

void manager::allow_hints() {
    for (auto& [_, ep_man] : _ep_managers) {
        ep_man.allow_hints();
    }
}

void manager::forbid_hints() {
    for (auto& [_, ep_man] : _ep_managers) {
        ep_man.forbid_hints();
    }
}

void manager::forbid_hints_for_eps_with_pending_hints() {
    for (auto& [host_id, ep_man] : _ep_managers) {
        if (has_ep_with_pending_hints(host_id)) {
            ep_man.forbid_hints();
        } else {
            ep_man.allow_hints();
        }
    }
}

sync_point::shard_rps manager::calculate_current_sync_point(std::span<const locator::host_id> target_eps) const {
    sync_point::shard_rps rps;

    for (auto addr : target_eps) {
        auto it = _ep_managers.find(addr);
        if (it != _ep_managers.end()) {
            const hint_endpoint_manager& ep_man = it->second;
            rps[addr] = ep_man.last_written_replay_position();
        }
    }

    // When `target_eps` is empty, it means the sync point should correspond to ALL hosts.
    //
    // It's worth noting here why this algorithm works. We don't have a guarantee that there's
    // an endpoint manager for each hint directory stored by this node. However, if a hint
    // directory doesn't have a corresponding endpoint manager, there is one of the two reasons
    // for that:
    //
    // Reason 1. The hint directory is rejected by the host filter, i.e. this node is forbidden
    //           to send hints to the node corresponding to the directory. In that case, the user
    //           must've specified that they don't want hints to be sent there on their own
    //           and it makes no sense to wait for those hints to be sent.
    //
    // Reason 2. When upgrading Scylla from a version with IP-based hinted handoff to a version
    //           with support for host-ID hinted handoff, there's a transition period when
    //           endpoint managers are identified by host IDs, while the names of hint directories
    //           stored on disk still represent IP addresses; we keep mappings between those two
    //           entities. It may happen that multiple IPs correspond to the same hint directory
    //           and so -- even if a hint directory is accepted by the host filter, there might not
    //           be an endpoint manager managing it. This reason is ONLY possible during the transition
    //           period. Once the transition is done, only reason 1 can apply.
    //           For more details on the mappings and related things, see:
    //              scylladb/scylladb#12278 and scylladb/scylladb#15567.
    //
    // Because of that, it suffices to browse the existing endpoint managers and gather their
    // last replay positions to abide by the design and guarantees of the sync point API, i.e.
    // if the parameter `target_hosts` of a request to create a sync point is empty, we should
    // create a sync point for ALL other nodes.
    if (target_eps.empty()) {
        for (const auto& [host_id, ep_man] : _ep_managers) {
            rps[host_id] = ep_man.last_written_replay_position();
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

    const auto tmptr = _proxy.get_token_metadata_ptr();
    std::unordered_map<endpoint_id, replay_position> hid_rps{};
    hid_rps.reserve(rps.size());

    for (const auto& [addr, rp] : rps) {
        if (std::holds_alternative<gms::inet_address>(addr)) {
            try {
                const auto hid = _gossiper_anchor->get_host_id(std::get<gms::inet_address>(addr));
                hid_rps.emplace(hid, rp);
            } catch (...) {
                // Ignore the IPs we cannot map.
            }
        } else {
            hid_rps.emplace(std::get<locator::host_id>(addr), rp);
        }
    }

    bool was_aborted = false;
    co_await coroutine::parallel_for_each(_ep_managers,
            coroutine::lambda([&hid_rps, &local_as, &was_aborted] (auto& pair) -> future<> {
        auto& [ep, ep_man] = pair;

        // When `hid_rps` doesn't specify a replay position for a given endpoint, we use
        // its default value. Normally, it should be equal to returning a ready future here.
        // However, foreign segments (i.e. segments that were moved from another shard at start-up)
        // are treated differently from "regular" segments -- we can think of their replay positions
        // as equal to negative infinity or simply smaller from any other replay position, which
        // also includes the default value. Because of that, we don't have a choice -- we have to
        // pass either hid_rps[ep] or the default replay position to the endpoint manager because
        // some hints MIGHT need to be sent.
        const replay_position rp = [&] {
            auto it = hid_rps.find(ep);
            if (it == hid_rps.end()) {
                return replay_position{};
            }
            return it->second;
        } ();

        try {
            co_await ep_man.wait_until_hints_are_replayed_up_to(local_as, rp);
        } catch (abort_requested_exception&) {
            if (!local_as.abort_requested()) {
                local_as.request_abort();
            }
            was_aborted = true;
        }
    }));

    if (was_aborted) {
        co_await coroutine::return_exception(abort_requested_exception{});
    }
}

hint_endpoint_manager& manager::get_ep_manager(const endpoint_id& host_id) {
    if (auto it = _ep_managers.find(host_id); it != _ep_managers.end()) {
        return it->second;
    }

    try {
        std::filesystem::path hint_directory = hints_dir() / fmt::to_string(host_id);
        auto [it, _] = _ep_managers.emplace(host_id, hint_endpoint_manager{host_id, std::move(hint_directory), *this, _hints_sending_sched_group});
        hint_endpoint_manager& ep_man = it->second;

        manager_logger.trace("Created an endpoint manager for {}", host_id);
        ep_man.start();

        return ep_man;
    } catch (...) {
        manager_logger.warn("Starting a hint endpoint manager {} has failed", host_id);
        _ep_managers.erase(host_id);
        throw;
    }
}

uint64_t manager::max_size_of_hints_in_progress() const noexcept {
    if (utils::get_local_injector().enter("decrease_max_size_of_hints_in_progress")) [[unlikely]] {
        return 1'000;
    } else {
        return MAX_SIZE_OF_HINTS_IN_PROGRESS;
    }
}

bool manager::have_ep_manager(locator::host_id ep) const noexcept {
    return _ep_managers.contains(ep);
}

bool manager::store_hint(endpoint_id host_id, schema_ptr s, lw_shared_ptr<const frozen_mutation> fm,
        tracing::trace_state_ptr tr_state) noexcept
{
    if (utils::get_local_injector().enter("reject_incoming_hints")) {
        manager_logger.debug("Rejecting a hint to {} due to an error injection", host_id);
        ++_stats.dropped;
        return false;
    }

    if (stopping() || draining_all() || !started() || !can_hint_for(host_id)) {
        manager_logger.trace("Can't store a hint to {}", host_id);
        ++_stats.dropped;
        return false;
    }

    try {
        manager_logger.trace("Going to store a hint to {}", host_id);
        tracing::trace(tr_state, "Going to store a hint to {}", host_id);

        return get_ep_manager(host_id).store_hint(std::move(s), std::move(fm), tr_state);
    } catch (...) {
        manager_logger.trace("Failed to store a hint to {}: {}", host_id, std::current_exception());
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", host_id, std::current_exception());

        ++_stats.errors;
        return false;
    }
}

bool manager::too_many_in_flight_hints_for(endpoint_id ep) const noexcept {
    // There is no need to check the DC here because if there is an in-flight hint for this
    // endpoint, then this means that its DC has already been checked and found to be ok.
    return _stats.size_of_hints_in_progress > max_size_of_hints_in_progress()
            && !_proxy.local_db().get_token_metadata().get_topology().is_me(ep)
            && hints_in_progress_for(ep) > 0
            && local_gossiper().get_endpoint_downtime(ep) <= _max_hint_window_us;
}

bool manager::can_hint_for(endpoint_id ep) const noexcept {
    if (_proxy.local_db().get_token_metadata().get_topology().is_me(ep)) {
        return false;
    }

    auto it = _ep_managers.find(ep);
    if (it != _ep_managers.end() && (it->second.stopping() || !it->second.can_hint())) {
        return false;
    }

    // Don't allow more than one in-flight (to the store) hint to a specific destination when
    // the total size of in-flight hints is more than the maximum allowed value.
    //
    // In the worst case there's going to be (_max_size_of_hints_in_progress + N - 1) in-flight
    // hints where N is the total number nodes in the cluster.
    const auto hipf = hints_in_progress_for(ep);
    if (_stats.size_of_hints_in_progress > max_size_of_hints_in_progress() && hipf > 0) {
        manager_logger.trace("can_hint_for: size_of_hints_in_progress {} hints_in_progress_for({}) {}",
                _stats.size_of_hints_in_progress, ep, hipf);
        return false;
    }

    // Check that the destination DC is "hintable".
    if (!check_dc_for(ep)) {
        manager_logger.trace("can_hint_for: {}'s DC is not hintable", ep);
        return false;
    }

    const bool node_is_alive = local_gossiper().get_endpoint_downtime(ep) <= _max_hint_window_us;
    if (!node_is_alive) {
        manager_logger.trace("can_hint_for: {} has been down for too long, not hinting", ep);
        return false;
    }

    return true;
}

future<> manager::change_host_filter(host_filter filter) {
    if (!started()) {
        co_await coroutine::return_exception(
                std::logic_error{"change_host_filter: called before the hints_manager was started"});
    }

    const auto holder = seastar::gate::holder{_draining_eps_gate};
    const auto sem_unit = co_await seastar::get_units(_drain_lock, 1);

    if (draining_all()) {
        co_await coroutine::return_exception(std::logic_error{
                "change_host_filter: cannot change the configuration because hints all hints were drained"});
    }

    manager_logger.info("change_host_filter: changing from {} to {}", _host_filter, filter);

    // Change the host_filter now and save the old one so that we can
    // roll back in case of failure
    std::swap(_host_filter, filter);
    std::exception_ptr eptr = nullptr;

    try {
        // Iterate over existing hint directories and see if we can enable an endpoint manager
        // for some of them
        co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(),
                [&] (fs::path datadir, directory_entry de) -> future<> {
            const auto maybe_host_id = std::invoke([&] () -> std::optional<locator::host_id> {
                try {
                    return locator::host_id(utils::UUID(de.name));
                } catch (...) {
                    return std::nullopt;
                }
            });

            if (!maybe_host_id) {
                manager_logger.warn("Encountered a hint directory of invalid name while changing the host filter: {}. "
                        "Hints stored in it won't be replayed.", de.name);
                co_return;
            }

            const auto& topology = _proxy.get_token_metadata_ptr()->get_topology();
            const auto& host_id = *maybe_host_id;

            if (_ep_managers.contains(host_id) || !_host_filter.can_hint_for(topology, host_id)) {
                co_return;
            }

            co_await get_ep_manager(host_id).populate_segments_to_replay();
        });
    } catch (...) {
        // Revert the changes in the filter. The code below will stop the additional managers
        // that were started so far.
        _host_filter = std::move(filter);
        eptr = std::current_exception();
    }

    try {
        // Remove endpoint managers which are rejected by the filter.
        co_await coroutine::parallel_for_each(_ep_managers, [this] (auto& pair) {
            auto& [ep, ep_man] = pair;

            if (_host_filter.can_hint_for(_proxy.get_token_metadata_ptr()->get_topology(), ep)) {
                return make_ready_future<>();
            }

            return ep_man.stop(drain::no).finally([this, ep] {
                _ep_managers.erase(ep);
            });
        });
    } catch (...) {
        const sstring exception_message = eptr
                ? seastar::format("{} + {}", eptr, std::current_exception())
                : seastar::format("{}", std::current_exception());

        manager_logger.warn("Changing the host filter has failed: {}", exception_message);

        if (eptr) {
            std::throw_with_nested(eptr);
        }
        throw;
    }

    manager_logger.info("The host filter has been changed successfully");
}

bool manager::check_dc_for(endpoint_id ep) const noexcept {
    try {
        // If target's DC is not a "hintable" DCs - don't hint.
        // If there is an end point manager then DC has already been checked and found to be ok.
        return _host_filter.is_enabled_for_all() || have_ep_manager(ep) ||
               _host_filter.can_hint_for(_proxy.get_token_metadata_ptr()->get_topology(), ep);
    } catch (...) {
        // if we failed to check the DC - block this hint
        return false;
    }
}

future<> manager::drain_for(endpoint_id host_id) noexcept {
    if (!started() || stopping() || draining_all()) {
        co_return;
    }

    if (!replay_allowed()) {
        auto reason = seastar::format("Precondition violated while trying to drain {}: "
                "hint replay is not allowed", host_id);
        on_internal_error(manager_logger, std::move(reason));
    }

    manager_logger.info("Draining starts for {}", host_id);

    const auto holder = seastar::gate::holder{_draining_eps_gate};
    const auto sem_unit = co_await seastar::get_units(_drain_lock, 1);

    // After an endpoint has been drained, we remove its directory with all of its contents.
    auto drain_ep_manager = [] (hint_endpoint_manager& ep_man) -> future<> {
        // Prevent a drain if the endpoint manager was marked to cancel it.
        if (ep_man.canceled_draining()) {
            return make_ready_future();
        }
        return ep_man.stop(drain::yes).finally([&ep_man] {
            // If draining was canceled, we can't remove the hint directory yet
            // because there might still be some hints that we should send.
            // We'll do that when the node starts again.
            // Note that canceling draining can ONLY occur when the node is simply stopping.
            // That cannot happen when decommissioning the node.
            if (ep_man.canceled_draining()) {
                return make_ready_future();
            }

            return ep_man.with_file_update_mutex([&ep_man] -> future<> {
                return remove_file(ep_man.hints_dir().native()).then([&ep_man] {
                    manager_logger.info("Removed hint directory for {}", ep_man.end_point_key());
                });
            });
        });
    };

    std::exception_ptr eptr = nullptr;

    if (_proxy.local_db().get_token_metadata().get_topology().is_me(host_id)) {
        set_draining_all();

        try {
            co_await coroutine::parallel_for_each(_ep_managers | std::views::values,
                    [&drain_ep_manager] (hint_endpoint_manager& ep_man) {
                return drain_ep_manager(ep_man);
            });
        } catch (...) {
            eptr = std::current_exception();
        }

        _ep_managers.clear();
    } else {
        auto it = _ep_managers.find(host_id);

        if (it != _ep_managers.end()) {
            try {
                co_await drain_ep_manager(it->second);
            } catch (...) {
                eptr = std::current_exception();
            }

            // We can't provide the function with `it` here because we co_await above,
            // so iterators could have been invalidated.
            // This never throws.
            _ep_managers.erase(host_id);
        }
    }

    if (eptr) {
        manager_logger.error("Exception when draining {}: {}", host_id, eptr);
    }

    manager_logger.info("drain_for: finished draining {}", host_id);
}

void manager::update_backlog(size_t backlog, size_t max_backlog) {
    if (backlog < max_backlog) {
        allow_hints();
    } else {
        forbid_hints_for_eps_with_pending_hints();
    }
}

future<> manager::with_file_update_mutex_for(locator::host_id ep,
        noncopyable_function<future<> ()> func) {
    return _ep_managers.at(ep).with_file_update_mutex(std::move(func));
}

future<> manager::initialize_endpoint_managers() {
    co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(),
            [this] (fs::path directory, directory_entry de) -> future<> {
        auto maybe_host_id = std::invoke([&] () -> std::optional<locator::host_id> {
            try {
                return locator::host_id(utils::UUID(de.name));
            } catch (...) {
                return std::nullopt;
            }
        });

        if (!maybe_host_id) {
            manager_logger.warn("Encountered a hint directory of invalid name while initializing endpoint managers: {}. "
                    "Hints stored in it won't be replayed", de.name);
            co_return;
        }

        if (!check_dc_for(*maybe_host_id)) {
            co_return;
        }

        co_await get_ep_manager(*maybe_host_id).populate_segments_to_replay();
    });
}

// Technical note: This function obviously doesn't need to be a coroutine. However, it's better to impose
//                 this constraint early on with possible future refactors in mind. It should be easier
//                 to modify the function this way.
future<> manager::drain_left_nodes() {
    for (const auto& [host_id, ep_man] : _ep_managers) {
        if (!_proxy.get_token_metadata_ptr()->is_normal_token_owner(host_id)) {
            // It's safe to discard this future. It's awaited in `manager::stop()`.
            (void) drain_for(host_id);
        }
    }

    co_return;
}

} // namespace db::hints
