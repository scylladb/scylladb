/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

// Boost features.
#include <boost/range/adaptors.hpp>

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
        resource_manager& res_manager, distributed<replica::database>& db)
    : _hints_dir(fs::path(hints_directory) / fmt::to_string(this_shard_id()))
    , _host_filter(std::move(filter))
    , _proxy(proxy)
    , _max_hint_window_us(max_hint_window_ms * 1000)
    , _local_db(db.local())
    , _resource_manager(res_manager)
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
                        sm::description("Number of hints files that were discarded during sending because the file was corrupted.")),

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

    if (_proxy.features().host_id_based_hinted_handoff) {
        _uses_host_id = true;
        co_await migrate_ip_directories();
    }

    co_await initialize_endpoint_managers();

    co_await compute_hints_dir_device_id();
    set_started();

    if (!_uses_host_id) {
        _migration_callback = _proxy.features().host_id_based_hinted_handoff.when_enabled([this] {
            _migrating_done = perform_migration();
        });
    }
}

future<> manager::stop() {
    manager_logger.info("Asked to stop a shard hint manager");

    set_stopping();

    return _migrating_done.finally([this] {
        return _draining_eps_gate.close();
    }).finally([this] {
        return parallel_for_each(_ep_managers | boost::adaptors::map_values, [] (hint_endpoint_manager& ep_man) {
            return ep_man.stop();
        }).finally([this] {
            _ep_managers.clear();
            _hint_directory_manager.clear();
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
        const auto ip = *_hint_directory_manager.get_mapping(host_id);
        if (has_ep_with_pending_hints(host_id) || has_ep_with_pending_hints(ip)) {
            ep_man.forbid_hints();
        } else {
            ep_man.allow_hints();
        }
    }
}

sync_point::shard_rps manager::calculate_current_sync_point(std::span<const gms::inet_address> target_eps) const {
    sync_point::shard_rps rps;
    const auto tmptr = _proxy.get_token_metadata_ptr();

    for (auto addr : target_eps) {
        const auto hid = tmptr->get_host_id_if_known(addr);
        // Ignore the IPs that we cannot map.
        if (!hid) {
            continue;
        }

        auto it = _ep_managers.find(*hid);
        if (it != _ep_managers.end()) {
            const hint_endpoint_manager& ep_man = it->second;
            rps[*hid] = ep_man.last_written_replay_position();
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
    // Prevent the migration to host-ID-based hinted handoff until this function finishes its execution.
    const auto shared_lock = co_await get_shared_lock(_migration_mutex);

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
            const auto maybe_hid = tmptr->get_host_id_if_known(std::get<gms::inet_address>(addr));
            // Ignore the IPs we cannot map.
            if (maybe_hid) [[likely]] {
                hid_rps.emplace(*maybe_hid, rp);
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

hint_endpoint_manager& manager::get_ep_manager(const endpoint_id& host_id, const gms::inet_address& ip) {
    // If this is enabled, we can't rely on the information obtained from `_hint_directory_manager`.
    if (_uses_host_id) {
        if (auto it = _ep_managers.find(host_id); it != _ep_managers.end()) {
            return it->second;
        }
    } else {
        if (const auto maybe_mapping = _hint_directory_manager.get_mapping(host_id, ip)) {
            return _ep_managers.at(maybe_mapping->first);
        }

        // If there is no mapping in `_hint_directory_manager` corresponding to either `host_id`, or `ip`,
        // we need to create a new endpoint manager.
        _hint_directory_manager.insert_mapping(host_id, ip);
    }

    try {
        std::filesystem::path hint_directory = hints_dir() / (_uses_host_id ? fmt::to_string(host_id) : fmt::to_string(ip));
        auto [it, _] = _ep_managers.emplace(host_id, hint_endpoint_manager{host_id, std::move(hint_directory), *this});
        hint_endpoint_manager& ep_man = it->second;

        manager_logger.trace("Created an endpoint manager for {}", host_id);
        ep_man.start();

        return ep_man;
    } catch (...) {
        manager_logger.warn("Starting a hint endpoint manager {}/{} has failed", host_id, ip);
        _hint_directory_manager.remove_mapping(host_id);
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

bool manager::have_ep_manager(const std::variant<locator::host_id, gms::inet_address>& ep) const noexcept {
    if (std::holds_alternative<locator::host_id>(ep)) {
        return _ep_managers.contains(std::get<locator::host_id>(ep));
    }
    return _hint_directory_manager.has_mapping(std::get<gms::inet_address>(ep));
}

bool manager::store_hint(endpoint_id host_id, gms::inet_address ip, schema_ptr s, lw_shared_ptr<const frozen_mutation> fm,
        tracing::trace_state_ptr tr_state) noexcept
{
    if (utils::get_local_injector().enter("reject_incoming_hints")) {
        manager_logger.debug("Rejecting a hint to {} / {} due to an error injection", host_id, ip);
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

        return get_ep_manager(host_id, ip).store_hint(std::move(s), std::move(fm), tr_state);
    } catch (...) {
        manager_logger.trace("Failed to store a hint to {}: {}", host_id, std::current_exception());
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", host_id, std::current_exception());

        ++_stats.errors;
        return false;
    }
}

/// Checks if there is a node corresponding to a given host ID that hasn't been down for longer
/// than a given amount of time. The function relies on information obtained from the passed `gms::gossiper`.
static bool endpoint_downtime_not_bigger_than(const gms::gossiper& gossiper, const locator::host_id& host_id,
        uint64_t max_downtime_us)
{
    // We want to enforce small buffer optimization in the call
    // to `gms::gossiper::for_each_endpoint_state_until()` below
    // to avoid an unnecessary allocation.
    // Since we need all these four pieces of information in the lambda,
    // the function object passed to the function might be too big.
    // That's why we create it locally on the stack and only pass a reference to it.
    struct sbo_info {
        locator::host_id host_id;
        const gms::gossiper& gossiper;
        int64_t max_hint_window_us;
        bool small_node_downtime;
    };

    sbo_info info {
        .host_id = host_id,
        .gossiper = gossiper,
        .max_hint_window_us = max_downtime_us,
        .small_node_downtime = false
    };

    gossiper.for_each_endpoint_state_until(
            [&info] (const gms::inet_address& ip, const gms::endpoint_state& state) {
        const auto app_state = state.get_application_state_ptr(gms::application_state::HOST_ID);
        const auto host_id = locator::host_id{utils::UUID{app_state->value()}};
        if (!app_state || host_id != info.host_id) {
            return stop_iteration::no;
        }
        if (info.gossiper.get_endpoint_downtime(ip) <= info.max_hint_window_us) {
            info.small_node_downtime = true;
            return stop_iteration::yes;
        }
        return stop_iteration::no;
    });

    return info.small_node_downtime;
}

bool manager::too_many_in_flight_hints_for(endpoint_id ep) const noexcept {
    // There is no need to check the DC here because if there is an in-flight hint for this
    // endpoint, then this means that its DC has already been checked and found to be ok.
    return _stats.size_of_hints_in_progress > max_size_of_hints_in_progress()
            && !_proxy.local_db().get_token_metadata().get_topology().is_me(ep)
            && hints_in_progress_for(ep) > 0
            && endpoint_downtime_not_bigger_than(local_gossiper(), ep, _max_hint_window_us);
}

bool manager::can_hint_for(endpoint_id ep) const noexcept {
    if (_state.contains(state::migrating)) {
        return false;
    }

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
        manager_logger.trace("size_of_hints_in_progress {} hints_in_progress_for({}) {}",
                _stats.size_of_hints_in_progress, ep, hipf);
        return false;
    }

    // Check that the destination DC is "hintable".
    if (!check_dc_for(ep)) {
        manager_logger.trace("{}'s DC is not hintable", ep);
        return false;
    }

    const bool node_is_alive = endpoint_downtime_not_bigger_than(local_gossiper(), ep, _max_hint_window_us);
    if (!node_is_alive) {
        manager_logger.trace("{} has been down for too long, not hinting", ep);
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

    manager_logger.debug("change_host_filter: changing from {} to {}", _host_filter, filter);

    // Change the host_filter now and save the old one so that we can
    // roll back in case of failure
    std::swap(_host_filter, filter);
    std::exception_ptr eptr = nullptr;

    try {
        const auto tmptr = _proxy.get_token_metadata_ptr();

        // Iterate over existing hint directories and see if we can enable an endpoint manager
        // for some of them
        co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(),
                [&] (fs::path datadir, directory_entry de) -> future<> {
            using pair_type = std::pair<locator::host_id, gms::inet_address>;

            const auto maybe_host_id_and_ip = std::invoke([&] () -> std::optional<pair_type> {
                try {
                    locator::host_id_or_endpoint hid_or_ep{de.name};

                    // If hinted handoff is host-ID-based, hint directories representing IP addresses must've
                    // been created by mistake and they're invalid. The same for pre-host-ID hinted handoff
                    // -- hint directories representing host IDs are NOT valid.
                    if (hid_or_ep.has_host_id() && _uses_host_id) {
                        return std::make_optional(pair_type{hid_or_ep.id(), hid_or_ep.resolve_endpoint(*tmptr)});
                    } else if (hid_or_ep.has_endpoint() && !_uses_host_id) {
                        return std::make_optional(pair_type{hid_or_ep.resolve_id(*tmptr), hid_or_ep.endpoint()});
                    } else {
                        return std::nullopt;
                    }
                } catch (...) {
                    return std::nullopt;
                }
            });

            if (!maybe_host_id_and_ip) {
                manager_logger.warn("Encountered a hint directory of invalid name while changing the host filter: {}. "
                        "Hints stored in it won't be replayed.", de.name);
                co_return;
            }

            const auto& topology = _proxy.get_token_metadata_ptr()->get_topology();
            const auto& [host_id, ip] = *maybe_host_id_and_ip;

            if (_ep_managers.contains(host_id) || !_host_filter.can_hint_for(topology, host_id)) {
                co_return;
            }

            co_await get_ep_manager(host_id, ip).populate_segments_to_replay();
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
                _hint_directory_manager.remove_mapping(ep);
            });
        });
    } catch (...) {
        if (eptr) {
            std::throw_with_nested(eptr);
        }
        throw;
    }
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

future<> manager::drain_for(endpoint_id host_id, gms::inet_address ip) noexcept {
    if (!started() || stopping() || draining_all()) {
        co_return;
    }

    manager_logger.trace("on_leave_cluster: {} is removed/decommissioned", host_id);

    const auto holder = seastar::gate::holder{_draining_eps_gate};
    // As long as we hold on to this lock, no migration of hinted handoff to host IDs
    // can be being performed because `manager::perform_migration()` takes it
    // at the beginning of its execution too.
    const auto sem_unit = co_await seastar::get_units(_drain_lock, 1);

    // After an endpoint has been drained, we remove its directory with all of its contents.
    auto drain_ep_manager = [] (hint_endpoint_manager& ep_man) -> future<> {
        return ep_man.stop(drain::yes).finally([&] {
            return ep_man.with_file_update_mutex([&ep_man] {
                return remove_file(ep_man.hints_dir().native());
            });
        });
    };

    std::exception_ptr eptr = nullptr;

    if (_proxy.local_db().get_token_metadata().get_topology().is_me(host_id)) {
        set_draining_all();

        try {
            co_await coroutine::parallel_for_each(_ep_managers | boost::adaptors::map_values,
                    [&drain_ep_manager] (hint_endpoint_manager& ep_man) {
                return drain_ep_manager(ep_man);
            });
        } catch (...) {
            eptr = std::current_exception();
        }

        _ep_managers.clear();
        _hint_directory_manager.clear();
    } else {
        const auto maybe_host_id = std::invoke([&] () -> std::optional<locator::host_id> {
            if (_uses_host_id) {
                return host_id;
            }
            // Before the whole cluster is migrated to the host-ID-based hinted handoff,
            // one hint directory may correspond to multiple target nodes. If *any* of them
            // leaves the cluster, we should drain the hint directory. This is why we need
            // to rely on this mapping here.
            const auto maybe_mapping = _hint_directory_manager.get_mapping(host_id, ip);
            if (maybe_mapping) {
                return maybe_mapping->first;
            }
            return std::nullopt;
        });

        if (maybe_host_id) {
            auto it = _ep_managers.find(*maybe_host_id);

            if (it != _ep_managers.end()) {
                try {
                    co_await drain_ep_manager(it->second);
                } catch (...) {
                    eptr = std::current_exception();
                }

                // We can't provide the function with `it` here because we co_await above,
                // so iterators could have been invalidated.
                // This never throws.
                _ep_managers.erase(*maybe_host_id);
                _hint_directory_manager.remove_mapping(*maybe_host_id);
            }
        }
    }

    if (eptr) {
        manager_logger.error("Exception when draining {}: {}", host_id, eptr);
    }

    manager_logger.trace("drain_for: finished draining {}", host_id);
}

void manager::update_backlog(size_t backlog, size_t max_backlog) {
    if (backlog < max_backlog) {
        allow_hints();
    } else {
        forbid_hints_for_eps_with_pending_hints();
    }
}

future<> manager::with_file_update_mutex_for(const std::variant<locator::host_id, gms::inet_address>& ep,
        noncopyable_function<future<> ()> func) {
    const locator::host_id host_id = std::invoke([&] {
        if (std::holds_alternative<locator::host_id>(ep)) {
            return std::get<locator::host_id>(ep);
        }
        return *_hint_directory_manager.get_mapping(std::get<gms::inet_address>(ep));
    });
    return _ep_managers.at(host_id).with_file_update_mutex(std::move(func));
}

future<> manager::initialize_endpoint_managers() {
    auto maybe_create_ep_mgr = [this] (const locator::host_id& host_id, const gms::inet_address& ip) -> future<> {
        if (!check_dc_for(host_id)) {
            co_return;
        }

        co_await get_ep_manager(host_id, ip).populate_segments_to_replay();
    };

    // We dispatch here to not hold on to the token metadata if hinted handoff is host-ID-based.
    // In that case, there are no directories that represent IP addresses, so we won't need to use it.
    // We want to avoid a situation when topology changes are prevented while we hold on to this pointer.
    const auto tmptr = _uses_host_id ? nullptr : _proxy.get_token_metadata_ptr();

    co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(),
            [&] (fs::path directory, directory_entry de) -> future<> {
        auto maybe_host_id_or_ep = std::invoke([&] () -> std::optional<locator::host_id_or_endpoint> {
            try {
                return locator::host_id_or_endpoint{de.name};
            } catch (...) {
                // The name represents neither an IP address, nor a host ID.
                return std::nullopt;
            }
        });

        // The directory is invalid, so there's nothing more to do.
        if (!maybe_host_id_or_ep) {
            manager_logger.warn("Encountered a hint directory of invalid name while initializing endpoint managers: {}. "
                    "Hints stored in it won't be replayed", de.name);
            co_return;
        }

        if (_uses_host_id) {
            // If hinted handoff is host-ID-based but the directory doesn't represent a host ID,
            // it's invalid. Ignore it.
            if (!maybe_host_id_or_ep->has_host_id()) {
                manager_logger.warn("Encountered a hint directory of invalid name while initializing endpoint managers: {}. "
                        "Hints stored in it won't be replayed", de.name);
                co_return;
            }

            // If hinted handoff is host-ID-based, `get_ep_manager` will NOT use the passed IP address,
            // so we simply pass the default value there.
            co_return co_await maybe_create_ep_mgr(maybe_host_id_or_ep->id(), gms::inet_address{});
        }

        // If we have got to this line, hinted handoff is still IP-based and we need to map the IP.

        if (!maybe_host_id_or_ep->has_endpoint()) {
            // If the directory name doesn't represent an IP, it's invalid. We ignore it.
            manager_logger.warn("Encountered a hint directory of invalid name while initializing endpoint managers: {}. "
                    "Hints stored in it won't be replayed", de.name);
            co_return;
        }

        const auto maybe_host_id = std::invoke([&] () -> std::optional<locator::host_id> {
            try {
                return maybe_host_id_or_ep->resolve_id(*tmptr);
            } catch (...) {
                return std::nullopt;
            }
        });

        if (!maybe_host_id) {
            co_return;
        }

        co_await maybe_create_ep_mgr(*maybe_host_id, maybe_host_id_or_ep->endpoint());
    });
}

// This function assumes that the hint directory is NOT modified as long as this function is being executed.
future<> manager::migrate_ip_directories() {
    std::vector<sstring> hint_directories{};

    // Step 1. Gather the names of the hint directories.
    co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(),
            [&] (std::filesystem::path, directory_entry de) -> future<> {
        hint_directories.push_back(std::move(de.name));
        co_return;
    });

    struct hint_dir_mapping {
        sstring current_name;
        sstring new_name;
    };

    std::vector<hint_dir_mapping> dirs_to_rename{};
    std::vector<std::filesystem::path> dirs_to_remove{};

    /* RAII lock for token metadata */ {
        // We need to keep the topology consistent throughout the loop below to
        // ensure that, for example, two different IPs won't be mapped to
        // the same host ID.
        //
        // We don't want to hold on to this pointer for longer than necessary.
        // Topology changes might be postponed otherwise.
        auto tmptr = _proxy.get_token_metadata_ptr();

        // Step 2. Obtain mappings IP -> host ID for the directories.
        for (auto& directory : hint_directories) {
            try {
                locator::host_id_or_endpoint hid_or_ep{directory};

                // If the directory's name already represents a host ID, there is nothing to do.
                if (hid_or_ep.has_host_id()) {
                    continue;
                }

                const locator::host_id host_id = hid_or_ep.resolve_id(*tmptr);
                dirs_to_rename.push_back({.current_name = std::move(directory), .new_name = host_id.to_sstring()});
            } catch (...) {
                // We cannot map the IP to the corresponding host ID either because
                // the relevant mapping doesn't exist anymore or an error occurred. Drop it.
                //
                // We only care about directories named after IPs during an upgrade,
                // so we don't want to make this more complex than necessary.
                manager_logger.warn("No mapping IP-host ID for hint directory {}. It is going to be removed", directory);
                dirs_to_remove.push_back(_hints_dir / std::move(directory));
            }
        }
    }

    // We don't need this memory anymore. The only remaining elements are the names of the directories
    // that already represent valid host IDs. We won't do anything with them. The rest have been moved
    // to either `dirs_to_rename` or `dirs_to_remove`.
    hint_directories.clear();

    // Step 3. Try to rename the directories.
    co_await coroutine::parallel_for_each(dirs_to_rename, [&] (auto& mapping) -> future<> {
        std::filesystem::path old_name = _hints_dir / std::move(mapping.current_name);
        std::filesystem::path new_name = _hints_dir / std::move(mapping.new_name);

        try {
            manager_logger.info("Renaming hint directory {} to {}", old_name.native(), new_name.native());
            co_await rename_file(old_name.native(), new_name.native());
        } catch (...) {
            manager_logger.warn("Renaming directory {} to {} has failed: {}",
                    old_name.native(), new_name.native(), std::current_exception());
            dirs_to_remove.push_back(std::move(old_name));
        }
    });

    // Step 4. Remove directories that don't represent host IDs.
    co_await coroutine::parallel_for_each(dirs_to_remove, [] (auto& directory) -> future<> {
        try {
            manager_logger.warn("Removing hint directory {}", directory.native());
            co_await lister::rmdir(directory);
        } catch (...) {
            on_internal_error(manager_logger,
                    seastar::format("Removing a hint directory has failed. Reason: {}", std::current_exception()));
        }
    });

    co_await io_check(sync_directory, _hints_dir.native());
}

future<> manager::perform_migration() {
    // This function isn't marked as noexcept, but the only parts of the code that
    // can throw an exception are:
    //   1. the call to `migrate_ip_directories()`: if we fail there, the failure is critical.
    //      It doesn't lead to any data corruption, but the node must be stopped;
    //   2. the re-initialization of the endpoint managers: a failure there is the same failure
    //      that can happen when starting a node. It may be seen as critical, but it should only
    //      boil down to not initializing some of the endpoint managers. No data corruption
    //      is possible.
    if (_state.contains(state::stopping) || _state.contains(state::draining_all)) {
        // It's possible the cluster feature is enabled right after the local node decides
        // to leave the cluster. In that case, the migration callback might still potentially
        // be called, but we don't want to perform it. We need to stop the node as soon as possible.
        //
        // The `state::draining_all` case is more tricky. The semantics of self-draining is not
        // specified, but based on the description of the state in the header file, it means
        // the node is leaving the cluster and it works like that indeed, so we apply the same reasoning.
        co_return;
    }

    manager_logger.info("Migration of hinted handoff to host ID is starting");
    // Step 1. Prevent accepting incoming hints.
    _state.set(state::migrating);

    // Step 2. Make sure during the migration there is no draining process and we don't await any sync points.

    // We're taking this lock for two reasons:
    //   1. we're waiting for the ongoing drains to finish so that there's no data race,
    //   2. we suspend new drain requests -- to prevent data races.
    const auto lock = co_await seastar::get_units(_drain_lock, 1);

    // We're taking this lock because we're about to stop endpoint managers here, whereas
    // `manager::wait_for_sync_point` browses them and awaits their corresponding sync points.
    // If we stop them during that process, that function will get exceptions.
    //
    // Although in the current implementation there is no danger of race conditions
    // (or at least race conditions that could be harmful in any way), it's better
    // to avoid them anyway. Hence this lock.
    const auto unique_lock = co_await get_unique_lock(_migration_mutex);
    // Step 3. Stop endpoint managers. We will modify the hint directory contents, so this is necessary.
    co_await coroutine::parallel_for_each(_ep_managers | std::views::values, [] (auto& ep_manager) -> future<> {
        return ep_manager.stop(drain::no);
    });

    // Step 4. Prevent resource manager from scanning the hint directory. Race conditions are unacceptable.
    auto resource_manager_lock = co_await seastar::get_units(_resource_manager.update_lock(), 1);

    // Once the resource manager cannot scan anything anymore, we can safely get rid of these.
    _ep_managers.clear();
    _eps_with_pending_hints.clear();

    // We won't need this anymore.
    _hint_directory_manager.clear();

    // Step 5. Rename the hint directories so that those that remain all represent valid host IDs.
    co_await migrate_ip_directories();
    _uses_host_id = true;

    // Step 6. Make resource manager scan the hint directory again.
    resource_manager_lock.return_all();
    // Step 7. Start accepting incoming hints again.
    _state.remove(state::migrating);
    // Step 8. Once resource manager is working again, endpoint managers can be safely recreated.
    //         We won't modify the contents of the hint directory anymore.
    co_await initialize_endpoint_managers();
    manager_logger.info("Migration of hinted handoff to host ID has finished successfully");
}

} // namespace db::hints
