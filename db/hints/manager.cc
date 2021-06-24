/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
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
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/gate.hh>
#include <boost/range/adaptors.hpp>
#include "utils/div_ceil.hh"
#include "db/extensions.hh"
#include "service/storage_proxy.hh"
#include "gms/versioned_value.hh"
#include "seastarx.hh"
#include "converting_mutation_partition_applier.hh"
#include "utils/disk-error-handler.hh"
#include "lister.hh"
#include "db/timeout_clock.hh"
#include "service/priority_manager.hh"
#include "database.hh"
#include "service_permit.hh"
#include "utils/directories.hh"
#include "locator/abstract_replication_strategy.hh"
#include "mutation_partition_view.hh"
#include "utils/runtime.hh"

using namespace std::literals::chrono_literals;

namespace db {
namespace hints {

static logging::logger manager_logger("hints_manager");
const std::string manager::FILENAME_PREFIX("HintsLog" + commitlog::descriptor::SEPARATOR);

const std::chrono::seconds manager::hint_file_write_timeout = std::chrono::seconds(2);
const std::chrono::seconds manager::hints_flush_period = std::chrono::seconds(10);

manager::manager(sstring hints_directory, host_filter filter, int64_t max_hint_window_ms, resource_manager& res_manager, distributed<database>& db)
    : _hints_dir(fs::path(hints_directory) / format("{:d}", this_shard_id()))
    , _host_filter(std::move(filter))
    , _local_snitch_ptr(locator::i_endpoint_snitch::get_local_snitch_ptr())
    , _max_hint_window_us(max_hint_window_ms * 1000)
    , _local_db(db.local())
    , _resource_manager(res_manager)
{}

manager::~manager() {
    assert(_ep_managers.empty());
}

void manager::register_metrics(const sstring& group_name) {
    namespace sm = seastar::metrics;

    _metrics.add_group(group_name, {
        sm::make_gauge("size_of_hints_in_progress", _stats.size_of_hints_in_progress,
                        sm::description("Size of hinted mutations that are scheduled to be written.")),

        sm::make_derive("written", _stats.written,
                        sm::description("Number of successfully written hints.")),

        sm::make_derive("errors", _stats.errors,
                        sm::description("Number of errors during hints writes.")),

        sm::make_derive("dropped", _stats.dropped,
                        sm::description("Number of dropped hints.")),

        sm::make_derive("sent", _stats.sent,
                        sm::description("Number of sent hints.")),

        sm::make_derive("discarded", _stats.discarded,
                        sm::description("Number of hints that were discarded during sending (too old, schema changed, etc.).")),

        sm::make_derive("corrupted_files", _stats.corrupted_files,
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
    return lister::scan_dir(_hints_dir, { directory_entry_type::directory }, [this] (fs::path datadir, directory_entry de) {
        ep_key_type ep = ep_key_type(de.name);
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
        end_point_hints_manager& ep_man = pair.second;
        if (has_ep_with_pending_hints(ep_man.end_point_key())) {
            ep_man.forbid_hints();
        } else {
            ep_man.allow_hints();
        }
    });
}

bool manager::end_point_hints_manager::store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept {
    try {
        // Future is waited on indirectly in `stop()` (via `_store_gate`).
        (void)with_gate(_store_gate, [this, s = std::move(s), fm = std::move(fm), tr_state] () mutable {
            ++_hints_in_progress;
            size_t mut_size = fm->representation().size();
            shard_stats().size_of_hints_in_progress += mut_size;

            return with_shared(file_update_mutex(), [this, fm, s, tr_state] () mutable -> future<> {
                return get_or_load().then([this, fm = std::move(fm), s = std::move(s), tr_state] (hints_store_ptr log_ptr) mutable {
                    commitlog_entry_writer cew(s, *fm, db::commitlog::force_sync::no);
                    return log_ptr->add_entry(s->id(), cew, db::timeout_clock::now() + _shard_manager.hint_file_write_timeout);
                }).then([this, tr_state] (db::rp_handle rh) {
                    auto rp = rh.release();
                    if (_last_written_rp < rp) {
                        _last_written_rp = rp;
                        manager_logger.debug("[{}] Updated last written replay position to {}", end_point_key(), rp);
                    }
                    ++shard_stats().written;

                    manager_logger.trace("Hint to {} was stored", end_point_key());
                    tracing::trace(tr_state, "Hint to {} was stored", end_point_key());
                }).handle_exception([this, tr_state] (std::exception_ptr eptr) {
                    ++shard_stats().errors;

                    manager_logger.debug("store_hint(): got the exception when storing a hint to {}: {}", end_point_key(), eptr);
                    tracing::trace(tr_state, "Failed to store a hint to {}: {}", end_point_key(), eptr);
                });
            }).finally([this, mut_size, fm, s] {
                --_hints_in_progress;
                shard_stats().size_of_hints_in_progress -= mut_size;
            });;
        });
    } catch (...) {
        manager_logger.trace("Failed to store a hint to {}: {}", end_point_key(), std::current_exception());
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", end_point_key(), std::current_exception());

        ++shard_stats().dropped;
        return false;
    }
    return true;
}

future<> manager::end_point_hints_manager::populate_segments_to_replay() {
    return with_lock(file_update_mutex(), [this] {
        return get_or_load().discard_result();
    });
}

void manager::end_point_hints_manager::start() {
    clear_stopped();
    allow_hints();
    _sender.start();
}

future<> manager::end_point_hints_manager::stop(drain should_drain) noexcept {
    if(stopped()) {
        return make_exception_future<>(std::logic_error(format("ep_manager[{}]: stop() is called twice", _key).c_str()));
    }

    return seastar::async([this, should_drain] {
        std::exception_ptr eptr;

        // This is going to prevent further storing of new hints and will break all sending in progress.
        set_stopping();

        _store_gate.close().handle_exception([&eptr] (auto e) { eptr = std::move(e); }).get();
        _sender.stop(should_drain).handle_exception([&eptr] (auto e) { eptr = std::move(e); }).get();

        with_lock(file_update_mutex(), [this] {
            if (_hints_store_anchor) {
                hints_store_ptr tmp = std::exchange(_hints_store_anchor, nullptr);
                return tmp->shutdown().finally([tmp] {
                    return tmp->release();
                }).finally([tmp] {});
            }
            return make_ready_future<>();
        }).handle_exception([&eptr] (auto e) { eptr = std::move(e); }).get();

        if (eptr) {
            manager_logger.error("ep_manager[{}]: exception: {}", _key, eptr);
        }

        set_stopped();
    });
}

manager::end_point_hints_manager::end_point_hints_manager(const key_type& key, manager& shard_manager)
    : _key(key)
    , _shard_manager(shard_manager)
    , _file_update_mutex_ptr(make_lw_shared<seastar::shared_mutex>())
    , _file_update_mutex(*_file_update_mutex_ptr)
    , _state(state_set::of<state::stopped>())
    , _hints_dir(_shard_manager.hints_dir() / format("{}", _key).c_str())
    // Approximate the position of the last written hint by using the same formula as for segment id calculation in commitlog
    // TODO: Should this logic be deduplicated with what is in the commitlog?
    , _last_written_rp(this_shard_id(), std::chrono::duration_cast<std::chrono::milliseconds>(runtime::get_boot_time().time_since_epoch()).count())
    , _sender(*this, _shard_manager.local_storage_proxy(), _shard_manager.local_db(), _shard_manager.local_gossiper())
{}

manager::end_point_hints_manager::end_point_hints_manager(end_point_hints_manager&& other)
    : _key(other._key)
    , _shard_manager(other._shard_manager)
    , _file_update_mutex_ptr(std::move(other._file_update_mutex_ptr))
    , _file_update_mutex(*_file_update_mutex_ptr)
    , _state(other._state)
    , _hints_dir(std::move(other._hints_dir))
    , _sender(other._sender, *this)
{}

manager::end_point_hints_manager::~end_point_hints_manager() {
    assert(stopped());
}

future<hints_store_ptr> manager::end_point_hints_manager::get_or_load() {
    if (!_hints_store_anchor) {
        return _shard_manager.store_factory().get_or_load(_key, [this] (const key_type&) noexcept {
            return add_store();
        }).then([this] (hints_store_ptr log_ptr) {
            _hints_store_anchor = log_ptr;
            return make_ready_future<hints_store_ptr>(std::move(log_ptr));
        });
    }

    return make_ready_future<hints_store_ptr>(_hints_store_anchor);
}

manager::end_point_hints_manager& manager::get_ep_manager(ep_key_type ep) {
    auto it = find_ep_manager(ep);
    if (it == ep_managers_end()) {
        manager_logger.trace("Creating an ep_manager for {}", ep);
        manager::end_point_hints_manager& ep_man = _ep_managers.emplace(ep, end_point_hints_manager(ep, *this)).first->second;
        ep_man.start();
        return ep_man;
    }
    return it->second;
}

inline bool manager::have_ep_manager(ep_key_type ep) const noexcept {
    return find_ep_manager(ep) != ep_managers_end();
}

bool manager::store_hint(ep_key_type ep, schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept {
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

future<db::commitlog> manager::end_point_hints_manager::add_store() noexcept {
    manager_logger.trace("Going to add a store to {}", _hints_dir.c_str());

    return futurize_invoke([this] {
        return io_check([name = _hints_dir.c_str()] { return recursive_touch_directory(name); }).then([this] () {
            commitlog::config cfg;

            cfg.commit_log_location = _hints_dir.c_str();
            cfg.commitlog_segment_size_in_mb = resource_manager::hint_segment_size_in_mb;
            cfg.commitlog_total_space_in_mb = resource_manager::max_hints_per_ep_size_mb;
            cfg.fname_prefix = manager::FILENAME_PREFIX;
            cfg.extensions = &_shard_manager.local_db().extensions();

            // HH doesn't utilize the flow that benefits from reusing segments.
            // Therefore let's simply disable it to avoid any possible confusion.
            cfg.reuse_segments = false;
            // HH leaves segments on disk after commitlog shutdown, and later reads
            // them when commitlog is re-created. This is expected to happen regularly
            // during standard HH workload, so no need to print a warning about it.
            cfg.warn_about_segments_left_on_disk_after_shutdown = false;
            // Allow going over the configured size limit of the commitlog
            // (resource_manager::max_hints_per_ep_size_mb). The commitlog will
            // be more conservative with its disk usage when going over the limit.
            // On the other hand, HH counts used space using the space_watchdog
            // in resource_manager, so its redundant for the commitlog to apply
            // a hard limit.
            cfg.allow_going_over_size_limit = true;

            return commitlog::create_commitlog(std::move(cfg)).then([this] (commitlog l) {
                // add_store() is triggered every time hint files are forcefully flushed to I/O (every hints_flush_period).
                // When this happens we want to refill _sender's segments only if it has finished with the segments he had before.
                if (_sender.have_segments()) {
                    return make_ready_future<commitlog>(std::move(l));
                }

                std::vector<sstring> segs_vec = l.get_segments_to_replay();
                std::vector<std::pair<db::segment_id_type, sstring>> local_segs_vec;
                local_segs_vec.reserve(segs_vec.size());

                // Divide segments into those that were created on this shard
                // and those which were moved to it during rebalancing.
                for (auto& seg : segs_vec) {
                    db::commitlog::descriptor desc(seg, manager::FILENAME_PREFIX);
                    unsigned shard_id = db::replay_position(desc).shard_id();
                    if (shard_id == this_shard_id()) {
                        local_segs_vec.emplace_back(desc.id, std::move(seg));
                    } else {
                        _sender.add_foreign_segment(std::move(seg));
                    }
                }

                // Sort local segments by their segment ids, which should
                // correspond to the chronological order.
                std::sort(local_segs_vec.begin(), local_segs_vec.end());

                for (auto& [segment_id, seg] : local_segs_vec) {
                    _sender.add_segment(std::move(seg));
                }

                return make_ready_future<commitlog>(std::move(l));
            });
        });
    });
}

future<> manager::end_point_hints_manager::flush_current_hints() noexcept {
    // flush the currently created hints to disk
    if (_hints_store_anchor) {
        return futurize_invoke([this] {
            return with_lock(file_update_mutex(), [this]() -> future<> {
                return get_or_load().then([] (hints_store_ptr cptr) {
                    return cptr->shutdown().finally([cptr] {
                        return cptr->release();
                    }).finally([cptr] {});
                }).then([this] {
                    // Un-hold the commitlog object. Since we are under the exclusive _file_update_mutex lock there are no
                    // other hints_store_ptr copies and this would destroy the commitlog shared value.
                    _hints_store_anchor = nullptr;

                    // Re-create the commitlog instance - this will re-populate the _segments_to_replay if needed.
                    return get_or_load().discard_result();
                });
            });
        });
    }

    return make_ready_future<>();
}

class no_column_mapping : public std::out_of_range {
public:
    no_column_mapping(const utils::UUID& id) : std::out_of_range(format("column mapping for CF {} is missing", id)) {}
};

future<> manager::end_point_hints_manager::sender::flush_maybe() noexcept {
    auto current_time = clock::now();
    if (current_time >= _next_flush_tp) {
        return _ep_manager.flush_current_hints().then([this, current_time] {
            _next_flush_tp = current_time + hints_flush_period;
        }).handle_exception([] (auto eptr) {
            manager_logger.trace("flush_maybe() failed: {}", eptr);
            return make_ready_future<>();
        });
    }
    return make_ready_future<>();
}

future<timespec> manager::end_point_hints_manager::sender::get_last_file_modification(const sstring& fname) {
    return open_file_dma(fname, open_flags::ro).then([] (file f) {
        return do_with(std::move(f), [] (file& f) {
            return f.stat();
        });
    }).then([] (struct stat st) {
        return make_ready_future<timespec>(st.st_mtim);
    });
}

future<> manager::end_point_hints_manager::sender::do_send_one_mutation(frozen_mutation_and_schema m, const inet_address_vector_replica_set& natural_endpoints) noexcept {
    return futurize_invoke([this, m = std::move(m), &natural_endpoints] () mutable -> future<> {
        // The fact that we send with CL::ALL in both cases below ensures that new hints are not going
        // to be generated as a result of hints sending.
        if (boost::range::find(natural_endpoints, end_point_key()) != natural_endpoints.end()) {
            manager_logger.trace("Sending directly to {}", end_point_key());
            return _proxy.send_hint_to_endpoint(std::move(m), end_point_key());
        } else {
            manager_logger.trace("Endpoints set has changed and {} is no longer a replica. Mutating from scratch...", end_point_key());
            return _proxy.send_hint_to_all_replicas(std::move(m));
        }
    });
}

bool manager::end_point_hints_manager::sender::can_send() noexcept {
    if (stopping() && !draining()) {
        return false;
    }

    try {
        auto ep_state_ptr = _gossiper. get_endpoint_state_for_endpoint_ptr(end_point_key());
        if (ep_state_ptr && ep_state_ptr->is_alive()) {
            _state.remove(state::ep_state_left_the_ring);
            return true;
        } else {
            if (!_state.contains(state::ep_state_left_the_ring)) {
                _state.set_if<state::ep_state_left_the_ring>(!_shard_manager.local_db().get_token_metadata().is_member(end_point_key()));
            }
            // send the hints out if the destination Node is part of the ring - we will send to all new replicas in this case
            return _state.contains(state::ep_state_left_the_ring);
        }
    } catch (...) {
        return false;
    }
}

frozen_mutation_and_schema manager::end_point_hints_manager::sender::get_mutation(lw_shared_ptr<send_one_file_ctx> ctx_ptr, fragmented_temporary_buffer& buf) {
    hint_entry_reader hr(buf);
    auto& fm = hr.mutation();
    auto& cm = get_column_mapping(std::move(ctx_ptr), fm, hr);
    auto schema = _db.find_schema(fm.column_family_id());

    if (schema->version() != fm.schema_version()) {
        mutation m(schema, fm.decorated_key(*schema));
        converting_mutation_partition_applier v(cm, *schema, m.partition());
        fm.partition().accept(cm, v);
        return {freeze(m), std::move(schema)};
    }
    return {std::move(hr).mutation(), std::move(schema)};
}

const column_mapping& manager::end_point_hints_manager::sender::get_column_mapping(lw_shared_ptr<send_one_file_ctx> ctx_ptr, const frozen_mutation& fm, const hint_entry_reader& hr) {
    auto cm_it = ctx_ptr->schema_ver_to_column_mapping.find(fm.schema_version());
    if (cm_it == ctx_ptr->schema_ver_to_column_mapping.end()) {
        if (!hr.get_column_mapping()) {
            throw no_column_mapping(fm.schema_version());
        }

        manager_logger.debug("new schema version {}", fm.schema_version());
        cm_it = ctx_ptr->schema_ver_to_column_mapping.emplace(fm.schema_version(), *hr.get_column_mapping()).first;
    }

    return cm_it->second;
}

bool manager::too_many_in_flight_hints_for(ep_key_type ep) const noexcept {
    // There is no need to check the DC here because if there is an in-flight hint for this end point then this means that
    // its DC has already been checked and found to be ok.
    return _stats.size_of_hints_in_progress > max_size_of_hints_in_progress && !utils::fb_utilities::is_me(ep) && hints_in_progress_for(ep) > 0 && local_gossiper().get_endpoint_downtime(ep) <= _max_hint_window_us;
}

bool manager::can_hint_for(ep_key_type ep) const noexcept {
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
    if (_stats.size_of_hints_in_progress > max_size_of_hints_in_progress && hints_in_progress_for(ep) > 0) {
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
            return lister::scan_dir(_hints_dir, { directory_entry_type::directory }, [this] (fs::path datadir, directory_entry de) {
                const ep_key_type ep = ep_key_type(de.name);
                if (_ep_managers.contains(ep) || !_host_filter.can_hint_for(_local_snitch_ptr, ep)) {
                    return make_ready_future<>();
                }
                return get_ep_manager(ep).populate_segments_to_replay();
            }).handle_exception([this, filter = std::move(filter)] (auto ep) mutable {
                // Bring back the old filter. The finally() block will cause us to stop
                // the additional ep_hint_managers that we started
                _host_filter = std::move(filter);
            }).finally([this] {
                // Remove endpoint managers which are rejected by the filter
                return parallel_for_each(_ep_managers, [this] (auto& pair) {
                    if (_host_filter.can_hint_for(_local_snitch_ptr, pair.first)) {
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

bool manager::check_dc_for(ep_key_type ep) const noexcept {
    try {
        // If target's DC is not a "hintable" DCs - don't hint.
        // If there is an end point manager then DC has already been checked and found to be ok.
        return _host_filter.is_enabled_for_all() || have_ep_manager(ep) ||
               _host_filter.can_hint_for(_local_snitch_ptr, ep);
    } catch (...) {
        // if we failed to check the DC - block this hint
        return false;
    }
}

void manager::drain_for(gms::inet_address endpoint) {
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

manager::end_point_hints_manager::sender::sender(end_point_hints_manager& parent, service::storage_proxy& local_storage_proxy, database& local_db, gms::gossiper& local_gossiper) noexcept
    : _stopped(make_ready_future<>())
    , _ep_key(parent.end_point_key())
    , _ep_manager(parent)
    , _shard_manager(_ep_manager._shard_manager)
    , _resource_manager(_shard_manager._resource_manager)
    , _proxy(local_storage_proxy)
    , _db(local_db)
    , _hints_cpu_sched_group(_db.get_streaming_scheduling_group())
    , _gossiper(local_gossiper)
    , _file_update_mutex(_ep_manager.file_update_mutex())
{}

manager::end_point_hints_manager::sender::sender(const sender& other, end_point_hints_manager& parent) noexcept
    : _stopped(make_ready_future<>())
    , _ep_key(parent.end_point_key())
    , _ep_manager(parent)
    , _shard_manager(_ep_manager._shard_manager)
    , _resource_manager(_shard_manager._resource_manager)
    , _proxy(other._proxy)
    , _db(other._db)
    , _hints_cpu_sched_group(other._hints_cpu_sched_group)
    , _gossiper(other._gossiper)
    , _file_update_mutex(_ep_manager.file_update_mutex())
{}


future<> manager::end_point_hints_manager::sender::stop(drain should_drain) noexcept {
    return seastar::async([this, should_drain] {
        set_stopping();
        _stopped.get();

        if (should_drain == drain::yes) {
            // "Draining" is performed by a sequence of following calls:
            // set_draining() -> send_hints_maybe() -> flush_current_hints() -> send_hints_maybe()
            //
            // Before sender::stop() is called the storing path for this end point is blocked and no new hints
            // will be generated when this method is running.
            //
            // send_hints_maybe() in a "draining" mode is going to send all hints from segments in the
            // _segments_to_replay.
            //
            // Therefore after the first call for send_hints_maybe() the _segments_to_replay is going to become empty
            // and the following flush_current_hints() is going to store all in-memory hints to the disk and re-populate
            // the _segments_to_replay.
            //
            // The next call for send_hints_maybe() will send the last hints to the current end point and when it is
            // done there is going to be no more pending hints and the corresponding hints directory may be removed.
            manager_logger.trace("Draining for {}: start", end_point_key());
            set_draining();
            send_hints_maybe();
            _ep_manager.flush_current_hints().handle_exception([] (auto e) {
                manager_logger.error("Failed to flush pending hints: {}. Ignoring...", e);
            }).get();
            send_hints_maybe();
            manager_logger.trace("Draining for {}: end", end_point_key());
        }
        manager_logger.trace("ep_manager({})::sender: exiting", end_point_key());
    });
}

void manager::end_point_hints_manager::sender::add_segment(sstring seg_name) {
    _segments_to_replay.emplace_back(std::move(seg_name));
}

void manager::end_point_hints_manager::sender::add_foreign_segment(sstring seg_name) {
    _foreign_segments_to_replay.emplace_back(std::move(seg_name));
}

manager::end_point_hints_manager::sender::clock::duration manager::end_point_hints_manager::sender::next_sleep_duration() const {
    clock::time_point current_time = clock::now();
    clock::time_point next_flush_tp = std::max(_next_flush_tp, current_time);
    clock::time_point next_retry_tp = std::max(_next_send_retry_tp, current_time);

    clock::duration d = std::min(next_flush_tp, next_retry_tp) - current_time;

    // Don't sleep for less than 10 ticks of the "clock" if we are planning to sleep at all - the sleep() function is not perfect.
    return clock::duration(10 * div_ceil(d.count(), 10));
}

void manager::end_point_hints_manager::sender::start() {
    seastar::thread_attributes attr;

    attr.sched_group = _hints_cpu_sched_group;
    _stopped = seastar::async(std::move(attr), [this] {
        manager_logger.trace("ep_manager({})::sender: started", end_point_key());
        while (!stopping()) {
            try {
                flush_maybe().get();
                send_hints_maybe();

                // If we got here means that either there are no more hints to send or we failed to send hints we have.
                // In both cases it makes sense to wait a little before continuing.
                sleep_abortable(next_sleep_duration()).get();
            } catch (seastar::sleep_aborted&) {
                break;
            } catch (...) {
                // log and keep on spinning
                manager_logger.trace("sender: got the exception: {}", std::current_exception());
            }
        }
    });
}

future<> manager::end_point_hints_manager::sender::send_one_mutation(frozen_mutation_and_schema m) {
    keyspace& ks = _db.find_keyspace(m.s->ks_name());
    auto& rs = ks.get_replication_strategy();
    auto token = dht::get_token(*m.s, m.fm.key());
    inet_address_vector_replica_set natural_endpoints = rs.get_natural_endpoints(std::move(token));

    return do_send_one_mutation(std::move(m), natural_endpoints);
}

future<> manager::end_point_hints_manager::sender::send_one_hint(lw_shared_ptr<send_one_file_ctx> ctx_ptr, fragmented_temporary_buffer buf, db::replay_position rp, gc_clock::duration secs_since_file_mod, const sstring& fname) {
    return _resource_manager.get_send_units_for(buf.size_bytes()).then([this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] (auto units) mutable {
        ctx_ptr->mark_hint_as_in_progress(rp);

        // Future is waited on indirectly in `send_one_file()` (via `ctx_ptr->file_send_gate`).
        (void)with_gate(ctx_ptr->file_send_gate, [this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] () mutable {
            try {
                auto m = this->get_mutation(ctx_ptr, buf);
                gc_clock::duration gc_grace_sec = m.s->gc_grace_seconds();

                // The hint is too old - drop it.
                //
                // Files are aggregated for at most manager::hints_timer_period therefore the oldest hint there is
                // (last_modification - manager::hints_timer_period) old.
                if (gc_clock::now().time_since_epoch() - secs_since_file_mod > gc_grace_sec - manager::hints_flush_period) {
                    return make_ready_future<>();
                }

                return this->send_one_mutation(std::move(m)).then([this, rp, ctx_ptr] {
                    ++this->shard_stats().sent;
                }).handle_exception([this, ctx_ptr, rp] (auto eptr) {
                    manager_logger.trace("send_one_hint(): failed to send to {}: {}", end_point_key(), eptr);
                    return make_exception_future<>(std::move(eptr));
                });

            // ignore these errors and move on - probably this hint is too old and the KS/CF has been deleted...
            } catch (no_such_column_family& e) {
                manager_logger.debug("send_hints(): no_such_column_family: {}", e.what());
                ++this->shard_stats().discarded;
            } catch (no_such_keyspace& e) {
                manager_logger.debug("send_hints(): no_such_keyspace: {}", e.what());
                ++this->shard_stats().discarded;
            } catch (no_column_mapping& e) {
                manager_logger.debug("send_hints(): {} at {}: {}", fname, rp, e.what());
                ++this->shard_stats().discarded;
            } catch (...) {
                auto eptr = std::current_exception();
                manager_logger.debug("send_hints(): unexpected error in file {} at {}: {}", fname, rp, eptr);
                return make_exception_future<>(std::move(eptr));
            }
            return make_ready_future<>();
        }).then_wrapped([units = std::move(units), ctx_ptr, rp] (future<>&& f) {
            // Information about the error was already printed somewhere higher.
            // We just need to account in the ctx that sending of this hint has failed.
            if (!f.failed()) {
                ctx_ptr->on_hint_send_success(rp);
            } else {
                ctx_ptr->on_hint_send_failure(rp);
            }
            f.ignore_ready_future();
        });
    }).handle_exception([this, ctx_ptr, rp] (auto eptr) {
        manager_logger.trace("send_one_file(): Hmmm. Something bad had happend: {}", eptr);
        ctx_ptr->on_hint_send_failure(rp);
    });
}

void manager::end_point_hints_manager::sender::send_one_file_ctx::mark_hint_as_in_progress(db::replay_position rp) {
}

void manager::end_point_hints_manager::sender::send_one_file_ctx::on_hint_send_success(db::replay_position rp) noexcept {
    if (!last_succeeded_rp || *last_succeeded_rp < rp) {
        last_succeeded_rp = rp;
    }
}

void manager::end_point_hints_manager::sender::send_one_file_ctx::on_hint_send_failure(db::replay_position rp) noexcept {
    segment_replay_failed = true;
    if (!first_failed_rp || rp < *first_failed_rp) {
        first_failed_rp = rp;
    }
}

// runs in a seastar::async context
bool manager::end_point_hints_manager::sender::send_one_file(const sstring& fname) {
    timespec last_mod = get_last_file_modification(fname).get0();
    gc_clock::duration secs_since_file_mod = std::chrono::seconds(last_mod.tv_sec);
    lw_shared_ptr<send_one_file_ctx> ctx_ptr = make_lw_shared<send_one_file_ctx>(_last_schema_ver_to_column_mapping);

    try {
        commitlog::read_log_file(fname, manager::FILENAME_PREFIX, service::get_local_streaming_priority(), [this, secs_since_file_mod, &fname, ctx_ptr] (commitlog::buffer_and_replay_position buf_rp) mutable {
            auto& buf = buf_rp.buffer;
            auto& rp = buf_rp.position;
            // Check that we can still send the next hint. Don't try to send it if the destination host
            // is DOWN or if we have already failed to send some of the previous hints.
            if (!draining() && ctx_ptr->segment_replay_failed) {
                return make_ready_future<>();
            }

            // Break early if stop() was called or the destination node went down.
            if (!can_send()) {
                ctx_ptr->segment_replay_failed = true;
                return make_ready_future<>();
            }

            return flush_maybe().finally([this, ctx_ptr, buf = std::move(buf), rp, secs_since_file_mod, &fname] () mutable {
                return send_one_hint(std::move(ctx_ptr), std::move(buf), rp, secs_since_file_mod, fname);
            });
        }, _last_not_complete_rp.pos, &_db.extensions()).get();
    } catch (db::commitlog::segment_error& ex) {
        manager_logger.error("{}: {}. Dropping...", fname, ex.what());
        ctx_ptr->segment_replay_failed = false;
        ++this->shard_stats().corrupted_files;
    } catch (...) {
        manager_logger.trace("sending of {} failed: {}", fname, std::current_exception());
        ctx_ptr->segment_replay_failed = true;
    }

    // wait till all background hints sending is complete
    ctx_ptr->file_send_gate.close().get();

    // If we are draining ignore failures and drop the segment even if we failed to send it.
    if (draining() && ctx_ptr->segment_replay_failed) {
        manager_logger.trace("send_one_file(): we are draining so we are going to delete the segment anyway");
        ctx_ptr->segment_replay_failed = false;
    }

    // update the next iteration replay position if needed
    if (ctx_ptr->segment_replay_failed) {
        // If some hints failed to be sent, first_failed_rp will tell the position of first such hint.
        // If there was an error thrown by read_log_file function itself, we will retry sending from
        // the last hint that was successfully sent (last_succeeded_rp).
        _last_not_complete_rp = ctx_ptr->first_failed_rp.value_or(ctx_ptr->last_succeeded_rp.value_or(_last_not_complete_rp));
        manager_logger.trace("send_one_file(): error while sending hints from {}, last RP is {}", fname, _last_not_complete_rp);
        return false;
    }

    // If we got here we are done with the current segment and we can remove it.
    with_shared(_file_update_mutex, [&fname, this] {
        auto p = _ep_manager.get_or_load().get0();
        return p->delete_segments({ fname });
    }).get();

    // clear the replay position - we are going to send the next segment...
    _last_not_complete_rp = replay_position();
    _last_schema_ver_to_column_mapping.clear();
    manager_logger.trace("send_one_file(): segment {} was sent in full and deleted", fname);
    return true;
}

const sstring* manager::end_point_hints_manager::sender::name_of_current_segment() const {
    // Foreign segments are replayed first
    if (!_foreign_segments_to_replay.empty()) {
        return &_foreign_segments_to_replay.front();
    }
    if (!_segments_to_replay.empty()) {
        return &_segments_to_replay.front();
    }
    return nullptr;
}

void manager::end_point_hints_manager::sender::pop_current_segment() {
    if (!_foreign_segments_to_replay.empty()) {
        _foreign_segments_to_replay.pop_front();
    } else if (!_segments_to_replay.empty()) {
        _segments_to_replay.pop_front();
    }
}

// Runs in the seastar::async context
void manager::end_point_hints_manager::sender::send_hints_maybe() noexcept {
    using namespace std::literals::chrono_literals;
    manager_logger.trace("send_hints(): going to send hints to {}, we have {} segment to replay", end_point_key(), _segments_to_replay.size() + _foreign_segments_to_replay.size());

    int replayed_segments_count = 0;

    try {
        while (true) {
            const sstring* seg_name = name_of_current_segment();
            if (!seg_name || !replay_allowed() || !can_send()) {
                break;
            }
            if (!send_one_file(*seg_name)) {
                break;
            }
            pop_current_segment();
            ++replayed_segments_count;
        }

    // Ignore exceptions, we will retry sending this file from where we left off the next time.
    // Exceptions are not expected here during the regular operation, so just log them.
    } catch (...) {
        manager_logger.trace("send_hints(): got the exception: {}", std::current_exception());
    }

    if (have_segments()) {
        // TODO: come up with something more sophisticated here
        _next_send_retry_tp = clock::now() + 1s;
    } else {
        // if there are no segments to send we want to retry when we maybe have some (after flushing)
        _next_send_retry_tp = _next_flush_tp;
    }

    manager_logger.trace("send_hints(): we handled {} segments", replayed_segments_count);
}

static future<> scan_for_hints_dirs(const sstring& hints_directory, std::function<future<> (fs::path dir, directory_entry de, unsigned shard_id)> f) {
    return lister::scan_dir(hints_directory, { directory_entry_type::directory }, [f = std::move(f)] (fs::path dir, directory_entry de) mutable {
        unsigned shard_id;
        try {
            shard_id = std::stoi(de.name.c_str());
        } catch (std::invalid_argument& ex) {
            manager_logger.debug("Ignore invalid directory {}", de.name);
            return make_ready_future<>();
        }
        return f(std::move(dir), std::move(de), shard_id);
    });
}

// runs in seastar::async context
manager::hints_segments_map manager::get_current_hints_segments(const sstring& hints_directory) {
    hints_segments_map current_hints_segments;

    // shards level
    scan_for_hints_dirs(hints_directory, [&current_hints_segments] (fs::path dir, directory_entry de, unsigned shard_id) {
        manager_logger.trace("shard_id = {}", shard_id);
        // IPs level
        return lister::scan_dir(dir / de.name.c_str(), { directory_entry_type::directory }, [&current_hints_segments, shard_id] (fs::path dir, directory_entry de) {
            manager_logger.trace("\tIP: {}", de.name);
            // hints files
            return lister::scan_dir(dir / de.name.c_str(), { directory_entry_type::regular }, [&current_hints_segments, shard_id, ep_addr = de.name] (fs::path dir, directory_entry de) {
                manager_logger.trace("\t\tfile: {}", de.name);
                current_hints_segments[ep_addr][shard_id].emplace_back(dir / de.name.c_str());
                return make_ready_future<>();
            });
        });
    }).get();

    return current_hints_segments;
}

// runs in seastar::async context
void manager::rebalance_segments(const sstring& hints_directory, hints_segments_map& segments_map) {
    // Count how many hints segments to each destination we have.
    std::unordered_map<sstring, size_t> per_ep_hints;
    for (auto& ep_info : segments_map) {
        per_ep_hints[ep_info.first] = boost::accumulate(ep_info.second | boost::adaptors::map_values | boost::adaptors::transformed(std::mem_fn(&std::list<fs::path>::size)), 0);
        manager_logger.trace("{}: total files: {}", ep_info.first, per_ep_hints[ep_info.first]);
    }

    // Create a map of lists of segments that we will move (for each destination end point): if a shard has segments
    // then we will NOT move q = int(N/S) segments out of them, where N is a total number of segments to the current
    // destination and S is a current number of shards.
    std::unordered_map<sstring, std::list<fs::path>> segments_to_move;
    for (auto& [ep, ep_segments] : segments_map) {
        size_t q = per_ep_hints[ep] / smp::count;
        auto& current_segments_to_move = segments_to_move[ep];

        for (auto& [shard_id, shard_segments] : ep_segments) {
            // Move all segments from the shards that are no longer relevant (re-sharding to the lower number of shards)
            if (shard_id >= smp::count) {
                current_segments_to_move.splice(current_segments_to_move.end(), shard_segments);
            } else if (shard_segments.size() > q) {
                current_segments_to_move.splice(current_segments_to_move.end(), shard_segments, std::next(shard_segments.begin(), q), shard_segments.end());
            }
        }
    }

    // Since N (a total number of segments to a specific destination) may be not a multiple of S (a current number of
    // shards) we will distribute files in two passes:
    //    * if N = S * q + r, then
    //       * one pass for segments_per_shard = q
    //       * another one for segments_per_shard = q + 1.
    //
    // This way we will ensure as close to the perfect distribution as possible.
    //
    // Right till this point we haven't moved any segments. However we have created a logical separation of segments
    // into two groups:
    //    * Segments that are not going to be moved: segments in the segments_map.
    //    * Segments that are going to be moved: segments in the segments_to_move.
    //
    // rebalance_segments_for() is going to consume segments from segments_to_move and move them to corresponding
    // lists in the segments_map AND actually move segments to the corresponding shard's sub-directory till the requested
    // segments_per_shard level is reached (see more details in the description of rebalance_segments_for()).
    for (auto& [ep, N] : per_ep_hints) {
        size_t q = N / smp::count;
        size_t r = N - q * smp::count;
        auto& current_segments_to_move = segments_to_move[ep];
        auto& current_segments_map = segments_map[ep];

        if (q) {
            rebalance_segments_for(ep, q, hints_directory, current_segments_map, current_segments_to_move);
        }

        if (r) {
            rebalance_segments_for(ep, q + 1, hints_directory, current_segments_map, current_segments_to_move);
        }
    }
}

// runs in seastar::async context
void manager::rebalance_segments_for(
        const sstring& ep,
        size_t segments_per_shard,
        const sstring& hints_directory,
        hints_ep_segments_map& ep_segments,
        std::list<fs::path>& segments_to_move)
{
    manager_logger.trace("{}: segments_per_shard: {}, total number of segments to move: {}", ep, segments_per_shard, segments_to_move.size());

    // sanity check
    if (segments_to_move.empty() || !segments_per_shard) {
        return;
    }

    for (unsigned i = 0; i < smp::count && !segments_to_move.empty(); ++i) {
        fs::path shard_path_dir(fs::path(hints_directory.c_str()) / seastar::format("{:d}", i).c_str() / ep.c_str());
        std::list<fs::path>& current_shard_segments = ep_segments[i];

        // Make sure that the shard_path_dir exists and if not - create it
        io_check([name = shard_path_dir.c_str()] { return recursive_touch_directory(name); }).get();

        while (current_shard_segments.size() < segments_per_shard && !segments_to_move.empty()) {
            auto seg_path_it = segments_to_move.begin();
            fs::path new_path(shard_path_dir / seg_path_it->filename());

            // Don't move the file to the same location - it's pointless.
            if (*seg_path_it != new_path) {
                manager_logger.trace("going to move: {} -> {}", *seg_path_it, new_path);
                io_check(rename_file, seg_path_it->native(), new_path.native()).get();
            } else {
                manager_logger.trace("skipping: {}", *seg_path_it);
            }
            current_shard_segments.splice(current_shard_segments.end(), segments_to_move, seg_path_it, std::next(seg_path_it));
        }
    }
}

// runs in seastar::async context
void manager::remove_irrelevant_shards_directories(const sstring& hints_directory) {
    // shards level
    scan_for_hints_dirs(hints_directory, [] (fs::path dir, directory_entry de, unsigned shard_id) {
        if (shard_id >= smp::count) {
            // IPs level
            return lister::scan_dir(dir / de.name.c_str(), { directory_entry_type::directory, directory_entry_type::regular }, lister::show_hidden::yes, [] (fs::path dir, directory_entry de) {
                return io_check(remove_file, (dir / de.name.c_str()).native());
            }).then([shard_base_dir = dir, shard_entry = de] {
                return io_check(remove_file, (shard_base_dir / shard_entry.name.c_str()).native());
            });
        }
        return make_ready_future<>();
    }).get();
}

future<> manager::rebalance(sstring hints_directory) {
    return seastar::async([hints_directory = std::move(hints_directory)] {
        // Scan currently present hints segments.
        hints_segments_map current_hints_segments = get_current_hints_segments(hints_directory);

        // Move segments to achieve an even distribution of files among all present shards.
        rebalance_segments(hints_directory, current_hints_segments);

        // Remove the directories of shards that are not present anymore - they should not have any segments by now
        remove_irrelevant_shards_directories(hints_directory);
    });
}

void manager::update_backlog(size_t backlog, size_t max_backlog) {
    if (backlog < max_backlog) {
        allow_hints();
    } else {
        forbid_hints_for_eps_with_pending_hints();
    }
}

class directory_initializer::impl {
    enum class state {
        uninitialized = 0,
        created_and_validated = 1,
        rebalanced = 2,
    };

    utils::directories& _dirs;
    sstring _hints_directory;
    state _state = state::uninitialized;
    seastar::named_semaphore _lock = {1, named_semaphore_exception_factory{"hints directory initialization lock"}};

public:
    impl(utils::directories& dirs, sstring hints_directory)
            : _dirs(dirs)
            , _hints_directory(std::move(hints_directory))
    { }

    future<> ensure_created_and_verified() {
        if (_state > state::uninitialized) {
            return make_ready_future<>();
        }

        return with_semaphore(_lock, 1, [this] () {
            utils::directories::set dir_set;
            dir_set.add_sharded(_hints_directory);
            return _dirs.create_and_verify(std::move(dir_set)).then([this] {
                manager_logger.debug("Creating and validating hint directories: {}", _hints_directory);
                _state = state::created_and_validated;
            });
        });
    }

    future<> ensure_rebalanced() {
        if (_state < state::created_and_validated) {
            return make_exception_future<>(std::logic_error("hints directory needs to be created and validated before rebalancing"));
        }

        if (_state > state::created_and_validated) {
            return make_ready_future<>();
        }

        return with_semaphore(_lock, 1, [this] () {
            manager_logger.debug("Rebalancing hints in {}", _hints_directory);
            return manager::rebalance(_hints_directory).then([this] {
                _state = state::rebalanced;
            });
        });
    }
};

directory_initializer::directory_initializer(std::shared_ptr<directory_initializer::impl> impl)
        : _impl(std::move(impl))
{ }

directory_initializer::~directory_initializer()
{ }

directory_initializer directory_initializer::make_dummy() {
    return directory_initializer{nullptr};
}

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

}
}
