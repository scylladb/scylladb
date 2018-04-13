/*
 * Modified by ScyllaDB
 * Copyright (C) 2017 ScyllaDB
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

#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/gate.hh>
#include "utils/div_ceil.hh"
#include "db/config.hh"
#include "service/storage_proxy.hh"
#include "gms/versioned_value.hh"
#include "seastarx.hh"
#include "converting_mutation_partition_applier.hh"
#include "disk-error-handler.hh"
#include "lister.hh"
#include "db/timeout_clock.hh"

namespace db {
namespace hints {

static logging::logger manager_logger("hints_manager");
const std::string manager::FILENAME_PREFIX("HintsLog" + commitlog::descriptor::SEPARATOR);

const std::chrono::seconds manager::hint_file_write_timeout = std::chrono::seconds(2);
const std::chrono::seconds manager::hints_flush_period = std::chrono::seconds(10);
const std::chrono::seconds manager::space_watchdog::_watchdog_period = std::chrono::seconds(1);
// TODO: remove this when we switch to C++17
constexpr size_t manager::_max_hints_send_queue_length;

size_t db::hints::manager::max_shard_disk_space_size;

manager::manager(sstring hints_directory, std::vector<sstring> hinted_dcs, int64_t max_hint_window_ms, distributed<database>& db)
    : _hints_dir(boost::filesystem::path(hints_directory) / format("{:d}", engine().cpu_id()).c_str())
    , _hinted_dcs(hinted_dcs.begin(), hinted_dcs.end())
    , _local_snitch_ptr(locator::i_endpoint_snitch::get_local_snitch_ptr())
    , _max_hint_window_us(max_hint_window_ms * 1000)
    , _local_db(db.local())
    , _max_send_in_flight_memory(std::max(memory::stats().total_memory() / 10, _max_hints_send_queue_length))
    , _min_send_hint_budget(_max_send_in_flight_memory / _max_hints_send_queue_length)
    , _send_limiter(_max_send_in_flight_memory)
    , _space_watchdog(*this)
{
    namespace sm = seastar::metrics;

    _metrics.add_group("hints_manager", {
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
    });
}

manager::~manager() {
    assert(_ep_managers.empty());
}

future<> manager::start(shared_ptr<service::storage_proxy> proxy_ptr, shared_ptr<gms::gossiper> gossiper_ptr) {
    _proxy_anchor = std::move(proxy_ptr);
    _gossiper_anchor = std::move(gossiper_ptr);
    return lister::scan_dir(_hints_dir, { directory_entry_type::directory }, [this] (lister::path datadir, directory_entry de) {
        ep_key_type ep = ep_key_type(de.name);
        if (!check_dc_for(ep)) {
            return make_ready_future<>();
        }
        return get_ep_manager(ep).populate_segments_to_replay();
    }).then([this] {
        // we are ready to store new hints...
        _space_watchdog.start();
    });
}

future<> manager::stop() {
    manager_logger.info("Asked to stop");

    _stopping = true;

    return when_all(
        parallel_for_each(_ep_managers, [] (auto& pair) {
            return pair.second.stop();
        }),
        _space_watchdog.stop()
    ).finally([] {
        manager_logger.info("Stopped");
    }).discard_result();
}

bool manager::end_point_hints_manager::store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept {
    try {
        with_gate(_store_gate, [this, s = std::move(s), fm = std::move(fm), tr_state] () mutable {
            ++_hints_in_progress;
            size_t mut_size = fm->representation().size();
            shard_stats().size_of_hints_in_progress += mut_size;

            return with_shared(file_update_mutex(), [this, fm, s, tr_state] () mutable -> future<> {
                return get_or_load().then([this, fm = std::move(fm), s = std::move(s), tr_state] (hints_store_ptr log_ptr) mutable {
                    commitlog_entry_writer cew(s, *fm);
                    return log_ptr->add_entry(s->id(), cew, db::timeout_clock::now() + _shard_manager.hint_file_write_timeout);
                }).then([this, tr_state] (db::rp_handle rh) {
                    rh.release();
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
    _sender.start();
}

future<> manager::end_point_hints_manager::stop() noexcept {
    return seastar::async([this] {
        std::exception_ptr eptr;

        // This is going to prevent further storing of new hints and will break all sending in progress.
        set_stopping();

        _store_gate.close().handle_exception([&eptr] (auto e) { eptr = std::move(e); }).get();
        _sender.stop().handle_exception([&eptr] (auto e) { eptr = std::move(e); }).get();

        with_lock(file_update_mutex(), [this] {
            if (_hints_store_anchor) {
                hints_store_ptr tmp = std::exchange(_hints_store_anchor, nullptr);
                return tmp->shutdown().finally([tmp] {});
            }
            return make_ready_future<>();
        }).handle_exception([&eptr] (auto e) { eptr = std::move(e); }).get();

        if (eptr) {
            manager_logger.error("ep_manager[{}]: exception: {}", _key, eptr);
        }
    });
}

manager::end_point_hints_manager::end_point_hints_manager(const key_type& key, manager& shard_manager)
    : _key(key)
    , _shard_manager(shard_manager)
    , _hints_dir(_shard_manager.hints_dir() / format("{}", _key).c_str())
    , _sender(*this, _shard_manager.local_storage_proxy(), _shard_manager.local_db(), _shard_manager.local_gossiper())
{
    allow_hints();
}

manager::end_point_hints_manager::end_point_hints_manager(end_point_hints_manager&& other)
    : _key(other._key)
    , _shard_manager(other._shard_manager)
    , _state(other._state)
    , _hints_dir(std::move(other._hints_dir))
    , _sender(other._sender, *this)
{}

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
    if (_stopping || !can_hint_for(ep)) {
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
    using namespace boost::filesystem;
    manager_logger.trace("Going to add a store to {}", _hints_dir.c_str());

    return futurize_apply([this] {
        return io_check(recursive_touch_directory, _hints_dir.c_str()).then([this] () {
            commitlog::config cfg;

            cfg.commit_log_location = _hints_dir.c_str();
            cfg.commitlog_segment_size_in_mb = _hint_segment_size_in_mb;
            cfg.commitlog_total_space_in_mb = _max_hints_per_ep_size_mb;
            cfg.fname_prefix = manager::FILENAME_PREFIX;
            cfg.extensions = &_shard_manager.local_db().get_config().extensions();

            return commitlog::create_commitlog(std::move(cfg)).then([this] (commitlog l) {
                // add_store() is triggered every time hint files are forcefully flushed to I/O (every hints_flush_period).
                // When this happens we want to refill _sender's segments only if it has finished with the segments he had before.
                if (_sender.have_segments()) {
                    return make_ready_future<commitlog>(std::move(l));
                }

                std::vector<sstring> segs_vec = l.get_segments_to_replay();

                std::for_each(segs_vec.begin(), segs_vec.end(), [this] (sstring& seg) {
                    _sender.add_segment(std::move(seg));
                });

                return make_ready_future<commitlog>(std::move(l));
            });
        });
    });
}

future<> manager::end_point_hints_manager::flush_current_hints() noexcept {
    // flush the currently created hints to disk
    if (_hints_store_anchor) {
        return futurize_apply([this] {
            return with_lock(file_update_mutex(), [this]() -> future<> {
                return get_or_load().then([] (hints_store_ptr cptr) {
                    return cptr->shutdown();
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

future<> manager::end_point_hints_manager::sender::do_send_one_mutation(mutation m, const std::vector<gms::inet_address>& natural_endpoints) noexcept {
    return futurize_apply([this, m = std::move(m), &natural_endpoints] () mutable -> future<> {
        // The fact that we send with CL::ALL in both cases below ensures that new hints are not going
        // to be generated as a result of hints sending.
        if (boost::range::find(natural_endpoints, end_point_key()) != natural_endpoints.end()) {
            manager_logger.trace("Sending directly to {}", end_point_key());
            return _proxy.send_to_endpoint(std::move(m), end_point_key(), { }, write_type::SIMPLE);
        } else {
            manager_logger.trace("Endpoints set has changed and {} is no longer a replica. Mutating from scratch...", end_point_key());
            return _proxy.mutate({std::move(m)}, consistency_level::ALL, nullptr);
        }
    });
}

bool manager::end_point_hints_manager::sender::can_send() noexcept {
    if (stopping()) {
        return false;
    }

    try {
        if (!_gossiper.is_alive(end_point_key())) {
            if (!_state.contains(state::ep_state_is_not_normal)) {
                _state.set_if<state::ep_state_is_not_normal>(_shard_manager.local_gossiper().get_gossip_status(end_point_key()) != sstring(gms::versioned_value::STATUS_NORMAL));
            }
            // send the hints out if the destination Node is not in a NORMAL state - we will send to all new replicas in this case
            return _state.contains(state::ep_state_is_not_normal);
        } else {
            _state.remove(state::ep_state_is_not_normal);
            return true;
        }
    } catch (...) {
        return false;
    }
}

mutation manager::end_point_hints_manager::sender::get_mutation(lw_shared_ptr<send_one_file_ctx> ctx_ptr, temporary_buffer<char>& buf) {
    hint_entry_reader hr(buf);
    auto& fm = hr.mutation();
    auto& cm = get_column_mapping(std::move(ctx_ptr), fm, hr);
    auto& cf = _db.find_column_family(fm.column_family_id());

    if (cf.schema()->version() != fm.schema_version()) {
        mutation m(cf.schema(), fm.decorated_key(*cf.schema()));
        converting_mutation_partition_applier v(cm, *cf.schema(), m.partition());
        fm.partition().accept(cm, v);

        return std::move(m);
    } else {
        return fm.unfreeze(cf.schema());
    }
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

manager::space_watchdog::space_watchdog(manager& shard_manager)
    : _shard_manager(shard_manager)
    , _timer([this] { on_timer(); })
{}

void manager::space_watchdog::start() {
    _timer.arm(timer_clock_type::now());
}

future<> manager::space_watchdog::stop() noexcept {
    try {
        return _gate.close().finally([this] { _timer.cancel(); });
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
}

future<> manager::space_watchdog::scan_one_ep_dir(boost::filesystem::path path, ep_key_type ep_key) {
    return lister::scan_dir(path, { directory_entry_type::regular }, [this, ep_key] (lister::path dir, directory_entry de) {
        // Put the current end point ID to state.eps_with_pending_hints when we see the second hints file in its directory
        if (_files_count == 1) {
            _eps_with_pending_hints.emplace(ep_key);
        }
        ++_files_count;

        return io_check(file_size, (dir / de.name.c_str()).c_str()).then([this] (uint64_t fsize) {
            _total_size += fsize;
        });
    });
}

void manager::space_watchdog::on_timer() {
    with_gate(_gate, [this] {
        return futurize_apply([this] {
            _eps_with_pending_hints.clear();
            _eps_with_pending_hints.reserve(_shard_manager._ep_managers.size());
            _total_size = 0;

            // The hints directories are organized as follows:
            // <hints root>
            //    |- <shard1 ID>
            //    |  |- <EP1 address>
            //    |     |- <hints file1>
            //    |     |- <hints file2>
            //    |     |- ...
            //    |  |- <EP2 address>
            //    |     |- ...
            //    |  |-...
            //    |- <shard2 ID>
            //    |  |- ...
            //    ...
            //    |- <shardN ID>
            //    |  |- ...
            //

            // This is a top level shard hints directory, let's enumerate per-end-point sub-directories...
            return lister::scan_dir(_shard_manager._hints_dir, {directory_entry_type::directory}, [this] (lister::path dir, directory_entry de) {
                _files_count = 0;
                // Let's scan per-end-point directories and enumerate hints files...
                //
                // Let's check if there is a corresponding end point manager (may not exist if the corresponding DC is
                // not hintable).
                // If exists - let's take a file update lock so that files are not changed under our feet. Otherwise, simply
                // continue to enumeration - there is no one to change them.
                auto it = _shard_manager.find_ep_manager(de.name);
                if (it != _shard_manager.ep_managers_end()) {
                    return with_lock(it->second.file_update_mutex(), [this, dir = std::move(dir), ep_name = std::move(de.name)]() mutable {
                         return scan_one_ep_dir(dir / ep_name.c_str(), ep_key_type(ep_name));
                    });
                } else {
                    return scan_one_ep_dir(dir / de.name.c_str(), ep_key_type(de.name));
                }
            }).then([this] {
                // Adjust the quota to take into account the space we guarantee to every end point manager
                size_t adjusted_quota = 0;
                size_t delta = _shard_manager._ep_managers.size() * _hint_segment_size_in_mb * 1024 * 1024;
                if (max_shard_disk_space_size > delta) {
                    adjusted_quota = max_shard_disk_space_size - delta;
                }

                bool can_hint = _total_size < adjusted_quota;
                manager_logger.trace("space_watchdog: total_size ({}) {} max_shard_disk_space_size ({})", _total_size, can_hint ? "<" : ">=", adjusted_quota);

                if (!can_hint) {
                    manager_logger.trace("space_watchdog: Going to block hints to: {}", _eps_with_pending_hints);
                    std::for_each(_shard_manager._ep_managers.begin(), _shard_manager._ep_managers.end(), [this] (auto& pair) {
                        end_point_hints_manager& ep_man = pair.second;
                        auto it = _eps_with_pending_hints.find(ep_man.end_point_key());
                        if (it != _eps_with_pending_hints.end()) {
                            ep_man.forbid_hints();
                        } else {
                            ep_man.allow_hints();
                        }
                    });
                } else {
                    std::for_each(_shard_manager._ep_managers.begin(), _shard_manager._ep_managers.end(), [] (auto& pair) {
                        pair.second.allow_hints();
                    });
                }
            });
        }).handle_exception([this] (auto eptr) {
            manager_logger.trace("space_watchdog: unexpected exception - stop all hints generators");
            // Stop all hint generators if space_watchdog callback failed
            std::for_each(_shard_manager._ep_managers.begin(), _shard_manager._ep_managers.end(), [this] (auto& pair) {
                pair.second.forbid_hints();
            });
        }).finally([this] {
            _timer.arm(_watchdog_period);
        });
    });
}

bool manager::too_many_in_flight_hints_for(ep_key_type ep) const noexcept {
    // There is no need to check the DC here because if there is an in-flight hint for this end point then this means that
    // its DC has already been checked and found to be ok.
    return _stats.size_of_hints_in_progress > _max_size_of_hints_in_progress && !utils::fb_utilities::is_me(ep) && hints_in_progress_for(ep) > 0 && local_gossiper().get_endpoint_downtime(ep) <= _max_hint_window_us;
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
    if (_stats.size_of_hints_in_progress > _max_size_of_hints_in_progress && hints_in_progress_for(ep) > 0) {
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

bool manager::check_dc_for(ep_key_type ep) const noexcept {
    try {
        // If target's DC is not a "hintable" DCs - don't hint.
        // If there is an end point manager then DC has already been checked and found to be ok.
        return _hinted_dcs.empty() || have_ep_manager(ep) ||
               _hinted_dcs.find(_local_snitch_ptr->get_datacenter(ep)) != _hinted_dcs.end();
    } catch (...) {
        // if we failed to check the DC - block this hint
        return false;
    }
}

manager::end_point_hints_manager::sender::sender(end_point_hints_manager& parent, service::storage_proxy& local_storage_proxy, database& local_db, gms::gossiper& local_gossiper) noexcept
    : _stopped(make_ready_future<>())
    , _ep_key(parent.end_point_key())
    , _ep_manager(parent)
    , _shard_manager(_ep_manager._shard_manager)
    , _proxy(local_storage_proxy)
    , _db(local_db)
    , _gossiper(local_gossiper)
    , _file_update_mutex(_ep_manager.file_update_mutex())
{}

manager::end_point_hints_manager::sender::sender(const sender& other, end_point_hints_manager& parent) noexcept
    : _stopped(make_ready_future<>())
    , _ep_key(parent.end_point_key())
    , _ep_manager(parent)
    , _shard_manager(_ep_manager._shard_manager)
    , _proxy(other._proxy)
    , _db(other._db)
    , _gossiper(other._gossiper)
    , _file_update_mutex(_ep_manager.file_update_mutex())
{}


future<> manager::end_point_hints_manager::sender::stop() noexcept {
    set_stopping();
    return std::move(_stopped);
}

void manager::end_point_hints_manager::sender::add_segment(sstring seg_name) {
    _segments_to_replay.emplace_back(std::move(seg_name));
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
    _stopped = seastar::async([this] {
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
        manager_logger.trace("ep_manager({})::sender: exiting", end_point_key());
    });
}

future<> manager::end_point_hints_manager::sender::send_one_mutation(mutation m) {
    keyspace& ks = _db.find_keyspace(m.schema()->ks_name());
    auto& rs = ks.get_replication_strategy();
    std::vector<gms::inet_address> natural_endpoints = rs.get_natural_endpoints(m.token());

    return do_send_one_mutation(std::move(m), natural_endpoints);
}

future<> manager::end_point_hints_manager::sender::send_one_hint(lw_shared_ptr<send_one_file_ctx> ctx_ptr, temporary_buffer<char> buf, db::replay_position rp, gc_clock::duration secs_since_file_mod, const sstring& fname) {
    // Let's approximate the memory size the mutation is going to consume by the size of its serialized form
    size_t hint_memory_budget = std::max(_shard_manager._min_send_hint_budget, buf.size());
    // Allow a very big mutation to be sent out by consuming the whole shard budget
    hint_memory_budget = std::min(hint_memory_budget, _shard_manager._max_send_in_flight_memory);

    manager_logger.trace("memory budget: need {} have {}", hint_memory_budget, _shard_manager._send_limiter.available_units());

    return get_units(_shard_manager._send_limiter, hint_memory_budget).then([this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] (auto units) mutable {
        with_gate(ctx_ptr->file_send_gate, [this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] () mutable {
            try {
                try {
                    ctx_ptr->rps_set.emplace(rp);
                } catch (...) {
                    // if we failed to insert the rp into the set then its contents can't be trusted and we have to re-send the current file from the beginning
                    ctx_ptr->state.set(send_state::restart_segment);
                    ctx_ptr->state.set(send_state::segment_replay_failed);
                    return make_ready_future<>();
                }

                mutation m = this->get_mutation(ctx_ptr, buf);
                gc_clock::duration gc_grace_sec = m.schema()->gc_grace_seconds();

                // The hint is too old - drop it.
                //
                // Files are aggregated for at most manager::hints_timer_period therefore the oldest hint there is
                // (last_modification - manager::hints_timer_period) old.
                if (gc_clock::now().time_since_epoch() - secs_since_file_mod > gc_grace_sec - manager::hints_flush_period) {
                    return make_ready_future<>();
                }

                return this->send_one_mutation(std::move(m)).then([this, rp, ctx_ptr] {
                    ctx_ptr->rps_set.erase(rp);
                    ++this->shard_stats().sent;
                }).handle_exception([this, ctx_ptr] (auto eptr) {
                    manager_logger.trace("send_one_hint(): failed to send to {}: {}", end_point_key(), eptr);
                    ctx_ptr->state.set(send_state::segment_replay_failed);
                });

            // ignore these errors and move on - probably this hint is too old and the KS/CF has been deleted...
            } catch (no_such_column_family& e) {
                manager_logger.debug("send_hints(): no_such_column_family: {}", e.what());
            } catch (no_such_keyspace& e) {
                manager_logger.debug("send_hints(): no_such_keyspace: {}", e.what());
            } catch (no_column_mapping& e) {
                manager_logger.debug("send_hints(): {}: {}", fname, e.what());
            }
            return make_ready_future<>();
        }).finally([units = std::move(units), ctx_ptr] {});
    }).handle_exception([this, ctx_ptr] (auto eptr) {
        manager_logger.trace("send_one_file(): Hmmm. Something bad had happend: {}", eptr);
        ctx_ptr->state.set(send_state::segment_replay_failed);
    });
}

// runs in a seastar::async context
bool manager::end_point_hints_manager::sender::send_one_file(const sstring& fname) {
    timespec last_mod = get_last_file_modification(fname).get0();
    gc_clock::duration secs_since_file_mod = std::chrono::seconds(last_mod.tv_sec);
    lw_shared_ptr<send_one_file_ctx> ctx_ptr = make_lw_shared<send_one_file_ctx>();

    try {
        auto s = commitlog::read_log_file(fname, [this, secs_since_file_mod, &fname, ctx_ptr] (temporary_buffer<char> buf, db::replay_position rp) mutable {
            // Check that we can still send the next hint. Don't try to send it if the destination host
            // is DOWN or if we have already failed to send some of the previous hints.
            if (ctx_ptr->state.contains(send_state::segment_replay_failed)) {
                return make_ready_future<>();
            }

            // Break early if stop() was called or the destination node went down.
            if (!can_send()) {
                ctx_ptr->state.set(send_state::segment_replay_failed);
                return make_ready_future<>();
            }

            return flush_maybe().finally([this, ctx_ptr, buf = std::move(buf), rp, secs_since_file_mod, &fname] () mutable {
                return send_one_hint(std::move(ctx_ptr), std::move(buf), rp, secs_since_file_mod, fname);
            });
        }, _last_not_complete_rp.pos, &_db.get_config().extensions()).get0();

        s->done().get();
    } catch (...) {
        manager_logger.trace("sending of {} failed: {}", fname, std::current_exception());
        ctx_ptr->state.set(send_state::segment_replay_failed);
    }

    // wait till all background hints sending is complete
    ctx_ptr->file_send_gate.close().get();

    // update the next iteration replay position if needed
    if (ctx_ptr->state.contains(send_state::segment_replay_failed)) {
        if (ctx_ptr->state.contains(send_state::restart_segment)) {
            // if _rps_set contents is inconsistent simply re-start the current file from the beginning
            _last_not_complete_rp = replay_position();
        } else if (!ctx_ptr->rps_set.empty()) {
            _last_not_complete_rp = *std::min_element(ctx_ptr->rps_set.begin(), ctx_ptr->rps_set.end());
        }

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
    manager_logger.trace("send_one_file(): segment {} was sent in full and deleted", fname);
    return true;
}

// Runs in the seastar::async context
void manager::end_point_hints_manager::sender::send_hints_maybe() noexcept {
    using namespace std::literals::chrono_literals;
    manager_logger.trace("send_hints(): going to send hints to {}, we have {} segment to replay", end_point_key(), _segments_to_replay.size());

    int replayed_segments_count = 0;

    try {
        while (have_segments()) {
            if (!send_one_file(*_segments_to_replay.begin())) {
                break;
            }
            _segments_to_replay.pop_front();
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

}
}
