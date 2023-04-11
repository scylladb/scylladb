/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/util/defer.hh>
#include <boost/range/adaptor/map.hpp>
#include "replica/database.hh"
#include "view_update_generator.hh"
#include "service/priority_manager.hh"
#include "utils/error_injection.hh"
#include "db/view/view_updating_consumer.hh"
#include "sstables/sstables.hh"
#include "sstables/progress_monitor.hh"
#include "readers/evictable.hh"
#include "dht/partition_filter.hh"

static logging::logger vug_logger("view_update_generator");

static inline void inject_failure(std::string_view operation) {
    utils::get_local_injector().inject(operation,
            [operation] { throw std::runtime_error(std::string(operation)); });
}

namespace db::view {

class view_update_generator::progress_tracker final : public sstables::read_monitor_generator {
    class read_monitor final : public  sstables::read_monitor {
        sstables::shared_sstable _sst;
        const sstables::reader_position_tracker* _tracker = nullptr;
        uint64_t _last_position_seen = 0;
    public:
        virtual void on_read_started(const sstables::reader_position_tracker& tracker) override {
            _tracker = &tracker;
        }

        virtual void on_read_completed() override {
            if (auto tracker = std::exchange(_tracker, nullptr)) {
                _last_position_seen = tracker->position;
            }
        }

        uint64_t pending_work() const noexcept {
            auto last_pos = (_tracker) ? _tracker->position : _last_position_seen;
            return _sst->data_size() - last_pos;
        }

        read_monitor& operator=(const read_monitor&) = delete;
        read_monitor(const read_monitor&) = delete;
        read_monitor& operator=(const read_monitor&&) = delete;
        read_monitor(read_monitor&&) = delete;

        explicit read_monitor(sstables::shared_sstable sst)
            : _sst(std::move(sst)) {
        }
    };
private:
    // Tracks SSTables that were registered in view_update_generator, but aren't being processed yet.
    uint64_t _inactive_pending_work = 0;
    // Tracks SSTables that are now being processed by view_update_generator's async loop
    // using unordered_map to provide a stable address for read_monitor, so operator() can safely return a reference.
    std::unordered_map<sstables::shared_sstable, read_monitor> _monitors;
public:
    virtual sstables::read_monitor& operator()(sstables::shared_sstable sst) override {
        auto p = _monitors.try_emplace(sst, sst);
        _inactive_pending_work -= sst->data_size();
        return p.first->second;
    }

    void on_sstable_registration(const sstables::shared_sstable& sst) {
        _inactive_pending_work += sst->data_size();
    }

    void on_sstables_deregistration(const std::vector<sstables::shared_sstable>& ssts) {
        for (auto& sst : ssts) {
            if (_monitors.contains(sst)) {
                _monitors.erase(sst);
            } else {
                _inactive_pending_work -= sst->data_size();
            }
        }
    }

    uint64_t sstables_pending_work() const noexcept {
        return _inactive_pending_work +
            boost::accumulate(_monitors | boost::adaptors::map_values | boost::adaptors::transformed(std::mem_fn(&read_monitor::pending_work)), uint64_t(0));
    }
};

view_update_generator::view_update_generator(replica::database& db, sharded<service::storage_proxy>& proxy)
        : _db(db)
        , _proxy(proxy)
        , _progress_tracker(std::make_unique<progress_tracker>()) {
    setup_metrics();
    _db.plug_view_update_generator(*this);
}

view_update_generator::~view_update_generator() {}

future<> view_update_generator::start() {
    thread_attributes attr;
    attr.sched_group = _db.get_streaming_scheduling_group();
    _started = seastar::async(std::move(attr), [this]() mutable {
        auto drop_sstable_references = defer([&] () noexcept {
            _progress_tracker = {};
        });
        while (!_as.abort_requested()) {
            auto sstables_with_tables = get_sstables_to_process();
            std::unordered_map<lw_shared_ptr<replica::table>, std::vector<sstables::shared_sstable>> sstables_to_move;
            if (sstables_with_tables.empty()) {
                _pending_sstables.wait().get();
                continue;
            }
            auto uuid = utils::make_random_uuid();
            // If we got here, we will process all tables we know about so far eventually so there
            // is no starvation
            for (auto table_it = sstables_with_tables.begin(); table_it != sstables_with_tables.end(); table_it = sstables_with_tables.erase(table_it)) {
                auto& [t, sstables] = *table_it;
                schema_ptr s = t->schema();

                for (auto& sst : sstables) {
                    _progress_tracker->on_sstable_registration(sst);
                }
                auto start_time = lowres_clock::now();
                vug_logger.info("[{}] Started processing {}.{}: {} sstables", uuid, s->ks_name(), s->cf_name(), sstables.size());

                try {
                    // Exploit the fact that sstables in the staging directory
                    // are usually non-overlapping and use a partitioned set for
                    // the read.
                    auto ssts = make_lw_shared<sstables::sstable_set>(sstables::make_partitioned_sstable_set(s, false));
                    for (auto& sst : sstables) {
                        ssts->insert(sst);
                    }

                    std::optional<reader_permit> permit;
                    while (!_as.abort_requested()) {
                        try {
                            auto timeout = db::timeout_clock::now() + std::chrono::minutes(5);
                            permit = _db.obtain_reader_permit(*t, "view_update_generator", timeout, {}).get0();
                            break;
                        } catch (...) {
                            vug_logger.warn("[{}] Processing {}.{}: failed to get reader permit {}, try again...", uuid, s->ks_name(), s->cf_name(), std::current_exception());
                        }
                    }

                    auto ms = mutation_source([this, ssts] (
                                schema_ptr s,
                                reader_permit permit,
                                const dht::partition_range& pr,
                                const query::partition_slice& ps,
                                const io_priority_class& pc,
                                tracing::trace_state_ptr ts,
                                streamed_mutation::forwarding fwd_ms,
                                mutation_reader::forwarding fwd_mr) {
                        return ssts->make_range_sstable_reader(s, std::move(permit), pr, ps, pc, std::move(ts), fwd_ms, fwd_mr, *_progress_tracker);
                    });
                    auto [staging_sstable_reader, staging_sstable_reader_handle] = make_manually_paused_evictable_reader_v2(
                            std::move(ms),
                            s,
                            *permit,
                            query::full_partition_range,
                            s->full_slice(),
                            service::get_local_streaming_priority(),
                            nullptr,
                            ::mutation_reader::forwarding::no);
                    auto close_sr = deferred_close(staging_sstable_reader);

                    inject_failure("view_update_generator_consume_staging_sstable");
                    auto result = staging_sstable_reader.consume_in_thread(view_updating_consumer(*this, s, std::move(*permit), *t, sstables, _as, staging_sstable_reader_handle),
                        dht::incremental_owned_ranges_checker::make_partition_filter(_db.get_keyspace_local_ranges(s->ks_name())));
                    if (result == stop_iteration::yes) {
                        break;
                    }
                } catch (...) {
                    vug_logger.warn("[{}] Processing {} failed for table {}:{}. Will retry...", uuid, s->ks_name(), s->cf_name(), std::current_exception());
                    break;
                }
                try {
                    inject_failure("view_update_generator_collect_consumed_sstables");
                    _progress_tracker->on_sstables_deregistration(sstables);
                    // collect all staging sstables to move in a map, grouped by table.
                    std::move(sstables.begin(), sstables.end(), std::back_inserter(sstables_to_move[t]));
                } catch (...) {
                    // Move from staging will be retried upon restart.
                    vug_logger.warn("Moving {} from staging failed: {}:{}. Ignoring...", s->ks_name(), s->cf_name(), std::current_exception());
                }
                auto duration = std::chrono::duration_cast<std::chrono::duration<float>>(lowres_clock::now() - start_time).count();
                vug_logger.info("[{}] Finished processing {}.{}: {} sstables, duration={}s", uuid, s->ks_name(), s->cf_name(), sstables.size(), duration);
            }
            // For each table, move the processed staging sstables into the table's base dir.
            for (auto it = sstables_to_move.begin(); it != sstables_to_move.end(); ) {
                auto& [t, sstables] = *it;
                try {
                    inject_failure("view_update_generator_move_staging_sstable");
                    t->move_sstables_from_staging(sstables).get();
                } catch (...) {
                    // Move from staging will be retried upon restart.
                    vug_logger.warn("Moving some sstable from staging failed: {}. Ignoring...", std::current_exception());
                }
                it = sstables_to_move.erase(it);
            }
        }
    });
    return make_ready_future<>();
}

future<> view_update_generator::stop() {
    _db.unplug_view_update_generator();
    _as.request_abort();
    _pending_sstables.signal();
    return std::move(_started);
}

bool view_update_generator::should_throttle() const {
    return !_started.available();
}

future<> view_update_generator::register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<replica::table> table) {
    if (_as.abort_requested()) {
        return make_ready_future<>();
    }
    inject_failure("view_update_generator_registering_staging_sstable");
    _pending_sstables.signal();

    return make_ready_future<>();
}

void view_update_generator::setup_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group("view_update_generator", {
        sm::make_gauge("pending_registrations", sm::description("Number of tasks waiting to register staging sstables"),
                [] { return 0; }),

        sm::make_gauge("queued_batches_count", 
                // TODO: return correct count
                sm::description("Number of sets of sstables queued for view update generation"),
                [] { return 0; }),

        sm::make_gauge("sstables_to_move_count",
                sm::description("Number of sets of sstables which are already processed and wait to be moved from their staging directory"),
                // TODO: return correct count
                [] { return 0; }),

        sm::make_gauge("sstables_pending_work",
                sm::description("Number of bytes remaining to be processed from SSTables for view updates"),
                [this] { return _progress_tracker ? _progress_tracker->sstables_pending_work() : 0; })
    });
}

std::unordered_map<lw_shared_ptr<replica::table>, std::vector<sstables::shared_sstable>>
view_update_generator::get_sstables_to_process() {
    size_t nr = 0;
    static constexpr size_t max_sst_per_batch = 32;
    std::unordered_map<lw_shared_ptr<replica::table>, std::vector<sstables::shared_sstable>> sstables;
    for (auto& x : _db.get_column_families()) {
        auto t = x.second->shared_from_this();
        for (auto ssts = t->get_sstables(); sstables::shared_sstable sst : *ssts) {
            if (sst->requires_view_building()) {
                sstables[t].push_back(std::move(sst));
                ++nr;
                if (nr >= max_sst_per_batch) {
                    return sstables;
                }
            }
        }
    }
    return sstables;
}

}
