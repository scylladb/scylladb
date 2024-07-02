/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/view/view_update_backlog.hh"
#include <seastar/core/timed_out_error.hh>
#include "gms/inet_address.hh"
#include <seastar/util/defer.hh>
#include <boost/range/adaptor/map.hpp>
#include "replica/database.hh"
#include "view_update_generator.hh"
#include "utils/error_injection.hh"
#include "db/view/view_updating_consumer.hh"
#include "sstables/sstables.hh"
#include "sstables/progress_monitor.hh"
#include "readers/evictable.hh"
#include "dht/partition_filter.hh"
#include "utils/pretty_printers.hh"
#include "readers/from_mutations_v2.hh"
#include "service/storage_proxy.hh"

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

view_update_generator::view_update_generator(replica::database& db, sharded<service::storage_proxy>& proxy, abort_source& as)
        : _db(db)
        , _proxy(proxy)
        , _progress_tracker(std::make_unique<progress_tracker>())
        , _early_abort_subscription(as.subscribe([this] () noexcept { do_abort(); }))
{
    setup_metrics();
    discover_staging_sstables();
    _db.plug_view_update_generator(*this);
}

view_update_generator::~view_update_generator() {}

future<> view_update_generator::start() {
    _started = seastar::async([this]() mutable {
        auto drop_sstable_references = defer([&] () noexcept {
            // Clear sstable references so sstables_manager::stop() doesn't hang.
            vug_logger.info("leaving {} unstaged sstables unprocessed",
                    _sstables_to_move.size(), _sstables_with_tables.size());
            _sstables_to_move.clear();
            _sstables_with_tables.clear();
            _progress_tracker = {};
        });
        while (!_as.abort_requested()) {
            if (_sstables_with_tables.empty()) {
                _pending_sstables.wait().get();
            }

            // To ensure we don't race with updates, move the entire content
            // into a local variable.
            auto sstables_with_tables = std::exchange(_sstables_with_tables, {});

            // If we got here, we will process all tables we know about so far eventually so there
            // is no starvation
            for (auto table_it = sstables_with_tables.begin(); table_it != sstables_with_tables.end(); table_it = sstables_with_tables.erase(table_it)) {
                auto& [t, sstables] = *table_it;
                schema_ptr s = t->schema();

                const auto num_sstables = sstables.size();
                auto start_time = db_clock::now();
                uint64_t input_size = 0;

                try {
                    // Exploit the fact that sstables in the staging directory
                    // are usually non-overlapping and use a partitioned set for
                    // the read.
                    auto ssts = make_lw_shared<sstables::sstable_set>(sstables::make_partitioned_sstable_set(s, false));
                    for (auto& sst : sstables) {
                        ssts->insert(sst);
                        input_size += sst->data_size();
                    }

                    vug_logger.info("Processing {}.{}: {} in {} sstables",
                                    s->ks_name(), s->cf_name(), utils::pretty_printed_data_size(input_size), sstables.size());

                    auto permit = _db.obtain_reader_permit(*t, "view_update_generator", db::no_timeout, {}).get();
                    auto ms = mutation_source([this, ssts] (
                                schema_ptr s,
                                reader_permit permit,
                                const dht::partition_range& pr,
                                const query::partition_slice& ps,
                                tracing::trace_state_ptr ts,
                                streamed_mutation::forwarding fwd_ms,
                                mutation_reader::forwarding fwd_mr) {
                        return ssts->make_range_sstable_reader(s, std::move(permit), pr, ps, std::move(ts), fwd_ms, fwd_mr, *_progress_tracker);
                    });
                    auto [staging_sstable_reader, staging_sstable_reader_handle] = make_manually_paused_evictable_reader_v2(
                            std::move(ms),
                            s,
                            permit,
                            query::full_partition_range,
                            s->full_slice(),
                            nullptr,
                            ::mutation_reader::forwarding::no);
                    auto close_sr = deferred_close(staging_sstable_reader);

                    inject_failure("view_update_generator_consume_staging_sstable");
                    auto result = staging_sstable_reader.consume_in_thread(view_updating_consumer(*this, s, std::move(permit), *t, sstables, _as, staging_sstable_reader_handle));
                    if (result == stop_iteration::yes) {
                        break;
                    }
                } catch (...) {
                    vug_logger.warn("Processing {} failed for table {}:{}. Will retry...", s->ks_name(), s->cf_name(), std::current_exception());
                    // Need to add sstables back to the set so we can retry later. By now it may
                    // have had other updates.
                    std::move(sstables.begin(), sstables.end(), std::back_inserter(_sstables_with_tables[t]));
                    // Sleep a bit, to avoid a tight loop repeatedly spamming the log with the same message.
                    seastar::sleep(std::chrono::seconds(1)).get();
                    break;
                }
                try {
                    inject_failure("view_update_generator_collect_consumed_sstables");
                    _progress_tracker->on_sstables_deregistration(sstables);
                    // collect all staging sstables to move in a map, grouped by table.
                    std::move(sstables.begin(), sstables.end(), std::back_inserter(_sstables_to_move[t]));
                } catch (...) {
                    // Move from staging will be retried upon restart.
                    vug_logger.warn("Moving {} from staging failed: {}:{}. Ignoring...", s->ks_name(), s->cf_name(), std::current_exception());
                }
                _registration_sem.signal(num_sstables);

                auto end_time = db_clock::now();
                auto duration = std::chrono::duration<float>(end_time - start_time);
                vug_logger.info("Processed {}.{}: {} sstables in {}ms = {}", s->ks_name(), s->cf_name(), sstables.size(),
                                std::chrono::duration_cast<std::chrono::milliseconds>(duration).count(),
                                utils::pretty_printed_throughput(input_size, duration));
            }
            // For each table, move the processed staging sstables into the table's base dir.
            for (auto it = _sstables_to_move.begin(); it != _sstables_to_move.end(); ) {
                auto& [t, sstables] = *it;
                try {
                    inject_failure("view_update_generator_move_staging_sstable");
                    t->move_sstables_from_staging(sstables).get();
                } catch (...) {
                    // Move from staging will be retried upon restart.
                    vug_logger.warn("Moving some sstable from staging failed: {}. Ignoring...", std::current_exception());
                }
                it = _sstables_to_move.erase(it);
            }
        }
    });
    return make_ready_future<>();
}

// The .do_abort() just kicks the v.u.g. background fiber to wrap up and it
// normally happens when scylla stops upon SIGINT. Doing it that early is safe,
// once the fiber is kicked, no new work can be added to it, see _as check in
// register_staging_sstable().
//
// The .stop() really stops the sharded<v.u.g.> service by waiting for the fiber
// to stop using 'this' and thus releasing any resources owned by it. It also
// calls do_abort() to handle the case when subscription didn't shoot which, in
// turn, can happen when main() throws in the middle and doesn't request abort
// via the stop-signal.

void view_update_generator::do_abort() noexcept {
    if (_as.abort_requested()) {
        // The below code is re-entrable, but avoid it explicitly to be
        // on the safe side in case it suddenly stops being such
        return;
    }

    vug_logger.info("Terminating background fiber");
    _as.request_abort();
    _pending_sstables.signal();
}

future<> view_update_generator::drain() {
    return _proxy.local().abort_view_writes();
}

future<> view_update_generator::stop() {
    _db.unplug_view_update_generator();
    do_abort();
    return std::move(_started).then([this] {
        _registration_sem.broken();
    });
}

bool view_update_generator::should_throttle() const {
    return !_started.available();
}

future<> view_update_generator::register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<replica::table> table) {
    if (_as.abort_requested()) {
        return make_ready_future<>();
    }
    inject_failure("view_update_generator_registering_staging_sstable");
    _progress_tracker->on_sstable_registration(sst);
    _sstables_with_tables[table].push_back(std::move(sst));

    _pending_sstables.signal();
    if (should_throttle()) {
        return _registration_sem.wait(1);
    } else {
        _registration_sem.consume(1);
        return make_ready_future<>();
    }
}

void view_update_generator::setup_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group("view_update_generator", {
        sm::make_gauge("pending_registrations", sm::description("Number of tasks waiting to register staging sstables"),
                [this] { return _registration_sem.waiters(); }),

        sm::make_gauge("queued_batches_count", 
                sm::description("Number of sets of sstables queued for view update generation"),
                [this] { return _sstables_with_tables.size(); }),

        sm::make_gauge("sstables_to_move_count",
                sm::description("Number of sets of sstables which are already processed and wait to be moved from their staging directory"),
                [this] { return _sstables_to_move.size(); }),

        sm::make_gauge("sstables_pending_work",
                sm::description("Number of bytes remaining to be processed from SSTables for view updates"),
                [this] { return _progress_tracker ? _progress_tracker->sstables_pending_work() : 0; })
    });
}

void view_update_generator::discover_staging_sstables() {
    _db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> table) {
        auto t = table->shared_from_this();
        for (auto sstables = t->get_sstables(); sstables::shared_sstable sst : *sstables) {
            if (sst->requires_view_building()) {
                _progress_tracker->on_sstable_registration(sst);
                _sstables_with_tables[t].push_back(std::move(sst));
                // we're at early stage here, no need to kick _pending_sstables (the
                // building fiber is not running), neither we can wait on the semaphore
                _registration_sem.consume(1);
            }
        }
    });
}

static size_t memory_usage_of(const utils::chunked_vector<frozen_mutation_and_schema>& ms) {
    return boost::accumulate(ms | boost::adaptors::transformed([] (const frozen_mutation_and_schema& m) {
        return memory_usage_of(m);
    }), 0);
}

/**
 * Given some updates on the base table and assuming there are no pre-existing, overlapping updates,
 * generates the mutations to be applied to the base table's views, and sends them to the paired
 * view replicas. The future resolves when the updates have been acknowledged by the repicas, i.e.,
 * propagating the view updates to the view replicas happens synchronously.
 *
 * @param views the affected views which need to be updated.
 * @param base_token The token to use to match the base replica with the paired replicas.
 * @param reader the base table updates being applied, which all correspond to the base token.
 * @return a future that resolves when the updates have been acknowledged by the view replicas
 */
future<> view_update_generator::populate_views(const replica::table& table,
        std::vector<view_and_base> views,
        dht::token base_token,
        mutation_reader&& reader,
        gc_clock::time_point now) {
    auto schema = reader.schema();
    view_update_builder builder = make_view_update_builder(
            get_db().as_data_dictionary(),
            table,
            schema,
            std::move(views),
            std::move(reader),
            { },
            now);

    std::exception_ptr err;
    while (true) {
        try {
            auto updates = co_await builder.build_some();
            if (!updates) {
                break;
            }
            size_t update_size = memory_usage_of(*updates);
            size_t units_to_wait_for = std::min(table.get_config().view_update_concurrency_semaphore_limit, update_size);
            auto units = co_await seastar::get_units(_db.view_update_sem(), units_to_wait_for);
            units.adopt(seastar::consume_units(_db.view_update_sem(), update_size - units_to_wait_for));
            if (utils::get_local_injector().enter("view_building_failure")) {
                co_await seastar::sleep(std::chrono::seconds(1));
                err = std::make_exception_ptr(std::runtime_error("Timeout a view building update"));
                continue;
            }
            co_await mutate_MV(schema, base_token, std::move(*updates), table.view_stats(), *table.cf_stats(),
                    tracing::trace_state_ptr(), std::move(units), service::allow_hints::no, wait_for_all_updates::yes);
        } catch (...) {
            if (!err) {
                err = std::current_exception();
            }
        }
    }
    co_await builder.close();
    if (err) {
        std::rethrow_exception(err);
    }
}


// Generating view updates for a single client request can take a long time and might not finish before the timeout is
// reached. In such case this exception is thrown.
// "Generating a view update" means creating a view update and scheduling it to be sent later.
// This exception isn't thrown if the sending timeouts, it's only concrened with generating.
struct view_update_generation_timeout_exception : public seastar::timed_out_error {
    const char* what() const noexcept override {
        return "Request timed out - couldn't prepare materialized view updates in time";
    }
};

/**
 * Given some updates on the base table and the existing values for the rows affected by that update, generates the
 * mutations to be applied to the base table's views, and sends them to the paired view replicas.
 *
 * @param base the base schema at a particular version.
 * @param views the affected views which need to be updated.
 * @param updates the base table updates being applied.
 * @param existings the existing values for the rows affected by updates. This is used to decide if a view is
 * @param now the current time, used to calculate the deletion time for tombstones
 * @param timeout client request timeout
 * obsoleted by the update and should be removed, gather the values for columns that may not be part of the update if
 * a new view entry needs to be created, and compute the minimal updates to be applied if the view entry isn't changed
 * but has simply some updated values.
 * @return a future resolving to the mutations to apply to the views, which can be empty.
 */
future<> view_update_generator::generate_and_propagate_view_updates(const replica::table& table,
        const schema_ptr& base,
        reader_permit permit,
        std::vector<view_and_base>&& views,
        mutation&& m,
        mutation_reader_opt existings,
        tracing::trace_state_ptr tr_state,
        gc_clock::time_point now,
        db::timeout_clock::time_point timeout) {
    auto base_token = m.token();
    auto m_schema = m.schema();
    view_update_builder builder = make_view_update_builder(
            get_db().as_data_dictionary(),
            table,
            base,
            std::move(views),
            make_mutation_reader_from_mutations_v2(std::move(m_schema), std::move(permit), std::move(m)),
            std::move(existings),
            now);

    std::exception_ptr err = nullptr;
    for (size_t batch_num = 0; ; batch_num++) {
        std::optional<utils::chunked_vector<frozen_mutation_and_schema>> updates;
        try {
            updates = co_await builder.build_some();
        } catch (...) {
            err = std::current_exception();
            break;
        }
        if (!updates) {
            break;
        }
        tracing::trace(tr_state, "Generated {} view update mutations", updates->size());
        auto units = seastar::consume_units(_db.view_update_sem(), memory_usage_of(*updates));
        if (batch_num == 0 && _db.view_update_sem().current() == 0) {
            // We don't have resources to propagate view updates for this write. If we reached this point, we failed to
            // throttle the client. The memory queue is already full, waiting on the semaphore would block view updates
            // that we've already started applying, and generating hints would ultimately result in the disk queue being
            // full. Instead, we drop the base write, which will create inconsistencies between base replicas, but we
            // will fix them using repair.
            err = std::make_exception_ptr(exceptions::overloaded_exception("Too many view updates started concurrently"));
            break;
        }
        // To prevent overload we sleep for a moment before sending another batch of view updates.
        // The amount of time to sleep for is chosen based on how full the view update backlog is,
        // the more full the queue of pending view updates is the more aggressively we should delay
        // new ones.
        // The first batch of updates doesn't have any delays because it's slowed down by the other throttling mechanism,
        // the one which limits the number of incoming client requests by delaying the response to the client.
        if (batch_num > 0) {
            update_backlog local_backlog = _db.get_view_update_backlog();
            std::chrono::microseconds throttle_delay =  calculate_view_update_throttling_delay(local_backlog, timeout);

            co_await seastar::sleep(throttle_delay);

            if (utils::get_local_injector().enter("view_update_limit") && _db.view_update_sem().current() == 0) {
                err = std::make_exception_ptr(std::runtime_error("View update backlog exceeded the limit"));
                break;
            }

            if (db::timeout_clock::now() > timeout) {
                err = std::make_exception_ptr(view_update_generation_timeout_exception());
                break;
            }
        }

        try {
            co_await mutate_MV(base, base_token, std::move(*updates), table.view_stats(), *table.cf_stats(), tr_state,
                std::move(units), service::allow_hints::yes, wait_for_all_updates::no);
        } catch (...) {
            // Ignore exceptions: any individual failure to propagate a view update will be reported
            // by a separate mechanism in mutate_MV() function. Moreover, we should continue trying
            // to generate updates even if some of them fail, in order to minimize the potential
            // inconsistencies caused by not being able to propagate an update
        }
    }
    co_await builder.close();
    _proxy.local().update_view_update_backlog();
    if (err) {
        std::rethrow_exception(err);
    }
}

}
