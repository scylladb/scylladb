/*
 * Modified by ScyllaDB
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/sleep.hh>
#include "load_meter.hh"
#include "load_broadcaster.hh"
#include "cache_hitrate_calculator.hh"
#include "db/system_keyspace.hh"
#include "gms/application_state.hh"
#include "service/storage_proxy.hh"
#include "service/view_update_backlog_broker.hh"
#include "replica/database.hh"
#include "locator/abstract_replication_strategy.hh"

#include <cstdlib>

namespace service {

constexpr std::chrono::milliseconds load_broadcaster::BROADCAST_INTERVAL;

logging::logger llogger("load_broadcaster");

future<> load_meter::init(distributed<replica::database>& db, gms::gossiper& gms) {
    _lb = make_shared<load_broadcaster>(db, gms);
    _lb->start_broadcasting();
    return make_ready_future<>();
}

future<> load_meter::exit() {
    return _lb->stop_broadcasting();
}

future<std::map<sstring, double>> load_meter::get_load_map() {
    return smp::submit_to(0, [this] () {
        std::map<sstring, double> load_map;
        if (_lb) {
            for (auto& x : _lb->get_load_info()) {
                load_map.emplace(format("{}", x.first), x.second);
                llogger.debug("get_load_map endpoint={}, load={}", x.first, x.second);
            }
            load_map.emplace(format("{}",
                    _lb->gossiper().get_broadcast_address()), get_load());
        } else {
            llogger.debug("load_broadcaster is not set yet!");
        }
        return load_map;
    });
}

double load_meter::get_load() const {
    double bytes = 0;
#if 0
    for (String keyspaceName : Schema.instance.getKeyspaces())
    {
        Keyspace keyspace = Schema.instance.getKeyspaceInstance(keyspaceName);
        if (keyspace == null)
            continue;
        for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            bytes += cfs.getLiveDiskSpaceUsed();
    }
#endif
    return bytes;
}

void load_broadcaster::start_broadcasting() {
    _done = make_ready_future<>();

    // send the first broadcast "right away" (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
    // after that send every BROADCAST_INTERVAL.

    _timer.set_callback([this] {
        llogger.debug("Disseminating load info ...");
        _done = _db.map_reduce0([](replica::database& db) {
            int64_t res = 0;
            db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> table) {
                res += table->get_stats().live_disk_space_used;
            });
            return res;
        }, int64_t(0), std::plus<int64_t>()).then([this] (int64_t size) {
            return _gossiper.add_local_application_state(gms::application_state::LOAD,
                gms::versioned_value::load(size)).then([this] {
                _timer.arm(BROADCAST_INTERVAL);
                return make_ready_future<>();
            });
        });
    });

    _timer.arm(2 * gms::gossiper::INTERVAL);
}

future<> load_broadcaster::stop_broadcasting() {
    _timer.cancel();
    return _gossiper.unregister_(shared_from_this()).then([this] {
        return std::move(_done);
    }).then([this] {
        _stopped = true;
    });
}


// cache_hitrate_calculator implementation
cache_hitrate_calculator::cache_hitrate_calculator(seastar::sharded<replica::database>& db, gms::gossiper& g)
        : _db(db), _gossiper(g),
        _timer(std::bind(std::mem_fn(&cache_hitrate_calculator::recalculate_timer), this))
{}

void cache_hitrate_calculator::recalculate_timer() {
    _done = recalculate_hitrates().then_wrapped([p = shared_from_this()] (future<lowres_clock::duration> f) {
        lowres_clock::duration d;
        if (f.failed()) {
            d = std::chrono::milliseconds(2000);
        } else {
            d = f.get();
        }
        p->run_on((this_shard_id() + 1) % smp::count, d);
    });
}

void cache_hitrate_calculator::run_on(size_t master, lowres_clock::duration d) {
    if (!_stopped) {
        // Do it in the background.
        (void)container().invoke_on(master, [d] (cache_hitrate_calculator& local) {
            local._timer.arm(d);
        }).handle_exception_type([] (seastar::no_sharded_instance_exception&) { /* ignore */ });
    }
}

future<lowres_clock::duration> cache_hitrate_calculator::recalculate_hitrates() {
    auto non_system_filter = [&] (const std::pair<table_id, lw_shared_ptr<replica::column_family>>& cf) {
        return _db.local().find_keyspace(cf.second->schema()->ks_name()).get_replication_strategy().get_type() != locator::replication_strategy_type::local;
    };

    auto cf_to_cache_hit_stats = [non_system_filter] (replica::database& db) {
        return boost::copy_range<std::unordered_map<table_id, stat>>(db.get_tables_metadata().filter(non_system_filter) |
                boost::adaptors::transformed([]  (const std::pair<table_id, lw_shared_ptr<replica::column_family>>& cf) {
            auto& stats = cf.second->get_row_cache().stats();
            return std::make_pair(cf.first, stat{float(stats.reads_with_no_misses.rate().rates[0]), float(stats.reads_with_misses.rate().rates[0])});
        }));
    };

    auto sum_stats_per_cf = [] (std::unordered_map<table_id, stat> a, std::unordered_map<table_id, stat> b) {
        for (auto& r : b) {
            a[r.first] += r.second;
        }
        return a;
    };

    return _db.map_reduce0(cf_to_cache_hit_stats, std::unordered_map<table_id, stat>(), sum_stats_per_cf).then([this] (std::unordered_map<table_id, stat> rates) mutable {
        _diff = 0;
        _gstate.reserve(_slen); // assume length did not change from previous iteration
        _slen = 0;
        _rates = std::move(rates);
        // set calculated rates on all shards
        return _db.invoke_on_all([this, cpuid = this_shard_id()] (replica::database& db) {
            return do_for_each(_rates, [this, cpuid, &db] (auto&& r) mutable {
                auto cf_opt = db.get_tables_metadata().get_table_if_exists(r.first);
                if (!cf_opt) { // a table may be added before map/reduce completes and this code runs
                    return;
                }
                auto& cf = cf_opt;
                stat& s = r.second;
                float rate = 0;
                if (s.h) {
                    rate = s.h / (s.h + s.m);
                }
                if (this_shard_id() == cpuid) {
                    // calculate max difference between old rate and new one for all cfs
                    _diff = std::max(_diff, std::abs(float(cf->get_global_cache_hit_rate()) - rate));
                    _gstate += format("{}.{}:{:0.6f};", cf->schema()->ks_name(), cf->schema()->cf_name(), rate);
                }
                cf->set_global_cache_hit_rate(cache_temperature(rate));
            });
        });
    }).then([this] {
        _slen = _gstate.size();
        using namespace std::chrono_literals;
        auto now = lowres_clock::now();
        // Publish CACHE_HITRATES in case:
        //
        // - We haven't published it at all
        // - The diff is bigger than 1% and we haven't published in the last 5 seconds
        // - The diff is really big 10%
        //
        // Note: A peer node can know the cache hitrate through read_data
        // read_mutation_data and read_digest RPC verbs which have
        // cache_temperature in the response. So there is no need to update
        // CACHE_HITRATES through gossip in high frequency.
        bool do_publish = (_published_nr == 0) ||
                          (_diff > 0.1) ||
                          ( _diff > 0.01 && (now - _published_time) > 5000ms);

        // We do the recalculation faster if the diff is bigger than 0.01. It
        // is useful to do the calculation even if we do not publish the
        // CACHE_HITRATES though gossip, since the recalculation will call the
        // table->set_global_cache_hit_rate to set the hitrate.
        auto recalculate_duration = _diff > 0.01 ? lowres_clock::duration(500ms) : lowres_clock::duration(2000ms);
        if (do_publish) {
            llogger.debug("Send CACHE_HITRATES update max_diff={}, published_nr={}", _diff, _published_nr);
            ++_published_nr;
            _published_time = now;
            return container().invoke_on(0, [&gstate = _gstate] (cache_hitrate_calculator& self) {
                return self._gossiper.add_local_application_state(gms::application_state::CACHE_HITRATES,
                        gms::versioned_value::cache_hitrates(gstate));
            }).then([recalculate_duration] {
                return recalculate_duration;
            });
        } else {
            llogger.debug("Skip CACHE_HITRATES update max_diff={}, published_nr={}", _diff, _published_nr);
            return make_ready_future<lowres_clock::duration>(recalculate_duration);
        }
    }).finally([this] {
        _gstate = std::string(); // free memory, do not trust clear() to do that for string
        _rates.clear();
    });
}

future<> cache_hitrate_calculator::stop() {
    _timer.cancel();
    _stopped = true;
    return std::move(_done);
}


view_update_backlog_broker::view_update_backlog_broker(
        seastar::sharded<service::storage_proxy>& sp,
        gms::gossiper& gossiper)
        : _sp(sp)
        , _gossiper(gossiper) {
}

future<> view_update_backlog_broker::start() {
    _gossiper.register_(shared_from_this());
    if (this_shard_id() == 0) {
        // Gossiper runs only on shard 0, and there's no API to add multiple, per-shard application states.
        // Also, right now we aggregate all backlogs, since the coordinator doesn't keep per-replica shard backlogs.
        _started = seastar::async([this] {
            while (!_as.abort_requested()) {
                auto backlog = _sp.local().get_view_update_backlog_if_changed().get();
                if (!backlog) {
                    sleep_abortable(gms::gossiper::INTERVAL, _as).get();
                    continue;
                }
                auto now = api::timestamp_type(std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count());
                //FIXME: discarded future.
                (void)_gossiper.add_local_application_state(
                        gms::application_state::VIEW_BACKLOG,
                        gms::versioned_value(seastar::format("{}:{}:{}", backlog->get_current_bytes(), backlog->get_max_bytes(), now)));
                sleep_abortable(gms::gossiper::INTERVAL, _as).get();
            }
        }).handle_exception_type([] (const seastar::sleep_aborted& ignored) { });
    }
    return make_ready_future<>();
}

future<> view_update_backlog_broker::stop() {
    return _gossiper.unregister_(shared_from_this()).then([this] {
        _as.request_abort();
        return std::move(_started);
    });
}

future<> view_update_backlog_broker::on_change(gms::inet_address endpoint, const gms::application_state_map& states, gms::permit_id pid) {
    return on_application_state_change(endpoint, states, gms::application_state::VIEW_BACKLOG, pid, [this] (gms::inet_address endpoint, const gms::versioned_value& value, gms::permit_id) {
        size_t current;
        size_t max;
        api::timestamp_type ticks;
        const char* start_bound = value.value().data();
        char* end_bound;
        for (auto* ptr : {&current, &max}) {
            errno = 0;
            *ptr = std::strtoull(start_bound, &end_bound, 10);
            if (errno == ERANGE) {
                return make_ready_future();
            }
            start_bound = end_bound + 1;
        }
        if (max == 0) {
            return make_ready_future();
        }
        errno = 0;
        ticks = std::strtoll(start_bound, &end_bound, 10);
        if (ticks == 0 || errno == ERANGE || end_bound != value.value().data() + value.value().size()) {
            return make_ready_future();
        }
        auto backlog = view_update_backlog_timestamped{db::view::update_backlog{current, max}, ticks};
        return _sp.invoke_on_all([endpoint, backlog] (service::storage_proxy& sp) {
            auto[it, inserted] = sp._view_update_backlogs.try_emplace(endpoint, backlog);
            if (!inserted && it->second.ts < backlog.ts) {
                it->second = backlog;
            }
            return make_ready_future();
        });
    });
}

future<> view_update_backlog_broker::on_remove(gms::inet_address endpoint, gms::permit_id) {
    _sp.local()._view_update_backlogs.erase(endpoint);
    return make_ready_future();
}

}
