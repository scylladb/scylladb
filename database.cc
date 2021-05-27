/*
 * Copyright (C) 2014 ScyllaDB
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

#include "log.hh"
#include "lister.hh"
#include "database.hh"
#include <seastar/core/future-util.hh>
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "to_string.hh"
#include "cql3/functions/functions.hh"
#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/classification.hpp>
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/compaction.hh"
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <boost/container/static_vector.hpp>
#include "frozen_mutation.hh"
#include <seastar/core/do_with.hh>
#include "service/migration_manager.hh"
#include "message/messaging_service.hh"
#include "cell_locking.hh"
#include "view_info.hh"
#include "db/schema_tables.hh"
#include "sstables/compaction_manager.hh"
#include "gms/feature_service.hh"

#include "utils/human_readable.hh"
#include "utils/fb_utilities.hh"

#include "db/timeout_clock.hh"
#include "db/large_data_handler.hh"
#include "db/data_listeners.hh"

#include "user_types_metadata.hh"
#include <seastar/core/shared_ptr_incomplete.hh>
#include <seastar/util/memory_diagnostics.hh>

#include "locator/abstract_replication_strategy.hh"

using namespace std::chrono_literals;
using namespace db;

logging::logger dblog("database");

// Used for tests where the CF exists without a database object. We need to pass a valid
// dirty_memory manager in that case.
thread_local dirty_memory_manager default_dirty_memory_manager;

inline
flush_controller
make_flush_controller(const db::config& cfg, seastar::scheduling_group sg, const ::io_priority_class& iop, std::function<double()> fn) {
    if (cfg.memtable_flush_static_shares() > 0) {
        return flush_controller(sg, iop, cfg.memtable_flush_static_shares());
    }
    return flush_controller(sg, iop, 50ms, cfg.virtual_dirty_soft_limit(), std::move(fn));
}

inline
std::unique_ptr<compaction_manager>
make_compaction_manager(const db::config& cfg, database_config& dbcfg, abort_source& as) {
    if (cfg.compaction_static_shares() > 0) {
        return std::make_unique<compaction_manager>(dbcfg.compaction_scheduling_group, service::get_local_compaction_priority(), dbcfg.available_memory, cfg.compaction_static_shares(), as);
    }
    return std::make_unique<compaction_manager>(dbcfg.compaction_scheduling_group, service::get_local_compaction_priority(), dbcfg.available_memory, as);
}

lw_shared_ptr<keyspace_metadata>
keyspace_metadata::new_keyspace(std::string_view name,
                                std::string_view strategy_name,
                                std::map<sstring, sstring> options,
                                bool durables_writes,
                                std::vector<schema_ptr> cf_defs)
{
    return ::make_lw_shared<keyspace_metadata>(name, strategy_name, options, durables_writes, cf_defs);
}

void keyspace_metadata::add_user_type(const user_type ut) {
    _user_types.add_type(ut);
}

void keyspace_metadata::remove_user_type(const user_type ut) {
    _user_types.remove_type(ut);
}

keyspace::keyspace(lw_shared_ptr<keyspace_metadata> metadata, config cfg)
    : _metadata(std::move(metadata))
    , _config(std::move(cfg))
{}

lw_shared_ptr<keyspace_metadata> keyspace::metadata() const {
    return _metadata;
}

void keyspace::add_or_update_column_family(const schema_ptr& s) {
    _metadata->add_or_update_column_family(s);
}

void keyspace::add_user_type(const user_type ut) {
    _metadata->add_user_type(ut);
}

void keyspace::remove_user_type(const user_type ut) {
    _metadata->remove_user_type(ut);
}

bool string_pair_eq::operator()(spair lhs, spair rhs) const {
    return lhs == rhs;
}

utils::UUID database::empty_version = utils::UUID_gen::get_name_UUID(bytes{});

namespace {

class memory_diagnostics_line_writer {
    std::array<char, 4096> _line_buf;
    memory::memory_diagnostics_writer _wr;

public:
    memory_diagnostics_line_writer(memory::memory_diagnostics_writer wr) : _wr(std::move(wr)) { }
    void operator() (const char* fmt) {
        _wr(fmt);
    }
    void operator() (const char* fmt, const auto& param1, const auto&... params) {
        const auto begin = _line_buf.begin();
        auto it = fmt::format_to(begin, fmt, param1, params...);
        _wr(std::string_view(begin, it - begin));
    }
};

const boost::container::static_vector<std::pair<size_t, boost::container::static_vector<table*, 16>>, 10>
phased_barrier_top_10_counts(const std::unordered_map<utils::UUID, lw_shared_ptr<column_family>>& tables, std::function<size_t(table&)> op_count_getter) {
    using table_list = boost::container::static_vector<table*, 16>;
    using count_and_tables = std::pair<size_t, table_list>;
    const auto less = [] (const count_and_tables& a, const count_and_tables& b) {
        return a.first < b.first;
    };

    boost::container::static_vector<count_and_tables, 10> res;
    count_and_tables* min_element = nullptr;

    for (const auto& [tid, table] : tables) {
        const auto count = op_count_getter(*table);
        if (!count) {
            continue;
        }
        if (res.size() < res.capacity()) {
            auto& elem = res.emplace_back(count, table_list({table.get()}));
            if (!min_element || min_element->first > count) {
                min_element = &elem;
            }
            continue;
        }
        if (min_element->first > count) {
            continue;
        }

        auto it = boost::find_if(res, [count] (const count_and_tables& x) {
            return x.first == count;
        });
        if (it != res.end()) {
            it->second.push_back(table.get());
            continue;
        }

        // If we are here, min_element->first < count
        *min_element = {count, table_list({table.get()})};
        min_element = &*boost::min_element(res, less);
    }

    boost::sort(res, less);

    return res;
}

} // anonymous namespace

void database::setup_scylla_memory_diagnostics_producer() {
    memory::set_additional_diagnostics_producer([this] (memory::memory_diagnostics_writer wr) {
        auto writeln = memory_diagnostics_line_writer(std::move(wr));

        const auto lsa_occupancy_stats = logalloc::lsa_global_occupancy_stats();
        writeln("LSA\n");
        writeln("  allocated: {}\n", utils::to_hr_size(lsa_occupancy_stats.total_space()));
        writeln("  used:      {}\n", utils::to_hr_size(lsa_occupancy_stats.used_space()));
        writeln("  free:      {}\n\n", utils::to_hr_size(lsa_occupancy_stats.free_space()));

        const auto row_cache_occupancy_stats = _row_cache_tracker.region().occupancy();
        writeln("Cache:\n");
        writeln("  total: {}\n", utils::to_hr_size(row_cache_occupancy_stats.total_space()));
        writeln("  used:  {}\n", utils::to_hr_size(row_cache_occupancy_stats.used_space()));
        writeln("  free:  {}\n\n", utils::to_hr_size(row_cache_occupancy_stats.free_space()));

        writeln("Memtables:\n");
        writeln(" total: {}\n", utils::to_hr_size(lsa_occupancy_stats.total_space() - row_cache_occupancy_stats.total_space()));

        writeln(" Regular:\n");
        writeln("  real dirty: {}\n", utils::to_hr_size(_dirty_memory_manager.real_dirty_memory()));
        writeln("  virt dirty: {}\n", utils::to_hr_size(_dirty_memory_manager.virtual_dirty_memory()));
        writeln(" System:\n");
        writeln("  real dirty: {}\n", utils::to_hr_size(_system_dirty_memory_manager.real_dirty_memory()));
        writeln("  virt dirty: {}\n\n", utils::to_hr_size(_system_dirty_memory_manager.virtual_dirty_memory()));

        writeln("Replica:\n");

        writeln("  Read Concurrency Semaphores:\n");
        const std::pair<const char*, reader_concurrency_semaphore&> semaphores[] = {
                {"user", _read_concurrency_sem},
                {"streaming", _streaming_concurrency_sem},
                {"system", _system_read_concurrency_sem},
                {"compaction", _compaction_concurrency_sem},
        };
        for (const auto& [name, sem] : semaphores) {
            const auto initial_res = sem.initial_resources();
            const auto available_res = sem.available_resources();
            if (sem.is_unlimited()) {
                writeln("    {}: {}/∞, {}/∞\n",
                        name,
                        initial_res.count - available_res.count,
                        utils::to_hr_size(initial_res.memory - available_res.memory),
                        sem.waiters());
            } else {
                writeln("    {}: {}/{}, {}/{}, queued: {}\n",
                        name,
                        initial_res.count - available_res.count,
                        initial_res.count,
                        utils::to_hr_size(initial_res.memory - available_res.memory),
                        utils::to_hr_size(initial_res.memory),
                        sem.waiters());
            }
        }

        writeln("  Execution Stages:\n");
        const std::pair<const char*, inheriting_execution_stage::stats> execution_stage_summaries[] = {
                {"data query stage", _data_query_stage.get_stats()},
                {"mutation query stage", _mutation_query_stage.get_stats()},
                {"apply stage", _apply_stage.get_stats()},
        };
        for (const auto& [name, exec_stage_summary] : execution_stage_summaries) {
            writeln("    {}:\n", name);
            size_t total = 0;
            for (const auto& [sg, stats ] : exec_stage_summary) {
                const auto count = stats.function_calls_enqueued - stats.function_calls_executed;
                if (!count) {
                    continue;
                }
                writeln("      {}\t{}\n", sg.name(), count);
                total += count;
            }
            writeln("         Total: {}\n", total);
        }

        writeln("  Tables - Ongoing Operations:\n");
        const std::pair<const char*, std::function<size_t(table&)>> phased_barriers[] = {
                {"Pending writes", std::mem_fn(&table::writes_in_progress)},
                {"Pending reads", std::mem_fn(&table::reads_in_progress)},
                {"Pending streams", std::mem_fn(&table::streams_in_progress)},
        };
        for (const auto& [name, op_count_getter] : phased_barriers) {
            writeln("    {} (top 10):\n", name);
            auto total = 0;
            for (const auto& [count, table_list] : phased_barrier_top_10_counts(_column_families, op_count_getter)) {
                total += count;
                writeln("      {}", count);
                if (table_list.empty()) {
                    writeln("\n");
                    continue;
                }
                auto it = table_list.begin();
                for (; it != table_list.end() - 1; ++it) {
                    writeln(" {}.{},", (*it)->schema()->ks_name(), (*it)->schema()->cf_name());
                }
                writeln(" {}.{}\n", (*it)->schema()->ks_name(), (*it)->schema()->cf_name());
            }
            writeln("      {} Total (all)\n", total);
        }
        writeln("\n");
    });
}

database::database(const db::config& cfg, database_config dbcfg, service::migration_notifier& mn, gms::feature_service& feat, const locator::shared_token_metadata& stm, abort_source& as, sharded<semaphore>& sst_dir_sem)
    : _stats(make_lw_shared<db_stats>())
    , _cl_stats(std::make_unique<cell_locker_stats>())
    , _cfg(cfg)
    // Allow system tables a pool of 10 MB memory to write, but never block on other regions.
    , _system_dirty_memory_manager(*this, 10 << 20, cfg.virtual_dirty_soft_limit(), default_scheduling_group())
    , _dirty_memory_manager(*this, dbcfg.available_memory * 0.50, cfg.virtual_dirty_soft_limit(), dbcfg.statement_scheduling_group)
    , _dbcfg(dbcfg)
    , _memtable_controller(make_flush_controller(_cfg, dbcfg.memtable_scheduling_group, service::get_local_memtable_flush_priority(), [this, limit = float(_dirty_memory_manager.throttle_threshold())] {
        auto backlog = (_dirty_memory_manager.virtual_dirty_memory()) / limit;
        if (_dirty_memory_manager.has_extraneous_flushes_requested()) {
            backlog = std::max(backlog, _memtable_controller.backlog_of_shares(200));
        }
        return backlog;
    }))
    , _read_concurrency_sem(max_count_concurrent_reads,
        max_memory_concurrent_reads(),
        "_read_concurrency_sem",
        max_inactive_queue_length(),
        [this] {
            ++_stats->sstable_read_queue_overloaded;
        })
    // No timeouts or queue length limits - a failure here can kill an entire repair.
    // Trust the caller to limit concurrency.
    , _streaming_concurrency_sem(
            max_count_streaming_concurrent_reads,
            max_memory_streaming_concurrent_reads(),
            "_streaming_concurrency_sem")
    // No limits, just for accounting.
    , _compaction_concurrency_sem(reader_concurrency_semaphore::no_limits{})
    , _system_read_concurrency_sem(
            // Using higher initial concurrency, see revert_initial_system_read_concurrency_boost().
            max_count_concurrent_reads,
            max_memory_system_concurrent_reads(),
            "_system_read_concurrency_sem")
    , _data_query_stage("data_query", &column_family::query)
    , _mutation_query_stage("mutation_query", &column_family::mutation_query)
    , _apply_stage("db_apply", &database::do_apply)
    , _version(empty_version)
    , _compaction_manager(make_compaction_manager(_cfg, dbcfg, as))
    , _enable_incremental_backups(cfg.incremental_backups())
    , _large_data_handler(std::make_unique<db::cql_table_large_data_handler>(_cfg.compaction_large_partition_warning_threshold_mb()*1024*1024,
              _cfg.compaction_large_row_warning_threshold_mb()*1024*1024,
              _cfg.compaction_large_cell_warning_threshold_mb()*1024*1024,
              _cfg.compaction_rows_count_warning_threshold()))
    , _nop_large_data_handler(std::make_unique<db::nop_large_data_handler>())
    , _user_sstables_manager(std::make_unique<sstables::sstables_manager>(*_large_data_handler, _cfg, feat))
    , _system_sstables_manager(std::make_unique<sstables::sstables_manager>(*_nop_large_data_handler, _cfg, feat))
    , _result_memory_limiter(dbcfg.available_memory / 10)
    , _data_listeners(std::make_unique<db::data_listeners>())
    , _mnotifier(mn)
    , _feat(feat)
    , _shared_token_metadata(stm)
    , _sst_dir_semaphore(sst_dir_sem)
{
    assert(dbcfg.available_memory != 0); // Detect misconfigured unit tests, see #7544

    local_schema_registry().init(*this); // TODO: we're never unbound.
    setup_metrics();

    _row_cache_tracker.set_compaction_scheduling_group(dbcfg.memory_compaction_scheduling_group);

    _infinite_bound_range_deletions_reg = _feat.cluster_supports_unbounded_range_tombstones().when_enabled([this] {
        dblog.debug("Enabling infinite bound range deletions");
        _supports_infinite_bound_range_deletions = true;
    });

    setup_scylla_memory_diagnostics_producer();
}

const db::extensions& database::extensions() const {
    return get_config().extensions();
}

void backlog_controller::adjust() {
    auto backlog = _current_backlog();

    if (backlog >= _control_points.back().input) {
        update_controller(_control_points.back().output);
        return;
    }

    // interpolate to find out which region we are. This run infrequently and there are a fixed
    // number of points so a simple loop will do.
    size_t idx = 1;
    while ((idx < _control_points.size() - 1) && (_control_points[idx].input < backlog)) {
        idx++;
    }

    control_point& cp = _control_points[idx];
    control_point& last = _control_points[idx - 1];
    float result = last.output + (backlog - last.input) * (cp.output - last.output)/(cp.input - last.input);
    update_controller(result);
}

float backlog_controller::backlog_of_shares(float shares) const {
    size_t idx = 1;
    // No control points means the controller is disabled.
    if (_control_points.size() == 0) {
            return 1.0f;
    }
    while ((idx < _control_points.size() - 1) && (_control_points[idx].output < shares)) {
        idx++;
    }
    const control_point& cp = _control_points[idx];
    const control_point& last = _control_points[idx - 1];
    // Compute the inverse function of the backlog in the interpolation interval that we fall
    // into.
    //
    // The formula for the backlog inside an interpolation point is y = a + bx, so the inverse
    // function is x = (y - a) / b

    return last.input + (shares - last.output) * (cp.input - last.input) / (cp.output - last.output);
}

void backlog_controller::update_controller(float shares) {
    _scheduling_group.set_shares(shares);
    if (!_inflight_update.available()) {
        return; // next timer will fix it
    }
    _inflight_update = engine().update_shares_for_class(_io_priority, uint32_t(shares));
}

void
dirty_memory_manager::setup_collectd(sstring namestr) {
    namespace sm = seastar::metrics;

    _metrics.add_group("memory", {
        sm::make_gauge(namestr + "_dirty_bytes", [this] { return real_dirty_memory(); },
                       sm::description("Holds the current size of a all non-free memory in bytes: used memory + released memory that hasn't been returned to a free memory pool yet. "
                                       "Total memory size minus this value represents the amount of available memory. "
                                       "If this value minus virtual_dirty_bytes is too high then this means that the dirty memory eviction lags behind.")),

        sm::make_gauge(namestr +"_virtual_dirty_bytes", [this] { return virtual_dirty_memory(); },
                       sm::description("Holds the size of used memory in bytes. Compare it to \"dirty_bytes\" to see how many memory is wasted (neither used nor available).")),
    });
}

static const metrics::label class_label("class");

void
database::setup_metrics() {
    _dirty_memory_manager.setup_collectd("regular");
    _system_dirty_memory_manager.setup_collectd("system");

    namespace sm = seastar::metrics;

    auto user_label_instance = class_label("user");
    auto streaming_label_instance = class_label("streaming");
    auto system_label_instance = class_label("system");

    _metrics.add_group("memory", {
        sm::make_gauge("dirty_bytes", [this] { return _dirty_memory_manager.real_dirty_memory() + _system_dirty_memory_manager.real_dirty_memory(); },
                       sm::description("Holds the current size of all (\"regular\", \"system\" and \"streaming\") non-free memory in bytes: used memory + released memory that hasn't been returned to a free memory pool yet. "
                                       "Total memory size minus this value represents the amount of available memory. "
                                       "If this value minus virtual_dirty_bytes is too high then this means that the dirty memory eviction lags behind.")),

        sm::make_gauge("virtual_dirty_bytes", [this] { return _dirty_memory_manager.virtual_dirty_memory() + _system_dirty_memory_manager.virtual_dirty_memory(); },
                       sm::description("Holds the size of all (\"regular\", \"system\" and \"streaming\") used memory in bytes. Compare it to \"dirty_bytes\" to see how many memory is wasted (neither used nor available).")),
    });

    _metrics.add_group("memtables", {
        sm::make_gauge("pending_flushes", _cf_stats.pending_memtables_flushes_count,
                       sm::description("Holds the current number of memtables that are currently being flushed to sstables. "
                                       "High value in this metric may be an indication of storage being a bottleneck.")),

        sm::make_gauge("pending_flushes_bytes", _cf_stats.pending_memtables_flushes_bytes,
                       sm::description("Holds the current number of bytes in memtables that are currently being flushed to sstables. "
                                       "High value in this metric may be an indication of storage being a bottleneck.")),
        sm::make_gauge("failed_flushes", _cf_stats.failed_memtables_flushes_count,
                       sm::description("Holds the number of failed memtable flushes. "
                                       "High value in this metric may indicate a permanent failure to flush a memtable.")),
    });

    _metrics.add_group("database", {
        sm::make_gauge("requests_blocked_memory_current", [this] { return _dirty_memory_manager.region_group().blocked_requests(); },
                       sm::description(
                           seastar::format("Holds the current number of requests blocked due to reaching the memory quota ({}B). "
                                           "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"database\" component.", _dirty_memory_manager.throttle_threshold()))),

        sm::make_derive("requests_blocked_memory", [this] { return _dirty_memory_manager.region_group().blocked_requests_counter(); },
                       sm::description(seastar::format("Holds the current number of requests blocked due to reaching the memory quota ({}B). "
                                       "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"database\" component.", _dirty_memory_manager.throttle_threshold()))),

        sm::make_derive("clustering_filter_count", _cf_stats.clustering_filter_count,
                       sm::description("Counts bloom filter invocations.")),

        sm::make_derive("clustering_filter_sstables_checked", _cf_stats.sstables_checked_by_clustering_filter,
                       sm::description("Counts sstables checked after applying the bloom filter. "
                                       "High value indicates that bloom filter is not very efficient.")),

        sm::make_derive("clustering_filter_fast_path_count", _cf_stats.clustering_filter_fast_path_count,
                       sm::description("Counts number of times bloom filtering short cut to include all sstables when only one full range was specified.")),

        sm::make_derive("clustering_filter_surviving_sstables", _cf_stats.surviving_sstables_after_clustering_filter,
                       sm::description("Counts sstables that survived the clustering key filtering. "
                                       "High value indicates that bloom filter is not very efficient and still have to access a lot of sstables to get data.")),

        sm::make_derive("dropped_view_updates", _cf_stats.dropped_view_updates,
                       sm::description("Counts the number of view updates that have been dropped due to cluster overload. ")),

       sm::make_derive("view_building_paused", _cf_stats.view_building_paused,
                      sm::description("Counts the number of times view building process was paused (e.g. due to node unavailability). ")),

        sm::make_derive("total_writes", _stats->total_writes,
                       sm::description("Counts the total number of successful write operations performed by this shard.")),

        sm::make_derive("total_writes_failed", _stats->total_writes_failed,
                       sm::description("Counts the total number of failed write operations. "
                                       "A sum of this value plus total_writes represents a total amount of writes attempted on this shard.")),

        sm::make_derive("total_writes_timedout", _stats->total_writes_timedout,
                       sm::description("Counts write operations failed due to a timeout. A positive value is a sign of storage being overloaded.")),

        sm::make_derive("total_reads", _read_concurrency_sem.get_stats().total_successful_reads,
                       sm::description("Counts the total number of successful user reads on this shard."),
                       {user_label_instance}),

        sm::make_derive("total_reads_failed", _read_concurrency_sem.get_stats().total_failed_reads,
                       sm::description("Counts the total number of failed user read operations. "
                                       "Add the total_reads to this value to get the total amount of reads issued on this shard."),
                       {user_label_instance}),

        sm::make_derive("total_reads", _system_read_concurrency_sem.get_stats().total_successful_reads,
                       sm::description("Counts the total number of successful system reads on this shard."),
                       {system_label_instance}),

        sm::make_derive("total_reads_failed", _system_read_concurrency_sem.get_stats().total_failed_reads,
                       sm::description("Counts the total number of failed system read operations. "
                                       "Add the total_reads to this value to get the total amount of reads issued on this shard."),
                       {system_label_instance}),

        sm::make_current_bytes("view_update_backlog", [this] { return get_view_update_backlog().current; },
                       sm::description("Holds the current size in bytes of the pending view updates for all tables")),

        sm::make_derive("querier_cache_lookups", _querier_cache.get_stats().lookups,
                       sm::description("Counts querier cache lookups (paging queries)")),

        sm::make_derive("querier_cache_misses", _querier_cache.get_stats().misses,
                       sm::description("Counts querier cache lookups that failed to find a cached querier")),

        sm::make_derive("querier_cache_drops", _querier_cache.get_stats().drops,
                       sm::description("Counts querier cache lookups that found a cached querier but had to drop it due to position mismatch")),

        sm::make_derive("querier_cache_time_based_evictions", _querier_cache.get_stats().time_based_evictions,
                       sm::description("Counts querier cache entries that timed out and were evicted.")),

        sm::make_derive("querier_cache_resource_based_evictions", _querier_cache.get_stats().resource_based_evictions,
                       sm::description("Counts querier cache entries that were evicted to free up resources "
                                       "(limited by reader concurency limits) necessary to create new readers.")),

        sm::make_gauge("querier_cache_population", _querier_cache.get_stats().population,
                       sm::description("The number of entries currently in the querier cache.")),

        sm::make_derive("sstable_read_queue_overloads", _stats->sstable_read_queue_overloaded,
                       sm::description("Counts the number of times the sstable read queue was overloaded. "
                                       "A non-zero value indicates that we have to drop read requests because they arrive faster than we can serve them.")),

        sm::make_gauge("active_reads", [this] { return max_count_concurrent_reads - _read_concurrency_sem.available_resources().count; },
                       sm::description("Holds the number of currently active read operations. "),
                       {user_label_instance}),

    });

    // Registering all the metrics with a single call causes the stack size to blow up.
    _metrics.add_group("database", {
        sm::make_gauge("active_reads_memory_consumption", [this] { return max_memory_concurrent_reads() - _read_concurrency_sem.available_resources().memory; },
                       sm::description(seastar::format("Holds the amount of memory consumed by currently active read operations. "
                                                       "If this value gets close to {} we are likely to start dropping new read requests. "
                                                       "In that case sstable_read_queue_overloads is going to get a non-zero value.", max_memory_concurrent_reads())),
                       {user_label_instance}),

        sm::make_gauge("queued_reads", [this] { return _read_concurrency_sem.waiters(); },
                       sm::description("Holds the number of currently queued read operations."),
                       {user_label_instance}),

        sm::make_gauge("paused_reads", _read_concurrency_sem.get_stats().inactive_reads,
                       sm::description("The number of currently active reads that are temporarily paused."),
                       {user_label_instance}),

        sm::make_derive("paused_reads_permit_based_evictions", _read_concurrency_sem.get_stats().permit_based_evictions,
                       sm::description("The number of paused reads evicted to free up permits."
                                       " Permits are required for new reads to start, and the database will evict paused reads (if any)"
                                       " to be able to admit new ones, if there is a shortage of permits."),
                       {user_label_instance}),

        sm::make_derive("reads_shed_due_to_overload", _read_concurrency_sem.get_stats().total_reads_shed_due_to_overload,
                       sm::description("The number of reads shed because the admission queue reached its max capacity."
                                       " When the queue is full, excessive reads are shed to avoid overload."),
                       {user_label_instance}),

        sm::make_gauge("active_reads", [this] { return max_count_streaming_concurrent_reads - _streaming_concurrency_sem.available_resources().count; },
                       sm::description("Holds the number of currently active read operations issued on behalf of streaming "),
                       {streaming_label_instance}),


        sm::make_gauge("active_reads_memory_consumption", [this] { return max_memory_streaming_concurrent_reads() - _streaming_concurrency_sem.available_resources().memory; },
                       sm::description(seastar::format("Holds the amount of memory consumed by currently active read operations issued on behalf of streaming "
                                                       "If this value gets close to {} we are likely to start dropping new read requests. "
                                                       "In that case sstable_read_queue_overloads is going to get a non-zero value.", max_memory_streaming_concurrent_reads())),
                       {streaming_label_instance}),

        sm::make_gauge("queued_reads", [this] { return _streaming_concurrency_sem.waiters(); },
                       sm::description("Holds the number of currently queued read operations on behalf of streaming."),
                       {streaming_label_instance}),

        sm::make_gauge("paused_reads", _streaming_concurrency_sem.get_stats().inactive_reads,
                       sm::description("The number of currently ongoing streaming reads that are temporarily paused."),
                       {streaming_label_instance}),

        sm::make_derive("paused_reads_permit_based_evictions", _streaming_concurrency_sem.get_stats().permit_based_evictions,
                       sm::description("The number of inactive streaming reads evicted to free up permits"
                                       " Permits are required for new reads to start, and the database will evict paused reads (if any)"
                                       " to be able to admit new ones, if there is a shortage of permits."),
                       {streaming_label_instance}),

        sm::make_derive("reads_shed_due_to_overload", _streaming_concurrency_sem.get_stats().total_reads_shed_due_to_overload,
                       sm::description("The number of reads shed because the admission queue reached its max capacity."
                                       " When the queue is full, excessive reads are shed to avoid overload."),
                       {streaming_label_instance}),

        sm::make_gauge("active_reads", [this] { return max_count_system_concurrent_reads - _system_read_concurrency_sem.available_resources().count; },
                       sm::description("Holds the number of currently active read operations from \"system\" keyspace tables. "),
                       {system_label_instance}),

        sm::make_gauge("active_reads_memory_consumption", [this] { return max_memory_system_concurrent_reads() - _system_read_concurrency_sem.available_resources().memory; },
                       sm::description(seastar::format("Holds the amount of memory consumed by currently active read operations from \"system\" keyspace tables. "
                                                       "If this value gets close to {} we are likely to start dropping new read requests. "
                                                       "In that case sstable_read_queue_overloads is going to get a non-zero value.", max_memory_system_concurrent_reads())),
                       {system_label_instance}),

        sm::make_gauge("queued_reads", [this] { return _system_read_concurrency_sem.waiters(); },
                       sm::description("Holds the number of currently queued read operations from \"system\" keyspace tables."),
                       {system_label_instance}),

        sm::make_gauge("paused_reads", _system_read_concurrency_sem.get_stats().inactive_reads,
                       sm::description("The number of currently ongoing system reads that are temporarily paused."),
                       {system_label_instance}),

        sm::make_derive("paused_reads_permit_based_evictions", _system_read_concurrency_sem.get_stats().permit_based_evictions,
                       sm::description("The number of paused system reads evicted to free up permits"
                                       " Permits are required for new reads to start, and the database will evict inactive reads (if any)"
                                       " to be able to admit new ones, if there is a shortage of permits."),
                       {system_label_instance}),

        sm::make_derive("reads_shed_due_to_overload", _system_read_concurrency_sem.get_stats().total_reads_shed_due_to_overload,
                       sm::description("The number of reads shed because the admission queue reached its max capacity."
                                       " When the queue is full, excessive reads are shed to avoid overload."),
                       {system_label_instance}),

        sm::make_gauge("total_result_bytes", [this] { return get_result_memory_limiter().total_used_memory(); },
                       sm::description("Holds the current amount of memory used for results.")),

        sm::make_derive("short_data_queries", _stats->short_data_queries,
                       sm::description("The rate of data queries (data or digest reads) that returned less rows than requested due to result size limiting.")),

        sm::make_derive("short_mutation_queries", _stats->short_mutation_queries,
                       sm::description("The rate of mutation queries that returned less rows than requested due to result size limiting.")),

        sm::make_derive("multishard_query_unpopped_fragments", _stats->multishard_query_unpopped_fragments,
                       sm::description("The total number of fragments that were extracted from the shard reader but were unconsumed by the query and moved back into the reader.")),

        sm::make_derive("multishard_query_unpopped_bytes", _stats->multishard_query_unpopped_bytes,
                       sm::description("The total number of bytes that were extracted from the shard reader but were unconsumed by the query and moved back into the reader.")),

        sm::make_derive("multishard_query_failed_reader_stops", _stats->multishard_query_failed_reader_stops,
                       sm::description("The number of times the stopping of a shard reader failed.")),

        sm::make_derive("multishard_query_failed_reader_saves", _stats->multishard_query_failed_reader_saves,
                       sm::description("The number of times the saving of a shard reader failed.")),

        sm::make_total_operations("counter_cell_lock_acquisition", _cl_stats->lock_acquisitions,
                                 sm::description("The number of acquired counter cell locks.")),

        sm::make_queue_length("counter_cell_lock_pending", _cl_stats->operations_waiting_for_lock,
                             sm::description("The number of counter updates waiting for a lock.")),

        sm::make_counter("large_partition_exceeding_threshold", [this] { return _large_data_handler->stats().partitions_bigger_than_threshold; },
            sm::description("Number of large partitions exceeding compaction_large_partition_warning_threshold_mb. "
                "Large partitions have performance impact and should be avoided, check the documentation for details.")),

        sm::make_total_operations("total_view_updates_pushed_local", _cf_stats.total_view_updates_pushed_local,
                sm::description("Total number of view updates generated for tables and applied locally.")),

        sm::make_total_operations("total_view_updates_pushed_remote", _cf_stats.total_view_updates_pushed_remote,
                sm::description("Total number of view updates generated for tables and sent to remote replicas.")),

        sm::make_total_operations("total_view_updates_failed_local", _cf_stats.total_view_updates_failed_local,
                sm::description("Total number of view updates generated for tables and failed to be applied locally.")),

        sm::make_total_operations("total_view_updates_failed_remote", _cf_stats.total_view_updates_failed_remote,
                sm::description("Total number of view updates generated for tables and failed to be sent to remote replicas.")),
    });
    if (this_shard_id() == 0) {
        _metrics.add_group("database", {
                sm::make_derive("schema_changed", _schema_change_count,
                        sm::description("The number of times the schema changed")),
        });
    }
}

void database::set_format(sstables::sstable_version_types format) {
    get_user_sstables_manager().set_format(format);
    get_system_sstables_manager().set_format(format);
}

void database::set_format_by_config() {
    if (_cfg.enable_sstables_md_format()) {
        set_format(sstables::sstable_version_types::md);
    } else {
        set_format(sstables::sstable_version_types::mc);
    }
}

database::~database() {
}

void database::update_version(const utils::UUID& version) {
    if (_version.get() != version) {
        _schema_change_count++;
    }
    _version.set(version);
}

const utils::UUID& database::get_version() const {
    return _version.get();
}

static future<>
do_parse_schema_tables(distributed<service::storage_proxy>& proxy, const sstring& _cf_name, std::function<future<> (db::schema_tables::schema_result_value_type&)> func) {
    using namespace db::schema_tables;

    auto cf_name = make_lw_shared<sstring>(_cf_name);
    return db::system_keyspace::query(proxy, db::schema_tables::NAME, *cf_name).then([] (auto rs) {
        auto names = std::set<sstring>();
        for (auto& r : rs->rows()) {
            auto keyspace_name = r.template get_nonnull<sstring>("keyspace_name");
            names.emplace(keyspace_name);
        }
        return names;
    }).then([&proxy, cf_name, func = std::move(func)] (std::set<sstring>&& names) mutable {
        return parallel_for_each(names.begin(), names.end(), [&proxy, cf_name, func = std::move(func)] (sstring name) mutable {
            if (is_system_keyspace(name)) {
                return make_ready_future<>();
            }

            return read_schema_partition_for_keyspace(proxy, *cf_name, name).then([func, cf_name] (auto&& v) mutable {
                return do_with(std::move(v), [func = std::move(func), cf_name] (auto& v) {
                    return func(v).then_wrapped([cf_name, &v] (future<> f) {
                        try {
                            f.get();
                        } catch (std::exception& e) {
                            dblog.error("Skipping: {}. Exception occurred when loading system table {}: {}", v.first, *cf_name, e.what());
                        }
                    });
                });
            });
        });
    });
}

future<> database::parse_system_tables(distributed<service::storage_proxy>& proxy, distributed<service::migration_manager>& mm) {
    using namespace db::schema_tables;
    return do_parse_schema_tables(proxy, db::schema_tables::KEYSPACES, [this] (schema_result_value_type &v) {
        auto ksm = create_keyspace_from_schema_partition(v);
        return create_keyspace(ksm, true /* bootstrap. do not mark populated yet */, system_keyspace::no);
    }).then([&proxy, this] {
        return do_parse_schema_tables(proxy, db::schema_tables::TYPES, [this, &proxy] (schema_result_value_type &v) {
            auto& ks = this->find_keyspace(v.first);
            auto&& user_types = create_types_from_schema_partition(*ks.metadata(), v.second);
            for (auto&& type : user_types) {
                ks.add_user_type(type);
            }
            return make_ready_future<>();
        });
    }).then([&proxy, this] {
        return do_parse_schema_tables(proxy, db::schema_tables::FUNCTIONS, [this, &proxy] (schema_result_value_type& v) {
            auto&& user_functions = create_functions_from_schema_partition(*this, v.second);
            for (auto&& func : user_functions) {
                cql3::functions::functions::add_function(func);
            }
            return make_ready_future<>();
        });
    }).then([&proxy, this] {
        return do_parse_schema_tables(proxy, db::schema_tables::TABLES, [this, &proxy] (schema_result_value_type &v) {
            return create_tables_from_tables_partition(proxy, v.second).then([this, &proxy] (std::map<sstring, schema_ptr> tables) {
                return parallel_for_each(tables.begin(), tables.end(), [this, &proxy] (auto& t) {
                    return this->add_column_family_and_make_directory(t.second).then([&proxy, s = t.second] {
                        // Recreate missing column mapping entries in case
                        // we failed to persist them for some reason after a schema change
                        return db::schema_tables::column_mapping_exists(s->id(), s->version()).then([&proxy, s] (bool cm_exists) {
                            if (cm_exists) {
                                return make_ready_future<>();
                            }
                            return db::schema_tables::store_column_mapping(proxy, s, false);
                        });
                    });
                });
            });
            });
    }).then([&proxy, &mm, this] {
        return do_parse_schema_tables(proxy, db::schema_tables::VIEWS, [this, &proxy, &mm] (schema_result_value_type &v) {
            return create_views_from_schema_partition(proxy, v.second).then([this, &mm, &proxy] (std::vector<view_ptr> views) {
                return parallel_for_each(views.begin(), views.end(), [this, &mm, &proxy] (auto&& v) {
                    // TODO: Remove once computed columns are guaranteed to be featured in the whole cluster.
                    // we fix here the schema in place in oreder to avoid races (write commands comming from other coordinators).
                    view_ptr fixed_v = maybe_fix_legacy_secondary_index_mv_schema(*this, v, nullptr, preserve_version::yes);
                    view_ptr v_to_add = fixed_v ? fixed_v : v;
                    future<> f = this->add_column_family_and_make_directory(v_to_add);
                    if (bool(fixed_v)) {
                        v_to_add = fixed_v;
                        auto&& keyspace = find_keyspace(v->ks_name()).metadata();
                        auto mutations = db::schema_tables::make_update_view_mutations(keyspace, view_ptr(v), fixed_v, api::new_timestamp(), true);
                        f = f.then([this, &proxy, mutations = std::move(mutations)] {
                            return db::schema_tables::merge_schema(proxy, _feat, std::move(mutations));
                        });
                    }
                    return f;
                });
            });
        });
    });
}

future<>
database::init_commitlog() {
    return db::commitlog::create_commitlog(db::commitlog::config::from_db_config(_cfg, _dbcfg.available_memory)).then([this](db::commitlog&& log) {
        _commitlog = std::make_unique<db::commitlog>(std::move(log));
        _commitlog->add_flush_handler([this](db::cf_id_type id, db::replay_position pos) {
            if (!_column_families.contains(id)) {
                // the CF has been removed.
                _commitlog->discard_completed_segments(id);
                return;
            }
            // Initiate a background flush. Waited upon in `stop()`.
            (void)_column_families[id]->flush(pos);
        }).release(); // we have longer life time than CL. Ignore reg anchor
    });
}

unsigned
database::shard_of(const mutation& m) {
    return dht::shard_of(*m.schema(), m.token());
}

unsigned
database::shard_of(const frozen_mutation& m) {
    // FIXME: This lookup wouldn't be necessary if we
    // sent the partition key in legacy form or together
    // with token.
    schema_ptr schema = find_schema(m.column_family_id());
    return dht::shard_of(*schema, dht::get_token(*schema, m.key()));
}

future<> database::update_keyspace(sharded<service::storage_proxy>& proxy, const sstring& name) {
    return db::schema_tables::read_schema_partition_for_keyspace(proxy, db::schema_tables::KEYSPACES, name).then([this, name](db::schema_tables::schema_result_value_type&& v) {
        auto& ks = find_keyspace(name);

        auto tmp_ksm = db::schema_tables::create_keyspace_from_schema_partition(v);
        auto new_ksm = ::make_lw_shared<keyspace_metadata>(tmp_ksm->name(), tmp_ksm->strategy_name(), tmp_ksm->strategy_options(), tmp_ksm->durable_writes(),
                        boost::copy_range<std::vector<schema_ptr>>(ks.metadata()->cf_meta_data() | boost::adaptors::map_values), std::move(ks.metadata()->user_types()));

        bool old_durable_writes = ks.metadata()->durable_writes();
        bool new_durable_writes = new_ksm->durable_writes();
        if (old_durable_writes != new_durable_writes) {
            for (auto& [cf_name, cf_schema] : new_ksm->cf_meta_data()) {
                auto& cf = find_column_family(cf_schema);
                cf.set_durable_writes(new_durable_writes);
            }
        }

        ks.update_from(get_shared_token_metadata(), std::move(new_ksm));
        return get_notifier().update_keyspace(ks.metadata());
    });
}

void database::drop_keyspace(const sstring& name) {
    _keyspaces.erase(name);
}

void database::add_column_family(keyspace& ks, schema_ptr schema, column_family::config cfg) {
    schema = local_schema_registry().learn(schema);
    schema->registry_entry()->mark_synced();

    lw_shared_ptr<column_family> cf;
    if (cfg.enable_commitlog && _commitlog) {
       cf = make_lw_shared<column_family>(schema, std::move(cfg), *_commitlog, *_compaction_manager, *_cl_stats, _row_cache_tracker);
    } else {
       cf = make_lw_shared<column_family>(schema, std::move(cfg), column_family::no_commitlog(), *_compaction_manager, *_cl_stats, _row_cache_tracker);
    }
    cf->set_durable_writes(ks.metadata()->durable_writes());

    auto uuid = schema->id();
    if (_column_families.contains(uuid)) {
        throw std::invalid_argument("UUID " + uuid.to_sstring() + " already mapped");
    }
    auto kscf = std::make_pair(schema->ks_name(), schema->cf_name());
    if (_ks_cf_to_uuid.contains(kscf)) {
        throw std::invalid_argument("Column family " + schema->cf_name() + " exists");
    }
    ks.add_or_update_column_family(schema);
    cf->start();
    _column_families.emplace(uuid, std::move(cf));
    _ks_cf_to_uuid.emplace(std::move(kscf), uuid);
    if (schema->is_view()) {
        find_column_family(schema->view_info()->base_id()).add_or_update_view(view_ptr(schema));
    }
}

future<> database::add_column_family_and_make_directory(schema_ptr schema) {
    auto& ks = find_keyspace(schema->ks_name());
    add_column_family(ks, schema, ks.make_column_family_config(*schema, *this));
    find_column_family(schema).get_index_manager().reload();
    return ks.make_directory_for_column_family(schema->cf_name(), schema->id());
}

bool database::update_column_family(schema_ptr new_schema) {
    column_family& cfm = find_column_family(new_schema->id());
    bool columns_changed = !cfm.schema()->equal_columns(*new_schema);
    auto s = local_schema_registry().learn(new_schema);
    s->registry_entry()->mark_synced();
    cfm.set_schema(s);
    find_keyspace(s->ks_name()).metadata()->add_or_update_column_family(s);
    if (s->is_view()) {
        try {
            find_column_family(s->view_info()->base_id()).add_or_update_view(view_ptr(s));
        } catch (no_such_column_family&) {
            // Update view mutations received after base table drop.
        }
    }
    cfm.get_index_manager().reload();
    return columns_changed;
}

future<> database::remove(const column_family& cf) noexcept {
    auto s = cf.schema();
    auto& ks = find_keyspace(s->ks_name());
    co_await _querier_cache.evict_all_for_table(s->id());
    _column_families.erase(s->id());
    ks.metadata()->remove_column_family(s);
    _ks_cf_to_uuid.erase(std::make_pair(s->ks_name(), s->cf_name()));
    if (s->is_view()) {
        try {
            find_column_family(s->view_info()->base_id()).remove_view(view_ptr(s));
        } catch (no_such_column_family&) {
            // Drop view mutations received after base table drop.
        }
    }
}

future<> database::drop_column_family(const sstring& ks_name, const sstring& cf_name, timestamp_func tsf, bool snapshot) {
    auto& ks = find_keyspace(ks_name);
    auto uuid = find_uuid(ks_name, cf_name);
    auto cf = _column_families.at(uuid);
    co_await remove(*cf);
    cf->clear_views();
    co_return co_await cf->await_pending_ops().then([this, &ks, cf, tsf = std::move(tsf), snapshot] {
        return truncate(ks, *cf, std::move(tsf), snapshot).finally([this, cf] {
            return cf->stop();
        });
    }).finally([cf] {});
}

const utils::UUID& database::find_uuid(std::string_view ks, std::string_view cf) const {
    try {
        return _ks_cf_to_uuid.at(std::make_pair(ks, cf));
    } catch (...) {
        throw std::out_of_range("");
    }
}

const utils::UUID& database::find_uuid(const schema_ptr& schema) const {
    return find_uuid(schema->ks_name(), schema->cf_name());
}

keyspace& database::find_keyspace(std::string_view name) {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

const keyspace& database::find_keyspace(std::string_view name) const {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

bool database::has_keyspace(std::string_view name) const {
    return _keyspaces.contains(name);
}

std::vector<sstring>  database::get_non_system_keyspaces() const {
    std::vector<sstring> res;
    for (auto const &i : _keyspaces) {
        if (!is_system_keyspace(i.first)) {
            res.push_back(i.first);
        }
    }
    return res;
}

std::vector<lw_shared_ptr<column_family>> database::get_non_system_column_families() const {
    return boost::copy_range<std::vector<lw_shared_ptr<column_family>>>(
        get_column_families()
            | boost::adaptors::map_values
            | boost::adaptors::filtered([](const lw_shared_ptr<column_family>& cf) {
                return !is_system_keyspace(cf->schema()->ks_name());
            }));
}

column_family& database::find_column_family(std::string_view ks_name, std::string_view cf_name) {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name, cf_name));
    }
}

const column_family& database::find_column_family(std::string_view ks_name, std::string_view cf_name) const {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name, cf_name));
    }
}

column_family& database::find_column_family(const utils::UUID& uuid) {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid));
    }
}

const column_family& database::find_column_family(const utils::UUID& uuid) const {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid));
    }
}

bool database::column_family_exists(const utils::UUID& uuid) const {
    return _column_families.contains(uuid);
}

void
keyspace::create_replication_strategy(const locator::shared_token_metadata& stm, const std::map<sstring, sstring>& options) {
    using namespace locator;

    _replication_strategy =
            abstract_replication_strategy::create_replication_strategy(
                _metadata->name(), _metadata->strategy_name(), stm, options);
}

locator::abstract_replication_strategy&
keyspace::get_replication_strategy() {
    return *_replication_strategy;
}


const locator::abstract_replication_strategy&
keyspace::get_replication_strategy() const {
    return *_replication_strategy;
}

void
keyspace::set_replication_strategy(std::unique_ptr<locator::abstract_replication_strategy> replication_strategy) {
    _replication_strategy = std::move(replication_strategy);
}

void keyspace::update_from(const locator::shared_token_metadata& stm, ::lw_shared_ptr<keyspace_metadata> ksm) {
    _metadata = std::move(ksm);
   create_replication_strategy(stm, _metadata->strategy_options());
}

future<> keyspace::ensure_populated() const {
    return _populated.get_shared_future();
}

void keyspace::mark_as_populated() {
    if (!_populated.available()) {
        _populated.set_value();
    }
}


static bool is_system_table(const schema& s) {
    return s.ks_name() == db::system_keyspace::NAME || s.ks_name() == db::system_distributed_keyspace::NAME
        || s.ks_name() == db::system_distributed_keyspace::NAME_EVERYWHERE;
}

column_family::config
keyspace::make_column_family_config(const schema& s, const database& db) const {
    column_family::config cfg;
    const db::config& db_config = db.get_config();

    for (auto& extra : _config.all_datadirs) {
        cfg.all_datadirs.push_back(column_family_directory(extra, s.cf_name(), s.id()));
    }
    cfg.datadir = cfg.all_datadirs[0];
    cfg.enable_disk_reads = _config.enable_disk_reads;
    cfg.enable_disk_writes = _config.enable_disk_writes;
    cfg.enable_commitlog = _config.enable_commitlog;
    cfg.enable_cache = _config.enable_cache;
    cfg.enable_dangerous_direct_import_of_cassandra_counters = _config.enable_dangerous_direct_import_of_cassandra_counters;
    cfg.compaction_enforce_min_threshold = _config.compaction_enforce_min_threshold;
    cfg.dirty_memory_manager = _config.dirty_memory_manager;
    cfg.streaming_read_concurrency_semaphore = _config.streaming_read_concurrency_semaphore;
    cfg.compaction_concurrency_semaphore = _config.compaction_concurrency_semaphore;
    cfg.cf_stats = _config.cf_stats;
    cfg.enable_incremental_backups = _config.enable_incremental_backups;
    cfg.compaction_scheduling_group = _config.compaction_scheduling_group;
    cfg.memory_compaction_scheduling_group = _config.memory_compaction_scheduling_group;
    cfg.memtable_scheduling_group = _config.memtable_scheduling_group;
    cfg.memtable_to_cache_scheduling_group = _config.memtable_to_cache_scheduling_group;
    cfg.streaming_scheduling_group = _config.streaming_scheduling_group;
    cfg.statement_scheduling_group = _config.statement_scheduling_group;
    cfg.enable_metrics_reporting = db_config.enable_keyspace_column_family_metrics();

    // avoid self-reporting
    if (is_system_table(s)) {
        cfg.sstables_manager = &db.get_system_sstables_manager();
    } else {
        cfg.sstables_manager = &db.get_user_sstables_manager();
    }

    cfg.view_update_concurrency_semaphore = _config.view_update_concurrency_semaphore;
    cfg.view_update_concurrency_semaphore_limit = _config.view_update_concurrency_semaphore_limit;
    cfg.data_listeners = &db.data_listeners();

    return cfg;
}

sstring
keyspace::column_family_directory(const sstring& name, utils::UUID uuid) const {
    return column_family_directory(_config.datadir, name, uuid);
}

sstring
keyspace::column_family_directory(const sstring& base_path, const sstring& name, utils::UUID uuid) const {
    auto uuid_sstring = uuid.to_sstring();
    boost::erase_all(uuid_sstring, "-");
    return format("{}/{}-{}", base_path, name, uuid_sstring);
}

future<>
keyspace::make_directory_for_column_family(const sstring& name, utils::UUID uuid) {
    std::vector<sstring> cfdirs;
    for (auto& extra : _config.all_datadirs) {
        cfdirs.push_back(column_family_directory(extra, name, uuid));
    }
    return parallel_for_each(cfdirs, [] (sstring cfdir) {
        return io_check([cfdir] { return recursive_touch_directory(cfdir); });
    }).then([cfdirs0 = cfdirs[0]] {
        return io_check([cfdirs0] { return touch_directory(cfdirs0 + "/upload"); });
    }).then([cfdirs0 = cfdirs[0]] {
        return io_check([cfdirs0] { return touch_directory(cfdirs0 + "/staging"); });
    });
}

no_such_keyspace::no_such_keyspace(std::string_view ks_name)
    : runtime_error{format("Can't find a keyspace {}", ks_name)}
{
}

no_such_column_family::no_such_column_family(const utils::UUID& uuid)
    : runtime_error{format("Can't find a column family with UUID {}", uuid)}
{
}

no_such_column_family::no_such_column_family(std::string_view ks_name, std::string_view cf_name)
    : runtime_error{format("Can't find a column family {} in keyspace {}", cf_name, ks_name)}
{
}

column_family& database::find_column_family(const schema_ptr& schema) {
    return find_column_family(schema->id());
}

const column_family& database::find_column_family(const schema_ptr& schema) const {
    return find_column_family(schema->id());
}

using strategy_class_registry = class_registry<
    locator::abstract_replication_strategy,
    const sstring&,
    const locator::shared_token_metadata&,
    locator::snitch_ptr&,
    const std::map<sstring, sstring>&>;

keyspace_metadata::keyspace_metadata(std::string_view name,
             std::string_view strategy_name,
             std::map<sstring, sstring> strategy_options,
             bool durable_writes,
             std::vector<schema_ptr> cf_defs)
    : keyspace_metadata(name,
                        strategy_name,
                        std::move(strategy_options),
                        durable_writes,
                        std::move(cf_defs),
                        user_types_metadata{}) { }

keyspace_metadata::keyspace_metadata(std::string_view name,
             std::string_view strategy_name,
             std::map<sstring, sstring> strategy_options,
             bool durable_writes,
             std::vector<schema_ptr> cf_defs,
             user_types_metadata user_types)
    : _name{name}
    , _strategy_name{strategy_class_registry::to_qualified_class_name(strategy_name.empty() ? "NetworkTopologyStrategy" : strategy_name)}
    , _strategy_options{std::move(strategy_options)}
    , _durable_writes{durable_writes}
    , _user_types{std::move(user_types)}
{
    for (auto&& s : cf_defs) {
        _cf_meta_data.emplace(s->cf_name(), s);
    }
}

void keyspace_metadata::validate(const locator::shared_token_metadata& stm) const {
    using namespace locator;
    abstract_replication_strategy::validate_replication_strategy(name(), strategy_name(), stm, strategy_options());
}

void database::validate_keyspace_update(keyspace_metadata& ksm) {
    ksm.validate(get_shared_token_metadata());
    if (!has_keyspace(ksm.name())) {
        throw exceptions::configuration_exception(format("Cannot update non existing keyspace '{}'.", ksm.name()));
    }
}

void database::validate_new_keyspace(keyspace_metadata& ksm) {
    ksm.validate(get_shared_token_metadata());
    if (has_keyspace(ksm.name())) {
        throw exceptions::already_exists_exception{ksm.name()};
    }
}

std::vector<schema_ptr> keyspace_metadata::tables() const {
    return boost::copy_range<std::vector<schema_ptr>>(_cf_meta_data
            | boost::adaptors::map_values
            | boost::adaptors::filtered([] (auto&& s) { return !s->is_view(); }));
}

std::vector<view_ptr> keyspace_metadata::views() const {
    return boost::copy_range<std::vector<view_ptr>>(_cf_meta_data
            | boost::adaptors::map_values
            | boost::adaptors::filtered(std::mem_fn(&schema::is_view))
            | boost::adaptors::transformed([] (auto&& s) { return view_ptr(s); }));
}

schema_ptr database::find_schema(const sstring& ks_name, const sstring& cf_name) const {
    try {
        return find_schema(find_uuid(ks_name, cf_name));
    } catch (std::out_of_range&) {
        std::throw_with_nested(no_such_column_family(ks_name, cf_name));
    }
}

schema_ptr database::find_schema(const utils::UUID& uuid) const {
    return find_column_family(uuid).schema();
}

bool database::has_schema(std::string_view ks_name, std::string_view cf_name) const {
    return _ks_cf_to_uuid.contains(std::make_pair(ks_name, cf_name));
}

std::vector<view_ptr> database::get_views() const {
    return boost::copy_range<std::vector<view_ptr>>(get_non_system_column_families()
            | boost::adaptors::filtered([] (auto& cf) { return cf->schema()->is_view(); })
            | boost::adaptors::transformed([] (auto& cf) { return view_ptr(cf->schema()); }));
}

void database::create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm, system_keyspace system) {
    auto kscfg = make_keyspace_config(*ksm);
    if (system == system_keyspace::yes) {
        kscfg.enable_disk_reads = kscfg.enable_disk_writes = kscfg.enable_commitlog = !_cfg.volatile_system_keyspace_for_testing();
        kscfg.enable_cache = _cfg.enable_cache();
        // don't make system keyspace writes wait for user writes (if under pressure)
        kscfg.dirty_memory_manager = &_system_dirty_memory_manager;
    }
    keyspace ks(ksm, std::move(kscfg));
    ks.create_replication_strategy(get_shared_token_metadata(), ksm->strategy_options());
    _keyspaces.emplace(ksm->name(), std::move(ks));
}

future<>
database::create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    return create_keyspace(ksm, false, system_keyspace::no);
}

future<>
database::create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm, bool is_bootstrap, system_keyspace system) {
    if (_keyspaces.contains(ksm->name())) {
        return make_ready_future<>();
    }

    create_in_memory_keyspace(ksm, system);
    auto& ks = _keyspaces.at(ksm->name());
    auto& datadir = ks.datadir();

    // keyspace created by either cql or migration 
    // is by definition populated
    if (!is_bootstrap) {
        ks.mark_as_populated();
    }

    if (datadir != "") {
        return io_check([&datadir] { return touch_directory(datadir); });
    } else {
        return make_ready_future<>();
    }
}

future<>
database::drop_caches() const {
    std::unordered_map<utils::UUID, lw_shared_ptr<column_family>> tables = get_column_families();
    for (auto&& e : tables) {
        table& t = *e.second;
        co_await t.get_row_cache().invalidate(row_cache::external_updater([] {}));
    }
    co_return;
}

std::set<sstring>
database::existing_index_names(const sstring& ks_name, const sstring& cf_to_exclude) const {
    std::set<sstring> names;
    for (auto& schema : find_keyspace(ks_name).metadata()->tables()) {
        if (!cf_to_exclude.empty() && schema->cf_name() == cf_to_exclude) {
            continue;
        }
        for (const auto& index_name : schema->index_names()) {
            names.emplace(index_name);
        }
    }
    return names;
}

// Based on:
//  - org.apache.cassandra.db.AbstractCell#reconcile()
//  - org.apache.cassandra.db.BufferExpiringCell#reconcile()
//  - org.apache.cassandra.db.BufferDeletedCell#reconcile()
int
compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right) {
    if (left.timestamp() != right.timestamp()) {
        return left.timestamp() > right.timestamp() ? 1 : -1;
    }
    if (left.is_live() != right.is_live()) {
        return left.is_live() ? -1 : 1;
    }
    if (left.is_live()) {
        auto c = compare_unsigned(left.value(), right.value());
        if (c != 0) {
            return c;
        }
        if (left.is_live_and_has_ttl() != right.is_live_and_has_ttl()) {
            // prefer expiring cells.
            return left.is_live_and_has_ttl() ? 1 : -1;
        }
        if (left.is_live_and_has_ttl() && left.expiry() != right.expiry()) {
            return left.expiry() < right.expiry() ? -1 : 1;
        }
    } else {
        // Both are deleted
        if (left.deletion_time() != right.deletion_time()) {
            // Origin compares big-endian serialized deletion time. That's because it
            // delegates to AbstractCell.reconcile() which compares values after
            // comparing timestamps, which in case of deleted cells will hold
            // serialized expiry.
            return (uint64_t) left.deletion_time().time_since_epoch().count()
                   < (uint64_t) right.deletion_time().time_since_epoch().count() ? -1 : 1;
        }
    }
    return 0;
}

future<std::tuple<lw_shared_ptr<query::result>, cache_temperature>>
database::query(schema_ptr s, const query::read_command& cmd, query::result_options opts, const dht::partition_range_vector& ranges,
                tracing::trace_state_ptr trace_state, db::timeout_clock::time_point timeout) {
    column_family& cf = find_column_family(cmd.cf_id);
    auto& semaphore = get_reader_concurrency_semaphore();
    auto class_config = query::query_class_config{.semaphore = semaphore, .max_memory_for_unlimited_query = *cmd.max_result_size};
    query::querier_cache_context cache_ctx(_querier_cache, cmd.query_uuid, cmd.is_first_page);
    return _data_query_stage(&cf,
            std::move(s),
            seastar::cref(cmd),
            class_config,
            opts,
            seastar::cref(ranges),
            std::move(trace_state),
            seastar::ref(get_result_memory_limiter()),
            timeout,
            std::move(cache_ctx)).then_wrapped([this, s = _stats, &semaphore, hit_rate = cf.get_global_cache_hit_rate(), op = cf.read_in_progress()] (auto f) {
        if (f.failed()) {
            ++semaphore.get_stats().total_failed_reads;
            return make_exception_future<std::tuple<lw_shared_ptr<query::result>, cache_temperature>>(f.get_exception());
        } else {
            ++semaphore.get_stats().total_successful_reads;
            auto result = f.get0();
            s->short_data_queries += bool(result->is_short_read());
            return make_ready_future<std::tuple<lw_shared_ptr<query::result>, cache_temperature>>(std::tuple(std::move(result), hit_rate));
        }
    });
}

future<std::tuple<reconcilable_result, cache_temperature>>
database::query_mutations(schema_ptr s, const query::read_command& cmd, const dht::partition_range& range,
                          tracing::trace_state_ptr trace_state, db::timeout_clock::time_point timeout) {
    const auto short_read_allwoed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());
  return get_result_memory_limiter().new_mutation_read(*cmd.max_result_size, short_read_allwoed).then(
          [&, s = std::move(s), trace_state = std::move(trace_state), timeout] (query::result_memory_accounter accounter) {
    column_family& cf = find_column_family(cmd.cf_id);
    auto& semaphore = get_reader_concurrency_semaphore();
    auto class_config = query::query_class_config{.semaphore = semaphore, .max_memory_for_unlimited_query = *cmd.max_result_size};
    query::querier_cache_context cache_ctx(_querier_cache, cmd.query_uuid, cmd.is_first_page);
    return _mutation_query_stage(&cf,
            std::move(s),
            seastar::cref(cmd),
            class_config,
            seastar::cref(range),
            std::move(trace_state),
            std::move(accounter),
            timeout,
            std::move(cache_ctx)).then_wrapped([this, s = _stats, &semaphore, hit_rate = cf.get_global_cache_hit_rate(), op = cf.read_in_progress()] (auto f) {
        if (f.failed()) {
            ++semaphore.get_stats().total_failed_reads;
            return make_exception_future<std::tuple<reconcilable_result, cache_temperature>>(f.get_exception());
        } else {
            ++semaphore.get_stats().total_successful_reads;
            auto result = f.get0();
            s->short_mutation_queries += bool(result.is_short_read());
            return make_ready_future<std::tuple<reconcilable_result, cache_temperature>>(std::tuple(std::move(result), hit_rate));
        }
    });
  });
}

std::unordered_set<sstring> database::get_initial_tokens() {
    std::unordered_set<sstring> tokens;
    sstring tokens_string = get_config().initial_token();
    try {
        boost::split(tokens, tokens_string, boost::is_any_of(sstring(", ")));
    } catch (...) {
        throw std::runtime_error(format("Unable to parse initial_token={}", tokens_string));
    }
    tokens.erase("");
    return tokens;
}

std::optional<gms::inet_address> database::get_replace_address() {
    auto& cfg = get_config();
    sstring replace_address = cfg.replace_address();
    sstring replace_address_first_boot = cfg.replace_address_first_boot();
    try {
        if (!replace_address.empty()) {
            return gms::inet_address(replace_address);
        } else if (!replace_address_first_boot.empty()) {
            return gms::inet_address(replace_address_first_boot);
        }
        return std::nullopt;
    } catch (...) {
        return std::nullopt;
    }
}

bool database::is_replacing() {
    sstring replace_address_first_boot = get_config().replace_address_first_boot();
    if (!replace_address_first_boot.empty() && db::system_keyspace::bootstrap_complete()) {
        dblog.info("Replace address on first boot requested; this node is already bootstrapped");
        return false;
    }
    return bool(get_replace_address());
}

void database::register_connection_drop_notifier(netw::messaging_service& ms) {
    ms.register_connection_drop_notifier([this] (gms::inet_address ep) {
        dblog.debug("Drop hit rate info for {} because of disconnect", ep);
        for (auto&& cf : get_non_system_column_families()) {
            cf->drop_hit_rate(ep);
        }
    });
}

namespace {

enum class query_class {
    user,
    system,
    maintenance,
};

query_class classify_query(const database_config& _dbcfg) {
    const auto current_group = current_scheduling_group();

    // Everything running in the statement group is considered a user query
    if (current_group == _dbcfg.statement_scheduling_group) {
        return query_class::user;
    // System queries run in the default (main) scheduling group
    // All queries executed on behalf of internal work also uses the system semaphore
    } else if (current_group == default_scheduling_group()
            || current_group == _dbcfg.compaction_scheduling_group
            || current_group == _dbcfg.gossip_scheduling_group
            || current_group == _dbcfg.memory_compaction_scheduling_group
            || current_group == _dbcfg.memtable_scheduling_group
            || current_group == _dbcfg.memtable_to_cache_scheduling_group) {
        return query_class::system;
    // Reads done on behalf of view update generation run in the streaming group
    } else if (current_scheduling_group() == _dbcfg.streaming_scheduling_group) {
        return query_class::maintenance;
    // Everything else is considered a user query
    } else {
        return query_class::user;
    }
}

} // anonymous namespace

query::max_result_size database::get_unlimited_query_max_result_size() const {
    switch (classify_query(_dbcfg)) {
        case query_class::user:
            return query::max_result_size(_cfg.max_memory_for_unlimited_query_soft_limit(), _cfg.max_memory_for_unlimited_query_hard_limit());
        case query_class::system: [[fallthrough]];
        case query_class::maintenance:
            return query::max_result_size(query::result_memory_limiter::unlimited_result_size);
    }
}

reader_concurrency_semaphore& database::get_reader_concurrency_semaphore() {
    switch (classify_query(_dbcfg)) {
        case query_class::user: return _read_concurrency_sem;
        case query_class::system: return _system_read_concurrency_sem;
        case query_class::maintenance: return _streaming_concurrency_sem;
    }
}

std::ostream& operator<<(std::ostream& out, const column_family& cf) {
    return fmt_print(out, "{{column_family: {}/{}}}", cf._schema->ks_name(), cf._schema->cf_name());
}

std::ostream& operator<<(std::ostream& out, const database& db) {
    out << "{\n";
    for (auto&& e : db._column_families) {
        auto&& cf = *e.second;
        out << "(" << e.first.to_sstring() << ", " << cf.schema()->cf_name() << ", " << cf.schema()->ks_name() << "): " << cf << "\n";
    }
    out << "}";
    return out;
}

future<mutation> database::do_apply_counter_update(column_family& cf, const frozen_mutation& fm, schema_ptr m_schema,
                                                   db::timeout_clock::time_point timeout,tracing::trace_state_ptr trace_state) {
    auto m = fm.unfreeze(m_schema);
    m.upgrade(cf.schema());

    // prepare partition slice
    query::column_id_vector static_columns;
    static_columns.reserve(m.partition().static_row().size());
    m.partition().static_row().for_each_cell([&] (auto id, auto&&) {
        static_columns.emplace_back(id);
    });

    query::clustering_row_ranges cr_ranges;
    cr_ranges.reserve(8);
    query::column_id_vector regular_columns;
    regular_columns.reserve(32);

    for (auto&& cr : m.partition().clustered_rows()) {
        cr_ranges.emplace_back(query::clustering_range::make_singular(cr.key()));
        cr.row().cells().for_each_cell([&] (auto id, auto&&) {
            regular_columns.emplace_back(id);
        });
    }

    boost::sort(regular_columns);
    regular_columns.erase(std::unique(regular_columns.begin(), regular_columns.end()),
                          regular_columns.end());

    auto slice = query::partition_slice(std::move(cr_ranges), std::move(static_columns),
        std::move(regular_columns), { }, { }, cql_serialization_format::internal(), query::max_rows);

    return do_with(std::move(slice), std::move(m), std::vector<locked_cell>(),
                   [this, &cf, timeout, trace_state = std::move(trace_state), op = cf.write_in_progress()] (const query::partition_slice& slice, mutation& m, std::vector<locked_cell>& locks) mutable {
        tracing::trace(trace_state, "Acquiring counter locks");
        return cf.lock_counter_cells(m, timeout).then([&, m_schema = cf.schema(), trace_state = std::move(trace_state), timeout, this] (std::vector<locked_cell> lcs) mutable {
            locks = std::move(lcs);

            // Before counter update is applied it needs to be transformed from
            // deltas to counter shards. To do that, we need to read the current
            // counter state for each modified cell...

            tracing::trace(trace_state, "Reading counter values from the CF");
            auto permit = get_reader_concurrency_semaphore().make_permit(m_schema.get(), "counter-read-before-write");
            return counter_write_query(m_schema, cf.as_mutation_source(), std::move(permit), m.decorated_key(), slice, trace_state, timeout)
                    .then([this, &cf, &m, m_schema, timeout, trace_state] (auto mopt) {
                // ...now, that we got existing state of all affected counter
                // cells we can look for our shard in each of them, increment
                // its clock and apply the delta.
                transform_counter_updates_to_shards(m, mopt ? &*mopt : nullptr, cf.failed_counter_applies_to_memtable(), _local_host_id);
                tracing::trace(trace_state, "Applying counter update");
                return this->apply_with_commitlog(cf, m, timeout);
            }).then([&m] {
                return std::move(m);
            });
        });
    });
}

future<> dirty_memory_manager::shutdown() {
    _db_shutdown_requested = true;
    _should_flush.signal();
    return std::move(_waiting_flush).then([this] {
        return _virtual_region_group.shutdown().then([this] {
            return _real_region_group.shutdown();
        });
    });
}

future<> memtable_list::flush() {
    if (!may_flush()) {
        return make_ready_future<>();
    } else if (!_flush_coalescing) {
        _flush_coalescing = shared_promise<>();
        _dirty_memory_manager->start_extraneous_flush();
        auto ef = defer([this] { _dirty_memory_manager->finish_extraneous_flush(); });
        return _dirty_memory_manager->get_flush_permit().then([this, ef = std::move(ef)] (auto permit) mutable {
            auto current_flush = std::move(*_flush_coalescing);
            _flush_coalescing = {};
            return _dirty_memory_manager->flush_one(*this, std::move(permit)).then_wrapped([this, ef = std::move(ef),
                                                                                            current_flush = std::move(current_flush)] (auto f) mutable {
                if (f.failed()) {
                    current_flush.set_exception(f.get_exception());
                } else {
                    current_flush.set_value();
                }
            });
        });
    } else {
        return _flush_coalescing->get_shared_future();
    }
}

lw_shared_ptr<memtable> memtable_list::new_memtable() {
    return make_lw_shared<memtable>(_current_schema(), *_dirty_memory_manager, _table_stats, this, _compaction_scheduling_group);
}

future<flush_permit> flush_permit::reacquire_sstable_write_permit() && {
    return _manager->get_flush_permit(std::move(_background_permit));
}

future<> dirty_memory_manager::flush_one(memtable_list& mtlist, flush_permit&& permit) {
    return mtlist.seal_active_memtable(std::move(permit)).handle_exception([this, schema = mtlist.back()->schema()] (std::exception_ptr ep) {
        dblog.error("Failed to flush memtable, {}:{} - {}", schema->ks_name(), schema->cf_name(), ep);
        return make_exception_future<>(ep);
    });
}

future<> dirty_memory_manager::flush_when_needed() {
    if (!_db) {
        return make_ready_future<>();
    }
    // If there are explicit flushes requested, we must wait for them to finish before we stop.
    return do_until([this] { return _db_shutdown_requested; }, [this] {
        auto has_work = [this] { return has_pressure() || _db_shutdown_requested; };
        return _should_flush.wait(std::move(has_work)).then([this] {
            return get_flush_permit().then([this] (auto permit) {
                // We give priority to explicit flushes. They are mainly user-initiated flushes,
                // flushes coming from a DROP statement, or commitlog flushes.
                if (_flush_serializer.waiters()) {
                    return make_ready_future<>();
                }
                // condition abated while we waited for the semaphore
                if (!this->has_pressure() || _db_shutdown_requested) {
                    return make_ready_future<>();
                }
                // There are many criteria that can be used to select what is the best memtable to
                // flush. Most of the time we want some coordination with the commitlog to allow us to
                // release commitlog segments as early as we can.
                //
                // But during pressure condition, we'll just pick the CF that holds the largest
                // memtable. The advantage of doing this is that this is objectively the one that will
                // release the biggest amount of memory and is less likely to be generating tiny
                // SSTables.
                memtable& candidate_memtable = memtable::from_region(*(this->_virtual_region_group.get_largest_region()));
                memtable_list& mtlist = *(candidate_memtable.get_memtable_list());

                if (!candidate_memtable.region().evictable_occupancy()) {
                    // Soft pressure, but nothing to flush. It could be due to fsync, memtable_to_cache lagging,
                    // or candidate_memtable failed to flush.
                    // Back off to avoid OOMing with flush continuations.
                    return sleep(1ms);
                }

                // Do not wait. The semaphore will protect us against a concurrent flush. But we
                // want to start a new one as soon as the permits are destroyed and the semaphore is
                // made ready again, not when we are done with the current one.
                (void)this->flush_one(mtlist, std::move(permit));
                return make_ready_future<>();
            });
        });
    }).finally([this] {
        // We'll try to acquire the permit here to make sure we only really stop when there are no
        // in-flight flushes. Our stop condition checks for the presence of waiters, but it could be
        // that we have no waiters, but a flush still in flight. We wait for all background work to
        // stop. When that stops, we know that the foreground work in the _flush_serializer has
        // stopped as well.
        return get_units(_background_work_flush_serializer, _max_background_work).discard_result();
    });
}

void dirty_memory_manager::start_reclaiming() noexcept {
    _should_flush.signal();
}

future<> database::apply_in_memory(const frozen_mutation& m, schema_ptr m_schema, db::rp_handle&& h, db::timeout_clock::time_point timeout) {
    auto& cf = find_column_family(m.column_family_id());

    data_listeners().on_write(m_schema, m);

  return cf.run_async([this, &m, m_schema = std::move(m_schema), h = std::move(h), &cf, timeout]() mutable {
    return cf.dirty_memory_region_group().run_when_memory_available([this, &m, m_schema = std::move(m_schema), h = std::move(h), &cf]() mutable {
        cf.apply(m, m_schema, std::move(h));
    }, timeout);
  });
}

future<> database::apply_in_memory(const mutation& m, column_family& cf, db::rp_handle&& h, db::timeout_clock::time_point timeout) {
  return cf.run_async([this, &m, h = std::move(h), &cf, timeout]() mutable {
    return cf.dirty_memory_region_group().run_when_memory_available([this, &m, &cf, h = std::move(h)]() mutable {
        cf.apply(m, std::move(h));
    }, timeout);
  });
}

future<mutation> database::apply_counter_update(schema_ptr s, const frozen_mutation& m, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_state) {
    if (timeout <= db::timeout_clock::now()) {
        update_write_metrics_for_timed_out_write();
        return make_exception_future<mutation>(timed_out_error{});
    }
  return update_write_metrics(seastar::futurize_invoke([&] {
    if (!s->is_synced()) {
        throw std::runtime_error(format("attempted to mutate using not synced schema of {}.{}, version={}",
                                        s->ks_name(), s->cf_name(), s->version()));
    }
    try {
        auto& cf = find_column_family(m.column_family_id());
        return do_apply_counter_update(cf, m, s, timeout, std::move(trace_state));
    } catch (no_such_column_family&) {
        dblog.error("Attempting to mutate non-existent table {}", m.column_family_id());
        throw;
    }
  }));
}

static future<> maybe_handle_reorder(std::exception_ptr exp) {
    try {
        std::rethrow_exception(exp);
        return make_exception_future(exp);
    } catch (mutation_reordered_with_truncate_exception&) {
        // This mutation raced with a truncate, so we can just drop it.
        dblog.debug("replay_position reordering detected");
        return make_ready_future<>();
    }
}

future<> database::apply_with_commitlog(column_family& cf, const mutation& m, db::timeout_clock::time_point timeout) {
    if (cf.commitlog() != nullptr && cf.durable_writes()) {
        return do_with(freeze(m), [this, &m, &cf, timeout] (frozen_mutation& fm) {
            commitlog_entry_writer cew(m.schema(), fm, db::commitlog::force_sync::no);
            return cf.commitlog()->add_entry(m.schema()->id(), cew, timeout);
        }).then([this, &m, &cf, timeout] (db::rp_handle h) {
            return apply_in_memory(m, cf, std::move(h), timeout).handle_exception(maybe_handle_reorder);
        });
    }
    return apply_in_memory(m, cf, {}, timeout);
}

future<> database::apply_with_commitlog(schema_ptr s, column_family& cf, utils::UUID uuid, const frozen_mutation& m, db::timeout_clock::time_point timeout,
        db::commitlog::force_sync sync) {
    auto cl = cf.commitlog();
    if (cl != nullptr && cf.durable_writes()) {
        commitlog_entry_writer cew(s, m, sync);
        return cf.commitlog()->add_entry(uuid, cew, timeout).then([&m, this, s, timeout, cl](db::rp_handle h) {
            return this->apply_in_memory(m, s, std::move(h), timeout).handle_exception(maybe_handle_reorder);
        });
    }
    return apply_in_memory(m, std::move(s), {}, timeout);
}

future<> database::do_apply(schema_ptr s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::timeout_clock::time_point timeout, db::commitlog::force_sync sync) {
    // I'm doing a nullcheck here since the init code path for db etc
    // is a little in flux and commitlog is created only when db is
    // initied from datadir.
    auto uuid = m.column_family_id();
    auto& cf = find_column_family(uuid);
    if (!s->is_synced()) {
        throw std::runtime_error(format("attempted to mutate using not synced schema of {}.{}, version={}",
                                 s->ks_name(), s->cf_name(), s->version()));
    }

    sync = sync || db::commitlog::force_sync(s->wait_for_sync_to_commitlog());

    // Signal to view building code that a write is in progress,
    // so it knows when new writes start being sent to a new view.
    auto op = cf.write_in_progress();
    if (cf.views().empty()) {
        return apply_with_commitlog(std::move(s), cf, std::move(uuid), m, timeout, sync).finally([op = std::move(op)] { });
    }
    future<row_locker::lock_holder> f = cf.push_view_replica_updates(s, m, timeout, std::move(tr_state), get_reader_concurrency_semaphore());
    return f.then([this, s = std::move(s), uuid = std::move(uuid), &m, timeout, &cf, op = std::move(op), sync] (row_locker::lock_holder lock) mutable {
        return apply_with_commitlog(std::move(s), cf, std::move(uuid), m, timeout, sync).finally(
                // Hold the local lock on the base-table partition or row
                // taken before the read, until the update is done.
                [lock = std::move(lock), op = std::move(op)] { });
    });
}

template<typename Future>
Future database::update_write_metrics(Future&& f) {
    return f.then_wrapped([this, s = _stats] (auto f) {
        if (f.failed()) {
            ++s->total_writes_failed;
            try {
                f.get();
            } catch (const timed_out_error&) {
                ++s->total_writes_timedout;
                throw;
            }
            assert(0 && "should not reach");
        }
        ++s->total_writes;
        return f;
    });
}

void database::update_write_metrics_for_timed_out_write() {
    ++_stats->total_writes;
    ++_stats->total_writes_failed;
    ++_stats->total_writes_timedout;
}

future<> database::apply(schema_ptr s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, db::timeout_clock::time_point timeout) {
    if (dblog.is_enabled(logging::log_level::trace)) {
        dblog.trace("apply {}", m.pretty_printer(s));
    }
    if (timeout <= db::timeout_clock::now()) {
        update_write_metrics_for_timed_out_write();
        return make_exception_future<>(timed_out_error{});
    }
    return update_write_metrics(_apply_stage(this, std::move(s), seastar::cref(m), std::move(tr_state), timeout, sync));
}

future<> database::apply_hint(schema_ptr s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::timeout_clock::time_point timeout) {
    if (dblog.is_enabled(logging::log_level::trace)) {
        dblog.trace("apply hint {}", m.pretty_printer(s));
    }
    return with_scheduling_group(_dbcfg.streaming_scheduling_group, [this, s = std::move(s), &m, tr_state = std::move(tr_state), timeout] () mutable {
        return update_write_metrics(_apply_stage(this, std::move(s), seastar::cref(m), std::move(tr_state), timeout, db::commitlog::force_sync::no));
    });
}

keyspace::config
database::make_keyspace_config(const keyspace_metadata& ksm) {
    keyspace::config cfg;
    if (_cfg.data_file_directories().size() > 0) {
        cfg.datadir = format("{}/{}", _cfg.data_file_directories()[0], ksm.name());
        for (auto& extra : _cfg.data_file_directories()) {
            cfg.all_datadirs.push_back(format("{}/{}", extra, ksm.name()));
        }
        cfg.enable_disk_writes = !_cfg.enable_in_memory_data_store();
        cfg.enable_disk_reads = true; // we allways read from disk
        cfg.enable_commitlog = _cfg.enable_commitlog() && !_cfg.enable_in_memory_data_store();
        cfg.enable_cache = _cfg.enable_cache();

    } else {
        cfg.datadir = "";
        cfg.enable_disk_writes = false;
        cfg.enable_disk_reads = false;
        cfg.enable_commitlog = false;
        cfg.enable_cache = false;
    }
    cfg.enable_dangerous_direct_import_of_cassandra_counters = _cfg.enable_dangerous_direct_import_of_cassandra_counters();
    cfg.compaction_enforce_min_threshold = _cfg.compaction_enforce_min_threshold;
    cfg.dirty_memory_manager = &_dirty_memory_manager;
    cfg.streaming_read_concurrency_semaphore = &_streaming_concurrency_sem;
    cfg.compaction_concurrency_semaphore = &_compaction_concurrency_sem;
    cfg.cf_stats = &_cf_stats;
    cfg.enable_incremental_backups = _enable_incremental_backups;

    cfg.compaction_scheduling_group = _dbcfg.compaction_scheduling_group;
    cfg.memory_compaction_scheduling_group = _dbcfg.memory_compaction_scheduling_group;
    cfg.memtable_scheduling_group = _dbcfg.memtable_scheduling_group;
    cfg.memtable_to_cache_scheduling_group = _dbcfg.memtable_to_cache_scheduling_group;
    cfg.streaming_scheduling_group = _dbcfg.streaming_scheduling_group;
    cfg.statement_scheduling_group = _dbcfg.statement_scheduling_group;
    cfg.enable_metrics_reporting = _cfg.enable_keyspace_column_family_metrics();

    cfg.view_update_concurrency_semaphore = &_view_update_concurrency_sem;
    cfg.view_update_concurrency_semaphore_limit = max_memory_pending_view_updates();
    return cfg;
}

namespace db {

std::ostream& operator<<(std::ostream& os, const write_type& t) {
    switch (t) {
        case write_type::SIMPLE: return os << "SIMPLE";
        case write_type::BATCH: return os << "BATCH";
        case write_type::UNLOGGED_BATCH: return os << "UNLOGGED_BATCH";
        case write_type::COUNTER: return os << "COUNTER";
        case write_type::BATCH_LOG: return os << "BATCH_LOG";
        case write_type::CAS: return os << "CAS";
        case write_type::VIEW: return os << "VIEW";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, db::consistency_level cl) {
    switch (cl) {
    case db::consistency_level::ANY: return os << "ANY";
    case db::consistency_level::ONE: return os << "ONE";
    case db::consistency_level::TWO: return os << "TWO";
    case db::consistency_level::THREE: return os << "THREE";
    case db::consistency_level::QUORUM: return os << "QUORUM";
    case db::consistency_level::ALL: return os << "ALL";
    case db::consistency_level::LOCAL_QUORUM: return os << "LOCAL_QUORUM";
    case db::consistency_level::EACH_QUORUM: return os << "EACH_QUORUM";
    case db::consistency_level::SERIAL: return os << "SERIAL";
    case db::consistency_level::LOCAL_SERIAL: return os << "LOCAL_SERIAL";
    case db::consistency_level::LOCAL_ONE: return os << "LOCAL_ONE";
    default: abort();
    }
}

}

std::ostream&
operator<<(std::ostream& os, const exploded_clustering_prefix& ecp) {
    // Can't pass to_hex() to transformed(), since it is overloaded, so wrap:
    auto enhex = [] (auto&& x) { return to_hex(x); };
    return fmt_print(os, "prefix{{{}}}", ::join(":", ecp._v | boost::adaptors::transformed(enhex)));
}

sstring database::get_available_index_name(const sstring &ks_name, const sstring &cf_name,
                                           std::optional<sstring> index_name_root) const
{
    auto existing_names = existing_index_names(ks_name);
    auto base_name = index_metadata::get_default_index_name(cf_name, index_name_root);
    sstring accepted_name = base_name;
    int i = 0;
    auto name_accepted = [&] {
        auto index_table_name = secondary_index::index_table_name(accepted_name);
        return !has_schema(ks_name, index_table_name) && !existing_names.contains(accepted_name);
    };
    while (!name_accepted()) {
        accepted_name = base_name + "_" + std::to_string(++i);
    }
    return accepted_name;
}

schema_ptr database::find_indexed_table(const sstring& ks_name, const sstring& index_name) const {
    for (auto& schema : find_keyspace(ks_name).metadata()->tables()) {
        if (schema->has_index(index_name)) {
            return schema;
        }
    }
    return nullptr;
}

future<> database::close_tables(table_kind kind_to_close) {
    return parallel_for_each(_column_families, [this, kind_to_close](auto& val_pair) {
        table_kind k = is_system_table(*val_pair.second->schema()) ? table_kind::system : table_kind::user;
        if (k == kind_to_close) {
            return val_pair.second->stop();
        } else {
            return make_ready_future<>();
        }
    });
}

future<> start_large_data_handler(sharded<database>& db) {
    return db.invoke_on_all([](database& db) {
        db.get_large_data_handler()->start();
    });
}

future<> stop_database(sharded<database>& sdb) {
    return sdb.invoke_on_all([](database& db) {
        return db.get_compaction_manager().stop();
    }).then([&sdb] {
        // Closing a table can cause us to find a large partition. Since we want to record that, we have to close
        // system.large_partitions after the regular tables.
        return sdb.invoke_on_all([](database& db) {
            return db.close_tables(database::table_kind::user);
        });
    }).then([&sdb] {
        return sdb.invoke_on_all([](database& db) {
            return db.close_tables(database::table_kind::system);
        });
    }).then([&sdb] {
        return sdb.invoke_on_all([](database& db) {
            return db.stop_large_data_handler();
        });
    });
}

future<> database::stop_large_data_handler() {
    return _large_data_handler->stop();
}

void database::revert_initial_system_read_concurrency_boost() {
    _system_read_concurrency_sem.consume({database::max_count_concurrent_reads - database::max_count_system_concurrent_reads, 0});
    dblog.debug("Reverted system read concurrency from initial {} to normal {}", database::max_count_concurrent_reads, database::max_count_system_concurrent_reads);
}

future<>
database::stop() {
    assert(!_large_data_handler->running());

    // try to ensure that CL has done disk flushing
    future<> maybe_shutdown_commitlog = _commitlog != nullptr ? _commitlog->shutdown() : make_ready_future<>();
    return maybe_shutdown_commitlog.then([this] {
        return _view_update_concurrency_sem.wait(max_memory_pending_view_updates());
    }).then([this] {
        if (_commitlog != nullptr) {
            return _commitlog->release();
        }
        return make_ready_future<>();
    }).then([this] {
        return _system_dirty_memory_manager.shutdown();
    }).then([this] {
        return _dirty_memory_manager.shutdown();
    }).then([this] {
        return _memtable_controller.shutdown();
    }).then([this] {
        return _user_sstables_manager->close();
    }).then([this] {
        return _system_sstables_manager->close();
    }).finally([this] {
        return when_all_succeed(
                _read_concurrency_sem.stop(),
                _streaming_concurrency_sem.stop(),
                _compaction_concurrency_sem.stop(),
                _system_read_concurrency_sem.stop()).discard_result().finally([this] {
            return _querier_cache.stop();
        });
    });
}

future<> database::flush_all_memtables() {
    return parallel_for_each(_column_families, [this] (auto& cfp) {
        return cfp.second->flush();
    });
}

future<> database::flush(const sstring& ksname, const sstring& cfname) {
    auto& cf = find_column_family(ksname, cfname);
    return cf.flush();
}

future<> database::truncate(sstring ksname, sstring cfname, timestamp_func tsf) {
    auto& ks = find_keyspace(ksname);
    auto& cf = find_column_family(ksname, cfname);
    return truncate(ks, cf, std::move(tsf));
}

future<> database::truncate(const keyspace& ks, column_family& cf, timestamp_func tsf, bool with_snapshot) {
    return cf.run_async([this, &ks, &cf, tsf = std::move(tsf), with_snapshot] {
        const auto auto_snapshot = with_snapshot && get_config().auto_snapshot();
        const auto should_flush = auto_snapshot;

        // Force mutations coming in to re-acquire higher rp:s
        // This creates a "soft" ordering, in that we will guarantee that
        // any sstable written _after_ we issue the flush below will
        // only have higher rp:s than we will get from the discard_sstable
        // call.
        auto low_mark = cf.set_low_replay_position_mark();


        return cf.run_with_compaction_disabled([this, &cf, should_flush, auto_snapshot, tsf = std::move(tsf), low_mark]() mutable {
            future<> f = make_ready_future<>();
            bool did_flush = false;
            if (should_flush && cf.can_flush()) {
                // TODO:
                // this is not really a guarantee at all that we've actually
                // gotten all things to disk. Again, need queue-ish or something.
                f = cf.flush();
                did_flush = true;
            } else {
                f = cf.clear();
            }
            return f.then([this, &cf, auto_snapshot, tsf = std::move(tsf), low_mark, should_flush, did_flush] {
                dblog.debug("Discarding sstable data for truncated CF + indexes");
                // TODO: notify truncation

                return tsf().then([this, &cf, auto_snapshot, low_mark, should_flush, did_flush](db_clock::time_point truncated_at) {
                    future<> f = make_ready_future<>();
                    if (auto_snapshot) {
                        auto name = format("{:d}-{}", truncated_at.time_since_epoch().count(), cf.schema()->cf_name());
                        f = cf.snapshot(*this, name);
                    }
                    return f.then([this, &cf, truncated_at, low_mark, should_flush, did_flush] {
                        return cf.discard_sstables(truncated_at).then([this, &cf, truncated_at, low_mark, should_flush, did_flush](db::replay_position rp) {
                            // TODO: indexes.
                            // Note: since discard_sstables was changed to only count tables owned by this shard,
                            // we can get zero rp back. Changed assert, and ensure we save at least low_mark.
                            // #6995 - the assert below was broken in c2c6c71 and remained so for many years. 
                            // We nowadays do not flush tables with sstables but autosnapshot=false. This means
                            // the low_mark assertion does not hold, because we maybe/probably never got around to 
                            // creating the sstables that would create them.
                            assert(!did_flush || low_mark <= rp || rp == db::replay_position());
                            rp = std::max(low_mark, rp);
                            return truncate_views(cf, truncated_at, should_flush).then([&cf, truncated_at, rp] {
                                // save_truncation_record() may actually fail after we cached the truncation time
                                // but this is not be worse that if failing without caching: at least the correct time
                                // will be available until next reboot and a client will have to retry truncation anyway.
                                cf.cache_truncation_record(truncated_at);
                                return db::system_keyspace::save_truncation_record(cf, truncated_at, rp);
                            });
                        });
                    });
                });
            });
        });
    });
}

future<> database::truncate_views(const column_family& base, db_clock::time_point truncated_at, bool should_flush) {
    return parallel_for_each(base.views(), [this, truncated_at, should_flush] (view_ptr v) {
        auto& vcf = find_column_family(v);
        return vcf.run_with_compaction_disabled([&vcf, truncated_at, should_flush] {
            return (should_flush ? vcf.flush() : vcf.clear()).then([&vcf, truncated_at, should_flush] {
                return vcf.discard_sstables(truncated_at).then([&vcf, truncated_at, should_flush](db::replay_position rp) {
                    return db::system_keyspace::save_truncation_record(vcf, truncated_at, rp);
                });
            });
        });
    });
}

const sstring& database::get_snitch_name() const {
    return _cfg.endpoint_snitch();
}

dht::token_range_vector database::get_keyspace_local_ranges(sstring ks) {
    return find_keyspace(ks).get_replication_strategy().get_ranges(utils::fb_utilities::get_broadcast_address());
}

/*!
 * \brief a helper function that gets a table name and returns a prefix
 * of the directory name of the table.
 */
static sstring get_snapshot_table_dir_prefix(const sstring& table_name) {
    return table_name + "-";
}

// For the filesystem operations, this code will assume that all keyspaces are visible in all shards
// (as we have been doing for a lot of the other operations, like the snapshot itself).
future<> database::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, const sstring& table_name) {
    std::vector<sstring> data_dirs = _cfg.data_file_directories();
    auto dirs_only_entries_ptr =
        make_lw_shared<lister::dir_entry_types>(lister::dir_entry_types{directory_entry_type::directory});
    lw_shared_ptr<sstring> tag_ptr = make_lw_shared<sstring>(std::move(tag));
    std::unordered_set<sstring> ks_names_set(keyspace_names.begin(), keyspace_names.end());

    return parallel_for_each(data_dirs, [this, tag_ptr, ks_names_set = std::move(ks_names_set), dirs_only_entries_ptr, table_name = table_name] (const sstring& parent_dir) {
        std::unique_ptr<lister::filter_type> filter = std::make_unique<lister::filter_type>([] (const fs::path& parent_dir, const directory_entry& dir_entry) { return true; });

        lister::filter_type table_filter = (table_name.empty()) ? lister::filter_type([] (const fs::path& parent_dir, const directory_entry& dir_entry) mutable { return true; }) :
                lister::filter_type([table_name = get_snapshot_table_dir_prefix(table_name)] (const fs::path& parent_dir, const directory_entry& dir_entry) mutable {
                    return dir_entry.name.find(table_name) == 0;
                });
        // if specific keyspaces names were given - filter only these keyspaces directories
        if (!ks_names_set.empty()) {
            filter = std::make_unique<lister::filter_type>([ks_names_set = std::move(ks_names_set)] (const fs::path& parent_dir, const directory_entry& dir_entry) {
                return ks_names_set.contains(dir_entry.name);
            });
        }

        //
        // The keyspace data directories and their snapshots are arranged as follows:
        //
        //  <data dir>
        //  |- <keyspace name1>
        //  |  |- <column family name1>
        //  |     |- snapshots
        //  |        |- <snapshot name1>
        //  |          |- <snapshot file1>
        //  |          |- <snapshot file2>
        //  |          |- ...
        //  |        |- <snapshot name2>
        //  |        |- ...
        //  |  |- <column family name2>
        //  |  |- ...
        //  |- <keyspace name2>
        //  |- ...
        //
        return lister::scan_dir(parent_dir, *dirs_only_entries_ptr, [this, tag_ptr, dirs_only_entries_ptr, table_filter = std::move(table_filter)] (fs::path parent_dir, directory_entry de) mutable {
            // KS directory
            return lister::scan_dir(parent_dir / de.name, *dirs_only_entries_ptr, [this, tag_ptr, dirs_only_entries_ptr] (fs::path parent_dir, directory_entry de) mutable {
                // CF directory
                return lister::scan_dir(parent_dir / de.name, *dirs_only_entries_ptr, [this, tag_ptr, dirs_only_entries_ptr] (fs::path parent_dir, directory_entry de) mutable {
                    // "snapshots" directory
                    fs::path snapshots_dir(parent_dir / de.name);
                    if (tag_ptr->empty()) {
                        dblog.info("Removing {}", snapshots_dir.native());
                        // kill the whole "snapshots" subdirectory
                        return lister::rmdir(std::move(snapshots_dir));
                    } else {
                        return lister::scan_dir(std::move(snapshots_dir), *dirs_only_entries_ptr, [this, tag_ptr] (fs::path parent_dir, directory_entry de) {
                            fs::path snapshot_dir(parent_dir / de.name);
                            dblog.info("Removing {}", snapshot_dir.native());
                            return lister::rmdir(std::move(snapshot_dir));
                        }, [tag_ptr] (const fs::path& parent_dir, const directory_entry& dir_entry) { return dir_entry.name == *tag_ptr; });
                    }
                 }, [] (const fs::path& parent_dir, const directory_entry& dir_entry) { return dir_entry.name == "snapshots"; });
            }, table_filter);
        }, *filter);
    });
}

std::ostream& operator<<(std::ostream& os, const user_types_metadata& m) {
    os << "org.apache.cassandra.config.UTMetaData@" << &m;
    return os;
}

std::ostream& operator<<(std::ostream& os, const keyspace_metadata& m) {
    os << "KSMetaData{";
    os << "name=" << m._name;
    os << ", strategyClass=" << m._strategy_name;
    os << ", strategyOptions={";
    int n = 0;
    for (auto& p : m._strategy_options) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ", cfMetaData={";
    n = 0;
    for (auto& p : m._cf_meta_data) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ", durable_writes=" << m._durable_writes;
    os << ", userTypes=" << m._user_types;
    os << "}";
    return os;
}

template <typename T>
using foreign_unique_ptr = foreign_ptr<std::unique_ptr<T>>;

flat_mutation_reader make_multishard_streaming_reader(distributed<database>& db, schema_ptr schema,
        std::function<std::optional<dht::partition_range>()> range_generator) {
    class streaming_reader_lifecycle_policy
            : public reader_lifecycle_policy
            , public enable_shared_from_this<streaming_reader_lifecycle_policy> {
        struct reader_context {
            foreign_unique_ptr<const dht::partition_range> range;
            foreign_unique_ptr<utils::phased_barrier::operation> read_operation;
            reader_concurrency_semaphore* semaphore;
        };
        distributed<database>& _db;
        utils::UUID _table_id;
        std::vector<reader_context> _contexts;
    public:
        streaming_reader_lifecycle_policy(distributed<database>& db, utils::UUID table_id) : _db(db), _table_id(table_id), _contexts(smp::count) {
        }
        virtual flat_mutation_reader create_reader(
                schema_ptr schema,
                reader_permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr,
                mutation_reader::forwarding fwd_mr) override {
            const auto shard = this_shard_id();
            auto& cf = _db.local().find_column_family(schema);

            _contexts[shard].range = make_foreign(std::make_unique<const dht::partition_range>(range));
            _contexts[shard].read_operation = make_foreign(std::make_unique<utils::phased_barrier::operation>(cf.read_in_progress()));
            _contexts[shard].semaphore = &cf.streaming_read_concurrency_semaphore();

            return cf.make_streaming_reader(std::move(schema), *_contexts[shard].range, slice, fwd_mr);
        }
        virtual future<> destroy_reader(shard_id shard, future<stopped_reader> reader_fut) noexcept override {
            return reader_fut.then([this, zis = shared_from_this(), shard] (stopped_reader&& reader) mutable {
                return smp::submit_to(shard, [ctx = std::move(_contexts[shard]), handle = std::move(reader.handle)] () mutable {
                    auto reader_opt = ctx.semaphore->unregister_inactive_read(std::move(*handle));
                    return reader_opt ? reader_opt->close() : make_ready_future<>();
                });
            }).handle_exception([shard] (std::exception_ptr e) {
                dblog.warn("Failed to destroy shard reader of streaming multishard reader on shard {}: {}", shard, e);
            });
        }
        virtual reader_concurrency_semaphore& semaphore() override {
            const auto shard = this_shard_id();
            if (!_contexts[shard].semaphore) {
                auto& cf = _db.local().find_column_family(_table_id);
                _contexts[shard].semaphore = &cf.streaming_read_concurrency_semaphore();
            }
            return *_contexts[shard].semaphore;
        }
    };
    auto ms = mutation_source([&db] (schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding,
            mutation_reader::forwarding fwd_mr) {
        auto table_id = s->id();
        return make_multishard_combining_reader(make_shared<streaming_reader_lifecycle_policy>(db, table_id), std::move(s), std::move(permit), pr, ps, pc,
                std::move(trace_state), fwd_mr);
    });
    auto&& full_slice = schema->full_slice();
    auto& cf = db.local().find_column_family(schema);
    return make_flat_multi_range_reader(schema, cf.streaming_read_concurrency_semaphore().make_permit(schema.get(), "multishard-streaming-reader"), std::move(ms),
            std::move(range_generator), std::move(full_slice), service::get_local_streaming_priority(), {}, mutation_reader::forwarding::no);
}

std::ostream& operator<<(std::ostream& os, gc_clock::time_point tp) {
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
    std::ostream tmp(os.rdbuf());
    tmp << std::setw(12) << sec;
    return os;
}

const timeout_config infinite_timeout_config = {
        // not really infinite, but long enough
        1h, 1h, 1h, 1h, 1h, 1h, 1h,
};
