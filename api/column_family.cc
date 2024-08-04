/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>
#include "column_family.hh"
#include "api/api.hh"
#include "api/api-doc/column_family.json.hh"
#include "api/api-doc/storage_service.json.hh"
#include <vector>
#include <seastar/http/exception.hh>
#include "sstables/sstables.hh"
#include "sstables/metadata_collector.hh"
#include "utils/assert.hh"
#include "utils/estimated_histogram.hh"
#include <algorithm>
#include "db/system_keyspace.hh"
#include "db/data_listeners.hh"
#include "storage_service.hh"
#include "compaction/compaction_manager.hh"
#include "unimplemented.hh"

extern logging::logger apilog;

namespace api {
using namespace httpd;

using namespace json;
namespace cf = httpd::column_family_json;
namespace ss = httpd::storage_service_json;

std::tuple<sstring, sstring> parse_fully_qualified_cf_name(sstring name) {
    auto pos = name.find("%3A");
    size_t end;
    if (pos == sstring::npos) {
        pos  = name.find(":");
        if (pos == sstring::npos) {
            throw bad_param_exception("Column family name should be in keyspace:column_family format");
        }
        end = pos + 1;
    } else {
        end = pos + 3;
    }
    return std::make_tuple(name.substr(0, pos), name.substr(end));
}

table_id get_uuid(const sstring& ks, const sstring& cf, const replica::database& db) {
    try {
        return db.find_uuid(ks, cf);
    } catch (replica::no_such_column_family& e) {
        throw bad_param_exception(e.what());
    }
}

table_id get_uuid(const sstring& name, const replica::database& db) {
    auto [ks, cf] = parse_fully_qualified_cf_name(name);
    return get_uuid(ks, cf, db);
}

future<> foreach_column_family(http_context& ctx, const sstring& name, std::function<void(replica::column_family&)> f) {
    auto uuid = get_uuid(name, ctx.db.local());

    return ctx.db.invoke_on_all([f, uuid](replica::database& db) {
        f(db.find_column_family(uuid));
    });
}

future<json::json_return_type>  get_cf_stats(http_context& ctx, const sstring& name,
        int64_t replica::column_family_stats::*f) {
    return map_reduce_cf(ctx, name, int64_t(0), [f](const replica::column_family& cf) {
        return cf.get_stats().*f;
    }, std::plus<int64_t>());
}

future<json::json_return_type>  get_cf_stats(http_context& ctx,
        int64_t replica::column_family_stats::*f) {
    return map_reduce_cf(ctx, int64_t(0), [f](const replica::column_family& cf) {
        return cf.get_stats().*f;
    }, std::plus<int64_t>());
}

static future<json::json_return_type> set_tables(http_context& ctx, const sstring& keyspace, std::vector<sstring> tables, std::function<future<>(replica::table&)> set) {
    if (tables.empty()) {
        tables = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
    }

    return do_with(keyspace, std::move(tables), [&ctx, set] (const sstring& keyspace, const std::vector<sstring>& tables) {
        return ctx.db.invoke_on_all([&keyspace, &tables, set] (replica::database& db) {
            return parallel_for_each(tables, [&db, &keyspace, set] (const sstring& table) {
                replica::table& t = db.find_column_family(keyspace, table);
                return set(t);
            });
        });
    }).then([] {
        return make_ready_future<json::json_return_type>(json_void());
    });
}

class autocompaction_toggle_guard {
    replica::database& _db;
public:
    autocompaction_toggle_guard(replica::database& db) : _db(db) {
        SCYLLA_ASSERT(this_shard_id() == 0);
        if (!_db._enable_autocompaction_toggle) {
            throw std::runtime_error("Autocompaction toggle is busy");
        }
        _db._enable_autocompaction_toggle = false;
    }
    autocompaction_toggle_guard(const autocompaction_toggle_guard&) = delete;
    autocompaction_toggle_guard(autocompaction_toggle_guard&&) = default;
    ~autocompaction_toggle_guard() {
        SCYLLA_ASSERT(this_shard_id() == 0);
        _db._enable_autocompaction_toggle = true;
    }
};

static future<json::json_return_type> set_tables_autocompaction(http_context& ctx, const sstring &keyspace, std::vector<sstring> tables, bool enabled) {
    apilog.info("set_tables_autocompaction: enabled={} keyspace={} tables={}", enabled, keyspace, tables);

    return ctx.db.invoke_on(0, [&ctx, keyspace, tables = std::move(tables), enabled] (replica::database& db) {
        auto g = autocompaction_toggle_guard(db);
        return set_tables(ctx, keyspace, tables, [enabled] (replica::table& cf) {
            if (enabled) {
                cf.enable_auto_compaction();
            } else {
                return cf.disable_auto_compaction();
            }
            return make_ready_future<>();
        }).finally([g = std::move(g)] {});
    });
}

static future<json::json_return_type> set_tables_tombstone_gc(http_context& ctx, const sstring &keyspace, std::vector<sstring> tables, bool enabled) {
    apilog.info("set_tables_tombstone_gc: enabled={} keyspace={} tables={}", enabled, keyspace, tables);
    return set_tables(ctx, keyspace, std::move(tables), [enabled] (replica::table& t) {
        t.set_tombstone_gc_enabled(enabled);
        return make_ready_future<>();
    });
}

static future<json::json_return_type>  get_cf_stats_count(http_context& ctx, const sstring& name,
        utils::timed_rate_moving_average_summary_and_histogram replica::column_family_stats::*f) {
    return map_reduce_cf(ctx, name, int64_t(0), [f](const replica::column_family& cf) {
        return (cf.get_stats().*f).hist.count;
    }, std::plus<int64_t>());
}

static future<json::json_return_type>  get_cf_stats_sum(http_context& ctx, const sstring& name,
        utils::timed_rate_moving_average_summary_and_histogram replica::column_family_stats::*f) {
    auto uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([uuid, f](replica::database& db) {
        // Histograms information is sample of the actual load
        // so to get an estimation of sum, we multiply the mean
        // with count. The information is gather in nano second,
        // but reported in micro
        replica::column_family& cf = db.find_column_family(uuid);
        return ((cf.get_stats().*f).hist.count/1000.0) * (cf.get_stats().*f).hist.mean;
    }, 0.0, std::plus<double>()).then([](double res) {
        return make_ready_future<json::json_return_type>((int64_t)res);
    });
}


static future<json::json_return_type>  get_cf_stats_count(http_context& ctx,
        utils::timed_rate_moving_average_summary_and_histogram replica::column_family_stats::*f) {
    return map_reduce_cf(ctx, int64_t(0), [f](const replica::column_family& cf) {
        return (cf.get_stats().*f).hist.count;
    }, std::plus<int64_t>());
}

static future<json::json_return_type>  get_cf_histogram(http_context& ctx, const sstring& name,
        utils::timed_rate_moving_average_and_histogram replica::column_family_stats::*f) {
    auto uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([f, uuid](const replica::database& p) {
        return (p.find_column_family(uuid).get_stats().*f).hist;},
            utils::ihistogram(),
            std::plus<utils::ihistogram>())
            .then([](const utils::ihistogram& val) {
                return make_ready_future<json::json_return_type>(to_json(val));
    });
}

static future<json::json_return_type>  get_cf_histogram(http_context& ctx, const sstring& name,
        utils::timed_rate_moving_average_summary_and_histogram replica::column_family_stats::*f) {
    auto uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([f, uuid](const replica::database& p) {
        return (p.find_column_family(uuid).get_stats().*f).hist;},
            utils::ihistogram(),
            std::plus<utils::ihistogram>())
            .then([](const utils::ihistogram& val) {
                return make_ready_future<json::json_return_type>(to_json(val));
    });
}

static future<json::json_return_type> get_cf_histogram(http_context& ctx, utils::timed_rate_moving_average_summary_and_histogram replica::column_family_stats::*f) {
    std::function<utils::ihistogram(const replica::database&)> fun = [f] (const replica::database& db)  {
        utils::ihistogram res;
        db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> table) mutable {
            res += (table->get_stats().*f).hist;
        });
        return res;
    };
    return ctx.db.map(fun).then([](const std::vector<utils::ihistogram> &res) {
        std::vector<httpd::utils_json::histogram> r;
        boost::copy(res | boost::adaptors::transformed(to_json), std::back_inserter(r));
        return make_ready_future<json::json_return_type>(r);
    });
}

static future<json::json_return_type>  get_cf_rate_and_histogram(http_context& ctx, const sstring& name,
        utils::timed_rate_moving_average_summary_and_histogram replica::column_family_stats::*f) {
    auto uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([f, uuid](const replica::database& p) {
        return (p.find_column_family(uuid).get_stats().*f).rate();},
            utils::rate_moving_average_and_histogram(),
            std::plus<utils::rate_moving_average_and_histogram>())
            .then([](const utils::rate_moving_average_and_histogram& val) {
                return make_ready_future<json::json_return_type>(timer_to_json(val));
    });
}

static future<json::json_return_type> get_cf_rate_and_histogram(http_context& ctx, utils::timed_rate_moving_average_summary_and_histogram replica::column_family_stats::*f) {
    std::function<utils::rate_moving_average_and_histogram(const replica::database&)> fun = [f] (const replica::database& db)  {
        utils::rate_moving_average_and_histogram res;
        db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> table) {
            res += (table->get_stats().*f).rate();
        });
        return res;
    };
    return ctx.db.map(fun).then([](const std::vector<utils::rate_moving_average_and_histogram> &res) {
        std::vector<httpd::utils_json::rate_moving_average_and_histogram> r;
        boost::copy(res | boost::adaptors::transformed(timer_to_json), std::back_inserter(r));
        return make_ready_future<json::json_return_type>(r);
    });
}

static future<json::json_return_type> get_cf_unleveled_sstables(http_context& ctx, const sstring& name) {
    return map_reduce_cf(ctx, name, int64_t(0), [](const replica::column_family& cf) {
        return cf.get_unleveled_sstables();
    }, std::plus<int64_t>());
}

static int64_t min_partition_size(replica::column_family& cf) {
    int64_t res = INT64_MAX;
    for (auto sstables = cf.get_sstables(); auto& i : *sstables) {
        res = std::min(res, i->get_stats_metadata().estimated_partition_size.min());
    }
    return (res == INT64_MAX) ? 0 : res;
}

static int64_t max_partition_size(replica::column_family& cf) {
    int64_t res = 0;
    for (auto sstables = cf.get_sstables(); auto& i : *sstables) {
        res = std::max(i->get_stats_metadata().estimated_partition_size.max(), res);
    }
    return res;
}

static integral_ratio_holder mean_partition_size(replica::column_family& cf) {
    integral_ratio_holder res;
    for (auto sstables = cf.get_sstables(); auto& i : *sstables) {
        auto c = i->get_stats_metadata().estimated_partition_size.count();
        res.sub += i->get_stats_metadata().estimated_partition_size.mean() * c;
        res.total += c;
    }
    return res;
}

static std::unordered_map<sstring, uint64_t> merge_maps(std::unordered_map<sstring, uint64_t> a,
        const std::unordered_map<sstring, uint64_t>& b) {
    a.insert(b.begin(), b.end());
    return a;
}

static json::json_return_type sum_map(const std::unordered_map<sstring, uint64_t>& val) {
    uint64_t res = 0;
    for (auto i : val) {
        res += i.second;
    }
    return res;
}

static future<json::json_return_type>  sum_sstable(http_context& ctx, const sstring name, bool total) {
    auto uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([uuid, total](replica::database& db) {
        std::unordered_map<sstring, uint64_t> m;
        auto sstables = (total) ? db.find_column_family(uuid).get_sstables_including_compacted_undeleted() :
                db.find_column_family(uuid).get_sstables();
        for (auto t : *sstables) {
            m[t->get_filename()] = t->bytes_on_disk();
        }
        return m;
    }, std::unordered_map<sstring, uint64_t>(), merge_maps).
            then([](const std::unordered_map<sstring, uint64_t>& val) {
        return sum_map(val);
    });
}


static future<json::json_return_type> sum_sstable(http_context& ctx, bool total) {
    return map_reduce_cf_raw(ctx, std::unordered_map<sstring, uint64_t>(), [total](replica::column_family& cf) {
        std::unordered_map<sstring, uint64_t> m;
        auto sstables = (total) ? cf.get_sstables_including_compacted_undeleted() :
                cf.get_sstables();
        for (auto t : *sstables) {
            m[t->get_filename()] = t->bytes_on_disk();
        }
        return m;
    },merge_maps).then([](const std::unordered_map<sstring, uint64_t>& val) {
        return sum_map(val);
    });
}

future<json::json_return_type> map_reduce_cf_time_histogram(http_context& ctx, const sstring& name, std::function<utils::time_estimated_histogram(const replica::column_family&)> f) {
    return map_reduce_cf_raw(ctx, name, utils::time_estimated_histogram(), f, utils::time_estimated_histogram_merge).then([](const utils::time_estimated_histogram& res) {
        return make_ready_future<json::json_return_type>(time_to_json_histogram(res));
    });
}

template <typename T>
class sum_ratio {
    uint64_t _n = 0;
    T _total = 0;
public:
    void operator()(T value) {
        if (value > 0) {
            _total += value;
            _n++;
        }
    }
    // Returns average value of all registered ratios.
    T get() && {
        return _n ? (_total / _n) : T(0);
    }
};

static double get_compression_ratio(replica::column_family& cf) {
    sum_ratio<double> result;
    for (auto sstables = cf.get_sstables(); auto& i : *sstables) {
        auto compression_ratio = i->get_compression_ratio();
        if (compression_ratio != sstables::metadata_collector::NO_COMPRESSION_RATIO) {
            result(compression_ratio);
        }
    }
    return std::move(result).get();
}

static std::vector<uint64_t> concat_sstable_count_per_level(std::vector<uint64_t> a, std::vector<uint64_t>&& b) {
    a.resize(std::max(a.size(), b.size()), 0UL);
    for (auto i = 0U; i < b.size(); i++) {
        a[i] += b[i];
    }
    return a;
}

ratio_holder filter_false_positive_as_ratio_holder(const sstables::shared_sstable& sst) {
    double f = sst->filter_get_false_positive();
    return ratio_holder(f + sst->filter_get_true_positive(), f);
}

ratio_holder filter_recent_false_positive_as_ratio_holder(const sstables::shared_sstable& sst) {
    double f = sst->filter_get_recent_false_positive();
    return ratio_holder(f + sst->filter_get_recent_true_positive(), f);
}

uint64_t accumulate_on_active_memtables(replica::table& t, noncopyable_function<uint64_t(replica::memtable& mt)> action) {
    uint64_t ret = 0;
    t.for_each_active_memtable([&] (replica::memtable& mt) {
        ret += action(mt);
    });
    return ret;
}

void set_column_family(http_context& ctx, routes& r, sharded<db::system_keyspace>& sys_ks) {
    cf::get_column_family_name.set(r, [&ctx] (const_req req){
        std::vector<sstring> res;
        const replica::database::tables_metadata& meta = ctx.db.local().get_tables_metadata();
        res.reserve(meta.size());
        meta.for_each_table_id([&] (const std::pair<sstring, sstring>& kscf, table_id) {
            res.push_back(kscf.first + ":" + kscf.second);
        });
        return res;
    });

    cf::get_column_family.set(r, [&ctx] (std::unique_ptr<http::request> req){
        std::list<cf::column_family_info> res;
            ctx.db.local().get_tables_metadata().for_each_table_id([&] (const std::pair<sstring, sstring>& kscf, table_id) {
                cf::column_family_info info;
                info.ks = kscf.first;
                info.cf =  kscf.second;
                info.type = "ColumnFamilies";
                res.push_back(info);
            });
            return make_ready_future<json::json_return_type>(json::stream_range_as_array(std::move(res), std::identity()));
        });

    cf::get_column_family_name_keyspace.set(r, [&ctx] (const_req req){
        std::vector<sstring> res;
        const flat_hash_map<sstring, replica::keyspace>& keyspaces = ctx.db.local().get_keyspaces();
        res.reserve(keyspaces.size());
        for (const auto& i : keyspaces) {
            res.push_back(i.first);
        }
        return res;
    });

    cf::get_memtable_columns_count.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), uint64_t{0}, [](replica::column_family& cf) {
            return accumulate_on_active_memtables(cf, std::mem_fn(&replica::memtable::partition_count));
        }, std::plus<>());
    });

    cf::get_all_memtable_columns_count.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, uint64_t{0}, [](replica::column_family& cf) {
            return accumulate_on_active_memtables(cf, std::mem_fn(&replica::memtable::partition_count));
        }, std::plus<>());
    });

    cf::get_memtable_on_heap_size.set(r, [] (const_req req) {
        return 0;
    });

    cf::get_all_memtable_on_heap_size.set(r, [] (const_req req) {
        return 0;
    });

    cf::get_memtable_off_heap_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), int64_t(0), [](replica::column_family& cf) {
            return accumulate_on_active_memtables(cf, [] (replica::memtable& active_memtable) {
                return active_memtable.region().occupancy().total_space();
            });
        }, std::plus<int64_t>());
    });

    cf::get_all_memtable_off_heap_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, int64_t(0), [](replica::column_family& cf) {
            return accumulate_on_active_memtables(cf, [] (replica::memtable& active_memtable) {
                return active_memtable.region().occupancy().total_space();
            });
        }, std::plus<int64_t>());
    });

    cf::get_memtable_live_data_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), int64_t(0), [](replica::column_family& cf) {
            return accumulate_on_active_memtables(cf, [] (replica::memtable& active_memtable) {
                return active_memtable.region().occupancy().used_space();
            });
        }, std::plus<int64_t>());
    });

    cf::get_all_memtable_live_data_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, int64_t(0), [](replica::column_family& cf) {
            return accumulate_on_active_memtables(cf, [] (replica::memtable& active_memtable) {
                return active_memtable.region().occupancy().used_space();
            });
        }, std::plus<int64_t>());
    });

    cf::get_cf_all_memtables_on_heap_size.set(r, [] (const_req req) {
        return 0;
    });

    cf::get_all_cf_all_memtables_on_heap_size.set(r, [] (const_req req) {
        return 0;
    });

    cf::get_cf_all_memtables_off_heap_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        warn(unimplemented::cause::INDEXES);
        return map_reduce_cf(ctx, req->get_path_param("name"), int64_t(0), [](replica::column_family& cf) {
            return cf.occupancy().total_space();
        }, std::plus<int64_t>());
    });

    cf::get_all_cf_all_memtables_off_heap_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        warn(unimplemented::cause::INDEXES);
        return ctx.db.map_reduce0([](const replica::database& db){
            return db.dirty_memory_region_group().real_memory_used();
        }, int64_t(0), std::plus<int64_t>()).then([](int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    cf::get_cf_all_memtables_live_data_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        warn(unimplemented::cause::INDEXES);
        return map_reduce_cf(ctx, req->get_path_param("name"), int64_t(0), [](replica::column_family& cf) {
            return cf.occupancy().used_space();
        }, std::plus<int64_t>());
    });

    cf::get_all_cf_all_memtables_live_data_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        warn(unimplemented::cause::INDEXES);
        return map_reduce_cf(ctx, int64_t(0), [](replica::column_family& cf) {
            return accumulate_on_active_memtables(cf, [] (replica::memtable& active_memtable) {
                return active_memtable.region().occupancy().used_space();
            });
        }, std::plus<int64_t>());
    });

    cf::get_memtable_switch_count.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats(ctx,req->get_path_param("name") ,&replica::column_family_stats::memtable_switch_count);
    });

    cf::get_all_memtable_switch_count.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats(ctx, &replica::column_family_stats::memtable_switch_count);
    });

    // FIXME: this refers to partitions, not rows.
    cf::get_estimated_row_size_histogram.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), utils::estimated_histogram(0), [](replica::column_family& cf) {
            utils::estimated_histogram res(0);
            for (auto sstables = cf.get_sstables(); auto& i : *sstables) {
                res.merge(i->get_stats_metadata().estimated_partition_size);
            }
            return res;
        },
        utils::estimated_histogram_merge, utils_json::estimated_histogram());
    });

    // FIXME: this refers to partitions, not rows.
    cf::get_estimated_row_count.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), int64_t(0), [](replica::column_family& cf) {
            uint64_t res = 0;
            for (auto sstables = cf.get_sstables(); auto& i : *sstables) {
                res += i->get_stats_metadata().estimated_partition_size.count();
            }
            return res;
        },
        std::plus<uint64_t>());
    });

    cf::get_estimated_column_count_histogram.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), utils::estimated_histogram(0), [](replica::column_family& cf) {
            utils::estimated_histogram res(0);
            for (auto sstables = cf.get_sstables(); auto& i : *sstables) {
                res.merge(i->get_stats_metadata().estimated_cells_count);
            }
            return res;
        },
        utils::estimated_histogram_merge, utils_json::estimated_histogram());
    });

    cf::get_all_compression_ratio.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_pending_flushes.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats(ctx,req->get_path_param("name") ,&replica::column_family_stats::pending_flushes);
    });

    cf::get_all_pending_flushes.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats(ctx, &replica::column_family_stats::pending_flushes);
    });

    cf::get_read.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats_count(ctx,req->get_path_param("name") ,&replica::column_family_stats::reads);
    });

    cf::get_all_read.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats_count(ctx, &replica::column_family_stats::reads);
    });

    cf::get_write.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats_count(ctx, req->get_path_param("name") ,&replica::column_family_stats::writes);
    });

    cf::get_all_write.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats_count(ctx, &replica::column_family_stats::writes);
    });

    cf::get_read_latency_histogram_depricated.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_histogram(ctx, req->get_path_param("name"), &replica::column_family_stats::reads);
    });

    cf::get_read_latency_histogram.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_rate_and_histogram(ctx, req->get_path_param("name"), &replica::column_family_stats::reads);
    });

    cf::get_read_latency.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats_sum(ctx,req->get_path_param("name") ,&replica::column_family_stats::reads);
    });

    cf::get_write_latency.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats_sum(ctx, req->get_path_param("name") ,&replica::column_family_stats::writes);
    });

    cf::get_all_read_latency_histogram_depricated.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_histogram(ctx, &replica::column_family_stats::writes);
    });

    cf::get_all_read_latency_histogram.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_rate_and_histogram(ctx, &replica::column_family_stats::writes);
    });

    cf::get_write_latency_histogram_depricated.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_histogram(ctx, req->get_path_param("name"), &replica::column_family_stats::writes);
    });

    cf::get_write_latency_histogram.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_rate_and_histogram(ctx, req->get_path_param("name"), &replica::column_family_stats::writes);
    });

    cf::get_all_write_latency_histogram_depricated.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_histogram(ctx, &replica::column_family_stats::writes);
    });

    cf::get_all_write_latency_histogram.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_rate_and_histogram(ctx, &replica::column_family_stats::writes);
    });

    cf::get_pending_compactions.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), int64_t(0), [](replica::column_family& cf) {
            return cf.estimate_pending_compactions();
        }, std::plus<int64_t>());
    });

    cf::get_all_pending_compactions.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, int64_t(0), [](replica::column_family& cf) {
            return cf.estimate_pending_compactions();
        }, std::plus<int64_t>());
    });

    cf::get_live_ss_table_count.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats(ctx, req->get_path_param("name"), &replica::column_family_stats::live_sstable_count);
    });

    cf::get_all_live_ss_table_count.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_stats(ctx, &replica::column_family_stats::live_sstable_count);
    });

    cf::get_unleveled_sstables.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_unleveled_sstables(ctx, req->get_path_param("name"));
    });

    cf::get_live_disk_space_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return sum_sstable(ctx, req->get_path_param("name"), false);
    });

    cf::get_all_live_disk_space_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return sum_sstable(ctx, false);
    });

    cf::get_total_disk_space_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return sum_sstable(ctx, req->get_path_param("name"), true);
    });

    cf::get_all_total_disk_space_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return sum_sstable(ctx, true);
    });

    // FIXME: this refers to partitions, not rows.
    cf::get_min_row_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), INT64_MAX, min_partition_size, min_int64);
    });

    // FIXME: this refers to partitions, not rows.
    cf::get_all_min_row_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, INT64_MAX, min_partition_size, min_int64);
    });

    // FIXME: this refers to partitions, not rows.
    cf::get_max_row_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), int64_t(0), max_partition_size, max_int64);
    });

    // FIXME: this refers to partitions, not rows.
    cf::get_all_max_row_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, int64_t(0), max_partition_size, max_int64);
    });

    // FIXME: this refers to partitions, not rows.
    cf::get_mean_row_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        // Cassandra 3.x mean values are truncated as integrals.
        return map_reduce_cf(ctx, req->get_path_param("name"), integral_ratio_holder(), mean_partition_size, std::plus<integral_ratio_holder>());
    });

    // FIXME: this refers to partitions, not rows.
    cf::get_all_mean_row_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        // Cassandra 3.x mean values are truncated as integrals.
        return map_reduce_cf(ctx, integral_ratio_holder(), mean_partition_size, std::plus<integral_ratio_holder>());
    });

    cf::get_bloom_filter_false_positives.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->filter_get_false_positive();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_all_bloom_filter_false_positives.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->filter_get_false_positive();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_recent_bloom_filter_false_positives.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->filter_get_recent_false_positive();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_all_recent_bloom_filter_false_positives.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->filter_get_recent_false_positive();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_bloom_filter_false_ratio.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), ratio_holder(), [] (replica::column_family& cf) {
            return boost::accumulate(*cf.get_sstables() | boost::adaptors::transformed(filter_false_positive_as_ratio_holder), ratio_holder());
        }, std::plus<>());
    });

    cf::get_all_bloom_filter_false_ratio.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, ratio_holder(), [] (replica::column_family& cf) {
            return boost::accumulate(*cf.get_sstables() | boost::adaptors::transformed(filter_false_positive_as_ratio_holder), ratio_holder());
        }, std::plus<>());
    });

    cf::get_recent_bloom_filter_false_ratio.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), ratio_holder(), [] (replica::column_family& cf) {
            return boost::accumulate(*cf.get_sstables() | boost::adaptors::transformed(filter_recent_false_positive_as_ratio_holder), ratio_holder());
        }, std::plus<>());
    });

    cf::get_all_recent_bloom_filter_false_ratio.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, ratio_holder(), [] (replica::column_family& cf) {
            return boost::accumulate(*cf.get_sstables() | boost::adaptors::transformed(filter_recent_false_positive_as_ratio_holder), ratio_holder());
        }, std::plus<>());
    });

    cf::get_bloom_filter_disk_space_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->filter_size();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_all_bloom_filter_disk_space_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->filter_size();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_bloom_filter_off_heap_memory_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->filter_memory_size();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_all_bloom_filter_off_heap_memory_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->filter_memory_size();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_index_summary_off_heap_memory_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->get_summary().memory_footprint();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_all_index_summary_off_heap_memory_used.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [] (replica::column_family& cf) {
            auto sstables = cf.get_sstables();
            return std::accumulate(sstables->begin(), sstables->end(), uint64_t(0), [](uint64_t s, auto& sst) {
                return s + sst->get_summary().memory_footprint();
            });
        }, std::plus<uint64_t>());
    });

    cf::get_compression_metadata_off_heap_memory_used.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        // FIXME
        // We are missing the off heap memory calculation
        // Return 0 is the wrong value. It's a work around
        // until the memory calculation will be available
        //auto id = get_uuid(req->get_path_param("name"), ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_compression_metadata_off_heap_memory_used.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_speculative_retries.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        //auto id = get_uuid(req->get_path_param("name"), ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_speculative_retries.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_key_cache_hit_rate.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        //auto id = get_uuid(req->get_path_param("name"), ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_row_cache_hit_out_of_range.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        //auto id = get_uuid(req->get_path_param("name"), ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_row_cache_hit_out_of_range.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_row_cache_hit.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf_raw(ctx, req->get_path_param("name"), utils::rate_moving_average(), [](const replica::column_family& cf) {
            return cf.get_row_cache().stats().hits.rate();
        }, std::plus<utils::rate_moving_average>()).then([](const utils::rate_moving_average& m) {
            return make_ready_future<json::json_return_type>(meter_to_json(m));
        });
    });

    cf::get_all_row_cache_hit.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf_raw(ctx, utils::rate_moving_average(), [](const replica::column_family& cf) {
            return cf.get_row_cache().stats().hits.rate();
        }, std::plus<utils::rate_moving_average>()).then([](const utils::rate_moving_average& m) {
            return make_ready_future<json::json_return_type>(meter_to_json(m));
        });
    });

    cf::get_row_cache_miss.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf_raw(ctx, req->get_path_param("name"), utils::rate_moving_average(), [](const replica::column_family& cf) {
            return cf.get_row_cache().stats().misses.rate();
        }, std::plus<utils::rate_moving_average>()).then([](const utils::rate_moving_average& m) {
            return make_ready_future<json::json_return_type>(meter_to_json(m));
        });
    });

    cf::get_all_row_cache_miss.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf_raw(ctx, utils::rate_moving_average(), [](const replica::column_family& cf) {
            return cf.get_row_cache().stats().misses.rate();
        }, std::plus<utils::rate_moving_average>()).then([](const utils::rate_moving_average& m) {
            return make_ready_future<json::json_return_type>(meter_to_json(m));
        });

    });

    cf::get_cas_prepare.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf_time_histogram(ctx, req->get_path_param("name"), [](const replica::column_family& cf) {
            return cf.get_stats().cas_prepare.histogram();
        });
    });

    cf::get_cas_propose.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf_time_histogram(ctx, req->get_path_param("name"), [](const replica::column_family& cf) {
            return cf.get_stats().cas_accept.histogram();
        });
    });

    cf::get_cas_commit.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf_time_histogram(ctx, req->get_path_param("name"), [](const replica::column_family& cf) {
            return cf.get_stats().cas_learn.histogram();
        });
    });

    cf::get_sstables_per_read_histogram.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, req->get_path_param("name"), utils::estimated_histogram(0), [](replica::column_family& cf) {
            return cf.get_stats().estimated_sstable_per_read;
        },
        utils::estimated_histogram_merge, utils_json::estimated_histogram());
    });

    cf::get_tombstone_scanned_histogram.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_histogram(ctx, req->get_path_param("name"), &replica::column_family_stats::tombstone_scanned);
    });

    cf::get_live_scanned_histogram.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return get_cf_histogram(ctx, req->get_path_param("name"), &replica::column_family_stats::live_scanned);
    });

    cf::get_col_update_time_delta_histogram.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        //auto id = get_uuid(req->get_path_param("name"), ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_auto_compaction.set(r, [&ctx] (const_req req) {
        auto uuid = get_uuid(req.get_path_param("name"), ctx.db.local());
        replica::column_family& cf = ctx.db.local().find_column_family(uuid);
        return !cf.is_auto_compaction_disabled_by_user();
    });

    cf::enable_auto_compaction.set(r, [&ctx](std::unique_ptr<http::request> req) {
        apilog.info("column_family/enable_auto_compaction: name={}", req->get_path_param("name"));
        auto [ks, cf] = parse_fully_qualified_cf_name(req->get_path_param("name"));
        validate_table(ctx, ks, cf);
        return set_tables_autocompaction(ctx, ks, {std::move(cf)}, true);
    });

    cf::disable_auto_compaction.set(r, [&ctx](std::unique_ptr<http::request> req) {
        apilog.info("column_family/disable_auto_compaction: name={}", req->get_path_param("name"));
        auto [ks, cf] = parse_fully_qualified_cf_name(req->get_path_param("name"));
        validate_table(ctx, ks, cf);
        return set_tables_autocompaction(ctx, ks, {std::move(cf)}, false);
    });

    ss::enable_auto_compaction.set(r, [&ctx](std::unique_ptr<http::request> req) {
        auto keyspace = validate_keyspace(ctx, req);
        auto tables = parse_tables(keyspace, ctx, req->query_parameters, "cf");

        apilog.info("enable_auto_compaction: keyspace={} tables={}", keyspace, tables);
        return set_tables_autocompaction(ctx, keyspace, tables, true);
    });

    ss::disable_auto_compaction.set(r, [&ctx](std::unique_ptr<http::request> req) {
        auto keyspace = validate_keyspace(ctx, req);
        auto tables = parse_tables(keyspace, ctx, req->query_parameters, "cf");

        apilog.info("disable_auto_compaction: keyspace={} tables={}", keyspace, tables);
        return set_tables_autocompaction(ctx, keyspace, tables, false);
    });

    cf::get_tombstone_gc.set(r, [&ctx] (const_req req) {
        auto uuid = get_uuid(req.get_path_param("name"), ctx.db.local());
        replica::table& t = ctx.db.local().find_column_family(uuid);
        return t.tombstone_gc_enabled();
    });

    cf::enable_tombstone_gc.set(r, [&ctx](std::unique_ptr<http::request> req) {
        apilog.info("column_family/enable_tombstone_gc: name={}", req->get_path_param("name"));
        auto [ks, cf] = parse_fully_qualified_cf_name(req->get_path_param("name"));
        validate_table(ctx, ks, cf);
        return set_tables_tombstone_gc(ctx, ks, {std::move(cf)}, true);
    });

    cf::disable_tombstone_gc.set(r, [&ctx](std::unique_ptr<http::request> req) {
        apilog.info("column_family/disable_tombstone_gc: name={}", req->get_path_param("name"));
        auto [ks, cf] = parse_fully_qualified_cf_name(req->get_path_param("name"));
        validate_table(ctx, ks, cf);
        return set_tables_tombstone_gc(ctx, ks, {std::move(cf)}, false);
    });

    ss::enable_tombstone_gc.set(r, [&ctx](std::unique_ptr<http::request> req) {
        auto keyspace = validate_keyspace(ctx, req);
        auto tables = parse_tables(keyspace, ctx, req->query_parameters, "cf");

        apilog.info("enable_tombstone_gc: keyspace={} tables={}", keyspace, tables);
        return set_tables_tombstone_gc(ctx, keyspace, tables, true);
    });

    ss::disable_tombstone_gc.set(r, [&ctx](std::unique_ptr<http::request> req) {
        auto keyspace = validate_keyspace(ctx, req);
        auto tables = parse_tables(keyspace, ctx, req->query_parameters, "cf");

        apilog.info("disable_tombstone_gc: keyspace={} tables={}", keyspace, tables);
        return set_tables_tombstone_gc(ctx, keyspace, tables, false);
    });

    cf::get_built_indexes.set(r, [&ctx, &sys_ks](std::unique_ptr<http::request> req) {
        auto ks_cf = parse_fully_qualified_cf_name(req->get_path_param("name"));
        auto&& ks = std::get<0>(ks_cf);
        auto&& cf_name = std::get<1>(ks_cf);
        return sys_ks.local().load_view_build_progress().then([ks, cf_name, &ctx](const std::vector<db::system_keyspace_view_build_progress>& vb) mutable {
            std::set<sstring> vp;
            for (auto b : vb) {
                if (b.view.first == ks) {
                    vp.insert(b.view.second);
                }
            }
            std::vector<sstring> res;
            auto uuid = get_uuid(ks, cf_name, ctx.db.local());
            replica::column_family& cf = ctx.db.local().find_column_family(uuid);
            res.reserve(cf.get_index_manager().list_indexes().size());
            for (auto&& i : cf.get_index_manager().list_indexes()) {
                if (!vp.contains(secondary_index::index_table_name(i.metadata().name()))) {
                    res.emplace_back(i.metadata().name());
                }
            }
            return make_ready_future<json::json_return_type>(res);
        });
    });

    cf::get_compression_metadata_off_heap_memory_used.set(r, [](const_req) {
        // FIXME
        // Currently there are no information on the compression
        // metadata, so we return 0
        return 0;
    });

    cf::get_compression_parameters.set(r, [](const_req) {
        // FIXME
        // Currently there are no compression parameters available
        // so we return an empty map
        return std::vector<sstring>();
    });

    cf::get_compression_ratio.set(r, [&ctx](std::unique_ptr<http::request> req) {
        auto uuid = get_uuid(req->get_path_param("name"), ctx.db.local());

        return ctx.db.map_reduce(sum_ratio<double>(), [uuid](replica::database& db) {
            replica::column_family& cf = db.find_column_family(uuid);
            return make_ready_future<double>(get_compression_ratio(cf));
        }).then([] (const double& result) {
            return make_ready_future<json::json_return_type>(result);
        });
    });

    cf::get_read_latency_estimated_histogram.set(r, [&ctx](std::unique_ptr<http::request> req) {
        return map_reduce_cf_time_histogram(ctx, req->get_path_param("name"), [](const replica::column_family& cf) {
            return cf.get_stats().reads.histogram();
        });
    });

    cf::get_write_latency_estimated_histogram.set(r, [&ctx](std::unique_ptr<http::request> req) {
        return map_reduce_cf_time_histogram(ctx, req->get_path_param("name"), [](const replica::column_family& cf) {
            return cf.get_stats().writes.histogram();
        });
    });

    cf::set_compaction_strategy_class.set(r, [&ctx](std::unique_ptr<http::request> req) {
        sstring strategy = req->get_query_param("class_name");
        apilog.info("column_family/set_compaction_strategy_class: name={} strategy={}", req->get_path_param("name"), strategy);
        return foreach_column_family(ctx, req->get_path_param("name"), [strategy](replica::column_family& cf) {
            cf.set_compaction_strategy(sstables::compaction_strategy::type(strategy));
        }).then([] {
                return make_ready_future<json::json_return_type>(json_void());
        });
    });

    cf::get_compaction_strategy_class.set(r, [&ctx](const_req req) {
        return ctx.db.local().find_column_family(get_uuid(req.get_path_param("name"), ctx.db.local())).get_compaction_strategy().name();
    });

    cf::set_compression_parameters.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    cf::set_crc_check_chance.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    cf::get_sstable_count_per_level.set(r, [&ctx](std::unique_ptr<http::request> req) {
        return map_reduce_cf_raw(ctx, req->get_path_param("name"), std::vector<uint64_t>(), [](const replica::column_family& cf) {
            return cf.sstable_count_per_level();
        }, concat_sstable_count_per_level).then([](const std::vector<uint64_t>& res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    cf::get_sstables_for_key.set(r, [&ctx](std::unique_ptr<http::request> req) {
        auto key = req->get_query_param("key");
        auto uuid = get_uuid(req->get_path_param("name"), ctx.db.local());

        return ctx.db.map_reduce0([key, uuid] (replica::database& db) -> future<std::unordered_set<sstring>> {
            auto sstables = co_await db.find_column_family(uuid).get_sstables_by_partition_key(key);
            co_return boost::copy_range<std::unordered_set<sstring>>(sstables | boost::adaptors::transformed([] (auto s) { return s->get_filename(); }));
        }, std::unordered_set<sstring>(),
        [](std::unordered_set<sstring> a, std::unordered_set<sstring>&& b) mutable {
            a.merge(b);
            return a;
        }).then([](const std::unordered_set<sstring>& res) {
            return make_ready_future<json::json_return_type>(container_to_vec(res));
        });
    });


    cf::toppartitions.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        auto name = req->get_path_param("name");
        auto [ks, cf] = parse_fully_qualified_cf_name(name);

        api::req_param<std::chrono::milliseconds, unsigned> duration{*req, "duration", 1000ms};
        api::req_param<unsigned> capacity(*req, "capacity", 256);
        api::req_param<unsigned> list_size(*req, "list_size", 10);

        apilog.info("toppartitions query: name={} duration={} list_size={} capacity={}",
            name, duration.param, list_size.param, capacity.param);

        return seastar::do_with(db::toppartitions_query(ctx.db, {{ks, cf}}, {}, duration.value, list_size, capacity), [&ctx] (db::toppartitions_query& q) {
            return run_toppartitions_query(q, ctx, true);
        });
    });

    cf::force_major_compaction.set(r, [&ctx](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto params = req_params({
            std::pair("name", mandatory::yes),
            std::pair("flush_memtables", mandatory::no),
            std::pair("split_output", mandatory::no),
        });
        params.process(*req);
        if (params.get("split_output")) {
            fail(unimplemented::cause::API);
        }
        auto [ks, cf] = parse_fully_qualified_cf_name(*params.get("name"));
        auto flush = params.get_as<bool>("flush_memtables").value_or(true);
        apilog.info("column_family/force_major_compaction: name={} flush={}", req->get_path_param("name"), flush);

        auto keyspace = validate_keyspace(ctx, ks);
        std::vector<table_info> table_infos = {table_info{
            .name = cf,
            .id = ctx.db.local().find_uuid(ks, cf)
        }};

        auto& compaction_module = ctx.db.local().get_compaction_manager().get_task_manager_module();
        std::optional<flush_mode> fmopt;
        if (!flush) {
            fmopt = flush_mode::skip;
        }
        auto task = co_await compaction_module.make_and_start_task<major_keyspace_compaction_task_impl>({}, std::move(keyspace), tasks::task_id::create_null_id(), ctx.db, std::move(table_infos), fmopt);
        co_await task->done();
        co_return json_void();
    });
}

void unset_column_family(http_context& ctx, routes& r) {
    cf::get_column_family_name.unset(r);
    cf::get_column_family.unset(r);
    cf::get_column_family_name_keyspace.unset(r);
    cf::get_memtable_columns_count.unset(r);
    cf::get_all_memtable_columns_count.unset(r);
    cf::get_memtable_on_heap_size.unset(r);
    cf::get_all_memtable_on_heap_size.unset(r);
    cf::get_memtable_off_heap_size.unset(r);
    cf::get_all_memtable_off_heap_size.unset(r);
    cf::get_memtable_live_data_size.unset(r);
    cf::get_all_memtable_live_data_size.unset(r);
    cf::get_cf_all_memtables_on_heap_size.unset(r);
    cf::get_all_cf_all_memtables_on_heap_size.unset(r);
    cf::get_cf_all_memtables_off_heap_size.unset(r);
    cf::get_all_cf_all_memtables_off_heap_size.unset(r);
    cf::get_cf_all_memtables_live_data_size.unset(r);
    cf::get_all_cf_all_memtables_live_data_size.unset(r);
    cf::get_memtable_switch_count.unset(r);
    cf::get_all_memtable_switch_count.unset(r);
    cf::get_estimated_row_size_histogram.unset(r);
    cf::get_estimated_row_count.unset(r);
    cf::get_estimated_column_count_histogram.unset(r);
    cf::get_all_compression_ratio.unset(r);
    cf::get_pending_flushes.unset(r);
    cf::get_all_pending_flushes.unset(r);
    cf::get_read.unset(r);
    cf::get_all_read.unset(r);
    cf::get_write.unset(r);
    cf::get_all_write.unset(r);
    cf::get_read_latency_histogram_depricated.unset(r);
    cf::get_read_latency_histogram.unset(r);
    cf::get_read_latency.unset(r);
    cf::get_write_latency.unset(r);
    cf::get_all_read_latency_histogram_depricated.unset(r);
    cf::get_all_read_latency_histogram.unset(r);
    cf::get_write_latency_histogram_depricated.unset(r);
    cf::get_write_latency_histogram.unset(r);
    cf::get_all_write_latency_histogram_depricated.unset(r);
    cf::get_all_write_latency_histogram.unset(r);
    cf::get_pending_compactions.unset(r);
    cf::get_all_pending_compactions.unset(r);
    cf::get_live_ss_table_count.unset(r);
    cf::get_all_live_ss_table_count.unset(r);
    cf::get_unleveled_sstables.unset(r);
    cf::get_live_disk_space_used.unset(r);
    cf::get_all_live_disk_space_used.unset(r);
    cf::get_total_disk_space_used.unset(r);
    cf::get_all_total_disk_space_used.unset(r);
    cf::get_min_row_size.unset(r);
    cf::get_all_min_row_size.unset(r);
    cf::get_max_row_size.unset(r);
    cf::get_all_max_row_size.unset(r);
    cf::get_mean_row_size.unset(r);
    cf::get_all_mean_row_size.unset(r);
    cf::get_bloom_filter_false_positives.unset(r);
    cf::get_all_bloom_filter_false_positives.unset(r);
    cf::get_recent_bloom_filter_false_positives.unset(r);
    cf::get_all_recent_bloom_filter_false_positives.unset(r);
    cf::get_bloom_filter_false_ratio.unset(r);
    cf::get_all_bloom_filter_false_ratio.unset(r);
    cf::get_recent_bloom_filter_false_ratio.unset(r);
    cf::get_all_recent_bloom_filter_false_ratio.unset(r);
    cf::get_bloom_filter_disk_space_used.unset(r);
    cf::get_all_bloom_filter_disk_space_used.unset(r);
    cf::get_bloom_filter_off_heap_memory_used.unset(r);
    cf::get_all_bloom_filter_off_heap_memory_used.unset(r);
    cf::get_index_summary_off_heap_memory_used.unset(r);
    cf::get_all_index_summary_off_heap_memory_used.unset(r);
    cf::get_compression_metadata_off_heap_memory_used.unset(r);
    cf::get_all_compression_metadata_off_heap_memory_used.unset(r);
    cf::get_speculative_retries.unset(r);
    cf::get_all_speculative_retries.unset(r);
    cf::get_key_cache_hit_rate.unset(r);
    cf::get_row_cache_hit_out_of_range.unset(r);
    cf::get_all_row_cache_hit_out_of_range.unset(r);
    cf::get_row_cache_hit.unset(r);
    cf::get_all_row_cache_hit.unset(r);
    cf::get_row_cache_miss.unset(r);
    cf::get_all_row_cache_miss.unset(r);
    cf::get_cas_prepare.unset(r);
    cf::get_cas_propose.unset(r);
    cf::get_cas_commit.unset(r);
    cf::get_sstables_per_read_histogram.unset(r);
    cf::get_tombstone_scanned_histogram.unset(r);
    cf::get_live_scanned_histogram.unset(r);
    cf::get_col_update_time_delta_histogram.unset(r);
    cf::get_auto_compaction.unset(r);
    cf::enable_auto_compaction.unset(r);
    cf::disable_auto_compaction.unset(r);
    ss::enable_auto_compaction.unset(r);
    ss::disable_auto_compaction.unset(r);
    cf::get_tombstone_gc.unset(r);
    cf::enable_tombstone_gc.unset(r);
    cf::disable_tombstone_gc.unset(r);
    ss::enable_tombstone_gc.unset(r);
    ss::disable_tombstone_gc.unset(r);
    cf::get_built_indexes.unset(r);
    cf::get_compression_metadata_off_heap_memory_used.unset(r);
    cf::get_compression_parameters.unset(r);
    cf::get_compression_ratio.unset(r);
    cf::get_read_latency_estimated_histogram.unset(r);
    cf::get_write_latency_estimated_histogram.unset(r);
    cf::set_compaction_strategy_class.unset(r);
    cf::get_compaction_strategy_class.unset(r);
    cf::set_compression_parameters.unset(r);
    cf::set_crc_check_chance.unset(r);
    cf::get_sstable_count_per_level.unset(r);
    cf::get_sstables_for_key.unset(r);
    cf::toppartitions.unset(r);
    cf::force_major_compaction.unset(r);
}
}
