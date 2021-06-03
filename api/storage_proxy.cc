/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "storage_proxy.hh"
#include "service/storage_proxy.hh"
#include "api/api-doc/storage_proxy.json.hh"
#include "api/api-doc/utils.json.hh"
#include "service/storage_service.hh"
#include "db/config.hh"
#include "utils/histogram.hh"
#include "database.hh"
#include "seastar/core/scheduling_specific.hh"

namespace api {

namespace sp = httpd::storage_proxy_json;
using proxy = service::storage_proxy;
using namespace json;


/**
 * This function implement a two dimentional map reduce where
 * the first level is a distributed storage_proxy class and the
 * second level is the stats per scheduling group class.
 * @param d -  a reference to the storage_proxy distributed class.
 * @param mapper -  the internal mapper that is used to map the internal
 * stat class into a value of type `V`.
 * @param reducer - the reducer that is used in both outer and inner
 * aggregations.
 * @param initial_value - the initial value to use for both aggregations
 * @return A future that resolves to the result of the aggregation.
 */
template<typename V, typename Reducer, typename InnerMapper>
future<V> two_dimensional_map_reduce(distributed<service::storage_proxy>& d,
        InnerMapper mapper, Reducer reducer, V initial_value) {
    return d.map_reduce0( [mapper, reducer, initial_value] (const service::storage_proxy& sp) {
        return map_reduce_scheduling_group_specific<service::storage_proxy_stats::stats>(
                mapper, reducer, initial_value, sp.get_stats_key());
    }, initial_value, reducer);
}

/**
 * This function implement a two dimentional map reduce where
 * the first level is a distributed storage_proxy class and the
 * second level is the stats per scheduling group class.
 * @param d -  a reference to the storage_proxy distributed class.
 * @param f - a field pointer which is the implicit internal reducer.
 * @param reducer - the reducer that is used in both outer and inner
 * aggregations.
 * @param initial_value - the initial value to use for both aggregations* @return
 * @return A future that resolves to the result of the aggregation.
 */
template<typename V, typename Reducer, typename F>
future<V> two_dimensional_map_reduce(distributed<service::storage_proxy>& d,
        V F::*f, Reducer reducer, V initial_value) {
    return two_dimensional_map_reduce(d, [f] (F& stats) {
        return stats.*f;
    }, reducer, initial_value);
}

/**
 * A partial Specialization of sum_stats for the storage proxy
 * case where the get stats function doesn't return a
 * stats object with fields but a per scheduling group
 * stats object, the name was also changed since functions
 * partial specialization is not supported in C++.
 *
 */
template<typename V, typename F>
future<json::json_return_type>  sum_stats_storage_proxy(distributed<proxy>& d, V F::*f) {
    return two_dimensional_map_reduce(d, [f] (F& stats) { return stats.*f; }, std::plus<V>(), V(0)).then([] (V val) {
        return make_ready_future<json::json_return_type>(val);
    });
}


static future<utils::rate_moving_average>  sum_timed_rate(distributed<proxy>& d, utils::timed_rate_moving_average service::storage_proxy_stats::stats::*f) {
    return two_dimensional_map_reduce(d, [f] (service::storage_proxy_stats::stats& stats) {
        return (stats.*f).rate();
    }, std::plus<utils::rate_moving_average>(), utils::rate_moving_average());
}

static future<json::json_return_type>  sum_timed_rate_as_obj(distributed<proxy>& d, utils::timed_rate_moving_average service::storage_proxy_stats::stats::*f) {
    return sum_timed_rate(d, f).then([](const utils::rate_moving_average& val) {
        httpd::utils_json::rate_moving_average m;
        m = val;
        return make_ready_future<json::json_return_type>(m);
    });
}

httpd::utils_json::rate_moving_average_and_histogram get_empty_moving_average() {
    return timer_to_json(utils::rate_moving_average_and_histogram());
}

static future<json::json_return_type>  sum_timed_rate_as_long(distributed<proxy>& d, utils::timed_rate_moving_average service::storage_proxy_stats::stats::*f) {
    return sum_timed_rate(d, f).then([](const utils::rate_moving_average& val) {
        return make_ready_future<json::json_return_type>(val.count);
    });
}

utils_json::estimated_histogram time_to_json_histogram(const utils::time_estimated_histogram& val) {
    utils_json::estimated_histogram res;
    for (size_t i = 0; i < val.size(); i++) {
        res.buckets.push(val.get(i));
        res.bucket_offsets.push(val.get_bucket_lower_limit(i));
    }
    return res;
}

static future<json::json_return_type>  sum_estimated_histogram(http_context& ctx, utils::time_estimated_histogram service::storage_proxy_stats::stats::*f) {

    return two_dimensional_map_reduce(ctx.sp, f, utils::time_estimated_histogram_merge,
            utils::time_estimated_histogram()).then([](const utils::time_estimated_histogram& val) {
        return make_ready_future<json::json_return_type>(time_to_json_histogram(val));
    });
}

static future<json::json_return_type>  sum_estimated_histogram(http_context& ctx, utils::estimated_histogram service::storage_proxy_stats::stats::*f) {

    return two_dimensional_map_reduce(ctx.sp, f, utils::estimated_histogram_merge,
            utils::estimated_histogram()).then([](const utils::estimated_histogram& val) {
        utils_json::estimated_histogram res;
        res = val;
        return make_ready_future<json::json_return_type>(res);
    });
}

static future<json::json_return_type>  total_latency(http_context& ctx, utils::timed_rate_moving_average_and_histogram service::storage_proxy_stats::stats::*f) {
    return two_dimensional_map_reduce(ctx.sp, [f] (service::storage_proxy_stats::stats& stats) {
            return (stats.*f).hist.mean * (stats.*f).hist.count;
        }, std::plus<double>(), 0.0).then([](double val) {
        int64_t res = val;
        return make_ready_future<json::json_return_type>(res);
    });
}

/**
 * A partial Specialization of sum_histogram_stats
 * for the storage proxy case where the get stats
 * function doesn't return a stats object with
 * fields but a per scheduling group stats object,
 * the name was also changed since function partial
 * specialization is not supported in C++.
 */
template<typename F>
future<json::json_return_type>
sum_histogram_stats_storage_proxy(distributed<proxy>& d,
        utils::timed_rate_moving_average_and_histogram F::*f) {
    return two_dimensional_map_reduce(d, [f] (service::storage_proxy_stats::stats& stats) {
        return (stats.*f).hist;
    }, std::plus<utils::ihistogram>(), utils::ihistogram()).
            then([](const utils::ihistogram& val) {
        return make_ready_future<json::json_return_type>(to_json(val));
    });
}

/**
 * A partial Specialization of sum_timer_stats for the
 * storage proxy case where the get stats function
 * doesn't return a stats object with fields but a
 * per scheduling group stats object, the name
 * was also changed since partial function specialization
 * is not supported in C++.
 */
template<typename F>
future<json::json_return_type>
sum_timer_stats_storage_proxy(distributed<proxy>& d,
        utils::timed_rate_moving_average_and_histogram F::*f) {

    return two_dimensional_map_reduce(d, [f] (service::storage_proxy_stats::stats& stats) {
        return (stats.*f).rate();
    }, std::plus<utils::rate_moving_average_and_histogram>(),
            utils::rate_moving_average_and_histogram()).then([](const utils::rate_moving_average_and_histogram& val) {
        return make_ready_future<json::json_return_type>(timer_to_json(val));
    });
}

void set_storage_proxy(http_context& ctx, routes& r) {
    sp::get_total_hints.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_hinted_handoff_enabled.set(r, [&ctx](std::unique_ptr<request> req)  {
        const auto& filter = service::get_storage_proxy().local().get_hints_host_filter();
        return make_ready_future<json::json_return_type>(!filter.is_disabled_for_all());
    });

    sp::set_hinted_handoff_enabled.set(r, [](std::unique_ptr<request> req)  {
        auto enable = req->get_query_param("enable");
        auto filter = (enable == "true" || enable == "1")
                ? db::hints::host_filter(db::hints::host_filter::enabled_for_all_tag {})
                : db::hints::host_filter(db::hints::host_filter::disabled_for_all_tag {});
        return service::get_storage_proxy().invoke_on_all([filter = std::move(filter)] (service::storage_proxy& sp) {
            return sp.change_hints_host_filter(filter);
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    sp::get_hinted_handoff_enabled_by_dc.set(r, [](std::unique_ptr<request> req)  {
        std::vector<sstring> res;
        const auto& filter = service::get_storage_proxy().local().get_hints_host_filter();
        const auto& dcs = filter.get_dcs();
        res.reserve(res.size());
        std::copy(dcs.begin(), dcs.end(), std::back_inserter(res));
        return make_ready_future<json::json_return_type>(res);
    });

    sp::set_hinted_handoff_enabled_by_dc_list.set(r, [](std::unique_ptr<request> req)  {
        auto dcs = req->get_query_param("dcs");
        auto filter = db::hints::host_filter::parse_from_dc_list(std::move(dcs));
        return service::get_storage_proxy().invoke_on_all([filter = std::move(filter)] (service::storage_proxy& sp) {
            return sp.change_hints_host_filter(filter);
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    sp::get_max_hint_window.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::set_max_hint_window.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("ms");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_max_hints_in_progress.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(1);
    });

    sp::set_max_hints_in_progress.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("qs");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_hints_in_progress.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_rpc_timeout.set(r, [&ctx](const_req req)  {
        return ctx.db.local().get_config().request_timeout_in_ms()/1000.0;
    });

    sp::set_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_read_rpc_timeout.set(r, [&ctx](const_req req)  {
        return ctx.db.local().get_config().read_request_timeout_in_ms()/1000.0;
    });

    sp::set_read_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_write_rpc_timeout.set(r, [&ctx](const_req req)  {
        return ctx.db.local().get_config().write_request_timeout_in_ms()/1000.0;
    });

    sp::set_write_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_counter_write_rpc_timeout.set(r, [&ctx](const_req req)  {
        return ctx.db.local().get_config().counter_write_request_timeout_in_ms()/1000.0;
    });

    sp::set_counter_write_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_cas_contention_timeout.set(r, [&ctx](const_req req)  {
        return ctx.db.local().get_config().cas_contention_timeout_in_ms()/1000.0;
    });

    sp::set_cas_contention_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_range_rpc_timeout.set(r, [&ctx](const_req req)  {
        return ctx.db.local().get_config().range_request_timeout_in_ms()/1000.0;
    });

    sp::set_range_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_truncate_rpc_timeout.set(r, [&ctx](const_req req)  {
        return ctx.db.local().get_config().truncate_request_timeout_in_ms()/1000.0;
    });

    sp::set_truncate_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::reload_trigger_classes.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_read_repair_attempted.set(r, [&ctx](std::unique_ptr<request> req)  {
        return sum_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::read_repair_attempts);
    });

    sp::get_read_repair_repaired_blocking.set(r, [&ctx](std::unique_ptr<request> req)  {
        return sum_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::read_repair_repaired_blocking);
    });

    sp::get_read_repair_repaired_background.set(r, [&ctx](std::unique_ptr<request> req)  {
        return sum_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::read_repair_repaired_background);
    });

    sp::get_schema_versions.set(r, [](std::unique_ptr<request> req)  {
        return service::get_local_storage_service().describe_schema_versions().then([] (auto result) {
            std::vector<sp::mapper_list> res;
            for (auto e : result) {
                sp::mapper_list entry;
                entry.key = std::move(e.first);
                entry.value = std::move(e.second);
                res.emplace_back(std::move(entry));
            }
            return make_ready_future<json::json_return_type>(std::move(res));
        });
    });

    sp::get_cas_read_timeouts.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &proxy::stats::cas_read_timeouts);
    });

    sp::get_cas_read_unavailables.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &proxy::stats::cas_read_unavailables);
    });

    sp::get_cas_write_timeouts.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &proxy::stats::cas_write_timeouts);
    });

    sp::get_cas_write_unavailables.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &proxy::stats::cas_write_unavailables);
    });

    sp::get_cas_write_metrics_unfinished_commit.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::cas_write_unfinished_commit);
    });

    sp::get_cas_write_metrics_contention.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_estimated_histogram(ctx, &proxy::stats::cas_write_contention);
    });

    sp::get_cas_write_metrics_condition_not_met.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::cas_write_condition_not_met);
    });

    sp::get_cas_write_metrics_failed_read_round_optimization.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::cas_failed_read_round_optimization);
    });

    sp::get_cas_read_metrics_unfinished_commit.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::cas_read_unfinished_commit);
    });

    sp::get_cas_read_metrics_contention.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_estimated_histogram(ctx, &proxy::stats::cas_read_contention);
    });

    sp::get_read_metrics_timeouts.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &service::storage_proxy_stats::stats::read_timeouts);
    });

    sp::get_read_metrics_unavailables.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &service::storage_proxy_stats::stats::read_unavailables);
    });

    sp::get_range_metrics_timeouts.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &service::storage_proxy_stats::stats::range_slice_timeouts);
    });

    sp::get_range_metrics_unavailables.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &service::storage_proxy_stats::stats::range_slice_unavailables);
    });

    sp::get_write_metrics_timeouts.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &service::storage_proxy_stats::stats::write_timeouts);
    });

    sp::get_write_metrics_unavailables.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_long(ctx.sp, &service::storage_proxy_stats::stats::write_unavailables);
    });

    sp::get_read_metrics_timeouts_rates.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_obj(ctx.sp, &service::storage_proxy_stats::stats::read_timeouts);
    });

    sp::get_read_metrics_unavailables_rates.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_obj(ctx.sp, &service::storage_proxy_stats::stats::read_unavailables);
    });

    sp::get_range_metrics_timeouts_rates.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_obj(ctx.sp, &service::storage_proxy_stats::stats::range_slice_timeouts);
    });

    sp::get_range_metrics_unavailables_rates.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_obj(ctx.sp, &service::storage_proxy_stats::stats::range_slice_unavailables);
    });

    sp::get_write_metrics_timeouts_rates.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_obj(ctx.sp, &service::storage_proxy_stats::stats::write_timeouts);
    });

    sp::get_write_metrics_unavailables_rates.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timed_rate_as_obj(ctx.sp, &service::storage_proxy_stats::stats::write_unavailables);
    });

    sp::get_range_metrics_latency_histogram_depricated.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_histogram_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::range);
    });

    sp::get_write_metrics_latency_histogram_depricated.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_histogram_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::write);
    });

    sp::get_read_metrics_latency_histogram_depricated.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_histogram_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::read);
    });

    sp::get_range_metrics_latency_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timer_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::range);
    });

    sp::get_write_metrics_latency_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timer_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::write);
    });
    sp::get_cas_write_metrics_latency_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timer_stats(ctx.sp, &proxy::stats::cas_write);
    });

    sp::get_cas_read_metrics_latency_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timer_stats(ctx.sp, &proxy::stats::cas_read);
    });

    sp::get_view_write_metrics_latency_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        // FIXME
        // No View metrics are available, so just return empty moving average

        return make_ready_future<json::json_return_type>(get_empty_moving_average());
    });

    sp::get_read_metrics_latency_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timer_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::read);
    });

    sp::get_read_estimated_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_estimated_histogram(ctx, &service::storage_proxy_stats::stats::estimated_read);
    });

    sp::get_read_latency.set(r, [&ctx](std::unique_ptr<request> req) {
        return total_latency(ctx, &service::storage_proxy_stats::stats::read);
    });
    sp::get_write_estimated_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_estimated_histogram(ctx, &service::storage_proxy_stats::stats::estimated_write);
    });

    sp::get_write_latency.set(r, [&ctx](std::unique_ptr<request> req) {
        return total_latency(ctx, &service::storage_proxy_stats::stats::write);
    });

    sp::get_range_estimated_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_timer_stats_storage_proxy(ctx.sp, &service::storage_proxy_stats::stats::range);
    });

    sp::get_range_latency.set(r, [&ctx](std::unique_ptr<request> req) {
        return total_latency(ctx, &service::storage_proxy_stats::stats::range);
    });
}

}
