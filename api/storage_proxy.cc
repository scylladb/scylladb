/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "storage_proxy.hh"
#include "service/storage_proxy.hh"
#include "api/api.hh"
#include "api/api-doc/storage_proxy.json.hh"
#include "api/api-doc/utils.json.hh"
#include "db/config.hh"
#include "utils/histogram.hh"
#include <seastar/core/scheduling_specific.hh>

namespace api {

namespace sp = httpd::storage_proxy_json;
using proxy = service::storage_proxy;
using namespace seastar::httpd;
using namespace json;

utils::time_estimated_histogram timed_rate_moving_average_summary_merge(utils::time_estimated_histogram a, const utils::timed_rate_moving_average_summary_and_histogram& b) {
    return a.merge(b.histogram());
}

/**
 * This function implement a two dimensional map reduce where
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
 * This function implement a two dimensional map reduce where
 * the first level is a distributed storage_proxy class and the
 * second level is the stats per scheduling group class.
 * @param d -  a reference to the storage_proxy distributed class.
 * @param f - a field pointer which is the implicit internal reducer.
 * @param reducer - the reducer that is used in both outer and inner
 * aggregations.
 * @param initial_value - the initial value to use for both aggregations* @return
 * @return A future that resolves to the result of the aggregation.
 */
template<typename V, typename Reducer, typename F, typename C>
future<V> two_dimensional_map_reduce(distributed<service::storage_proxy>& d,
        C F::*f, Reducer reducer, V initial_value) {
    return two_dimensional_map_reduce(d, [f] (F& stats) -> V {
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

static future<json::json_return_type>  sum_estimated_histogram(sharded<service::storage_proxy>& proxy, utils::timed_rate_moving_average_summary_and_histogram service::storage_proxy_stats::stats::*f) {
    return two_dimensional_map_reduce(proxy, [f] (service::storage_proxy_stats::stats& stats) {
        return (stats.*f).histogram();
    }, utils::time_estimated_histogram_merge, utils::time_estimated_histogram()).then([](const utils::time_estimated_histogram& val) {
        return make_ready_future<json::json_return_type>(time_to_json_histogram(val));
    });
}

static future<json::json_return_type>  sum_estimated_histogram(sharded<service::storage_proxy>& proxy, utils::estimated_histogram service::storage_proxy_stats::stats::*f) {

    return two_dimensional_map_reduce(proxy, f, utils::estimated_histogram_merge,
            utils::estimated_histogram()).then([](const utils::estimated_histogram& val) {
        utils_json::estimated_histogram res;
        res = val;
        return make_ready_future<json::json_return_type>(res);
    });
}

static future<json::json_return_type>  total_latency(sharded<service::storage_proxy>& proxy, utils::timed_rate_moving_average_summary_and_histogram service::storage_proxy_stats::stats::*f) {
    return two_dimensional_map_reduce(proxy, [f] (service::storage_proxy_stats::stats& stats) {
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
        utils::timed_rate_moving_average_summary_and_histogram F::*f) {
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
        utils::timed_rate_moving_average_summary_and_histogram F::*f) {

    return two_dimensional_map_reduce(d, [f] (service::storage_proxy_stats::stats& stats) {
        return (stats.*f).rate();
    }, std::plus<utils::rate_moving_average_and_histogram>(),
            utils::rate_moving_average_and_histogram()).then([](const utils::rate_moving_average_and_histogram& val) {
        return make_ready_future<json::json_return_type>(timer_to_json(val));
    });
}

void set_storage_proxy(http_context& ctx, routes& r, sharded<service::storage_proxy>& proxy) {
    sp::get_total_hints.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_hinted_handoff_enabled.set(r, [&proxy](std::unique_ptr<http::request> req)  {
        const auto& filter = proxy.local().get_hints_host_filter();
        return make_ready_future<json::json_return_type>(!filter.is_disabled_for_all());
    });

    sp::set_hinted_handoff_enabled.set(r, [&proxy](std::unique_ptr<http::request> req)  {
        auto enable = req->get_query_param("enable");
        auto filter = (enable == "true" || enable == "1")
                ? db::hints::host_filter(db::hints::host_filter::enabled_for_all_tag {})
                : db::hints::host_filter(db::hints::host_filter::disabled_for_all_tag {});
        return proxy.invoke_on_all([filter = std::move(filter)] (service::storage_proxy& sp) {
            return sp.change_hints_host_filter(filter);
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    sp::get_hinted_handoff_enabled_by_dc.set(r, [&proxy](std::unique_ptr<http::request> req)  {
        std::vector<sstring> res;
        const auto& filter = proxy.local().get_hints_host_filter();
        const auto& dcs = filter.get_dcs();
        res.reserve(res.size());
        std::copy(dcs.begin(), dcs.end(), std::back_inserter(res));
        return make_ready_future<json::json_return_type>(res);
    });

    sp::set_hinted_handoff_enabled_by_dc_list.set(r, [&proxy](std::unique_ptr<http::request> req)  {
        auto dcs = req->get_query_param("dcs");
        auto filter = db::hints::host_filter::parse_from_dc_list(std::move(dcs));
        return proxy.invoke_on_all([filter = std::move(filter)] (service::storage_proxy& sp) {
            return sp.change_hints_host_filter(filter);
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    sp::get_max_hint_window.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::set_max_hint_window.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("ms");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_max_hints_in_progress.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(1);
    });

    sp::set_max_hints_in_progress.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("qs");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_hints_in_progress.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::reload_trigger_classes.set(r, [](std::unique_ptr<http::request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_read_repair_attempted.set(r, [&proxy](std::unique_ptr<http::request> req)  {
        return sum_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::read_repair_attempts);
    });

    sp::get_read_repair_repaired_blocking.set(r, [&proxy](std::unique_ptr<http::request> req)  {
        return sum_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::read_repair_repaired_blocking);
    });

    sp::get_read_repair_repaired_background.set(r, [&proxy](std::unique_ptr<http::request> req)  {
        return sum_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::read_repair_repaired_background);
    });

    sp::get_cas_read_timeouts.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &proxy::stats::cas_read_timeouts);
    });

    sp::get_cas_read_unavailables.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &proxy::stats::cas_read_unavailables);
    });

    sp::get_cas_write_timeouts.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &proxy::stats::cas_write_timeouts);
    });

    sp::get_cas_write_unavailables.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &proxy::stats::cas_write_unavailables);
    });

    sp::get_cas_write_metrics_unfinished_commit.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_stats(proxy, &proxy::stats::cas_write_unfinished_commit);
    });

    sp::get_cas_write_metrics_contention.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_estimated_histogram(proxy, &proxy::stats::cas_write_contention);
    });

    sp::get_cas_write_metrics_condition_not_met.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_stats(proxy, &proxy::stats::cas_write_condition_not_met);
    });

    sp::get_cas_write_metrics_failed_read_round_optimization.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_stats(proxy, &proxy::stats::cas_failed_read_round_optimization);
    });

    sp::get_cas_read_metrics_unfinished_commit.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_stats(proxy, &proxy::stats::cas_read_unfinished_commit);
    });

    sp::get_cas_read_metrics_contention.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_estimated_histogram(proxy, &proxy::stats::cas_read_contention);
    });

    sp::get_read_metrics_timeouts.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &service::storage_proxy_stats::stats::read_timeouts);
    });

    sp::get_read_metrics_unavailables.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &service::storage_proxy_stats::stats::read_unavailables);
    });

    sp::get_range_metrics_timeouts.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &service::storage_proxy_stats::stats::range_slice_timeouts);
    });

    sp::get_range_metrics_unavailables.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &service::storage_proxy_stats::stats::range_slice_unavailables);
    });

    sp::get_write_metrics_timeouts.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &service::storage_proxy_stats::stats::write_timeouts);
    });

    sp::get_write_metrics_unavailables.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_long(proxy, &service::storage_proxy_stats::stats::write_unavailables);
    });

    sp::get_read_metrics_timeouts_rates.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_obj(proxy, &service::storage_proxy_stats::stats::read_timeouts);
    });

    sp::get_read_metrics_unavailables_rates.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_obj(proxy, &service::storage_proxy_stats::stats::read_unavailables);
    });

    sp::get_range_metrics_timeouts_rates.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_obj(proxy, &service::storage_proxy_stats::stats::range_slice_timeouts);
    });

    sp::get_range_metrics_unavailables_rates.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_obj(proxy, &service::storage_proxy_stats::stats::range_slice_unavailables);
    });

    sp::get_write_metrics_timeouts_rates.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_obj(proxy, &service::storage_proxy_stats::stats::write_timeouts);
    });

    sp::get_write_metrics_unavailables_rates.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timed_rate_as_obj(proxy, &service::storage_proxy_stats::stats::write_unavailables);
    });

    sp::get_range_metrics_latency_histogram_depricated.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_histogram_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::range);
    });

    sp::get_write_metrics_latency_histogram_depricated.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_histogram_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::write);
    });

    sp::get_read_metrics_latency_histogram_depricated.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_histogram_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::read);
    });

    sp::get_range_metrics_latency_histogram.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timer_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::range);
    });

    sp::get_write_metrics_latency_histogram.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timer_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::write);
    });
    sp::get_cas_write_metrics_latency_histogram.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timer_stats(proxy, &proxy::stats::cas_write);
    });

    sp::get_cas_read_metrics_latency_histogram.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timer_stats(proxy, &proxy::stats::cas_read);
    });

    sp::get_view_write_metrics_latency_histogram.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        // FIXME
        // No View metrics are available, so just return empty moving average

        return make_ready_future<json::json_return_type>(get_empty_moving_average());
    });

    sp::get_read_metrics_latency_histogram.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timer_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::read);
    });

    sp::get_read_estimated_histogram.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_estimated_histogram(proxy, &service::storage_proxy_stats::stats::read);
    });

    sp::get_read_latency.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return total_latency(proxy, &service::storage_proxy_stats::stats::read);
    });
    sp::get_write_estimated_histogram.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_estimated_histogram(proxy, &service::storage_proxy_stats::stats::write);
    });

    sp::get_write_latency.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return total_latency(proxy, &service::storage_proxy_stats::stats::write);
    });

    sp::get_range_estimated_histogram.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return sum_timer_stats_storage_proxy(proxy, &service::storage_proxy_stats::stats::range);
    });

    sp::get_range_latency.set(r, [&proxy](std::unique_ptr<http::request> req) {
        return total_latency(proxy, &service::storage_proxy_stats::stats::range);
    });
}

void unset_storage_proxy(http_context& ctx, routes& r) {
    sp::get_total_hints.unset(r);
    sp::get_hinted_handoff_enabled.unset(r);
    sp::set_hinted_handoff_enabled.unset(r);
    sp::get_hinted_handoff_enabled_by_dc.unset(r);
    sp::set_hinted_handoff_enabled_by_dc_list.unset(r);
    sp::get_max_hint_window.unset(r);
    sp::set_max_hint_window.unset(r);
    sp::get_max_hints_in_progress.unset(r);
    sp::set_max_hints_in_progress.unset(r);
    sp::get_hints_in_progress.unset(r);
    sp::reload_trigger_classes.unset(r);
    sp::get_read_repair_attempted.unset(r);
    sp::get_read_repair_repaired_blocking.unset(r);
    sp::get_read_repair_repaired_background.unset(r);
    sp::get_cas_read_timeouts.unset(r);
    sp::get_cas_read_unavailables.unset(r);
    sp::get_cas_write_timeouts.unset(r);
    sp::get_cas_write_unavailables.unset(r);
    sp::get_cas_write_metrics_unfinished_commit.unset(r);
    sp::get_cas_write_metrics_contention.unset(r);
    sp::get_cas_write_metrics_condition_not_met.unset(r);
    sp::get_cas_write_metrics_failed_read_round_optimization.unset(r);
    sp::get_cas_read_metrics_unfinished_commit.unset(r);
    sp::get_cas_read_metrics_contention.unset(r);
    sp::get_read_metrics_timeouts.unset(r);
    sp::get_read_metrics_unavailables.unset(r);
    sp::get_range_metrics_timeouts.unset(r);
    sp::get_range_metrics_unavailables.unset(r);
    sp::get_write_metrics_timeouts.unset(r);
    sp::get_write_metrics_unavailables.unset(r);
    sp::get_read_metrics_timeouts_rates.unset(r);
    sp::get_read_metrics_unavailables_rates.unset(r);
    sp::get_range_metrics_timeouts_rates.unset(r);
    sp::get_range_metrics_unavailables_rates.unset(r);
    sp::get_write_metrics_timeouts_rates.unset(r);
    sp::get_write_metrics_unavailables_rates.unset(r);
    sp::get_range_metrics_latency_histogram_depricated.unset(r);
    sp::get_write_metrics_latency_histogram_depricated.unset(r);
    sp::get_read_metrics_latency_histogram_depricated.unset(r);
    sp::get_range_metrics_latency_histogram.unset(r);
    sp::get_write_metrics_latency_histogram.unset(r);
    sp::get_cas_write_metrics_latency_histogram.unset(r);
    sp::get_cas_read_metrics_latency_histogram.unset(r);
    sp::get_view_write_metrics_latency_histogram.unset(r);
    sp::get_read_metrics_latency_histogram.unset(r);
    sp::get_read_estimated_histogram.unset(r);
    sp::get_read_latency.unset(r);
    sp::get_write_estimated_histogram.unset(r);
    sp::get_write_latency.unset(r);
    sp::get_range_estimated_histogram.unset(r);
    sp::get_range_latency.unset(r);
}

}
