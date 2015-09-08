/*
 * Copyright 2015 Cloudius Systems
 */

#include "storage_proxy.hh"
#include "service/storage_proxy.hh"
#include "api/api-doc/storage_proxy.json.hh"
#include "api/api-doc/utils.json.hh"

namespace api {

namespace sp = httpd::storage_proxy_json;
using proxy = service::storage_proxy;
using namespace json;

void set_storage_proxy(http_context& ctx, routes& r) {
    sp::get_total_hints.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_hinted_handoff_enabled.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(false);
    });

    sp::set_hinted_handoff_enabled.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("enable");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_hinted_handoff_enabled_by_dc.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        std::vector<sp::mapper_list> res;
        return make_ready_future<json::json_return_type>(res);
    });

    sp::set_hinted_handoff_enabled_by_dc_list.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("dcs");
        return make_ready_future<json::json_return_type>(json_void());
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

    sp::get_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(1);
    });

    sp::set_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_read_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::set_read_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_write_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::set_write_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_counter_write_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });
    sp::set_counter_write_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_cas_contention_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::set_cas_contention_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_range_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::set_range_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        auto enable = req->get_query_param("timeout");
        return make_ready_future<json::json_return_type>(json_void());
    });

    sp::get_truncate_rpc_timeout.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
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

    sp::get_read_repair_attempted.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_read_repair_repaired_blocking.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_read_repair_repaired_background.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_schema_versions.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        std::vector<sp::mapper_list> res;
        return make_ready_future<json::json_return_type>(res);
    });

    sp::get_cas_read_timeouts.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_cas_write_metrics_unfinished_commit.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_cas_write_metrics_contention.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_cas_write_metrics_condition_not_met.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_cas_read_metrics_unfinished_commit.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_cas_read_metrics_contention.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_cas_read_metrics_condition_not_met.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_read_metrics_timeouts.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::read_timeouts);
    });

    sp::get_read_metrics_unavailables.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::read_unavailables);
    });

    sp::get_range_metrics_timeouts.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::range_slice_timeouts);
    });

    sp::get_range_metrics_unavailables.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::range_slice_unavailables);
    });

    sp::get_write_metrics_timeouts.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::write_timeouts);
    });

    sp::get_write_metrics_unavailables.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_stats(ctx.sp, &proxy::stats::write_unavailables);
    });

    sp::get_range_metrics_latency_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_histogram_stats(ctx.sp, &proxy::stats::range);
    });

    sp::get_write_metrics_latency_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_histogram_stats(ctx.sp, &proxy::stats::write);
    });

    sp::get_read_metrics_latency_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_histogram_stats(ctx.sp, &proxy::stats::read);
    });
}

}
