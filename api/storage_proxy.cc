/*
 * Copyright 2015 Cloudius Systems
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

namespace api {

namespace sp = httpd::storage_proxy_json;
using proxy = service::storage_proxy;
using namespace json;

static future<json::json_return_type>  sum_estimated_histogram(http_context& ctx, sstables::estimated_histogram proxy::stats::*f) {
    return ctx.sp.map_reduce0([f](const proxy& p) {return p.get_stats().*f;}, sstables::estimated_histogram(),
            sstables::merge).then([](const sstables::estimated_histogram& val) {
        utils_json::estimated_histogram res;
        res = val;
        return make_ready_future<json::json_return_type>(res);
    });
}

static future<json::json_return_type>  total_latency(http_context& ctx, utils::ihistogram proxy::stats::*f) {
    return ctx.sp.map_reduce0([f](const proxy& p) {return (p.get_stats().*f).mean * (p.get_stats().*f).count;}, 0.0,
            std::plus<double>()).then([](double val) {
        int64_t res = val;
        return make_ready_future<json::json_return_type>(res);
    });
}

void set_storage_proxy(http_context& ctx, routes& r) {
    sp::get_total_hints.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_hinted_handoff_enabled.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        // FIXME
        // hinted handoff is not supported currently,
        // so we should return false
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
        return sum_stats(ctx.sp, &proxy::stats::read_repair_attempts);
    });

    sp::get_read_repair_repaired_blocking.set(r, [&ctx](std::unique_ptr<request> req)  {
        return sum_stats(ctx.sp, &proxy::stats::read_repair_repaired_blocking);
    });

    sp::get_read_repair_repaired_background.set(r, [&ctx](std::unique_ptr<request> req)  {
        return sum_stats(ctx.sp, &proxy::stats::read_repair_repaired_background);
    });

    sp::get_schema_versions.set(r, [](std::unique_ptr<request> req)  {
        //TBD
        // FIXME
        // describe_schema_versions is not implemented yet
        // this is a work around
        std::vector<sp::mapper_list> res;
        sp::mapper_list entry;
        entry.key = boost::lexical_cast<std::string>(utils::fb_utilities::get_broadcast_address());
        entry.value.push(service::get_local_storage_service().get_schema_version());
        res.push_back(entry);
        return make_ready_future<json::json_return_type>(res);
    });

    sp::get_cas_read_timeouts.set(r, [](std::unique_ptr<request> req) {
        //TBD
        // FIXME
        // cas is not supported yet, so just return 0
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_cas_read_unavailables.set(r, [](std::unique_ptr<request> req) {
        //TBD
        // FIXME
        // cas is not supported yet, so just return 0
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_cas_write_timeouts.set(r, [](std::unique_ptr<request> req) {
        //TBD
        // FIXME
        // cas is not supported yet, so just return 0
        return make_ready_future<json::json_return_type>(0);
    });

    sp::get_cas_write_unavailables.set(r, [](std::unique_ptr<request> req) {
        //TBD
        // FIXME
        // cas is not supported yet, so just return 0
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

    sp::get_read_estimated_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_estimated_histogram(ctx, &proxy::stats::estimated_read);
    });

    sp::get_read_latency.set(r, [&ctx](std::unique_ptr<request> req) {
        return total_latency(ctx, &proxy::stats::read);
    });
    sp::get_write_estimated_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_estimated_histogram(ctx, &proxy::stats::estimated_write);
    });

    sp::get_write_latency.set(r, [&ctx](std::unique_ptr<request> req) {
        return total_latency(ctx, &proxy::stats::write);
    });

    sp::get_range_estimated_histogram.set(r, [&ctx](std::unique_ptr<request> req) {
        return sum_histogram_stats(ctx.sp, &proxy::stats::read);
    });

    sp::get_range_latency.set(r, [&ctx](std::unique_ptr<request> req) {
        return total_latency(ctx, &proxy::stats::range);
    });
}

}
