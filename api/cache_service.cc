/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cache_service.hh"
#include "api/api.hh"
#include "api/api-doc/cache_service.json.hh"
#include "column_family.hh"

namespace api {
using namespace json;
using namespace seastar::httpd;
namespace cs = httpd::cache_service_json;

void set_cache_service(http_context& ctx, routes& r) {
    cs::get_row_cache_save_period_in_seconds.set(r, [](std::unique_ptr<http::request> req) {
        // We never save the cache
        // Origin uses 0 for never
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_row_cache_save_period_in_seconds.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        auto period = req->get_query_param("period");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_key_cache_save_period_in_seconds.set(r, [](std::unique_ptr<http::request> req) {
        // We never save the cache
        // Origin uses 0 for never
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_key_cache_save_period_in_seconds.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        auto period = req->get_query_param("period");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_counter_cache_save_period_in_seconds.set(r, [](std::unique_ptr<http::request> req) {
        // We never save the cache
        // Origin uses 0 for never
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_counter_cache_save_period_in_seconds.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        auto ccspis = req->get_query_param("ccspis");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_row_cache_keys_to_save.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_row_cache_keys_to_save.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        auto rckts = req->get_query_param("rckts");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_key_cache_keys_to_save.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_key_cache_keys_to_save.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        auto kckts = req->get_query_param("kckts");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_counter_cache_keys_to_save.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_counter_cache_keys_to_save.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        auto cckts = req->get_query_param("cckts");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::invalidate_key_cache.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::invalidate_counter_cache.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::set_row_cache_capacity_in_mb.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        auto capacity = req->get_query_param("capacity");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::set_key_cache_capacity_in_mb.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        auto period = req->get_query_param("period");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::set_counter_cache_capacity_in_mb.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        auto capacity = req->get_query_param("capacity");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::save_caches.set(r, [](std::unique_ptr<http::request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_key_capacity.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for capacity is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_hits.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for hits is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_requests.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for request is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_hit_rate.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for rate is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_hits_moving_avrage.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // See above
        return make_ready_future<json::json_return_type>(meter_to_json(utils::rate_moving_average()));
    });

    cs::get_key_requests_moving_avrage.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // See above
        return make_ready_future<json::json_return_type>(meter_to_json(utils::rate_moving_average()));
    });

    cs::get_key_size.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for size is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_entries.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for key entries is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_row_capacity.set(r, [] (std::unique_ptr<http::request> req) {
        return seastar::map_reduce(smp::all_cpus(), [] (int cpu) {
            return make_ready_future<uint64_t>(memory::stats().total_memory());
        }, uint64_t(0), std::plus<uint64_t>()).then([](const int64_t& res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    cs::get_row_hits.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [](const replica::column_family& cf) {
            return cf.get_row_cache().stats().hits.count();
        }, std::plus<uint64_t>());
    });

    cs::get_row_requests.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [](const replica::column_family& cf) {
            return cf.get_row_cache().stats().hits.count() + cf.get_row_cache().stats().misses.count();
        }, std::plus<uint64_t>());
    });

    cs::get_row_hit_rate.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf(ctx, ratio_holder(), [](const replica::column_family& cf) {
            return ratio_holder(cf.get_row_cache().stats().hits.count() + cf.get_row_cache().stats().misses.count(),
                    cf.get_row_cache().stats().hits.count());
        }, std::plus<ratio_holder>());
    });

    cs::get_row_hits_moving_avrage.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf_raw(ctx, utils::rate_moving_average(), [](const replica::column_family& cf) {
            return cf.get_row_cache().stats().hits.rate();
        }, std::plus<utils::rate_moving_average>()).then([](const utils::rate_moving_average& m) {
            return make_ready_future<json::json_return_type>(meter_to_json(m));
        });
    });

    cs::get_row_requests_moving_avrage.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return map_reduce_cf_raw(ctx, utils::rate_moving_average(), [](const replica::column_family& cf) {
            return cf.get_row_cache().stats().hits.rate() + cf.get_row_cache().stats().misses.rate();
        }, std::plus<utils::rate_moving_average>()).then([](const utils::rate_moving_average& m) {
            return make_ready_future<json::json_return_type>(meter_to_json(m));
        });
    });

    cs::get_row_size.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        // In origin row size is the weighted size.
        // We currently do not support weights, so we use raw size in bytes instead
        return ctx.db.map_reduce0([](replica::database& db) -> uint64_t {
            return db.row_cache_tracker().region().occupancy().used_space();
        }, uint64_t(0), std::plus<uint64_t>()).then([](const int64_t& res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    cs::get_row_entries.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        return ctx.db.map_reduce0([](replica::database& db) -> uint64_t {
            return db.row_cache_tracker().partitions();
        }, uint64_t(0), std::plus<uint64_t>()).then([](const int64_t& res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    cs::get_counter_capacity.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for rate is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_hits.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for hits is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_requests.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for hits is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_hit_rate.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for rate is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_hits_moving_avrage.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // See above
        return make_ready_future<json::json_return_type>(meter_to_json(utils::rate_moving_average()));
    });

    cs::get_counter_requests_moving_avrage.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // See above
        return make_ready_future<json::json_return_type>(meter_to_json(utils::rate_moving_average()));
    });

    cs::get_counter_size.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for size is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_entries.set(r, [] (std::unique_ptr<http::request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for entries is ok
        return make_ready_future<json::json_return_type>(0);
    });
}

void unset_cache_service(http_context& ctx, routes& r) {
    cs::get_row_cache_save_period_in_seconds.unset(r);
    cs::set_row_cache_save_period_in_seconds.unset(r);
    cs::get_key_cache_save_period_in_seconds.unset(r);
    cs::set_key_cache_save_period_in_seconds.unset(r);
    cs::get_counter_cache_save_period_in_seconds.unset(r);
    cs::set_counter_cache_save_period_in_seconds.unset(r);
    cs::get_row_cache_keys_to_save.unset(r);
    cs::set_row_cache_keys_to_save.unset(r);
    cs::get_key_cache_keys_to_save.unset(r);
    cs::set_key_cache_keys_to_save.unset(r);
    cs::get_counter_cache_keys_to_save.unset(r);
    cs::set_counter_cache_keys_to_save.unset(r);
    cs::invalidate_key_cache.unset(r);
    cs::invalidate_counter_cache.unset(r);
    cs::set_row_cache_capacity_in_mb.unset(r);
    cs::set_key_cache_capacity_in_mb.unset(r);
    cs::set_counter_cache_capacity_in_mb.unset(r);
    cs::save_caches.unset(r);
    cs::get_key_capacity.unset(r);
    cs::get_key_hits.unset(r);
    cs::get_key_requests.unset(r);
    cs::get_key_hit_rate.unset(r);
    cs::get_key_hits_moving_avrage.unset(r);
    cs::get_key_requests_moving_avrage.unset(r);
    cs::get_key_size.unset(r);
    cs::get_key_entries.unset(r);
    cs::get_row_capacity.unset(r);
    cs::get_row_hits.unset(r);
    cs::get_row_requests.unset(r);
    cs::get_row_hit_rate.unset(r);
    cs::get_row_hits_moving_avrage.unset(r);
    cs::get_row_requests_moving_avrage.unset(r);
    cs::get_row_size.unset(r);
    cs::get_row_entries.unset(r);
    cs::get_counter_capacity.unset(r);
    cs::get_counter_hits.unset(r);
    cs::get_counter_requests.unset(r);
    cs::get_counter_hit_rate.unset(r);
    cs::get_counter_hits_moving_avrage.unset(r);
    cs::get_counter_requests_moving_avrage.unset(r);
    cs::get_counter_size.unset(r);
    cs::get_counter_entries.unset(r);
}

}

