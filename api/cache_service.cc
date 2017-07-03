/*
 * Copyright (C) 2015 ScyllaDB
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

#include "cache_service.hh"
#include "api/api-doc/cache_service.json.hh"
#include "column_family.hh"

namespace api {
using namespace json;
namespace cs = httpd::cache_service_json;

void set_cache_service(http_context& ctx, routes& r) {
    cs::get_row_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // We never save the cache
        // Origin uses 0 for never
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_row_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        auto period = req->get_query_param("period");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_key_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // We never save the cache
        // Origin uses 0 for never
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_key_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        auto period = req->get_query_param("period");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_counter_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // We never save the cache
        // Origin uses 0 for never
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_counter_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        auto ccspis = req->get_query_param("ccspis");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_row_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_row_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        auto rckts = req->get_query_param("rckts");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_key_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_key_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        auto kckts = req->get_query_param("kckts");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_counter_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_counter_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        auto cckts = req->get_query_param("cckts");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::invalidate_key_cache.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::invalidate_counter_cache.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::set_row_cache_capacity_in_mb.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        auto capacity = req->get_query_param("capacity");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::set_key_cache_capacity_in_mb.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        auto period = req->get_query_param("period");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::set_counter_cache_capacity_in_mb.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        auto capacity = req->get_query_param("capacity");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::save_caches.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_key_capacity.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for capacity is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_hits.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for hits is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_requests.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for request is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_hit_rate.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for rate is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_hits_moving_avrage.set(r, [&ctx] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // See above
        return make_ready_future<json::json_return_type>(meter_to_json(utils::rate_moving_average()));
    });

    cs::get_key_requests_moving_avrage.set(r, [&ctx] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // See above
        return make_ready_future<json::json_return_type>(meter_to_json(utils::rate_moving_average()));
    });

    cs::get_key_size.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for size is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_entries.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support keys cache,
        // so currently returning a 0 for key entries is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_row_capacity.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [](const column_family& cf) {
            return cf.get_row_cache().get_cache_tracker().region().occupancy().used_space();
        }, std::plus<uint64_t>());
    });

    cs::get_row_hits.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [](const column_family& cf) {
            return cf.get_row_cache().stats().hits.count();
        }, std::plus<uint64_t>());
    });

    cs::get_row_requests.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, uint64_t(0), [](const column_family& cf) {
            return cf.get_row_cache().stats().hits.count() + cf.get_row_cache().stats().misses.count();
        }, std::plus<uint64_t>());
    });

    cs::get_row_hit_rate.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, ratio_holder(), [](const column_family& cf) {
            return ratio_holder(cf.get_row_cache().stats().hits.count() + cf.get_row_cache().stats().misses.count(),
                    cf.get_row_cache().stats().hits.count());
        }, std::plus<ratio_holder>());
    });

    cs::get_row_hits_moving_avrage.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf_raw(ctx, utils::rate_moving_average(), [](const column_family& cf) {
            return cf.get_row_cache().stats().hits.rate();
        }, std::plus<utils::rate_moving_average>()).then([](const utils::rate_moving_average& m) {
            return make_ready_future<json::json_return_type>(meter_to_json(m));
        });
    });

    cs::get_row_requests_moving_avrage.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf_raw(ctx, utils::rate_moving_average(), [](const column_family& cf) {
            return cf.get_row_cache().stats().hits.rate() + cf.get_row_cache().stats().misses.rate();
        }, std::plus<utils::rate_moving_average>()).then([](const utils::rate_moving_average& m) {
            return make_ready_future<json::json_return_type>(meter_to_json(m));
        });
    });

    cs::get_row_size.set(r, [&ctx] (std::unique_ptr<request> req) {
        // In origin row size is the weighted size.
        // We currently do not support weights, so we use num entries instead
        return map_reduce_cf(ctx, 0, [](const column_family& cf) {
            return cf.get_row_cache().partitions();
        }, std::plus<uint64_t>());
    });

    cs::get_row_entries.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, 0, [](const column_family& cf) {
            return cf.get_row_cache().partitions();
        }, std::plus<uint64_t>());
    });

    cs::get_counter_capacity.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for rate is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_hits.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for hits is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_requests.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for hits is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_hit_rate.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for rate is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_hits_moving_avrage.set(r, [&ctx] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // See above
        return make_ready_future<json::json_return_type>(meter_to_json(utils::rate_moving_average()));
    });

    cs::get_counter_requests_moving_avrage.set(r, [&ctx] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // See above
        return make_ready_future<json::json_return_type>(meter_to_json(utils::rate_moving_average()));
    });

    cs::get_counter_size.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for size is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_entries.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        // FIXME
        // we don't support counter cache,
        // so currently returning a 0 for entries is ok
        return make_ready_future<json::json_return_type>(0);
    });
}

}

