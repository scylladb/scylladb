/*
 * Copyright 2015 Cloudius Systems
 */

#include "cache_service.hh"
#include "api/api-doc/cache_service.json.hh"

namespace api {
using namespace json;
namespace cs = httpd::cache_service_json;

void set_cache_service(http_context& ctx, routes& r) {
    cs::get_row_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_row_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // TBD
        auto period = req->get_query_param("period");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_key_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_key_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // TBD
        auto period = req->get_query_param("period");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_counter_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_counter_cache_save_period_in_seconds.set(r, [](std::unique_ptr<request> req) {
        // TBD
        auto ccspis = req->get_query_param("ccspis");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_row_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_row_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        auto rckts = req->get_query_param("rckts");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_key_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_key_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        auto kckts = req->get_query_param("kckts");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_counter_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::set_counter_cache_keys_to_save.set(r, [](std::unique_ptr<request> req) {
        // TBD
        auto cckts = req->get_query_param("cckts");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::invalidate_key_cache.set(r, [](std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::invalidate_counter_cache.set(r, [](std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::set_row_cache_capacity_in_mb.set(r, [](std::unique_ptr<request> req) {
        // TBD
        auto capacity = req->get_query_param("capacity");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::set_key_cache_capacity_in_mb.set(r, [](std::unique_ptr<request> req) {
        // TBD
        auto period = req->get_query_param("period");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::set_counter_cache_capacity_in_mb.set(r, [](std::unique_ptr<request> req) {
        // TBD
        auto capacity = req->get_query_param("capacity");
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::save_caches.set(r, [](std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    cs::get_key_capacity.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_hits.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_requests.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_hit_rate.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_size.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_key_entries.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_row_capacity.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_row_hits.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_row_requests.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_row_hit_rate.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_row_size.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_row_entries.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_capacity.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_hits.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_requests.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_hit_rate.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_size.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cs::get_counter_entries.set(r, [] (std::unique_ptr<request> req) {
        // TBD
        return make_ready_future<json::json_return_type>(0);
    });
}

}

