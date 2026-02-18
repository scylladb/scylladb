/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "api/api-doc/error_injection.json.hh"
#include "api/api_init.hh"
#include <seastar/http/exception.hh>
#include "utils/error_injection.hh"
#include "utils/rjson.hh"
#include <seastar/core/future-util.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/sharded.hh>

namespace api {
using namespace seastar::httpd;

namespace hf = httpd::error_injection_json;

// Structure to hold error injection event data
struct injection_event {
    sstring injection_name;
    sstring injection_type;
    unsigned shard_id;
};

void set_error_injection(http_context& ctx, routes& r) {

    hf::enable_injection.set(r, [](std::unique_ptr<request> req) -> future<json::json_return_type> {
        sstring injection = req->get_path_param("injection");
        bool one_shot = req->get_query_param("one_shot") == "True";
        auto params = co_await util::read_entire_stream_contiguous(*req->content_stream);

        const size_t max_params_size = 1024 * 1024;
        if (params.size() > max_params_size) {
            // This is a hard limit, because we don't want to allocate
            // too much memory or block the thread for too long.
            throw httpd::bad_param_exception(format("Injection parameters are too long, max length is {}", max_params_size));
        }

        try {
            auto parameters = params.empty()
                ? utils::error_injection_parameters{}
                : rjson::parse_to_map<utils::error_injection_parameters>(params);

            auto& errinj = utils::get_local_injector();
            co_await errinj.enable_on_all(injection, one_shot, std::move(parameters));
        } catch (const rjson::error& e) {
            throw httpd::bad_param_exception(format("Failed to parse injections parameters: {}", e.what()));
        }
        co_return json::json_void();
    });

    hf::get_enabled_injections_on_all.set(r, [](std::unique_ptr<request> req) {
        auto& errinj = utils::get_local_injector();
        auto ret = errinj.enabled_injections_on_all();
        return make_ready_future<json::json_return_type>(ret);
    });

    hf::disable_injection.set(r, [](std::unique_ptr<request> req) {
        sstring injection = req->get_path_param("injection");

        auto& errinj = utils::get_local_injector();
        return errinj.disable_on_all(injection).then([] {
            return make_ready_future<json::json_return_type>(json::json_void());
        });
    });

    hf::read_injection.set(r, [](std::unique_ptr<request> req) -> future<json::json_return_type> {
        const sstring injection = req->get_path_param("injection");

        std::vector<error_injection_json::error_injection_info> error_injection_infos(smp::count, error_injection_json::error_injection_info{});

        co_await smp::invoke_on_all([&] {
            auto& info = error_injection_infos[this_shard_id()];
            auto& errinj = utils::get_local_injector();
            const auto enabled = errinj.is_enabled(injection);
            info.enabled = enabled;
            if (!enabled) {
                return;
            }
            std::vector<error_injection_json::mapper> parameters;
            for (const auto& p : errinj.get_injection_parameters(injection)) {
                error_injection_json::mapper param;
                param.key = p.first;
                param.value = p.second;
                parameters.push_back(std::move(param));
            }
            info.parameters = std::move(parameters);
        });

        co_return json::json_return_type(error_injection_infos);
    });

    hf::disable_on_all.set(r, [](std::unique_ptr<request> req) {
        auto& errinj = utils::get_local_injector();
        return errinj.disable_on_all().then([] {
            return make_ready_future<json::json_return_type>(json::json_void());
        });
    });

    hf::message_injection.set(r, [](std::unique_ptr<request> req) {
        sstring injection = req->get_path_param("injection");
        auto& errinj = utils::get_local_injector();
        return errinj.receive_message_on_all(injection).then([] {
            return make_ready_future<json::json_return_type>(json::json_void());
        });
    });

    // Server-Sent Events endpoint for injection events
    // This allows clients to subscribe to real-time injection events instead of log parsing
    r.add(operation_type::GET, url("/v2/error_injection/events"), [](std::unique_ptr<request> req) -> future<json::json_return_type> {
        // Create a shared foreign_ptr to a queue that will receive events from all shards
        // Using a queue on the current shard to collect events
        using event_queue_t = seastar::queue<injection_event>;
        auto event_queue = make_lw_shared<event_queue_t>();
        auto queue_ptr = make_foreign(event_queue);
        
        // Register callback on all shards to send events to our queue
        auto& errinj = utils::get_local_injector();
        
        // Capture the current shard ID for event delivery
        auto target_shard = this_shard_id();
        
        // Setup event callback that forwards events to the queue on the target shard
        auto callback = [queue_ptr = std::move(queue_ptr), target_shard] (std::string_view name, std::string_view type) mutable {
            injection_event evt{
                .injection_name = sstring(name),
                .injection_type = sstring(type),
                .shard_id = this_shard_id()
            };
            
            // Send event to the target shard's queue
            (void)smp::submit_to(target_shard, [queue_ptr = queue_ptr.copy(), evt = std::move(evt)] () mutable {
                return queue_ptr->push_eventually(std::move(evt));
            });
        };
        
        // Register the callback on all shards
        co_await errinj.register_event_callback_on_all(callback);
        
        // Return a streaming function that sends SSE events
        noncopyable_function<future<>(output_stream<char>&&)> stream_func = 
            [event_queue](output_stream<char>&& os) -> future<> {
            
            auto s = std::move(os);
            std::exception_ptr ex;
            
            try {
                // Send initial SSE comment to establish connection
                co_await s.write(": connected\n\n");
                co_await s.flush();
                
                // Stream events as they arrive from any shard
                while (true) {
                    auto evt = co_await event_queue->pop_eventually();
                    
                    // Format as SSE event
                    // data: {"injection":"name","type":"handler","shard":0}
                    auto json_data = format("{{\"injection\":\"{}\",\"type\":\"{}\",\"shard\":{}}}",
                        evt.injection_name, evt.injection_type, evt.shard_id);
                    
                    co_await s.write(format("data: {}\n\n", json_data));
                    co_await s.flush();
                }
            } catch (...) {
                ex = std::current_exception();
            }
            
            // Cleanup: clear callbacks on all shards
            co_await utils::get_local_injector().clear_event_callbacks_on_all();
            
            co_await s.close();
            if (ex) {
                co_await coroutine::return_exception_ptr(std::move(ex));
            }
        };
        
        co_return json::json_return_type(std::move(stream_func));
    });
}

} // namespace api
