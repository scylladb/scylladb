/*
 * Copyright 2015-present ScyllaDB
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

#include "api.hh"
#include <seastar/http/file_handler.hh>
#include <seastar/http/transformers.hh>
#include <seastar/http/api_docs.hh>
#include "storage_service.hh"
#include "commitlog.hh"
#include "gossiper.hh"
#include "failure_detector.hh"
#include "column_family.hh"
#include "lsa.hh"
#include "messaging_service.hh"
#include "storage_proxy.hh"
#include "cache_service.hh"
#include "collectd.hh"
#include "endpoint_snitch.hh"
#include "compaction_manager.hh"
#include "hinted_handoff.hh"
#include "error_injection.hh"
#include <seastar/http/exception.hh>
#include "stream_manager.hh"
#include "system.hh"
#include "api/config.hh"

logging::logger apilog("api");

namespace api {

static std::unique_ptr<reply> exception_reply(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (const no_such_keyspace& ex) {
        throw bad_param_exception(ex.what());
    }
    // We never going to get here
    throw std::runtime_error("exception_reply");
}

future<> set_server_init(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);
    auto rb02 = std::make_shared < api_registry_builder20 > (ctx.api_doc, "/v2");

    return ctx.http_server.set_routes([rb, &ctx, rb02](routes& r) {
        r.register_exeption_handler(exception_reply);
        r.put(GET, "/ui", new httpd::file_handler(ctx.api_dir + "/index.html",
                new content_replace("html")));
        r.add(GET, url("/ui").remainder("path"), new httpd::directory_handler(ctx.api_dir,
                new content_replace("html")));
        rb->set_api_doc(r);
        rb02->set_api_doc(r);
        rb02->register_api_file(r, "swagger20_header");
        rb->register_function(r, "system",
                "The system related API");
        set_system(ctx, r);
    });
}

future<> set_server_config(http_context& ctx) {
    auto rb02 = std::make_shared < api_registry_builder20 > (ctx.api_doc, "/v2");
    return ctx.http_server.set_routes([&ctx, rb02](routes& r) {
        set_config(rb02, ctx, r);
    });
}

static future<> register_api(http_context& ctx, const sstring& api_name,
        const sstring api_desc,
        std::function<void(http_context& ctx, routes& r)> f) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx, api_name, api_desc, f](routes& r) {
        rb->register_function(r, api_name, api_desc);
        f(ctx,r);
    });
}

future<> set_transport_controller(http_context& ctx, cql_transport::controller& ctl) {
    return ctx.http_server.set_routes([&ctx, &ctl] (routes& r) { set_transport_controller(ctx, r, ctl); });
}

future<> unset_transport_controller(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_transport_controller(ctx, r); });
}

future<> set_rpc_controller(http_context& ctx, thrift_controller& ctl) {
    return ctx.http_server.set_routes([&ctx, &ctl] (routes& r) { set_rpc_controller(ctx, r, ctl); });
}

future<> unset_rpc_controller(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_rpc_controller(ctx, r); });
}

future<> set_server_storage_service(http_context& ctx) {
    return register_api(ctx, "storage_service", "The storage service API", set_storage_service);
}

future<> set_server_repair(http_context& ctx, sharded<repair_service>& repair) {
    return ctx.http_server.set_routes([&ctx, &repair] (routes& r) { set_repair(ctx, r, repair); });
}

future<> unset_server_repair(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_repair(ctx, r); });
}

future<> set_server_snapshot(http_context& ctx, sharded<db::snapshot_ctl>& snap_ctl) {
    return ctx.http_server.set_routes([&ctx, &snap_ctl] (routes& r) { set_snapshot(ctx, r, snap_ctl); });
}

future<> unset_server_snapshot(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_snapshot(ctx, r); });
}

future<> set_server_snitch(http_context& ctx) {
    return register_api(ctx, "endpoint_snitch_info", "The endpoint snitch info API", set_endpoint_snitch);
}

future<> set_server_gossip(http_context& ctx) {
    return register_api(ctx, "gossiper",
                "The gossiper API", set_gossiper);
}

future<> set_server_load_sstable(http_context& ctx) {
    return register_api(ctx, "column_family",
                "The column family API", set_column_family);
}

future<> set_server_messaging_service(http_context& ctx, sharded<netw::messaging_service>& ms) {
    return register_api(ctx, "messaging_service",
                "The messaging service API", [&ms] (http_context& ctx, routes& r) {
                    set_messaging_service(ctx, r, ms);
                });
}
future<> unset_server_messaging_service(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_messaging_service(ctx, r); });
}

future<> set_server_storage_proxy(http_context& ctx) {
    return register_api(ctx, "storage_proxy",
                "The storage proxy API", set_storage_proxy);
}

future<> set_server_stream_manager(http_context& ctx) {
    return register_api(ctx, "stream_manager",
                "The stream manager API", set_stream_manager);
}

future<> set_server_cache(http_context& ctx) {
    return register_api(ctx, "cache_service",
            "The cache service API", set_cache_service);
}

future<> set_hinted_handoff(http_context& ctx) {
    return register_api(ctx, "hinted_handoff",
                "The hinted handoff API", [] (http_context& ctx, routes& r) {
                    set_hinted_handoff(ctx, r);
                });
}

future<> unset_hinted_handoff(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_hinted_handoff(ctx, r); });
}

future<> set_server_gossip_settle(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx](routes& r) {
        rb->register_function(r, "failure_detector",
                "The failure detector API");
        set_failure_detector(ctx,r);
    });
}

future<> set_server_done(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx](routes& r) {
        rb->register_function(r, "compaction_manager",
                "The Compaction manager API");
        set_compaction_manager(ctx, r);
        rb->register_function(r, "lsa", "Log-structured allocator API");
        set_lsa(ctx, r);

        rb->register_function(r, "commitlog",
                "The commit log API");
        set_commitlog(ctx,r);
        rb->register_function(r, "collectd",
                "The collectd API");
        set_collectd(ctx, r);
        rb->register_function(r, "error_injection",
                "The error injection API");
        set_error_injection(ctx, r);
    });
}

}

