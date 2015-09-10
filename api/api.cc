/*
 * Copyright 2015 Cloudius Systems
 */

#include "api.hh"
#include "http/file_handler.hh"
#include "http/transformers.hh"
#include "http/api_docs.hh"
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
#include "http/exception.hh"
#include "stream_manager.hh"

namespace api {

static std::unique_ptr<reply> exception_reply(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (const no_such_keyspace& ex) {
        throw bad_param_exception(ex.what());
    }
    // We never going to get here
    return std::make_unique<reply>();
}

future<> set_server(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx](routes& r) {
        r.register_exeption_handler(exception_reply);
        httpd::directory_handler* dir = new httpd::directory_handler(ctx.api_dir,
                new content_replace("html"));
        r.put(GET, "/ui", new httpd::file_handler(ctx.api_dir + "/index.html",
                new content_replace("html")));
        r.add(GET, url("/ui").remainder("path"), dir);

        rb->set_api_doc(r);
        rb->register_function(r, "storage_service",
                                "The storage service API");
        set_storage_service(ctx,r);
        rb->register_function(r, "commitlog",
                                "The commit log API");
        set_commitlog(ctx,r);
        rb->register_function(r, "gossiper",
                                "The gossiper API");
        set_gossiper(ctx,r);
        rb->register_function(r, "column_family",
                                        "The column family API");
        set_column_family(ctx, r);

        rb->register_function(r, "lsa", "Log-structured allocator API");
        set_lsa(ctx, r);

        rb->register_function(r, "failure_detector",
                                "The failure detector API");
        set_failure_detector(ctx,r);

        rb->register_function(r, "messaging_service",
                "The messaging service API");
        set_messaging_service(ctx, r);
        rb->register_function(r, "storage_proxy",
                                        "The storage proxy API");
        set_storage_proxy(ctx, r);

        rb->register_function(r, "cache_service",
                                                "The cache service API");
        set_cache_service(ctx,r);
        rb->register_function(r, "collectd",
                "The collectd API");
        set_collectd(ctx, r);
        rb->register_function(r, "endpoint_snitch_info",
                        "The endpoint snitch info API");
        set_endpoint_snitch(ctx, r);
        rb->register_function(r, "compaction_manager",
                        "The Compaction manager API");
        set_compaction_manager(ctx, r);
        rb->register_function(r, "hinted_handoff",
                        "The hinted handoff API");
        set_hinted_handoff(ctx, r);
        rb->register_function(r, "stream_manager",
                "The stream manager API");
        set_stream_manager(ctx, r);
    });
}

}

