/*
 * Copyright 2015 Cloudius Systems
 */

#include "api.hh"
#include "http/api_docs.hh"
#include "storage_service.hh"
#include "commitlog.hh"
#include "gossiper.hh"
#include "failure_detector.hh"

namespace api {

future<> set_server(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > ("api/api-doc/");

    return ctx.http_server.set_routes([rb, &ctx](routes& r) {
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
        rb->register_function(r, "failure_detector",
                                "The failure detector API");
        set_failure_detector(ctx,r);

    });
}

}

