/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "api.hh"
#include <seastar/http/file_handler.hh>
#include <seastar/http/transformers.hh>
#include <seastar/http/api_docs.hh>
#include "cql_server_test.hh"
#include "storage_service.hh"
#include "token_metadata.hh"
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
#include "authorization_cache.hh"
#include <seastar/http/exception.hh>
#include "stream_manager.hh"
#include "system.hh"
#include "api/config.hh"
#include "task_manager.hh"
#include "task_manager_test.hh"
#include "tasks.hh"
#include "raft.hh"

logging::logger apilog("api");

namespace api {
using namespace seastar::httpd;

static std::unique_ptr<reply> exception_reply(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (const replica::no_such_keyspace& ex) {
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
        rb02->register_api_file(r, "metrics");
        rb->register_function(r, "system",
                "The system related API");
        rb02->add_definitions_file(r, "metrics");
        set_system(ctx, r);
        rb->register_function(r, "error_injection",
            "The error injection API");
        set_error_injection(ctx, r);
        rb->register_function(r, "storage_proxy",
                "The storage proxy API");
        rb->register_function(r, "storage_service",
                "The storage service API");
    });
}

future<> set_server_config(http_context& ctx, const db::config& cfg) {
    auto rb02 = std::make_shared < api_registry_builder20 > (ctx.api_doc, "/v2");
    return ctx.http_server.set_routes([&ctx, &cfg, rb02](routes& r) {
        set_config(rb02, ctx, r, cfg, false);
    });
}

future<> unset_server_config(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_config(ctx, r); });
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

future<> set_thrift_controller(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { set_thrift_controller(ctx, r); });
}

future<> unset_thrift_controller(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_thrift_controller(ctx, r); });
}

future<> set_server_storage_service(http_context& ctx, sharded<service::storage_service>& ss, service::raft_group0_client& group0_client) {
    return ctx.http_server.set_routes([&ctx, &ss, &group0_client] (routes& r) {
            set_storage_service(ctx, r, ss, group0_client);
        });
}

future<> unset_server_storage_service(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_storage_service(ctx, r); });
}

future<> set_load_meter(http_context& ctx, service::load_meter& lm) {
    return ctx.http_server.set_routes([&ctx, &lm] (routes& r) { set_load_meter(ctx, r, lm); });
}

future<> unset_load_meter(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_load_meter(ctx, r); });
}

future<> set_server_sstables_loader(http_context& ctx, sharded<sstables_loader>& sst_loader) {
    return ctx.http_server.set_routes([&ctx, &sst_loader] (routes& r) { set_sstables_loader(ctx, r, sst_loader); });
}

future<> unset_server_sstables_loader(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_sstables_loader(ctx, r); });
}

future<> set_server_view_builder(http_context& ctx, sharded<db::view::view_builder>& vb) {
    return ctx.http_server.set_routes([&ctx, &vb] (routes& r) { set_view_builder(ctx, r, vb); });
}

future<> unset_server_view_builder(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_view_builder(ctx, r); });
}

future<> set_server_repair(http_context& ctx, sharded<repair_service>& repair) {
    return ctx.http_server.set_routes([&ctx, &repair] (routes& r) { set_repair(ctx, r, repair); });
}

future<> unset_server_repair(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_repair(ctx, r); });
}

future<> set_server_authorization_cache(http_context &ctx, sharded<auth::service> &auth_service) {
    return register_api(ctx, "authorization_cache",
                "The authorization cache API", [&auth_service] (http_context &ctx, routes &r) {
                     set_authorization_cache(ctx, r, auth_service);
                 });
}

future<> unset_server_authorization_cache(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_authorization_cache(ctx, r); });
}

future<> set_server_snapshot(http_context& ctx, sharded<db::snapshot_ctl>& snap_ctl) {
    return ctx.http_server.set_routes([&ctx, &snap_ctl] (routes& r) { set_snapshot(ctx, r, snap_ctl); });
}

future<> unset_server_snapshot(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_snapshot(ctx, r); });
}

future<> set_server_token_metadata(http_context& ctx, sharded<locator::shared_token_metadata>& tm) {
    return ctx.http_server.set_routes([&ctx, &tm] (routes& r) { set_token_metadata(ctx, r, tm); });
}

future<> unset_server_token_metadata(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_token_metadata(ctx, r); });
}

future<> set_server_snitch(http_context& ctx, sharded<locator::snitch_ptr>& snitch) {
    return register_api(ctx, "endpoint_snitch_info", "The endpoint snitch info API", [&snitch] (http_context& ctx, routes& r) {
        set_endpoint_snitch(ctx, r, snitch);
    });
}

future<> unset_server_snitch(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_endpoint_snitch(ctx, r); });
}

future<> set_server_gossip(http_context& ctx, sharded<gms::gossiper>& g) {
    co_await register_api(ctx, "gossiper",
                "The gossiper API", [&g] (http_context& ctx, routes& r) {
                    set_gossiper(ctx, r, g.local());
                });
    co_await register_api(ctx, "failure_detector",
                "The failure detector API", [&g] (http_context& ctx, routes& r) {
                    set_failure_detector(ctx, r, g.local());
                });
}

future<> unset_server_gossip(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) {
        unset_gossiper(ctx, r);
        unset_failure_detector(ctx, r);
    });
}

future<> set_server_column_family(http_context& ctx, sharded<db::system_keyspace>& sys_ks) {
    return register_api(ctx, "column_family",
                "The column family API", [&sys_ks] (http_context& ctx, routes& r) {
                    set_column_family(ctx, r, sys_ks);
                });
}

future<> unset_server_column_family(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_column_family(ctx, r); });
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

future<> set_server_storage_proxy(http_context& ctx, sharded<service::storage_proxy>& proxy) {
    return ctx.http_server.set_routes([&ctx, &proxy] (routes& r) { set_storage_proxy(ctx, r, proxy); });
}

future<> unset_server_storage_proxy(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_storage_proxy(ctx, r); });
}

future<> set_server_stream_manager(http_context& ctx, sharded<streaming::stream_manager>& sm) {
    return register_api(ctx, "stream_manager",
                "The stream manager API", [&sm] (http_context& ctx, routes& r) {
                    set_stream_manager(ctx, r, sm);
                });
}

future<> unset_server_stream_manager(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_stream_manager(ctx, r); });
}

future<> set_server_cache(http_context& ctx) {
    return register_api(ctx, "cache_service",
            "The cache service API", set_cache_service);
}

future<> unset_server_cache(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_cache_service(ctx, r); });
}

future<> set_hinted_handoff(http_context& ctx, sharded<service::storage_proxy>& proxy) {
    return register_api(ctx, "hinted_handoff",
                "The hinted handoff API", [&proxy] (http_context& ctx, routes& r) {
                    set_hinted_handoff(ctx, r, proxy);
                });
}

future<> unset_hinted_handoff(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_hinted_handoff(ctx, r); });
}

future<> set_server_compaction_manager(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx](routes& r) {
        rb->register_function(r, "compaction_manager",
                "The Compaction manager API");
        set_compaction_manager(ctx, r);
    });
}

future<> set_server_done(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx](routes& r) {
        rb->register_function(r, "lsa", "Log-structured allocator API");
        set_lsa(ctx, r);

        rb->register_function(r, "commitlog",
                "The commit log API");
        set_commitlog(ctx,r);
        rb->register_function(r, "collectd",
                "The collectd API");
        set_collectd(ctx, r);
    });
}

future<> set_server_task_manager(http_context& ctx, sharded<tasks::task_manager>& tm, lw_shared_ptr<db::config> cfg) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx, &tm, &cfg = *cfg](routes& r) {
        rb->register_function(r, "task_manager",
                "The task manager API");
        set_task_manager(ctx, r, tm, cfg);
    });
}

future<> unset_server_task_manager(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_task_manager(ctx, r); });
}

#ifndef SCYLLA_BUILD_MODE_RELEASE

future<> set_server_task_manager_test(http_context& ctx, sharded<tasks::task_manager>& tm) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx, &tm](routes& r) mutable {
        rb->register_function(r, "task_manager_test",
                "The task manager test API");
        set_task_manager_test(ctx, r, tm);
    });
}

future<> unset_server_task_manager_test(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_task_manager_test(ctx, r); });
}

future<> set_server_cql_server_test(http_context& ctx, cql_transport::controller& ctl) {
    return register_api(ctx, "cql_server_test", "The CQL server test API", [&ctl] (http_context& ctx, routes& r) {
        set_cql_server_test(ctx, r, ctl);
    });
}

future<> unset_server_cql_server_test(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_cql_server_test(ctx, r); });
}

#endif

future<> set_server_tasks_compaction_module(http_context& ctx, sharded<service::storage_service>& ss, sharded<db::snapshot_ctl>& snap_ctl) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx, &ss, &snap_ctl](routes& r) {
        rb->register_function(r, "tasks",
                "The tasks API");
        set_tasks_compaction_module(ctx, r, ss, snap_ctl);
    });
}

future<> unset_server_tasks_compaction_module(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_tasks_compaction_module(ctx, r); });
}

future<> set_server_raft(http_context& ctx, sharded<service::raft_group_registry>& raft_gr) {
    auto rb = std::make_shared<api_registry_builder>(ctx.api_doc);
    return ctx.http_server.set_routes([rb, &ctx, &raft_gr] (routes& r) {
        rb->register_function(r, "raft", "The Raft API");
        set_raft(ctx, r, raft_gr);
    });
}

future<> unset_server_raft(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_raft(ctx, r); });
}

void req_params::process(const request& req) {
    // Process mandatory parameters
    for (auto& [name, ent] : params) {
        if (!ent.is_mandatory) {
            continue;
        }
        try {
            ent.value = req.get_path_param(name);
        } catch (std::out_of_range&) {
            throw httpd::bad_param_exception(fmt::format("Mandatory parameter '{}' was not provided", name));
        }
    }

    // Process optional parameters
    for (auto& [name, value] : req.query_parameters) {
        try {
            auto& ent = params.at(name);
            if (ent.is_mandatory) {
                throw httpd::bad_param_exception(fmt::format("Parameter '{}' is expected to be provided as part of the request url", name));
            }
            ent.value = value;
        } catch (std::out_of_range&) {
            throw httpd::bad_param_exception(fmt::format("Unsupported optional parameter '{}'", name));
        }
    }
}

}

