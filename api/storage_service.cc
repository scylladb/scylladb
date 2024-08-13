/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "storage_service.hh"
#include "api/api.hh"
#include "api/api-doc/column_family.json.hh"
#include "api/api-doc/storage_service.json.hh"
#include "api/api-doc/storage_proxy.json.hh"
#include "api/scrub_status.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "utils/hash.hh"
#include <optional>
#include <sstream>
#include <time.h>
#include <algorithm>
#include <functional>
#include <iterator>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/algorithm/string/trim_all.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/functional/hash.hpp>
#include <fmt/ranges.h>
#include "service/raft/raft_group0_client.hh"
#include "service/storage_service.hh"
#include "service/load_meter.hh"
#include "db/commitlog/commitlog.hh"
#include "gms/gossiper.hh"
#include "db/system_keyspace.hh"
#include <seastar/http/exception.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/exception.hh>
#include "repair/row_level.hh"
#include "locator/snitch_base.hh"
#include "column_family.hh"
#include "log.hh"
#include "release.hh"
#include "compaction/compaction_manager.hh"
#include "compaction/task_manager_module.hh"
#include "sstables/sstables.hh"
#include "replica/database.hh"
#include "db/extensions.hh"
#include "db/snapshot-ctl.hh"
#include "transport/controller.hh"
#include "locator/token_metadata.hh"
#include "cdc/generation_service.hh"
#include "locator/abstract_replication_strategy.hh"
#include "sstables_loader.hh"
#include "db/view/view_builder.hh"
#include "utils/user_provided_param.hh"

using namespace seastar::httpd;
using namespace std::chrono_literals;

extern logging::logger apilog;

namespace api {

namespace ss = httpd::storage_service_json;
namespace sp = httpd::storage_proxy_json;
namespace cf = httpd::column_family_json;
using namespace json;

sstring validate_keyspace(const http_context& ctx, sstring ks_name) {
    if (ctx.db.local().has_keyspace(ks_name)) {
        return ks_name;
    }
    throw bad_param_exception(replica::no_such_keyspace(ks_name).what());
}

sstring validate_keyspace(const http_context& ctx, const std::unique_ptr<http::request>& req) {
    return validate_keyspace(ctx, req->get_path_param("keyspace"));
}

sstring validate_keyspace(const http_context& ctx, const http::request& req) {
    return validate_keyspace(ctx, req.get_path_param("keyspace"));
}

void validate_table(const http_context& ctx, sstring ks_name, sstring table_name) {
    auto& db = ctx.db.local();
    try {
        db.find_column_family(ks_name, table_name);
    } catch (replica::no_such_column_family& e) {
        throw bad_param_exception(e.what());
    }
}

static void ensure_tablets_disabled(const http_context& ctx, const sstring& ks_name, const sstring& api_endpoint_path) {
    if (ctx.db.local().find_keyspace(ks_name).uses_tablets()) {
        throw bad_param_exception{fmt::format("{} is per-table in keyspace '{}'. Please provide table name using 'cf' parameter.", api_endpoint_path, ks_name)};
    }
}

static bool any_of_keyspaces_use_tablets(const http_context& ctx) {
    auto& db = ctx.db.local();
    auto uses_tablets = [&db](const auto& ks_name) {
        return db.find_keyspace(ks_name).uses_tablets();
    };

    auto keyspaces = db.get_all_keyspaces();
    return std::any_of(std::begin(keyspaces), std::end(keyspaces), uses_tablets);
}

locator::host_id validate_host_id(const sstring& param) {
    auto hoep = locator::host_id_or_endpoint(param, locator::host_id_or_endpoint::param_type::host_id);
    return hoep.id();
}

bool validate_bool(const sstring& param) {
    if (param == "true") {
        return true;
    } else if (param == "false") {
        return false;
    } else {
        throw std::runtime_error("Parameter must be either 'true' or 'false'");
    }
}

static
int64_t validate_int(const sstring& param) {
    return std::atoll(param.c_str());
}

// splits a request parameter assumed to hold a comma-separated list of table names
// verify that the tables are found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective no_such_column_family error.
std::vector<sstring> parse_tables(const sstring& ks_name, const http_context& ctx, sstring value) {
    if (value.empty()) {
        return map_keys(ctx.db.local().find_keyspace(ks_name).metadata().get()->cf_meta_data());
    }
    std::vector<sstring> names = split(value, ",");
    try {
        for (const auto& table_name : names) {
            ctx.db.local().find_column_family(ks_name, table_name);
        }
    } catch (const replica::no_such_column_family& e) {
        throw bad_param_exception(e.what());
    }
    return names;
}

std::vector<sstring> parse_tables(const sstring& ks_name, const http_context& ctx, const std::unordered_map<sstring, sstring>& query_params, sstring param_name) {
    auto it = query_params.find(param_name);
    if (it == query_params.end()) {
        return {};
    }
    return parse_tables(ks_name, ctx, it->second);
}

std::vector<table_info> parse_table_infos(const sstring& ks_name, const http_context& ctx, sstring value) {
    std::vector<table_info> res;
    try {
        if (value.empty()) {
            const auto& cf_meta_data = ctx.db.local().find_keyspace(ks_name).metadata().get()->cf_meta_data();
            res.reserve(cf_meta_data.size());
            for (const auto& [name, schema] : cf_meta_data) {
                res.emplace_back(table_info{name, schema->id()});
            }
        } else {
            std::vector<sstring> names = split(value, ",");
            res.reserve(names.size());
            const auto& db = ctx.db.local();
            for (const auto& table_name : names) {
                res.emplace_back(table_info{table_name, db.find_uuid(ks_name, table_name)});
            }
        }
    } catch (const replica::no_such_keyspace& e) {
        throw bad_param_exception(e.what());
    } catch (const replica::no_such_column_family& e) {
        throw bad_param_exception(e.what());
    }
    return res;
}

std::vector<table_info> parse_table_infos(const sstring& ks_name, const http_context& ctx, const std::unordered_map<sstring, sstring>& query_params, sstring param_name) {
    auto it = query_params.find(param_name);
    return parse_table_infos(ks_name, ctx, it != query_params.end() ? it->second : "");
}

static ss::token_range token_range_endpoints_to_json(const dht::token_range_endpoints& d) {
    ss::token_range r;
    r.start_token = d._start_token;
    r.end_token = d._end_token;
    r.endpoints = d._endpoints;
    r.rpc_endpoints = d._rpc_endpoints;
    for (auto det : d._endpoint_details) {
        ss::endpoint_detail ed;
        ed.host = fmt::to_string(det._host);
        ed.datacenter = det._datacenter;
        if (det._rack != "") {
            ed.rack = det._rack;
        }
        r.endpoint_details.push(ed);
    }
    return r;
}

using ks_cf_func = std::function<future<json::json_return_type>(http_context&, std::unique_ptr<http::request>, sstring, std::vector<table_info>)>;

static auto wrap_ks_cf(http_context &ctx, ks_cf_func f) {
    return [&ctx, f = std::move(f)](std::unique_ptr<http::request> req) {
        auto keyspace = validate_keyspace(ctx, req);
        auto table_infos = parse_table_infos(keyspace, ctx, req->query_parameters, "cf");
        return f(ctx, std::move(req), std::move(keyspace), std::move(table_infos));
    };
}

seastar::future<json::json_return_type> run_toppartitions_query(db::toppartitions_query& q, http_context &ctx, bool legacy_request) {
    return q.scatter().then([&q, legacy_request] {
        return sleep(q.duration()).then([&q, legacy_request] {
            return q.gather(q.capacity()).then([&q, legacy_request] (auto topk_results) {
                apilog.debug("toppartitions query: processing results");
                cf::toppartitions_query_results results;

                results.read_cardinality = topk_results.read.size();
                results.write_cardinality = topk_results.write.size();

                for (auto& d: topk_results.read.top(q.list_size())) {
                    cf::toppartitions_record r;
                    r.partition = (legacy_request ? "" : "(" + d.item.schema->ks_name() + ":" + d.item.schema->cf_name() + ") ") + sstring(d.item);
                    r.count = d.count;
                    r.error = d.error;
                    results.read.push(r);
                }
                for (auto& d: topk_results.write.top(q.list_size())) {
                    cf::toppartitions_record r;
                    r.partition = (legacy_request ? "" : "(" + d.item.schema->ks_name() + ":" + d.item.schema->cf_name() + ") ") + sstring(d.item);
                    r.count = d.count;
                    r.error = d.error;
                    results.write.push(r);
                }
                return make_ready_future<json::json_return_type>(results);
            });
        });
    });
}

future<scrub_info> parse_scrub_options(const http_context& ctx, sharded<db::snapshot_ctl>& snap_ctl, std::unique_ptr<http::request> req) {
    scrub_info info;
    auto rp = req_params({
        {"keyspace", {mandatory::yes}},
        {"cf", {""}},
        {"scrub_mode", {}},
        {"skip_corrupted", {}},
        {"disable_snapshot", {}},
        {"quarantine_mode", {}},
    });
    rp.process(*req);
    info.keyspace = validate_keyspace(ctx, *rp.get("keyspace"));
    info.column_families = parse_tables(info.keyspace, ctx, *rp.get("cf"));
    auto scrub_mode_opt = rp.get("scrub_mode");
    auto scrub_mode = sstables::compaction_type_options::scrub::mode::abort;

    if (!scrub_mode_opt) {
        const auto skip_corrupted = rp.get_as<bool>("skip_corrupted").value_or(false);

        if (skip_corrupted) {
            scrub_mode = sstables::compaction_type_options::scrub::mode::skip;
        }
    } else {
        auto scrub_mode_str = *scrub_mode_opt;
        if (scrub_mode_str == "ABORT") {
            scrub_mode = sstables::compaction_type_options::scrub::mode::abort;
        } else if (scrub_mode_str == "SKIP") {
            scrub_mode = sstables::compaction_type_options::scrub::mode::skip;
        } else if (scrub_mode_str == "SEGREGATE") {
            scrub_mode = sstables::compaction_type_options::scrub::mode::segregate;
        } else if (scrub_mode_str == "VALIDATE") {
            scrub_mode = sstables::compaction_type_options::scrub::mode::validate;
        } else {
            throw httpd::bad_param_exception(fmt::format("Unknown argument for 'scrub_mode' parameter: {}", scrub_mode_str));
        }
    }

    if (!req_param<bool>(*req, "disable_snapshot", false)) {
        auto tag = format("pre-scrub-{:d}", db_clock::now().time_since_epoch().count());
        co_await coroutine::parallel_for_each(info.column_families, [&snap_ctl, keyspace = info.keyspace, tag](sstring cf) {
            // We always pass here db::snapshot_ctl::snap_views::no since:
            // 1. When scrubbing particular tables, there's no need to auto-snapshot their views.
            // 2. When scrubbing the whole keyspace, column_families will contain both base tables and views.
            return snap_ctl.local().take_column_family_snapshot(keyspace, cf, tag, db::snapshot_ctl::snap_views::no, db::snapshot_ctl::skip_flush::no);
        });
    }

    info.opts = {
        .operation_mode = scrub_mode,
    };
    const sstring quarantine_mode_str = req_param<sstring>(*req, "quarantine_mode", "INCLUDE");
    if (quarantine_mode_str == "INCLUDE") {
        info.opts.quarantine_operation_mode = sstables::compaction_type_options::scrub::quarantine_mode::include;
    } else if (quarantine_mode_str == "EXCLUDE") {
        info.opts.quarantine_operation_mode = sstables::compaction_type_options::scrub::quarantine_mode::exclude;
    } else if (quarantine_mode_str == "ONLY") {
        info.opts.quarantine_operation_mode = sstables::compaction_type_options::scrub::quarantine_mode::only;
    } else {
        throw httpd::bad_param_exception(fmt::format("Unknown argument for 'quarantine_mode' parameter: {}", quarantine_mode_str));
    }

    co_return info;
}

void set_transport_controller(http_context& ctx, routes& r, cql_transport::controller& ctl) {
    ss::start_native_transport.set(r, [&ctl](std::unique_ptr<http::request> req) {
        return smp::submit_to(0, [&] {
            return ctl.start_server();
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::stop_native_transport.set(r, [&ctl](std::unique_ptr<http::request> req) {
        return smp::submit_to(0, [&] {
            return ctl.request_stop_server();
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_native_transport_running.set(r, [&ctl] (std::unique_ptr<http::request> req) {
        return smp::submit_to(0, [&] {
            return !ctl.listen_addresses().empty();
        }).then([] (bool running) {
            return make_ready_future<json::json_return_type>(running);
        });
    });
}

void unset_transport_controller(http_context& ctx, routes& r) {
    ss::start_native_transport.unset(r);
    ss::stop_native_transport.unset(r);
    ss::is_native_transport_running.unset(r);
}

// NOTE: preserved only for backward compatibility
void set_thrift_controller(http_context& ctx, routes& r) {
    ss::is_thrift_server_running.set(r, [] (std::unique_ptr<http::request> req) {
        return smp::submit_to(0, [] {
            return make_ready_future<json::json_return_type>(false);
        });
    });
}

void unset_thrift_controller(http_context& ctx, routes& r) {
    ss::is_thrift_server_running.unset(r);
}

void set_repair(http_context& ctx, routes& r, sharded<repair_service>& repair) {
    ss::repair_async.set(r, [&ctx, &repair](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        static std::unordered_set<sstring> options = {"primaryRange", "parallelism", "incremental",
                "jobThreads", "ranges", "columnFamilies", "dataCenters", "hosts", "ignore_nodes", "trace",
                "startToken", "endToken", "ranges_parallelism", "small_table_optimization"};

        // Nodetool still sends those unsupported options. Ignore them to avoid failing nodetool repair.
        static std::unordered_set<sstring> legacy_options_to_ignore = {"pullRepair", "ignoreUnreplicatedKeyspaces"};

        for (auto& x : req->query_parameters) {
            if (legacy_options_to_ignore.contains(x.first)) {
                continue;
            }
            if (!options.contains(x.first)) {
                throw httpd::bad_param_exception(format("option {} is not supported", x.first));
            }
        }
        std::unordered_map<sstring, sstring> options_map;
        for (auto o : options) {
            auto s = req->get_query_param(o);
            if (s != "") {
                options_map[o] = s;
            }
        }

        // The repair process is asynchronous: repair_start only starts it and
        // returns immediately, not waiting for the repair to finish. The user
        // then has other mechanisms to track the ongoing repair's progress,
        // or stop it.
        try {
            int res = co_await repair_start(repair, validate_keyspace(ctx, req), options_map);
            co_return json::json_return_type(res);
        } catch (const std::invalid_argument& e) {
            // if the option is not sane, repair_start() throws immediately, so
            // convert the exception to an HTTP error
            throw httpd::bad_param_exception(e.what());
        }
    });

    ss::get_active_repair_async.set(r, [&repair] (std::unique_ptr<http::request> req) {
        return repair.local().get_active_repairs().then([] (std::vector<int> res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    ss::repair_async_status.set(r, [&repair] (std::unique_ptr<http::request> req) {
        return repair.local().get_status(boost::lexical_cast<int>( req->get_query_param("id")))
                .then_wrapped([] (future<repair_status>&& fut) {
            ss::ns_repair_async_status::return_type_wrapper res;
            try {
                res = fut.get();
            } catch(std::runtime_error& e) {
                throw httpd::bad_param_exception(e.what());
            }
            return make_ready_future<json::json_return_type>(json::json_return_type(res));
        });
    });

    ss::repair_await_completion.set(r, [&repair] (std::unique_ptr<http::request> req) {
        int id;
        using clock = std::chrono::steady_clock;
        clock::time_point expire;
        try {
            id = boost::lexical_cast<int>(req->get_query_param("id"));
            // If timeout is not provided, it means no timeout.
            sstring s = req->get_query_param("timeout");
            int64_t timeout = s.empty() ? int64_t(-1) : boost::lexical_cast<int64_t>(s);
            if (timeout < 0 && timeout != -1) {
                return make_exception_future<json::json_return_type>(
                        httpd::bad_param_exception("timeout can only be -1 (means no timeout) or non negative integer"));
            }
            if (timeout < 0) {
                expire = clock::time_point::max();
            } else {
                expire = clock::now() + std::chrono::seconds(timeout);
            }
        } catch (std::exception& e) {
            return make_exception_future<json::json_return_type>(httpd::bad_param_exception(e.what()));
        }
        return repair.local().await_completion(id, expire)
                .then_wrapped([] (future<repair_status>&& fut) {
            ss::ns_repair_async_status::return_type_wrapper res;
            try {
                res = fut.get();
            } catch (std::exception& e) {
                return make_exception_future<json::json_return_type>(httpd::bad_param_exception(e.what()));
            }
            return make_ready_future<json::json_return_type>(json::json_return_type(res));
        });
    });

    ss::force_terminate_all_repair_sessions.set(r, [&repair] (std::unique_ptr<http::request> req) {
        return repair.local().abort_all().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::force_terminate_all_repair_sessions_new.set(r, [&repair] (std::unique_ptr<http::request> req) {
        return repair.local().abort_all().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

}

void unset_repair(http_context& ctx, routes& r) {
    ss::repair_async.unset(r);
    ss::get_active_repair_async.unset(r);
    ss::repair_async_status.unset(r);
    ss::repair_await_completion.unset(r);
    ss::force_terminate_all_repair_sessions.unset(r);
    ss::force_terminate_all_repair_sessions_new.unset(r);
}

void set_sstables_loader(http_context& ctx, routes& r, sharded<sstables_loader>& sst_loader) {
    ss::load_new_ss_tables.set(r, [&ctx, &sst_loader](std::unique_ptr<http::request> req) {
        auto ks = validate_keyspace(ctx, req);
        auto cf = req->get_query_param("cf");
        auto stream = req->get_query_param("load_and_stream");
        auto primary_replica = req->get_query_param("primary_replica_only");
        boost::algorithm::to_lower(stream);
        boost::algorithm::to_lower(primary_replica);
        bool load_and_stream = stream == "true" || stream == "1";
        bool primary_replica_only = primary_replica == "true" || primary_replica == "1";
        // No need to add the keyspace, since all we want is to avoid always sending this to the same
        // CPU. Even then I am being overzealous here. This is not something that happens all the time.
        auto coordinator = std::hash<sstring>()(cf) % smp::count;
        return sst_loader.invoke_on(coordinator,
                [ks = std::move(ks), cf = std::move(cf),
                load_and_stream, primary_replica_only] (sstables_loader& loader) {
            return loader.load_new_sstables(ks, cf, load_and_stream, primary_replica_only);
        }).then_wrapped([] (auto&& f) {
            if (f.failed()) {
                auto msg = fmt::format("Failed to load new sstables: {}", f.get_exception());
                return make_exception_future<json::json_return_type>(httpd::server_error_exception(msg));
            }
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::start_restore.set(r, [&sst_loader] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto endpoint = req->get_query_param("endpoint");
        auto keyspace = req->get_query_param("keyspace");
        auto table = req->get_query_param("table");
        auto bucket = req->get_query_param("bucket");
        auto snapshot_name = req->get_query_param("snapshot");
        if (table.empty()) {
            // TODO: If missing, should restore all tables
            throw httpd::bad_param_exception("The table name must be specified");
        }

        auto task_id = co_await sst_loader.local().download_new_sstables(keyspace, table, endpoint, bucket, snapshot_name);
        co_return json::json_return_type(fmt::to_string(task_id));
    });

}

void unset_sstables_loader(http_context& ctx, routes& r) {
    ss::load_new_ss_tables.unset(r);
    ss::start_restore.unset(r);
}

void set_view_builder(http_context& ctx, routes& r, sharded<db::view::view_builder>& vb) {
    ss::view_build_statuses.set(r, [&ctx, &vb] (std::unique_ptr<http::request> req) {
        auto keyspace = validate_keyspace(ctx, req);
        auto view = req->get_path_param("view");
        return vb.local().view_build_statuses(std::move(keyspace), std::move(view)).then([] (std::unordered_map<sstring, sstring> status) {
            std::vector<storage_service_json::mapper> res;
            return make_ready_future<json::json_return_type>(map_to_key_value(std::move(status), res));
        });
    });

}

void unset_view_builder(http_context& ctx, routes& r) {
    ss::view_build_statuses.unset(r);
}

static future<json::json_return_type> describe_ring_as_json(sharded<service::storage_service>& ss, sstring keyspace) {
    co_return json::json_return_type(stream_range_as_array(co_await ss.local().describe_ring(keyspace), token_range_endpoints_to_json));
}

static future<json::json_return_type> describe_ring_as_json_for_table(const sharded<service::storage_service>& ss, sstring keyspace, sstring table) {
    co_return json::json_return_type(stream_range_as_array(co_await ss.local().describe_ring_for_table(keyspace, table), token_range_endpoints_to_json));
}

void set_storage_service(http_context& ctx, routes& r, sharded<service::storage_service>& ss, service::raft_group0_client& group0_client) {
    ss::get_commitlog.set(r, [&ctx](const_req req) {
        return ctx.db.local().commitlog()->active_config().commit_log_location;
    });

    ss::get_token_endpoint.set(r, [&ctx, &ss] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        const auto keyspace_name = req->get_query_param("keyspace");
        const auto table_name = req->get_query_param("cf");

        std::map<dht::token, gms::inet_address> token_endpoints;
        if (keyspace_name.empty() && table_name.empty()) {
            token_endpoints = ss.local().get_token_to_endpoint_map();
        } else if (!keyspace_name.empty() && !table_name.empty()) {
            auto& db = ctx.db.local();
            if (!db.has_schema(keyspace_name, table_name)) {
                throw bad_param_exception(fmt::format("Failed to find table {}.{}", keyspace_name, table_name));
            }
            token_endpoints = co_await ss.local().get_tablet_to_endpoint_map(db.find_schema(keyspace_name, table_name)->id());
        } else {
            throw bad_param_exception("Either provide both keyspace and table (for tablet table) or neither (for vnodes)");
        }

        co_return json::json_return_type(stream_range_as_array(token_endpoints, [](const auto& i) {
            storage_service_json::mapper val;
            val.key = fmt::to_string(i.first);
            val.value = fmt::to_string(i.second);
            return val;
        }));
    });

    ss::toppartitions_generic.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        bool filters_provided = false;

        std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash> table_filters {};
        if (req->query_parameters.contains("table_filters")) {
            filters_provided = true;
            auto filters = req->get_query_param("table_filters");
            std::stringstream ss { filters };
            std::string filter;
            while (!filters.empty() && ss.good()) {
                std::getline(ss, filter, ',');
                table_filters.emplace(parse_fully_qualified_cf_name(filter));
            }
        }

        std::unordered_set<sstring> keyspace_filters {};
        if (req->query_parameters.contains("keyspace_filters")) {
            filters_provided = true;
            auto filters = req->get_query_param("keyspace_filters");
            std::stringstream ss { filters };
            std::string filter;
            while (!filters.empty() && ss.good()) {
                std::getline(ss, filter, ',');
                keyspace_filters.emplace(std::move(filter));
            }
        }

        // when the query is empty return immediately
        if (filters_provided && table_filters.empty() && keyspace_filters.empty()) {
            apilog.debug("toppartitions query: processing results");
            httpd::column_family_json::toppartitions_query_results results;

            results.read_cardinality = 0;
            results.write_cardinality = 0;

            return make_ready_future<json::json_return_type>(results);
        }

        api::req_param<std::chrono::milliseconds, unsigned> duration{*req, "duration", 1000ms};
        api::req_param<unsigned> capacity(*req, "capacity", 256);
        api::req_param<unsigned> list_size(*req, "list_size", 10);

        apilog.info("toppartitions query: #table_filters={} #keyspace_filters={} duration={} list_size={} capacity={}",
            !table_filters.empty() ? std::to_string(table_filters.size()) : "all", !keyspace_filters.empty() ? std::to_string(keyspace_filters.size()) : "all", duration.param, list_size.param, capacity.param);

        return seastar::do_with(db::toppartitions_query(ctx.db, std::move(table_filters), std::move(keyspace_filters), duration.value, list_size, capacity), [&ctx] (db::toppartitions_query& q) {
            return run_toppartitions_query(q, ctx);
        });
    });

    ss::get_release_version.set(r, [&ss](const_req req) {
        return ss.local().get_release_version();
    });

    ss::get_scylla_release_version.set(r, [](const_req req) {
        return scylla_version();
    });
    ss::get_schema_version.set(r, [&ss](const_req req) {
        return ss.local().get_schema_version();
    });

    ss::get_range_to_endpoint_map.set(r, [&ctx, &ss](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto keyspace = validate_keyspace(ctx, req);
        auto table = req->get_query_param("cf");

        auto erm = std::invoke([&]() -> locator::effective_replication_map_ptr {
            auto& ks = ctx.db.local().find_keyspace(keyspace);
            if (table.empty()) {
                ensure_tablets_disabled(ctx, keyspace, "storage_service/range_to_endpoint_map");
                return ks.get_vnode_effective_replication_map();
            } else {
                validate_table(ctx, keyspace, table);

                auto& cf = ctx.db.local().find_column_family(keyspace, table);
                return cf.get_effective_replication_map();
            }
        });

        std::vector<ss::maplist_mapper> res;
        co_return stream_range_as_array(co_await ss.local().get_range_to_address_map(erm),
                [](const std::pair<dht::token_range, inet_address_vector_replica_set>& entry){
            ss::maplist_mapper m;
            if (entry.first.start()) {
                m.key.push(entry.first.start().value().value().to_sstring());
            } else {
                m.key.push("");
            }
            if (entry.first.end()) {
                m.key.push(entry.first.end().value().value().to_sstring());
            } else {
                m.key.push("");
            }
            for (const gms::inet_address& address : entry.second) {
                m.value.push(fmt::to_string(address));
            }
            return m;
        });
    });

    ss::get_pending_range_to_endpoint_map.set(r, [&ctx](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto keyspace = validate_keyspace(ctx, req);
        std::vector<ss::maplist_mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::describe_ring.set(r, [&ctx, &ss](std::unique_ptr<http::request> req) {
        if (!req->param.exists("keyspace")) {
            throw bad_param_exception("The keyspace param is not provided");
        }
        auto keyspace = req->get_path_param("keyspace");
        auto table = req->get_query_param("table");
        if (!table.empty()) {
            validate_table(ctx, keyspace, table);
            return describe_ring_as_json_for_table(ss, keyspace, table);
        }
        return describe_ring_as_json(ss, validate_keyspace(ctx, req));
    });

    ss::get_load.set(r, [&ctx](std::unique_ptr<http::request> req) {
        return get_cf_stats(ctx, &replica::column_family_stats::live_disk_space_used);
    });

    ss::get_current_generation_number.set(r, [&ss](std::unique_ptr<http::request> req) {
        auto ep = ss.local().get_token_metadata().get_topology().my_address();
        return ss.local().gossiper().get_current_generation_number(ep).then([](gms::generation_type res) {
            return make_ready_future<json::json_return_type>(res.value());
        });
    });

    ss::get_natural_endpoints.set(r, [&ctx, &ss](const_req req) {
        auto keyspace = validate_keyspace(ctx, req);
        return container_to_vec(ss.local().get_natural_endpoints(keyspace, req.get_query_param("cf"),
                req.get_query_param("key")));
    });

    ss::cdc_streams_check_and_repair.set(r, [&ss] (std::unique_ptr<http::request> req) {
        return ss.invoke_on(0, [] (service::storage_service& ss) {
            return ss.check_and_repair_cdc_streams();
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::force_compaction.set(r, [&ctx](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto params = req_params({
            std::pair("flush_memtables", mandatory::no),
        });
        params.process(*req);
        auto flush = params.get_as<bool>("flush_memtables").value_or(true);
        apilog.info("force_compaction: flush={}", flush);

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        std::optional<flush_mode> fmopt;
        if (!flush) {
            fmopt = flush_mode::skip;
        }
        auto task = co_await compaction_module.make_and_start_task<global_major_compaction_task_impl>({}, db, fmopt);
        try {
            co_await task->done();
        } catch (...) {
            apilog.error("force_compaction failed: {}", std::current_exception());
            throw;
        }

        co_return json_void();
    });

    ss::force_keyspace_compaction.set(r, [&ctx](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto params = req_params({
            std::pair("keyspace", mandatory::yes),
            std::pair("cf", mandatory::no),
            std::pair("flush_memtables", mandatory::no),
        });
        params.process(*req);
        auto keyspace = validate_keyspace(ctx, *params.get("keyspace"));
        auto table_infos = parse_table_infos(keyspace, ctx, params.get("cf").value_or(""));
        auto flush = params.get_as<bool>("flush_memtables").value_or(true);
        apilog.debug("force_keyspace_compaction: keyspace={} tables={}, flush={}", keyspace, table_infos, flush);

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        std::optional<flush_mode> fmopt;
        if (!flush) {
            fmopt = flush_mode::skip;
        }
        auto task = co_await compaction_module.make_and_start_task<major_keyspace_compaction_task_impl>({}, std::move(keyspace), tasks::task_id::create_null_id(), db, table_infos, fmopt);
        try {
            co_await task->done();
        } catch (...) {
            apilog.error("force_keyspace_compaction: keyspace={} tables={} failed: {}", task->get_status().keyspace, table_infos, std::current_exception());
            throw;
        }

        co_return json_void();
    });

    ss::force_keyspace_cleanup.set(r, [&ctx, &ss](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto keyspace = validate_keyspace(ctx, req);
        auto table_infos = parse_table_infos(keyspace, ctx, req->query_parameters, "cf");
        const auto& rs = db.local().find_keyspace(keyspace).get_replication_strategy();
        if (rs.get_type() == locator::replication_strategy_type::local || !rs.is_vnode_based()) {
            auto reason = rs.get_type() == locator::replication_strategy_type::local ? "require" : "support";
            apilog.info("Keyspace {} does not {} cleanup", keyspace, reason);
            co_return json::json_return_type(0);
        }
        apilog.info("force_keyspace_cleanup: keyspace={} tables={}", keyspace, table_infos);
        if (!co_await ss.local().is_cleanup_allowed(keyspace)) {
            auto msg = "Can not perform cleanup operation when topology changes";
            apilog.warn("force_keyspace_cleanup: keyspace={} tables={}: {}", keyspace, table_infos, msg);
            co_await coroutine::return_exception(std::runtime_error(msg));
        }

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<cleanup_keyspace_compaction_task_impl>(
            {}, std::move(keyspace), db, table_infos, flush_mode::all_tables);
        try {
            co_await task->done();
        } catch (...) {
            apilog.error("force_keyspace_cleanup: keyspace={} tables={} failed: {}", task->get_status().keyspace, table_infos, std::current_exception());
            throw;
        }

        co_return json::json_return_type(0);
    });

    ss::cleanup_all.set(r, [&ctx, &ss](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        apilog.info("cleanup_all");
        auto done = co_await ss.invoke_on(0, [] (service::storage_service& ss) -> future<bool> {
            if (!ss.is_topology_coordinator_enabled()) {
                co_return false;
            }
            co_await ss.do_cluster_cleanup();
            co_return true;
        });
        if (done) {
            co_return json::json_return_type(0);
        }
        // fall back to the local global cleanup if topology coordinator is not enabled
        auto& db = ctx.db;
        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<global_cleanup_compaction_task_impl>({}, db);
        try {
            co_await task->done();
        } catch (...) {
            apilog.error("cleanup_all failed: {}", std::current_exception());
            throw;
        }
        co_return json::json_return_type(0);
    });

    ss::perform_keyspace_offstrategy_compaction.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<http::request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        apilog.info("perform_keyspace_offstrategy_compaction: keyspace={} tables={}", keyspace, table_infos);
        bool res = false;
        auto& compaction_module = ctx.db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<offstrategy_keyspace_compaction_task_impl>({}, std::move(keyspace), ctx.db, table_infos, &res);
        try {
            co_await task->done();
        } catch (...) {
            apilog.error("perform_keyspace_offstrategy_compaction: keyspace={} tables={} failed: {}", task->get_status().keyspace, table_infos, std::current_exception());
            throw;
        }

        co_return json::json_return_type(res);
    }));

    ss::upgrade_sstables.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<http::request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        auto& db = ctx.db;
        bool exclude_current_version = req_param<bool>(*req, "exclude_current_version", false);

        apilog.info("upgrade_sstables: keyspace={} tables={} exclude_current_version={}", keyspace, table_infos, exclude_current_version);

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<upgrade_sstables_compaction_task_impl>({}, std::move(keyspace), db, table_infos, exclude_current_version);
        try {
            co_await task->done();
        } catch (...) {
            apilog.error("upgrade_sstables: keyspace={} tables={} failed: {}", keyspace, table_infos, std::current_exception());
            throw;
        }

        co_return json::json_return_type(0);
    }));

    ss::force_flush.set(r, [&ctx](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        apilog.info("flush all tables");
        co_await ctx.db.invoke_on_all([] (replica::database& db) {
            return db.flush_all_tables();
        });
        co_return json_void();
    });

    ss::force_keyspace_flush.set(r, [&ctx](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto keyspace = validate_keyspace(ctx, req);
        auto column_families = parse_tables(keyspace, ctx, req->query_parameters, "cf");
        apilog.info("perform_keyspace_flush: keyspace={} tables={}", keyspace, column_families);
        auto& db = ctx.db;
        if (column_families.empty()) {
            co_await replica::database::flush_keyspace_on_all_shards(db, keyspace);
        } else {
            co_await replica::database::flush_tables_on_all_shards(db, keyspace, std::move(column_families));
        }
        co_return json_void();
    });


    ss::decommission.set(r, [&ss](std::unique_ptr<http::request> req) {
        apilog.info("decommission");
        return ss.local().decommission().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::move.set(r, [&ss] (std::unique_ptr<http::request> req) {
        auto new_token = req->get_query_param("new_token");
        return ss.local().move(new_token).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::remove_node.set(r, [&ss](std::unique_ptr<http::request> req) {
        auto host_id = validate_host_id(req->get_query_param("host_id"));
        std::vector<sstring> ignore_nodes_strs = utils::split_comma_separated_list(req->get_query_param("ignore_nodes"));
        apilog.info("remove_node: host_id={} ignore_nodes={}", host_id, ignore_nodes_strs);
        auto ignore_nodes = std::list<locator::host_id_or_endpoint>();
        for (const sstring& n : ignore_nodes_strs) {
            try {
                auto hoep = locator::host_id_or_endpoint(n);
                if (!ignore_nodes.empty() && hoep.has_host_id() != ignore_nodes.front().has_host_id()) {
                    throw std::runtime_error("All nodes should be identified using the same method: either Host IDs or ip addresses.");
                }
                ignore_nodes.push_back(std::move(hoep));
            } catch (...) {
                throw std::runtime_error(format("Failed to parse ignore_nodes parameter: ignore_nodes={}, node={}: {}", ignore_nodes_strs, n, std::current_exception()));
            }
        }
        return ss.local().removenode(host_id, std::move(ignore_nodes)).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::get_removal_status.set(r, [&ss](std::unique_ptr<http::request> req) {
        return ss.local().get_removal_status().then([] (auto status) {
            return make_ready_future<json::json_return_type>(status);
        });
    });

    ss::force_remove_completion.set(r, [&ss](std::unique_ptr<http::request> req) {
        return ss.local().force_remove_completion().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::set_logging_level.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto class_qualifier = req->get_query_param("class_qualifier");
        auto level = req->get_query_param("level");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_logging_levels.set(r, [](std::unique_ptr<http::request> req) {
        std::vector<ss::mapper> res;
        for (auto i : logging::logger_registry().get_all_logger_names()) {
            ss::mapper log;
            log.key = i;
            log.value = logging::level_name(logging::logger_registry().get_logger_level(i));
            res.push_back(log);
        }
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_operation_mode.set(r, [&ss](std::unique_ptr<http::request> req) {
        return ss.local().get_operation_mode().then([] (auto mode) {
            return make_ready_future<json::json_return_type>(format("{}", mode));
        });
    });

    ss::is_starting.set(r, [&ss](std::unique_ptr<http::request> req) {
        return ss.local().get_operation_mode().then([] (auto mode) {
            return make_ready_future<json::json_return_type>(mode <= service::storage_service::mode::STARTING);
        });
    });

    ss::get_drain_progress.set(r, [&ctx](std::unique_ptr<http::request> req) {
        return ctx.db.map_reduce(adder<replica::database::drain_progress>(), [] (auto& db) {
            return db.get_drain_progress();
        }).then([] (auto&& progress) {
            auto progress_str = format("Drained {}/{} ColumnFamilies", progress.remaining_cfs, progress.total_cfs);
            return make_ready_future<json::json_return_type>(std::move(progress_str));
        });
    });

    ss::drain.set(r, [&ss](std::unique_ptr<http::request> req) {
        apilog.info("drain");
        return ss.local().drain().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });
    ss::truncate.set(r, [&ctx](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto keyspace = validate_keyspace(ctx, req);
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_keyspaces.set(r, [&ctx](const_req req) {
        auto type = req.get_query_param("type");
        auto replication = req.get_query_param("replication");
        std::vector<sstring> keyspaces;
        if (type == "user") {
            keyspaces = ctx.db.local().get_user_keyspaces();
        } else if (type == "non_local_strategy") {
            keyspaces = ctx.db.local().get_non_local_strategy_keyspaces();
        } else {
            keyspaces = map_keys(ctx.db.local().get_keyspaces());
        }
        if (replication.empty() || replication == "all") {
            return keyspaces;
        }
        const auto want_tablets = replication == "tablets";
        return boost::copy_range<std::vector<sstring>>(keyspaces | boost::adaptors::filtered([&ctx, want_tablets] (const sstring& ks) {
            return ctx.db.local().find_keyspace(ks).get_replication_strategy().uses_tablets() == want_tablets;
        }));
    });

    ss::stop_gossiping.set(r, [&ss](std::unique_ptr<http::request> req) {
        apilog.info("stop_gossiping");
        return ss.local().stop_gossiping().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::start_gossiping.set(r, [&ss](std::unique_ptr<http::request> req) {
        apilog.info("start_gossiping");
        return ss.local().start_gossiping().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_gossip_running.set(r, [&ss](std::unique_ptr<http::request> req) {
        return ss.local().is_gossip_running().then([] (bool running){
            return make_ready_future<json::json_return_type>(running);
        });
    });


    ss::stop_daemon.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_initialized.set(r, [&ss](std::unique_ptr<http::request> req) {
        return ss.local().get_operation_mode().then([&ss] (auto mode) {
            bool is_initialized = mode >= service::storage_service::mode::STARTING && mode != service::storage_service::mode::MAINTENANCE;
            if (mode == service::storage_service::mode::NORMAL) {
                is_initialized = ss.local().gossiper().is_enabled();
            }
            return make_ready_future<json::json_return_type>(is_initialized);
        });
    });

    ss::join_ring.set(r, [](std::unique_ptr<http::request> req) {
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_joined.set(r, [&ss] (std::unique_ptr<http::request> req) {
        return ss.local().get_operation_mode().then([] (auto mode) {
            return make_ready_future<json::json_return_type>(mode >= service::storage_service::mode::JOINING && mode != service::storage_service::mode::MAINTENANCE);
        });
    });

    ss::set_stream_throughput_mb_per_sec.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_stream_throughput_mb_per_sec.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_compaction_throughput_mb_per_sec.set(r, [&ctx](std::unique_ptr<http::request> req) {
        int value = ctx.db.local().get_compaction_manager().throughput_mbs();
        return make_ready_future<json::json_return_type>(value);
    });

    ss::set_compaction_throughput_mb_per_sec.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_incremental_backups_enabled.set(r, [&ctx](std::unique_ptr<http::request> req) {
        // If this is issued in parallel with an ongoing change, we may see values not agreeing.
        // Reissuing is asking for trouble, so we will just return true upon seeing any true value.
        return ctx.db.map_reduce(adder<bool>(), [] (replica::database& db) {
            for (auto& pair: db.get_keyspaces()) {
                auto& ks = pair.second;
                if (ks.incremental_backups_enabled()) {
                    return true;
                }
            }
            return false;
        }).then([] (bool val) {
            return make_ready_future<json::json_return_type>(val);
        });
    });

    ss::set_incremental_backups_enabled.set(r, [&ctx](std::unique_ptr<http::request> req) {
        auto val_str = req->get_query_param("value");
        bool value = (val_str == "True") || (val_str == "true") || (val_str == "1");
        return ctx.db.invoke_on_all([value] (replica::database& db) {
            db.set_enable_incremental_backups(value);

            // Change both KS and CF, so they are in sync
            for (auto& pair: db.get_keyspaces()) {
                auto& ks = pair.second;
                ks.set_incremental_backups(value);
            }

            db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> table) {
                table->set_incremental_backups(value);
            });
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::rebuild.set(r, [&ss](std::unique_ptr<http::request> req) {
        utils::optional_param source_dc;
        if (auto source_dc_str = req->get_query_param("source_dc"); !source_dc_str.empty()) {
            source_dc.emplace(std::move(source_dc_str)).set_user_provided();
        }
        if (auto force_str = req->get_query_param("force"); !force_str.empty() && service::loosen_constraints(validate_bool(force_str))) {
            if (!source_dc) {
                throw bad_param_exception("The `source_dc` option must be provided for using the `force` option");
            }
            source_dc.set_force();
        }
        apilog.info("rebuild: source_dc={}", source_dc);
        return ss.local().rebuild(std::move(source_dc)).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::bulk_load.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto path = req->get_path_param("path");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::bulk_load_async.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto path = req->get_path_param("path");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::reschedule_failed_deletions.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::sample_key_range.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::reset_local_schema.set(r, [&ss](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        // FIXME: We should truncate schema tables if more than one node in the cluster.
        apilog.info("reset_local_schema");
        co_await ss.local().reload_schema();
        co_return json_void();
    });

    ss::set_trace_probability.set(r, [](std::unique_ptr<http::request> req) {
        auto probability = req->get_query_param("probability");
        apilog.info("set_trace_probability: probability={}", probability);
        return futurize_invoke([probability] {
            double real_prob = std::stod(probability.c_str());
            return tracing::tracing::tracing_instance().invoke_on_all([real_prob] (auto& local_tracing) {
                local_tracing.set_trace_probability(real_prob);
            }).then([] {
                return make_ready_future<json::json_return_type>(json_void());
            });
        }).then_wrapped([probability] (auto&& f) {
            try {
                f.get();
                return make_ready_future<json::json_return_type>(json_void());
            } catch (std::out_of_range& e) {
                throw httpd::bad_param_exception(e.what());
            } catch (std::invalid_argument&){
                throw httpd::bad_param_exception(format("Bad format in a probability value: \"{}\"", probability.c_str()));
            }
        });
    });

    ss::get_trace_probability.set(r, [](std::unique_ptr<http::request> req) {
        return make_ready_future<json::json_return_type>(tracing::tracing::get_local_tracing_instance().get_trace_probability());
    });

    ss::get_slow_query_info.set(r, [](const_req req) {
        ss::slow_query_info res;
        res.enable = tracing::tracing::get_local_tracing_instance().slow_query_tracing_enabled();
        res.ttl = tracing::tracing::get_local_tracing_instance().slow_query_record_ttl().count() ;
        res.threshold = tracing::tracing::get_local_tracing_instance().slow_query_threshold().count();
        res.fast = tracing::tracing::get_local_tracing_instance().ignore_trace_events_enabled();
        return res;
    });

    ss::set_slow_query.set(r, [](std::unique_ptr<http::request> req) {
        auto enable = req->get_query_param("enable");
        auto ttl = req->get_query_param("ttl");
        auto threshold = req->get_query_param("threshold");
        auto fast = req->get_query_param("fast");
        apilog.info("set_slow_query: enable={} ttl={} threshold={} fast={}", enable, ttl, threshold, fast);
        try {
            return tracing::tracing::tracing_instance().invoke_on_all([enable, ttl, threshold, fast] (auto& local_tracing) {
                if (threshold != "") {
                    local_tracing.set_slow_query_threshold(std::chrono::microseconds(std::stol(threshold.c_str())));
                }
                if (ttl != "") {
                    local_tracing.set_slow_query_record_ttl(std::chrono::seconds(std::stol(ttl.c_str())));
                }
                if (enable != "") {
                    local_tracing.set_slow_query_enabled(strcasecmp(enable.c_str(), "true") == 0);
                }
                if (fast != "") {
                    local_tracing.set_ignore_trace_events(strcasecmp(fast.c_str(), "true") == 0);
                }
            }).then([] {
                return make_ready_future<json::json_return_type>(json_void());
            });
        } catch (...) {
            throw httpd::bad_param_exception(format("Bad format value: "));
        }
    });

    ss::deliver_hints.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
      });

    ss::get_cluster_name.set(r, [&ss](const_req req) {
        return ss.local().gossiper().get_cluster_name();
    });

    ss::get_partitioner_name.set(r, [&ss](const_req req) {
        return ss.local().gossiper().get_partitioner_name();
    });

    ss::get_tombstone_warn_threshold.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_tombstone_warn_threshold.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto debug_threshold = req->get_query_param("debug_threshold");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_tombstone_failure_threshold.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_tombstone_failure_threshold.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto debug_threshold = req->get_query_param("debug_threshold");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_batch_size_failure_threshold.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_batch_size_failure_threshold.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto threshold = req->get_query_param("threshold");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::set_hinted_handoff_throttle_in_kb.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        auto debug_threshold = req->get_query_param("throttle");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_metrics_load.set(r, [&ctx](std::unique_ptr<http::request> req) {
        return get_cf_stats(ctx, &replica::column_family_stats::live_disk_space_used);
    });

    ss::get_exceptions.set(r, [&ss](const_req req) {
        return ss.local().get_exception_count();
    });

    ss::get_total_hints_in_progress.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_total_hints.set(r, [](std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_ownership.set(r, [&ctx, &ss] (std::unique_ptr<http::request> req) {
        if (any_of_keyspaces_use_tablets(ctx)) {
            throw httpd::bad_param_exception("storage_service/ownership cannot be used when a keyspace uses tablets");
        }

        return ss.local().get_ownership().then([] (auto&& ownership) {
            std::vector<storage_service_json::mapper> res;
            return make_ready_future<json::json_return_type>(map_to_key_value(ownership, res));
        });
    });

    ss::get_effective_ownership.set(r, [&ctx, &ss] (std::unique_ptr<http::request> req) {
        auto keyspace_name = req->get_path_param("keyspace") == "null" ? "" : validate_keyspace(ctx, req);
        auto table_name = req->get_query_param("cf");

        if (!keyspace_name.empty()) {
            if (table_name.empty()) {
                ensure_tablets_disabled(ctx, keyspace_name, "storage_service/ownership");
            } else {
                validate_table(ctx, keyspace_name, table_name);
            }
        }

        return ss.local().effective_ownership(keyspace_name, table_name).then([] (auto&& ownership) {
            std::vector<storage_service_json::mapper> res;
            return make_ready_future<json::json_return_type>(map_to_key_value(ownership, res));
        });
    });

    ss::sstable_info.set(r, [&ctx] (std::unique_ptr<http::request> req) {
        auto ks = api::req_param<sstring>(*req, "keyspace", {}).value;
        auto cf = api::req_param<sstring>(*req, "cf", {}).value;

        // The size of this vector is bound by ks::cf. I.e. it is as most Nks + Ncf long
        // which is not small, but not huge either. 
        using table_sstables_list = std::vector<ss::table_sstables>;

        return do_with(table_sstables_list{}, [ks, cf, &ctx](table_sstables_list& dst) {
            return ctx.db.map_reduce([&dst](table_sstables_list&& res) {
                for (auto&& t : res) {
                    auto i = std::find_if(dst.begin(), dst.end(), [&t](const ss::table_sstables& t2) {
                        return t.keyspace() == t2.keyspace() && t.table() == t2.table();
                    });
                    if (i == dst.end()) {
                        dst.emplace_back(std::move(t));
                        continue;
                    }
                    auto& ssd = i->sstables; 
                    for (auto&& sd : t.sstables._elements) {
                        auto j = std::find_if(ssd._elements.begin(), ssd._elements.end(), [&sd](const ss::sstable& s) {
                            return s.generation() == sd.generation();
                        });
                        if (j == ssd._elements.end()) {
                            i->sstables.push(std::move(sd));
                        }
                    }
                }
            }, [ks, cf](const replica::database& db) {
                // see above
                table_sstables_list res;

                auto& ext = db.get_config().extensions();

                db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> t) {
                    auto& schema = t->schema();
                    if ((ks.empty() || ks == schema->ks_name()) && (cf.empty() || cf == schema->cf_name())) {
                        // at most Nsstables long
                        ss::table_sstables tst;
                        tst.keyspace = schema->ks_name();
                        tst.table = schema->cf_name();

                        for (auto sstables = t->get_sstables_including_compacted_undeleted(); auto sstable : *sstables) {
                            auto ts = db_clock::to_time_t(sstable->data_file_write_time());
                            ::tm t;
                            ::gmtime_r(&ts, &t);

                            ss::sstable info;

                            info.timestamp = t;
                            info.generation = fmt::to_string(sstable->generation());
                            info.level = sstable->get_sstable_level();
                            info.size = sstable->bytes_on_disk();
                            info.data_size = sstable->ondisk_data_size();
                            info.index_size = sstable->index_size();
                            info.filter_size = sstable->filter_size();
                            info.version = sstable->get_version();

                            if (sstable->has_component(sstables::component_type::CompressionInfo)) {
                                auto& c = sstable->get_compression();
                                auto cp = sstables::get_sstable_compressor(c);

                                ss::named_maps nm;
                                nm.group = "compression_parameters";
                                for (auto& p : cp->options()) {
                                    ss::mapper e;
                                    e.key = p.first;
                                    e.value = p.second;
                                    nm.attributes.push(std::move(e));
                                }
                                if (!cp->options().contains(compression_parameters::SSTABLE_COMPRESSION)) {
                                    ss::mapper e;
                                    e.key = compression_parameters::SSTABLE_COMPRESSION;
                                    e.value = cp->name();
                                    nm.attributes.push(std::move(e));
                                }
                                info.extended_properties.push(std::move(nm));
                            }

                            sstables::file_io_extension::attr_value_map map;

                            for (auto* ep : ext.sstable_file_io_extensions()) {
                                map.merge(ep->get_attributes(*sstable));
                            }

                            for (auto& p : map) {
                                struct {
                                    const sstring& key; 
                                    ss::sstable& info;
                                    void operator()(const std::map<sstring, sstring>& map) const {
                                        ss::named_maps nm;
                                        nm.group = key;
                                        for (auto& p : map) {
                                            ss::mapper e;
                                            e.key = p.first;
                                            e.value = p.second;
                                            nm.attributes.push(std::move(e));
                                        }
                                        info.extended_properties.push(std::move(nm));
                                    }
                                    void operator()(const sstring& value) const {
                                        ss::mapper e;
                                        e.key = key;
                                        e.value = value;
                                        info.properties.push(std::move(e));                                        
                                    }
                                } v{p.first, info};

                                std::visit(v, p.second);
                            }

                            tst.sstables.push(std::move(info));
                        }
                        res.emplace_back(std::move(tst));
                    }
                });
                std::sort(res.begin(), res.end(), [](const ss::table_sstables& t1, const ss::table_sstables& t2) {
                    return t1.keyspace() < t2.keyspace() || (t1.keyspace() == t2.keyspace() && t1.table() < t2.table());
                });
                return res;
            }).then([&dst] {
                return make_ready_future<json::json_return_type>(stream_object(dst));
            });
        });
    });

    ss::reload_raft_topology_state.set(r,
            [&ss, &group0_client] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        co_await ss.invoke_on(0, [&group0_client] (service::storage_service& ss) -> future<> {
            apilog.info("Waiting for group 0 read/apply mutex before reloading Raft topology state...");
            auto holder = co_await group0_client.hold_read_apply_mutex();
            apilog.info("Reloading Raft topology state");
            // Using topology_transition() instead of topology_state_load(), because the former notifies listeners
            co_await ss.topology_transition();
            apilog.info("Reloaded Raft topology state");
        });
        co_return json_void();
    });

    ss::upgrade_to_raft_topology.set(r,
            [&ss] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        apilog.info("Requested to schedule upgrade to raft topology");
        try {
            co_await ss.invoke_on(0, [] (auto& ss) {
                return ss.start_upgrade_to_raft_topology();
            });
        } catch (...) {
            auto ex = std::current_exception();
            apilog.error("Failed to schedule upgrade to raft topology: {}", ex);
            std::rethrow_exception(std::move(ex));
        }
        co_return json_void();
    });

    ss::raft_topology_upgrade_status.set(r,
            [&ss] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        const auto ustate = co_await ss.invoke_on(0, [] (auto& ss) {
            return ss.get_topology_upgrade_state();
        });
        co_return sstring(format("{}", ustate));
    });

    ss::move_tablet.set(r, [&ctx, &ss] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        auto src_host_id = validate_host_id(req->get_query_param("src_host"));
        shard_id src_shard_id = validate_int(req->get_query_param("src_shard"));
        auto dst_host_id = validate_host_id(req->get_query_param("dst_host"));
        shard_id dst_shard_id = validate_int(req->get_query_param("dst_shard"));
        auto token = dht::token::from_int64(validate_int(req->get_query_param("token")));
        auto ks = req->get_query_param("ks");
        auto table = req->get_query_param("table");
        validate_table(ctx, ks, table);
        auto table_id = ctx.db.local().find_column_family(ks, table).schema()->id();
        auto force_str = req->get_query_param("force");
        auto force = service::loosen_constraints(force_str == "" ? false : validate_bool(force_str));

        co_await ss.local().move_tablet(table_id, token,
            locator::tablet_replica{src_host_id, src_shard_id},
            locator::tablet_replica{dst_host_id, dst_shard_id},
            force);

        co_return json_void();
    });

    ss::add_tablet_replica.set(r, [&ctx, &ss] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        auto dst_host_id = validate_host_id(req->get_query_param("dst_host"));
        shard_id dst_shard_id = validate_int(req->get_query_param("dst_shard"));
        auto token = dht::token::from_int64(validate_int(req->get_query_param("token")));
        auto ks = req->get_query_param("ks");
        auto table = req->get_query_param("table");
        auto table_id = ctx.db.local().find_column_family(ks, table).schema()->id();
        auto force_str = req->get_query_param("force");
        auto force = service::loosen_constraints(force_str == "" ? false : validate_bool(force_str));

        co_await ss.local().add_tablet_replica(table_id, token,
            locator::tablet_replica{dst_host_id, dst_shard_id},
            force);

        co_return json_void();
    });

    ss::del_tablet_replica.set(r, [&ctx, &ss] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        auto dst_host_id = validate_host_id(req->get_query_param("host"));
        shard_id dst_shard_id = validate_int(req->get_query_param("shard"));
        auto token = dht::token::from_int64(validate_int(req->get_query_param("token")));
        auto ks = req->get_query_param("ks");
        auto table = req->get_query_param("table");
        auto table_id = ctx.db.local().find_column_family(ks, table).schema()->id();
        auto force_str = req->get_query_param("force");
        auto force = service::loosen_constraints(force_str == "" ? false : validate_bool(force_str));

        co_await ss.local().del_tablet_replica(table_id, token,
            locator::tablet_replica{dst_host_id, dst_shard_id},
            force);

        co_return json_void();
    });

    ss::tablet_balancing_enable.set(r, [&ss] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        auto enabled = validate_bool(req->get_query_param("enabled"));
        co_await ss.local().set_tablet_balancing_enabled(enabled);
        co_return json_void();
    });

    ss::quiesce_topology.set(r, [&ss] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        co_await ss.local().await_topology_quiesced();
        co_return json_void();
    });

    sp::get_schema_versions.set(r, [&ss](std::unique_ptr<http::request> req)  {
        return ss.local().describe_schema_versions().then([] (auto result) {
            std::vector<sp::mapper_list> res;
            for (auto e : result) {
                sp::mapper_list entry;
                entry.key = std::move(e.first);
                entry.value = std::move(e.second);
                res.emplace_back(std::move(entry));
            }
            return make_ready_future<json::json_return_type>(std::move(res));
        });
    });
}

void unset_storage_service(http_context& ctx, routes& r) {
    ss::get_commitlog.unset(r);
    ss::get_token_endpoint.unset(r);
    ss::toppartitions_generic.unset(r);
    ss::get_release_version.unset(r);
    ss::get_scylla_release_version.unset(r);
    ss::get_schema_version.unset(r);
    ss::get_range_to_endpoint_map.unset(r);
    ss::get_pending_range_to_endpoint_map.unset(r);
    ss::describe_ring.unset(r);
    ss::get_load.unset(r);
    ss::get_current_generation_number.unset(r);
    ss::get_natural_endpoints.unset(r);
    ss::cdc_streams_check_and_repair.unset(r);
    ss::force_compaction.unset(r);
    ss::force_keyspace_compaction.unset(r);
    ss::force_keyspace_cleanup.unset(r);
    ss::cleanup_all.unset(r);
    ss::perform_keyspace_offstrategy_compaction.unset(r);
    ss::upgrade_sstables.unset(r);
    ss::force_flush.unset(r);
    ss::force_keyspace_flush.unset(r);
    ss::decommission.unset(r);
    ss::move.unset(r);
    ss::remove_node.unset(r);
    ss::get_removal_status.unset(r);
    ss::force_remove_completion.unset(r);
    ss::set_logging_level.unset(r);
    ss::get_logging_levels.unset(r);
    ss::get_operation_mode.unset(r);
    ss::is_starting.unset(r);
    ss::get_drain_progress.unset(r);
    ss::drain.unset(r);
    ss::truncate.unset(r);
    ss::get_keyspaces.unset(r);
    ss::stop_gossiping.unset(r);
    ss::start_gossiping.unset(r);
    ss::is_gossip_running.unset(r);
    ss::stop_daemon.unset(r);
    ss::is_initialized.unset(r);
    ss::join_ring.unset(r);
    ss::is_joined.unset(r);
    ss::set_stream_throughput_mb_per_sec.unset(r);
    ss::get_stream_throughput_mb_per_sec.unset(r);
    ss::get_compaction_throughput_mb_per_sec.unset(r);
    ss::set_compaction_throughput_mb_per_sec.unset(r);
    ss::is_incremental_backups_enabled.unset(r);
    ss::set_incremental_backups_enabled.unset(r);
    ss::rebuild.unset(r);
    ss::bulk_load.unset(r);
    ss::bulk_load_async.unset(r);
    ss::reschedule_failed_deletions.unset(r);
    ss::sample_key_range.unset(r);
    ss::reset_local_schema.unset(r);
    ss::set_trace_probability.unset(r);
    ss::get_trace_probability.unset(r);
    ss::get_slow_query_info.unset(r);
    ss::set_slow_query.unset(r);
    ss::deliver_hints.unset(r);
    ss::get_cluster_name.unset(r);
    ss::get_partitioner_name.unset(r);
    ss::get_tombstone_warn_threshold.unset(r);
    ss::set_tombstone_warn_threshold.unset(r);
    ss::get_tombstone_failure_threshold.unset(r);
    ss::set_tombstone_failure_threshold.unset(r);
    ss::get_batch_size_failure_threshold.unset(r);
    ss::set_batch_size_failure_threshold.unset(r);
    ss::set_hinted_handoff_throttle_in_kb.unset(r);
    ss::get_metrics_load.unset(r);
    ss::get_exceptions.unset(r);
    ss::get_total_hints_in_progress.unset(r);
    ss::get_total_hints.unset(r);
    ss::get_ownership.unset(r);
    ss::get_effective_ownership.unset(r);
    ss::sstable_info.unset(r);
    ss::reload_raft_topology_state.unset(r);
    ss::upgrade_to_raft_topology.unset(r);
    ss::raft_topology_upgrade_status.unset(r);
    ss::move_tablet.unset(r);
    ss::add_tablet_replica.unset(r);
    ss::del_tablet_replica.unset(r);
    ss::tablet_balancing_enable.unset(r);
    ss::quiesce_topology.unset(r);
    sp::get_schema_versions.unset(r);
}

void set_load_meter(http_context& ctx, routes& r, service::load_meter& lm) {
    ss::get_load_map.set(r, [&lm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto load_map = co_await lm.get_load_map();
        std::vector<ss::map_string_double> res;
        for (auto i : load_map) {
            ss::map_string_double val;
            val.key = i.first;
            val.value = i.second;
            res.push_back(val);
        }
        co_return res;
    });
}

void unset_load_meter(http_context& ctx, routes& r) {
    ss::get_load_map.unset(r);
}

void set_snapshot(http_context& ctx, routes& r, sharded<db::snapshot_ctl>& snap_ctl) {
    ss::get_snapshot_details.set(r, [&snap_ctl](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto result = co_await snap_ctl.local().get_snapshot_details();
        co_return std::function([res = std::move(result)] (output_stream<char>&& o) -> future<> {
            std::exception_ptr ex;
            output_stream<char> out = std::move(o);
            try {
                auto result = std::move(res);
                bool first = true;

                co_await out.write("[");
                for (auto& [name, details] : result) {
                    if (!first) {
                        co_await out.write(", ");
                    }
                    std::vector<ss::snapshot> snapshot;
                    for (auto& cf : details) {
                        ss::snapshot snp;
                        snp.ks = cf.ks;
                        snp.cf = cf.cf;
                        snp.live = cf.details.live;
                        snp.total = cf.details.total;
                        snapshot.push_back(std::move(snp));
                    }
                    ss::snapshots all_snapshots;
                    all_snapshots.key = name;
                    all_snapshots.value = std::move(snapshot);
                    co_await all_snapshots.write(out);
                    first = false;
                }
                co_await out.write("]");
                co_await out.flush();
            } catch (...) {
              ex = std::current_exception();
            }
            co_await out.close();
            if (ex) {
                co_await coroutine::return_exception_ptr(std::move(ex));
            }
        });
    });

    ss::take_snapshot.set(r, [&ctx, &snap_ctl](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        apilog.info("take_snapshot: {}", req->query_parameters);
        auto tag = req->get_query_param("tag");
        auto column_families = split(req->get_query_param("cf"), ",");
        auto sfopt = req->get_query_param("sf");
        auto sf = db::snapshot_ctl::skip_flush(strcasecmp(sfopt.c_str(), "true") == 0);

        std::vector<sstring> keynames = split(req->get_query_param("kn"), ",");
        try {
            if (column_families.empty()) {
                co_await snap_ctl.local().take_snapshot(tag, keynames, sf);
            } else {
                if (keynames.empty()) {
                    throw httpd::bad_param_exception("The keyspace of column families must be specified");
                }
                if (keynames.size() > 1) {
                    throw httpd::bad_param_exception("Only one keyspace allowed when specifying a column family");
                }
                for (const auto& table_name : column_families) {
                    auto& t = ctx.db.local().find_column_family(keynames[0], table_name);
                    if (t.schema()->is_view()) {
                        throw std::invalid_argument("Do not take a snapshot of a materialized view or a secondary index by itself. Run snapshot on the base table instead.");
                    }
                }
                co_await snap_ctl.local().take_column_family_snapshot(keynames[0], column_families, tag, db::snapshot_ctl::snap_views::yes, sf);
            }
            co_return json_void();
        } catch (...) {
            apilog.error("take_snapshot failed: {}", std::current_exception());
            throw;
        }
    });

    ss::del_snapshot.set(r, [&snap_ctl](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        apilog.info("del_snapshot: {}", req->query_parameters);
        auto tag = req->get_query_param("tag");
        auto column_family = req->get_query_param("cf");

        std::vector<sstring> keynames = split(req->get_query_param("kn"), ",");
        try {
            co_await snap_ctl.local().clear_snapshot(tag, keynames, column_family);
            co_return json_void();
        } catch (...) {
            apilog.error("del_snapshot failed: {}", std::current_exception());
            throw;
        }
    });

    ss::true_snapshots_size.set(r, [&snap_ctl](std::unique_ptr<http::request> req) {
        return snap_ctl.local().true_snapshots_size().then([] (int64_t size) {
            return make_ready_future<json::json_return_type>(size);
        });
    });

    ss::scrub.set(r, [&ctx, &snap_ctl] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto info = co_await parse_scrub_options(ctx, snap_ctl, std::move(req));

        sstables::compaction_stats stats;
        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<scrub_sstables_compaction_task_impl>({}, info.keyspace, db, info.column_families, info.opts, &stats);
        try {
            co_await task->done();
            if (stats.validation_errors) {
                co_return json::json_return_type(static_cast<int>(scrub_status::validation_errors));
            }
        } catch (const sstables::compaction_aborted_exception&) {
            co_return json::json_return_type(static_cast<int>(scrub_status::aborted));
        } catch (...) {
            apilog.error("scrub keyspace={} tables={} failed: {}", info.keyspace, info.column_families, std::current_exception());
            throw;
        }

        co_return json::json_return_type(static_cast<int>(scrub_status::successful));
    });

    ss::start_backup.set(r, [&snap_ctl] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto endpoint = req->get_query_param("endpoint");
        auto keyspace = req->get_query_param("keyspace");
        auto bucket = req->get_query_param("bucket");
        auto snapshot_name = req->get_query_param("snapshot");
        if (snapshot_name.empty()) {
            // TODO: If missing, snapshot should be taken by scylla, then removed
            throw httpd::bad_param_exception("The snapshot name must be specified");
        }

        auto& ctl = snap_ctl.local();
        auto task_id = co_await ctl.start_backup(std::move(endpoint), std::move(bucket), std::move(keyspace), std::move(snapshot_name));
        co_return json::json_return_type(fmt::to_string(task_id));
    });

    cf::get_true_snapshots_size.set(r, [&snap_ctl] (std::unique_ptr<http::request> req) {
        auto [ks, cf] = parse_fully_qualified_cf_name(req->get_path_param("name"));
        return snap_ctl.local().true_snapshots_size(std::move(ks), std::move(cf)).then([] (int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    cf::get_all_true_snapshots_size.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

}

void unset_snapshot(http_context& ctx, routes& r) {
    ss::get_snapshot_details.unset(r);
    ss::take_snapshot.unset(r);
    ss::del_snapshot.unset(r);
    ss::true_snapshots_size.unset(r);
    ss::scrub.unset(r);
    ss::start_backup.unset(r);
    cf::get_true_snapshots_size.unset(r);
    cf::get_all_true_snapshots_size.unset(r);
}

}
