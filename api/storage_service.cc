/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "storage_service.hh"
#include "api/api-doc/storage_service.json.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "utils/hash.hh"
#include <optional>
#include <sstream>
#include <time.h>
#include <algorithm>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/algorithm/string/trim_all.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/functional/hash.hpp>
#include "service/storage_service.hh"
#include "service/load_meter.hh"
#include "db/commitlog/commitlog.hh"
#include "gms/gossiper.hh"
#include "db/system_keyspace.hh"
#include <seastar/http/exception.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "repair/row_level.hh"
#include "locator/snitch_base.hh"
#include "column_family.hh"
#include "log.hh"
#include "release.hh"
#include "compaction/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "replica/database.hh"
#include "db/extensions.hh"
#include "db/snapshot-ctl.hh"
#include "transport/controller.hh"
#include "thrift/controller.hh"
#include "locator/token_metadata.hh"
#include "cdc/generation_service.hh"
#include "service/storage_proxy.hh"
#include "locator/abstract_replication_strategy.hh"
#include "sstables_loader.hh"
#include "db/view/view_builder.hh"

extern logging::logger apilog;

namespace std {

std::ostream& operator<<(std::ostream& os, const api::table_info& ti) {
    return os << "table{name=" << ti.name << ", id=" << ti.id << "}";
}

} // namespace std

namespace api {

const locator::token_metadata& http_context::get_token_metadata() {
        return *shared_token_metadata.local().get();
}

namespace ss = httpd::storage_service_json;
using namespace json;

sstring validate_keyspace(http_context& ctx, sstring ks_name) {
    if (ctx.db.local().has_keyspace(ks_name)) {
        return ks_name;
    }
    throw bad_param_exception(replica::no_such_keyspace(ks_name).what());
}

sstring validate_keyspace(http_context& ctx, const parameters& param) {
    return validate_keyspace(ctx, param["keyspace"]);
}

locator::host_id validate_host_id(const sstring& param) {
    auto hoep = locator::host_id_or_endpoint(param, locator::host_id_or_endpoint::param_type::host_id);
    return hoep.id;
}

// splits a request parameter assumed to hold a comma-separated list of table names
// verify that the tables are found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective no_such_column_family error.
std::vector<sstring> parse_tables(const sstring& ks_name, http_context& ctx, sstring value) {
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

std::vector<sstring> parse_tables(const sstring& ks_name, http_context& ctx, const std::unordered_map<sstring, sstring>& query_params, sstring param_name) {
    auto it = query_params.find(param_name);
    if (it == query_params.end()) {
        return {};
    }
    return parse_tables(ks_name, ctx, it->second);
}

std::vector<table_info> parse_table_infos(const sstring& ks_name, http_context& ctx, sstring value) {
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

std::vector<table_info> parse_table_infos(const sstring& ks_name, http_context& ctx, const std::unordered_map<sstring, sstring>& query_params, sstring param_name) {
    auto it = query_params.find(param_name);
    return parse_table_infos(ks_name, ctx, it != query_params.end() ? it->second : "");
}

// Run on all tables, skipping dropped tables
future<> run_on_existing_tables(sstring op, replica::database& db, std::string_view keyspace, const std::vector<table_info> local_tables, std::function<future<> (replica::table&)> func) {
    std::exception_ptr ex;
    for (const auto& ti : local_tables) {
        apilog.debug("Starting {} on {}.{}", op, keyspace, ti);
        try {
            co_await func(db.find_column_family(ti.id));
        } catch (const replica::no_such_column_family& e) {
            apilog.warn("Skipping {} of {}.{}: {}", op, keyspace, ti, e.what());
        } catch (...) {
            ex = std::current_exception();
            apilog.error("Failed {} of {}.{}: {}", op, keyspace, ti, ex);
        }
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    }
}

static ss::token_range token_range_endpoints_to_json(const dht::token_range_endpoints& d) {
    ss::token_range r;
    r.start_token = d._start_token;
    r.end_token = d._end_token;
    r.endpoints = d._endpoints;
    r.rpc_endpoints = d._rpc_endpoints;
    for (auto det : d._endpoint_details) {
        ss::endpoint_detail ed;
        ed.host = boost::lexical_cast<std::string>(det._host);
        ed.datacenter = det._datacenter;
        if (det._rack != "") {
            ed.rack = det._rack;
        }
        r.endpoint_details.push(ed);
    }
    return r;
}

using ks_cf_func = std::function<future<json::json_return_type>(http_context&, std::unique_ptr<request>, sstring, std::vector<table_info>)>;

static auto wrap_ks_cf(http_context &ctx, ks_cf_func f) {
    return [&ctx, f = std::move(f)](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto table_infos = parse_table_infos(keyspace, ctx, req->query_parameters, "cf");
        return f(ctx, std::move(req), std::move(keyspace), std::move(table_infos));
    };
}

seastar::future<json::json_return_type> run_toppartitions_query(db::toppartitions_query& q, http_context &ctx, bool legacy_request) {
    namespace cf = httpd::column_family_json;
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

future<json::json_return_type> set_tables_autocompaction(http_context& ctx, const sstring &keyspace, std::vector<sstring> tables, bool enabled) {
    if (tables.empty()) {
        tables = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
    }

    apilog.info("set_tables_autocompaction: enabled={} keyspace={} tables={}", enabled, keyspace, tables);
    return do_with(keyspace, std::move(tables), [&ctx, enabled] (const sstring &keyspace, const std::vector<sstring>& tables) {
        return ctx.db.invoke_on(0, [&ctx, &keyspace, &tables, enabled] (replica::database& db) {
            auto g = replica::database::autocompaction_toggle_guard(db);
            return ctx.db.invoke_on_all([&keyspace, &tables, enabled] (replica::database& db) {
                return parallel_for_each(tables, [&db, &keyspace, enabled] (const sstring& table) {
                    replica::column_family& cf = db.find_column_family(keyspace, table);
                    if (enabled) {
                        cf.enable_auto_compaction();
                    } else {
                        return cf.disable_auto_compaction();
                    }
                    return make_ready_future<>();
                });
            }).finally([g = std::move(g)] {});
        });
    }).then([] {
        return make_ready_future<json::json_return_type>(json_void());
    });
}

void set_transport_controller(http_context& ctx, routes& r, cql_transport::controller& ctl) {
    ss::start_native_transport.set(r, [&ctx, &ctl](std::unique_ptr<request> req) {
        return smp::submit_to(0, [&] {
            return with_scheduling_group(ctx.db.local().get_statement_scheduling_group(), [&ctl] {
                return ctl.start_server();
            });
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::stop_native_transport.set(r, [&ctx, &ctl](std::unique_ptr<request> req) {
        return smp::submit_to(0, [&] {
            return with_scheduling_group(ctx.db.local().get_statement_scheduling_group(), [&ctl] {
                return ctl.request_stop_server();
            });
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_native_transport_running.set(r, [&ctl] (std::unique_ptr<request> req) {
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

void set_rpc_controller(http_context& ctx, routes& r, thrift_controller& ctl) {
    ss::stop_rpc_server.set(r, [&ctx, &ctl] (std::unique_ptr<request> req) {
        return smp::submit_to(0, [&ctx, &ctl] {
            return with_scheduling_group(ctx.db.local().get_statement_scheduling_group(), [&ctl] () mutable {
                return ctl.request_stop_server();
            });
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::start_rpc_server.set(r, [&ctx, &ctl](std::unique_ptr<request> req) {
        return smp::submit_to(0, [&ctx, &ctl] {
            return with_scheduling_group(ctx.db.local().get_statement_scheduling_group(), [&ctl] () mutable {
                return ctl.start_server();
            });
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_rpc_server_running.set(r, [&ctl] (std::unique_ptr<request> req) {
        return smp::submit_to(0, [&] {
            return !ctl.listen_addresses().empty();
        }).then([] (bool running) {
            return make_ready_future<json::json_return_type>(running);
        });
    });
}

void unset_rpc_controller(http_context& ctx, routes& r) {
    ss::stop_rpc_server.unset(r);
    ss::start_rpc_server.unset(r);
    ss::is_rpc_server_running.unset(r);
}

void set_repair(http_context& ctx, routes& r, sharded<repair_service>& repair) {
    ss::repair_async.set(r, [&ctx, &repair](std::unique_ptr<request> req) {
        static std::vector<sstring> options = {"primaryRange", "parallelism", "incremental",
                "jobThreads", "ranges", "columnFamilies", "dataCenters", "hosts", "ignore_nodes", "trace",
                "startToken", "endToken" };
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
        return repair_start(repair, validate_keyspace(ctx, req->param),
                options_map).then([] (int i) {
                    return make_ready_future<json::json_return_type>(i);
                });
    });

    ss::get_active_repair_async.set(r, [&repair] (std::unique_ptr<request> req) {
        return repair.local().get_active_repairs().then([] (std::vector<int> res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    ss::repair_async_status.set(r, [&repair] (std::unique_ptr<request> req) {
        return repair.local().get_status(boost::lexical_cast<int>( req->get_query_param("id")))
                .then_wrapped([] (future<repair_status>&& fut) {
            ss::ns_repair_async_status::return_type_wrapper res;
            try {
                res = fut.get0();
            } catch(std::runtime_error& e) {
                throw httpd::bad_param_exception(e.what());
            }
            return make_ready_future<json::json_return_type>(json::json_return_type(res));
        });
    });

    ss::repair_await_completion.set(r, [&repair] (std::unique_ptr<request> req) {
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
                res = fut.get0();
            } catch (std::exception& e) {
                return make_exception_future<json::json_return_type>(httpd::bad_param_exception(e.what()));
            }
            return make_ready_future<json::json_return_type>(json::json_return_type(res));
        });
    });

    ss::force_terminate_all_repair_sessions.set(r, [&repair] (std::unique_ptr<request> req) {
        return repair.local().abort_all().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::force_terminate_all_repair_sessions_new.set(r, [&repair] (std::unique_ptr<request> req) {
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
    ss::load_new_ss_tables.set(r, [&ctx, &sst_loader](std::unique_ptr<request> req) {
        auto ks = validate_keyspace(ctx, req->param);
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
}

void unset_sstables_loader(http_context& ctx, routes& r) {
    ss::load_new_ss_tables.unset(r);
}

void set_view_builder(http_context& ctx, routes& r, sharded<db::view::view_builder>& vb) {
    ss::view_build_statuses.set(r, [&ctx, &vb] (std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto view = req->param["view"];
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

void set_storage_service(http_context& ctx, routes& r, sharded<service::storage_service>& ss, gms::gossiper& g, sharded<cdc::generation_service>& cdc_gs, sharded<db::system_keyspace>& sys_ks) {
    ss::local_hostid.set(r, [&ctx](std::unique_ptr<request> req) {
        auto id = ctx.db.local().get_config().host_id;
        return make_ready_future<json::json_return_type>(id.to_sstring());
    });

    ss::get_tokens.set(r, [&ctx] (std::unique_ptr<request> req) {
        return make_ready_future<json::json_return_type>(stream_range_as_array(ctx.get_token_metadata().sorted_tokens(), [](const dht::token& i) {
           return boost::lexical_cast<std::string>(i);
        }));
    });

    ss::get_node_tokens.set(r, [&ctx] (std::unique_ptr<request> req) {
        gms::inet_address addr(req->param["endpoint"]);
        return make_ready_future<json::json_return_type>(stream_range_as_array(ctx.get_token_metadata().get_tokens(addr), [](const dht::token& i) {
           return boost::lexical_cast<std::string>(i);
       }));
    });

    ss::get_commitlog.set(r, [&ctx](const_req req) {
        return ctx.db.local().commitlog()->active_config().commit_log_location;
    });

    ss::get_token_endpoint.set(r, [&ss] (std::unique_ptr<request> req) {
        return make_ready_future<json::json_return_type>(stream_range_as_array(ss.local().get_token_to_endpoint_map(), [](const auto& i) {
            storage_service_json::mapper val;
            val.key = boost::lexical_cast<std::string>(i.first);
            val.value = boost::lexical_cast<std::string>(i.second);
            return val;
        }));
    });

    ss::toppartitions_generic.set(r, [&ctx] (std::unique_ptr<request> req) {
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

    ss::get_leaving_nodes.set(r, [&ctx](const_req req) {
        return container_to_vec(ctx.get_token_metadata().get_leaving_endpoints());
    });

    ss::get_moving_nodes.set(r, [](const_req req) {
        std::unordered_set<sstring> addr;
        return container_to_vec(addr);
    });

    ss::get_joining_nodes.set(r, [&ctx](const_req req) {
        auto points = ctx.get_token_metadata().get_bootstrap_tokens();
        std::unordered_set<sstring> addr;
        for (auto i: points) {
            addr.insert(boost::lexical_cast<std::string>(i.second));
        }
        return container_to_vec(addr);
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

    ss::get_all_data_file_locations.set(r, [&ctx](const_req req) {
        return container_to_vec(ctx.db.local().get_config().data_file_directories());
    });

    ss::get_saved_caches_location.set(r, [&ctx](const_req req) {
        return ctx.db.local().get_config().saved_caches_directory();
    });

    ss::get_range_to_endpoint_map.set(r, [&ctx, &ss](std::unique_ptr<request> req) -> future<json::json_return_type> {
        auto keyspace = validate_keyspace(ctx, req->param);
        std::vector<ss::maplist_mapper> res;
        co_return stream_range_as_array(co_await ss.local().get_range_to_address_map(keyspace),
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
                m.value.push(address.to_sstring());
            }
            return m;
        });
    });

    ss::get_pending_range_to_endpoint_map.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto keyspace = validate_keyspace(ctx, req->param);
        std::vector<ss::maplist_mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::describe_any_ring.set(r, [&ctx, &ss](std::unique_ptr<request> req) {
        // Find an arbitrary non-system keyspace.
        auto keyspaces = ctx.db.local().get_non_local_strategy_keyspaces();
        if (keyspaces.empty()) {
            throw std::runtime_error("No keyspace provided and no non system kespace exist");
        }
        auto ks = keyspaces[0];
        return describe_ring_as_json(ss, ks);
    });

    ss::describe_ring.set(r, [&ctx, &ss](std::unique_ptr<request> req) {
        return describe_ring_as_json(ss, validate_keyspace(ctx, req->param));
    });

    ss::get_host_id_map.set(r, [&ctx](const_req req) {
        std::vector<ss::mapper> res;
        return map_to_key_value(ctx.get_token_metadata().get_endpoint_to_host_id_map_for_reading(), res);
    });

    ss::get_load.set(r, [&ctx](std::unique_ptr<request> req) {
        return get_cf_stats(ctx, &replica::column_family_stats::live_disk_space_used);
    });

    ss::get_load_map.set(r, [&ctx] (std::unique_ptr<request> req) {
        return ctx.lmeter.get_load_map().then([] (auto&& load_map) {
            std::vector<ss::map_string_double> res;
            for (auto i : load_map) {
                ss::map_string_double val;
                val.key = i.first;
                val.value = i.second;
                res.push_back(val);
            }
            return make_ready_future<json::json_return_type>(res);
        });
    });

    ss::get_current_generation_number.set(r, [&g](std::unique_ptr<request> req) {
        gms::inet_address ep(utils::fb_utilities::get_broadcast_address());
        return g.get_current_generation_number(ep).then([](int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    ss::get_natural_endpoints.set(r, [&ctx, &ss](const_req req) {
        auto keyspace = validate_keyspace(ctx, req.param);
        return container_to_vec(ss.local().get_natural_endpoints(keyspace, req.get_query_param("cf"),
                req.get_query_param("key")));
    });

    ss::cdc_streams_check_and_repair.set(r, [&ctx, &cdc_gs] (std::unique_ptr<request> req) {
        if (!cdc_gs.local_is_initialized()) {
            throw std::runtime_error("get_cdc_generation_service: not initialized yet");
        }
        return cdc_gs.local().check_and_repair_cdc_streams().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::force_keyspace_compaction.set(r, [&ctx](std::unique_ptr<request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto keyspace = validate_keyspace(ctx, req->param);
        auto table_infos = parse_table_infos(keyspace, ctx, req->query_parameters, "cf");
        apilog.debug("force_keyspace_compaction: keyspace={} tables={}", keyspace, table_infos);
        try {
            co_await db.invoke_on_all([&] (replica::database& db) -> future<> {
                auto local_tables = table_infos;
                // major compact smaller tables first, to increase chances of success if low on space.
                std::ranges::sort(local_tables, std::less<>(), [&] (const table_info& ti) {
                    try {
                        return db.find_column_family(ti.id).get_stats().live_disk_space_used;
                    } catch (const replica::no_such_column_family& e) {
                        return int64_t(-1);
                    }
                });
                co_await run_on_existing_tables("force_keyspace_compaction", db, keyspace, local_tables, [] (replica::table& t) {
                    return t.compact_all_sstables();
                });
            });
        } catch (...) {
            apilog.error("force_keyspace_compaction: keyspace={} tables={} failed: {}", keyspace, table_infos, std::current_exception());
            throw;
        }

        co_return json_void();
    });

    ss::force_keyspace_cleanup.set(r, [&ctx, &ss](std::unique_ptr<request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto keyspace = validate_keyspace(ctx, req->param);
        auto table_infos = parse_table_infos(keyspace, ctx, req->query_parameters, "cf");
        apilog.info("force_keyspace_cleanup: keyspace={} tables={}", keyspace, table_infos);
        if (!co_await ss.local().is_cleanup_allowed(keyspace)) {
            auto msg = "Can not perform cleanup operation when topology changes";
            apilog.warn("force_keyspace_cleanup: keyspace={} tables={}: {}", keyspace, table_infos, msg);
            co_await coroutine::return_exception(std::runtime_error(msg));
        }
        try {
            co_await db.invoke_on_all([&] (replica::database& db) -> future<> {
                auto local_tables = table_infos;
                // cleanup smaller tables first, to increase chances of success if low on space.
                std::ranges::sort(local_tables, std::less<>(), [&] (const table_info& ti) {
                    try {
                        return db.find_column_family(ti.id).get_stats().live_disk_space_used;
                    } catch (const replica::no_such_column_family& e) {
                        return int64_t(-1);
                    }
                });
                auto& cm = db.get_compaction_manager();
                auto owned_ranges_ptr = compaction::make_owned_ranges_ptr(db.get_keyspace_local_ranges(keyspace));
                co_await run_on_existing_tables("force_keyspace_cleanup", db, keyspace, local_tables, [&] (replica::table& t) {
                    return t.perform_cleanup_compaction(owned_ranges_ptr);
                });
            });
        } catch (...) {
            apilog.error("force_keyspace_cleanup: keyspace={} tables={} failed: {}", keyspace, table_infos, std::current_exception());
            throw;
        }

        co_return json::json_return_type(0);
    });

    ss::perform_keyspace_offstrategy_compaction.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        apilog.info("perform_keyspace_offstrategy_compaction: keyspace={} tables={}", keyspace, table_infos);
        bool res = false;
        try {
            res = co_await ctx.db.map_reduce0([&] (replica::database& db) -> future<bool> {
                bool needed = false;
                co_await run_on_existing_tables("perform_keyspace_offstrategy_compaction", db, keyspace, table_infos, [&needed] (replica::table& t) -> future<> {
                    needed |= co_await t.perform_offstrategy_compaction();
                });
                co_return needed;
            }, false, std::plus<bool>());
        } catch (...) {
            apilog.error("perform_keyspace_offstrategy_compaction: keyspace={} tables={} failed: {}", keyspace, table_infos, std::current_exception());
            throw;
        }

        co_return json::json_return_type(res);
    }));

    ss::upgrade_sstables.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        auto& db = ctx.db;
        bool exclude_current_version = req_param<bool>(*req, "exclude_current_version", false);

        apilog.info("upgrade_sstables: keyspace={} tables={} exclude_current_version={}", keyspace, table_infos, exclude_current_version);
        try {
            co_await db.invoke_on_all([&] (replica::database& db) -> future<> {
                auto owned_ranges_ptr = compaction::make_owned_ranges_ptr(db.get_keyspace_local_ranges(keyspace));
                co_await run_on_existing_tables("upgrade_sstables", db, keyspace, table_infos, [&] (replica::table& t) {
                    return t.parallel_foreach_table_state([&] (compaction::table_state& ts) {
                        return t.get_compaction_manager().perform_sstable_upgrade(owned_ranges_ptr, ts, exclude_current_version);
                    });
                });
            });
        } catch (...) {
            apilog.error("upgrade_sstables: keyspace={} tables={} failed: {}", keyspace, table_infos, std::current_exception());
            throw;
        }

        co_return json::json_return_type(0);
    }));

    ss::force_keyspace_flush.set(r, [&ctx](std::unique_ptr<request> req) -> future<json::json_return_type> {
        auto keyspace = validate_keyspace(ctx, req->param);
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


    ss::decommission.set(r, [&ss](std::unique_ptr<request> req) {
        apilog.info("decommission");
        return ss.local().decommission().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::move.set(r, [&ss] (std::unique_ptr<request> req) {
        auto new_token = req->get_query_param("new_token");
        return ss.local().move(new_token).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::remove_node.set(r, [&ss](std::unique_ptr<request> req) {
        auto host_id = validate_host_id(req->get_query_param("host_id"));
        std::vector<sstring> ignore_nodes_strs= split(req->get_query_param("ignore_nodes"), ",");
        apilog.info("remove_node: host_id={} ignore_nodes={}", host_id, ignore_nodes_strs);
        auto ignore_nodes = std::list<locator::host_id_or_endpoint>();
        for (std::string n : ignore_nodes_strs) {
            try {
                std::replace(n.begin(), n.end(), '\"', ' ');
                std::replace(n.begin(), n.end(), '\'', ' ');
                boost::trim_all(n);
                if (!n.empty()) {
                    auto hoep = locator::host_id_or_endpoint(n);
                    if (!ignore_nodes.empty() && hoep.has_host_id() != ignore_nodes.front().has_host_id()) {
                        throw std::runtime_error("All nodes should be identified using the same method: either Host IDs or ip addresses.");
                    }
                    ignore_nodes.push_back(std::move(hoep));
                }
            } catch (...) {
                throw std::runtime_error(format("Failed to parse ignore_nodes parameter: ignore_nodes={}, node={}: {}", ignore_nodes_strs, n, std::current_exception()));
            }
        }
        return ss.local().removenode(host_id, std::move(ignore_nodes)).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::get_removal_status.set(r, [&ss](std::unique_ptr<request> req) {
        return ss.local().get_removal_status().then([] (auto status) {
            return make_ready_future<json::json_return_type>(status);
        });
    });

    ss::force_remove_completion.set(r, [&ss](std::unique_ptr<request> req) {
        return ss.local().force_remove_completion().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::set_logging_level.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto class_qualifier = req->get_query_param("class_qualifier");
        auto level = req->get_query_param("level");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_logging_levels.set(r, [](std::unique_ptr<request> req) {
        std::vector<ss::mapper> res;
        for (auto i : logging::logger_registry().get_all_logger_names()) {
            ss::mapper log;
            log.key = i;
            log.value = logging::level_name(logging::logger_registry().get_logger_level(i));
            res.push_back(log);
        }
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_operation_mode.set(r, [&ss](std::unique_ptr<request> req) {
        return ss.local().get_operation_mode().then([] (auto mode) {
            return make_ready_future<json::json_return_type>(format("{}", mode));
        });
    });

    ss::is_starting.set(r, [&ss](std::unique_ptr<request> req) {
        return ss.local().get_operation_mode().then([] (auto mode) {
            return make_ready_future<json::json_return_type>(mode <= service::storage_service::mode::STARTING);
        });
    });

    ss::get_drain_progress.set(r, [&ctx](std::unique_ptr<request> req) {
        return ctx.db.map_reduce(adder<replica::database::drain_progress>(), [] (auto& db) {
            return db.get_drain_progress();
        }).then([] (auto&& progress) {
            auto progress_str = format("Drained {}/{} ColumnFamilies", progress.remaining_cfs, progress.total_cfs);
            return make_ready_future<json::json_return_type>(std::move(progress_str));
        });
    });

    ss::drain.set(r, [&ss](std::unique_ptr<request> req) {
        apilog.info("drain");
        return ss.local().drain().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });
    ss::truncate.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_keyspaces.set(r, [&ctx](const_req req) {
        auto type = req.get_query_param("type");
        if (type == "user") {
            return ctx.db.local().get_user_keyspaces();
        } else if (type == "non_local_strategy") {
            return ctx.db.local().get_non_local_strategy_keyspaces();
        }
        return map_keys(ctx.db.local().get_keyspaces());
    });

    ss::stop_gossiping.set(r, [&ss](std::unique_ptr<request> req) {
        apilog.info("stop_gossiping");
        return ss.local().stop_gossiping().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::start_gossiping.set(r, [&ss](std::unique_ptr<request> req) {
        apilog.info("start_gossiping");
        return ss.local().start_gossiping().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_gossip_running.set(r, [&ss](std::unique_ptr<request> req) {
        return ss.local().is_gossip_running().then([] (bool running){
            return make_ready_future<json::json_return_type>(running);
        });
    });


    ss::stop_daemon.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_initialized.set(r, [&ss, &g](std::unique_ptr<request> req) {
        return ss.local().get_operation_mode().then([&g] (auto mode) {
            bool is_initialized = mode >= service::storage_service::mode::STARTING;
            if (mode == service::storage_service::mode::NORMAL) {
                is_initialized = g.is_enabled();
            }
            return make_ready_future<json::json_return_type>(is_initialized);
        });
    });

    ss::join_ring.set(r, [](std::unique_ptr<request> req) {
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_joined.set(r, [&ss] (std::unique_ptr<request> req) {
        return ss.local().get_operation_mode().then([] (auto mode) {
            return make_ready_future<json::json_return_type>(mode >= service::storage_service::mode::JOINING);
        });
    });

    ss::set_stream_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_stream_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_compaction_throughput_mb_per_sec.set(r, [&ctx](std::unique_ptr<request> req) {
        int value = ctx.db.local().get_config().compaction_throughput_mb_per_sec();
        return make_ready_future<json::json_return_type>(value);
    });

    ss::set_compaction_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_incremental_backups_enabled.set(r, [&ctx](std::unique_ptr<request> req) {
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

    ss::set_incremental_backups_enabled.set(r, [&ctx](std::unique_ptr<request> req) {
        auto val_str = req->get_query_param("value");
        bool value = (val_str == "True") || (val_str == "true") || (val_str == "1");
        return ctx.db.invoke_on_all([value] (replica::database& db) {
            db.set_enable_incremental_backups(value);

            // Change both KS and CF, so they are in sync
            for (auto& pair: db.get_keyspaces()) {
                auto& ks = pair.second;
                ks.set_incremental_backups(value);
            }

            for (auto& pair: db.get_column_families()) {
                auto cf_ptr = pair.second;
                cf_ptr->set_incremental_backups(value);
            }
        }).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::rebuild.set(r, [&ss](std::unique_ptr<request> req) {
        auto source_dc = req->get_query_param("source_dc");
        apilog.info("rebuild: source_dc={}", source_dc);
        return ss.local().rebuild(std::move(source_dc)).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::bulk_load.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto path = req->param["path"];
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::bulk_load_async.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto path = req->param["path"];
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::reschedule_failed_deletions.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::sample_key_range.set(r, [](std::unique_ptr<request> req) {
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

    ss::set_trace_probability.set(r, [](std::unique_ptr<request> req) {
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

    ss::get_trace_probability.set(r, [](std::unique_ptr<request> req) {
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

    ss::set_slow_query.set(r, [](std::unique_ptr<request> req) {
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

    ss::enable_auto_compaction.set(r, [&ctx](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto tables = parse_tables(keyspace, ctx, req->query_parameters, "cf");

        apilog.info("enable_auto_compaction: keyspace={} tables={}", keyspace, tables);
        return set_tables_autocompaction(ctx, keyspace, tables, true);
    });

    ss::disable_auto_compaction.set(r, [&ctx](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto tables = parse_tables(keyspace, ctx, req->query_parameters, "cf");

        apilog.info("disable_auto_compaction: keyspace={} tables={}", keyspace, tables);
        return set_tables_autocompaction(ctx, keyspace, tables, false);
    });

    ss::deliver_hints.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
      });

    ss::get_cluster_name.set(r, [&g](const_req req) {
        return g.get_cluster_name();
    });

    ss::get_partitioner_name.set(r, [&g](const_req req) {
        return g.get_partitioner_name();
    });

    ss::get_tombstone_warn_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_tombstone_warn_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto debug_threshold = req->get_query_param("debug_threshold");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_tombstone_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_tombstone_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto debug_threshold = req->get_query_param("debug_threshold");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_batch_size_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_batch_size_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto threshold = req->get_query_param("threshold");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::set_hinted_handoff_throttle_in_kb.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto debug_threshold = req->get_query_param("throttle");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_metrics_load.set(r, [&ctx](std::unique_ptr<request> req) {
        return get_cf_stats(ctx, &replica::column_family_stats::live_disk_space_used);
    });

    ss::get_exceptions.set(r, [&ss](const_req req) {
        return ss.local().get_exception_count();
    });

    ss::get_total_hints_in_progress.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_total_hints.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_ownership.set(r, [&ss] (std::unique_ptr<request> req) {
        return ss.local().get_ownership().then([] (auto&& ownership) {
            std::vector<storage_service_json::mapper> res;
            return make_ready_future<json::json_return_type>(map_to_key_value(ownership, res));
        });
    });

    ss::get_effective_ownership.set(r, [&ctx, &ss] (std::unique_ptr<request> req) {
        auto keyspace_name = req->param["keyspace"] == "null" ? "" : validate_keyspace(ctx, req->param);
        return ss.local().effective_ownership(keyspace_name).then([] (auto&& ownership) {
            std::vector<storage_service_json::mapper> res;
            return make_ready_future<json::json_return_type>(map_to_key_value(ownership, res));
        });
    });

    ss::sstable_info.set(r, [&ctx] (std::unique_ptr<request> req) {
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

                for (auto& t : db.get_column_families() | boost::adaptors::map_values) {
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
                            info.generation = sstables::generation_value(sstable->generation());
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
                }
                std::sort(res.begin(), res.end(), [](const ss::table_sstables& t1, const ss::table_sstables& t2) {
                    return t1.keyspace() < t2.keyspace() || (t1.keyspace() == t2.keyspace() && t1.table() < t2.table());
                });
                return res;
            }).then([&dst] {
                return make_ready_future<json::json_return_type>(stream_object(dst));
            });
        });
    });
}

enum class scrub_status {
    successful = 0,
    aborted,
    unable_to_cancel,   // Not used in Scylla, included to ensure compability with nodetool api.
    validation_errors,
};

void set_snapshot(http_context& ctx, routes& r, sharded<db::snapshot_ctl>& snap_ctl) {
    ss::get_snapshot_details.set(r, [&snap_ctl](std::unique_ptr<request> req) {
        return snap_ctl.local().get_snapshot_details().then([] (std::unordered_map<sstring, std::vector<db::snapshot_ctl::snapshot_details>>&& result) {
            std::function<future<>(output_stream<char>&&)> f = [result = std::move(result)](output_stream<char>&& s) {
                return do_with(output_stream<char>(std::move(s)), true, [&result] (output_stream<char>& s, bool& first){
                    return s.write("[").then([&s, &first, &result] {
                        return do_for_each(result, [&s, &first](std::tuple<sstring, std::vector<db::snapshot_ctl::snapshot_details>>&& map){
                            return do_with(ss::snapshots(), [&s, &first, &map](ss::snapshots& all_snapshots) {
                                all_snapshots.key = std::get<0>(map);
                                future<> f = first ? make_ready_future<>() : s.write(", ");
                                first = false;
                                std::vector<ss::snapshot> snapshot;
                                for (auto& cf: std::get<1>(map)) {
                                    ss::snapshot snp;
                                    snp.ks = cf.ks;
                                    snp.cf = cf.cf;
                                    snp.live = cf.live;
                                    snp.total = cf.total;
                                    snapshot.push_back(std::move(snp));
                                }
                                all_snapshots.value = std::move(snapshot);
                                return f.then([&s, &all_snapshots] {
                                    return all_snapshots.write(s);
                                });
                            });
                        });
                    }).then([&s] {
                        return s.write("]").then([&s] {
                            return s.close();
                        });
                    });
                });
            };

            return make_ready_future<json::json_return_type>(std::move(f));
        });
    });

    ss::take_snapshot.set(r, [&ctx, &snap_ctl](std::unique_ptr<request> req) -> future<json::json_return_type> {
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

    ss::del_snapshot.set(r, [&snap_ctl](std::unique_ptr<request> req) -> future<json::json_return_type> {
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

    ss::true_snapshots_size.set(r, [&snap_ctl](std::unique_ptr<request> req) {
        return snap_ctl.local().true_snapshots_size().then([] (int64_t size) {
            return make_ready_future<json::json_return_type>(size);
        });
    });

    ss::scrub.set(r, [&ctx, &snap_ctl] (std::unique_ptr<request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto rp = req_params({
            {"keyspace", {mandatory::yes}},
            {"cf", {""}},
            {"scrub_mode", {}},
            {"skip_corrupted", {}},
            {"disable_snapshot", {}},
            {"quarantine_mode", {}},
        });
        rp.process(*req);
        auto keyspace = validate_keyspace(ctx, *rp.get("keyspace"));
        auto column_families = parse_tables(keyspace, ctx, *rp.get("cf"));
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
            co_await coroutine::parallel_for_each(column_families, [&snap_ctl, keyspace, tag](sstring cf) {
                // We always pass here db::snapshot_ctl::snap_views::no since:
                // 1. When scrubbing particular tables, there's no need to auto-snapshot their views.
                // 2. When scrubbing the whole keyspace, column_families will contain both base tables and views.
                return snap_ctl.local().take_column_family_snapshot(keyspace, cf, tag, db::snapshot_ctl::snap_views::no, db::snapshot_ctl::skip_flush::no);
            });
        }

        sstables::compaction_type_options::scrub opts = {
            .operation_mode = scrub_mode,
        };
        const sstring quarantine_mode_str = req_param<sstring>(*req, "quarantine_mode", "INCLUDE");
        if (quarantine_mode_str == "INCLUDE") {
            opts.quarantine_operation_mode = sstables::compaction_type_options::scrub::quarantine_mode::include;
        } else if (quarantine_mode_str == "EXCLUDE") {
            opts.quarantine_operation_mode = sstables::compaction_type_options::scrub::quarantine_mode::exclude;
        } else if (quarantine_mode_str == "ONLY") {
            opts.quarantine_operation_mode = sstables::compaction_type_options::scrub::quarantine_mode::only;
        } else {
            throw httpd::bad_param_exception(fmt::format("Unknown argument for 'quarantine_mode' parameter: {}", quarantine_mode_str));
        }

        const auto& reduce_compaction_stats = [] (const compaction_manager::compaction_stats_opt& lhs, const compaction_manager::compaction_stats_opt& rhs) {
            sstables::compaction_stats stats{};
            stats += lhs.value();
            stats += rhs.value();
            return stats;
        };

        try {
            auto opt_stats = co_await db.map_reduce0([&] (replica::database& db) {
                return map_reduce(column_families, [&] (sstring cfname) -> future<std::optional<sstables::compaction_stats>> {
                    auto& cm = db.get_compaction_manager();
                    auto& cf = db.find_column_family(keyspace, cfname);
                    sstables::compaction_stats stats{};
                    co_await cf.parallel_foreach_table_state([&] (compaction::table_state& ts) mutable -> future<> {
                        auto r = co_await cm.perform_sstable_scrub(ts, opts);
                        stats += r.value_or(sstables::compaction_stats{});
                    });
                    co_return stats;
                }, std::make_optional(sstables::compaction_stats{}), reduce_compaction_stats);
            }, std::make_optional(sstables::compaction_stats{}), reduce_compaction_stats);
            if (opt_stats && opt_stats->validation_errors) {
                co_return json::json_return_type(static_cast<int>(scrub_status::validation_errors));
            }
        } catch (const sstables::compaction_aborted_exception&) {
            co_return json::json_return_type(static_cast<int>(scrub_status::aborted));
        } catch (...) {
            apilog.error("scrub keyspace={} tables={} failed: {}", keyspace, column_families, std::current_exception());
            throw;
        }

        co_return json::json_return_type(static_cast<int>(scrub_status::successful));
    });
}

void unset_snapshot(http_context& ctx, routes& r) {
    ss::get_snapshot_details.unset(r);
    ss::take_snapshot.unset(r);
    ss::del_snapshot.unset(r);
    ss::true_snapshots_size.unset(r);
    ss::scrub.unset(r);
}

}
