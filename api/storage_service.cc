/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "storage_service.hh"
#include "api/api-doc/storage_service.json.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "utils/hash.hh"
#include <sstream>
#include <time.h>
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
#include "seastar/http/exception.hh"
#include "repair/repair.hh"
#include "locator/snitch_base.hh"
#include "column_family.hh"
#include "log.hh"
#include "release.hh"
#include "compaction/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "database.hh"
#include "db/extensions.hh"
#include "db/snapshot-ctl.hh"
#include "transport/controller.hh"
#include "thrift/controller.hh"
#include "locator/token_metadata.hh"
#include "cdc/generation_service.hh"
#include "service/storage_proxy.hh"
#include "locator/abstract_replication_strategy.hh"

extern logging::logger apilog;

namespace api {

const locator::token_metadata& http_context::get_token_metadata() {
        return *shared_token_metadata.local().get();
}

namespace ss = httpd::storage_service_json;
using namespace json;

static sstring validate_keyspace(http_context& ctx, const parameters& param) {
    if (ctx.db.local().has_keyspace(param["keyspace"])) {
        return param["keyspace"];
    }
    throw bad_param_exception("Keyspace " + param["keyspace"] + " Does not exist");
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

using ks_cf_func = std::function<future<json::json_return_type>(http_context&, std::unique_ptr<request>, sstring, std::vector<sstring>)>;

static auto wrap_ks_cf(http_context &ctx, ks_cf_func f) {
    return [&ctx, f = std::move(f)](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_families = split_cf(req->get_query_param("cf"));
        if (column_families.empty()) {
            column_families = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
        }
        return f(ctx, std::move(req), std::move(keyspace), std::move(column_families));
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

future<json::json_return_type> set_tables_autocompaction(http_context& ctx, service::storage_service& ss, const sstring &keyspace, std::vector<sstring> tables, bool enabled) {
    if (tables.empty()) {
        tables = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
    }

    return ss.set_tables_autocompaction(keyspace, tables, enabled).then([]{
        return make_ready_future<json::json_return_type>(json_void());
    });
}

void set_transport_controller(http_context& ctx, routes& r, cql_transport::controller& ctl) {
    ss::start_native_transport.set(r, [&ctl](std::unique_ptr<request> req) {
        return ctl.start_server().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::stop_native_transport.set(r, [&ctl](std::unique_ptr<request> req) {
        return ctl.stop_server().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_native_transport_running.set(r, [&ctl] (std::unique_ptr<request> req) {
        return ctl.is_server_running().then([] (bool running) {
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
    ss::stop_rpc_server.set(r, [&ctl](std::unique_ptr<request> req) {
        return ctl.stop_server().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::start_rpc_server.set(r, [&ctl](std::unique_ptr<request> req) {
        return ctl.start_server().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_rpc_server_running.set(r, [&ctl] (std::unique_ptr<request> req) {
        return ctl.is_server_running().then([] (bool running) {
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

    ss::get_active_repair_async.set(r, [&ctx](std::unique_ptr<request> req) {
        return get_active_repairs(ctx.db).then([] (std::vector<int> res){
            return make_ready_future<json::json_return_type>(res);
        });
    });

    ss::repair_async_status.set(r, [&ctx](std::unique_ptr<request> req) {
        return repair_get_status(ctx.db, boost::lexical_cast<int>( req->get_query_param("id")))
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

    ss::repair_await_completion.set(r, [&ctx](std::unique_ptr<request> req) {
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
        return repair_await_completion(ctx.db, id, expire)
                .then_wrapped([] (future<repair_status>&& fut) {
            ss::ns_repair_async_status::return_type_wrapper res;
            try {
                res = fut.get0();
            } catch (std::exception& e) {
                return make_exception_future<json::json_return_type>(httpd::server_error_exception(e.what()));
            }
            return make_ready_future<json::json_return_type>(json::json_return_type(res));
        });
    });

    ss::force_terminate_all_repair_sessions.set(r, [&ctx](std::unique_ptr<request> req) {
        return repair_abort_all(ctx.db).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::force_terminate_all_repair_sessions_new.set(r, [&ctx](std::unique_ptr<request> req) {
        return repair_abort_all(ctx.db).then([] {
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

void set_storage_service(http_context& ctx, routes& r, sharded<service::storage_service>& ss) {
    ss::local_hostid.set(r, [](std::unique_ptr<request> req) {
        return db::system_keyspace::get_local_host_id().then([](const utils::UUID& id) {
            return make_ready_future<json::json_return_type>(id.to_sstring());
        });
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

    ss::get_range_to_endpoint_map.set(r, [&ctx, &ss](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        std::vector<ss::maplist_mapper> res;
        return make_ready_future<json::json_return_type>(stream_range_as_array(ss.local().get_range_to_address_map(keyspace),
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
        }));
    });

    ss::get_pending_range_to_endpoint_map.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto keyspace = validate_keyspace(ctx, req->param);
        std::vector<ss::maplist_mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::describe_any_ring.set(r, [&ctx, &ss](std::unique_ptr<request> req) {
        return make_ready_future<json::json_return_type>(stream_range_as_array(ss.local().describe_ring(""), token_range_endpoints_to_json));
    });

    ss::describe_ring.set(r, [&ctx, &ss](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        return make_ready_future<json::json_return_type>(stream_range_as_array(ss.local().describe_ring(keyspace), token_range_endpoints_to_json));
    });

    ss::get_host_id_map.set(r, [&ctx](const_req req) {
        std::vector<ss::mapper> res;
        return map_to_key_value(ctx.get_token_metadata().get_endpoint_to_host_id_map_for_reading(), res);
    });

    ss::get_load.set(r, [&ctx](std::unique_ptr<request> req) {
        return get_cf_stats(ctx, &column_family_stats::live_disk_space_used);
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

    ss::get_current_generation_number.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address ep(utils::fb_utilities::get_broadcast_address());
        return gms::get_local_gossiper().get_current_generation_number(ep).then([](int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    ss::get_natural_endpoints.set(r, [&ctx, &ss](const_req req) {
        auto keyspace = validate_keyspace(ctx, req.param);
        return container_to_vec(ss.local().get_natural_endpoints(keyspace, req.get_query_param("cf"),
                req.get_query_param("key")));
    });

    ss::cdc_streams_check_and_repair.set(r, [&ctx, &ss] (std::unique_ptr<request> req) {
        return ss.local().get_cdc_generation_service().check_and_repair_cdc_streams().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::force_keyspace_compaction.set(r, [&ctx](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_families = split_cf(req->get_query_param("cf"));
        if (column_families.empty()) {
            column_families = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
        }
        return ctx.db.invoke_on_all([keyspace, column_families] (database& db) {
            std::vector<column_family*> column_families_vec;
            for (auto cf : column_families) {
                column_families_vec.push_back(&db.find_column_family(keyspace, cf));
            }
            return parallel_for_each(column_families_vec, [] (column_family* cf) {
                    return cf->compact_all_sstables();
            });
        }).then([]{
                return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::force_keyspace_cleanup.set(r, [&ctx, &ss](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_families = split_cf(req->get_query_param("cf"));
        if (column_families.empty()) {
            column_families = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
        }
        return ss.local().is_cleanup_allowed(keyspace).then([&ctx, keyspace,
                column_families = std::move(column_families)] (bool is_cleanup_allowed) mutable {
            if (!is_cleanup_allowed) {
                return make_exception_future<json::json_return_type>(
                        std::runtime_error("Can not perform cleanup operation when topology changes"));
            }
            return ctx.db.invoke_on_all([keyspace, column_families] (database& db) {
                std::vector<column_family*> column_families_vec;
                auto& cm = db.get_compaction_manager();
                for (auto cf : column_families) {
                    column_families_vec.push_back(&db.find_column_family(keyspace, cf));
                }
                return parallel_for_each(column_families_vec, [&cm, &db] (column_family* cf) {
                    return cm.perform_cleanup(db, cf);
                });
            }).then([]{
                return make_ready_future<json::json_return_type>(0);
            });
        });
    });

    ss::validate.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<request> req, sstring keyspace, std::vector<sstring> column_families ) {
        return ctx.db.invoke_on_all([=] (database& db) {
            return do_for_each(column_families, [=, &db](sstring cfname) {
                auto& cm = db.get_compaction_manager();
                auto& cf = db.find_column_family(keyspace, cfname);
                return cm.perform_sstable_scrub(&cf, sstables::compaction_options::scrub::mode::validate);
            });
        }).then([]{
            return make_ready_future<json::json_return_type>(0);
        });

    }));

    ss::upgrade_sstables.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<request> req, sstring keyspace, std::vector<sstring> column_families) {
        bool exclude_current_version = req_param<bool>(*req, "exclude_current_version", false);

        return ctx.db.invoke_on_all([=] (database& db) {
            return do_for_each(column_families, [=, &db](sstring cfname) {
                auto& cm = db.get_compaction_manager();
                auto& cf = db.find_column_family(keyspace, cfname);
                return cm.perform_sstable_upgrade(db, &cf, exclude_current_version);
            });
        }).then([]{
            return make_ready_future<json::json_return_type>(0);
        });
    }));

    ss::force_keyspace_flush.set(r, [&ctx](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_families = split_cf(req->get_query_param("cf"));
        if (column_families.empty()) {
            column_families = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
        }
        return ctx.db.invoke_on_all([keyspace, column_families] (database& db) {
            return parallel_for_each(column_families, [&db, keyspace](const sstring& cf) mutable {
                return db.find_column_family(keyspace, cf).flush();
            });
        }).then([]{
                return make_ready_future<json::json_return_type>(json_void());
        });
    });


    ss::decommission.set(r, [&ss](std::unique_ptr<request> req) {
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
        auto host_id = req->get_query_param("host_id");
        std::vector<sstring> ignore_nodes_strs= split(req->get_query_param("ignore_nodes"), ",");
        auto ignore_nodes = std::list<gms::inet_address>();
        for (std::string n : ignore_nodes_strs) {
            try {
                std::replace(n.begin(), n.end(), '\"', ' ');
                std::replace(n.begin(), n.end(), '\'', ' ');
                boost::trim_all(n);
                if (!n.empty()) {
                    auto node = gms::inet_address(n);
                    ignore_nodes.push_back(node);
                }
            } catch (...) {
                throw std::runtime_error(format("Failed to parse ignore_nodes parameter: ignore_nodes={}, node={}", ignore_nodes_strs, n));
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
            return make_ready_future<json::json_return_type>(mode);
        });
    });

    ss::is_starting.set(r, [&ss](std::unique_ptr<request> req) {
        return ss.local().is_starting().then([] (auto starting) {
            return make_ready_future<json::json_return_type>(starting);
        });
    });

    ss::get_drain_progress.set(r, [&ss](std::unique_ptr<request> req) {
        return ss.map_reduce(adder<service::storage_service::drain_progress>(), [] (auto& ss) {
            return ss.get_drain_progress();
        }).then([] (auto&& progress) {
            auto progress_str = format("Drained {}/{} ColumnFamilies", progress.remaining_cfs, progress.total_cfs);
            return make_ready_future<json::json_return_type>(std::move(progress_str));
        });
    });

    ss::drain.set(r, [&ss](std::unique_ptr<request> req) {
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
            return ctx.db.local().get_non_system_keyspaces();
        } else if (type == "non_local_strategy") {
            return map_keys(ctx.db.local().get_keyspaces() | boost::adaptors::filtered([](const auto& p) {
                return p.second.get_replication_strategy().get_type() != locator::replication_strategy_type::local;
            }));
        }
        return map_keys(ctx.db.local().get_keyspaces());
    });

    ss::update_snitch.set(r, [](std::unique_ptr<request> req) {
        auto ep_snitch_class_name = req->get_query_param("ep_snitch_class_name");
        return locator::i_endpoint_snitch::reset_snitch(ep_snitch_class_name).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::stop_gossiping.set(r, [&ss](std::unique_ptr<request> req) {
        return ss.local().stop_gossiping().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::start_gossiping.set(r, [&ss](std::unique_ptr<request> req) {
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

    ss::is_initialized.set(r, [&ss](std::unique_ptr<request> req) {
        return ss.local().is_initialized().then([] (bool initialized) {
            return make_ready_future<json::json_return_type>(initialized);
        });
    });

    ss::join_ring.set(r, [](std::unique_ptr<request> req) {
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_joined.set(r, [&ss] (std::unique_ptr<request> req) {
        return make_ready_future<json::json_return_type>(ss.local().is_joined());
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
        return ctx.db.map_reduce(adder<bool>(), [] (database& db) {
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
        return ctx.db.invoke_on_all([value] (database& db) {
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

    ss::load_new_ss_tables.set(r, [&ctx, &ss](std::unique_ptr<request> req) {
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
        return ss.invoke_on(coordinator,
                [ks = std::move(ks), cf = std::move(cf),
                load_and_stream, primary_replica_only] (service::storage_service& s) {
            return s.load_new_sstables(ks, cf, load_and_stream, primary_replica_only);
        }).then_wrapped([] (auto&& f) {
            if (f.failed()) {
                auto msg = fmt::format("Failed to load new sstables: {}", f.get_exception());
                return make_exception_future<json::json_return_type>(httpd::server_error_exception(msg));
            }
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::sample_key_range.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::reset_local_schema.set(r, [](std::unique_ptr<request> req) {
        // FIXME: We should truncate schema tables if more than one node in the cluster.
        auto& sp = service::get_storage_proxy();
        auto& fs = sp.local().features();
        return db::schema_tables::recalculate_schema_version(sp, fs).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::set_trace_probability.set(r, [](std::unique_ptr<request> req) {
        auto probability = req->get_query_param("probability");
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

    ss::enable_auto_compaction.set(r, [&ctx, &ss](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto tables = split_cf(req->get_query_param("cf"));

        return set_tables_autocompaction(ctx, ss.local(), keyspace, tables, true);
    });

    ss::disable_auto_compaction.set(r, [&ctx, &ss](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto tables = split_cf(req->get_query_param("cf"));

        return set_tables_autocompaction(ctx, ss.local(), keyspace, tables, false);
    });

    ss::deliver_hints.set(r, [](std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        auto host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
      });

    ss::get_cluster_name.set(r, [](const_req req) {
        return gms::get_local_gossiper().get_cluster_name();
    });

    ss::get_partitioner_name.set(r, [](const_req req) {
        return gms::get_local_gossiper().get_partitioner_name();
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
        return get_cf_stats(ctx, &column_family_stats::live_disk_space_used);
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

    ss::view_build_statuses.set(r, [&ctx, &ss] (std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto view = req->param["view"];
        return ss.local().view_build_statuses(std::move(keyspace), std::move(view)).then([] (std::unordered_map<sstring, sstring> status) {
            std::vector<storage_service_json::mapper> res;
            return make_ready_future<json::json_return_type>(map_to_key_value(std::move(status), res));
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
            }, [ks, cf](const database& db) {
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
                            info.generation = sstable->generation();
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

    ss::take_snapshot.set(r, [&snap_ctl](std::unique_ptr<request> req) {
        apilog.debug("take_snapshot: {}", req->query_parameters);
        auto tag = req->get_query_param("tag");
        auto column_families = split(req->get_query_param("cf"), ",");
        auto sfopt = req->get_query_param("sf");
        auto sf = db::snapshot_ctl::skip_flush(strcasecmp(sfopt.c_str(), "true") == 0);

        std::vector<sstring> keynames = split(req->get_query_param("kn"), ",");

        auto resp = make_ready_future<>();
        if (column_families.empty()) {
            resp = snap_ctl.local().take_snapshot(tag, keynames, sf);
        } else {
            if (keynames.empty()) {
                throw httpd::bad_param_exception("The keyspace of column families must be specified");
            }
            if (keynames.size() > 1) {
                throw httpd::bad_param_exception("Only one keyspace allowed when specifying a column family");
            }
            resp = snap_ctl.local().take_column_family_snapshot(keynames[0], column_families, tag, sf);
        }
        return resp.then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::del_snapshot.set(r, [&snap_ctl](std::unique_ptr<request> req) {
        auto tag = req->get_query_param("tag");
        auto column_family = req->get_query_param("cf");

        std::vector<sstring> keynames = split(req->get_query_param("kn"), ",");
        return snap_ctl.local().clear_snapshot(tag, keynames, column_family).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::true_snapshots_size.set(r, [&snap_ctl](std::unique_ptr<request> req) {
        return snap_ctl.local().true_snapshots_size().then([] (int64_t size) {
            return make_ready_future<json::json_return_type>(size);
        });
    });

    ss::scrub.set(r, wrap_ks_cf(ctx, [&snap_ctl] (http_context& ctx, std::unique_ptr<request> req, sstring keyspace, std::vector<sstring> column_families) {
        auto scrub_mode = sstables::compaction_options::scrub::mode::abort;

        const sstring scrub_mode_str = req_param<sstring>(*req, "scrub_mode", "");
        if (scrub_mode_str == "") {
            const auto skip_corrupted = req_param<bool>(*req, "skip_corrupted", false);

            if (skip_corrupted) {
                scrub_mode = sstables::compaction_options::scrub::mode::skip;
            }
        } else {
            if (scrub_mode_str == "ABORT") {
                scrub_mode = sstables::compaction_options::scrub::mode::abort;
            } else if (scrub_mode_str == "SKIP") {
                scrub_mode = sstables::compaction_options::scrub::mode::skip;
            } else if (scrub_mode_str == "SEGREGATE") {
                scrub_mode = sstables::compaction_options::scrub::mode::segregate;
            } else {
                throw std::invalid_argument(fmt::format("Unknown argument for 'scrub_mode' parameter: {}", scrub_mode_str));
            }
        }

        auto f = make_ready_future<>();
        if (!req_param<bool>(*req, "disable_snapshot", false)) {
            auto tag = format("pre-scrub-{:d}", db_clock::now().time_since_epoch().count());
            f = parallel_for_each(column_families, [&snap_ctl, keyspace, tag](sstring cf) {
                return snap_ctl.local().take_column_family_snapshot(keyspace, cf, tag);
            });
        }

        return f.then([&ctx, keyspace, column_families, scrub_mode] {
            return ctx.db.invoke_on_all([=] (database& db) {
                return do_for_each(column_families, [=, &db](sstring cfname) {
                    auto& cm = db.get_compaction_manager();
                    auto& cf = db.find_column_family(keyspace, cfname);
                    return cm.perform_sstable_scrub(&cf, scrub_mode);
                });
            });
        }).then([]{
            return make_ready_future<json::json_return_type>(0);
        });
    }));
}

void unset_snapshot(http_context& ctx, routes& r) {
    ss::get_snapshot_details.unset(r);
    ss::take_snapshot.unset(r);
    ss::del_snapshot.unset(r);
    ss::true_snapshots_size.unset(r);
    ss::scrub.unset(r);
}

}
