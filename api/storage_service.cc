/*
 * Copyright 2015 Cloudius Systems
 */

#include "storage_service.hh"
#include "api/api-doc/storage_service.json.hh"
#include "db/config.hh"
#include <service/storage_service.hh>
#include <db/commitlog/commitlog.hh>
#include <gms/gossiper.hh>
#include <db/system_keyspace.hh>
#include "http/exception.hh"
#include "repair/repair.hh"
#include "locator/snitch_base.hh"

namespace api {

namespace ss = httpd::storage_service_json;
using namespace json;

static sstring validate_keyspace(http_context& ctx, const parameters& param) {
    if (ctx.db.local().has_keyspace(param["keyspace"])) {
        return param["keyspace"];
    }
    throw bad_param_exception("Keyspace " + param["keyspace"] + " Does not exist");
}

void set_storage_service(http_context& ctx, routes& r) {
    ss::local_hostid.set(r, [](std::unique_ptr<request> req) {
        return db::system_keyspace::get_local_host_id().then([](const utils::UUID& id) {
            return make_ready_future<json::json_return_type>(id.to_sstring());
        });
    });

    ss::get_tokens.set(r, [](std::unique_ptr<request> req) {
        return service::sorted_tokens().then([](const std::vector<dht::token>& tokens) {
            return make_ready_future<json::json_return_type>(container_to_vec(tokens));
        });
    });

    ss::get_node_tokens.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address addr(req->param["endpoint"]);
        return service::get_tokens(addr).then([](const std::vector<dht::token>& tokens) {
            return make_ready_future<json::json_return_type>(container_to_vec(tokens));
        });
    });

    ss::get_commitlog.set(r, [&ctx](const_req req) {
        return ctx.db.local().commitlog()->active_config().commit_log_location;
    });

    ss::get_token_endpoint.set(r, [](std::unique_ptr<request> req) {
        return service::get_token_to_endpoint().then([] (const std::map<dht::token, gms::inet_address>& tokens){
            std::vector<storage_service_json::mapper> res;
            return make_ready_future<json::json_return_type>(map_to_key_value(tokens, res));
        });
    });

    ss::get_leaving_nodes.set(r, [](const_req req) {
        return container_to_vec(service::get_local_storage_service().get_token_metadata().get_leaving_endpoints());
    });

    ss::get_moving_nodes.set(r, [](const_req req) {
        auto points = service::get_local_storage_service().get_token_metadata().get_moving_endpoints();
        std::unordered_set<sstring> addr;
        for (auto i: points) {
            addr.insert(boost::lexical_cast<std::string>(i.second));
        }
        return container_to_vec(addr);
    });

    ss::get_joining_nodes.set(r, [](const_req req) {
        auto points = service::get_local_storage_service().get_token_metadata().get_bootstrap_tokens();
        std::unordered_set<sstring> addr;
        for (auto i: points) {
            addr.insert(boost::lexical_cast<std::string>(i.second));
        }
        return container_to_vec(addr);
    });

    ss::get_release_version.set(r, [](const_req req) {
        return service::get_local_storage_service().get_release_version();
    });

    ss::get_schema_version.set(r, [](const_req req) {
        return service::get_local_storage_service().get_schema_version();
    });

    ss::get_all_data_file_locations.set(r, [&ctx](const_req req) {
        return container_to_vec(ctx.db.local().get_config().data_file_directories());
    });

    ss::get_saved_caches_location.set(r, [&ctx](const_req req) {
        return ctx.db.local().get_config().saved_caches_directory();
    });

    ss::get_range_to_endpoint_map.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        std::vector<ss::maplist_mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_pending_range_to_endpoint_map.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        std::vector<ss::maplist_mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::describe_ring_jmx.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_host_id_map.set(r, [](const_req req) {
        std::vector<ss::mapper> res;
        return map_to_key_value(service::get_local_storage_service().
                get_token_metadata().get_endpoint_to_host_id_map_for_reading(), res);
    });

    ss::get_load.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_load_map.set(r, [](std::unique_ptr<request> req) {
        //TBD
        std::vector<ss::mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_current_generation_number.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address ep(utils::fb_utilities::get_broadcast_address());
        return gms::get_current_generation_number(ep).then([](int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    ss::get_natural_endpoints.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_family = req->get_query_param("cf");
        auto key = req->get_query_param("key");

        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_snapshot_details.set(r, [](std::unique_ptr<request> req) {
        //TBD
        std::vector<ss::snapshots> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::take_snapshot.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto tag = req->get_query_param("tag");
        auto keyname = req->get_query_param("kn");
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::del_snapshot.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto tag = req->get_query_param("tag");
        auto keyname = req->get_query_param("kn");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::true_snapshots_size.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
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

    ss::force_keyspace_cleanup.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::scrub.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_family = req->get_query_param("cf");
        auto disable_snapshot = req->get_query_param("disable_snapshot");
        auto skip_corrupted = req->get_query_param("skip_corrupted");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::upgrade_sstables.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_family = req->get_query_param("cf");
        auto exclude_current_version = req->get_query_param("exclude_current_version");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::force_keyspace_flush.set(r, [&ctx](std::unique_ptr<request> req) {
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_families = split_cf(req->get_query_param("cf"));
        if (column_families.empty()) {
            column_families = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
        }
        return ctx.db.invoke_on_all([keyspace, column_families] (database& db) {
            for (auto cf : column_families) {
                db.find_column_family(keyspace, cf).flush();
            }
        }).then([]{
                return make_ready_future<json::json_return_type>(json_void());
        });
    });


    ss::repair_async.set(r, [&ctx](std::unique_ptr<request> req) {
        // Currently, we get all the repair options encoded in a single
        // "options" option, and split it to a map using the "," and ":"
        // delimiters. TODO: consider if it doesn't make more sense to just
        // take all the query parameters as this map and pass it to the repair
        // function.
        std::unordered_map<sstring, sstring> options_map;
        for (auto s : split(req->get_query_param("options"), ",")) {
            auto kv = split(s, ":");
            if (kv.size() != 2) {
                throw httpd::bad_param_exception("malformed async repair options");
            }
            options_map.emplace(std::move(kv[0]), std::move(kv[1]));
        }

        // The repair process is asynchronous: repair_start only starts it and
        // returns immediately, not waiting for the repair to finish. The user
        // then has other mechanisms to track the ongoing repair's progress,
        // or stop it.
        return repair_start(ctx.db, validate_keyspace(ctx, req->param),
                options_map).then([] (int i) {
                    return make_ready_future<json::json_return_type>(i);
                });
    });

    ss::repair_async_status.set(r, [&ctx](std::unique_ptr<request> req) {
        return repair_get_status(ctx.db, boost::lexical_cast<int>( req->get_query_param("id")))
                .then_wrapped([] (future<repair_status>&& fut) {
            ss::ns_repair_async_status::return_type_wrapper res;
            try {
                res = fut.get0();
            } catch(std::runtime_error& e) {
                return make_ready_future<json::json_return_type>(json_exception(httpd::bad_param_exception(e.what())));
            }
            return make_ready_future<json::json_return_type>(json::json_return_type(res));
        });
    });

    ss::force_terminate_all_repair_sessions.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::decommission.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::move.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto new_token = req->get_query_param("new_token");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::remove_node.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto token = req->get_query_param("token");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_removal_status.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::force_remove_completion.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::set_logging_level.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto class_qualifier = req->get_query_param("class_qualifier");
        auto level = req->get_query_param("level");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_logging_levels.set(r, [](std::unique_ptr<request> req) {
        //TBD
        std::vector<ss::mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_operation_mode.set(r, [](std::unique_ptr<request> req) {
        auto mode = service::get_local_storage_service().get_operation_mode();
        return make_ready_future<json::json_return_type>(mode);
    });

    ss::is_starting.set(r, [](const_req req) {
        return service::get_local_storage_service().is_starting();
    });

    ss::get_drain_progress.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::drain.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });
    ss::truncate.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_keyspaces.set(r, [&ctx](const_req req) {
        auto non_system = req.get_query_param("non_system");
        return map_keys(ctx.db.local().keyspaces());
    });

    ss::update_snitch.set(r, [](std::unique_ptr<request> req) {
        auto ep_snitch_class_name = req->get_query_param("ep_snitch_class_name");
        return locator::i_endpoint_snitch::reset_snitch(ep_snitch_class_name).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::stop_gossiping.set(r, [](std::unique_ptr<request> req) {
        return service::get_local_storage_service().stop_gossiping().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::start_gossiping.set(r, [](std::unique_ptr<request> req) {
        return service::get_local_storage_service().start_gossiping().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_gossip_running.set(r, [](const_req req) {
        return service::get_local_storage_service().is_gossip_running();
    });

    ss::stop_daemon.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_initialized.set(r, [](const_req req) {
        return service::get_local_storage_service().is_initialized();
    });

    ss::stop_rpc_server.set(r, [](std::unique_ptr<request> req) {
        return service::get_local_storage_service().stop_rpc_server().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::start_rpc_server.set(r, [](std::unique_ptr<request> req) {
        return service::get_local_storage_service().start_rpc_server().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_rpc_server_running.set(r, [](const_req req) {
        return service::get_local_storage_service().is_rpc_server_running();
    });

    ss::start_native_transport.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::stop_native_transport.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_native_transport_running.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(true);
    });

    ss::join_ring.set(r, [](std::unique_ptr<request> req) {
        return service::get_local_storage_service().join_ring().then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    ss::is_joined.set(r, [](const_req req) {
        return service::get_local_storage_service().is_joined();
    });

    ss::set_stream_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_stream_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_compaction_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_compaction_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::is_incremental_backups_enabled.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(false);
    });

    ss::set_incremental_backups_enabled.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::rebuild.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto source_dc = req->get_query_param("source_dc");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::bulk_load.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto path = req->param["path"];
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::bulk_load_async.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto path = req->param["path"];
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::reschedule_failed_deletions.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::load_new_ss_tables.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::sample_key_range.set(r, [](std::unique_ptr<request> req) {
        //TBD
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::reset_local_schema.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::set_trace_probability.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto probability = req->get_query_param("probability");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_trace_probability.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::enable_auto_compaction.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::disable_auto_compaction.set(r, [&ctx](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = validate_keyspace(ctx, req->param);
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::deliver_hints.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
      });

    ss::get_cluster_name.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_partitioner_name.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_tombstone_warn_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_tombstone_warn_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto debug_threshold = req->get_query_param("debug_threshold");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_tombstone_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_tombstone_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto debug_threshold = req->get_query_param("debug_threshold");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_batch_size_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_batch_size_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto threshold = req->get_query_param("threshold");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::set_hinted_handoff_throttle_in_kb.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto debug_threshold = req->get_query_param("throttle");
        return make_ready_future<json::json_return_type>(json_void());
    });

    ss::get_metrics_load.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_exceptions.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_total_hints_in_progress.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_total_hints.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_ownership.set(r, [](const_req req) {
        auto tokens = service::get_local_storage_service().get_ownership();
        std::vector<storage_service_json::mapper> res;
        return map_to_key_value(tokens, res);
    });

    ss::get_effective_ownership.set(r, [&ctx](const_req req) {
        auto tokens = service::get_local_storage_service().effective_ownership(
                (req.param["keyspace"] == "null")? "" : validate_keyspace(ctx, req.param));
        std::vector<storage_service_json::mapper> res;
        return map_to_key_value(tokens, res);
    });
}

}
