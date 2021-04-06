/*
 * Copyright (C) 2018 ScyllaDB
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

#include <boost/range/adaptors.hpp>
#include "gms/gossiper.hh"
#include "service/storage_proxy.hh"
#include "sstables/sstables.hh"
#include "utils/stall_free.hh"
#include "service/migration_manager.hh"
#include "repair/hash.hh"
#include "repair/sink_source_for_repair.hh"
#include "repair/row.hh"
#include "repair/reader.hh"
#include "repair/writer.hh"
#include "repair/row_level.hh"
#include "repair/repair_meta.hh"

extern logging::logger rlogger;

namespace repair {
    inline bool inject_rpc_stream_error = false;

    inline distributed<db::system_distributed_keyspace> *_sys_dist_ks;
    inline distributed<db::view::view_update_generator> *_view_update_generator;
    inline sharded<netw::messaging_service> *_messaging;

    inline thread_local row_level_repair_metrics _metrics;
}

using namespace repair;

static future<stop_iteration> repair_get_row_diff_with_rpc_stream_process_op(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_row_on_wire_with_cmd> sink,
        rpc::source<repair_hash_with_cmd> source,
        bool &error,
        repair_hash_set& current_set_diff,
        std::optional<std::tuple<repair_hash_with_cmd>> hash_cmd_opt) {
    repair_hash_with_cmd hash_cmd = std::get<0>(hash_cmd_opt.value());
    rlogger.trace("Got repair_hash_with_cmd from peer={}, hash={}, cmd={}", from, hash_cmd.hash, int(hash_cmd.cmd));
    if (hash_cmd.cmd == repair_stream_cmd::hash_data) {
        current_set_diff.insert(hash_cmd.hash);
        return make_ready_future<stop_iteration>(stop_iteration::no);
    } else if (hash_cmd.cmd == repair_stream_cmd::end_of_current_hash_set || hash_cmd.cmd == repair_stream_cmd::needs_all_rows) {
        if (inject_rpc_stream_error) {
            return make_exception_future<stop_iteration>(std::runtime_error("get_row_diff_with_rpc_stream: Inject error in handler loop"));
        }
        bool needs_all_rows = hash_cmd.cmd == repair_stream_cmd::needs_all_rows;
        _metrics.rx_hashes_nr += current_set_diff.size();
        auto fp = make_foreign(std::make_unique<repair_hash_set>(std::move(current_set_diff)));
        return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, needs_all_rows, fp = std::move(fp)] {
            auto rm = get_repair_meta(from, repair_meta_id);
            rm->set_repair_state_for_local_node(repair_state::get_row_diff_with_rpc_stream_started);
            if (fp.get_owner_shard() == this_shard_id()) {
                return rm->get_row_diff_handler(std::move(*fp), repair_meta::needs_all_rows_t(needs_all_rows)).then([rm] (repair_rows_on_wire rows) {
                    rm->set_repair_state_for_local_node(repair_state::get_row_diff_with_rpc_stream_finished);
                    return rows;
                });
            } else {
                return rm->get_row_diff_handler(*fp, repair_meta::needs_all_rows_t(needs_all_rows)).then([rm] (repair_rows_on_wire rows) {
                    rm->set_repair_state_for_local_node(repair_state::get_row_diff_with_rpc_stream_finished);
                    return rows;
                });
            }
        }).then([sink] (repair_rows_on_wire rows_on_wire) mutable {
            if (rows_on_wire.empty()) {
                return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::end_of_current_rows, repair_row_on_wire()});
            }
            return do_with(std::move(rows_on_wire), [sink] (repair_rows_on_wire& rows_on_wire) mutable {
                return do_for_each(rows_on_wire, [sink] (repair_row_on_wire& row) mutable {
                    return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::row_data, std::move(row)});
                }).then([sink] () mutable {
                    return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::end_of_current_rows, repair_row_on_wire()});
                });
            });
        }).then([sink] () mutable {
            return sink.flush();
        }).then([sink] {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    } else {
        return make_exception_future<stop_iteration>(std::runtime_error("Got unexpected repair_stream_cmd"));
    }
}

static future<stop_iteration> repair_put_row_diff_with_rpc_stream_process_op(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_stream_cmd> sink,
        rpc::source<repair_row_on_wire_with_cmd> source,
        bool& error,
        repair_rows_on_wire& current_rows,
        std::optional<std::tuple<repair_row_on_wire_with_cmd>> row_opt) {
    auto row = std::move(std::get<0>(row_opt.value()));
    if (row.cmd == repair_stream_cmd::row_data) {
        rlogger.trace("Got repair_rows_on_wire from peer={}, got row_data", from);
        current_rows.push_back(std::move(row.row));
        return make_ready_future<stop_iteration>(stop_iteration::no);
    } else if (row.cmd == repair_stream_cmd::end_of_current_rows) {
        rlogger.trace("Got repair_rows_on_wire from peer={}, got end_of_current_rows", from);
        auto fp = make_foreign(std::make_unique<repair_rows_on_wire>(std::move(current_rows)));
        return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, fp = std::move(fp)] () mutable {
            auto rm = get_repair_meta(from, repair_meta_id);
            rm->set_repair_state_for_local_node(repair_state::put_row_diff_with_rpc_stream_started);
            if (fp.get_owner_shard() == this_shard_id()) {
                return rm->put_row_diff_handler(std::move(*fp), from).then([rm] {
                    rm->set_repair_state_for_local_node(repair_state::put_row_diff_with_rpc_stream_finished);
                });
            } else {
                return rm->put_row_diff_handler(*fp, from).then([rm] {
                    rm->set_repair_state_for_local_node(repair_state::put_row_diff_with_rpc_stream_finished);
                });
            }
        }).then([sink] () mutable {
            return sink(repair_stream_cmd::put_rows_done);
        }).then([sink] () mutable {
            return sink.flush();
        }).then([sink] {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    } else {
        return make_exception_future<stop_iteration>(std::runtime_error("Got unexpected repair_stream_cmd"));
    }
}

static future<stop_iteration> repair_get_full_row_hashes_with_rpc_stream_process_op(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_hash_with_cmd> sink,
        rpc::source<repair_stream_cmd> source,
        bool &error,
        std::optional<std::tuple<repair_stream_cmd>> status_opt) {
    repair_stream_cmd status = std::get<0>(status_opt.value());
    rlogger.trace("Got register_repair_get_full_row_hashes_with_rpc_stream from peer={}, status={}", from, int(status));
    if (status == repair_stream_cmd::get_full_row_hashes) {
        return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id] {
            auto rm = get_repair_meta(from, repair_meta_id);
            rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_started);
            return rm->get_full_row_hashes_handler().then([rm] (repair_hash_set hashes) {
                rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_started);
                _metrics.tx_hashes_nr += hashes.size();
                return hashes;
            });
        }).then([sink] (repair_hash_set hashes) mutable {
            return do_with(std::move(hashes), [sink] (repair_hash_set& hashes) mutable {
                return do_for_each(hashes, [sink] (const repair_hash& hash) mutable {
                    return sink(repair_hash_with_cmd{repair_stream_cmd::hash_data, hash});
                }).then([sink] () mutable {
                    return sink(repair_hash_with_cmd{repair_stream_cmd::end_of_current_hash_set, repair_hash()});
                });
            });
        }).then([sink] () mutable {
            return sink.flush();
        }).then([sink] {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    } else {
        return make_exception_future<stop_iteration>(std::runtime_error("Got unexpected repair_stream_cmd"));
    }
}

static future<> repair_get_row_diff_with_rpc_stream_handler(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_row_on_wire_with_cmd> sink,
        rpc::source<repair_hash_with_cmd> source) {
    return do_with(false, repair_hash_set(), [from, src_cpu_id, repair_meta_id, sink, source] (bool& error, repair_hash_set& current_set_diff) mutable {
        return repeat([from, src_cpu_id, repair_meta_id, sink, source, &error, &current_set_diff] () mutable {
            return source().then([from, src_cpu_id, repair_meta_id, sink, source, &error, &current_set_diff] (std::optional<std::tuple<repair_hash_with_cmd>> hash_cmd_opt) mutable {
                if (hash_cmd_opt) {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    return repair_get_row_diff_with_rpc_stream_process_op(from,
                            src_cpu_id,
                            repair_meta_id,
                            sink,
                            source,
                            error,
                            current_set_diff,
                            std::move(hash_cmd_opt)).handle_exception([sink, &error] (std::exception_ptr ep) mutable {
                        error = true;
                        return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::error, repair_row_on_wire()}).then([] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                } else {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
            });
        });
    }).finally([sink] () mutable {
        return sink.close().finally([sink] { });
    });
}

static future<> repair_put_row_diff_with_rpc_stream_handler(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_stream_cmd> sink,
        rpc::source<repair_row_on_wire_with_cmd> source) {
    return do_with(false, repair_rows_on_wire(), [from, src_cpu_id, repair_meta_id, sink, source] (bool& error, repair_rows_on_wire& current_rows) mutable {
        return repeat([from, src_cpu_id, repair_meta_id, sink, source, &current_rows, &error] () mutable {
            return source().then([from, src_cpu_id, repair_meta_id, sink, source, &current_rows, &error] (std::optional<std::tuple<repair_row_on_wire_with_cmd>> row_opt) mutable {
                if (row_opt) {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    return repair_put_row_diff_with_rpc_stream_process_op(from,
                            src_cpu_id,
                            repair_meta_id,
                            sink,
                            source,
                            error,
                            current_rows,
                            std::move(row_opt)).handle_exception([sink, &error] (std::exception_ptr ep) mutable {
                        error = true;
                        return sink(repair_stream_cmd::error).then([] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                } else {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
            });
        });
    }).finally([sink] () mutable {
        return sink.close().finally([sink] { });
    });
}

static future<> repair_get_full_row_hashes_with_rpc_stream_handler(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_hash_with_cmd> sink,
        rpc::source<repair_stream_cmd> source) {
    return repeat([from, src_cpu_id, repair_meta_id, sink, source] () mutable {
        return do_with(false, [from, src_cpu_id, repair_meta_id, sink, source] (bool& error) mutable {
            return source().then([from, src_cpu_id, repair_meta_id, sink, source, &error] (std::optional<std::tuple<repair_stream_cmd>> status_opt) mutable {
                if (status_opt) {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    return repair_get_full_row_hashes_with_rpc_stream_process_op(from,
                            src_cpu_id,
                            repair_meta_id,
                            sink,
                            source,
                            error,
                            std::move(status_opt)).handle_exception([sink, &error] (std::exception_ptr ep) mutable {
                        error = true;
                        return sink(repair_hash_with_cmd{repair_stream_cmd::error, repair_hash()}).then([] () {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                } else {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
            });
        });
    }).finally([sink] () mutable {
        return sink.close().finally([sink] { });
    });
}

// RPC handler
static future<repair_row_level_start_response>
repair_row_level_start_handler(gms::inet_address from, uint32_t src_cpu_id, uint32_t repair_meta_id, sstring ks_name, sstring cf_name,
                               dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size,
                               uint64_t seed, shard_config master_node_shard_config, table_schema_version schema_version, streaming::stream_reason reason) {
    if (!_sys_dist_ks->local_is_initialized() || !_view_update_generator->local_is_initialized()) {
        return make_exception_future<repair_row_level_start_response>(std::runtime_error(format("Node {} is not fully initialized for repair, try again later",
                                                                                                utils::fb_utilities::get_broadcast_address())));
    }
    rlogger.debug(">>> Started Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, schema_version={}, range={}, seed={}, max_row_buf_siz={}",
                  utils::fb_utilities::get_broadcast_address(), from, repair_meta_id, ks_name, cf_name, schema_version, range, seed, max_row_buf_size);
    return insert_repair_meta(from, src_cpu_id, repair_meta_id, std::move(range), algo, max_row_buf_size, seed, std::move(master_node_shard_config), std::move(schema_version), reason).then([] {
        return repair_row_level_start_response{repair_row_level_start_status::ok};
    }).handle_exception_type([] (no_such_column_family&) {
        return repair_row_level_start_response{repair_row_level_start_status::no_such_column_family};
    });
}

// RPC handler
/* static */ future<uint64_t>
repair_get_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id) {
    auto rm = get_repair_meta(from, repair_meta_id);
    rm->set_repair_state_for_local_node(repair_state::get_estimated_partitions_started);
    return rm->get_estimated_partitions().then([rm] (uint64_t partitions) {
        rm->set_repair_state_for_local_node(repair_state::get_estimated_partitions_finished);
        return partitions;
    });
}

// RPC handler
static future<>
repair_row_level_stop_handler(gms::inet_address from, uint32_t repair_meta_id, sstring ks_name, sstring cf_name, dht::token_range range) {
    rlogger.debug("<<< Finished Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, range={}",
                  utils::fb_utilities::get_broadcast_address(), from, repair_meta_id, ks_name, cf_name, range);
    auto rm = get_repair_meta(from, repair_meta_id);
    rm->set_repair_state_for_local_node(repair_state::row_level_stop_started);
    return remove_repair_meta(from, repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range)).then([rm] {
        rm->set_repair_state_for_local_node(repair_state::row_level_stop_finished);
    });
}

// RPC handler
/* static */ future<>
repair_set_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id, uint64_t estimated_partitions) {
    auto rm = get_repair_meta(from, repair_meta_id);
    rm->set_repair_state_for_local_node(repair_state::set_estimated_partitions_started);
    return rm->set_estimated_partitions(estimated_partitions).then([rm] {
        rm->set_repair_state_for_local_node(repair_state::set_estimated_partitions_finished);
    });
}

// RPC handler
future<> row_level_repair_init_messaging_service_handler(repair_service& rs, distributed<db::system_distributed_keyspace>& sys_dist_ks,
        distributed<db::view::view_update_generator>& view_update_generator, sharded<netw::messaging_service>& ms) {
    _sys_dist_ks = &sys_dist_ks;
    _view_update_generator = &view_update_generator;
    _messaging = &ms;
    return ms.invoke_on_all([] (auto& ms) {
        ms.register_repair_get_row_diff_with_rpc_stream([&ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_hash_with_cmd> source) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            auto sink = ms.make_sink_for_repair_get_row_diff_with_rpc_stream(source);
            // Start a new fiber.
            (void)repair_get_row_diff_with_rpc_stream_handler(from, src_cpu_id, repair_meta_id, sink, source).handle_exception(
                    [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
                rlogger.warn("Failed to process get_row_diff_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
            });
            return make_ready_future<rpc::sink<repair_row_on_wire_with_cmd>>(sink);
        });
        ms.register_repair_put_row_diff_with_rpc_stream([&ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_row_on_wire_with_cmd> source) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            auto sink = ms.make_sink_for_repair_put_row_diff_with_rpc_stream(source);
            // Start a new fiber.
            (void)repair_put_row_diff_with_rpc_stream_handler(from, src_cpu_id, repair_meta_id, sink, source).handle_exception(
                    [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
                rlogger.warn("Failed to process put_row_diff_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
            });
            return make_ready_future<rpc::sink<repair_stream_cmd>>(sink);
        });
        ms.register_repair_get_full_row_hashes_with_rpc_stream([&ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_stream_cmd> source) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            auto sink = ms.make_sink_for_repair_get_full_row_hashes_with_rpc_stream(source);
            // Start a new fiber.
            (void)repair_get_full_row_hashes_with_rpc_stream_handler(from, src_cpu_id, repair_meta_id, sink, source).handle_exception(
                    [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
                rlogger.warn("Failed to process get_full_row_hashes_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
            });
            return make_ready_future<rpc::sink<repair_hash_with_cmd>>(sink);
        });
        ms.register_repair_get_full_row_hashes([] (const rpc::client_info& cinfo, uint32_t repair_meta_id) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id] {
                auto rm = get_repair_meta(from, repair_meta_id);
                rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_started);
                return rm->get_full_row_hashes_handler().then([rm] (repair_hash_set hashes) {
                    rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_finished);
                    _metrics.tx_hashes_nr += hashes.size();
                    return hashes;
                });
            }) ;
        });
        ms.register_repair_get_combined_row_hash([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                std::optional<repair_sync_boundary> common_sync_boundary) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id,
                    common_sync_boundary = std::move(common_sync_boundary)] () mutable {
                auto rm = get_repair_meta(from, repair_meta_id);
                _metrics.tx_hashes_nr++;
                rm->set_repair_state_for_local_node(repair_state::get_combined_row_hash_started);
                return rm->get_combined_row_hash_handler(std::move(common_sync_boundary)).then([rm] (get_combined_row_hash_response resp) {
                    rm->set_repair_state_for_local_node(repair_state::get_combined_row_hash_finished);
                    return resp;
                });
            });
        });
        ms.register_repair_get_sync_boundary([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                std::optional<repair_sync_boundary> skipped_sync_boundary) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id,
                    skipped_sync_boundary = std::move(skipped_sync_boundary)] () mutable {
                auto rm = get_repair_meta(from, repair_meta_id);
                rm->set_repair_state_for_local_node(repair_state::get_sync_boundary_started);
                return rm->get_sync_boundary_handler(std::move(skipped_sync_boundary)).then([rm] (get_sync_boundary_response resp) {
                    rm->set_repair_state_for_local_node(repair_state::get_sync_boundary_finished);
                    return resp;
                });
            });
        });
        ms.register_repair_get_row_diff([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                repair_hash_set set_diff, bool needs_all_rows) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            _metrics.rx_hashes_nr += set_diff.size();
            auto fp = make_foreign(std::make_unique<repair_hash_set>(std::move(set_diff)));
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, fp = std::move(fp), needs_all_rows] () mutable {
                auto rm = get_repair_meta(from, repair_meta_id);
                rm->set_repair_state_for_local_node(repair_state::get_row_diff_started);
                if (fp.get_owner_shard() == this_shard_id()) {
                    return rm->get_row_diff_handler(std::move(*fp), repair_meta::needs_all_rows_t(needs_all_rows)).then([rm] (repair_rows_on_wire rows) {
                        rm->set_repair_state_for_local_node(repair_state::get_row_diff_finished);
                        return rows;
                    });
                } else {
                    return rm->get_row_diff_handler(*fp, repair_meta::needs_all_rows_t(needs_all_rows)).then([rm] (repair_rows_on_wire rows) {
                        rm->set_repair_state_for_local_node(repair_state::get_row_diff_finished);
                        return rows;
                    });
                }
            });
        });
        ms.register_repair_put_row_diff([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                repair_rows_on_wire row_diff) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            auto fp = make_foreign(std::make_unique<repair_rows_on_wire>(std::move(row_diff)));
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, fp = std::move(fp)] () mutable {
                auto rm = get_repair_meta(from, repair_meta_id);
                rm->set_repair_state_for_local_node(repair_state::put_row_diff_started);
                if (fp.get_owner_shard() == this_shard_id()) {
                    return rm->put_row_diff_handler(std::move(*fp), from).then([rm] {
                        rm->set_repair_state_for_local_node(repair_state::put_row_diff_finished);
                    });
                } else {
                    return rm->put_row_diff_handler(*fp, from).then([rm] {
                        rm->set_repair_state_for_local_node(repair_state::put_row_diff_finished);
                    });
                }
            });
        });
        ms.register_repair_row_level_start([] (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring ks_name,
                sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed,
                unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, rpc::optional<streaming::stream_reason> reason) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, src_cpu_id, repair_meta_id, ks_name, cf_name,
                    range, algo, max_row_buf_size, seed, remote_shard, remote_shard_count, remote_ignore_msb, schema_version, reason] () mutable {
                streaming::stream_reason r = reason ? *reason : streaming::stream_reason::repair;
                return repair_row_level_start_handler(from, src_cpu_id, repair_meta_id, std::move(ks_name),
                        std::move(cf_name), std::move(range), algo, max_row_buf_size, seed,
                        shard_config{remote_shard, remote_shard_count, remote_ignore_msb},
                        schema_version, r);
            });
        });
        ms.register_repair_row_level_stop([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                sstring ks_name, sstring cf_name, dht::token_range range) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, ks_name, cf_name, range] () mutable {
                return repair_row_level_stop_handler(from, repair_meta_id,
                        std::move(ks_name), std::move(cf_name), std::move(range));
            });
        });
        ms.register_repair_get_estimated_partitions([] (const rpc::client_info& cinfo, uint32_t repair_meta_id) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id] () mutable {
                return repair_get_estimated_partitions_handler(from, repair_meta_id);
            });
        });
        ms.register_repair_set_estimated_partitions([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                uint64_t estimated_partitions) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, estimated_partitions] () mutable {
                return repair_set_estimated_partitions_handler(from, repair_meta_id, estimated_partitions);
            });
        });
        ms.register_repair_get_diff_algorithms([] (const rpc::client_info& cinfo) {
            return make_ready_future<std::vector<row_level_diff_detect_algorithm>>(suportted_diff_detect_algorithms());
        });
    });
}

future<> row_level_repair_uninit_messaging_service_handler() {
    return _messaging->invoke_on_all([] (auto& ms) {
        return when_all_succeed(
            ms.unregister_repair_get_row_diff_with_rpc_stream(),
            ms.unregister_repair_put_row_diff_with_rpc_stream(),
            ms.unregister_repair_get_full_row_hashes_with_rpc_stream(),
            ms.unregister_repair_get_full_row_hashes(),
            ms.unregister_repair_get_combined_row_hash(),
            ms.unregister_repair_get_sync_boundary(),
            ms.unregister_repair_get_row_diff(),
            ms.unregister_repair_put_row_diff(),
            ms.unregister_repair_row_level_start(),
            ms.unregister_repair_row_level_stop(),
            ms.unregister_repair_get_estimated_partitions(),
            ms.unregister_repair_set_estimated_partitions(),
            ms.unregister_repair_get_diff_algorithms()).discard_result();
    });
}

future<> repair_cf_range_row_level(repair_info& ri,
        sstring cf_name, utils::UUID table_id, dht::token_range range,
        const std::vector<gms::inet_address>& all_peer_nodes) {
    return seastar::futurize_invoke([&ri, cf_name = std::move(cf_name), table_id = std::move(table_id), range = std::move(range), &all_peer_nodes] () mutable {
        auto repair = row_level_repair(ri, std::move(cf_name), std::move(table_id), std::move(range), all_peer_nodes);
        return do_with(std::move(repair), [] (row_level_repair& repair) {
            return repair.run();
        });
    });
}

future<> shutdown_all_row_level_repair() {
    return smp::invoke_on_all([] {
        return remove_repair_meta();
    });
}

class row_level_repair_gossip_helper : public gms::i_endpoint_state_change_subscriber {
    void remove_row_level_repair(gms::inet_address node) {
        rlogger.debug("Started to remove row level repair on all shards for node {}", node);
        smp::invoke_on_all([node] {
            return remove_repair_meta(node);
        }).then([node] {
            rlogger.debug("Finished to remove row level repair on all shards for node {}", node);
        }).handle_exception([node] (std::exception_ptr ep) {
            rlogger.warn("Failed to remove row level repair for node {}: {}", node, ep);
        }).get();
    }
    virtual void on_join(
            gms::inet_address endpoint,
            gms::endpoint_state ep_state) override {
    }
    virtual void before_change(
            gms::inet_address endpoint,
            gms::endpoint_state current_state,
            gms::application_state new_state_key,
            const gms::versioned_value& new_value) override {
    }
    virtual void on_change(
            gms::inet_address endpoint,
            gms::application_state state,
            const gms::versioned_value& value) override {
    }
    virtual void on_alive(
            gms::inet_address endpoint,
            gms::endpoint_state state) override {
    }
    virtual void on_dead(
            gms::inet_address endpoint,
            gms::endpoint_state state) override {
        remove_row_level_repair(endpoint);
    }
    virtual void on_remove(
            gms::inet_address endpoint) override {
        remove_row_level_repair(endpoint);
    }
    virtual void on_restart(
            gms::inet_address endpoint,
            gms::endpoint_state ep_state) override {
        remove_row_level_repair(endpoint);
    }
};

repair_service::repair_service(distributed<gms::gossiper>& gossiper, size_t max_repair_memory)
    : _gossiper(gossiper)
    , _gossip_helper(make_shared<row_level_repair_gossip_helper>())
    , _tracker(smp::count, max_repair_memory) {
    _gossiper.local().register_(_gossip_helper);
}

future<> repair_service::stop() {
    return _gossiper.local().unregister_(_gossip_helper).then([this] {
        _stopped = true;
    });
}

repair_service::~repair_service() {
    assert(_stopped);
}
