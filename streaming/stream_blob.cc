/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "message/messaging_service.hh"
#include "streaming/stream_blob.hh"
#include "streaming/stream_plan.hh"
#include "gms/inet_address.hh"
#include "utils/pretty_printers.hh"
#include "utils/error_injection.hh"
#include "locator/host_id.hh"
#include "replica/database.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_version.hh"
#include "sstables/generation_type.hh"
#include "sstables/types.hh"
#include "idl/streaming.dist.hh"
#include "service/topology_guard.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/fstream.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/all.hh>
#include <vector>
#include <cfloat>
#include <filesystem>
#include <fmt/ranges.h>

namespace streaming {

static logging::logger blogger("stream_blob");

constexpr size_t file_stream_buffer_size = 128 * 1024;
constexpr size_t file_stream_write_behind = 10;
constexpr size_t file_stream_read_ahead = 4;

static sstables::sstable_state sstable_state(const streaming::stream_blob_meta& meta) {
    return meta.sstable_state.value_or(sstables::sstable_state::normal);
}

static future<> load_sstable_for_tablet(const file_stream_id& ops_id, replica::database& db, table_id table, sstables::sstable_state state, sstring filename, seastar::shard_id shard) {
    blogger.debug("stream_sstables[{}] Loading sstable {} on shard {}", ops_id, filename, shard);
    auto s = db.find_column_family(table).schema();
    auto data_path = std::filesystem::path(filename);
    auto desc = sstables::parse_path(data_path, s->ks_name(), s->cf_name());
    co_await db.container().invoke_on(shard, [id = s->id(), desc, state] (replica::database& db) -> future<> {
        replica::table& t = db.find_column_family(id);
        auto erm = t.get_effective_replication_map();
        auto& sstm = t.get_sstables_manager();
        auto sst = sstm.make_sstable(t.schema(), t.get_storage_options(), desc.generation, state, desc.version, desc.format);
        co_await sst->load(erm->get_sharder(*t.schema()));
        co_await t.add_sstable_and_update_cache(sst);
    });
    blogger.info("stream_sstables[{}] Loaded sstable {} on shard {} successfully", ops_id, filename, shard);
}

static utils::pretty_printed_throughput get_bw(size_t total_size, std::chrono::steady_clock::time_point start_time) {
    auto duration = std::chrono::steady_clock::now() - start_time;
    return utils::pretty_printed_throughput(total_size, duration);
}

// For tablet stream checks
class tablet_stream_status {
public:
    bool finished = false;
    void check_valid_stream();
};

void tablet_stream_status::check_valid_stream() {
    if (finished) {
        throw std::runtime_error("The stream has finished already");
    }
}

static thread_local std::unordered_map<file_stream_id, lw_shared_ptr<tablet_stream_status>> tablet_streams;

future<> mark_tablet_stream_start(file_stream_id ops_id) {
    return seastar::smp::invoke_on_all([ops_id] {
        auto status = make_lw_shared<tablet_stream_status>();
        tablet_streams.emplace(ops_id, status);
    });
}

future<> mark_tablet_stream_done(file_stream_id ops_id) {
    return seastar::smp::invoke_on_all([ops_id] {
        auto it = tablet_streams.find(ops_id);
        if (it == tablet_streams.end()) {
            return;
        }
        auto status = it->second;
        if (status) {
            status->finished = true;
        }
        tablet_streams.erase(ops_id);
    });
}

lw_shared_ptr<tablet_stream_status> get_tablet_stream(file_stream_id ops_id) {
    auto status = tablet_streams[ops_id];
    if (!status) {
        auto msg = format("stream_sstables[{}] Could not find ops_id={}", ops_id, ops_id);
        blogger.warn("{}", msg);
        throw std::runtime_error(msg);
    }
    return status;
}

static void may_inject_error(const streaming::stream_blob_meta& meta, bool may_inject, const sstring& error) {
    if (may_inject) {
        if (rand() % 500 == 0) {
            auto msg = format("fstream[{}] Injected file stream error={} file={}",
                meta.ops_id, error, meta.filename);
            blogger.warn("{}", msg);
            throw std::runtime_error(msg);
        }
    }
}

future<> stream_blob_handler(replica::database& db,
        netw::messaging_service& ms,
        gms::inet_address from,
        streaming::stream_blob_meta meta,
        rpc::sink<streaming::stream_blob_cmd_data> sink,
        rpc::source<streaming::stream_blob_cmd_data> source,
        stream_blob_create_output_fn create_output,
        bool inject_errors) {
    bool fstream_closed = false;
    bool sink_closed = false;
    bool status_sent = false;
    size_t total_size = 0;
    auto start_time = std::chrono::steady_clock::now();
    std::optional<output_stream<char>> fstream;
    std::exception_ptr error;
    stream_blob_finish_fn finish;

    // Will log a message when streaming is done. Used to synchronize tests.
    lw_shared_ptr<std::any> log_done;
    if (utils::get_local_injector().is_enabled("stream_mutation_fragments")) {
        log_done = make_lw_shared<std::any>(seastar::make_shared(seastar::defer([] {
            blogger.info("stream_mutation_fragments: done (tablets)");
        })));
    }

    try {
        auto status = get_tablet_stream(meta.ops_id);
        auto guard = service::topology_guard(meta.topo_guard);

        // Reject any file_ops that is not support by this node
        if (meta.fops != streaming::file_ops::stream_sstables &&
            meta.fops != streaming::file_ops::load_sstables) {
            auto msg = format("fstream[{}] Unsupported file_ops={} peer={} file={}",
                    meta.ops_id, int(meta.fops), from, meta.filename);
            blogger.warn("{}", msg);
            throw std::runtime_error(msg);
        }

        blogger.debug("fstream[{}] Follower started peer={} file={}",
                meta.ops_id, from, meta.filename);

        auto [f, out] = co_await create_output(db, meta);
        finish = std::move(f);
        fstream = std::move(out);

        bool got_end_of_stream = false;
        for (;;) {
          try {
            auto opt = co_await source();
            if (!opt) {
                break;
            }

            co_await utils::get_local_injector().inject("stream_mutation_fragments", [&guard] (auto& handler) -> future<> {
                blogger.info("stream_mutation_fragments: waiting (tablets)");
                while (!handler.poll_for_message()) {
                    guard.check();
                    co_await sleep(std::chrono::milliseconds(5));
                }
                blogger.info("stream_mutation_fragments: released (tablets)");
            });

            stream_blob_cmd_data& cmd_data = std::get<0>(*opt);
            auto cmd = cmd_data.cmd;
            if (cmd == streaming::stream_blob_cmd::error) {
                blogger.warn("fstream[{}] Follower got stream_blob_cmd::error from peer={} file={}",
                        meta.ops_id, from, meta.filename);
                throw std::runtime_error(format("Got stream_blob_cmd::error from peer={} file={}", from, meta.filename));
            } else if (cmd == streaming::stream_blob_cmd::end_of_stream) {
                blogger.debug("fstream[{}] Follower got stream_blob_cmd::end_of_stream from peer={} file={}",
                        meta.ops_id, from, meta.filename);
                got_end_of_stream = true;
            } else if (cmd == streaming::stream_blob_cmd::data) {
                std::optional<streaming::stream_blob_data> data = std::move(cmd_data.data);
                if (data) {
                    total_size += data->size();
                    blogger.trace("fstream[{}] Follower received data from peer={} data={}", meta.ops_id, from, data->size());
                    status->check_valid_stream();
                    if (!data->empty()) {
                        co_await fstream->write((char*)data->data(), data->size());
                    }
                }
            }
          } catch (seastar::rpc::stream_closed&) {
              // After we get streaming::stream_blob_cmd::end_of_stream which
              // is the last message from peer, it does not matter if the
              // source() is closed or not.
              if (got_end_of_stream) {
                  break;
              } else {
                  throw;
              }
          } catch (...) {
              throw;
          }
            may_inject_error(meta, inject_errors, "rx_data");
        }

        // If we reach here, streaming::stream_blob_cmd::end_of_stream should be received. Otherwise there
        // must be an error, e.g., the sender closed the stream without sending streaming::stream_blob_cmd::error.
        if (!got_end_of_stream) {
            throw std::runtime_error(format("fstream[{}] Follower failed to get end_of_stream", meta.ops_id));
        }

        status->check_valid_stream();
        co_await fstream->flush();
        co_await fstream->close();
        fstream_closed = true;

        may_inject_error(meta, inject_errors, "flush_and_close");

        co_await finish(store_result::ok);

        // Send status code and close the sink
        co_await sink(streaming::stream_blob_cmd_data(streaming::stream_blob_cmd::ok));
        status_sent = true;
        co_await sink.close();
        sink_closed = true;
    } catch (...) {
        error = std::current_exception();
    }
    if (error) {
        blogger.warn("fstream[{}] Follower failed peer={} file={} received_size={} bw={} error={}",
                meta.ops_id, from, meta.filename, total_size, get_bw(total_size, start_time), error);
        if (!fstream_closed) {
            try {
                if (fstream) {
                    // Make sure fstream is always closed
                    co_await fstream->close();
                }
            } catch (...) {
                blogger.warn("fstream[{}] Follower failed to close the file stream: {}",
                        meta.ops_id, std::current_exception());
                // We could do nothing but continue to cleanup more
            }
        }
        if (!status_sent) {
            try {
                may_inject_error(meta, inject_errors, "no_error_code_back");
                co_await sink(streaming::stream_blob_cmd_data(streaming::stream_blob_cmd::error));
            } catch (...) {
                // Try our best to send the status code.
                // If we could not send it, we could do nothing but close the sink.
                blogger.warn("fstream[{}] Follower failed to send error code: {}",
                        meta.ops_id, std::current_exception());
            }
        }
        try {
            if (!sink_closed) {
                // Make sure sink is always closed
                co_await sink.close();
            }
        } catch (...) {
            blogger.warn("fstream[{}] Follower failed to close the stream sink: {}",
                    meta.ops_id, std::current_exception());
        }
        try {
            // Drain everything in source
            for (;;) {
                auto opt = co_await source();
                if (!opt) {
                    break;
                }
            }
        } catch (...) {
            blogger.warn("fstream[{}] Follower failed to drain rpc stream source: {}",
                    meta.ops_id, std::current_exception());
        }

        try {
            // Remove the file in case of error
            if (finish) {
                co_await finish(store_result::failure);
                blogger.info("fstream[{}] Follower removed partial file={}", meta.ops_id, meta.filename);
            }
        } catch (...) {
            blogger.warn("fstream[{}] Follower failed to remove partial file={}: {}",
                    meta.ops_id, meta.filename, std::current_exception());
        }

        // Do not call rethrow_exception(error) because the caller could do nothing but log
        // the error. We have already logged the error here.
    } else {
        // Get some statistics
        blogger.debug("fstream[{}] Follower finished peer={} file={} received_size={} bw={}",
                meta.ops_id, from, meta.filename, total_size, get_bw(total_size, start_time));
    }
    co_return;
}


future<> stream_blob_handler(replica::database& db, netw::messaging_service& ms,
        gms::inet_address from,
        streaming::stream_blob_meta meta,
        rpc::sink<streaming::stream_blob_cmd_data> sink,
        rpc::source<streaming::stream_blob_cmd_data> source) {

    co_await stream_blob_handler(db, ms, std::move(from), meta, std::move(sink), std::move(source), [](replica::database& db, const streaming::stream_blob_meta& meta) -> future<output_result> {
        auto foptions = file_open_options();
        foptions.sloppy_size = true;
        foptions.extent_allocation_size_hint = 32 << 20;

        auto stream_options = file_output_stream_options();
        stream_options.buffer_size = file_stream_buffer_size;
        stream_options.write_behind = file_stream_write_behind;

        auto& table = db.find_column_family(meta.table);
        auto& sstm = table.get_sstables_manager();
        auto sstable_sink = sstables::create_stream_sink(table.schema(), sstm, table.get_storage_options(), sstable_state(meta), meta.filename, meta.fops == file_ops::load_sstables);
        auto out = co_await sstable_sink->output(foptions, stream_options);
        co_return output_result{
            [sstable_sink = std::move(sstable_sink), &meta, &db](store_result res) -> future<> {
                if (res != store_result::ok) {
                    co_await sstable_sink->abort();
                    co_return;
                }
                auto sst = co_await sstable_sink->close_and_seal();
                if (sst) {
                    auto filename = sst->toc_filename();
                    sst = {};
                    co_await load_sstable_for_tablet(meta.ops_id, db, meta.table, sstable_state(meta), std::move(filename), meta.dst_shard_id);
                }
            },
            std::move(out)
        };
    });
}

// Get a new sstable name using the new generation
// For example:
//     oldname: me-3ga1_0iiv_2e5uo2flv7lgdl2j0d-big-Index.db
//      newgen:    3ga1_0iiv_3vj5c2flv7lgdl2j0d
//     newname: me-3ga1_0iiv_3vj5c2flv7lgdl2j0d-big-Index.db
static std::string get_sstable_name_with_generation(const file_stream_id& ops_id, const std::string& oldname, const std::string& newgen) {
    std::string newname = oldname;
    // The generation name starts after the first '-'.
    auto it = newname.find("-");
    if (it != std::string::npos) {
        newname.replace(++it, newgen.size(), newgen);
        return newname;
    } else {
        auto msg = fmt::format("fstream[{}] Failed to get sstable name for {} with generation {}", ops_id, oldname, newgen);
        blogger.warn("{}", msg);
        throw std::runtime_error(msg);
    }
}
}

template<> struct fmt::formatter<streaming::stream_blob_info> : fmt::ostream_formatter {};

namespace streaming {

// Send files in the files list to the nodes in targets list over network
// Returns number of bytes sent over network
future<size_t>
tablet_stream_files(netw::messaging_service& ms, std::list<stream_blob_info> sources, std::vector<node_and_shard> targets, table_id table, file_stream_id ops_id, host2ip_t host2ip, service::frozen_topology_guard topo_guard, bool inject_errors) {
    size_t ops_total_size = 0;
    if (targets.empty()) {
        co_return ops_total_size;
    }
    if (sources.empty()) {
        co_return ops_total_size;
    }

    blogger.debug("fstream[{}] Master started sending n={}, sources={}, targets={}",
            ops_id, sources.size(), sources, targets);

    struct sink_and_source {
        gms::inet_address node;
        rpc::sink<streaming::stream_blob_cmd_data> sink;
        rpc::source<streaming::stream_blob_cmd_data> source;
        bool sink_closed = false;
        bool status_sent = false;
    };

    auto ops_start_time = std::chrono::steady_clock::now();
    streaming::stream_blob_meta meta;
    meta.ops_id = ops_id;
    meta.table = table;
    meta.topo_guard = topo_guard;
    std::exception_ptr error;

    auto stream_options = file_input_stream_options();
    stream_options.buffer_size = file_stream_buffer_size;
    stream_options.read_ahead = file_stream_read_ahead;

    for (auto& info : sources) {
        auto& filename = info.filename;
        std::optional<input_stream<char>> fstream;
        bool fstream_closed = false;
        try {
            meta.fops = info.fops;
            meta.filename = info.filename;
            meta.sstable_state = info.sstable_state;
            fstream = co_await info.source(stream_options);
        } catch (...) {
            blogger.warn("fstream[{}] Master failed sources={} targets={} error={}",
                ops_id, sources, targets, std::current_exception());
            throw;
        }

        std::vector<sink_and_source> ss;
        size_t total_size = 0;
        auto start_time = std::chrono::steady_clock::now();
        bool got_error_from_peer = false;
        try {
            for (auto& x : targets) {
                const auto& node = x.node;
                meta.dst_shard_id = x.shard;
                auto ip = co_await host2ip(node);
                blogger.debug("fstream[{}] Master creating sink and source for node={}/{}, file={}, targets={}", ops_id, node, ip, filename, targets);
                auto [sink, source] = co_await ms.make_sink_and_source_for_stream_blob(meta, node);
                ss.push_back(sink_and_source{ip, std::move(sink), std::move(source)});
            }

            // This fiber sends data to peer node
            auto send_data_to_peer = [&] () mutable -> future<> {
                std::exception_ptr error;
                try {
                    while (!got_error_from_peer) {
                        may_inject_error(meta, inject_errors, "read_data");
                        auto buf = co_await fstream->read_up_to(file_stream_buffer_size);
                        if (buf.size() == 0) {
                            break;
                        }
                        streaming::stream_blob_data data(std::move(buf));
                        auto data_size = data.size();
                        stream_blob_cmd_data cmd_data(streaming::stream_blob_cmd::data, std::move(data));
                        co_await coroutine::parallel_for_each(ss, [&] (sink_and_source& s) mutable -> future<> {
                            total_size += data_size;
                            ops_total_size += data_size;
                            blogger.trace("fstream[{}] Master sending file={} to node={} chunk_size={}",
                                ops_id, filename, s.node, data_size);
                            may_inject_error(meta, inject_errors, "tx_data");
                            co_await s.sink(cmd_data);
                        });
                    }
                } catch (...) {
                    error = std::current_exception();
                }
                if (error) {
                    // We have to close the stream otherwise if the stream is
                    // ok, the get_status_code_from_peer fiber below might
                    // wait for the source() forever.
                    for (auto& s : ss) {
                        try {
                            co_await s.sink.close();
                            s.sink_closed = true;
                        } catch (...) {
                        }
                    }
                    std::rethrow_exception(error);
                }

                if (fstream) {
                    co_await fstream->close();
                    fstream_closed = true;
                }

                for (auto& s : ss) {
                    blogger.debug("fstream[{}] Master done sending file={} to node={}", ops_id, filename, s.node);
                    if (!got_error_from_peer) {
                        co_await s.sink(streaming::stream_blob_cmd_data(streaming::stream_blob_cmd::end_of_stream));
                        s.status_sent = true;
                    }
                    co_await s.sink.close();
                    s.sink_closed = true;
                }
            };

            // This fiber gets status code from peer node
            auto get_status_code_from_peer = [&] () mutable -> future<> {
                co_await coroutine::parallel_for_each(ss, [&] (sink_and_source& s) mutable -> future<> {
                    bool got_cmd_ok = false;
                    while (!got_error_from_peer) {
                      try {
                        auto opt = co_await s.source();
                        if (opt) {
                            stream_blob_cmd_data& cmd_data = std::get<0>(*opt);
                            if (cmd_data.cmd == streaming::stream_blob_cmd::error) {
                                got_error_from_peer = true;
                                blogger.warn("fstream[{}] Master got stream_blob_cmd::error file={} peer={}",
                                        ops_id, filename, s.node);
                                throw std::runtime_error(format("Got stream_blob_cmd::error from peer {}", s.node));
                            } if (cmd_data.cmd == streaming::stream_blob_cmd::ok) {
                                got_cmd_ok = true;
                            }
                            blogger.debug("fstream[{}] Master got stream_blob_cmd={} file={} peer={}",
                                    ops_id, int(cmd_data.cmd), filename, s.node);
                        } else {
                            break;
                        }
                      } catch (seastar::rpc::stream_closed&) {
                          // After we get streaming::stream_blob_cmd::ok
                          // which is the last message from peer, it does not
                          // matter if the source() is closed or not.
                          if (got_cmd_ok) {
                              break;
                          } else {
                              throw;
                          }
                      } catch (...) {
                          throw;
                      }
                    }
                });
            };

            co_await coroutine::all(send_data_to_peer, get_status_code_from_peer);
        } catch (...) {
            error = std::current_exception();
        }
        if (error) {
            blogger.warn("fstream[{}] Master failed sending file={} to targets={} send_size={} bw={} error={}",
                    ops_id, filename, targets, total_size, get_bw(total_size, start_time), error);
            // Error handling for fstream and sink
            if (!fstream_closed) {
                try {
                    if (fstream) {
                        co_await fstream->close();
                    }
                } catch (...) {
                    // We could do nothing but continue to cleanup more
                    blogger.warn("fstream[{}] Master failed to close file stream: {}",
                            ops_id, std::current_exception());
                }
            }
            for (auto& s : ss) {
                try {
                    if (!s.status_sent && !s.sink_closed) {
                        co_await s.sink(streaming::stream_blob_cmd_data(streaming::stream_blob_cmd::error));
                        s.status_sent = true;
                    }
                } catch (...) {
                    // We could do nothing but continue to close
                    blogger.warn("fstream[{}] Master failed to send error code: {}",
                            ops_id, std::current_exception());
                }
                try {
                    if (!s.sink_closed) {
                        co_await s.sink.close();
                        s.sink_closed = true;
                    }
                } catch (...) {
                    // We could do nothing but continue
                    blogger.warn("fstream[{}] Master failed to close rpc stream sink: {}",
                            ops_id, std::current_exception());
                }

                try {
                    // Drain everything in source
                    for (;;) {
                        auto opt = co_await s.source();
                        if (!opt) {
                            break;
                        }
                    }
                } catch (...) {
                    blogger.warn("fstream[{}] Master failed to drain rpc stream source: {}",
                            ops_id, std::current_exception());
                }
            }
            // Stop handling remaining files
            break;
        } else {
            blogger.debug("fstream[{}] Master done sending file={} to targets={} send_size={} bw={}",
                    ops_id, filename, targets, total_size, get_bw(total_size, start_time));
        }
    }
    if (error) {
        blogger.warn("fstream[{}] Master failed sending files_nr={} files={} targets={} send_size={} bw={} error={}",
                ops_id, sources.size(), sources, targets, ops_total_size, get_bw(ops_total_size, ops_start_time), error);
        std::rethrow_exception(error);
    } else {
        blogger.debug("fstream[{}] Master finished sending files_nr={} files={} targets={} send_size={} bw={}",
                ops_id, sources.size(), sources, targets, ops_total_size, get_bw(ops_total_size, ops_start_time));
    }
    co_return ops_total_size;
}


future<stream_files_response> tablet_stream_files_handler(replica::database& db, netw::messaging_service& ms, streaming::stream_files_request req, host2ip_t host2ip) {
    stream_files_response resp;
    auto& table = db.find_column_family(req.table);
    auto sstables = co_await table.take_storage_snapshot(req.range);
    co_await utils::get_local_injector().inject("order_sstables_for_streaming", [&sstables] (auto& handler) -> future<> {
        if (sstables.size() == 3) {
            // make sure the sstables are ordered so that the sstable containing shadowed data is streamed last
            const std::string_view shadowed_file = handler.template get<std::string_view>("shadowed_file").value();
            for (int index: {0, 1}) {
                if (sstables[index].sst->component_basename(component_type::Data) == shadowed_file) {
                    std::swap(sstables[index], sstables[2]);
                }
            }
        }
        return make_ready_future<>();
    });
    auto files = std::list<stream_blob_info>();

    sstables::sstable_generation_generator sst_gen(0);

    for (auto& sst_snapshot : sstables) {
        auto& sst = sst_snapshot.sst;
        // stable state (across files) is a must for load to work on destination
        auto sst_state = sst->state();

        auto sources = create_stream_sources(sst_snapshot);
        auto newgen = fmt::to_string(sst_gen(sstables::uuid_identifiers::yes));

        for (auto&& s : sources) {
            auto oldname = s->component_basename();
            auto newname = get_sstable_name_with_generation(req.ops_id, oldname, newgen);

            blogger.debug("fstream[{}] Get name oldname={}, newname={}", req.ops_id, oldname, newname);

            auto& info = files.emplace_back();
            info.fops = file_ops::stream_sstables;
            info.sstable_state = sst_state;
            info.filename = std::move(newname);
            info.source = [s = std::move(s)](const file_input_stream_options& options) {
                return s->input(options);
            };
        }
        // ensure we mark the end of each component sequence.
        if (!files.empty()) {
            files.back().fops = file_ops::load_sstables;
        }
    }
    if (files.empty()) {
        co_return resp;
    }
    blogger.debug("stream_sstables[{}] Started sending sstable_nr={} files_nr={} files={} range={}",
            req.ops_id, sstables.size(), files.size(), files, req.range);
    auto ops_start_time = std::chrono::steady_clock::now();
    size_t stream_bytes = co_await tablet_stream_files(ms, std::move(files), req.targets, req.table, req.ops_id, std::move(host2ip), req.topo_guard);
    resp.stream_bytes = stream_bytes;
    auto duration = std::chrono::steady_clock::now() - ops_start_time;
    blogger.info("stream_sstables[{}] Finished sending sstable_nr={} files_nr={} files={} range={} stream_bytes={} stream_time={} stream_bw={}",
            req.ops_id, sstables.size(), files.size(), files, req.range, stream_bytes, duration, get_bw(stream_bytes, ops_start_time));
    co_return resp;
}

future<stream_files_response> tablet_stream_files(const file_stream_id& ops_id,
        replica::table& table,
        const dht::token_range& range,
        const locator::host_id& src_host,
        const locator::host_id& dst_host,
        seastar::shard_id dst_shard_id,
        netw::messaging_service& ms,
        abort_source& as,
        service::frozen_topology_guard topo_guard) {
    stream_files_response resp;
    std::exception_ptr error;
    try {
        co_await mark_tablet_stream_start(ops_id);
    } catch (...) {
        error = std::current_exception();
    }
    if (!error) {
        try {
            streaming::stream_files_request req;
            req.ops_id = ops_id;
            req.keyspace_name = table.schema()->ks_name(),
            req.table_name = table.schema()->cf_name();
            req.table = table.schema()->id();
            req.range = range;
            req.targets = std::vector<node_and_shard>{node_and_shard{dst_host, dst_shard_id}};
            req.topo_guard = topo_guard;
            resp = co_await ser::streaming_rpc_verbs::send_tablet_stream_files(&ms, src_host, as, req);
        } catch (...) {
            error = std::current_exception();
        }
    }
    co_await mark_tablet_stream_done(ops_id);
    if (error) {
        std::rethrow_exception(error);
    }
    co_return resp;
}

}
