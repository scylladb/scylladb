/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/rpc/rpc.hh>
#include "sstables_loader.hh"
#include "replica/distributed_loader.hh"
#include "replica/database.hh"
#include "sstables/sstables.hh"
#include "gms/inet_address.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "streaming/stream_reason.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "locator/abstract_replication_strategy.hh"
#include "message/messaging_service.hh"

#include <cfloat>
#include <algorithm>

static logging::logger llog("sstables_loader");

namespace {

class send_meta_data {
    gms::inet_address _node;
    seastar::rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd> _sink;
    seastar::rpc::source<int32_t> _source;
    bool _error_from_peer = false;
    size_t _num_partitions_sent = 0;
    size_t _num_bytes_sent = 0;
    future<> _receive_done;
private:
    future<> do_receive() {
        int32_t status = 0;
        while (auto status_opt = co_await _source()) {
            status = std::get<0>(*status_opt);
            llog.debug("send_meta_data: got error code={}, from node={}", status, _node);
            if (status == -1) {
                _error_from_peer = true;
            }
        }
        llog.debug("send_meta_data: finished reading source from node={}", _node);
        if (_error_from_peer) {
            throw std::runtime_error(format("send_meta_data: got error code={} from node={}", status, _node));
        }
        co_return;
    }
public:
    send_meta_data(gms::inet_address node,
            seastar::rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd> sink,
            seastar::rpc::source<int32_t> source)
        : _node(std::move(node))
        , _sink(std::move(sink))
        , _source(std::move(source))
        , _receive_done(make_ready_future<>()) {
    }
    void receive() {
        _receive_done = do_receive();
    }
    future<> send(const frozen_mutation_fragment& fmf, bool is_partition_start) {
        if (_error_from_peer) {
            throw std::runtime_error(format("send_meta_data: got error from peer node={}", _node));
        }
        auto size = fmf.representation().size();
        if (is_partition_start) {
            ++_num_partitions_sent;
        }
        _num_bytes_sent += size;
        llog.trace("send_meta_data: send mf to node={}, size={}", _node, size);
        co_return co_await _sink(fmf, streaming::stream_mutation_fragments_cmd::mutation_fragment_data);
    }
    future<> finish(bool failed) {
        std::exception_ptr eptr;
        try {
            if (failed) {
                co_await _sink(frozen_mutation_fragment(bytes_ostream()), streaming::stream_mutation_fragments_cmd::error);
            } else {
                co_await _sink(frozen_mutation_fragment(bytes_ostream()), streaming::stream_mutation_fragments_cmd::end_of_stream);
            }
        } catch (...) {
            eptr = std::current_exception();
            llog.warn("send_meta_data: failed to send {} to node={}, err={}",
                    failed ? "stream_mutation_fragments_cmd::error" : "stream_mutation_fragments_cmd::end_of_stream", _node, eptr);
        }
        try {
            co_await _sink.close();
        } catch (...)  {
            eptr = std::current_exception();
            llog.warn("send_meta_data: failed to close sink to node={}, err={}", _node, eptr);
        }
        try {
            co_await std::move(_receive_done);
        } catch (...)  {
            eptr = std::current_exception();
            llog.warn("send_meta_data: failed to process source from node={}, err={}", _node, eptr);
        }
        if (eptr) {
            std::rethrow_exception(eptr);
        }
        co_return;
    }
    size_t num_partitions_sent() {
        return _num_partitions_sent;
    }
    size_t num_bytes_sent() {
        return _num_bytes_sent;
    }
};

} // anonymous namespace

future<> sstables_loader::load_and_stream(sstring ks_name, sstring cf_name,
        ::table_id table_id, std::vector<sstables::shared_sstable> sstables, bool primary_replica_only) {
    const auto full_partition_range = dht::partition_range::make_open_ended_both_sides();
    const auto full_token_range = dht::token_range::make_open_ended_both_sides();
    auto& table = _db.local().find_column_family(table_id);
    auto s = table.schema();
    const auto cf_id = s->id();
    const auto reason = streaming::stream_reason::repair;
    auto erm = _db.local().find_column_family(s).get_effective_replication_map();

    // By sorting SSTables by their primary key, we allow SSTable runs to be
    // incrementally streamed.
    // Overlapping run fragments can have their content deduplicated, reducing
    // the amount of data we need to put on the wire.
    // Elements are popped off from the back of the vector, therefore we're sorting
    // it in descending order, to start from the smaller tokens.
    std::ranges::sort(sstables, [] (const sstables::shared_sstable& x, const sstables::shared_sstable& y) {
        return x->compare_by_first_key(*y) > 0;
    });

    size_t nr_sst_total = sstables.size();
    size_t nr_sst_current = 0;
    while (!sstables.empty()) {
        auto ops_uuid = streaming::plan_id{utils::make_random_uuid()};
        auto sst_set = make_lw_shared<sstables::sstable_set>(sstables::make_partitioned_sstable_set(s, false));
        size_t batch_sst_nr = 16;
        std::vector<sstring> sst_names;
        std::vector<sstables::shared_sstable> sst_processed;
        size_t estimated_partitions = 0;
        while (batch_sst_nr-- && !sstables.empty()) {
            auto sst = sstables.back();
            estimated_partitions += sst->estimated_keys_for_range(full_token_range);
            sst_names.push_back(sst->get_filename());
            sst_set->insert(sst);
            sst_processed.push_back(sst);
            sstables.pop_back();
        }

        llog.info("load_and_stream: started ops_uuid={}, process [{}-{}] out of {} sstables={}",
                ops_uuid, nr_sst_current, nr_sst_current + sst_processed.size(), nr_sst_total, sst_names);

        auto start_time = std::chrono::steady_clock::now();
        inet_address_vector_replica_set current_targets;
        std::unordered_map<gms::inet_address, send_meta_data> metas;
        size_t num_partitions_processed = 0;
        size_t num_bytes_read = 0;
        nr_sst_current += sst_processed.size();
        auto permit = co_await _db.local().obtain_reader_permit(table, "sstables_loader::load_and_stream()", db::no_timeout, {});
        auto reader = mutation_fragment_v1_stream(table.make_streaming_reader(s, std::move(permit), full_partition_range, sst_set, {}));
        std::exception_ptr eptr;
        bool failed = false;
        try {
            netw::messaging_service& ms = _messaging;
            while (auto mf = co_await reader()) {
                bool is_partition_start = mf->is_partition_start();
                if (is_partition_start) {
                    ++num_partitions_processed;
                    auto& start = mf->as_partition_start();
                    const auto& current_dk = start.key();

                    current_targets = erm->get_natural_endpoints(current_dk.token());
                    if (primary_replica_only && current_targets.size() > 1) {
                        current_targets.resize(1);
                    }
                    llog.trace("load_and_stream: ops_uuid={}, current_dk={}, current_targets={}", ops_uuid,
                            current_dk.token(), current_targets);
                    for (auto& node : current_targets) {
                        if (!metas.contains(node)) {
                            auto [sink, source] = co_await ms.make_sink_and_source_for_stream_mutation_fragments(reader.schema()->version(),
                                    ops_uuid, cf_id, estimated_partitions, reason, netw::messaging_service::msg_addr(node));
                            llog.debug("load_and_stream: ops_uuid={}, make sink and source for node={}", ops_uuid, node);
                            metas.emplace(node, send_meta_data(node, std::move(sink), std::move(source)));
                            metas.at(node).receive();
                        }
                    }
                }
                frozen_mutation_fragment fmf = freeze(*s, *mf);
                num_bytes_read += fmf.representation().size();
                co_await coroutine::parallel_for_each(current_targets, [&metas, &fmf, is_partition_start] (const gms::inet_address& node) {
                    return metas.at(node).send(fmf, is_partition_start);
                });
            }
        } catch (...) {
            failed = true;
            eptr = std::current_exception();
            llog.warn("load_and_stream: ops_uuid={}, ks={}, table={}, send_phase, err={}",
                    ops_uuid, ks_name, cf_name, eptr);
        }
        co_await reader.close();
        try {
            co_await coroutine::parallel_for_each(metas.begin(), metas.end(), [failed] (std::pair<const gms::inet_address, send_meta_data>& pair) {
                auto& meta = pair.second;
                return meta.finish(failed);
            });
        } catch (...) {
            failed = true;
            eptr = std::current_exception();
            llog.warn("load_and_stream: ops_uuid={}, ks={}, table={}, finish_phase, err={}",
                    ops_uuid, ks_name, cf_name, eptr);
        }
        if (!failed) {
            try {
                co_await coroutine::parallel_for_each(sst_processed, [&] (sstables::shared_sstable& sst) {
                    llog.debug("load_and_stream: ops_uuid={}, ks={}, table={}, remove sst={}",
                            ops_uuid, ks_name, cf_name, sst->component_filenames());
                    return sst->unlink();
                });
            } catch (...) {
                failed = true;
                eptr = std::current_exception();
                llog.warn("load_and_stream: ops_uuid={}, ks={}, table={}, del_sst_phase, err={}",
                        ops_uuid, ks_name, cf_name, eptr);
            }
        }
        auto duration = std::chrono::duration_cast<std::chrono::duration<float>>(std::chrono::steady_clock::now() - start_time).count();
        for (auto& [node, meta] : metas) {
            llog.info("load_and_stream: ops_uuid={}, ks={}, table={}, target_node={}, num_partitions_sent={}, num_bytes_sent={}",
                    ops_uuid, ks_name, cf_name, node, meta.num_partitions_sent(), meta.num_bytes_sent());
        }
        auto partition_rate = std::fabs(duration) > FLT_EPSILON ? num_partitions_processed / duration : 0;
        auto bytes_rate = std::fabs(duration) > FLT_EPSILON ? num_bytes_read / duration / 1024 / 1024 : 0;
        auto status = failed ? "failed" : "succeeded";
        llog.info("load_and_stream: finished ops_uuid={}, ks={}, table={}, partitions_processed={} partitions, bytes_processed={} bytes, partitions_per_second={} partitions/s, bytes_per_second={} MiB/s, duration={} s, status={}",
                ops_uuid, ks_name, cf_name, num_partitions_processed, num_bytes_read, partition_rate, bytes_rate, duration, status);
        if (failed) {
            std::rethrow_exception(eptr);
        }
    }
    co_return;
}

// For more details, see distributed_loader::process_upload_dir().
// All the global operations are going to happen here, and just the reloading happens
// in there.
future<> sstables_loader::load_new_sstables(sstring ks_name, sstring cf_name,
    bool load_and_stream, bool primary_replica_only) {
    if (_loading_new_sstables) {
        throw std::runtime_error("Already loading SSTables. Try again later");
    } else {
        _loading_new_sstables = true;
    }
    llog.info("Loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}",
            ks_name, cf_name, load_and_stream, primary_replica_only);
    try {
        if (load_and_stream) {
            ::table_id table_id;
            std::vector<std::vector<sstables::shared_sstable>> sstables_on_shards;
            // Load-and-stream reads the entire content from SSTables, therefore it can afford to discard the bloom filter
            // that might otherwise consume a significant amount of memory.
            sstables::sstable_open_config cfg {
                .load_bloom_filter = false,
            };
            std::tie(table_id, sstables_on_shards) = co_await replica::distributed_loader::get_sstables_from_upload_dir(_db, ks_name, cf_name, cfg);
            co_await container().invoke_on_all([&sstables_on_shards, ks_name, cf_name, table_id, primary_replica_only] (sstables_loader& loader) mutable -> future<> {
                co_await loader.load_and_stream(ks_name, cf_name, table_id, std::move(sstables_on_shards[this_shard_id()]), primary_replica_only);
            });
        } else {
            co_await replica::distributed_loader::process_upload_dir(_db, _sys_dist_ks, _view_update_generator, ks_name, cf_name);
        }
    } catch (...) {
        llog.warn("Done loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}, status=failed: {}",
                ks_name, cf_name, load_and_stream, primary_replica_only, std::current_exception());
        _loading_new_sstables = false;
        throw;
    }
    llog.info("Done loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}, status=succeeded",
            ks_name, cf_name, load_and_stream, primary_replica_only);
    _loading_new_sstables = false;
    co_return;
}
