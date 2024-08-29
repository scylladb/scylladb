/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/rpc/rpc.hh>
#include "sstables_loader.hh"
#include "replica/distributed_loader.hh"
#include "replica/database.hh"
#include "sstables/sstables_manager.hh"
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

class sstable_streamer {
protected:
    netw::messaging_service& _ms;
    replica::database& _db;
    replica::table& _table;
    locator::effective_replication_map_ptr _erm;
    std::vector<sstables::shared_sstable> _sstables;
public:
    sstable_streamer(netw::messaging_service& ms, replica::database& db, ::table_id table_id, std::vector<sstables::shared_sstable> sstables)
            : _ms(ms)
            , _db(db)
            , _table(db.find_column_family(table_id))
            , _erm(_table.get_effective_replication_map())
            , _sstables(std::move(sstables)) {
        // By sorting SSTables by their primary key, we allow SSTable runs to be
        // incrementally streamed.
        // Overlapping run fragments can have their content deduplicated, reducing
        // the amount of data we need to put on the wire.
        // Elements are popped off from the back of the vector, therefore we're sorting
        // it in descending order, to start from the smaller tokens.
        std::ranges::sort(_sstables, [] (const sstables::shared_sstable& x, const sstables::shared_sstable& y) {
            return x->compare_by_first_key(*y) > 0;
        });
    }

    virtual ~sstable_streamer() {}

    virtual future<> stream(bool primary_replica_only);
    inet_address_vector_replica_set get_endpoints(const dht::token& token, bool primary_replica_only) const;
protected:
    virtual inet_address_vector_replica_set get_primary_endpoints(const dht::token& token) const;
    future<> stream_sstables(const dht::partition_range&, std::vector<sstables::shared_sstable>, bool primary_replica_only);
    future<> stream_sstable_mutations(const dht::partition_range&, std::vector<sstables::shared_sstable>, bool primary_replica_only);
};

class tablet_sstable_streamer : public sstable_streamer {
    const locator::tablet_map& _tablet_map;
public:
    tablet_sstable_streamer(netw::messaging_service& ms, replica::database& db, ::table_id table_id, std::vector<sstables::shared_sstable> sstables)
        : sstable_streamer(ms, db, table_id, std::move(sstables))
        , _tablet_map(_erm->get_token_metadata().tablets().get_tablet_map(table_id)) {
    }

    inet_address_vector_replica_set to_replica_set(const locator::tablet_replica_set& replicas) const {
        auto& tm = _erm->get_token_metadata();
        inet_address_vector_replica_set result;
        result.reserve(replicas.size());
        for (auto&& replica : replicas) {
            result.push_back(tm.get_endpoint_for_host_id(replica.host));
        }
        return result;
    }

    virtual future<> stream(bool primary_replica_only) override;
    virtual inet_address_vector_replica_set get_primary_endpoints(const dht::token& token) const override;
private:
    future<> stream_fully_contained_sstables(const dht::partition_range& pr, std::vector<sstables::shared_sstable> sstables,
                                             bool primary_replica_only) {
        // FIXME: fully contained sstables can be optimized.
        return stream_sstables(pr, std::move(sstables), primary_replica_only);
    }
};

inet_address_vector_replica_set sstable_streamer::get_endpoints(const dht::token& token, bool primary_replica_only) const {
    if (primary_replica_only) {
        return get_primary_endpoints(token);
    }
    auto current_targets = _erm->get_natural_endpoints_without_node_being_replaced(token);
    auto pending = _erm->get_pending_endpoints(token);
    std::move(pending.begin(), pending.end(), std::back_inserter(current_targets));
    return current_targets;
}

inet_address_vector_replica_set sstable_streamer::get_primary_endpoints(const dht::token& token) const {
    auto current_targets = _erm->get_natural_endpoints(token);
    current_targets.resize(1);
    return current_targets;
}

inet_address_vector_replica_set tablet_sstable_streamer::get_primary_endpoints(const dht::token& token) const {
    auto tid = _tablet_map.get_tablet_id(token);
    auto replicas = locator::get_primary_replicas(_tablet_map.get_tablet_info(tid), _tablet_map.get_tablet_transition_info(tid));
    return to_replica_set(replicas);
}

future<> sstable_streamer::stream(bool primary_replica_only) {
    const auto full_partition_range = dht::partition_range::make_open_ended_both_sides();

    co_await stream_sstables(full_partition_range, std::move(_sstables), primary_replica_only);
}

future<> tablet_sstable_streamer::stream(bool primary_replica_only) {
    // sstables are sorted by first key in reverse order.
    auto sstable_it = _sstables.rbegin();

    for (auto tablet_id : _tablet_map.tablet_ids()) {
        auto tablet_range = _tablet_map.get_token_range(tablet_id);

        auto sstable_token_range = [] (const sstables::shared_sstable& sst) {
            return dht::token_range(sst->get_first_decorated_key().token(),
                                    sst->get_last_decorated_key().token());
        };

        std::vector<sstables::shared_sstable> sstables_fully_contained;
        std::vector<sstables::shared_sstable> sstables_partially_contained;

        // sstable is exhausted if its last key is before the current tablet range
        auto exhausted = [&tablet_range] (const sstables::shared_sstable& sst) {
            return tablet_range.before(sst->get_last_decorated_key().token(), dht::token_comparator{});
        };
        while (sstable_it != _sstables.rend() && exhausted(*sstable_it)) {
            sstable_it++;
        }

        for (auto sst_it = sstable_it; sst_it != _sstables.rend(); sst_it++) {
            auto sst_token_range = sstable_token_range(*sst_it);
            // sstables are sorted by first key, so we're done with current tablet when
            // the next sstable doesn't overlap with its owned token range.
            if (!tablet_range.overlaps(sst_token_range, dht::token_comparator{})) {
                break;
            }

            if (tablet_range.contains(sst_token_range, dht::token_comparator{})) {
                sstables_fully_contained.push_back(*sst_it);
            } else {
                sstables_partially_contained.push_back(*sst_it);
            }
            co_await coroutine::maybe_yield();
        }

        auto tablet_pr = dht::to_partition_range(tablet_range);
        co_await stream_sstables(tablet_pr, std::move(sstables_partially_contained), primary_replica_only);
        co_await stream_fully_contained_sstables(tablet_pr, std::move(sstables_fully_contained), primary_replica_only);
    }
}

future<> sstable_streamer::stream_sstables(const dht::partition_range& pr, std::vector<sstables::shared_sstable> sstables,
                                           bool primary_replica_only) {
    while (!sstables.empty()) {
        size_t batch_sst_nr = 16;
        std::vector<sstables::shared_sstable> sst_processed;
        sst_processed.reserve(std::min(sstables.size(), size_t(16)));
        while (batch_sst_nr-- && !sstables.empty()) {
            auto sst = sstables.back();
            sst_processed.push_back(sst);
            sstables.pop_back();
        }

        co_await stream_sstable_mutations(pr, std::move(sst_processed), primary_replica_only);
    }
}

future<> sstable_streamer::stream_sstable_mutations(const dht::partition_range& pr, std::vector<sstables::shared_sstable> sstables,
                                                    bool primary_replica_only) {
    const auto token_range = pr.transform(std::mem_fn(&dht::ring_position::token));
    auto s = _table.schema();
    const auto cf_id = s->id();
    const auto reason = streaming::stream_reason::repair;

    size_t nr_sst_total = _sstables.size();
    size_t nr_sst_current = 0;

        // FIXME: indentation
        auto ops_uuid = streaming::plan_id{utils::make_random_uuid()};
        auto sst_set = make_lw_shared<sstables::sstable_set>(sstables::make_partitioned_sstable_set(s, false));
        size_t estimated_partitions = 0;
        for (auto& sst : sstables) {
            estimated_partitions += sst->estimated_keys_for_range(token_range);
            sst_set->insert(sst);
        }

        llog.info("load_and_stream: started ops_uuid={}, process [{}-{}] out of {} sstables=[{}]",
                ops_uuid, nr_sst_current, nr_sst_current + sstables.size(), nr_sst_total,
                fmt::join(sstables | boost::adaptors::transformed([] (auto sst) { return sst->get_filename(); }), ", "));

        auto start_time = std::chrono::steady_clock::now();
        inet_address_vector_replica_set current_targets;
        std::unordered_map<gms::inet_address, send_meta_data> metas;
        size_t num_partitions_processed = 0;
        size_t num_bytes_read = 0;
        nr_sst_current += sstables.size();
        auto permit = co_await _db.obtain_reader_permit(_table, "sstables_loader::load_and_stream()", db::no_timeout, {});
        auto reader = mutation_fragment_v1_stream(_table.make_streaming_reader(s, std::move(permit), pr, sst_set, gc_clock::now()));
        std::exception_ptr eptr;
        bool failed = false;

        try {
            while (auto mf = co_await reader()) {
                bool is_partition_start = mf->is_partition_start();
                if (is_partition_start) {
                    ++num_partitions_processed;
                    auto& start = mf->as_partition_start();
                    const auto& current_dk = start.key();

                    current_targets = get_endpoints(current_dk.token(), primary_replica_only);
                    llog.trace("load_and_stream: ops_uuid={}, current_dk={}, current_targets={}", ops_uuid,
                            current_dk.token(), current_targets);
                    for (auto& node : current_targets) {
                        if (!metas.contains(node)) {
                            auto [sink, source] = co_await _ms.make_sink_and_source_for_stream_mutation_fragments(reader.schema()->version(),
                                    ops_uuid, cf_id, estimated_partitions, reason, service::default_session_id, netw::messaging_service::msg_addr(node));
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
                    ops_uuid, s->ks_name(), s->cf_name(), eptr);
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
                    ops_uuid, s->ks_name(), s->cf_name(), eptr);
        }
        if (!failed) {
            try {
                co_await coroutine::parallel_for_each(sstables, [&] (sstables::shared_sstable& sst) {
                    llog.debug("load_and_stream: ops_uuid={}, ks={}, table={}, remove sst={}",
                            ops_uuid, s->ks_name(), s->cf_name(), sst->component_filenames());
                    return sst->unlink();
                });
            } catch (...) {
                failed = true;
                eptr = std::current_exception();
                llog.warn("load_and_stream: ops_uuid={}, ks={}, table={}, del_sst_phase, err={}",
                        ops_uuid, s->ks_name(), s->cf_name(), eptr);
            }
        }
        auto duration = std::chrono::duration_cast<std::chrono::duration<float>>(std::chrono::steady_clock::now() - start_time).count();
        for (auto& [node, meta] : metas) {
            llog.info("load_and_stream: ops_uuid={}, ks={}, table={}, target_node={}, num_partitions_sent={}, num_bytes_sent={}",
                    ops_uuid, s->ks_name(), s->cf_name(), node, meta.num_partitions_sent(), meta.num_bytes_sent());
        }
        auto partition_rate = std::fabs(duration) > FLT_EPSILON ? num_partitions_processed / duration : 0;
        auto bytes_rate = std::fabs(duration) > FLT_EPSILON ? num_bytes_read / duration / 1024 / 1024 : 0;
        auto status = failed ? "failed" : "succeeded";
        llog.info("load_and_stream: finished ops_uuid={}, ks={}, table={}, partitions_processed={} partitions, bytes_processed={} bytes, partitions_per_second={} partitions/s, bytes_per_second={} MiB/s, duration={} s, status={}",
                ops_uuid, s->ks_name(), s->cf_name(), num_partitions_processed, num_bytes_read, partition_rate, bytes_rate, duration, status);
        if (failed) {
            std::rethrow_exception(eptr);
        }

    co_return;
}

template <typename... Args>
static std::unique_ptr<sstable_streamer> make_sstable_streamer(bool uses_tablets, Args&&... args) {
    if (uses_tablets) {
        return std::make_unique<tablet_sstable_streamer>(std::forward<Args>(args)...);
    }
    return std::make_unique<sstable_streamer>(std::forward<Args>(args)...);
}

future<> sstables_loader::load_and_stream(sstring ks_name, sstring cf_name,
        ::table_id table_id, std::vector<sstables::shared_sstable> sstables, bool primary_replica_only) {
    // streamer guarantees topology stability, for correctness, by holding effective_replication_map
    // throughout its lifetime.
    auto streamer = make_sstable_streamer(_db.local().find_column_family(table_id).uses_tablets(),
                                          _messaging, _db.local(), table_id, std::move(sstables));

    co_await streamer->stream(primary_replica_only);
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

    co_await coroutine::switch_to(_sched_group);

    sstring load_and_stream_desc = fmt::format("{}", load_and_stream);
    const auto& rs = _db.local().find_keyspace(ks_name).get_replication_strategy();
    if (rs.is_per_table() && !load_and_stream) {
        load_and_stream = true;
        load_and_stream_desc = "auto-enabled-for-tablets";
    }

    llog.info("Loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}",
            ks_name, cf_name, load_and_stream_desc, primary_replica_only);
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
            co_await replica::distributed_loader::process_upload_dir(_db, _view_builder, ks_name, cf_name);
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

class sstables_loader::download_task_impl : public tasks::task_manager::task::impl {
    sharded<sstables_loader>& _loader;
    sstring _endpoint;
    sstring _bucket;
    sstring _ks;
    sstring _cf;
    sstring _snapshot_name;

protected:
    virtual future<> run() override;

public:
    download_task_impl(tasks::task_manager::module_ptr module, sharded<sstables_loader>& loader,
            sstring endpoint, sstring bucket,
            sstring ks, sstring cf, sstring snapshot) noexcept
        : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
        , _loader(loader)
        , _endpoint(std::move(endpoint))
        , _bucket(std::move(bucket))
        , _ks(std::move(ks))
        , _cf(std::move(cf))
        , _snapshot_name(std::move(snapshot))
    {}

    virtual std::string type() const override {
        return "download_sstables";
    }

    virtual tasks::is_internal is_internal() const noexcept override {
        return tasks::is_internal::no;
    }
};

future<> sstables_loader::download_task_impl::run() {
    // Load-and-stream reads the entire content from SSTables, therefore it can afford to discard the bloom filter
    // that might otherwise consume a significant amount of memory.
    sstables::sstable_open_config cfg {
        .load_bloom_filter = false,
    };
    auto prefix = format("{}/{}", _cf, _snapshot_name);
    llog.debug("Loading sstables from {}({}/{})", _endpoint, _bucket, prefix);
    auto [ table_id, sstables_on_shards ] = co_await replica::distributed_loader::get_sstables_from_object_store(_loader.local()._db, _ks, _cf, _endpoint, _bucket, prefix, cfg);
    llog.debug("Streaming sstables from {}({}/{})", _endpoint, _bucket, prefix);
    co_await _loader.invoke_on_all([this, &sstables_on_shards, table_id] (sstables_loader& loader) mutable -> future<> {
        co_await loader.load_and_stream(_ks, _cf, table_id, std::move(sstables_on_shards[this_shard_id()]), false);
    });
}

sstables_loader::sstables_loader(sharded<replica::database>& db,
        netw::messaging_service& messaging,
        sharded<db::view::view_builder>& vb,
        tasks::task_manager& tm,
        sstables::storage_manager& sstm,
        seastar::scheduling_group sg)
    : _db(db)
    , _messaging(messaging)
    , _view_builder(vb)
    , _task_manager_module(make_shared<task_manager_module>(tm))
    , _storage_manager(sstm)
    , _sched_group(std::move(sg))
{
    tm.register_module("sstables_loader", _task_manager_module);
}

future<> sstables_loader::stop() {
    co_await _task_manager_module->stop();
}

future<tasks::task_id> sstables_loader::download_new_sstables(sstring ks_name, sstring cf_name,
            sstring endpoint, sstring bucket, sstring snapshot) {
    if (!_storage_manager.is_known_endpoint(endpoint)) {
        throw std::invalid_argument(format("endpoint {} not found", endpoint));
    }
    llog.info("Restore sstables from {}({}) to {}", endpoint, snapshot, ks_name);
    auto task = co_await _task_manager_module->make_and_start_task<download_task_impl>({}, container(), std::move(endpoint), std::move(bucket), std::move(ks_name), std::move(cf_name), std::move(snapshot));
    co_return task->id();
}
