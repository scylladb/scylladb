/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <exception>
#include <fmt/ranges.h>
#include <seastar/util/defer.hh>
#include "gms/endpoint_state.hh"
#include "repair/repair.hh"
#include "message/messaging_service.hh"
#include "repair/task_manager_module.hh"
#include <seastar/coroutine/exception.hh>
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "mutation/mutation_fragment.hh"
#include "mutation_writer/multishard_writer.hh"
#include "dht/i_partitioner.hh"
#include "dht/sharder.hh"
#include "utils/assert.hh"
#include "utils/xx_hasher.hh"
#include "utils/UUID.hh"
#include "replica/database.hh"
#include <seastar/util/bool_class.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <list>
#include <vector>
#include <algorithm>
#include <random>
#include <optional>
#include <boost/range/adaptors.hpp>
#include <boost/intrusive/list.hpp>
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "gms/gossiper.hh"
#include "repair/row_level.hh"
#include "utils/stall_free.hh"
#include "utils/to_string.hh"
#include "service/migration_manager.hh"
#include "streaming/consumer.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/all.hh>
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "service/storage_proxy.hh"
#include "service/raft/raft_address_map.hh"
#include "db/batchlog_manager.hh"
#include "idl/partition_checksum.dist.hh"
#include "readers/empty_v2.hh"
#include "readers/evictable.hh"
#include "readers/queue.hh"
#include "readers/filtering.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "repair/hash.hh"
#include "repair/decorated_key_with_hash.hh"
#include "repair/row.hh"
#include "repair/writer.hh"
#include "repair/reader.hh"
#include "compaction/compaction_manager.hh"
#include "utils/xx_hasher.hh"

extern logging::logger rlogger;

static bool inject_rpc_stream_error = false;

static shard_id get_dst_shard_id(uint32_t src_cpu_id, const rpc::optional<shard_id>& dst_cpu_id_opt) {
    uint32_t dst_cpu_id = 0;
    if (dst_cpu_id_opt && *dst_cpu_id_opt != repair_unspecified_shard) {
        dst_cpu_id = *dst_cpu_id_opt;
    } else {
        dst_cpu_id = src_cpu_id % smp::count;
    }
    return dst_cpu_id;
}

enum class repair_state : uint16_t {
    unknown,
    row_level_start_started,
    row_level_start_finished,
    get_estimated_partitions_started,
    get_estimated_partitions_finished,
    set_estimated_partitions_started,
    set_estimated_partitions_finished,
    get_sync_boundary_started,
    get_sync_boundary_finished,
    get_combined_row_hash_started,
    get_combined_row_hash_finished,
    get_row_diff_with_rpc_stream_started,
    get_row_diff_with_rpc_stream_finished,
    get_row_diff_and_update_peer_row_hash_sets_started,
    get_row_diff_and_update_peer_row_hash_sets_finished,
    get_full_row_hashes_with_rpc_stream_started,
    get_full_row_hashes_with_rpc_stream_finished,
    get_full_row_hashes_started,
    get_full_row_hashes_finished,
    get_row_diff_started,
    get_row_diff_finished,
    put_row_diff_with_rpc_stream_started,
    put_row_diff_with_rpc_stream_finished,
    put_row_diff_started,
    put_row_diff_finished,
    row_level_stop_started,
    row_level_stop_finished,
};

struct repair_node_state {
    gms::inet_address node;
    repair_state state = repair_state::unknown;
    // The shard that repair instance runs on
    shard_id shard;
    explicit repair_node_state(gms::inet_address n) : node(n) { }
    explicit repair_node_state(gms::inet_address n, shard_id s) : node(n), shard(s) { }
};

// Wraps sink and source objects for repair master or repair follower nodes.
// For repair master, it stores sink and source pair for each of the followers.
// For repair follower, it stores one sink and source pair for repair master.
template<class SinkType, class SourceType>
class sink_source_for_repair {
    uint32_t _repair_meta_id;
    using get_sink_source_fn_type = std::function<future<std::tuple<rpc::sink<SinkType>, rpc::source<SourceType>>> (uint32_t repair_meta_id, std::optional<shard_id> dst_cpu_id, netw::messaging_service::msg_addr addr)>;
    using sink_type  = std::reference_wrapper<rpc::sink<SinkType>>;
    using source_type = std::reference_wrapper<rpc::source<SourceType>>;
    // The vectors below store sink and source object for peer nodes.
    std::vector<std::optional<rpc::sink<SinkType>>> _sinks;
    std::vector<std::optional<rpc::source<SourceType>>> _sources;
    std::vector<bool> _sources_closed;
    get_sink_source_fn_type _fn;
public:
    sink_source_for_repair(uint32_t repair_meta_id, size_t nr_peer_nodes, get_sink_source_fn_type fn)
        : _repair_meta_id(repair_meta_id)
        , _sinks(nr_peer_nodes)
        , _sources(nr_peer_nodes)
        , _sources_closed(nr_peer_nodes, false)
        , _fn(std::move(fn)) {
    }
    void mark_source_closed(unsigned node_idx) {
        _sources_closed[node_idx] = true;
    }
    future<std::tuple<sink_type, source_type>> get_sink_source(gms::inet_address remote_node, unsigned node_idx, std::optional<shard_id> dst_cpu_id) {
        using value_type = std::tuple<sink_type, source_type>;
        if (_sinks[node_idx] && _sources[node_idx]) {
            return make_ready_future<value_type>(value_type(_sinks[node_idx].value(), _sources[node_idx].value()));
        }
        if (_sinks[node_idx] || _sources[node_idx]) {
            return make_exception_future<value_type>(std::runtime_error(format("sink or source is missing for node {}", remote_node)));
        }
        return _fn(_repair_meta_id, dst_cpu_id, netw::messaging_service::msg_addr(remote_node)).then_unpack([this, node_idx] (rpc::sink<SinkType> sink, rpc::source<SourceType> source) mutable {
            _sinks[node_idx].emplace(std::move(sink));
            _sources[node_idx].emplace(std::move(source));
            return make_ready_future<value_type>(value_type(_sinks[node_idx].value(), _sources[node_idx].value()));
        });
    }
    future<> close() {
        return parallel_for_each(boost::irange(unsigned(0), unsigned(_sources.size())), [this] (unsigned node_idx) mutable {
            std::optional<rpc::sink<SinkType>>& sink_opt = _sinks[node_idx];
            auto f = sink_opt ? sink_opt->close() : make_ready_future<>();
            return f.finally([this, node_idx] {
                std::optional<rpc::source<SourceType>>& source_opt = _sources[node_idx];
                if (source_opt && !_sources_closed[node_idx]) {
                    return repeat([&source_opt] () mutable {
                        // Keep reading source until end of stream
                        return (*source_opt)().then([] (std::optional<std::tuple<SourceType>> opt) mutable {
                            if (opt) {
                                return make_ready_future<stop_iteration>(stop_iteration::no);
                            } else {
                                return make_ready_future<stop_iteration>(stop_iteration::yes);
                            }
                        }).handle_exception([] (std::exception_ptr ep) {
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        });
                    });
                }
                return make_ready_future<>();
            });
        });
    }
};

using sink_source_for_get_full_row_hashes = sink_source_for_repair<repair_stream_cmd, repair_hash_with_cmd>;
using sink_source_for_get_row_diff = sink_source_for_repair<repair_hash_with_cmd, repair_row_on_wire_with_cmd>;
using sink_source_for_put_row_diff = sink_source_for_repair<repair_row_on_wire_with_cmd, repair_stream_cmd>;

struct row_level_repair_metrics {
    seastar::metrics::metric_groups _metrics;
    uint64_t tx_row_nr{0};
    uint64_t rx_row_nr{0};
    uint64_t tx_row_bytes{0};
    uint64_t rx_row_bytes{0};
    uint64_t row_from_disk_nr{0};
    uint64_t row_from_disk_bytes{0};
    uint64_t tx_hashes_nr{0};
    uint64_t rx_hashes_nr{0};
    row_level_repair_metrics() {
        namespace sm = seastar::metrics;
        _metrics.add_group("repair", {
            sm::make_counter("tx_row_nr", tx_row_nr,
                            sm::description("Total number of rows sent on this shard.")),
            sm::make_counter("rx_row_nr", rx_row_nr,
                            sm::description("Total number of rows received on this shard.")),
            sm::make_counter("tx_row_bytes", tx_row_bytes,
                            sm::description("Total bytes of rows sent on this shard.")),
            sm::make_counter("rx_row_bytes", rx_row_bytes,
                            sm::description("Total bytes of rows received on this shard.")),
            sm::make_counter("tx_hashes_nr", tx_hashes_nr,
                            sm::description("Total number of row hashes sent on this shard.")),
            sm::make_counter("rx_hashes_nr", rx_hashes_nr,
                            sm::description("Total number of row hashes received on this shard.")),
            sm::make_counter("row_from_disk_nr", row_from_disk_nr,
                            sm::description("Total number of rows read from disk on this shard.")),
            sm::make_counter("row_from_disk_bytes", row_from_disk_bytes,
                            sm::description("Total bytes of rows read from disk on this shard.")),
        });
    }
};

static thread_local row_level_repair_metrics _metrics;

static const std::vector<row_level_diff_detect_algorithm>& suportted_diff_detect_algorithms() {
    static std::vector<row_level_diff_detect_algorithm> _algorithms = {
        row_level_diff_detect_algorithm::send_full_set,
        row_level_diff_detect_algorithm::send_full_set_rpc_stream,
    };
    return _algorithms;
};

static row_level_diff_detect_algorithm get_common_diff_detect_algorithm(netw::messaging_service& ms, const inet_address_vector_replica_set& nodes) {
    std::vector<std::vector<row_level_diff_detect_algorithm>> nodes_algorithms(nodes.size());
    parallel_for_each(boost::irange(size_t(0), nodes.size()), [&ms, &nodes_algorithms, &nodes] (size_t idx) {
        return ms.send_repair_get_diff_algorithms(netw::messaging_service::msg_addr(nodes[idx])).then(
                [&nodes_algorithms, &nodes, idx] (std::vector<row_level_diff_detect_algorithm> algorithms) {
            std::sort(algorithms.begin(), algorithms.end());
            nodes_algorithms[idx] = std::move(algorithms);
            rlogger.trace("Got node_algorithms={}, from node={}", nodes_algorithms[idx], nodes[idx]);
        });
    }).get();

    auto common_algorithms = suportted_diff_detect_algorithms();
    for (auto& algorithms : nodes_algorithms) {
        std::sort(common_algorithms.begin(), common_algorithms.end());
        std::vector<row_level_diff_detect_algorithm> results;
        std::set_intersection(algorithms.begin(), algorithms.end(),
                common_algorithms.begin(), common_algorithms.end(),
                std::back_inserter(results));
        common_algorithms = std::move(results);
    }
    rlogger.trace("peer_algorithms={}, local_algorithms={}, common_diff_detect_algorithms={}",
            nodes_algorithms, suportted_diff_detect_algorithms(), common_algorithms);
    if (common_algorithms.empty()) {
        throw std::runtime_error("Can not find row level repair diff detect algorithm");
    }
    return common_algorithms.back();
}

static bool is_rpc_stream_supported(row_level_diff_detect_algorithm algo) {
    // send_full_set is the only algorithm that does not support rpc stream
    return algo != row_level_diff_detect_algorithm::send_full_set;
}

static uint64_t get_random_seed() {
    static thread_local std::default_random_engine random_engine{std::random_device{}()};
    static thread_local std::uniform_int_distribution<uint64_t> random_dist{};
    return random_dist(random_engine);
}

repair_hash repair_hasher::do_hash_for_mf(const decorated_key_with_hash& dk_with_hash, const mutation_fragment& mf) {
    xx_hasher h(_seed);
    feed_hash(h, mf, *_schema);
    feed_hash(h, dk_with_hash.hash.hash);
    return repair_hash(h.finalize_uint64());
}

mutation_reader repair_reader::make_reader(
    seastar::sharded<replica::database>& db,
    replica::column_family& cf,
    read_strategy strategy,
    const dht::sharder& remote_sharder,
    unsigned remote_shard,
    gc_clock::time_point compaction_time) {
    switch (strategy) {
        case read_strategy::local: {
            auto ms = mutation_source([&cf, compaction_time] (
                schema_ptr s,
                reader_permit permit,
                const dht::partition_range& pr,
                const query::partition_slice& ps,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding,
                mutation_reader::forwarding fwd_mr) {
                return cf.make_streaming_reader(std::move(s), std::move(permit), pr, ps, fwd_mr, compaction_time);
            });
            mutation_reader rd(nullptr);
            std::tie(rd, _reader_handle) = make_manually_paused_evictable_reader_v2(
                std::move(ms),
                _schema,
                _permit,
                _range,
                _schema->full_slice(),
                {},
                mutation_reader::forwarding::no);
            return rd;
        }
        case read_strategy::multishard_split: {
            // We can't have two permits with count resource for 1 repair.
            // So we release the one on _permit so the only one is the one the
            // shard reader will obtain.
            _permit.release_base_resources();
            return make_multishard_streaming_reader(db, _schema, _permit, [this] {
                auto shard_range = _sharder.next();
                if (shard_range) {
                    return std::optional<dht::partition_range>(dht::to_partition_range(*shard_range));
                }
                return std::optional<dht::partition_range>();
            }, compaction_time);
        }
        case read_strategy::multishard_filter: {
            // We can't have two permits with count resource for 1 repair.
            // So we release the one on _permit so the only one is the one the
            // shard reader will obtain.
            _permit.release_base_resources();
            return make_filtering_reader(make_multishard_streaming_reader(db, _schema, _permit, _range, compaction_time),
                [&remote_sharder, remote_shard](const dht::decorated_key& k) {
                    return remote_sharder.shard_for_reads(k.token()) == remote_shard;
                });
        }
        default:
            on_internal_error(rlogger,
                format("make_reader: unexpected read_strategy {}", static_cast<int>(strategy)));
    }
}

repair_reader::repair_reader(
    seastar::sharded<replica::database>& db,
    replica::column_family& cf,
    schema_ptr s,
    reader_permit permit,
    dht::token_range range,
    const dht::static_sharder& remote_sharder,
    unsigned remote_shard,
    uint64_t seed,
    read_strategy strategy,
    gc_clock::time_point compaction_time)
    : _schema(s)
    , _permit(std::move(permit))
    , _range(dht::to_partition_range(range))
    , _sharder(remote_sharder, range, remote_shard)
    , _seed(seed)
    , _local_read_op(strategy == read_strategy::local ? std::optional(cf.read_in_progress()) : std::nullopt)
    , _reader(make_reader(db, cf, strategy, remote_sharder, remote_shard, compaction_time))
{ }

future<mutation_fragment_opt>
repair_reader::read_mutation_fragment() {
    ++_reads_issued;
    // Use a very long timeout for the reader to break out any eventual
    // deadlock within the reader. Thirty minutes should be more than
    // enough to read a single mutation fragment.
    auto timeout = db::timeout_clock::now() + std::chrono::minutes(30);
    _reader.set_timeout(timeout);   // reset to db::no_timeout in pause()
    return _reader().then_wrapped([this] (future<mutation_fragment_opt> f) {
        try {
            auto mfopt = f.get();
            ++_reads_finished;
            return mfopt;
        } catch (seastar::timed_out_error& e) {
            rlogger.warn("Failed to read a fragment from the reader, keyspace={}, table={}, range={}: {}",
                _schema->ks_name(), _schema->cf_name(), _range, e);
            throw;
        } catch (...) {
            throw;
        }
    });
}

future<> repair_reader::on_end_of_stream() noexcept {
    return _reader.close().then([this] {
        _permit.release_base_resources();
        _reader = mutation_fragment_v1_stream(make_empty_flat_reader_v2(_schema, _permit));
        _reader_handle.reset();
    });
}

future<> repair_reader::close() noexcept {
    return _reader.close().then([this] {
        _permit.release_base_resources();
        _reader_handle.reset();
    });
}

void repair_reader::set_current_dk(const dht::decorated_key& key) {
    _current_dk = make_lw_shared<const decorated_key_with_hash>(*_schema, key, _seed);
}

void repair_reader::clear_current_dk() {
    _current_dk = {};
}

void repair_reader::check_current_dk() {
    if (!_current_dk) {
        throw std::runtime_error("Current partition_key is unknown");
    }
}

void repair_reader::pause() {
    _reader.set_timeout(db::no_timeout);
    if (_reader_handle) {
        _reader_handle->pause();
    }
}

class repair_writer_impl : public repair_writer::impl {
    schema_ptr _schema;
    reader_permit _permit;
    std::optional<future<>> _writer_done;
    mutation_fragment_queue _mq;
    sharded<replica::database>& _db;
    sharded<db::view::view_builder>& _view_builder;
    streaming::stream_reason _reason;
    mutation_reader _queue_reader;
public:
    repair_writer_impl(
        schema_ptr schema,
        reader_permit permit,
        sharded<replica::database>& db,
        sharded<db::view::view_builder>& view_builder,
        streaming::stream_reason reason,
        mutation_fragment_queue queue,
        mutation_reader queue_reader)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _mq(std::move(queue))
        , _db(db)
        , _view_builder(view_builder)
        , _reason(reason)
        , _queue_reader(std::move(queue_reader))
    {}

    virtual void create_writer(lw_shared_ptr<repair_writer> writer) override;

    virtual mutation_fragment_queue& queue() override {
        return _mq;
    }

    virtual future<> wait_for_writer_done() override;

private:
    static sstables::offstrategy is_offstrategy_supported(streaming::stream_reason reason) {
        static const std::unordered_set<streaming::stream_reason> operations_supported = {
            streaming::stream_reason::bootstrap,
            streaming::stream_reason::replace,
            streaming::stream_reason::removenode,
            streaming::stream_reason::decommission,
            streaming::stream_reason::repair,
            streaming::stream_reason::rebuild,
        };
        return sstables::offstrategy(operations_supported.contains(reason));
    }
};

future<> repair_writer::write_start_and_mf(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf) {
    _current_dk_written_to_sstable = dk;
    if (mf.is_partition_start()) {
        return _mq->push(std::move(mf)).then([this] {
            _partition_opened = true;
        });
    } else {
        auto start = mutation_fragment(*_schema, _permit, partition_start(dk->dk, tombstone()));
        return _mq->push(std::move(start)).then([this, mf = std::move(mf)] () mutable {
            _partition_opened = true;
            return _mq->push(std::move(mf));
        });
    }
};

class queue_reader_handle_adapter : public mutation_fragment_queue::impl {
    queue_reader_handle_v2 _handle;
public:
    queue_reader_handle_adapter(queue_reader_handle_v2 handle)
        : _handle(std::move(handle))
    {}

    virtual future<> push(mutation_fragment_v2 mf) override {
        return _handle.push(std::move(mf));
    }

    virtual void abort(std::exception_ptr ep) override {
        _handle.abort(std::move(ep));
    }

    virtual void push_end_of_stream() override {
        _handle.push_end_of_stream();
    }
};

mutation_fragment_queue make_mutation_fragment_queue(schema_ptr s, reader_permit permit, queue_reader_handle_v2 handle) {
    return mutation_fragment_queue(std::move(s), std::move(permit), seastar::make_shared<queue_reader_handle_adapter>(std::move(handle)));
}

void repair_writer_impl::create_writer(lw_shared_ptr<repair_writer> w) {
    if (_writer_done) {
        return;
    }
    replica::table& t = _db.local().find_column_family(_schema->id());
    rlogger.debug("repair_writer: keyspace={}, table={}, estimated_partitions={}", w->schema()->ks_name(), w->schema()->cf_name(), w->get_estimated_partitions());
    service::frozen_topology_guard topo_guard = service::null_topology_guard; // FIXME: propagate
    // The sharder is valid only when the erm is valid. Keep a reference of the erm to keep the sharder valid.
    auto erm = t.get_effective_replication_map();
    auto& sharder = erm->get_sharder(*(w->schema()));
    _writer_done = mutation_writer::distribute_reader_and_consume_on_shards(_schema, sharder, std::move(_queue_reader),
            streaming::make_streaming_consumer(sstables::repair_origin, _db, _view_builder, w->get_estimated_partitions(), _reason, is_offstrategy_supported(_reason), topo_guard),
    t.stream_in_progress()).then([w, erm] (uint64_t partitions) {
        rlogger.debug("repair_writer: keyspace={}, table={}, managed to write partitions={} to sstable",
            w->schema()->ks_name(), w->schema()->cf_name(), partitions);
    }).handle_exception([w, erm] (std::exception_ptr ep) {
        rlogger.warn("repair_writer: keyspace={}, table={}, multishard_writer failed: {}",
                w->schema()->ks_name(), w->schema()->cf_name(), ep);
        w->queue().abort(ep);
        return make_exception_future<>(std::move(ep));
    });
}

lw_shared_ptr<repair_writer> make_repair_writer(
            schema_ptr schema,
            reader_permit permit,
            streaming::stream_reason reason,
            sharded<replica::database>& db,
            sharded<db::view::view_builder>& view_builder) {
    auto [queue_reader, queue_handle] = make_queue_reader_v2(schema, permit);
    auto queue = make_mutation_fragment_queue(schema, permit, std::move(queue_handle));
    auto i = std::make_unique<repair_writer_impl>(schema, permit, db, view_builder, reason, std::move(queue), std::move(queue_reader));
    return make_lw_shared<repair_writer>(schema, permit, std::move(i));
}

future<> repair_writer::write_partition_end() {
    if (_partition_opened) {
        return _mq->push(mutation_fragment(*_schema, _permit, partition_end())).then([this] {
            _partition_opened = false;
        });
    }
    return make_ready_future<>();
}

future<> repair_writer::do_write(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf) {
    if (_current_dk_written_to_sstable) {
        const auto cmp_res = _current_dk_written_to_sstable->dk.tri_compare(*_schema, dk->dk);
        if (cmp_res > 0) {
            on_internal_error(rlogger, format("repair_writer::do_write(): received out-of-order partition, current: {}, next: {}", _current_dk_written_to_sstable->dk, dk->dk));
        } else if (cmp_res == 0) {
            return _mq->push(std::move(mf));
        } else {
            return write_partition_end().then([this,
                    dk = std::move(dk), mf = std::move(mf)] () mutable {
                return write_start_and_mf(std::move(dk), std::move(mf));
            });
        }
    } else {
        return write_start_and_mf(std::move(dk), std::move(mf));
    }
}

future<> repair_writer::write_end_of_stream() {
    if (_created_writer) {
      return with_semaphore(_sem, 1, [this] {
        // Partition_end is never sent on wire, so we have to write one ourselves.
        return write_partition_end().then([this] () mutable {
            _mq->push_end_of_stream();
        }).handle_exception([this] (std::exception_ptr ep) {
            _mq->abort(ep);
            rlogger.warn("repair_writer: keyspace={}, table={}, write_end_of_stream failed: {}",
                    _schema->ks_name(), _schema->cf_name(), ep);
            return make_exception_future<>(std::move(ep));
        });
      });
    } else {
        return make_ready_future<>();
    }
}

future<> repair_writer_impl::wait_for_writer_done() {
    if (_writer_done) {
        return std::move(*(_writer_done));
    } else {
        return make_ready_future<>();
    }
}

future<> repair_writer::wait_for_writer_done() {
    return when_all_succeed(write_end_of_stream(), _impl->wait_for_writer_done()).discard_result().handle_exception(
            [this] (std::exception_ptr ep) {
        rlogger.warn("repair_writer: keyspace={}, table={}, wait_for_writer_done failed: {}",
                _schema->ks_name(), _schema->cf_name(), ep);
        return make_exception_future<>(std::move(ep));
    });
}

class repair_meta;
class repair_meta_tracker;
class row_level_repair;

static void add_to_repair_meta_for_masters(repair_meta& rm);
static void add_to_repair_meta_for_followers(repair_meta& rm);

future<std::list<repair_row>> to_repair_rows_list(repair_rows_on_wire rows, schema_ptr s, uint64_t seed, repair_master is_master, reader_permit permit, repair_hasher hasher) {
    std::list<repair_row> row_list;
    std::exception_ptr ex;
    try {
        lw_shared_ptr<const decorated_key_with_hash> dk_ptr;
        lw_shared_ptr<mutation_fragment> last_mf;
        position_in_partition::tri_compare cmp(*s);

        // Consume the rows and mutation_fragment:s below incrementally
        // as it interleaves frees with allocation, reducing memory pressure,
        // and to prevent reactor stalls when freeing a large repair_rows_on_wire
        // object in one shot when the function returns.
        for (auto it = rows.begin(); it != rows.end(); it = rows.erase(it)) {
            auto x = std::move(*it);

            dht::decorated_key dk = dht::decorate_key(*s, x.get_key());
            if (!(dk_ptr && dk_ptr->dk.equal(*s, dk))) {
                dk_ptr = make_lw_shared<const decorated_key_with_hash>(*s, dk, seed);
            }
            auto& mutation_fragments = x.get_mutation_fragments();
            if (is_master) {
                for (auto fmfit = mutation_fragments.begin(); fmfit != mutation_fragments.end(); fmfit = mutation_fragments.erase(fmfit)) {
                    auto fmf = std::move(*fmfit);

                    _metrics.rx_row_nr += 1;
                    _metrics.rx_row_bytes += fmf.representation().size();
                    // Keep the mutation_fragment in repair_row as an
                    // optimization to avoid unfreeze again when
                    // mutation_fragment is needed by _repair_writer.do_write()
                    // to apply the repair_row to disk
                    auto mf = make_lw_shared<mutation_fragment>(fmf.unfreeze(*s, permit));
                    auto hash = hasher.do_hash_for_mf(*dk_ptr, *mf);
                    position_in_partition pos(mf->position());
                    row_list.push_back(repair_row(std::move(fmf), std::move(pos), dk_ptr, std::move(hash), is_dirty_on_master::yes, std::move(mf)));
                    co_await coroutine::maybe_yield();
                }
            } else {
                last_mf = {};
                for (auto fmfit = mutation_fragments.begin(); fmfit != mutation_fragments.end(); fmfit = mutation_fragments.erase(fmfit)) {
                    auto fmf = std::move(*fmfit);

                    _metrics.rx_row_nr += 1;
                    _metrics.rx_row_bytes += fmf.representation().size();
                    auto mf = make_lw_shared<mutation_fragment>(fmf.unfreeze(*s, permit));
                    // If the mutation_fragment has the same position as
                    // the last mutation_fragment, it means they are the
                    // same row with different contents. We can not feed
                    // such rows into the sstable writer. Instead we apply
                    // the mutation_fragment into the previous one.
                    if (last_mf && cmp(last_mf->position(), mf->position()) == 0 && last_mf->mergeable_with(*mf)) {
                        last_mf->apply(*s, std::move(*mf));
                    } else {
                        last_mf = mf;
                        // On repair follower node, only decorated_key_with_hash and the mutation_fragment inside repair_row are used.
                        row_list.push_back(repair_row({}, {}, dk_ptr, {}, is_dirty_on_master::no, std::move(mf)));
                    }
                    co_await coroutine::maybe_yield();
                }
            }
            co_await coroutine::maybe_yield();
        }
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        co_await utils::clear_gently(rows);
        co_await utils::clear_gently(row_list);
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
    co_return std::move(row_list);
}

void flush_rows(schema_ptr s, std::list<repair_row>& rows, lw_shared_ptr<repair_writer>& writer, locator::effective_replication_map_ptr erm, bool small_table_optimization) {
    auto cmp = position_in_partition::tri_compare(*s);
    lw_shared_ptr<mutation_fragment> last_mf;
    lw_shared_ptr<const decorated_key_with_hash> last_dk;
    bool do_small_table_optimization = erm && small_table_optimization;
    auto* strat = do_small_table_optimization ? &erm->get_replication_strategy() : nullptr;
    auto* tm = do_small_table_optimization ? &erm->get_token_metadata() : nullptr;
    auto myip = do_small_table_optimization ? erm->get_topology().my_address() : gms::inet_address();
    for (auto& r : rows) {
        thread::maybe_yield();
        if (!r.dirty_on_master()) {
            continue;
        }
        const auto& dk = r.get_dk_with_hash()->dk;
        if (do_small_table_optimization) {
            // Check if the token is owned by the node
            auto eps = strat->calculate_natural_ips(dk.token(), *tm).get();
            if (!eps.contains(myip)) {
                rlogger.trace("master: ignore row, token={}", dk.token());
                continue;
            }
        }
        writer->create_writer();
        auto mf = r.get_mutation_fragment_ptr();
        if (last_mf && last_dk &&
                cmp(last_mf->position(), mf->position()) == 0 &&
                dk.tri_compare(*s, last_dk->dk) == 0 &&
                last_mf->mergeable_with(*mf)) {
            last_mf->apply(*s, std::move(*mf));
        } else {
            if (last_mf && last_dk) {
                writer->do_write(std::move(last_dk), std::move(*last_mf)).get();
            }
            last_mf = mf;
            last_dk = r.get_dk_with_hash();
        }
        r.reset_mutation_fragment();
    }
    if (last_mf && last_dk) {
        writer->do_write(std::move(last_dk), std::move(*last_mf)).get();
    }
}

class repair_meta {
    friend repair_meta_tracker;
public:
    using update_working_row_buf = bool_class<class update_working_row_buf_tag>;
    using update_peer_row_hash_sets = bool_class<class update_peer_row_hash_sets_tag>;
    using needs_all_rows_t = bool_class<class needs_all_rows_tag>;
    using msg_addr = netw::messaging_service::msg_addr;
    using tracker_link_type = boost::intrusive::list_member_hook<bi::link_mode<boost::intrusive::auto_unlink>>;
private:
    repair_service& _rs;
    seastar::sharded<replica::database>& _db;
    netw::messaging_service& _messaging;
    schema_ptr _schema;
    reader_permit _permit;
    dht::token_range _range;
    repair_sync_boundary::tri_compare _cmp;
    // The algorithm used to find the row difference
    row_level_diff_detect_algorithm _algo;
    // Max rows size can be stored in _row_buf
    size_t _max_row_buf_size;
    uint64_t _seed = 0;
    repair_master _repair_master;
    uint32_t _repair_meta_id;
    streaming::stream_reason _reason;
    // Repair master's sharding configuration
    shard_config _master_node_shard_config;
    // sharding info of repair master
    dht::static_sharder _remote_sharder;
    bool _same_sharding_config = false;
    struct local_range_estimation {
        size_t master_subranges_count;
        size_t partitions_count;
    };
    std::optional<local_range_estimation> _local_range_estimation;
    uint64_t _estimated_partitions = 0;
    // For repair master nr peers is the number of repair followers, for repair
    // follower nr peers is always one because repair master is the only peer.
    size_t _nr_peer_nodes= 1;
    repair_stats _stats;
    std::optional<repair_reader> _repair_reader;
    lw_shared_ptr<repair_writer> _repair_writer;
    // Contains rows read from disk
    std::list<repair_row> _row_buf;
    // Contains rows we are working on to sync between peers
    std::list<repair_row> _working_row_buf;
    // Combines all the repair_hash in _working_row_buf
    repair_hash _working_row_buf_combined_hash;
    // Tracks the last sync boundary
    std::optional<repair_sync_boundary> _last_sync_boundary;
    // Tracks current sync boundary
    std::optional<repair_sync_boundary> _current_sync_boundary;
    // Contains the hashes of rows in the _working_row_buffor for all peer nodes
    std::vector<repair_hash_set> _peer_row_hash_sets;
    // Gate used to make sure pending operation of meta data is done
    seastar::gate _gate;
    sink_source_for_get_full_row_hashes _sink_source_for_get_full_row_hashes;
    sink_source_for_get_row_diff _sink_source_for_get_row_diff;
    sink_source_for_put_row_diff _sink_source_for_put_row_diff;
    tracker_link_type _tracker_link;
    row_level_repair* _row_level_repair_ptr;
    std::vector<repair_node_state> _all_node_states;
    is_dirty_on_master _dirty_on_master = is_dirty_on_master::no;
    std::optional<shared_future<>> _stopped;
    repair_hasher _repair_hasher;
    gc_clock::time_point _compaction_time;
    bool _is_tablet;
    reader_concurrency_semaphore::inactive_read_handle _fake_inactive_read_handle;
public:
    std::vector<repair_node_state>& all_nodes() {
        return _all_node_states;
    }
    void set_repair_state(repair_state state, gms::inet_address node) {
        for (auto& ns : all_nodes()) {
            if (ns.node == node) {
                ns.state = state;
            }
        }
    }
    void set_repair_state_for_local_node(repair_state state) {
        // The first node is the local node
        all_nodes().front().state = state;
    }
    repair_stats& stats() {
        return _stats;
    }
    gms::inet_address myip() const {
        return _rs.my_address();
    }
    uint32_t repair_meta_id() const {
        return _repair_meta_id;
    }
    const std::optional<repair_sync_boundary>& current_sync_boundary() const {
        return _current_sync_boundary;
    }
    const std::optional<repair_sync_boundary>& last_sync_boundary() const {
        return _last_sync_boundary;
    };
    const repair_hash& working_row_buf_combined_hash() const {
        return _working_row_buf_combined_hash;
    }
    bool use_rpc_stream() const {
        return is_rpc_stream_supported(_algo);
    }

public:
    // master constructor
    repair_meta(
            repair_service& rs,
            replica::column_family& cf,
            schema_ptr s,
            reader_permit permit,
            dht::token_range range,
            row_level_diff_detect_algorithm algo,
            size_t max_row_buf_size,
            uint64_t seed,
            repair_master master,
            uint32_t repair_meta_id,
            streaming::stream_reason reason,
            shard_config master_node_shard_config,
            inet_address_vector_replica_set all_live_peer_nodes,
            size_t nr_peer_nodes,
            std::vector<std::optional<shard_id>> all_live_peer_shards,
            row_level_repair* row_level_repair_ptr,
            gc_clock::time_point compaction_time)
            : _rs(rs)
            , _db(rs.get_db())
            , _messaging(rs.get_messaging())
            , _schema(s)
            , _permit(std::move(permit))
            , _range(range)
            , _cmp(repair_sync_boundary::tri_compare(*_schema))
            , _algo(algo)
            , _max_row_buf_size(max_row_buf_size)
            , _seed(seed)
            , _repair_master(master)
            , _repair_meta_id(repair_meta_id)
            , _reason(reason)
            , _master_node_shard_config(std::move(master_node_shard_config))
            , _remote_sharder(make_remote_sharder())
            , _same_sharding_config(is_same_sharding_config(cf))
            , _nr_peer_nodes(nr_peer_nodes)
            , _repair_writer(make_repair_writer(_schema, _permit, _reason, _db, rs.get_view_builder()))
            , _sink_source_for_get_full_row_hashes(_repair_meta_id, _nr_peer_nodes,
                    [&rs] (uint32_t repair_meta_id, std::optional<shard_id> dst_cpu_id_opt, netw::messaging_service::msg_addr addr) {
                        auto dst_cpu_id = dst_cpu_id_opt.value_or(repair_unspecified_shard);
                        rlogger.debug("get_full_row_hashes: repair_meta_id={} dst_cpu_id={}", repair_meta_id, dst_cpu_id);
                        return rs.get_messaging().make_sink_and_source_for_repair_get_full_row_hashes_with_rpc_stream(repair_meta_id, dst_cpu_id, addr);
                })
            , _sink_source_for_get_row_diff(_repair_meta_id, _nr_peer_nodes,
                    [&rs] (uint32_t repair_meta_id, std::optional<shard_id> dst_cpu_id_opt, netw::messaging_service::msg_addr addr) {
                        auto dst_cpu_id = dst_cpu_id_opt.value_or(repair_unspecified_shard);
                        rlogger.debug("get_row_diff: repair_meta_id={} dst_cpu_id={}", repair_meta_id, dst_cpu_id);
                        return rs.get_messaging().make_sink_and_source_for_repair_get_row_diff_with_rpc_stream(repair_meta_id, dst_cpu_id, addr);
                })
            , _sink_source_for_put_row_diff(_repair_meta_id, _nr_peer_nodes,
                    [&rs] (uint32_t repair_meta_id, std::optional<shard_id> dst_cpu_id_opt, netw::messaging_service::msg_addr addr) {
                        auto dst_cpu_id = dst_cpu_id_opt.value_or(repair_unspecified_shard);
                        rlogger.debug("put_row_diff: repair_meta_id={} dst_cpu_id={}", repair_meta_id, dst_cpu_id);
                        return rs.get_messaging().make_sink_and_source_for_repair_put_row_diff_with_rpc_stream(repair_meta_id, dst_cpu_id, addr);
                })
            , _row_level_repair_ptr(row_level_repair_ptr)
            , _repair_hasher(_seed, _schema)
            , _compaction_time(compaction_time)
            , _is_tablet(cf.uses_tablets())
            {
            if (master) {
                add_to_repair_meta_for_masters(*this);
            } else {
                add_to_repair_meta_for_followers(*this);
            }
            SCYLLA_ASSERT(all_live_peer_shards.size() == all_live_peer_nodes.size());
            _all_node_states.push_back(repair_node_state(myip(), this_shard_id()));
            for (unsigned i = 0; i < all_live_peer_nodes.size(); i++) {
                _all_node_states.push_back(repair_node_state(all_live_peer_nodes[i], all_live_peer_shards[i].value_or(repair_unspecified_shard)));
            }
            // Mark the permit as evictable immediately. If there are multiple
            // repairs launched, they can deadlock in the initial phase, before
            // they start reading from disk (which is the point where they
            // become evictable normally).
            // Prevent this by marking the permit as evictable ASAP.
            // FIXME: provide a better API for this, this is very clunky
            _fake_inactive_read_handle = _db.local().get_reader_concurrency_semaphore().register_inactive_read(make_empty_flat_reader_v2(_schema, _permit));
    }

    // follower constructor
    repair_meta(
            repair_service& rs,
            replica::column_family& cf,
            schema_ptr s,
            reader_permit permit,
            dht::token_range range,
            row_level_diff_detect_algorithm algo,
            size_t max_row_buf_size,
            uint64_t seed,
            repair_master master,
            uint32_t repair_meta_id,
            streaming::stream_reason reason,
            shard_config master_node_shard_config,
            inet_address_vector_replica_set all_live_peer_nodes,
            gc_clock::time_point compaction_time)
        : repair_meta(rs, cf, std::move(s), std::move(permit), std::move(range), algo, max_row_buf_size, seed, master, repair_meta_id, reason,
                std::move(master_node_shard_config), std::move(all_live_peer_nodes), 1, {std::nullopt}, nullptr, compaction_time)
    {
    }

public:
    std::optional<shard_id> get_peer_node_dst_cpu_id(uint32_t peer_node_idx) {
        SCYLLA_ASSERT(peer_node_idx + 1 < all_nodes().size());
        return all_nodes()[peer_node_idx + 1].shard;
    }

public:
    future<> clear_gently() noexcept {
        co_await utils::clear_gently(_peer_row_hash_sets);
        co_await utils::clear_gently(_working_row_buf);
        co_await utils::clear_gently(_row_buf);
    }

    future<> stop() {
        // Handle deferred stop
        if (_stopped) {
            return _stopped->get_future();
        }
        promise<> stopped;
        _stopped.emplace(stopped.get_future());
        auto gate_future = _gate.close();
        auto f1 = _sink_source_for_get_full_row_hashes.close();
        auto f2 = _sink_source_for_get_row_diff.close();
        auto f3 = _sink_source_for_put_row_diff.close();
        rlogger.debug("repair_meta::stop");
        // move to background.  waited on via _stopped->get_future.
        when_all_succeed(std::move(gate_future), std::move(f1), std::move(f2), std::move(f3)).discard_result().finally([this] {
            return _repair_writer->wait_for_writer_done().finally([this] {
                return close().then([this] {
                    return clear_gently();
                });
            });
        }).forward_to(std::move(stopped));
        return _stopped->get_future();
    }

    void reset_peer_row_hash_sets() {
        if (_peer_row_hash_sets.size() != _nr_peer_nodes) {
            _peer_row_hash_sets.resize(_nr_peer_nodes);
        } else {
            for (auto& x : _peer_row_hash_sets) {
                x.clear();
            }
        }

    }

    repair_hash_set& peer_row_hash_sets(unsigned node_idx) {
        return _peer_row_hash_sets[node_idx];
    }

    // Get a list of row hashes in _working_row_buf
    future<repair_hash_set>
    working_row_hashes() {
        auto hashes = repair_hash_set();
        for (auto& r : _working_row_buf) {
            hashes.emplace(r.hash());
            co_await coroutine::maybe_yield();
        }
        co_return std::move(hashes);
    }

    std::pair<std::optional<repair_sync_boundary>, bool>
    get_common_sync_boundary(bool zero_rows,
            std::vector<repair_sync_boundary>& sync_boundaries,
            std::vector<repair_hash>& combined_hashes) {
        if (sync_boundaries.empty()) {
            throw std::runtime_error("sync_boundaries is empty");
        }
        if(combined_hashes.empty()) {
            throw std::runtime_error("combined_hashes is empty");
        }
        // Get the smallest sync boundary in the list as the common sync boundary
        std::sort(sync_boundaries.begin(), sync_boundaries.end(),
                [this] (const auto& a, const auto& b) { return this->_cmp(a, b) < 0; });
        repair_sync_boundary sync_boundary_min = sync_boundaries.front();
        // Check if peers have identical combined hashes and sync boundary
        bool same_hashes = std::adjacent_find(combined_hashes.begin(), combined_hashes.end(),
                std::not_equal_to<repair_hash>()) == combined_hashes.end();
        bool same_boundary = std::adjacent_find(sync_boundaries.begin(), sync_boundaries.end(),
                [this] (const repair_sync_boundary& a, const repair_sync_boundary& b) { return this->_cmp(a, b) != 0; }) == sync_boundaries.end();
        rlogger.debug("get_common_sync_boundary: zero_rows={}, same_hashes={}, same_boundary={}, combined_hashes={}, sync_boundaries={}",
            zero_rows, same_hashes, same_boundary, combined_hashes, sync_boundaries);
        bool already_synced = same_hashes && same_boundary && !zero_rows;
        return std::pair<std::optional<repair_sync_boundary>, bool>(sync_boundary_min, already_synced);
    }

    future<> close() noexcept {
        return _repair_reader ? _repair_reader->close() : make_ready_future<>();
    }

private:
    future<uint64_t> do_estimate_partitions_on_all_shards(const dht::token_range& range) {
        return estimate_partitions(_db, _schema->ks_name(), _schema->cf_name(), range);
    }

    future<uint64_t> do_estimate_partitions_on_local_shard() {
        auto& cf = _db.local().find_column_family(_schema->id());
        return do_with(cf.get_sstables(), uint64_t(0), [this] (lw_shared_ptr<const sstable_list>& sstables, uint64_t& partition_count) {
            return do_for_each(*sstables, [this, &partition_count] (const sstables::shared_sstable& sst) mutable {
                partition_count += sst->estimated_keys_for_range(_range);
            }).then([&partition_count] {
                return partition_count;
            });
        });
    }

    future<uint64_t> get_estimated_partitions() {
      return with_gate(_gate, [this] {
        if (_repair_master || _same_sharding_config || _is_tablet) {
            return do_estimate_partitions_on_local_shard();
        } else {
            return do_with(dht::selective_token_range_sharder(_remote_sharder, _range, _master_node_shard_config.shard), uint64_t(0), uint64_t(0), [this] (auto& sharder, auto& partitions_sum, auto& subranges) mutable {
                return repeat([this, &sharder, &partitions_sum, &subranges] () mutable {
                    auto shard_range = sharder.next();
                    if (shard_range) {
                        ++subranges;
                        return do_estimate_partitions_on_all_shards(*shard_range).then([&partitions_sum] (uint64_t partitions) mutable {
                            partitions_sum += partitions;
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    } else {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                }).then([this, &partitions_sum, &subranges] {
                    _local_range_estimation = local_range_estimation {
                        .master_subranges_count = subranges,
                        .partitions_count = partitions_sum
                    };
                    return partitions_sum;
                });
            });
        }
      });
    }

    future<> set_estimated_partitions(uint64_t estimated_partitions) {
        return with_gate(_gate, [this, estimated_partitions] {
            _estimated_partitions = estimated_partitions;
            _repair_writer->set_estimated_partitions(_estimated_partitions);
        });
    }

    dht::static_sharder make_remote_sharder() {
        return dht::static_sharder(_master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb);
    }

    bool is_same_sharding_config(replica::column_family& cf) {
        rlogger.debug("is_same_sharding_config: remote_shard={}, remote_shard_count={}, remote_ignore_msb={}",
                _master_node_shard_config.shard, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb);
        auto& sharder = cf.get_effective_replication_map()->get_sharder(*_schema);
        return sharder.shard_count() == _master_node_shard_config.shard_count
               && sharder.sharding_ignore_msb() == _master_node_shard_config.ignore_msb
               && this_shard_id() == _master_node_shard_config.shard;
    }

    future<size_t> get_repair_rows_size(const std::list<repair_row>& rows) const {
        return do_with(size_t(0), [&rows] (size_t& sz) {
            return do_for_each(rows, [&sz] (const repair_row& r) mutable {
                sz += r.size();
            }).then([&sz] {
                return sz;
            });
        });
    }

    // Get the size of rows in _row_buf
    future<size_t> row_buf_size() const {
        return get_repair_rows_size(_row_buf);
    }

    // return the combined checksum of rows in _row_buf
    future<repair_hash> row_buf_csum() {
        return do_with(repair_hash(), [this] (repair_hash& combined) {
            return do_for_each(_row_buf, [&combined] (repair_row& r) mutable {
                combined.add(r.hash());
            }).then([&combined] {
                return combined;
            });
        });
    }

    void handle_mutation_fragment(mutation_fragment& mf, size_t& cur_size, size_t& new_rows_size, std::list<repair_row>& cur_rows) {
        if (mf.is_partition_start()) {
            auto& start = mf.as_partition_start();
            _repair_reader->set_current_dk(start.key());
            if (!start.partition_tombstone()) {
                // Ignore partition_start with empty partition tombstone
                return;
            }
        } else if (mf.is_end_of_partition()) {
            _repair_reader->clear_current_dk();
            return;
        }
        auto hash = _repair_hasher.do_hash_for_mf(*_repair_reader->get_current_dk(), mf);
        repair_row r(freeze(*_schema, mf), position_in_partition(mf.position()), _repair_reader->get_current_dk(), hash, is_dirty_on_master::no);
        rlogger.trace("Reading: r.boundary={}, r.hash={}", r.boundary(), r.hash());
        auto sz = r.size();
        _metrics.row_from_disk_nr++;
        _metrics.row_from_disk_bytes += sz;
        cur_size += sz;
        new_rows_size += sz;
        cur_rows.push_back(std::move(r));
    }

    // Read rows from sstable until the size of rows exceeds _max_row_buf_size  - current_size
    // This reads rows from where the reader left last time into _row_buf
    // _current_sync_boundary or _last_sync_boundary have no effect on the reader neither.
    future<std::tuple<std::list<repair_row>, size_t>>
    read_rows_from_disk(size_t cur_size) {
        using value_type = std::tuple<std::list<repair_row>, size_t>;
        size_t new_rows_size = 0;
        std::list<repair_row> cur_rows;
        std::exception_ptr ex;
        if (!_repair_reader) {
            // We are about to create a real evictable reader, so drop the fake
            // reader (evicted or not), we don't need it anymore.
            _db.local().get_reader_concurrency_semaphore().unregister_inactive_read(std::move(_fake_inactive_read_handle));
            _repair_reader.emplace(_db,
                _db.local().find_column_family(_schema->id()),
                _schema,
                _permit,
                _range,
                _remote_sharder,
                _master_node_shard_config.shard,
                _seed,
                std::invoke([this]() {
                    if (_repair_master || _same_sharding_config || _is_tablet) {
                        rlogger.debug("repair_reader: meta_id={}, _repair_master={}, _same_sharding_config={},"
                                      "read_strategy {} is chosen",
                           _repair_meta_id, _repair_master, _same_sharding_config,
                           repair_reader::read_strategy::local);
                        return repair_reader::read_strategy::local;
                    }

                    // multishard_filter means load all the data in the range and apply
                    // filter by master shard on top, discarding partitions from other shards.
                    // multishard_split means split the range into multiple subranges,
                    // each containing only the required data from the master shard.
                    // For situations with a sparse data set spread across numerous ranges,
                    // the overhead from continuously switching between these ranges,
                    // specifically during the fast_forward_to function on the multishard_reader,
                    // can become the main factor in performance.
                    // Similarly, with multishard_filter, reading all the partitions within
                    // the range can lead to the next_partition cost dominating the overall cost.
                    // The heuristic here chooses the strategy with minimal such cost.
                    // Note that with multishard_filter we don't read entire partitions which
                    // can be a huge waste. We only fill the buffer inside
                    // mutation_reader, if a partition is found to belong to the incorrect
                    // master shard, we call next_partition(), which effectively clears
                    // the buffer until the next partition is reached.

                    if (!_local_range_estimation) {
                        // this should not normally happen since the master
                        // calls get_estimated_partitions before get_sync_boundary
                        rlogger.warn("repair_reader: meta_id={}, no _local_range_estimation, "
                                     "read_strategy {} is chosen",
                            _repair_meta_id, repair_reader::read_strategy::multishard_split);
                        return repair_reader::read_strategy::multishard_split;
                    }

                    const auto read_strategy =
                        _local_range_estimation->partitions_count <= _local_range_estimation->master_subranges_count
                        ? repair_reader::read_strategy::multishard_filter
                        : repair_reader::read_strategy::multishard_split;
                    rlogger.debug("repair_reader: meta_id={}, _local_range_estimation: partitions_count={}, "
                                  "master_subranges_count={}, read_strategy {} is chosen",
                        _repair_meta_id,
                        _local_range_estimation->partitions_count,
                        _local_range_estimation->master_subranges_count,
                        read_strategy);
                    return read_strategy;
                }),
                _compaction_time);
        }
        try {
            while (cur_size < _max_row_buf_size) {
                _gate.check();
                mutation_fragment_opt mfopt = co_await _repair_reader->read_mutation_fragment();
                if (!mfopt) {
                    co_await _repair_reader->on_end_of_stream();
                    break;
                }
                handle_mutation_fragment(*mfopt, cur_size, new_rows_size, cur_rows);
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await _repair_reader->on_end_of_stream();
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
        _repair_reader->pause();
        co_return value_type(std::move(cur_rows), new_rows_size);
    }

    future<> clear_row_buf() {
        return utils::clear_gently(_row_buf);
    }

    future<> clear_working_row_buf() {
        return utils::clear_gently(_working_row_buf).then([this] {
            _working_row_buf_combined_hash.clear();
        });
    }

    // Read rows from disk until _max_row_buf_size of rows are filled into _row_buf.
    // Calculate the combined checksum of the rows
    // Calculate the total size of the rows in _row_buf
    future<get_sync_boundary_response>
    get_sync_boundary(std::optional<repair_sync_boundary> skipped_sync_boundary) {
        if (skipped_sync_boundary) {
            _current_sync_boundary = skipped_sync_boundary;
            co_await clear_row_buf();
        }
        // Here is the place we update _last_sync_boundary
        rlogger.trace("SET _last_sync_boundary from {} to {}", _last_sync_boundary, _current_sync_boundary);
        _last_sync_boundary = _current_sync_boundary;
        co_await clear_working_row_buf();
        size_t cur_size = co_await row_buf_size();
        auto rows = co_await read_rows_from_disk(cur_size);
        auto&& [new_rows, new_rows_size] = rows;
        size_t new_rows_nr = new_rows.size();
        _row_buf.splice(_row_buf.end(), new_rows);
        repair_hash row_buf_combined_hash = co_await row_buf_csum();
        size_t row_buf_bytes = co_await row_buf_size();
        std::optional<repair_sync_boundary> sb_max;
        if (!_row_buf.empty()) {
            sb_max = _row_buf.back().boundary();
        }
        rlogger.debug("get_sync_boundary: Got nr={} rows, sb_max={}, row_buf_size={}, repair_hash={}, skipped_sync_boundary={}",
                      new_rows_nr, sb_max, row_buf_bytes, row_buf_combined_hash, skipped_sync_boundary);
        co_return get_sync_boundary_response{sb_max, row_buf_combined_hash, row_buf_bytes, new_rows_size, new_rows_nr};
    }

    future<> move_row_buf_to_working_row_buf() {
        if (_cmp(_row_buf.back().boundary(), *_current_sync_boundary) <= 0) {
            // Fast path
            _working_row_buf.swap(_row_buf);
            co_return;
        }
        size_t sz = _row_buf.size();
        for (auto it = _row_buf.rbegin(); it != _row_buf.rend(); ++it) {
            // Move the rows > _current_sync_boundary to _working_row_buf
            // Delete the rows > _current_sync_boundary from _row_buf
            // Swap _working_row_buf and _row_buf so that _working_row_buf
            // contains rows within (_last_sync_boundary,
            // _current_sync_boundary], _row_buf contains rows within
            // (_current_sync_boundary, ...]
            repair_row& r = *it;
            if (_cmp(r.boundary(), *_current_sync_boundary) <= 0) {
                break;
            }
            _working_row_buf.push_front(std::move(r));
            co_await coroutine::maybe_yield();
        }
        _row_buf.resize(_row_buf.size() - _working_row_buf.size());
        _row_buf.swap(_working_row_buf);
        if (sz != _working_row_buf.size() + _row_buf.size()) {
            throw std::runtime_error(format("incorrect row_buf and working_row_buf size, before={}, after={} + {}",
                                            sz, _working_row_buf.size(), _row_buf.size()));
        }
    }

    // Move rows from <_row_buf> to <_working_row_buf> according to
    // _last_sync_boundary and common_sync_boundary. That is rows within the
    // (_last_sync_boundary, _current_sync_boundary] in <_row_buf> are moved
    // into the <_working_row_buf>
    future<get_combined_row_hash_response>
    request_row_hashes(const std::optional<repair_sync_boundary>& common_sync_boundary) {
        if (!common_sync_boundary) {
            throw std::runtime_error("common_sync_boundary is empty");
        }
        _current_sync_boundary = common_sync_boundary;
        rlogger.trace("SET _current_sync_boundary to {}, common_sync_boundary={}", _current_sync_boundary, common_sync_boundary);
        _working_row_buf.clear();
        _working_row_buf_combined_hash.clear();

        if (_row_buf.empty()) {
            return make_ready_future<get_combined_row_hash_response>(get_combined_row_hash_response());
        }
        return move_row_buf_to_working_row_buf().then([this] {
            return do_for_each(_working_row_buf, [this] (repair_row& r) {
                _working_row_buf_combined_hash.add(r.hash());
                return make_ready_future<>();
            }).then([this] {
                return get_combined_row_hash_response{_working_row_buf_combined_hash};
            });
        });
    }

    future<std::list<repair_row>>
    copy_rows_from_working_row_buf() {
        return do_with(std::list<repair_row>(), [this] (std::list<repair_row>& rows) {
            return do_for_each(_working_row_buf, [&rows] (const repair_row& r) {
                rows.push_back(r);
            }).then([&rows] {
                return std::move(rows);
            });
        });
    }

    future<std::list<repair_row>>
    copy_rows_from_working_row_buf_within_set_diff(repair_hash_set set_diff) {
        return do_with(std::list<repair_row>(), std::move(set_diff),
                [this] (std::list<repair_row>& rows, repair_hash_set& set_diff) {
            return do_for_each(_working_row_buf, [&set_diff, &rows] (const repair_row& r) {
                if (set_diff.contains(r.hash())) {
                    rows.push_back(r);
                }
            }).then([&rows] {
                return std::move(rows);
            });
        });
    }

    // Return rows in the _working_row_buf with hash within the given sef_diff
    // Give a set of row hashes, return the corresponding rows
    // If needs_all_rows is set, return all the rows in _working_row_buf, ignore the set_diff
    future<std::list<repair_row>>
    get_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows = needs_all_rows_t::no) {
        if (needs_all_rows) {
            if (!_repair_master || _nr_peer_nodes == 1) {
                return make_ready_future<std::list<repair_row>>(std::move(_working_row_buf));
            }
            return copy_rows_from_working_row_buf();
        } else {
            return copy_rows_from_working_row_buf_within_set_diff(std::move(set_diff));
        }
    }

    future<> do_apply_rows(std::list<repair_row>&& row_diff, update_working_row_buf update_buf) {
        return do_with(std::move(row_diff), [this, update_buf] (std::list<repair_row>& row_diff) {
            return with_semaphore(_repair_writer->sem(), 1, [this, update_buf, &row_diff] {
                _repair_writer->create_writer();
                return repeat([this, update_buf, &row_diff] () mutable {
                    if (row_diff.empty()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                    repair_row& r = row_diff.front();
                    if (update_buf) {
                        _working_row_buf_combined_hash.add(r.hash());
                    }
                    // The repair_row here is supposed to have
                    // mutation_fragment attached because we have stored it in
                    // to_repair_rows_list above where the repair_row is created.
                    mutation_fragment mf = std::move(r.get_mutation_fragment());
                    r.reset_mutation_fragment();
                    auto dk_with_hash = r.get_dk_with_hash();
                    return _repair_writer->do_write(std::move(dk_with_hash), std::move(mf)).then([&row_diff] {
                        row_diff.pop_front();
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                });
            });
        });
    }

    // Give a list of rows, apply the rows to disk and update the _working_row_buf and _peer_row_hash_sets if requested
    // Must run inside a seastar thread
    void apply_rows_on_master_in_thread(repair_rows_on_wire rows, gms::inet_address from, update_working_row_buf update_buf,
            update_peer_row_hash_sets update_hash_set, unsigned node_idx) {
        if (rows.empty()) {
            return;
        }
        auto row_diff = to_repair_rows_list(std::move(rows), _schema, _seed, _repair_master, _permit, _repair_hasher).get();
        auto sz = get_repair_rows_size(row_diff).get();
        stats().rx_row_bytes += sz;
        stats().rx_row_nr += row_diff.size();
        stats().rx_row_nr_peer[from] += row_diff.size();
        if (update_buf) {
            // Both row_diff and _working_row_buf and are ordered, merging
            // two sored list to make sure the combination of row_diff
            // and _working_row_buf are ordered.
            utils::merge_to_gently(_working_row_buf, row_diff,
                 [this] (const repair_row& x, const repair_row& y) { return _cmp(x.boundary(), y.boundary()) < 0; });
            for (auto& r : row_diff) {
                thread::maybe_yield();
                _working_row_buf_combined_hash.add(r.hash());
            }
        }
        if (update_hash_set) {
            _peer_row_hash_sets[node_idx] = boost::copy_range<repair_hash_set>(row_diff |
                    boost::adaptors::transformed([] (repair_row& r) { thread::maybe_yield(); return r.hash(); }));
        }
        // Repair rows in row_diff will be flushed to disk by flush_rows_in_working_row_buf,
        // so we skip calling do_apply_rows here.
        _dirty_on_master = is_dirty_on_master::yes;
        // Clear gently to avoid stalls
        utils::clear_gently(row_diff).get();
    }
public:
    // Must run inside a seastar thread
    void flush_rows_in_working_row_buf(locator::effective_replication_map_ptr erm, bool small_table_optimization) {
        if (_dirty_on_master) {
            _dirty_on_master = is_dirty_on_master::no;
        } else {
            return;
        }
        flush_rows(_schema, _working_row_buf, _repair_writer, erm, small_table_optimization);
    }

private:
    future<>
    apply_rows_on_follower(repair_rows_on_wire rows) {
        if (rows.empty()) {
            return make_ready_future<>();
        }
        return to_repair_rows_list(std::move(rows), _schema, _seed, _repair_master, _permit, _repair_hasher).then([this] (std::list<repair_row> row_diff) {
            return do_apply_rows(std::move(row_diff), update_working_row_buf::no);
        });
    }

    future<repair_rows_on_wire> to_repair_rows_on_wire(std::list<repair_row> row_list) {
        lw_shared_ptr<const decorated_key_with_hash> last_dk_with_hash;
        return do_with(repair_rows_on_wire(), std::move(row_list), std::move(last_dk_with_hash),
                [this] (repair_rows_on_wire& rows, std::list<repair_row>& row_list, lw_shared_ptr<const decorated_key_with_hash>& last_dk_with_hash) {
            return get_repair_rows_size(row_list).then([this, &rows, &row_list, &last_dk_with_hash] (size_t row_bytes) {
                _metrics.tx_row_nr += row_list.size();
                _metrics.tx_row_bytes += row_bytes;
                return do_for_each(row_list, [this, &rows, &last_dk_with_hash] (repair_row& r) {
                    const auto& dk_with_hash = r.get_dk_with_hash();
                    // No need to search from the beginning of the rows. Look at the end of repair_rows_on_wire is enough.
                    if (rows.empty()) {
                        auto pk = dk_with_hash->dk.key();
                        last_dk_with_hash = dk_with_hash;
                        rows.push_back(repair_row_on_wire(std::move(pk), {std::move(r.get_frozen_mutation())}));
                    } else {
                        auto& row = rows.back();
                        if (last_dk_with_hash && dk_with_hash->dk.tri_compare(*_schema, last_dk_with_hash->dk) == 0) {
                            row.push_mutation_fragment(std::move(r.get_frozen_mutation()));
                        } else {
                            auto pk = dk_with_hash->dk.key();
                            last_dk_with_hash = dk_with_hash;
                            rows.push_back(repair_row_on_wire(std::move(pk), {std::move(r.get_frozen_mutation())}));
                        }
                    }
                }).then([&rows] {
                    return std::move(rows);
                });
            });
        });
    };

public:
    // RPC API
    // Return the hashes of the rows in _working_row_buf
    future<repair_hash_set>
    get_full_row_hashes(gms::inet_address remote_node, shard_id dst_cpu_id) {
        if (remote_node == myip()) {
            return get_full_row_hashes_handler();
        }
        return _messaging.send_repair_get_full_row_hashes(msg_addr(remote_node),
                _repair_meta_id, dst_cpu_id).then([this, remote_node] (repair_hash_set hashes) {
            rlogger.debug("Got full hashes from peer={}, nr_hashes={}", remote_node, hashes.size());
            _metrics.rx_hashes_nr += hashes.size();
            stats().rx_hashes_nr += hashes.size();
            stats().rpc_call_nr++;
            return hashes;
        });
    }

private:
    future<> get_full_row_hashes_source_op(
            lw_shared_ptr<repair_hash_set> current_hashes,
            gms::inet_address remote_node,
            unsigned node_idx,
            rpc::source<repair_hash_with_cmd>& source) {
        return repeat([this, current_hashes, remote_node, node_idx, &source] () mutable {
            return source().then([this, current_hashes, remote_node, node_idx] (std::optional<std::tuple<repair_hash_with_cmd>> hash_cmd_opt) mutable {
                if (hash_cmd_opt) {
                    repair_hash_with_cmd hash_cmd = std::get<0>(hash_cmd_opt.value());
                    rlogger.trace("get_full_row_hashes: Got repair_hash_with_cmd from peer={}, hash={}, cmd={}", remote_node, hash_cmd.hash, int(hash_cmd.cmd));
                    if (hash_cmd.cmd == repair_stream_cmd::hash_data) {
                        current_hashes->insert(hash_cmd.hash);
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    } else if (hash_cmd.cmd == repair_stream_cmd::end_of_current_hash_set) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    } else if (hash_cmd.cmd == repair_stream_cmd::error) {
                        throw std::runtime_error("get_full_row_hashes: Peer failed to process");
                    } else {
                        throw std::runtime_error("get_full_row_hashes: Got unexpected repair_stream_cmd");
                    }
                } else {
                    _sink_source_for_get_full_row_hashes.mark_source_closed(node_idx);
                    throw std::runtime_error("get_full_row_hashes: Got unexpected end of stream");
                }
            });
        });
    }

    future<> get_full_row_hashes_sink_op(rpc::sink<repair_stream_cmd>& sink) {
        return sink(repair_stream_cmd::get_full_row_hashes).then([&sink] {
            return sink.flush();
        }).handle_exception([&sink] (std::exception_ptr ep) {
            return sink.close().then([ep = std::move(ep)] () mutable {
                return make_exception_future<>(std::move(ep));
            });
        });
    }

public:
    future<repair_hash_set>
    get_full_row_hashes_with_rpc_stream(gms::inet_address remote_node, unsigned node_idx, shard_id dst_cpu_id) {
        if (remote_node == myip()) {
            return get_full_row_hashes_handler();
        }
        auto current_hashes = make_lw_shared<repair_hash_set>();
        return _sink_source_for_get_full_row_hashes.get_sink_source(remote_node, node_idx, dst_cpu_id).then_unpack(
                [this, current_hashes, remote_node, node_idx]
                (rpc::sink<repair_stream_cmd>& sink, rpc::source<repair_hash_with_cmd>& source) mutable {
            auto source_op = get_full_row_hashes_source_op(current_hashes, remote_node, node_idx, source);
            auto sink_op = get_full_row_hashes_sink_op(sink);
            return when_all_succeed(std::move(source_op), std::move(sink_op)).discard_result();
        }).then([this, current_hashes] () mutable {
            stats().rx_hashes_nr += current_hashes->size();
            _metrics.rx_hashes_nr += current_hashes->size();
            return std::move(*current_hashes);
        });
    }

    // RPC handler
    future<repair_hash_set>
    get_full_row_hashes_handler() {
        return with_gate(_gate, [this] {
            return working_row_hashes();
        });
    }

    // RPC API
    // Return the combined hashes of the current working row buf
    future<get_combined_row_hash_response>
    get_combined_row_hash(std::optional<repair_sync_boundary> common_sync_boundary, gms::inet_address remote_node, shard_id dst_cpu_id) {
        if (remote_node == myip()) {
            return get_combined_row_hash_handler(common_sync_boundary);
        }
        return _messaging.send_repair_get_combined_row_hash(msg_addr(remote_node),
                _repair_meta_id, common_sync_boundary, dst_cpu_id).then([this] (get_combined_row_hash_response resp) {
            stats().rpc_call_nr++;
            stats().rx_hashes_nr++;
            _metrics.rx_hashes_nr++;
            return resp;
        });
    }

    // RPC handler
    future<get_combined_row_hash_response>
    get_combined_row_hash_handler(std::optional<repair_sync_boundary> common_sync_boundary) {
        // We can not call this function twice. The good thing is we do not use
        // retransmission at messaging_service level, so no message will be retransmitted.
        rlogger.trace("Calling get_combined_row_hash_handler");
        return with_gate(_gate, [this, common_sync_boundary = std::move(common_sync_boundary)] () mutable {
            auto& cf = _db.local().find_column_family(_schema->id());
            cf.update_off_strategy_trigger();
            return request_row_hashes(common_sync_boundary);
        });
    }

    // RPC API
    future<>
    repair_row_level_start(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range, table_schema_version schema_version, streaming::stream_reason reason, gc_clock::time_point compaction_time, shard_id dst_cpu_id) {
        if (remote_node == myip()) {
            return make_ready_future<>();
        }
        stats().rpc_call_nr++;
        // Even though remote partitioner name is ignored in the current version of
        // repair, we still have to send something to keep compatibility with nodes
        // that run older versions. This will make it possible to run mixed cluster.
        // Murmur3 is appropriate because that's the only supported partitioner at
        // the time this change is introduced.
        sstring remote_partitioner_name = "org.apache.cassandra.dht.Murmur3Partitioner";
        return _messaging.send_repair_row_level_start(msg_addr(remote_node),
                _repair_meta_id, ks_name, cf_name, std::move(range), _algo, _max_row_buf_size, _seed,
                _master_node_shard_config.shard, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb,
                remote_partitioner_name, std::move(schema_version), reason, compaction_time, dst_cpu_id).then([ks_name, cf_name] (rpc::optional<repair_row_level_start_response> resp) {
            if (resp && resp->status == repair_row_level_start_status::no_such_column_family) {
                return make_exception_future<>(replica::no_such_column_family(ks_name, cf_name));
            } else {
                return make_ready_future<>();
            }
        });
    }

    // RPC handler
    static future<repair_row_level_start_response>
    repair_row_level_start_handler(repair_service& repair, gms::inet_address from, uint32_t src_cpu_id, uint32_t repair_meta_id, sstring ks_name, sstring cf_name,
            dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size,
            uint64_t seed, shard_config master_node_shard_config, table_schema_version schema_version, streaming::stream_reason reason,
            gc_clock::time_point compaction_time, abort_source& as) {
        rlogger.debug(">>> Started Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, schema_version={}, range={}, seed={}, max_row_buf_siz={}",
                repair.my_address(), from, repair_meta_id, ks_name, cf_name, schema_version, range, seed, max_row_buf_size);
        return repair.insert_repair_meta(from, src_cpu_id, repair_meta_id, std::move(range), algo, max_row_buf_size, seed, std::move(master_node_shard_config), std::move(schema_version), reason, compaction_time, as).then([] {
            return repair_row_level_start_response{repair_row_level_start_status::ok};
        }).handle_exception_type([] (replica::no_such_column_family&) {
            return repair_row_level_start_response{repair_row_level_start_status::no_such_column_family};
        });
    }

    // RPC API
    future<> repair_row_level_stop(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range, shard_id dst_cpu_id) {
        if (remote_node == myip()) {
            return stop();
        }
        stats().rpc_call_nr++;
        return _messaging.send_repair_row_level_stop(msg_addr(remote_node),
                _repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range), dst_cpu_id);
    }

    // RPC handler
    static future<>
    repair_row_level_stop_handler(repair_service& rs, gms::inet_address from, uint32_t repair_meta_id, sstring ks_name, sstring cf_name, dht::token_range range) {
        rlogger.debug("<<< Finished Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, range={}",
                rs.my_address(), from, repair_meta_id, ks_name, cf_name, range);
        auto rm = rs.get_repair_meta(from, repair_meta_id);
        rm->set_repair_state_for_local_node(repair_state::row_level_stop_started);
        return rs.remove_repair_meta(from, repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range)).then([rm] {
            rm->set_repair_state_for_local_node(repair_state::row_level_stop_finished);
        });
    }

    // RPC API
    future<uint64_t> repair_get_estimated_partitions(gms::inet_address remote_node, shard_id dst_cpu_id) {
        if (remote_node == myip()) {
            return get_estimated_partitions();
        }
        stats().rpc_call_nr++;
        return _messaging.send_repair_get_estimated_partitions(msg_addr(remote_node), _repair_meta_id, dst_cpu_id);
    }


    // RPC handler
    static future<uint64_t> repair_get_estimated_partitions_handler(repair_service& rs, gms::inet_address from, uint32_t repair_meta_id) {
        auto rm = rs.get_repair_meta(from, repair_meta_id);
        rm->set_repair_state_for_local_node(repair_state::get_estimated_partitions_started);
        return rm->get_estimated_partitions().then([rm] (uint64_t partitions) {
            rm->set_repair_state_for_local_node(repair_state::get_estimated_partitions_finished);
            return partitions;
        });
    }

    // RPC API
    future<> repair_set_estimated_partitions(gms::inet_address remote_node, uint64_t estimated_partitions, shard_id dst_cpu_id) {
        if (remote_node == myip()) {
            return set_estimated_partitions(estimated_partitions);
        }
        stats().rpc_call_nr++;
        return _messaging.send_repair_set_estimated_partitions(msg_addr(remote_node), _repair_meta_id, estimated_partitions, dst_cpu_id);
    }


    // RPC handler
    static future<> repair_set_estimated_partitions_handler(repair_service& rs, gms::inet_address from, uint32_t repair_meta_id, uint64_t estimated_partitions) {
        auto rm = rs.get_repair_meta(from, repair_meta_id);
        rm->set_repair_state_for_local_node(repair_state::set_estimated_partitions_started);
        return rm->set_estimated_partitions(estimated_partitions).then([rm] {
            rm->set_repair_state_for_local_node(repair_state::set_estimated_partitions_finished);
        });
    }

    // RPC API
    // Return the largest sync point contained in the _row_buf , current _row_buf checksum, and the _row_buf size
    future<get_sync_boundary_response>
    get_sync_boundary(gms::inet_address remote_node, std::optional<repair_sync_boundary> skipped_sync_boundary, shard_id dst_cpu_id) {
        if (remote_node == myip()) {
            return get_sync_boundary_handler(skipped_sync_boundary);
        }
        stats().rpc_call_nr++;
        return _messaging.send_repair_get_sync_boundary(msg_addr(remote_node), _repair_meta_id, skipped_sync_boundary, dst_cpu_id);
    }

    // RPC handler
    future<get_sync_boundary_response>
    get_sync_boundary_handler(std::optional<repair_sync_boundary> skipped_sync_boundary) {
        return with_gate(_gate, [this, skipped_sync_boundary = std::move(skipped_sync_boundary)] () mutable {
            auto& cf = _db.local().find_column_family(_schema->id());
            cf.update_off_strategy_trigger();
            return get_sync_boundary(std::move(skipped_sync_boundary));
        });
    }

    // RPC API
    // Return rows in the _working_row_buf with hash within the given sef_diff
    // Must run inside a seastar thread
    void get_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node, unsigned node_idx, shard_id dst_cpu_id) {
        if (needs_all_rows || !set_diff.empty()) {
            if (remote_node == myip()) {
                return;
            }
            if (needs_all_rows) {
                set_diff.clear();
            } else {
                stats().tx_hashes_nr += set_diff.size();
                _metrics.tx_hashes_nr += set_diff.size();
            }
            stats().rpc_call_nr++;
            repair_rows_on_wire rows = _messaging.send_repair_get_row_diff(msg_addr(remote_node),
                    _repair_meta_id, std::move(set_diff), bool(needs_all_rows), dst_cpu_id).get();
            if (!rows.empty()) {
                apply_rows_on_master_in_thread(std::move(rows), remote_node, update_working_row_buf::yes, update_peer_row_hash_sets::no, node_idx);
            }
        }
    }

    // Must run inside a seastar thread
    void get_row_diff_and_update_peer_row_hash_sets(gms::inet_address remote_node, unsigned node_idx, shard_id dst_cpu_id) {
        if (remote_node == myip()) {
            return;
        }
        stats().rpc_call_nr++;
        repair_rows_on_wire rows = _messaging.send_repair_get_row_diff(msg_addr(remote_node),
                _repair_meta_id, {}, bool(needs_all_rows_t::yes), dst_cpu_id).get();
        if (!rows.empty()) {
            apply_rows_on_master_in_thread(std::move(rows), remote_node, update_working_row_buf::yes, update_peer_row_hash_sets::yes, node_idx);
        }
    }

private:
    // Must run inside a seastar thread
    void get_row_diff_source_op(
            update_peer_row_hash_sets update_hash_set,
            gms::inet_address remote_node,
            unsigned node_idx,
            rpc::sink<repair_hash_with_cmd>& sink,
            rpc::source<repair_row_on_wire_with_cmd>& source) {
        repair_rows_on_wire current_rows;
        for (;;) {
            std::optional<std::tuple<repair_row_on_wire_with_cmd>> row_opt = source().get();
            if (row_opt) {
                if (inject_rpc_stream_error) {
                    throw std::runtime_error("get_row_diff: Inject sender error in source loop");
                }
                auto row = std::move(std::get<0>(row_opt.value()));
                if (row.cmd == repair_stream_cmd::row_data) {
                    rlogger.trace("get_row_diff: Got repair_row_on_wire with data");
                    current_rows.push_back(std::move(row.row));
                } else if (row.cmd == repair_stream_cmd::end_of_current_rows) {
                    rlogger.trace("get_row_diff: Got repair_row_on_wire with nullopt");
                    apply_rows_on_master_in_thread(std::move(current_rows), remote_node, update_working_row_buf::yes, update_hash_set, node_idx);
                    break;
                } else if (row.cmd == repair_stream_cmd::error) {
                    throw std::runtime_error("get_row_diff: Peer failed to process");
                } else {
                    throw std::runtime_error("get_row_diff: Got unexpected repair_stream_cmd");
                }
            } else {
                _sink_source_for_get_row_diff.mark_source_closed(node_idx);
                throw std::runtime_error("get_row_diff: Got unexpected end of stream");
            }
        }
    }

    future<> get_row_diff_sink_op(
            repair_hash_set set_diff,
            needs_all_rows_t needs_all_rows,
            rpc::sink<repair_hash_with_cmd>& sink,
            gms::inet_address remote_node) {
        return do_with(std::move(set_diff), [needs_all_rows, &sink] (repair_hash_set& set_diff) mutable {
            if (inject_rpc_stream_error) {
                return make_exception_future<>(std::runtime_error("get_row_diff: Inject sender error in sink loop"));
            }
            if (needs_all_rows) {
                rlogger.trace("get_row_diff: request with repair_stream_cmd::needs_all_rows");
                return sink(repair_hash_with_cmd{repair_stream_cmd::needs_all_rows, repair_hash()}).then([&sink] () mutable {
                    return sink.flush();
                });
            }
            return do_for_each(set_diff, [&sink] (const repair_hash& hash) mutable {
                return sink(repair_hash_with_cmd{repair_stream_cmd::hash_data, hash});
            }).then([&sink] () mutable {
                return sink(repair_hash_with_cmd{repair_stream_cmd::end_of_current_hash_set, repair_hash()});
            }).then([&sink] () mutable {
                return sink.flush();
            });
        }).handle_exception([&sink] (std::exception_ptr ep) {
            return sink.close().then([ep = std::move(ep)] () mutable {
                return make_exception_future<>(std::move(ep));
            });
        });
    }

public:
    // Must run inside a seastar thread
    void get_row_diff_with_rpc_stream(
            repair_hash_set set_diff,
            needs_all_rows_t needs_all_rows,
            update_peer_row_hash_sets update_hash_set,
            gms::inet_address remote_node,
            unsigned node_idx,
            shard_id dst_cpu_id) {
        if (needs_all_rows || !set_diff.empty()) {
            if (remote_node == myip()) {
                return;
            }
            if (needs_all_rows) {
                set_diff.clear();
            } else {
                stats().tx_hashes_nr += set_diff.size();
                _metrics.tx_hashes_nr += set_diff.size();
            }
            stats().rpc_call_nr++;
            auto f = _sink_source_for_get_row_diff.get_sink_source(remote_node, node_idx, dst_cpu_id).get();
            rpc::sink<repair_hash_with_cmd>& sink = std::get<0>(f);
            rpc::source<repair_row_on_wire_with_cmd>& source = std::get<1>(f);
            auto sink_op = get_row_diff_sink_op(std::move(set_diff), needs_all_rows, sink, remote_node);
            get_row_diff_source_op(update_hash_set, remote_node, node_idx, sink, source);
            sink_op.get();
        }
    }

    // RPC handler
    future<repair_rows_on_wire> get_row_diff_handler(repair_hash_set set_diff, needs_all_rows_t needs_all_rows) {
        return with_gate(_gate, [this, set_diff = std::move(set_diff), needs_all_rows] () mutable {
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this] (std::list<repair_row> row_diff) {
                return to_repair_rows_on_wire(std::move(row_diff));
            });
        });
    }

    // RPC API
    // Send rows in the _working_row_buf with hash within the given sef_diff
    future<> put_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node, shard_id dst_cpu_id) {
        if (!set_diff.empty()) {
            if (remote_node == myip()) {
                return make_ready_future<>();
            }
            size_t sz = set_diff.size();
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this, remote_node, dst_cpu_id, sz] (std::list<repair_row> row_diff) {
                if (row_diff.size() != sz) {
                    rlogger.warn("Hash conflict detected, keyspace={}, table={}, range={}, row_diff.size={}, set_diff.size={}. It is recommended to compact the table and rerun repair for the range.",
                            _schema->ks_name(), _schema->cf_name(), _range, row_diff.size(), sz);
                }
                return do_with(std::move(row_diff), [this, remote_node, dst_cpu_id] (std::list<repair_row>& row_diff) {
                    return get_repair_rows_size(row_diff).then([this, remote_node, dst_cpu_id, &row_diff] (size_t row_bytes) mutable {
                        stats().tx_row_nr += row_diff.size();
                        stats().tx_row_nr_peer[remote_node] += row_diff.size();
                        stats().tx_row_bytes += row_bytes;
                        stats().rpc_call_nr++;
                        return to_repair_rows_on_wire(std::move(row_diff)).then([this, remote_node, dst_cpu_id] (repair_rows_on_wire rows)  {
                            return _messaging.send_repair_put_row_diff(msg_addr(remote_node), _repair_meta_id, std::move(rows), dst_cpu_id);
                        });
                    });
                });
            });
        }
        return make_ready_future<>();
    }

private:
    future<> put_row_diff_source_op(
            gms::inet_address remote_node,
            unsigned node_idx,
            rpc::source<repair_stream_cmd>& source) {
        return repeat([this, remote_node, node_idx, &source] () mutable {
            return source().then([this, remote_node, node_idx] (std::optional<std::tuple<repair_stream_cmd>> status_opt) mutable {
                if (status_opt) {
                    repair_stream_cmd status = std::move(std::get<0>(status_opt.value()));
                    rlogger.trace("put_row_diff: Got status code from follower={} for put_row_diff, status={}", remote_node, int(status));
                    if (status == repair_stream_cmd::put_rows_done) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    } else if (status == repair_stream_cmd::error) {
                        throw std::runtime_error(format("put_row_diff: Repair follower={} failed in put_row_diff handler, status={}", remote_node, int(status)));
                    } else {
                        throw std::runtime_error("put_row_diff: Got unexpected repair_stream_cmd");
                    }
                } else {
                    _sink_source_for_put_row_diff.mark_source_closed(node_idx);
                    throw std::runtime_error("put_row_diff: Got unexpected end of stream");
                }
            });
        });
    }

    future<> put_row_diff_sink_op(
            repair_rows_on_wire rows,
            rpc::sink<repair_row_on_wire_with_cmd>& sink,
            gms::inet_address remote_node) {
        return do_with(std::move(rows), [&sink] (repair_rows_on_wire& rows) mutable {
            return do_for_each(rows, [&sink] (repair_row_on_wire& row) mutable {
                rlogger.trace("put_row_diff: send row");
                return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::row_data, std::move(row)});
            }).then([&sink] () mutable {
                rlogger.trace("put_row_diff: send empty row");
                return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::end_of_current_rows, repair_row_on_wire()}).then([&sink] () mutable {
                    rlogger.trace("put_row_diff: send done");
                    return sink.flush();
                });
            });
        }).handle_exception([&sink] (std::exception_ptr ep) {
            return sink.close().then([ep = std::move(ep)] () mutable {
                return make_exception_future<>(std::move(ep));
            });
        });
    }

public:
    future<> put_row_diff_with_rpc_stream(
            repair_hash_set set_diff,
            needs_all_rows_t needs_all_rows,
            gms::inet_address remote_node, unsigned node_idx,
            const locator::effective_replication_map& erm, bool small_table_optimization,
            shard_id dst_cpu_id) {
        if (set_diff.empty()) {
            co_return;
        }
        if (remote_node == myip()) {
            co_return;
        }
        size_t sz = set_diff.size();

        std::list<repair_row> row_diff = co_await get_row_diff(std::move(set_diff), needs_all_rows);
        if (row_diff.size() != sz) {
            rlogger.warn("Hash conflict detected, keyspace={}, table={}, range={}, row_diff.size={}, set_diff.size={}. It is recommended to compact the table and rerun repair for the range.",
                    _schema->ks_name(), _schema->cf_name(), _range, row_diff.size(), sz);
        }
        if (small_table_optimization) {
            auto& strat = erm.get_replication_strategy();
            const auto& tm = erm.get_token_metadata();
            std::list<repair_row> tmp;
            for (auto& row : row_diff) {
                repair_row r = std::move(row);
                const auto& dk = r.get_dk_with_hash()->dk;
                auto eps = co_await strat.calculate_natural_ips(dk.token(), tm);
                if (eps.contains(remote_node)) {
                    tmp.push_back(std::move(r));
                } else {
                    rlogger.trace("master: put : ignore row, token={}", dk.token());
                }
            }
            row_diff = std::move(tmp);
        }

        size_t row_bytes = co_await get_repair_rows_size(row_diff);

        stats().tx_row_nr += row_diff.size();
        stats().tx_row_nr_peer[remote_node] += row_diff.size();
        stats().tx_row_bytes += row_bytes;
        stats().rpc_call_nr++;

        repair_rows_on_wire rows = co_await to_repair_rows_on_wire(std::move(row_diff));

        auto [sink, source] = co_await _sink_source_for_put_row_diff.get_sink_source(remote_node, node_idx, dst_cpu_id);
        auto source_op = [&] () mutable -> future<> {
            co_await put_row_diff_source_op(remote_node, node_idx, source);
        };
        auto sink_op = [&] () mutable -> future<> {
            co_await put_row_diff_sink_op(std::move(rows), sink, remote_node);
        };
        co_await coroutine::all(source_op, sink_op);
    }

    // RPC handler
    future<> put_row_diff_handler(repair_rows_on_wire rows, gms::inet_address from) {
        return with_gate(_gate, [this, rows = std::move(rows)] () mutable {
            auto& cf = _db.local().find_column_family(_schema->id());
            cf.update_off_strategy_trigger();
            return apply_rows_on_follower(std::move(rows));
        });
    }
};

// Must run inside a seastar thread
static repair_hash_set
get_set_diff(const repair_hash_set& x, const repair_hash_set& y) {
    repair_hash_set set_diff;
    // Note std::set_difference needs x and y are sorted.
    std::copy_if(x.begin(), x.end(), std::inserter(set_diff, set_diff.end()),
            [&y] (auto& item) { thread::maybe_yield(); return !y.contains(item); });
    return set_diff;
}

static future<stop_iteration> repair_get_row_diff_with_rpc_stream_process_op(
        sharded<repair_service>& repair,
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t dst_cpu_id,
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
        return repair.invoke_on(dst_cpu_id, [from, repair_meta_id, needs_all_rows, fp = std::move(fp)] (repair_service& local_repair) {
            auto rm = local_repair.get_repair_meta(from, repair_meta_id);
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
        sharded<repair_service>& repair,
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t dst_cpu_id,
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
        return repair.invoke_on(dst_cpu_id, [from, repair_meta_id, fp = std::move(fp)] (repair_service& local_repair) mutable {
            auto rm = local_repair.get_repair_meta(from, repair_meta_id);
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
        sharded<repair_service>& repair,
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t dst_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_hash_with_cmd> sink,
        rpc::source<repair_stream_cmd> source,
        bool &error,
        std::optional<std::tuple<repair_stream_cmd>> status_opt) {
    repair_stream_cmd status = std::get<0>(status_opt.value());
    rlogger.trace("Got register_repair_get_full_row_hashes_with_rpc_stream from peer={}, status={}", from, int(status));
    if (status == repair_stream_cmd::get_full_row_hashes) {
        return repair.invoke_on(dst_cpu_id, [from, repair_meta_id] (repair_service& local_repair) {
            auto rm = local_repair.get_repair_meta(from, repair_meta_id);
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
        sharded<repair_service>& repair,
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t dst_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_row_on_wire_with_cmd> sink,
        rpc::source<repair_hash_with_cmd> source) {
    return do_with(false, repair_hash_set(), [&repair, from, src_cpu_id, dst_cpu_id, repair_meta_id, sink, source] (bool& error, repair_hash_set& current_set_diff) mutable {
        return repeat([&repair, from, src_cpu_id, dst_cpu_id, repair_meta_id, sink, source, &error, &current_set_diff] () mutable {
            return source().then([&repair, from, src_cpu_id, dst_cpu_id, repair_meta_id, sink, source, &error, &current_set_diff] (std::optional<std::tuple<repair_hash_with_cmd>> hash_cmd_opt) mutable {
                if (hash_cmd_opt) {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    return repair_get_row_diff_with_rpc_stream_process_op(repair, from,
                            src_cpu_id,
                            dst_cpu_id,
                            repair_meta_id,
                            sink,
                            source,
                            error,
                            current_set_diff,
                            std::move(hash_cmd_opt)).handle_exception([from, repair_meta_id, sink, &error] (std::exception_ptr ep) mutable {
                        rlogger.warn("repair_get_row_diff_with_rpc_stream_handler: from={} repair_meta_id={} error={}", from, repair_meta_id, ep);
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
        sharded<repair_service>& repair,
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t dst_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_stream_cmd> sink,
        rpc::source<repair_row_on_wire_with_cmd> source) {
    return do_with(false, repair_rows_on_wire(), [&repair, from, src_cpu_id, dst_cpu_id, repair_meta_id, sink, source] (bool& error, repair_rows_on_wire& current_rows) mutable {
        return repeat([&repair, from, src_cpu_id, dst_cpu_id, repair_meta_id, sink, source, &current_rows, &error] () mutable {
            return source().then([&repair, from, src_cpu_id, dst_cpu_id, repair_meta_id, sink, source, &current_rows, &error] (std::optional<std::tuple<repair_row_on_wire_with_cmd>> row_opt) mutable {
                if (row_opt) {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    return repair_put_row_diff_with_rpc_stream_process_op(repair, from,
                            src_cpu_id,
                            dst_cpu_id,
                            repair_meta_id,
                            sink,
                            source,
                            error,
                            current_rows,
                            std::move(row_opt)).handle_exception([from, repair_meta_id, sink, &error] (std::exception_ptr ep) mutable {
                        rlogger.warn("repair_put_row_diff_with_rpc_stream_handler: from={} repair_meta_id={} error={}", from, repair_meta_id, ep);
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
        sharded<repair_service>& repair,
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t dst_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_hash_with_cmd> sink,
        rpc::source<repair_stream_cmd> source) {
    return repeat([&repair, from, src_cpu_id, dst_cpu_id, repair_meta_id, sink, source] () mutable {
        return do_with(false, [&repair, from, src_cpu_id, dst_cpu_id, repair_meta_id, sink, source] (bool& error) mutable {
            return source().then([&repair, from, src_cpu_id, dst_cpu_id, repair_meta_id, sink, source, &error] (std::optional<std::tuple<repair_stream_cmd>> status_opt) mutable {
                if (status_opt) {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    return repair_get_full_row_hashes_with_rpc_stream_process_op(repair, from,
                            src_cpu_id,
                            dst_cpu_id,
                            repair_meta_id,
                            sink,
                            source,
                            error,
                            std::move(status_opt)).handle_exception([from, repair_meta_id, sink, &error] (std::exception_ptr ep) mutable {
                        rlogger.warn("repair_get_full_row_hashes_with_rpc_stream_handler: from={} repair_meta_id={} error={}", from, repair_meta_id, ep);
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

future<repair_update_system_table_response> repair_service::repair_update_system_table_handler(gms::inet_address from, repair_update_system_table_request req) {
    rlogger.debug("repair[{}]: Got repair_update_system_table_request from node={}, range={}, repair_time={}", req.repair_uuid, from, req.range, req.repair_time);
    auto& db = this->get_db();
    bool is_valid_range = true;
    if (req.range.start()) {
        if (req.range.start()->is_inclusive()) {
            is_valid_range = false;
        }
    }
    if (req.range.end()) {
        if (!req.range.end()->is_inclusive()) {
            is_valid_range = false;
        }
    }
    if (!is_valid_range) {
        throw std::runtime_error(format("repair[{}]: range {} is not in the format of (start, end]", req.repair_uuid, req.range));
    }
    co_await db.invoke_on_all([&req] (replica::database& local_db) {
        auto& gc_state = local_db.get_compaction_manager().get_tombstone_gc_state();
        return gc_state.update_repair_time(req.table_uuid, req.range, req.repair_time);
    });
    db::system_keyspace::repair_history_entry ent;
    ent.id = req.repair_uuid;
    ent.table_uuid = req.table_uuid;
    ent.ts = db_clock::from_time_t(gc_clock::to_time_t(req.repair_time));
    ent.ks = req.keyspace_name;
    ent.cf = req.table_name;
    auto range_start = req.range.start() ? req.range.start()->value() : dht::minimum_token();
    ent.range_start = dht::token::to_int64(range_start);
    auto range_end = req.range.end() ? req.range.end()->value() : dht::maximum_token();
    ent.range_end = dht::token::to_int64(range_end);
    co_await _sys_ks.local().update_repair_history(std::move(ent));
    co_return repair_update_system_table_response();
}

future<repair_flush_hints_batchlog_response> repair_service::repair_flush_hints_batchlog_handler(gms::inet_address from, repair_flush_hints_batchlog_request req) {
    rlogger.info("repair[{}]: Started to process repair_flush_hints_batchlog_request from node={}, target_nodes={}, hints_timeout={}s, batchlog_timeout={}s",
            req.repair_uuid, from, req.target_nodes, req.hints_timeout.count(), req.batchlog_timeout.count());
    std::vector<gms::inet_address> target_nodes(req.target_nodes.begin(), req.target_nodes.end());
    db::hints::sync_point sync_point = co_await _sp.local().create_hint_sync_point(std::move(target_nodes));
    lowres_clock::time_point deadline = lowres_clock::now() + req.hints_timeout;
    try {
        bool bm_throw = utils::get_local_injector().enter("repair_flush_hints_batchlog_handler_bm_uninitialized");
        if (!_bm.local_is_initialized() || bm_throw) {
            throw std::runtime_error("Backlog manager isn't initialized");
        }
        co_await coroutine::all(
            [this, &from, &req, &sync_point, &deadline] () -> future<> {
                rlogger.info("repair[{}]: Started to flush hints for repair_flush_hints_batchlog_request from node={}, target_nodes={}", req.repair_uuid, from, req.target_nodes);
                co_await _sp.local().wait_for_hint_sync_point(std::move(sync_point), deadline);
                rlogger.info("repair[{}]: Finished to flush hints for repair_flush_hints_batchlog_request from node={}, target_hosts={}", req.repair_uuid, from, req.target_nodes);
                co_return;
            },
            [this, &from, &req] () -> future<>  {
                rlogger.info("repair[{}]: Started to flush batchlog for repair_flush_hints_batchlog_request from node={}, target_nodes={}", req.repair_uuid, from, req.target_nodes);
                co_await _bm.local().do_batch_log_replay();
                rlogger.info("repair[{}]: Finished to flush batchlog for repair_flush_hints_batchlog_request from node={}, target_nodes={}", req.repair_uuid, from, req.target_nodes);
            }
        );
    } catch (...) {
        rlogger.warn("repair[{}]: Failed to process repair_flush_hints_batchlog_request from node={}, target_hosts={}, {}",
                req.repair_uuid, from, req.target_nodes, std::current_exception());
        throw;
    }
    rlogger.info("repair[{}]: Finished to process repair_flush_hints_batchlog_request from node={}, target_nodes={}", req.repair_uuid, from, req.target_nodes);
    co_return repair_flush_hints_batchlog_response();
}

future<> repair_service::init_ms_handlers() {
    auto& ms = this->_messaging;

    ms.register_repair_get_row_diff_with_rpc_stream([this, &ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_hash_with_cmd> source, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto sink = ms.make_sink_for_repair_get_row_diff_with_rpc_stream(source);
        // Start a new fiber.
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        (void)repair_get_row_diff_with_rpc_stream_handler(container(), from, src_cpu_id, shard, repair_meta_id, sink, source).handle_exception(
                [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
            rlogger.warn("Failed to process get_row_diff_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
        });
        return make_ready_future<rpc::sink<repair_row_on_wire_with_cmd>>(sink);
    });
    ms.register_repair_put_row_diff_with_rpc_stream([this, &ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_row_on_wire_with_cmd> source, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto sink = ms.make_sink_for_repair_put_row_diff_with_rpc_stream(source);
        // Start a new fiber.
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        (void)repair_put_row_diff_with_rpc_stream_handler(container(), from, src_cpu_id, shard, repair_meta_id, sink, source).handle_exception(
                [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
            rlogger.warn("Failed to process put_row_diff_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
        });
        return make_ready_future<rpc::sink<repair_stream_cmd>>(sink);
    });
    ms.register_repair_get_full_row_hashes_with_rpc_stream([this, &ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_stream_cmd> source, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto sink = ms.make_sink_for_repair_get_full_row_hashes_with_rpc_stream(source);
        // Start a new fiber.
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        (void)repair_get_full_row_hashes_with_rpc_stream_handler(container(), from, src_cpu_id, shard, repair_meta_id, sink, source).handle_exception(
                [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
            rlogger.warn("Failed to process get_full_row_hashes_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
        });
        return make_ready_future<rpc::sink<repair_hash_with_cmd>>(sink);
    });
    ms.register_repair_get_full_row_hashes([this] (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        return container().invoke_on(shard, [from, repair_meta_id] (repair_service& local_repair) {
            auto rm = local_repair.get_repair_meta(from, repair_meta_id);
            rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_started);
            return rm->get_full_row_hashes_handler().then([rm] (repair_hash_set hashes) {
                rm->set_repair_state_for_local_node(repair_state::get_full_row_hashes_finished);
                _metrics.tx_hashes_nr += hashes.size();
                return hashes;
            });
        }) ;
    });
    ms.register_repair_get_combined_row_hash([this] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
            std::optional<repair_sync_boundary> common_sync_boundary, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return container().invoke_on(shard, [from, repair_meta_id,
                common_sync_boundary = std::move(common_sync_boundary)] (repair_service& local_repair) mutable {
            auto rm = local_repair.get_repair_meta(from, repair_meta_id);
            _metrics.tx_hashes_nr++;
            rm->set_repair_state_for_local_node(repair_state::get_combined_row_hash_started);
            return rm->get_combined_row_hash_handler(std::move(common_sync_boundary)).then([rm] (get_combined_row_hash_response resp) {
                rm->set_repair_state_for_local_node(repair_state::get_combined_row_hash_finished);
                return resp;
            });
        });
    });
    ms.register_repair_get_sync_boundary([this] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
            std::optional<repair_sync_boundary> skipped_sync_boundary, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        return container().invoke_on(shard, [from, repair_meta_id,
                skipped_sync_boundary = std::move(skipped_sync_boundary)] (repair_service& local_repair) mutable {
            auto rm = local_repair.get_repair_meta(from, repair_meta_id);
            rm->set_repair_state_for_local_node(repair_state::get_sync_boundary_started);
            return rm->get_sync_boundary_handler(std::move(skipped_sync_boundary)).then([rm] (get_sync_boundary_response resp) {
                rm->set_repair_state_for_local_node(repair_state::get_sync_boundary_finished);
                return resp;
            });
        });
    });
    ms.register_repair_get_row_diff([this] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
            repair_hash_set set_diff, bool needs_all_rows, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        _metrics.rx_hashes_nr += set_diff.size();
        auto fp = make_foreign(std::make_unique<repair_hash_set>(std::move(set_diff)));
        return container().invoke_on(shard, [from, repair_meta_id, fp = std::move(fp), needs_all_rows] (repair_service& local_repair) mutable {
            auto rm = local_repair.get_repair_meta(from, repair_meta_id);
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
    ms.register_repair_put_row_diff([this] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
            repair_rows_on_wire row_diff, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto fp = make_foreign(std::make_unique<repair_rows_on_wire>(std::move(row_diff)));
        return container().invoke_on(shard, [from, repair_meta_id, fp = std::move(fp)] (repair_service& local_repair) mutable {
            auto rm = local_repair.get_repair_meta(from, repair_meta_id);
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
    ms.register_repair_row_level_start([this] (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring ks_name,
            sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed,
            unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version,
            rpc::optional<streaming::stream_reason> reason, rpc::optional<gc_clock::time_point> compaction_time, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return container().invoke_on(shard, [from, src_cpu_id, repair_meta_id, ks_name, cf_name,
                range, algo, max_row_buf_size, seed, remote_shard, remote_shard_count, remote_ignore_msb, schema_version, reason, compaction_time, this] (repair_service& local_repair) mutable {
            if (!local_repair._view_builder.local_is_initialized()) {
                return make_exception_future<repair_row_level_start_response>(std::runtime_error(format("Node {} is not fully initialized for repair, try again later",
                        local_repair.my_address())));
            }
            streaming::stream_reason r = reason ? *reason : streaming::stream_reason::repair;
            const gc_clock::time_point ct = compaction_time ? *compaction_time : gc_clock::now();
            return repair_meta::repair_row_level_start_handler(local_repair, from, src_cpu_id, repair_meta_id, std::move(ks_name),
                    std::move(cf_name), std::move(range), algo, max_row_buf_size, seed,
                    shard_config{remote_shard, remote_shard_count, remote_ignore_msb},
                    schema_version, r, ct, _repair_module->abort_source());
        });
    });
    ms.register_repair_row_level_stop([this] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
            sstring ks_name, sstring cf_name, dht::token_range range, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return container().invoke_on(shard, [from, repair_meta_id, ks_name, cf_name, range] (repair_service& local_repair) mutable {
            return repair_meta::repair_row_level_stop_handler(local_repair, from, repair_meta_id,
                    std::move(ks_name), std::move(cf_name), std::move(range));
        });
    });
    ms.register_repair_get_estimated_partitions([this] (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return container().invoke_on(shard, [from, repair_meta_id] (repair_service& local_repair) mutable {
            return repair_meta::repair_get_estimated_partitions_handler(local_repair, from, repair_meta_id);
        });
    });
    ms.register_repair_set_estimated_partitions([this] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
            uint64_t estimated_partitions, rpc::optional<shard_id> dst_cpu_id_opt) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto shard = get_dst_shard_id(src_cpu_id, dst_cpu_id_opt);
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return container().invoke_on(shard, [from, repair_meta_id, estimated_partitions] (repair_service& local_repair) mutable {
            return repair_meta::repair_set_estimated_partitions_handler(local_repair, from, repair_meta_id, estimated_partitions);
        });
    });
    ms.register_repair_get_diff_algorithms([] (const rpc::client_info& cinfo) {
        return make_ready_future<std::vector<row_level_diff_detect_algorithm>>(suportted_diff_detect_algorithms());
    });
    ser::partition_checksum_rpc_verbs::register_repair_update_system_table(&ms, [this] (const rpc::client_info& cinfo, repair_update_system_table_request req) {
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return repair_update_system_table_handler(from, std::move(req));
    });
    ser::partition_checksum_rpc_verbs::register_repair_flush_hints_batchlog(&ms, [this] (const rpc::client_info& cinfo, repair_flush_hints_batchlog_request req) {
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return repair_flush_hints_batchlog_handler(from, std::move(req));
    });

    return make_ready_future<>();
}

future<> repair_service::uninit_ms_handlers() {
    auto& ms = this->_messaging;

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
        ms.unregister_repair_get_diff_algorithms(),
        ser::partition_checksum_rpc_verbs::unregister_repair_update_system_table(&ms),
        ser::partition_checksum_rpc_verbs::unregister_repair_flush_hints_batchlog(&ms)
        ).discard_result();
}

class repair_meta_tracker {
    boost::intrusive::list<repair_meta,
        boost::intrusive::member_hook<repair_meta, repair_meta::tracker_link_type, &repair_meta::_tracker_link>,
        boost::intrusive::constant_time_size<false>> _repair_metas;
public:
    void add(repair_meta& rm) {
        _repair_metas.push_back(rm);
    }
};

namespace debug {
    static thread_local repair_meta_tracker repair_meta_for_masters;
    static thread_local repair_meta_tracker repair_meta_for_followers;
}

static void add_to_repair_meta_for_masters(repair_meta& rm) {
    debug::repair_meta_for_masters.add(rm);
}
static void add_to_repair_meta_for_followers(repair_meta& rm) {
    debug::repair_meta_for_followers.add(rm);
}

class row_level_repair {
    repair::shard_repair_task_impl& _shard_task;
    sstring _cf_name;
    table_id _table_id;
    dht::token_range _range;
    inet_address_vector_replica_set _all_live_peer_nodes;
    std::vector<std::optional<shard_id>> _all_live_peer_shards;
    bool _small_table_optimization;

    // Repair master and followers will propose a sync boundary. Each of them
    // read N bytes of rows from disk, the row with largest
    // `position_in_partition` value is the proposed sync boundary of that
    // node. The repair master uses `get_sync_boundary` rpc call to
    // get all the proposed sync boundary and stores in in
    // `_sync_boundaries`. The `get_sync_boundary` rpc call also
    // returns the combined hashes and the total size for the rows which are
    // in the `_row_buf`. `_row_buf` buffers the rows read from sstable. It
    // contains rows at most of `_max_row_buf_size` bytes.
    // If all the peers return the same `_sync_boundaries` and
    // `_combined_hashes`, we think the rows are synced.
    // If not, we proceed to the next step.
    std::vector<repair_sync_boundary> _sync_boundaries;
    std::vector<repair_hash> _combined_hashes;

    // `common_sync_boundary` is the boundary all the peers agrees on
    std::optional<repair_sync_boundary> _common_sync_boundary = {};

    // `_skipped_sync_boundary` is used in case we find the range is synced
    // only with the `get_sync_boundary` rpc call. We use it to make
    // sure the remote peers update the `_current_sync_boundary` and
    // `_last_sync_boundary` correctly.
    std::optional<repair_sync_boundary> _skipped_sync_boundary = {};

    // If the total size of the `_row_buf` on either of the nodes is zero,
    // we set this flag, which is an indication that rows are not synced.
    bool _zero_rows = false;

    // Sum of estimated_partitions on all peers
    uint64_t _estimated_partitions = 0;

    // A flag indicates any error during the repair
    bool _failed = false;

    // Seed for the repair row hashing. If we ever had a hash conflict for a row
    // and we are not using stable hash, there is chance we will fix the row in
    // the next repair.
    uint64_t _seed;

    gc_clock::time_point _start_time;

    bool _is_tablet;

public:
    row_level_repair(repair::shard_repair_task_impl& shard_task,
            sstring cf_name,
            table_id table_id,
            dht::token_range range,
            std::vector<gms::inet_address> all_live_peer_nodes,
            bool small_table_optimization)
        : _shard_task(shard_task)
        , _cf_name(std::move(cf_name))
        , _table_id(std::move(table_id))
        , _range(std::move(range))
        , _all_live_peer_nodes(sort_peer_nodes(all_live_peer_nodes))
        , _small_table_optimization(small_table_optimization)
        , _seed(get_random_seed())
        , _start_time(gc_clock::now())
        , _is_tablet(_shard_task.db.local().find_column_family(_table_id).uses_tablets())
    {
        repair_neighbors r_neighbors = _shard_task.get_repair_neighbors(_range);
        auto& map = r_neighbors.shard_map;
        for (auto& n : _all_live_peer_nodes) {
            auto it = map.find(n);
            if (it != map.end()) {
                _all_live_peer_shards.push_back(it->second);
            } else {
                _all_live_peer_shards.push_back(std::nullopt);
            }
        }
        if (_all_live_peer_shards.size() != _all_live_peer_nodes.size()) {
            on_internal_error(rlogger, format("The size of shards and nodes do not match table={} range={} shards={} nodes={}",
                _cf_name, _range, _all_live_peer_shards, _all_live_peer_nodes));
        }

    }

private:
    enum class op_status {
        next_round,
        next_step,
        all_done,
    };

    inet_address_vector_replica_set sort_peer_nodes(const std::vector<gms::inet_address>& nodes) {
        inet_address_vector_replica_set sorted_nodes(nodes.begin(), nodes.end());
        auto& topology = get_erm()->get_topology();
        topology.sort_by_proximity(topology.my_address(), sorted_nodes);
        return sorted_nodes;
    }

    size_t get_max_row_buf_size(row_level_diff_detect_algorithm algo) {
        // Max buffer size per repair round
        return is_rpc_stream_supported(algo) ?  repair::task_manager_module::max_repair_memory_per_range : 256 * 1024;
    }

    // Step A: Negotiate sync boundary to use
    op_status negotiate_sync_boundary(repair_meta& master) {
        _shard_task.check_in_abort_or_shutdown();
        _sync_boundaries.clear();
        _combined_hashes.clear();
        _zero_rows = false;
        rlogger.debug("ROUND {}, _last_sync_boundary={}, _current_sync_boundary={}, _skipped_sync_boundary={}",
                master.stats().round_nr, master.last_sync_boundary(), master.current_sync_boundary(), _skipped_sync_boundary);
        master.stats().round_nr++;
        parallel_for_each(master.all_nodes(), [&, this] (repair_node_state& ns) {
            const auto& node = ns.node;
            auto dst_cpu_id = ns.shard;
            // By calling `get_sync_boundary`, the `_last_sync_boundary`
            // is moved to the `_current_sync_boundary` or
            // `_skipped_sync_boundary` if it is not std::nullopt.
            ns.state = repair_state::get_sync_boundary_started;
            return master.get_sync_boundary(node, _skipped_sync_boundary, dst_cpu_id).then([&, this] (get_sync_boundary_response res) {
                ns.state = repair_state::get_sync_boundary_finished;
                master.stats().row_from_disk_bytes[node] += res.new_rows_size;
                master.stats().row_from_disk_nr[node] += res.new_rows_nr;
                if (res.boundary && res.row_buf_size > 0) {
                    _sync_boundaries.push_back(*res.boundary);
                    _combined_hashes.push_back(res.row_buf_combined_csum);
                } else {
                    // row_size equals 0 means there is no data from on
                    // that node, so we ignore the sync boundary of
                    // this node when calculating common sync boundary
                    _zero_rows = true;
                }
                rlogger.debug("Called master.get_sync_boundary for node {} sb={}, combined_csum={}, row_size={}, zero_rows={}, skipped_sync_boundary={}",
                    node, res.boundary, res.row_buf_combined_csum, res.row_buf_size, _zero_rows, _skipped_sync_boundary);
            }).handle_exception([this, node] (std::exception_ptr ep) {
                rlogger.warn("repair[{}]: get_sync_boundary: got error from node={}, keyspace={}, table={}, range={}, error={}",
                        _shard_task.global_repair_id.uuid(), node, _shard_task.get_keyspace(), _cf_name, _range, ep);
                return make_exception_future<>(std::move(ep));
            });
        }).get();
        rlogger.debug("sync_boundaries nr={}, combined_hashes nr={}",
            _sync_boundaries.size(), _combined_hashes.size());
        if (!_sync_boundaries.empty()) {
            // We have data to sync between (_last_sync_boundary, _current_sync_boundary]
            auto res = master.get_common_sync_boundary(_zero_rows, _sync_boundaries, _combined_hashes);
            _common_sync_boundary = res.first;
            bool already_synced = res.second;
            rlogger.debug("Calling master._get_common_sync_boundary: common_sync_boundary={}, already_synced={}",
                    _common_sync_boundary, already_synced);
            // If rows between (_last_sync_boundary, _current_sync_boundary] are synced, goto first step
            // This is the first fast path.
            if (already_synced) {
                _skipped_sync_boundary = _common_sync_boundary;
                rlogger.debug("Skip set skipped_sync_boundary={}", _skipped_sync_boundary);
                master.stats().round_nr_fast_path_already_synced++;
                return op_status::next_round;
            } else {
                _skipped_sync_boundary = std::nullopt;
            }
        } else {
            master.stats().round_nr_fast_path_already_synced++;
            // We are done with this range because all the nodes have no more data.
            return op_status::all_done;
        }
        return op_status::next_step;
    }

    // Step B: Get missing rows from peer nodes so that local node contains all the rows
    op_status get_missing_rows_from_follower_nodes(repair_meta& master) {
        _shard_task.check_in_abort_or_shutdown();
        // `combined_hashes` contains the combined hashes for the
        // `_working_row_buf`. Like `_row_buf`, `_working_row_buf` contains
        // rows which are within the (_last_sync_boundary, _current_sync_boundary]
        // By calling `get_combined_row_hash(_common_sync_boundary)`,
        // all the nodes move the `_current_sync_boundary` to `_common_sync_boundary`,
        // Rows within the (_last_sync_boundary, _current_sync_boundary] are
        // moved from the `_row_buf` to `_working_row_buf`.
        std::vector<repair_hash> combined_hashes;
        combined_hashes.resize(master.all_nodes().size());
        parallel_for_each(boost::irange(size_t(0), master.all_nodes().size()), [&, this] (size_t idx) {
            // Request combined hashes from all nodes between (_last_sync_boundary, _current_sync_boundary]
            // Each node will
            // - Set `_current_sync_boundary` to `_common_sync_boundary`
            // - Move rows from `_row_buf` to `_working_row_buf`
            // But the full hashes (each and every hashes for the rows in
            // the `_working_row_buf`) are not returned until repair master
            // explicitly requests with get_full_row_hashes() below as
            // an optimization. Because if the combined_hashes from all
            // peers are identical, we think rows in the `_working_row_buff`
            // are identical, there is no need to transfer each and every
            // row hashes to the repair master.
            auto& ns = master.all_nodes()[idx];
            ns.state = repair_state::get_combined_row_hash_started;
            return master.get_combined_row_hash(_common_sync_boundary, ns.node, ns.shard).then([&, idx] (get_combined_row_hash_response resp) {
                master.all_nodes()[idx].state = repair_state::get_combined_row_hash_finished;
                rlogger.debug("Calling master.get_combined_row_hash for node {}, got combined_hash={}", master.all_nodes()[idx].node, resp);
                combined_hashes[idx]= std::move(resp);
            }).handle_exception([this, &master, idx] (std::exception_ptr ep) {
                auto& node = master.all_nodes()[idx].node;
                rlogger.warn("repair[{}]: get_combined_row_hash: got error from node={}, keyspace={}, table={}, range={}, error={}",
                        _shard_task.global_repair_id.uuid(), node, _shard_task.get_keyspace(), _cf_name, _range, ep);
                return make_exception_future<>(std::move(ep));
            });
        }).get();

        // If all the peers has the same combined_hashes. This means they contain
        // the identical rows. So there is no need to sync for this sync boundary.
        bool same_combined_hashes = std::adjacent_find(combined_hashes.begin(), combined_hashes.end(),
            std::not_equal_to<repair_hash>()) == combined_hashes.end();
        if (same_combined_hashes) {
            // `_working_row_buf` on all the nodes are the same
            // This is the second fast path.
            master.stats().round_nr_fast_path_same_combined_hashes++;
            return op_status::next_round;
        }

        master.reset_peer_row_hash_sets();
        // Note: We can not work on _all_live_peer_nodes in parallel,
        // because syncing with _all_live_peer_nodes in serial avoids
        // getting the same rows from more than one peers.
        for (unsigned node_idx = 0; node_idx < _all_live_peer_nodes.size(); node_idx++) {
            auto& ns = master.all_nodes()[node_idx + 1];
            auto& node = _all_live_peer_nodes[node_idx];
            auto dst_cpu_id = ns.shard;
          try {
            // Here is an optimization to avoid transferring full rows hashes,
            // if remote and local node, has the same combined_hashes.
            // For example:
            // node1: 1 2 3
            // node2: 1 2 3 4
            // node3: 1 2 3 4
            // After node1 get the row 4 from node2, node1 update it is
            // combined_hashes, so we can avoid fetching the full row hashes from node3.
            if (combined_hashes[node_idx + 1] == master.working_row_buf_combined_hash()) {
                // local node and peer node have the same combined hash. This
                // means we can set peer_row_hash_sets[n] to local row hashes
                // without fetching it from peers to save network traffic.
                master.peer_row_hash_sets(node_idx) = master.working_row_hashes().get();
                rlogger.debug("Calling optimize master.working_row_hashes for node {}, hash_sets={}",
                    node, master.peer_row_hash_sets(node_idx).size());
                continue;
            }

            // Fast path: if local has zero row and remote has rows, request them all.
            if (master.working_row_buf_combined_hash() == repair_hash() && combined_hashes[node_idx + 1] != repair_hash()) {
                master.peer_row_hash_sets(node_idx).clear();
                if (master.use_rpc_stream()) {
                    rlogger.debug("FastPath: get_row_diff with needs_all_rows_t::yes rpc stream");
                    ns.state = repair_state::get_row_diff_with_rpc_stream_started;
                    master.get_row_diff_with_rpc_stream({}, repair_meta::needs_all_rows_t::yes, repair_meta::update_peer_row_hash_sets::yes, node, node_idx, dst_cpu_id);
                    ns.state = repair_state::get_row_diff_with_rpc_stream_finished;
                } else {
                    rlogger.debug("FastPath: get_row_diff with needs_all_rows_t::yes rpc verb");
                    ns.state = repair_state::get_row_diff_and_update_peer_row_hash_sets_started;
                    master.get_row_diff_and_update_peer_row_hash_sets(node, node_idx, dst_cpu_id);
                    ns.state = repair_state::get_row_diff_and_update_peer_row_hash_sets_finished;
                }
                continue;
            }

            rlogger.debug("Before master.get_full_row_hashes for node {}, hash_sets={}",
                node, master.peer_row_hash_sets(node_idx).size());
            // Ask the peer to send the full list hashes in the working row buf.
            if (master.use_rpc_stream()) {
                ns.state = repair_state::get_full_row_hashes_with_rpc_stream_started;
                master.peer_row_hash_sets(node_idx) = master.get_full_row_hashes_with_rpc_stream(node, node_idx, dst_cpu_id).get();
                ns.state = repair_state::get_full_row_hashes_with_rpc_stream_finished;
            } else {
                ns.state = repair_state::get_full_row_hashes_started;
                master.peer_row_hash_sets(node_idx) = master.get_full_row_hashes(node, dst_cpu_id).get();
                ns.state = repair_state::get_full_row_hashes_finished;
            }
            rlogger.debug("After master.get_full_row_hashes for node {}, hash_sets={}",
                node, master.peer_row_hash_sets(node_idx).size());

            // With hashes of rows from peer node, we can figure out
            // what rows repair master is missing. Note we get missing
            // data from repair follower 1, apply the rows, then get
            // missing data from repair follower 2 and so on. We do it
            // sequentially because the rows from repair follower 1 to
            // repair master might reduce the amount of missing data
            // between repair master and repair follower 2.
            repair_hash_set set_diff = get_set_diff(master.peer_row_hash_sets(node_idx), master.working_row_hashes().get());
            // Request missing sets from peer node
            rlogger.debug("Before get_row_diff to node {}, local={}, peer={}, set_diff={}",
                    node, master.working_row_hashes().get().size(), master.peer_row_hash_sets(node_idx).size(), set_diff.size());
            // If we need to pull all rows from the peer. We can avoid
            // sending the row hashes on wire by setting needs_all_rows flag.
            auto needs_all_rows = repair_meta::needs_all_rows_t(set_diff.size() == master.peer_row_hash_sets(node_idx).size());
            if (master.use_rpc_stream()) {
                ns.state = repair_state::get_row_diff_with_rpc_stream_started;
                master.get_row_diff_with_rpc_stream(std::move(set_diff), needs_all_rows, repair_meta::update_peer_row_hash_sets::no, node, node_idx, dst_cpu_id);
                ns.state = repair_state::get_row_diff_with_rpc_stream_finished;
            } else {
                ns.state = repair_state::get_row_diff_started;
                master.get_row_diff(std::move(set_diff), needs_all_rows, node, node_idx, dst_cpu_id);
                ns.state = repair_state::get_row_diff_finished;
            }
            rlogger.debug("After get_row_diff node {}, hash_sets={}", master.myip(), master.working_row_hashes().get().size());
          } catch (...) {
            rlogger.warn("repair[{}]: get_row_diff: got error from node={}, keyspace={}, table={}, range={}, error={}",
                    _shard_task.global_repair_id.uuid(), node, _shard_task.get_keyspace(), _cf_name, _range, std::current_exception());
            throw;
          }
        }
        master.flush_rows_in_working_row_buf(get_erm(), _small_table_optimization);
        return op_status::next_step;
    }

    // Step C: Send missing rows to the peer nodes
    void send_missing_rows_to_follower_nodes(repair_meta& master) {
        // At this time, repair master contains all the rows between (_last_sync_boundary, _current_sync_boundary]
        // So we can figure out which rows peer node are missing and send the missing rows to them
        _shard_task.check_in_abort_or_shutdown();
        repair_hash_set local_row_hash_sets = master.working_row_hashes().get();
        auto sz = _all_live_peer_nodes.size();
        std::vector<repair_hash_set> set_diffs(sz);
        for (size_t idx : boost::irange(size_t(0), sz)) {
            set_diffs[idx] = get_set_diff(local_row_hash_sets, master.peer_row_hash_sets(idx));
        }
        parallel_for_each(boost::irange(size_t(0), sz), [&, this] (size_t idx) {
            auto& ns = master.all_nodes()[idx + 1];
            auto dst_cpu_id = ns.shard;
            auto needs_all_rows = repair_meta::needs_all_rows_t(master.peer_row_hash_sets(idx).empty());
            auto& set_diff = set_diffs[idx];
            rlogger.debug("Calling master.put_row_diff to node {}, set_diff={}, needs_all_rows={}", _all_live_peer_nodes[idx], set_diff.size(), needs_all_rows);
            auto& node = master.all_nodes()[idx].node;
            if (master.use_rpc_stream()) {
                ns.state = repair_state::put_row_diff_with_rpc_stream_started;
                return master.put_row_diff_with_rpc_stream(std::move(set_diff), needs_all_rows, _all_live_peer_nodes[idx], idx, *get_erm(), _small_table_optimization, dst_cpu_id).then([&ns] {
                    ns.state = repair_state::put_row_diff_with_rpc_stream_finished;
                }).handle_exception([this, &node] (std::exception_ptr ep) {
                    rlogger.warn("repair[{}]: put_row_diff: got error from node={}, keyspace={}, table={}, range={}, error={}",
                            _shard_task.global_repair_id.uuid(), node, _shard_task.get_keyspace(), _cf_name, _range, ep);
                    return make_exception_future<>(std::move(ep));
                });

            } else {
                ns.state = repair_state::put_row_diff_started;
                return master.put_row_diff(std::move(set_diff), needs_all_rows, _all_live_peer_nodes[idx], dst_cpu_id).then([&ns] {
                    ns.state = repair_state::put_row_diff_finished;
                }).handle_exception([this, &node] (std::exception_ptr ep) {
                    rlogger.warn("repair[{}]: put_row_diff: got error from node={}, keyspace={}, table={}, range={}, error={}",
                            _shard_task.global_repair_id.uuid(), node, _shard_task.get_keyspace(), _cf_name, _range, ep);
                    return make_exception_future<>(std::move(ep));
                });
            }
        }).get();
        master.stats().round_nr_slow_path++;
    }

private:
    locator::effective_replication_map_ptr get_erm() {
        return _shard_task.erm;
    }

private:
    // Update system.repair_history table
    future<> update_system_repair_table() {
        // Update repair_history table only if it is a reguar repair.
        if (_shard_task.reason() != streaming::stream_reason::repair) {
            co_return;
        }
        auto my_address = get_erm()->get_topology().my_address();
        // Update repair_history table only if all replicas have been repaired
        size_t repaired_replicas = _all_live_peer_nodes.size() + 1;
        if (_shard_task.total_rf != repaired_replicas){
            rlogger.debug("repair[{}]: Skipped to update system.repair_history total_rf={}, repaired_replicas={}, local={}, peers={}",
                    _shard_task.global_repair_id.uuid(), _shard_task.total_rf, repaired_replicas, my_address, _all_live_peer_nodes);
            co_return;
        }
        // Update repair_history table only if both hints and batchlog have been flushed.
        if (!_shard_task.hints_batchlog_flushed()) {
            co_return;
        }
        repair_service& rs = _shard_task.rs;
        std::optional<gc_clock::time_point> repair_time_opt = co_await rs.update_history(_shard_task.global_repair_id.uuid(), _table_id, _range, _start_time, _is_tablet);
        if (!repair_time_opt) {
            co_return;
        }
        auto repair_time = repair_time_opt.value();
        repair_update_system_table_request req{_shard_task.global_repair_id.uuid(), _table_id, _shard_task.get_keyspace(), _cf_name, _range, repair_time};
        auto all_nodes = _all_live_peer_nodes;
        all_nodes.push_back(my_address);
        co_await coroutine::parallel_for_each(all_nodes, [this, req] (gms::inet_address node) -> future<> {
            try {
                auto& ms = _shard_task.messaging.local();
                repair_update_system_table_response resp = co_await ser::partition_checksum_rpc_verbs::send_repair_update_system_table(&ms, netw::messaging_service::msg_addr(node), req);
                (void)resp;  // nothing to do with the response yet
                rlogger.debug("repair[{}]: Finished to update system.repair_history table of node {}", _shard_task.global_repair_id.uuid(), node);
            } catch (...) {
                rlogger.warn("repair[{}]: Failed to update system.repair_history table of node {}: {}", _shard_task.global_repair_id.uuid(), node, std::current_exception());
            }
        });
        co_return;
    }

public:
    future<> run() {
        return seastar::async([this] {
            _shard_task.check_in_abort_or_shutdown();
            auto repair_meta_id = _shard_task.rs.get_next_repair_meta_id().get();
            auto algorithm = get_common_diff_detect_algorithm(_shard_task.messaging.local(), _all_live_peer_nodes);
            auto max_row_buf_size = get_max_row_buf_size(algorithm);
            auto& cf = _shard_task.db.local().find_column_family(_table_id);
            auto& sharder = cf.get_effective_replication_map()->get_sharder(*(cf.schema()));
            auto master_node_shard_config = shard_config {
                    this_shard_id(),
                    sharder.shard_count(),
                    sharder.sharding_ignore_msb()
            };
            auto s = cf.schema();
            auto schema_version = s->version();
            bool table_dropped = false;

            auto& mem_sem = _shard_task.rs.memory_sem();
            auto max = _shard_task.rs.max_repair_memory();
            auto wanted = (_all_live_peer_nodes.size() + 1) * repair::task_manager_module::max_repair_memory_per_range;
            wanted = std::min(max, wanted);
            rlogger.trace("repair[{}]: Started to get memory budget, wanted={}, available={}, max_repair_memory={}",
                    _shard_task.global_repair_id.uuid(), wanted, mem_sem.current(), max);
            auto mem_permit = seastar::get_units(mem_sem, wanted).get();
            rlogger.trace("repair[{}]: Finished to get memory budget, wanted={}, available={}, max_repair_memory={}",
                    _shard_task.global_repair_id.uuid(), wanted, mem_sem.current(), max);

            auto permit = _shard_task.db.local().obtain_reader_permit(_shard_task.db.local().find_column_family(_table_id), "repair-meta", db::no_timeout, {}).get();

            auto compaction_time = gc_clock::now();

            repair_meta master(_shard_task.rs,
                    _shard_task.db.local().find_column_family(_table_id),
                    s,
                    std::move(permit),
                    _range,
                    algorithm,
                    max_row_buf_size,
                    _seed,
                    repair_master::yes,
                    repair_meta_id,
                    _shard_task.reason(),
                    std::move(master_node_shard_config),
                    _all_live_peer_nodes,
                    _all_live_peer_nodes.size(),
                    _all_live_peer_shards,
                    this,
                    compaction_time);
            auto auto_stop_master = defer([&master] {
                master.stop().handle_exception([] (std::exception_ptr ep) {
                    rlogger.warn("Failed auto-stopping Row Level Repair (Master): {}. Ignored.", ep);
                }).get();
            });

            rlogger.debug(">>> Started Row Level Repair (Master): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, schema_version={}, range={}, seed={}, max_row_buf_size={}",
                    master.myip(), _all_live_peer_nodes, master.repair_meta_id(), _shard_task.get_keyspace(), _cf_name, schema_version, _range, _seed, max_row_buf_size);

            std::exception_ptr ex = nullptr;
            std::vector<repair_node_state> nodes_to_stop;
            nodes_to_stop.reserve(master.all_nodes().size());
            try {
                parallel_for_each(master.all_nodes(), [&, this] (repair_node_state& ns) {
                    const auto& node = ns.node;
                    ns.state = repair_state::row_level_start_started;
                    return master.repair_row_level_start(node, _shard_task.get_keyspace(), _cf_name, _range, schema_version, _shard_task.reason(), compaction_time, ns.shard).then([&] () {
                        ns.state = repair_state::row_level_start_finished;
                        nodes_to_stop.push_back(ns);
                        ns.state = repair_state::get_estimated_partitions_started;
                        return master.repair_get_estimated_partitions(node, ns.shard).then([this, node, &ns] (uint64_t partitions) {
                            ns.state = repair_state::get_estimated_partitions_finished;
                            rlogger.trace("Get repair_get_estimated_partitions for node={}, estimated_partitions={}", node, partitions);
                            _estimated_partitions += partitions;
                        });
                    });
                }).get();

                if (!master.all_nodes().empty()) {
                    // Use the average number of partitions, instead of the sum
                    // of the partitions, as the estimated partitions in a
                    // given range. The bigger the estimated partitions, the
                    // more memory bloom filter for the sstable would consume.
                    _estimated_partitions /= master.all_nodes().size();

                    // In addition, estimate the difference between nodes is
                    // less than the specified ratio for regular repair.
                    // Underestimation will not be a big problem since those
                    // sstables produced by repair will go through off-strategy
                    // later anyway. The worst case is that we have a worse
                    // false positive ratio than expected temporarily when the
                    // sstable is still in maintenance set.
                    //
                    // To save memory and have less different conditions, we
                    // use the estimation for RBNO repair as well.

                    _estimated_partitions *= _shard_task.db.local().get_config().repair_partition_count_estimation_ratio();
                }

                parallel_for_each(master.all_nodes(), [&, this] (repair_node_state& ns) {
                    const auto& node = ns.node;
                    rlogger.trace("Get repair_set_estimated_partitions for node={}, estimated_partitions={}", node, _estimated_partitions);
                    ns.state = repair_state::set_estimated_partitions_started;
                    return master.repair_set_estimated_partitions(node, _estimated_partitions, ns.shard).then([&ns] {
                        ns.state = repair_state::set_estimated_partitions_finished;
                    });
                }).get();

                while (true) {
                    auto status = negotiate_sync_boundary(master);
                    if (status == op_status::next_round) {
                        continue;
                    } else if (status == op_status::all_done) {
                        break;
                    }
                    status = get_missing_rows_from_follower_nodes(master);
                    if (status == op_status::next_round) {
                        continue;
                    }
                    send_missing_rows_to_follower_nodes(master);
                }
            } catch (replica::no_such_column_family& e) {
                table_dropped = true;
                rlogger.warn("repair[{}]: shard={}, keyspace={}, cf={}, range={}, got error in row level repair: {}",
                        _shard_task.global_repair_id.uuid(), this_shard_id(), _shard_task.get_keyspace(), _cf_name, _range, e);
                _failed = true;
            } catch (std::exception& e) {
                rlogger.warn("repair[{}]: shard={}, keyspace={}, cf={}, range={}, got error in row level repair: {}",
                        _shard_task.global_repair_id.uuid(), this_shard_id(), _shard_task.get_keyspace(), _cf_name, _range, e);
                // In case the repair process fail, we need to call repair_row_level_stop to clean up repair followers
                _failed = true;
                ex = std::current_exception();
            }

            parallel_for_each(nodes_to_stop, [&] (repair_node_state& ns) {
                auto node = ns.node;
                master.set_repair_state(repair_state::row_level_stop_started, node);
                return master.repair_row_level_stop(node, _shard_task.get_keyspace(), _cf_name, _range, ns.shard).then([node, &master] {
                    master.set_repair_state(repair_state::row_level_stop_finished, node);
                });
            }).get();

            _shard_task.update_statistics(master.stats());
            if (_failed) {
                if (table_dropped) {
                    throw replica::no_such_column_family(_shard_task.get_keyspace(),  _cf_name);
                } else {
                    throw nested_exception(std::make_exception_ptr(std::runtime_error(format("Failed to repair for keyspace={}, cf={}, range={}", _shard_task.get_keyspace(),
                                            _cf_name, _range))), std::move(ex));
                }
            } else {
                update_system_repair_table().get();
            }
            rlogger.debug("<<< Finished Row Level Repair (Master): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, range={}, tx_hashes_nr={}, rx_hashes_nr={}, tx_row_nr={}, rx_row_nr={}, row_from_disk_bytes={}, row_from_disk_nr={}",
                    master.myip(), _all_live_peer_nodes, master.repair_meta_id(), _shard_task.get_keyspace(), _cf_name, _range, master.stats().tx_hashes_nr, master.stats().rx_hashes_nr, master.stats().tx_row_nr, master.stats().rx_row_nr, master.stats().row_from_disk_bytes, master.stats().row_from_disk_nr);
        });
    }
};

future<> repair_cf_range_row_level(repair::shard_repair_task_impl& shard_task,
        sstring cf_name, table_id table_id, dht::token_range range,
        const std::vector<gms::inet_address>& all_peer_nodes, bool small_table_optimization) {
    auto repair = row_level_repair(shard_task, std::move(cf_name), std::move(table_id), std::move(range), all_peer_nodes, small_table_optimization);
    co_return co_await repair.run();
}

class row_level_repair_gossip_helper : public gms::i_endpoint_state_change_subscriber {
    repair_service& _repair_service;
public:
    row_level_repair_gossip_helper(repair_service& repair_service) noexcept
        : _repair_service(repair_service)
    {}
    future<> remove_row_level_repair(gms::inet_address node) {
        rlogger.debug("Started to remove row level repair on all shards for node {}", node);
        try {
            co_await _repair_service.container().invoke_on_all([node] (repair_service& local_repair) {
                return local_repair.remove_repair_meta(node);
            });
            rlogger.debug("Finished to remove row level repair on all shards for node {}", node);
        } catch(...) {
            rlogger.warn("Failed to remove row level repair for node {}: {}", node, std::current_exception());
        }
    }
    virtual future<> on_join(
            gms::inet_address endpoint,
            gms::endpoint_state_ptr ep_state,
            gms::permit_id) override {
        return make_ready_future();
    }
    virtual future<> on_change(
            gms::inet_address endpoint,
            const gms::application_state_map& states,
            gms::permit_id) override {
        return make_ready_future();
    }
    virtual future<> on_alive(
            gms::inet_address endpoint,
            gms::endpoint_state_ptr state,
            gms::permit_id) override {
        return make_ready_future();
    }
    virtual future<> on_dead(
            gms::inet_address endpoint,
            gms::endpoint_state_ptr state,
            gms::permit_id) override {
        return remove_row_level_repair(endpoint);
    }
    virtual future<> on_remove(
            gms::inet_address endpoint,
            gms::permit_id) override {
        return remove_row_level_repair(endpoint);
    }
    virtual future<> on_restart(
            gms::inet_address endpoint,
            gms::endpoint_state_ptr ep_state,
            gms::permit_id) override {
        return remove_row_level_repair(endpoint);
    }
};

repair_service::repair_service(sharded<service::topology_state_machine>& tsm,
        distributed<gms::gossiper>& gossiper,
        netw::messaging_service& ms,
        sharded<replica::database>& db,
        sharded<service::storage_proxy>& sp,
        sharded<service::raft_address_map>& addr_map,
        sharded<db::batchlog_manager>& bm,
        sharded<db::system_keyspace>& sys_ks,
        sharded<db::view::view_builder>& vb,
        tasks::task_manager& tm,
        service::migration_manager& mm,
        size_t max_repair_memory)
    : _tsm(tsm)
    , _gossiper(gossiper)
    , _messaging(ms)
    , _db(db)
    , _sp(sp)
    , _addr_map(addr_map)
    , _bm(bm)
    , _sys_ks(sys_ks)
    , _view_builder(vb)
    , _repair_module(seastar::make_shared<repair::task_manager_module>(tm, *this, max_repair_memory))
    , _mm(mm)
    , _node_ops_metrics(_repair_module)
    , _max_repair_memory(max_repair_memory)
    , _memory_sem(max_repair_memory)
{
    tm.register_module("repair", _repair_module);
    if (this_shard_id() == 0) {
        _gossip_helper = make_shared<row_level_repair_gossip_helper>(*this);
        _gossiper.local().register_(_gossip_helper);
    }
}

future<> repair_service::start() {
    _load_history_done = load_history();
    co_await init_ms_handlers();
}

future<> repair_service::stop() {
  try {
    rlogger.debug("Stopping repair task module");
    co_await _repair_module->stop();
    rlogger.debug("Waiting on load_history_done");
    co_await std::move(_load_history_done);
    rlogger.debug("Uninitializing messaging service handlers");
    co_await uninit_ms_handlers();
    if (this_shard_id() == 0) {
        rlogger.debug("Unregistering gossiper helper");
        co_await _gossiper.local().unregister_(_gossip_helper);
    }
    _stopped = true;
    rlogger.info("Stopped repair_service");
  } catch (...) {
    on_fatal_internal_error(rlogger, format("Failed stopping repair_service: {}", std::current_exception()));
  }
}

repair_service::~repair_service() {
    SCYLLA_ASSERT(_stopped);
}

static shard_id repair_id_to_shard(tasks::task_id& repair_id) {
    return shard_id(repair_id.uuid().get_most_significant_bits()) % smp::count;
}

future<std::optional<gc_clock::time_point>>
repair_service::update_history(tasks::task_id repair_id, table_id table_id, dht::token_range range, gc_clock::time_point repair_time, bool is_tablet) {
    auto shard = repair_id_to_shard(repair_id);
    return container().invoke_on(shard, [repair_id, table_id, range, repair_time, is_tablet] (repair_service& rs) mutable -> future<std::optional<gc_clock::time_point>> {
        repair_history& rh = rs._finished_ranges_history[repair_id];
        if (rh.repair_time > repair_time) {
            rh.repair_time = repair_time;
        }
        auto finished_shards = ++(rh.finished_ranges[table_id][range]);
        // Tablet repair runs only on one shard
        if (finished_shards == smp::count || is_tablet) {
            // All shards have finished repair the range. Send an rpc to ask peers to update system.repair_history table
            rlogger.debug("repair[{}]: Finished range {} for table {} on all shards, updating system.repair_history table, finished_shards={}",
                    repair_id, range, table_id, finished_shards);
            co_return rh.repair_time;
        } else {
            rlogger.debug("repair[{}]: Finished range {} for table {} on all shards, updating system.repair_historytable, finished_shards={}",
                    repair_id, range, table_id, finished_shards);
            co_return std::nullopt;
        }
    });
}

future<> repair_service::cleanup_history(tasks::task_id repair_id) {
    auto shard = repair_id_to_shard(repair_id);
    return container().invoke_on(shard, [repair_id] (repair_service& rs) mutable {
        rs._finished_ranges_history.erase(repair_id);
        rlogger.debug("repair[{}]: Finished cleaning up repair_service history", repair_id);
    });
}

future<> repair_service::load_history() {
  try {
    co_await get_db().local().get_tables_metadata().parallel_for_each_table(coroutine::lambda([&] (table_id table_uuid, lw_shared_ptr<replica::table> table) -> future<> {
        auto shard = utils::uuid_xor_to_uint32(table_uuid.uuid()) % smp::count;
        if (shard != this_shard_id()) {
            co_return;
        }
        auto permit = co_await seastar::get_units(_load_parallelism_semaphore, 1);

        rlogger.info("Loading repair history for keyspace={}, table={}, table_uuid={}",
                table->schema()->ks_name(), table->schema()->cf_name(), table_uuid);
        co_await _sys_ks.local().get_repair_history(table_uuid, [this] (const auto& entry) -> future<> {
            get_repair_module().check_in_shutdown();
            auto start = entry.range_start == std::numeric_limits<int64_t>::min() ? dht::minimum_token() : dht::token::from_int64(entry.range_start);
            auto end = entry.range_end == std::numeric_limits<int64_t>::min() ? dht::maximum_token() : dht::token::from_int64(entry.range_end);
            auto range = dht::token_range(dht::token_range::bound(start, false), dht::token_range::bound(end, true));
            auto repair_time = to_gc_clock(entry.ts);
            rlogger.debug("Loading repair history for keyspace={}, table={}, table_uuid={}, repair_time={}, range={}",
                    entry.ks, entry.cf, entry.table_uuid, entry.ts, range);
            try {
                co_await get_db().invoke_on_all([table_uuid = entry.table_uuid, range, repair_time] (replica::database& local_db) {
                    auto& gc_state = local_db.get_compaction_manager().get_tombstone_gc_state();
                    gc_state.update_repair_time(table_uuid, range, repair_time);
                });
            } catch (...) {
                rlogger.warn("Failed to update repair history time for keyspace={}, table={}, range={}, repair_time={}",
                        entry.ks, entry.cf, range, repair_time);
            }
        });
    }));
  } catch (const abort_requested_exception&) {
    // Ignore
  } catch (...) {
    rlogger.warn("Failed to update repair history time: {}.  Ignored", std::current_exception());
  }
}

repair_meta_ptr repair_service::get_repair_meta(gms::inet_address from, uint32_t repair_meta_id) {
    node_repair_meta_id id{from, repair_meta_id};
    auto it = repair_meta_map().find(id);
    if (it == repair_meta_map().end()) {
        throw std::runtime_error(format("get_repair_meta: repair_meta_id {} for node {} does not exist", id.repair_meta_id, id.ip));
    } else {
        return it->second;
    }
}

future<>
repair_service::insert_repair_meta(
        const gms::inet_address& from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        dht::token_range range,
        row_level_diff_detect_algorithm algo,
        uint64_t max_row_buf_size,
        uint64_t seed,
        shard_config master_node_shard_config,
        table_schema_version schema_version,
        streaming::stream_reason reason,
        gc_clock::time_point compaction_time,
        abort_source& as) {
    return get_migration_manager().get_schema_for_write(schema_version, {from, src_cpu_id}, get_messaging(), as).then([this,
            from,
            repair_meta_id,
            range,
            algo,
            max_row_buf_size,
            seed,
            master_node_shard_config,
            reason,
            compaction_time] (schema_ptr s) {
        auto& db = get_db();
        return db.local().obtain_reader_permit(db.local().find_column_family(s->id()), "repair-meta", db::no_timeout, {}).then([s = std::move(s),
                this,
                from,
                repair_meta_id,
                range,
                algo,
                max_row_buf_size,
                seed,
                master_node_shard_config,
                reason,
                compaction_time] (reader_permit permit) mutable {
        node_repair_meta_id id{from, repair_meta_id};
        auto rm = seastar::make_shared<repair_meta>(*this,
                get_db().local().find_column_family(s->id()),
                s,
                std::move(permit),
                range,
                algo,
                max_row_buf_size,
                seed,
                repair_master::no,
                repair_meta_id,
                reason,
                std::move(master_node_shard_config),
                inet_address_vector_replica_set{from},
                compaction_time);
        rm->set_repair_state_for_local_node(repair_state::row_level_start_started);
        bool insertion = repair_meta_map().emplace(id, rm).second;
        if (!insertion) {
            rlogger.warn("insert_repair_meta: repair_meta_id {} for node {} already exists, replace existing one", id.repair_meta_id, id.ip);
            repair_meta_map()[id] = rm;
            rm->set_repair_state_for_local_node(repair_state::row_level_start_finished);
        } else {
            rlogger.debug("insert_repair_meta: Inserted repair_meta_id {} for node {}", id.repair_meta_id, id.ip);
        }
        });
    });
}

future<>
repair_service::remove_repair_meta(const gms::inet_address& from,
        uint32_t repair_meta_id,
        sstring ks_name,
        sstring cf_name,
        dht::token_range range) {
    node_repair_meta_id id{from, repair_meta_id};
    auto it = repair_meta_map().find(id);
    if (it == repair_meta_map().end()) {
        rlogger.warn("remove_repair_meta: repair_meta_id {} for node {} does not exist", id.repair_meta_id, id.ip);
        return make_ready_future<>();
    } else {
        auto rm = it->second;
        repair_meta_map().erase(it);
        rlogger.debug("remove_repair_meta: Stop repair_meta_id {} for node {} started", id.repair_meta_id, id.ip);
        return rm->stop().then([rm, id] {
            rlogger.debug("remove_repair_meta: Stop repair_meta_id {} for node {} finished", id.repair_meta_id, id.ip);
        });
    }
}

future<>
repair_service::remove_repair_meta(gms::inet_address from) {
    rlogger.debug("Remove all repair_meta for single node {}", from);
    auto repair_metas = make_lw_shared<utils::chunked_vector<repair_meta_ptr>>();
    for (auto it = repair_meta_map().begin(); it != repair_meta_map().end();) {
        if (it->first.ip == from) {
            repair_metas->push_back(it->second);
            it = repair_meta_map().erase(it);
        } else {
            it++;
        }
    }
    return parallel_for_each(*repair_metas, [repair_metas] (auto& rm) {
        return rm->stop().then([&rm] {
            rm = {};
        });
    }).then([repair_metas, from] {
        rlogger.debug("Removed all repair_meta for single node {}", from);
    });
}

future<>
repair_service::remove_repair_meta() {
    rlogger.debug("Remove all repair_meta for all nodes");
    auto repair_metas = make_lw_shared<utils::chunked_vector<repair_meta_ptr>>(
            boost::copy_range<utils::chunked_vector<repair_meta_ptr>>(repair_meta_map()
            | boost::adaptors::map_values));
    repair_meta_map().clear();
    return parallel_for_each(*repair_metas, [repair_metas] (auto& rm) {
        return rm->stop().then([&rm] {
            rm = {};
        });
    }).then([repair_metas] {
        rlogger.debug("Removed all repair_meta for all nodes");
    });
}

future<uint32_t> repair_service::get_next_repair_meta_id() {
    return container().invoke_on(0, [] (repair_service& local_repair) {
        return local_repair._next_repair_meta_id++;
    });
}

gms::inet_address repair_service::my_address() const noexcept {
    return _sp.local().my_address();
}
