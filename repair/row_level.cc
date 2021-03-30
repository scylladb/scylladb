/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "repair/repair.hh"
#include "message/messaging_service.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "mutation_fragment.hh"
#include "mutation_writer/multishard_writer.hh"
#include "dht/i_partitioner.hh"
#include "dht/sharder.hh"
#include "to_string.hh"
#include "xx_hasher.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "service/priority_manager.hh"
#include "database.hh"
#include <seastar/util/bool_class.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/coroutine.hh>
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
#include "mutation_source_metadata.hh"
#include "utils/stall_free.hh"
#include "service/migration_manager.hh"
#include "streaming/consumer.hh"
#include <seastar/core/coroutine.hh>

extern logging::logger rlogger;

struct shard_config {
    unsigned shard;
    unsigned shard_count;
    unsigned ignore_msb;
};

static bool inject_rpc_stream_error = false;

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
    explicit repair_node_state(gms::inet_address n) : node(n) { }
};

// Wraps sink and source objects for repair master or repair follower nodes.
// For repair master, it stores sink and source pair for each of the followers.
// For repair follower, it stores one sink and source pair for repair master.
template<class SinkType, class SourceType>
class sink_source_for_repair {
    uint32_t _repair_meta_id;
    using get_sink_source_fn_type = std::function<future<std::tuple<rpc::sink<SinkType>, rpc::source<SourceType>>> (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr)>;
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
    future<std::tuple<sink_type, source_type>> get_sink_source(gms::inet_address remote_node, unsigned node_idx) {
        using value_type = std::tuple<sink_type, source_type>;
        if (_sinks[node_idx] && _sources[node_idx]) {
            return make_ready_future<value_type>(value_type(_sinks[node_idx].value(), _sources[node_idx].value()));
        }
        if (_sinks[node_idx] || _sources[node_idx]) {
            return make_exception_future<value_type>(std::runtime_error(format("sink or source is missing for node {}", remote_node)));
        }
        return _fn(_repair_meta_id, netw::messaging_service::msg_addr(remote_node)).then_unpack([this, node_idx] (rpc::sink<SinkType> sink, rpc::source<SourceType> source) mutable {
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
            sm::make_derive("tx_row_nr", tx_row_nr,
                            sm::description("Total number of rows sent on this shard.")),
            sm::make_derive("rx_row_nr", rx_row_nr,
                            sm::description("Total number of rows received on this shard.")),
            sm::make_derive("tx_row_bytes", tx_row_bytes,
                            sm::description("Total bytes of rows sent on this shard.")),
            sm::make_derive("rx_row_bytes", rx_row_bytes,
                            sm::description("Total bytes of rows received on this shard.")),
            sm::make_derive("tx_hashes_nr", tx_hashes_nr,
                            sm::description("Total number of row hashes sent on this shard.")),
            sm::make_derive("rx_hashes_nr", rx_hashes_nr,
                            sm::description("Total number of row hashes received on this shard.")),
            sm::make_derive("row_from_disk_nr", row_from_disk_nr,
                            sm::description("Total number of rows read from disk on this shard.")),
            sm::make_derive("row_from_disk_bytes", row_from_disk_bytes,
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

static row_level_diff_detect_algorithm get_common_diff_detect_algorithm(netw::messaging_service& ms, const std::vector<gms::inet_address>& nodes) {
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

class decorated_key_with_hash {
public:
    dht::decorated_key dk;
    repair_hash hash;
    decorated_key_with_hash(const schema& s, dht::decorated_key key, uint64_t seed)
        : dk(key) {
        xx_hasher h(seed);
        feed_hash(h, dk.key(), s);
        hash = repair_hash(h.finalize_uint64());
    }
};

class fragment_hasher {
    const schema& _schema;
    xx_hasher& _hasher;
private:
    void consume_cell(const column_definition& col, const atomic_cell_or_collection& cell) {
        feed_hash(_hasher, col.kind);
        feed_hash(_hasher, col.id);
        feed_hash(_hasher, cell, col);
    }
public:
    explicit fragment_hasher(const schema&s, xx_hasher& h)
        : _schema(s), _hasher(h) { }

    void hash(const mutation_fragment& mf) {
        mf.visit(seastar::make_visitor(
            [&] (const clustering_row& cr) {
                consume(cr);
            },
            [&] (const static_row& sr) {
                consume(sr);
            },
            [&] (const range_tombstone& rt) {
                consume(rt);
            },
            [&] (const partition_start& ps) {
                consume(ps);
            },
            [&] (const partition_end& pe) {
                throw std::runtime_error("partition_end is not expected");
            }
        ));
    }

private:

    void consume(const tombstone& t) {
        feed_hash(_hasher, t);
    }

    void consume(const static_row& sr) {
        sr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            auto&& col = _schema.static_column_at(id);
            consume_cell(col, cell);
        });
    }

    void consume(const clustering_row& cr) {
        feed_hash(_hasher, cr.key(), _schema);
        feed_hash(_hasher, cr.tomb());
        feed_hash(_hasher, cr.marker());
        cr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            auto&& col = _schema.regular_column_at(id);
            consume_cell(col, cell);
        });
    }

    void consume(const range_tombstone& rt) {
        feed_hash(_hasher, rt.start, _schema);
        feed_hash(_hasher, rt.start_kind);
        feed_hash(_hasher, rt.tomb);
        feed_hash(_hasher, rt.end, _schema);
        feed_hash(_hasher, rt.end_kind);
    }

    void consume(const partition_start& ps) {
        feed_hash(_hasher, ps.key().key(), _schema);
        if (ps.partition_tombstone()) {
            consume(ps.partition_tombstone());
        }
    }
};

using is_dirty_on_master = bool_class<class is_dirty_on_master_tag>;

class repair_row {
    std::optional<frozen_mutation_fragment> _fm;
    lw_shared_ptr<const decorated_key_with_hash> _dk_with_hash;
    std::optional<repair_sync_boundary> _boundary;
    std::optional<repair_hash> _hash;
    is_dirty_on_master _dirty_on_master;
    lw_shared_ptr<mutation_fragment> _mf;
public:
    repair_row() = default;
    repair_row(std::optional<frozen_mutation_fragment> fm,
            std::optional<position_in_partition> pos,
            lw_shared_ptr<const decorated_key_with_hash> dk_with_hash,
            std::optional<repair_hash> hash,
            is_dirty_on_master dirty_on_master,
            lw_shared_ptr<mutation_fragment> mf = {})
            : _fm(std::move(fm))
            , _dk_with_hash(std::move(dk_with_hash))
            , _boundary(pos ? std::optional<repair_sync_boundary>(repair_sync_boundary{_dk_with_hash->dk, std::move(*pos)}) : std::nullopt)
            , _hash(std::move(hash))
            , _dirty_on_master(dirty_on_master)
            , _mf(std::move(mf)) {
    }
    lw_shared_ptr<mutation_fragment>& get_mutation_fragment_ptr () {
        return _mf;
    }
    mutation_fragment& get_mutation_fragment() {
        if (!_mf) {
            throw std::runtime_error("empty mutation_fragment");
        }
        return *_mf;
    }
    frozen_mutation_fragment& get_frozen_mutation() {
        if (!_fm) {
            throw std::runtime_error("empty frozen_mutation_fragment");
        }
        return *_fm;
    }
    const frozen_mutation_fragment& get_frozen_mutation() const {
        if (!_fm) {
            throw std::runtime_error("empty frozen_mutation_fragment");
        }
        return *_fm;
    }
    const lw_shared_ptr<const decorated_key_with_hash>& get_dk_with_hash() const {
        return _dk_with_hash;
    }
    size_t size() const {
        if (!_fm) {
            throw std::runtime_error("empty size due to empty frozen_mutation_fragment");
        }
        return _fm->representation().size();
    }
    const repair_sync_boundary& boundary() const {
        if (!_boundary) {
            throw std::runtime_error("empty repair_sync_boundary");
        }
        return *_boundary;
    }
    const repair_hash& hash() const {
        if (!_hash) {
            throw std::runtime_error("empty hash");
        }
        return *_hash;
    }
    is_dirty_on_master dirty_on_master() const {
        return _dirty_on_master;
    }
    future<> clear_gently() noexcept {
        if (_fm) {
            co_await _fm->clear_gently();
            _fm.reset();
        }
        _dk_with_hash = {};
        _boundary.reset();
        _hash.reset();
        _mf = {};
    }
};

class repair_reader {
public:
using is_local_reader = bool_class<class is_local_reader_tag>;

private:
    schema_ptr _schema;
    reader_permit _permit;
    dht::partition_range _range;
    // Used to find the range that repair master will work on
    dht::selective_token_range_sharder _sharder;
    // Seed for the repair row hashing
    uint64_t _seed;
    // Pin the table while the reader is alive.
    // Only needed for local readers, the multishard reader takes care
    // of pinning tables on used shards.
    std::optional<utils::phased_barrier::operation> _local_read_op;
    // Local reader or multishard reader to read the range
    flat_mutation_reader _reader;
    std::optional<evictable_reader_handle> _reader_handle;
    // Current partition read from disk
    lw_shared_ptr<const decorated_key_with_hash> _current_dk;
    uint64_t _reads_issued = 0;
    uint64_t _reads_finished = 0;

public:
    repair_reader(
            seastar::sharded<database>& db,
            column_family& cf,
            schema_ptr s,
            reader_permit permit,
            dht::token_range range,
            const dht::sharder& remote_sharder,
            unsigned remote_shard,
            uint64_t seed,
            is_local_reader local_reader)
            : _schema(s)
            , _permit(std::move(permit))
            , _range(dht::to_partition_range(range))
            , _sharder(remote_sharder, range, remote_shard)
            , _seed(seed)
            , _local_read_op(local_reader ? std::optional(cf.read_in_progress()) : std::nullopt)
            , _reader(nullptr) {
        if (local_reader) {
            auto ms = mutation_source([&cf] (
                        schema_ptr s,
                        reader_permit permit,
                        const dht::partition_range& pr,
                        const query::partition_slice& ps,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr,
                        streamed_mutation::forwarding,
                        mutation_reader::forwarding fwd_mr) {
                return cf.make_streaming_reader(std::move(s), std::move(permit), pr, ps, fwd_mr);
            });
            std::tie(_reader, _reader_handle) = make_manually_paused_evictable_reader(
                    std::move(ms),
                    _schema,
                    _permit,
                    _range,
                    _schema->full_slice(),
                    service::get_local_streaming_priority(),
                    {},
                    mutation_reader::forwarding::no);
        } else {
            _reader = make_multishard_streaming_reader(db, _schema, [this] {
                auto shard_range = _sharder.next();
                if (shard_range) {
                    return std::optional<dht::partition_range>(dht::to_partition_range(*shard_range));
                }
                return std::optional<dht::partition_range>();
            });
        }
    }

    future<mutation_fragment_opt>
    read_mutation_fragment() {
        ++_reads_issued;
        return _reader(db::no_timeout).then([this] (mutation_fragment_opt mfopt) {
            ++_reads_finished;
            return mfopt;
        });
    }

    future<> on_end_of_stream() noexcept {
      return _reader.close().then([this] {
        _reader = make_empty_flat_reader(_schema, _permit);
        _reader_handle.reset();
      });
    }

    future<> close() noexcept {
      return _reader.close().then([this] {
        _reader_handle.reset();
      });
    }

    lw_shared_ptr<const decorated_key_with_hash>& get_current_dk() {
        return _current_dk;
    }

    void set_current_dk(const dht::decorated_key& key) {
        _current_dk = make_lw_shared<const decorated_key_with_hash>(*_schema, key, _seed);
    }

    void clear_current_dk() {
        _current_dk = {};
    }

    void check_current_dk() {
        if (!_current_dk) {
            throw std::runtime_error("Current partition_key is unknown");
        }
    }

    void pause() {
        if (_reader_handle) {
            _reader_handle->pause();
        }
    }
};

class repair_writer : public enable_lw_shared_from_this<repair_writer> {
    schema_ptr _schema;
    reader_permit _permit;
    uint64_t _estimated_partitions;
    std::optional<future<>> _writer_done;
    std::optional<queue_reader_handle> _mq;
    // Current partition written to disk
    lw_shared_ptr<const decorated_key_with_hash> _current_dk_written_to_sstable;
    // Is current partition still open. A partition is opened when a
    // partition_start is written and is closed when a partition_end is
    // written.
    bool _partition_opened;
    streaming::stream_reason _reason;
    named_semaphore _sem{1, named_semaphore_exception_factory{"repair_writer"}};
public:
    repair_writer(
            schema_ptr schema,
            reader_permit permit,
            uint64_t estimated_partitions,
            streaming::stream_reason reason)
            : _schema(std::move(schema))
            , _permit(std::move(permit))
            , _estimated_partitions(estimated_partitions)
            , _reason(reason) {
    }

    future<> write_start_and_mf(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf)  {
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

    static sstables::offstrategy is_offstrategy_supported(streaming::stream_reason reason) {
        static const std::unordered_set<streaming::stream_reason> operations_supported = {
            streaming::stream_reason::bootstrap,
            streaming::stream_reason::replace,
            streaming::stream_reason::removenode,
            streaming::stream_reason::decommission,
            streaming::stream_reason::repair,
        };
        return sstables::offstrategy(operations_supported.contains(reason));
    }

    void create_writer(sharded<database>& db, sharded<db::system_distributed_keyspace>& sys_dist_ks, sharded<db::view::view_update_generator>& view_update_gen) {
        if (_writer_done) {
            return;
        }
        table& t = db.local().find_column_family(_schema->id());
        auto [queue_reader, queue_handle] = make_queue_reader(_schema, _permit);
        _mq = std::move(queue_handle);
        auto writer = shared_from_this();
        _writer_done = mutation_writer::distribute_reader_and_consume_on_shards(_schema, std::move(queue_reader),
                streaming::make_streaming_consumer("repair", db, sys_dist_ks, view_update_gen, _estimated_partitions, _reason, is_offstrategy_supported(_reason)),
        t.stream_in_progress()).then([writer] (uint64_t partitions) {
            rlogger.debug("repair_writer: keyspace={}, table={}, managed to write partitions={} to sstable",
                writer->_schema->ks_name(), writer->_schema->cf_name(), partitions);
        }).handle_exception([writer] (std::exception_ptr ep) {
            rlogger.warn("repair_writer: keyspace={}, table={}, multishard_writer failed: {}",
                    writer->_schema->ks_name(), writer->_schema->cf_name(), ep);
            writer->_mq->abort(ep);
            return make_exception_future<>(std::move(ep));
        });
    }

    future<> write_partition_end() {
        if (_partition_opened) {
            return _mq->push(mutation_fragment(*_schema, _permit, partition_end())).then([this] {
                _partition_opened = false;
            });
        }
        return make_ready_future<>();
    }

    future<> do_write(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf) {
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

    future<> write_end_of_stream() {
        if (_mq) {
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

    future<> do_wait_for_writer_done() {
        if (_writer_done) {
            return std::move(*(_writer_done));
        } else {
            return make_ready_future<>();
        }
    }

    future<> wait_for_writer_done() {
        return when_all_succeed(write_end_of_stream(), do_wait_for_writer_done()).discard_result().handle_exception(
                [this] (std::exception_ptr ep) {
            rlogger.warn("repair_writer: keyspace={}, table={}, wait_for_writer_done failed: {}",
                    _schema->ks_name(), _schema->cf_name(), ep);
            return make_exception_future<>(std::move(ep));
        });
    }

    named_semaphore& sem() {
        return _sem;
    }
};

class repair_meta;
class repair_meta_tracker;
class row_level_repair;

static void add_to_repair_meta_for_masters(repair_meta& rm);
static void add_to_repair_meta_for_followers(repair_meta& rm);

class repair_meta {
    friend repair_meta_tracker;
public:
    using repair_master = bool_class<class repair_master_tag>;
    using update_working_row_buf = bool_class<class update_working_row_buf_tag>;
    using update_peer_row_hash_sets = bool_class<class update_peer_row_hash_sets_tag>;
    using needs_all_rows_t = bool_class<class needs_all_rows_tag>;
    using msg_addr = netw::messaging_service::msg_addr;
    using tracker_link_type = boost::intrusive::list_member_hook<bi::link_mode<boost::intrusive::auto_unlink>>;
private:
    seastar::sharded<database>& _db;
    seastar::sharded<netw::messaging_service>& _messaging;
    seastar::sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    seastar::sharded<db::view::view_update_generator>& _view_update_generator;
    column_family& _cf;
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
    gms::inet_address _myip;
    uint32_t _repair_meta_id;
    streaming::stream_reason _reason;
    // Repair master's sharding configuration
    shard_config _master_node_shard_config;
    // sharding info of repair master
    dht::sharder _remote_sharder;
    bool _same_sharding_config = false;
    uint64_t _estimated_partitions = 0;
    // For repair master nr peers is the number of repair followers, for repair
    // follower nr peers is always one because repair master is the only peer.
    size_t _nr_peer_nodes= 1;
    repair_stats _stats;
    repair_reader _repair_reader;
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
    std::optional<shared_promise<>> _stop_promise;
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
        return _myip;
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
    repair_meta(
            seastar::sharded<database>& db,
            seastar::sharded<netw::messaging_service>& ms,
            seastar::sharded<db::system_distributed_keyspace>& sys_dist_ks,
            seastar::sharded<db::view::view_update_generator>& view_update_generator,
            column_family& cf,
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
            std::vector<gms::inet_address> all_live_peer_nodes,
            size_t nr_peer_nodes = 1,
            row_level_repair* row_level_repair_ptr = nullptr)
            : _db(db)
            , _messaging(ms)
            , _sys_dist_ks(sys_dist_ks)
            , _view_update_generator(view_update_generator)
            , _cf(cf)
            , _schema(s)
            , _permit(std::move(permit))
            , _range(range)
            , _cmp(repair_sync_boundary::tri_compare(*_schema))
            , _algo(algo)
            , _max_row_buf_size(max_row_buf_size)
            , _seed(seed)
            , _repair_master(master)
            , _myip(utils::fb_utilities::get_broadcast_address())
            , _repair_meta_id(repair_meta_id)
            , _reason(reason)
            , _master_node_shard_config(std::move(master_node_shard_config))
            , _remote_sharder(make_remote_sharder())
            , _same_sharding_config(is_same_sharding_config())
            , _nr_peer_nodes(nr_peer_nodes)
            , _repair_reader(
                    _db,
                    _cf,
                    _schema,
                    _permit,
                    _range,
                    _remote_sharder,
                    _master_node_shard_config.shard,
                    _seed,
                    repair_reader::is_local_reader(_repair_master || _same_sharding_config)
              )
            , _repair_writer(make_lw_shared<repair_writer>(_schema, _permit, _estimated_partitions, _reason))
            , _sink_source_for_get_full_row_hashes(_repair_meta_id, _nr_peer_nodes,
                    [&ms] (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr) {
                        return ms.local().make_sink_and_source_for_repair_get_full_row_hashes_with_rpc_stream(repair_meta_id, addr);
                })
            , _sink_source_for_get_row_diff(_repair_meta_id, _nr_peer_nodes,
                    [&ms] (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr) {
                        return ms.local().make_sink_and_source_for_repair_get_row_diff_with_rpc_stream(repair_meta_id, addr);
                })
            , _sink_source_for_put_row_diff(_repair_meta_id, _nr_peer_nodes,
                    [&ms] (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr) {
                        return ms.local().make_sink_and_source_for_repair_put_row_diff_with_rpc_stream(repair_meta_id, addr);
                })
            , _row_level_repair_ptr(row_level_repair_ptr)
            {
            if (master) {
                add_to_repair_meta_for_masters(*this);
            } else {
                add_to_repair_meta_for_followers(*this);
            }
            _all_node_states.push_back(repair_node_state(utils::fb_utilities::get_broadcast_address()));
            for (auto& node : all_live_peer_nodes) {
                _all_node_states.push_back(repair_node_state(node));
            }
    }

public:
    future<> clear_gently() noexcept {
        co_await utils::clear_gently(_peer_row_hash_sets);
        co_await utils::clear_gently(_working_row_buf);
        co_await utils::clear_gently(_row_buf);
    }

    future<> stop() {
        // Handle deferred stop
        if (_stop_promise) {
            if (!_stop_promise->available()) {
                rlogger.debug("repair_meta::stop: wait on previous stop");
            }
            return _stop_promise->get_shared_future();
        }
        _stop_promise.emplace();
        auto ret = _stop_promise->get_shared_future();
        auto gate_future = _gate.close();
        auto f1 = _sink_source_for_get_full_row_hashes.close();
        auto f2 = _sink_source_for_get_row_diff.close();
        auto f3 = _sink_source_for_put_row_diff.close();
        rlogger.debug("repair_meta::stop");
        // move to background.  waited on via _stop_promise->get_future.
        (void)when_all_succeed(std::move(gate_future), std::move(f1), std::move(f2), std::move(f3)).discard_result().finally([this] {
            return _repair_writer->wait_for_writer_done().finally([this] {
                return close().then([this] {
                    return clear_gently();
                });
            });
        }).then_wrapped([this] (future<> f) {
            if (f.failed()) {
                _stop_promise->set_exception(f.get_exception());
            } else {
                _stop_promise->set_value();
            }
        });
        return ret;
    }

    static std::unordered_map<node_repair_meta_id, lw_shared_ptr<repair_meta>>& repair_meta_map();

    static lw_shared_ptr<repair_meta> get_repair_meta(gms::inet_address from, uint32_t repair_meta_id) {
        node_repair_meta_id id{from, repair_meta_id};
        auto it = repair_meta_map().find(id);
        if (it == repair_meta_map().end()) {
            throw std::runtime_error(format("get_repair_meta: repair_meta_id {} for node {} does not exist", id.repair_meta_id, id.ip));
        } else {
            return it->second;
        }
    }

    static future<>
    insert_repair_meta(repair_service& repair,
            const gms::inet_address& from,
            uint32_t src_cpu_id,
            uint32_t repair_meta_id,
            dht::token_range range,
            row_level_diff_detect_algorithm algo,
            uint64_t max_row_buf_size,
            uint64_t seed,
            shard_config master_node_shard_config,
            table_schema_version schema_version,
            streaming::stream_reason reason) {
        return repair.get_migration_manager().get_schema_for_write(schema_version, {from, src_cpu_id}, repair.get_messaging()).then([&repair,
                from,
                repair_meta_id,
                range,
                algo,
                max_row_buf_size,
                seed,
                master_node_shard_config,
                schema_version,
                reason] (schema_ptr s) {
            auto& db = repair.get_db();
            auto& cf = db.local().find_column_family(s->id());
          return db.local().obtain_reader_permit(cf, "repair-meta", db::no_timeout).then([s = std::move(s),
                    &db,
                    &cf,
                    &repair,
                    from,
                    repair_meta_id,
                    range,
                    algo,
                    max_row_buf_size,
                    seed,
                    master_node_shard_config,
                    schema_version,
                    reason] (reader_permit permit) mutable {
            node_repair_meta_id id{from, repair_meta_id};
            auto rm = make_lw_shared<repair_meta>(db,
                    repair.get_messaging().container(),
                    repair.get_sys_dist_ks(),
                    repair.get_view_update_generator(),
                    cf,
                    s,
                    std::move(permit),
                    range,
                    algo,
                    max_row_buf_size,
                    seed,
                    repair_meta::repair_master::no,
                    repair_meta_id,
                    reason,
                    std::move(master_node_shard_config),
                    std::vector<gms::inet_address>{from});
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

    static future<>
    remove_repair_meta(const gms::inet_address& from,
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

    static future<>
    remove_repair_meta(gms::inet_address from) {
        rlogger.debug("Remove all repair_meta for single node {}", from);
        auto repair_metas = make_lw_shared<utils::chunked_vector<lw_shared_ptr<repair_meta>>>();
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

    static future<>
    remove_repair_meta() {
        rlogger.debug("Remove all repair_meta for all nodes");
        auto repair_metas = make_lw_shared<utils::chunked_vector<lw_shared_ptr<repair_meta>>>(
                boost::copy_range<utils::chunked_vector<lw_shared_ptr<repair_meta>>>(repair_meta_map()
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

    static future<uint32_t> get_next_repair_meta_id() {
        return smp::submit_to(0, [] {
            static uint32_t next_id = 0;
            return next_id++;
        });
    }

    // Must run inside a seastar thread
    static repair_hash_set
    get_set_diff(const repair_hash_set& x, const repair_hash_set& y) {
        repair_hash_set set_diff;
        // Note std::set_difference needs x and y are sorted.
        std::copy_if(x.begin(), x.end(), std::inserter(set_diff, set_diff.end()),
                [&y] (auto& item) { thread::maybe_yield(); return !y.contains(item); });
        return set_diff;
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
        return do_with(repair_hash_set(), [this] (repair_hash_set& hashes) {
            return do_for_each(_working_row_buf, [&hashes] (repair_row& r) {
                hashes.emplace(r.hash());
            }).then([&hashes] {
                return std::move(hashes);
            });
        });
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
        return _repair_reader.close();
    }

private:
    future<uint64_t> do_estimate_partitions_on_all_shards() {
        return estimate_partitions(_db, _schema->ks_name(), _schema->cf_name(), _range);
    }

    future<uint64_t> do_estimate_partitions_on_local_shard() {
        return do_with(_cf.get_sstables(), uint64_t(0), [this] (lw_shared_ptr<const sstable_list>& sstables, uint64_t& partition_count) {
            return do_for_each(*sstables, [this, &partition_count] (const sstables::shared_sstable& sst) mutable {
                partition_count += sst->estimated_keys_for_range(_range);
            }).then([&partition_count] {
                return partition_count;
            });
        });
    }

    future<uint64_t> get_estimated_partitions() {
      return with_gate(_gate, [this] {
        if (_repair_master || _same_sharding_config) {
            return do_estimate_partitions_on_local_shard();
        } else {
            return do_with(dht::selective_token_range_sharder(_remote_sharder, _range, _master_node_shard_config.shard), uint64_t(0), [this] (auto& sharder, auto& partitions_sum) mutable {
                return repeat([this, &sharder, &partitions_sum] () mutable {
                    auto shard_range = sharder.next();
                    if (shard_range) {
                        return do_estimate_partitions_on_all_shards().then([this, &partitions_sum] (uint64_t partitions) mutable {
                            partitions_sum += partitions;
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    } else {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                }).then([&partitions_sum] {
                    return partitions_sum;
                });
            });
        }
      });
    }

    future<> set_estimated_partitions(uint64_t estimated_partitions) {
        return with_gate(_gate, [this, estimated_partitions] {
            _estimated_partitions = estimated_partitions;
        });
    }

    dht::sharder make_remote_sharder() {
        return dht::sharder(_master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb);
    }

    bool is_same_sharding_config() {
        rlogger.debug("is_same_sharding_config: remote_shard={}, remote_shard_count={}, remote_ignore_msb={}",
                _master_node_shard_config.shard, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb);
        return _schema->get_sharder().shard_count() == _master_node_shard_config.shard_count
               && _schema->get_sharder().sharding_ignore_msb() == _master_node_shard_config.ignore_msb
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

    repair_hash do_hash_for_mf(const decorated_key_with_hash& dk_with_hash, const mutation_fragment& mf) {
        xx_hasher h(_seed);
        fragment_hasher fh(*_schema, h);
        fh.hash(mf);
        feed_hash(h, dk_with_hash.hash.hash);
        return repair_hash(h.finalize_uint64());
    }

    stop_iteration handle_mutation_fragment(mutation_fragment& mf, size_t& cur_size, size_t& new_rows_size, std::list<repair_row>& cur_rows) {
        if (mf.is_partition_start()) {
            auto& start = mf.as_partition_start();
            _repair_reader.set_current_dk(start.key());
            if (!start.partition_tombstone()) {
                // Ignore partition_start with empty partition tombstone
                return stop_iteration::no;
            }
        } else if (mf.is_end_of_partition()) {
            _repair_reader.clear_current_dk();
            return stop_iteration::no;
        }
        auto hash = do_hash_for_mf(*_repair_reader.get_current_dk(), mf);
        repair_row r(freeze(*_schema, mf), position_in_partition(mf.position()), _repair_reader.get_current_dk(), hash, is_dirty_on_master::no);
        rlogger.trace("Reading: r.boundary={}, r.hash={}", r.boundary(), r.hash());
        _metrics.row_from_disk_nr++;
        _metrics.row_from_disk_bytes += r.size();
        cur_size += r.size();
        new_rows_size += r.size();
        cur_rows.push_back(std::move(r));
        return stop_iteration::no;
    }

    // Read rows from sstable until the size of rows exceeds _max_row_buf_size  - current_size
    // This reads rows from where the reader left last time into _row_buf
    // _current_sync_boundary or _last_sync_boundary have no effect on the reader neither.
    future<std::tuple<std::list<repair_row>, size_t>>
    read_rows_from_disk(size_t cur_size) {
        using value_type = std::tuple<std::list<repair_row>, size_t>;
        return do_with(cur_size, size_t(0), std::list<repair_row>(), [this] (size_t& cur_size, size_t& new_rows_size, std::list<repair_row>& cur_rows) {
            return repeat([this, &cur_size, &cur_rows, &new_rows_size] () mutable {
                if (cur_size >= _max_row_buf_size) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                _gate.check();
                return _repair_reader.read_mutation_fragment().then([this, &cur_size, &new_rows_size, &cur_rows] (mutation_fragment_opt mfopt) mutable {
                    if (!mfopt) {
                      return _repair_reader.on_end_of_stream().then([] {
                        return stop_iteration::yes;
                      });
                    }
                    return make_ready_future<stop_iteration>(handle_mutation_fragment(*mfopt, cur_size, new_rows_size, cur_rows));
                });
            }).then_wrapped([this, &cur_rows, &new_rows_size] (future<> fut) mutable {
                if (fut.failed()) {
                    return make_exception_future<value_type>(fut.get_exception()).finally([this] {
                        return _repair_reader.on_end_of_stream();
                    });
                }
                _repair_reader.pause();
                return make_ready_future<value_type>(value_type(std::move(cur_rows), new_rows_size));
            });
        });
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
        auto f = make_ready_future<>();
        if (skipped_sync_boundary) {
            _current_sync_boundary = skipped_sync_boundary;
            f = clear_row_buf();
        }
        // Here is the place we update _last_sync_boundary
        rlogger.trace("SET _last_sync_boundary from {} to {}", _last_sync_boundary, _current_sync_boundary);
        _last_sync_boundary = _current_sync_boundary;
      return f.then([this, sb = std::move(skipped_sync_boundary)] () mutable {
       return clear_working_row_buf().then([this, sb = sb] () mutable {
        return row_buf_size().then([this, sb = std::move(sb)] (size_t cur_size) {
            return read_rows_from_disk(cur_size).then_unpack([this, sb = std::move(sb)] (std::list<repair_row> new_rows, size_t new_rows_size) mutable {
                size_t new_rows_nr = new_rows.size();
                _row_buf.splice(_row_buf.end(), new_rows);
                return row_buf_csum().then([this, new_rows_size, new_rows_nr, sb = std::move(sb)] (repair_hash row_buf_combined_hash) {
                    return row_buf_size().then([this, new_rows_size, new_rows_nr, row_buf_combined_hash, sb = std::move(sb)] (size_t row_buf_bytes) {
                        std::optional<repair_sync_boundary> sb_max;
                        if (!_row_buf.empty()) {
                            sb_max = _row_buf.back().boundary();
                        }
                        rlogger.debug("get_sync_boundary: Got nr={} rows, sb_max={}, row_buf_size={}, repair_hash={}, skipped_sync_boundary={}",
                                new_rows_nr, sb_max, row_buf_bytes, row_buf_combined_hash, sb);
                        return get_sync_boundary_response{sb_max, row_buf_combined_hash, row_buf_bytes, new_rows_size, new_rows_nr};
                    });
                });
            });
        });
       });
      });
    }

    future<> move_row_buf_to_working_row_buf() {
        if (_cmp(_row_buf.back().boundary(), *_current_sync_boundary) <= 0) {
            // Fast path
            _working_row_buf.swap(_row_buf);
            return make_ready_future<>();
        }
        return do_with(_row_buf.rbegin(), [this, sz = _row_buf.size()] (auto& it) {
            // Move the rows > _current_sync_boundary to _working_row_buf
            // Delete the rows > _current_sync_boundary from _row_buf
            // Swap _working_row_buf and _row_buf so that _working_row_buf
            // contains rows within (_last_sync_boundary,
            // _current_sync_boundary], _row_buf contains rows wthin
            // (_current_sync_boundary, ...]
            return repeat([this, &it, sz] () {
                if (it == _row_buf.rend()) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                repair_row& r = *(it++);
                if (_cmp(r.boundary(), *_current_sync_boundary) > 0) {
                    _working_row_buf.push_front(std::move(r));
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                }
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }).then([this, sz] {
                _row_buf.resize(_row_buf.size() - _working_row_buf.size());
                _row_buf.swap(_working_row_buf);
                if (sz != _working_row_buf.size() + _row_buf.size()) {
                    throw std::runtime_error(format("incorrect row_buf and working_row_buf size, before={}, after={} + {}",
                            sz, _working_row_buf.size(), _row_buf.size()));
                }
            });
        });
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
            return do_for_each(_working_row_buf, [this, &rows] (const repair_row& r) {
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
            return do_for_each(_working_row_buf, [this, &set_diff, &rows] (const repair_row& r) {
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
                _repair_writer->create_writer(_db, _sys_dist_ks, _view_update_generator);
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
        auto row_diff = to_repair_rows_list(std::move(rows)).get0();
        auto sz = get_repair_rows_size(row_diff).get0();
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
    }
public:
    // Must run inside a seastar thread
    void flush_rows_in_working_row_buf() {
        if (_dirty_on_master) {
            _dirty_on_master = is_dirty_on_master::no;
        } else {
            return;
        }
        auto cmp = position_in_partition::tri_compare(*_schema);
        lw_shared_ptr<mutation_fragment> last_mf;
        lw_shared_ptr<const decorated_key_with_hash> last_dk;
        for (auto& r : _working_row_buf) {
            thread::maybe_yield();
            if (!r.dirty_on_master()) {
                continue;
            }
            _repair_writer->create_writer(_db, _sys_dist_ks, _view_update_generator);
            auto mf = r.get_mutation_fragment_ptr();
            const auto& dk = r.get_dk_with_hash()->dk;
            if (last_mf && last_dk &&
                    cmp(last_mf->position(), mf->position()) == 0 &&
                    dk.tri_compare(*_schema, last_dk->dk) == 0 &&
                    last_mf->mergeable_with(*mf)) {
                last_mf->apply(*_schema, std::move(*mf));
            } else {
                if (last_mf && last_dk) {
                    _repair_writer->do_write(std::move(last_dk), std::move(*last_mf)).get();
                }
                last_mf = mf;
                last_dk = r.get_dk_with_hash();
            }
        }
        if (last_mf && last_dk) {
            _repair_writer->do_write(std::move(last_dk), std::move(*last_mf)).get();
        }
    }

private:
    future<>
    apply_rows_on_follower(repair_rows_on_wire rows) {
        if (rows.empty()) {
            return make_ready_future<>();
        }
        return to_repair_rows_list(std::move(rows)).then([this] (std::list<repair_row> row_diff) {
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

    future<std::list<repair_row>> to_repair_rows_list(repair_rows_on_wire rows) {
        return do_with(std::move(rows), std::list<repair_row>(), lw_shared_ptr<const decorated_key_with_hash>(), lw_shared_ptr<mutation_fragment>(), position_in_partition::tri_compare(*_schema),
          [this] (repair_rows_on_wire& rows, std::list<repair_row>& row_list, lw_shared_ptr<const decorated_key_with_hash>& dk_ptr, lw_shared_ptr<mutation_fragment>& last_mf, position_in_partition::tri_compare& cmp) mutable {
            return do_for_each(rows, [this, &dk_ptr, &row_list, &last_mf, &cmp] (partition_key_and_mutation_fragments& x) mutable {
                dht::decorated_key dk = dht::decorate_key(*_schema, x.get_key());
                if (!(dk_ptr && dk_ptr->dk.equal(*_schema, dk))) {
                    dk_ptr = make_lw_shared<const decorated_key_with_hash>(*_schema, dk, _seed);
                }
                if (_repair_master) {
                    return do_for_each(x.get_mutation_fragments(), [this, &dk_ptr, &row_list] (frozen_mutation_fragment& fmf) mutable {
                        _metrics.rx_row_nr += 1;
                        _metrics.rx_row_bytes += fmf.representation().size();
                        // Keep the mutation_fragment in repair_row as an
                        // optimization to avoid unfreeze again when
                        // mutation_fragment is needed by _repair_writer.do_write()
                        // to apply the repair_row to disk
                        auto mf = make_lw_shared<mutation_fragment>(fmf.unfreeze(*_schema, _permit));
                        auto hash = do_hash_for_mf(*dk_ptr, *mf);
                        position_in_partition pos(mf->position());
                        row_list.push_back(repair_row(std::move(fmf), std::move(pos), dk_ptr, std::move(hash), is_dirty_on_master::yes, std::move(mf)));
                    });
                } else {
                    last_mf = {};
                    return do_for_each(x.get_mutation_fragments(), [this, &dk_ptr, &row_list, &last_mf, &cmp] (frozen_mutation_fragment& fmf) mutable {
                        _metrics.rx_row_nr += 1;
                        _metrics.rx_row_bytes += fmf.representation().size();
                        auto mf = make_lw_shared<mutation_fragment>(fmf.unfreeze(*_schema, _permit));
                        // If the mutation_fragment has the same position as
                        // the last mutation_fragment, it means they are the
                        // same row with different contents. We can not feed
                        // such rows into the sstable writer. Instead we apply
                        // the mutation_fragment into the previous one.
                        if (last_mf && cmp(last_mf->position(), mf->position()) == 0 && last_mf->mergeable_with(*mf)) {
                            last_mf->apply(*_schema, std::move(*mf));
                        } else {
                            last_mf = mf;
                            // On repair follower node, only decorated_key_with_hash and the mutation_fragment inside repair_row are used.
                            row_list.push_back(repair_row({}, {}, dk_ptr, {}, is_dirty_on_master::no, std::move(mf)));
                        }
                    });
                }
            }).then([&row_list] {
                return std::move(row_list);
            });
        });
    }

public:
    // RPC API
    // Return the hashes of the rows in _working_row_buf
    future<repair_hash_set>
    get_full_row_hashes(gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return get_full_row_hashes_handler();
        }
        return _messaging.local().send_repair_get_full_row_hashes(msg_addr(remote_node),
                _repair_meta_id).then([this, remote_node] (repair_hash_set hashes) {
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
    get_full_row_hashes_with_rpc_stream(gms::inet_address remote_node, unsigned node_idx) {
        if (remote_node == _myip) {
            return get_full_row_hashes_handler();
        }
        auto current_hashes = make_lw_shared<repair_hash_set>();
        return _sink_source_for_get_full_row_hashes.get_sink_source(remote_node, node_idx).then_unpack(
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
    get_combined_row_hash(std::optional<repair_sync_boundary> common_sync_boundary, gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return get_combined_row_hash_handler(common_sync_boundary);
        }
        return _messaging.local().send_repair_get_combined_row_hash(msg_addr(remote_node),
                _repair_meta_id, common_sync_boundary).then([this] (get_combined_row_hash_response resp) {
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
        // retransmission at messaging_service level, so no message will be retransmited.
        rlogger.trace("Calling get_combined_row_hash_handler");
        return with_gate(_gate, [this, common_sync_boundary = std::move(common_sync_boundary)] () mutable {
            _cf.update_off_strategy_trigger();
            return request_row_hashes(common_sync_boundary);
        });
    }

    // RPC API
    future<>
    repair_row_level_start(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range, table_schema_version schema_version, streaming::stream_reason reason) {
        if (remote_node == _myip) {
            return make_ready_future<>();
        }
        stats().rpc_call_nr++;
        // Even though remote partitioner name is ignored in the current version of
        // repair, we still have to send something to keep compatibility with nodes
        // that run older versions. This will make it possible to run mixed cluster.
        // Murmur3 is appropriate because that's the only supported partitioner at
        // the time this change is introduced.
        sstring remote_partitioner_name = "org.apache.cassandra.dht.Murmur3Partitioner";
        return _messaging.local().send_repair_row_level_start(msg_addr(remote_node),
                _repair_meta_id, ks_name, cf_name, std::move(range), _algo, _max_row_buf_size, _seed,
                _master_node_shard_config.shard, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb,
                remote_partitioner_name, std::move(schema_version), reason).then([ks_name, cf_name] (rpc::optional<repair_row_level_start_response> resp) {
            if (resp && resp->status == repair_row_level_start_status::no_such_column_family) {
                return make_exception_future<>(no_such_column_family(ks_name, cf_name));
            } else {
                return make_ready_future<>();
            }
        });
    }

    // RPC handler
    static future<repair_row_level_start_response>
    repair_row_level_start_handler(repair_service& repair, gms::inet_address from, uint32_t src_cpu_id, uint32_t repair_meta_id, sstring ks_name, sstring cf_name,
            dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size,
            uint64_t seed, shard_config master_node_shard_config, table_schema_version schema_version, streaming::stream_reason reason) {
        rlogger.debug(">>> Started Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, schema_version={}, range={}, seed={}, max_row_buf_siz={}",
            utils::fb_utilities::get_broadcast_address(), from, repair_meta_id, ks_name, cf_name, schema_version, range, seed, max_row_buf_size);
        return insert_repair_meta(repair, from, src_cpu_id, repair_meta_id, std::move(range), algo, max_row_buf_size, seed, std::move(master_node_shard_config), std::move(schema_version), reason).then([] {
            return repair_row_level_start_response{repair_row_level_start_status::ok};
        }).handle_exception_type([] (no_such_column_family&) {
            return repair_row_level_start_response{repair_row_level_start_status::no_such_column_family};
        });
    }

    // RPC API
    future<> repair_row_level_stop(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range) {
        if (remote_node == _myip) {
            return stop();
        }
        stats().rpc_call_nr++;
        return _messaging.local().send_repair_row_level_stop(msg_addr(remote_node),
                _repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range));
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

    // RPC API
    future<uint64_t> repair_get_estimated_partitions(gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return get_estimated_partitions();
        }
        stats().rpc_call_nr++;
        return _messaging.local().send_repair_get_estimated_partitions(msg_addr(remote_node), _repair_meta_id);
    }


    // RPC handler
    static future<uint64_t> repair_get_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id) {
        auto rm = get_repair_meta(from, repair_meta_id);
        rm->set_repair_state_for_local_node(repair_state::get_estimated_partitions_started);
        return rm->get_estimated_partitions().then([rm] (uint64_t partitions) {
            rm->set_repair_state_for_local_node(repair_state::get_estimated_partitions_finished);
            return partitions;
        });
    }

    // RPC API
    future<> repair_set_estimated_partitions(gms::inet_address remote_node, uint64_t estimated_partitions) {
        if (remote_node == _myip) {
            return set_estimated_partitions(estimated_partitions);
        }
        stats().rpc_call_nr++;
        return _messaging.local().send_repair_set_estimated_partitions(msg_addr(remote_node), _repair_meta_id, estimated_partitions);
    }


    // RPC handler
    static future<> repair_set_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id, uint64_t estimated_partitions) {
        auto rm = get_repair_meta(from, repair_meta_id);
        rm->set_repair_state_for_local_node(repair_state::set_estimated_partitions_started);
        return rm->set_estimated_partitions(estimated_partitions).then([rm] {
            rm->set_repair_state_for_local_node(repair_state::set_estimated_partitions_finished);
        });
    }

    // RPC API
    // Return the largest sync point contained in the _row_buf , current _row_buf checksum, and the _row_buf size
    future<get_sync_boundary_response>
    get_sync_boundary(gms::inet_address remote_node, std::optional<repair_sync_boundary> skipped_sync_boundary) {
        if (remote_node == _myip) {
            return get_sync_boundary_handler(skipped_sync_boundary);
        }
        stats().rpc_call_nr++;
        return _messaging.local().send_repair_get_sync_boundary(msg_addr(remote_node), _repair_meta_id, skipped_sync_boundary);
    }

    // RPC handler
    future<get_sync_boundary_response>
    get_sync_boundary_handler(std::optional<repair_sync_boundary> skipped_sync_boundary) {
        return with_gate(_gate, [this, skipped_sync_boundary = std::move(skipped_sync_boundary)] () mutable {
            _cf.update_off_strategy_trigger();
            return get_sync_boundary(std::move(skipped_sync_boundary));
        });
    }

    // RPC API
    // Return rows in the _working_row_buf with hash within the given sef_diff
    // Must run inside a seastar thread
    void get_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node, unsigned node_idx) {
        if (needs_all_rows || !set_diff.empty()) {
            if (remote_node == _myip) {
                return;
            }
            if (needs_all_rows) {
                set_diff.clear();
            } else {
                stats().tx_hashes_nr += set_diff.size();
                _metrics.tx_hashes_nr += set_diff.size();
            }
            stats().rpc_call_nr++;
            repair_rows_on_wire rows = _messaging.local().send_repair_get_row_diff(msg_addr(remote_node),
                    _repair_meta_id, std::move(set_diff), bool(needs_all_rows)).get0();
            if (!rows.empty()) {
                apply_rows_on_master_in_thread(std::move(rows), remote_node, update_working_row_buf::yes, update_peer_row_hash_sets::no, node_idx);
            }
        }
    }

    // Must run inside a seastar thread
    void get_row_diff_and_update_peer_row_hash_sets(gms::inet_address remote_node, unsigned node_idx) {
        if (remote_node == _myip) {
            return;
        }
        stats().rpc_call_nr++;
        repair_rows_on_wire rows = _messaging.local().send_repair_get_row_diff(msg_addr(remote_node),
                _repair_meta_id, {}, bool(needs_all_rows_t::yes)).get0();
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
            std::optional<std::tuple<repair_row_on_wire_with_cmd>> row_opt = source().get0();
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
        return do_with(std::move(set_diff), [needs_all_rows, remote_node, &sink] (repair_hash_set& set_diff) mutable {
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
            unsigned node_idx) {
        if (needs_all_rows || !set_diff.empty()) {
            if (remote_node == _myip) {
                return;
            }
            if (needs_all_rows) {
                set_diff.clear();
            } else {
                stats().tx_hashes_nr += set_diff.size();
                _metrics.tx_hashes_nr += set_diff.size();
            }
            stats().rpc_call_nr++;
            auto f = _sink_source_for_get_row_diff.get_sink_source(remote_node, node_idx).get0();
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
    future<> put_row_diff(repair_hash_set set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node) {
        if (!set_diff.empty()) {
            if (remote_node == _myip) {
                return make_ready_future<>();
            }
            size_t sz = set_diff.size();
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this, remote_node, sz] (std::list<repair_row> row_diff) {
                if (row_diff.size() != sz) {
                    rlogger.warn("Hash conflict detected, keyspace={}, table={}, range={}, row_diff.size={}, set_diff.size={}. It is recommended to compact the table and rerun repair for the range.",
                            _schema->ks_name(), _schema->cf_name(), _range, row_diff.size(), sz);
                }
                return do_with(std::move(row_diff), [this, remote_node] (std::list<repair_row>& row_diff) {
                    return get_repair_rows_size(row_diff).then([this, remote_node, &row_diff] (size_t row_bytes) mutable {
                        stats().tx_row_nr += row_diff.size();
                        stats().tx_row_nr_peer[remote_node] += row_diff.size();
                        stats().tx_row_bytes += row_bytes;
                        stats().rpc_call_nr++;
                        return to_repair_rows_on_wire(std::move(row_diff)).then([this, remote_node] (repair_rows_on_wire rows)  {
                            return _messaging.local().send_repair_put_row_diff(msg_addr(remote_node), _repair_meta_id, std::move(rows));
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
                        throw std::runtime_error(format("put_row_diff: Repair follower={} failed in put_row_diff hanlder, status={}", remote_node, int(status)));
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
        return do_with(std::move(rows), [&sink, remote_node] (repair_rows_on_wire& rows) mutable {
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
            gms::inet_address remote_node, unsigned node_idx) {
        if (!set_diff.empty()) {
            if (remote_node == _myip) {
                return make_ready_future<>();
            }
            size_t sz = set_diff.size();
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this, remote_node, node_idx, sz] (std::list<repair_row> row_diff) {
                if (row_diff.size() != sz) {
                    rlogger.warn("Hash conflict detected, keyspace={}, table={}, range={}, row_diff.size={}, set_diff.size={}. It is recommended to compact the table and rerun repair for the range.",
                            _schema->ks_name(), _schema->cf_name(), _range, row_diff.size(), sz);
                }
                return do_with(std::move(row_diff), [this, remote_node, node_idx] (std::list<repair_row>& row_diff) {
                    return get_repair_rows_size(row_diff).then([this, remote_node, node_idx, &row_diff] (size_t row_bytes) mutable {
                        stats().tx_row_nr += row_diff.size();
                        stats().tx_row_nr_peer[remote_node] += row_diff.size();
                        stats().tx_row_bytes += row_bytes;
                        stats().rpc_call_nr++;
                        return to_repair_rows_on_wire(std::move(row_diff)).then([this, remote_node, node_idx] (repair_rows_on_wire rows)  {
                            return  _sink_source_for_put_row_diff.get_sink_source(remote_node, node_idx).then_unpack(
                                    [this, rows = std::move(rows), remote_node, node_idx]
                                    (rpc::sink<repair_row_on_wire_with_cmd>& sink, rpc::source<repair_stream_cmd>& source) mutable {
                                auto source_op = put_row_diff_source_op(remote_node, node_idx, source);
                                auto sink_op = put_row_diff_sink_op(std::move(rows), sink, remote_node);
                                return when_all_succeed(std::move(source_op), std::move(sink_op)).discard_result();
                            });
                        });
                    });
                });
            });
        }
        return make_ready_future<>();
    }

    // RPC handler
    future<> put_row_diff_handler(repair_rows_on_wire rows, gms::inet_address from) {
        return with_gate(_gate, [this, rows = std::move(rows)] () mutable {
            _cf.update_off_strategy_trigger();
            return apply_rows_on_follower(std::move(rows));
        });
    }
};

// The repair_metas created passively, i.e., local node is the repair follower.
static thread_local std::unordered_map<node_repair_meta_id, lw_shared_ptr<repair_meta>> _repair_metas;
std::unordered_map<node_repair_meta_id, lw_shared_ptr<repair_meta>>& repair_meta::repair_meta_map() {
    return _repair_metas;
}

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
            auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
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
            auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
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
            auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
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

future<> repair_service::init_row_level_ms_handlers() {
    auto& ms = this->_messaging;

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
            auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
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
            auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
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
            auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
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
            auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
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
            auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
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
            unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, rpc::optional<streaming::stream_reason> reason) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return container().invoke_on(src_cpu_id % smp::count, [from, src_cpu_id, repair_meta_id, ks_name, cf_name,
                range, algo, max_row_buf_size, seed, remote_shard, remote_shard_count, remote_ignore_msb, schema_version, reason] (repair_service& local_repair) mutable {
            if (!local_repair._sys_dist_ks.local_is_initialized() || !local_repair._view_update_generator.local_is_initialized()) {
                return make_exception_future<repair_row_level_start_response>(std::runtime_error(format("Node {} is not fully initialized for repair, try again later",
                        utils::fb_utilities::get_broadcast_address())));
            }
            streaming::stream_reason r = reason ? *reason : streaming::stream_reason::repair;
            return repair_meta::repair_row_level_start_handler(local_repair, from, src_cpu_id, repair_meta_id, std::move(ks_name),
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
            return repair_meta::repair_row_level_stop_handler(from, repair_meta_id,
                    std::move(ks_name), std::move(cf_name), std::move(range));
        });
    });
    ms.register_repair_get_estimated_partitions([] (const rpc::client_info& cinfo, uint32_t repair_meta_id) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id] () mutable {
            return repair_meta::repair_get_estimated_partitions_handler(from, repair_meta_id);
        });
    });
    ms.register_repair_set_estimated_partitions([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
            uint64_t estimated_partitions) {
        auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, estimated_partitions] () mutable {
            return repair_meta::repair_set_estimated_partitions_handler(from, repair_meta_id, estimated_partitions);
        });
    });
    ms.register_repair_get_diff_algorithms([] (const rpc::client_info& cinfo) {
        return make_ready_future<std::vector<row_level_diff_detect_algorithm>>(suportted_diff_detect_algorithms());
    });

    return make_ready_future<>();
}

future<> repair_service::uninit_row_level_ms_handlers() {
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
        ms.unregister_repair_get_diff_algorithms()).discard_result();
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
    repair_info& _ri;
    sstring _cf_name;
    utils::UUID _table_id;
    dht::token_range _range;
    std::vector<gms::inet_address> _all_live_peer_nodes;
    column_family& _cf;

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

public:
    row_level_repair(repair_info& ri,
            sstring cf_name,
            utils::UUID table_id,
            dht::token_range range,
            std::vector<gms::inet_address> all_live_peer_nodes)
        : _ri(ri)
        , _cf_name(std::move(cf_name))
        , _table_id(std::move(table_id))
        , _range(std::move(range))
        , _all_live_peer_nodes(std::move(all_live_peer_nodes))
        , _cf(_ri.db.local().find_column_family(_table_id))
        , _seed(get_random_seed()) {
    }

private:
    enum class op_status {
        next_round,
        next_step,
        all_done,
    };

    size_t get_max_row_buf_size(row_level_diff_detect_algorithm algo) {
        // Max buffer size per repair round
        return is_rpc_stream_supported(algo) ?  tracker::max_repair_memory_per_range() : 256 * 1024;
    }

    // Step A: Negotiate sync boundary to use
    op_status negotiate_sync_boundary(repair_meta& master) {
        check_in_shutdown();
        _ri.check_in_abort();
        _sync_boundaries.clear();
        _combined_hashes.clear();
        _zero_rows = false;
        rlogger.debug("ROUND {}, _last_sync_boundary={}, _current_sync_boundary={}, _skipped_sync_boundary={}",
                master.stats().round_nr, master.last_sync_boundary(), master.current_sync_boundary(), _skipped_sync_boundary);
        master.stats().round_nr++;
        parallel_for_each(master.all_nodes(), [&, this] (repair_node_state& ns) {
            const auto& node = ns.node;
            // By calling `get_sync_boundary`, the `_last_sync_boundary`
            // is moved to the `_current_sync_boundary` or
            // `_skipped_sync_boundary` if it is not std::nullopt.
            ns.state = repair_state::get_sync_boundary_started;
            return master.get_sync_boundary(node, _skipped_sync_boundary).then([&, this] (get_sync_boundary_response res) {
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
        check_in_shutdown();
        _ri.check_in_abort();
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
            master.all_nodes()[idx].state = repair_state::get_combined_row_hash_started;
            return master.get_combined_row_hash(_common_sync_boundary, master.all_nodes()[idx].node).then([&, this, idx] (get_combined_row_hash_response resp) {
                master.all_nodes()[idx].state = repair_state::get_combined_row_hash_finished;
                rlogger.debug("Calling master.get_combined_row_hash for node {}, got combined_hash={}", master.all_nodes()[idx].node, resp);
                combined_hashes[idx]= std::move(resp);
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
                master.peer_row_hash_sets(node_idx) = master.working_row_hashes().get0();
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
                    master.get_row_diff_with_rpc_stream({}, repair_meta::needs_all_rows_t::yes, repair_meta::update_peer_row_hash_sets::yes, node, node_idx);
                    ns.state = repair_state::get_row_diff_with_rpc_stream_finished;
                } else {
                    rlogger.debug("FastPath: get_row_diff with needs_all_rows_t::yes rpc verb");
                    ns.state = repair_state::get_row_diff_and_update_peer_row_hash_sets_started;
                    master.get_row_diff_and_update_peer_row_hash_sets(node, node_idx);
                    ns.state = repair_state::get_row_diff_and_update_peer_row_hash_sets_finished;
                }
                continue;
            }

            rlogger.debug("Before master.get_full_row_hashes for node {}, hash_sets={}",
                node, master.peer_row_hash_sets(node_idx).size());
            // Ask the peer to send the full list hashes in the working row buf.
            if (master.use_rpc_stream()) {
                ns.state = repair_state::get_full_row_hashes_with_rpc_stream_started;
                master.peer_row_hash_sets(node_idx) = master.get_full_row_hashes_with_rpc_stream(node, node_idx).get0();
                ns.state = repair_state::get_full_row_hashes_with_rpc_stream_finished;
            } else {
                ns.state = repair_state::get_full_row_hashes_started;
                master.peer_row_hash_sets(node_idx) = master.get_full_row_hashes(node).get0();
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
            repair_hash_set set_diff = repair_meta::get_set_diff(master.peer_row_hash_sets(node_idx), master.working_row_hashes().get0());
            // Request missing sets from peer node
            rlogger.debug("Before get_row_diff to node {}, local={}, peer={}, set_diff={}",
                    node, master.working_row_hashes().get0().size(), master.peer_row_hash_sets(node_idx).size(), set_diff.size());
            // If we need to pull all rows from the peer. We can avoid
            // sending the row hashes on wire by setting needs_all_rows flag.
            auto needs_all_rows = repair_meta::needs_all_rows_t(set_diff.size() == master.peer_row_hash_sets(node_idx).size());
            if (master.use_rpc_stream()) {
                ns.state = repair_state::get_row_diff_with_rpc_stream_started;
                master.get_row_diff_with_rpc_stream(std::move(set_diff), needs_all_rows, repair_meta::update_peer_row_hash_sets::no, node, node_idx);
                ns.state = repair_state::get_row_diff_with_rpc_stream_finished;
            } else {
                ns.state = repair_state::get_row_diff_started;
                master.get_row_diff(std::move(set_diff), needs_all_rows, node, node_idx);
                ns.state = repair_state::get_row_diff_finished;
            }
            rlogger.debug("After get_row_diff node {}, hash_sets={}", master.myip(), master.working_row_hashes().get0().size());
        }
        master.flush_rows_in_working_row_buf();
        return op_status::next_step;
    }

    // Step C: Send missing rows to the peer nodes
    void send_missing_rows_to_follower_nodes(repair_meta& master) {
        // At this time, repair master contains all the rows between (_last_sync_boundary, _current_sync_boundary]
        // So we can figure out which rows peer node are missing and send the missing rows to them
        check_in_shutdown();
        _ri.check_in_abort();
        repair_hash_set local_row_hash_sets = master.working_row_hashes().get0();
        auto sz = _all_live_peer_nodes.size();
        std::vector<repair_hash_set> set_diffs(sz);
        for (size_t idx : boost::irange(size_t(0), sz)) {
            set_diffs[idx] = repair_meta::get_set_diff(local_row_hash_sets, master.peer_row_hash_sets(idx));
        }
        parallel_for_each(boost::irange(size_t(0), sz), [&, this] (size_t idx) {
            auto& ns = master.all_nodes()[idx + 1];
            auto needs_all_rows = repair_meta::needs_all_rows_t(master.peer_row_hash_sets(idx).empty());
            auto& set_diff = set_diffs[idx];
            rlogger.debug("Calling master.put_row_diff to node {}, set_diff={}, needs_all_rows={}", _all_live_peer_nodes[idx], set_diff.size(), needs_all_rows);
            if (master.use_rpc_stream()) {
                ns.state = repair_state::put_row_diff_with_rpc_stream_started;
                return master.put_row_diff_with_rpc_stream(std::move(set_diff), needs_all_rows, _all_live_peer_nodes[idx], idx).then([&ns] {
                    ns.state = repair_state::put_row_diff_with_rpc_stream_finished;
                });
            } else {
                ns.state = repair_state::put_row_diff_finished;
                return master.put_row_diff(std::move(set_diff), needs_all_rows, _all_live_peer_nodes[idx]).then([&ns] {
                    ns.state = repair_state::put_row_diff_finished;
                });
            }
        }).get();
        master.stats().round_nr_slow_path++;
    }

public:
    future<> run() {
        return seastar::async([this] {
            check_in_shutdown();
            _ri.check_in_abort();
            auto repair_meta_id = repair_meta::get_next_repair_meta_id().get0();
            auto algorithm = get_common_diff_detect_algorithm(_ri.messaging.local(), _all_live_peer_nodes);
            auto max_row_buf_size = get_max_row_buf_size(algorithm);
            auto master_node_shard_config = shard_config {
                    this_shard_id(),
                    _ri.sharder.shard_count(),
                    _ri.sharder.sharding_ignore_msb()
            };
            auto s = _cf.schema();
            auto schema_version = s->version();
            bool table_dropped = false;

            auto permit = _ri.db.local().obtain_reader_permit(_cf, "repair-meta", db::no_timeout).get0();

            repair_meta master(_ri.db,
                    _ri.messaging,
                    _ri.sys_dist_ks,
                    _ri.view_update_generator,
                    _cf,
                    s,
                    std::move(permit),
                    _range,
                    algorithm,
                    max_row_buf_size,
                    _seed,
                    repair_meta::repair_master::yes,
                    repair_meta_id,
                    _ri.reason,
                    std::move(master_node_shard_config),
                    _all_live_peer_nodes,
                    _all_live_peer_nodes.size(),
                    this);
            auto auto_stop_master = defer([&master] {
                master.stop().handle_exception([] (std::exception_ptr ep) {
                    rlogger.warn("Failed auto-stopping Row Level Repair (Master): {}. Ignored.", ep);
                }).get();
            });

            rlogger.debug(">>> Started Row Level Repair (Master): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, schema_version={}, range={}, seed={}, max_row_buf_size={}",
                    master.myip(), _all_live_peer_nodes, master.repair_meta_id(), _ri.keyspace, _cf_name, schema_version, _range, _seed, max_row_buf_size);


            std::vector<gms::inet_address> nodes_to_stop;
            nodes_to_stop.reserve(master.all_nodes().size());
            try {
                parallel_for_each(master.all_nodes(), [&, this] (repair_node_state& ns) {
                    const auto& node = ns.node;
                    ns.state = repair_state::row_level_start_started;
                    return master.repair_row_level_start(node, _ri.keyspace, _cf_name, _range, schema_version, _ri.reason).then([&] () {
                        ns.state = repair_state::row_level_start_finished;
                        nodes_to_stop.push_back(node);
                        ns.state = repair_state::get_estimated_partitions_started;
                        return master.repair_get_estimated_partitions(node).then([this, node, &ns] (uint64_t partitions) {
                            ns.state = repair_state::get_estimated_partitions_finished;
                            rlogger.trace("Get repair_get_estimated_partitions for node={}, estimated_partitions={}", node, partitions);
                            _estimated_partitions += partitions;
                        });
                    });
                }).get();

                parallel_for_each(master.all_nodes(), [&, this] (repair_node_state& ns) {
                    const auto& node = ns.node;
                    rlogger.trace("Get repair_set_estimated_partitions for node={}, estimated_partitions={}", node, _estimated_partitions);
                    ns.state = repair_state::set_estimated_partitions_started;
                    return master.repair_set_estimated_partitions(node, _estimated_partitions).then([&ns] {
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
            } catch (no_such_column_family& e) {
                table_dropped = true;
                rlogger.warn("repair id {} on shard {}, keyspace={}, cf={}, range={}, got error in row level repair: {}",
                        _ri.id, this_shard_id(), _ri.keyspace, _cf_name, _range, e);
                _failed = true;
            } catch (std::exception& e) {
                rlogger.warn("repair id {} on shard {}, keyspace={}, cf={}, range={}, got error in row level repair: {}",
                        _ri.id, this_shard_id(), _ri.keyspace, _cf_name, _range, e);
                // In case the repair process fail, we need to call repair_row_level_stop to clean up repair followers
                _failed = true;
            }

            parallel_for_each(nodes_to_stop, [&] (const gms::inet_address& node) {
                master.set_repair_state(repair_state::row_level_stop_started, node);
                return master.repair_row_level_stop(node, _ri.keyspace, _cf_name, _range).then([node, &master] {
                    master.set_repair_state(repair_state::row_level_stop_finished, node);
                });
            }).get();

            _ri.update_statistics(master.stats());
            if (_failed) {
                if (table_dropped) {
                    throw no_such_column_family(_ri.keyspace,  _cf_name);
                } else {
                    throw std::runtime_error(format("Failed to repair for keyspace={}, cf={}, range={}", _ri.keyspace, _cf_name, _range));
                }
            }
            rlogger.debug("<<< Finished Row Level Repair (Master): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, range={}, tx_hashes_nr={}, rx_hashes_nr={}, tx_row_nr={}, rx_row_nr={}, row_from_disk_bytes={}, row_from_disk_nr={}",
                    master.myip(), _all_live_peer_nodes, master.repair_meta_id(), _ri.keyspace, _cf_name, _range, master.stats().tx_hashes_nr, master.stats().rx_hashes_nr, master.stats().tx_row_nr, master.stats().rx_row_nr, master.stats().row_from_disk_bytes, master.stats().row_from_disk_nr);
        });
    }
};

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
        return repair_meta::remove_repair_meta();
    });
}

class row_level_repair_gossip_helper : public gms::i_endpoint_state_change_subscriber {
    void remove_row_level_repair(gms::inet_address node) {
        rlogger.debug("Started to remove row level repair on all shards for node {}", node);
        smp::invoke_on_all([node] {
            return repair_meta::remove_repair_meta(node);
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

repair_service::repair_service(distributed<gms::gossiper>& gossiper,
        netw::messaging_service& ms,
        sharded<database>& db,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<db::view::view_update_generator>& vug,
        service::migration_manager& mm,
        size_t max_repair_memory)
    : _gossiper(gossiper)
    , _messaging(ms)
    , _db(db)
    , _sys_dist_ks(sys_dist_ks)
    , _view_update_generator(vug)
    , _mm(mm)
{
    if (this_shard_id() == 0) {
        _gossip_helper = make_shared<row_level_repair_gossip_helper>();
        _tracker = std::make_unique<tracker>(smp::count, max_repair_memory);
        _gossiper.local().register_(_gossip_helper);
    }
}

future<> repair_service::start() {
    return when_all_succeed(
            init_metrics(),
            init_ms_handlers(),
            init_row_level_ms_handlers()
    ).discard_result();
}

future<> repair_service::stop() {
    return when_all_succeed(
            uninit_ms_handlers(),
            uninit_row_level_ms_handlers()
    ).discard_result().then([this] {
        if (this_shard_id() != 0) {
            _stopped = true;
            return make_ready_future<>();
        }

        return _gossiper.local().unregister_(_gossip_helper).then([this] {
            _stopped = true;
        });
    });
}

repair_service::~repair_service() {
    assert(_stopped);
}
