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
#include "db/view/view_update_checks.hh"
#include "database.hh"
#include <seastar/util/bool_class.hh>
#include <seastar/core/metrics_registration.hh>
#include <list>
#include <vector>
#include <algorithm>
#include <random>
#include <optional>
#include <boost/range/adaptors.hpp>
#include "../db/view/view_update_generator.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "gms/gossiper.hh"
#include "repair/row_level.hh"
#include "mutation_source_metadata.hh"

extern logging::logger rlogger;

struct shard_config {
    unsigned shard;
    unsigned shard_count;
    unsigned ignore_msb;
};

static bool inject_rpc_stream_error = false;

distributed<db::system_distributed_keyspace>* _sys_dist_ks;
distributed<db::view::view_update_generator>* _view_update_generator;

// Wraps sink and source objects for repair master or repair follower nodes.
// For repair master, it stores sink and source pair for each of the followers.
// For repair follower, it stores one sink and source pair for repair master.
template<class SinkType, class SourceType>
class sink_source_for_repair {
    uint32_t _repair_meta_id;
    using get_sink_source_fn_type = std::function<future<rpc::sink<SinkType>, rpc::source<SourceType>> (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr)>;
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
    future<sink_type, source_type> get_sink_source(gms::inet_address remote_node, unsigned node_idx) {
        if (_sinks[node_idx] && _sources[node_idx]) {
            return make_ready_future<sink_type, source_type>(_sinks[node_idx].value(), _sources[node_idx].value());
        }
        if (_sinks[node_idx] || _sources[node_idx]) {
            return make_exception_future<sink_type, source_type>(std::runtime_error(format("sink or source is missing for node {}", remote_node)));
        }
        return _fn(_repair_meta_id, netw::messaging_service::msg_addr(remote_node)).then([this, node_idx] (rpc::sink<SinkType> sink, rpc::source<SourceType> source) mutable {
            _sinks[node_idx].emplace(std::move(sink));
            _sources[node_idx].emplace(std::move(source));
            return make_ready_future<sink_type, source_type>(_sinks[node_idx].value(), _sources[node_idx].value());
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

static row_level_diff_detect_algorithm get_common_diff_detect_algorithm(const std::vector<gms::inet_address>& nodes) {
    std::vector<std::vector<row_level_diff_detect_algorithm>> nodes_algorithms(nodes.size());
    parallel_for_each(boost::irange(size_t(0), nodes.size()), [&nodes_algorithms, &nodes] (size_t idx) {
        return netw::get_local_messaging_service().send_repair_get_diff_algorithms(netw::messaging_service::msg_addr(nodes[idx])).then(
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

class repair_row {
    std::optional<frozen_mutation_fragment> _fm;
    lw_shared_ptr<const decorated_key_with_hash> _dk_with_hash;
    std::optional<repair_sync_boundary> _boundary;
    std::optional<repair_hash> _hash;
    lw_shared_ptr<mutation_fragment> _mf;
public:
    repair_row() = default;
    repair_row(std::optional<frozen_mutation_fragment> fm,
            std::optional<position_in_partition> pos,
            lw_shared_ptr<const decorated_key_with_hash> dk_with_hash,
            std::optional<repair_hash> hash,
            lw_shared_ptr<mutation_fragment> mf = {})
            : _fm(std::move(fm))
            , _dk_with_hash(std::move(dk_with_hash))
            , _boundary(pos ? std::optional<repair_sync_boundary>(repair_sync_boundary{_dk_with_hash->dk, std::move(*pos)}) : std::nullopt)
            , _hash(std::move(hash))
            , _mf(std::move(mf)) {
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
};

class repair_reader {
public:
using is_local_reader = bool_class<class is_local_reader_tag>;

private:
    schema_ptr _schema;
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
    // Current partition read from disk
    lw_shared_ptr<const decorated_key_with_hash> _current_dk;

public:
    repair_reader(
            seastar::sharded<database>& db,
            column_family& cf,
            schema_ptr s,
            dht::token_range range,
            const dht::sharder& remote_sharder,
            unsigned remote_shard,
            uint64_t seed,
            is_local_reader local_reader)
            : _schema(s)
            , _range(dht::to_partition_range(range))
            , _sharder(remote_sharder, range, remote_shard)
            , _seed(seed)
            , _local_read_op(local_reader ? std::optional(cf.read_in_progress()) : std::nullopt)
            , _reader(make_reader(db, cf, local_reader)) {
    }

private:
    flat_mutation_reader
    make_reader(seastar::sharded<database>& db,
            column_family& cf,
            is_local_reader local_reader) {
        if (local_reader) {
            return cf.make_streaming_reader(_schema, _range);
        }
        return make_multishard_streaming_reader(db, _schema, [this] {
            auto shard_range = _sharder.next();
            if (shard_range) {
                return std::optional<dht::partition_range>(dht::to_partition_range(*shard_range));
            }
            return std::optional<dht::partition_range>();
        });
    }

public:
    future<mutation_fragment_opt>
    read_mutation_fragment() {
        return _reader(db::no_timeout);
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

};

class repair_writer {
    schema_ptr _schema;
    uint64_t _estimated_partitions;
    size_t _nr_peer_nodes;
    // Needs more than one for repair master
    std::vector<std::optional<future<>>> _writer_done;
    std::vector<std::optional<seastar::queue<mutation_fragment_opt>>> _mq;
    // Current partition written to disk
    std::vector<lw_shared_ptr<const decorated_key_with_hash>> _current_dk_written_to_sstable;
    // Is current partition still open. A partition is opened when a
    // partition_start is written and is closed when a partition_end is
    // written.
    std::vector<bool> _partition_opened;
    streaming::stream_reason _reason;
public:
    repair_writer(
            schema_ptr schema,
            uint64_t estimated_partitions,
            size_t nr_peer_nodes,
            streaming::stream_reason reason)
            : _schema(std::move(schema))
            , _estimated_partitions(estimated_partitions)
            , _nr_peer_nodes(nr_peer_nodes)
            , _reason(reason) {
        init_writer();
    }

    future<> write_start_and_mf(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf, unsigned node_idx)  {
        _current_dk_written_to_sstable[node_idx] = dk;
        if (mf.is_partition_start()) {
            return _mq[node_idx]->push_eventually(mutation_fragment_opt(std::move(mf))).then([this, node_idx] {
                _partition_opened[node_idx] = true;
            });
        } else {
            auto start = mutation_fragment(partition_start(dk->dk, tombstone()));
            return _mq[node_idx]->push_eventually(mutation_fragment_opt(std::move(start))).then([this, node_idx, mf = std::move(mf)] () mutable {
                _partition_opened[node_idx] = true;
                return _mq[node_idx]->push_eventually(mutation_fragment_opt(std::move(mf)));
            });
        }
    };

    void init_writer() {
        _writer_done.resize(_nr_peer_nodes);
        _mq.resize(_nr_peer_nodes);
        _current_dk_written_to_sstable.resize(_nr_peer_nodes);
        _partition_opened.resize(_nr_peer_nodes, false);
    }

    void create_writer(sharded<database>& db, unsigned node_idx) {
        if (_writer_done[node_idx]) {
            return;
        }
        _mq[node_idx] = seastar::queue<mutation_fragment_opt>(16);
        auto get_next_mutation_fragment = [this, node_idx] () mutable {
            return _mq[node_idx]->pop_eventually();
        };
        table& t = db.local().find_column_family(_schema->id());
        _writer_done[node_idx] = mutation_writer::distribute_reader_and_consume_on_shards(_schema,
                make_generating_reader(_schema, std::move(get_next_mutation_fragment)),
                [&db, reason = this->_reason, estimated_partitions = this->_estimated_partitions] (flat_mutation_reader reader) {
            auto& t = db.local().find_column_family(reader.schema());
            return db::view::check_needs_view_update_path(_sys_dist_ks->local(), t, reason).then([t = t.shared_from_this(), estimated_partitions, reader = std::move(reader)] (bool use_view_update_path) mutable {
                //FIXME: for better estimations this should be transmitted from remote
                auto metadata = mutation_source_metadata{};
                auto& cs = t->get_compaction_strategy();
                const auto adjusted_estimated_partitions = cs.adjust_partition_estimate(metadata, estimated_partitions);
                auto consumer = cs.make_interposer_consumer(metadata,
                        [t = std::move(t), use_view_update_path, adjusted_estimated_partitions] (flat_mutation_reader reader) {
                    sstables::shared_sstable sst = use_view_update_path ? t->make_streaming_staging_sstable() : t->make_streaming_sstable_for_write();
                    schema_ptr s = reader.schema();
                    auto& pc = service::get_local_streaming_write_priority();
                    return sst->write_components(std::move(reader), std::max(1ul, adjusted_estimated_partitions), s,
                                                 t->get_sstables_manager().configure_writer(),
                                                 encoding_stats{}, pc).then([sst] {
                        return sst->open_data();
                    }).then([t, sst] {
                        return t->add_sstable_and_update_cache(sst);
                    }).then([t, s, sst, use_view_update_path]() mutable -> future<> {
                        if (!use_view_update_path) {
                            return make_ready_future<>();
                        }
                        return _view_update_generator->local().register_staging_sstable(sst, std::move(t));
                    });
                });
                return consumer(std::move(reader));
            });
        },
        t.stream_in_progress()).then([this, node_idx] (uint64_t partitions) {
            rlogger.debug("repair_writer: keyspace={}, table={}, managed to write partitions={} to sstable",
                _schema->ks_name(), _schema->cf_name(), partitions);
        }).handle_exception([this, node_idx] (std::exception_ptr ep) {
            rlogger.warn("repair_writer: keyspace={}, table={}, multishard_writer failed: {}",
                    _schema->ks_name(), _schema->cf_name(), ep);
            _mq[node_idx]->abort(ep);
            return make_exception_future<>(std::move(ep));
        });
    }

    future<> write_partition_end(unsigned node_idx) {
        if (_partition_opened[node_idx]) {
            return _mq[node_idx]->push_eventually(mutation_fragment(partition_end())).then([this, node_idx] {
                _partition_opened[node_idx] = false;
            });
        }
        return make_ready_future<>();
    }

    future<> do_write(unsigned node_idx, lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf) {
        if (_current_dk_written_to_sstable[node_idx]) {
            if (_current_dk_written_to_sstable[node_idx]->dk.equal(*_schema, dk->dk)) {
                return _mq[node_idx]->push_eventually(mutation_fragment_opt(std::move(mf)));
            } else {
                return write_partition_end(node_idx).then([this,
                        node_idx, dk = std::move(dk), mf = std::move(mf)] () mutable {
                    return write_start_and_mf(std::move(dk), std::move(mf), node_idx);
                });
            }
        } else {
            return write_start_and_mf(std::move(dk), std::move(mf), node_idx);
        }
    }

    future<> write_end_of_stream(unsigned node_idx) {
        if (_mq[node_idx]) {
            // Partition_end is never sent on wire, so we have to write one ourselves.
            return write_partition_end(node_idx).then([this, node_idx] () mutable {
                // Empty mutation_fragment_opt means no more data, so the writer can seal the sstables.
                return _mq[node_idx]->push_eventually(mutation_fragment_opt());
            }).handle_exception([this, node_idx] (std::exception_ptr ep) {
                _mq[node_idx]->abort(ep);
                rlogger.warn("repair_writer: keyspace={}, table={}, write_end_of_stream failed: {}",
                        _schema->ks_name(), _schema->cf_name(), ep);
                return make_exception_future<>(std::move(ep));
            });
        } else {
            return make_ready_future<>();
        }
    }

    future<> do_wait_for_writer_done(unsigned node_idx) {
        if (_writer_done[node_idx]) {
            return std::move(*(_writer_done[node_idx]));
        } else {
            return make_ready_future<>();
        }
    }

    future<> wait_for_writer_done() {
        return parallel_for_each(boost::irange(unsigned(0), unsigned(_nr_peer_nodes)), [this] (unsigned node_idx) {
            return when_all_succeed(write_end_of_stream(node_idx), do_wait_for_writer_done(node_idx));
        }).handle_exception([this] (std::exception_ptr ep) {
            rlogger.warn("repair_writer: keyspace={}, table={}, wait_for_writer_done failed: {}",
                    _schema->ks_name(), _schema->cf_name(), ep);
            return make_exception_future<>(std::move(ep));
        });
    }
};

class repair_meta {
public:
    using repair_master = bool_class<class repair_master_tag>;
    using update_working_row_buf = bool_class<class update_working_row_buf_tag>;
    using update_peer_row_hash_sets = bool_class<class update_peer_row_hash_sets_tag>;
    using needs_all_rows_t = bool_class<class needs_all_rows_tag>;
    using msg_addr = netw::messaging_service::msg_addr;
private:
    seastar::sharded<database>& _db;
    column_family& _cf;
    schema_ptr _schema;
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
    repair_writer _repair_writer;
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
    std::vector<std::unordered_set<repair_hash>> _peer_row_hash_sets;
    // Gate used to make sure pending operation of meta data is done
    seastar::gate _gate;
    sink_source_for_get_full_row_hashes _sink_source_for_get_full_row_hashes;
    sink_source_for_get_row_diff _sink_source_for_get_row_diff;
    sink_source_for_put_row_diff _sink_source_for_put_row_diff;
public:
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
            column_family& cf,
            schema_ptr s,
            dht::token_range range,
            row_level_diff_detect_algorithm algo,
            size_t max_row_buf_size,
            uint64_t seed,
            repair_master master,
            uint32_t repair_meta_id,
            streaming::stream_reason reason,
            shard_config master_node_shard_config,
            size_t nr_peer_nodes = 1)
            : _db(db)
            , _cf(cf)
            , _schema(s)
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
                    _range,
                    _remote_sharder,
                    _master_node_shard_config.shard,
                    _seed,
                    repair_reader::is_local_reader(_repair_master || _same_sharding_config)
              )
            , _repair_writer(_schema, _estimated_partitions, _nr_peer_nodes, _reason)
            , _sink_source_for_get_full_row_hashes(_repair_meta_id, _nr_peer_nodes,
                    [] (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr) {
                        return netw::get_local_messaging_service().make_sink_and_source_for_repair_get_full_row_hashes_with_rpc_stream(repair_meta_id, addr);
                })
            , _sink_source_for_get_row_diff(_repair_meta_id, _nr_peer_nodes,
                    [] (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr) {
                        return netw::get_local_messaging_service().make_sink_and_source_for_repair_get_row_diff_with_rpc_stream(repair_meta_id, addr);
                })
            , _sink_source_for_put_row_diff(_repair_meta_id, _nr_peer_nodes,
                    [] (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr) {
                        return netw::get_local_messaging_service().make_sink_and_source_for_repair_put_row_diff_with_rpc_stream(repair_meta_id, addr);
                })
            {
    }

public:
    future<> stop() {
        auto gate_future = _gate.close();
        auto writer_future = _repair_writer.wait_for_writer_done();
        auto f1 = _sink_source_for_get_full_row_hashes.close();
        auto f2 = _sink_source_for_get_row_diff.close();
        auto f3 = _sink_source_for_put_row_diff.close();
        return when_all_succeed(std::move(gate_future), std::move(writer_future), std::move(f1), std::move(f2), std::move(f3));
    }

    static std::unordered_map<node_repair_meta_id, lw_shared_ptr<repair_meta>>& repair_meta_map() {
        static thread_local std::unordered_map<node_repair_meta_id, lw_shared_ptr<repair_meta>> _repair_metas;
        return _repair_metas;
    }

    static lw_shared_ptr<repair_meta> get_repair_meta(gms::inet_address from, uint32_t repair_meta_id) {
        node_repair_meta_id id{from, repair_meta_id};
        auto it = repair_meta_map().find(id);
        if (it == repair_meta_map().end()) {
            throw std::runtime_error(format("get_repair_meta: repair_meta_id {:d} for node {} does not exist", id.repair_meta_id, id.ip));
        } else {
            return it->second;
        }
    }

    static future<>
    insert_repair_meta(const gms::inet_address& from,
            uint32_t src_cpu_id,
            uint32_t repair_meta_id,
            dht::token_range range,
            row_level_diff_detect_algorithm algo,
            uint64_t max_row_buf_size,
            uint64_t seed,
            shard_config master_node_shard_config,
            table_schema_version schema_version,
            streaming::stream_reason reason) {
        return service::get_schema_for_write(schema_version, {from, src_cpu_id}).then([from,
                repair_meta_id,
                range,
                algo,
                max_row_buf_size,
                seed,
                master_node_shard_config,
                schema_version,
                reason] (schema_ptr s) {
            auto& db = service::get_local_storage_proxy().get_db();
            auto& cf = db.local().find_column_family(s->id());
            node_repair_meta_id id{from, repair_meta_id};
            auto rm = make_lw_shared<repair_meta>(db,
                    cf,
                    s,
                    range,
                    algo,
                    max_row_buf_size,
                    seed,
                    repair_meta::repair_master::no,
                    repair_meta_id,
                    reason,
                    std::move(master_node_shard_config));
            bool insertion = repair_meta_map().emplace(id, rm).second;
            if (!insertion) {
                rlogger.warn("insert_repair_meta: repair_meta_id {} for node {} already exists, replace existing one", id.repair_meta_id, id.ip);
                repair_meta_map()[id] = rm;
            } else {
                rlogger.debug("insert_repair_meta: Inserted repair_meta_id {} for node {}", id.repair_meta_id, id.ip);
            }
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
    static std::unordered_set<repair_hash>
    get_set_diff(const std::unordered_set<repair_hash>& x, const std::unordered_set<repair_hash>& y) {
        std::unordered_set<repair_hash> set_diff;
        // Note std::set_difference needs x and y are sorted.
        std::copy_if(x.begin(), x.end(), std::inserter(set_diff, set_diff.end()),
                [&y] (auto& item) { thread::maybe_yield(); return y.find(item) == y.end(); });
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

    std::unordered_set<repair_hash>& peer_row_hash_sets(unsigned node_idx) {
        return _peer_row_hash_sets[node_idx];
    }

    // Get a list of row hashes in _working_row_buf
    future<std::unordered_set<repair_hash>>
    working_row_hashes() {
        return do_with(std::unordered_set<repair_hash>(), [this] (std::unordered_set<repair_hash>& hashes) {
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

    stop_iteration handle_mutation_fragment(mutation_fragment_opt mfopt, size_t& cur_size, size_t& new_rows_size, std::list<repair_row>& cur_rows) {
        if (!mfopt) {
            return stop_iteration::yes;
        }
        mutation_fragment& mf = *mfopt;
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
        repair_row r(freeze(*_schema, mf), position_in_partition(mf.position()), _repair_reader.get_current_dk(), hash);
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
    future<std::list<repair_row>, size_t>
    read_rows_from_disk(size_t cur_size) {
        return do_with(cur_size, size_t(0), std::list<repair_row>(), [this] (size_t& cur_size, size_t& new_rows_size, std::list<repair_row>& cur_rows) {
            return repeat([this, &cur_size, &cur_rows, &new_rows_size] () mutable {
                if (cur_size >= _max_row_buf_size) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                _gate.check();
                return _repair_reader.read_mutation_fragment().then([this, &cur_size, &new_rows_size, &cur_rows] (mutation_fragment_opt mfopt) mutable {
                    return handle_mutation_fragment(std::move(mfopt), cur_size, new_rows_size, cur_rows);
                });
            }).then([&cur_rows, &new_rows_size] () mutable {
                return make_ready_future<std::list<repair_row>, size_t>(std::move(cur_rows), new_rows_size);
            });
        });
    }

    // Read rows from disk until _max_row_buf_size of rows are filled into _row_buf.
    // Calculate the combined checksum of the rows
    // Calculate the total size of the rows in _row_buf
    future<get_sync_boundary_response>
    get_sync_boundary(std::optional<repair_sync_boundary> skipped_sync_boundary) {
        if (skipped_sync_boundary) {
            _current_sync_boundary = skipped_sync_boundary;
            _row_buf.clear();
            _working_row_buf.clear();
            _working_row_buf_combined_hash.clear();
        } else {
            _working_row_buf.clear();
            _working_row_buf_combined_hash.clear();
        }
        // Here is the place we update _last_sync_boundary
        rlogger.trace("SET _last_sync_boundary from {} to {}", _last_sync_boundary, _current_sync_boundary);
        _last_sync_boundary = _current_sync_boundary;
        return row_buf_size().then([this, sb = std::move(skipped_sync_boundary)] (size_t cur_size) {
            return read_rows_from_disk(cur_size).then([this, sb = std::move(sb)] (std::list<repair_row> new_rows, size_t new_rows_size) mutable {
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
    copy_rows_from_working_row_buf_within_set_diff(std::unordered_set<repair_hash> set_diff) {
        return do_with(std::list<repair_row>(), std::move(set_diff),
                [this] (std::list<repair_row>& rows, std::unordered_set<repair_hash>& set_diff) {
            return do_for_each(_working_row_buf, [this, &set_diff, &rows] (const repair_row& r) {
                if (set_diff.count(r.hash()) > 0) {
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
    get_row_diff(std::unordered_set<repair_hash> set_diff, needs_all_rows_t needs_all_rows = needs_all_rows_t::no) {
        if (needs_all_rows) {
            if (!_repair_master || _nr_peer_nodes == 1) {
                return make_ready_future<std::list<repair_row>>(std::move(_working_row_buf));
            }
            return copy_rows_from_working_row_buf();
        } else {
            return copy_rows_from_working_row_buf_within_set_diff(std::move(set_diff));
        }
    }

    // Give a list of rows, apply the rows to disk and update the _working_row_buf and _peer_row_hash_sets if requested
    // Must run inside a seastar thread
    void apply_rows_on_master_in_thread(repair_rows_on_wire rows, gms::inet_address from, update_working_row_buf update_buf,
            update_peer_row_hash_sets update_hash_set, unsigned node_idx = 0) {
        if (rows.empty()) {
            return;
        }
        auto row_diff = to_repair_rows_list(rows).get0();
        auto sz = get_repair_rows_size(row_diff).get0();
        stats().rx_row_bytes += sz;
        stats().rx_row_nr += row_diff.size();
        stats().rx_row_nr_peer[from] += row_diff.size();
        if (update_buf) {
            std::list<repair_row> tmp;
            tmp.swap(_working_row_buf);
            // Both row_diff and _working_row_buf and are ordered, merging
            // two sored list to make sure the combination of row_diff
            // and _working_row_buf are ordered.
            std::merge(tmp.begin(), tmp.end(), row_diff.begin(), row_diff.end(), std::back_inserter(_working_row_buf),
                [this] (const repair_row& x, const repair_row& y) { thread::maybe_yield(); return _cmp(x.boundary(), y.boundary()) < 0; });
        }
        if (update_hash_set) {
            _peer_row_hash_sets[node_idx] = boost::copy_range<std::unordered_set<repair_hash>>(row_diff |
                    boost::adaptors::transformed([] (repair_row& r) { thread::maybe_yield(); return r.hash(); }));
        }
        _repair_writer.create_writer(_db, node_idx);
        for (auto& r : row_diff) {
            if (update_buf) {
                _working_row_buf_combined_hash.add(r.hash());
            }
            // The repair_row here is supposed to have
            // mutation_fragment attached because we have stored it in
            // to_repair_rows_list above where the repair_row is created.
            mutation_fragment mf = std::move(r.get_mutation_fragment());
            auto dk_with_hash = r.get_dk_with_hash();
            _repair_writer.do_write(node_idx, std::move(dk_with_hash), std::move(mf)).get();
        }
    }

    future<>
    apply_rows_on_follower(repair_rows_on_wire rows) {
        if (rows.empty()) {
            return make_ready_future<>();
        }
        return to_repair_rows_list(rows).then([this] (std::list<repair_row> row_diff) {
            return do_with(std::move(row_diff), [this] (std::list<repair_row>& row_diff) {
                unsigned node_idx = 0;
                _repair_writer.create_writer(_db, node_idx);
                return do_for_each(row_diff, [this, node_idx] (repair_row& r) {
                    // The repair_row here is supposed to have
                    // mutation_fragment attached because we have stored it in
                    // to_repair_rows_list above where the repair_row is created.
                    mutation_fragment mf = std::move(r.get_mutation_fragment());
                    auto dk_with_hash = r.get_dk_with_hash();
                    return _repair_writer.do_write(node_idx, std::move(dk_with_hash), std::move(mf));
                });
            });
        });
    }

    future<repair_rows_on_wire> to_repair_rows_on_wire(std::list<repair_row> row_list) {
        return do_with(repair_rows_on_wire(), std::move(row_list), [this] (repair_rows_on_wire& rows, std::list<repair_row>& row_list) {
            return get_repair_rows_size(row_list).then([this, &rows, &row_list] (size_t row_bytes) {
                _metrics.tx_row_nr += row_list.size();
                _metrics.tx_row_bytes += row_bytes;
                return do_for_each(row_list, [this, &rows] (repair_row& r) {
                    auto pk = r.get_dk_with_hash()->dk.key();
                    // No need to search from the beginning of the rows. Look at the end of repair_rows_on_wire is enough.
                    if (rows.empty()) {
                        rows.push_back(repair_row_on_wire(std::move(pk), {std::move(r.get_frozen_mutation())}));
                    } else {
                        auto& row = rows.back();
                        if (pk.legacy_equal(*_schema, row.get_key())) {
                            row.push_mutation_fragment(std::move(r.get_frozen_mutation()));
                        } else {
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
                        auto mf = make_lw_shared<mutation_fragment>(fmf.unfreeze(*_schema));
                        auto hash = do_hash_for_mf(*dk_ptr, *mf);
                        position_in_partition pos(mf->position());
                        row_list.push_back(repair_row(std::move(fmf), std::move(pos), dk_ptr, std::move(hash), std::move(mf)));
                    });
                } else {
                    last_mf = {};
                    return do_for_each(x.get_mutation_fragments(), [this, &dk_ptr, &row_list, &last_mf, &cmp] (frozen_mutation_fragment& fmf) mutable {
                        _metrics.rx_row_nr += 1;
                        _metrics.rx_row_bytes += fmf.representation().size();
                        auto mf = make_lw_shared<mutation_fragment>(fmf.unfreeze(*_schema));
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
                            row_list.push_back(repair_row({}, {}, dk_ptr, {}, std::move(mf)));
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
    future<std::unordered_set<repair_hash>>
    get_full_row_hashes(gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return get_full_row_hashes_handler();
        }
        return netw::get_local_messaging_service().send_repair_get_full_row_hashes(msg_addr(remote_node),
                _repair_meta_id).then([this, remote_node] (std::unordered_set<repair_hash> hashes) {
            rlogger.debug("Got full hashes from peer={}, nr_hashes={}", remote_node, hashes.size());
            _metrics.rx_hashes_nr += hashes.size();
            stats().rx_hashes_nr += hashes.size();
            stats().rpc_call_nr++;
            return hashes;
        });
    }

private:
    future<> get_full_row_hashes_source_op(
            lw_shared_ptr<std::unordered_set<repair_hash>> current_hashes,
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
    future<std::unordered_set<repair_hash>>
    get_full_row_hashes_with_rpc_stream(gms::inet_address remote_node, unsigned node_idx) {
        if (remote_node == _myip) {
            return get_full_row_hashes_handler();
        }
        auto current_hashes = make_lw_shared<std::unordered_set<repair_hash>>();
        return _sink_source_for_get_full_row_hashes.get_sink_source(remote_node, node_idx).then(
                [this, current_hashes, remote_node, node_idx]
                (rpc::sink<repair_stream_cmd>& sink, rpc::source<repair_hash_with_cmd>& source) mutable {
            auto source_op = get_full_row_hashes_source_op(current_hashes, remote_node, node_idx, source);
            auto sink_op = get_full_row_hashes_sink_op(sink);
            return when_all_succeed(std::move(source_op), std::move(sink_op));
        }).then([this, current_hashes] () mutable {
            stats().rx_hashes_nr += current_hashes->size();
            _metrics.rx_hashes_nr += current_hashes->size();
            return std::move(*current_hashes);
        });
    }

    // RPC handler
    future<std::unordered_set<repair_hash>>
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
        return netw::get_local_messaging_service().send_repair_get_combined_row_hash(msg_addr(remote_node),
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
        return netw::get_local_messaging_service().send_repair_row_level_start(msg_addr(remote_node),
                _repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range), _algo, _max_row_buf_size, _seed,
                _master_node_shard_config.shard, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb,
                remote_partitioner_name, std::move(schema_version), reason);
    }

    // RPC handler
    static future<>
    repair_row_level_start_handler(gms::inet_address from, uint32_t src_cpu_id, uint32_t repair_meta_id, sstring ks_name, sstring cf_name,
            dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size,
            uint64_t seed, shard_config master_node_shard_config, table_schema_version schema_version, streaming::stream_reason reason) {
        if (!_sys_dist_ks->local_is_initialized() || !_view_update_generator->local_is_initialized()) {
            return make_exception_future<>(std::runtime_error(format("Node {} is not fully initialized for repair, try again later",
                    utils::fb_utilities::get_broadcast_address())));
        }
        rlogger.debug(">>> Started Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, schema_version={}, range={}, seed={}, max_row_buf_siz={}",
            utils::fb_utilities::get_broadcast_address(), from, repair_meta_id, ks_name, cf_name, schema_version, range, seed, max_row_buf_size);
        return insert_repair_meta(from, src_cpu_id, repair_meta_id, std::move(range), algo, max_row_buf_size, seed, std::move(master_node_shard_config), std::move(schema_version), reason);
    }

    // RPC API
    future<> repair_row_level_stop(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range) {
        if (remote_node == _myip) {
            return stop();
        }
        stats().rpc_call_nr++;
        return netw::get_local_messaging_service().send_repair_row_level_stop(msg_addr(remote_node),
                _repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range));
    }

    // RPC handler
    static future<>
    repair_row_level_stop_handler(gms::inet_address from, uint32_t repair_meta_id, sstring ks_name, sstring cf_name, dht::token_range range) {
        rlogger.debug("<<< Finished Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, range={}",
            utils::fb_utilities::get_broadcast_address(), from, repair_meta_id, ks_name, cf_name, range);
        auto rm = get_repair_meta(from, repair_meta_id);
        return remove_repair_meta(from, repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range));
    }

    // RPC API
    future<uint64_t> repair_get_estimated_partitions(gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return get_estimated_partitions();
        }
        stats().rpc_call_nr++;
        return netw::get_local_messaging_service().send_repair_get_estimated_partitions(msg_addr(remote_node), _repair_meta_id);
    }


    // RPC handler
    static future<uint64_t> repair_get_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id) {
        auto rm = get_repair_meta(from, repair_meta_id);
        return rm->get_estimated_partitions();
    }

    // RPC API
    future<> repair_set_estimated_partitions(gms::inet_address remote_node, uint64_t estimated_partitions) {
        if (remote_node == _myip) {
            return set_estimated_partitions(estimated_partitions);
        }
        stats().rpc_call_nr++;
        return netw::get_local_messaging_service().send_repair_set_estimated_partitions(msg_addr(remote_node), _repair_meta_id, estimated_partitions);
    }


    // RPC handler
    static future<> repair_set_estimated_partitions_handler(gms::inet_address from, uint32_t repair_meta_id, uint64_t estimated_partitions) {
        auto rm = get_repair_meta(from, repair_meta_id);
        return rm->set_estimated_partitions(estimated_partitions);
    }

    // RPC API
    // Return the largest sync point contained in the _row_buf , current _row_buf checksum, and the _row_buf size
    future<get_sync_boundary_response>
    get_sync_boundary(gms::inet_address remote_node, std::optional<repair_sync_boundary> skipped_sync_boundary) {
        if (remote_node == _myip) {
            return get_sync_boundary_handler(skipped_sync_boundary);
        }
        stats().rpc_call_nr++;
        return netw::get_local_messaging_service().send_repair_get_sync_boundary(msg_addr(remote_node), _repair_meta_id, skipped_sync_boundary);
    }

    // RPC handler
    future<get_sync_boundary_response>
    get_sync_boundary_handler(std::optional<repair_sync_boundary> skipped_sync_boundary) {
        return with_gate(_gate, [this, skipped_sync_boundary = std::move(skipped_sync_boundary)] () mutable {
            return get_sync_boundary(std::move(skipped_sync_boundary));
        });
    }

    // RPC API
    // Return rows in the _working_row_buf with hash within the given sef_diff
    // Must run inside a seastar thread
    void get_row_diff(std::unordered_set<repair_hash> set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node, unsigned node_idx) {
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
            repair_rows_on_wire rows = netw::get_local_messaging_service().send_repair_get_row_diff(msg_addr(remote_node),
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
        repair_rows_on_wire rows = netw::get_local_messaging_service().send_repair_get_row_diff(msg_addr(remote_node),
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
            std::unordered_set<repair_hash> set_diff,
            needs_all_rows_t needs_all_rows,
            rpc::sink<repair_hash_with_cmd>& sink,
            gms::inet_address remote_node) {
        return do_with(std::move(set_diff), [needs_all_rows, remote_node, &sink] (std::unordered_set<repair_hash>& set_diff) mutable {
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
            std::unordered_set<repair_hash> set_diff,
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
            auto f = _sink_source_for_get_row_diff.get_sink_source(remote_node, node_idx).get();
            rpc::sink<repair_hash_with_cmd>& sink = std::get<0>(f);
            rpc::source<repair_row_on_wire_with_cmd>& source = std::get<1>(f);
            auto sink_op = get_row_diff_sink_op(std::move(set_diff), needs_all_rows, sink, remote_node);
            get_row_diff_source_op(update_hash_set, remote_node, node_idx, sink, source);
            sink_op.get();
        }
    }

    // RPC handler
    future<repair_rows_on_wire> get_row_diff_handler(std::unordered_set<repair_hash> set_diff, needs_all_rows_t needs_all_rows) {
        return with_gate(_gate, [this, set_diff = std::move(set_diff), needs_all_rows] () mutable {
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this] (std::list<repair_row> row_diff) {
                return to_repair_rows_on_wire(std::move(row_diff));
            });
        });
    }

    // RPC API
    // Send rows in the _working_row_buf with hash within the given sef_diff
    future<> put_row_diff(std::unordered_set<repair_hash> set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node) {
        if (!set_diff.empty()) {
            if (remote_node == _myip) {
                return make_ready_future<>();
            }
            auto sz = set_diff.size();
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this, remote_node, sz] (std::list<repair_row> row_diff) {
                if (row_diff.size() != sz) {
                    throw std::runtime_error("row_diff.size() != set_diff.size()");
                }
                return do_with(std::move(row_diff), [this, remote_node] (std::list<repair_row>& row_diff) {
                    return get_repair_rows_size(row_diff).then([this, remote_node, &row_diff] (size_t row_bytes) mutable {
                        stats().tx_row_nr += row_diff.size();
                        stats().tx_row_nr_peer[remote_node] += row_diff.size();
                        stats().tx_row_bytes += row_bytes;
                        stats().rpc_call_nr++;
                        return to_repair_rows_on_wire(std::move(row_diff)).then([this, remote_node] (repair_rows_on_wire rows)  {
                            return netw::get_local_messaging_service().send_repair_put_row_diff(msg_addr(remote_node), _repair_meta_id, std::move(rows));
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
            std::unordered_set<repair_hash> set_diff,
            needs_all_rows_t needs_all_rows,
            gms::inet_address remote_node, unsigned node_idx) {
        if (!set_diff.empty()) {
            if (remote_node == _myip) {
                return make_ready_future<>();
            }
            auto sz = set_diff.size();
            return get_row_diff(std::move(set_diff), needs_all_rows).then([this, remote_node, node_idx, sz] (std::list<repair_row> row_diff) {
                if (row_diff.size() != sz) {
                    throw std::runtime_error("row_diff.size() != set_diff.size()");
                }
                return do_with(std::move(row_diff), [this, remote_node, node_idx] (std::list<repair_row>& row_diff) {
                    return get_repair_rows_size(row_diff).then([this, remote_node, node_idx, &row_diff] (size_t row_bytes) mutable {
                        stats().tx_row_nr += row_diff.size();
                        stats().tx_row_nr_peer[remote_node] += row_diff.size();
                        stats().tx_row_bytes += row_bytes;
                        stats().rpc_call_nr++;
                        return to_repair_rows_on_wire(std::move(row_diff)).then([this, remote_node, node_idx] (repair_rows_on_wire rows)  {
                            return  _sink_source_for_put_row_diff.get_sink_source(remote_node, node_idx).then(
                                    [this, rows = std::move(rows), remote_node, node_idx]
                                    (rpc::sink<repair_row_on_wire_with_cmd>& sink, rpc::source<repair_stream_cmd>& source) mutable {
                                auto source_op = put_row_diff_source_op(remote_node, node_idx, source);
                                auto sink_op = put_row_diff_sink_op(std::move(rows), sink, remote_node);
                                return when_all_succeed(std::move(source_op), std::move(sink_op));
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
            return apply_rows_on_follower(std::move(rows));
        });
    }
};

static future<stop_iteration> repair_get_row_diff_with_rpc_stream_process_op(
        gms::inet_address from,
        uint32_t src_cpu_id,
        uint32_t repair_meta_id,
        rpc::sink<repair_row_on_wire_with_cmd> sink,
        rpc::source<repair_hash_with_cmd> source,
        bool &error,
        std::unordered_set<repair_hash>& current_set_diff,
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
        auto fp = make_foreign(std::make_unique<std::unordered_set<repair_hash>>(std::move(current_set_diff)));
        return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, needs_all_rows, fp = std::move(fp)] {
            auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
            if (fp.get_owner_shard() == this_shard_id()) {
                return rm->get_row_diff_handler(std::move(*fp), repair_meta::needs_all_rows_t(needs_all_rows));
            } else {
                return rm->get_row_diff_handler(*fp, repair_meta::needs_all_rows_t(needs_all_rows));
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
            if (fp.get_owner_shard() == this_shard_id()) {
                return rm->put_row_diff_handler(std::move(*fp), from);
            } else {
                return rm->put_row_diff_handler(*fp, from);
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
            return rm->get_full_row_hashes_handler().then([] (std::unordered_set<repair_hash> hashes) {
                _metrics.tx_hashes_nr += hashes.size();
                return hashes;
            });
        }).then([sink] (std::unordered_set<repair_hash> hashes) mutable {
            return do_with(std::move(hashes), [sink] (std::unordered_set<repair_hash>& hashes) mutable {
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
    return do_with(false, std::unordered_set<repair_hash>(), [from, src_cpu_id, repair_meta_id, sink, source] (bool& error, std::unordered_set<repair_hash>& current_set_diff) mutable {
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
                        return sink(repair_row_on_wire_with_cmd{repair_stream_cmd::error, repair_row_on_wire()}).then([sink] ()  mutable {
                            return sink.close();
                        }).then([sink] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                } else {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                    return sink.close().then([sink] {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    });
                }
            });
        });
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
                        return sink(repair_stream_cmd::error).then([sink] ()  mutable {
                            return sink.close();
                        }).then([sink] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                } else {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                    return sink.close().then([sink] {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    });
                }
            });
        });
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
                        return sink(repair_hash_with_cmd{repair_stream_cmd::error, repair_hash()}).then([sink] ()  mutable {
                            return sink.close();
                        }).then([sink] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                } else {
                    if (error) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                    return sink.close().then([sink] {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    });
                }
            });
        });
    });
}

future<> repair_init_messaging_service_handler(repair_service& rs, distributed<db::system_distributed_keyspace>& sys_dist_ks, distributed<db::view::view_update_generator>& view_update_generator) {
    _sys_dist_ks = &sys_dist_ks;
    _view_update_generator = &view_update_generator;
    return netw::get_messaging_service().invoke_on_all([] (auto& ms) {
        ms.register_repair_get_row_diff_with_rpc_stream([&ms] (const rpc::client_info& cinfo, uint64_t repair_meta_id, rpc::source<repair_hash_with_cmd> source) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            auto sink = ms.make_sink_for_repair_get_row_diff_with_rpc_stream(source);
            // Start a new fiber.
            (void)repair_get_row_diff_with_rpc_stream_handler(from, src_cpu_id, repair_meta_id, sink, source).handle_exception(
                    [from, repair_meta_id, sink, source] (std::exception_ptr ep) {
                rlogger.info("Failed to process get_row_diff_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
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
                rlogger.info("Failed to process put_row_diff_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
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
                rlogger.info("Failed to process get_full_row_hashes_with_rpc_stream_handler from={}, repair_meta_id={}: {}", from, repair_meta_id, ep);
            });
            return make_ready_future<rpc::sink<repair_hash_with_cmd>>(sink);
        });
        ms.register_repair_get_full_row_hashes([] (const rpc::client_info& cinfo, uint32_t repair_meta_id) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id] {
                auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
                return rm->get_full_row_hashes_handler().then([] (std::unordered_set<repair_hash> hashes) {
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
                return rm->get_combined_row_hash_handler(std::move(common_sync_boundary));
            });
        });
        ms.register_repair_get_sync_boundary([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                std::optional<repair_sync_boundary> skipped_sync_boundary) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id,
                    skipped_sync_boundary = std::move(skipped_sync_boundary)] () mutable {
                auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
                return rm->get_sync_boundary_handler(std::move(skipped_sync_boundary));
            });
        });
        ms.register_repair_get_row_diff([] (const rpc::client_info& cinfo, uint32_t repair_meta_id,
                std::unordered_set<repair_hash> set_diff, bool needs_all_rows) {
            auto src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
            auto from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
            _metrics.rx_hashes_nr += set_diff.size();
            auto fp = make_foreign(std::make_unique<std::unordered_set<repair_hash>>(std::move(set_diff)));
            return smp::submit_to(src_cpu_id % smp::count, [from, repair_meta_id, fp = std::move(fp), needs_all_rows] () mutable {
                auto rm = repair_meta::get_repair_meta(from, repair_meta_id);
                if (fp.get_owner_shard() == this_shard_id()) {
                    return rm->get_row_diff_handler(std::move(*fp), repair_meta::needs_all_rows_t(needs_all_rows));
                } else {
                    return rm->get_row_diff_handler(*fp, repair_meta::needs_all_rows_t(needs_all_rows));
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
                if (fp.get_owner_shard() == this_shard_id()) {
                    return rm->put_row_diff_handler(std::move(*fp), from);
                } else {
                    return rm->put_row_diff_handler(*fp, from);
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
                return repair_meta::repair_row_level_start_handler(from, src_cpu_id, repair_meta_id, std::move(ks_name),
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
    });
}

class row_level_repair {
    repair_info& _ri;
    sstring _cf_name;
    dht::token_range _range;
    std::vector<gms::inet_address> _all_live_peer_nodes;
    column_family& _cf;

    // All particular peer nodes not including the node itself.
    std::vector<gms::inet_address> _all_nodes;

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
            dht::token_range range,
            std::vector<gms::inet_address> all_live_peer_nodes)
        : _ri(ri)
        , _cf_name(std::move(cf_name))
        , _range(std::move(range))
        , _all_live_peer_nodes(std::move(all_live_peer_nodes))
        , _cf(_ri.db.local().find_column_family(_ri.keyspace, _cf_name))
        , _all_nodes(_all_live_peer_nodes)
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
        parallel_for_each(_all_nodes, [&, this] (const gms::inet_address& node) {
            // By calling `get_sync_boundary`, the `_last_sync_boundary`
            // is moved to the `_current_sync_boundary` or
            // `_skipped_sync_boundary` if it is not std::nullopt.
            return master.get_sync_boundary(node, _skipped_sync_boundary).then([&, this] (get_sync_boundary_response res) {
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
        combined_hashes.resize(_all_nodes.size());
        parallel_for_each(boost::irange(size_t(0), _all_nodes.size()), [&, this] (size_t idx) {
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
            return master.get_combined_row_hash(_common_sync_boundary, _all_nodes[idx]).then([&, this, idx] (get_combined_row_hash_response resp) {
                rlogger.debug("Calling master.get_combined_row_hash for node {}, got combined_hash={}", _all_nodes[idx], resp);
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
                    master.get_row_diff_with_rpc_stream({}, repair_meta::needs_all_rows_t::yes, repair_meta::update_peer_row_hash_sets::yes, node, node_idx);
                } else {
                    rlogger.debug("FastPath: get_row_diff with needs_all_rows_t::yes rpc verb");
                    master.get_row_diff_and_update_peer_row_hash_sets(node, node_idx);
                }
                continue;
            }

            rlogger.debug("Before master.get_full_row_hashes for node {}, hash_sets={}",
                node, master.peer_row_hash_sets(node_idx).size());
            // Ask the peer to send the full list hashes in the working row buf.
            if (master.use_rpc_stream()) {
                master.peer_row_hash_sets(node_idx) = master.get_full_row_hashes_with_rpc_stream(node, node_idx).get0();
            } else {
                master.peer_row_hash_sets(node_idx) = master.get_full_row_hashes(node).get0();
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
            std::unordered_set<repair_hash> set_diff = repair_meta::get_set_diff(master.peer_row_hash_sets(node_idx), master.working_row_hashes().get0());
            // Request missing sets from peer node
            rlogger.debug("Before get_row_diff to node {}, local={}, peer={}, set_diff={}",
                    node, master.working_row_hashes().get0().size(), master.peer_row_hash_sets(node_idx).size(), set_diff.size());
            // If we need to pull all rows from the peer. We can avoid
            // sending the row hashes on wire by setting needs_all_rows flag.
            auto needs_all_rows = repair_meta::needs_all_rows_t(set_diff.size() == master.peer_row_hash_sets(node_idx).size());
            if (master.use_rpc_stream()) {
                master.get_row_diff_with_rpc_stream(std::move(set_diff), needs_all_rows, repair_meta::update_peer_row_hash_sets::no, node, node_idx);
            } else {
                master.get_row_diff(std::move(set_diff), needs_all_rows, node, node_idx);
            }
            rlogger.debug("After get_row_diff node {}, hash_sets={}", master.myip(), master.working_row_hashes().get0().size());
        }
        return op_status::next_step;
    }

    // Step C: Send missing rows to the peer nodes
    void send_missing_rows_to_follower_nodes(repair_meta& master) {
        // At this time, repair master contains all the rows between (_last_sync_boundary, _current_sync_boundary]
        // So we can figure out which rows peer node are missing and send the missing rows to them
        check_in_shutdown();
        _ri.check_in_abort();
        std::unordered_set<repair_hash> local_row_hash_sets = master.working_row_hashes().get0();
        auto sz = _all_live_peer_nodes.size();
        std::vector<std::unordered_set<repair_hash>> set_diffs(sz);
        for (size_t idx : boost::irange(size_t(0), sz)) {
            set_diffs[idx] = repair_meta::get_set_diff(local_row_hash_sets, master.peer_row_hash_sets(idx));
        }
        parallel_for_each(boost::irange(size_t(0), sz), [&, this] (size_t idx) {
            auto needs_all_rows = repair_meta::needs_all_rows_t(master.peer_row_hash_sets(idx).empty());
            auto& set_diff = set_diffs[idx];
            rlogger.debug("Calling master.put_row_diff to node {}, set_diff={}, needs_all_rows={}", _all_live_peer_nodes[idx], set_diff.size(), needs_all_rows);
            if (master.use_rpc_stream()) {
                return master.put_row_diff_with_rpc_stream(std::move(set_diff), needs_all_rows, _all_live_peer_nodes[idx], idx);
            } else {
                return master.put_row_diff(std::move(set_diff), needs_all_rows, _all_live_peer_nodes[idx]);
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
            auto algorithm = get_common_diff_detect_algorithm(_all_live_peer_nodes);
            auto max_row_buf_size = get_max_row_buf_size(algorithm);
            auto master_node_shard_config = shard_config {
                    this_shard_id(),
                    _ri.sharder.shard_count(),
                    _ri.sharder.sharding_ignore_msb()
            };
            auto s = _cf.schema();
            auto schema_version = s->version();

            repair_meta master(_ri.db,
                    _cf,
                    s,
                    _range,
                    algorithm,
                    max_row_buf_size,
                    _seed,
                    repair_meta::repair_master::yes,
                    repair_meta_id,
                    _ri.reason,
                    std::move(master_node_shard_config),
                    _all_live_peer_nodes.size());

            // All nodes including the node itself.
            _all_nodes.insert(_all_nodes.begin(), master.myip());

            rlogger.debug(">>> Started Row Level Repair (Master): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, schema_version={}, range={}, seed={}, max_row_buf_size={}",
                    master.myip(), _all_live_peer_nodes, master.repair_meta_id(), _ri.keyspace, _cf_name, schema_version, _range, _seed, max_row_buf_size);


            std::vector<gms::inet_address> nodes_to_stop;
            nodes_to_stop.reserve(_all_nodes.size());
            try {
                parallel_for_each(_all_nodes, [&, this] (const gms::inet_address& node) {
                    return master.repair_row_level_start(node, _ri.keyspace, _cf_name, _range, schema_version, _ri.reason).then([&] () {
                        nodes_to_stop.push_back(node);
                        return master.repair_get_estimated_partitions(node).then([this, node] (uint64_t partitions) {
                            rlogger.trace("Get repair_get_estimated_partitions for node={}, estimated_partitions={}", node, partitions);
                            _estimated_partitions += partitions;
                        });
                    });
                }).get();

                parallel_for_each(_all_nodes, [&, this] (const gms::inet_address& node) {
                    rlogger.trace("Get repair_set_estimated_partitions for node={}, estimated_partitions={}", node, _estimated_partitions);
                    return master.repair_set_estimated_partitions(node, _estimated_partitions);
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
            } catch (std::exception& e) {
                rlogger.info("Got error in row level repair: {}", e);
                // In case the repair process fail, we need to call repair_row_level_stop to clean up repair followers
                _failed = true;
                _ri.nr_failed_ranges++;
            }

            parallel_for_each(nodes_to_stop, [&] (const gms::inet_address& node) {
                return master.repair_row_level_stop(node, _ri.keyspace, _cf_name, _range);
            }).get();

            _ri.update_statistics(master.stats());
            if (_failed) {
                throw std::runtime_error(format("Failed to repair for keyspace={}, cf={}, range={}", _ri.keyspace, _cf_name, _range));
            }
            rlogger.debug("<<< Finished Row Level Repair (Master): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, range={}, tx_hashes_nr={}, rx_hashes_nr={}, tx_row_nr={}, rx_row_nr={}, row_from_disk_bytes={}, row_from_disk_nr={}",
                    master.myip(), _all_live_peer_nodes, master.repair_meta_id(), _ri.keyspace, _cf_name, _range, master.stats().tx_hashes_nr, master.stats().rx_hashes_nr, master.stats().tx_row_nr, master.stats().rx_row_nr, master.stats().row_from_disk_bytes, master.stats().row_from_disk_nr);
        });
    }
};

future<> repair_cf_range_row_level(repair_info& ri,
        sstring cf_name, dht::token_range range,
        const std::vector<gms::inet_address>& all_peer_nodes) {
    return do_with(row_level_repair(ri, std::move(cf_name), std::move(range), all_peer_nodes), [] (row_level_repair& repair) {
        return repair.run();
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
