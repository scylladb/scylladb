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
#include "mutation_fragment.hh"
#include "multishard_writer.hh"
#include "dht/i_partitioner.hh"
#include "to_string.hh"
#include "xx_hasher.hh"
#include "dht/i_partitioner.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "service/storage_service.hh"
#include "service/priority_manager.hh"
#include <seastar/util/bool_class.hh>
#include <list>
#include <vector>
#include <algorithm>
#include <random>
#include <optional>
#include <boost/range/adaptors.hpp>

extern logging::logger rlogger;

struct shard_config {
    unsigned shard;
    unsigned shard_count;
    unsigned ignore_msb;
    sstring partitioner_name;
};

static const std::vector<row_level_diff_detect_algorithm>& suportted_diff_detect_algorithms() {
    static std::vector<row_level_diff_detect_algorithm> _algorithms = {
        row_level_diff_detect_algorithm::send_full_set,
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
        feed_hash(_hasher, col.name());
        feed_hash(_hasher, col.type->name());
        feed_hash(_hasher, cell, col);
    }
public:
    explicit fragment_hasher(const schema&s, xx_hasher& h)
        : _schema(s), _hasher(h) { }

    void hash(const mutation_fragment& mf) {
        if (mf.is_static_row()) {
            consume(mf.as_static_row());
        } else if (mf.is_clustering_row()) {
            consume(mf.as_clustering_row());
        } else if (mf.is_range_tombstone()) {
            consume(mf.as_range_tombstone());
        } else if (mf.is_partition_start()) {
            consume(mf.as_partition_start());
        } else {
            throw std::runtime_error("Wrong type to consume");
        }
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
    frozen_mutation_fragment _fm;
    lw_shared_ptr<const decorated_key_with_hash> _dk_with_hash;
    repair_sync_boundary _boundary;
    repair_hash _hash;
    lw_shared_ptr<mutation_fragment> _mf;
public:
    repair_row() = delete;
    repair_row(frozen_mutation_fragment fm,
            position_in_partition pos,
            lw_shared_ptr<const decorated_key_with_hash> dk_with_hash,
            repair_hash hash,
            lw_shared_ptr<mutation_fragment> mf = {})
            : _fm(std::move(fm))
            , _dk_with_hash(std::move(dk_with_hash))
            , _boundary({_dk_with_hash->dk, std::move(pos)})
            , _hash(std::move(hash))
            , _mf(std::move(mf)) {
    }
    mutation_fragment& get_mutation_fragment() {
        if (!_mf) {
            throw std::runtime_error("get empty mutation_fragment");
        }
        return *_mf;
    }
    frozen_mutation_fragment& get_frozen_mutation() { return _fm; }
    const frozen_mutation_fragment& get_frozen_mutation() const { return _fm; }
    const lw_shared_ptr<const decorated_key_with_hash>& get_dk_with_hash() const {
        return _dk_with_hash;
    }
    size_t size() const {
        return _fm.representation().size();
    }
    const repair_sync_boundary& boundary() const {
        return _boundary;
    }
    const repair_hash& hash() const {
        return _hash;
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
    // Local reader or multishard reader to read the range
    flat_mutation_reader _reader;
    // Current partition read from disk
    lw_shared_ptr<const decorated_key_with_hash> _current_dk;

public:
    repair_reader(
            seastar::sharded<database>& db,
            column_family& cf,
            dht::token_range range,
            dht::i_partitioner& local_partitioner,
            dht::i_partitioner& remote_partitioner,
            unsigned remote_shard,
            uint64_t seed,
            is_local_reader local_reader)
            : _schema(cf.schema())
            , _range(dht::to_partition_range(range))
            , _sharder(remote_partitioner, range, remote_shard)
            , _seed(seed)
            , _reader(make_reader(db, cf, local_partitioner, local_reader)) {
    }

private:
    flat_mutation_reader
    make_reader(seastar::sharded<database>& db,
            column_family& cf,
            dht::i_partitioner& local_partitioner,
            is_local_reader local_reader) {
        if (local_reader) {
            return cf.make_streaming_reader(_schema, _range);
        }
        return make_multishard_streaming_reader(db, local_partitioner, _schema, [this] {
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
    std::vector<std::optional<future<uint64_t>>> _writer_done;
    std::vector<std::optional<seastar::queue<mutation_fragment_opt>>> _mq;
    // Current partition written to disk
    std::vector<lw_shared_ptr<const decorated_key_with_hash>> _current_dk_written_to_sstable;
public:
    repair_writer(
            schema_ptr schema,
            uint64_t estimated_partitions,
            size_t nr_peer_nodes)
            : _schema(std::move(schema))
            , _estimated_partitions(estimated_partitions)
            , _nr_peer_nodes(nr_peer_nodes) {
        init_writer();
    }

    future<> write_start_and_mf(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf, unsigned node_idx)  {
        _current_dk_written_to_sstable[node_idx] = dk;
        if (mf.is_partition_start()) {
            return _mq[node_idx]->push_eventually(mutation_fragment_opt(std::move(mf)));
        } else {
            auto start = mutation_fragment(partition_start(dk->dk, tombstone()));
            return _mq[node_idx]->push_eventually(mutation_fragment_opt(std::move(start))).then([this, node_idx, mf = std::move(mf)] () mutable {
                return _mq[node_idx]->push_eventually(mutation_fragment_opt(std::move(mf)));
            });
        }
    };

    void init_writer() {
        _writer_done.resize(_nr_peer_nodes);
        _mq.resize(_nr_peer_nodes);
        _current_dk_written_to_sstable.resize(_nr_peer_nodes);
    }

    void create_writer(unsigned node_idx) {
        if (_writer_done[node_idx]) {
            return;
        }
        _mq[node_idx] = seastar::queue<mutation_fragment_opt>(16);
        auto get_next_mutation_fragment = [this, node_idx] () mutable {
            return _mq[node_idx]->pop_eventually();
        };
        _writer_done[node_idx] = distribute_reader_and_consume_on_shards(_schema, dht::global_partitioner(),
            make_generating_reader(_schema, std::move(get_next_mutation_fragment)),
            [ks_name = this->_schema->ks_name(), cf_name = this->_schema->cf_name(), estimated_partitions = this->_estimated_partitions] (flat_mutation_reader reader) {
                column_family& cf = service::get_local_storage_service().db().local().find_column_family(ks_name, cf_name);
                sstables::sstable_writer_config sst_cfg;
                sst_cfg.large_partition_handler = cf.get_large_partition_handler();
                sstables::shared_sstable sst = cf.make_streaming_sstable_for_write();
                schema_ptr s = reader.schema();
                auto& pc = service::get_local_streaming_write_priority();
                return sst->write_components(std::move(reader), std::max(1ul, estimated_partitions), s, sst_cfg, {}, pc).then([sst] {
                    return sst->open_data();
                }).then([&cf, sst] {
                    return cf.add_sstable_and_update_cache(sst);
                });
            }
        );
    }

    future<> do_write(unsigned node_idx, lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf) {
        if (_current_dk_written_to_sstable[node_idx]) {
            if (_current_dk_written_to_sstable[node_idx]->dk.equal(*_schema, dk->dk)) {
                return _mq[node_idx]->push_eventually(mutation_fragment_opt(std::move(mf)));
            } else {
                return _mq[node_idx]->push_eventually(mutation_fragment(partition_end())).then([this,
                        node_idx, dk = std::move(dk), mf = std::move(mf)] () mutable {
                    return write_start_and_mf(std::move(dk), std::move(mf), node_idx);
                });
            }
        } else {
            return write_start_and_mf(std::move(dk), std::move(mf), node_idx);
        }
    }

    future<> wait_for_writer_done() {
        return parallel_for_each(boost::irange(unsigned(0), unsigned(_nr_peer_nodes)), [this] (unsigned node_idx) {
            if (_writer_done[node_idx] && _mq[node_idx]) {
                // Partition_end is never sent on wire, so we have to write one ourselves.
                return _mq[node_idx]->push_eventually(mutation_fragment(partition_end())).then([this, node_idx] () mutable {
                    // Empty mutation_fragment_opt means no more data, so the writer can seal the sstables.
                    return _mq[node_idx]->push_eventually(mutation_fragment_opt()).then([this, node_idx] () mutable {
                        return (*_writer_done[node_idx]).then([] (uint64_t partitions) {
                            rlogger.debug("Managed to write partitions={} to sstable", partitions);
                            return make_ready_future<>();
                        });
                    });
                });
            }
            return make_ready_future<>();
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
    dht::token_range _range;
    schema_ptr _schema;
    repair_sync_boundary::tri_compare _cmp;
    // The algorithm used to find the row difference
    row_level_diff_detect_algorithm _algo;
    // Max rows size can be stored in _row_buf
    size_t _max_row_buf_size;
    uint64_t _seed = 0;
    repair_master _repair_master;
    gms::inet_address _myip;
    uint32_t _repair_meta_id;
    // Repair master's sharding configuration
    shard_config _master_node_shard_config;
    // Partitioner of repair master
    std::unique_ptr<dht::i_partitioner> _remote_partitioner;
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

public:
    repair_meta(
            seastar::sharded<database>& db,
            column_family& cf,
            dht::token_range range,
            row_level_diff_detect_algorithm algo,
            size_t max_row_buf_size,
            uint64_t seed,
            repair_master master,
            uint32_t repair_meta_id,
            shard_config master_node_shard_config,
            size_t nr_peer_nodes = 1)
            : _db(db)
            , _cf(cf)
            , _range(range)
            , _schema(cf.schema())
            , _cmp(repair_sync_boundary::tri_compare(*_schema))
            , _algo(algo)
            , _max_row_buf_size(max_row_buf_size)
            , _seed(seed)
            , _repair_master(master)
            , _myip(utils::fb_utilities::get_broadcast_address())
            , _repair_meta_id(repair_meta_id)
            , _master_node_shard_config(std::move(master_node_shard_config))
            , _remote_partitioner(make_remote_partitioner())
            , _same_sharding_config(is_same_sharding_config())
            , _nr_peer_nodes(nr_peer_nodes)
            , _repair_reader(
                    _db,
                    _cf,
                    _range,
                    dht::global_partitioner(),
                    *_remote_partitioner,
                    _master_node_shard_config.shard,
                    _seed,
                    repair_reader::is_local_reader(_repair_master || _same_sharding_config)
              )
            , _repair_writer(_schema, _estimated_partitions, _nr_peer_nodes) {
    }

public:
    future<> wait_for_writer_done() {
        return _repair_writer.wait_for_writer_done();
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

    static void
    insert_repair_meta(const gms::inet_address& from,
            uint32_t repair_meta_id,
            sstring ks_name,
            sstring cf_name,
            dht::token_range range,
            row_level_diff_detect_algorithm algo,
            uint64_t max_row_buf_size,
            uint64_t seed,
            shard_config master_node_shard_config) {
        node_repair_meta_id id{from, repair_meta_id};
        auto& db = service::get_local_storage_proxy().get_db();
        auto& cf = db.local().find_column_family(ks_name, cf_name);
        auto rm = make_lw_shared<repair_meta>(db,
                cf,
                range,
                algo,
                max_row_buf_size,
                seed,
                repair_meta::repair_master::no,
                repair_meta_id,
                std::move(master_node_shard_config));
        bool insertion = repair_meta_map().emplace(id, rm).second;
        if (!insertion) {
            rlogger.warn("insert_repair_meta: repair_meta_id {} for node {} already exists, replace existing one", id.repair_meta_id, id.ip);
            repair_meta_map()[id] = rm;
        } else {
            rlogger.debug("insert_repair_meta: Inserted repair_meta_id {} for node {}", id.repair_meta_id, id.ip);
        }
    }

    static void
    remove_repair_meta(const gms::inet_address& from,
            uint32_t repair_meta_id,
            sstring ks_name,
            sstring cf_name,
            dht::token_range range) {
        node_repair_meta_id id{from, repair_meta_id};
        auto it = repair_meta_map().find(id);
        if (it == repair_meta_map().end()) {
            rlogger.warn("remove_repair_meta: repair_meta_id {} for node {} does not exist", id.repair_meta_id, id.ip);
        } else {
            rlogger.debug("remove_repair_meta: Removed repair_meta_id {} for node {}", id.repair_meta_id, id.ip);
            repair_meta_map().erase(it);
        }
    }

    static future<uint32_t> get_next_repair_meta_id() {
        return smp::submit_to(0, [] {
            static uint32_t next_id = 0;
            return next_id++;
        });
    }

    static std::unordered_set<repair_hash>
    get_set_diff(const std::unordered_set<repair_hash>& x, const std::unordered_set<repair_hash>& y) {
        std::unordered_set<repair_hash> set_diff;
        // Note std::set_difference needs x and y are sorted.
        std::copy_if(x.begin(), x.end(), std::inserter(set_diff, set_diff.end()),
                [&y] (auto& item) { return y.find(item) == y.end(); });
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
    std::unordered_set<repair_hash>
    working_row_hashes() {
        return boost::copy_range<std::unordered_set<repair_hash>>(_working_row_buf |
                boost::adaptors::transformed([] (repair_row& r) { return r.hash(); }));
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
        auto sstables = _cf.get_sstables();
        uint64_t partition_count = boost::accumulate(*sstables, uint64_t(0), [this] (uint64_t x, auto&& sst) { return x + sst->estimated_keys_for_range(_range); });
        return make_ready_future<uint64_t>(partition_count);
    }

    future<uint64_t> get_estimated_partitions() {
        if (_repair_master || _same_sharding_config) {
            return do_estimate_partitions_on_local_shard();
        } else {
            return do_with(dht::selective_token_range_sharder(*_remote_partitioner, _range, _master_node_shard_config.shard), uint64_t(0), [this] (auto& sharder, auto& partitions_sum) mutable {
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
    }

    future<> set_estimated_partitions(uint64_t estimated_partitions) {
        _estimated_partitions = estimated_partitions;
        return make_ready_future<>();
    }

    std::unique_ptr<dht::i_partitioner> make_remote_partitioner() {
        return dht::make_partitioner(_master_node_shard_config.partitioner_name, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb);
    }

    bool is_same_sharding_config() {
        rlogger.debug("is_same_sharding_config: remote_partitioner_name={}, remote_shard={}, remote_shard_count={}, remote_ignore_msb={}",
                _master_node_shard_config.partitioner_name, _master_node_shard_config.shard, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb);
        return dht::global_partitioner().name() == _master_node_shard_config.partitioner_name
               && dht::global_partitioner().shard_count() == _master_node_shard_config.shard_count
               && dht::global_partitioner().sharding_ignore_msb() == _master_node_shard_config.ignore_msb
               && engine().cpu_id() == _master_node_shard_config.shard;
    }

    size_t get_repair_rows_size(const std::list<repair_row>& rows) const {
        return boost::accumulate(rows, size_t(0), [] (size_t x, const repair_row& r) { return x + r.size(); });
    }

    // Get the size of rows in _row_buf
    size_t row_buf_size() const {
        return get_repair_rows_size(_row_buf);
    }

    // return the combined checksum of rows in _row_buf
    repair_hash row_buf_csum() {
        repair_hash combined;
        boost::for_each(_row_buf, [&combined] (repair_row& r) { combined.add(r.hash()); });
        return combined;
    }

    repair_hash do_hash_for_mf(const decorated_key_with_hash& dk_with_hash, const mutation_fragment& mf) {
        xx_hasher h(_seed);
        fragment_hasher fh(*_schema, h);
        fh.hash(mf);
        feed_hash(h, dk_with_hash.hash.hash);
        return repair_hash(h.finalize_uint64());
    }

    stop_iteration handle_mutation_fragment(mutation_fragment_opt mfopt, size_t& cur_size, std::list<repair_row>& cur_rows) {
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
        cur_size += r.size();
        cur_rows.push_back(std::move(r));
        return stop_iteration::no;
    }

    // Read rows from sstable until the size of rows exceeds _max_row_buf_size  - current_size
    // This reads rows from where the reader left last time into _row_buf
    // _current_sync_boundary or _last_sync_boundary have no effect on the reader neither.
    future<std::list<repair_row>>
    read_rows_from_disk(size_t cur_size) {
        return do_with(cur_size, std::list<repair_row>(), [this] (size_t& cur_size, std::list<repair_row>& cur_rows) {
            return repeat([this, &cur_size, &cur_rows] () mutable {
                if (cur_size >= _max_row_buf_size) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return _repair_reader.read_mutation_fragment().then([this, &cur_size, &cur_rows] (mutation_fragment_opt mfopt) mutable {
                    return handle_mutation_fragment(std::move(mfopt), cur_size, cur_rows);
                });
            }).then([&cur_rows] () mutable {
                return std::move(cur_rows);
            });
        });
    }

    // Read rows from disk until _max_row_buf_size of rows are filled into _row_buf.
    // Calculate the combined checksum of the rows
    // Calculate the total size of the rows in _row_buf
    future<get_sync_boundary_response>
    request_sync_boundary(std::optional<repair_sync_boundary> skipped_sync_boundary) {
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
        size_t current_size = row_buf_size();
        return read_rows_from_disk(current_size).then([this, skipped_sync_boundary = std::move(skipped_sync_boundary)] (std::list<repair_row> new_rows) mutable {
            size_t new_rows_size = boost::accumulate(new_rows, size_t(0), [] (size_t x, const repair_row& r) { return x + r.size(); });
            size_t new_rows_nr = new_rows.size();
            _row_buf.splice(_row_buf.end(), new_rows);
            std::optional<repair_sync_boundary> sb_max;
            if (!_row_buf.empty()) {
                sb_max = _row_buf.back().boundary();
            }
            rlogger.debug("request_sync_boundary: Got nr={} rows, sb_max={}, row_buf_size={}, repair_hash={}, skipped_sync_boundary={}",
                    new_rows.size(), sb_max, row_buf_size(), row_buf_csum(), skipped_sync_boundary);
            return get_sync_boundary_response{sb_max, row_buf_csum(), row_buf_size(), new_rows_size, new_rows_nr};
        });
    }

    // Move rows from <_row_buf> to <_working_row_buf> according to
    // _last_sync_boundary and common_sync_boundary. That is rows within the
    // (_last_sync_boundary, _current_sync_boundary] in <_row_buf> are moved
    // into the <_working_row_buf>
    future<repair_hash>
    request_row_hashes(const std::optional<repair_sync_boundary>& common_sync_boundary) {
        if (!common_sync_boundary) {
            throw std::runtime_error("common_sync_boundary is empty");
        }
        _current_sync_boundary = common_sync_boundary;
        rlogger.trace("SET _current_sync_boundary to {}, common_sync_boundary={}", _current_sync_boundary, common_sync_boundary);
        _working_row_buf.clear();
        _working_row_buf_combined_hash.clear();

        if (_row_buf.empty()) {
            return make_ready_future<repair_hash>();
        }

        auto sz = _row_buf.size();

        // Fast path
        if (_cmp(_row_buf.back().boundary(), *_current_sync_boundary) <= 0) {
            _working_row_buf.swap(_row_buf);
        } else {
            auto it = std::find_if(_row_buf.rbegin(), _row_buf.rend(),
                    [this] (repair_row& r) { return _cmp(r.boundary(), *_current_sync_boundary) <= 0; });
            // Copy the items > _current_sync_boundary to _working_row_buf
            // Delete the items > _current_sync_boundary from _row_buf
            // Swap _working_row_buf and _row_buf
            std::copy(it.base(), _row_buf.end(), std::back_inserter(_working_row_buf));
            _row_buf.erase(it.base(), _row_buf.end());
            _row_buf.swap(_working_row_buf);
        }
        rlogger.trace("before={}, after={} + {}", sz, _working_row_buf.size(), _row_buf.size());
        if (sz != _working_row_buf.size() + _row_buf.size()) {
            throw std::runtime_error("incorrect row_buf and working_row_buf size");
        }

        return parallel_for_each(_working_row_buf, [this] (repair_row& r) {
            _working_row_buf_combined_hash.add(r.hash());
            return make_ready_future<>();
        }).then([this] {
            return _working_row_buf_combined_hash;
        });
    }

    std::unordered_set<repair_hash>
    request_full_row_hashes() {
        return working_row_hashes();
    }

    // Return rows in the _working_row_buf with hash within the given sef_diff
    // Give a set of row hashes, return the corresponding rows
    // If needs_all_rows is set, return all the rows in _working_row_buf, ignore the set_diff
    std::list<repair_row>
    get_row_diff(const std::unordered_set<repair_hash>& set_diff, needs_all_rows_t needs_all_rows = needs_all_rows_t::no) {
        std::list<repair_row> rows;
        if (needs_all_rows) {
            rows = _working_row_buf;
        } else {
            rows = boost::copy_range<std::list<repair_row>>(_working_row_buf |
                    boost::adaptors::filtered([&set_diff] (repair_row& r) { return set_diff.count(r.hash()) > 0; }));
        }
        return rows;
    }

    // Give a list of rows, apply the rows to disk and update the _working_row_buf and _peer_row_hash_sets if requested
    future<>
    apply_rows(repair_rows_on_wire rows, gms::inet_address from, update_working_row_buf update_buf,
            update_peer_row_hash_sets update_hash_set, unsigned node_idx = 0) {
        if (rows.empty()) {
            return make_ready_future<>();
        }
        return to_repair_rows_list(rows).then([this, from, node_idx, update_buf, update_hash_set] (std::list<repair_row> row_diff) {
            return do_with(std::move(row_diff), [this, from, node_idx, update_buf, update_hash_set] (std::list<repair_row>& row_diff) {
                stats().rx_row_bytes += get_repair_rows_size(row_diff);
                stats().rx_row_nr += row_diff.size();
                stats().rx_row_nr_peer[from] += row_diff.size();
                if (update_buf) {
                    std::list<repair_row> tmp;
                    tmp.swap(_working_row_buf);
                    // Both row_diff and _working_row_buf and are ordered, merging
                    // two sored list to make sure the combination of row_diff
                    // and _working_row_buf are ordered.
                    std::merge(tmp.begin(), tmp.end(), row_diff.begin(), row_diff.end(), std::back_inserter(_working_row_buf),
                        [this] (const repair_row& x, const repair_row& y) { return _cmp(x.boundary(), y.boundary()) < 0; });
                }
                if (update_hash_set) {
                    _peer_row_hash_sets[node_idx] = boost::copy_range<std::unordered_set<repair_hash>>(row_diff |
                            boost::adaptors::transformed([] (repair_row& r) { return r.hash(); }));
                }
                _repair_writer.create_writer(node_idx);
                return do_for_each(row_diff, [this, node_idx, update_buf] (repair_row& r) {
                    if (update_buf) {
                        _working_row_buf_combined_hash.add(r.hash());
                    }
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
            return do_for_each(row_list, [this, &rows] (repair_row& r) {
                auto pk = r.get_dk_with_hash()->dk.key();
                auto it = std::find_if(rows.begin(), rows.end(), [&pk, s=_schema] (partition_key_and_mutation_fragments& row) { return pk.legacy_equal(*s, row.get_key()); });
                if (it == rows.end()) {
                    rows.push_back(partition_key_and_mutation_fragments(std::move(pk), {std::move(r.get_frozen_mutation())}));
                } else {
                    it->push_mutation_fragment(std::move(r.get_frozen_mutation()));
                }
            }).then([&rows] {
                return std::move(rows);
            });
        });
    };

    future<std::list<repair_row>> to_repair_rows_list(repair_rows_on_wire rows) {
        return do_with(std::move(rows), std::list<repair_row>(), lw_shared_ptr<const decorated_key_with_hash>(),
          [this] (repair_rows_on_wire& rows, std::list<repair_row>& row_list, lw_shared_ptr<const decorated_key_with_hash>& dk_ptr) mutable {
            return do_for_each(rows, [this, &dk_ptr, &row_list] (partition_key_and_mutation_fragments& x) mutable {
                dht::decorated_key dk = dht::global_partitioner().decorate_key(*_schema, x.get_key());
                if (!(dk_ptr && dk_ptr->dk.equal(*_schema, dk))) {
                    dk_ptr = make_lw_shared<const decorated_key_with_hash>(*_schema, dk, _seed);
                }
                return do_for_each(x.get_mutation_fragments(), [this, &dk_ptr, &row_list] (frozen_mutation_fragment& fmf) mutable {
                    // Keep the mutation_fragment in repair_row as an
                    // optimization to avoid unfreeze again when
                    // mutation_fragment is needed by _repair_writer.do_write()
                    // to apply the repair_row to disk
                    auto mf = make_lw_shared<mutation_fragment>(fmf.unfreeze(*_schema));
                    auto hash = do_hash_for_mf(*dk_ptr, *mf);
                    position_in_partition pos(mf->position());
                    row_list.push_back(repair_row(std::move(fmf), std::move(pos), dk_ptr, std::move(hash), std::move(mf)));
                });
            }).then([&row_list] {
                return std::move(row_list);
            });
        });
    }

public:
    // RPC API
    // Return the hashes of the rows in _working_row_buf
    future<std::unordered_set<repair_hash>>
    request_full_row_hashes(gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return make_ready_future<std::unordered_set<repair_hash>>(request_full_row_hashes_handler());
        }
        return netw::get_local_messaging_service().send_repair_get_full_row_hashes(msg_addr(remote_node),
                _repair_meta_id).then([this, remote_node] (std::unordered_set<repair_hash> hashes) {
            rlogger.debug("Got full hashes from peer={}, nr_hashes={}", remote_node, hashes.size());
            stats().rx_hashes_nr += hashes.size();
            stats().rpc_call_nr++;
            return hashes;
        });
    }

    // RPC handler
    std::unordered_set<repair_hash>
    request_full_row_hashes_handler() {
        return request_full_row_hashes();
    }

    // RPC API
    // Return the combined hashes of the current working row buf
    future<repair_hash>
    request_combined_row_hash(std::optional<repair_sync_boundary> common_sync_boundary, gms::inet_address remote_node) {
        if (remote_node == _myip) {
            return request_combined_row_hash_handler(common_sync_boundary);
        }
        return netw::get_local_messaging_service().send_repair_get_combined_row_hash(msg_addr(remote_node),
                _repair_meta_id, common_sync_boundary).then([this] (repair_hash combined_hash) {
            stats().rpc_call_nr++;
            stats().rx_hashes_nr++;
            return combined_hash;
        });
    }

    // RPC handler
    future<repair_hash>
    request_combined_row_hash_handler(std::optional<repair_sync_boundary> common_sync_boundary) {
        // We can not call this function twice. The good thing is we do not use
        // retransmission at messaging_service level, so no message will be retransmited.
        rlogger.trace("Calling request_combined_row_hash_handler");
        return request_row_hashes(common_sync_boundary);
    }

    // RPC API
    future<>
    repair_row_level_start(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range) {
        if (remote_node == _myip) {
            return make_ready_future<>();
        }
        stats().rpc_call_nr++;
        return netw::get_local_messaging_service().send_repair_row_level_start(msg_addr(remote_node),
                _repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range), _algo, _max_row_buf_size, _seed,
                _master_node_shard_config.shard, _master_node_shard_config.shard_count, _master_node_shard_config.ignore_msb, _master_node_shard_config.partitioner_name);
    }

    // RPC handler
    static future<>
    repair_row_level_start_handler(gms::inet_address from, uint32_t repair_meta_id, sstring ks_name, sstring cf_name,
            dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size,
            uint64_t seed, shard_config master_node_shard_config) {
        rlogger.debug(">>> Started Row Level Repair (Follower): local={}, peers={}, repair_meta_id={}, keyspace={}, cf={}, range={}",
            utils::fb_utilities::get_broadcast_address(), from, repair_meta_id, ks_name, cf_name, range);
        insert_repair_meta(from, repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range), algo, max_row_buf_size, seed, std::move(master_node_shard_config));
        return make_ready_future<>();
    }

    // RPC API
    future<> repair_row_level_stop(gms::inet_address remote_node, sstring ks_name, sstring cf_name, dht::token_range range) {
        if (remote_node == _myip) {
            return wait_for_writer_done();
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
        return rm->wait_for_writer_done().then([rm, from, repair_meta_id, ks_name, cf_name, range] () mutable {
            remove_repair_meta(from, repair_meta_id, std::move(ks_name), std::move(cf_name), std::move(range));
        });
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
    request_sync_boundary(gms::inet_address remote_node, std::optional<repair_sync_boundary> skipped_sync_boundary) {
        if (remote_node == _myip) {
            return request_sync_boundary_handler(skipped_sync_boundary);
        }
        stats().rpc_call_nr++;
        return netw::get_local_messaging_service().send_repair_get_sync_boundary(msg_addr(remote_node), _repair_meta_id, skipped_sync_boundary);
    }

    // RPC handler
    future<get_sync_boundary_response>
    request_sync_boundary_handler(std::optional<repair_sync_boundary> skipped_sync_boundary) {
        return request_sync_boundary(std::move(skipped_sync_boundary));
    }

    // RPC API
    // Return rows in the _working_row_buf with hash within the given sef_diff
    future<> request_row_diff(std::unordered_set<repair_hash> set_diff, needs_all_rows_t needs_all_rows, gms::inet_address remote_node, unsigned node_idx) {
        if (needs_all_rows || !set_diff.empty()) {
            if (remote_node == _myip) {
                return make_ready_future<>();
            }
            if (needs_all_rows) {
                set_diff.clear();
            } else {
                stats().tx_hashes_nr += set_diff.size();
            }
            stats().rpc_call_nr++;
            return netw::get_local_messaging_service().send_repair_get_row_diff(msg_addr(remote_node),
                    _repair_meta_id, std::move(set_diff), bool(needs_all_rows)).then([this, remote_node, node_idx] (repair_rows_on_wire rows) {
                if (!rows.empty()) {
                    return apply_rows(std::move(rows), remote_node, update_working_row_buf::yes, update_peer_row_hash_sets::no, node_idx);
                }
                return make_ready_future<>();
            });
        }
        return make_ready_future<>();
    }

    future<> request_row_diff_and_update_peer_row_hash_sets(gms::inet_address remote_node, unsigned node_idx) {
        if (remote_node == _myip) {
            return make_ready_future<>();
        }
        stats().rpc_call_nr++;
        return netw::get_local_messaging_service().send_repair_get_row_diff(msg_addr(remote_node),
                _repair_meta_id, {}, bool(needs_all_rows_t::yes)).then([this, remote_node, node_idx] (repair_rows_on_wire rows) {
            if (!rows.empty()) {
                return apply_rows(std::move(rows), remote_node, update_working_row_buf::yes, update_peer_row_hash_sets::yes, node_idx);
            }
            return make_ready_future<>();
        });
        return make_ready_future<>();
    }

    // RPC handler
    future<repair_rows_on_wire> request_row_diff_handler(const std::unordered_set<repair_hash>& set_diff, needs_all_rows_t needs_all_rows) {
        std::list<repair_row> row_diff = get_row_diff(set_diff, needs_all_rows);
        return to_repair_rows_on_wire(std::move(row_diff));
    }

    // RPC API
    // Send rows in the _working_row_buf with hash within the given sef_diff
    future<> send_row_diff(const std::unordered_set<repair_hash>& set_diff, gms::inet_address remote_node) {
        if (!set_diff.empty()) {
            if (remote_node == _myip) {
                return make_ready_future<>();
            }
            std::list<repair_row> row_diff = get_row_diff(set_diff);
            if (row_diff.size() != set_diff.size()) {
                throw std::runtime_error("row_diff.size() != set_diff.size()");
            }
            stats().tx_row_nr += row_diff.size();
            stats().tx_row_nr_peer[remote_node] += row_diff.size();
            stats().tx_row_bytes += get_repair_rows_size(row_diff);
            stats().rpc_call_nr++;
            return to_repair_rows_on_wire(std::move(row_diff)).then([this, remote_node] (repair_rows_on_wire rows)  {
                return netw::get_local_messaging_service().send_repair_put_row_diff(msg_addr(remote_node), _repair_meta_id, std::move(rows));
            });
        }
        return make_ready_future<>();
    }

    // RPC handler
    future<> send_row_diff_handler(repair_rows_on_wire rows, gms::inet_address from) {
        return apply_rows(std::move(rows), from, update_working_row_buf::no, update_peer_row_hash_sets::no);
    }
};
