/*
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

#include <unordered_map>
#include <exception>

#include <seastar/core/sstring.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>

#include "database_fwd.hh"
#include "flat_mutation_reader.hh"
#include "utils/UUID.hh"
#include "streaming/stream_plan.hh"

class repair_exception : public std::exception {
private:
    sstring _what;
public:
    repair_exception(sstring msg) : _what(std::move(msg)) { }
    virtual const char* what() const noexcept override { return _what.c_str(); }
};

class repair_stopped_exception : public repair_exception {
public:
    repair_stopped_exception() : repair_exception("Repair stopped") { }
};

// The tokens are the tokens assigned to the bootstrap node.
future<> bootstrap_with_repair(seastar::sharded<database>& db, locator::token_metadata tm, std::unordered_set<dht::token> bootstrap_tokens);
future<> decommission_with_repair(seastar::sharded<database>& db, locator::token_metadata tm);

// NOTE: repair_start() can be run on any node, but starts a node-global
// operation.
// repair_start() starts the requested repair on this node. It returns an
// integer id which can be used to query the repair's status with
// repair_get_status(). The returned future<int> becomes available quickly,
// as soon as repair_get_status() can be used - it doesn't wait for the
// repair to complete.
future<int> repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options);

// TODO: Have repair_progress contains a percentage progress estimator
// instead of just "RUNNING".
enum class repair_status { RUNNING, SUCCESSFUL, FAILED };

// repair_get_status() returns a future because it needs to run code on a
// different CPU (cpu 0) and that might be a deferring operation.
future<repair_status> repair_get_status(seastar::sharded<database>& db, int id);

// returns a vector with the ids of the active repairs
future<std::vector<int>> get_active_repairs(seastar::sharded<database>& db);

// repair_shutdown() stops all ongoing repairs started on this node (and
// prevents any further repairs from being started). It returns a future
// saying when all repairs have stopped, and attempts to stop them as
// quickly as possible (we do not wait for repairs to finish but rather
// stop them abruptly).
future<> repair_shutdown(seastar::sharded<database>& db);

void check_in_shutdown();

// Abort all the repairs
future<> repair_abort_all(seastar::sharded<database>& db);

enum class repair_checksum {
    legacy = 0,
    streamed = 1,
};

// The class partition_checksum calculates a 256-bit cryptographically-secure
// checksum of a set of partitions fed to it. The checksum of a partition set
// is calculated by calculating a strong hash function (SHA-256) of each
// individual partition, and then XORing the individual hashes together.
// XOR is good enough for merging strong checksums, and allows us to
// independently calculate the checksums of different subsets of the original
// set, and then combine the results into one checksum with the add() method.
// The hash of an individual partition uses both its key and value.
class partition_checksum {
private:
    std::array<uint8_t, 32> _digest; // 256 bits
private:
    static future<partition_checksum> compute_legacy(flat_mutation_reader m);
    static future<partition_checksum> compute_streamed(flat_mutation_reader m);
public:
    constexpr partition_checksum() : _digest{} { }
    explicit partition_checksum(std::array<uint8_t, 32> digest) : _digest(std::move(digest)) { }
    static future<partition_checksum> compute(flat_mutation_reader mr, repair_checksum rt);
    void add(const partition_checksum& other);
    bool operator==(const partition_checksum& other) const;
    bool operator!=(const partition_checksum& other) const { return !operator==(other); }
    friend std::ostream& operator<<(std::ostream&, const partition_checksum&);
    const std::array<uint8_t, 32>& digest() const;
};

// Calculate the checksum of the data held on all shards of a column family,
// in the given token range.
// All parameters to this function are constant references, and the caller
// must ensure they live as long as the future returned by this function is
// not resolved.
future<partition_checksum> checksum_range(seastar::sharded<database> &db,
        const sstring& keyspace, const sstring& cf,
        const ::dht::token_range& range, repair_checksum rt);

class repair_stats {
public:
    uint64_t round_nr = 0;
    uint64_t round_nr_fast_path_already_synced = 0;
    uint64_t round_nr_fast_path_same_combined_hashes= 0;
    uint64_t round_nr_slow_path = 0;

    uint64_t rpc_call_nr = 0;

    uint64_t tx_hashes_nr = 0;
    uint64_t rx_hashes_nr = 0;

    uint64_t tx_row_nr = 0;
    uint64_t rx_row_nr = 0;

    uint64_t tx_row_bytes = 0;
    uint64_t rx_row_bytes = 0;

    std::map<gms::inet_address, uint64_t> row_from_disk_bytes;
    std::map<gms::inet_address, uint64_t> row_from_disk_nr;

    std::map<gms::inet_address, uint64_t> tx_row_nr_peer;
    std::map<gms::inet_address, uint64_t> rx_row_nr_peer;

    lowres_clock::time_point start_time = lowres_clock::now();

public:
    void add(const repair_stats& o);
    sstring get_stats();
};

class repair_neighbors {
public:
    std::vector<gms::inet_address> all;
    std::vector<gms::inet_address> mandatory;
    repair_neighbors() = default;
    explicit repair_neighbors(std::vector<gms::inet_address> a)
        : all(std::move(a)) {
    }
    repair_neighbors(std::vector<gms::inet_address> a, std::vector<gms::inet_address> m)
        : all(std::move(a))
        , mandatory(std::move(m)) {
    }
};

class repair_info {
public:
    seastar::sharded<database>& db;
    sstring keyspace;
    dht::token_range_vector ranges;
    std::vector<sstring> cfs;
    int id;
    shard_id shard;
    std::vector<sstring> data_centers;
    std::vector<sstring> hosts;
    std::unordered_map<dht::token_range, repair_neighbors> neighbors;
    size_t nr_failed_ranges = 0;
    bool aborted = false;
    // Map of peer -> <cf, ranges>
    std::unordered_map<gms::inet_address, std::unordered_map<sstring, dht::token_range_vector>> ranges_need_repair_in;
    std::unordered_map<gms::inet_address, std::unordered_map<sstring, dht::token_range_vector>> ranges_need_repair_out;
    // FIXME: this "100" needs to be a parameter.
    uint64_t target_partitions = 100;
    // This affects how many ranges we put in a stream plan. The more the more
    // memory we use to store the ranges in memory. However, it can reduce the
    // total number of stream_plan we use for the repair.
    size_t sub_ranges_to_stream = 10 * 1024;
    size_t sp_index = 0;
    size_t current_sub_ranges_nr_in = 0;
    size_t current_sub_ranges_nr_out = 0;
    int ranges_index = 0;
    // Only allow one stream_plan in flight
    named_semaphore sp_parallelism_semaphore{1, named_semaphore_exception_factory{"repair sp parallelism"}};
    lw_shared_ptr<streaming::stream_plan> _sp_in;
    lw_shared_ptr<streaming::stream_plan> _sp_out;
    repair_stats _stats;
    bool _row_level_repair;
    uint64_t _sub_ranges_nr = 0;
public:
    repair_info(seastar::sharded<database>& db_,
            const sstring& keyspace_,
            const dht::token_range_vector& ranges_,
            const std::vector<sstring>& cfs_,
            int id_,
            const std::vector<sstring>& data_centers_,
            const std::vector<sstring>& hosts_);
    future<> do_streaming();
    void check_failed_ranges();
    future<> request_transfer_ranges(const sstring& cf,
        const ::dht::token_range& range,
        const std::vector<gms::inet_address>& neighbors_in,
        const std::vector<gms::inet_address>& neighbors_out);
    void abort();
    void check_in_abort();
    repair_neighbors get_repair_neighbors(const dht::token_range& range);
    void update_statistics(const repair_stats& stats) {
        _stats.add(stats);
    }
    bool row_level_repair() {
        return _row_level_repair;
    }
};

// The repair_tracker tracks ongoing repair operations and their progress.
// A repair which has already finished successfully is dropped from this
// table, but a failed repair will remain in the table forever so it can
// be queried about more than once (FIXME: reconsider this. But note that
// failed repairs should be rare anwyay).
// This object is not thread safe, and must be used by only one cpu.
class tracker {
private:
    // Each repair_start() call returns a unique int which the user can later
    // use to follow the status of this repair with repair_status().
    // We can't use the number 0 - if repair_start() returns 0, it means it
    // decide quickly that there is nothing to repair.
    int _next_repair_command = 1;
    // Note that there are no "SUCCESSFUL" entries in the "status" map:
    // Successfully-finished repairs are those with id < _next_repair_command
    // but aren't listed as running or failed the status map.
    std::unordered_map<int, repair_status> _status;
    // Used to allow shutting down repairs in progress, and waiting for them.
    seastar::gate _gate;
    // Set when the repair service is being shutdown
    std::atomic_bool _shutdown alignas(seastar::cache_line_size);
    // Map repair id into repair_info. The vector has smp::count elements, each
    // element will be accessed by only one shard.
    std::vector<std::unordered_map<int, lw_shared_ptr<repair_info>>> _repairs;
    // Each element in the vector is the semaphore used to control the maximum
    // ranges that can be repaired in parallel. Each element will be accessed
    // by one shared.
    std::vector<named_semaphore> _range_parallelism_semaphores;
    static constexpr size_t _max_repair_memory_per_range = 32 * 1024 * 1024;
    void start(int id);
    void done(int id, bool succeeded);
public:
    explicit tracker(size_t nr_shards, size_t max_repair_memory);
    ~tracker();
    repair_status get(int id);
    int next_repair_command();
    future<> shutdown();
    void check_in_shutdown();
    void add_repair_info(int id, lw_shared_ptr<repair_info> ri);
    void remove_repair_info(int id);
    lw_shared_ptr<repair_info> get_repair_info(int id);
    std::vector<int> get_active() const;
    size_t nr_running_repair_jobs();
    void abort_all_repairs();
    named_semaphore& range_parallelism_semaphore();
    static size_t max_repair_memory_per_range() { return _max_repair_memory_per_range; }
    future<> run(int id, std::function<future<> ()> func);
};

future<uint64_t> estimate_partitions(seastar::sharded<database>& db, const sstring& keyspace,
        const sstring& cf, const dht::token_range& range);

// Represent a position of a mutation_fragment read from a flat mutation
// reader. Repair nodes negotiate a small range identified by two
// repair_sync_boundary to work on in each round.
struct repair_sync_boundary {
    dht::decorated_key pk;
    position_in_partition position;
    class tri_compare {
        dht::ring_position_comparator _pk_cmp;
        position_in_partition::tri_compare _position_cmp;
    public:
        tri_compare(const schema& s) : _pk_cmp(s), _position_cmp(s) { }
        int operator()(const repair_sync_boundary& a, const repair_sync_boundary& b) const {
            int ret = _pk_cmp(a.pk, b.pk);
            if (ret == 0) {
                ret = _position_cmp(a.position, b.position);
            }
            return ret;
        }
    };
    friend std::ostream& operator<<(std::ostream& os, const repair_sync_boundary& x) {
        return os << "{ " << x.pk << "," <<  x.position << " }";
    }
};

// Hash of a repair row
class repair_hash {
public:
    uint64_t hash = 0;
    repair_hash() = default;
    explicit repair_hash(uint64_t h) : hash(h) {
    }
    void clear() {
        hash = 0;
    }
    void add(const repair_hash& other) {
        hash ^= other.hash;
    }
    bool operator==(const repair_hash& x) const {
        return x.hash == hash;
    }
    bool operator!=(const repair_hash& x) const {
        return x.hash != hash;
    }
    bool operator<(const repair_hash& x) const {
        return x.hash < hash;
    }
    friend std::ostream& operator<<(std::ostream& os, const repair_hash& x) {
        return os << x.hash;
    }
};

// Return value of the REPAIR_GET_SYNC_BOUNDARY RPC verb
struct get_sync_boundary_response {
    std::optional<repair_sync_boundary> boundary;
    repair_hash row_buf_combined_csum;
    // The current size of the row buf
    uint64_t row_buf_size;
    // The number of bytes this verb read from disk
    uint64_t new_rows_size;
    // The number of rows this verb read from disk
    uint64_t new_rows_nr;
};

// Return value of the REPAIR_GET_COMBINED_ROW_HASH RPC verb
using get_combined_row_hash_response = repair_hash;

struct node_repair_meta_id {
    gms::inet_address ip;
    uint32_t repair_meta_id;
    bool operator==(const node_repair_meta_id& x) const {
        return x.ip == ip && x.repair_meta_id == repair_meta_id;
    }
};

// Represent a partition_key and frozen_mutation_fragments within the partition_key.
class partition_key_and_mutation_fragments {
    partition_key _key;
    std::list<frozen_mutation_fragment> _mfs;
public:
    partition_key_and_mutation_fragments()
        : _key(std::vector<bytes>() ) {
    }
    partition_key_and_mutation_fragments(partition_key key, std::list<frozen_mutation_fragment> mfs)
        : _key(std::move(key))
        , _mfs(std::move(mfs)) {
    }
    const partition_key& get_key() const { return _key; }
    const std::list<frozen_mutation_fragment>& get_mutation_fragments() const { return _mfs; }
    partition_key& get_key() { return _key; }
    std::list<frozen_mutation_fragment>& get_mutation_fragments() { return _mfs; }
    void push_mutation_fragment(frozen_mutation_fragment mf) { _mfs.push_back(std::move(mf)); }
};

using repair_row_on_wire = partition_key_and_mutation_fragments;
using repair_rows_on_wire = std::list<partition_key_and_mutation_fragments>;

enum class repair_stream_cmd : uint8_t {
    error,
    hash_data,
    row_data,
    end_of_current_hash_set,
    needs_all_rows,
    end_of_current_rows,
    get_full_row_hashes,
    put_rows_done,
};

struct repair_hash_with_cmd {
    repair_stream_cmd cmd;
    repair_hash hash;
};

struct repair_row_on_wire_with_cmd {
    repair_stream_cmd cmd;
    repair_row_on_wire row;
};

enum class row_level_diff_detect_algorithm : uint8_t {
    send_full_set,
    send_full_set_rpc_stream,
};

std::ostream& operator<<(std::ostream& out, row_level_diff_detect_algorithm algo);

namespace std {
template<>
struct hash<partition_checksum> {
    size_t operator()(partition_checksum sum) const {
        size_t h = 0;
        std::copy_n(sum.digest().begin(), std::min(sizeof(size_t), sizeof(sum.digest())), reinterpret_cast<uint8_t*>(&h));
        return h;
    }
};

template<>
struct hash<repair_hash> {
    size_t operator()(repair_hash h) const { return h.hash; }
};

template<>
struct hash<node_repair_meta_id> {
    size_t operator()(node_repair_meta_id id) const { return utils::tuple_hash()(id.ip, id.repair_meta_id); }
};

}
