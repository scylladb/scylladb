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

#include "database.hh"
#include "utils/UUID.hh"


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

using repair_rows_on_wire = std::list<partition_key_and_mutation_fragments>;

enum class row_level_diff_detect_algorithm : uint8_t {
    send_full_set,
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
