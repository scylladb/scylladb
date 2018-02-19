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

namespace std {
template<>
struct hash<partition_checksum> {
    size_t operator()(partition_checksum sum) const {
        size_t h = 0;
        std::copy_n(sum.digest().begin(), std::min(sizeof(size_t), sizeof(sum.digest())), reinterpret_cast<uint8_t*>(&h));
        return h;
    }
};
}
