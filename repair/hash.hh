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

#pragma once

#include <functional>
#include <ostream>
#include "schema.hh"
#include "xx_hasher.hh"
#include "dht/i_partitioner.hh"

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

namespace std {
    template<>
    struct hash<repair_hash> {
        size_t operator()(repair_hash h) const { return h.hash; }
    };
}

class decorated_key_with_hash {
public:
    dht::decorated_key dk;
    repair_hash hash;
    decorated_key_with_hash(const schema& s, dht::decorated_key key, uint64_t seed);
};

class column_definition;
class atomic_cell_or_collection;
class mutation_fragment;
class tombstone;
class static_row;
class clustering_row;
class range_tombstone;
class partition_start;

class fragment_hasher {
    const schema& _schema;
    xx_hasher& _hasher;

private:
    void consume_cell(const column_definition& col, const atomic_cell_or_collection& cell);

public:
    explicit fragment_hasher(const schema&s, xx_hasher& h)
        : _schema(s), _hasher(h) { }

    void hash(const mutation_fragment& mf);

private:

    void consume(const tombstone& t);
    void consume(const static_row& sr);
    void consume(const clustering_row& cr);
    void consume(const range_tombstone& rt);
    void consume(const partition_start& ps);
};
