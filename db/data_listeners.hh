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

#pragma once

#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/weak_ptr.hh>

#include "utils/hash.hh"
#include "schema_fwd.hh"
#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "frozen_mutation.hh"
#include "utils/top_k.hh"
#include "schema_registry.hh"

#include <vector>
#include <set>

namespace db {

class data_listener {
public:
    // Invoked for each write, with partition granularity.
    // The schema_ptr passed is the one which corresponds to the incoming mutation, not the current schema of the table.
    virtual void on_write(const schema_ptr&, const frozen_mutation&) { }

    // Invoked for each query (both data query and mutation query) when a mutation reader is created.
    // Paging queries may invoke this once for a page, or less often, depending on whether they hit in the querier cache or not.
    //
    // The flat_mutation_reader passed to this method is the reader from which the query results are built (uncompacted).
    // This method replaces that reader with the one returned from this method.
    // This allows the listener to install on-the-fly processing for the mutation stream.
    //
    // The schema_ptr passed is the one which corresponds to the reader, not the current schema of the table.
    virtual flat_mutation_reader on_read(const schema_ptr& s, const dht::partition_range& range,
            const query::partition_slice& slice, flat_mutation_reader&& rd) {
        return std::move(rd);
    }
};

class data_listeners {
    std::set<data_listener*> _listeners;

public:
    void install(data_listener* listener);
    void uninstall(data_listener* listener);

    flat_mutation_reader on_read(const schema_ptr& s, const dht::partition_range& range,
            const query::partition_slice& slice, flat_mutation_reader&& rd);
    void on_write(const schema_ptr& s, const frozen_mutation& m);

    bool exists(data_listener* listener) const;
    bool empty() const { return _listeners.empty(); }
};


struct toppartitions_item_key {
    schema_ptr schema;
    dht::decorated_key key;

    toppartitions_item_key(const schema_ptr& schema, const dht::decorated_key& key) : schema(schema), key(key) {}
    toppartitions_item_key(const toppartitions_item_key& key) noexcept : schema(key.schema), key(key.key) {}

    struct hash {
        size_t operator()(const toppartitions_item_key& k) const {
            return std::hash<dht::token>()(k.key.token());
        }
    };

    struct comp {
        bool operator()(const toppartitions_item_key& k1, const toppartitions_item_key& k2) const {
            return k1.schema->id() == k2.schema->id() && k1.key.equal(*k2.schema, k2.key);
        }
    };

    explicit operator sstring() const;
};

// Like toppartitions_item_key, but uses global_schema_ptr, so can be safely transported across shards
struct toppartitions_global_item_key {
    global_schema_ptr schema;
    dht::decorated_key key;

    toppartitions_global_item_key(toppartitions_item_key&& tik) : schema(std::move(tik.schema)), key(std::move(tik.key)) {}
    operator toppartitions_item_key() const {
        return toppartitions_item_key(schema, key);
    }

    struct hash {
        size_t operator()(const toppartitions_global_item_key& k) const {
            return std::hash<dht::token>()(k.key.token());
        }
    };

    struct comp {
        bool operator()(const toppartitions_global_item_key& k1, const toppartitions_global_item_key& k2) const {
            return k1.schema.get()->id() == k2.schema.get()->id() && k1.key.equal(*k2.schema.get(), k2.key);
        }
    };

    explicit operator sstring() const;
};

class toppartitions_data_listener : public data_listener, public weakly_referencable<toppartitions_data_listener> {
    friend class toppartitions_query;

    database& _db;
    std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash> _table_filters;
    std::unordered_set<sstring> _keyspace_filters;

public:
    using top_k = utils::space_saving_top_k<toppartitions_item_key, toppartitions_item_key::hash, toppartitions_item_key::comp>;
    using global_top_k = utils::space_saving_top_k<toppartitions_global_item_key, toppartitions_global_item_key::hash, toppartitions_global_item_key::comp>;
public:
    static global_top_k::results globalize(top_k::results&& r);
    static top_k::results localize(const global_top_k::results& r);
private:
    top_k _top_k_read;
    top_k _top_k_write;

public:
    toppartitions_data_listener(database& db, std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash> table_filters, std::unordered_set<sstring> keyspace_filters);
    ~toppartitions_data_listener();

    virtual flat_mutation_reader on_read(const schema_ptr& s, const dht::partition_range& range,
            const query::partition_slice& slice, flat_mutation_reader&& rd) override;

    virtual void on_write(const schema_ptr& s, const frozen_mutation& m) override;

    future<> stop();
};

class toppartitions_query {
    distributed<database>& _xdb;
    std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash> _table_filters;
    std::unordered_set<sstring> _keyspace_filters;
    std::chrono::milliseconds _duration;
    size_t _list_size;
    size_t _capacity;
    std::unique_ptr<sharded<toppartitions_data_listener>> _query;

public:
    toppartitions_query(seastar::distributed<database>& xdb, std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash>&& table_filters,
        std::unordered_set<sstring>&& keyspace_filters, std::chrono::milliseconds duration, size_t list_size, size_t capacity);

    struct results {
        toppartitions_data_listener::top_k read;
        toppartitions_data_listener::top_k write;

        results(size_t capacity) : read(capacity), write(capacity) {}
    };

    std::chrono::milliseconds duration() const { return _duration; }
    size_t list_size() const { return _list_size; }
    size_t capacity() const { return _capacity; }

    future<> scatter();
    future<results> gather(unsigned results_size = 256);
};

} // namespace db
