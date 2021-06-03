/*
 * Copyright 2020-present ScyllaDB
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

#include "mutation_reader.hh"
#include "memtable.hh"
#include "schema.hh"
#include "database_fwd.hh"

namespace db {

class virtual_table {
protected:
    schema_ptr _s;
    database* _db = nullptr; // Always valid when attached to a database.

protected: // opt-ins
    // If set to true, the implementation ensures that produced data
    // only contains partitions owned by the current shard.
    // Implementations can do this by checking the result of this_shard_owns().
    // If set to false, data will be filtered out automatically.
    bool _shard_aware = false;

protected:
    void set_cell(row&, const bytes& column_name, data_value);
    bool contains_key(const dht::partition_range&, const dht::decorated_key&) const;
    bool this_shard_owns(const dht::decorated_key&) const;

public:
    class query_restrictions {
    public:
        virtual const dht::partition_range& partition_range() const = 0;
    };

    explicit virtual_table(schema_ptr s) : _s(std::move(s)) {}

    const schema_ptr& schema() const { return _s; }

    // Keep this object alive as long as the returned mutation_source is alive.
    virtual mutation_source as_mutation_source() = 0;

    void set_database(database& db) { _db = &db; }
};

// Produces results by filling a memtable on each read.
// Use when the amount of data is not significant relative to shard's memory size.
class memtable_filling_virtual_table : public virtual_table {
public:
    using virtual_table::virtual_table;

    // Override one of these execute() overloads.
    // The handler is always allowed to produce more data than implied by the query_restrictions.
    virtual future<> execute(std::function<void(mutation)> mutation_sink, db::timeout_clock::time_point timeout) { return make_ready_future<>(); }
    virtual future<> execute(std::function<void(mutation)> mutation_sink, db::timeout_clock::time_point timeout, const query_restrictions&) { return execute(mutation_sink, timeout); }

    mutation_source as_mutation_source() override;
};


}
