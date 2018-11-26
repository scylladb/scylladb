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

#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>

#include "schema.hh"
#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "frozen_mutation.hh"

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
    database& _db;
    std::set<data_listener*> _listeners;

public:
    data_listeners(database& db) : _db(db) {}

    void install(data_listener* listener);
    void uninstall(data_listener* listener);

    flat_mutation_reader on_read(const schema_ptr& s, const dht::partition_range& range,
            const query::partition_slice& slice, flat_mutation_reader&& rd);
    void on_write(const schema_ptr& s, const frozen_mutation& m);

    bool exists(data_listener* listener) const;
    bool empty() const { return _listeners.empty(); }
};


} // namespace db
