/*
 * Copyright (C) 2016 ScyllaDB
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

#include "dht/i_partitioner.hh"
#include "query-request.hh"
#include "schema.hh"
#include "stdx.hh"

namespace cql3 {
namespace statements {
class select_statement;
}
}

namespace db {

namespace view {

class view final {
    view_ptr _schema;
    mutable shared_ptr<cql3::statements::select_statement> _select_statement;
    mutable stdx::optional<query::partition_slice> _partition_slice;
    mutable stdx::optional<dht::partition_range_vector> _partition_ranges;
public:
    explicit view(view_ptr schema)
            : _schema(std::move(schema)) {
    }

    view_ptr schema() const {
        return _schema;
    }

    void update(view_ptr new_schema) {
        _schema = new_schema;
        _select_statement = nullptr;
        _partition_slice = { };
        _partition_ranges = { };
    }

    /**
     * Whether the view filter considers the specified partition key.
     *
     * @param base the base table schema.
     * @param key the partition key that is updated.
     * @return false if we can guarantee that inserting an update for specified key
     * won't affect the view in any way, true otherwise.
     */
    bool partition_key_matches(const ::schema& base, const dht::decorated_key& key) const;

    /**
     * Whether the view might be affected by the provided update.
     *
     * Note that having this method return true is not an absolute guarantee that the view will be
     * updated, just that it most likely will, but a false return guarantees it won't be affected.
     *
     * @param base the base table schema.
     * @param key the partition key that is updated.
     * @param update the base table update being applied.
     * @return false if we can guarantee that inserting update for key
     * won't affect the view in any way, true otherwise.
     */
    bool may_be_affected_by(const ::schema& base, const dht::decorated_key& key, const rows_entry& update) const;

private:
    cql3::statements::select_statement& select_statement() const;
    const query::partition_slice& partition_slice() const;
    const dht::partition_range_vector& partition_ranges() const;
    bool clustering_prefix_matches(const ::schema& base, const partition_key& key, const clustering_key_prefix& ck) const;
};

}

}
