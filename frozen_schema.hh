/*
 * Copyright 2015-present ScyllaDB
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

#include "schema_fwd.hh"
#include "frozen_mutation.hh"
#include "bytes.hh"

namespace db {
class schema_ctxt;
}

// Transport for schema_ptr across shards/nodes.
// It's safe to access from another shard by const&.
class frozen_schema {
    bytes _data;
public:
    explicit frozen_schema(bytes);
    frozen_schema(const schema_ptr&);
    frozen_schema(frozen_schema&&) = default;
    frozen_schema(const frozen_schema&) = default;
    frozen_schema& operator=(const frozen_schema&) = default;
    frozen_schema& operator=(frozen_schema&&) = default;
    schema_ptr unfreeze(const db::schema_ctxt&) const;
    bytes_view representation() const;
};
