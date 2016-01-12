/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "database_fwd.hh"
#include "mutation_partition_visitor.hh"

// View on serialized mutation partition. See mutation_partition_serializer.
class mutation_partition_view {
    bytes_view _bytes;
private:
    mutation_partition_view(bytes_view v)
        : _bytes(v)
    { }
public:
    static mutation_partition_view from_bytes(bytes_view v) { return { v }; }
    void accept(const schema& schema, mutation_partition_visitor& visitor) const;
    void accept(const column_mapping&, mutation_partition_visitor& visitor) const;
};
