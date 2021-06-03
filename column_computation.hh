/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "bytes.hh"

class schema;
class partition_key;
class clustering_row;

class column_computation;
using column_computation_ptr = std::unique_ptr<column_computation>;

/*
 * Column computation represents a computation performed in order to obtain a value for a computed column.
 * Computed columns description is also available at docs/system_schema_keyspace.md. They hold values
 * not provided directly by the user, but rather computed: from other column values and possibly other sources.
 * This class is able to serialize/deserialize column computations and perform the computation itself,
 * based on given schema, partition key and clustering row. Responsibility for providing enough data
 * in the clustering row in order for computation to succeed belongs to the caller. In particular,
 * generating a value might involve performing a read-before-write if the computation is performed
 * on more values than are present in the update request.
 */
class column_computation {
public:
    virtual ~column_computation() = default;

    static column_computation_ptr deserialize(bytes_view raw);

    virtual column_computation_ptr clone() const = 0;

    virtual bytes serialize() const = 0;
    virtual bytes_opt compute_value(const schema& schema, const partition_key& key, const clustering_row& row) const = 0;
};

/*
 * Computes token value of partition key and returns it as bytes.
 *
 * Should NOT be used (use token_column_computation), because ordering
 * of bytes is different than ordering of tokens (signed vs unsigned comparison).
 *
 * The type name stored for computations of this class is "token" - this was
 * the original implementation. (now depracated for new tables)
 */
class legacy_token_column_computation : public column_computation {
public:
    virtual column_computation_ptr clone() const override {
        return std::make_unique<legacy_token_column_computation>(*this);
    }
    virtual bytes serialize() const override;
    virtual bytes_opt compute_value(const schema& schema, const partition_key& key, const clustering_row& row) const override;
};


/*
 * Computes token value of partition key and returns it as long_type.
 * The return type means that it can be trivially sorted (for example
 * if computed column using this computation is a clustering key),
 * preserving the correct order of tokens (using signed comparisons).
 *
 * Please use this class instead of legacy_token_column_computation.
 * 
 * The type name stored for computations of this class is "token_v2".
 * (the name "token" refers to the depracated legacy_token_column_computation)
 */
class token_column_computation : public column_computation {
public:
    virtual column_computation_ptr clone() const override {
        return std::make_unique<token_column_computation>(*this);
    }
    virtual bytes serialize() const override;
    virtual bytes_opt compute_value(const schema& schema, const partition_key& key, const clustering_row& row) const override;
};
