/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "bytes.hh"

class schema;
class partition_key;
struct atomic_cell_view;
struct tombstone;

namespace db::view {
struct clustering_or_static_row;
struct view_key_and_action;
}

class column_computation;
using column_computation_ptr = std::unique_ptr<column_computation>;

/*
 * Column computation represents a computation performed in order to obtain a value for a computed column.
 * Computed columns description is also available at docs/dev/system_schema_keyspace.md. They hold values
 * not provided directly by the user, but rather computed: from other column values and possibly other sources.
 * This class is able to serialize/deserialize column computations and perform the computation itself,
 * based on given schema, and partition key. Responsibility for providing enough data
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
    virtual bytes compute_value(const schema& schema, const partition_key& key) const = 0;
    /*
     * depends_on_non_primary_key_column for a column computation is needed to
     * detect a case where the primary key of a materialized view depends on a
     * non primary key column from the base table, but at the same time, the view
     * itself doesn't have non-primary key columns. This is an issue, since as
     * for now, it was assumed that no non-primary key columns in view schema
     * meant that the update cannot change the primary key of the view, and
     * therefore the update path can be simplified.
     */
    virtual bool depends_on_non_primary_key_column() const {
        return false;
    }
};

/*
 * Computes token value of partition key and returns it as bytes.
 *
 * Should NOT be used (use token_column_computation), because ordering
 * of bytes is different than ordering of tokens (signed vs unsigned comparison).
 *
 * The type name stored for computations of this class is "token" - this was
 * the original implementation. (now deprecated for new tables)
 */
class legacy_token_column_computation : public column_computation {
public:
    virtual column_computation_ptr clone() const override {
        return std::make_unique<legacy_token_column_computation>(*this);
    }
    virtual bytes serialize() const override;
    virtual bytes compute_value(const schema& schema, const partition_key& key) const override;
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
 * (the name "token" refers to the deprecated legacy_token_column_computation)
 */
class token_column_computation : public column_computation {
public:
    virtual column_computation_ptr clone() const override {
        return std::make_unique<token_column_computation>(*this);
    }
    virtual bytes serialize() const override;
    virtual bytes compute_value(const schema& schema, const partition_key& key) const override;
};

/*
 * collection_column_computation is used for a secondary index on a collection
 * column. In this case we don't have a single value to compute, but rather we
 * want to return multiple values (e.g., all the keys in the collection).
 * So this class does not implement the base class's compute_value() -
 * instead it implements a new method compute_collection_values(), which
 * can return multiple values. This new method is currently called only from
 * the materialized-view code which uses collection_column_computation.
 */
class collection_column_computation final : public column_computation {
    enum class kind {
        keys,
        values,
        entries,
    };
    const bytes _collection_name;
    const kind _kind;
    collection_column_computation(const bytes& collection_name, kind kind) : _collection_name(collection_name), _kind(kind) {}

    using collection_kv = std::pair<bytes_view, atomic_cell_view>;
    void operate_on_collection_entries(
            std::invocable<collection_kv*, collection_kv*, tombstone> auto&& old_and_new_row_func, const schema& schema,
            const partition_key& key, const db::view::clustering_or_static_row& update, const std::optional<db::view::clustering_or_static_row>& existing) const;

public:
    static collection_column_computation for_keys(const bytes& collection_name) {
        return {collection_name, kind::keys};
    }
    static collection_column_computation for_values(const bytes& collection_name) {
        return {collection_name, kind::values};
    }
    static collection_column_computation for_entries(const bytes& collection_name) {
        return {collection_name, kind::entries};
    }
    static column_computation_ptr for_target_type(std::string_view type, const bytes& collection_name);

    virtual bytes serialize() const override;
    virtual bytes compute_value(const schema& schema, const partition_key& key) const override;
    virtual column_computation_ptr clone() const override {
        return std::make_unique<collection_column_computation>(*this);
    }
    virtual bool depends_on_non_primary_key_column() const override {
        return true;
    }

    std::vector<db::view::view_key_and_action> compute_values_with_action(const schema& schema, const partition_key& key,
            const db::view::clustering_or_static_row& row, const std::optional<db::view::clustering_or_static_row>& existing) const;
};
