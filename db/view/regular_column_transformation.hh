/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "column_computation.hh"
#include "mutation/atomic_cell.hh"
#include "timestamp.hh"
#include <type_traits>

// In a basic column_computation defined in column_computation.hh, the
// compute_value() method is only based on the partition key, and it must
// return a value. That API has very limited applications - basically the
// only thing we can implement with it is token_column_computation which
// we used to create the token column in secondary indexes.
// The regular_column_transformation base class here is more powerful, but
// still is not a completely general computation: Its compute_value() virtual
// method can transform the value read from a single cell of regular column
// in a row, into a new cell (i.e., a value or deletion, timestamp, and TTL),
// stored in the structure regular_column_transformation::result.
//
// In more details, the assumptions of regular_column_transformation is:
// 1. compute_value() computes the value based on a *single* column in a
//    row passed to compute_value().
//    This assumption means that the value or deletion of the value always
//    has a single known timestamp (and the value can't be half-missing)
//    and TTL information. That would not have been possible if we allowed
//    the computation to depend on multiple columns.
// 2. The source is a *regular* column in the base table. This means that an
//    update can modify it (unlike a base-table key column that can't change
//    in an update) so we may need to read the value before and after the
//    update, and delete and create view rows.
// 3. compute_value() can return 1. a value, or 2. "missing" if the column
//    to be transformed is missing from the given row, or 3. "deleted"
//    if the row contains a deletion (a tombstone) for the column.

class regular_column_transformation : public column_computation {
public:
    struct result {
        std::optional<bytes> _value;
        bool _deleted = false; // if _deleted, "value" is unset
        api::timestamp_type _ts = api::missing_timestamp; // defined if has_value() or is_deleted() but not if is_missing()
        gc_clock::duration _ttl; // defined if has_value()
        gc_clock::time_point _expiry; // defined if has_value()

        // Same convention as in class row_marker
        static constexpr gc_clock::duration no_ttl { 0 };
        static constexpr gc_clock::time_point no_expiry { gc_clock::duration(0) };

        bool has_value() const {
            return _value.has_value();
        }
        // Should only be called if has_value() is true
        const bytes& get_value() const {
            return *_value;
        }

        // Should only be called if has_value() or is_deleted() (i.e., !is_missing())
        api::timestamp_type get_ts() const {
            return _ts;
        }
        gc_clock::duration get_ttl() const {
            return _ttl;
        }
        gc_clock::time_point get_expiry() const {
            return _expiry;
        }

        // Note: the existing code doesn't use the is_deleted() or is_missing()
        // functions, or makes a distinction between a missing and deleted
        // cells, so this part of this class is kind of superflous. The code
        // using this assumes that if a cell existed prior to an update and
        // gone after the update, it is a is_deleted() and has a deletion
        // timestamp. If a cell didn't exist prior to the update nor after the
        // update, we don't even care if it has a timestamp (is_missing()
        // or is_deleted() and don't try to retrieve its timestamp)
        bool is_deleted() const {
            return _deleted;
        }
        bool is_missing() const {
            return !has_value() && !is_deleted();
        }

        // A missing computation result
        result() { }

        // Construct a computation result by copying a given atomic_cell -
        // including its value, timestamp, and ttl - or deletion timestamp.
        // The second parameter is an optional transformation function f -
        // taking a bytes and returning an optional<bytes> - that transforms
        // the value of the cell but keeps its other liveness information.
        // If f returns a nullopt, it means the view row should no longer
        // exists - i.e., it's a deletion.
        template<typename Func=std::identity>
        result(atomic_cell_view cell, Func f = {}) {
            static_assert(std::is_invocable_v<Func, bytes> && std::is_convertible_v<decltype(f(bytes{})), std::optional<bytes>>);
            _ts = cell.timestamp();
            if (cell.is_live()) {
                _value = f(to_bytes(cell.value()));
                if (_value) {
                    if (cell.is_live_and_has_ttl()) {
                        _ttl = cell.ttl();
                        _expiry = cell.expiry();
                    } else {
                        _ttl = no_ttl;
                        _expiry = no_expiry;
                    }
                } else {
                    // can only happen if f() is non-identity and can return nullopt,
                    // e.g., Alternator's serialized_value_if_type().
                    _deleted = true;
                }
            } else {
                _deleted = true;
            }
        }
    };

    virtual ~regular_column_transformation() = default;
    virtual result compute_value(
        const schema& schema,
        const partition_key& key,
        const db::view::clustering_or_static_row& row) const = 0;
 };
