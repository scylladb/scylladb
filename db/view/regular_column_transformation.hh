/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "column_computation.hh"
#include "mutation/atomic_cell.hh"
#include "timestamp.hh"
#include <type_traits>

class row_marker;

// In a basic column_computation defined in column_computation.hh, the
// compute_value() method is only based on the partition key, and it must
// return a value. That API has very limited applications - basically the
// only thing we can implement with it is token_column_computation which
// we used to create the token column in secondary indexes.
// The regular_column_transformation base class here is more powerful, but
// still is not a completely general computation: Its compute_value() virtual
// method can transform the value read from a single cell of a regular column
// into a new cell stored in a structure regular_column_transformation::result.
//
// In more details, the assumptions of regular_column_transformation is:
// 1. compute_value() computes the value based on a *single* column in a
//    row passed to compute_value().
//    This assumption means that the value or deletion of the value always
//    has a single known timestamp (and the value can't be half-missing)
//    and single TTL information. That would not have been possible if we
//    allowed the computation to depend on multiple columns.
// 2. compute_value() computes the value based on a *regular* column in the
//    base table. This means that an update can modify this value (unlike a
//    base-table key column that can't change in an update), so the view
//    update code needs to compute the value before and after the update,
//    and potentially delete and create view rows.
// 3. compute_value() returns a column_computation::result which includes
//    a value and its liveness information (timestamp and ttl/expiry) or
//    is missing a value.

class regular_column_transformation : public column_computation {
public:
    struct result {
        // We can use "bytes" instead of "managed_bytes" here because we know
        // that a column_computation is only used for generating a key value,
        // and that is limited to 64K. This limitation is enforced below -
        // we never linearize a cell's value if its size is more than 64K.
        std::optional<bytes> _value;

        // _ttl and _expiry are only defined if _value is set.
        // The default values below are used when the source cell does not
        // expire, and are the same values that row_marker uses for a non-
        // expiring marker. This is useful when creating a row_marker from
        // get_ttl() and get_expiry().
        gc_clock::duration _ttl { 0 };
        gc_clock::time_point _expiry { gc_clock::duration(0) };

        // _ts may be set even if _value is missing, which can remember the
        // timestamp of a tombstone. Note that the current view-update code
        // that uses this class doesn't use _ts when _value is missing.
        api::timestamp_type _ts = api::missing_timestamp;

        api::timestamp_type get_ts() const {
            return _ts;
        }

        bool has_value() const {
            return _value.has_value();
        }

        // Should only be called if has_value() is true:
        const bytes& get_value() const {
            return *_value;
        }
        gc_clock::duration get_ttl() const {
            return _ttl;
        }
        gc_clock::time_point get_expiry() const {
            return _expiry;
        }

        // A missing computation result
        result() { }

        // Construct a computation result by copying a given atomic_cell -
        // including its value, timestamp, and ttl - or deletion timestamp.
        // The second parameter is an optional transformation function f -
        // taking a bytes and returning an optional<bytes> - that transforms
        // the value of the cell but keeps its other liveness information.
        // If f returns a nullopt, it causes the view row should be deleted.
        template<typename Func=std::identity>
        requires std::invocable<Func, bytes> && std::convertible_to<std::invoke_result_t<Func, bytes>, std::optional<bytes>>
        result(atomic_cell_view cell, Func f = {}) {
            _ts = cell.timestamp();
            if (cell.is_live()) {
                // If the cell is larger than what a key can hold (64KB),
                // return a missing value. This lets us skip this item during
                // view building and avoid hanging the view build as described
                // in #8627. But it doesn't prevent later inserting such a item
                // to the base table, nor does it implement front-end specific
                // limits (such as Alternator's 1K or 2K limits - see #10347).
                // Those stricter limits should be validated in the base-table
                // write code, not here - deep inside the view update code.
                // Note also we assume that f() doesn't grow the value further.
                if (cell.value().size() >= 65536) {
                    return;
                }
                _value = f(to_bytes(cell.value()));
                if (_value) {
                    if (cell.is_live_and_has_ttl()) {
                        _ttl = cell.ttl();
                        _expiry = cell.expiry();
                    }
                }
            }
        }
    };

    virtual ~regular_column_transformation() = default;
    virtual result compute_value(
        const schema& schema,
        const partition_key& key,
        const db::view::clustering_or_static_row& row) const = 0;
 };
