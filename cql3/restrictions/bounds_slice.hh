/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "utils/assert.hh"
#include <seastar/core/shared_ptr.hh>
#include "index/secondary_index_manager.hh"
#include "cql3/expr/expression.hh"
#include "cql3/statements/bound.hh"

namespace cql3 {

namespace restrictions {

class bounds_slice final {
private:
    struct bound {
        bool inclusive;
        std::optional<expr::expression> value;
    };
    bound _bounds[2];
private:
    bounds_slice(std::optional<expr::expression> start, bool include_start,
                 std::optional<expr::expression> end, bool include_end)
        : _bounds{{include_start, std::move(start)}, {include_end, std::move(end)}}
    { }
public:
    static bounds_slice new_instance(statements::bound bound, bool include, std::optional<expr::expression> value) {
        if (is_start(bound)) {
            return bounds_slice(std::move(value), include, std::nullopt, false);
        } else {
            return bounds_slice(std::nullopt, false, std::move(value), include);
        }
    }

    static bounds_slice from_binary_operator(const expr::binary_operator& binop) {
        if (binop.op == expr::oper_t::LT) {
            return new_instance(statements::bound::END, false, binop.rhs);
        }

        if (binop.op == expr::oper_t::LTE) {
            return new_instance(statements::bound::END, true, binop.rhs);
        }

        if (binop.op == expr::oper_t::GT) {
            return new_instance(statements::bound::START, false, binop.rhs);
        }

        if (binop.op == expr::oper_t::GTE) {
            return new_instance(statements::bound::START, true, binop.rhs);
        }

        throw std::runtime_error(format("bounds_slice::from_binary_operator - invalid operator: {}", binop.op));
    }

    /**
     * Checks if this slice has a boundary for the specified type.
     *
     * @param b the boundary type
     * @return <code>true</code> if this slice has a boundary for the specified type, <code>false</code> otherwise.
     */
    bool has_bound(statements::bound b) const {
        return _bounds[get_idx(b)].value.has_value();
    }

    /**
     * Checks if this slice boundary is inclusive for the specified type.
     *
     * @param b the boundary type
     * @return <code>true</code> if this slice boundary is inclusive for the specified type,
     * <code>false</code> otherwise.
     */
    bool is_inclusive(statements::bound b) const {
        return !_bounds[get_idx(b)].value.has_value() || _bounds[get_idx(b)].inclusive;
    }

    /**
     * Merges this slice with the specified one.
     *
     * @param other the slice to merge with
     */
    void merge(const bounds_slice& other) {
        if (has_bound(statements::bound::START)) {
            SCYLLA_ASSERT(!other.has_bound(statements::bound::START));
            _bounds[get_idx(statements::bound::END)] = other._bounds[get_idx(statements::bound::END)];
        } else {
            SCYLLA_ASSERT(!other.has_bound(statements::bound::END));
            _bounds[get_idx(statements::bound::START)] = other._bounds[get_idx(statements::bound::START)];
        }
    }

    bool is_supported_by(const column_definition& cdef, const secondary_index::index& index) const {
        bool supported = false;
        if (has_bound(statements::bound::START)) {
            supported |= is_inclusive(statements::bound::START)
                    ? index.supports_expression(cdef, cql3::expr::oper_t::GTE)
                    : index.supports_expression(cdef, cql3::expr::oper_t::GT);
        }
        if (has_bound(statements::bound::END)) {
            supported |= is_inclusive(statements::bound::END)
                    ? index.supports_expression(cdef, cql3::expr::oper_t::LTE)
                    : index.supports_expression(cdef, cql3::expr::oper_t::LT);
        }
        return supported;
    }

    sstring to_string() const {
        static auto print_value = [] (const std::optional<expr::expression>& v) -> sstring {
            return v.has_value() ? expr::to_string(*v) : "none";
        };
        return format("({} {}, {} {})",
            _bounds[0].inclusive ? ">=" : ">", print_value(_bounds[0].value),
            _bounds[1].inclusive ? "<=" : "<", print_value(_bounds[1].value));
    }

    friend std::ostream& operator<<(std::ostream& out, const bounds_slice& slice) {
        return out << slice.to_string();
    }

#if 0
    /**
     * Returns the index operator corresponding to the specified boundary.
     *
     * @param b the boundary type
     * @return the index operator corresponding to the specified boundary
     */
    public Operator getIndexOperator(statements::bound b)
    {
        if (b.isStart())
            return boundInclusive[get_idx(b)] ? Operator.GTE : Operator.GT;

        return boundInclusive[get_idx(b)] ? Operator.LTE : Operator.LT;
    }

    /**
     * Check if this <code>TermSlice</code> is supported by the specified index.
     *
     * @param index the Secondary index
     * @return <code>true</code> this type of <code>TermSlice</code> is supported by the specified index,
     * <code>false</code> otherwise.
     */
    public bool isSupportedBy(SecondaryIndex index)
    {
        bool supported = false;

        if (has_bound(statements::bound::START))
            supported |= isInclusive(statements::bound::START) ? index.supportsOperator(Operator.GTE)
                    : index.supportsOperator(Operator.GT);
        if (has_bound(statements::bound::END))
            supported |= isInclusive(statements::bound::END) ? index.supportsOperator(Operator.LTE)
                    : index.supportsOperator(Operator.LT);

        return supported;
    }
#endif
};

}
}
