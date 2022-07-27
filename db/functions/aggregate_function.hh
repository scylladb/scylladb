/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "function.hh"
#include <optional>

namespace db {
namespace functions {


/**
 * Performs a calculation on a set of values and return a single value.
 */
class aggregate_function : public virtual function {
public:
    class aggregate;

    /**
     * Creates a new <code>Aggregate</code> instance.
     *
     * @return a new <code>Aggregate</code> instance.
     */
    virtual std::unique_ptr<aggregate> new_aggregate() = 0;

    /**
     * Checks wheather the function can be distributed and is able to reduce states.
     *
     * @return <code>true</code> if the function is reducible, <code>false</code> otherwise.
     */
    virtual bool is_reducible() const = 0;

    /**
     * Creates a <code>Aggregate Function</code> that can be reduced.
     * Such <code>Aggregate Function</code> returns <code>Aggregate</code>,
     * which returns not finalized output, but its accumulator that can be 
     * later reduced with other one.
     *
     * Reducible aggregate function can be obtained only if <code>is_reducible()</code>
     * return true. Otherwise, it should return <code>nullptr</code>.
     *
     * @return a reducible <code>Aggregate Function</code>.
     */
    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() = 0;

    /**
     * An aggregation operation.
     */
    class aggregate {
    public:
        using opt_bytes = aggregate_function::opt_bytes;

        virtual ~aggregate() {}

        /**
         * Adds the specified input to this aggregate.
         *
         * @param protocol_version native protocol version
         * @param values the values to add to the aggregate.
         */
        virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) = 0;

        /**
         * Computes and returns the aggregate current value.
         *
         * @param protocol_version native protocol version
         * @return the aggregate current value.
         */
        virtual opt_bytes compute(cql_serialization_format sf) = 0;

        virtual void set_accumulator(const opt_bytes& acc) = 0;

        virtual opt_bytes get_accumulator() const = 0;

        virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) = 0;

        /**
         * Reset this aggregate.
         */
        virtual void reset() = 0;
    };
};

}
}
