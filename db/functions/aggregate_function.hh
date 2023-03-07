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
#include "stateless_aggregate_function.hh"
#include <optional>

namespace db {
namespace functions {


/**
 * Performs a calculation on a set of values and return a single value.
 */
class aggregate_function : public virtual function {
protected:
    stateless_aggregate_function _agg;
private:
    shared_ptr<aggregate_function> _reducible;
private:
    class aggregate_adapter;
    static shared_ptr<aggregate_function> make_reducible_variant(stateless_aggregate_function saf);
public:
    class aggregate;

    explicit aggregate_function(stateless_aggregate_function saf, bool reducible_variant = false);

    /**
     * Creates a new <code>Aggregate</code> instance.
     *
     * @return a new <code>Aggregate</code> instance.
     */
    std::unique_ptr<aggregate> new_aggregate();

    /**
     * Checks wheather the function can be distributed and is able to reduce states.
     *
     * @return <code>true</code> if the function is reducible, <code>false</code> otherwise.
     */
    bool is_reducible() const;

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
    ::shared_ptr<aggregate_function> reducible_aggregate_function();

    virtual const function_name& name() const override;
    virtual const std::vector<data_type>& arg_types() const override;
    virtual const data_type& return_type() const override;
    virtual bool is_pure() const override;
    virtual bool is_native() const override;
    virtual bool requires_thread() const override;
    virtual bool is_aggregate() const override;
    virtual void print(std::ostream& os) const override;
    virtual sstring column_name(const std::vector<sstring>& column_names) const override;

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
         * @param values the values to add to the aggregate.
         */
        virtual void add_input(const std::vector<opt_bytes>& values) = 0;

        /**
         * Computes and returns the aggregate current value.
         *
         * @return the aggregate current value.
         */
        virtual opt_bytes compute() = 0;

        virtual void set_accumulator(const opt_bytes& acc) = 0;

        virtual opt_bytes get_accumulator() const = 0;

        virtual void reduce(const opt_bytes& acc) = 0;

        /**
         * Reset this aggregate.
         */
        virtual void reset() = 0;
    };
};

}
}
