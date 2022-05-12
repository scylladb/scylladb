/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "function.hh"
#include <optional>

namespace cql3 {
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

        /**
         * Reset this aggregate.
         */
        virtual void reset() = 0;
    };
};

}
}
