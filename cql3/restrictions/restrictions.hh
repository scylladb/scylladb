/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <vector>

#include "cql3/query_options.hh"
#include "types.hh"
#include "schema_fwd.hh"
#include "index/secondary_index_manager.hh"
#include "restriction.hh"

namespace cql3 {

namespace restrictions {

/**
 * Sets of restrictions
 */
class restrictions {
public:
    virtual ~restrictions() {}

    /**
     * Returns the column definitions in position order.
     * @return the column definitions in position order.
     */
    virtual std::vector<const column_definition*> get_column_defs() const = 0;

    virtual bytes_opt value_for(const column_definition& cdef, const query_options& options) const {
        throw exceptions::invalid_request_exception("Single value can be obtained from single-column restrictions only");
    }

    /**
     * Check if the restriction is on indexed columns.
     *
     * @param index_manager the index manager
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code>
     */
    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager,
                                      expr::allow_local_index allow_local) const = 0;

#if 0
    /**
     * Adds to the specified list the <code>index_expression</code>s corresponding to this <code>Restriction</code>.
     *
     * @param expressions the list to add the <code>index_expression</code>s to
     * @param options the query options
     * @throws InvalidRequestException if this <code>Restriction</code> cannot be converted into
     * <code>index_expression</code>s
     */
    virtual void add_index_expression_to(std::vector<::shared_ptr<index_expression>>& expressions,
                                         const query_options& options) = 0;
#endif

    /**
     * Checks if this <code>SingleColumnprimary_key_restrictions</code> is empty or not.
     *
     * @return <code>true</code> if this <code>SingleColumnprimary_key_restrictions</code> is empty, <code>false</code> otherwise.
     */
    virtual bool empty() const = 0;

    /**
     * Returns the number of columns that have a restriction.
     *
     * @return the number of columns that have a restriction.
     */
    virtual uint32_t size() const = 0;
};

}

}
