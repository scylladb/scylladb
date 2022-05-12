/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "restriction.hh"
#include "primary_key_restrictions.hh"
#include "exceptions/exceptions.hh"
#include "bounds_slice.hh"
#include "keys.hh"

class column_definition;

namespace cql3 {

namespace restrictions {

/**
 * <code>Restriction</code> using the token function.
 */
class token_restriction: public partition_key_restrictions {
private:
    /**
     * The definition of the columns to which apply the token restriction.
     */
    std::vector<const column_definition *> _column_definitions;
public:
    token_restriction(std::vector<const column_definition *> c)
            : _column_definitions(std::move(c)) {
    }

    std::vector<const column_definition*> get_column_defs() const override {
        return _column_definitions;
    }

    void merge_with(::shared_ptr<restriction> restriction) override {
        if (!has_token(restriction->expression)) {
            throw exceptions::invalid_request_exception(
                    format("Columns \"{}\" cannot be restricted by both a normal relation and a token relation",
                            join(", ", get_column_defs())));
        }
        expression = make_conjunction(std::move(expression), restriction->expression);
    }

    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager,
                                      expr::allow_local_index allow_local) const override {
        return false;
    }

#if 0
    void add_index_expression_to(std::vector<::shared_ptr<index_expression>>& expressions,
                                         const query_options& options) override {
        throw exceptions::unsupported_operation_exception();
    }
#endif
};

}

}
