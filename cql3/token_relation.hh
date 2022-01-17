/*
 */
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

#include <seastar/core/shared_ptr.hh>
#include "to_string.hh"

#include "relation.hh"
#include "column_identifier.hh"
#include "restrictions/restriction.hh"
#include "expr/expression.hh"

namespace cql3 {

/**
 * A relation using the token function.
 * Examples:
 * <ul>
 * <li>SELECT ... WHERE token(a) &gt; token(1)</li>
 * <li>SELECT ... WHERE token(a, b) &gt; token(1, 3)</li>
 * </ul>
 */
class token_relation : public relation {
private:
    std::vector<::shared_ptr<column_identifier::raw>> _entities;
    expr::expression _value;

    /**
     * Returns the definition of the columns to which apply the token restriction.
     *
     * @param cfm the column family metadata
     * @return the definition of the columns to which apply the token restriction.
     * @throws InvalidRequestException if the entity cannot be resolved
     */
    std::vector<const column_definition*> get_column_definitions(const schema& s);

    /**
     * Returns the receivers for this relation.
     *
     * @param cfm the Column Family meta data
     * @param columnDefs the column definitions
     * @return the receivers for the specified relation.
     * @throws InvalidRequestException if the relation is invalid
     */
    std::vector<lw_shared_ptr<column_specification>> to_receivers(const schema& schema, const std::vector<const column_definition*>& column_defs) const;

public:
    token_relation(std::vector<::shared_ptr<column_identifier::raw>> entities,
            expr::oper_t type, expr::expression value)
            : relation(type), _entities(std::move(entities)), _value(
                    std::move(value)) {
    }

    bool on_token() const override {
        return true;
    }

    ::shared_ptr<restrictions::restriction> new_EQ_restriction(data_dictionary::database db,
            schema_ptr schema,
            prepare_context& ctx) override;

    ::shared_ptr<restrictions::restriction> new_IN_restriction(data_dictionary::database db,
            schema_ptr schema,
            prepare_context& ctx) override;

    ::shared_ptr<restrictions::restriction> new_slice_restriction(data_dictionary::database db,
            schema_ptr schema,
            prepare_context& ctx,
            statements::bound bound,
            bool inclusive) override;

    ::shared_ptr<restrictions::restriction> new_contains_restriction(
            data_dictionary::database db, schema_ptr schema,
            prepare_context& ctx, bool isKey) override;

    ::shared_ptr<restrictions::restriction> new_LIKE_restriction(data_dictionary::database db,
            schema_ptr schema,
            prepare_context& ctx) override;

    ::shared_ptr<relation> maybe_rename_identifier(const column_identifier::raw& from, column_identifier::raw to) override;

    sstring to_string() const override;

protected:
    expr::expression to_expression(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                                   const expr::expression& raw,
                                   data_dictionary::database db,
                                   const sstring& keyspace,
                                   prepare_context& ctx) const override;
};

}
