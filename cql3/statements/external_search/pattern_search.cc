/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "pattern_search.hh"

#include "cql3/column_identifier.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "exceptions/exceptions.hh"
#include "index/pattern_index.hh"
#include "index/secondary_index_manager.hh"
#include "types/types.hh"
#include "utils/assert.hh"

#include <algorithm>

namespace cql3::statements::pattern_search {

namespace {

sstring one_like_message(std::string_view column) {
    return seastar::format("Pattern search queries support exactly one LIKE restriction, on the indexed column {}, and no other WHERE restrictions",
            column);
}

expr::binary_operator single_like_restriction(const restrictions::select_restrictions& select_restrictions, const secondary_index::index& index) {
    if (!select_restrictions.partition_key_restrictions_is_empty() || !restrictions::is_empty_restriction(select_restrictions.get_clustering_columns_restrictions())) {
        throw exceptions::invalid_request_exception("Pattern search queries do not support additional WHERE restrictions");
    }
    const auto& non_pk = select_restrictions.get_non_pk_restriction();
    if (non_pk.size() != 1) {
        throw exceptions::invalid_request_exception(one_like_message(index.target_column()));
    }
    const auto& [column, restriction] = *non_pk.begin();
    if (column->name_as_text() != index.target_column()) {
        throw exceptions::invalid_request_exception(
                seastar::format("Pattern search queries must restrict the indexed column {}", index.target_column()));
    }
    auto factors = expr::boolean_factors(restriction);
    const auto* binop = factors.size() == 1 ? expr::as_if<expr::binary_operator>(&factors.front()) : nullptr;
    if (!binop || binop->op != expr::oper_t::LIKE) {
        throw exceptions::invalid_request_exception(one_like_message(index.target_column()));
    }
    return *binop;
}

} // anonymous namespace

std::optional<secondary_index::index> index_for(data_dictionary::database db, const schema_ptr& schema,
        const expr::expression& where_clause) {
    auto indexes = db.find_column_family(schema).get_index_manager().list_indexes();
    for (const auto& factor : expr::boolean_factors(where_clause)) {
        const auto* binop = expr::as_if<expr::binary_operator>(&factor);
        const auto* lhs = binop && binop->op == expr::oper_t::LIKE ? expr::as_if<expr::unresolved_identifier>(&binop->lhs) : nullptr;
        if (!lhs) {
            continue;
        }
        auto column = lhs->ident->prepare_column_identifier(*schema)->text();
        auto on_column = [&] (const secondary_index::index& index) {
            return secondary_index::pattern_index::is_pattern_index_on_column(index.metadata(), column);
        };
        if (auto it = std::ranges::find_if(indexes, on_column); it != indexes.end()) {
            return std::move(*it);
        }
    }
    return std::nullopt;
}

search_source source_for(secondary_index::index index, const schema& schema, const restrictions::select_restrictions& restrictions,
        bool has_ordering) {
    if (has_ordering) {
        throw exceptions::invalid_request_exception("Pattern search queries do not support ORDER BY");
    }
    auto like = single_like_restriction(restrictions, index);
    const auto* column = schema.get_column_definition(to_bytes(index.target_column()));
    throwing_assert(column);
    return search_source{
            .family = functions::search_family::pattern,
            .index = std::move(index),
            .column = column,
            .query_value = std::move(like.rhs),
    };
}

sstring query_pattern(const cql3::raw_value& value) {
    return value_cast<sstring>(utf8_type->deserialize(cql3::raw_value(value).to_bytes()));
}

} // namespace cql3::statements::pattern_search
