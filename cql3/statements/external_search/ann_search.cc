/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "ann_search.hh"

#include "cql3/functions/functions.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "index/vector_index.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "types/vector.hh"
#include "utils/assert.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/util.hh"

#include <cmath>

namespace cql3::statements::ann_search {

std::vector<float> query_vector(const column_definition& column, const cql3::raw_value& value) {
    throwing_assert(!value.is_null());

    auto values = value_cast<vector_type_impl::native_type>(column.type->deserialize(value.to_managed_bytes_view()));
    return util::to_vector<float>(values);
}

uint64_t candidates_wanted(const secondary_index::index& index, uint64_t wanted) {
    return static_cast<uint64_t>(std::ceil(
            static_cast<double>(wanted) * secondary_index::vector_index::get_oversampling(index.metadata().options())));
}

expr::expression similarity_expression(const secondary_index::index& index, const column_definition* column,
        const expr::expression& query_vector, data_dictionary::database db, const schema_ptr& schema) {
    auto similarity_function_name = secondary_index::vector_index::get_cql_similarity_function_name(index.metadata().options());
    auto func_name = functions::function_name::native_function(sstring(similarity_function_name));

    std::vector<expr::expression> args;
    args.push_back(expr::column_value(column));
    args.push_back(query_vector);

    std::vector<shared_ptr<assignment_testable>> provided_args;
    provided_args.push_back(expr::as_assignment_testable(args[0], expr::type_of(args[0])));
    provided_args.push_back(expr::as_assignment_testable(args[1], expr::type_of(args[1])));

    auto func = cql3::functions::instance().get(db, schema->ks_name(), func_name, provided_args, schema->ks_name(), schema->cf_name(), nullptr);

    return expr::function_call{
        .func = func,
        .args = std::move(args),
    };
}

} // namespace cql3::statements::ann_search
