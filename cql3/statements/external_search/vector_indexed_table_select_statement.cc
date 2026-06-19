/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/vector_indexed_table_select_statement.hh"

#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/functions.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/util.hh"

#include "db/consistency_level_validations.hh"
#include "exceptions/exceptions.hh"
#include "index/vector_index.hh"
#include "types/vector.hh"

#include <seastar/core/future.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/exception.hh>


namespace cql3 {

namespace statements {

static logging::logger logger("vector_indexed_table_select_statement");

namespace {

std::vector<float> get_ann_ordering_vector(const select_statement::prepared_ann_ordering_type& prepared_ann_ordering, const query_options& options) {
    auto const& [ann_column, ann_vector_expr] = prepared_ann_ordering;
    auto expr_value = expr::evaluate(ann_vector_expr, options);
    if (expr_value.is_null()) {
        throw exceptions::invalid_request_exception(fmt::format("Unsupported null value for column {}", ann_column->name_as_text()));
    }
    auto values = value_cast<vector_type_impl::native_type>(ann_column->type->deserialize(std::move(expr_value).to_bytes()));
    return util::to_vector<float>(values);
}

} // anonymous namespace

std::optional<ann_ordering_info> get_ann_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        lw_shared_ptr<const raw::select_statement::parameters> parameters,
        prepare_context& ctx) {

    if (parameters->orderings().empty()) {
        return std::nullopt;
    }

    auto [column_id, ordering] = parameters->orderings().front();
    const auto& ann_vector = std::get_if<raw::select_statement::ann_vector>(&ordering);
    if (!ann_vector) {
        return std::nullopt;
    }

    ::shared_ptr<column_identifier> column = column_id->prepare_column_identifier(*schema);
    const column_definition* def = schema->get_column_definition(column->name());
    if (!def) {
        throw exceptions::invalid_request_exception(
                fmt::format("Undefined column name {}", column->text()));
    }

    if (!def->type->is_vector() || static_cast<const vector_type_impl*>(def->type.get())->get_elements_type()->get_kind() != abstract_type::kind::float_kind) {
        throw exceptions::invalid_request_exception("ANN ordering is only supported on float vector indexes");
    }

    auto e =  expr::prepare_expression(*ann_vector, db, schema->ks_name(), nullptr, def->column_specification);
    expr::fill_prepare_context(e, ctx);

    raw::select_statement::prepared_ann_ordering_type prepared_ann_ordering = std::make_pair(std::move(def), std::move(e));

    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();

    auto indexes = sim.list_indexes();
    auto it = std::find_if(indexes.begin(), indexes.end(), [&prepared_ann_ordering](const auto& ind) {
        return secondary_index::vector_index::is_vector_index_on_column(ind.metadata(), prepared_ann_ordering.first->name_as_text());
    });

    if (it == indexes.end()) {
        throw exceptions::invalid_request_exception("ANN ordering by vector requires the column to be indexed using 'vector_index'");
    }

    return ann_ordering_info{
        *it,
        std::move(prepared_ann_ordering),
        secondary_index::vector_index::is_rescoring_enabled(it->metadata().options())
    };
}

uint32_t add_similarity_function_to_selectors(
        std::vector<selection::prepared_selector>& prepared_selectors,
        const ann_ordering_info& ann_ordering_info,
        data_dictionary::database db,
        schema_ptr schema) {
    auto similarity_function_name = secondary_index::vector_index::get_cql_similarity_function_name(ann_ordering_info._index.metadata().options());
    // Create the function name
    auto func_name = functions::function_name::native_function(sstring(similarity_function_name));

    // Create the function arguments
    std::vector<expr::expression> args;
    args.push_back(expr::column_value(ann_ordering_info._prepared_ann_ordering.first));
    args.push_back(ann_ordering_info._prepared_ann_ordering.second);

    // Get the function object
    std::vector<shared_ptr<assignment_testable>> provided_args;
    provided_args.push_back(expr::as_assignment_testable(args[0], expr::type_of(args[0])));
    provided_args.push_back(expr::as_assignment_testable(args[1], expr::type_of(args[1])));

    auto func = cql3::functions::instance().get(db, schema->ks_name(), func_name, provided_args, schema->ks_name(), schema->cf_name(), nullptr);

    // Create the function call expression
    expr::function_call similarity_func_call{
        .func = func,
        .args = std::move(args),
    };

    // Add the similarity function as a prepared selector (last)
    prepared_selectors.push_back(selection::prepared_selector{
        .expr = std::move(similarity_func_call),
        .alias = nullptr,
    });
    return prepared_selectors.size() - 1;
}

select_statement::ordering_comparator_type get_similarity_ordering_comparator(std::vector<selection::prepared_selector>& prepared_selectors, uint32_t similarity_column_index) {
    auto type = expr::type_of(prepared_selectors[similarity_column_index].expr);
    if (type->get_kind() != abstract_type::kind::float_kind) {
        seastar::on_internal_error(logger, "Similarity function must return float type.");
    }
    return [similarity_column_index, type] (const raw::select_statement::result_row_type& r1, const raw::select_statement::result_row_type& r2) {
        auto& c1 = r1[similarity_column_index];
        auto& c2 = r2[similarity_column_index];
        auto f1 = c1 ? value_cast<float>(type->deserialize(*c1)) : std::numeric_limits<float>::quiet_NaN();
        auto f2 = c2 ? value_cast<float>(type->deserialize(*c2)) : std::numeric_limits<float>::quiet_NaN();
        if (std::isfinite(f1) && std::isfinite(f2)) {
            return f1 > f2;
        }
        return std::isfinite(f1);
    };
}

::shared_ptr<cql3::statements::select_statement> vector_indexed_table_select_statement::prepare(data_dictionary::database db, schema_ptr schema,
        uint32_t bound_terms, lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::statement_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats, const secondary_index::index& index, std::unique_ptr<attributes> attrs) {

    auto prepared_filter = external_search::prepare_filter(*restrictions, parameters->allow_filtering());

    return ::make_shared<cql3::statements::vector_indexed_table_select_statement>(schema, bound_terms, parameters, std::move(selection), std::move(restrictions),
            std::move(group_by_cell_indices), is_reversed, std::move(ordering_comparator), std::move(prepared_ann_ordering), std::move(limit),
            std::move(per_partition_limit), stats, index, std::move(prepared_filter), std::move(attrs));
}

vector_indexed_table_select_statement::vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
        prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats, const secondary_index::index& index,
        external_search::prepared_filter prepared_filter, std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices,
              is_reversed, ordering_comparator, limit, per_partition_limit, stats, index, std::move(attrs)}
    , _prepared_ann_ordering(std::move(prepared_ann_ordering))
    , _prepared_filter(std::move(prepared_filter)) {

    if (!limit.has_value()) {
        throw exceptions::invalid_request_exception("Vector ANN queries must have a limit specified");
    }

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Vector ANN queries do not support per-partition limits");
    }

    if (selection->is_aggregate()) {
        throw exceptions::invalid_request_exception("Vector ANN queries cannot be run with aggregation");
    }
}

future<shared_ptr<cql_transport::messages::result_message>> vector_indexed_table_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_ann_query_limit) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                fmt::format("Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than {}. LIMIT was {}", max_ann_query_limit, limit)));
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);
    auto filter_json = _prepared_filter.to_json(options);
    uint64_t fetch = static_cast<uint64_t>(std::ceil(limit * secondary_index::vector_index::get_oversampling(_index.metadata().options())));
    auto pkeys = co_await qp.vector_store_client().ann(_schema->ks_name(), _index.metadata().name(), _schema,
            get_ann_ordering_vector(_prepared_ann_ordering, options), fetch, filter_json, aoe.abort_source());
    if (!pkeys.has_value()) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::ann_error_visitor{}, pkeys.error())));
    }

    if (pkeys->size() > limit && !secondary_index::vector_index::is_rescoring_enabled(_index.metadata().options())) {
        pkeys->erase(pkeys->begin() + limit, pkeys->end());
    }

    co_return co_await query_base_table(qp, state, options, pkeys.value(), timeout);
}

} // namespace statements

} // namespace cql3
