/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/statements/select_no_from_statement.hh"
#include "cql3/statements/raw/select_no_from_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/functions/aggregate_function.hh"
#include "cql3/result_set.hh"
#include "cql3/query_options.hh"
#include "cql3/column_identifier.hh"
#include "service/query_state.hh"
#include "service/client_state.hh"
#include "transport/messages/result_message.hh"
#include "audit/audit.hh"
#include "auth/permission.hh"
#include "auth/resource.hh"
#include "timeout_config.hh"
#include "exceptions/exceptions.hh"
#include <seastar/core/thread.hh>

namespace cql3 {

namespace statements {

select_no_from_statement::select_no_from_statement(
        uint32_t bound_terms,
        std::vector<expr::expression> exprs,
        std::vector<lw_shared_ptr<column_specification>> column_specs)
    : cql_statement(&timeout_config::other_timeout)
    , _bound_terms(bound_terms)
    , _exprs(std::move(exprs))
    , _column_specs(std::move(column_specs))
    , _result_metadata(::make_shared<const metadata>(_column_specs))
{}

uint32_t select_no_from_statement::get_bound_terms() const {
    return _bound_terms;
}

::shared_ptr<const metadata> select_no_from_statement::get_result_metadata() const {
    return _result_metadata;
}

future<> select_no_from_statement::check_access(query_processor&, const service::client_state& state) const {
    state.validate_login();
    std::vector<shared_ptr<db::functions::function>> used_functions;
    for (const auto& expr : _exprs) {
        expr::recurse_until(expr, [&] (const expr::expression& e) {
            if (auto fc = expr::as_if<expr::function_call>(&e)) {
                auto func = std::get<shared_ptr<db::functions::function>>(fc->func);
                if (!func->is_native()) {
                    used_functions.push_back(func);
                }
                if (auto agg = dynamic_pointer_cast<cql3::functions::aggregate_function>(func)) {
                    auto& a = agg->get_aggregate();
                    if (a.aggregation_function && !a.aggregation_function->is_native()) {
                        used_functions.push_back(a.aggregation_function);
                    }
                    if (a.state_to_result_function && !a.state_to_result_function->is_native()) {
                        used_functions.push_back(a.state_to_result_function);
                    }
                }
            }
            return false;
        });
    }
    for (const auto& func : used_functions) {
        auto encoded = auth::encode_signature(func->name().name, func->arg_types());
        co_await state.has_function_access(func->name().keyspace, encoded, auth::permission::EXECUTE);
    }
}

bool select_no_from_statement::depends_on(std::string_view, std::optional<std::string_view>) const {
    return false;
}

// Evaluate an expression that may contain aggregate function calls.
// Aggregate functions are executed over a single virtual row, following
// the PostgreSQL/MySQL convention for SELECT without FROM:
//   count(5) -> 1, sum(5) -> 5, min(5) -> 5, max(5) -> 5, etc.
static cql3::raw_value evaluate_no_from(const expr::expression& e, const query_options& options) {
    auto fc = expr::as_if<expr::function_call>(&e);
    if (!fc) {
        // Not a function call - delegate to the standard evaluator.
        return expr::evaluate(e, options);
    }

    auto* fn_ptr = std::get_if<shared_ptr<db::functions::function>>(&fc->func);
    if (!fn_ptr || !(*fn_ptr)->is_aggregate()) {
        // Scalar function call or unresolved name - delegate to the standard evaluator.
        return expr::evaluate(e, options);
    }

    // Aggregate function: run the init -> step -> finalize cycle over one virtual row.
    auto agg_fn = dynamic_pointer_cast<cql3::functions::aggregate_function>(*fn_ptr);
    auto& agg = agg_fn->get_aggregate();

    // Start with the initial accumulator state.
    bytes_opt state = agg.initial_state;

    // Evaluate the argument expressions (recursively, so nested aggregates work).
    std::vector<bytes_opt> step_args;
    step_args.reserve(1 + fc->args.size());
    step_args.push_back(state);
    for (const auto& arg : fc->args) {
        step_args.push_back(evaluate_no_from(arg, options).to_bytes_opt());
    }

    // Execute the accumulation step once (one virtual row).
    state = agg.aggregation_function->execute(step_args);

    // Finalize: convert accumulator state to result.
    if (agg.state_to_result_function) {
        std::vector<bytes_opt> final_args = {state};
        return cql3::raw_value::make_value(agg.state_to_result_function->execute(final_args));
    }
    if (state) {
        return cql3::raw_value::make_value(std::move(*state));
    }
    return cql3::raw_value::make_null();
}

// Run func in a seastar::thread if any expression contains a function
// that requires a thread context (e.g. UDFs backed by Lua/WASM).
template <typename Func>
static auto with_thread_if_needed(const std::vector<expr::expression>& exprs, Func&& func) {
    bool needs_thread = std::ranges::any_of(exprs, [](const expr::expression& e) {
        return expr::find_in_expression<expr::function_call>(e, [](const expr::function_call& fc) {
            return std::get<shared_ptr<db::functions::function>>(fc.func)->requires_thread();
        });
    });
    if (needs_thread) {
        return seastar::async(std::forward<Func>(func));
    }
    return futurize_invoke(std::forward<Func>(func));
}

future<::shared_ptr<cql_transport::messages::result_message>> select_no_from_statement::execute(
        query_processor&, service::query_state&, const query_options& options, std::optional<service::group0_guard>) const {
    co_return co_await with_thread_if_needed(_exprs, [this, &options] {
        auto rs = std::make_unique<result_set>(_column_specs);
        std::vector<bytes_opt> row;
        row.reserve(_exprs.size());
        for (const auto& expr : _exprs) {
            row.push_back(std::move(evaluate_no_from(expr, options)).to_bytes_opt());
        }
        rs->add_row(std::move(row));
        return ::make_shared<cql_transport::messages::result_message::rows>(cql3::result(std::move(rs)));
    });
}

namespace raw {

select_no_from_statement::select_no_from_statement(
        std::vector<::shared_ptr<cql3::selection::raw_selector>> select_clause)
    : _select_clause(std::move(select_clause))
{}

std::unique_ptr<prepared_statement> select_no_from_statement::prepare(data_dictionary::database db, cql_stats&) {
    if (_select_clause.empty()) {
        throw exceptions::invalid_request_exception("SELECT * is not allowed without a FROM clause");
    }

    std::vector<expr::expression> exprs;
    std::vector<lw_shared_ptr<column_specification>> specs;
    exprs.reserve(_select_clause.size());
    specs.reserve(_select_clause.size());

    for (const auto& raw : _select_clause) {
        auto prepared = expr::prepare_expression(raw->selectable_, db, _keyspace, nullptr, nullptr);

        auto type = expr::type_of(prepared);

        sstring col_name;
        if (raw->alias) {
            col_name = raw->alias->to_string();
        } else {
            col_name = fmt::format("{}", raw->selectable_);
        }
        specs.push_back(make_lw_shared<column_specification>(
                _keyspace, "", ::make_shared<column_identifier>(col_name, true), type));
        exprs.push_back(std::move(prepared));
    }

    auto bound_terms = _prepare_ctx.bound_variables_size();
    auto stmt = ::make_shared<cql3::statements::select_no_from_statement>(
            bound_terms, std::move(exprs), std::move(specs));

    return std::make_unique<prepared_statement>(audit_info(), std::move(stmt), _prepare_ctx, std::vector<uint16_t>{});
}

audit::statement_category select_no_from_statement::category() const {
    return audit::statement_category::QUERY;
}

audit::audit_info_ptr select_no_from_statement::audit_info() const {
    return audit::audit::create_audit_info(category(), "", "");
}

} // namespace raw

} // namespace statements

} // namespace cql3
