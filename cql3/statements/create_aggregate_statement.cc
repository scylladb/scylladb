/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/create_aggregate_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_aggregate.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "data_dictionary/data_dictionary.hh"
#include "mutation/mutation.hh"
#include "cql3/query_processor.hh"
#include "gms/feature_service.hh"
#include <boost/range/adaptor/transformed.hpp>


namespace cql3 {

namespace statements {

seastar::future<shared_ptr<db::functions::function>> create_aggregate_statement::create(query_processor& qp, db::functions::function* old) const {
    if (!qp.proxy().features().user_defined_aggregates) {
        throw exceptions::invalid_request_exception("Cluster does not support user-defined aggregates, upgrade the whole cluster in order to use UDA");
    }
    if (old && !dynamic_cast<functions::user_aggregate*>(old)) {
        throw exceptions::invalid_request_exception(format("Cannot replace '{}' which is not a user defined aggregate", *old));
    }
    data_type state_type = prepare_type(qp, *_stype);

    auto&& db = qp.db();
    std::vector<data_type> acc_types{state_type};
    acc_types.insert(acc_types.end(), _arg_types.begin(), _arg_types.end());
    auto state_func = dynamic_pointer_cast<functions::scalar_function>(functions::instance().find(functions::function_name{_name.keyspace, _sfunc}, acc_types));
    if (!state_func) {
        auto acc_type_names = acc_types | boost::adaptors::transformed([] (auto&& t) { return t->cql3_type_name(); });
        throw exceptions::invalid_request_exception(format("State function {}({}) not found", _sfunc, fmt::join(acc_type_names, ", ")));
    }
    if (state_func->return_type() != state_type) {
        throw exceptions::invalid_request_exception(format("State function '{}' doesn't return state ({})", _sfunc, state_type->cql3_type_name()));
    }

    ::shared_ptr<cql3::functions::scalar_function> reduce_func = nullptr;
    if (_rfunc) {
        if (!qp.proxy().features().uda_native_parallelized_aggregation) {
            throw exceptions::invalid_request_exception("Cluster does not support reduction function for user-defined aggregates, upgrade the whole cluster in order to define REDUCEFUNC for UDA");
        }

        reduce_func = dynamic_pointer_cast<functions::scalar_function>(functions::instance().find(functions::function_name{_name.keyspace, _rfunc.value()}, {state_type, state_type}));
        if (!reduce_func) {
            throw exceptions::invalid_request_exception(format("Scalar reduce function {} for state type {} not found.", _rfunc.value(), state_type->name()));
        }
    }
    ::shared_ptr<cql3::functions::scalar_function> final_func = nullptr;
    if (_ffunc) {
        final_func = dynamic_pointer_cast<functions::scalar_function>(functions::instance().find(functions::function_name{_name.keyspace, _ffunc.value()}, {state_type}));
        if (!final_func) {
            throw exceptions::invalid_request_exception(format("Final function {}({}) not found", _ffunc.value(), state_type->cql3_type_name()));
        }
    }

    bytes_opt initcond = std::nullopt;
    if (_ival) {
        auto dummy_ident = ::make_shared<column_identifier>("", true);
        auto column_spec = make_lw_shared<column_specification>("", "", dummy_ident, state_type);
        auto initcond_expr = prepare_expression(_ival.value(), db, _name.keyspace, nullptr, {column_spec});
        expr::verify_no_aggregate_functions(initcond_expr, "INITCOND clause");
        auto initcond_term = expr::evaluate(initcond_expr, query_options::DEFAULT);
        initcond = std::move(initcond_term).to_bytes_opt();
    }

    co_return ::make_shared<functions::user_aggregate>(_name, initcond, std::move(state_func), std::move(reduce_func), std::move(final_func));
}

std::unique_ptr<prepared_statement> create_aggregate_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_aggregate_statement>(*this));
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
create_aggregate_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    auto aggregate = dynamic_pointer_cast<functions::user_aggregate>(co_await validate_while_executing(qp));
    if (aggregate) {
        m = co_await service::prepare_new_aggregate_announcement(qp.proxy(), aggregate, ts);
        ret = create_schema_change(*aggregate, true);
    }

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

seastar::future<> create_aggregate_statement::check_access(query_processor &qp, const service::client_state &state) const {
    co_await create_function_statement_base::check_access(qp, state);
    auto&& ks = _name.has_keyspace() ? _name.keyspace : state.get_keyspace();
    create_arg_types(qp);
    std::vector<data_type> sfunc_args = _arg_types;
    data_type stype = prepare_type(qp, *_stype);
    sfunc_args.insert(sfunc_args.begin(), stype);
    co_await state.has_function_access(ks, auth::encode_signature(_sfunc,sfunc_args), auth::permission::EXECUTE);
    if (_rfunc) {
        co_await state.has_function_access(ks, auth::encode_signature(*_rfunc,{stype, stype}), auth::permission::EXECUTE);
    }
    if (_ffunc) {
        co_await state.has_function_access(ks, auth::encode_signature(*_ffunc,{stype}), auth::permission::EXECUTE);
    }
}

create_aggregate_statement::create_aggregate_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            sstring sfunc, shared_ptr<cql3_type::raw> stype, std::optional<sstring> rfunc, std::optional<sstring> ffunc, std::optional<expr::expression> ival, bool or_replace, bool if_not_exists)
        : create_function_statement_base(std::move(name), std::move(arg_types), or_replace, if_not_exists)
        , _sfunc(std::move(sfunc))
        , _stype(std::move(stype))
        , _rfunc(std::move(rfunc))
        , _ffunc(std::move(ffunc))
        , _ival(std::move(ival))
    {}
}
}
