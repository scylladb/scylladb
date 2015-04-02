/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "functions.hh"

namespace cql3 {
namespace functions {

thread_local std::unordered_multimap<function_name, shared_ptr<function>> functions::_declared = init();

std::unordered_multimap<function_name, shared_ptr<function>>
functions::init() {
    std::unordered_multimap<function_name, shared_ptr<function>> ret;
    auto declare = [&ret] (shared_ptr<function> f) { ret.emplace(f->name(), f); };
    declare(aggregate_fcts::make_count_rows_function());
    declare(time_uuid_fcts::make_now_fct());
    declare(time_uuid_fcts::make_min_timeuuid_fct());
    declare(time_uuid_fcts::make_max_timeuuid_fct());
    declare(time_uuid_fcts::make_date_of_fct());
    declare(time_uuid_fcts::make_unix_timestamp_of_fcf());
    declare(make_uuid_fct());

    for (auto&& type : cql3_type::values()) {
        // Note: because text and varchar ends up being synonimous, our automatic makeToBlobFunction doesn't work
        // for varchar, so we special case it below. We also skip blob for obvious reasons.
        if (type == cql3_type::varchar || type == cql3_type::blob) {
            continue;
        }

        declare(make_to_blob_function(type->get_type()));
        declare(make_from_blob_function(type->get_type()));
    }
    declare(aggregate_fcts::make_count_function<int32_t>());
    declare(aggregate_fcts::make_max_function<int32_t>());
    declare(aggregate_fcts::make_min_function<int32_t>());

    declare(aggregate_fcts::make_count_function<int64_t>());
    declare(aggregate_fcts::make_max_function<int64_t>());
    declare(aggregate_fcts::make_min_function<int64_t>());

    //FIXME:
    //declare(aggregate_fcts::make_count_function<bytes>());
    //declare(aggregate_fcts::make_max_function<bytes>());
    //declare(aggregate_fcts::make_min_function<bytes>());

    // FIXME: more count/min/max

    declare(make_varchar_as_blob_fct());
    declare(make_blob_as_varchar_fct());
    declare(aggregate_fcts::make_sum_function<int32_t>());
    declare(aggregate_fcts::make_sum_function<int64_t>());
    declare(aggregate_fcts::make_avg_function<int32_t>());
    declare(aggregate_fcts::make_avg_function<int64_t>());
#if 0
    declare(AggregateFcts.sumFunctionForFloat);
    declare(AggregateFcts.sumFunctionForDouble);
    declare(AggregateFcts.sumFunctionForDecimal);
    declare(AggregateFcts.sumFunctionForVarint);
    declare(AggregateFcts.avgFunctionForFloat);
    declare(AggregateFcts.avgFunctionForDouble);
    declare(AggregateFcts.avgFunctionForVarint);
    declare(AggregateFcts.avgFunctionForDecimal);
#endif

    // also needed for smp:
#if 0
    MigrationManager.instance.register(new FunctionsMigrationListener());
#endif
    return ret;
}

shared_ptr<column_specification>
functions::make_arg_spec(const sstring& receiver_ks, const sstring& receiver_cf,
        const function& fun, size_t i) {
    auto&& name = boost::lexical_cast<std::string>(fun.name());
    std::transform(name.begin(), name.end(), name.begin(), ::tolower);
    return ::make_shared<column_specification>(receiver_ks,
                                   receiver_cf,
                                   ::make_shared<column_identifier>(sprint("arg%d(%s)", i, name), true),
                                   fun.arg_types()[i]);
}

int
functions::get_overload_count(const function_name& name) {
    return _declared.count(name);
}

shared_ptr<function>
functions::get(const sstring& keyspace,
        const function_name& name,
        const std::vector<shared_ptr<assignment_testable>>& provided_args,
        const sstring& receiver_ks,
        const sstring& receiver_cf) {
    // FIXME:
#if 0
    // later
    if (name.has_keyspace()
        ? name.equals(TOKEN_FUNCTION_NAME)
        : name.name.equals(TOKEN_FUNCTION_NAME.name))
        return new TokenFct(Schema.instance.getCFMetaData(receiverKs, receiverCf));
#endif
    std::vector<shared_ptr<function>> candidates;
    auto&& add_declared = [&] (function_name fn) {
        auto&& fns = _declared.equal_range(fn);
        for (auto i = fns.first; i != fns.second; ++i) {
            candidates.push_back(i->second);
        }
    };
    if (!name.has_keyspace()) {
        // add 'SYSTEM' (native) candidates
        add_declared(name.as_native_function());
        add_declared(function_name(keyspace, name.name));
    } else {
        // function name is fully qualified (keyspace + name)
        add_declared(name);
    }

    if (candidates.empty()) {
        return {};
    }

    // Fast path if there is only one choice
    if (candidates.size() == 1) {
        auto fun = std::move(candidates[0]);
        validate_types(keyspace, fun, provided_args, receiver_ks, receiver_cf);
        return fun;
    }

    std::vector<shared_ptr<function>> compatibles;
    for (auto&& to_test : candidates) {
        auto r = match_arguments(keyspace, to_test, provided_args, receiver_ks, receiver_cf);
        switch (r) {
            case assignment_testable::test_result::EXACT_MATCH:
                // We always favor exact matches
                return to_test;
            case assignment_testable::test_result::WEAKLY_ASSIGNABLE:
                compatibles.push_back(std::move(to_test));
                break;
            default:
                ;
        };
    }

    if (compatibles.empty()) {
        throw exceptions::invalid_request_exception(
                sprint("Invalid call to function %s, none of its type signatures match (known type signatures: %s)",
                                                        name, join(", ", candidates)));
    }

    if (compatibles.size() > 1) {
        throw exceptions::invalid_request_exception(
                sprint("Ambiguous call to function %s (can be matched by following signatures: %s): use type casts to disambiguate",
                    name, join(", ", compatibles)));
    }

    return std::move(compatibles[0]);
}

std::vector<shared_ptr<function>>
functions::find(const function_name& name) {
    auto range = _declared.equal_range(name);
    std::vector<shared_ptr<function>> ret;
    for (auto i = range.first; i != range.second; ++i) {
        ret.push_back(i->second);
    }
    return ret;
}

shared_ptr<function>
functions::find(const function_name& name, const std::vector<data_type>& arg_types) {
    assert(name.has_keyspace()); // : "function name not fully qualified";
    for (auto&& f : find(name)) {
        if (type_equals(f->arg_types(), arg_types)) {
            return f;
        }
    }
    return {};
}

// This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
// case where there is no override for a given function. This is thus probably worth the minor code duplication.
void
functions::validate_types(const sstring& keyspace,
                          shared_ptr<function> fun,
                          const std::vector<shared_ptr<assignment_testable>>& provided_args,
                          const sstring& receiver_ks,
                          const sstring& receiver_cf) {
    if (provided_args.size() != fun->arg_types().size()) {
        throw exceptions::invalid_request_exception(
                sprint("Invalid number of arguments in call to function %s: %d required but %d provided",
                        fun->name(), fun->arg_types().size(), provided_args.size()));
    }

    for (size_t i = 0; i < provided_args.size(); ++i) {
        auto&& provided = provided_args[i];

        // If the concrete argument is a bind variables, it can have any type.
        // We'll validate the actually provided value at execution time.
        if (!provided) {
            continue;
        }

        auto&& expected = make_arg_spec(receiver_ks, receiver_cf, *fun, i);
        if (!is_assignable(provided->test_assignment(keyspace, expected))) {
            throw exceptions::invalid_request_exception(
                    sprint("Type error: %s cannot be passed as argument %d of function %s of type %s",
                            provided, i, fun->name(), expected->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result
functions::match_arguments(const sstring& keyspace,
        shared_ptr<function> fun,
        const std::vector<shared_ptr<assignment_testable>>& provided_args,
        const sstring& receiver_ks,
        const sstring& receiver_cf) {
    if (provided_args.size() != fun->arg_types().size()) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }

    // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
    auto res = assignment_testable::test_result::EXACT_MATCH;
    for (size_t i = 0; i < provided_args.size(); ++i) {
        auto&& provided = provided_args[i];
        if (!provided) {
            res = assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            continue;
        }
        auto&& expected = make_arg_spec(receiver_ks, receiver_cf, *fun, i);
        auto arg_res = provided->test_assignment(keyspace, expected);
        if (arg_res == assignment_testable::test_result::NOT_ASSIGNABLE) {
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        }
        if (arg_res == assignment_testable::test_result::WEAKLY_ASSIGNABLE) {
            res = assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        }
    }
    return res;
}

bool
functions::type_equals(const std::vector<data_type>& t1, const std::vector<data_type>& t2) {
#if 0
    if (t1.size() != t2.size())
        return false;
    for (int i = 0; i < t1.size(); i ++)
        if (!typeEquals(t1.get(i), t2.get(i)))
            return false;
    return true;
#endif
    abort();
}

}
}


