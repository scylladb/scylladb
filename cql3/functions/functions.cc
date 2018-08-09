/*
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "functions.hh"

#include "function_call.hh"
#include "token_fct.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"
#include "cql3/lists.hh"
#include "cql3/constants.hh"

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
    declare(time_uuid_fcts::make_unix_timestamp_of_fct());
    declare(time_uuid_fcts::make_currenttimestamp_fct());
    declare(time_uuid_fcts::make_currentdate_fct());
    declare(time_uuid_fcts::make_currenttime_fct());
    declare(time_uuid_fcts::make_currenttimeuuid_fct());
    declare(time_uuid_fcts::make_timeuuidtodate_fct());
    declare(time_uuid_fcts::make_timestamptodate_fct());
    declare(time_uuid_fcts::make_timeuuidtotimestamp_fct());
    declare(time_uuid_fcts::make_datetotimestamp_fct());
    declare(time_uuid_fcts::make_timeuuidtounixtimestamp_fct());
    declare(time_uuid_fcts::make_timestamptounixtimestamp_fct());
    declare(time_uuid_fcts::make_datetounixtimestamp_fct());
    declare(make_uuid_fct());

    for (auto&& type : cql3_type::values()) {
        // Note: because text and varchar ends up being synonymous, our automatic makeToBlobFunction doesn't work
        // for varchar, so we special case it below. We also skip blob for obvious reasons.
        if (type == cql3_type::varchar || type == cql3_type::blob) {
            continue;
        }
        // counters are not supported yet
        if (type->is_counter()) {
            warn(unimplemented::cause::COUNTERS);
            continue;
        }

        declare(make_to_blob_function(type->get_type()));
        declare(make_from_blob_function(type->get_type()));
    }
    declare(aggregate_fcts::make_count_function<int8_t>());
    declare(aggregate_fcts::make_max_function<int8_t>());
    declare(aggregate_fcts::make_min_function<int8_t>());

    declare(aggregate_fcts::make_count_function<int16_t>());
    declare(aggregate_fcts::make_max_function<int16_t>());
    declare(aggregate_fcts::make_min_function<int16_t>());

    declare(aggregate_fcts::make_count_function<int32_t>());
    declare(aggregate_fcts::make_max_function<int32_t>());
    declare(aggregate_fcts::make_min_function<int32_t>());

    declare(aggregate_fcts::make_count_function<int64_t>());
    declare(aggregate_fcts::make_max_function<int64_t>());
    declare(aggregate_fcts::make_min_function<int64_t>());

    declare(aggregate_fcts::make_count_function<boost::multiprecision::cpp_int>());
    declare(aggregate_fcts::make_max_function<boost::multiprecision::cpp_int>());
    declare(aggregate_fcts::make_min_function<boost::multiprecision::cpp_int>());

    declare(aggregate_fcts::make_count_function<big_decimal>());
    declare(aggregate_fcts::make_max_function<big_decimal>());
    declare(aggregate_fcts::make_min_function<big_decimal>());

    declare(aggregate_fcts::make_count_function<float>());
    declare(aggregate_fcts::make_max_function<float>());
    declare(aggregate_fcts::make_min_function<float>());

    declare(aggregate_fcts::make_count_function<double>());
    declare(aggregate_fcts::make_max_function<double>());
    declare(aggregate_fcts::make_min_function<double>());

    declare(aggregate_fcts::make_count_function<sstring>());
    declare(aggregate_fcts::make_max_function<sstring>());
    declare(aggregate_fcts::make_min_function<sstring>());

    declare(aggregate_fcts::make_count_function<simple_date_native_type>());
    declare(aggregate_fcts::make_max_function<simple_date_native_type>());
    declare(aggregate_fcts::make_min_function<simple_date_native_type>());

    declare(aggregate_fcts::make_count_function<timestamp_native_type>());
    declare(aggregate_fcts::make_max_function<timestamp_native_type>());
    declare(aggregate_fcts::make_min_function<timestamp_native_type>());

    declare(aggregate_fcts::make_count_function<timeuuid_native_type>());
    declare(aggregate_fcts::make_max_function<timeuuid_native_type>());
    declare(aggregate_fcts::make_min_function<timeuuid_native_type>());

    declare(aggregate_fcts::make_count_function<utils::UUID>());
    declare(aggregate_fcts::make_max_function<utils::UUID>());
    declare(aggregate_fcts::make_min_function<utils::UUID>());

    //FIXME:
    //declare(aggregate_fcts::make_count_function<bytes>());
    //declare(aggregate_fcts::make_max_function<bytes>());
    //declare(aggregate_fcts::make_min_function<bytes>());

    // FIXME: more count/min/max

    declare(make_varchar_as_blob_fct());
    declare(make_blob_as_varchar_fct());
    declare(aggregate_fcts::make_sum_function<int8_t>());
    declare(aggregate_fcts::make_sum_function<int16_t>());
    declare(aggregate_fcts::make_sum_function<int32_t>());
    declare(aggregate_fcts::make_sum_function<int64_t>());
    declare(aggregate_fcts::make_sum_function<float>());
    declare(aggregate_fcts::make_sum_function<double>());
    declare(aggregate_fcts::make_sum_function<boost::multiprecision::cpp_int>());
    declare(aggregate_fcts::make_sum_function<big_decimal>());
    declare(aggregate_fcts::make_avg_function<int8_t>());
    declare(aggregate_fcts::make_avg_function<int16_t>());
    declare(aggregate_fcts::make_avg_function<int32_t>());
    declare(aggregate_fcts::make_avg_function<int64_t>());
    declare(aggregate_fcts::make_avg_function<float>());
    declare(aggregate_fcts::make_avg_function<double>());
    declare(aggregate_fcts::make_avg_function<boost::multiprecision::cpp_int>());
    declare(aggregate_fcts::make_avg_function<big_decimal>());

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

inline
shared_ptr<function>
make_to_json_function(data_type t) {
    return make_native_scalar_function<true>("tojson", utf8_type, {t},
            [t](cql_serialization_format sf, const std::vector<bytes_opt>& parameters) -> bytes_opt {
        return utf8_type->decompose(t->to_json_string(parameters[0]));
    });
}

inline
shared_ptr<function>
make_from_json_function(database& db, const sstring& keyspace, data_type t) {
    return make_native_scalar_function<true>("fromjson", t, {utf8_type},
            [&db, &keyspace, t](cql_serialization_format sf, const std::vector<bytes_opt>& parameters) -> bytes_opt {
        Json::Value json_value = json::to_json_value(utf8_type->to_string(parameters[0].value()));
        bytes_opt parsed_json_value;
        if (!json_value.isNull()) {
            parsed_json_value.emplace(t->from_json_object(json_value, sf));
        }
        return std::move(parsed_json_value);
    });
}

shared_ptr<function>
functions::get(database& db,
        const sstring& keyspace,
        const function_name& name,
        const std::vector<shared_ptr<assignment_testable>>& provided_args,
        const sstring& receiver_ks,
        const sstring& receiver_cf,
        shared_ptr<column_specification> receiver) {

    static const function_name TOKEN_FUNCTION_NAME = function_name::native_function("token");
    static const function_name TO_JSON_FUNCTION_NAME = function_name::native_function("tojson");
    static const function_name FROM_JSON_FUNCTION_NAME = function_name::native_function("fromjson");

    if (name.has_keyspace()
                ? name == TOKEN_FUNCTION_NAME
                : name.name == TOKEN_FUNCTION_NAME.name) {
        return ::make_shared<token_fct>(db.find_schema(receiver_ks, receiver_cf));
    }

    if (name.has_keyspace()
                ? name == TO_JSON_FUNCTION_NAME
                : name.name == TO_JSON_FUNCTION_NAME.name) {
        if (provided_args.size() != 1) {
            throw exceptions::invalid_request_exception("toJson() accepts 1 argument only");
        }
        selection::selector *sp = dynamic_cast<selection::selector *>(provided_args[0].get());
        if (!sp) {
            throw exceptions::invalid_request_exception("toJson() is only valid in SELECT clause");
        }
        return make_to_json_function(sp->get_type());
    }

    if (name.has_keyspace()
                ? name == FROM_JSON_FUNCTION_NAME
                : name.name == FROM_JSON_FUNCTION_NAME.name) {
        if (provided_args.size() != 1) {
            throw exceptions::invalid_request_exception("fromJson() accepts 1 argument only");
        }
        if (!receiver) {
            throw exceptions::invalid_request_exception("fromJson() can only be called if receiver type is known");
        }
        return make_from_json_function(db, keyspace, receiver->type);
    }

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
        validate_types(db, keyspace, fun, provided_args, receiver_ks, receiver_cf);
        return fun;
    }

    std::vector<shared_ptr<function>> compatibles;
    for (auto&& to_test : candidates) {
        auto r = match_arguments(db, keyspace, to_test, provided_args, receiver_ks, receiver_cf);
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
functions::validate_types(database& db,
                          const sstring& keyspace,
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
        if (!is_assignable(provided->test_assignment(db, keyspace, expected))) {
            throw exceptions::invalid_request_exception(
                    sprint("Type error: %s cannot be passed as argument %d of function %s of type %s",
                            provided, i, fun->name(), expected->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result
functions::match_arguments(database& db, const sstring& keyspace,
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
        auto arg_res = provided->test_assignment(db, keyspace, expected);
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

bool
function_call::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return _fun->uses_function(ks_name, function_name);
}

void
function_call::collect_marker_specification(shared_ptr<variable_specifications> bound_names) {
    for (auto&& t : _terms) {
        t->collect_marker_specification(bound_names);
    }
}

shared_ptr<terminal>
function_call::bind(const query_options& options) {
    return make_terminal(_fun, cql3::raw_value::make_value(bind_and_get(options)), options.get_cql_serialization_format());
}

cql3::raw_value_view
function_call::bind_and_get(const query_options& options) {
    std::vector<bytes_opt> buffers;
    buffers.reserve(_terms.size());
    for (auto&& t : _terms) {
        // For now, we don't allow nulls as argument as no existing function needs it and it
        // simplify things.
        auto val = t->bind_and_get(options);
        if (!val) {
            throw exceptions::invalid_request_exception(sprint("Invalid null value for argument to %s", *_fun));
        }
        buffers.push_back(std::move(to_bytes_opt(val)));
    }
    auto result = execute_internal(options.get_cql_serialization_format(), *_fun, std::move(buffers));
    return options.make_temporary(cql3::raw_value::make_value(result));
}

bytes_opt
function_call::execute_internal(cql_serialization_format sf, scalar_function& fun, std::vector<bytes_opt> params) {
    bytes_opt result = fun.execute(sf, params);
    try {
        // Check the method didn't lied on it's declared return type
        if (result) {
            fun.return_type()->validate(*result);
        }
        return result;
    } catch (marshal_exception& e) {
        throw runtime_exception(sprint("Return of function %s (%s) is not a valid value for its declared return type %s",
                                       fun, to_hex(result),
                                       *fun.return_type()->as_cql3_type()
                                       ));
    }
}

bool
function_call::contains_bind_marker() const {
    for (auto&& t : _terms) {
        if (t->contains_bind_marker()) {
            return true;
        }
    }
    return false;
}

shared_ptr<terminal>
function_call::make_terminal(shared_ptr<function> fun, cql3::raw_value result, cql_serialization_format sf)  {
    if (!dynamic_pointer_cast<const collection_type_impl>(fun->return_type())) {
        return ::make_shared<constants::value>(std::move(result));
    }

    auto ctype = static_pointer_cast<const collection_type_impl>(fun->return_type());
    fragmented_temporary_buffer::view res;
    if (result) {
        res = fragmented_temporary_buffer::view(bytes_view(*result));
    }
    if (&ctype->_kind == &collection_type_impl::kind::list) {
        return make_shared(lists::value::from_serialized(std::move(res), static_pointer_cast<const list_type_impl>(ctype), sf));
    } else if (&ctype->_kind == &collection_type_impl::kind::set) {
        return make_shared(sets::value::from_serialized(std::move(res), static_pointer_cast<const set_type_impl>(ctype), sf));
    } else if (&ctype->_kind == &collection_type_impl::kind::map) {
        return make_shared(maps::value::from_serialized(std::move(res), static_pointer_cast<const map_type_impl>(ctype), sf));
    }
    abort();
}

::shared_ptr<term>
function_call::raw::prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) {
    std::vector<shared_ptr<assignment_testable>> args;
    args.reserve(_terms.size());
    std::transform(_terms.begin(), _terms.end(), std::back_inserter(args),
            [] (auto&& x) -> shared_ptr<assignment_testable> {
        return x;
    });
    auto&& fun = functions::functions::get(db, keyspace, _name, args, receiver->ks_name, receiver->cf_name, receiver);
    if (!fun) {
        throw exceptions::invalid_request_exception(sprint("Unknown function %s called", _name));
    }
    if (fun->is_aggregate()) {
        throw exceptions::invalid_request_exception("Aggregation function are not supported in the where clause");
    }

    // Can't use static_pointer_cast<> because function is a virtual base class of scalar_function
    auto&& scalar_fun = dynamic_pointer_cast<scalar_function>(fun);

    // Functions.get() will complain if no function "name" type check with the provided arguments.
    // We still have to validate that the return type matches however
    if (!receiver->type->is_value_compatible_with(*scalar_fun->return_type())) {
        throw exceptions::invalid_request_exception(sprint("Type error: cannot assign result of function %s (type %s) to %s (type %s)",
                                                    fun->name(), fun->return_type()->as_cql3_type(),
                                                    receiver->name, receiver->type->as_cql3_type()));
    }

    if (scalar_fun->arg_types().size() != _terms.size()) {
        throw exceptions::invalid_request_exception(sprint("Incorrect number of arguments specified for function %s (expected %d, found %d)",
                                                    fun->name(), fun->arg_types().size(), _terms.size()));
    }

    std::vector<shared_ptr<term>> parameters;
    parameters.reserve(_terms.size());
    bool all_terminal = true;
    for (size_t i = 0; i < _terms.size(); ++i) {
        auto&& t = _terms[i]->prepare(db, keyspace, functions::make_arg_spec(receiver->ks_name, receiver->cf_name, *scalar_fun, i));
        if (dynamic_cast<non_terminal*>(t.get())) {
            all_terminal = false;
        }
        parameters.push_back(t);
    }

    // If all parameters are terminal and the function is pure, we can
    // evaluate it now, otherwise we'd have to wait execution time
    if (all_terminal && scalar_fun->is_pure()) {
        return make_terminal(scalar_fun, cql3::raw_value::make_value(execute(*scalar_fun, parameters)), query_options::DEFAULT.get_cql_serialization_format());
    } else {
        return ::make_shared<function_call>(scalar_fun, parameters);
    }
}

bytes_opt
function_call::raw::execute(scalar_function& fun, std::vector<shared_ptr<term>> parameters) {
    std::vector<bytes_opt> buffers;
    buffers.reserve(parameters.size());
    for (auto&& t : parameters) {
        assert(dynamic_cast<terminal*>(t.get()));
        auto&& param = static_cast<terminal*>(t.get())->get(query_options::DEFAULT);
        buffers.push_back(std::move(to_bytes_opt(param)));
    }

    return execute_internal(cql_serialization_format::internal(), fun, buffers);
}

assignment_testable::test_result
function_call::raw::test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
    // Note: Functions.get() will return null if the function doesn't exist, or throw is no function matching
    // the arguments can be found. We may get one of those if an undefined/wrong function is used as argument
    // of another, existing, function. In that case, we return true here because we'll throw a proper exception
    // later with a more helpful error message that if we were to return false here.
    try {
        auto&& fun = functions::get(db, keyspace, _name, _terms, receiver->ks_name, receiver->cf_name, receiver);
        if (fun && receiver->type->equals(fun->return_type())) {
            return assignment_testable::test_result::EXACT_MATCH;
        } else if (!fun || receiver->type->is_value_compatible_with(*fun->return_type())) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        } else {
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        }
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
}

sstring
function_call::raw::to_string() const {
    return sprint("%s(%s)", _name, join(", ", _terms));
}


}
}


