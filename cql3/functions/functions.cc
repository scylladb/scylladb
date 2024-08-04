/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "functions.hh"
#include "token_fct.hh"
#include "cql3/ut_name.hh"
#include "cql3/type_json.hh"
#include "cql3/functions/aggregate_fcts.hh"
#include "cql3/functions/bytes_conversion_fcts.hh"
#include "cql3/functions/time_uuid_fcts.hh"
#include "cql3/functions/user_function.hh"
#include "cql3/functions/user_aggregate.hh"
#include "cql3/functions/uuid_fcts.hh"
#include "data_dictionary/data_dictionary.hh"
#include "as_json_function.hh"
#include "cql3/prepare_context.hh"
#include "user_aggregate.hh"
#include "cql3/expr/expression.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptors.hpp>

#include "error_injection_fcts.hh"

auto fmt::formatter<std::vector<data_type>>::format(const std::vector<data_type>& arg_types, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    for (size_t i = 0; i < arg_types.size(); ++i) {
        if (i > 0) {
            out = fmt::format_to(out, ", ");
        }
        out = fmt::format_to(out, "{}", arg_types[i]->as_cql3_type());
    }
    return out;
}

namespace cql3 {
namespace functions {

logging::logger log("cql3_fuctions");

bool abstract_function::requires_thread() const { return false; }

bool as_json_function::requires_thread() const { return false; }

static bool same_signature(const shared_ptr<function>& f1, const shared_ptr<function>& f2) {
    if (f1 == nullptr || f2 == nullptr) {
        return false;
    }
    return f1->name() == f2->name() && f1->arg_types() == f2->arg_types();
}

std::unordered_multimap<function_name, shared_ptr<function>>
functions::init() noexcept {
    // It is possible that this function will fail with a
    // std::bad_alloc causing std::unexpected to be called. Since
    // this is used during initialization, we would have to abort
    // somehow. We could add a try/catch to print a better error
    // message before aborting, but that would produce a core file
    // that has less information in it. Given how unlikely it is that
    // we will run out of memory this early, having a better core dump
    // if we do seems like a good trade-off.
    memory::scoped_critical_alloc_section dfg;

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
        if (type == cql3_type::blob) {
            continue;
        }

        declare(make_to_blob_function(type.get_type()));
        declare(make_from_blob_function(type.get_type()));
    }

    declare(make_varchar_as_blob_fct());
    declare(make_blob_as_varchar_fct());
    add_agg_functions(ret);

    declare(error_injection::make_enable_injection_function());
    declare(error_injection::make_disable_injection_function());
    declare(error_injection::make_enabled_injections_function());

    // also needed for smp:
#if 0
    MigrationManager.instance.register(new FunctionsMigrationListener());
#endif
    return ret;
}

void functions::add_function(shared_ptr<function> func) {
    if (find(func->name(), func->arg_types())) {
        throw std::logic_error(format("duplicated function {}", func));
    }
    _declared.emplace(func->name(), func);
}

void functions::with_udf_iter(const function_name& name, const std::vector<data_type>& arg_types, std::function<void(declared_t::iterator)> f) {
    auto cit = find_iter(name, arg_types);
    if (cit == _declared.end() || cit->second->is_native()) {
        log.error("attempted to remove or alter non existent user defined function {}({})", name, arg_types);
        return;
    }
    // erase here is only to convert from const_iterator to iterator
    auto it = _declared.erase(cit, cit);
    f(it);
}

void functions::replace_function(shared_ptr<function> func) {
    with_udf_iter(func->name(), func->arg_types(), [func] (declared_t::iterator it) {
        it->second = std::move(func);
    });
    auto scalar_func = dynamic_pointer_cast<scalar_function>(func);
    if (!scalar_func) {
        return;
    }
    for (auto& fit : _declared) {
        auto aggregate = dynamic_pointer_cast<user_aggregate>(fit.second);
        if (aggregate && (same_signature(aggregate->sfunc(), scalar_func)
            || (same_signature(aggregate->finalfunc(), scalar_func))
            || (same_signature(aggregate->reducefunc(), scalar_func))))
        {
            // we need to replace at least one underlying function
            shared_ptr<scalar_function> sfunc = same_signature(aggregate->sfunc(), scalar_func) ? scalar_func : aggregate->sfunc();
            shared_ptr<scalar_function> finalfunc = same_signature(aggregate->finalfunc(), scalar_func) ? scalar_func : aggregate->finalfunc();
            shared_ptr<scalar_function> reducefunc = same_signature(aggregate->reducefunc(), scalar_func) ? scalar_func : aggregate->reducefunc();
            fit.second = ::make_shared<user_aggregate>(aggregate->name(), aggregate->initcond(), sfunc, reducefunc, finalfunc);
        }
    }
}

void functions::remove_function(const function_name& name, const std::vector<data_type>& arg_types) {
    with_udf_iter(name, arg_types, [this] (declared_t::iterator it) { _declared.erase(it); });
}

std::optional<function_name> functions::used_by_user_aggregate(shared_ptr<user_function> func) const {
    for (const shared_ptr<function>& fptr : _declared | boost::adaptors::map_values) {
        auto aggregate = dynamic_pointer_cast<user_aggregate>(fptr);
        if (aggregate && (same_signature(aggregate->sfunc(), func)
            || (same_signature(aggregate->finalfunc(), func))
            || (same_signature(aggregate->reducefunc(), func))))
        {
            return aggregate->name();
        }
    }
    return {};
}

std::optional<function_name> functions::used_by_user_function(const ut_name& user_type) const {
    for (const shared_ptr<function>& fptr : _declared | boost::adaptors::map_values) {
        for (auto& arg_type : fptr->arg_types()) {
            if (arg_type->references_user_type(user_type.get_keyspace(), user_type.get_user_type_name())) {
                return fptr->name();
            }
        }
        if (fptr->return_type()->references_user_type(user_type.get_keyspace(), user_type.get_user_type_name())) {
            return fptr->name();
        }
    }
    return {};
}

lw_shared_ptr<column_specification>
functions::make_arg_spec(const sstring& receiver_ks, std::optional<const std::string_view> receiver_cf_opt,
        const function& fun, size_t i) const {
    auto&& name = fmt::to_string(fun.name());
    const std::string_view receiver_cf = receiver_cf_opt.has_value() ? *receiver_cf_opt : "<unknown_col_family>";
    std::transform(name.begin(), name.end(), name.begin(), ::tolower);
    return make_lw_shared<column_specification>(receiver_ks,
                                   receiver_cf,
                                   ::make_shared<column_identifier>(format("arg{:d}({})", i, name), true),
                                   fun.arg_types()[i]);
}

inline
shared_ptr<function>
make_to_json_function(data_type t) {
    return make_native_scalar_function<true>("tojson", utf8_type, {t},
            [t](std::span<const bytes_opt> parameters) -> bytes_opt {
        return utf8_type->decompose(to_json_string(*t, parameters[0]));
    });
}

inline
shared_ptr<function>
make_from_json_function(data_dictionary::database db, const sstring& keyspace, data_type t) {
    return make_native_scalar_function<true>("fromjson", t, {utf8_type},
            [keyspace, t](std::span<const bytes_opt> parameters) -> bytes_opt {
        try {
            rjson::value json_value = rjson::parse(utf8_type->to_string(parameters[0].value_or("null")));
            bytes_opt parsed_json_value;
            if (!json_value.IsNull()) {
                parsed_json_value.emplace(from_json_object(*t, json_value));
            }
            return parsed_json_value;
        } catch(rjson::error& e) {
            throw exceptions::function_execution_exception("fromJson",
                format("Failed parsing fromJson parameter: {}", e.what()), keyspace, {t->name()});
        }
    });
}

static shared_ptr<function> get_dynamic_aggregate(const function_name &name, const std::variant<std::vector<data_type>, std::vector<shared_ptr<assignment_testable>>>& provided_args) {
    static const function_name MIN_NAME = function_name::native_function("min");
    static const function_name MAX_NAME = function_name::native_function("max");
    static const function_name COUNT_NAME = function_name::native_function("count");
    static const function_name COUNT_ROWS_NAME = function_name::native_function("countRows");

    auto get_arguments = [&] (const sstring& function_name) {
        return std::visit(overloaded_functor {
            [&] (const std::vector<data_type>& args) {
                return args;
            },
            [&] (const std::vector<shared_ptr<assignment_testable>>& args) {
                std::vector<data_type> arg_types;
                for (const auto& arg : args) {
                    auto arg_type_opt = arg->assignment_testable_type_opt();
                    if (!arg_type_opt) {
                        throw exceptions::invalid_request_exception(format("{}() function is only valid when argument types are known", function_name));
                    }
                    arg_types.push_back(*arg_type_opt);
                }
                return arg_types;
            }
        }, provided_args);
    };

    if (name.has_keyspace()
                ? name == MIN_NAME
                : name.name == MIN_NAME.name) {
        auto arg_types = get_arguments(MIN_NAME.name);
        if (arg_types.size() != 1) {
            throw std::runtime_error("min() function requires only 1 argument");
        }

        auto& arg = arg_types[0];
        return aggregate_fcts::make_min_function(arg);
    } else if (name.has_keyspace()
                ? name == MAX_NAME
                : name.name == MAX_NAME.name) {
        auto arg_types = get_arguments(MAX_NAME.name);
        if (arg_types.size() != 1) {
            throw std::runtime_error("max() function requires only 1 argument");
        }

        auto& arg = arg_types[0];
        return aggregate_fcts::make_max_function(arg);
    } else if (name.has_keyspace()
                ? name == COUNT_NAME
                : name.name == COUNT_NAME.name) {
        auto arg_types = get_arguments(COUNT_NAME.name);
        if (arg_types.size() != 1) {
            throw std::runtime_error("count() function requires only 1 argument");
        }

        auto& arg = arg_types[0];
        return aggregate_fcts::make_count_function(arg);
    } else if (name.has_keyspace()
                ? name == COUNT_ROWS_NAME
                : name.name == COUNT_ROWS_NAME.name) {
        auto arg_types = get_arguments(COUNT_ROWS_NAME.name);
        if (arg_types.size() != 1 && arg_types.size() != 0) {
            throw std::runtime_error(format("countRows() function requires 0 or 1 argument, proveded {}", arg_types.size()));
        }

        if (arg_types.size() == 0) {
            return aggregate_fcts::make_count_rows_function();
        }
        auto& arg = arg_types[0];
        if (arg->is_collection() || arg->is_tuple() || arg->is_user_type()) {
            return aggregate_fcts::make_count_rows_function();
        }
    } 
    return {};
}

shared_ptr<function>
functions::get(data_dictionary::database db,
        const sstring& keyspace,
        const function_name& name,
        const std::vector<shared_ptr<assignment_testable>>& provided_args,
        const sstring& receiver_ks,
        std::optional<const std::string_view> receiver_cf,
        const column_specification* receiver) const {

    static const function_name TOKEN_FUNCTION_NAME = function_name::native_function("token");
    static const function_name TO_JSON_FUNCTION_NAME = function_name::native_function("tojson");
    static const function_name FROM_JSON_FUNCTION_NAME = function_name::native_function("fromjson");

    auto schema = std::invoke([&] () -> schema_ptr {
        if (receiver_cf.has_value() && db.has_schema(receiver_ks, *receiver_cf)) {
            return db.find_schema(receiver_ks, *receiver_cf);
        } else {
            return nullptr;
        }
    });

    if (name.has_keyspace()
                ? name == TOKEN_FUNCTION_NAME
                : name.name == TOKEN_FUNCTION_NAME.name) {

        if (!receiver_cf.has_value()) {
            throw exceptions::invalid_request_exception("functions::get for token doesn't have a known column family");
        }
        if (schema == nullptr) {
            throw exceptions::invalid_request_exception(format("functions::get for token cannot find {} table", *receiver_cf));
        }
        auto fun = ::make_shared<token_fct>(schema);
        validate_types(db, keyspace, schema.get(), fun, provided_args, receiver_ks, receiver_cf);
        return fun;
    }

    if (name.has_keyspace()
                ? name == TO_JSON_FUNCTION_NAME
                : name.name == TO_JSON_FUNCTION_NAME.name) {
        if (provided_args.size() != 1) {
            throw exceptions::invalid_request_exception("toJson() accepts 1 argument only");
        }
        auto arg_type_opt = provided_args[0]->assignment_testable_type_opt();
        if (!arg_type_opt) {
            throw exceptions::invalid_request_exception("toJson() is only valid when its argument type is known");
        }
        return make_to_json_function(*arg_type_opt);
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

    auto aggr_fun = get_dynamic_aggregate(name, provided_args);
    if (aggr_fun) {
        return aggr_fun;
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
        validate_types(db, keyspace, schema.get(), fun, provided_args, receiver_ks, receiver_cf);
        return fun;
    }

    std::vector<shared_ptr<function>> compatibles;
    for (auto&& to_test : candidates) {
        auto r = match_arguments(db, keyspace, schema.get(), to_test, provided_args, receiver_ks, receiver_cf);
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
                format("Invalid call to function {}, none of its type signatures match (known type signatures: {})",
                                                        name, fmt::join(candidates, ", ")));
    }

    if (compatibles.size() > 1) {
        throw exceptions::invalid_request_exception(
                format("Ambiguous call to function {} (can be matched by following signatures: {}): use type casts to disambiguate",
                    name, fmt::join(compatibles, ", ")));
    }

    return std::move(compatibles[0]);
}

template<typename F>
std::vector<shared_ptr<F>> functions::get_filtered_transformed(const sstring& keyspace) const {
    auto filter = [&] (const std::pair<const function_name, shared_ptr<function>>& d) -> bool {
        return d.first.keyspace == keyspace && dynamic_cast<F*>(d.second.get());
    };
    auto transformer = [] (const std::pair<const function_name, shared_ptr<function>>& d) -> shared_ptr<F> {
        return dynamic_pointer_cast<F>(d.second);
    };
    
    return boost::copy_range<std::vector<shared_ptr<F>>>(
        _declared 
        | boost::adaptors::filtered(filter) 
        | boost::adaptors::transformed(transformer)
    );
}

std::vector<shared_ptr<user_function>>
functions::get_user_functions(const sstring& keyspace) const {
    return get_filtered_transformed<user_function>(keyspace);
}

std::vector<shared_ptr<user_aggregate>>
functions::get_user_aggregates(const sstring& keyspace) const {
    return get_filtered_transformed<user_aggregate>(keyspace);
}

boost::iterator_range<functions::declared_t::const_iterator>
functions::find(const function_name& name) const {
    SCYLLA_ASSERT(name.has_keyspace()); // : "function name not fully qualified";
    auto pair = _declared.equal_range(name);
    return boost::make_iterator_range(pair.first, pair.second);
}

functions::declared_t::const_iterator
functions::find_iter(const function_name& name, const std::vector<data_type>& arg_types) const {
    auto range = find(name);
    auto i = std::find_if(range.begin(), range.end(), [&] (const std::pair<const function_name, shared_ptr<function>>& d) {
        return type_equals(d.second->arg_types(), arg_types);
    });
    if (i == range.end()) {
        return _declared.end();
    }
    return i;
}

shared_ptr<function>
functions::find(const function_name& name, const std::vector<data_type>& arg_types) const {
    auto i = find_iter(name, arg_types);
    if (i != _declared.end()) {
        return i->second;
    }
    return {};
}

// This function is created only for mapreduce_service use, thus it only checks for
// aggregate functions if no declared function was found.
//
// The reason for this function is, there is no serialization of `cql3::selection::selection`,
// so functions lying underneath these selections has to be refound.
//
// Most of this code is copied from `functions::get()`, however `functions::get()` requires to 
// mock or serialize expressions and `functions::find()` is not enough,
// because it does not search for dynamic aggregate functions
shared_ptr<function>
functions::mock_get(const function_name &name, const std::vector<data_type>& arg_types) const {
    auto func = find(name, arg_types);
    if (!func) {
        func = get_dynamic_aggregate(name, arg_types);
    }
    return func;
}

// This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
// case where there is no override for a given function. This is thus probably worth the minor code duplication.
void
functions::validate_types(data_dictionary::database db,
                          const sstring& keyspace,
                          const schema* schema_opt,
                          shared_ptr<function> fun,
                          const std::vector<shared_ptr<assignment_testable>>& provided_args,
                          const sstring& receiver_ks,
                          std::optional<const std::string_view> receiver_cf) const {
    if (provided_args.size() != fun->arg_types().size()) {
        throw exceptions::invalid_request_exception(
                format("Invalid number of arguments in call to function {}: {:d} required but {:d} provided",
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
        if (!is_assignable(provided->test_assignment(db, keyspace, schema_opt, *expected))) {
            throw exceptions::invalid_request_exception(
                    format("Type error: {} cannot be passed as argument {:d} of function {} of type {}",
                            provided, i, fun->name(), expected->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result
functions::match_arguments(data_dictionary::database db, const sstring& keyspace,
        const schema* schema_opt,
        shared_ptr<function> fun,
        const std::vector<shared_ptr<assignment_testable>>& provided_args,
        const sstring& receiver_ks,
        std::optional<const std::string_view> receiver_cf) const {
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
        auto arg_res = provided->test_assignment(db, keyspace, schema_opt, *expected);
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
functions::type_equals(const std::vector<data_type>& t1, const std::vector<data_type>& t2) const {
    return t1 == t2;
}

static thread_local functions f;

const functions& instance() {
    return f;
}

void change_batch::commit() {
    if (_declared.empty()) {
        return;
    }
    f._declared = std::move(_declared);
}

void change_batch::clear_functions() noexcept {
    _declared = init();
}

}
}


