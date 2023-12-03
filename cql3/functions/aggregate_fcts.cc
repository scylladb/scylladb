/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "bytes.hh"
#include "types/types.hh"
#include "types/tuple.hh"
#include "cql3/functions/scalar_function.hh"
#include "db/functions/aggregate_function.hh"
#include "cql3/util.hh"
#include "utils/big_decimal.hh"
#include "aggregate_fcts.hh"
#include "user_aggregate.hh"
#include "functions.hh"
#include "first_function.hh"
#include "exceptions/exceptions.hh"
#include "utils/multiprecision_int.hh"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>

using namespace cql3;
using namespace functions;
using namespace aggregate_fcts;

namespace cql3::functions {
    extern logging::logger log;
}

namespace {

class internal_scalar_function : public scalar_function {
    function_name _name;
    data_type _return_type;
    std::vector<data_type> _arg_types;
    noncopyable_function<bytes_opt (std::span<const bytes_opt> parameters)> _func;
public:
    internal_scalar_function(
            sstring name,
            data_type return_type,
            std::vector<data_type> arg_types,
            noncopyable_function<bytes_opt (std::span<const bytes_opt> parameters)> func)
            : _name(function_name::native_function(std::move(name)))
            , _return_type(std::move(return_type))
            , _arg_types(std::move(arg_types))
            , _func(std::move(func)) {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override {
        return _func(parameters);
    }

    virtual const function_name& name() const override {
        return _name;
    }

    virtual const std::vector<data_type>& arg_types() const override {
        return _arg_types;
    }

    virtual const data_type& return_type() const override {
        return _return_type;
    }

    virtual bool is_pure() const override {
        return true;
    }

    virtual bool is_native() const override {
        return true;
    }

    virtual bool requires_thread() const override {
        return false;
    }

    virtual bool is_aggregate() const override {
        return false;
    }

    virtual void print(std::ostream& os) const override {
        fmt::print(os, "{}", _name);
    }

    virtual sstring column_name(const std::vector<sstring>& column_names) const override {
        return _name.name;
    }
};

// Called if any of the inputs is NULL
using null_handler = bytes_opt (*)(std::span<const bytes_opt>);

bytes_opt
return_accumulator_on_null(std::span<const bytes_opt> args) {
    return args[0];
}

bytes_opt
return_any_nonnull(std::span<const bytes_opt> args) {
    auto i = std::ranges::find_if(args, std::mem_fn(&bytes_opt::has_value));
    return i != args.end() ? *i : bytes_opt();
}

template <typename Ret, typename... Args>
noncopyable_function<bytes_opt (std::span<const bytes_opt>)>
wrap_function_autonull(null_handler nullhandler, Ret (*func)(Args...)) {
    return [nullhandler, func] (std::span<const bytes_opt> args) -> bytes_opt {
        if (!std::all_of(args.begin(), args.end(), std::mem_fn(&bytes_opt::has_value))) {
            return nullhandler(args);
        }
        using args_tuple_type = std::tuple<Args...>;
        auto ret = std::invoke([&] <size_t... Indexes> (std::index_sequence<Indexes...>) {
            return func(value_cast<std::tuple_element_t<Indexes, args_tuple_type>>(
                         data_type_for<std::tuple_element_t<Indexes, args_tuple_type>>()->deserialize_value(*args[Indexes]))...);
        }, std::index_sequence_for<Args...>());
        return data_value(std::move(ret)).serialize_nonnull();
    };
}

template <typename Ret, typename... Args>
shared_ptr<scalar_function>
make_internal_scalar_function(sstring name, null_handler nullhandler, Ret (*func)(Args...)) {
    return ::make_shared<internal_scalar_function>(
            std::move(name),
            data_type_for<Ret>(),
            std::vector({data_type_for<Args>()...}),
            wrap_function_autonull(nullhandler, func)
    );
}

template <typename Lambda>
requires std::is_class_v<Lambda>
shared_ptr<scalar_function>
make_internal_scalar_function(sstring name, null_handler nullhandler, Lambda func) {
    // "+func" decays the lambda into a pointer-to-function, so that its signature
    // can be inferred by the other overload.
    return make_internal_scalar_function(std::move(name), nullhandler, +func);
}

template<typename NarrowT, typename WideT>
NarrowT
narrow(WideT acc) {
    NarrowT ret = static_cast<NarrowT>(acc);
    // The following check only makes sense when NarrowT and WideT are two
    // different integral types and we want to check that NarrowT isn't too
    // narrow. Let's avoid the check when they are the same type - it is
    // useless, and worse - wrong for the floating-point case (issue #13564).
    if constexpr (!std::is_same<WideT, NarrowT>::value) {
        if (static_cast<WideT>(ret) != acc) {
            throw exceptions::overflow_error_exception("Sum overflow. Values should be casted to a wider type.");
        }
    }
    return ret;
}

// We need a wider accumulator for sum and average,
// since summing the inputs can overflow the input type
template <typename T>
using accumulator_for = std::conditional_t<std::is_integral_v<T>, utils::multiprecision_int, T>;

template <typename Type>
static
shared_ptr<aggregate_function>
make_sum_function() {
    using Acc = accumulator_for<Type>;
    return make_shared<db::functions::aggregate_function>(
        db::functions::stateless_aggregate_function{
            .name = function_name::native_function("sum"),
            .state_type = data_type_for<accumulator_for<Type>>(),
            .result_type = data_type_for<Type>(),
            .argument_types = {data_type_for<Type>()},
            .initial_state = data_type_for<accumulator_for<Type>>()->decompose(Acc(0)),
            .aggregation_function = make_internal_scalar_function("sum_step", return_accumulator_on_null, [] (Acc acc, Type addend) -> Acc { return acc + addend; }),
            .state_to_result_function = make_internal_scalar_function("sum_finalizer", return_any_nonnull, [] (Acc acc) -> Type { return narrow<Type>(acc); }),
            .state_reduction_function = make_internal_scalar_function("sum_reducer", return_any_nonnull, [] (Acc a1, Acc a2) -> Acc { return a1 + a2; }),
        }
    );
}

template <typename Type>
class impl_div_for_avg {
public:
    static Type div(const accumulator_for<Type>& x, const int64_t y) {
        return Type(x/y);
    }
};

template <>
class impl_div_for_avg<big_decimal> {
public:
    static big_decimal div(const big_decimal& x, const int64_t y) {
        return x.div(y, big_decimal::rounding_mode::HALF_EVEN);
    }
};
template <typename Type>
static
shared_ptr<aggregate_function>
make_avg_function() {
    using sum_type = accumulator_for<Type>;
    auto accumulator_tuple_type = tuple_type_impl::get_instance({data_type_for<sum_type>(), data_type_for<int64_t>()});
    return make_shared<db::functions::aggregate_function>(
        db::functions::stateless_aggregate_function{
            .name = function_name::native_function("avg"),
            .state_type = accumulator_tuple_type,
            .result_type = data_type_for<Type>(),
            .argument_types = {data_type_for<Type>()},
            .initial_state = make_tuple_value(accumulator_tuple_type, std::vector({data_value(sum_type(0)), data_value(int64_t(0))})).serialize(),
            .aggregation_function = ::make_shared<internal_scalar_function>(
                    "avg_step",
                    accumulator_tuple_type,
                    std::vector<data_type>({accumulator_tuple_type, data_type_for<Type>()}),
                    [accumulator_tuple_type] (std::span<const bytes_opt> args) -> bytes_opt {
                        if (!args[0]) {
                            return std::nullopt;
                        }
                        if (!args[1]) {
                            return args[0];
                        }
                        data_value acc_value = accumulator_tuple_type->deserialize(*args[0]);
                        std::vector<data_value> acc = value_cast<tuple_type_impl::native_type>(std::move(acc_value));
                        auto sum = value_cast<sum_type>(acc[0]);
                        auto count = value_cast<int64_t>(acc[1]);
                        auto input = value_cast<Type>(data_type_for<Type>()->deserialize(*args[1]));
                        sum += input;
                        count += 1;
                        acc[0] = data_value(std::move(sum));
                        acc[1] = data_value(count);
                        return make_tuple_value(accumulator_tuple_type, acc).serialize();
                    }),
            .state_to_result_function = ::make_shared<internal_scalar_function>(
                    "avg_finalizer",
                    data_type_for<Type>(),
                    std::vector<data_type>({accumulator_tuple_type}),
                    [accumulator_tuple_type] (std::span<const bytes_opt> args) -> bytes_opt {
                        data_value acc_value = accumulator_tuple_type->deserialize(*args[0]);
                        std::vector<data_value> acc = value_cast<tuple_type_impl::native_type>(std::move(acc_value));
                        auto sum = value_cast<sum_type>(acc[0]);
                        auto count = value_cast<int64_t>(acc[1]);
                        auto result = count ? impl_div_for_avg<Type>::div(sum, count) : Type();
                        return data_type_for<Type>()->decompose(result);
                    }),
            .state_reduction_function = ::make_shared<internal_scalar_function>(
                    "avg_reducer",
                    accumulator_tuple_type,
                    std::vector<data_type>({accumulator_tuple_type, accumulator_tuple_type}),
                    [accumulator_tuple_type] (std::span<const bytes_opt> args) -> bytes_opt {
                        data_value acc1_value = accumulator_tuple_type->deserialize(*args[0]);
                        std::vector<data_value> acc1 = value_cast<tuple_type_impl::native_type>(std::move(acc1_value));
                        auto sum1 = value_cast<sum_type>(acc1[0]);
                        auto count1 = value_cast<int64_t>(acc1[1]);
                        data_value acc2_value = accumulator_tuple_type->deserialize(*args[1]);
                        std::vector<data_value> acc2 = value_cast<tuple_type_impl::native_type>(std::move(acc2_value));
                        auto sum2 = value_cast<sum_type>(acc2[0]);
                        auto count2 = value_cast<int64_t>(acc2[1]);
                        acc1[0] = data_value(sum1 + sum2);
                        acc1[1] = data_value(count1 + count2);
                        return make_tuple_value(accumulator_tuple_type, acc1).serialize();
                    }),
        });
}

template <typename T>
struct aggregate_type_for {
    using type = T;
};

template<>
struct aggregate_type_for<ascii_native_type> {
    using type = ascii_native_type::primary_type;
};

template<>
struct aggregate_type_for<simple_date_native_type> {
    using type = simple_date_native_type::primary_type;
};

template<>
struct aggregate_type_for<timeuuid_native_type> {
    using type = timeuuid_native_type;
};

template<>
struct aggregate_type_for<time_native_type> {
    using type = time_native_type::primary_type;
};

} // anonymous namespace

/**
 * Creates a COUNT function for the specified type.
 *
 * @param input_type the function input type
 * @return a COUNT function for the specified type.
 */
shared_ptr<aggregate_function>
aggregate_fcts::make_count_function(data_type input_type) {
    return make_shared<db::functions::aggregate_function>(
        db::functions::stateless_aggregate_function{
            .name = function_name::native_function("count"),
            .state_type = long_type,
            .result_type = long_type,
            .argument_types = {input_type},
            .initial_state = data_value(int64_t(0)).serialize(),
            .aggregation_function = ::make_shared<internal_scalar_function>(
                    "count_step",
                    long_type,
                    std::vector<data_type>({long_type, input_type}),
                    [] (std::span<const bytes_opt> args) {
                        if (!args[1]) {
                            return args[0];
                        }
                        auto count = value_cast<int64_t>(long_type->deserialize(*args[0]));
                        count += 1;
                        return data_value(count).serialize();
                    }),
            .state_to_result_function = make_internal_scalar_function("count_finalizer", return_any_nonnull, [] (int64_t count) { return count; }),
            .state_reduction_function = make_internal_scalar_function("count_reducer", return_any_nonnull, [] (int64_t c1, int64_t c2) { return c1 + c2; }),
        });
}

// Drops the first arg type from the types declaration (which denotes the accumulator)
// in order to compute the actual type of given user-defined-aggregate (UDA)
static std::vector<data_type> state_arg_types_to_uda_arg_types(const std::vector<data_type>& arg_types) {
    if(arg_types.size() < 2) {
        on_internal_error(cql3::functions::log, "State function for user-defined aggregates needs at least two arguments");
    }
    std::vector<data_type> types;
    types.insert(types.end(), std::next(arg_types.begin()), arg_types.end());
    return types;
}

static data_type uda_return_type(const ::shared_ptr<scalar_function>& ffunc, const ::shared_ptr<scalar_function>& sfunc) {
    return ffunc ? ffunc->return_type() : sfunc->return_type();
}

user_aggregate::user_aggregate(function_name fname, bytes_opt initcond, ::shared_ptr<scalar_function> sfunc, ::shared_ptr<scalar_function> reducefunc, ::shared_ptr<scalar_function> finalfunc)
        : aggregate_function(db::functions::stateless_aggregate_function{
                .name = fname,
                .state_type = sfunc->return_type(),
                .result_type = finalfunc ? finalfunc->return_type() : sfunc->return_type(),
                .argument_types = std::vector(std::next(sfunc->arg_types().begin()), sfunc->arg_types().end()),
                .initial_state = std::move(initcond),
                .aggregation_function = std::move(sfunc),
                .state_to_result_function = std::move(finalfunc),
                .state_reduction_function = std::move(reducefunc),
          }) {
}

bool user_aggregate::has_finalfunc() const { return _agg.state_to_result_function != nullptr; }

std::ostream& user_aggregate::describe(std::ostream& os) const {
    auto ks = cql3::util::maybe_quote(name().keyspace);
    auto na = cql3::util::maybe_quote(name().name);

    os << "CREATE AGGREGATE " << ks << "." << na << "(";
    auto a = arg_types();
    for (size_t i = 0; i < a.size(); i++) {
        if (i > 0) {
            os << ", ";
        }
        os << a[i]->cql3_type_name();
    }
    os << ")\n";

    os << "SFUNC " << cql3::util::maybe_quote(_agg.aggregation_function->name().name) << "\n"
       << "STYPE " << _agg.aggregation_function->return_type()->cql3_type_name();
    if (is_reducible()) {
        os << "\n" << "REDUCEFUNC " << cql3::util::maybe_quote(_agg.state_reduction_function->name().name);
    }
    if (has_finalfunc()) {
        os << "\n" << "FINALFUNC " << cql3::util::maybe_quote(_agg.state_to_result_function->name().name);
    }
    if (_agg.initial_state) {
        os << "\n" << "INITCOND " << _agg.aggregation_function->return_type()->deserialize(bytes_view(*_agg.initial_state)).to_parsable_string();
    }
    os << ";";

    return os;
}

shared_ptr<aggregate_function>
aggregate_fcts::make_count_rows_function() {
    return make_shared<db::functions::aggregate_function>(
        db::functions::stateless_aggregate_function{
            .name = function_name::native_function(COUNT_ROWS_FUNCTION_NAME),
            .column_name_override = "count",
            .state_type = long_type,
            .result_type = long_type,
            .argument_types = {},
            .initial_state = data_value(int64_t(0)).serialize(),
            .aggregation_function = make_internal_scalar_function("count_step", return_any_nonnull, [] (int64_t accumulator) {
                return accumulator + 1;
            }),
            .state_to_result_function = make_internal_scalar_function("count_finalizer", return_any_nonnull, [] (int64_t accumulator) {
                return accumulator;
            }),
            .state_reduction_function = make_internal_scalar_function("count_reducer", return_any_nonnull, [] (int64_t acc1, int64_t acc2) {
                return acc1 + acc2;
            }),
        }
    );
}

shared_ptr<aggregate_function>
aggregate_fcts::make_max_function(data_type io_type) {
    io_type = io_type->without_reversed().shared_from_this();
    auto max = ::make_shared<internal_scalar_function>("max_step", io_type, std::vector({io_type, io_type}), [io_type] (std::span<const bytes_opt> args) -> bytes_opt {
        if (!args[0]) {
            return args[1];
        }
        if (!args[1]) {
            return args[0];
        }
        return std::max(*args[0], *args[1], io_type->as_less_comparator());
    });
    return ::make_shared<db::functions::aggregate_function>(
        db::functions::stateless_aggregate_function{
            .name = function_name::native_function("max"),
            .state_type = io_type,
            .result_type = io_type,
            .argument_types = {io_type},
            .initial_state = std::nullopt,
            .aggregation_function = max,
            .state_to_result_function = ::make_shared<internal_scalar_function>("max_finalizer", io_type, std::vector({io_type}), [] (std::span<const bytes_opt> args) {
                return args[0];
            }),
            .state_reduction_function = max,
        }
    );
}

shared_ptr<aggregate_function>
aggregate_fcts::make_min_function(data_type io_type) {
    io_type = io_type->without_reversed().shared_from_this();
    auto min = ::make_shared<internal_scalar_function>("min_step", io_type, std::vector({io_type, io_type}), [io_type] (std::span<const bytes_opt> args) -> bytes_opt {
        if (!args[0]) {
            return args[1];
        }
        if (!args[1]) {
            return args[0];
        }
        return std::min(*args[0], *args[1], io_type->as_less_comparator());
    });
    return ::make_shared<db::functions::aggregate_function>(
        db::functions::stateless_aggregate_function{
            .name = function_name::native_function("min"),
            .state_type = io_type,
            .result_type = io_type,
            .argument_types = {io_type},
            .initial_state = std::nullopt,
            .aggregation_function = min,
            .state_to_result_function = ::make_shared<internal_scalar_function>("min_finalizer", io_type, std::vector({io_type}), [] (std::span<const bytes_opt> args) {
                return args[0];
            }),
            .state_reduction_function = min,
        }
    );
}

function_name
aggregate_fcts::first_function_name() {
    return function_name::native_function("$$first$$");
}

shared_ptr<aggregate_function>
aggregate_fcts::make_first_function(data_type io_type) {
    io_type = io_type->without_reversed().shared_from_this();
    // The function's state is a one-element tuple containing the value, if the tuple
    // itself is null then the an input hasn't been seen yet
    auto state_type = data_type(tuple_type_impl::get_instance({io_type}));
    return ::make_shared<db::functions::aggregate_function>(
        db::functions::stateless_aggregate_function{
            .name = first_function_name(),
            .state_type = state_type,
            .result_type = io_type,
            .argument_types = {io_type},
            .initial_state = std::nullopt,
            .aggregation_function = ::make_shared<internal_scalar_function>("first_agg", state_type, std::vector({state_type, io_type}), [] (std::span<const bytes_opt> args) -> bytes_opt {
                if (!args[0]) {
                    // First call: create a tuple with the input
                    return tuple_type_impl::build_value(boost::make_iterator_range_n(&args[1], 1));
                } else {
                    // Second or later call: return result of first call
                    return args[0];
                }
            }),
            .state_to_result_function = ::make_shared<internal_scalar_function>("first_finalizer", io_type, std::vector({state_type}), [] (std::span<const bytes_opt> args) -> bytes_opt {
                if (!args[0]) {
                    return std::nullopt;
                } else {
                    return to_bytes_opt(get_nth_tuple_element(managed_bytes_view(*args[0]), 0));
                }
            }),
            .state_reduction_function = ::make_shared<internal_scalar_function>("first_reducer", state_type, std::vector({state_type, state_type}), return_any_nonnull),
        }
    );
}


void cql3::functions::add_agg_functions(declared_t& funcs) {
    auto declare = [&funcs] (shared_ptr<function> f) { funcs.emplace(f->name(), f); };

    declare(make_sum_function<int8_t>());
    declare(make_sum_function<int16_t>());
    declare(make_sum_function<int32_t>());
    declare(make_sum_function<int64_t>());
    declare(make_sum_function<float>());
    declare(make_sum_function<double>());
    declare(make_sum_function<utils::multiprecision_int>());
    declare(make_sum_function<big_decimal>());
    declare(make_avg_function<int8_t>());
    declare(make_avg_function<int16_t>());
    declare(make_avg_function<int32_t>());
    declare(make_avg_function<int64_t>());
    declare(make_avg_function<float>());
    declare(make_avg_function<double>());
    declare(make_avg_function<utils::multiprecision_int>());
    declare(make_avg_function<big_decimal>());
}
