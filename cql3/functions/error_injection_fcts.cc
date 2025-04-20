/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "error_injection_fcts.hh"
#include "utils/error_injection.hh"
#include "types/list.hh"
#include <seastar/core/map_reduce.hh>

namespace cql3
{

namespace functions
{

namespace error_injection
{

namespace
{

template <typename Func, bool Pure>
class failure_injection_function_for : public failure_injection_function  {
    Func _func;
public:
    failure_injection_function_for(sstring name,
                                   data_type return_type,
                                   const std::vector<data_type> arg_types,
                                   Func&& func)
        : failure_injection_function(std::move(name), std::move(return_type), std::move(arg_types))
        , _func(std::forward<Func>(func)) {}

    bool is_pure() const override {
        return Pure;
    }

    bytes_opt execute(std::span<const bytes_opt> parameters) override {
        return _func(parameters);
    }
};

template <bool Pure, typename Func>
shared_ptr<function>
make_failure_injection_function(sstring name,
        data_type return_type,
        std::vector<data_type> args_type,
        Func&& func) {
    return ::make_shared<failure_injection_function_for<Func, Pure>>(std::move(name),
        std::move(return_type),
        std::move(args_type),
        std::forward<Func>(func));
}

} // anonymous namespace

shared_ptr<function> make_enable_injection_function() {
    return make_failure_injection_function<false>("enable_injection", empty_type, { ascii_type, ascii_type },
            [] (std::span<const bytes_opt> parameters) {
        sstring injection_name = ascii_type->get_string(parameters[0].value());
        const bool one_shot = ascii_type->get_string(parameters[1].value()) == "true";
        smp::invoke_on_all([injection_name, one_shot] () mutable {
            utils::get_local_injector().enable(injection_name, one_shot);
        }).get();
        return std::nullopt;
    });
}

shared_ptr<function> make_disable_injection_function() {
    return make_failure_injection_function<false>("disable_injection", empty_type, { ascii_type },
            [] (std::span<const bytes_opt> parameters) {
        sstring injection_name = ascii_type->get_string(parameters[0].value());
        smp::invoke_on_all([injection_name] () mutable {
            utils::get_local_injector().disable(injection_name);
        }).get();
        return std::nullopt;
    });
}

shared_ptr<function> make_enabled_injections_function() {
    const auto list_type_inst = list_type_impl::get_instance(ascii_type, false);
    return make_failure_injection_function<false>("enabled_injections", list_type_inst, {},
        [list_type_inst] (std::span<const bytes_opt>) -> bytes {
            return seastar::map_reduce(smp::all_cpus(), [] (unsigned) {
                return make_ready_future<std::vector<sstring>>(utils::get_local_injector().enabled_injections());
            }, std::vector<data_value>(),
            [](std::vector<data_value> a, std::vector<sstring>&& b) -> std::vector<data_value> {
                for (auto&& x : b) {
                    if (a.end() == std::find(a.begin(), a.end(), x)) {
                        a.push_back(data_value(std::move(x)));
                    }
                }
                return a;
            }).then([list_type_inst](std::vector<data_value> const& active_injections) {
                auto list_val = make_list_value(list_type_inst, active_injections);
                return list_type_inst->decompose(list_val);
            }).get();
        });
}

} // namespace error_injection

} // namespace functions

} // namespace cql3
