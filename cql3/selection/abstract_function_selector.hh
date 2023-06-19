/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "selector.hh"
#include "cql3/functions/function.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/functions/user_aggregate.hh"
#include "utils/small_vector.hh"
#include <boost/algorithm/cxx11/any_of.hpp>

namespace cql3 {
namespace selection {

class selector_factories;

class abstract_function_selector : public selector {
protected:
    shared_ptr<functions::function> _fun;

    /**
     * The list used to pass the function arguments is recycled to avoid the cost of instantiating a new list
     * with each function call.
     */
    utils::small_vector<bytes_opt, 4> _args;
    std::vector<shared_ptr<selector>> _arg_selectors;
    const bool _requires_thread;

public:
    static shared_ptr<factory> new_factory(shared_ptr<functions::function> fun, shared_ptr<selector_factories> factories);

    // If reserve_extra_arg is set, the internal buffer used for holding function argument lists is enlarged to account for
    // an aggregate function's accumulator.
    abstract_function_selector(shared_ptr<functions::function> fun, std::vector<shared_ptr<selector>> arg_selectors, bool reserve_extra_arg = false)
            : _fun(std::move(fun)), _arg_selectors(std::move(arg_selectors)),
              _requires_thread(boost::algorithm::any_of(_arg_selectors, [] (auto& s) { return s->requires_thread(); })
                    || _fun->requires_thread()) {
        _args.resize(_arg_selectors.size() + unsigned(reserve_extra_arg));
    }

    virtual bool requires_thread() const override;

    virtual data_type get_type() const override {
        return _fun->return_type();
    }

    shared_ptr<functions::function> function() const {
        return _fun;
    }

#if 0
    @Override
    public String toString()
    {
        return new StrBuilder().append(fun.name())
                               .append("(")
                               .appendWithSeparators(argSelectors, ", ")
                               .append(")")
                               .toString();
    }
#endif
};

template <typename T /* extends Function */>
class abstract_function_selector_for : public abstract_function_selector {
    shared_ptr<T> _tfun;  // We can't use static_pointer_cast due to virtual inheritance,
                          // so store it locally to amortize cost of dynamic_pointer_cast
protected:
    shared_ptr<T> fun() { return _tfun; }

    shared_ptr<const T> fun() const { return _tfun; }
public:
    // If reserve_extra_arg is set, the internal buffer used for holding function argument lists is enlarged to account for
    // an aggregate function's accumulator.
    abstract_function_selector_for(shared_ptr<T> fun, std::vector<shared_ptr<selector>> arg_selectors, bool reserve_extra_arg = false)
            : abstract_function_selector(fun, std::move(arg_selectors), reserve_extra_arg)
            , _tfun(dynamic_pointer_cast<T>(fun)) {
    }

    const functions::function_name& name() const {
        return _tfun->name();
    }
};

}
}
