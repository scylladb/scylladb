/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "abstract_function_selector.hh"
#include "cql3/functions/scalar_function.hh"

namespace cql3 {

namespace selection {

class scalar_function_selector : public abstract_function_selector_for<functions::scalar_function> {
public:
    virtual bool is_aggregate() const override {
        // We cannot just return true as it is possible to have a scalar function wrapping an aggregation function
        if (_arg_selectors.empty()) {
            return false;
        }

        return _arg_selectors[0]->is_aggregate();
    }

    virtual void add_input(result_set_builder& rs) override {
        size_t m = _arg_selectors.size();
        for (size_t i = 0; i < m; ++i) {
            auto&& s = _arg_selectors[i];
            s->add_input(rs);
        }
    }

    virtual void reset() override {
    }

    virtual managed_bytes_opt get_output() override {
        size_t m = _arg_selectors.size();
        for (size_t i = 0; i < m; ++i) {
            auto&& s = _arg_selectors[i];
            _args[i] = to_bytes_opt(s->get_output());
            s->reset();
        }
        return to_managed_bytes_opt(fun()->execute(_args));
    }

    virtual bool requires_thread() const override;

    scalar_function_selector(shared_ptr<functions::function> fun, std::vector<shared_ptr<selector>> arg_selectors)
            : abstract_function_selector_for<functions::scalar_function>(
                dynamic_pointer_cast<functions::scalar_function>(std::move(fun)), std::move(arg_selectors)) {
    }
};

}
}
