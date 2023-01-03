/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "abstract_function_selector.hh"
#include "cql3/functions/aggregate_function.hh"

#pragma once

namespace cql3 {

namespace selection {

class aggregate_function_selector : public abstract_function_selector_for<functions::aggregate_function> {
    std::unique_ptr<functions::aggregate_function::aggregate> _aggregate;
public:
    virtual bool is_aggregate() const override {
        return true;
    }

    virtual void add_input(result_set_builder& rs) override {
        // Aggregation of aggregation is not supported
        size_t m = _arg_selectors.size();
        for (size_t i = 0; i < m; ++i) {
            auto&& s = _arg_selectors[i];
            s->add_input(rs);
            _args[i] = s->get_output();
            s->reset();
        }
        _aggregate->add_input(_args);
    }

    virtual bytes_opt get_output() override {
        return _aggregate->compute();
    }

    virtual void reset() override {
        _aggregate->reset();
    }

    aggregate_function_selector(shared_ptr<functions::function> func,
                std::vector<shared_ptr<selector>> arg_selectors)
            : abstract_function_selector_for<functions::aggregate_function>(
                    dynamic_pointer_cast<functions::aggregate_function>(func), std::move(arg_selectors))
            , _aggregate(fun()->new_aggregate()) {
    }
};

}
}
