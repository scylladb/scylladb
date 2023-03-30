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
    const db::functions::stateless_aggregate_function& _aggregate;
    bytes_opt _accumulator;
public:
    virtual bool is_aggregate() const override {
        return true;
    }

    virtual void add_input(result_set_builder& rs) override {
        // Aggregation of aggregation is not supported
        size_t m = _arg_selectors.size();
        _args[0] = std::move(_accumulator);
        for (size_t i = 0; i < m; ++i) {
            auto&& s = _arg_selectors[i];
            s->add_input(rs);
            _args[i + 1] = s->get_output();
            s->reset();
        }
        _accumulator = _aggregate.aggregation_function->execute(_args);
    }

    virtual bytes_opt get_output() override {
        return _aggregate.state_to_result_function
                ? _aggregate.state_to_result_function->execute({std::move(_accumulator)})
                : std::move(_accumulator);
    }

    virtual void reset() override {
        _accumulator = _aggregate.initial_state;
    }

    aggregate_function_selector(shared_ptr<functions::function> func,
                std::vector<shared_ptr<selector>> arg_selectors)
            : abstract_function_selector_for<functions::aggregate_function>(
                    dynamic_pointer_cast<functions::aggregate_function>(func), std::move(arg_selectors), true)
            , _aggregate(fun()->get_aggregate())
            , _accumulator(_aggregate.initial_state) {
    }
};

}
}
