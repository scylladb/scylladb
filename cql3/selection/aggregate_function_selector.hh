/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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

    virtual void add_input(cql_serialization_format sf, result_set_builder& rs) override {
        // Aggregation of aggregation is not supported
        size_t m = _arg_selectors.size();
        for (size_t i = 0; i < m; ++i) {
            auto&& s = _arg_selectors[i];
            s->add_input(sf, rs);
            _args[i] = s->get_output(sf);
            s->reset();
        }
        _aggregate->add_input(sf, _args);
    }

    virtual bytes_opt get_output(cql_serialization_format sf) override {
        return _aggregate->compute(sf);
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
