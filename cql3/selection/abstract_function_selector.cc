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
 * Copyright (C) 2015 ScyllaDB
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
#include "aggregate_function_selector.hh"
#include "scalar_function_selector.hh"
#include "to_string.hh"

namespace cql3 {

namespace selection {

shared_ptr<selector::factory>
abstract_function_selector::new_factory(shared_ptr<functions::function> fun, shared_ptr<selector_factories> factories) {
    if (fun->is_aggregate()) {
        if (factories->does_aggregation()) {
            throw exceptions::invalid_request_exception("aggregate functions cannot be used as arguments of aggregate functions");
        }
    } else {
        if (factories->does_aggregation() && !factories->contains_only_aggregate_functions()) {
            throw exceptions::invalid_request_exception(sprint("the %s function arguments must be either all aggregates or all none aggregates",
                                                            fun->name()));
        }
    }

    struct fun_selector_factory : public factory {
        shared_ptr<functions::function> _fun;
        shared_ptr<selector_factories> _factories;

        fun_selector_factory(shared_ptr<functions::function> fun,
                             shared_ptr<selector_factories> factories)
                : _fun(std::move(fun)), _factories(std::move(factories)) {
        }

        virtual sstring column_name() override {
            return _fun->column_name(_factories->get_column_names());
        }

        virtual data_type get_return_type() override {
            return _fun->return_type();
        }

        virtual bool uses_function(const sstring& ks_name, const sstring& function_name) override {
            return _fun->uses_function(ks_name, function_name);
        }

        virtual shared_ptr<selector> new_instance() override {
            using ret_type = shared_ptr<selector>;
            return _fun->is_aggregate() ? ret_type(::make_shared<aggregate_function_selector>(_fun, _factories->new_instances()))
                                        : ret_type(::make_shared<scalar_function_selector>(_fun, _factories->new_instances()));
        }

        virtual bool is_write_time_selector_factory() override {
            return _factories->contains_write_time_selector_factory();
        }

        virtual bool is_ttl_selector_factory() override {
            return _factories->contains_ttl_selector_factory();
        }

        virtual bool is_aggregate_selector_factory() override {
            return _fun->is_aggregate() || _factories->contains_only_aggregate_functions();
        }
    };

    return make_shared<fun_selector_factory>(std::move(fun), std::move(factories));
}

}

}
