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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#pragma once

#include "selector.hh"
#include "types.hh"
#include "types/user.hh"
#include "cql_serialization_format.hh"

namespace cql3 {

namespace selection {

class field_selector : public selector {
    user_type _type;
    size_t _field;
    shared_ptr<selector> _selected;
public:
    static shared_ptr<factory> new_factory(user_type type, size_t field, shared_ptr<selector::factory> factory) {
        struct field_selector_factory : selector::factory {
            user_type _type;
            size_t _field;
            shared_ptr<selector::factory> _factory;

            field_selector_factory(user_type type, size_t field, shared_ptr<selector::factory> factory)
                    : _type(std::move(type)), _field(field), _factory(std::move(factory)) {
            }

            virtual sstring column_name() const override {
                auto&& name = _type->field_name(_field);
                auto sname = sstring(reinterpret_cast<const char*>(name.begin()), name.size());
                return format("{}.{}", _factory->column_name(), sname);
            }

            virtual data_type get_return_type() const override {
                return _type->field_type(_field);
            }

            shared_ptr<selector> new_instance() const override {
                return make_shared<field_selector>(_type, _field, _factory->new_instance());
            }

            bool is_aggregate_selector_factory() const override {
                return _factory->is_aggregate_selector_factory();
            }
        };
        return make_shared<field_selector_factory>(std::move(type), field, std::move(factory));
    }

    virtual bool is_aggregate() const override {
        return false;
    }

    virtual void add_input(cql_serialization_format sf, result_set_builder& rs) override {
        _selected->add_input(sf, rs);
    }

    virtual bytes_opt get_output(cql_serialization_format sf) override {
        auto&& value = _selected->get_output(sf);
        if (!value) {
            return std::nullopt;
        }
        return get_nth_tuple_element(single_fragmented_view(*value), _field);
    }

    virtual data_type get_type() const override {
        return _type->field_type(_field);
    }

    virtual void reset() {
        _selected->reset();
    }

    virtual sstring assignment_testable_source_context() const override {
        auto&& name = _type->field_name(_field);
        auto sname = sstring(reinterpret_cast<const char*>(name.begin(), name.size()));
        return format("{}.{}", _selected, sname);
    }

    field_selector(user_type type, size_t field, shared_ptr<selector> selected)
            : _type(std::move(type)), _field(field), _selected(std::move(selected)) {
    }
};

}
}
