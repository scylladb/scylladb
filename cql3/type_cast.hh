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

#pragma once

#include "term.hh"
#include "cql3_type.hh"

namespace cql3 {

class type_cast : public term::raw {
    shared_ptr<cql3_type::raw> _type;
    shared_ptr<term::raw> _term;
public:
    type_cast(shared_ptr<cql3_type::raw> type, shared_ptr<cql3::term::raw> term)
            : _type(std::move(type)), _term(std::move(term)) {
    }

    virtual shared_ptr<term> prepare(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) override {
        if (!is_assignable(_term->test_assignment(db, keyspace, casted_spec_of(db, keyspace, receiver)))) {
            throw exceptions::invalid_request_exception(sprint("Cannot cast value %s to type %s", _term, _type));
        }
        if (!is_assignable(test_assignment(db, keyspace, receiver))) {
            throw exceptions::invalid_request_exception(sprint("Cannot assign value %s to %s of type %s", *this, receiver->name, receiver->type->as_cql3_type()));
        }
        return _term->prepare(db, keyspace, receiver);
    }
private:
    shared_ptr<column_specification> casted_spec_of(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
        return make_shared<column_specification>(receiver->ks_name, receiver->cf_name,
                make_shared<column_identifier>(to_string(), true), _type->prepare(db, keyspace)->get_type());
    }
public:
    virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) override {
        try {
            auto&& casted_type = _type->prepare(db, keyspace)->get_type();
            if (receiver->type->equals(casted_type)) {
                return assignment_testable::test_result::EXACT_MATCH;
            } else if (receiver->type->is_value_compatible_with(*casted_type)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            } else {
                return assignment_testable::test_result::NOT_ASSIGNABLE;
            }
        } catch (exceptions::invalid_request_exception& e) {
            abort();
        }
    }

    virtual sstring to_string() const override {
        return sprint("(%s)%s", _type, _term);
    }
};

}
