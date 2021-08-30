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
 * Copyright (C) 2015-present ScyllaDB
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

#include "cql3/assignment_testable.hh"
#include "cql3/query_options.hh"
#include "cql3/values.hh"

#include <variant>
#include <vector>

namespace cql3 {

class term;
class terminal;
class prepare_context;

/**
 * A CQL3 term, i.e. a column value with or without bind variables.
 *
 * A Term can be either terminal or non terminal. A term object is one that is typed and is obtained
 * from a raw term (Term.Raw) by poviding the actual receiver to which the term is supposed to be a
 * value of.
 */
class term : public ::enable_shared_from_this<term> {
public:
    virtual ~term() {}

    /**
     * Collects the column specification for the bind variables in this Term.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of this term in.
     */
    virtual void fill_prepare_context(prepare_context& ctx) const = 0;

    /**
     * Bind the values in this term to the values contained in {@code values}.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param options the values to bind markers to.
     * @return the result of binding all the variables of this NonTerminal (or
     * 'this' if the term is terminal).
     */
    virtual ::shared_ptr<terminal> bind(const query_options& options) = 0;

    /**
     * A shorter for bind(values).get().
     * We expose it mainly because for constants it can avoids allocating a temporary
     * object between the bind and the get (note that we still want to be able
     * to separate bind and get for collections).
     */
    virtual cql3::raw_value_view bind_and_get(const query_options& options) = 0;

    /**
     * Whether or not that term contains at least one bind marker.
     *
     * Note that this is slightly different from being or not a NonTerminal,
     * because calls to non pure functions will be NonTerminal (see #5616)
     * even if they don't have bind markers.
     */
    virtual bool contains_bind_marker() const = 0;

    virtual sstring to_string() const {
        return format("term@{:p}", static_cast<const void*>(this));
    }

    friend std::ostream& operator<<(std::ostream& out, const term& t) {
        return out << t.to_string();
    }
};

/**
 * A terminal term, one that can be reduced to a byte buffer directly.
 *
 * This includes most terms that don't have a bind marker (an exception
 * being delayed call for non pure function that are NonTerminal even
 * if they don't have bind markers).
 *
 * This can be only one of:
 *   - a constant value
 *   - a collection value
 *
 * Note that a terminal term will always have been type checked, and thus
 * consumer can (and should) assume so.
 */
class terminal : public term {
    data_type _my_type;

public:
    terminal(data_type my_type) : _my_type(std::move(my_type)) {
    }

    virtual void fill_prepare_context(prepare_context& ctx) const override {
    }

    virtual ::shared_ptr<terminal> bind(const query_options& options) override {
        return static_pointer_cast<terminal>(this->shared_from_this());
    }

    // While some NonTerminal may not have bind markers, no Term can be Terminal
    // with a bind marker
    virtual bool contains_bind_marker() const override {
        return false;
    }

    /**
     * @return the serialized value of this terminal.
     */
    virtual cql3::raw_value get(const query_options& options) = 0;

    virtual cql3::raw_value_view bind_and_get(const query_options& options) override {
        return raw_value_view::make_temporary(get(options));
    }

    virtual sstring to_string() const override = 0;

    data_type get_value_type() const {
        return _my_type;
    }
};

class multi_item_terminal : public terminal {
public:
    multi_item_terminal(data_type my_type) : terminal(std::move(my_type)) {
    }

    virtual std::vector<managed_bytes_opt> copy_elements() const = 0;
};

class collection_terminal {
public:
    virtual ~collection_terminal() {}
    /** Gets the value of the collection when serialized with the given protocol version format */
    virtual managed_bytes get_with_protocol_version(cql_serialization_format sf) = 0;
};

/**
 * A non terminal term, i.e. a term that can only be reduce to a byte buffer
 * at execution time.
 *
 * We have the following type of NonTerminal:
 *   - marker for a constant value
 *   - marker for a collection value (list, set, map)
 *   - a function having bind marker
 *   - a non pure function (even if it doesn't have bind marker - see #5616)
 */
class non_terminal : public term {
public:
    virtual cql3::raw_value_view bind_and_get(const query_options& options) override {
        auto t = bind(options);
        if (t) {
            return cql3::raw_value_view::make_temporary(t->get(options));
        }
        return cql3::raw_value_view::make_null();
    };
};

}
