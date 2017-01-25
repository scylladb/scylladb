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

#include <experimental/optional>
#include "variable_specifications.hh"
#include "cql3/assignment_testable.hh"
#include "cql3/query_options.hh"
#include "cql3/values.hh"
#include "types.hh"

namespace cql3 {

class terminal;

class term;

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
    virtual void collect_marker_specification(::shared_ptr<variable_specifications> bound_names) = 0;

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

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const = 0;

    virtual sstring to_string() const {
        return sprint("term@%p", static_cast<const void*>(this));
    }

    friend std::ostream& operator<<(std::ostream& out, const term& t) {
        return out << t.to_string();
    }

    /**
     * A parsed, non prepared (thus untyped) term.
     *
     * This can be one of:
     *   - a constant
     *   - a collection literal
     *   - a function call
     *   - a marker
     */
    class raw : public virtual assignment_testable {
    public:
        /**
         * This method validates this RawTerm is valid for provided column
         * specification and "prepare" this RawTerm, returning the resulting
         * prepared Term.
         *
         * @param receiver the "column" this RawTerm is supposed to be a value of. Note
         * that the ColumnSpecification may not correspond to a real column in the
         * case this RawTerm describe a list index or a map key, etc...
         * @return the prepared term.
         */
        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) = 0;

        virtual sstring to_string() const = 0;

        virtual sstring assignment_testable_source_context() const override {
            return to_string();
        }

        friend std::ostream& operator<<(std::ostream& os, const raw& r) {
            // FIXME: kill const_cast
            return os << const_cast<raw&>(r).to_string();
        }
    };

    class multi_column_raw : public virtual raw {
    public:
        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, const std::vector<shared_ptr<column_specification>>& receiver) = 0;
    };
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
public:
    virtual void collect_marker_specification(::shared_ptr<variable_specifications> bound_names) {
    }

    virtual ::shared_ptr<terminal> bind(const query_options& options) override {
        return static_pointer_cast<terminal>(this->shared_from_this());
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return false;
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
        return options.make_temporary(get(options));
    }

    virtual sstring to_string() const = 0;
};

class multi_item_terminal : public terminal {
public:
    virtual std::vector<bytes_opt> get_elements() = 0;
};

class collection_terminal {
public:
    virtual ~collection_terminal() {}
    /** Gets the value of the collection when serialized with the given protocol version format */
    virtual bytes get_with_protocol_version(cql_serialization_format sf) = 0;
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
    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return false;
    }

    virtual cql3::raw_value_view bind_and_get(const query_options& options) override {
        auto t = bind(options);
        if (t) {
            return options.make_temporary(t->get(options));
        }
        return cql3::raw_value_view::make_null();
    };
};

}
