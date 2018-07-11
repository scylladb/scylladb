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

#include "core/shared_ptr.hh"
#include "exceptions/exceptions.hh"
#include "database_fwd.hh"
#include "term.hh"
#include "update_parameters.hh"

#include <experimental/optional>

namespace cql3 {

class update_parameters;

/**
 * An UPDATE or DELETE operation.
 *
 * For UPDATE this includes:
 *   - setting a constant
 *   - counter operations
 *   - collections operations
 * and for DELETE:
 *   - deleting a column
 *   - deleting an element of collection column
 *
 * Fine grained operation are obtained from their raw counterpart (Operation.Raw, which
 * correspond to a parsed, non-checked operation) by provided the receiver for the operation.
 */
class operation {
public:
    // the column the operation applies to
    // We can hold a reference because all operations have life bound to their statements and
    // statements pin the schema.
    const column_definition& column;

protected:
    // Term involved in the operation. In theory this should not be here since some operation
    // may require none of more than one term, but most need 1 so it simplify things a bit.
    const ::shared_ptr<term> _t;

public:
    operation(const column_definition& column_, ::shared_ptr<term> t)
        : column{column_}
        , _t{t}
    { }

    virtual ~operation() {}

    static atomic_cell make_dead_cell(const update_parameters& params) {
        return params.make_dead_cell();
    }

    static atomic_cell make_cell(const abstract_type& type, bytes_view value, const update_parameters& params) {
        return params.make_cell(type, fragmented_temporary_buffer::view(value));
    }

    static atomic_cell make_cell(const abstract_type& type, const fragmented_temporary_buffer::view& value, const update_parameters& params) {
        return params.make_cell(type, value);
    }

    static atomic_cell make_counter_update_cell(int64_t delta, const update_parameters& params) {
        return params.make_counter_update_cell(delta);
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const {
        return _t && _t->uses_function(ks_name, function_name);
    }

    virtual bool is_raw_counter_shard_write() const {
        return false;
    }

    /**
    * @return whether the operation requires a read of the previous value to be executed
    * (only lists setterByIdx, discard and discardByIdx requires that).
    */
    virtual bool requires_read() {
        return false;
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param bound_names the list of column specification where to collect the
     * bind variables of this term in.
     */
    virtual void collect_marker_specification(::shared_ptr<variable_specifications> bound_names) {
        if (_t) {
            _t->collect_marker_specification(bound_names);
        }
    }

    /**
     * Execute the operation.
     */
    virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) = 0;

    /**
     * A parsed raw UPDATE operation.
     *
     * This can be one of:
     *   - Setting a value: c = v
     *   - Setting an element of a collection: c[x] = v
     *   - An addition/subtraction to a variable: c = c +/- v (where v can be a collection literal)
     *   - An prepend operation: c = v + c
     */
    class raw_update {
    public:
        virtual ~raw_update() {}

        /**
         * This method validates the operation (i.e. validate it is well typed)
         * based on the specification of the receiver of the operation.
         *
         * It returns an Operation which can be though as post-preparation well-typed
         * Operation.
         *
         * @param receiver the "column" this operation applies to. Note that
         * contrarly to the method of same name in Term.Raw, the receiver should always
         * be a true column.
         * @return the prepared update operation.
         */
        virtual ::shared_ptr<operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver) = 0;

        /**
         * @return whether this operation can be applied alongside the {@code
         * other} update (in the same UPDATE statement for the same column).
         */
        virtual bool is_compatible_with(::shared_ptr<raw_update> other) = 0;
    };

    /**
     * A parsed raw DELETE operation.
     *
     * This can be one of:
     *   - Deleting a column
     *   - Deleting an element of a collection
     */
    class raw_deletion {
    public:
        ~raw_deletion() {}

        /**
         * The name of the column affected by this delete operation.
         */
        virtual ::shared_ptr<column_identifier::raw> affected_column() = 0;

        /**
         * This method validates the operation (i.e. validate it is well typed)
         * based on the specification of the column affected by the operation (i.e the
         * one returned by affectedColumn()).
         *
         * It returns an Operation which can be though as post-preparation well-typed
         * Operation.
         *
         * @param receiver the "column" this operation applies to.
         * @return the prepared delete operation.
         */
        virtual ::shared_ptr<operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver) = 0;
    };

    class set_value;
    class set_counter_value_from_tuple_list;

    class set_element : public raw_update {
        const shared_ptr<term::raw> _selector;
        const shared_ptr<term::raw> _value;
        const bool _by_uuid;
    private:
        sstring to_string(const column_definition& receiver) const;
    public:
        set_element(shared_ptr<term::raw> selector, shared_ptr<term::raw> value, bool by_uuid = false)
            : _selector(std::move(selector)), _value(std::move(value)), _by_uuid(by_uuid) {
        }

        virtual shared_ptr<operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver);

        virtual bool is_compatible_with(shared_ptr<raw_update> other) override;
    };

    class addition : public raw_update {
        const shared_ptr<term::raw> _value;
    private:
        sstring to_string(const column_definition& receiver) const;
    public:
        addition(shared_ptr<term::raw> value)
                : _value(value) {
        }

        virtual shared_ptr<operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver) override;

        virtual bool is_compatible_with(shared_ptr<raw_update> other) override;
    };

    class subtraction : public raw_update {
        const shared_ptr<term::raw> _value;
    private:
        sstring to_string(const column_definition& receiver) const;
    public:
        subtraction(shared_ptr<term::raw> value)
                : _value(value) {
        }

        virtual shared_ptr<operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver) override;

        virtual bool is_compatible_with(shared_ptr<raw_update> other) override;
    };

    class prepend : public raw_update {
        shared_ptr<term::raw> _value;
    private:
        sstring to_string(const column_definition& receiver) const;
    public:
        prepend(shared_ptr<term::raw> value)
                : _value(std::move(value)) {
        }

        virtual shared_ptr<operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver) override;

        virtual bool is_compatible_with(shared_ptr<raw_update> other) override;
    };

    class column_deletion;

    class element_deletion : public raw_deletion {
        shared_ptr<column_identifier::raw> _id;
        shared_ptr<term::raw> _element;
    public:
        element_deletion(shared_ptr<column_identifier::raw> id, shared_ptr<term::raw> element)
                : _id(std::move(id)), _element(std::move(element)) {
        }
        virtual shared_ptr<column_identifier::raw> affected_column() override;
        virtual shared_ptr<operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver) override;
    };
};

}
