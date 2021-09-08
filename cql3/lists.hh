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

#include "cql3/abstract_marker.hh"
#include "to_string.hh"
#include "operation.hh"
#include "utils/chunked_vector.hh"

namespace cql3 {

/**
 * Static helper methods and classes for lists.
 */
class lists {
    lists() = delete;
public:
    static lw_shared_ptr<column_specification> index_spec_of(const column_specification&);
    static lw_shared_ptr<column_specification> value_spec_of(const column_specification&);
    static lw_shared_ptr<column_specification> uuid_index_spec_of(const column_specification&);

    class value : public multi_item_terminal, collection_terminal {
    public:
        utils::chunked_vector<managed_bytes_opt> _elements;
    public:
        explicit value(utils::chunked_vector<managed_bytes_opt> elements, data_type my_type)
            : multi_item_terminal(std::move(my_type)), _elements(std::move(elements)) {
        }
        static value from_serialized(const raw_value_view& v, const list_type_impl& type, cql_serialization_format sf);
        virtual cql3::raw_value get(const query_options& options) override;
        virtual managed_bytes get_with_protocol_version(cql_serialization_format sf) override;
        bool equals(const list_type_impl& lt, const value& v);
        const utils::chunked_vector<managed_bytes_opt>& get_elements() const;
        virtual std::vector<managed_bytes_opt> copy_elements() const override;
        virtual sstring to_string() const override;
        friend class lists;
    };
    /**
     * Basically similar to a Value, but with some non-pure function (that need
     * to be evaluated at execution time) in it.
     *
     * Note: this would also work for a list with bind markers, but we don't support
     * that because 1) it's not excessively useful and 2) we wouldn't have a good
     * column name to return in the ColumnSpecification for those markers (not a
     * blocker per-se but we don't bother due to 1)).
     */
    class delayed_value : public non_terminal {
        std::vector<shared_ptr<term>> _elements;
        data_type _my_type;
    public:
        explicit delayed_value(std::vector<shared_ptr<term>> elements, data_type my_type)
                : _elements(std::move(elements)), _my_type(std::move(my_type)) {
        }
        virtual bool contains_bind_marker() const override;
        virtual void fill_prepare_context(prepare_context& ctx) const override;
        virtual shared_ptr<terminal> bind(const query_options& options) override;

        // Binds the value, but skips all nulls inside the list
        virtual shared_ptr<terminal> bind_ignore_null(const query_options& options);
        const std::vector<shared_ptr<term>>& get_elements() const {
            return _elements;
        }
    };

    /**
     * A marker for List values and IN relations
     */
    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        { }
        virtual ::shared_ptr<terminal> bind(const query_options& options) override;
    };

public:
    class setter : public operation {
    public:
        setter(const column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, ::shared_ptr<terminal> value);
    };

    class setter_by_index : public operation {
    protected:
        shared_ptr<term> _idx;
    public:
        setter_by_index(const column_definition& column, shared_ptr<term> idx, shared_ptr<term> t)
            : operation(column, std::move(t)), _idx(std::move(idx)) {
        }
        virtual bool requires_read() const override;
        virtual void fill_prepare_context(prepare_context& ctx) const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class setter_by_uuid : public setter_by_index {
    public:
        setter_by_uuid(const column_definition& column, shared_ptr<term> idx, shared_ptr<term> t)
            : setter_by_index(column, std::move(idx), std::move(t)) {
        }
        virtual bool requires_read() const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class appender : public operation {
    public:
        using operation::operation;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    static void do_append(shared_ptr<term> value,
            mutation& m,
            const clustering_key_prefix& prefix,
            const column_definition& column,
            const update_parameters& params);

    class prepender : public operation {
    public:
        using operation::operation;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class discarder : public operation {
    public:
        discarder(const column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }
        virtual bool requires_read() const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class discarder_by_index : public operation {
    public:
        discarder_by_index(const column_definition& column, shared_ptr<term> idx)
                : operation(column, std::move(idx)) {
        }
        virtual bool requires_read() const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };
};

}
