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

#include "cql3/abstract_marker.hh"
#include "to_string.hh"
#include "utils/UUID_gen.hh"
#include "operation.hh"

namespace cql3 {

/**
 * Static helper methods and classes for lists.
 */
class lists {
    lists() = delete;
public:
    static shared_ptr<column_specification> index_spec_of(shared_ptr<column_specification> column);
    static shared_ptr<column_specification> value_spec_of(shared_ptr<column_specification> column);
    static shared_ptr<column_specification> uuid_index_spec_of(shared_ptr<column_specification>);

    class literal : public term::raw {
        const std::vector<shared_ptr<term::raw>> _elements;
    public:
        explicit literal(std::vector<shared_ptr<term::raw>> elements)
            : _elements(std::move(elements)) {
        }
        shared_ptr<term> prepare(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver);
    private:
        void validate_assignable_to(database& db, const sstring keyspace, shared_ptr<column_specification> receiver);
    public:
        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) override;
        virtual sstring to_string() const override;
    };

    class value : public multi_item_terminal, collection_terminal {
    public:
        std::vector<bytes_opt> _elements;
    public:
        explicit value(std::vector<bytes_opt> elements)
            : _elements(std::move(elements)) {
        }
        static value from_serialized(const fragmented_temporary_buffer::view& v, list_type type, cql_serialization_format sf);
        virtual cql3::raw_value get(const query_options& options) override;
        virtual bytes get_with_protocol_version(cql_serialization_format sf) override;
        bool equals(shared_ptr<list_type_impl> lt, const value& v);
        virtual std::vector<bytes_opt> get_elements() override;
        virtual sstring to_string() const;
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
    public:
        explicit delayed_value(std::vector<shared_ptr<term>> elements)
                : _elements(std::move(elements)) {
        }
        virtual bool contains_bind_marker() const override;
        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names);
        virtual shared_ptr<terminal> bind(const query_options& options) override;
    };

    /**
     * A marker for List values and IN relations
     */
    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, ::shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        { }
        virtual ::shared_ptr<terminal> bind(const query_options& options) override;
    };

    /*
     * For prepend, we need to be able to generate unique but decreasing time
     * UUID, which is a bit challenging. To do that, given a time in milliseconds,
     * we adds a number representing the 100-nanoseconds precision and make sure
     * that within the same millisecond, that number is always decreasing. We
     * do rely on the fact that the user will only provide decreasing
     * milliseconds timestamp for that purpose.
     */
private:
    class precision_time {
    public:
        // Our reference time (1 jan 2010, 00:00:00) in milliseconds.
        static constexpr db_clock::time_point REFERENCE_TIME{std::chrono::milliseconds(1262304000000)};
    private:
        static thread_local precision_time _last;
    public:
        db_clock::time_point millis;
        int32_t nanos;

        static precision_time get_next(db_clock::time_point millis);
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
        virtual bool requires_read() override;
        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names);
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class setter_by_uuid : public setter_by_index {
    public:
        setter_by_uuid(const column_definition& column, shared_ptr<term> idx, shared_ptr<term> t)
            : setter_by_index(column, std::move(idx), std::move(t)) {
        }
        virtual bool requires_read() override;
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
        virtual bool requires_read() override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class discarder_by_index : public operation {
    public:
        discarder_by_index(const column_definition& column, shared_ptr<term> idx)
                : operation(column, std::move(idx)) {
        }
        virtual bool requires_read() override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params);
    };
};

}
