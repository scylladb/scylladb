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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
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
#include "cql3/update_parameters.hh"
#include "cql3/operation.hh"
#include "cql3/term.hh"
#include "core/shared_ptr.hh"

namespace cql3 {

/**
 * Static helper methods and classes for constants.
 */
class constants {
public:
#if 0
    private static final Logger logger = LoggerFactory.getLogger(Constants.class);
#endif
public:
    enum class type {
        STRING, INTEGER, UUID, FLOAT, BOOLEAN, HEX
    };

    /**
    * A constant value, i.e. a ByteBuffer.
    */
    class value : public terminal {
    public:
        bytes_opt _bytes;
        value(bytes_opt bytes_) : _bytes(std::move(bytes_)) {}
        virtual bytes_opt get(const query_options& options) override { return _bytes; }
        virtual bytes_view_opt bind_and_get(const query_options& options) override { return as_bytes_view_opt(_bytes); }
        virtual sstring to_string() const override { return to_hex(*_bytes); }
    };

    class null_literal final : public term::raw {
    private:
        class null_value final : public value {
        public:
            null_value() : value({}) {}
            virtual ::shared_ptr<terminal> bind(const query_options& options) override { return {}; }
            virtual sstring to_string() const override { return "null"; }
        };
        static thread_local const ::shared_ptr<terminal> NULL_VALUE;
    public:
        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override {
            if (!is_assignable(test_assignment(db, keyspace, receiver))) {
                throw exceptions::invalid_request_exception("Invalid null value for counter increment/decrement");
            }
            return NULL_VALUE;
        }

        virtual assignment_testable::test_result test_assignment(database& db,
            const sstring& keyspace,
            ::shared_ptr<column_specification> receiver) override {
                return receiver->type->is_counter()
                    ? assignment_testable::test_result::NOT_ASSIGNABLE
                    : assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        }

        virtual sstring to_string() const override {
            return "null";
        }
    };

    static thread_local const ::shared_ptr<term::raw> NULL_LITERAL;

    class literal : public term::raw {
    private:
        const type _type;
        const sstring _text;
    public:
        literal(type type_, sstring text)
            : _type{type_}
            , _text{text}
        { }

        static ::shared_ptr<literal> string(sstring text) {
            // This is a workaround for antlr3 not distinguishing between
            // calling in lexer setText() with an empty string and not calling
            // setText() at all.
            if (text.size() == 1 && text[0] == -1) {
                text.reset();
            }
            return ::make_shared<literal>(type::STRING, text);
        }

        static ::shared_ptr<literal> integer(sstring text) {
            return ::make_shared<literal>(type::INTEGER, text);
        }

        static ::shared_ptr<literal> floating_point(sstring text) {
            return ::make_shared<literal>(type::FLOAT, text);
        }

        static ::shared_ptr<literal> uuid(sstring text) {
            return ::make_shared<literal>(type::UUID, text);
        }

        static ::shared_ptr<literal> bool_(sstring text) {
            return ::make_shared<literal>(type::BOOLEAN, text);
        }

        static ::shared_ptr<literal> hex(sstring text) {
            return ::make_shared<literal>(type::HEX, text);
        }

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver);
    private:
        bytes parsed_value(data_type validator);
    public:
        const sstring& get_raw_text() {
            return _text;
        }

        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver);

        virtual sstring to_string() const override {
            return _type == type::STRING ? sstring(sprint("'%s'", _text)) : _text;
        }
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, ::shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        {
            assert(!_receiver->type->is_collection());
        }

        virtual bytes_view_opt bind_and_get(const query_options& options) override {
            try {
                auto value = options.get_value_at(_bind_index);
                if (value) {
                    _receiver->type->validate(*value);
                    return *value;
                }
                return std::experimental::nullopt;
            } catch (const marshal_exception& e) {
                throw exceptions::invalid_request_exception(e.what());
            }
        }

        virtual ::shared_ptr<terminal> bind(const query_options& options) override {
            auto bytes = bind_and_get(options);
            if (!bytes) {
                return ::shared_ptr<terminal>{};
            }
            return ::make_shared<constants::value>(std::move(to_bytes_opt(*bytes)));
        }
    };

    class setter : public operation {
    public:
        using operation::operation;

        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override {
            auto value = _t->bind_and_get(params._options);
            auto cell = value ? make_cell(*value, params) : make_dead_cell(params);
            m.set_cell(prefix, column, std::move(cell));
        }
    };

#if 0
    public static class Adder extends Operation
    {
        public Adder(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            ByteBuffer bytes = t.bindAndGet(params.options);
            if (bytes == null)
                throw new InvalidRequestException("Invalid null value for counter increment");
            long increment = ByteBufferUtil.toLong(bytes);
            CellName cname = cf.getComparator().create(prefix, column);
            cf.addColumn(params.makeCounter(cname, increment));
        }
    }

    public static class Substracter extends Operation
    {
        public Substracter(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            ByteBuffer bytes = t.bindAndGet(params.options);
            if (bytes == null)
                throw new InvalidRequestException("Invalid null value for counter increment");

            long increment = ByteBufferUtil.toLong(bytes);
            if (increment == Long.MIN_VALUE)
                throw new InvalidRequestException("The negation of " + increment + " overflows supported counter precision (signed 8 bytes integer)");

            CellName cname = cf.getComparator().create(prefix, column);
            cf.addColumn(params.makeCounter(cname, -increment));
        }
    }
#endif

    class deleter : public operation {
    public:
        deleter(const column_definition& column)
            : operation(column, {})
        { }

        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override;
    };
};

std::ostream& operator<<(std::ostream&out, constants::type t);

}
