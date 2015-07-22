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

#pragma once

#include "cql3/abstract_marker.hh"
#include "cql3/term.hh"
#include "operation.hh"
#include "update_parameters.hh"
#include "constants.hh"

namespace cql3 {

/**
 * Static helper methods and classes for maps.
 */
class maps {
private:
    maps() = delete;
public:
    static shared_ptr<column_specification> key_spec_of(column_specification& column);
    static shared_ptr<column_specification> value_spec_of(column_specification& column);

    class literal : public term::raw {
    public:
        const std::vector<std::pair<::shared_ptr<term::raw>, ::shared_ptr<term::raw>>> entries;

        literal(const std::vector<std::pair<::shared_ptr<term::raw>, ::shared_ptr<term::raw>>>& entries_)
            : entries{entries_}
        { }
        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override;
    private:
        void validate_assignable_to(database& db, const sstring& keyspace, column_specification& receiver);
    public:
        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override;
        virtual sstring to_string() const override;
    };

    class value : public terminal, collection_terminal {
    public:
        std::map<bytes, bytes, serialized_compare> map;

        value(std::map<bytes, bytes, serialized_compare> map)
            : map(std::move(map)) {
        }
        static value from_serialized(bytes_view value, map_type type, serialization_format sf);
        virtual bytes_opt get(const query_options& options) override;
        virtual bytes get_with_protocol_version(serialization_format sf);
        bool equals(map_type mt, const value& v);
        virtual sstring to_string() const;
    };

    // See Lists.DelayedValue
    class delayed_value : public non_terminal {
        serialized_compare _comparator;
        std::unordered_map<shared_ptr<term>, shared_ptr<term>> _elements;
    public:
        delayed_value(serialized_compare comparator,
                      std::unordered_map<shared_ptr<term>, shared_ptr<term>> elements)
                : _comparator(std::move(comparator)), _elements(std::move(elements)) {
        }
        virtual bool contains_bind_marker() const override;
        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override;
        shared_ptr<terminal> bind(const query_options& options);
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, ::shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        { }
        virtual ::shared_ptr<terminal> bind(const query_options& options) override;
    };

    class setter : public operation {
    public:
        setter(const column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }

        virtual void execute(mutation& m, const exploded_clustering_prefix& row_key, const update_parameters& params) override;
    };

    class setter_by_key : public operation {
        const shared_ptr<term> _k;
    public:
        setter_by_key(const column_definition& column, shared_ptr<term> k, shared_ptr<term> t)
            : operation(column, std::move(t)), _k(std::move(k)) {
        }
        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override;
        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override;
    };

    class putter : public operation {
    public:
        putter(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override;
    };

    static void do_put(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params,
            shared_ptr<term> t, const column_definition& column);

    class discarder_by_key : public operation {
    public:
        discarder_by_key(const column_definition& column, shared_ptr<term> k)
                : operation(column, std::move(k)) {
        }
        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override;
    };
};

}
