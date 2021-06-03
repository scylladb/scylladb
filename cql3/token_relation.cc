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

#include "restrictions/token_restriction.hh"
#include "token_relation.hh"
#include "column_identifier.hh"
#include "term.hh"
#include "to_string.hh"

std::vector<const column_definition*> cql3::token_relation::get_column_definitions(const schema& s) {
    std::vector<const column_definition*> res;
    std::transform(_entities.begin(), _entities.end(), std::back_inserter(res),
            [this, &s](const auto& cr) {
                return &this->to_column_definition(s, *cr);
            });
    return res;
}

std::vector<lw_shared_ptr<cql3::column_specification>> cql3::token_relation::to_receivers(
        const schema& schema,
        const std::vector<const column_definition*>& column_defs) const {
    auto pk = schema.partition_key_columns();
    if (!std::equal(column_defs.begin(), column_defs.end(), pk.begin(),
            pk.end(), [](auto* c1, auto& c2) {
                return c1 == &c2; // same, not "equal".
        })) {
#if 0
        checkTrue(columnDefs.containsAll(cfm.partitionKeyColumns()),
                "The token() function must be applied to all partition key components or none of them");

        checkContainsNoDuplicates(columnDefs, "The token() function contains duplicate partition key components");

        checkContainsOnly(columnDefs, cfm.partitionKeyColumns(), "The token() function must contains only partition key components");
#endif
        throw exceptions::invalid_request_exception(
                format("The token function arguments must be in the partition key order: {}",
                        std::to_string(column_defs)));
    }
    //auto* c = column_defs.front();
    return {make_lw_shared<column_specification>(schema.ks_name(), schema.cf_name(),
                ::make_shared<column_identifier>("partition key token", true),
                dht::token::get_token_validator())};
}

::shared_ptr<cql3::restrictions::restriction> cql3::token_relation::new_EQ_restriction(
        database& db, schema_ptr schema,
        variable_specifications& bound_names) {
    auto column_defs = get_column_definitions(*schema);
    auto term = to_term(to_receivers(*schema, column_defs), *_value, db,
            schema->ks_name(), bound_names);
    auto r = ::make_shared<restrictions::token_restriction>(column_defs);
    using namespace expr;
    r->expression = binary_operator{token{}, oper_t::EQ, std::move(term)};
    return r;
}

::shared_ptr<cql3::restrictions::restriction> cql3::token_relation::new_IN_restriction(
        database& db, schema_ptr schema,
        variable_specifications& bound_names) {
    throw exceptions::invalid_request_exception(
            format("{} cannot be used with the token function",
                    get_operator()));
}

::shared_ptr<cql3::restrictions::restriction> cql3::token_relation::new_slice_restriction(
        database& db, schema_ptr schema,
        variable_specifications& bound_names,
        statements::bound bound,
        bool inclusive) {
    auto column_defs = get_column_definitions(*schema);
    auto term = to_term(to_receivers(*schema, column_defs), *_value, db,
            schema->ks_name(), bound_names);
    auto r = ::make_shared<restrictions::token_restriction>(column_defs);
    using namespace expr;
    r->expression = binary_operator{token{}, pick_operator(bound, inclusive), std::move(term)};
    return r;
}

::shared_ptr<cql3::restrictions::restriction> cql3::token_relation::new_contains_restriction(
        database& db, schema_ptr schema,
        variable_specifications& bound_names, bool isKey) {
    throw exceptions::invalid_request_exception(
            format("{} cannot be used with the token function",
                    get_operator()));
}

::shared_ptr<cql3::restrictions::restriction> cql3::token_relation::new_LIKE_restriction(
        database&, schema_ptr, variable_specifications&) {
    throw exceptions::invalid_request_exception("LIKE cannot be used with the token function");
}

sstring cql3::token_relation::to_string() const {
    return format("token({}) {} {}", join(", ", _entities), get_operator(), _value);
}

::shared_ptr<cql3::term> cql3::token_relation::to_term(
        const std::vector<lw_shared_ptr<column_specification>>& receivers,
        const term::raw& raw, database& db, const sstring& keyspace,
        variable_specifications& bound_names) const {
    auto term = raw.prepare(db, keyspace, receivers.front());
    term->collect_marker_specification(bound_names);
    return term;
}

::shared_ptr<cql3::relation> cql3::token_relation::maybe_rename_identifier(const cql3::column_identifier::raw& from, cql3::column_identifier::raw to) {
    auto new_entities = boost::copy_range<decltype(_entities)>(_entities | boost::adaptors::transformed([&] (auto&& entity) {
        return *entity == from ? ::make_shared<column_identifier::raw>(to) : entity;
    }));
    return ::make_shared<token_relation>(std::move(new_entities), _relation_type, _value);
}
