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

#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "cql3/single_column_relation.hh"
#include "validation.hh"
#include "core/shared_ptr.hh"
#include "query-result-reader.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/filtered.hpp>

namespace cql3 {

namespace statements {

thread_local const ::shared_ptr<column_identifier> modification_statement::CAS_RESULT_COLUMN = ::make_shared<column_identifier>("[applied]", false);

std::ostream&
operator<<(std::ostream& out, modification_statement::statement_type t) {
    switch (t) {
        case modification_statement::statement_type::UPDATE:
            out << "UPDATE";
            break;
        case modification_statement::statement_type::INSERT:
            out << "INSERT";
            break;
        case modification_statement::statement_type::DELETE:
            out << "DELETE";
            break;
    }
    return out;
}

modification_statement::modification_statement(statement_type type_, uint32_t bound_terms, schema_ptr schema_, std::unique_ptr<attributes> attrs_)
    : type{type_}
    , _bound_terms{bound_terms}
    , s{schema_}
    , attrs{std::move(attrs_)}
    , _column_operations{}
{ }

bool modification_statement::uses_function(const sstring& ks_name, const sstring& function_name) const {
    if (attrs->uses_function(ks_name, function_name)) {
        return true;
    }
    for (auto&& e : _processed_keys) {
        auto r = e.second;
        if (r && r->uses_function(ks_name, function_name)) {
            return true;
        }
    }
    for (auto&& operation : _column_operations) {
        if (operation && operation->uses_function(ks_name, function_name)) {
            return true;
        }
    }
    for (auto&& condition : _column_conditions) {
        if (condition && condition->uses_function(ks_name, function_name)) {
            return true;
        }
    }
    for (auto&& condition : _static_conditions) {
        if (condition && condition->uses_function(ks_name, function_name)) {
            return true;
        }
    }
    return false;
}

uint32_t modification_statement::get_bound_terms() {
    return _bound_terms;
}

sstring modification_statement::keyspace() const {
    return s->ks_name();
}

sstring modification_statement::column_family() const {
    return s->cf_name();
}

bool modification_statement::is_counter() const {
    return s->is_counter();
}

int64_t modification_statement::get_timestamp(int64_t now, const query_options& options) const {
    return attrs->get_timestamp(now, options);
}

bool modification_statement::is_timestamp_set() const {
    return attrs->is_timestamp_set();
}

gc_clock::duration modification_statement::get_time_to_live(const query_options& options) const {
    return gc_clock::duration(attrs->get_time_to_live(options));
}

future<> modification_statement::check_access(const service::client_state& state) {
    auto f = state.has_column_family_access(keyspace(), column_family(), auth::permission::MODIFY);
    if (has_conditions()) {
        f = f.then([this, &state] {
           return state.has_column_family_access(keyspace(), column_family(), auth::permission::SELECT);
        });
    }
    return f;
}

future<std::vector<mutation>>
modification_statement::get_mutations(distributed<service::storage_proxy>& proxy, const query_options& options, bool local, int64_t now) {
    auto keys = make_lw_shared(build_partition_keys(options));
    auto prefix = make_lw_shared(create_exploded_clustering_prefix(options));
    return make_update_parameters(proxy, keys, prefix, options, local, now).then(
            [this, keys, prefix, now] (auto params_ptr) {
                std::vector<mutation> mutations;
                mutations.reserve(keys->size());
                for (auto key : *keys) {
                    mutations.emplace_back(std::move(key), s);
                    auto& m = mutations.back();
                    this->add_update_for_key(m, *prefix, *params_ptr);
                }
                return make_ready_future<decltype(mutations)>(std::move(mutations));
            });
}

future<std::unique_ptr<update_parameters>>
modification_statement::make_update_parameters(
        distributed<service::storage_proxy>& proxy,
        lw_shared_ptr<std::vector<partition_key>> keys,
        lw_shared_ptr<exploded_clustering_prefix> prefix,
        const query_options& options,
        bool local,
        int64_t now) {
    return read_required_rows(proxy, std::move(keys), std::move(prefix), local, options.get_consistency()).then(
            [this, &options, now] (auto rows) {
                return make_ready_future<std::unique_ptr<update_parameters>>(
                        std::make_unique<update_parameters>(s, options,
                                this->get_timestamp(now, options),
                                this->get_time_to_live(options),
                                std::move(rows)));
            });
}


// Implements ResultVisitor concept from query.hh
class prefetch_data_builder {
    update_parameters::prefetch_data& _data;
    const query::partition_slice& _ps;
    schema_ptr _schema;
    std::experimental::optional<partition_key> _pkey;
private:
    void add_cell(update_parameters::prefetch_data::row& cells, const column_definition& def, const std::experimental::optional<bytes_view>& cell) {
        if (cell) {
            auto ctype = static_pointer_cast<const collection_type_impl>(def.type);
            if (!ctype->is_multi_cell()) {
                throw std::logic_error(sprint("cannot prefetch frozen collection: %s", def.name_as_text()));
            }
            auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
            update_parameters::prefetch_data::cell_list list;
            // FIXME: Iterate over a range instead of fully exploded collection
            auto dv = map_type->deserialize(*cell);
            for (auto&& el : value_cast<map_type_impl::native_type>(dv)) {
                list.emplace_back(update_parameters::prefetch_data::cell{el.first.serialize(), el.second.serialize()});
            }
            cells.emplace(def.id, std::move(list));
        }
    };
public:
    prefetch_data_builder(schema_ptr s, update_parameters::prefetch_data& data, const query::partition_slice& ps)
        : _data(data)
        , _ps(ps)
        , _schema(std::move(s))
    { }

    void accept_new_partition(const partition_key& key, uint32_t row_count) {
        _pkey = key;
    }

    void accept_new_partition(uint32_t row_count) {
        assert(0);
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row,
                    const query::result_row_view& row) {
        update_parameters::prefetch_data::row cells;

        auto row_iterator = row.iterator();
        for (auto&& id : _ps.regular_columns) {
            add_cell(cells, _schema->regular_column_at(id), row_iterator.next_collection_cell());
        }

        _data.rows.emplace(std::make_pair(*_pkey, key), std::move(cells));
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        assert(0);
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        update_parameters::prefetch_data::row cells;

        auto static_row_iterator = static_row.iterator();
        for (auto&& id : _ps.static_columns) {
            add_cell(cells, _schema->static_column_at(id), static_row_iterator.next_collection_cell());
        }

        _data.rows.emplace(std::make_pair(*_pkey, std::experimental::nullopt), std::move(cells));
    }
};

future<update_parameters::prefetched_rows_type>
modification_statement::read_required_rows(
        distributed<service::storage_proxy>& proxy,
        lw_shared_ptr<std::vector<partition_key>> keys,
        lw_shared_ptr<exploded_clustering_prefix> prefix,
        bool local,
        db::consistency_level cl) {
    if (!requires_read()) {
        return make_ready_future<update_parameters::prefetched_rows_type>(
                update_parameters::prefetched_rows_type{});
    }
    try {
        validate_for_read(keyspace(), cl);
    } catch (exceptions::invalid_request_exception& e) {
        throw exceptions::invalid_request_exception(sprint("Write operation require a read but consistency %s is not supported on reads", cl));
    }

    static auto is_collection = [] (const column_definition& def) {
        return def.type->is_collection();
    };

    // FIXME: we read all collection columns, but could be enhanced just to read the list(s) being RMWed
    std::vector<column_id> static_cols;
    boost::range::push_back(static_cols, s->static_columns()
        | boost::adaptors::filtered(is_collection) | boost::adaptors::transformed([] (auto&& col) { return col.id; }));
    std::vector<column_id> regular_cols;
    boost::range::push_back(regular_cols, s->regular_columns()
        | boost::adaptors::filtered(is_collection) | boost::adaptors::transformed([] (auto&& col) { return col.id; }));
    query::partition_slice ps(
            {query::clustering_range(clustering_key_prefix::from_clustering_prefix(*s, *prefix))},
            std::move(static_cols),
            std::move(regular_cols),
            query::partition_slice::option_set::of<
                query::partition_slice::option::send_partition_key,
                query::partition_slice::option::send_clustering_key,
                query::partition_slice::option::collections_as_maps>());
    std::vector<query::partition_range> pr;
    for (auto&& pk : *keys) {
        pr.emplace_back(dht::global_partitioner().decorate_key(*s, pk));
    }
    query::read_command cmd(s->id(), s->version(), ps, std::numeric_limits<uint32_t>::max());
    // FIXME: ignoring "local"
    return proxy.local().query(s, make_lw_shared(std::move(cmd)), std::move(pr), cl).then([this, ps] (auto result) {
        return query::result_view::do_with(*result, [&] (query::result_view v) {
            auto prefetched_rows = update_parameters::prefetched_rows_type({update_parameters::prefetch_data(s)});
            v.consume(ps, prefetch_data_builder(s, prefetched_rows.value(), ps));
            return prefetched_rows;
        });
    });
}

const column_definition*
modification_statement::get_first_empty_key() {
    for (auto& def : s->clustering_key_columns()) {
        if (_processed_keys.find(&def) == _processed_keys.end()) {
            return &def;
        }
    }
    return {};
}

exploded_clustering_prefix
modification_statement::create_exploded_clustering_prefix_internal(const query_options& options) {
    std::vector<bytes> components;
    const column_definition* first_empty_key = nullptr;

    for (auto& def : s->clustering_key_columns()) {
        auto i = _processed_keys.find(&def);
        if (i == _processed_keys.end()) {
            first_empty_key = &def;
            // Tomek: Origin had "&& s->comparator->is_composite()" in the condition below.
            // Comparator is a thrift concept, not CQL concept, and we want to avoid
            // using thrift concepts here. I think it's safe to drop this here because the only
            // case in which we would get a non-composite comparator here would be if the cell
            // name type is SimpleSparse, which means:
            //   (a) CQL compact table without clustering columns
            //   (b) thrift static CF with non-composite comparator
            // Those tables don't have clustering columns so we wouldn't reach this code, thus
            // the check seems redundant.
            if (require_full_clustering_key() && !s->is_dense()) {
                throw exceptions::invalid_request_exception(sprint("Missing mandatory PRIMARY KEY part %s", def.name_as_text()));
            }
        } else if (first_empty_key) {
            throw exceptions::invalid_request_exception(sprint("Missing PRIMARY KEY part %s since %s is set", first_empty_key->name_as_text(), def.name_as_text()));
        } else {
            auto values = i->second->values(options);
            assert(values.size() == 1);
            auto val = values[0];
            if (!val) {
                throw exceptions::invalid_request_exception(sprint("Invalid null value for clustering key part %s", def.name_as_text()));
            }
            components.push_back(*val);
        }
    }
    return exploded_clustering_prefix(std::move(components));
}

exploded_clustering_prefix
modification_statement::create_exploded_clustering_prefix(const query_options& options) {
    // If the only updated/deleted columns are static, then we don't need clustering columns.
    // And in fact, unless it is an INSERT, we reject if clustering columns are provided as that
    // suggest something unintended. For instance, given:
    //   CREATE TABLE t (k int, v int, s int static, PRIMARY KEY (k, v))
    // it can make sense to do:
    //   INSERT INTO t(k, v, s) VALUES (0, 1, 2)
    // but both
    //   UPDATE t SET s = 3 WHERE k = 0 AND v = 1
    //   DELETE v FROM t WHERE k = 0 AND v = 1
    // sounds like you don't really understand what your are doing.
    if (_sets_static_columns && !_sets_regular_columns) {
        // If we set no non-static columns, then it's fine not to have clustering columns
        if (_has_no_clustering_columns) {
            return {};
        }

        // If we do have clustering columns however, then either it's an INSERT and the query is valid
        // but we still need to build a proper prefix, or it's not an INSERT, and then we want to reject
        // (see above)
        if (type != statement_type::INSERT) {
            for (auto& def : s->clustering_key_columns()) {
                if (_processed_keys.count(&def)) {
                    throw exceptions::invalid_request_exception(sprint(
                            "Invalid restriction on clustering column %s since the %s statement modifies only static columns",
                            def.name_as_text(), type));
                }
            }

            // we should get there as it contradicts _has_no_clustering_columns == false
            throw std::logic_error("contradicts _has_no_clustering_columns == false");
        }
    }

    return create_exploded_clustering_prefix_internal(options);
}

std::vector<partition_key>
modification_statement::build_partition_keys(const query_options& options) {
    std::vector<partition_key> result;
    std::vector<bytes> components;

    auto remaining = s->partition_key_size();

    for (auto& def : s->partition_key_columns()) {
        auto i = _processed_keys.find(&def);
        if (i == _processed_keys.end()) {
            throw exceptions::invalid_request_exception(sprint("Missing mandatory PRIMARY KEY part %s", def.name_as_text()));
        }

        auto values = i->second->values(options);

        if (remaining == 1) {
            if (values.size() == 1) {
                auto val = values[0];
                if (!val) {
                    throw exceptions::invalid_request_exception(sprint("Invalid null value for partition key part %s", def.name_as_text()));
                }
                components.push_back(*val);
                auto key = partition_key::from_exploded(*s, components);
                validation::validate_cql_key(s, key);
                result.emplace_back(std::move(key));
            } else {
                for (auto&& val : values) {
                    if (!val) {
                        throw exceptions::invalid_request_exception(sprint("Invalid null value for partition key part %s", def.name_as_text()));
                    }
                    std::vector<bytes> full_components;
                    full_components.reserve(components.size() + 1);
                    auto i = std::copy(components.begin(), components.end(), std::back_inserter(full_components));
                    *i = *val;
                    auto key = partition_key::from_exploded(*s, full_components);
                    validation::validate_cql_key(s, key);
                    result.emplace_back(std::move(key));
                }
            }
        } else {
            if (values.size() != 1) {
                throw exceptions::invalid_request_exception("IN is only supported on the last column of the partition key");
            }
            auto val = values[0];
            if (!val) {
                throw exceptions::invalid_request_exception(sprint("Invalid null value for partition key part %s", def.name_as_text()));
            }
            components.push_back(*val);
        }

        remaining--;
    }
    return result;
}

future<::shared_ptr<transport::messages::result_message>>
modification_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& qs, const query_options& options) {
    if (has_conditions() && options.get_protocol_version() == 1) {
        throw exceptions::invalid_request_exception("Conditional updates are not supported by the protocol version in use. You need to upgrade to a driver using the native protocol v2.");
    }

    if (has_conditions()) {
        return execute_with_condition(proxy, qs, options);
    }

    return execute_without_condition(proxy, qs, options).then([] {
        return make_ready_future<::shared_ptr<transport::messages::result_message>>(
                ::shared_ptr<transport::messages::result_message>{});
    });
}

future<>
modification_statement::execute_without_condition(distributed<service::storage_proxy>& proxy, service::query_state& qs, const query_options& options) {
    auto cl = options.get_consistency();
    if (is_counter()) {
        db::validate_counter_for_write(s, cl);
    } else {
        db::validate_for_write(s->ks_name(), cl);
    }

    return get_mutations(proxy, options, false, options.get_timestamp(qs)).then([cl, &proxy] (auto mutations) {
        if (mutations.empty()) {
            return now();
        }
        return proxy.local().mutate_with_triggers(std::move(mutations), cl, false);
    });
}

future<::shared_ptr<transport::messages::result_message>>
modification_statement::execute_with_condition(distributed<service::storage_proxy>& proxy, service::query_state& qs, const query_options& options) {
    fail(unimplemented::cause::LWT);
#if 0
        List<ByteBuffer> keys = buildPartitionKeyNames(options);
        // We don't support IN for CAS operation so far
        if (keys.size() > 1)
            throw new InvalidRequestException("IN on the partition key is not supported with conditional updates");

        ByteBuffer key = keys.get(0);
        long now = options.getTimestamp(queryState);
        Composite prefix = createClusteringPrefix(options);

        CQL3CasRequest request = new CQL3CasRequest(cfm, key, false);
        addConditions(prefix, request, options);
        request.addRowUpdate(prefix, this, options, now);

        ColumnFamily result = StorageProxy.cas(keyspace(),
                                               columnFamily(),
                                               key,
                                               request,
                                               options.getSerialConsistency(),
                                               options.getConsistency(),
                                               queryState.getClientState());
        return new ResultMessage.Rows(buildCasResultSet(key, result, options));
#endif
}

future<::shared_ptr<transport::messages::result_message>>
modification_statement::execute_internal(distributed<service::storage_proxy>& proxy, service::query_state& qs, const query_options& options) {
    if (has_conditions()) {
        throw exceptions::unsupported_operation_exception();
    }
    return get_mutations(proxy, options, true, options.get_timestamp(qs)).then(
            [&proxy] (auto mutations) {
                return proxy.local().mutate_locally(std::move(mutations));
            }).then(
            [] {
                return make_ready_future<::shared_ptr<transport::messages::result_message>>(
                        ::shared_ptr<transport::messages::result_message> {});
            });
}

void
modification_statement::add_key_values(const column_definition& def, ::shared_ptr<restrictions::restriction> values) {
    if (def.is_clustering_key()) {
        _has_no_clustering_columns = false;
    }

    auto insert_result = _processed_keys.insert({&def, values});
    if (!insert_result.second) {
        throw exceptions::invalid_request_exception(sprint("Multiple definitions found for PRIMARY KEY part %s", def.name_as_text()));
    }
}

void
modification_statement::add_key_value(const column_definition& def, ::shared_ptr<term> value) {
    add_key_values(def, ::make_shared<restrictions::single_column_restriction::EQ>(def, value));
}

void
modification_statement::process_where_clause(database& db, std::vector<relation_ptr> where_clause, ::shared_ptr<variable_specifications> names) {
    for (auto&& relation : where_clause) {
        if (relation->is_multi_column()) {
            throw exceptions::invalid_request_exception(sprint("Multi-column relations cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", relation->to_string()));
        }

        auto rel = dynamic_pointer_cast<single_column_relation>(relation);
        if (rel->on_token()) {
            throw exceptions::invalid_request_exception(sprint("The token function cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", relation->to_string()));
        }

        auto id = rel->get_entity()->prepare_column_identifier(s);
        auto def = get_column_definition(s, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(sprint("Unknown key identifier %s", *id));
        }

        if (def->is_primary_key()) {
            if (rel->is_EQ() || (def->is_partition_key() && rel->is_IN())) {
                add_key_values(*def, rel->to_restriction(db, s, names));
            } else {
                throw exceptions::invalid_request_exception(sprint("Invalid operator %s for PRIMARY KEY part %s", rel->get_operator(), def->name_as_text()));
            }
        } else {
            throw exceptions::invalid_request_exception(sprint("Non PRIMARY KEY %s found in where clause", def->name_as_text()));
        }
    }
}

::shared_ptr<prepared_statement>
modification_statement::parsed::prepare(database& db) {
    auto bound_names = get_bound_variables();
    auto statement = prepare(db, bound_names);
    return ::make_shared<prepared>(std::move(statement), *bound_names);
}

::shared_ptr<modification_statement>
modification_statement::parsed::prepare(database& db, ::shared_ptr<variable_specifications> bound_names) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());

    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());
    prepared_attributes->collect_marker_specification(bound_names);

    ::shared_ptr<modification_statement> stmt = prepare_internal(db, schema, bound_names, std::move(prepared_attributes));

    if (_if_not_exists || _if_exists || !_conditions.empty()) {
        if (stmt->is_counter()) {
            throw exceptions::invalid_request_exception("Conditional updates are not supported on counter tables");
        }
        if (_attrs->timestamp) {
            throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional updates");
        }

        if (_if_not_exists) {
            // To have both 'IF NOT EXISTS' and some other conditions doesn't make sense.
            // So far this is enforced by the parser, but let's assert it for sanity if ever the parse changes.
            assert(_conditions.empty());
            assert(!_if_exists);
            stmt->set_if_not_exist_condition();
        } else if (_if_exists) {
            assert(_conditions.empty());
            assert(!_if_not_exists);
            stmt->set_if_exist_condition();
        } else {
            for (auto&& entry : _conditions) {
                auto id = entry.first->prepare_column_identifier(schema);
                const column_definition* def = get_column_definition(schema, *id);
                if (!def) {
                    throw exceptions::invalid_request_exception(sprint("Unknown identifier %s", *id));
                }

                auto condition = entry.second->prepare(db, keyspace(), *def);
                condition->collect_marker_specificaton(bound_names);

                if (def->is_primary_key()) {
                    throw exceptions::invalid_request_exception(sprint("PRIMARY KEY column '%s' cannot have IF conditions", *id));
                }
                stmt->add_condition(condition);
            }
        }
        stmt->validate_where_clause_for_conditions();
    }
    return stmt;
}

void
modification_statement::validate(distributed<service::storage_proxy>&, const service::client_state& state) {
    if (has_conditions() && attrs->is_timestamp_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional updates");
    }

    if (is_counter() && attrs->is_timestamp_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for counter updates");
    }

    if (is_counter() && attrs->is_time_to_live_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom TTL for counter updates");
    }
}

bool modification_statement::depends_on_keyspace(const sstring& ks_name) const {
    return keyspace() == ks_name;
}

bool modification_statement::depends_on_column_family(const sstring& cf_name) const {
    return column_family() == cf_name;
}

void modification_statement::add_operation(::shared_ptr<operation> op) {
    if (op->column.is_static()) {
        _sets_static_columns = true;
    } else {
        _sets_regular_columns = true;
    }
    _column_operations.push_back(std::move(op));
}

void modification_statement::add_condition(::shared_ptr<column_condition> cond) {
    if (cond->column.is_static()) {
        _sets_static_columns = true;
        _static_conditions.emplace_back(std::move(cond));
    } else {
        _sets_regular_columns = true;
        _column_conditions.emplace_back(std::move(cond));
    }
}

void modification_statement::set_if_not_exist_condition() {
    _if_not_exists = true;
}

bool modification_statement::has_if_not_exist_condition() const {
    return _if_not_exists;
}

void modification_statement::set_if_exist_condition() {
    _if_exists = true;
}

bool modification_statement::has_if_exist_condition() const {
    return _if_exists;
}

bool modification_statement::requires_read() {
    return std::any_of(_column_operations.begin(), _column_operations.end(), [] (auto&& op) {
        return op->requires_read();
    });
}

bool modification_statement::has_conditions() {
    return _if_not_exists || _if_exists || !_column_conditions.empty() || !_static_conditions.empty();
}

void modification_statement::validate_where_clause_for_conditions() {
    //  no-op by default
}

modification_statement::parsed::parsed(::shared_ptr<cf_name> name, ::shared_ptr<attributes::raw> attrs, conditions_vector conditions, bool if_not_exists, bool if_exists)
    : cf_statement{std::move(name)}
    , _attrs{std::move(attrs)}
    , _conditions{std::move(conditions)}
    , _if_not_exists{if_not_exists}
    , _if_exists{if_exists}
{ }

}

}
