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
#include "cql3/statements/raw/modification_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "validation.hh"
#include "db/consistency_level_validations.hh"
#include <seastar/core/shared_ptr.hh>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include "service/storage_service.hh"
#include "database.hh"
#include <seastar/core/execution_stage.hh>

namespace cql3 {

namespace statements {

thread_local const ::shared_ptr<column_identifier> modification_statement::CAS_RESULT_COLUMN = ::make_shared<column_identifier>("[applied]", false);

timeout_config_selector
modification_statement_timeout(const schema& s) {
    if (s.is_counter()) {
        return &timeout_config::counter_write_timeout;
    } else {
        return &timeout_config::write_timeout;
    }
}

modification_statement::modification_statement(statement_type type_, uint32_t bound_terms, schema_ptr schema_, std::unique_ptr<attributes> attrs_, uint64_t* cql_stats_counter_ptr)
    : cql_statement_no_metadata(modification_statement_timeout(*schema_))
    , type{type_}
    , _bound_terms{bound_terms}
    , s{schema_}
    , attrs{std::move(attrs_)}
    , _column_operations{}
    , _cql_modification_counter_ptr(cql_stats_counter_ptr)
{ }

bool modification_statement::uses_function(const sstring& ks_name, const sstring& function_name) const {
    if (attrs->uses_function(ks_name, function_name)) {
        return true;
    }
    if (_restrictions->uses_function(ks_name, function_name)) {
        return true;
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

const sstring& modification_statement::keyspace() const {
    return s->ks_name();
}

const sstring& modification_statement::column_family() const {
    return s->cf_name();
}

bool modification_statement::is_counter() const {
    return s->is_counter();
}

bool modification_statement::is_view() const {
    return s->is_view();
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
    // MV updates need to get the current state from the table, and might update the views
    // Require Permission.SELECT on the base table, and Permission.MODIFY on the views
    auto& db = service::get_local_storage_service().db().local();
    auto&& views = db.find_column_family(keyspace(), column_family()).views();
    if (!views.empty()) {
        f = f.then([this, &state] {
            return state.has_column_family_access(keyspace(), column_family(), auth::permission::SELECT);
        }).then([this, &state, views = std::move(views)] {
            return parallel_for_each(views, [this, &state] (auto&& view) {
                return state.has_column_family_access(this->keyspace(), view->cf_name(), auth::permission::MODIFY);
            });
        });
    }
    return f;
}

future<std::vector<mutation>>
modification_statement::get_mutations(service::storage_proxy& proxy, const query_options& options, db::timeout_clock::time_point timeout, bool local, int64_t now, service::query_state& qs) {
    auto json_cache = maybe_prepare_json_cache(options);
    auto keys = build_partition_keys(options, json_cache);
    auto ranges = create_clustering_ranges(options, json_cache);
    auto f = make_ready_future<update_parameters::prefetch_data>(s);

    if (requires_read()) {
        lw_shared_ptr<query::read_command> cmd = read_command(ranges, options.get_consistency());
        // FIXME: ignoring "local"
        f = proxy.query(s, cmd, dht::partition_range_vector(keys), options.get_consistency(),
                {timeout, qs.get_permit(), qs.get_client_state(), qs.get_trace_state()}).then(

                [this, cmd] (auto cqr) {

            return update_parameters::build_prefetch_data(s, *cqr.query_result, cmd->slice);
        });
    }

    return f.then([this, keys = std::move(keys), ranges = std::move(ranges), json_cache = std::move(json_cache), &options, now]
            (auto rows) {

        update_parameters params(s, options, this->get_timestamp(now, options),
                this->get_time_to_live(options), std::move(rows));

        std::vector<mutation> mutations = apply_updates(keys, ranges, params, json_cache);

        return make_ready_future<std::vector<mutation>>(std::move(mutations));
    });
}

std::vector<mutation> modification_statement::apply_updates(
        const std::vector<dht::partition_range>& keys,
        const std::vector<query::clustering_range>& ranges,
        const update_parameters& params,
        const json_cache_opt& json_cache) {

    std::vector<mutation> mutations;
    mutations.reserve(keys.size());
    for (auto key : keys) {
        // We know key.start() must be defined since we only allow EQ relations on the partition key.
        mutations.emplace_back(s, std::move(*key.start()->value().key()));
        auto& m = mutations.back();
        for (auto&& r : ranges) {
            this->add_update_for_key(m, r, params, json_cache);
        }
    }
    return mutations;
}

lw_shared_ptr<query::read_command>
modification_statement::read_command(query::clustering_row_ranges ranges, db::consistency_level cl) const {
    try {
        validate_for_read(cl);
    } catch (exceptions::invalid_request_exception& e) {
        throw exceptions::invalid_request_exception(format("Write operation require a read but consistency {} is not supported on reads", cl));
    }
    query::partition_slice ps(std::move(ranges), *s, columns_to_read(), update_parameters::options);
    return make_lw_shared<query::read_command>(s->id(), s->version(), std::move(ps));
}

std::vector<query::clustering_range>
modification_statement::create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) {
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
    if (applies_only_to_static_columns()) {
        // If we set no non-static columns, then it's fine not to have clustering columns
        if (!_restrictions->has_clustering_columns_restriction()) {
            return { query::clustering_range::make_open_ended_both_sides() };
        }

        // If we do have clustering columns however, then either it's an INSERT and the query is valid
        // but we still need to build a proper prefix, or it's not an INSERT, and then we want to reject
        // (see above)
        if (!type.is_insert()) {
            if (_restrictions->has_clustering_columns_restriction()) {
                throw exceptions::invalid_request_exception(format("Invalid restriction on clustering column {} since the {} statement modifies only static columns",
                    _restrictions->get_clustering_columns_restrictions()->get_column_defs().front()->name_as_text(), type));
            }

            // we should get there as it contradicts !_restrictions->has_clustering_columns_restriction()
            throw std::logic_error("contradicts !_restrictions->has_clustering_columns_restriction()");
        }
    }

    return _restrictions->get_clustering_bounds(options);
}

dht::partition_range_vector
modification_statement::build_partition_keys(const query_options& options, const json_cache_opt& json_cache) {
    auto keys = _restrictions->get_partition_key_restrictions()->bounds_ranges(options);
    for (auto&& k : keys) {
        validation::validate_cql_key(s, *k.start()->value().key());
    }
    return keys;
}

struct modification_statement_executor {
    static auto get() { return &modification_statement::do_execute; }
};
static thread_local inheriting_concrete_execution_stage<
        future<::shared_ptr<cql_transport::messages::result_message>>,
        modification_statement*,
        service::storage_proxy&,
        service::query_state&,
        const query_options&> modify_stage{"cql3_modification", modification_statement_executor::get()};

future<::shared_ptr<cql_transport::messages::result_message>>
modification_statement::execute(service::storage_proxy& proxy, service::query_state& qs, const query_options& options) {
    return modify_stage(this, seastar::ref(proxy), seastar::ref(qs), seastar::cref(options));
}

future<::shared_ptr<cql_transport::messages::result_message>>
modification_statement::do_execute(service::storage_proxy& proxy, service::query_state& qs, const query_options& options) {
    if (has_conditions() && options.get_protocol_version() == 1) {
        throw exceptions::invalid_request_exception("Conditional updates are not supported by the protocol version in use. You need to upgrade to a driver using the native protocol v2.");
    }

    tracing::add_table_name(qs.get_trace_state(), keyspace(), column_family());

    if (has_conditions()) {
        return execute_with_condition(proxy, qs, options);
    }

    inc_cql_stats();

    return execute_without_condition(proxy, qs, options).then([] {
        return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(
                ::shared_ptr<cql_transport::messages::result_message>{});
    });
}

future<>
modification_statement::execute_without_condition(service::storage_proxy& proxy, service::query_state& qs, const query_options& options) {
    auto cl = options.get_consistency();
    if (is_counter()) {
        db::validate_counter_for_write(*s, cl);
    } else {
        db::validate_for_write(cl);
    }

    auto timeout = db::timeout_clock::now() + options.get_timeout_config().*get_timeout_config_selector();
    return get_mutations(proxy, options, timeout, false, options.get_timestamp(qs), qs).then([this, cl, timeout, &proxy, &qs] (auto mutations) {
        if (mutations.empty()) {
            return now();
        }

        return proxy.mutate_with_triggers(std::move(mutations), cl, timeout, false, qs.get_trace_state(), qs.get_permit(), this->is_raw_counter_shard_write());
    });
}

future<::shared_ptr<cql_transport::messages::result_message>>
modification_statement::execute_with_condition(service::storage_proxy& proxy, service::query_state& qs, const query_options& options) {
    fail(unimplemented::cause::LWT);
}

void
modification_statement::process_where_clause(database& db, std::vector<relation_ptr> where_clause, ::shared_ptr<variable_specifications> names) {
    _restrictions = ::make_shared<restrictions::statement_restrictions>(
            db, s, type, where_clause, std::move(names), applies_only_to_static_columns(), _sets_a_collection, false);
    if (_restrictions->get_partition_key_restrictions()->is_on_token()) {
        throw exceptions::invalid_request_exception(format("The token function cannot be used in WHERE clauses for UPDATE and DELETE statements: {}",
                _restrictions->get_partition_key_restrictions()->to_string()));
    }
    if (!_restrictions->get_non_pk_restriction().empty()) {
        auto column_names = ::join(", ", _restrictions->get_non_pk_restriction()
                                         | boost::adaptors::map_keys
                                         | boost::adaptors::indirected
                                         | boost::adaptors::transformed(std::mem_fn(&column_definition::name)));
        throw exceptions::invalid_request_exception(format("Invalid where clause contains non PRIMARY KEY columns: {}", column_names));
    }
    auto ck_restrictions = _restrictions->get_clustering_columns_restrictions();
    if (ck_restrictions->is_slice() && !allow_clustering_key_slices()) {
        throw exceptions::invalid_request_exception(format("Invalid operator in where clause {}", ck_restrictions->to_string()));
    }
    if (_restrictions->has_unrestricted_clustering_columns() && !applies_only_to_static_columns() && !s->is_dense()) {
        // Tomek: Origin had "&& s->comparator->is_composite()" in the condition below.
        // Comparator is a thrift concept, not CQL concept, and we want to avoid
        // using thrift concepts here. I think it's safe to drop this here because the only
        // case in which we would get a non-composite comparator here would be if the cell
        // name type is SimpleSparse, which means:
        //   (a) CQL compact table without clustering columns
        //   (b) thrift static CF with non-composite comparator
        // Those tables don't have clustering columns so we wouldn't reach this code, thus
        // the check seems redundant.
        if (require_full_clustering_key()) {
            auto& col = s->column_at(column_kind::clustering_key, ck_restrictions->size());
            throw exceptions::invalid_request_exception(format("Missing mandatory PRIMARY KEY part {}", col.name_as_text()));
        }
        // In general, we can't modify specific columns if not all clustering columns have been specified.
        // However, if we modify only static columns, it's fine since we won't really use the prefix anyway.
        if (!ck_restrictions->is_slice()) {
            auto& col = s->column_at(column_kind::clustering_key, ck_restrictions->size());
            for (auto&& op : _column_operations) {
                if (!op->column.is_static()) {
                    throw exceptions::invalid_request_exception(format("Primary key column '{}' must be specified in order to modify column '{}'",
                        col.name_as_text(), op->column.name_as_text()));
                }
            }
        }
    }
    if (_restrictions->has_partition_key_unrestricted_components()) {
        auto& col = s->column_at(column_kind::partition_key, _restrictions->get_partition_key_restrictions()->size());
        throw exceptions::invalid_request_exception(format("Missing mandatory PRIMARY KEY part {}", col.name_as_text()));
    }
}

namespace raw {

std::unique_ptr<prepared_statement>
modification_statement::prepare(database& db, cql_stats& stats) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto bound_names = get_bound_variables();
    auto statement = prepare(db, bound_names, stats);
    auto partition_key_bind_indices = bound_names->get_partition_key_bind_indexes(schema);
    return std::make_unique<prepared>(std::move(statement), *bound_names, std::move(partition_key_bind_indices));
}

::shared_ptr<cql3::statements::modification_statement>
modification_statement::prepare(database& db, ::shared_ptr<variable_specifications> bound_names, cql_stats& stats) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());

    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());
    prepared_attributes->collect_marker_specification(bound_names);

    ::shared_ptr<cql3::statements::modification_statement> stmt = prepare_internal(db, schema, bound_names, std::move(prepared_attributes), stats);
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
                    throw exceptions::invalid_request_exception(format("Unknown identifier {}", *id));
                }

                auto condition = entry.second->prepare(db, keyspace(), *def);
                condition->collect_marker_specificaton(bound_names);

                if (def->is_primary_key()) {
                    throw exceptions::invalid_request_exception(format("PRIMARY KEY column '{}' cannot have IF conditions", *id));
                }
                stmt->add_condition(condition);
            }
        }
        stmt->validate_where_clause_for_conditions();
    }
    return stmt;
}

}

void
modification_statement::validate(service::storage_proxy&, const service::client_state& state) {
    if (has_conditions() && attrs->is_timestamp_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional updates");
    }

    if (is_counter() && attrs->is_timestamp_set() && !is_raw_counter_shard_write()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for counter updates");
    }

    if (is_counter() && attrs->is_time_to_live_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom TTL for counter updates");
    }

    if (is_view()) {
        throw exceptions::invalid_request_exception("Cannot directly modify a materialized view");
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
        _sets_a_collection |= op->column.type->is_collection();
    }
    if (op->requires_read()) {
        _requires_read = true;
        _columns_to_read.set(op->column.ordinal_id);
    }

    if (op->column.is_counter()) {
        auto is_raw_counter_shard_write = op->is_raw_counter_shard_write();
        if (_is_raw_counter_shard_write && _is_raw_counter_shard_write != is_raw_counter_shard_write) {
            throw exceptions::invalid_request_exception("Cannot mix regular and raw counter updates");
        }
        _is_raw_counter_shard_write = is_raw_counter_shard_write;
    }

    _column_operations.push_back(std::move(op));
}

void modification_statement::add_condition(::shared_ptr<column_condition> cond) {
    if (cond->column.is_static()) {
        _sets_static_columns = true;
        _static_conditions.emplace_back(std::move(cond));
    } else {
        _sets_regular_columns = true;
        _sets_a_collection |= cond->column.type->is_collection();
        _column_conditions.emplace_back(std::move(cond));
    }
    _has_conditions = true;
}

void modification_statement::set_if_not_exist_condition() {
    _if_not_exists = true;
    _has_conditions = true;
}

bool modification_statement::has_if_not_exist_condition() const {
    return _if_not_exists;
}

void modification_statement::set_if_exist_condition() {
    _if_exists = true;
    _has_conditions = true;
}

bool modification_statement::has_if_exist_condition() const {
    return _if_exists;
}


void modification_statement::validate_where_clause_for_conditions() {
    //  no-op by default
}

modification_statement::json_cache_opt modification_statement::maybe_prepare_json_cache(const query_options& options) {
    return {};
}

const statement_type statement_type::INSERT = statement_type(statement_type::type::insert);
const statement_type statement_type::UPDATE = statement_type(statement_type::type::update);
const statement_type statement_type::DELETE = statement_type(statement_type::type::del);
const statement_type statement_type::SELECT = statement_type(statement_type::type::select);

namespace raw {

modification_statement::modification_statement(::shared_ptr<cf_name> name, ::shared_ptr<attributes::raw> attrs, conditions_vector conditions, bool if_not_exists, bool if_exists)
    : cf_statement{std::move(name)}
    , _attrs{std::move(attrs)}
    , _conditions{std::move(conditions)}
    , _if_not_exists{if_not_exists}
    , _if_exists{if_exists}
{ }

}

}

}
