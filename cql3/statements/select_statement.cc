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

#include "cql3/statements/select_statement.hh"
#include "cql3/statements/raw/select_statement.hh"

#include "transport/messages/result_message.hh"
#include "cql3/selection/selection.hh"
#include "cql3/util.hh"
#include "core/shared_ptr.hh"
#include "query-result-reader.hh"
#include "query_result_merger.hh"
#include "service/pager/query_pagers.hh"
#include <seastar/core/execution_stage.hh>
#include "view_info.hh"
#include "partition_slice_builder.hh"
#include "cql3/untyped_result_set.hh"

namespace cql3 {

namespace statements {

thread_local const shared_ptr<select_statement::parameters> select_statement::_default_parameters = ::make_shared<select_statement::parameters>();

select_statement::parameters::parameters()
    : _is_distinct{false}
    , _allow_filtering{false}
{ }

select_statement::parameters::parameters(orderings_type orderings,
                                         bool is_distinct,
                                         bool allow_filtering)
    : _orderings{std::move(orderings)}
    , _is_distinct{is_distinct}
    , _allow_filtering{allow_filtering}
{ }

bool select_statement::parameters::is_distinct() {
    return _is_distinct;
}

bool select_statement::parameters::allow_filtering() {
    return _allow_filtering;
}

select_statement::parameters::orderings_type const& select_statement::parameters::orderings() {
    return _orderings;
}

select_statement::select_statement(schema_ptr schema,
                                   uint32_t bound_terms,
                                   ::shared_ptr<parameters> parameters,
                                   ::shared_ptr<selection::selection> selection,
                                   ::shared_ptr<restrictions::statement_restrictions> restrictions,
                                   bool is_reversed,
                                   ordering_comparator_type ordering_comparator,
                                   ::shared_ptr<term> limit,
                                   cql_stats& stats)
    : _schema(schema)
    , _bound_terms(bound_terms)
    , _parameters(std::move(parameters))
    , _selection(std::move(selection))
    , _restrictions(std::move(restrictions))
    , _is_reversed(is_reversed)
    , _limit(std::move(limit))
    , _ordering_comparator(std::move(ordering_comparator))
    , _stats(stats)
{
    _opts = _selection->get_query_options();
}

bool select_statement::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return _selection->uses_function(ks_name, function_name)
        || _restrictions->uses_function(ks_name, function_name)
        || (_limit && _limit->uses_function(ks_name, function_name));
}

::shared_ptr<const cql3::metadata> select_statement::get_result_metadata() const {
    // FIXME: COUNT needs special result metadata handling.
    return _selection->get_result_metadata();
}

uint32_t select_statement::get_bound_terms() {
    return _bound_terms;
}

future<> select_statement::check_access(const service::client_state& state) {
    try {
        auto&& s = service::get_local_storage_proxy().get_db().local().find_schema(keyspace(), column_family());
        auto& cf_name = s->is_view() ? s->view_info()->base_name() : column_family();
        return state.has_column_family_access(keyspace(), cf_name, auth::permission::SELECT);
    } catch (const no_such_column_family& e) {
        // Will be validated afterwards.
        return make_ready_future<>();
    }
}

void select_statement::validate(distributed<service::storage_proxy>&, const service::client_state& state) {
    // Nothing to do, all validation has been done by raw_statemet::prepare()
}

bool select_statement::depends_on_keyspace(const sstring& ks_name) const {
    return keyspace() == ks_name;
}

bool select_statement::depends_on_column_family(const sstring& cf_name) const {
    return column_family() == cf_name;
}

const sstring& select_statement::keyspace() const {
    return _schema->ks_name();
}

const sstring& select_statement::column_family() const {
    return _schema->cf_name();
}

query::partition_slice
select_statement::make_partition_slice(const query_options& options)
{
    std::vector<column_id> static_columns;
    std::vector<column_id> regular_columns;

    if (_selection->contains_static_columns()) {
        static_columns.reserve(_selection->get_column_count());
    }

    regular_columns.reserve(_selection->get_column_count());

    for (auto&& col : _selection->get_columns()) {
        if (col->is_static()) {
            static_columns.push_back(col->id);
        } else if (col->is_regular()) {
            regular_columns.push_back(col->id);
        }
    }

    if (_parameters->is_distinct()) {
        _opts.set(query::partition_slice::option::distinct);
        return query::partition_slice({ query::clustering_range::make_open_ended_both_sides() },
            std::move(static_columns), {}, _opts, nullptr, options.get_cql_serialization_format());
    }

    auto bounds = _restrictions->get_clustering_bounds(options);
    if (_is_reversed) {
        _opts.set(query::partition_slice::option::reversed);
        std::reverse(bounds.begin(), bounds.end());
    }
    return query::partition_slice(std::move(bounds),
        std::move(static_columns), std::move(regular_columns), _opts, nullptr, options.get_cql_serialization_format());
}

int32_t select_statement::get_limit(const query_options& options) const {
    if (!_limit) {
        return std::numeric_limits<int32_t>::max();
    }

    auto val = _limit->bind_and_get(options);
    if (val.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value of limit");
    }
    if (val.is_unset_value()) {
        return std::numeric_limits<int32_t>::max();
    }
    try {
        int32_type->validate(*val);
        auto l = value_cast<int32_t>(int32_type->deserialize(*val));
        if (l <= 0) {
            throw exceptions::invalid_request_exception("LIMIT must be strictly positive");
        }
        return l;
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid limit value");
    }
}

bool select_statement::needs_post_query_ordering() const {
    // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
    return _restrictions->key_is_in_relation() && !_parameters->orderings().empty();
}

struct select_statement_executor {
    static auto get() { return &select_statement::do_execute; }
};
static thread_local auto select_stage = seastar::make_execution_stage("cql3_select", select_statement_executor::get());

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::execute(distributed<service::storage_proxy>& proxy,
                             service::query_state& state,
                             const query_options& options)
{
    return select_stage(this, seastar::ref(proxy), seastar::ref(state), seastar::cref(options));
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::do_execute(distributed<service::storage_proxy>& proxy,
                          service::query_state& state,
                          const query_options& options)
{
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    auto cl = options.get_consistency();

    validate_for_read(_schema->ks_name(), cl);

    int32_t limit = get_limit(options);
    auto now = gc_clock::now();

    ++_stats.reads;

    auto command = ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(),
        make_partition_slice(options), limit, now, tracing::make_trace_info(state.get_trace_state()), query::max_partitions, options.get_timestamp(state));

    int32_t page_size = options.get_page_size();

    // An aggregation query will never be paged for the user, but we always page it internally to avoid OOM.
    // If we user provided a page_size we'll use that to page internally (because why not), otherwise we use our default
    // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
    auto aggregate = _selection->is_aggregate();
    if (aggregate && page_size <= 0) {
        page_size = DEFAULT_COUNT_PAGE_SIZE;
    }

    auto key_ranges = _restrictions->get_partition_key_ranges(options);

    if (!aggregate && (page_size <= 0
            || !service::pager::query_pagers::may_need_paging(page_size,
                    *command, key_ranges))) {
        return execute(proxy, command, std::move(key_ranges), state, options, now);
    }

    command->slice.options.set<query::partition_slice::option::allow_short_read>();
    auto p = service::pager::query_pagers::pager(_schema, _selection,
            state, options, command, std::move(key_ranges));

    if (aggregate) {
        return do_with(
                cql3::selection::result_set_builder(*_selection, now,
                        options.get_cql_serialization_format()),
                [p, page_size, now](auto& builder) {
                    return do_until([p] {return p->is_exhausted();},
                            [p, &builder, page_size, now] {
                                return p->fetch_page(builder, page_size, now);
                            }
                    ).then([&builder] {
                                auto rs = builder.build();
                                auto msg = ::make_shared<cql_transport::messages::result_message::rows>(std::move(rs));
                                return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(std::move(msg));
                            });
                });
    }

    if (needs_post_query_ordering()) {
        throw exceptions::invalid_request_exception(
                "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                        " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");
    }

    return p->fetch_page(page_size, now).then(
            [this, p, &options, limit, now](std::unique_ptr<cql3::result_set> rs) {

                if (!p->is_exhausted()) {
                    rs->get_metadata().set_has_more_pages(p->state());
                }

                auto msg = ::make_shared<cql_transport::messages::result_message::rows>(std::move(rs));
                return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(std::move(msg));
            });
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::execute(distributed<service::storage_proxy>& proxy,
                          lw_shared_ptr<query::read_command> cmd,
                          dht::partition_range_vector&& partition_ranges,
                          service::query_state& state,
                          const query_options& options,
                          gc_clock::time_point now)
{
    // If this is a query with IN on partition key, ORDER BY clause and LIMIT
    // is specified we need to get "limit" rows from each partition since there
    // is no way to tell which of these rows belong to the query result before
    // doing post-query ordering.
    if (needs_post_query_ordering() && _limit) {
        return do_with(std::forward<dht::partition_range_vector>(partition_ranges), [this, &proxy, &state, &options, cmd](auto prs) {
            assert(cmd->partition_limit == query::max_partitions);
            query::result_merger merger(cmd->row_limit * prs.size(), query::max_partitions);
            return map_reduce(prs.begin(), prs.end(), [this, &proxy, &state, &options, cmd] (auto pr) {
                dht::partition_range_vector prange { pr };
                auto command = ::make_lw_shared<query::read_command>(*cmd);
                return proxy.local().query(_schema, command, std::move(prange), options.get_consistency(), state.get_trace_state());
            }, std::move(merger));
        }).then([this, &options, now, cmd] (auto result) {
            return this->process_results(std::move(result), cmd, options, now);
        });
    } else {
        return proxy.local().query(_schema, cmd, std::move(partition_ranges), options.get_consistency(), state.get_trace_state())
            .then([this, &options, now, cmd] (auto result) {
                return this->process_results(std::move(result), cmd, options, now);
            });
    }
}

future<::shared_ptr<cql_transport::messages::result_message>>
select_statement::execute_internal(distributed<service::storage_proxy>& proxy,
                                   service::query_state& state,
                                   const query_options& options)
{
    if (options.get_specific_options().page_size > 0) {
        // need page, use regular execute
        return do_execute(proxy, state, options);
    }
    int32_t limit = get_limit(options);
    auto now = gc_clock::now();
    auto command = ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(),
        make_partition_slice(options), limit, now, std::experimental::nullopt, query::max_partitions, options.get_timestamp(state));
    auto partition_ranges = _restrictions->get_partition_key_ranges(options);

    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    ++_stats.reads;

    if (needs_post_query_ordering() && _limit) {
        return do_with(std::move(partition_ranges), [this, &proxy, &state, command] (auto prs) {
            assert(command->partition_limit == query::max_partitions);
            query::result_merger merger(command->row_limit * prs.size(), query::max_partitions);
            return map_reduce(prs.begin(), prs.end(), [this, &proxy, &state, command] (auto pr) {
                dht::partition_range_vector prange { pr };
                auto cmd = ::make_lw_shared<query::read_command>(*command);
                return proxy.local().query(_schema, cmd, std::move(prange), db::consistency_level::ONE, state.get_trace_state());
            }, std::move(merger));
        }).then([command, this, &options, now] (auto result) {
            return this->process_results(std::move(result), command, options, now);
        }).finally([command] { });
    } else {
        return proxy.local().query(_schema, command, std::move(partition_ranges), db::consistency_level::ONE, state.get_trace_state()).then([command, this, &options, now] (auto result) {
            return this->process_results(std::move(result), command, options, now);
        }).finally([command] {});
    }
}

shared_ptr<cql_transport::messages::result_message>
select_statement::process_results(foreign_ptr<lw_shared_ptr<query::result>> results,
                                  lw_shared_ptr<query::read_command> cmd,
                                  const query_options& options,
                                  gc_clock::time_point now)
{
    cql3::selection::result_set_builder builder(*_selection, now,
            options.get_cql_serialization_format());
    query::result_view::consume(*results, cmd->slice,
            cql3::selection::result_set_builder::visitor(builder, *_schema,
                    *_selection));
    auto rs = builder.build();

    if (needs_post_query_ordering()) {
        rs->sort(_ordering_comparator);
        if (_is_reversed) {
            rs->reverse();
        }
        rs->trim(cmd->row_limit);
    }
    return ::make_shared<cql_transport::messages::result_message::rows>(std::move(rs));
}

::shared_ptr<restrictions::statement_restrictions> select_statement::get_restrictions() const {
    return _restrictions;
}

primary_key_select_statement::primary_key_select_statement(schema_ptr schema, uint32_t bound_terms,
                                                           ::shared_ptr<parameters> parameters,
                                                           ::shared_ptr<selection::selection> selection,
                                                           ::shared_ptr<restrictions::statement_restrictions> restrictions,
                                                           bool is_reversed,
                                                           ordering_comparator_type ordering_comparator,
                                                           ::shared_ptr<term> limit, cql_stats &stats)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, is_reversed, ordering_comparator, limit, stats}
{}

::shared_ptr<cql3::statements::select_statement>
indexed_table_select_statement::prepare(database& db,
                                        schema_ptr schema,
                                        uint32_t bound_terms,
                                        ::shared_ptr<parameters> parameters,
                                        ::shared_ptr<selection::selection> selection,
                                        ::shared_ptr<restrictions::statement_restrictions> restrictions,
                                        bool is_reversed,
                                        ordering_comparator_type ordering_comparator,
                                        ::shared_ptr<term> limit, cql_stats &stats)
{
    auto index_opt = find_idx(db, schema, restrictions);
    if (!index_opt) {
        throw std::runtime_error("No index found.");
    }
    return ::make_shared<cql3::statements::indexed_table_select_statement>(
            schema,
            bound_terms,
            parameters,
            std::move(selection),
            std::move(restrictions),
            is_reversed,
            std::move(ordering_comparator),
            limit,
            stats,
            *index_opt);

}


stdx::optional<secondary_index::index> indexed_table_select_statement::find_idx(database& db,
                                                                                schema_ptr schema,
                                                                                ::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    auto& sim = db.find_column_family(schema).get_index_manager();
    for (::shared_ptr<cql3::restrictions::restrictions> restriction : restrictions->index_restrictions()) {
        for (const auto& cdef : restriction->get_column_defs()) {
            for (auto index : sim.list_indexes()) {
                if (index.depends_on(*cdef)) {
                    return stdx::make_optional<secondary_index::index>(std::move(index));
                }
            }
        }
    }
    return stdx::nullopt;
}

indexed_table_select_statement::indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
                                                           ::shared_ptr<parameters> parameters,
                                                           ::shared_ptr<selection::selection> selection,
                                                           ::shared_ptr<restrictions::statement_restrictions> restrictions,
                                                           bool is_reversed,
                                                           ordering_comparator_type ordering_comparator,
                                                           ::shared_ptr<term> limit, cql_stats &stats,
                                                           const secondary_index::index& index)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, is_reversed, ordering_comparator, limit, stats}
    , _index{index}
{}

future<shared_ptr<cql_transport::messages::result_message>>
indexed_table_select_statement::do_execute(distributed<service::storage_proxy>& proxy,
                             service::query_state& state,
                             const query_options& options)
{
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    auto cl = options.get_consistency();

    validate_for_read(_schema->ks_name(), cl);

    int32_t limit = get_limit(options);
    auto now = gc_clock::now();

    ++_stats.reads;

    assert(_restrictions->uses_secondary_indexing());

    return find_index_partition_ranges(proxy, state, options).then([&, this] (dht::partition_range_vector partition_ranges) {
        auto command = ::make_lw_shared<query::read_command>(
                _schema->id(),
                _schema->version(),
                make_partition_slice(options),
                limit,
                now,
                tracing::make_trace_info(state.get_trace_state()),
                query::max_partitions,
                options.get_timestamp(state));
        return this->execute(proxy, command, std::move(partition_ranges), state, options, now);
    });
}

future<dht::partition_range_vector>
indexed_table_select_statement::find_index_partition_ranges(distributed<service::storage_proxy>& proxy,
                                             service::query_state& state,
                                             const query_options& options)
{
    const auto& im = _index.metadata();
    sstring index_table_name = sprint("%s_index", im.name());
    tracing::add_table_name(state.get_trace_state(), keyspace(), index_table_name);
    auto& db = proxy.local().get_db().local();
    const auto& view = db.find_column_family(_schema->ks_name(), index_table_name);
    dht::partition_range_vector partition_ranges;
    for (const auto& entry : _restrictions->get_non_pk_restriction()) {
        auto pk = partition_key::from_optional_exploded(*view.schema(), entry.second->values(options));
        auto dk = dht::global_partitioner().decorate_key(*view.schema(), pk);
        auto range = dht::partition_range::make_singular(dk);
        partition_ranges.emplace_back(range);
    }

    auto now = gc_clock::now();
    int32_t limit = get_limit(options);

    partition_slice_builder partition_slice_builder{*view.schema()};
    auto cmd = ::make_lw_shared<query::read_command>(
            view.schema()->id(),
            view.schema()->version(),
            partition_slice_builder.build(),
            limit,
            now,
            tracing::make_trace_info(state.get_trace_state()),
            query::max_partitions,
            options.get_timestamp(state));
    return proxy.local().query(view.schema(),
                               cmd,
                               std::move(partition_ranges),
                               options.get_consistency(),
                               state.get_trace_state()).then([cmd, this, &options, now, &view] (foreign_ptr<lw_shared_ptr<query::result>> result) {
        std::vector<const column_definition*> columns;
        for (const column_definition& cdef : _schema->partition_key_columns()) {
            columns.emplace_back(view.schema()->get_column_definition(cdef.name()));
        }
        auto selection = selection::selection::for_columns(view.schema(), columns);
        cql3::selection::result_set_builder builder(*selection, now, options.get_cql_serialization_format());
        query::result_view::consume(*result,
                                    cmd->slice,
                                    cql3::selection::result_set_builder::visitor(builder, *view.schema(), *selection));
        auto rs = cql3::untyped_result_set(::make_shared<cql_transport::messages::result_message::rows>(std::move(builder.build())));
        dht::partition_range_vector partition_ranges;
        for (size_t i = 0; i < rs.size(); i++) {
            const auto& row = rs.at(i);
            for (const auto& column : row.get_columns()) {
                auto blob = row.get_blob(column->name->to_cql_string());
                auto pk = partition_key::from_exploded(*_schema, { blob });
                auto dk = dht::global_partitioner().decorate_key(*_schema, pk);
                auto range = dht::partition_range::make_singular(dk);
                partition_ranges.emplace_back(range);
            }
        }
        return make_ready_future<dht::partition_range_vector>(partition_ranges);
    }).finally([cmd] {});
}

namespace raw {

select_statement::select_statement(::shared_ptr<cf_name> cf_name,
                                   ::shared_ptr<parameters> parameters,
                                   std::vector<::shared_ptr<selection::raw_selector>> select_clause,
                                   std::vector<::shared_ptr<relation>> where_clause,
                                   ::shared_ptr<term::raw> limit)
    : cf_statement(std::move(cf_name))
    , _parameters(std::move(parameters))
    , _select_clause(std::move(select_clause))
    , _where_clause(std::move(where_clause))
    , _limit(std::move(limit))
{ }

std::unique_ptr<prepared_statement> select_statement::prepare(database& db, cql_stats& stats, bool for_view) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto bound_names = get_bound_variables();

    auto selection = _select_clause.empty()
                     ? selection::selection::wildcard(schema)
                     : selection::selection::from_selectors(db, schema, _select_clause);

    auto restrictions = prepare_restrictions(db, schema, bound_names, selection, for_view);

    if (_parameters->is_distinct()) {
        validate_distinct_selection(schema, selection, restrictions);
    }

    select_statement::ordering_comparator_type ordering_comparator;
    bool is_reversed_ = false;

    if (!_parameters->orderings().empty()) {
        assert(!for_view);
        verify_ordering_is_allowed(restrictions);
        ordering_comparator = get_ordering_comparator(schema, selection, restrictions);
        is_reversed_ = is_reversed(schema);
    }

    check_needs_filtering(restrictions);

    ::shared_ptr<cql3::statements::select_statement> stmt;
    if (restrictions->uses_secondary_indexing()) {
        stmt = indexed_table_select_statement::prepare(
                db,
                schema,
                bound_names->size(),
                _parameters,
                std::move(selection),
                std::move(restrictions),
                is_reversed_,
                std::move(ordering_comparator),
                prepare_limit(db, bound_names),
                stats);
    } else {
        stmt = ::make_shared<cql3::statements::primary_key_select_statement>(
                schema,
                bound_names->size(),
                _parameters,
                std::move(selection),
                std::move(restrictions),
                is_reversed_,
                std::move(ordering_comparator),
                prepare_limit(db, bound_names),
                stats);
    }

    auto partition_key_bind_indices = bound_names->get_partition_key_bind_indexes(schema);

    return std::make_unique<prepared>(std::move(stmt), std::move(*bound_names), std::move(partition_key_bind_indices));
}

::shared_ptr<restrictions::statement_restrictions>
select_statement::prepare_restrictions(database& db,
                                       schema_ptr schema,
                                       ::shared_ptr<variable_specifications> bound_names,
                                       ::shared_ptr<selection::selection> selection,
                                       bool for_view)
{
    try {
        return ::make_shared<restrictions::statement_restrictions>(db, schema, statement_type::SELECT, std::move(_where_clause), bound_names,
            selection->contains_only_static_columns(), selection->contains_a_collection(), for_view);
    } catch (const exceptions::unrecognized_entity_exception& e) {
        if (contains_alias(e.entity)) {
            throw exceptions::invalid_request_exception(sprint("Aliases aren't allowed in the where clause ('%s')", e.relation->to_string()));
        }
        throw;
    }
}

/** Returns a ::shared_ptr<term> for the limit or null if no limit is set */
::shared_ptr<term>
select_statement::prepare_limit(database& db, ::shared_ptr<variable_specifications> bound_names)
{
    if (!_limit) {
        return {};
    }

    auto prep_limit = _limit->prepare(db, keyspace(), limit_receiver());
    prep_limit->collect_marker_specification(bound_names);
    return prep_limit;
}

void select_statement::verify_ordering_is_allowed(::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    if (restrictions->uses_secondary_indexing()) {
        throw exceptions::invalid_request_exception("ORDER BY with 2ndary indexes is not supported.");
    }
    if (restrictions->is_key_range()) {
        throw exceptions::invalid_request_exception("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
    }
}

void select_statement::validate_distinct_selection(schema_ptr schema,
                                                   ::shared_ptr<selection::selection> selection,
                                                   ::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    for (auto&& def : selection->get_columns()) {
        if (!def->is_partition_key() && !def->is_static()) {
            throw exceptions::invalid_request_exception(sprint(
                "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)",
                def->name_as_text()));
        }
    }

    // If it's a key range, we require that all partition key columns are selected so we don't have to bother
    // with post-query grouping.
    if (!restrictions->is_key_range()) {
        return;
    }

    for (auto&& def : schema->partition_key_columns()) {
        if (!selection->has_column(def)) {
            throw exceptions::invalid_request_exception(sprint(
                "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name_as_text()));
        }
    }
}

void select_statement::handle_unrecognized_ordering_column(::shared_ptr<column_identifier> column)
{
    if (contains_alias(column)) {
        throw exceptions::invalid_request_exception(sprint("Aliases are not allowed in order by clause ('%s')", *column));
    }
    throw exceptions::invalid_request_exception(sprint("Order by on unknown column %s", *column));
}

select_statement::ordering_comparator_type
select_statement::get_ordering_comparator(schema_ptr schema,
                                          ::shared_ptr<selection::selection> selection,
                                          ::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    if (!restrictions->key_is_in_relation()) {
        return {};
    }

    std::vector<std::pair<uint32_t, data_type>> sorters;
    sorters.reserve(_parameters->orderings().size());

    // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting,
    // even if we don't
    // ultimately ship them to the client (CASSANDRA-4911).
    for (auto&& e : _parameters->orderings()) {
        auto&& raw = e.first;
        ::shared_ptr<column_identifier> column = raw->prepare_column_identifier(schema);
        const column_definition* def = schema->get_column_definition(column->name());
        if (!def) {
            handle_unrecognized_ordering_column(column);
        }
        auto index = selection->index_of(*def);
        if (index < 0) {
            index = selection->add_column_for_ordering(*def);
        }

        sorters.emplace_back(index, def->type);
    }

    return [sorters = std::move(sorters)] (const result_row_type& r1, const result_row_type& r2) mutable {
        for (auto&& e : sorters) {
            auto& c1 = r1[e.first];
            auto& c2 = r2[e.first];
            auto type = e.second;

            if (bool(c1) != bool(c2)) {
                return bool(c2);
            }
            if (c1) {
                int result = type->compare(*c1, *c2);
                if (result != 0) {
                    return result < 0;
                }
            }
        }
        return false;
    };
}

bool select_statement::is_reversed(schema_ptr schema) {
    assert(_parameters->orderings().size() > 0);
    parameters::orderings_type::size_type i = 0;
    bool is_reversed_ = false;
    bool relation_order_unsupported = false;

    for (auto&& e : _parameters->orderings()) {
        ::shared_ptr<column_identifier> column = e.first->prepare_column_identifier(schema);
        bool reversed = e.second;

        auto def = schema->get_column_definition(column->name());
        if (!def) {
            handle_unrecognized_ordering_column(column);
        }

        if (!def->is_clustering_key()) {
            throw exceptions::invalid_request_exception(sprint(
                "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", *column));
        }

        if (i != def->component_index()) {
            throw exceptions::invalid_request_exception(
                "Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY");
        }

        bool current_reverse_status = (reversed != def->type->is_reversed());

        if (i == 0) {
            is_reversed_ = current_reverse_status;
        }

        if (is_reversed_ != current_reverse_status) {
            relation_order_unsupported = true;
        }
        ++i;
    }

    if (relation_order_unsupported) {
        throw exceptions::invalid_request_exception("Unsupported order by relation");
    }

    return is_reversed_;
}

/** If ALLOW FILTERING was not specified, this verifies that it is not needed */
void select_statement::check_needs_filtering(::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    // non-key-range non-indexed queries cannot involve filtering underneath
    if (!_parameters->allow_filtering() && (restrictions->is_key_range() || restrictions->uses_secondary_indexing())) {
        // We will potentially filter data if either:
        //  - Have more than one IndexExpression
        //  - Have no index expression and the column filter is not the identity
        if (restrictions->need_filtering()) {
            throw exceptions::invalid_request_exception(
                "Cannot execute this query as it might involve data filtering and "
                    "thus may have unpredictable performance. If you want to execute "
                    "this query despite the performance unpredictability, use ALLOW FILTERING");
        }
    }
}

bool select_statement::contains_alias(::shared_ptr<column_identifier> name) {
    return std::any_of(_select_clause.begin(), _select_clause.end(), [name] (auto raw) {
        return raw->alias && *name == *raw->alias;
    });
}

::shared_ptr<column_specification> select_statement::limit_receiver() {
    return ::make_shared<column_specification>(keyspace(), column_family(), ::make_shared<column_identifier>("[limit]", true),
        int32_type);
}

}

}

namespace util {

shared_ptr<cql3::statements::raw::select_statement> build_select_statement(
            const sstring_view& cf_name,
            const sstring_view& where_clause,
            std::vector<sstring_view> included_columns) {
    std::ostringstream out;
    out << "SELECT ";
    if (included_columns.empty()) {
        out << "*";
    } else {
        out << join(", ", included_columns);
    }
    out << " FROM " << cf_name << " WHERE " << where_clause << " ALLOW FILTERING";
    return do_with_parser(out.str(), std::mem_fn(&cql3_parser::CqlParser::selectStatement));
}

}

}
