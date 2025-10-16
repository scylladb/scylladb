/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "cql3/statements/select_statement.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/prune_materialized_view_statement.hh"
#include "cql3/statements/strongly_consistent_select_statement.hh"

#include "exceptions/exceptions.hh"
#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>
#include "service/broadcast_tables/experimental/lang.hh"
#include "service/qos/qos_common.hh"
#include "vector_search/vector_store_client.hh"
#include "transport/messages/result_message.hh"
#include "cql3/functions/as_json_function.hh"
#include "cql3/selection/selection.hh"
#include "cql3/util.hh"
#include "cql3/restrictions/statement_restrictions.hh"

#include "types/vector.hh"
#include "validation.hh"
#include "exceptions/unrecognized_entity_exception.hh"
#include <optional>
#include <ranges>
#include <variant>
#include <seastar/core/shared_ptr.hh>
#include "query/query-result-reader.hh"
#include "query/query_result_merger.hh"
#include "service/pager/query_pagers.hh"
#include "service/storage_proxy.hh"
#include <seastar/core/execution_stage.hh>
#include <seastar/core/on_internal_error.hh>
#include "db/timeout_clock.hh"
#include "db/consistency_level_validations.hh"
#include "data_dictionary/data_dictionary.hh"
#include "test/lib/select_statement_utils.hh"
#include "gms/feature_service.hh"
#include "utils/assert.hh"
#include "utils/result_combinators.hh"
#include "utils/result_loop.hh"
#include "replica/database.hh"
#include "replica/mutation_dump.hh"
#include "db/config.hh"
#include "view_info.hh"
#include "indexed_table_select_statement.hh"

template<typename T = void>
using coordinator_result = cql3::statements::select_statement::coordinator_result<T>;

bool is_internal_keyspace(std::string_view name);

namespace cql3 {

namespace statements {

static logging::logger logger("select_statement");

static constexpr int DEFAULT_INTERNAL_PAGING_SIZE = select_statement::DEFAULT_COUNT_PAGE_SIZE;
thread_local int internal_paging_size = DEFAULT_INTERNAL_PAGING_SIZE;
thread_local const lw_shared_ptr<const select_statement::parameters> select_statement::_default_parameters = make_lw_shared<select_statement::parameters>();

bool check_needs_allow_filtering_anyway(const restrictions::statement_restrictions& restrictions) {
    // Even if no filtering happens on the coordinator, we still warn about poor performance when partition
    // slice is defined but in potentially unlimited number of partitions (see #7608).
    return (restrictions.partition_key_restrictions_is_empty() || restrictions.has_token_restrictions()) // Potentially unlimited partitions.
    && restrictions.has_clustering_columns_restriction() // Slice defined.
    && !restrictions.uses_secondary_indexing(); // Base-table is used. (Index-table use always limits partitions.)
}

select_statement::parameters::parameters()
    : _is_distinct{false}
    , _allow_filtering{false}
    , _statement_subtype{statement_subtype::REGULAR}
{ }

select_statement::parameters::parameters(orderings_type orderings,
                                         bool is_distinct,
                                         bool allow_filtering)
    : _orderings{std::move(orderings)}
    , _is_distinct{is_distinct}
    , _allow_filtering{allow_filtering}
    , _statement_subtype{statement_subtype::REGULAR}
{ }

select_statement::parameters::parameters(orderings_type orderings,
                                         bool is_distinct,
                                         bool allow_filtering,
                                         statement_subtype statement_subtype,
                                         bool bypass_cache)
    : _orderings{std::move(orderings)}
    , _is_distinct{is_distinct}
    , _allow_filtering{allow_filtering}
    , _statement_subtype{statement_subtype}
    , _bypass_cache{bypass_cache}
{ }

bool select_statement::parameters::is_distinct() const {
    return _is_distinct;
}

bool select_statement::parameters::is_json() const {
   return _statement_subtype == statement_subtype::JSON;
}

bool select_statement::parameters::is_mutation_fragments() const {
   return _statement_subtype == statement_subtype::MUTATION_FRAGMENTS;
}

bool select_statement::parameters::allow_filtering() const {
    return _allow_filtering;
}

bool select_statement::parameters::bypass_cache() const {
    return _bypass_cache;
}

bool select_statement::parameters::is_prune_materialized_view() const {
    return _statement_subtype == statement_subtype::PRUNE_MATERIALIZED_VIEW;
}

select_statement::parameters::orderings_type const& select_statement::parameters::orderings() const {
    return _orderings;
}

timeout_config_selector
select_timeout(const restrictions::statement_restrictions& restrictions) {
    if (restrictions.is_key_range()) {
        return &timeout_config::range_read_timeout;
    } else {
        return &timeout_config::read_timeout;
    }
}

select_statement::select_statement(schema_ptr schema,
                                   uint32_t bound_terms,
                                   lw_shared_ptr<const parameters> parameters,
                                   ::shared_ptr<selection::selection> selection,
                                   ::shared_ptr<const restrictions::statement_restrictions> restrictions,
                                   ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
                                   bool is_reversed,
                                   ordering_comparator_type ordering_comparator,
                                   std::optional<expr::expression> limit,
                                   std::optional<expr::expression> per_partition_limit,
                                   cql_stats& stats,
                                   std::unique_ptr<attributes> attrs)
    : cql_statement(select_timeout(*restrictions))
    , _schema(schema)
    , _query_schema(is_reversed ? schema->get_reversed() : schema)
    , _bound_terms(bound_terms)
    , _parameters(std::move(parameters))
    , _selection(std::move(selection))
    , _restrictions(std::move(restrictions))
    , _restrictions_need_filtering(_restrictions->need_filtering())
    , _group_by_cell_indices(group_by_cell_indices)
    , _is_reversed(is_reversed)
    , _limit_unset_guard(limit)
    , _limit(std::move(limit))
    , _per_partition_limit_unset_guard(per_partition_limit)
    , _per_partition_limit(std::move(per_partition_limit))
    , _ordering_comparator(std::move(ordering_comparator))
    , _stats(stats)
    , _ks_sel(::is_internal_keyspace(schema->ks_name()) ? ks_selector::SYSTEM : ks_selector::NONSYSTEM)
    , _attrs(std::move(attrs))
{
    _opts = _selection->get_query_options();
    _opts.set_if<query::partition_slice::option::bypass_cache>(_parameters->bypass_cache());
    _opts.set_if<query::partition_slice::option::distinct>(_parameters->is_distinct());
    _opts.set_if<query::partition_slice::option::reversed>(_is_reversed);
}

db::timeout_clock::duration select_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    if (_attrs->is_timeout_set()) {
        return _attrs->get_timeout(options);
    }

    if (_attrs->is_service_level_set()) {
        auto slo = _attrs->get_service_level(state.get_service_level_controller());
        if (auto* timeout = std::get_if<lowres_clock::duration>(&slo.timeout)) {
            return *timeout;
        }
    }

    return state.get_timeout_config().*get_timeout_config_selector();
}

::shared_ptr<const cql3::metadata> select_statement::get_result_metadata() const {
    // FIXME: COUNT needs special result metadata handling.
    return _selection->get_result_metadata();
}

uint32_t select_statement::get_bound_terms() const {
    return _bound_terms;
}

future<> select_statement::check_access(query_processor& qp, const service::client_state& state) const {
    try {
        const data_dictionary::database db = qp.db();
        auto&& s = db.find_schema(keyspace(), column_family());
        auto cdc = db.get_cdc_base_table(*s);
        auto& cf_name = s->is_view()
            ? s->view_info()->base_name()
            : (cdc ? cdc->cf_name() : column_family());
        co_await state.has_column_family_access(keyspace(), cf_name, auth::permission::SELECT);
    } catch (const data_dictionary::no_such_column_family& e) {
        // Will be validated afterwards.
        co_return;
    }
    if (!_selection->is_trivial()) {
        std::vector<::shared_ptr<functions::function>> used_functions = _selection->used_functions();
        auto not_native = [] (::shared_ptr<functions::function> func) { return !func->is_native(); };
        for (const auto& used_function : used_functions | std::ranges::views::filter(not_native)) {
            sstring encoded_signature = auth::encode_signature(used_function->name().name, used_function->arg_types());
            co_await state.has_function_access(used_function->name().keyspace, encoded_signature, auth::permission::EXECUTE);
        }
    }
}

bool select_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return keyspace() == ks_name && (!cf_name || column_family() == *cf_name);
}

const sstring& select_statement::keyspace() const {
    return _schema->ks_name();
}

const sstring& select_statement::column_family() const {
    return _schema->cf_name();
}

query::partition_slice
select_statement::make_partition_slice(const query_options& options) const
{
    query::column_id_vector static_columns;
    query::column_id_vector regular_columns;

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
        return query::partition_slice({ query::clustering_range::make_open_ended_both_sides() },
            std::move(static_columns), {}, _opts, nullptr);
    }

    auto bounds =_restrictions->get_clustering_bounds(options);
    if (bounds.size() > 1) {
        auto comparer = position_in_partition::less_compare(*_schema);
        auto bounds_sorter = [&comparer] (const query::clustering_range& lhs, const query::clustering_range& rhs) {
            return comparer(position_in_partition_view::for_range_start(lhs), position_in_partition_view::for_range_start(rhs));
        };
        std::sort(bounds.begin(), bounds.end(), bounds_sorter);
    }
    if (_is_reversed) {
        std::reverse(bounds.begin(), bounds.end());
        for (auto& bound : bounds) {
            bound = query::reverse(bound);
        }
        ++_stats.reverse_queries;
    }

    const uint64_t per_partition_limit = get_inner_loop_limit(get_limit(options, _per_partition_limit, true),
        _selection->is_aggregate());
    return query::partition_slice(std::move(bounds),
        std::move(static_columns), std::move(regular_columns), _opts, nullptr, per_partition_limit);
}

uint64_t select_statement::get_limit(const query_options& options, const std::optional<expr::expression>& limit, bool is_per_partition_limit) const
{
    const auto& unset_guard = is_per_partition_limit ? _per_partition_limit_unset_guard : _limit_unset_guard;
    if (!limit.has_value() || unset_guard.is_unset(options)) {
        return query::max_rows;
    }
    try {
        auto val = expr::evaluate(*limit, options);
        if (val.is_null()) {
            throw exceptions::invalid_request_exception("Invalid null value of limit");
        }
        auto l = val.view().validate_and_deserialize<int32_t>(*int32_type);
        if (l <= 0) {
            auto msg = is_per_partition_limit ? "PER PARTITION LIMIT must be strictly positive" : "LIMIT must be strictly positive";
            throw exceptions::invalid_request_exception(msg);
        }
        return l;
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid limit value");
    }
}

uint64_t select_statement::get_inner_loop_limit(uint64_t limit, bool is_aggregate)
{
    if (is_aggregate) {
        return query::max_rows;
    }
    return limit;
}

bool select_statement::needs_post_query_ordering() const {
    // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
    return _restrictions->key_is_in_relation() && !_parameters->orderings().empty();
}

struct select_statement_executor {
    static auto get() { return &select_statement::do_execute; }
};

static thread_local inheriting_concrete_execution_stage<
        future<shared_ptr<cql_transport::messages::result_message>>,
        const select_statement*,
        query_processor&,
        service::query_state&,
        const query_options&> select_stage{"cql3_select", select_statement_executor::get()};

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::execute(query_processor& qp,
                             service::query_state& state,
                             const query_options& options,
                             std::optional<service::group0_guard> guard) const
{
    return execute_without_checking_exception_message(qp, state, options, std::move(guard))
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<cql_transport::messages::result_message>>);
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::execute_without_checking_exception_message(query_processor& qp,
                             service::query_state& state,
                             const query_options& options,
                             std::optional<service::group0_guard> guard) const
{
    return select_stage(this, seastar::ref(qp), seastar::ref(state), seastar::cref(options));
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::do_execute(query_processor& qp,
                          service::query_state& state,
                          const query_options& options) const
{
    (void)validation::validate_column_family(qp.db(), keyspace(), column_family());

    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    auto cl = options.get_consistency();

    validate_for_read(cl);

    const auto parsed_limit = get_limit(options, _limit);
    const uint64_t inner_loop_limit = get_inner_loop_limit(parsed_limit, _selection->is_aggregate());
    auto now = gc_clock::now();

    _stats.filtered_reads += _restrictions_need_filtering;

    const source_selector src_sel = state.get_client_state().is_internal()
            ? source_selector::INTERNAL : source_selector::USER;
    ++_stats.query_cnt(src_sel, _ks_sel, cond_selector::NO_CONDITIONS, statement_type::SELECT);

    _stats.select_bypass_caches += _parameters->bypass_cache();
    _stats.select_allow_filtering += _parameters->allow_filtering();
    _stats.select_partition_range_scan += _range_scan;
    _stats.select_partition_range_scan_no_bypass_cache += _range_scan_no_bypass_cache;

    auto slice = make_partition_slice(options);
    auto max_result_size = qp.proxy().get_max_result_size(slice);
    auto command = ::make_lw_shared<query::read_command>(
            _query_schema->id(),
            _query_schema->version(),
            std::move(slice),
            max_result_size,
            query::tombstone_limit(qp.proxy().get_tombstone_limit()),
            query::row_limit(inner_loop_limit),
            query::partition_limit(query::max_partitions),
            now,
            tracing::make_trace_info(state.get_trace_state()),
            query_id::create_null_id(),
            query::is_first_page::no,
            options.get_timestamp(state));
    command->allow_limit = db::allow_per_partition_rate_limit::yes;
    logger.trace("Executing read query (reversed {}): table schema {}, query schema {}",
        command->slice.is_reversed(), _schema->version(), _query_schema->version());
    tracing::trace(state.get_trace_state(), "Executing read query (reversed {})", command->slice.is_reversed());

    int32_t page_size = options.get_page_size();

    _stats.unpaged_select_queries(_ks_sel) += page_size <= 0;

    // An aggregation query may not be paged for the user, but we always page it internally to avoid OOM.
    // If the user provided a page_size we'll use that to page internally (because why not), otherwise we use our default
    // Also note: all GROUP BY queries are considered aggregation.
    const bool aggregate = _selection->is_aggregate() || has_group_by();
    const bool nonpaged_filtering = _restrictions_need_filtering && page_size <= 0;
    if (aggregate || nonpaged_filtering) {
        page_size = page_size <= 0 ? internal_paging_size : page_size;
    }

    auto key_ranges = _restrictions->get_partition_key_ranges(options);

    auto token = dht::token();
    std::optional<locator::tablet_routing_info> tablet_info = {};

    auto&& table = _schema->table();
    if (_may_use_token_aware_routing && table.uses_tablets() && state.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1)) {
        if (key_ranges.size() == 1 && query::is_single_partition(key_ranges.front())) {
            token = key_ranges[0].start()->value().as_decorated_key().token();

            auto erm = table.get_effective_replication_map();
            tablet_info = erm->check_locality(token);
        }
    }

    std::optional<service::cas_shard> cas_shard;

    if (db::is_serial_consistency(options.get_consistency())) {
        if (key_ranges.size() != 1 || !query::is_single_partition(key_ranges.front())) {
             throw exceptions::invalid_request_exception(
                     "SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time");
        }
        const auto token = key_ranges[0].start()->value().as_decorated_key().token();
        cas_shard.emplace(*_schema, token);
        if (!cas_shard->this_shard()) {
            return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                    qp.bounce_to_shard(cas_shard->shard(), std::move(const_cast<cql3::query_options&>(options).take_cached_pk_function_calls()))
                );
        }
    }

    auto f = make_ready_future<shared_ptr<cql_transport::messages::result_message>>();

    if (!aggregate && !_restrictions_need_filtering && (page_size <= 0
            || !service::pager::query_pagers::may_need_paging(*_query_schema, page_size,
                    *command, key_ranges))) {
        f = execute_without_checking_exception_message_non_aggregate_unpaged(qp, command, std::move(key_ranges), state, options, now, std::move(cas_shard));
    } else {
        f = execute_without_checking_exception_message_aggregate_or_paged(qp, command,
            std::move(key_ranges), state, options, now, page_size, aggregate,
            nonpaged_filtering, parsed_limit, std::move(cas_shard));
    }

    if (!tablet_info.has_value()) {
        return f;
    }

    return f.then([tablet_replicas = std::move(tablet_info->tablet_replicas), token_range = tablet_info->token_range] (auto res) mutable {
        res->add_tablet_info(std::move(tablet_replicas), token_range);
        return res;
    });
}

future<::shared_ptr<cql_transport::messages::result_message>>
select_statement::execute_without_checking_exception_message_aggregate_or_paged(query_processor& qp,
        lw_shared_ptr<query::read_command> command, dht::partition_range_vector&& key_ranges, service::query_state& state,
        const query_options& options, gc_clock::time_point now, int32_t page_size, bool aggregate, bool nonpaged_filtering,
        uint64_t limit, std::optional<service::cas_shard> cas_shard) const {
    command->slice.options.set<query::partition_slice::option::allow_short_read>();
    auto timeout_duration = get_timeout(state.get_client_state(), options);
    auto timeout = db::timeout_clock::now() + timeout_duration;
    auto p = service::pager::query_pagers::pager(qp.proxy(), _query_schema, _selection,
            state, options, command, std::move(key_ranges), _restrictions_need_filtering ? _restrictions : nullptr, std::move(cas_shard));

    auto per_partition_limit = get_limit(options, _per_partition_limit, true);

    if (aggregate || nonpaged_filtering) {
        auto builder = cql3::selection::result_set_builder(*_selection, now, &options, *_group_by_cell_indices, limit, per_partition_limit);
        coordinator_result<void> result_void = co_await utils::result_do_until(
                [&p, &builder, limit] {
                    return p->is_exhausted() || (limit < builder.result_set_size());
                },
                [&p, &builder, page_size, now, timeout] {
                    return p->fetch_page_result(builder, page_size, now, timeout);
                }
        );
        if (result_void.has_error()) {
            co_return failed_result_to_result_message(std::move(result_void));
        }
        co_return co_await builder.with_thread_if_needed([this, &p, &builder] {
            auto rs = builder.build();
            if (_restrictions_need_filtering) {
                _stats.filtered_rows_read_total += p->stats().rows_read_total;
                _stats.filtered_rows_matched_total += rs->size();
            }
            update_stats_rows_read(rs->size());
            auto msg = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
            return shared_ptr<cql_transport::messages::result_message>(std::move(msg));
        });
    }

    if (needs_post_query_ordering()) {
        throw exceptions::invalid_request_exception(
                "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                        " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");
    }

    if (_selection->is_trivial() && !_restrictions_need_filtering && !_per_partition_limit) {
        coordinator_result<result_generator> result_gen = co_await p->fetch_page_generator_result(page_size, now, timeout, _stats);
        if (result_gen.has_error()) {
            co_return failed_result_to_result_message(std::move(result_gen));
        }
        result_generator&& generator = std::move(result_gen).assume_value();
        auto meta = [&] () -> shared_ptr<const cql3::metadata> {
            if (!p->is_exhausted()) {
                auto meta = make_shared<metadata>(*_selection->get_result_metadata());
                meta->set_paging_state(p->state());
                return meta;
            } else {
                return _selection->get_result_metadata();
            }
        }();

        co_return shared_ptr<cql_transport::messages::result_message>(
            ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(generator), std::move(meta)))
        );
    }

    coordinator_result<std::unique_ptr<cql3::result_set>> result_rs = co_await p->fetch_page_result(page_size, now, timeout);
    if (result_rs.has_error()) {
        co_return failed_result_to_result_message(std::move(result_rs));
    }
    std::unique_ptr<cql3::result_set>&& rs = std::move(result_rs).assume_value();
    if (!p->is_exhausted()) {
        rs->get_metadata().set_paging_state(p->state());
    }

    if (_restrictions_need_filtering) {
        _stats.filtered_rows_read_total += p->stats().rows_read_total;
        _stats.filtered_rows_matched_total += rs->size();
    }
    update_stats_rows_read(rs->size());
    auto msg = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
    co_return msg;
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::execute_non_aggregate_unpaged(query_processor& qp,
                          lw_shared_ptr<query::read_command> cmd,
                          dht::partition_range_vector&& partition_ranges,
                          service::query_state& state,
                          const query_options& options,
                          gc_clock::time_point now) const
{
    return execute_without_checking_exception_message_non_aggregate_unpaged(qp, std::move(cmd), std::move(partition_ranges), state, options, now, {})
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<cql_transport::messages::result_message>>);
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::execute_without_checking_exception_message_non_aggregate_unpaged(query_processor& qp,
                          lw_shared_ptr<query::read_command> cmd,
                          dht::partition_range_vector&& partition_ranges,
                          service::query_state& state,
                          const query_options& options,
                          gc_clock::time_point now,
                          std::optional<service::cas_shard> cas_shard) const
{
    // If this is a query with IN on partition key, ORDER BY clause and LIMIT
    // is specified we need to get "limit" rows from each partition since there
    // is no way to tell which of these rows belong to the query result before
    // doing post-query ordering.
    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    if (needs_post_query_ordering() && _limit) {
        return do_with(std::forward<dht::partition_range_vector>(partition_ranges), [this, &qp, &state, &options, cmd, timeout, cas_shard = std::move(cas_shard)](auto& prs) {
            SCYLLA_ASSERT(cmd->partition_limit == query::max_partitions);
            query::result_merger merger(cmd->get_row_limit() * prs.size(), query::max_partitions);
            return utils::result_map_reduce(prs.begin(), prs.end(), [this, &qp, &state, &options, cmd, timeout, cas_shard = std::move(cas_shard)] (auto& pr) {
                dht::partition_range_vector prange { pr };
                auto command = ::make_lw_shared<query::read_command>(*cmd);
                return qp.proxy().query_result(_query_schema,
                        command,
                        std::move(prange),
                        options.get_consistency(),
                        {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state(), {}, {}, options.get_specific_options().node_local_only},
                        cas_shard).then(utils::result_wrap([] (service::storage_proxy::coordinator_query_result qr) {
                    return make_ready_future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>>(std::move(qr.query_result));
                }));
            }, std::move(merger));
        }).then(wrap_result_to_error_message([this, &options, now, cmd] (auto result) {
            return this->process_results(std::move(result), cmd, options, now);
        }));
    } else {
        return qp.proxy().query_result(_query_schema, cmd, std::move(partition_ranges), options.get_consistency(), {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state(), {}, {}, options.get_specific_options().node_local_only}, std::move(cas_shard))
            .then(wrap_result_to_error_message([this, &options, now, cmd] (service::storage_proxy::coordinator_query_result qr) {
                return this->process_results(std::move(qr.query_result), cmd, options, now);
            }));
    }
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::process_results(foreign_ptr<lw_shared_ptr<query::result>> results,
                                  lw_shared_ptr<query::read_command> cmd,
                                  const query_options& options,
                                  gc_clock::time_point now) const
{
    const bool fast_path = !needs_post_query_ordering() && _selection->is_trivial() && !_restrictions_need_filtering;
    if (fast_path) {
        return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(make_shared<cql_transport::messages::result_message::rows>(result(
            result_generator(_query_schema, std::move(results), std::move(cmd), _selection, _stats),
            _selection->get_result_metadata())
        ));
    }
    return process_results_complex(std::move(results), std::move(cmd), options, now);
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::process_results_complex(foreign_ptr<lw_shared_ptr<query::result>> results,
                                  lw_shared_ptr<query::read_command> cmd,
                                  const query_options& options,
                                  gc_clock::time_point now) const {
    cql3::selection::result_set_builder builder(*_selection, now, &options);
    co_return co_await builder.with_thread_if_needed([&] {
        if (_restrictions_need_filtering) {
            results->ensure_counts();
            _stats.filtered_rows_read_total += *results->row_count();
            query::result_view::consume(*results, cmd->slice,
                    cql3::selection::result_set_builder::visitor(builder, *_query_schema,
                            *_selection, cql3::selection::result_set_builder::restrictions_filter(_restrictions, options, cmd->get_row_limit(), _query_schema, cmd->slice.partition_row_limit())));
        } else {
            query::result_view::consume(*results, cmd->slice,
                    cql3::selection::result_set_builder::visitor(builder, *_query_schema,
                            *_selection));
        }
        auto rs = builder.build();

        if (needs_post_query_ordering()) {
            rs->sort(_ordering_comparator);
            if (_is_reversed) {
                rs->reverse();
            }
            rs->trim(cmd->get_row_limit());
        }
        update_stats_rows_read(rs->size());
        _stats.filtered_rows_matched_total += _restrictions_need_filtering ? rs->size() : 0;
        return shared_ptr<cql_transport::messages::result_message>(::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs))));
    });
}

const ::shared_ptr<const restrictions::statement_restrictions> select_statement::get_restrictions() const {
    return _restrictions;
}

primary_key_select_statement::primary_key_select_statement(schema_ptr schema, uint32_t bound_terms,
                                                           lw_shared_ptr<const parameters> parameters,
                                                           ::shared_ptr<selection::selection> selection,
                                                           ::shared_ptr<const restrictions::statement_restrictions> restrictions,
                                                           ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
                                                           bool is_reversed,
                                                           ordering_comparator_type ordering_comparator,
                                                           std::optional<expr::expression> limit,
                                                           std::optional<expr::expression> per_partition_limit,
                                                           cql_stats &stats,
                                                           std::unique_ptr<attributes> attrs)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices, is_reversed, ordering_comparator, std::move(limit), std::move(per_partition_limit), stats, std::move(attrs)}
{
    if (_ks_sel == ks_selector::NONSYSTEM) {
        if (_restrictions->need_filtering() ||
                _restrictions->partition_key_restrictions_is_empty() ||
                (_restrictions->has_token_restrictions() &&
                 !find(_restrictions->get_partition_key_restrictions(), expr::oper_t::EQ))) {
            _range_scan = true;
            if (!_parameters->bypass_cache())
                _range_scan_no_bypass_cache = true;
        }
    }
}

class parallelized_select_statement : public select_statement {
public:
    static ::shared_ptr<cql3::statements::select_statement> prepare(
        schema_ptr schema,
        uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection,
        ::shared_ptr<restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
        bool is_reversed,
        ordering_comparator_type ordering_comparator,
        std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit,
        cql_stats& stats,
        std::unique_ptr<cql3::attributes> attrs
    );

    parallelized_select_statement(
        schema_ptr schema,
        uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
        bool is_reversed,
        ordering_comparator_type ordering_comparator,
        std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit,
        cql_stats& stats,
        std::unique_ptr<cql3::attributes> attrs
    );

private:
    virtual future<::shared_ptr<cql_transport::messages::result_message>> do_execute(
        query_processor& qp,
        service::query_state& state,
        const query_options& options
    ) const override;
};

::shared_ptr<cql3::statements::select_statement> parallelized_select_statement::prepare(
    schema_ptr schema,
    uint32_t bound_terms,
    lw_shared_ptr<const select_statement::parameters> parameters,
    ::shared_ptr<selection::selection> selection,
    ::shared_ptr<restrictions::statement_restrictions> restrictions,
    ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
    bool is_reversed,
    parallelized_select_statement::ordering_comparator_type ordering_comparator,
    std::optional<expr::expression> limit,
    std::optional<expr::expression> per_partition_limit,
    cql_stats& stats,
    std::unique_ptr<cql3::attributes> attrs
) {
    return ::make_shared<cql3::statements::parallelized_select_statement>(
        schema,
        bound_terms,
        std::move(parameters),
        std::move(selection),
        std::move(restrictions),
        std::move(group_by_cell_indices),
        is_reversed,
        std::move(ordering_comparator),
        std::move(limit),
        std::move(per_partition_limit),
        stats,
        std::move(attrs)
    );
}

parallelized_select_statement::parallelized_select_statement(
    schema_ptr schema,
    uint32_t bound_terms,
    lw_shared_ptr<const parallelized_select_statement::parameters> parameters,
    ::shared_ptr<selection::selection> selection,
    ::shared_ptr<const restrictions::statement_restrictions> restrictions,
    ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
    bool is_reversed,
    parallelized_select_statement::ordering_comparator_type ordering_comparator,
    std::optional<expr::expression> limit,
    std::optional<expr::expression> per_partition_limit,
    cql_stats& stats,
    std::unique_ptr<cql3::attributes> attrs
) : select_statement(
    schema,
    bound_terms,
    std::move(parameters),
    std::move(selection),
    std::move(restrictions),
    std::move(group_by_cell_indices),
    is_reversed,
    std::move(ordering_comparator),
    std::move(limit),
    std::move(per_partition_limit),
    stats,
    std::move(attrs)
) {
}

future<::shared_ptr<cql_transport::messages::result_message>>
parallelized_select_statement::do_execute(
    query_processor& qp,
    service::query_state& state,
    const query_options& options
) const {
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    auto cl = options.get_consistency();
    validate_for_read(cl);

    auto now = gc_clock::now();

    const source_selector src_sel = state.get_client_state().is_internal()
            ? source_selector::INTERNAL : source_selector::USER;
    ++_stats.query_cnt(src_sel, _ks_sel, cond_selector::NO_CONDITIONS, statement_type::SELECT);

    _stats.select_bypass_caches += _parameters->bypass_cache();
    _stats.select_allow_filtering += _parameters->allow_filtering();
    _stats.select_partition_range_scan += _range_scan;
    _stats.select_partition_range_scan_no_bypass_cache += _range_scan_no_bypass_cache;
    _stats.select_parallelized += 1;

    auto slice = make_partition_slice(options);
    auto command = ::make_lw_shared<query::read_command>(
        _schema->id(),
        _schema->version(),
        std::move(slice),
        qp.proxy().get_max_result_size(slice),
        query::tombstone_limit(qp.proxy().get_tombstone_limit()),
        query::row_limit(query::max_rows),
        query::partition_limit(query::max_partitions),
        now,
        tracing::make_trace_info(state.get_trace_state()),
        query_id::create_null_id(),
        query::is_first_page::no,
        options.get_timestamp(state)
    );
    auto key_ranges = _restrictions->get_partition_key_ranges(options);

    if (db::is_serial_consistency(options.get_consistency())) {
        throw exceptions::invalid_request_exception(
            "SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time"
        );
    }

    command->slice.options.set<query::partition_slice::option::allow_short_read>();
    auto timeout_duration = get_timeout(state.get_client_state(), options);
    auto timeout = lowres_system_clock::now() + timeout_duration;
    auto reductions = _selection->get_reductions();

    query::mapreduce_request req = {
        .reduction_types = reductions.types,
        .cmd = *command,
        .pr = std::move(key_ranges),
        .cl = options.get_consistency(),
        .timeout = timeout,
        .aggregation_infos = reductions.infos,
    };

    // dispatch execution of this statement to other nodes
    return qp.mapreduce(req, state.get_trace_state()).then([this] (query::mapreduce_result res) {
        auto meta = _selection->get_result_metadata();
        auto rs = std::make_unique<result_set>(std::move(meta));
        rs->add_row(res.query_results);
        update_stats_rows_read(rs->size());
        return shared_ptr<cql_transport::messages::result_message>(
            make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)))
        );
    });
}

mutation_fragments_select_statement::mutation_fragments_select_statement(
            schema_ptr output_schema,
            schema_ptr underlying_schema,
            uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
            bool is_reversed,
            ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit,
            cql_stats &stats,
            std::unique_ptr<cql3::attributes> attrs)
    : select_statement(
            std::move(output_schema),
            bound_terms,
            std::move(parameters),
            std::move(selection),
            std::move(restrictions),
            std::move(group_by_cell_indices),
            is_reversed,
            std::move(ordering_comparator),
            std::move(limit),
            std::move(per_partition_limit),
            stats,
            std::move(attrs))
    , _underlying_schema(std::move(underlying_schema))
{ }

schema_ptr mutation_fragments_select_statement::generate_output_schema(schema_ptr underlying_schema) {
    return replica::mutation_dump::generate_output_schema_from_underlying_schema(std::move(underlying_schema));
}

future<exceptions::coordinator_result<service::storage_proxy_coordinator_query_result>>
mutation_fragments_select_statement::do_query(
        locator::effective_replication_map_ptr erm_keepalive,
        locator::host_id this_node,
        service::storage_proxy& sp,
        schema_ptr schema,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector partition_ranges,
        db::consistency_level cl,
        service::storage_proxy_coordinator_query_options optional_params) const {
    auto res = co_await replica::mutation_dump::dump_mutations(sp.get_db(), std::move(erm_keepalive), schema, _underlying_schema, partition_ranges, *cmd, optional_params.timeout(sp));
    service::replicas_per_token_range last_replicas;
    if (this_node) {
        last_replicas.emplace(dht::token_range::make_open_ended_both_sides(), std::vector<locator::host_id>{this_node});
    }
    co_return service::storage_proxy_coordinator_query_result{std::move(res), std::move(last_replicas), {}};
}

future<::shared_ptr<cql_transport::messages::result_message>>
mutation_fragments_select_statement::do_execute(query_processor& qp, service::query_state& state, const query_options& options) const {
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    auto cl = options.get_consistency();

    const uint64_t limit = get_inner_loop_limit(get_limit(options, _limit), _selection->is_aggregate());
    auto now = gc_clock::now();

    _stats.filtered_reads += _restrictions_need_filtering;

    const source_selector src_sel = state.get_client_state().is_internal()
            ? source_selector::INTERNAL : source_selector::USER;
    ++_stats.query_cnt(src_sel, _ks_sel, cond_selector::NO_CONDITIONS, statement_type::SELECT);

    _stats.select_bypass_caches += _parameters->bypass_cache();
    _stats.select_allow_filtering += _parameters->allow_filtering();
    _stats.select_partition_range_scan += _range_scan;
    _stats.select_partition_range_scan_no_bypass_cache += _range_scan_no_bypass_cache;

    auto slice = make_partition_slice(options);
    auto max_result_size = qp.proxy().get_max_result_size(slice);
    auto command = ::make_lw_shared<query::read_command>(
            _schema->id(),
            _schema->version(),
            std::move(slice),
            max_result_size,
            query::tombstone_limit(qp.proxy().get_tombstone_limit()),
            query::row_limit(limit),
            query::partition_limit(query::max_partitions),
            now,
            tracing::make_trace_info(state.get_trace_state()),
            query_id::create_null_id(),
            query::is_first_page::no,
            options.get_timestamp(state));
    command->allow_limit = db::allow_per_partition_rate_limit::yes;

    int32_t page_size = options.get_page_size();

    _stats.unpaged_select_queries(_ks_sel) += page_size <= 0;

    // An aggregation query will never be paged for the user, but we always page it internally to avoid OOM.
    // If we user provided a page_size we'll use that to page internally (because why not), otherwise we use our default
    // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
    // Also note: all GROUP BY queries are considered aggregation.
    const bool aggregate = _selection->is_aggregate() || has_group_by();
    const bool nonpaged_filtering = _restrictions_need_filtering && page_size <= 0;
    if (aggregate || nonpaged_filtering) {
        page_size = internal_paging_size;
    }

    auto key_ranges = _restrictions->get_partition_key_ranges(options);

    auto timeout_duration = get_timeout(state.get_client_state(), options);
    auto timeout = db::timeout_clock::now() + timeout_duration;

    auto& tbl = qp.proxy().local_db().find_column_family(_underlying_schema);

    // Since this query doesn't go through storage-proxy, we have to take care of pinning erm here.
    auto erm_keepalive = tbl.get_effective_replication_map();

    if (!aggregate && !_restrictions_need_filtering && (page_size <= 0
            || !service::pager::query_pagers::may_need_paging(*_schema, page_size,
                    *command, key_ranges))) {
        return do_query(erm_keepalive, {}, qp.proxy(), _schema, command, std::move(key_ranges), cl,
                {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state(), {}, {}})
        .then(wrap_result_to_error_message([this, erm_keepalive, now, &options, slice = command->slice] (service::storage_proxy_coordinator_query_result&& qr) mutable {
            cql3::selection::result_set_builder builder(*_selection, now, &options);
            query::result_view::consume(*qr.query_result, std::move(slice),
                    cql3::selection::result_set_builder::visitor(builder, *_schema, *_selection));
            auto msg = ::make_shared<cql_transport::messages::result_message::rows>(result(builder.build()));
            return ::shared_ptr<cql_transport::messages::result_message>(std::move(msg));
        }));
    }

    locator::host_id this_node;
    {
        auto& topo = erm_keepalive->get_topology();
        this_node = topo.this_node()->host_id();
        auto state = options.get_paging_state();
        if (state && !state->get_last_replicas().empty()) {
            auto last_host = state->get_last_replicas().begin()->second.front();
            if (last_host != this_node) {
                const auto last_node = topo.find_node(last_host);
                throw exceptions::invalid_request_exception(seastar::format(
                            "Moving between coordinators is not allowed in SELECT FROM MUTATION_FRAGMENTS() statements, last page's coordinator was {}{}",
                            last_host,
                            last_node ? fmt::format("({})", last_node->host_id()) : ""));
            }
        }
    }

    command->slice.options.set<query::partition_slice::option::allow_short_read>();
    auto p = service::pager::query_pagers::pager(
            qp.proxy(),
            _schema,
            _selection,
            state,
            options,
            command,
            std::move(key_ranges),
            _restrictions_need_filtering ? _restrictions : nullptr,
            std::nullopt,
            [this, erm_keepalive, this_node] (service::storage_proxy& sp, schema_ptr schema, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector partition_ranges,
                    db::consistency_level cl, service::storage_proxy_coordinator_query_options optional_params, std::optional<service::cas_shard>) mutable {
                return do_query(std::move(erm_keepalive), this_node, sp, std::move(schema), std::move(cmd), std::move(partition_ranges), cl, std::move(optional_params));
            });

    if (_selection->is_trivial() && !_restrictions_need_filtering && !_per_partition_limit) {
        return p->fetch_page_generator_result(page_size, now, timeout, _stats).then(wrap_result_to_error_message([this, p = std::move(p)] (result_generator&& generator) {
            auto meta = [&] () -> shared_ptr<const cql3::metadata> {
                if (!p->is_exhausted()) {
                    auto meta = make_shared<metadata>(*_selection->get_result_metadata());
                    meta->set_paging_state(p->state());
                    return meta;
                } else {
                    return _selection->get_result_metadata();
                }
            }();

            return shared_ptr<cql_transport::messages::result_message>(
                make_shared<cql_transport::messages::result_message::rows>(result(std::move(generator), std::move(meta)))
            );
        }));
    }

    return p->fetch_page_result(page_size, now, timeout).then(wrap_result_to_error_message(
            [this, p = std::move(p)](std::unique_ptr<cql3::result_set>&& rs) {
                if (!p->is_exhausted()) {
                    rs->get_metadata().set_paging_state(p->state());
                }

                if (_restrictions_need_filtering) {
                    _stats.filtered_rows_read_total += p->stats().rows_read_total;
                    _stats.filtered_rows_matched_total += rs->size();
                }
                update_stats_rows_read(rs->size());
                auto msg = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
                return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(std::move(msg));
            }));
}

namespace raw {

static void validate_attrs(const cql3::attributes::raw& attrs) {
    SCYLLA_ASSERT(!attrs.timestamp.has_value());
    SCYLLA_ASSERT(!attrs.time_to_live.has_value());
}

audit::statement_category select_statement::category() const {
    return audit::statement_category::QUERY;
}

select_statement::select_statement(cf_name cf_name,
                                   lw_shared_ptr<const parameters> parameters,
                                   std::vector<::shared_ptr<selection::raw_selector>> select_clause,
                                   expr::expression where_clause,
                                   std::optional<expr::expression> limit,
                                   std::optional<expr::expression> per_partition_limit,
                                   std::vector<::shared_ptr<cql3::column_identifier::raw>> group_by_columns,
                                   std::unique_ptr<attributes::raw> attrs)
    : cf_statement(cf_name)
    , _parameters(std::move(parameters))
    , _select_clause(std::move(select_clause))
    , _where_clause(std::move(where_clause))
    , _limit(std::move(limit))
    , _per_partition_limit(std::move(per_partition_limit))
    , _group_by_columns(std::move(group_by_columns))
    , _attrs(std::move(attrs))
{
    validate_attrs(*_attrs);
}

std::vector<selection::prepared_selector>
select_statement::maybe_jsonize_select_clause(std::vector<selection::prepared_selector> prepared_selectors, data_dictionary::database db, schema_ptr schema) {
    // Fill wildcard clause with explicit column identifiers for as_json function
    if (_parameters->is_json()) {
        if (prepared_selectors.empty()) {
            prepared_selectors.reserve(schema->all_columns().size());
            for (const column_definition& column_def : schema->all_columns_in_select_order()) {
                prepared_selectors.push_back(selection::prepared_selector{
                    .expr = expr::column_value{&column_def},
                });
            }
        }

        // Prepare selector names + types for as_json function
        std::vector<sstring> selector_names;
        std::vector<data_type> selector_types;
        std::vector<const column_definition*> defs;
        selector_names.reserve(prepared_selectors.size());
        selector_types.reserve(prepared_selectors.size());
        for (auto&& [sel, alias] : prepared_selectors) {
            if (alias) {
                selector_names.push_back(alias->to_string());
            } else {
                selector_names.push_back(fmt::format("{:result_set_metadata}", sel));
            }
            selector_types.push_back(expr::type_of(sel));
        }

        // Prepare args for as_json_function
        std::vector<expr::expression> args;
        args.reserve(prepared_selectors.size());
        for (const auto& prepared_selector : prepared_selectors) {
            args.push_back(std::move(prepared_selector.expr));
        }
        auto as_json = ::make_shared<functions::as_json_function>(std::move(selector_names), std::move(selector_types));
        auto as_json_selector = selection::prepared_selector{.expr = expr::function_call{as_json, std::move(args)}, .alias = nullptr};
        prepared_selectors.clear();
        prepared_selectors.push_back(as_json_selector);
    }
    return prepared_selectors;
}

static
bool
group_by_references_clustering_keys(const selection::selection& sel, const std::vector<size_t>& group_by_cell_indices) {
    return std::ranges::any_of(group_by_cell_indices, [&] (size_t idx) {
        return sel.get_columns()[idx]->kind == column_kind::clustering_key;
    });
}

std::unique_ptr<prepared_statement> select_statement::prepare(data_dictionary::database db, cql_stats& stats, bool for_view) {
    schema_ptr underlying_schema = validation::validate_column_family(db, keyspace(), column_family());
    schema_ptr schema = _parameters->is_mutation_fragments() ? mutation_fragments_select_statement::generate_output_schema(underlying_schema) : underlying_schema;
    prepare_context& ctx = get_prepare_context();

    auto prepared_selectors = selection::raw_selector::to_prepared_selectors(_select_clause, *schema, db, keyspace());

    prepared_selectors = maybe_jsonize_select_clause(std::move(prepared_selectors), db, schema);

    auto aggregation_depth = 0u;

    // Force aggregation if GROUP BY is used. This will wrap every column x as first(x).
    if (!_group_by_columns.empty()) {
        aggregation_depth = std::max(aggregation_depth, 1u);
        if (prepared_selectors.empty()) {
            // We have a "SELECT * GROUP BY". If we leave prepared_selectors
            // empty, below we choose selection::wildcard() for SELECT *, and
            // forget to do the "levellize" trick needed for the GROUP BY.
            // So we need to set prepared_selectors. See #16531.
            auto all_columns = selection::selection::wildcard_columns(schema);
            std::vector<::shared_ptr<selection::raw_selector>> select_all;
            select_all.reserve(all_columns.size());
            for (const column_definition *cdef : all_columns) {
                auto name = ::make_shared<cql3::column_identifier::raw>(cdef->name_as_text(), true);
                select_all.push_back(::make_shared<selection::raw_selector>(
                    expr::unresolved_identifier(std::move(name)), nullptr));
            }
            prepared_selectors = selection::raw_selector::to_prepared_selectors(select_all, *schema, db, keyspace());
        }
    }

    for (auto& ps : prepared_selectors) {
        expr::fill_prepare_context(ps.expr, ctx);
    }

    for (auto& ps : prepared_selectors) {
        aggregation_depth = std::max(aggregation_depth, expr::aggregation_depth(ps.expr));
    }
    if (aggregation_depth > 1) {
        throw exceptions::invalid_request_exception("SELECT clause contains aggeregation of an aggregation");
    }

    auto levellized_prepared_selectors = prepared_selectors;

    for (auto& ps : levellized_prepared_selectors) {
        ps.expr = levellize_aggregation_depth(ps.expr, aggregation_depth);
    }

    auto selection = prepared_selectors.empty()
                     ? selection::selection::wildcard(schema)
                     : selection::selection::from_selectors(db, schema, keyspace(), levellized_prepared_selectors);

    // Cassandra 5.0.2 disallows PER PARTITION LIMIT with aggregate queries
    // but only if GROUP BY is not used.
    // See #9879 for more details.
    if (selection->is_aggregate() && _per_partition_limit && _group_by_columns.empty()) {
        throw exceptions::invalid_request_exception("PER PARTITION LIMIT is not allowed with aggregate queries.");
    }

    auto restrictions = prepare_restrictions(db, schema, ctx, selection, for_view, _parameters->allow_filtering(),
            restrictions::check_indexes(!_parameters->is_mutation_fragments()));

    if (_parameters->is_distinct()) {
        validate_distinct_selection(*schema, *selection, *restrictions);
    }

    select_statement::ordering_comparator_type ordering_comparator;
    bool is_reversed_ = false;

    std::optional<prepared_ann_ordering_type> prepared_ann_ordering;

    auto orderings = _parameters->orderings();

    if (!orderings.empty()) {
        std::visit([&](auto&& ordering) {
            using T = std::decay_t<decltype(ordering)>;
            if constexpr (std::is_same_v<T, select_statement::ann_vector>) {
                prepared_ann_ordering = prepare_ann_ordering(*schema, ctx, db);
            } else {
                SCYLLA_ASSERT(!for_view);
                verify_ordering_is_allowed(*_parameters, *restrictions);
                prepared_orderings_type prepared_orderings = prepare_orderings(*schema);
                verify_ordering_is_valid(prepared_orderings, *schema, *restrictions);

                ordering_comparator = get_ordering_comparator(prepared_orderings, *selection, *restrictions);
                is_reversed_ = is_ordering_reversed(prepared_orderings);
            }
        }, orderings.front().second);
    }

    std::vector<sstring> warnings;
    if (!prepared_ann_ordering.has_value()) {
        check_needs_filtering(*restrictions, db.get_config().strict_allow_filtering(), warnings);
        ensure_filtering_columns_retrieval(db, *selection, *restrictions);
    }
    auto group_by_cell_indices = ::make_shared<std::vector<size_t>>(prepare_group_by(*schema, *selection));

    if (_parameters->is_distinct() && group_by_references_clustering_keys(*selection, *group_by_cell_indices)) {
        throw exceptions::invalid_request_exception(
                "Grouping on clustering columns is not allowed for SELECT DISTINCT queries");
    }

    ::shared_ptr<cql3::statements::select_statement> stmt;
    auto prepared_attrs = _attrs->prepare(db, keyspace(), column_family());
    prepared_attrs->fill_prepare_context(ctx);

    auto all_aggregates = [] (const std::vector<selection::prepared_selector>& prepared_selectors) {
        return std::ranges::all_of(
            prepared_selectors | std::views::transform(std::mem_fn(&selection::prepared_selector::expr)),
            [] (const expr::expression& e) {
                auto fn_expr = expr::as_if<expr::function_call>(&e);
                if (!fn_expr) {
                    return false;
                }
                auto func = std::get_if<shared_ptr<functions::function>>(&fn_expr->func);
                if (!func) {
                    return false;
                }
                return (*func)->is_aggregate();
            }
        );
    };

    auto is_local_table = [&] {
        return underlying_schema->table().get_effective_replication_map()->get_replication_strategy().is_local();
    };

    // Used to determine if an execution of this statement can be parallelized
    // using `mapreduce_service`.
    auto can_be_mapreduced = [&] {
        return all_aggregates(prepared_selectors)   // Note: before we levellized aggregation depth
            && ( // SUPPORTED PARALLELIZATION
                 // All potential intermediate coordinators must support mapreduceing
                (db.features().parallelized_aggregation && selection->is_count())
                || (db.features().uda_native_parallelized_aggregation && selection->is_reducible())
            )
            && !restrictions->need_filtering()  // No filtering
            && group_by_cell_indices->empty()   // No GROUP BY
            && db.get_config().enable_parallelized_aggregation()
            && !is_local_table()
            && !( // Do not parallelize the request if it's single partition read
                restrictions->partition_key_restrictions_is_all_eq() 
                && restrictions->partition_key_restrictions_size() == schema->partition_key_size());
    };

    if (_parameters->is_prune_materialized_view()) {
        stmt = ::make_shared<cql3::statements::prune_materialized_view_statement>(
                schema,
                ctx.bound_variables_size(),
                _parameters,
                std::move(selection),
                std::move(restrictions),
                std::move(group_by_cell_indices),
                is_reversed_,
                std::move(ordering_comparator),
                prepare_limit(db, ctx, _limit),
                prepare_limit(db, ctx, _per_partition_limit),
                stats,
                std::move(prepared_attrs));
    } else if (_parameters->is_mutation_fragments()) {
        stmt = ::make_shared<cql3::statements::mutation_fragments_select_statement>(
                schema,
                underlying_schema,
                ctx.bound_variables_size(),
                _parameters,
                std::move(selection),
                std::move(restrictions),
                std::move(group_by_cell_indices),
                is_reversed_,
                std::move(ordering_comparator),
                prepare_limit(db, ctx, _limit),
                prepare_limit(db, ctx, _per_partition_limit),
                stats,
                std::move(prepared_attrs));
    } else if (prepared_ann_ordering) {
        stmt = vector_indexed_table_select_statement::prepare(db, schema, ctx.bound_variables_size(), _parameters, std::move(selection), std::move(restrictions),
                std::move(group_by_cell_indices), is_reversed_, std::move(ordering_comparator), std::move(*prepared_ann_ordering),
                prepare_limit(db, ctx, _limit), prepare_limit(db, ctx, _per_partition_limit), stats, std::move(prepared_attrs));
    } else if (restrictions->uses_secondary_indexing()) {
        stmt = view_indexed_table_select_statement::prepare(
                db,
                schema,
                ctx.bound_variables_size(),
                _parameters,
                std::move(selection),
                std::move(restrictions),
                std::move(group_by_cell_indices),
                is_reversed_,
                std::move(ordering_comparator),
                prepare_limit(db, ctx, _limit),
                prepare_limit(db, ctx, _per_partition_limit),
                stats,
                std::move(prepared_attrs));
    } else if (can_be_mapreduced()) {
        stmt = parallelized_select_statement::prepare(
            schema,
            ctx.bound_variables_size(),
            _parameters,
            std::move(selection),
            std::move(restrictions),
            std::move(group_by_cell_indices),
            is_reversed_,
            std::move(ordering_comparator),
            prepare_limit(db, ctx, _limit),
            prepare_limit(db, ctx, _per_partition_limit),
            stats,
            std::move(prepared_attrs)
        );
    } else if (service::broadcast_tables::is_broadcast_table_statement(keyspace(), column_family())) {
        stmt = ::make_shared<cql3::statements::strongly_consistent_select_statement>(
                schema,
                ctx.bound_variables_size(),
                _parameters,
                std::move(selection),
                std::move(restrictions),
                std::move(group_by_cell_indices),
                is_reversed_,
                std::move(ordering_comparator),
                prepare_limit(db, ctx, _limit),
                prepare_limit(db, ctx, _per_partition_limit),
                stats,
                std::move(prepared_attrs));
    } else {
        stmt = ::make_shared<cql3::statements::primary_key_select_statement>(
                schema,
                ctx.bound_variables_size(),
                _parameters,
                std::move(selection),
                std::move(restrictions),
                std::move(group_by_cell_indices),
                is_reversed_,
                std::move(ordering_comparator),
                prepare_limit(db, ctx, _limit),
                prepare_limit(db, ctx, _per_partition_limit),
                stats,
                std::move(prepared_attrs));
    }

    auto partition_key_bind_indices = ctx.get_partition_key_bind_indexes(*schema);

    stmt->_may_use_token_aware_routing = partition_key_bind_indices.size() != 0;
    return make_unique<prepared_statement>(audit_info(), std::move(stmt), ctx, std::move(partition_key_bind_indices), std::move(warnings));
}

::shared_ptr<restrictions::statement_restrictions>
select_statement::prepare_restrictions(data_dictionary::database db,
                                       schema_ptr schema,
                                       prepare_context& ctx,
                                       ::shared_ptr<selection::selection> selection,
                                       bool for_view,
                                       bool allow_filtering,
                                       restrictions::check_indexes do_check_indexes)
{
    try {
        return ::make_shared<restrictions::statement_restrictions>(restrictions::analyze_statement_restrictions(db, schema, statement_type::SELECT, _where_clause, ctx,
            selection->contains_only_static_columns(), for_view, allow_filtering, do_check_indexes));
    } catch (const exceptions::unrecognized_entity_exception& e) {
        if (contains_alias(e.entity)) {
            throw exceptions::invalid_request_exception(format("Aliases aren't allowed in the WHERE clause (name: '{}')", e.entity));
        }
        throw;
    }
}

/** Returns a expr::expression for the limit or nullopt if no limit is set */
std::optional<expr::expression>
select_statement::prepare_limit(data_dictionary::database db, prepare_context& ctx, const std::optional<expr::expression>& limit)
{
    if (!limit.has_value()) {
        return std::nullopt;
    }

    expr::expression prep_limit = prepare_expression(*limit, db, keyspace(), nullptr, limit_receiver());
    expr::verify_no_aggregate_functions(prep_limit, "LIMIT clause");
    expr::fill_prepare_context(prep_limit, ctx);
    return prep_limit;
}

void select_statement::verify_ordering_is_allowed(const parameters& params, const restrictions::statement_restrictions& restrictions)
{
    if (restrictions.uses_secondary_indexing()) {
        throw exceptions::invalid_request_exception("ORDER BY with 2ndary indexes is not supported.");
    }
    if (restrictions.is_key_range()) {
        throw exceptions::invalid_request_exception("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
    }
    if (params.is_mutation_fragments()) {
        throw exceptions::invalid_request_exception("ORDER BY is not supported in SELECT FROM MUTATION_FRAGMENTS() statements.");
    }
}

void select_statement::handle_unrecognized_ordering_column(const column_identifier& column) const
{
    if (contains_alias(column)) {
        throw exceptions::invalid_request_exception(format("Aliases are not allowed in order by clause ('{}')", column));
    }
    throw exceptions::invalid_request_exception(format("Order by on unknown column {}", column));
}

select_statement::prepared_orderings_type select_statement::prepare_orderings(const schema& schema) const {
    prepared_orderings_type prepared_orderings;
    prepared_orderings.reserve(_parameters->orderings().size());

    for (auto&& [column_id, column_ordering] : _parameters->orderings()) {
        ::shared_ptr<column_identifier> column = column_id->prepare_column_identifier(schema);

        const column_definition* def = schema.get_column_definition(column->name());
        if (def == nullptr) {
            handle_unrecognized_ordering_column(*column);
        }

        if (!def->is_clustering_key()) {
            throw exceptions::invalid_request_exception(format("Order by is currently only supported on the clustered columns of the PRIMARY KEY, got {}", *column));
        }

        prepared_orderings.emplace_back(def, column_ordering);
    }

    // Uncomment this to allow specifying ORDER BY columns in any order.
    // Right now specifying ORDER BY (c2 asc, c1 asc) is illegal, it can only be ORDER BY (c1 asc, c2 asc).
    //
    // std::sort(prepared_orderings.begin(), prepared_orderings.end(),
    //     [](const std::pair<const column_definition*, ordering>& a, const std::pair<const column_definition*, ordering>& b) {
    //         return a.first->component_index() < b.first->component_index();
    //     }
    // );

    return prepared_orderings;
}

// Checks whether this ordering causes select results on this column to be reversed.
// A clustering column can be ordered in the descending order in the table.
// Then specifying ascending order would cause the results of this column to be reverse in comparison to a standard select.
static bool are_column_select_results_reversed(const column_definition& column, select_statement::ordering_type column_ordering) {
    auto ordering = std::get_if<select_statement::ordering>(&column_ordering);
    SCYLLA_ASSERT(ordering);

    if (*ordering == select_statement::ordering::ascending) {
        return column.type->is_reversed();
    } else { // descending
        return !column.type->is_reversed();
    }
}

bool select_statement::is_ordering_reversed(const prepared_orderings_type& orderings) const {
    if (orderings.empty()) {
        return false;
    }

    return are_column_select_results_reversed(*orderings[0].first, orderings[0].second);
}

void select_statement::verify_ordering_is_valid(const prepared_orderings_type& orderings,
                                                const schema& schema,
                                                const restrictions::statement_restrictions& restrictions) const {
    if (orderings.empty()) {
        return;
    }

    // Verify that columns are in the same order as in schema definition
    for (size_t i = 0; i + 1 < orderings.size(); i++) {
        if (orderings[i].first->component_index() >= orderings[i + 1].first->component_index()) {
            throw exceptions::invalid_request_exception("Order by currently only supports the ordering of columns following their declared order in the PRIMARY KEY");
        }
    }

    // We only allow leaving select results unchanged or reversing them. Find out if they should be reversed.
    bool is_reversed = is_ordering_reversed(orderings);

    // Now verify that
    // 1. Each column in the specified clustering prefix either has an ordering, or an EQ restriction.
    //    When there is an EQ restriction ordering is not needed because only a single value is possible.
    // 2. The ordering leaves the selected rows unchanged or reverses the normal result.
    //    For that to happen the ordering must reverse all columns or none at all.
    uint32_t max_ck_index = orderings.back().first->component_index();
    auto orderings_iterator = orderings.begin();
    for (uint32_t ck_column_index = 0; ck_column_index <= max_ck_index; ck_column_index++) {
         if (orderings_iterator->first->component_index() == ck_column_index) {
            // We have ordering for this column, let's see if its reversing is right.
            const column_definition* cur_ck_column = orderings_iterator->first;
            bool is_cur_column_reversed = are_column_select_results_reversed(*cur_ck_column, orderings_iterator->second);
            if (is_cur_column_reversed != is_reversed) {
                throw exceptions::invalid_request_exception(
                    format("Unsupported order by relation - only reversing all columns is supported, "
                           "but column {} has opposite ordering", cur_ck_column->name_as_text()));
            }
            orderings_iterator++;
        } else {
            // We don't have ordering for this column. Check if there is an EQ restriction or fail with an error.
            const column_definition& cur_ck_column = schema.clustering_column_at(ck_column_index);
            if (!restrictions.has_eq_restriction_on_column(cur_ck_column)) {
                throw exceptions::invalid_request_exception(format(
                    "Unsupported order by relation - column {} doesn't have an ordering or EQ relation.",
                    cur_ck_column.name_as_text()));
            }
        }
    }
}

select_statement::prepared_ann_ordering_type select_statement::prepare_ann_ordering(const schema& schema, prepare_context& ctx, data_dictionary::database db) const {
    auto [column_id, ordering] = _parameters->orderings().front();
    const auto& ann_vector = std::get_if<select_statement::ann_vector>(&ordering);
    SCYLLA_ASSERT(ann_vector);

    ::shared_ptr<column_identifier> column = column_id->prepare_column_identifier(schema);
    const column_definition* def = schema.get_column_definition(column->name());
    if (!def) {
        throw exceptions::invalid_request_exception(
                fmt::format("Undefined column name {}", column->text()));
    }

    if (!def->type->is_vector() || static_cast<const vector_type_impl*>(def->type.get())->get_elements_type()->get_kind() != abstract_type::kind::float_kind) {
        throw exceptions::invalid_request_exception("ANN ordering is only supported on float vector indexes");
    }

    auto e =  expr::prepare_expression(*ann_vector, db, keyspace(), nullptr, def->column_specification);
    expr::fill_prepare_context(e, ctx);

    return std::make_pair(std::move(def), std::move(e));
}

select_statement::ordering_comparator_type select_statement::get_ordering_comparator(const prepared_orderings_type& orderings,
    selection::selection& selection,
    const restrictions::statement_restrictions& restrictions) {
    if (!restrictions.key_is_in_relation()) {
        return {};
    }

    // For the comparator we only need columns that have ordering defined.
    // Other columns are required to have an EQ restriction, so they will have no more than a single value.

    std::vector<std::pair<uint32_t, data_type>> sorters;
    sorters.reserve(orderings.size());

    // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting,
    // even if we don't
    // ultimately ship them to the client (CASSANDRA-4911).
    for (auto&& [column_def, is_descending] : orderings) {
        auto index = selection.index_of(*column_def);
        if (index < 0) {
            index = selection.add_column_for_post_processing(*column_def);
        }

        sorters.emplace_back(index, column_def->type);
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
                auto result = type->compare(*c1, *c2);
                if (result != 0) {
                    return result < 0;
                }
            }
        }
        return false;
    };
}

void select_statement::validate_distinct_selection(const schema& schema,
                                                   const selection::selection& selection,
                                                   const restrictions::statement_restrictions& restrictions) const
{
    if (_per_partition_limit) {
        throw exceptions::invalid_request_exception("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");
    }

    if (restrictions.has_non_primary_key_restriction() || restrictions.has_clustering_columns_restriction()) {
        throw exceptions::invalid_request_exception(
            "SELECT DISTINCT with WHERE clause only supports restriction by partition key.");
    }
    for (auto&& def : selection.get_columns()) {
        if (!def->is_partition_key() && !def->is_static()) {
            throw exceptions::invalid_request_exception(format("SELECT DISTINCT queries must only request partition key columns and/or static columns (not {})",
                def->name_as_text()));
        }
    }

    // If it's a key range, we require that all partition key columns are selected so we don't have to bother
    // with post-query grouping.
    if (!restrictions.is_key_range()) {
        return;
    }

    for (auto&& def : schema.partition_key_columns()) {
        if (!selection.has_column(def)) {
            throw exceptions::invalid_request_exception(format("SELECT DISTINCT queries must request all the partition key columns (missing {})", def.name_as_text()));
        }
    }
}

/// True iff restrictions require ALLOW FILTERING despite there being no coordinator-side filtering.
static bool needs_allow_filtering_anyway(
        const restrictions::statement_restrictions& restrictions,
        db::tri_mode_restriction_t::mode strict_allow_filtering,
        std::vector<sstring>& warnings) {
    using flag_t = db::tri_mode_restriction_t::mode;
    if (strict_allow_filtering == flag_t::FALSE) {
        return false;
    }
    if (check_needs_allow_filtering_anyway(restrictions)) {
        if (strict_allow_filtering == flag_t::WARN) {
            warnings.emplace_back("This query should use ALLOW FILTERING and will be rejected in future versions.");
            return false;
        }
        return true;
    }
    return false;
}

/** If ALLOW FILTERING was not specified, this verifies that it is not needed */
void select_statement::check_needs_filtering(
        const restrictions::statement_restrictions& restrictions,
        db::tri_mode_restriction_t::mode strict_allow_filtering,
        std::vector<sstring>& warnings)
{
    // non-key-range non-indexed queries cannot involve filtering underneath
    if (!_parameters->allow_filtering() && (restrictions.is_key_range() || restrictions.uses_secondary_indexing())) {
        if (restrictions.need_filtering() || needs_allow_filtering_anyway(restrictions, strict_allow_filtering, warnings)) {
            throw exceptions::invalid_request_exception(
                "Cannot execute this query as it might involve data filtering and "
                    "thus may have unpredictable performance. If you want to execute "
                    "this query despite the performance unpredictability, use ALLOW FILTERING");
        }
    }
}

/**
 * Adds columns that are needed for the purpose of filtering to the selection.
 * The columns that are added to the selection are columns that
 * are needed for filtering on the coordinator but are not part of the selection.
 * The columns are added with a meta-data indicating they are not to be returned
 * to the user.
 */
void select_statement::ensure_filtering_columns_retrieval(data_dictionary::database db,
                                        selection::selection& selection,
                                        const restrictions::statement_restrictions& restrictions) {
    for (auto&& cdef : restrictions.get_column_defs_for_filtering(db)) {
        if (!selection.has_column(*cdef)) {
            selection.add_column_for_post_processing(*cdef);
        }
    }
}

bool select_statement::contains_alias(const column_identifier& name) const {
    return std::any_of(_select_clause.begin(), _select_clause.end(), [&name] (auto raw) {
        return raw->alias && name == *raw->alias;
    });
}

lw_shared_ptr<column_specification> select_statement::limit_receiver(bool per_partition) {
    sstring name = per_partition ? "[per_partition_limit]" : "[limit]";
    return make_lw_shared<column_specification>(keyspace(), column_family(), ::make_shared<column_identifier>(name, true),
        int32_type);
}

namespace {

/// True iff one of \p relations is a single-column EQ involving \p def.
bool equality_restricted(
        const column_definition& def, const schema& schema, const expr::expression& relations) {
    for (const auto& relation : boolean_factors(relations)) {
        if (auto binop = expr::as_if<expr::binary_operator>(&relation)) {
            if (binop->op != expr::oper_t::EQ) {
                continue;
            }

            if (auto lhs_col_ident = expr::as_if<expr::unresolved_identifier>(&binop->lhs)) {
                const ::shared_ptr<column_identifier> lhs_cdef = lhs_col_ident->ident->prepare_column_identifier(schema);
                if (lhs_cdef->name() == def.name()) {
                    return true;
                }
            }

            if (auto lhs_col = expr::as_if<expr::column_value>(&binop->lhs)) {
                if (lhs_col->col->name() == def.name()) {
                    return true;
                }
            }
        }
    }
    return false;
}

/// Returns an exception to throw when \p col is out of order in GROUP BY.
auto make_order_exception(const column_identifier::raw& col) {
    return exceptions::invalid_request_exception(format("Group by column {} is out of order", col));
}

} // anonymous namespace

std::vector<size_t> select_statement::prepare_group_by(const schema& schema, selection::selection& selection) const {
    if (_group_by_columns.empty()) {
        return {};
    }

    std::vector<size_t> indices;

    // We compare GROUP BY columns to the primary-key columns (in their primary-key order).  If a
    // primary-key column is equality-restricted by the WHERE clause, it can be skipped in GROUP BY.
    // It's OK if GROUP BY columns list ends before the primary key is exhausted.

    const auto key_size = schema.partition_key_size() + schema.clustering_key_size();
    const auto all_columns = schema.all_columns_in_select_order();
    uint32_t expected_index = 0; // Index of the next column we expect to encounter.

    using exceptions::invalid_request_exception;
    for (const auto& col : _group_by_columns) {
        auto def = schema.get_column_definition(col->prepare_column_identifier(schema)->name());
        if (!def) {
            throw invalid_request_exception(format("Group by unknown column {}", *col));
        }
        if (!def->is_primary_key()) {
            throw invalid_request_exception(format("Group by non-primary-key column {}", *col));
        }
        if (expected_index >= key_size) {
            throw make_order_exception(*col);
        }
        while (*def != all_columns[expected_index]
               && equality_restricted(all_columns[expected_index], schema, _where_clause)) {
            if (++expected_index >= key_size) {
                throw make_order_exception(*col);
            }
        }
        if (*def != all_columns[expected_index]) {
            throw make_order_exception(*col);
        }
        ++expected_index;
        auto index = selection.index_of(*def);
        if (index == -1) {
            selection.add_column_for_post_processing(*def);
            index = selection.index_of(*def);
        }
        indices.push_back(index);
    }

    if (expected_index < schema.partition_key_size()) {
        throw invalid_request_exception(format("GROUP BY must include the entire partition key"));
    }

    return indices;
}

}

future<> set_internal_paging_size(int paging_size) {
    return seastar::smp::invoke_on_all([paging_size] {
        internal_paging_size = paging_size;
    });
}

future<> reset_internal_paging_size() {
    return set_internal_paging_size(DEFAULT_INTERNAL_PAGING_SIZE);
}

int get_internal_page_size() {
    return internal_paging_size;
}

}

namespace util {

std::unique_ptr<cql3::statements::raw::select_statement> build_select_statement(
            const std::string_view& cf_name,
            const std::string_view& where_clause,
            bool select_all_columns,
            const std::vector<column_definition>& selected_columns) {
    std::ostringstream out;
    out << "SELECT ";
    if (select_all_columns) {
        out << "*";
    } else {
        // If the column name is not entirely lowercase (or digits or _),
        // when output to CQL it must be quoted to preserve case as well
        // as non alphanumeric characters.
        auto cols = selected_columns
                | std::views::transform(std::mem_fn(&column_definition::name_as_cql_string))
                | std::ranges::to<std::vector>();
        fmt::print(out, "{}", fmt::join(cols, ", "));
    }
    // Note that cf_name may need to be quoted, just like column names above.
    out << " FROM " << util::maybe_quote(sstring(cf_name));
    if (!where_clause.empty()) {
        out << " WHERE " << where_clause << " ALLOW FILTERING";
    }
    // In general it's not a good idea to use the default dialect, but here the database is talking to
    // itself, so we can hope the dialects are mutually compatible here.
    return do_with_parser(out.str(), dialect{}, std::mem_fn(&cql3_parser::CqlParser::selectStatement));
}

}

}
