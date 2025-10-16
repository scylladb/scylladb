/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "indexed_table_select_statement.hh"
#include "index/secondary_index.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "db/consistency_level_validations.hh"
#include "partition_slice_builder.hh"
#include "utils/result_combinators.hh"
#include "utils/result_loop.hh"
#include "cql3/query_processor.hh"
#include "cql3/util.hh"
#include "cql3/statements/index_target.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/expr/evaluate.hh"
#include "service/pager/query_pagers.hh"
#include "service/storage_proxy.hh"
#include "query_ranges_to_vnodes.hh"
#include "view_info.hh"
#include "query/query_result_merger.hh"
#include "types/vector.hh"

namespace cql3 {
namespace statements {
namespace {

constexpr std::string_view ANN_CUSTOM_INDEX_OPTION = "vector_index";

std::vector<dht::partition_range> to_partition_ranges(const std::vector<primary_key>& pkeys) {
    std::vector<dht::partition_range> partition_ranges;
    std::ranges::transform(pkeys, std::back_inserter(partition_ranges), [](const auto& pkey) {
        return dht::partition_range::make_singular(pkey.partition);
    });

    return partition_ranges;
}

template <typename Func>
auto measure_index_latency(const schema& schema, const secondary_index::index& index, Func&& func) -> std::invoke_result_t<Func> {
    auto start_time = lowres_system_clock::now();
    auto result = co_await func();
    auto duration = lowres_system_clock::now() - start_time;

    auto stats = schema.table().get_index_manager().get_index_stats(index.metadata().name());
    if (stats) {
        stats->add_latency(duration);
    }

    co_return result;
}

}

template<typename KeyType>
requires (std::is_same_v<KeyType, partition_key> || std::is_same_v<KeyType, clustering_key_prefix>)
static KeyType
generate_base_key_from_index_pk(const partition_key& index_pk, const std::optional<clustering_key>& index_ck, const schema& base_schema, const schema& view_schema) {
    const auto& base_columns = std::is_same_v<KeyType, partition_key> ? base_schema.partition_key_columns() : base_schema.clustering_key_columns();

    // An empty key in the index paging state translates to an empty base key
    if (index_pk.is_empty() && !index_ck) {
        return KeyType::make_empty();
    }

    std::vector<managed_bytes_view> exploded_base_key;
    exploded_base_key.reserve(base_columns.size());

    for (const column_definition& base_col : base_columns) {
        const column_definition* view_col = view_schema.view_info()->view_column(base_col);
        if (!view_col) {
            throw std::runtime_error(format("Base key column not found in the view: {}", base_col.name_as_text()));
        }
        if (base_col.type->without_reversed() != view_col->type->without_reversed()) {
            throw std::runtime_error(format("Mismatched types for base and view columns {}: {} and {}",
                    base_col.name_as_text(), base_col.type->cql3_type_name(), view_col->type->cql3_type_name()));
        }
        if (view_col->is_partition_key()) {
            exploded_base_key.push_back(index_pk.get_component(view_schema, view_col->id));
        } else {
            if (!view_col->is_clustering_key()) {
                throw std::runtime_error(
                        format("Base primary key column {} is not a primary key column in the index (kind: {})",
                                view_col->name_as_text(), to_sstring(view_col->kind)));
            }
            if (!index_ck) {
                throw std::runtime_error(format("Column {} was expected to be provided "
                        "in the index clustering key, but the whole index clustering key is missing", view_col->name_as_text()));
            }
            exploded_base_key.push_back(index_ck->get_component(view_schema, view_col->id));
        }
    }
    return KeyType::from_range(exploded_base_key);
}

lw_shared_ptr<query::read_command>
view_indexed_table_select_statement::prepare_command_for_base_query(query_processor& qp, const query_options& options,
        service::query_state& state, gc_clock::time_point now, bool use_paging) const {
    auto slice = make_partition_slice(options);
    if (use_paging) {
        slice.options.set<query::partition_slice::option::allow_short_read>();
        slice.options.set<query::partition_slice::option::send_partition_key>();
        if (_schema->clustering_key_size() > 0) {
            slice.options.set<query::partition_slice::option::send_clustering_key>();
        }
    }
    lw_shared_ptr<query::read_command> cmd = ::make_lw_shared<query::read_command>(
            _schema->id(),
            _schema->version(),
            std::move(slice),
            qp.proxy().get_max_result_size(slice),
            query::tombstone_limit(qp.proxy().get_tombstone_limit()),
            query::row_limit(get_inner_loop_limit(get_limit(options, _limit), _selection->is_aggregate())),
            query::partition_limit(query::max_partitions),
            now,
            tracing::make_trace_info(state.get_trace_state()),
            query_id::create_null_id(),
            query::is_first_page::no,
            options.get_timestamp(state));
    cmd->allow_limit = db::allow_per_partition_rate_limit::yes;
    return cmd;
}

future<cql3::statements::select_statement::coordinator_result<std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, lw_shared_ptr<query::read_command>>>>
view_indexed_table_select_statement::do_execute_base_query(
        query_processor& qp,
        dht::partition_range_vector&& partition_ranges,
        service::query_state& state,
        const query_options& options,
        gc_clock::time_point now,
        lw_shared_ptr<const service::pager::paging_state> paging_state) const {
    using value_type = std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, lw_shared_ptr<query::read_command>>;
    auto cmd = prepare_command_for_base_query(qp, options, state, now, bool(paging_state));
    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    uint32_t queried_ranges_count = partition_ranges.size();
    auto&& table = qp.proxy().local_db().find_column_family(_schema);
    auto erm = table.get_effective_replication_map();
    query_ranges_to_vnodes_generator ranges_to_vnodes(erm->make_splitter(), _schema, std::move(partition_ranges));

    struct base_query_state {
        query::result_merger merger;
        query_ranges_to_vnodes_generator ranges_to_vnodes;
        size_t concurrency = 1;
        size_t previous_result_size = 0;
        base_query_state(uint64_t row_limit, query_ranges_to_vnodes_generator&& ranges_to_vnodes_)
                : merger(row_limit, query::max_partitions)
                , ranges_to_vnodes(std::move(ranges_to_vnodes_))
                {}
        base_query_state(base_query_state&&) = default;
        base_query_state(const base_query_state&) = delete;
    };

    const column_definition* target_cdef = _schema->get_column_definition(to_bytes(_index.target_column()));
    if (!target_cdef) {
        throw exceptions::invalid_request_exception("Indexed column not found in schema");
    }

    const bool is_paged = bool(paging_state);
    base_query_state query_state{cmd->get_row_limit() * queried_ranges_count, std::move(ranges_to_vnodes)};
    {
        auto& merger = query_state.merger;
        auto& ranges_to_vnodes = query_state.ranges_to_vnodes;
        auto& concurrency = query_state.concurrency;
        auto& previous_result_size = query_state.previous_result_size;
        query::short_read is_short_read = query::short_read::no;
        bool page_limit_reached = false;
        while (!is_short_read && !ranges_to_vnodes.empty() && !page_limit_reached) {
            // Starting with 1 range, we check if the result was a short read, and if not,
            // we continue exponentially, asking for 2x more ranges than before
            dht::partition_range_vector prange = ranges_to_vnodes(concurrency);
            auto command = ::make_lw_shared<query::read_command>(*cmd);
            auto old_paging_state = options.get_paging_state();
            if (old_paging_state && concurrency == 1) {
                auto base_pk = generate_base_key_from_index_pk<partition_key>(old_paging_state->get_partition_key(),
                        old_paging_state->get_clustering_key(), *_schema, *_view_schema);
                auto row_ranges = command->slice.default_row_ranges();
                if (old_paging_state->get_clustering_key() && _schema->clustering_key_size() > 0 && !target_cdef->is_static()) {
                    auto base_ck = generate_base_key_from_index_pk<clustering_key>(old_paging_state->get_partition_key(),
                            old_paging_state->get_clustering_key(), *_schema, *_view_schema);

                    query::trim_clustering_row_ranges_to(*_schema, row_ranges, base_ck);
                    command->slice.set_range(*_schema, base_pk, row_ranges);
                } else {
                    // There is no clustering key in old_paging_state and/or no clustering key in 
                    // _schema, therefore read an entire partition (whole clustering range).
                    //
                    // The only exception to applying no restrictions on clustering key
                    // is a case when we have a secondary index on the first column
                    // of clustering key. In such a case we should not read the
                    // entire clustering range - only a range in which first column
                    // of clustering key has the correct value. 
                    //
                    // This means that we should not set a open_ended_both_sides
                    // clustering range on base_pk, instead intersect it with
                    // _row_ranges (which contains the restrictions necessary for the
                    // case described above). The result of such intersection is just
                    // _row_ranges, which we explicitly set on base_pk.
                    command->slice.set_range(*_schema, base_pk, row_ranges);
                }
            }
            if (previous_result_size < query::result_memory_limiter::maximum_result_size && concurrency < max_base_table_query_concurrency) {
                concurrency *= 2;
            }
            coordinator_result<service::storage_proxy::coordinator_query_result> rqr = co_await qp.proxy().query_result(_schema, command, std::move(prange), options.get_consistency(), {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state()});
            if (!rqr.has_value()) {
                co_return std::move(rqr).as_failure();
            }
            auto& qr = rqr.value();
            is_short_read = qr.query_result->is_short_read();
            // Results larger than 1MB should be shipped to the client immediately
            page_limit_reached = is_paged && qr.query_result->buf().size() >= query::result_memory_limiter::maximum_result_size;
            previous_result_size = qr.query_result->buf().size();
            merger(std::move(qr.query_result));
        }
        co_return coordinator_result<value_type>(value_type(merger.get(), std::move(cmd)));
    }
}


future<shared_ptr<cql_transport::messages::result_message>>
view_indexed_table_select_statement::process_base_query_results(
        foreign_ptr<lw_shared_ptr<query::result>> results,
        lw_shared_ptr<query::read_command> cmd,
        service::query_state& state,
        const query_options& options,
        gc_clock::time_point now,
        lw_shared_ptr<const service::pager::paging_state> paging_state) const
{
    if (paging_state) {
        paging_state = generate_view_paging_state_from_base_query_results(paging_state, results, state, options);
        _selection->get_result_metadata()->maybe_set_paging_state(std::move(paging_state));
    }
    return process_results(std::move(results), std::move(cmd), options, now);
}

future<shared_ptr<cql_transport::messages::result_message>>
view_indexed_table_select_statement::execute_base_query(
        query_processor& qp,
        dht::partition_range_vector&& partition_ranges,
        service::query_state& state,
        const query_options& options,
        gc_clock::time_point now,
        lw_shared_ptr<const service::pager::paging_state> paging_state) const {
    return do_execute_base_query(qp, std::move(partition_ranges), state, options, now, paging_state).then(wrap_result_to_error_message(
            [this, &state, &options, now, paging_state = std::move(paging_state)] (std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, lw_shared_ptr<query::read_command>> result_and_cmd) {
        auto&& [result, cmd] = result_and_cmd;
        return process_base_query_results(std::move(result), std::move(cmd), state, options, now, std::move(paging_state));
    }));
}

future<cql3::statements::select_statement::coordinator_result<std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, lw_shared_ptr<query::read_command>>>>
view_indexed_table_select_statement::do_execute_base_query(
        query_processor& qp,
        std::vector<primary_key>&& primary_keys,
        service::query_state& state,
        const query_options& options,
        gc_clock::time_point now,
        lw_shared_ptr<const service::pager::paging_state> paging_state) const {
    using value_type = std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, lw_shared_ptr<query::read_command>>;
    auto cmd = prepare_command_for_base_query(qp, options, state, now, bool(paging_state));
    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);

    query::result_merger merger(cmd->get_row_limit(), query::max_partitions);
    std::vector<primary_key> keys = std::move(primary_keys);
    std::vector<primary_key>::iterator key_it(keys.begin());
    size_t previous_result_size = 0;
    size_t next_iteration_size = 0;

    const bool is_paged = bool(paging_state);
    while (key_it != keys.end()) {
        // Starting with 1 key, we check if the result was a short read, and if not,
        // we continue exponentially, asking for 2x more key than before
        auto already_done = std::distance(keys.begin(), key_it);
        // If the previous result already provided 1MB worth of data,
        // stop increasing the number of fetched partitions
        if (previous_result_size < query::result_memory_limiter::maximum_result_size) {
            next_iteration_size = already_done + 1;
        }
        next_iteration_size = std::min<size_t>({next_iteration_size, keys.size() - already_done, max_base_table_query_concurrency});
        auto key_it_end = key_it + next_iteration_size;

        query::result_merger oneshot_merger(cmd->get_row_limit(), query::max_partitions);
        coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>> rresult = co_await utils::result_map_reduce(key_it, key_it_end, coroutine::lambda([&] (auto& key)
                -> future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> {
            auto command = ::make_lw_shared<query::read_command>(*cmd);
            // for each partition, read just one clustering row (TODO: can
            // get all needed rows of one partition at once.)
            command->slice._row_ranges.clear();
            if (key.clustering) {
                command->slice._row_ranges.push_back(query::clustering_range::make_singular(key.clustering));
            }
            coordinator_result<service::storage_proxy::coordinator_query_result> rqr
                    = co_await qp.proxy().query_result(_schema, command, {dht::partition_range::make_singular(key.partition)}, options.get_consistency(), {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state()});
            if (!rqr.has_value()) {
                co_return std::move(rqr).as_failure();
            }
            co_return std::move(rqr.value().query_result);
        }), std::move(oneshot_merger));
        if (!rresult.has_value()) {
            co_return std::move(rresult).as_failure();
        }
        auto& result = rresult.value();
        auto is_short_read = result->is_short_read();
        // Results larger than 1MB should be shipped to the client immediately
        const bool page_limit_reached = is_paged && result->buf().size() >= query::result_memory_limiter::maximum_result_size;
        previous_result_size = result->buf().size();
        merger(std::move(result));
        key_it = key_it_end;
        if (is_short_read || page_limit_reached) {
            break;
        }
    }
    co_return value_type(merger.get(), std::move(cmd));
}

future<shared_ptr<cql_transport::messages::result_message>>
view_indexed_table_select_statement::execute_base_query(
        query_processor& qp,
        std::vector<primary_key>&& primary_keys,
        service::query_state& state,
        const query_options& options,
        gc_clock::time_point now,
        lw_shared_ptr<const service::pager::paging_state> paging_state) const {
    return do_execute_base_query(qp, std::move(primary_keys), state, options, now, paging_state).then(wrap_result_to_error_message(
            [this, &state, &options, now, paging_state = std::move(paging_state)] (std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, lw_shared_ptr<query::read_command>> result_and_cmd){
        auto&& [result, cmd] = result_and_cmd;
        return process_base_query_results(std::move(result), std::move(cmd), state, options, now, std::move(paging_state));
    }));
}
    
::shared_ptr<cql3::statements::select_statement>
view_indexed_table_select_statement::prepare(data_dictionary::database db,
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
                                         cql_stats &stats,
                                         std::unique_ptr<attributes> attrs)
{
    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();
    auto [index_opt, used_index_restrictions] = restrictions->find_idx(sim);

    if (!index_opt) {
        throw std::runtime_error("No index found.");
    }

    auto it = index_opt->metadata().options().find(db::index::secondary_index::custom_class_option_name);
    if (it != index_opt->metadata().options().end() && it->second == ANN_CUSTOM_INDEX_OPTION) {
        throw exceptions::invalid_request_exception("Vector indexes only support ANN queries");
    }

    schema_ptr view_schema = restrictions->get_view_schema();

    return ::make_shared<cql3::statements::view_indexed_table_select_statement>(
            schema,
            bound_terms,
            parameters,
            std::move(selection),
            std::move(restrictions),
            std::move(group_by_cell_indices),
            is_reversed,
            std::move(ordering_comparator),
            std::move(limit),
            std::move(per_partition_limit),
            stats,
            *index_opt,
            std::move(used_index_restrictions),
            view_schema,
            std::move(attrs));

}

view_indexed_table_select_statement::view_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
                                                           lw_shared_ptr<const parameters> parameters,
                                                           ::shared_ptr<selection::selection> selection,
                                                           ::shared_ptr<const restrictions::statement_restrictions> restrictions,
                                                           ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
                                                           bool is_reversed,
                                                           ordering_comparator_type ordering_comparator,
                                                           std::optional<expr::expression> limit,
                                                           std::optional<expr::expression> per_partition_limit,
                                                           cql_stats &stats,
                                                           const secondary_index::index& index,
                                                           expr::expression used_index_restrictions,
                                                           schema_ptr view_schema,
                                                           std::unique_ptr<attributes> attrs)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit, stats, std::move(attrs)}
    , _index{index}
    , _used_index_restrictions(std::move(used_index_restrictions))
    , _view_schema(view_schema)
{
    SCYLLA_ASSERT(_view_schema);
    if (_index.metadata().local()) {
        _get_partition_ranges_for_posting_list = [this] (const query_options& options) { return get_partition_ranges_for_local_index_posting_list(options); };
        _get_partition_slice_for_posting_list = [this] (const query_options& options) { return get_partition_slice_for_local_index_posting_list(options); };
    } else {
        _get_partition_ranges_for_posting_list = [this] (const query_options& options) { return get_partition_ranges_for_global_index_posting_list(options); };
        _get_partition_slice_for_posting_list = [this] (const query_options& options) { return get_partition_slice_for_global_index_posting_list(options); };
    }
}

template<typename KeyType>
requires (std::is_same_v<KeyType, partition_key> || std::is_same_v<KeyType, clustering_key_prefix>)
static void append_base_key_to_index_ck(std::vector<managed_bytes_view>& exploded_index_ck, const KeyType& base_key, const column_definition& index_cdef) {
    auto key_view = base_key.view();
    auto begin = key_view.begin();
    if ((std::is_same_v<KeyType, partition_key> && index_cdef.is_partition_key())
            || (std::is_same_v<KeyType, clustering_key_prefix> && index_cdef.is_clustering_key())) {
        auto key_position = std::next(begin, index_cdef.id);
        std::move(begin, key_position, std::back_inserter(exploded_index_ck));
        begin = std::next(key_position);
    }
    std::move(begin, key_view.end(), std::back_inserter(exploded_index_ck));
}

bytes view_indexed_table_select_statement::compute_idx_token(const partition_key& key) const {
    const column_definition& cdef = *_view_schema->clustering_key_columns().begin();
    if (!cdef.is_computed()) {
        throw std::logic_error{format(
            "Detected legacy non-computed token column {} in table {}.{}",
            cdef.name_as_text(), _schema->ks_name(), _schema->cf_name())};
    }
    return cdef.get_computation().compute_value(*_schema, key);
}

lw_shared_ptr<const service::pager::paging_state> view_indexed_table_select_statement::generate_view_paging_state_from_base_query_results(lw_shared_ptr<const service::pager::paging_state> paging_state,
        const foreign_ptr<lw_shared_ptr<query::result>>& results, service::query_state& state, const query_options& options) const {
    const column_definition* cdef = _schema->get_column_definition(to_bytes(_index.target_column()));
    if (!cdef) {
        throw exceptions::invalid_request_exception("Indexed column not found in schema");
    }

    auto result_view = query::result_view(*results);
    if (!results->row_count() || *results->row_count() == 0) {
        return paging_state;
    }

    auto&& last_pos = results->get_or_calculate_last_position();
    auto& last_base_pk = last_pos.partition;
    auto* last_base_ck = last_pos.position.has_key() ? &last_pos.position.key() : nullptr;

    bytes_opt indexed_column_value = restrictions::value_for(*cdef, _used_index_restrictions, options);

    auto index_pk = [&]() {
        if (_index.metadata().local()) {
            return last_base_pk;
        } else {
            return partition_key::from_single_value(*_view_schema, *indexed_column_value);
        }
    }();

    std::vector<managed_bytes_view> exploded_index_ck;
    exploded_index_ck.reserve(_view_schema->clustering_key_size());

    bytes token_bytes;
    if (_index.metadata().local()) {
        exploded_index_ck.push_back(bytes_view(*indexed_column_value));
    } else {
        token_bytes = compute_idx_token(last_base_pk);
        exploded_index_ck.push_back(bytes_view(token_bytes));
        append_base_key_to_index_ck<partition_key>(exploded_index_ck, last_base_pk, *cdef);
    }
    if (last_base_ck && !cdef->is_static()) {
        append_base_key_to_index_ck<clustering_key>(exploded_index_ck, *last_base_ck, *cdef);
    }

    auto index_ck = clustering_key::from_range(std::move(exploded_index_ck));
    if (partition_key::tri_compare(*_view_schema)(paging_state->get_partition_key(), index_pk) == 0
            && (!paging_state->get_clustering_key() || clustering_key::prefix_equal_tri_compare(*_view_schema)(*paging_state->get_clustering_key(), index_ck) == 0)) {
        return paging_state;
    }

    auto paging_state_copy = make_lw_shared<service::pager::paging_state>(service::pager::paging_state(*paging_state));
    paging_state_copy->set_remaining(get_internal_page_size());
    paging_state_copy->set_partition_key(std::move(index_pk));
    paging_state_copy->set_clustering_key(std::move(index_ck));
    return paging_state_copy;
}

future<shared_ptr<cql_transport::messages::result_message>> view_indexed_table_select_statement::do_execute(
        query_processor& qp, service::query_state& state, const query_options& options) const {

    return measure_index_latency(*_schema, _index, [this, &qp, &state, &options]() -> future<shared_ptr<cql_transport::messages::result_message>> {
        return actually_do_execute(qp, state, options);
    });
}

future<shared_ptr<cql_transport::messages::result_message>>
view_indexed_table_select_statement::actually_do_execute(query_processor& qp,
                             service::query_state& state,
                             const query_options& options) const
{
    if (_view_schema) {
        tracing::add_table_name(state.get_trace_state(), _view_schema->ks_name(), _view_schema->cf_name());
    }
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    auto cl = options.get_consistency();

    validate_for_read(cl);

    auto now = gc_clock::now();

    ++_stats.secondary_index_reads;

    const source_selector src_sel = state.get_client_state().is_internal()
            ? source_selector::INTERNAL : source_selector::USER;
    ++_stats.query_cnt(src_sel, _ks_sel, cond_selector::NO_CONDITIONS, statement_type::SELECT);

    SCYLLA_ASSERT(_restrictions->uses_secondary_indexing());

    _stats.unpaged_select_queries(_ks_sel) += options.get_page_size() <= 0;

    // Secondary index search has two steps: 1. use the index table to find a
    // list of primary keys matching the query. 2. read the rows matching
    // these primary keys from the base table and return the selected columns.
    // In "whole_partitions" case, we can do the above in whole partition
    // granularity. "partition_slices" is similar, but we fetch the same
    // clustering prefix (make_partition_slice()) from a list of partitions.
    // In other cases we need to list, and retrieve, individual rows and
    // not entire partitions. See issue #3405 for more details.
    bool whole_partitions = false;
    bool partition_slices = false;
    if (_schema->clustering_key_size() == 0) {
        // Obviously, if there are no clustering columns, then we can work at
        // the granularity of whole partitions.
        whole_partitions = true;
    } else if (_schema->get_column_definition(to_bytes(_index.target_column()))->is_static()) {
        // Index table for a static index does not have the original tables'
        // clustering key columns, so we must not fetch them.
        whole_partitions = true;
    } else {
        if (_index.depends_on(*(_schema->clustering_key_columns().begin()))) {
            // Searching on the *first* clustering column means in each of
            // matching partition, we can take the same contiguous clustering
            // slice (clustering prefix).
            partition_slices = true;
        } else {
            // Search on any partition column means that either all rows
            // match or all don't, so we can work with whole partitions.
            for (auto& cdef : _schema->partition_key_columns()) {
                if (_index.depends_on(cdef)) {
                    whole_partitions = true;
                    break;
                }
            }
        }
    }

    // Aggregated and paged filtering needs to aggregate the results from all pages
    // in order to avoid returning partial per-page results (issue #4540).
    // It's a little bit more complicated than regular aggregation, because each paging state
    // needs to be translated between the base table and the underlying view.
    // The routine below keeps fetching pages from the underlying view, which are then
    // used to fetch base rows, which go straight to the result set builder.
    // A local, internal copy of query_options is kept in order to keep updating
    // the paging state between requesting data from replicas.
    const bool aggregate = _selection->is_aggregate() || has_group_by();
    if (aggregate) {
        cql3::selection::result_set_builder builder(*_selection, now, &options, *_group_by_cell_indices);
        std::unique_ptr<cql3::query_options> internal_options = std::make_unique<cql3::query_options>(cql3::query_options(options));
        stop_iteration stop;
        // page size is set to the internal count page size, regardless of the user-provided value
        internal_options.reset(new cql3::query_options(std::move(internal_options), options.get_paging_state(), get_internal_page_size()));
        do {
            auto consume_results = [this, &builder, &options, &internal_options, &state] (foreign_ptr<lw_shared_ptr<query::result>> results, lw_shared_ptr<query::read_command> cmd, lw_shared_ptr<const service::pager::paging_state> paging_state) -> stop_iteration {
                if (paging_state) {
                    paging_state = generate_view_paging_state_from_base_query_results(paging_state, results, state, options);
                }
                internal_options.reset(new cql3::query_options(std::move(internal_options), paging_state ? make_lw_shared<service::pager::paging_state>(*paging_state) : nullptr));
                if (_restrictions_need_filtering) {
                    _stats.filtered_rows_read_total += *results->row_count();
                    query::result_view::consume(*results, cmd->slice, cql3::selection::result_set_builder::visitor(builder, *_schema, *_selection,
                            cql3::selection::result_set_builder::restrictions_filter(_restrictions, options, cmd->get_row_limit(), _schema, cmd->slice.partition_row_limit())));
                } else {
                    query::result_view::consume(*results, cmd->slice, cql3::selection::result_set_builder::visitor(builder, *_schema, *_selection));
                }
                bool has_more_pages = paging_state && paging_state->get_remaining() > 0;
                return stop_iteration(!has_more_pages);
            };

            if (whole_partitions || partition_slices) {
                tracing::trace(state.get_trace_state(), "Consulting index {} for a single slice of keys, aggregation query", _index.metadata().name());
                auto result_partition_ranges_and_paging_state = co_await find_index_partition_ranges(qp, state, *internal_options);
                if (result_partition_ranges_and_paging_state.has_error()) {
                    co_return failed_result_to_result_message(std::move(result_partition_ranges_and_paging_state));
                }
                auto&& [partition_ranges, paging_state] = result_partition_ranges_and_paging_state.assume_value();
                auto result_results_and_cmd = co_await do_execute_base_query(qp, std::move(partition_ranges), state, *internal_options, now, paging_state);
                if (result_results_and_cmd.has_error()) {
                    co_return failed_result_to_result_message(std::move(result_results_and_cmd));
                }
                auto&& [results, cmd] = result_results_and_cmd.assume_value();
                stop = consume_results(std::move(results), std::move(cmd), std::move(paging_state));
            } else {
                tracing::trace(state.get_trace_state(), "Consulting index {} for a list of rows containing keys, aggregation query", _index.metadata().name());
                auto result_primary_keys_paging_state = co_await find_index_clustering_rows(qp, state, *internal_options);
                if (result_primary_keys_paging_state.has_error()) {
                    co_return failed_result_to_result_message(std::move(result_primary_keys_paging_state));
                }
                auto&& [primary_keys, paging_state] = result_primary_keys_paging_state.assume_value();
                auto result_results_cmd = co_await this->do_execute_base_query(qp, std::move(primary_keys), state, *internal_options, now, paging_state);
                if (result_results_cmd.has_error()) {
                    co_return failed_result_to_result_message(std::move(result_results_cmd));
                }
                auto&& [results, cmd] = result_results_cmd.assume_value();
                stop = consume_results(std::move(results), std::move(cmd), std::move(paging_state));
            }
        } while (!stop);

        auto rs = builder.build();
        update_stats_rows_read(rs->size());
        _stats.filtered_rows_matched_total += _restrictions_need_filtering ? rs->size() : 0;
        auto msg = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
        co_return shared_ptr<cql_transport::messages::result_message>(std::move(msg));
    }

    if (whole_partitions || partition_slices) {
        tracing::trace(state.get_trace_state(), "Consulting index {} for a single slice of keys", _index.metadata().name());
        // In this case, can use our normal query machinery, which retrieves
        // entire partitions or the same slice for many partitions.
        coordinator_result<std::tuple<dht::partition_range_vector, lw_shared_ptr<const service::pager::paging_state>>> result = co_await find_index_partition_ranges(qp, state, options);
        if (result.has_error()) {
            co_return failed_result_to_result_message(std::move(result));
        }
        {
            auto&& [partition_ranges, paging_state] = result.assume_value();
            co_return co_await this->execute_base_query(qp, std::move(partition_ranges), state, options, now, std::move(paging_state));
        }
    } else {
        tracing::trace(state.get_trace_state(), "Consulting index {} for a list of rows containing keys", _index.metadata().name());
        // In this case, we need to retrieve a list of rows (not entire
        // partitions) and then retrieve those specific rows.
        coordinator_result<std::tuple<std::vector<primary_key>, lw_shared_ptr<const service::pager::paging_state>>> result = co_await find_index_clustering_rows(qp, state, options);
        if (result.has_error()) {
            co_return failed_result_to_result_message(std::move(result));
        }
        {
            auto&& [primary_keys, paging_state] = result.assume_value();
            co_return co_await this->execute_base_query(qp, std::move(primary_keys), state, options, now, std::move(paging_state));
        }
    }
}

dht::partition_range_vector view_indexed_table_select_statement::get_partition_ranges_for_local_index_posting_list(const query_options& options) const {
    return _restrictions->get_partition_key_ranges(options);
}

dht::partition_range_vector view_indexed_table_select_statement::get_partition_ranges_for_global_index_posting_list(const query_options& options) const {
    dht::partition_range_vector partition_ranges;

    const column_definition* cdef = _schema->get_column_definition(to_bytes(_index.target_column()));
    if (!cdef) {
        throw exceptions::invalid_request_exception("Indexed column not found in schema");
    }

    bytes_opt value = restrictions::value_for(*cdef, _used_index_restrictions, options);
    if (value) {
        auto pk = partition_key::from_single_value(*_view_schema, *value);
        auto dk = dht::decorate_key(*_view_schema, pk);
        auto range = dht::partition_range::make_singular(dk);
        partition_ranges.emplace_back(range);
    }

    return partition_ranges;
}

query::partition_slice view_indexed_table_select_statement::get_partition_slice_for_global_index_posting_list(const query_options& options) const {
    partition_slice_builder partition_slice_builder{*_view_schema};

    if (!_restrictions->has_partition_key_unrestricted_components()) {
        bool pk_restrictions_is_single = !_restrictions->has_token_restrictions();
        // Only EQ restrictions on base partition key can be used in an index view query
        if (pk_restrictions_is_single && _restrictions->partition_key_restrictions_is_all_eq()) {
            partition_slice_builder.with_ranges(
                    _restrictions->get_global_index_clustering_ranges(options, *_view_schema));
        } else if (_restrictions->has_token_restrictions()) {
            // Restrictions like token(p1, p2) < 0 have all partition key components restricted, but require special handling.
            partition_slice_builder.with_ranges(
                    _restrictions->get_global_index_token_clustering_ranges(options, *_view_schema));
        }
    }

    return partition_slice_builder.build();
}

query::partition_slice view_indexed_table_select_statement::get_partition_slice_for_local_index_posting_list(const query_options& options) const {
    partition_slice_builder partition_slice_builder{*_view_schema};

    partition_slice_builder.with_ranges(
        _restrictions->get_local_index_clustering_ranges(options, *_view_schema));

    return partition_slice_builder.build();
}

// Utility function for reading from the index view (get_index_view()))
// the posting-list for a particular value of the indexed column.
// Remember a secondary index can only be created on a single column.
future<cql3::statements::select_statement::coordinator_result<::shared_ptr<cql_transport::messages::result_message::rows>>>
view_indexed_table_select_statement::read_posting_list(query_processor& qp,
                  const query_options& options,
                  uint64_t limit,
                  service::query_state& state,
                  gc_clock::time_point now,
                  db::timeout_clock::time_point timeout,
                  bool include_base_clustering_key) const
{
    dht::partition_range_vector partition_ranges = _get_partition_ranges_for_posting_list(options);
    auto partition_slice = _get_partition_slice_for_posting_list(options);

    auto cmd = ::make_lw_shared<query::read_command>(
            _view_schema->id(),
            _view_schema->version(),
            partition_slice,
            qp.proxy().get_max_result_size(partition_slice),
            query::tombstone_limit(qp.proxy().get_tombstone_limit()),
            query::row_limit(limit),
            query::partition_limit(query::max_partitions),
            now,
            tracing::make_trace_info(state.get_trace_state()),
            query_id::create_null_id(),
            query::is_first_page::no,
            options.get_timestamp(state));

    std::vector<const column_definition*> columns;
    for (const column_definition& cdef : _schema->partition_key_columns()) {
        columns.emplace_back(_view_schema->get_column_definition(cdef.name()));
    }
    if (include_base_clustering_key) {
        for (const column_definition& cdef : _schema->clustering_key_columns()) {
            columns.emplace_back(_view_schema->get_column_definition(cdef.name()));
        }
    }
    auto selection = selection::selection::for_columns(_view_schema, columns);

    int32_t page_size = options.get_page_size();
    if (page_size <= 0 || !service::pager::query_pagers::may_need_paging(*_view_schema, page_size, *cmd, partition_ranges)) {
        return qp.proxy().query_result(_view_schema, cmd, std::move(partition_ranges), options.get_consistency(), {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state()})
        .then(utils::result_wrap([this, now, &options, selection = std::move(selection), partition_slice = std::move(partition_slice)] (service::storage_proxy::coordinator_query_result qr)
                -> coordinator_result<::shared_ptr<cql_transport::messages::result_message::rows>> {
            cql3::selection::result_set_builder builder(*selection, now, &options);
            query::result_view::consume(*qr.query_result,
                                        std::move(partition_slice),
                                        cql3::selection::result_set_builder::visitor(builder, *_view_schema, *selection));
            return ::make_shared<cql_transport::messages::result_message::rows>(result(builder.build()));
        }));
    }

    auto p = service::pager::query_pagers::pager(qp.proxy(), _view_schema, selection,
            state, options, cmd, std::move(partition_ranges), nullptr);
    return p->fetch_page_result(options.get_page_size(), now, timeout).then(utils::result_wrap([p = std::move(p)] (std::unique_ptr<cql3::result_set> rs)
            -> coordinator_result<::shared_ptr<cql_transport::messages::result_message::rows>> {
        rs->get_metadata().set_paging_state(p->state());
        return ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
    }));
}

// Note: the partitions keys returned by this function are sorted
// in token order. See issue #3423.
future<cql3::statements::select_statement::coordinator_result<std::tuple<dht::partition_range_vector, lw_shared_ptr<const service::pager::paging_state>>>>
view_indexed_table_select_statement::find_index_partition_ranges(query_processor& qp,
                                             service::query_state& state,
                                             const query_options& options) const
{
    using value_type = std::tuple<dht::partition_range_vector, lw_shared_ptr<const service::pager::paging_state>>;
    auto now = gc_clock::now();
    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    bool is_paged = options.get_page_size() >= 0;
    constexpr size_t MAX_PARTITION_RANGES = 1000;
    // Check that `partition_range_vector` won't cause a large allocation (see #18536).
    // If this fails, please adjust MAX_PARTITION_RANGES accordingly.
    static_assert(MAX_PARTITION_RANGES * sizeof(dht::partition_range_vector::value_type) <= 128 * 1024,
            "MAX_PARTITION_RANGES too high - will cause large allocations from partition_range_vector");
    size_t max_vector_size = is_paged ? MAX_PARTITION_RANGES : std::numeric_limits<size_t>::max();
    std::unique_ptr<query_options> paging_adjusted_options = [&] () {
        if (is_paged && _schema->clustering_key_size() == 0 && static_cast<size_t>(options.get_page_size()) > max_vector_size) {
            // Result is guaranteed to contain one partition key per row.
            // Limit the page size a priori.
            auto internal_options = std::make_unique<cql3::query_options>(cql3::query_options(options));
            internal_options.reset(new cql3::query_options(std::move(internal_options), options.get_paging_state(), max_vector_size));
            return internal_options;
        }
        return std::unique_ptr<cql3::query_options>(nullptr);
    }();
    return do_with(std::move(paging_adjusted_options), [&, max_vector_size](auto& paging_adjusted_options) {
    // FIXME: Indent.
    const query_options& _options = paging_adjusted_options ? *paging_adjusted_options : options;
    const uint64_t limit = get_inner_loop_limit(get_limit(options, _limit), _selection->is_aggregate());
    return read_posting_list(qp, _options, limit, state, now, timeout, false).then(utils::result_wrap(
            [this, &_options, max_vector_size] (::shared_ptr<cql_transport::messages::result_message::rows> rows) {
        auto rs = cql3::untyped_result_set(rows);
        dht::partition_range_vector partition_ranges;
        partition_ranges.reserve(std::min(rs.size(), max_vector_size));
        // We are reading the list of primary keys as rows of a single
        // partition (in the index view), so they are sorted in
        // lexicographical order (N.B. this is NOT token order!). We need
        // to avoid outputting the same partition key twice, but luckily in
        // the sorted order, these will be adjacent.
        std::optional<dht::decorated_key> last_dk;
        if (_options.get_paging_state()) {
            auto paging_state = _options.get_paging_state();
            auto base_pk = generate_base_key_from_index_pk<partition_key>(paging_state->get_partition_key(),
                paging_state->get_clustering_key(), *_schema, *_view_schema);
            last_dk = dht::decorate_key(*_schema, base_pk);
        }
        for (size_t i = 0; i < rs.size(); i++) {
            const auto& row = rs.at(i);
            std::vector<bytes> pk_columns;
            const auto& columns = row.get_columns();
            pk_columns.reserve(columns.size());
            for (const auto& column : columns) {
                pk_columns.push_back(row.get_blob_unfragmented(column->name->to_string()));
            }
            auto pk = partition_key::from_exploded(*_schema, pk_columns);
            auto dk = dht::decorate_key(*_schema, pk);
            if (last_dk && last_dk->equal(*_schema, dk)) {
                // Another row of the same partition, no need to output the
                // same partition key again.
                continue;
            }
            last_dk = dk;
            auto range = dht::partition_range::make_singular(dk);
            partition_ranges.emplace_back(range);
            if (partition_ranges.size() >= partition_ranges.capacity()) {
                break;
            }
        }
        auto paging_state = rows->rs().get_metadata().paging_state();
        return make_ready_future<coordinator_result<value_type>>(value_type(std::move(partition_ranges), std::move(paging_state)));
    }));
    });
}

// Note: the partitions keys returned by this function are sorted
// in token order. See issue #3423.
future<cql3::statements::select_statement::coordinator_result<std::tuple<std::vector<primary_key>, lw_shared_ptr<const service::pager::paging_state>>>>
view_indexed_table_select_statement::find_index_clustering_rows(query_processor& qp, service::query_state& state, const query_options& options) const
{
    using value_type = std::tuple<std::vector<primary_key>, lw_shared_ptr<const service::pager::paging_state>>;
    auto now = gc_clock::now();
    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    const uint64_t limit = get_inner_loop_limit(get_limit(options, _limit), _selection->is_aggregate());
    return read_posting_list(qp, options, limit, state, now, timeout, true).then(utils::result_wrap(
            [this, &options] (::shared_ptr<cql_transport::messages::result_message::rows> rows) {

        auto rs = cql3::untyped_result_set(rows);
        std::vector<primary_key> primary_keys;
        primary_keys.reserve(rs.size());

        std::optional<std::reference_wrapper<primary_key>> last_primary_key;
        // Set last_primary_key if indexing map values and not in the first
        // query page. See comment below why last_primary_key is needed for
        // indexing map values. We have a test for this with paging:
        // test_secondary_index.py::test_index_map_values_paging.
        std::optional<primary_key> page_start_primary_key;
        if (_index.target_type() == cql3::statements::index_target::target_type::collection_values &&
            options.get_paging_state()) {
            auto paging_state = options.get_paging_state();
            auto base_pk = generate_base_key_from_index_pk<partition_key>(paging_state->get_partition_key(),
                paging_state->get_clustering_key(), *_schema, *_view_schema);
            auto base_dk = dht::decorate_key(*_schema, base_pk);
            auto base_ck = generate_base_key_from_index_pk<clustering_key>(paging_state->get_partition_key(),
                paging_state->get_clustering_key(), *_schema, *_view_schema);
            page_start_primary_key = primary_key{std::move(base_dk), std::move(base_ck)};
            last_primary_key = *page_start_primary_key;
        }
        for (size_t i = 0; i < rs.size(); i++) {
            const auto& row = rs.at(i);
            auto pk_columns = _schema->partition_key_columns() | std::views::transform([&] (auto& cdef) {
                return row.get_blob_unfragmented(cdef.name_as_text());
            });
            auto pk = partition_key::from_range(pk_columns);
            auto dk = dht::decorate_key(*_schema, pk);
            auto ck_columns = _schema->clustering_key_columns() | std::views::transform([&] (auto& cdef) {
                return row.get_blob_unfragmented(cdef.name_as_text());
            });
            auto ck = clustering_key::from_range(ck_columns);

            if (_index.target_type() == cql3::statements::index_target::target_type::collection_values) {
                // The index on collection values is special in a way, as its' clustering key contains not only the
                // base primary key, but also a column that holds the keys of the cells in the collection, which
                // allows to distinguish cells with different keys but the same value.
                // This has an unwanted consequence, that it's possible to receive two identical base table primary
                // keys. Thankfully, they are guaranteed to occur consequently.
                if (last_primary_key) {
                    const auto& [last_dk, last_ck] = last_primary_key->get();
                    if (last_dk.equal(*_schema, dk) && last_ck.equal(*_schema, ck)) {
                        continue;
                    }
                }
            }

            primary_keys.emplace_back(primary_key{std::move(dk), std::move(ck)});
            // Last use of this reference will be before next .emplace_back to the vector.
            last_primary_key = primary_keys.back();
        }
        auto paging_state = rows->rs().get_metadata().paging_state();
        return make_ready_future<coordinator_result<value_type>>(value_type(std::move(primary_keys), std::move(paging_state)));
    }));
}

::shared_ptr<cql3::statements::select_statement> vector_indexed_table_select_statement::prepare(data_dictionary::database db, schema_ptr schema,
        uint32_t bound_terms, lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<restrictions::statement_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats, std::unique_ptr<attributes> attrs) {
    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();
    auto [index_opt, _] = restrictions->find_idx(sim);

    auto indexes = sim.list_indexes();
    auto it = std::find_if(indexes.begin(), indexes.end(), [&prepared_ann_ordering](const auto& ind) {
        return (ind.metadata().options().contains(db::index::secondary_index::custom_class_option_name) &&
                       ind.metadata().options().at(db::index::secondary_index::custom_class_option_name) == ANN_CUSTOM_INDEX_OPTION) &&
               (ind.target_column() == prepared_ann_ordering.first->name_as_text());
    });

    if (it == indexes.end()) {
        throw exceptions::invalid_request_exception("ANN ordering by vector requires the column to be indexed using 'vector_index'");
    }

    if (index_opt || parameters->allow_filtering() || restrictions->need_filtering() || check_needs_allow_filtering_anyway(*restrictions)) {
        throw exceptions::invalid_request_exception("ANN ordering by vector does not support filtering");
    }
    index_opt = *it;

    if (!index_opt) {
        throw std::runtime_error("No index found.");
    }

    return ::make_shared<cql3::statements::vector_indexed_table_select_statement>(schema, bound_terms, parameters, std::move(selection), std::move(restrictions),
            std::move(group_by_cell_indices), is_reversed, std::move(ordering_comparator), std::move(prepared_ann_ordering), std::move(limit),
            std::move(per_partition_limit), stats, *index_opt, std::move(attrs));
}

vector_indexed_table_select_statement::vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
        prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats, const secondary_index::index& index, std::unique_ptr<attributes> attrs)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices, is_reversed, ordering_comparator, limit,
              per_partition_limit, stats, std::move(attrs)}
    , _index{index}
    , _prepared_ann_ordering(std::move(prepared_ann_ordering)) {

    if (!limit.has_value()) {
        throw exceptions::invalid_request_exception("Vector ANN queries must have a limit specified");
    }

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Vector ANN queries do not support per-partition limits");
    }

    if (selection->is_aggregate()) {
        throw exceptions::invalid_request_exception("Vector ANN queries cannot be run with aggregation");
    }
}

future<shared_ptr<cql_transport::messages::result_message>> vector_indexed_table_select_statement::do_execute(
        query_processor& qp, service::query_state& state, const query_options& options) const {

    return measure_index_latency(*_schema, _index, [this, &qp, &state, &options](this auto) -> future<shared_ptr<cql_transport::messages::result_message>> {
        tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());
        validate_for_read(options.get_consistency());

        _query_start_time_point = gc_clock::now();

        update_stats();

        auto limit = get_limit(options, _limit);

        if (limit > max_ann_query_limit) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(
                    fmt::format("Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than {}. LIMIT was {}", max_ann_query_limit, limit)));
        }

        auto as = abort_source();
        auto pkeys = co_await qp.vector_store_client().ann(_schema->ks_name(), _index.metadata().name(), _schema, get_ann_ordering_vector(options), limit, as);
        if (!pkeys.has_value()) {
            co_await coroutine::return_exception(
                    exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::ann_error_visitor{}, pkeys.error())));
        }

        co_return co_await query_base_table(qp, state, options, pkeys.value());
    });
}

void vector_indexed_table_select_statement::update_stats() const {
    ++_stats.secondary_index_reads;
    ++_stats.query_cnt(source_selector::USER, _ks_sel, cond_selector::NO_CONDITIONS, statement_type::SELECT);
}

lw_shared_ptr<query::read_command> vector_indexed_table_select_statement::prepare_command_for_base_query(
        query_processor& qp, service::query_state& state, const query_options& options) const {
    auto slice = make_partition_slice(options);
    return ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(), std::move(slice), qp.proxy().get_max_result_size(slice),
            query::tombstone_limit(qp.proxy().get_tombstone_limit()),
            query::row_limit(get_inner_loop_limit(get_limit(options, _limit), _selection->is_aggregate())), query::partition_limit(query::max_partitions),
            _query_start_time_point, tracing::make_trace_info(state.get_trace_state()), query_id::create_null_id(), query::is_first_page::no,
            options.get_timestamp(state));
}

std::vector<float> vector_indexed_table_select_statement::get_ann_ordering_vector(const query_options& options) const {
    auto [ann_column, ann_vector_expr] = _prepared_ann_ordering;
    auto values = value_cast<vector_type_impl::native_type>(ann_column->type->deserialize(expr::evaluate(ann_vector_expr, options).to_bytes()));
    return util::to_vector<float>(values);
}

future<::shared_ptr<cql_transport::messages::result_message>> vector_indexed_table_select_statement::query_base_table(
        query_processor& qp, service::query_state& state, const query_options& options, const std::vector<primary_key>& pkeys) const {
    auto command = prepare_command_for_base_query(qp, state, options);
    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    co_return co_await qp.proxy()
            .query_result(_query_schema, command, to_partition_ranges(pkeys), options.get_consistency(),
                    {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state(), {}, {}, options.get_specific_options().node_local_only},
                    std::nullopt)
            .then(wrap_result_to_error_message([this, &options, command](service::storage_proxy::coordinator_query_result qr) {
                return this->process_results(std::move(qr.query_result), command, options, _query_start_time_point);
            }));
}

}
}
