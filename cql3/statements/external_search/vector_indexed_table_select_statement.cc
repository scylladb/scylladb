/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/vector_indexed_table_select_statement.hh"

#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/vector_similarity_fcts.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/util.hh"

#include "db/consistency_level_validations.hh"
#include "replica/database.hh"
#include "exceptions/exceptions.hh"
#include "index/vector_index.hh"
#include "query/query_result_merger.hh"
#include "service/storage_proxy.hh"
#include "types/vector.hh"
#include "utils/result_loop.hh"

#include <algorithm>
#include <cmath>
#include <seastar/core/future.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/exception.hh>


template<typename T = void>
using coordinator_result = cql3::statements::select_statement::coordinator_result<T>;

namespace cql3 {

namespace statements {

static logging::logger logger("vector_indexed_table_select_statement");

namespace {

/// Re-computes similarity scores live from the fetched embedding column.
/// Used in rescoring mode: no reliance on VS-provided distances.
class rescoring_similarity_provider : public cql3::selection::result_set_builder::temporaries_provider {
    const seastar::shared_ptr<cql3::functions::vector_similarity_fct> _similarity_fct;
    const bytes_opt _query_vec_bytes;
    /// Storage class of the indexed column.
    const column_kind _col_kind;
    /// Unified index into the relevant data source:
    ///  - partition_key / clustering_key: component offset in the exploded key span.
    ///  - static_column / regular_column: cell-stream offset (only same-kind columns counted).
    const size_t _index;
    /// True if the indexed column is a multi-cell (collection) type.
    const bool _is_multi_cell;
    const size_t _temporary_index;
public:
    rescoring_similarity_provider(
            seastar::shared_ptr<cql3::functions::vector_similarity_fct> similarity_fct,
            bytes_opt query_vec_bytes,
            column_kind col_kind,
            size_t index,
            bool is_multi_cell,
            size_t temporary_index)
        : _similarity_fct(std::move(similarity_fct))
        , _query_vec_bytes(std::move(query_vec_bytes))
        , _col_kind(col_kind)
        , _index(index)
        , _is_multi_cell(is_multi_cell)
        , _temporary_index(temporary_index) {}

    bool try_fill(
            std::vector<cql3::raw_value>& temporaries,
            std::span<const bytes> partition_key,
            std::span<const bytes> clustering_key,
            const query::result_row_view& static_row,
            const query::result_row_view* row) const override {
        bytes_opt row_vec_bytes;
        switch (_col_kind) {
        case column_kind::partition_key:
            row_vec_bytes = partition_key[_index];
            break;
        case column_kind::clustering_key:
            row_vec_bytes = clustering_key[_index];
            break;
        case column_kind::static_column:
        case column_kind::regular_column: {
            if (_col_kind == column_kind::regular_column && !row) {
                return false;
            }
            auto iter = (_col_kind == column_kind::static_column) ? static_row.iterator()
                                                                   : row->iterator();
            for (size_t i = 0; i < _index; ++i) {
                iter.next_atomic_cell();
            }
            if (_is_multi_cell) {
                auto cell = iter.next_collection_cell();
                if (cell) {
                    row_vec_bytes = linearized(*cell);
                }
            } else {
                auto cell = iter.next_atomic_cell();
                if (cell) {
                    row_vec_bytes = linearized(cell->value());
                }
            }
            break;
        }
        }
        auto result_bytes = _similarity_fct->execute(std::array<bytes_opt, 2>{_query_vec_bytes, row_vec_bytes});
        if (!result_bytes) {
            return false;
        }
        float similarity_value = value_cast<float>(float_type->deserialize(*result_bytes));
        if (!std::isfinite(similarity_value)) {
            return false;
        }
        temporaries[_temporary_index] = cql3::raw_value::make_value(std::move(*result_bytes));
        return true;
    }
};

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

template<typename C>
struct result_to_error_message_wrapper {
    C c;

    template<typename T>
    auto operator()(coordinator_result<T>&& arg) {
        if constexpr (std::is_void_v<T>) {
            if (arg) {
                return futurize_invoke(c);
            } else {
                return make_ready_future<typename futurize_t<std::invoke_result_t<C>>::value_type>(
                    ::make_shared<cql_transport::messages::result_message::exception>(std::move(arg).assume_error())
                );
            }
        } else {
            if (arg) {
                return futurize_invoke(c, std::move(arg).value());
            } else {
                return make_ready_future<typename futurize_t<std::invoke_result_t<C, T>>::value_type>(
                    ::make_shared<cql_transport::messages::result_message::exception>(std::move(arg).assume_error())
                );
            }
        }
    }
};

template<typename C>
auto wrap_result_to_error_message(C&& c) {
    return result_to_error_message_wrapper<C>{std::move(c)};
}

std::vector<float> get_ann_ordering_vector(const select_statement::prepared_ann_ordering_type& prepared_ann_ordering, const query_options& options) {
    auto const& [ann_column, ann_vector_expr] = prepared_ann_ordering;
    auto expr_value = expr::evaluate(ann_vector_expr, options);
    if (expr_value.is_null()) {
        throw exceptions::invalid_request_exception(fmt::format("Unsupported null value for column {}", ann_column->name_as_text()));
    }
    auto values = value_cast<vector_type_impl::native_type>(ann_column->type->deserialize(std::move(expr_value).to_bytes()));
    return util::to_vector<float>(values);
}

} // anonymous namespace

vector_indexed_table_select_statement::rescoring_config
vector_indexed_table_select_statement::rescoring_config::make(
        const secondary_index::index& index,
        const column_definition* column,
        const cql3::selection::selection& sel) {
    rescoring_config cfg;
    const auto& index_options = index.metadata().options();

    if (secondary_index::vector_index::is_rescoring_enabled(index_options)) {
        const auto sim_fn_name = secondary_index::vector_index::get_cql_similarity_function_name(index_options);
        cfg.function = seastar::make_shared<cql3::functions::vector_similarity_fct>(
            sstring(sim_fn_name), std::vector<data_type>{column->type, column->type});

        cfg.indexed_col_kind = column->kind;
        cfg.indexed_col_is_multi_cell = column->type->is_multi_cell();

        if (column->is_primary_key()) {
            cfg.index = column->component_index();
        } else {
            size_t stream_pos = 0;
            for (const column_definition* def : sel.get_columns()) {
                if (def == column) {
                    break;
                }
                if (column->is_static() ? def->is_static() : def->is_regular()) {
                    ++stream_pos;
                }
            }
            cfg.index = stream_pos;
        }
    }
    return cfg;
}

std::unique_ptr<cql3::selection::result_set_builder::temporaries_provider>
vector_indexed_table_select_statement::rescoring_config::make_similarity_provider(
        const cql3::query_options& options,
        const cql3::expr::expression& ann_vector_expr,
        size_t similarity_temporary_index) const {
    if (!is_enabled()) {
        return nullptr;
    }
    auto query_vec_bytes = cql3::expr::evaluate(ann_vector_expr, options).to_bytes();
    return std::make_unique<rescoring_similarity_provider>(
        function, std::move(query_vec_bytes),
        indexed_col_kind, index, indexed_col_is_multi_cell, similarity_temporary_index);
}

std::optional<ann_ordering_info> get_ann_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        lw_shared_ptr<const raw::select_statement::parameters> parameters,
        prepare_context& ctx) {

    if (parameters->orderings().empty()) {
        return std::nullopt;
    }

    auto [column_id, ordering] = parameters->orderings().front();
    const auto& ann_vector = std::get_if<raw::select_statement::ann_vector>(&ordering);
    if (!ann_vector) {
        return std::nullopt;
    }

    ::shared_ptr<column_identifier> column = column_id->prepare_column_identifier(*schema);
    const column_definition* def = schema->get_column_definition(column->name());
    if (!def) {
        throw exceptions::invalid_request_exception(
                fmt::format("Undefined column name {}", column->text()));
    }

    if (!def->type->is_vector() || static_cast<const vector_type_impl*>(def->type.get())->get_elements_type()->get_kind() != abstract_type::kind::float_kind) {
        throw exceptions::invalid_request_exception("ANN ordering is only supported on float vector indexes");
    }

    auto e =  expr::prepare_expression(*ann_vector, db, schema->ks_name(), nullptr, def->column_specification);
    expr::fill_prepare_context(e, ctx);

    raw::select_statement::prepared_ann_ordering_type prepared_ann_ordering = std::make_pair(std::move(def), std::move(e));

    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();

    auto indexes = sim.list_indexes();
    auto it = std::find_if(indexes.begin(), indexes.end(), [&prepared_ann_ordering](const auto& ind) {
        return secondary_index::vector_index::is_vector_index_on_column(ind.metadata(), prepared_ann_ordering.first->name_as_text());
    });

    if (it == indexes.end()) {
        throw exceptions::invalid_request_exception("ANN ordering by vector requires the column to be indexed using 'vector_index'");
    }

    return ann_ordering_info{
        *it,
        std::move(prepared_ann_ordering),
        secondary_index::vector_index::is_rescoring_enabled(it->metadata().options())
    };
}

// Appends a temporary expression for the similarity score to prepared_selectors.
// Returns the index of the appended selector within prepared_selectors.
uint32_t append_similarity_temporary_selector(
        std::vector<selection::prepared_selector>& prepared_selectors,
        size_t temp_index) {
    // Create a temporary expression instead of a function call.
    // The value will be injected during result processing by rescoring_similarity_provider.
    expr::temporary similarity_temp{
        .index = temp_index,
        .type = float_type,
    };

    // Add the temporary as a prepared selector (last)
    prepared_selectors.push_back(selection::prepared_selector{
        .expr = expr::expression(similarity_temp),
        .alias = nullptr,
    });
    return prepared_selectors.size() - 1;
}

select_statement::ordering_comparator_type get_similarity_ordering_comparator(std::vector<selection::prepared_selector>& prepared_selectors, uint32_t similarity_column_index) {
    auto type = expr::type_of(prepared_selectors[similarity_column_index].expr);
    if (type->get_kind() != abstract_type::kind::float_kind) {
        seastar::on_internal_error(logger, "Similarity function must return float type.");
    }
    return [similarity_column_index, type] (const raw::select_statement::result_row_type& r1, const raw::select_statement::result_row_type& r2) {
        auto& c1 = r1[similarity_column_index];
        auto& c2 = r2[similarity_column_index];
        auto f1 = c1 ? value_cast<float>(type->deserialize(*c1)) : std::numeric_limits<float>::quiet_NaN();
        auto f2 = c2 ? value_cast<float>(type->deserialize(*c2)) : std::numeric_limits<float>::quiet_NaN();
        if (std::isfinite(f1) && std::isfinite(f2)) {
            return f1 > f2;
        }
        return std::isfinite(f1);
    };
}

::shared_ptr<cql3::statements::select_statement> vector_indexed_table_select_statement::prepare(data_dictionary::database db, schema_ptr schema,
        uint32_t bound_terms, lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::statement_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats, const secondary_index::index& index, std::unique_ptr<attributes> attrs) {

    auto prepared_filter = external_search::prepare_filter(*restrictions, parameters->allow_filtering());

    return ::make_shared<cql3::statements::vector_indexed_table_select_statement>(schema, bound_terms, parameters, std::move(selection), std::move(restrictions),
            std::move(group_by_cell_indices), is_reversed, std::move(ordering_comparator), std::move(prepared_ann_ordering), std::move(limit),
            std::move(per_partition_limit), stats, index, std::move(prepared_filter), std::move(attrs));
}

vector_indexed_table_select_statement::vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
        prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats, const secondary_index::index& index,
        external_search::prepared_filter prepared_filter, std::unique_ptr<attributes> attrs)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices, is_reversed, ordering_comparator, limit,
              per_partition_limit, stats, std::move(attrs)}
    , _index{index}
    , _prepared_ann_ordering(std::move(prepared_ann_ordering))
    , _prepared_filter(std::move(prepared_filter)) {

    if (!limit.has_value()) {
        throw exceptions::invalid_request_exception("Vector ANN queries must have a limit specified");
    }

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Vector ANN queries do not support per-partition limits");
    }

    if (selection->is_aggregate()) {
        throw exceptions::invalid_request_exception("Vector ANN queries cannot be run with aggregation");
    }

    // Compute rescoring decision once at prepare time.
    _rescoring = rescoring_config::make(_index, _prepared_ann_ordering.first, *_selection);
}

future<shared_ptr<cql_transport::messages::result_message>> vector_indexed_table_select_statement::do_execute(
        query_processor& qp, service::query_state& state, const query_options& options) const {

    auto limit = get_limit(options, _limit);

    auto result = co_await measure_index_latency(*_schema, _index, [this, &qp, &state, &options, &limit](this auto) -> future<shared_ptr<cql_transport::messages::result_message>> {
        tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());
        validate_for_read(options.get_consistency());

        _query_start_time_point = gc_clock::now();

        update_stats();

        if (limit > max_ann_query_limit) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(
                    fmt::format("Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than {}. LIMIT was {}", max_ann_query_limit, limit)));
        }

        auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
        auto aoe = abort_on_expiry(timeout);
        auto filter_json = _prepared_filter.to_json(options);
        uint64_t fetch = static_cast<uint64_t>(std::ceil(limit * secondary_index::vector_index::get_oversampling(_index.metadata().options())));
        auto pkeys = co_await qp.vector_store_client().ann(_schema->ks_name(), _index.metadata().name(), _schema,
                get_ann_ordering_vector(_prepared_ann_ordering, options), fetch, filter_json, aoe.abort_source());
        if (!pkeys.has_value()) {
            co_await coroutine::return_exception(
                    exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::ann_error_visitor{}, pkeys.error())));
        }

        if (pkeys->size() > limit && !_rescoring.is_enabled()) {
            pkeys->erase(pkeys->begin() + limit, pkeys->end());
        }

        co_return co_await query_base_table(qp, state, options, pkeys.value(), timeout);
    });

    auto page_size = options.get_page_size();
    if (page_size > 0 && (uint64_t) page_size < limit) {
        result->add_warning("Paging is not supported for Vector Search queries. The entire result set has been returned.");
    }
    co_return result;
}

void vector_indexed_table_select_statement::update_stats() const {
    ++_stats.secondary_index_reads;
    ++_stats.query_cnt(source_selector::USER, _ks_sel, cond_selector::NO_CONDITIONS, statement_type::SELECT);
}

lw_shared_ptr<query::read_command> vector_indexed_table_select_statement::prepare_command_for_base_query(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t fetch_limit) const {
    auto slice = make_partition_slice(options);
    return ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(), std::move(slice), qp.proxy().get_max_result_size(slice),
            query::tombstone_limit(qp.proxy().get_tombstone_limit()),
            query::row_limit(get_inner_loop_limit(fetch_limit, _selection->is_aggregate())), query::partition_limit(query::max_partitions),
            _query_start_time_point, tracing::make_trace_info(state.get_trace_state()), query_id::create_null_id(), query::is_first_page::no,
            options.get_timestamp(state));
}

future<::shared_ptr<cql_transport::messages::result_message>> vector_indexed_table_select_statement::query_base_table(query_processor& qp,
        service::query_state& state, const query_options& options, const std::vector<vector_search::primary_key>& pkeys,
        lowres_clock::time_point timeout) const {
    auto command = prepare_command_for_base_query(qp, state, options, pkeys.size());

    auto result = co_await query_base_table(qp, state, options, command, timeout, pkeys);

    command->set_row_limit(get_limit(options, _limit));

    // Build a provider to inject similarity scores as temporaries during result processing.
    auto provider = _rescoring.make_similarity_provider(
        options, _prepared_ann_ordering.second, similarity_temporary_index);

    co_return co_await wrap_result_to_error_message([this, command = std::move(command), &options, &provider](auto query_result) {
        return process_results(std::move(query_result), command, options, _query_start_time_point, provider.get());
    })(std::move(result));
}

future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> vector_indexed_table_select_statement::query_base_table(query_processor& qp,
        service::query_state& state, const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
        const std::vector<vector_search::primary_key>& pkeys) const {

    // For tables without clustering columns, we can optimize by querying
    // partition ranges instead of individual primary keys, since the
    // partition key alone uniquely identifies each row.
    if (_schema->clustering_key_size() == 0) {
        auto to_partition_ranges = [](const std::vector<vector_search::primary_key>& pkeys) -> std::vector<dht::partition_range> {
            std::vector<dht::partition_range> partition_ranges;
            std::ranges::transform(pkeys, std::back_inserter(partition_ranges), [](const auto& pkey) {
                return dht::partition_range::make_singular(pkey.partition);
            });

            return partition_ranges;
        };
        co_return co_await query_base_table(qp, state, options, std::move(command), timeout, to_partition_ranges(pkeys));
    }
    co_return co_await utils::result_map_reduce(
            pkeys.begin(), pkeys.end(),
            [&](this auto, auto& key) -> future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> {
                auto cmd = ::make_lw_shared<query::read_command>(*command);
                cmd->slice._row_ranges = query::clustering_row_ranges{query::clustering_range::make_singular(key.clustering)};
                coordinator_result<service::storage_proxy::coordinator_query_result> rqr =
                        co_await qp.proxy().query_result(_schema, cmd, {dht::partition_range::make_singular(key.partition)}, options.get_consistency(),
                                {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state()});
                if (!rqr) {
                    co_return std::move(rqr).as_failure();
                }
                co_return std::move(rqr.value().query_result);
            },
            query::result_merger{command->get_row_limit(), query::max_partitions});
}

future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> vector_indexed_table_select_statement::query_base_table(query_processor& qp,
        service::query_state& state, const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
        std::vector<dht::partition_range> partition_ranges) const {

    coordinator_result<service::storage_proxy::coordinator_query_result> rqr = co_await qp.proxy()
            .query_result(_query_schema, command, std::move(partition_ranges), options.get_consistency(),
                    {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state(), {}, {}, options.get_specific_options().node_local_only},
                    std::nullopt);
    if (!rqr) {
        co_return std::move(rqr).as_failure();
    }
    co_return std::move(rqr.value().query_result);
}

} // namespace statements

} // namespace cql3
