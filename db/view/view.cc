/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/range/adaptor/transformed.hpp>
#include <deque>
#include <functional>
#include <optional>
#include <unordered_set>
#include <vector>

#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/transform.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>

#include <fmt/ranges.h>

#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "replica/database.hh"
#include "clustering_bounds_comparator.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/util.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/expr/evaluate.hh"
#include "db/view/view.hh"
#include "db/view/view_builder.hh"
#include "db/view/view_updating_consumer.hh"
#include "db/view/view_update_generator.hh"
#include "db/system_keyspace_view_types.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/tags/utils.hh"
#include "db/tags/extension.hh"
#include "gms/inet_address.hh"
#include "keys.hh"
#include "locator/network_topology_strategy.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "compaction/compaction_manager.hh"
#include "utils/assert.hh"
#include "utils/small_vector.hh"
#include "view_info.hh"
#include "view_update_checks.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "utils/error_injection.hh"
#include "utils/exponential_backoff_retry.hh"
#include "query-result-writer.hh"
#include "readers/from_fragments_v2.hh"
#include "readers/evictable.hh"
#include "delete_ghost_rows_visitor.hh"
#include "locator/host_id.hh"
#include "cartesian_product.hh"

using namespace std::chrono_literals;

static logging::logger vlogger("view");

static inline void inject_failure(std::string_view operation) {
    utils::get_local_injector().inject(operation,
            [operation] { throw std::runtime_error(std::string(operation)); });
}

view_info::view_info(const schema& schema, const raw_view_info& raw_view_info)
        : _schema(schema)
        , _raw(raw_view_info)
        , _has_computed_column_depending_on_base_non_primary_key(false)
{ }

cql3::statements::select_statement& view_info::select_statement(data_dictionary::database db) const {
    if (!_select_statement) {
        std::unique_ptr<cql3::statements::raw::select_statement> raw;
        // FIXME(sarna): legacy code, should be removed after "computed_columns" feature is guaranteed
        // to be available on every node. Then, we won't need to check if this view is backing a secondary index.
        const column_definition* legacy_token_column = nullptr;
        if (db.find_column_family(base_id()).get_index_manager().is_global_index(_schema)) {
           if (!_schema.clustering_key_columns().empty()) {
               legacy_token_column = &_schema.clustering_key_columns().front();
           }
        }

        if (legacy_token_column || boost::algorithm::any_of(_schema.all_columns(), std::mem_fn(&column_definition::is_computed))) {
            auto real_columns = _schema.all_columns() | boost::adaptors::filtered([legacy_token_column] (const column_definition& cdef) {
                return &cdef != legacy_token_column && !cdef.is_computed();
            });
            schema::columns_type columns = boost::copy_range<schema::columns_type>(std::move(real_columns));
            raw = cql3::util::build_select_statement(base_name(), where_clause(), include_all_columns(), columns);
        } else {
            raw = cql3::util::build_select_statement(base_name(), where_clause(), include_all_columns(), _schema.all_columns());
        }
        raw->prepare_keyspace(_schema.ks_name());
        raw->set_bound_variables({});
        cql3::cql_stats ignored;
        auto prepared = raw->prepare(db, ignored, true);
        _select_statement = static_pointer_cast<cql3::statements::select_statement>(prepared->statement);
    }
    return *_select_statement;
}

const query::partition_slice& view_info::partition_slice(data_dictionary::database db) const {
    if (!_partition_slice) {
        _partition_slice = select_statement(db).make_partition_slice(cql3::query_options({ }));
    }
    return *_partition_slice;
}

const column_definition* view_info::view_column(const schema& base, column_kind kind, column_id base_id) const {
    // FIXME: Map base column_ids to view_column_ids, which can be something like
    // a boost::small_vector where the position is the base column_id, and the
    // value is either empty or the view's column_id.
    return view_column(base.column_at(kind, base_id));
}

const column_definition* view_info::view_column(const column_definition& base_def) const {
    return _schema.get_column_definition(base_def.name());
}

void view_info::set_base_info(db::view::base_info_ptr base_info) {
    _base_info = std::move(base_info);
    // Forget the cached objects which may refer to the base schema.
    _select_statement = nullptr;
    _partition_slice = std::nullopt;
}

// A constructor for a base info that can facilitate reads and writes from the materialized view.
db::view::base_dependent_view_info::base_dependent_view_info(schema_ptr base_schema,
        std::vector<column_id>&& base_regular_columns_in_view_pk,
        std::vector<column_id>&& base_static_columns_in_view_pk)
        : _base_schema{std::move(base_schema)}
        , _base_regular_columns_in_view_pk{std::move(base_regular_columns_in_view_pk)}
        , _base_static_columns_in_view_pk{std::move(base_static_columns_in_view_pk)}
        , has_base_non_pk_columns_in_view_pk{!_base_regular_columns_in_view_pk.empty() || !_base_static_columns_in_view_pk.empty()}
        , use_only_for_reads{false} {

}

// A constructor for a base info that can facilitate only reads from the materialized view.
db::view::base_dependent_view_info::base_dependent_view_info(bool has_base_non_pk_columns_in_view_pk, std::optional<bytes>&& column_missing_in_base)
        : _base_schema{nullptr}
        , _column_missing_in_base{std::move(column_missing_in_base)}
        , has_base_non_pk_columns_in_view_pk{has_base_non_pk_columns_in_view_pk}
        , use_only_for_reads{true} {
}

const std::vector<column_id>& db::view::base_dependent_view_info::base_regular_columns_in_view_pk() const {
    if (use_only_for_reads) {
        on_internal_error(vlogger,
                format("base_regular_columns_in_view_pk(): operation unsupported when initialized only for view reads. "
                "Missing column in the base table: {}", to_sstring_view(_column_missing_in_base.value_or(bytes()))));
    }
    return _base_regular_columns_in_view_pk;
}

const std::vector<column_id>& db::view::base_dependent_view_info::base_static_columns_in_view_pk() const {
    if (use_only_for_reads) {
        on_internal_error(vlogger,
                format("base_static_columns_in_view_pk(): operation unsupported when initialized only for view reads. "
                "Missing column in the base table: {}", to_sstring_view(_column_missing_in_base.value_or(bytes()))));
    }
    return _base_static_columns_in_view_pk;
}

const schema_ptr& db::view::base_dependent_view_info::base_schema() const {
    if (use_only_for_reads) {
        on_internal_error(vlogger,
                format("base_schema(): operation unsupported when initialized only for view reads. "
                "Missing column in the base table: {}", to_sstring_view(_column_missing_in_base.value_or(bytes()))));
    }
    return _base_schema;
}

db::view::base_info_ptr view_info::make_base_dependent_view_info(const schema& base) const {
    std::vector<column_id> base_regular_columns_in_view_pk;
    std::vector<column_id> base_static_columns_in_view_pk;

    _is_partition_key_permutation_of_base_partition_key =
        boost::algorithm::all_of(_schema.partition_key_columns(), [&base] (const column_definition& view_col) {
            const column_definition* base_col = base.get_column_definition(view_col.name());
            return base_col && base_col->is_partition_key();
            })
        && _schema.partition_key_size() == base.partition_key_size();

    for (auto&& view_col : boost::range::join(_schema.partition_key_columns(), _schema.clustering_key_columns())) {
        if (view_col.is_computed()) {
            // we are not going to find it in the base table...
            if (view_col.get_computation().depends_on_non_primary_key_column()) {
                _has_computed_column_depending_on_base_non_primary_key = true;
            }
            continue;
        }
        const bytes& view_col_name = view_col.name();
        auto* base_col = base.get_column_definition(view_col_name);
        if (base_col && base_col->is_regular()) {
            base_regular_columns_in_view_pk.push_back(base_col->id);
        } else if (base_col && base_col->is_static()) {
            base_static_columns_in_view_pk.push_back(base_col->id);
        } else if (!base_col) {
            vlogger.error("Column {} in view {}.{} was not found in the base table {}.{}",
                    to_sstring_view(view_col_name), _schema.ks_name(), _schema.cf_name(), base.ks_name(), base.cf_name());
            if (to_sstring_view(view_col_name) == "idx_token") {
                vlogger.warn("Missing idx_token column is caused by an incorrect upgrade of a secondary index. "
                        "Please recreate index {}.{} to avoid future issues.", _schema.ks_name(), _schema.cf_name());
            }
            // If we didn't find the column in the base column then it must have been deleted
            // or not yet added (by alter command), this means it is for sure not a pk column
            // in the base table. This can happen if the version of the base schema is not the
            // one that the view was created with. Setting this schema as the base can't harm since
            // if we got to such a situation then it means it is only going to be used for reading
            // (computation of shadowable tombstones) and in that case the existence of such a column
            // is the only thing that is of interest to us.
            return make_lw_shared<db::view::base_dependent_view_info>(true, view_col_name);
        }
    }

    return make_lw_shared<db::view::base_dependent_view_info>(base.shared_from_this(), std::move(base_regular_columns_in_view_pk), std::move(base_static_columns_in_view_pk));
}

bool view_info::has_base_non_pk_columns_in_view_pk() const {
    // The base info is not always available, this is because
    // the base info initialization is separate from the view
    // info construction. If we are trying to get this info without
    // initializing the base information it means that we have a
    // schema integrity problem as the creator of owning view schema
    // didn't make sure to initialize it with base information.
    if (!_base_info) {
        on_internal_error(vlogger, "Tried to perform a view query which is base info dependent without initializing it");
    }
    return _base_info->has_base_non_pk_columns_in_view_pk;
}

clustering_row db::view::clustering_or_static_row::as_clustering_row(const schema& s) const {
    if (!is_clustering_row()) {
        on_internal_error(vlogger, "Tried to interpret a static row as a clustering row");
    }
    return clustering_row(*_key, tomb(), marker(), row(s, column_kind::regular_column, cells()));
}

static_row db::view::clustering_or_static_row::as_static_row(const schema& s) const {
    if (!is_static_row()) {
        on_internal_error(vlogger, "Tried to interpret a clustering row as a static row");
    }
    return static_row(s, cells());
}

namespace db {

namespace view {
stats::stats(const sstring& category, label_instance ks_label, label_instance cf_label) :
        service::storage_proxy_stats::write_stats(category, false),
        _ks_label(ks_label),
        _cf_label(cf_label) {
}

void stats::register_stats() {
    namespace ms = seastar::metrics;
    namespace sp_stats = service::storage_proxy_stats;
    _metrics.add_group("column_family", {
            ms::make_total_operations("view_updates_pushed_remote", view_updates_pushed_remote, ms::description("Number of updates (mutations) pushed to remote view replicas"),
                    {_cf_label, _ks_label}),
            ms::make_total_operations("view_updates_failed_remote", view_updates_failed_remote, ms::description("Number of updates (mutations) that failed to be pushed to remote view replicas"),
                    {_cf_label, _ks_label}),
            ms::make_total_operations("view_updates_pushed_local", view_updates_pushed_local, ms::description("Number of updates (mutations) pushed to local view replicas"),
                    {_cf_label, _ks_label}),
            ms::make_total_operations("view_updates_failed_local", view_updates_failed_local, ms::description("Number of updates (mutations) that failed to be pushed to local view replicas"),
                    {_cf_label, _ks_label}),
            ms::make_gauge("view_updates_pending", ms::description("Number of updates pushed to view and are still to be completed"),
                    {_cf_label, _ks_label}, writes),
    });
}

bool partition_key_matches(data_dictionary::database db, const schema& base, const view_info& view, const dht::decorated_key& key) {
    const cql3::expr::expression& pk_restrictions = view.select_statement(db).get_restrictions()->get_partition_key_restrictions();
    std::vector<bytes> exploded_pk = key.key().explode();
    std::vector<bytes> exploded_ck;
    std::vector<const column_definition*> pk_columns;
    pk_columns.reserve(base.partition_key_size());
    for (const column_definition& column : base.partition_key_columns()) {
        pk_columns.push_back(&column);
    }
    auto selection = cql3::selection::selection::for_columns(base.shared_from_this(), pk_columns);
    uint64_t zero = 0;
    auto dummy_row = query::result_row_view(ser::qr_row_view{simple_memory_input_stream(reinterpret_cast<const char*>(&zero), 8)});
    auto dummy_options = cql3::query_options({ });
    // FIXME: pass nullptrs for some of these dummies
    return cql3::expr::is_satisfied_by(
            pk_restrictions,
            cql3::expr::evaluation_inputs{
                .partition_key = exploded_pk,
                .clustering_key = exploded_ck,
                .static_and_regular_columns = {}, // partition key filtering only
                .selection = selection.get(),
                .options = &dummy_options,
            });
}

bool clustering_prefix_matches(data_dictionary::database db, const schema& base, const view_info& view, const partition_key& key, const clustering_key_prefix& ck) {
    const cql3::expr::expression& r = view.select_statement(db).get_restrictions()->get_clustering_columns_restrictions();
    std::vector<bytes> exploded_pk = key.explode();
    std::vector<bytes> exploded_ck = ck.explode();
    std::vector<const column_definition*> ck_columns;
    ck_columns.reserve(base.clustering_key_size());
    for (const column_definition& column : base.clustering_key_columns()) {
        ck_columns.push_back(&column);
    }
    auto selection = cql3::selection::selection::for_columns(base.shared_from_this(), ck_columns);
    uint64_t zero = 0;
    auto dummy_options = cql3::query_options({ });
    // FIXME: pass nullptrs for some of  these dummies
    return cql3::expr::is_satisfied_by(
            r,
            cql3::expr::evaluation_inputs{
                .partition_key = exploded_pk,
                .clustering_key = exploded_ck,
                .static_and_regular_columns = {}, // clustering key only filtering here
                .selection = selection.get(),
                .options = &dummy_options,
            });
}

bool may_be_affected_by(data_dictionary::database db, const schema& base, const view_info& view, const dht::decorated_key& key, const rows_entry& update) {
    // We can guarantee that the view won't be affected if:
    //  - the primary key is excluded by the view filter (note that this isn't true of the filter on regular columns:
    //    even if an update don't match a view condition on a regular column, that update can still invalidate a
    //    pre-existing entry) - note that the upper layers should already have checked the partition key;
    return clustering_prefix_matches(db, base, view, key.key(), update.key());
}

static bool update_requires_read_before_write(data_dictionary::database db, const schema& base,
        const std::vector<view_and_base>& views,
        const dht::decorated_key& key,
        const rows_entry& update) {
    for (auto&& v : views) {
        view_info& vf = *v.view->view_info();
        if (may_be_affected_by(db, base, vf, key, update)) {
            return true;
        }
    }
    return false;
}

// Checks if the result matches the provided view filter.
// It's currently assumed that the result consists of just a single row.
class view_filter_checking_visitor {
    data_dictionary::database _db;
    const schema& _base;
    const view_info& _view;
    ::shared_ptr<cql3::selection::selection> _selection;
    std::vector<bytes> _pk;

    bool _matches_view_filter = true;
public:
    view_filter_checking_visitor(data_dictionary::database db, const schema& base, const view_info& view)
        : _db(std::move(db))
        , _base(base)
        , _view(view)
        , _selection(cql3::selection::selection::for_columns(_base.shared_from_this(),
            boost::copy_range<std::vector<const column_definition*>>(
                _base.regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return &cdef; }))
            )
        )
    {}

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _pk = key.explode();
    }
    void accept_new_partition(uint64_t row_count) {
        throw std::logic_error("view_filter_checking_visitor expects an explicit partition key");
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) {
        _matches_view_filter = _matches_view_filter && check_if_matches(key, static_row, row);
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        throw std::logic_error("view_filter_checking_visitor expects an explicit clustering key");
    }
    void accept_partition_end(const query::result_row_view& static_row) {}

    bool check_if_matches(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) const {
        std::vector<bytes> ck = key.explode();
        return boost::algorithm::all_of(
            _view.select_statement(_db).get_restrictions()->get_non_pk_restriction() | boost::adaptors::map_values,
            [&] (auto&& r) {
                // FIXME: move outside all_of(). However, crashes.
                auto static_and_regular_columns = cql3::expr::get_non_pk_values(*_selection, static_row, &row);
                // FIXME: pass dummy_options as nullptr
                auto dummy_options = cql3::query_options({});
                return cql3::expr::is_satisfied_by(
                        r,
                        cql3::expr::evaluation_inputs{
                            .partition_key = _pk,
                            .clustering_key = ck,
                            .static_and_regular_columns = static_and_regular_columns,
                            .selection = _selection.get(),
                            .options = &dummy_options,
                        });
            }
        );
    }

    bool matches_view_filter() const {
        return _matches_view_filter;
    }
};

static query::partition_slice make_partition_slice(const schema& s) {
    query::partition_slice::option_set opts;
    opts.set(query::partition_slice::option::send_partition_key);
    opts.set(query::partition_slice::option::send_clustering_key);
    opts.set(query::partition_slice::option::send_timestamp);
    opts.set(query::partition_slice::option::send_ttl);
    opts.set(query::partition_slice::option::always_return_static_content);
    return query::partition_slice(
            {query::full_clustering_range},
            boost::copy_range<query::column_id_vector>(s.static_columns()
                    | boost::adaptors::transformed(std::mem_fn(&column_definition::id))),
            boost::copy_range<query::column_id_vector>(s.regular_columns()
                    | boost::adaptors::transformed(std::mem_fn(&column_definition::id))),
            std::move(opts));
}

class data_query_result_builder {
public:
    using result_type = query::result;

private:
    query::result::builder _res_builder;
    query_result_builder _builder;

public:
    data_query_result_builder(const schema& s, const query::partition_slice& slice)
        : _res_builder(slice, query::result_options::only_result(), query::result_memory_accounter{query::result_memory_limiter::unlimited_result_size}, query::max_tombstones)
        , _builder(s, _res_builder) { }

    void consume_new_partition(const dht::decorated_key& dk) { _builder.consume_new_partition(dk); }
    void consume(tombstone t) { _builder.consume(t); }
    stop_iteration consume(static_row&& sr, tombstone t, bool is_alive) { return _builder.consume(std::move(sr), t, is_alive); }
    stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_alive) { return _builder.consume(std::move(cr), t, is_alive); }
    stop_iteration consume(range_tombstone_change&& rtc) { return _builder.consume(std::move(rtc)); }
    stop_iteration consume_end_of_partition()  { return _builder.consume_end_of_partition(); }
    result_type consume_end_of_stream() {
        _builder.consume_end_of_stream();
        return _res_builder.build();
    }
};

bool matches_view_filter(data_dictionary::database db, const schema& base, const view_info& view, const partition_key& key, const clustering_or_static_row& update, gc_clock::time_point now) {
    // TODO: Filtering is only supported in materialized views which don't support
    // static rows yet. Skip the whole function if it is a static row update.
    if (update.is_static_row()) {
        return true;
    }

    auto slice = make_partition_slice(base);

    data_query_result_builder builder(base, slice);
    builder.consume_new_partition(dht::decorate_key(base, key));
    builder.consume(clustering_row(base, update.as_clustering_row(base)), row_tombstone{}, update.is_live(base, tombstone(), now));
    builder.consume_end_of_partition();
    auto result = builder.consume_end_of_stream();
    view_filter_checking_visitor visitor(db, base, view);
    query::result_view::consume(result, slice, visitor);

    return clustering_prefix_matches(db, base, view, key, *update.key())
            && visitor.matches_view_filter();
}

future<> view_updates::move_to(utils::chunked_vector<frozen_mutation_and_schema>& mutations) {
    mutations.reserve(mutations.size() + _updates.size());
    for (auto it = _updates.begin(); it != _updates.end(); it = _updates.erase(it)) {
        auto&& m = std::move(*it);
        auto mut = mutation(_view, dht::decorate_key(*_view, std::move(m.first)), std::move(m.second));
        mutations.emplace_back(frozen_mutation_and_schema{freeze(mut), _view});
        co_await coroutine::maybe_yield();
    }
    _op_count = 0;
}

mutation_partition& view_updates::partition_for(partition_key&& key) {
    auto it = _updates.find(key);
    if (it != _updates.end()) {
        return it->second;
    }
    return _updates.emplace(std::move(key), mutation_partition(*_view)).first->second;
}

size_t view_updates::op_count() const {
    return _op_count;
}

row_marker view_updates::compute_row_marker(const clustering_or_static_row& base_row) const {
    /*
     * We need to compute both the timestamp and expiration for view rows.
     *
     * Below there are several distinct cases depending on how many new key
     * columns the view has - i.e., how many of the view's key columns were
     * regular columns in the base. base_regular_columns_in_view_pk.size():
     *
     * Zero new key columns:
     *     The view rows key is composed only from base key columns, and those
     *     cannot be changed in an update, so the view row remains alive as
     *     long as the base row is alive. We need to return the same row
     *     marker as the base for the view - to keep an empty view row alive
    *      for as long as an empty base row exists.
     *     Note that in this case, if there are *unselected* base columns, we
     *     may need to keep an empty view row alive even without a row marker
     *     because the base row (which has additional columns) is still alive.
     *     For that we have the "virtual columns" feature: In the zero new
     *     key columns case, we put unselected columns in the view as empty
     *     columns, to keep the view row alive.
     *
     * One new key column:
     *     In this case, there is a regular base column that is part of the
     *     view key. This regular column can be added or deleted in an update,
     *     or its expiration be set, and those can cause the view row -
     *     including its row marker - to need to appear or disappear as well.
     *     So the liveness of cell of this one column determines the liveness
     *     of the view row and the row marker that we return.
     *
     * Two or more new key columns:
     *     This case is explicitly NOT supported in CQL - one cannot create a
     *     view with more than one base-regular columns in its key. In general
     *     picking one liveness (timestamp and expiration) is not possible
     *     if there are multiple regular base columns in the view key, as
     *     those can have different liveness.
     *     However, we do allow this case for Alternator - we need to allow
     *     the case of two (but not more) because the DynamoDB API allows
     *     creating a GSI whose two key columns (hash and range key) were
     *     regular columns.
     *     We can support this case in Alternator because it doesn't use
     *     expiration (the "TTL" it does support is different), and doesn't
     *     support user-defined timestamps. But, the two columns can still
     *     have different timestamps - this happens if an update modifies
     *     just one of them. In this case the timestamp of the view update
     *     (and that of the row marker we return) is the later of these two
     *     updated columns.
     */
    const auto& col_ids = base_row.is_clustering_row()
            ? _base_info->base_regular_columns_in_view_pk()
            : _base_info->base_static_columns_in_view_pk();
    if (!col_ids.empty()) {
        auto& def = _base->column_at(base_row.column_kind(), col_ids[0]);
        // Note: multi-cell columns can't be part of the primary key.
        auto cell = base_row.cells().cell_at(col_ids[0]).as_atomic_cell(def);
        auto ts = cell.timestamp();
        if (col_ids.size() > 1){
            // As explained above, this case only happens in Alternator,
            // and we may need to pick a higher ts:
            auto& second_def = _base->column_at(base_row.column_kind(), col_ids[1]);
            auto second_cell = base_row.cells().cell_at(col_ids[1]).as_atomic_cell(second_def);
            auto second_ts = second_cell.timestamp();
            ts = std::max(ts, second_ts);
            // Alternator isn't supposed to have TTL or more than two col_ids!
            if (col_ids.size() != 2 || cell.is_live_and_has_ttl() || second_cell.is_live_and_has_ttl()) [[unlikely]] {
                utils::on_internal_error(format("Unexpected col_ids length {} or has TTL", col_ids.size()));
            }
        }
        return cell.is_live_and_has_ttl() ? row_marker(ts, cell.ttl(), cell.expiry()) : row_marker(ts);
    }

    return base_row.marker();
}

namespace {
// The following struct is identical to view_key_with_action, except the key
// is stored as a managed_bytes_view instead of bytes.
struct view_managed_key_view_and_action {
    managed_bytes_view _key_view;
    view_key_and_action::action _action;
    view_managed_key_view_and_action(managed_bytes_view key_view, view_key_and_action::action action)
        : _key_view(key_view)
        , _action(action)
    {}
    view_managed_key_view_and_action(managed_bytes_view key_view)
        : _key_view(key_view)
    {}
    view_managed_key_view_and_action(view_key_and_action&& bwa, std::deque<bytes>& linearized_values)
        : view_managed_key_view_and_action(managed_bytes_view(linearized_values.emplace_back(std::move(bwa._key_bytes))), bwa._action)
    {}
    static managed_bytes_view get_key_view(const view_managed_key_view_and_action& bvwa) {
        return bvwa._key_view;
    }
};

// value_getter is used to extract values for specific columns during view update.
struct value_getter {
    // linearized_values hold bytes for values of computed columns, for which we later store references in the form of managed_bytes_view.
    // deque doesn't invalidate references at emplace_back.
    std::deque<bytes> linearized_values;

    // Index of column being currently processed.
    size_t column_position = 0;
    // Discovered index of collection computed column.
    std::optional<size_t> collection_column_position;
private:
    // Schemas of base table and view.
    const schema& _base;

    const partition_key& _base_key;
    const clustering_or_static_row& _update;
    const std::optional<clustering_or_static_row>& _existing;

public:
    value_getter(const schema& base, const partition_key& base_key, const clustering_or_static_row& update, const std::optional<clustering_or_static_row>& existing)
        : _base(base)
        , _base_key(base_key)
        , _update(update)
        , _existing(existing)
    {
    }

    using vector_type = utils::small_vector<view_managed_key_view_and_action, 1>;
    vector_type operator()(const column_definition& cdef) {
        column_position++;

        auto* base_col = _base.get_column_definition(cdef.name());
        if (!base_col) {
            return handle_computed_column(cdef);
        }
        switch (base_col->kind) {
        case column_kind::partition_key:
            return {_base_key.get_component(_base, base_col->position())};
        case column_kind::clustering_key:
            if (_update.is_static_row()) {
                on_internal_error(vlogger, "Tried to get view row value for a static row update in a view with partition key having clustering columns from original table");
            }
            return {_update.key()->get_component(_base, base_col->position())};
        default:
            if (base_col->kind != _update.column_kind()) {
                on_internal_error(vlogger, format("Tried to get a {} column from a {} row update, which is impossible",
                        to_sstring(base_col->kind), _update.is_clustering_row() ? "clustering" : "static"));
            }
            auto& c = _update.cells().cell_at(base_col->id);
            auto value_view = base_col->is_atomic() ? c.as_atomic_cell(cdef).value() : c.as_collection_mutation().data;
            return {managed_bytes_view{value_view}};
        }
    }

private:
    vector_type handle_computed_column(const column_definition& cdef) {
        if (!cdef.is_computed()) {
            throw std::logic_error{format(
                "Detected legacy non-computed token column {} in view for table {}.{}",
                cdef.name_as_text(), _base.ks_name(), _base.cf_name())};
        }

        auto& computation = cdef.get_computation();
        if (auto* collection_computation = dynamic_cast<const collection_column_computation*>(&computation)) {
            return handle_collection_column_computation(collection_computation);
        }

        auto computed_value = computation.compute_value(_base, _base_key);
        return {managed_bytes_view(linearized_values.emplace_back(std::move(computed_value)))};
    }

    vector_type handle_collection_column_computation(const collection_column_computation* collection_computation) {
        vector_type ret;
        if (collection_column_position.has_value()) {
            on_internal_error(vlogger, format("Multiple columns in view (either pk or ck) are collection computed columns. Current is {}, the previous one found was {}", column_position - 1, *collection_column_position));
        }
        collection_column_position = column_position - 1;

        for (auto& bwa : collection_computation->compute_values_with_action(_base, _base_key, _update, _existing)) {
            ret.push_back({std::move(bwa), linearized_values});
        }
        return ret;
    }
};
}


std::vector<view_updates::view_row_entry>
view_updates::get_view_rows(const partition_key& base_key, const clustering_or_static_row& update, const std::optional<clustering_or_static_row>& existing, row_tombstone row_delete_tomb) {
    value_getter getter(*_base, base_key, update, existing);
    auto get_value = boost::adaptors::transformed(std::ref(getter));


    std::vector<value_getter::vector_type> pk_elems, ck_elems;
    boost::copy(_view->partition_key_columns() | get_value, std::back_inserter(pk_elems));
    // If no collection column was found, each of the actions will contain no_action,
    // in particular, it does not harm to use column 0.
    const bool had_multiple_values_in_pk = bool(getter.collection_column_position);
    const size_t action_column = getter.collection_column_position.value_or(0);
    // Allow for at most one collection computed column in pk and in ck.
    getter.collection_column_position.reset();
    boost::copy(_view->clustering_key_columns() | get_value, std::back_inserter(ck_elems));
    const bool had_multiple_values_in_ck = bool(getter.collection_column_position);


    std::vector<view_updates::view_row_entry> ret;
    auto compute_row = [&]<typename Range>(Range&& pk, Range&& ck) {
        partition_key pkey = partition_key::from_range(boost::adaptors::transform(pk, view_managed_key_view_and_action::get_key_view));
        clustering_key ckey = clustering_key::from_range(boost::adaptors::transform(ck, view_managed_key_view_and_action::get_key_view));
        auto action = (action_column < pk.size() ? pk[action_column] : ck[action_column - pk.size()])._action;
        mutation_partition& partition = partition_for(std::move(pkey));

        // Skip adding the row if we already wrote a partition tombstone for this partition, and the update
        // is deleting the row with an equal row tombstone. This means the entire partition is deleted
        // so we don't need to generate updates for individual rows.
        if (partition.partition_tombstone() && partition.partition_tombstone() == row_delete_tomb.tomb()) {
            return;
        }

        ret.push_back({&partition.clustered_row(*_view, std::move(ckey)), action});
    };

    if (had_multiple_values_in_pk) {
        // cartesian_product expects std::vector<std::vector<>>, while we have std::vector<small_vector>.
        std::vector<std::vector<view_managed_key_view_and_action>> pk_elems_, ck_elems_;
        auto std_vector_from_small_vector = boost::adaptors::transformed([](const auto& vector) {
            return std::vector<view_managed_key_view_and_action>{vector.begin(), vector.end()};
        });
        boost::copy(pk_elems | std_vector_from_small_vector, std::back_inserter(pk_elems_));
        boost::copy(ck_elems | std_vector_from_small_vector, std::back_inserter(ck_elems_));

        auto cartesian_product_pk = cartesian_product(pk_elems_),
             cartesian_product_ck = cartesian_product(ck_elems_);
        auto ck_it = cartesian_product_ck.begin();

        if (had_multiple_values_in_ck) {
            // The computed collection column in clustering key was associated with the computed collection column from the partition key.
            // This is a case for indexes over collection values.

            auto throw_length_error = [&] {
                size_t pk_size = cartesian_product_size(pk_elems_),
                       ck_size = cartesian_product_size(ck_elems_);
                on_internal_error(vlogger, format("Computed sizes of possible partition keys and clustering keys don't match: {} != {}", pk_size, ck_size));
            };
            for (std::vector<view_managed_key_view_and_action>& pk : cartesian_product_pk) {
                if (ck_it == cartesian_product_ck.end()) {
                    throw_length_error();
                }
                compute_row(pk, *ck_it);
                ++ck_it;
            }
            if (ck_it != cartesian_product_ck.end()) {
                throw_length_error();
            }
        } else {
            for (std::vector<view_managed_key_view_and_action>& pk : cartesian_product_pk) {
                for (std::vector<view_managed_key_view_and_action>& ck : cartesian_product_ck) {
                    compute_row(pk, ck);
                }
            }
        }
    } else {
        // Here it's the old regular index over regular values. Each vector has just one element.
        auto get_front = boost::adaptors::transformed([](const auto& v) { return v.front(); });
        compute_row(pk_elems | get_front, ck_elems | get_front);
    }

    return ret;
}

static const column_definition* view_column(const schema& base, const schema& view, column_kind kind, column_id base_id) {
    // FIXME: Map base column_ids to view_column_ids, which can be something like
    // a boost::small_vector where the position is the base column_id, and the
    // value is either empty or the view's column_id.
    return view.get_column_definition(base.column_at(kind, base_id).name());
}

// Utility function for taking an existing cell, and creating a copy with an
// empty value instead of the original value, but with the original liveness
// information (expiration and deletion time) unchanged.
static atomic_cell make_empty(const atomic_cell_view& ac) {
    if (ac.is_live_and_has_ttl()) {
        return atomic_cell::make_live(*empty_type, ac.timestamp(), bytes_view{}, ac.expiry(), ac.ttl());
    } else if (ac.is_live()) {
        return atomic_cell::make_live(*empty_type, ac.timestamp(), bytes_view{});
    } else {
        return atomic_cell::make_dead(ac.timestamp(), ac.deletion_time());
    }
}

// Utility function for taking an existing collection which has both keys and
// values (i.e., either a list or map, but not a set), and creating a copy of
// this collection with all the values replaced by empty values.
// The make_empty() function above is used to ensure that liveness information
// is copied unchanged.
static collection_mutation make_empty(
        const collection_mutation_view& cm,
        const abstract_type& type) {
    collection_mutation_description n;
    cm.with_deserialized(type, [&] (collection_mutation_view_description m_view) {
        n.tomb = m_view.tomb;
        for (auto&& c : m_view.cells) {
            n.cells.emplace_back(c.first, make_empty(c.second));
        }
    });
    return n.serialize(type);
}

// In some cases, we need to copy to a view table even columns which have not
// been SELECTed. For these columns we only need to save liveness information
// (timestamp, deletion, ttl), but not the value. We call these columns
// "virtual columns", and the reason why we need them is explained in
// issue #3362. The following function, maybe_make_virtual() takes a full
// value c (taken from the base table) for the given column col, and if that
// column is a virtual column it modifies c to remove the unwanted value.
// The function create_virtual_column(), below, creates the virtual column in
// the view schema, that maybe_make_virtual() will fill.
static void maybe_make_virtual(atomic_cell_or_collection& c, const column_definition* col) {
    if (!col->is_view_virtual()) {
        // This is a regular selected column. Leave c untouched.
        return;
    }
    if (col->type->is_atomic()) {
        // A virtual cell for an atomic value or frozen collection. Its
        // value is empty (of type empty_type).
        if (col->type != empty_type) {
            throw std::logic_error("Virtual cell has wrong type");
        }
        c = make_empty(c.as_atomic_cell(*col));
    } else if (col->type->is_collection()) {
        auto ctype = static_pointer_cast<const collection_type_impl>(col->type);
        if (ctype->is_list()) {
            // A list has timeuuids as keys, and values (the list's items).
            // We just need to build a list with the same keys (and liveness
            // information), but empty values.
            auto ltype = static_cast<const list_type_impl*>(col->type.get());
            if (ltype->get_elements_type() != empty_type) {
                throw std::logic_error("Virtual cell has wrong list type");
            }
            c = make_empty(c.as_collection_mutation(), *ctype);
        } else if (ctype->is_map()) {
            // A map has keys and values. We just need to build a map with
            // the same keys (and liveness information), but empty values.
            auto mtype = static_cast<const map_type_impl*>(col->type.get());
            if (mtype->get_values_type() != empty_type) {
                throw std::logic_error("Virtual cell has wrong map type");
            }
            c = make_empty(c.as_collection_mutation(), *ctype);
        } else if (ctype->is_set()) {
            // A set has just keys (and liveness information). We need
            // all of it as a virtual column, unfortunately, so we
            // leave c unmodified.
        } else {
            // A collection can't be anything but a list, map or set...
            throw std::logic_error("Virtual cell has unexpected collection type");
        }
    } else if (col->type->is_user_type()) {
        // We leave c unmodified. See the comment in create_virtual_column regarding user types.
    } else {
        throw std::logic_error("Virtual cell is neither atomic nor collection, nor user type");
    }

}

void create_virtual_column(schema_builder& builder, const bytes& name, const data_type& type) {
    if (type->is_atomic()) {
        builder.with_column(name, empty_type, column_kind::regular_column, column_view_virtual::yes);
        return;
    }
    // A multi-cell collection or user type (a frozen collection
    // or user type is a single cell and handled in the is_atomic() case above).
    // The virtual version can't be just one cell, it has to be
    // itself a collection of cells.
    if (type->is_collection()) {
        auto ctype = static_pointer_cast<const collection_type_impl>(type);
        if (ctype->is_list()) {
            // A list has timeuuids as keys, and values (the list's items).
            // We just need these timeuuids, i.e., a list of empty items.
            builder.with_column(name, list_type_impl::get_instance(empty_type, true),
                    column_kind::regular_column, column_view_virtual::yes);
        } else if (ctype->is_map()) {
            // A map has keys and values. We don't need these values,
            // and can use empty values instead.
            auto mtype = static_pointer_cast<const map_type_impl>(type);
            builder.with_column(name, map_type_impl::get_instance(mtype->get_keys_type(), empty_type, true),
                    column_kind::regular_column, column_view_virtual::yes);
        } else if (ctype->is_set()) {
            // A set's cell has nothing beyond the keys, so the
            // virtual version of a set is, unfortunately, a complete
            // copy of the set.
            builder.with_column(name, type, column_kind::regular_column, column_view_virtual::yes);
        } else {
            // A collection can't be anything but a list, map or set...
            abort();
        }
    } else if (type->is_user_type()) {
        // FIXME (kbraun): we currently use the original type itself for the virtual version.
        // Instead we could try to:
        // 1. use a modified UDT with all value types replaced with empty_type,
        //    which would require creating and storing a completely new type in the DB
        //    just for the purpose of virtual columns,
        // 2. or use a map, which would require the make_empty function above
        //    to receive both the original type (UDT in this case) and virtual type (map in this case)
        //    to perform conversion correctly.
        builder.with_column(name, type, column_kind::regular_column, column_view_virtual::yes);
    } else {
        throw exceptions::invalid_request_exception(
                format("Unsupported unselected multi-cell non-collection, non-UDT column {} for Materialized View", name));
    }
}

static void add_cells_to_view(const schema& base, const schema& view, column_kind kind, row base_cells, row& view_cells) {
    base_cells.for_each_cell([&] (column_id id, atomic_cell_or_collection& c) {
        auto* view_col = view_column(base, view, kind, id);
        if (view_col && !view_col->is_primary_key()) {
            maybe_make_virtual(c, view_col);
            view_cells.append_cell(view_col->id, std::move(c));
        }
    });
}

/**
 * Creates a view entry corresponding to the provided base row.
 * This method checks that the base row does match the view filter before applying anything.
 */
void view_updates::create_entry(data_dictionary::database db, const partition_key& base_key, const clustering_or_static_row& update, gc_clock::time_point now) {
    if (!matches_view_filter(db, *_base, _view_info, base_key, update, now)) {
        return;
    }

    auto view_rows = get_view_rows(base_key, update, std::nullopt, {});
    auto update_marker = compute_row_marker(update);
    const auto kind = update.column_kind();
    for (const auto& [r, action]: view_rows) {
        if (auto rm = std::get_if<row_marker>(&action)) {
            r->apply(*rm);
        } else {
            r->apply(update_marker);
        }
        r->apply(update.tomb());
        add_cells_to_view(*_base, *_view, kind, row(*_base, kind, update.cells()), r->cells());
    }
    _op_count += view_rows.size();
}

/**
 * Deletes the view entry corresponding to the provided base row.
 * This method checks that the base row does match the view filter before bothering.
 */
void view_updates::delete_old_entry(data_dictionary::database db, const partition_key& base_key, const clustering_or_static_row& existing, const clustering_or_static_row& update, gc_clock::time_point now) {
    // Before deleting an old entry, make sure it was matching the view filter
    // (otherwise there is nothing to delete)
    if (matches_view_filter(db, *_base, _view_info, base_key, existing, now)) {
        do_delete_old_entry(base_key, existing, update, now);
    }
}

void view_updates::do_delete_old_entry(const partition_key& base_key, const clustering_or_static_row& existing, const clustering_or_static_row& update, gc_clock::time_point now) {
    auto view_rows = get_view_rows(base_key, existing, std::nullopt, update.tomb());
    const auto kind = existing.column_kind();
    for (const auto& [r, action] : view_rows) {
        const auto& col_ids = existing.is_clustering_row()
                ? _base_info->base_regular_columns_in_view_pk()
                : _base_info->base_static_columns_in_view_pk();
        if (_view_info.has_computed_column_depending_on_base_non_primary_key()) {
            if (auto ts_tag = std::get_if<view_key_and_action::shadowable_tombstone_tag>(&action)) {
                r->apply(ts_tag->into_shadowable_tombstone(now));
            }
        } else if (!col_ids.empty()) {
            // We delete the old row using a shadowable row tombstone, making sure that
            // the tombstone deletes everything in the row (or it might still show up).
            // Note: multi-cell columns can't be part of the primary key.
            auto& def = _base->column_at(kind, col_ids[0]);
            auto cell = existing.cells().cell_at(col_ids[0]).as_atomic_cell(def);
            auto ts = cell.timestamp();
            if (col_ids.size() > 1) {
                // This is the Alternator-only support for two regular base
                // columns that become view key columns. See explanation in
                // view_updates::compute_row_marker().
                auto& second_def = _base->column_at(kind, col_ids[1]);
                auto second_cell = existing.cells().cell_at(col_ids[1]).as_atomic_cell(second_def);
                auto second_ts = second_cell.timestamp();
                ts = std::max(ts, second_ts);
                // Alternator isn't supposed to have more than two col_ids!
                if (col_ids.size() != 2) [[unlikely]] {
                    utils::on_internal_error(format("Unexpected col_ids length {}", col_ids.size()));
                }
            }
            if (cell.is_live()) {
                r->apply(shadowable_tombstone(ts, now));
            }
        } else {
            // "update" caused the base row to have been deleted, and !col_id
            // means view row is the same - so it needs to be deleted as well
            // using the same deletion timestamps for the individual cells.
            r->apply(update.marker());
            auto diff = update.cells().difference(*_base, kind, existing.cells());
            add_cells_to_view(*_base, *_view, kind, std::move(diff), r->cells());
        }
        r->apply(update.tomb());
    }
    _op_count += view_rows.size();
}

/*
 * Atomic cells have equal liveness if they're either both dead, or both non-expiring,
 * or have exactly the same expiration. Comparing liveness is useful for view-virtual
 * cells, as generating updates from them is not needed if their livenesses match.
 */
static bool atomic_cells_liveness_equal(atomic_cell_view left, atomic_cell_view right) {
    if (left.is_live() != right.is_live()) {
        return false;
    }
    if (left.is_live()) {
        if (left.is_live_and_has_ttl() != right.is_live_and_has_ttl()) {
            return false;
        }
        if (left.is_live_and_has_ttl() && left.expiry() != right.expiry()) {
            return false;
        }
    }
    return true;
}

bool view_updates::can_skip_view_updates(const clustering_or_static_row& update, const clustering_or_static_row& existing) const {
    const row& existing_row = existing.cells();
    const row& updated_row = update.cells();

    const bool base_has_nonexpiring_marker = update.marker().is_live() && !update.marker().is_expiring();
    return boost::algorithm::all_of(_base->regular_columns(), [this, &updated_row, &existing_row, base_has_nonexpiring_marker] (const column_definition& cdef) {
        const auto view_it = _view->columns_by_name().find(cdef.name());
        const bool column_is_selected = view_it != _view->columns_by_name().end();

        //TODO(sarna): Optimize collections case - currently they do not go under optimization
        if (!cdef.is_atomic()) {
            return false;
        }

        // We cannot skip if the value was created or deleted, unless we have a non-expiring marker
        const auto* existing_cell = existing_row.find_cell(cdef.id);
        const auto* updated_cell = updated_row.find_cell(cdef.id);
        if (existing_cell == nullptr || updated_cell == nullptr) {
            return existing_cell == updated_cell || (!column_is_selected && base_has_nonexpiring_marker);
        }
        atomic_cell_view existing_cell_view = existing_cell->as_atomic_cell(cdef);
        atomic_cell_view updated_cell_view = updated_cell->as_atomic_cell(cdef);

        // We cannot skip when a selected column is changed
        if (column_is_selected) {
            if (view_it->second->is_view_virtual()) {
                return atomic_cells_liveness_equal(existing_cell_view, updated_cell_view);
            }
            return compare_atomic_cell_for_merge(existing_cell_view, updated_cell_view) == 0;
        }

        // With non-expiring row marker, liveness checks below are not relevant
        if (base_has_nonexpiring_marker) {
            return true;
        }

        if (existing_cell_view.is_live() != updated_cell_view.is_live()) {
            return false;
        }

        // We cannot skip if the change updates TTL
        const bool existing_has_ttl = existing_cell_view.is_live_and_has_ttl();
        const bool updated_has_ttl = updated_cell_view.is_live_and_has_ttl();
        if (existing_has_ttl || updated_has_ttl) {
            return existing_has_ttl == updated_has_ttl && existing_cell_view.expiry() == updated_cell_view.expiry();
        }

        return true;
    });
}

/**
 * Creates the updates to apply to the existing view entry given the base table row before
 * and after the update, assuming that the update hasn't changed to which view entry the
 * row corresponds (that is, we know the columns composing the view PK haven't changed).
 *
 * This method checks that the base row (before and after) matches the view filter before
 * applying anything.
 */
void view_updates::update_entry(data_dictionary::database db, const partition_key& base_key, const clustering_or_static_row& update, const clustering_or_static_row& existing, gc_clock::time_point now) {
    // While we know update and existing correspond to the same view entry,
    // they may not match the view filter.
    if (!matches_view_filter(db, *_base, _view_info, base_key, existing, now)) {
        create_entry(db, base_key, update, now);
        return;
    }
    if (!matches_view_filter(db, *_base, _view_info, base_key, update, now)) {
        do_delete_old_entry(base_key, existing, update, now);
        return;
    }

    if (can_skip_view_updates(update, existing)) {
        return;
    }

    auto view_rows = get_view_rows(base_key, update, std::nullopt, {});
    auto update_marker = compute_row_marker(update);
    const auto kind = update.column_kind();
    for (const auto& [r, action] : view_rows) {
        if (auto rm = std::get_if<row_marker>(&action)) {
            r->apply(*rm);
        } else {
            r->apply(update_marker);
        }
        r->apply(update.tomb());

        auto diff = update.cells().difference(*_base, kind, existing.cells());
        add_cells_to_view(*_base, *_view, kind, std::move(diff), r->cells());
    }
    _op_count += view_rows.size();
}

void view_updates::update_entry_for_computed_column(
        const partition_key& base_key,
        const clustering_or_static_row& update,
        const std::optional<clustering_or_static_row>& existing,
        gc_clock::time_point now) {
    auto view_rows = get_view_rows(base_key, update, existing, {});
    for (const auto& [r, action] : view_rows) {
        struct visitor {
            deletable_row* row;
            gc_clock::time_point now;
            void operator()(view_key_and_action::no_action) {}
            void operator()(view_key_and_action::shadowable_tombstone_tag t) {
                row->apply(t.into_shadowable_tombstone(now));
            }
            void operator()(row_marker rm) {
                row->apply(rm);
            }
        };
        std::visit(visitor{r, now}, action);
    }
}

void view_updates::generate_update(
        data_dictionary::database db,
        const partition_key& base_key,
        const clustering_or_static_row& update,
        const std::optional<clustering_or_static_row>& existing,
        gc_clock::time_point now) {

    // Note that the base PK columns in update and existing are the same, since we're intrinsically dealing
    // with the same base row. So we have to check 3 things:
    //   1) that the clustering key doesn't have a null, which can happen for compact tables. If that's the case,
    //      there is no corresponding entries.
    //   2) if there is a column not part of the base PK in the view PK, whether it is changed by the update.
    //   3) whether the update actually matches the view SELECT filter

    if (update.is_clustering_row()) {
        if (!update.key()->is_full(*_base)) {
            return;
        }
    }

    if (_view_info.has_computed_column_depending_on_base_non_primary_key()) {
        return update_entry_for_computed_column(base_key, update, existing, now);
    }
    if (!_base_info->has_base_non_pk_columns_in_view_pk) {
        if (update.is_static_row()) {
            // TODO: support static rows in views with pk only including columns from base pk
            return;
        }
        // The view key is necessarily the same pre and post update.
        if (existing && existing->is_live(*_base)) {
            if (update.is_live(*_base)) {
                update_entry(db, base_key, update, *existing, now);
            } else {
                delete_old_entry(db, base_key, *existing, update, now);
            }
        } else if (update.is_live(*_base)) {
            create_entry(db, base_key, update, now);
        }
        return;
    }

    const auto& col_ids = update.is_clustering_row()
            ? _base_info->base_regular_columns_in_view_pk()
            : _base_info->base_static_columns_in_view_pk();

    // The view has a non-primary-key column from the base table as its primary key.
    // That means it's either a regular or static column. If we are currently
    // processing an update which does not correspond to the column's kind,
    // just stop here.
    if (col_ids.empty()) {
        return;
    }

    const auto kind = update.column_kind();

    // If one of the key columns is missing, set has_new_row = false
    // meaning that after the update there will be no view row.
    // If one of the key columns is missing in the existing value,
    // set has_old_row = false meaning we don't have an old row to
    // delete.
    bool has_old_row = true;
    bool has_new_row = true;
    bool same_row = true;
    for (auto col_id : col_ids) {
        auto* after = update.cells().find_cell(col_id);
        auto& cdef = _base->column_at(kind, col_id);
        if (existing) {
            auto* before = existing->cells().find_cell(col_id);
            // Note that this cell is necessarily atomic, because col_ids are
            // view key columns, and keys must be atomic.
            if (before && before->as_atomic_cell(cdef).is_live()) {
                if (after && after->as_atomic_cell(cdef).is_live()) {
                    // We need to compare just the values of the keys, not
                    // metadata like the timestamp. This is because below,
                    // if the old and new view row have the same key, we need
                    // to be sure to reach the update_entry() case.
                    auto cmp = compare_unsigned(before->as_atomic_cell(cdef).value(), after->as_atomic_cell(cdef).value());
                    if (cmp != 0) {
                        same_row = false;
                    }
                }
            } else {
                has_old_row = false;
            }
        } else {
            has_old_row = false;
        }
        if (!after || !after->as_atomic_cell(cdef).is_live()) {
            has_new_row = false;
        }
    }
    if (has_old_row) {
        if (has_new_row) {
            if (same_row) {
                update_entry(db, base_key, update, *existing, now);
            } else {
                // This code doesn't work if the old and new view row have the
                // same key, because if they do we get both data and tombstone
                // for the same timestamp (now) and the tombstone wins. This
                // is why we need the "same_row" case above - it's not just a
                // performance optimization.
                delete_old_entry(db, base_key, *existing, update, now);
                create_entry(db, base_key, update, now);
            }
        } else {
            delete_old_entry(db, base_key, *existing, update, now);
        }
    } else if (has_new_row) {
        create_entry(db, base_key, update, now);
    }
}

bool view_updates::is_partition_key_permutation_of_base_partition_key() const {
    return _view_info.is_partition_key_permutation_of_base_partition_key();
}

std::optional<partition_key> view_updates::construct_view_partition_key_from_base(const partition_key& base_pk)
{
    // We check that the view partition key is a permutation of the
    // base partition key. If so, we can construct the corresponding
    // view partition key from the base key and apply an optimized
    // partition level update. Otherwise, we return std::nullopt.

    if (!is_partition_key_permutation_of_base_partition_key()) {
        return std::nullopt;
    }

    auto base_exploded_pk = base_pk.explode();
    std::vector<bytes> view_exploded_pk(_view->partition_key_size());

    // Construct the view partition key by finding each component
    // in the base partition key.
    for (const column_definition& view_cdef : _view->partition_key_columns()) {
        const column_definition* base_cdef = _base->get_column_definition(view_cdef.name());
        if (base_cdef && base_cdef->is_partition_key()) {
            view_exploded_pk[view_cdef.id] = base_exploded_pk[base_cdef->id];
        } else {
            // This shouldn't happen because we already checked that all
            // the view partition key columns appear in the base partition key.
            on_internal_error(vlogger, format("Unexpected failure to construct view partition update for view {}.{} of {}.{}, ",
                _view->ks_name(), _view->cf_name(), _base->ks_name(), _base->cf_name()));
        }
    }

    partition_key view_pk = partition_key::from_exploded(view_exploded_pk);
    return view_pk;
}

bool view_updates::generate_partition_tombstone_update(
        data_dictionary::database db,
        const partition_key& base_key,
        tombstone partition_tomb) {

    // Try to construct the view partition key from the base partition key.
    // This will succeed if the view partition key columns are a permutation
    // of the base partition key columns. If it fails, we skip the optimization.
    auto view_key_opt = construct_view_partition_key_from_base(base_key);
    if (!view_key_opt) {
        return false;
    }

    // Apply the partition tombstone on the view partition
    mutation_partition& mp = partition_for(std::move(*view_key_opt));
    mp.apply(partition_tomb);

    _op_count++;
    return true;
}

future<> view_update_builder::close() noexcept {
    return when_all_succeed(_updates.close(), _existings->close()).discard_result();
}

future<stop_iteration> view_update_builder::advance_all() {
    auto existings_f = _existings ? (*_existings)() : make_ready_future<mutation_fragment_v2_opt>();
    return when_all(_updates(), std::move(existings_f)).then([this] (auto&& fragments) mutable {
        _update = std::move(std::get<0>(fragments).get());
        _existing = std::move(std::get<1>(fragments).get());
        return stop_iteration::no;
    });
}

future<stop_iteration> view_update_builder::advance_updates() {
    return _updates().then([this] (auto&& update) mutable {
        _update = std::move(update);
        return stop_iteration::no;
    });
}

future<stop_iteration> view_update_builder::advance_existings() {
    if (!_existings) {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    return (*_existings)().then([this] (auto&& existing) mutable {
        _existing = std::move(existing);
        return stop_iteration::no;
    });
}

future<stop_iteration> view_update_builder::stop() const {
    return make_ready_future<stop_iteration>(stop_iteration::yes);
}

future<std::optional<utils::chunked_vector<frozen_mutation_and_schema>>> view_update_builder::build_some() {
    (void)co_await advance_all();
    if (!_update && !_existing) {
        // Tell the caller there is no more data to build.
        co_return std::nullopt;
    }
    bool do_advance_updates = false;
    bool do_advance_existings = false;
    bool is_partition_tombstone_applied_on_all_views = false;
    if (_update && _update->is_partition_start()) {
        _key = std::move(std::move(_update)->as_partition_start().key().key());
        _update_partition_tombstone = _update->as_partition_start().partition_tombstone();
        do_advance_updates = true;

        if (_update_partition_tombstone) {
            // For views that have the same partition key as base, generate an update of partition tombstone to delete
            // the entire partition in one operation, instead of generating an update for each row.
            is_partition_tombstone_applied_on_all_views = true;
            for (auto&& v : _view_updates) {
                bool is_applied = v.is_partition_key_permutation_of_base_partition_key() && v.generate_partition_tombstone_update(_db, _key, _update_partition_tombstone);
                is_partition_tombstone_applied_on_all_views &= is_applied;
            }
        }
    }
    if (_existing && _existing->is_partition_start()) {
        _existing_partition_tombstone = _existing->as_partition_start().partition_tombstone();
        do_advance_existings = true;
    }
    if (do_advance_updates) {
        co_await (do_advance_existings ? advance_all() : advance_updates());
    } else if (do_advance_existings) {
        co_await advance_existings();
    }

    // If the partition tombstone update is applied to all the views and there are no other updates, we can skip going over
    // all the rows trying to generate row updates, because the partition tombstones already cover everything.
    if (is_partition_tombstone_applied_on_all_views && _update->is_end_of_partition()) {
        _skip_row_updates = true;
    }

    while (!_skip_row_updates && co_await on_results() == stop_iteration::no) {};

    utils::chunked_vector<frozen_mutation_and_schema> mutations;
    for (auto& update : _view_updates) {
        co_await update.move_to(mutations);
    }
    co_return mutations;
}

void view_update_builder::generate_update(clustering_row&& update, std::optional<clustering_row>&& existing) {
    if (update.empty()) {
        // An empty update row (no cells and no tombstone) is rare, but it is
        // possible (see #15228): A mutation can modify a column that was
        // later dropped, and upgrade()ing the mutation's schema in
        // table::do_push_view_replica_updates() left an empty row.
        return;
    }

    auto dk = dht::decorate_key(*_schema, _key);
    const auto& gc_state = _base.get_compaction_manager().get_tombstone_gc_state();
    auto gc_before = gc_state.get_gc_before_for_key(_schema, dk, _now);

    // We allow existing to be disengaged, which we treat the same as an empty row.
    if (existing) {
        existing->marker().compact_and_expire(existing->tomb().tomb(), _now, always_gc, gc_before);
        existing->cells().compact_and_expire(*_schema, column_kind::regular_column, existing->tomb(), _now, always_gc, gc_before, existing->marker());
        update.apply(*_schema, *existing);
    }

    update.marker().compact_and_expire(update.tomb().tomb(), _now, always_gc, gc_before);
    update.cells().compact_and_expire(*_schema, column_kind::regular_column, update.tomb(), _now, always_gc, gc_before, update.marker());

    const auto update_row = clustering_or_static_row(std::move(update));
    const auto existing_row = existing
            ? std::make_optional<clustering_or_static_row>(std::move(*existing))
            : std::optional<clustering_or_static_row>();
    for (auto&& v : _view_updates) {
        v.generate_update(_db, _key, update_row, existing_row, _now);
    }
}

void view_update_builder::generate_update(static_row&& update, const tombstone& update_tomb,
        std::optional<static_row>&& existing, const tombstone& existing_tomb) {
    if (!update_tomb && update.empty()) {
        throw std::logic_error("A materialized view update cannot be empty");
    }

    auto dk = dht::decorate_key(*_schema, _key);
    const auto& gc_state = _base.get_compaction_manager().get_tombstone_gc_state();
    auto gc_before = gc_state.get_gc_before_for_key(_schema, dk, _now);

    // We allow existing to be disengaged, which we treat the same as an empty row.
    if (existing) {
        existing->cells().compact_and_expire(*_schema, column_kind::static_column, row_tombstone(existing_tomb), _now, always_gc, gc_before);
        update.apply(*_schema, static_row(*_schema, *existing));
    }

    update.cells().compact_and_expire(*_schema, column_kind::static_column, row_tombstone(update_tomb), _now, always_gc, gc_before);

    const auto update_row = clustering_or_static_row(std::move(update));
    const auto existing_row = existing
            ? std::make_optional<clustering_or_static_row>(std::move(*existing))
            : std::optional<clustering_or_static_row>();
    for (auto&& v : _view_updates) {
        v.generate_update(_db, _key, update_row, existing_row, _now);
    }
}

future<stop_iteration> view_update_builder::on_results() {
    constexpr size_t max_rows_for_view_updates = 100;
    auto should_stop_updates = [this] () -> bool {
        size_t rows_for_view_updates = std::accumulate(_view_updates.begin(), _view_updates.end(), 0, [] (size_t acc, const view_updates& vu) {
            return acc + vu.op_count();
        });
        return rows_for_view_updates >= max_rows_for_view_updates;
    };
    if (_update && !_update->is_end_of_partition() && _existing && !_existing->is_end_of_partition()) {
        auto cmp = position_in_partition::tri_compare(*_schema)(_update->position(), _existing->position());
        if (cmp < 0) {
            // We have an update where there was nothing before
            if (_update->is_range_tombstone_change()) {
                _update_current_tombstone = _update->as_range_tombstone_change().tombstone();
            } else if (_update->is_clustering_row()) {
                auto update = std::move(*_update).as_clustering_row();
                update.apply(std::max(_update_partition_tombstone, _update_current_tombstone));
                auto tombstone = std::max(_existing_partition_tombstone, _existing_current_tombstone);
                auto existing = tombstone
                              ? std::optional<clustering_row>(std::in_place, update.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row())
                              : std::nullopt;
                generate_update(std::move(update), std::move(existing));
            } else if (_update->is_static_row()) {
                auto update = std::move(*_update).as_static_row();
                auto tombstone = _existing_partition_tombstone;
                auto existing = tombstone
                              ? std::optional<static_row>(std::in_place)
                              : std::nullopt;
                generate_update(std::move(update), _update_partition_tombstone, std::move(existing), _existing_partition_tombstone);
            }
            return should_stop_updates() ? stop() : advance_updates();
        }
        if (cmp > 0) {
            // We have something existing but no update (which will happen either because it's a range tombstone marker in
            // existing, or because we've fetched the existing row due to some partition/range deletion in the updates).
            // Due to how the read command for existing rows is constructed, it is also possible that there is a static
            // row is included, even though we didn't modify it.
            if (_existing->is_range_tombstone_change()) {
                _existing_current_tombstone = _existing->as_range_tombstone_change().tombstone();
            } else if (_existing->is_clustering_row()) {
                auto existing = std::move(*_existing).as_clustering_row();
                existing.apply(std::max(_existing_partition_tombstone, _existing_current_tombstone));
                auto tombstone = std::max(_update_partition_tombstone, _update_current_tombstone);
                // The way we build the read command used for existing rows, we should always have a non-empty
                // tombstone, since we wouldn't have read the existing row otherwise. We don't SCYLLA_ASSERT that in case the
                // read method ever changes.
                if (tombstone) {
                    auto update = clustering_row(existing.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row());
                    generate_update(std::move(update), { std::move(existing) });
                }
            } else if (_existing->is_static_row()) {
                auto existing = std::move(*_existing).as_static_row();
                auto tombstone = _update_partition_tombstone;
                // The static row might be unintentionally included when fetching existing clustering rows,
                // even if the static row was not updated. We can detect it. A static row can be affected either by:
                //
                // 1. A static row in the update mutation
                // 2. A partition tombstone in the update mutation
                //
                // If neither of those is present, this means that the static row is included accidentally.
                // If we are here, this means that (1) is not present. The `if` that follows checks for (2).
                if (tombstone) {
                    auto update = static_row();
                    generate_update(std::move(update), _update_partition_tombstone, { std::move(existing) }, _existing_partition_tombstone);
                }
            }
            return should_stop_updates() ? stop () : advance_existings();
        }
        // We're updating a row that had pre-existing data
        if (_update->is_range_tombstone_change()) {
            SCYLLA_ASSERT(_existing->is_range_tombstone_change());
            _existing_current_tombstone = std::move(*_existing).as_range_tombstone_change().tombstone();
            _update_current_tombstone = std::move(*_update).as_range_tombstone_change().tombstone();
        } else if (_update->is_clustering_row()) {
            SCYLLA_ASSERT(_existing->is_clustering_row());
            _update->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                cr.apply(std::max(_update_partition_tombstone, _update_current_tombstone));
            });
            _existing->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                cr.apply(std::max(_existing_partition_tombstone, _existing_current_tombstone));
            });
            generate_update(std::move(*_update).as_clustering_row(), { std::move(*_existing).as_clustering_row() });
        } else if (_update->is_static_row()) {
            if (!_existing->is_static_row()) {
                on_internal_error(vlogger, format("Static row update mutation part {} shouldn't compare equal with an existing, non-static row mutation part {}",
                                                  mutation_fragment_v2::printer(*_schema, *_update), mutation_fragment_v2::printer(*_schema, *_existing)));
            }
            generate_update(std::move(*_update).as_static_row(), _update_partition_tombstone, { std::move(*_existing).as_static_row() }, _existing_partition_tombstone);

        }
        return should_stop_updates() ? stop() : advance_all();
    }

    auto tombstone = std::max(_update_partition_tombstone, _update_current_tombstone);
    if (tombstone && _existing && !_existing->is_end_of_partition()) {
        // We don't care if it's a range tombstone, as we're only looking for existing entries that get deleted
        if (_existing->is_clustering_row()) {
            auto existing = clustering_row(*_schema, _existing->as_clustering_row());
            auto update = clustering_row(existing.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row());
            generate_update(std::move(update), { std::move(existing) });
        } else if (_existing->is_static_row()) {
            auto existing = static_row(*_schema, _existing->as_static_row());
            auto update = static_row();
            generate_update(std::move(update), _update_partition_tombstone, { std::move(existing) }, _existing_partition_tombstone);
        }
        return should_stop_updates() ? stop() : advance_existings();
    }

    // If we have updates and it's a range tombstone, it removes nothing pre-exisiting, so we can ignore it
    if (_update && !_update->is_end_of_partition()) {
        if (_update->is_clustering_row()) {
            _update->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                cr.apply(std::max(_update_partition_tombstone, _update_current_tombstone));
            });
            auto existing_tombstone = std::max(_existing_partition_tombstone, _existing_current_tombstone);
            auto existing = existing_tombstone
                          ? std::optional<clustering_row>(std::in_place, _update->as_clustering_row().key(), row_tombstone(std::move(existing_tombstone)), row_marker(), ::row())
                          : std::nullopt;
            generate_update(std::move(*_update).as_clustering_row(), std::move(existing));
        } else if (_update->is_static_row()) {
            auto existing_tombstone = _existing_partition_tombstone;
            auto existing = existing_tombstone
                          ? std::optional<static_row>(std::in_place)
                          : std::nullopt;
            generate_update(std::move(*_update).as_static_row(), _update_partition_tombstone, std::move(existing), _existing_partition_tombstone);
        }
        return should_stop_updates() ? stop() : advance_updates();
    }

    return stop();
}

view_update_builder make_view_update_builder(
        data_dictionary::database db,
        const replica::table& base_table,
        const schema_ptr& base,
        std::vector<view_and_base>&& views_to_update,
        mutation_reader&& updates,
        mutation_reader_opt&& existings,
        gc_clock::time_point now) {
    auto vs = boost::copy_range<std::vector<view_updates>>(views_to_update | boost::adaptors::transformed([&] (view_and_base v) {
        if (base->version() != v.base->base_schema()->version()) {
            on_internal_error(vlogger, format("Schema version used for view updates ({}) does not match the current"
                                              " base schema version of the view ({}) for view {}.{} of {}.{}",
                base->version(), v.base->base_schema()->version(), v.view->ks_name(), v.view->cf_name(), base->ks_name(), base->cf_name()));
        }
        return view_updates(std::move(v));
    }));
    return view_update_builder(std::move(db), base_table, base, std::move(vs), std::move(updates), std::move(existings), now);
}

future<query::clustering_row_ranges> calculate_affected_clustering_ranges(data_dictionary::database db,
        const schema& base,
        const dht::decorated_key& key,
        const mutation_partition& mp,
        const std::vector<view_and_base>& views) {
    utils::chunked_vector<interval<clustering_key_prefix_view>> row_ranges;
    utils::chunked_vector<interval<clustering_key_prefix_view>> view_row_ranges;
    clustering_key_prefix_view::tri_compare cmp(base);
    if (mp.partition_tombstone() || !mp.row_tombstones().empty()) {
        for (auto&& v : views) {
            // FIXME: #2371
            if (v.view->view_info()->select_statement(db).get_restrictions()->has_unrestricted_clustering_columns()) {
                view_row_ranges.push_back(interval<clustering_key_prefix_view>::make_open_ended_both_sides());
                break;
            }
            for (auto&& r : v.view->view_info()->partition_slice(db).default_row_ranges()) {
                view_row_ranges.push_back(r.transform(std::mem_fn(&clustering_key_prefix::view)));
                co_await coroutine::maybe_yield();
            }
        }
    }
    if (mp.partition_tombstone()) {
        std::swap(row_ranges, view_row_ranges);
    } else {
        // FIXME: Optimize, as most often than not clustering keys will not be restricted.
        for (auto&& rt : mp.row_tombstones()) {
            interval<clustering_key_prefix_view> rtr(
                    bound_view::to_interval_bound<interval>(rt.start_bound()),
                    bound_view::to_interval_bound<interval>(rt.end_bound()));
            for (auto&& vr : view_row_ranges) {
                auto overlap = rtr.intersection(vr, cmp);
                if (overlap) {
                    row_ranges.push_back(std::move(overlap).value());
                }
                co_await coroutine::maybe_yield();
            }
        }
    }

    for (auto&& row : mp.clustered_rows()) {
        if (update_requires_read_before_write(db, base, views, key, row)) {
            row_ranges.emplace_back(row.key());
        }
        co_await coroutine::maybe_yield();
    }

    // Note that the views could have restrictions on regular columns,
    // but even if that's the case we shouldn't apply those when we read,
    // because even if an existing row doesn't match the view filter, the
    // update can change that in which case we'll need to know the existing
    // content, in case the view includes a column that is not included in
    // this mutation.

    //FIXME: Unfortunate copy.
    co_return boost::copy_range<query::clustering_row_ranges>(
            interval<clustering_key_prefix_view>::deoverlap(std::move(row_ranges), cmp)
            | boost::adaptors::transformed([] (auto&& v) {
                return std::move(v).transform([] (auto&& ckv) { return clustering_key_prefix(ckv); });
            }));

}

bool needs_static_row(const mutation_partition& mp, const std::vector<view_and_base>& views) {
    // TODO: We could also check whether any of the views need static rows
    // and return false if none of them do
    return mp.partition_tombstone() || !mp.static_row().empty();
}

bool should_generate_view_updates_on_this_shard(const schema_ptr& base, const locator::effective_replication_map_ptr& ermp, dht::token token) {
    // Based on the computation in get_view_natural_endpoint, this is used
    // to detect beforehand the case that we're a "normal" replica which is
    // paired with a view replica and sends view updates to.
    // For a pending replica, for example, this will return false.
    // Also, for the case of intra-node migration, we check that this shard is ready for reads.
    const auto my_host_id = ermp->get_token_metadata_ptr()->get_topology().my_host_id();
    const auto replicas = ermp->get_replicas(token);
    return std::find(replicas.begin(), replicas.end(), my_host_id) != replicas.end()
        && ermp->shard_for_reads(*base, token) == this_shard_id();
}

// Calculate the node ("natural endpoint") to which this node should send
// a view update.
//
// A materialized view table is in the same keyspace as its base table,
// and in particular both have the same replication factor. Therefore it
// is possible, for a particular base partition and related view partition
// to "pair" between the base replicas and view replicas holding those
// partitions. The first (in ring order) base replica is paired with the
// first view replica, the second with the second, and so on. The purpose
// of this function is to find, assuming that this node is one of the base
// replicas for a given partition, the paired view replica.
//
// In the past, we used an optimization called "self-pairing" that if a single
// node was both a base replica and a view replica for a write, the pairing is
// modified so that this node would send the update to itself. This self-
// pairing optimization could cause the pairing to change after view ranges
// are moved between nodes, so currently we only use it if
// use_legacy_self_pairing is set to true. When using tablets - where range
// movements are common - it is strongly recommended to set it to false.
//
// If the keyspace's replication strategy is a NetworkTopologyStrategy,
// we pair only nodes in the same datacenter.
// If one of the base replicas also happens to be a view replica, it is
// paired with itself (with the other nodes paired by order in the list
// after taking this node out).
//
// If the assumption that the given base token belongs to this replica
// does not hold, we return an empty optional.
static std::optional<gms::inet_address>
get_view_natural_endpoint(
        const locator::effective_replication_map_ptr& base_erm,
        const locator::effective_replication_map_ptr& view_erm,
        bool network_topology,
        const dht::token& base_token,
        const dht::token& view_token,
        bool use_legacy_self_pairing,
        replica::cf_stats& cf_stats) {
    auto& topology = base_erm->get_token_metadata_ptr()->get_topology();
    auto me = topology.my_host_id();
    auto my_datacenter = topology.get_datacenter();
    std::vector<locator::host_id> base_endpoints, view_endpoints;

    // We need to use get_replicas() for pairing to be stable in case base or view tablet
    // is rebuilding a replica which has left the ring. get_natural_endpoints() filters such replicas.
    for (auto&& base_endpoint : base_erm->get_replicas(base_token)) {
        if (!network_topology || topology.get_datacenter(base_endpoint) == my_datacenter) {
            base_endpoints.push_back(base_endpoint);
        }
    }

    auto& view_topology = view_erm->get_token_metadata_ptr()->get_topology();
    for (auto&& view_endpoint : view_erm->get_replicas(view_token)) {
        if (use_legacy_self_pairing) {
            auto it = std::find(base_endpoints.begin(), base_endpoints.end(),
                view_endpoint);
            // If this base replica is also one of the view replicas, we use
            // ourselves as the view replica.
            if (view_endpoint == me && it != base_endpoints.end()) {
                return topology.my_address();
            }
            // We have to remove any endpoint which is shared between the base
            // and the view, as it will select itself and throw off the counts
            // otherwise.
            if (it != base_endpoints.end()) {
                base_endpoints.erase(it);
            } else if (!network_topology || view_topology.get_datacenter(view_endpoint) == my_datacenter) {
                view_endpoints.push_back(view_endpoint);
            }
        } else {
            if (!network_topology || view_topology.get_datacenter(view_endpoint) == my_datacenter) {
                view_endpoints.push_back(view_endpoint);
            }
        }
    }

    SCYLLA_ASSERT(base_endpoints.size() == view_endpoints.size());
    auto base_it = std::find(base_endpoints.begin(), base_endpoints.end(), me);
    if (base_it == base_endpoints.end()) {
        // This node is not a base replica of this key, so we return empty
        // FIXME: This case shouldn't happen, and if it happens, a view update
        // would be lost.
        ++cf_stats.total_view_updates_on_wrong_node;
        return {};
    }
    auto replica = view_endpoints[base_it - base_endpoints.begin()];

    // https://github.com/scylladb/scylladb/issues/19439
    // With tablets, a node being replaced might transition to "left" state
    // but still be kept as a replica. In such case, the IP of the replaced
    // node will be lost and `endpoint()` will return an empty IP here.
    // As of writing this, storage proxy was not migrated to host IDs yet
    // (#6403) and hints are not prepared to handle nodes that are left
    // but are still replicas. Therefore, there is no other sensible option
    // right now but to give up attempt to send the update or write a hint
    // to the paired, permanently down replica.
    const auto ep = view_topology.get_node(replica).endpoint();
    if (ep != gms::inet_address{}) {
        return ep;
    } else {
        return std::nullopt;
    }
}

static future<> apply_to_remote_endpoints(service::storage_proxy& proxy, locator::effective_replication_map_ptr ermp,
        gms::inet_address target, inet_address_vector_topology_change pending_endpoints,
        frozen_mutation_and_schema mut, const dht::token& base_token, const dht::token& view_token,
        service::allow_hints allow_hints, tracing::trace_state_ptr tr_state) {
    // The "delay_before_remote_view_update" injection point can be
    // used to add a short delay (currently 0.5 seconds) before a base
    // replica sends its update to the remote view replica.
    co_await utils::get_local_injector().inject("delay_before_remote_view_update", 500ms);
    tracing::trace(tr_state, "Sending view update for {}.{} to {}, with pending endpoints = {}; base token = {}; view token = {}",
            mut.s->ks_name(), mut.s->cf_name(), target, pending_endpoints, base_token, view_token);
    co_await proxy.send_to_endpoint(
            std::move(mut),
            std::move(ermp),
            target,
            std::move(pending_endpoints),
            db::write_type::VIEW,
            std::move(tr_state),
            allow_hints,
            service::is_cancellable::yes);
    while (utils::get_local_injector().enter("never_finish_remote_view_updates")) {
        co_await seastar::sleep(100ms);
    }
}

static bool should_update_synchronously(const schema& s) {
    auto tag_opt = db::find_tag(s, db::SYNCHRONOUS_VIEW_UPDATES_TAG_KEY);
    if (!tag_opt.has_value()) {
        return false;
    }

    return *tag_opt == "true";
}

size_t memory_usage_of(const frozen_mutation_and_schema& mut) {
    // Overhead of sending a view mutation, in terms of data structures used by the storage_proxy, as well as possible background tasks
    // allocated for a remote view update.
    constexpr size_t base_overhead_bytes = 2288;
    return base_overhead_bytes + mut.fm.representation().size();
}

// Take the view mutations generated by generate_view_updates(), which pertain
// to a modification of a single base partition, and apply them to the
// appropriate paired replicas. This is done asynchronously - we do not wait
// for the writes to complete.
future<> view_update_generator::mutate_MV(
        schema_ptr base,
        dht::token base_token,
        utils::chunked_vector<frozen_mutation_and_schema> view_updates,
        db::view::stats& stats,
        replica::cf_stats& cf_stats,
        tracing::trace_state_ptr tr_state,
        db::timeout_semaphore_units pending_view_updates,
        service::allow_hints allow_hints,
        wait_for_all_updates wait_for_all)
{
    auto base_ermp = base->table().get_effective_replication_map();
    static constexpr size_t max_concurrent_updates = 128;
    co_await utils::get_local_injector().inject("delay_before_get_view_natural_endpoint", 8000ms);
    co_await max_concurrent_for_each(view_updates, max_concurrent_updates,
            [this, base_token, &stats, &cf_stats, tr_state, &pending_view_updates, allow_hints, wait_for_all, base_ermp] (frozen_mutation_and_schema mut) mutable -> future<> {
        auto view_token = dht::get_token(*mut.s, mut.fm.key());
        auto view_ermp = mut.s->table().get_effective_replication_map();
        auto& ks = _proxy.local().local_db().find_keyspace(mut.s->ks_name());
        bool network_topology = dynamic_cast<const locator::network_topology_strategy*>(&ks.get_replication_strategy());
        // We set legacy self-pairing for old vnode-based tables (for backward
        // compatibility), and unset it for tablets - where range movements
        // are more frequent and backward compatibility is less important.
        // TODO: Maybe allow users to set use_legacy_self_pairing explicitly
        // on a view, like we have the synchronous_updates_flag.
        bool use_legacy_self_pairing = !ks.uses_tablets();
        auto target_endpoint = get_view_natural_endpoint(base_ermp, view_ermp, network_topology, base_token, view_token, use_legacy_self_pairing, cf_stats);
        auto remote_endpoints = view_ermp->get_pending_endpoints(view_token);
        auto sem_units = seastar::make_lw_shared<db::timeout_semaphore_units>(pending_view_updates.split(memory_usage_of(mut)));

        const bool update_synchronously = should_update_synchronously(*mut.s);
        if (update_synchronously) {
            tracing::trace(tr_state, "Forcing {}.{} view update to be synchronous (synchronous_updates property was set)",
                mut.s->ks_name(), mut.s->cf_name()
            );
        }
        // If a view is marked with the synchronous_updates property, we should wait for all.
        const bool apply_update_synchronously = wait_for_all || update_synchronously;

        // First, find the local endpoint and ensure that if it exists,
        // it will be the target endpoint. That way, all endpoints in the
        // remote_endpoints list are guaranteed to be remote.
        auto my_address = view_ermp->get_topology().my_address();
        auto remote_it = std::find(remote_endpoints.begin(), remote_endpoints.end(), my_address);
        if (remote_it != remote_endpoints.end()) {
            if (!target_endpoint) {
                target_endpoint = *remote_it;
                remote_endpoints.erase(remote_it);
            } else {
                // Remove the duplicated entry
                if (*target_endpoint == *remote_it) {
                    remote_endpoints.erase(remote_it);
                } else {
                    std::swap(*target_endpoint, *remote_it);
                }
            }
        }
        // It's still possible that a target endpoint is duplicated in the remote endpoints list,
        // so let's get rid of the duplicate if it exists
        if (target_endpoint) {
            auto remote_it = std::find(remote_endpoints.begin(), remote_endpoints.end(), *target_endpoint);
            if (remote_it != remote_endpoints.end()) {
                remote_endpoints.erase(remote_it);
            }
        }

        future<> local_view_update = make_ready_future<>();
        if (target_endpoint && *target_endpoint == my_address) {
            ++stats.view_updates_pushed_local;
            ++cf_stats.total_view_updates_pushed_local;
            ++stats.writes;
            auto mut_ptr = remote_endpoints.empty() ? std::make_unique<frozen_mutation>(std::move(mut.fm)) : std::make_unique<frozen_mutation>(mut.fm);
            tracing::trace(tr_state, "Locally applying view update for {}.{}; base token = {}; view token = {}",
                    mut.s->ks_name(), mut.s->cf_name(), base_token, view_token);
            local_view_update = _proxy.local().mutate_mv_locally(mut.s, *mut_ptr, tr_state, db::commitlog::force_sync::no).then_wrapped(
                    [s = mut.s, &stats, &cf_stats, tr_state, base_token, view_token, my_address, mut_ptr = std::move(mut_ptr),
                            sem_units, this] (future<>&& f) mutable {
                --stats.writes;
                sem_units = nullptr;
                _proxy.local().update_view_update_backlog();
                if (f.failed()) {
                    ++stats.view_updates_failed_local;
                    ++cf_stats.total_view_updates_failed_local;
                    auto ep = f.get_exception();
                    tracing::trace(tr_state, "Failed to apply local view update for {}", my_address);
                    vlogger.error("Error applying view update to {} (view: {}.{}, base token: {}, view token: {}): {}",
                            my_address, s->ks_name(), s->cf_name(), base_token, view_token, ep);
                    return make_exception_future<>(std::move(ep));
                }
                tracing::trace(tr_state, "Successfully applied local view update for {}", my_address);
                return make_ready_future<>();
            });
            // We just applied a local update to the target endpoint, so it should now be removed
            // from the possible targets
            target_endpoint.reset();
        }

        // If target endpoint is not engaged, but there are remote endpoints,
        // one of the remote endpoints should become a primary target
        if (!target_endpoint && !remote_endpoints.empty()) {
            target_endpoint = std::move(remote_endpoints.back());
            remote_endpoints.pop_back();
        }

        // If target_endpoint is engaged by this point, then either the update
        // is not local, or the local update was already applied but we still
        // have pending endpoints to send to.
        if (target_endpoint) {
            size_t updates_pushed_remote = remote_endpoints.size() + 1;
            stats.view_updates_pushed_remote += updates_pushed_remote;
            cf_stats.total_view_updates_pushed_remote += updates_pushed_remote;
            schema_ptr s = mut.s;
            future<> remote_view_update = apply_to_remote_endpoints(_proxy.local(), std::move(view_ermp), *target_endpoint, std::move(remote_endpoints), std::move(mut), base_token, view_token, allow_hints, tr_state).then_wrapped(
                [s = std::move(s), &stats, &cf_stats, tr_state, base_token, view_token, target_endpoint, updates_pushed_remote,
                 sem_units, apply_update_synchronously, this] (future<>&& f) mutable {
                sem_units = nullptr;
                _proxy.local().update_view_update_backlog();
                if (f.failed()) {
                    stats.view_updates_failed_remote += updates_pushed_remote;
                    cf_stats.total_view_updates_failed_remote += updates_pushed_remote;
                    auto ep = f.get_exception();
                    tracing::trace(tr_state, "Failed to apply view update for {} and {} remote endpoints",
                        *target_endpoint, updates_pushed_remote);

                    // Printing an error on every failed view mutation would cause log spam, so a rate limit is needed.
                    static thread_local logger::rate_limit view_update_error_rate_limit(std::chrono::seconds(4));
                    vlogger.log(log_level::warn, view_update_error_rate_limit,
                        "Error applying view update to {} (view: {}.{}, base token: {}, view token: {}): {}",
                        *target_endpoint, s->ks_name(), s->cf_name(), base_token, view_token, ep);
                    return apply_update_synchronously ? make_exception_future<>(std::move(ep)) : make_ready_future<>();
                }
                tracing::trace(tr_state, "Successfully applied view update for {} and {} remote endpoints",
                    *target_endpoint, updates_pushed_remote);
                return make_ready_future<>();
            });
            if (apply_update_synchronously) {
                co_return co_await when_all_succeed(
                    std::move(local_view_update), std::move(remote_view_update)).discard_result();
            } else {
                // The update is sent to background in order to preserve availability,
                // its parallelism is limited by view_update_concurrency_semaphore
                (void)remote_view_update;
            }
        }
        co_return co_await std::move(local_view_update);
    });
}

view_builder::view_builder(replica::database& db, db::system_keyspace& sys_ks, db::system_distributed_keyspace& sys_dist_ks, service::migration_notifier& mn, view_update_generator& vug)
        : _db(db)
        , _sys_ks(sys_ks)
        , _sys_dist_ks(sys_dist_ks)
        , _mnotifier(mn)
        , _vug(vug)
        , _permit(_db.get_reader_concurrency_semaphore().make_tracking_only_permit(nullptr, "view_builder", db::no_timeout, {})) {
    setup_metrics();
}

void view_builder::setup_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group("view_builder", {
        sm::make_gauge("pending_bookkeeping_ops",
                sm::description("Number of tasks waiting to perform bookkeeping operations"),
                [this] { return _sem.waiters(); }),

        sm::make_counter("steps_performed",
                sm::description("Number of performed build steps."),
                _stats.steps_performed),

        sm::make_counter("steps_failed",
                sm::description("Number of failed build steps."),
                _stats.steps_failed),

        sm::make_gauge("builds_in_progress",
                sm::description("Number of currently active view builds."),
                [this] { return _base_to_build_step.size(); })
    });
}

future<> view_builder::start_in_background(service::migration_manager& mm, utils::cross_shard_barrier barrier) {
    try {
        view_builder_init_state vbi;
        auto fail = defer([&barrier] mutable { barrier.abort(); });
        // Guard the whole startup routine with a semaphore,
        // so that it's not intercepted by `on_drop_view`, `on_create_view`
        // or `on_update_view` events.
        auto units = co_await get_units(_sem, 1);
        // Wait for schema agreement even if we're a seed node.
        co_await mm.wait_for_schema_agreement(_db, db::timeout_clock::time_point::max(), &_as);

        auto built = co_await _sys_ks.load_built_views();
        auto in_progress = co_await _sys_ks.load_view_build_progress();
        setup_shard_build_step(vbi, std::move(built), std::move(in_progress));
        // All shards need to arrive at the same decisions on whether or not to
        // restart a view build at some common token (reshard), and which token
        // to restart at. So we need to wait until all shards have read the view
        // build statuses before they can all proceed to make the (same) decision.
        // If we don't synchronize here, a fast shard may make a decision, start
        // building and finish a build step - before the slowest shard even read
        // the view build information.
        fail.cancel();
        co_await barrier.arrive_and_wait();
        units.return_all();

        co_await calculate_shard_build_step(vbi);
        _mnotifier.register_listener(this);
        _current_step = _base_to_build_step.begin();
        // Waited on indirectly in stop().
        (void)_build_step.trigger();
    } catch (...) {
        auto ex = std::current_exception();
        auto ll = log_level::error;
        try {
            std::rethrow_exception(ex);
        } catch (const seastar::sleep_aborted& e) {
            ll = log_level::debug;
        } catch (const seastar::abort_requested_exception& e) {
            ll = log_level::debug;
        } catch (const utils::barrier_aborted_exception& e) {
            ll = log_level::debug;
        }
        vlogger.log(ll, "start aborted: {}", ex);
    }
}

future<> view_builder::start(service::migration_manager& mm, utils::cross_shard_barrier barrier) {
    _started = start_in_background(mm, std::move(barrier));
    return make_ready_future<>();
}

future<> view_builder::drain() {
    if (_as.abort_requested()) {
        co_return;
    }
    vlogger.info("Draining view builder");
    _as.request_abort();
    co_await std::move(_started);
    co_await _mnotifier.unregister_listener(this);
    co_await _vug.drain();
    co_await _sem.wait();
    _sem.broken();
    co_await _build_step.join();
    co_await coroutine::parallel_for_each(_base_to_build_step, [] (std::pair<const table_id, build_step>& p) {
        return p.second.reader.close();
    });
}

future<> view_builder::stop() {
    vlogger.info("Stopping view builder");
    return drain();
}

view_builder::build_step& view_builder::get_or_create_build_step(table_id base_id) {
    auto it = _base_to_build_step.find(base_id);
    if (it == _base_to_build_step.end()) {
        auto base = _db.find_column_family(base_id).shared_from_this();
        auto p = _base_to_build_step.emplace(base_id, build_step{base, make_partition_slice(*base->schema())});
        // Iterators could have been invalidated if there was rehashing, so just reset the cursor.
        _current_step = p.first;
        it = p.first;
    }
    return it->second;
}

future<> view_builder::initialize_reader_at_current_token(build_step& step) {
  return step.reader.close().then([this, &step] {
    step.pslice = make_partition_slice(*step.base->schema());
    step.prange = dht::partition_range(dht::ring_position::starting_at(step.current_token()), dht::ring_position::max());
    step.reader = step.base->get_sstable_set().make_local_shard_sstable_reader(
            step.base->schema(),
            _permit,
            step.prange,
            step.pslice,
            nullptr,
            streamed_mutation::forwarding::no,
            mutation_reader::forwarding::no);
  });
}

void view_builder::load_view_status(view_builder::view_build_status status, std::unordered_set<table_id>& loaded_views) {
    if (!status.next_token) {
        // No progress was made on this view, so we'll treat it as new.
        return;
    }
    vlogger.info0("Resuming to build view {}.{} at {}", status.view->ks_name(), status.view->cf_name(), *status.next_token);
    loaded_views.insert(status.view->id());
    if (status.first_token == *status.next_token) {
        // Completed, so nothing to do for this shard. Consider the view
        // as loaded and not as a new view.
        _built_views.emplace(status.view->id());
        return;
    }
    get_or_create_build_step(status.view->view_info()->base_id()).build_status.emplace_back(std::move(status));
}

void view_builder::reshard(
        std::vector<std::vector<view_builder::view_build_status>> view_build_status_per_shard,
        std::unordered_set<table_id>& loaded_views) {
    // We must reshard. We aim for a simple algorithm, a step above not starting from scratch.
    // Shards build entries at different paces, so both first and last tokens will differ. We
    // want to be conservative when selecting the range that has been built. To do that, we
    // select the intersection of all the previous shard's ranges for each view.
    struct view_ptr_hash {
        std::size_t operator()(const view_ptr& v) const noexcept {
            return std::hash<table_id>()(v->id());
        }
    };
    struct view_ptr_equals {
        bool operator()(const view_ptr& v1, const view_ptr& v2) const noexcept {
            return v1->id() == v2->id();
        }
    };
    std::unordered_map<view_ptr, std::optional<interval<dht::token>>, view_ptr_hash, view_ptr_equals> my_status;
    for (auto& shard_status : view_build_status_per_shard) {
        for (auto& [view, first_token, next_token] : shard_status ) {
            // We start from an open-ended range, which we'll try to restrict.
            auto& my_range = my_status.emplace(
                    std::move(view),
                    interval<dht::token>::make_open_ended_both_sides()).first->second;
            if (!next_token || !my_range) {
                // A previous shard made no progress, so for this view we'll start over.
                my_range = std::nullopt;
                continue;
            }
            if (first_token == *next_token) {
                // Completed, so don't consider this shard's progress. We know that if the view
                // is marked as in-progress, then at least one shard will have a non-full range.
                continue;
            }
            wrapping_interval<dht::token> other_range(first_token, *next_token);
            if (other_range.is_wrap_around(dht::token_comparator())) {
                // The intersection of a wrapping range with a non-wrapping range may yield more
                // multiple non-contiguous ranges. To avoid the complexity of dealing with more
                // than one range, we'll just take one of the intersections.
                auto [bottom_range, top_range] = other_range.unwrap();
                if (auto bottom_int = my_range->intersection(interval(std::move(bottom_range)), dht::token_comparator())) {
                    my_range = std::move(bottom_int);
                } else {
                    my_range = my_range->intersection(interval(std::move(top_range)), dht::token_comparator());
                }
            } else {
                my_range = my_range->intersection(interval(std::move(other_range)), dht::token_comparator());
            }
        }
    }
    view_builder::base_to_build_step_type build_step;
    for (auto& [view, opt_range] : my_status) {
        if (!opt_range) {
            continue; // Treat it as a new table.
        }
        auto start_bound = opt_range->start() ? std::move(opt_range->start()->value()) : dht::minimum_token();
        auto end_bound = opt_range->end() ? std::move(opt_range->end()->value()) : dht::minimum_token();
        auto s = view_build_status{std::move(view), std::move(start_bound), std::move(end_bound)};
        load_view_status(std::move(s), loaded_views);
    }
}

void view_builder::setup_shard_build_step(
        view_builder_init_state& vbi,
        std::vector<system_keyspace_view_name> built,
        std::vector<system_keyspace_view_build_progress> in_progress) {
    // Shard 0 makes cleanup changes to the system tables, but none that could conflict
    // with the other shards; everyone is thus able to proceed independently.
    auto maybe_fetch_view = [&, this] (system_keyspace_view_name& name) {
        try {
            auto s = _db.find_schema(name.first, name.second);
            if (s->is_view()) {
                auto view = view_ptr(std::move(s));
                // This is a safety check in case this node missed a create MV statement
                // but got a drop table for the base, and another node didn't get the
                // drop notification and sent us the view schema.
                if (_db.column_family_exists(view->view_info()->base_id())) {
                    return view;
                }
            }
            // The view was dropped and a table was re-created with the same name,
            // but the write to the view-related system tables didn't make it.
        } catch (const replica::no_such_column_family&) {
            // Fall-through
        }
        if (this_shard_id() == 0) {
            vbi.bookkeeping_ops.push_back(_sys_dist_ks.remove_view(name.first, name.second));
            vbi.bookkeeping_ops.push_back(_sys_ks.remove_built_view(name.first, name.second));
            vbi.bookkeeping_ops.push_back(
                    _sys_ks.remove_view_build_progress_across_all_shards(
                            std::move(name.first),
                            std::move(name.second)));
        }
        return view_ptr(nullptr);
    };

    vbi.built_views = boost::copy_range<std::unordered_set<table_id>>(built
            | boost::adaptors::transformed(maybe_fetch_view)
            | boost::adaptors::filtered([] (const view_ptr& v) { return bool(v); })
            | boost::adaptors::transformed([] (const view_ptr& v) { return v->id(); }));

    for (auto& [view_name, first_token, next_token_opt, cpu_id] : in_progress) {
        if (auto view = maybe_fetch_view(view_name)) {
            if (vbi.built_views.contains(view->id())) {
                if (this_shard_id() == 0) {
                    auto f = _sys_dist_ks.finish_view_build(std::move(view_name.first), std::move(view_name.second)).then([this, view = std::move(view)] {
                        return _sys_ks.remove_view_build_progress_across_all_shards(view->cf_name(), view->ks_name());
                    });
                    vbi.bookkeeping_ops.push_back(std::move(f));
                }
                continue;
            }
            vbi.status_per_shard.resize(std::max(vbi.status_per_shard.size(), size_t(cpu_id + 1)));
            vbi.status_per_shard[cpu_id].emplace_back(view_build_status{
                    std::move(view),
                    std::move(first_token),
                    std::move(next_token_opt)});
        }
    }
}

future<> view_builder::calculate_shard_build_step(view_builder_init_state& vbi) {
    std::unordered_set<table_id> loaded_views;
    if (vbi.status_per_shard.size() != smp::count) {
        reshard(std::move(vbi.status_per_shard), loaded_views);
    } else if (!vbi.status_per_shard.empty()) {
        for (auto& status : vbi.status_per_shard[this_shard_id()]) {
            load_view_status(std::move(status), loaded_views);
        }
    }

    for (auto& [_, build_step] : _base_to_build_step) {
        boost::sort(build_step.build_status, [] (view_build_status s1, view_build_status s2) {
            return *s1.next_token < *s2.next_token;
        });
        if (!build_step.build_status.empty()) {
            build_step.current_key = dht::decorated_key{*build_step.build_status.front().next_token, partition_key::make_empty()};
        }
    }

    auto all_views = _db.get_views();
    auto is_new = [&] (const view_ptr& v) {
        // This is a safety check in case this node missed a create MV statement
        // but got a drop table for the base, and another node didn't get the
        // drop notification and sent us the view schema.
        return _db.column_family_exists(v->view_info()->base_id()) && !loaded_views.contains(v->id())
                && !vbi.built_views.contains(v->id());
    };
    for (auto&& view : all_views | boost::adaptors::filtered(is_new)) {
        vbi.bookkeeping_ops.push_back(add_new_view(view, get_or_create_build_step(view->view_info()->base_id())));
    }

    return parallel_for_each(_base_to_build_step, [this] (auto& p) {
        return initialize_reader_at_current_token(p.second);
    }).then([&vbi] {
        return seastar::when_all_succeed(vbi.bookkeeping_ops.begin(), vbi.bookkeeping_ops.end()).handle_exception([] (std::exception_ptr ep) {
            vlogger.warn("Failed to update materialized view bookkeeping while synchronizing view builds on all shards ({}), continuing anyway.", ep);
        });
    });
}

future<std::unordered_map<sstring, sstring>>
view_builder::view_build_statuses(sstring keyspace, sstring view_name) const {
    std::unordered_map<locator::host_id, sstring> status = co_await _sys_dist_ks.view_status(std::move(keyspace), std::move(view_name));
    std::unordered_map<sstring, sstring> status_map;
    const auto& topo = _db.get_token_metadata().get_topology();
    topo.for_each_node([&] (const locator::node *node) {
        auto it = status.find(node->host_id());
        auto s = it != status.end() ? std::move(it->second) : "UNKNOWN";
        status_map.emplace(fmt::to_string(node->endpoint()), std::move(s));
    });
    co_return status_map;
}

future<> view_builder::add_new_view(view_ptr view, build_step& step) {
    vlogger.info0("Building view {}.{}, starting at token {}", view->ks_name(), view->cf_name(), step.current_token());
    step.build_status.emplace(step.build_status.begin(), view_build_status{view, step.current_token(), std::nullopt});
    auto f = this_shard_id() == 0 ? _sys_dist_ks.start_view_build(view->ks_name(), view->cf_name()) : make_ready_future<>();
    return when_all_succeed(
            std::move(f),
            _sys_ks.register_view_for_building(view->ks_name(), view->cf_name(), step.current_token())).discard_result();
}

static future<> flush_base(lw_shared_ptr<replica::column_family> base, abort_source& as) {
    struct empty_state { };
    return exponential_backoff_retry::do_until_value(1s, 1min, as, [base = std::move(base)] {
        return base->flush().then_wrapped([base] (future<> f) -> std::optional<empty_state> {
            if (f.failed()) {
                vlogger.error("Error flushing base table {}.{}: {}; retrying", base->schema()->ks_name(), base->schema()->cf_name(), f.get_exception());
                return { };
            }
            return { empty_state{} };
        });
    }).discard_result();
}

void view_builder::on_create_view(const sstring& ks_name, const sstring& view_name) {
    // Do it in the background, serialized.
    (void)with_semaphore(_sem, 1, [ks_name, view_name, this] {
        auto view = view_ptr(_db.find_schema(ks_name, view_name));
        auto& step = get_or_create_build_step(view->view_info()->base_id());
        return when_all(step.base->await_pending_writes(), step.base->await_pending_streams()).discard_result().then([this, &step] {
            return flush_base(step.base, _as);
        }).then([this, view, &step] () mutable {
            // This resets the build step to the current token. It may result in views currently
            // being built to receive duplicate updates, but it simplifies things as we don't have
            // to keep around a list of new views to build the next time the reader crosses a token
            // threshold.
          return initialize_reader_at_current_token(step).then([this, view, &step] () mutable {
            return add_new_view(view, step).then_wrapped([this, view] (future<>&& f) {
                if (f.failed()) {
                    vlogger.error("Error setting up view for building {}.{}: {}", view->ks_name(), view->cf_name(), f.get_exception());
                }
                // Waited on indirectly in stop().
                (void)_build_step.trigger();
            });
          });
        });
    }).handle_exception_type([] (replica::no_such_column_family&) { });
}

void view_builder::on_update_view(const sstring& ks_name, const sstring& view_name, bool) {
    // Do it in the background, serialized.
    (void)with_semaphore(_sem, 1, [ks_name, view_name, this] {
        auto view = view_ptr(_db.find_schema(ks_name, view_name));
        auto step_it = _base_to_build_step.find(view->view_info()->base_id());
        if (step_it == _base_to_build_step.end()) {
            return;// In case all the views for this CF have finished building already.
        }
        auto status_it = boost::find_if(step_it->second.build_status, [view] (const view_build_status& bs) {
            return bs.view->id() == view->id();
        });
        if (status_it != step_it->second.build_status.end()) {
            status_it->view = std::move(view);
        }
    }).handle_exception_type([] (replica::no_such_column_family&) { });
}

void view_builder::on_drop_view(const sstring& ks_name, const sstring& view_name) {
    vlogger.info0("Stopping to build view {}.{}", ks_name, view_name);
    // Do it in the background, serialized.
    (void)with_semaphore(_sem, 1, [ks_name, view_name, this] {
        // The view is absent from the database at this point, so find it by brute force.
        ([&, this] {
            for (auto& [_, step] : _base_to_build_step) {
                if (step.build_status.empty() || step.build_status.front().view->ks_name() != ks_name) {
                    continue;
                }
                for (auto it = step.build_status.begin(); it != step.build_status.end(); ++it) {
                    if (it->view->cf_name() == view_name) {
                        _built_views.erase(it->view->id());
                        step.build_status.erase(it);
                        return;
                    }
                }
            }
        })();
        if (this_shard_id() != 0) {
            // Shard 0 can't remove the entry in the build progress system table on behalf of the
            // current shard, since shard 0 may have already processed the notification, and this
            // shard may since have updated the system table if the drop happened concurrently
            // with the build.
            return _sys_ks.remove_view_build_progress(ks_name, view_name);
        }
        return when_all_succeed(
                    _sys_ks.remove_view_build_progress(ks_name, view_name),
                    _sys_ks.remove_built_view(ks_name, view_name),
                    _sys_dist_ks.remove_view(ks_name, view_name))
                        .discard_result()
                        .handle_exception([ks_name, view_name] (std::exception_ptr ep) {
            vlogger.warn("Failed to cleanup view {}.{}: {}", ks_name, view_name, ep);
        });
    });
}

future<> view_builder::do_build_step() {
    return seastar::async([this] {
        exponential_backoff_retry r(1s, 1min);
        while (!_base_to_build_step.empty() && !_as.abort_requested()) {
            auto units = get_units(_sem, 1).get();
            ++_stats.steps_performed;
            try {
                execute(_current_step->second, exponential_backoff_retry(1s, 1min));
                r.reset();
            } catch (const abort_requested_exception&) {
                return;
            } catch (...) {
                ++_current_step->second.base->cf_stats()->view_building_paused;
                ++_stats.steps_failed;
                auto base = _current_step->second.base->schema();
                vlogger.warn("Error executing build step for base {}.{}: {}", base->ks_name(), base->cf_name(), std::current_exception());
                r.retry(_as).get();
                initialize_reader_at_current_token(_current_step->second).get();
            }
            if (_current_step->second.build_status.empty()) {
                auto base = _current_step->second.base->schema();
                auto reader = std::move(_current_step->second.reader);
                _current_step = _base_to_build_step.erase(_current_step);
                reader.close().get();
            } else {
                ++_current_step;
            }
            if (_current_step == _base_to_build_step.end()) {
                _current_step = _base_to_build_step.begin();
            }
        }
    }).handle_exception([] (std::exception_ptr ex) {
        vlogger.warn("Unexcepted error executing build step: {}. Ignored.", ex);
    });
}

// Called in the context of a seastar::thread.
class view_builder::consumer {
public:
    struct built_views {
        build_step& step;
        std::vector<view_build_status> views;

        built_views(build_step& step)
                : step(step) {
        }

        built_views(built_views&& other)
                : step(other.step)
                , views(std::move(other.views)) {
        }

        ~built_views() {
            for (auto&& status : views) {
                // Use step.current_token(), which may have wrapped around and become < first_token.
                step.build_status.emplace_back(view_build_status{std::move(status.view), step.current_token(), step.current_token()});
            }
        }

        void release() {
            views.clear();
        }
    };

private:
    view_builder& _builder;
    shared_ptr<view_update_generator> _gen;
    build_step& _step;
    built_views _built_views;
    gc_clock::time_point _now;
    std::vector<view_ptr> _views_to_build;
    std::deque<mutation_fragment_v2> _fragments;
    // The compact_for_query<> that feeds this consumer is already configured
    // to feed us up to view_builder::batchsize (128) rows and not an entire
    // partition. Still, if rows contain large blobs, saving 128 of them in
    // _fragments may be too much. So we want to track _fragment's memory
    // usage, and flush the _fragments if it has grown too large.
    // Additionally, limiting _fragment's size also solves issue #4213:
    // A single view mutation can be as large as the size of the base rows
    // used to build it, and we cannot allow its serialized size to grow
    // beyond our limit on mutation size (by default 32 MB).
    size_t _fragments_memory_usage = 0;
public:
    consumer(view_builder& builder, shared_ptr<view_update_generator> gen, build_step& step, gc_clock::time_point now)
            : _builder(builder)
            , _gen(std::move(gen))
            , _step(step)
            , _built_views{step}
            , _now(now) {
        if (!step.current_key.key().is_empty(*_step.reader.schema())) {
            load_views_to_build();
        }
    }

    void load_views_to_build() {
        inject_failure("view_builder_load_views");
        for (auto&& vs : _step.build_status) {
            if (_step.current_token() >= vs.next_token) {
                if (partition_key_matches(_builder.get_db().as_data_dictionary(), *_step.reader.schema(), *vs.view->view_info(), _step.current_key)) {
                    _views_to_build.push_back(vs.view);
                }
                if (vs.next_token || _step.current_token() != vs.first_token) {
                    vs.next_token = _step.current_key.token();
                }
            } else {
                break;
            }
        }
    }

    void check_for_built_views() {
        inject_failure("view_builder_check_for_built_views");
        for (auto it = _step.build_status.begin(); it != _step.build_status.end();) {
            // A view starts being built at token t1. Due to resharding, that may not necessarily be a
            // shard-owned token. We finish building the view when the next_token to build is just before
            // (or at) the first token, but the shard-owned current token is after (or at) the first token.
            // In the system tables, we set first_token = next_token to signal the completion of the build
            // process in case of a restart.
            if (it->next_token && *it->next_token <= it->first_token && _step.current_token() >= it->first_token) {
                _built_views.views.push_back(std::move(*it));
                it = _step.build_status.erase(it);
            } else {
                ++it;
            }
        }
    }

    stop_iteration consume_new_partition(const dht::decorated_key& dk) {
        inject_failure("view_builder_consume_new_partition");
        if (dk.key().is_empty()) {
            on_internal_error(vlogger, format("Trying to consume empty partition key {}", dk));
        }
        _step.current_key = std::move(dk);
        check_for_built_views();
        _views_to_build.clear();
        load_views_to_build();
        return stop_iteration(_views_to_build.empty());
    }

    stop_iteration consume(tombstone) {
        inject_failure("view_builder_consume_tombstone");
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr, tombstone, bool) {
        inject_failure("view_builder_consume_static_row");
        if (_views_to_build.empty() || _builder._as.abort_requested()) {
            return stop_iteration::yes;
        }

        add_fragment(std::move(sr));
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr, row_tombstone, bool is_live) {
        inject_failure("view_builder_consume_clustering_row");
        if (!is_live) {
            return stop_iteration::no;
        }
        if (_views_to_build.empty() || _builder._as.abort_requested()) {
            return stop_iteration::yes;
        }

        add_fragment(std::move(cr));
        return stop_iteration::no;
    }

    void add_fragment(auto&& fragment) {
        _fragments_memory_usage += fragment.memory_usage(*_step.reader.schema());
        _fragments.emplace_back(*_step.reader.schema(), _builder._permit, std::move(fragment));
        if (_fragments_memory_usage > batch_memory_max) {
            // Although we have not yet completed the batch of base rows that
            // compact_for_query<> planned for us (view_builder::batchsize),
            // we've still collected enough rows to reach sizeable memory use,
            // so let's flush these rows now.
            flush_fragments();
        }
    }

    stop_iteration consume(range_tombstone_change&&) {
        inject_failure("view_builder_consume_range_tombstone");
        return stop_iteration::no;
    }

    void flush_fragments() {
        inject_failure("view_builder_flush_fragments");
        _builder._as.check();
        if (!_fragments.empty()) {
            _fragments.emplace_front(*_step.reader.schema(), _builder._permit, partition_start(_step.current_key, tombstone()));
            auto base_schema = _step.base->schema();
            auto views = with_base_info_snapshot(_views_to_build);
            auto reader = make_mutation_reader_from_fragments(_step.reader.schema(), _builder._permit, std::move(_fragments));
            auto close_reader = defer([&reader] { reader.close().get(); });
            reader.upgrade_schema(base_schema);
            _gen->populate_views(
                    *_step.base,
                    std::move(views),
                    _step.current_token(),
                    std::move(reader),
                    _now).get();
            close_reader.cancel();
            _fragments.clear();
            _fragments_memory_usage = 0;
        }
    }

    stop_iteration consume_end_of_partition() {
        inject_failure("view_builder_consume_end_of_partition");
        flush_fragments();
        return stop_iteration(_step.build_status.empty());
    }

    // Must be called in a seastar thread.
    built_views consume_end_of_stream() {
        inject_failure("view_builder_consume_end_of_stream");
        if (vlogger.is_enabled(log_level::debug)) {
            auto view_names = boost::copy_range<std::vector<sstring>>(
                    _views_to_build | boost::adaptors::transformed([](auto v) {
                        return v->cf_name();
                    }));
            vlogger.debug("Completed build step for base {}.{}, at token {}; views={}", _step.base->schema()->ks_name(),
                          _step.base->schema()->cf_name(), _step.current_token(), view_names);
        }
        if (_step.reader.is_end_of_stream() && _step.reader.is_buffer_empty()) {
            if (_step.current_key.key().is_empty()) {
                // consumer got end-of-stream without consuming a single partition
                vlogger.debug("Reader didn't produce anything, marking views as built");
                while (!_step.build_status.empty()) {
                    _built_views.views.push_back(std::move(_step.build_status.back()));
                    _step.build_status.pop_back();
                }
            }
            _step.current_key = {dht::minimum_token(), partition_key::make_empty()};
            for (auto&& vs : _step.build_status) {
                vs.next_token = dht::minimum_token();
            }
            _builder.initialize_reader_at_current_token(_step).get();
            check_for_built_views();
        }
        return std::move(_built_views);
    }
};

// Called in the context of a seastar::thread.
void view_builder::execute(build_step& step, exponential_backoff_retry r) {
    gc_clock::time_point now = gc_clock::now();
    auto compaction_state = make_lw_shared<compact_for_query_state_v2>(
            *step.reader.schema(),
            now,
            step.pslice,
            batch_size,
            query::max_partitions);
    auto consumer = compact_for_query_v2<view_builder::consumer>(compaction_state, view_builder::consumer{*this, _vug.shared_from_this(), step, now});
    auto built = step.reader.consume_in_thread(std::move(consumer));
    if (auto ds = std::move(*compaction_state).detach_state()) {
        if (ds->current_tombstone) {
            step.reader.unpop_mutation_fragment(mutation_fragment_v2(*step.reader.schema(), step.reader.permit(), std::move(*ds->current_tombstone)));
        }
        step.reader.unpop_mutation_fragment(mutation_fragment_v2(*step.reader.schema(), step.reader.permit(), std::move(ds->partition_start)));
    }

    _as.check();

    std::vector<future<>> bookkeeping_ops;
    bookkeeping_ops.reserve(built.views.size() + step.build_status.size());
    for (auto& [view, first_token, _] : built.views) {
        bookkeeping_ops.push_back(maybe_mark_view_as_built(view, first_token));
    }
    built.release();
    for (auto& [view, _, next_token] : step.build_status) {
        if (next_token) {
            bookkeeping_ops.push_back(
                    _sys_ks.update_view_build_progress(view->ks_name(), view->cf_name(), *next_token));
        }
    }
    seastar::when_all_succeed(bookkeeping_ops.begin(), bookkeeping_ops.end()).handle_exception([] (std::exception_ptr ep) {
        vlogger.warn("Failed to update materialized view bookkeeping ({}), continuing anyway.", ep);
    }).get();
}

future<> view_builder::mark_as_built(view_ptr view) {
    return seastar::when_all_succeed(
            _sys_ks.mark_view_as_built(view->ks_name(), view->cf_name()),
            _sys_dist_ks.finish_view_build(view->ks_name(), view->cf_name())).discard_result();
}

future<> view_builder::mark_existing_views_as_built() {
    SCYLLA_ASSERT(this_shard_id() == 0);
    auto views = _db.get_views();
    co_await coroutine::parallel_for_each(views, [this] (view_ptr& view) {
        return mark_as_built(view);
    });
}

future<> view_builder::maybe_mark_view_as_built(view_ptr view, dht::token next_token) {
    _built_views.emplace(view->id());
    vlogger.debug("Shard finished building view {}.{}", view->ks_name(), view->cf_name());
    return container().map_reduce0(
            [view_id = view->id()] (view_builder& builder) {
                return builder._built_views.contains(view_id);
            },
            true,
            [] (bool result, bool shard_complete) {
                return result && shard_complete;
            }).then([this, view, next_token = std::move(next_token)] (bool built) {
        if (built) {
            inject_failure("view_builder_mark_view_as_built");
            return container().invoke_on_all([view_id = view->id()] (view_builder& builder) {
                if (builder._built_views.erase(view_id) == 0 || this_shard_id() != 0) {
                    return make_ready_future<>();
                }
                auto view = builder._db.find_schema(view_id);
                vlogger.info("Finished building view {}.{}", view->ks_name(), view->cf_name());
                return builder.mark_as_built(view_ptr(view)).then([&builder, view] {
                    // The view is built, so shard 0 can remove the entry in the build progress system table on
                    // behalf of all shards. It is guaranteed to have a higher timestamp than the per-shard entries.
                    return builder._sys_ks.remove_view_build_progress_across_all_shards(view->ks_name(), view->cf_name());
                }).then([&builder, view] {
                    auto it = builder._build_notifiers.find(std::pair(view->ks_name(), view->cf_name()));
                    if (it != builder._build_notifiers.end()) {
                        it->second.set_value();
                    }
                });
            });
        }
        return _sys_ks.update_view_build_progress(view->ks_name(), view->cf_name(), next_token);
    });
}

future<> view_builder::wait_until_built(const sstring& ks_name, const sstring& view_name) {
    return container().invoke_on(0, [ks_name, view_name] (view_builder& builder) {
        auto v = std::pair(std::move(ks_name), std::move(view_name));
        return builder._build_notifiers[std::move(v)].get_shared_future();
    });
}

void node_update_backlog::add(update_backlog backlog) {
    _backlogs[this_shard_id()].backlog.store(backlog, std::memory_order_relaxed);
    _backlogs[this_shard_id()].need_publishing = need_publishing::yes;
}

update_backlog node_update_backlog::fetch() {
    auto now = clock::now();
    if (now >= _last_update.load(std::memory_order_relaxed) + _interval) {
        _last_update.store(now, std::memory_order_relaxed);
        auto new_max = boost::accumulate(
                _backlogs,
                update_backlog::no_backlog(),
                [] (const update_backlog& lhs, const per_shard_backlog& rhs) {
                    return std::max(lhs, rhs.load());
                });
        _max.store(new_max, std::memory_order_relaxed);
        return new_max;
    }
    // If we perform a shard-aware write, we can read the backlog of the current shard,
    // which was just updated.
    // We still need to compare it to the max, aggregated from all shards, which might
    // still be higher despite being most likely slightly outdated.
    return std::max(fetch_shard(this_shard_id()), _max.load(std::memory_order_relaxed));
}

future<std::optional<update_backlog>> node_update_backlog::fetch_if_changed() {
    _last_update.store(clock::now(), std::memory_order_relaxed);
    auto [np, max] = co_await map_reduce(boost::irange(0u, smp::count),
            [this] (shard_id shard) {
                return smp::submit_to(shard, [this, shard] {
                    // Even if the shard's backlog didn't change, we still need to take it into account when calculating the new max.
                    return std::make_pair(std::exchange(_backlogs[shard].need_publishing, need_publishing::no), fetch_shard(shard));
                });
            },
            std::make_pair(need_publishing::no, db::view::update_backlog::no_backlog()),
            [] (std::pair<need_publishing, db::view::update_backlog> a, std::pair<need_publishing, db::view::update_backlog> b) {
                return std::make_pair(a.first || b.first, std::max(a.second, b.second));
            });
    _max.store(max, std::memory_order_relaxed);
    co_return np ? std::make_optional(max) : std::nullopt;
}

update_backlog node_update_backlog::fetch_shard(unsigned shard) {
    return _backlogs[shard].backlog.load(std::memory_order_relaxed);
}

future<bool> view_builder::check_view_build_ongoing(const locator::token_metadata& tm, const sstring& ks_name, const sstring& cf_name) {
    using view_statuses_type = std::unordered_map<locator::host_id, sstring>;
    return _sys_dist_ks.view_status(ks_name, cf_name).then([&tm] (view_statuses_type&& view_statuses) {
        return boost::algorithm::any_of(view_statuses, [&tm] (const view_statuses_type::value_type& view_status) {
            // Only consider status of known hosts.
            return view_status.second == "STARTED" && tm.get_endpoint_for_host_id_if_known(view_status.first);
        });
    });
}

future<> view_builder::register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<replica::table> table) {
    return _vug.register_staging_sstable(std::move(sst), std::move(table));
}

future<bool> check_needs_view_update_path(view_builder& vb, const locator::token_metadata& tm, const replica::table& t, streaming::stream_reason reason) {
    if (is_internal_keyspace(t.schema()->ks_name())) {
        return make_ready_future<bool>(false);
    }
    if (reason == streaming::stream_reason::repair && !t.views().empty()) {
        return make_ready_future<bool>(true);
    }
    return do_with(t.views(), [&vb, &tm] (auto& views) {
        return map_reduce(views,
                [&vb, &tm] (const view_ptr& view) { return vb.check_view_build_ongoing(tm, view->ks_name(), view->cf_name()); },
                false,
                std::logical_or<bool>());
    });
}

void view_updating_consumer::do_flush_buffer() {
    _staging_reader_handle.pause();

    if (_buffer.front().partition().empty()) {
        // If we flushed mid-partition we can have an empty mutation if we
        // flushed right before getting the end-of-partition fragment.
        _buffer.pop_front();
    }

    while (!_buffer.empty()) {
        try {
            auto lock_holder = _view_update_pusher(std::move(_buffer.front())).get();
        } catch (...) {
            vlogger.warn("Failed to push replica updates for table {}.{}: {}", _schema->ks_name(), _schema->cf_name(), std::current_exception());
        }
        _buffer.pop_front();
    }

    _buffer_size = 0;
}

void view_updating_consumer::flush_builder() {
    _buffer.emplace_back(_mut_builder->flush());
}

void view_updating_consumer::end_builder() {
    _mut_builder->consume_end_of_partition();
    if (auto mut_opt = _mut_builder->consume_end_of_stream()) {
        _buffer.emplace_back(std::move(*mut_opt));
    }
    _mut_builder.reset();
}

void view_updating_consumer::maybe_flush_buffer_mid_partition() {
    if (_buffer_size >= _buffer_size_hard_limit) {
        flush_builder();
        do_flush_buffer();
    }
}

view_updating_consumer::view_updating_consumer(view_update_generator& gen, schema_ptr schema, reader_permit permit, replica::table& table, std::vector<sstables::shared_sstable> excluded_sstables, const seastar::abort_source& as,
        evictable_reader_handle_v2& staging_reader_handle)
    : view_updating_consumer(std::move(schema), std::move(permit), as, staging_reader_handle,
            [table = table.shared_from_this(), excluded_sstables = std::move(excluded_sstables), gen = gen.shared_from_this()] (mutation m) mutable {
        auto s = m.schema();
        return table->stream_view_replica_updates(gen, std::move(s), std::move(m), db::no_timeout, excluded_sstables);
    })
{ }

std::vector<db::view::view_and_base> with_base_info_snapshot(std::vector<view_ptr> vs) {
    return boost::copy_range<std::vector<db::view::view_and_base>>(vs | boost::adaptors::transformed([] (const view_ptr& v) {
        return db::view::view_and_base{v, v->view_info()->base_info()};
    }));
}

delete_ghost_rows_visitor::delete_ghost_rows_visitor(service::storage_proxy& proxy, service::query_state& state, view_ptr view, db::timeout_clock::duration timeout_duration)
        : _proxy(proxy)
        , _state(state)
        , _timeout_duration(timeout_duration)
        , _view(view)
        , _view_table(_proxy.get_db().local().find_column_family(view))
        , _base_schema(_proxy.get_db().local().find_schema(_view->view_info()->base_id()))
        , _view_pk()
{}

void delete_ghost_rows_visitor::accept_new_partition(const partition_key& key, uint32_t row_count) {
    SCYLLA_ASSERT(thread::running_in_thread());
    _view_pk = key;
}

// Assumes running in seastar::thread
void delete_ghost_rows_visitor::accept_new_row(const clustering_key& ck, const query::result_row_view& static_row, const query::result_row_view& row) {
    auto view_exploded_pk = _view_pk->explode();
    auto view_exploded_ck = ck.explode();
    std::vector<bytes> base_exploded_pk(_base_schema->partition_key_size());
    std::vector<bytes> base_exploded_ck(_base_schema->clustering_key_size());
    for (const column_definition& view_cdef : _view->all_columns()) {
        const column_definition* base_cdef = _base_schema->get_column_definition(view_cdef.name());
        if (base_cdef) {
            std::vector<bytes>& view_exploded_key = view_cdef.is_partition_key() ? view_exploded_pk : view_exploded_ck;
            if (base_cdef->is_partition_key()) {
                base_exploded_pk[base_cdef->id] = view_exploded_key[view_cdef.id];
            } else if (base_cdef->is_clustering_key()) {
                base_exploded_ck[base_cdef->id] = view_exploded_key[view_cdef.id];
            }
        }
    }
    partition_key base_pk = partition_key::from_exploded(base_exploded_pk);
    clustering_key base_ck = clustering_key::from_exploded(base_exploded_ck);

    dht::partition_range_vector partition_ranges({dht::partition_range::make_singular(dht::decorate_key(*_base_schema, base_pk))});
    auto selection = cql3::selection::selection::for_columns(_base_schema, std::vector<const column_definition*>({&_base_schema->partition_key_columns().front()}));

    std::vector<query::clustering_range> bounds{query::clustering_range::make_singular(base_ck)};
    query::partition_slice partition_slice(std::move(bounds), {},  {}, selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(_base_schema->id(), _base_schema->version(), partition_slice,
            _proxy.get_max_result_size(partition_slice), query::tombstone_limit(_proxy.get_tombstone_limit()));
    auto timeout = db::timeout_clock::now() + _timeout_duration;
    service::storage_proxy::coordinator_query_options opts{timeout, _state.get_permit(), _state.get_client_state(), _state.get_trace_state()};
    auto base_qr = _proxy.query(_base_schema, command, std::move(partition_ranges), db::consistency_level::ALL, opts).get();
    query::result& result = *base_qr.query_result;
    if (result.row_count().value_or(0) == 0) {
        mutation m(_view, *_view_pk);
        auto& row = m.partition().clustered_row(*_view, ck);
        row.apply(tombstone(api::new_timestamp(), gc_clock::now()));
        timeout = db::timeout_clock::now() + _timeout_duration;
        _proxy.mutate({m}, db::consistency_level::ALL, timeout, _state.get_trace_state(), empty_service_permit(), db::allow_per_partition_rate_limit::no).get();
    }
}

std::chrono::microseconds calculate_view_update_throttling_delay(db::view::update_backlog backlog,
                                                                 db::timeout_clock::time_point timeout) {
    constexpr auto delay_limit_us = 1000000;
    auto adjust = [] (float x) { return x * x * x; };
    auto budget = std::max(service::storage_proxy::clock_type::duration(0),
        timeout - service::storage_proxy::clock_type::now());
    std::chrono::microseconds ret(uint32_t(adjust(backlog.relative_size()) * delay_limit_us));
    // "budget" has millisecond resolution and can potentially be long
    // in the future so converting it to microseconds may overflow.
    // So to compare buget and ret we need to convert both to the lower
    // resolution.
    if (std::chrono::duration_cast<service::storage_proxy::clock_type::duration>(ret) < budget) {
        return ret;
    } else {
        // budget is small (< ret) so can be converted to microseconds
        return std::chrono::duration_cast<std::chrono::microseconds>(budget);
    }
}
} // namespace view
} // namespace db
