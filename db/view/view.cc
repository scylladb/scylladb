/*
 */

/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

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

#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "replica/database.hh"
#include "clustering_bounds_comparator.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/util.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "db/view/view.hh"
#include "db/view/view_builder.hh"
#include "db/view/view_updating_consumer.hh"
#include "db/system_keyspace_view_types.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "gms/inet_address.hh"
#include "keys.hh"
#include "locator/network_topology_strategy.hh"
#include "mutation.hh"
#include "mutation_partition.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "view_info.hh"
#include "view_update_checks.hh"
#include "types/user.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "utils/error_injection.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/fb_utilities.hh"
#include "query-result-writer.hh"

using namespace std::chrono_literals;

static logging::logger vlogger("view");

static inline void inject_failure(std::string_view operation) {
    utils::get_local_injector().inject(operation,
            [operation] { throw std::runtime_error(std::string(operation)); });
}

view_info::view_info(const schema& schema, const raw_view_info& raw_view_info)
        : _schema(schema)
        , _raw(raw_view_info)
{ }

cql3::statements::select_statement& view_info::select_statement() const {
    if (!_select_statement) {
        std::unique_ptr<cql3::statements::raw::select_statement> raw;
        // FIXME(sarna): legacy code, should be removed after "computed_columns" feature is guaranteed
        // to be available on every node. Then, we won't need to check if this view is backing a secondary index.
        const column_definition* legacy_token_column = nullptr;
        if (service::get_local_storage_proxy().local_db().find_column_family(base_id()).get_index_manager().is_global_index(_schema)) {
           if (!_schema.clustering_key_columns().empty()) {
               legacy_token_column = &_schema.clustering_key_columns().front();
           }
        }

        if (legacy_token_column || boost::algorithm::any_of(_schema.all_columns(), std::mem_fn(&column_definition::is_computed))) {
            auto real_columns = _schema.all_columns() | boost::adaptors::filtered([this, legacy_token_column] (const column_definition& cdef) {
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
        auto prepared = raw->prepare(service::get_local_storage_proxy().data_dictionary(), ignored, true);
        _select_statement = static_pointer_cast<cql3::statements::select_statement>(prepared->statement);
    }
    return *_select_statement;
}

const query::partition_slice& view_info::partition_slice() const {
    if (!_partition_slice) {
        _partition_slice = select_statement().make_partition_slice(cql3::query_options({ }));
    }
    return *_partition_slice;
}

const column_definition* view_info::view_column(const schema& base, column_id base_id) const {
    // FIXME: Map base column_ids to view_column_ids, which can be something like
    // a boost::small_vector where the position is the base column_id, and the
    // value is either empty or the view's column_id.
    return view_column(base.regular_column_at(base_id));
}

const column_definition* view_info::view_column(const column_definition& base_def) const {
    return _schema.get_column_definition(base_def.name());
}

void view_info::set_base_info(db::view::base_info_ptr base_info) {
    _base_info = std::move(base_info);
}

// A constructor for a base info that can facilitate reads and writes from the materialized view.
db::view::base_dependent_view_info::base_dependent_view_info(schema_ptr base_schema, std::vector<column_id>&& base_non_pk_columns_in_view_pk)
        : _base_schema{std::move(base_schema)}
        , _base_non_pk_columns_in_view_pk{std::move(base_non_pk_columns_in_view_pk)}
        , has_base_non_pk_columns_in_view_pk{!_base_non_pk_columns_in_view_pk.empty()}
        , use_only_for_reads{false} {

}

// A constructor for a base info that can facilitate only reads from the materialized view.
db::view::base_dependent_view_info::base_dependent_view_info(bool has_base_non_pk_columns_in_view_pk, std::optional<bytes>&& column_missing_in_base)
        : _base_schema{nullptr}
        , _column_missing_in_base{std::move(column_missing_in_base)}
        , has_base_non_pk_columns_in_view_pk{has_base_non_pk_columns_in_view_pk}
        , use_only_for_reads{true} {
}

const std::vector<column_id>& db::view::base_dependent_view_info::base_non_pk_columns_in_view_pk() const {
    if (use_only_for_reads) {
        on_internal_error(vlogger,
                format("base_non_pk_columns_in_view_pk(): operation unsupported when initialized only for view reads. "
                "Missing column in the base table: {}", to_sstring_view(_column_missing_in_base.value_or(bytes()))));
    }
    return _base_non_pk_columns_in_view_pk;
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
    std::vector<column_id> base_non_pk_columns_in_view_pk;

    for (auto&& view_col : boost::range::join(_schema.partition_key_columns(), _schema.clustering_key_columns())) {
        if (view_col.is_computed()) {
            // we are not going to find it in the base table...
            continue;
        }
        const bytes& view_col_name = view_col.name();
        auto* base_col = base.get_column_definition(view_col_name);
        if (base_col && !base_col->is_primary_key()) {
            base_non_pk_columns_in_view_pk.push_back(base_col->id);
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
            // one that the view was created with. Seting this schema as the base can't harm since
            // if we got to such a situation then it means it is only going to be used for reading
            // (computation of shadowable tombstones) and in that case the existence of such a column
            // is the only thing that is of interest to us.
            return make_lw_shared<db::view::base_dependent_view_info>(true, view_col_name);
        }
    }

    return make_lw_shared<db::view::base_dependent_view_info>(base.shared_from_this(), std::move(base_non_pk_columns_in_view_pk));
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

bool partition_key_matches(const schema& base, const view_info& view, const dht::decorated_key& key) {
    const auto r = view.select_statement().get_restrictions()->get_partition_key_restrictions();
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
    return cql3::expr::is_satisfied_by(
            r->expression, exploded_pk, exploded_ck, dummy_row, &dummy_row, *selection, cql3::query_options({ }));
}

bool clustering_prefix_matches(const schema& base, const view_info& view, const partition_key& key, const clustering_key_prefix& ck) {
    const auto r = view.select_statement().get_restrictions()->get_clustering_columns_restrictions();
    std::vector<bytes> exploded_pk = key.explode();
    std::vector<bytes> exploded_ck = ck.explode();
    std::vector<const column_definition*> ck_columns;
    ck_columns.reserve(base.clustering_key_size());
    for (const column_definition& column : base.clustering_key_columns()) {
        ck_columns.push_back(&column);
    }
    auto selection = cql3::selection::selection::for_columns(base.shared_from_this(), ck_columns);
    uint64_t zero = 0;
    auto dummy_row = query::result_row_view(ser::qr_row_view{simple_memory_input_stream(reinterpret_cast<const char*>(&zero), 8)});
    return cql3::expr::is_satisfied_by(
            r->expression, exploded_pk, exploded_ck, dummy_row, &dummy_row, *selection, cql3::query_options({ }));
}

bool may_be_affected_by(const schema& base, const view_info& view, const dht::decorated_key& key, const rows_entry& update) {
    // We can guarantee that the view won't be affected if:
    //  - the primary key is excluded by the view filter (note that this isn't true of the filter on regular columns:
    //    even if an update don't match a view condition on a regular column, that update can still invalidate a
    //    pre-existing entry) - note that the upper layers should already have checked the partition key;
    return clustering_prefix_matches(base, view, key.key(), update.key());
}

static bool update_requires_read_before_write(const schema& base,
        const std::vector<view_and_base>& views,
        const dht::decorated_key& key,
        const rows_entry& update) {
    for (auto&& v : views) {
        view_info& vf = *v.view->view_info();
        if (may_be_affected_by(base, vf, key, update)) {
            return true;
        }
    }
    return false;
}

static bool is_partition_key_empty(
        const schema& base,
        const schema& view_schema,
        const partition_key& base_key,
        const clustering_row& update) {
    // Empty partition keys are not supported on normal tables - they cannot
    // be inserted or queried, so enforce those rules here.
    if (view_schema.partition_key_columns().size() > 1) {
        // Composite partition keys are different: all components
        // are then allowed to be empty.
        return false;
    }
    auto* base_col = base.get_column_definition(view_schema.partition_key_columns().front().name());
    switch (base_col->kind) {
    case column_kind::partition_key:
        return base_key.get_component(base, base_col->position()).empty();
    case column_kind::clustering_key:
        return update.key().get_component(base, base_col->position()).empty();
    default:
        // No multi-cell columns in the view's partition key
        auto& c = update.cells().cell_at(base_col->id);
        atomic_cell_view col_value = c.as_atomic_cell(*base_col);
        return !col_value.is_live() || col_value.value().empty();
    }
}

// Checks if the result matches the provided view filter.
// It's currently assumed that the result consists of just a single row.
class view_filter_checking_visitor {
    const schema& _base;
    const view_info& _view;
    ::shared_ptr<cql3::selection::selection> _selection;
    std::vector<bytes> _pk;

    bool _matches_view_filter = true;
public:
    view_filter_checking_visitor(const schema& base, const view_info& view)
        : _base(base)
        , _view(view)
        , _selection(cql3::selection::selection::wildcard(_base.shared_from_this()))
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
            _view.select_statement().get_restrictions()->get_non_pk_restriction() | boost::adaptors::map_values,
            [&] (auto&& r) {
                return cql3::expr::is_satisfied_by(
                        r->expression, _pk, ck, static_row, &row, *_selection, cql3::query_options({ }));
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
    return query::partition_slice(
            {query::full_clustering_range},
            { },
            boost::copy_range<query::column_id_vector>(s.regular_columns()
                    | boost::adaptors::transformed(std::mem_fn(&column_definition::id))),
            std::move(opts));
}

class data_query_result_builder {
public:
    using result_type = query::result;
    static constexpr emit_only_live_rows only_live = emit_only_live_rows::yes;

private:
    query::result::builder _res_builder;
    query_result_builder _builder;

public:
    data_query_result_builder(const schema& s, const query::partition_slice& slice)
        : _res_builder(slice, query::result_options::only_result(), query::result_memory_accounter{query::result_memory_limiter::unlimited_result_size})
        , _builder(s, _res_builder) { }

    void consume_new_partition(const dht::decorated_key& dk) { _builder.consume_new_partition(dk); }
    void consume(tombstone t) { _builder.consume(t); }
    stop_iteration consume(static_row&& sr, tombstone t, bool is_alive) { return _builder.consume(std::move(sr), t, is_alive); }
    stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_alive) { return _builder.consume(std::move(cr), t, is_alive); }
    stop_iteration consume(range_tombstone&& rt) { return _builder.consume(std::move(rt)); }
    stop_iteration consume_end_of_partition()  { return _builder.consume_end_of_partition(); }
    result_type consume_end_of_stream() {
        _builder.consume_end_of_stream();
        return _res_builder.build();
    }
};

bool matches_view_filter(const schema& base, const view_info& view, const partition_key& key, const clustering_row& update, gc_clock::time_point now) {
    auto slice = make_partition_slice(base);

    data_query_result_builder builder(base, slice);
    builder.consume_new_partition(dht::decorate_key(base, key));
    builder.consume(clustering_row(base, update), row_tombstone{}, update.is_live(base, tombstone{}, now));
    builder.consume_end_of_partition();
    auto result = builder.consume_end_of_stream();
    view_filter_checking_visitor visitor(base, view);
    query::result_view::consume(result, slice, visitor);

    return clustering_prefix_matches(base, view, key, update.key())
            && visitor.matches_view_filter();
}

void view_updates::move_to(utils::chunked_vector<frozen_mutation_and_schema>& mutations) {
    std::transform(_updates.begin(), _updates.end(), std::back_inserter(mutations), [&, this] (auto&& m) {
        auto mut = mutation(_view, dht::decorate_key(*_view, std::move(m.first)), std::move(m.second));
        return frozen_mutation_and_schema{freeze(mut), _view};
    });
    _updates.clear();
    _op_count = 0;
}

mutation_partition& view_updates::partition_for(partition_key&& key) {
    auto it = _updates.find(key);
    if (it != _updates.end()) {
        return it->second;
    }
    return _updates.emplace(std::move(key), mutation_partition(_view)).first->second;
}

size_t view_updates::op_count() const {
    return _op_count++;;
}

row_marker view_updates::compute_row_marker(const clustering_row& base_row) const {
    /*
     * We need to compute both the timestamp and expiration.
     *
     * There are 3 cases:
     *   1) There is a column that is not in the base PK but is in the view PK. In that case, as long as that column
     *      lives, the view entry does too, but as soon as it expires (or is deleted for that matter) the entry also
     *      should expire. So the expiration for the view is the one of that column, regardless of any other expiration.
     *      To take an example of that case, if you have:
     *        CREATE TABLE t (a int, b int, c int, PRIMARY KEY (a, b))
     *        CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)
     *        INSERT INTO t(a, b) VALUES (0, 0) USING TTL 3;
     *        UPDATE t SET c = 0 WHERE a = 0 AND b = 0;
     *      then even after 3 seconds elapsed, the row will still exist (it just won't have a "row marker" anymore) and so
     *      the MV should still have a corresponding entry.
     *      This cell determines the liveness of the view row.
     *   2) The columns for the base and view PKs are exactly the same, and all base columns are selected by the view.
     *      In that case, all components (marker, deletion and cells) are the same and trivially mapped.
     *   3) The columns for the base and view PKs are exactly the same, but some base columns are not selected in the view.
     *      Use the max timestamp out of the base row marker and all the unselected columns - this ensures we can keep the
     *      view row alive. Do the same thing for the expiration, if the marker is dead or will expire, and so
     *      will all unselected columns.
     */

    auto marker = base_row.marker();
    // WARNING: The code assumes that if multiple regular base columns are present in the view key,
    // they share liveness information. It's true especially in the only case currently allowed by CQL,
    // which assumes there's up to one non-pk column in the view key. It's also true in alternator,
    // which does not carry TTL information.
    const auto& col_ids = _base_info->base_non_pk_columns_in_view_pk();
    if (!col_ids.empty()) {
        auto& def = _base->regular_column_at(col_ids[0]);
        // Note: multi-cell columns can't be part of the primary key.
        auto cell = base_row.cells().cell_at(col_ids[0]).as_atomic_cell(def);
        return cell.is_live_and_has_ttl() ? row_marker(cell.timestamp(), cell.ttl(), cell.expiry()) : row_marker(cell.timestamp());
    }

    return marker;
}

deletable_row& view_updates::get_view_row(const partition_key& base_key, const clustering_row& update) {
    std::vector<bytes> linearized_values;
    auto get_value = boost::adaptors::transformed([&, this] (const column_definition& cdef) -> managed_bytes_view {
        auto* base_col = _base->get_column_definition(cdef.name());
        if (!base_col) {
            bytes_opt computed_value;
            if (!cdef.is_computed()) {
                //FIXME(sarna): this legacy code is here for backward compatibility and should be removed
                // once "computed_columns feature" is supported by every node
                if (!service::get_local_storage_proxy().local_db().find_column_family(_base->id()).get_index_manager().is_index(*_view)) {
                    throw std::logic_error(format("Column {} doesn't exist in base and this view is not backing a secondary index", cdef.name_as_text()));
                }
                computed_value = legacy_token_column_computation().compute_value(*_base, base_key, update);
            } else {
                computed_value = cdef.get_computation().compute_value(*_base, base_key, update);
            }
            if (!computed_value) {
                throw std::logic_error(format("No value computed for primary key column {}", cdef.name()));
            }
            return managed_bytes_view(linearized_values.emplace_back(*computed_value));
        }
        switch (base_col->kind) {
        case column_kind::partition_key:
            return base_key.get_component(*_base, base_col->position());
        case column_kind::clustering_key:
            return update.key().get_component(*_base, base_col->position());
        default:
            auto& c = update.cells().cell_at(base_col->id);
            auto value_view = base_col->is_atomic() ? c.as_atomic_cell(cdef).value() : c.as_collection_mutation().data;
            return value_view;
        }
    });
    auto& partition = partition_for(partition_key::from_range(_view->partition_key_columns() | get_value));
    auto ckey = clustering_key::from_range(_view->clustering_key_columns() | get_value);
    return partition.clustered_row(*_view, std::move(ckey));
}

static const column_definition* view_column(const schema& base, const schema& view, column_id base_id) {
    // FIXME: Map base column_ids to view_column_ids, which can be something like
    // a boost::small_vector where the position is the base column_id, and the
    // value is either empty or the view's column_id.
    return view.get_column_definition(base.regular_column_at(base_id).name());
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

static void add_cells_to_view(const schema& base, const schema& view, row base_cells, row& view_cells) {
    base_cells.for_each_cell([&] (column_id id, atomic_cell_or_collection& c) {
        auto* view_col = view_column(base, view, id);
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
void view_updates::create_entry(const partition_key& base_key, const clustering_row& update, gc_clock::time_point now) {
    if (is_partition_key_empty(*_base, *_view, base_key, update) || !matches_view_filter(*_base, _view_info, base_key, update, now)) {
        return;
    }
    deletable_row& r = get_view_row(base_key, update);
    auto marker = compute_row_marker(update);
    r.apply(marker);
    r.apply(update.tomb());
    add_cells_to_view(*_base, *_view, row(*_base, column_kind::regular_column, update.cells()), r.cells());
    _op_count++;
}

/**
 * Deletes the view entry corresponding to the provided base row.
 * This method checks that the base row does match the view filter before bothering.
 */
void view_updates::delete_old_entry(const partition_key& base_key, const clustering_row& existing, const clustering_row& update, gc_clock::time_point now) {
    // Before deleting an old entry, make sure it was matching the view filter
    // (otherwise there is nothing to delete)
    if (!is_partition_key_empty(*_base, *_view, base_key, existing) && matches_view_filter(*_base, _view_info, base_key, existing, now)) {
        do_delete_old_entry(base_key, existing, update, now);
    }
}

void view_updates::do_delete_old_entry(const partition_key& base_key, const clustering_row& existing, const clustering_row& update, gc_clock::time_point now) {
    auto& r = get_view_row(base_key, existing);
    const auto& col_ids = _base_info->base_non_pk_columns_in_view_pk();
    if (!col_ids.empty()) {
        // We delete the old row using a shadowable row tombstone, making sure that
        // the tombstone deletes everything in the row (or it might still show up).
        // Note: multi-cell columns can't be part of the primary key.
        auto& def = _base->regular_column_at(col_ids[0]);
        auto cell = existing.cells().cell_at(col_ids[0]).as_atomic_cell(def);
        if (cell.is_live()) {
            r.apply(shadowable_tombstone(cell.timestamp(), now));
        }
    } else {
        // "update" caused the base row to have been deleted, and !col_id
        // means view row is the same - so it needs to be deleted as well
        // using the same deletion timestamps for the individual cells.
        r.apply(update.marker());
        auto diff = update.cells().difference(*_base, column_kind::regular_column, existing.cells());
        add_cells_to_view(*_base, *_view, std::move(diff), r.cells());
    }
    r.apply(update.tomb());
    _op_count++;
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

bool view_updates::can_skip_view_updates(const clustering_row& update, const clustering_row& existing) const {
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
void view_updates::update_entry(const partition_key& base_key, const clustering_row& update, const clustering_row& existing, gc_clock::time_point now) {
    // While we know update and existing correspond to the same view entry,
    // they may not match the view filter.
    if (is_partition_key_empty(*_base, *_view, base_key, existing) || !matches_view_filter(*_base, _view_info, base_key, existing, now)) {
        create_entry(base_key, update, now);
        return;
    }
    if (is_partition_key_empty(*_base, *_view, base_key, update) || !matches_view_filter(*_base, _view_info, base_key, update, now)) {
        do_delete_old_entry(base_key, existing, update, now);
        return;
    }

    if (can_skip_view_updates(update, existing)) {
        return;
    }

    deletable_row& r = get_view_row(base_key, update);
    auto marker = compute_row_marker(update);
    r.apply(marker);
    r.apply(update.tomb());

    auto diff = update.cells().difference(*_base, column_kind::regular_column, existing.cells());
    add_cells_to_view(*_base, *_view, std::move(diff), r.cells());
    _op_count++;
}

void view_updates::generate_update(
        const partition_key& base_key,
        const clustering_row& update,
        const std::optional<clustering_row>& existing,
        gc_clock::time_point now) {
    // Note that the base PK columns in update and existing are the same, since we're intrinsically dealing
    // with the same base row. So we have to check 3 things:
    //   1) that the clustering key doesn't have a null, which can happen for compact tables. If that's the case,
    //      there is no corresponding entries.
    //   2) if there is a column not part of the base PK in the view PK, whether it is changed by the update.
    //   3) whether the update actually matches the view SELECT filter

    if (!update.key().is_full(*_base)) {
        return;
    }

    const auto& col_ids = _base_info->base_non_pk_columns_in_view_pk();
    if (col_ids.empty()) {
        // The view key is necessarily the same pre and post update.
        if (existing && existing->is_live(*_base)) {
            if (update.is_live(*_base)) {
                update_entry(base_key, update, *existing, now);
            } else {
                delete_old_entry(base_key, *existing, update, now);
            }
        } else if (update.is_live(*_base)) {
            create_entry(base_key, update, now);
        }
        return;
    }

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
        // Note: multi-cell columns can't be part of the primary key.
        auto& cdef = _base->regular_column_at(col_id);
        if (existing) {
            auto* before = existing->cells().find_cell(col_id);
            if (before && before->as_atomic_cell(cdef).is_live()) {
                if (after && after->as_atomic_cell(cdef).is_live()) {
                    auto cmp = compare_atomic_cell_for_merge(before->as_atomic_cell(cdef), after->as_atomic_cell(cdef));
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
                update_entry(base_key, update, *existing, now);
            } else {
                replace_entry(base_key, update, *existing, now);
            }
        } else {
            delete_old_entry(base_key, *existing, update, now);
        }
    } else if (has_new_row) {
        create_entry(base_key, update, now);
    }
}

future<> view_update_builder::close() noexcept {
    return when_all_succeed(_updates.close(), _existings->close()).discard_result();
}

future<stop_iteration> view_update_builder::advance_all() {
    auto existings_f = _existings ? (*_existings)() : make_ready_future<optimized_optional<mutation_fragment>>();
    return when_all(_updates(), std::move(existings_f)).then([this] (auto&& fragments) mutable {
        _update = std::move(std::get<0>(fragments).get0());
        _existing = std::move(std::get<1>(fragments).get0());
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

future<utils::chunked_vector<frozen_mutation_and_schema>> view_update_builder::build_some() {
    return advance_all().then([this] (stop_iteration ignored) {
        bool do_advance_updates = false;
        bool do_advance_existings = false;
        if (_update && _update->is_partition_start()) {
            _key = std::move(std::move(_update)->as_partition_start().key().key());
            _update_tombstone_tracker.set_partition_tombstone(_update->as_partition_start().partition_tombstone());
            do_advance_updates = true;
        }
        if (_existing && _existing->is_partition_start()) {
            _existing_tombstone_tracker.set_partition_tombstone(_existing->as_partition_start().partition_tombstone());
            do_advance_existings = true;
        }
        if (do_advance_updates) {
            return do_advance_existings ? advance_all() : advance_updates();
        } else if (do_advance_existings) {
            return advance_existings();
        }
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }).then([this] (stop_iteration ignored) {
        return repeat([this] {
            return this->on_results();
        });
    }).then([this] {
        utils::chunked_vector<frozen_mutation_and_schema> mutations;
        for (auto& update : _view_updates) {
            update.move_to(mutations);
        }
        return mutations;
    });
}

void view_update_builder::generate_update(clustering_row&& update, std::optional<clustering_row>&& existing) {
    // If we have no update at all, we shouldn't get there.
    if (update.empty()) {
        throw std::logic_error("Empty materialized view updated");
    }

    auto dk = dht::decorate_key(*_schema, _key);
    auto gc_before = ::get_gc_before_for_key(_schema, dk, _now);

    // We allow existing to be disengaged, which we treat the same as an empty row.
    if (existing) {
        existing->marker().compact_and_expire(existing->tomb().tomb(), _now, always_gc, gc_before);
        existing->cells().compact_and_expire(*_schema, column_kind::regular_column, existing->tomb(), _now, always_gc, gc_before, existing->marker());
        update.apply(*_schema, *existing);
    }

    update.marker().compact_and_expire(update.tomb().tomb(), _now, always_gc, gc_before);
    update.cells().compact_and_expire(*_schema, column_kind::regular_column, update.tomb(), _now, always_gc, gc_before, update.marker());

    for (auto&& v : _view_updates) {
        v.generate_update(_key, update, existing, _now);
    }
}

static void apply_tracked_tombstones(range_tombstone_accumulator& tracker, clustering_row& row) {
    row.apply(tracker.tombstone_for_row(row.key()));
}

future<stop_iteration> view_update_builder::on_results() {
    constexpr size_t max_rows_for_view_updates = 100;
    size_t rows_for_view_updates = std::accumulate(_view_updates.begin(), _view_updates.end(), 0, [] (size_t acc, const view_updates& vu) {
        return acc + vu.op_count();
    });
    const bool stop_updates = rows_for_view_updates >= max_rows_for_view_updates;

    if (_update && !_update->is_end_of_partition() && _existing && !_existing->is_end_of_partition()) {
        auto cmp = position_in_partition::tri_compare(*_schema)(_update->position(), _existing->position());
        if (cmp < 0) {
            // We have an update where there was nothing before
            if (_update->is_range_tombstone()) {
                _update_tombstone_tracker.apply(std::move(_update->as_range_tombstone()));
            } else if (_update->is_clustering_row()) {
                auto update = std::move(*_update).as_clustering_row();
                apply_tracked_tombstones(_update_tombstone_tracker, update);
                auto tombstone = _existing_tombstone_tracker.current_tombstone();
                auto existing = tombstone
                              ? std::optional<clustering_row>(std::in_place, update.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row())
                              : std::nullopt;
                generate_update(std::move(update), std::move(existing));
            }
            return stop_updates ? stop() : advance_updates();
        }
        if (cmp > 0) {
            // We have something existing but no update (which will happen either because it's a range tombstone marker in
            // existing, or because we've fetched the existing row due to some partition/range deletion in the updates)
            if (_existing->is_range_tombstone()) {
                _existing_tombstone_tracker.apply(std::move(_existing->as_range_tombstone()));
            } else if (_existing->is_clustering_row()) {
                auto existing = std::move(*_existing).as_clustering_row();
                apply_tracked_tombstones(_existing_tombstone_tracker, existing);
                auto tombstone = _update_tombstone_tracker.current_tombstone();
                // The way we build the read command used for existing rows, we should always have a non-empty
                // tombstone, since we wouldn't have read the existing row otherwise. We don't assert that in case the
                // read method ever changes.
                if (tombstone) {
                    auto update = clustering_row(existing.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row());
                    generate_update(std::move(update), { std::move(existing) });
                }
            }
            return stop_updates ? stop () : advance_existings();
        }
        // We're updating a row that had pre-existing data
        if (_update->is_range_tombstone()) {
            assert(_existing->is_range_tombstone());
            _existing_tombstone_tracker.apply(std::move(*_existing).as_range_tombstone());
            _update_tombstone_tracker.apply(std::move(*_update).as_range_tombstone());
        } else if (_update->is_clustering_row()) {
            assert(_existing->is_clustering_row());
            _update->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                apply_tracked_tombstones(_update_tombstone_tracker, cr);
            });
            _existing->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                apply_tracked_tombstones(_existing_tombstone_tracker, cr);
            });
            generate_update(std::move(*_update).as_clustering_row(), { std::move(*_existing).as_clustering_row() });
        }
        return stop_updates ? stop() : advance_all();
    }

    auto tombstone = _update_tombstone_tracker.current_tombstone();
    if (tombstone && _existing && !_existing->is_end_of_partition()) {
        // We don't care if it's a range tombstone, as we're only looking for existing entries that get deleted
        if (_existing->is_clustering_row()) {
            auto existing = clustering_row(*_schema, _existing->as_clustering_row());
            auto update = clustering_row(existing.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row());
            generate_update(std::move(update), { std::move(existing) });
        }
        return stop_updates ? stop() : advance_existings();
    }

    // If we have updates and it's a range tombstone, it removes nothing pre-exisiting, so we can ignore it
    if (_update && !_update->is_end_of_partition()) {
        if (_update->is_clustering_row()) {
            _update->mutate_as_clustering_row(*_schema, [&] (clustering_row& cr) mutable {
                apply_tracked_tombstones(_update_tombstone_tracker, cr);
            });
            auto existing_tombstone = _existing_tombstone_tracker.current_tombstone();
            auto existing = existing_tombstone
                          ? std::optional<clustering_row>(std::in_place, _update->as_clustering_row().key(), row_tombstone(std::move(existing_tombstone)), row_marker(), ::row())
                          : std::nullopt;
            generate_update(std::move(*_update).as_clustering_row(), std::move(existing));
        }
        return stop_updates ? stop() : advance_updates();
    }

    return stop();
}

future<view_update_builder> make_view_update_builder(
        const schema_ptr& base,
        std::vector<view_and_base>&& views_to_update,
        flat_mutation_reader&& updates,
        flat_mutation_reader_opt&& existings,
        gc_clock::time_point now) {
    auto vs = boost::copy_range<std::vector<view_updates>>(views_to_update | boost::adaptors::transformed([&] (view_and_base v) {
        if (base->version() != v.base->base_schema()->version()) {
            on_internal_error(vlogger, format("Schema version used for view updates ({}) does not match the current"
                                              " base schema version of the view ({}) for view {}.{} of {}.{}",
                base->version(), v.base->base_schema()->version(), v.view->ks_name(), v.view->cf_name(), base->ks_name(), base->cf_name()));
        }
        return view_updates(std::move(v));
    }));
    return make_ready_future<view_update_builder>(view_update_builder(base, std::move(vs), std::move(updates), std::move(existings), now));
}

future<query::clustering_row_ranges> calculate_affected_clustering_ranges(const schema& base,
        const dht::decorated_key& key,
        const mutation_partition& mp,
        const std::vector<view_and_base>& views) {
    utils::chunked_vector<nonwrapping_range<clustering_key_prefix_view>> row_ranges;
    utils::chunked_vector<nonwrapping_range<clustering_key_prefix_view>> view_row_ranges;
    clustering_key_prefix_view::tri_compare cmp(base);
    if (mp.partition_tombstone() || !mp.row_tombstones().empty()) {
        for (auto&& v : views) {
            // FIXME: #2371
            if (v.view->view_info()->select_statement().get_restrictions()->has_unrestricted_clustering_columns()) {
                view_row_ranges.push_back(nonwrapping_range<clustering_key_prefix_view>::make_open_ended_both_sides());
                break;
            }
            for (auto&& r : v.view->view_info()->partition_slice().default_row_ranges()) {
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
            nonwrapping_range<clustering_key_prefix_view> rtr(
                    bound_view::to_range_bound<nonwrapping_range>(rt.start_bound()),
                    bound_view::to_range_bound<nonwrapping_range>(rt.end_bound()));
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
        if (update_requires_read_before_write(base, views, key, row)) {
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
            nonwrapping_range<clustering_key_prefix_view>::deoverlap(std::move(row_ranges), cmp)
            | boost::adaptors::transformed([] (auto&& v) {
                return std::move(v).transform([] (auto&& ckv) { return clustering_key_prefix(ckv); });
            }));

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
// If the keyspace's replication strategy is a NetworkTopologyStrategy,
// we pair only nodes in the same datacenter.
// If one of the base replicas also happens to be a view replica, it is
// paired with itself (with the other nodes paired by order in the list
// after taking this node out).
//
// If the assumption that the given base token belongs to this replica
// does not hold, we return an empty optional.
static std::optional<gms::inet_address>
get_view_natural_endpoint(const sstring& keyspace_name,
        const dht::token& base_token, const dht::token& view_token) {
    auto &db = service::get_local_storage_proxy().local_db();
    auto& ks = db.find_keyspace(keyspace_name);
    auto erm = ks.get_effective_replication_map();
    auto my_address = utils::fb_utilities::get_broadcast_address();
    auto my_datacenter = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(my_address);
    bool network_topology = dynamic_cast<const locator::network_topology_strategy*>(&ks.get_replication_strategy());
    std::vector<gms::inet_address> base_endpoints, view_endpoints;
    for (auto&& base_endpoint : erm->get_natural_endpoints(base_token)) {
        if (!network_topology || locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(base_endpoint) == my_datacenter) {
            base_endpoints.push_back(base_endpoint);
        }
    }

    for (auto&& view_endpoint : erm->get_natural_endpoints(view_token)) {
        // If this base replica is also one of the view replicas, we use
        // ourselves as the view replica.
        if (view_endpoint == my_address) {
            return view_endpoint;
        }
        // We have to remove any endpoint which is shared between the base
        // and the view, as it will select itself and throw off the counts
        // otherwise.
        auto it = std::find(base_endpoints.begin(), base_endpoints.end(),
            view_endpoint);
        if (it != base_endpoints.end()) {
            base_endpoints.erase(it);
        } else if (!network_topology || locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(view_endpoint) == my_datacenter) {
            view_endpoints.push_back(view_endpoint);
        }
    }

    assert(base_endpoints.size() == view_endpoints.size());
    auto base_it = std::find(base_endpoints.begin(), base_endpoints.end(), my_address);
    if (base_it == base_endpoints.end()) {
        // This node is not a base replica of this key, so we return empty
        return {};
    }
    return view_endpoints[base_it - base_endpoints.begin()];
}

static future<> apply_to_remote_endpoints(gms::inet_address target, inet_address_vector_topology_change&& pending_endpoints,
        frozen_mutation_and_schema&& mut, const dht::token& base_token, const dht::token& view_token,
        service::allow_hints allow_hints, tracing::trace_state_ptr tr_state) {

    tracing::trace(tr_state, "Sending view update for {}.{} to {}, with pending endpoints = {}; base token = {}; view token = {}",
            mut.s->ks_name(), mut.s->cf_name(), target, pending_endpoints, base_token, view_token);
    return service::get_local_storage_proxy().send_to_endpoint(
            std::move(mut),
            target,
            std::move(pending_endpoints),
            db::write_type::VIEW,
            std::move(tr_state),
            allow_hints);
}

// Take the view mutations generated by generate_view_updates(), which pertain
// to a modification of a single base partition, and apply them to the
// appropriate paired replicas. This is done asynchronously - we do not wait
// for the writes to complete.
future<> mutate_MV(
        dht::token base_token,
        utils::chunked_vector<frozen_mutation_and_schema> view_updates,
        db::view::stats& stats,
        replica::cf_stats& cf_stats,
        tracing::trace_state_ptr tr_state,
        db::timeout_semaphore_units pending_view_updates,
        service::allow_hints allow_hints,
        wait_for_all_updates wait_for_all)
{
    static constexpr size_t max_concurrent_updates = 128;
    co_await max_concurrent_for_each(view_updates, max_concurrent_updates,
            [base_token, &stats, &cf_stats, tr_state, &pending_view_updates, allow_hints, wait_for_all] (frozen_mutation_and_schema mut) mutable -> future<> {
        auto view_token = dht::get_token(*mut.s, mut.fm.key());
        auto& keyspace_name = mut.s->ks_name();
        auto target_endpoint = get_view_natural_endpoint(keyspace_name, base_token, view_token);
        auto remote_endpoints = service::get_local_storage_proxy().get_token_metadata_ptr()->pending_endpoints_for(view_token, keyspace_name);
        auto sem_units = pending_view_updates.split(mut.fm.representation().size());

        // First, find the local endpoint and ensure that if it exists,
        // it will be the target endpoint. That way, all endpoints in the
        // remote_endpoints list are guaranteed to be remote.
        auto my_address = utils::fb_utilities::get_broadcast_address();
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
        // It's still possible that a target endpoint is dupliated in the remote endpoints list,
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
            local_view_update = service::get_local_storage_proxy().mutate_locally(mut.s, *mut_ptr, std::move(tr_state), db::commitlog::force_sync::no).then_wrapped(
                    [s = mut.s, &stats, &cf_stats, tr_state, base_token, view_token, my_address, mut_ptr = std::move(mut_ptr),
                            units = sem_units.split(sem_units.count())] (future<>&& f) {
                --stats.writes;
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

        future<> remote_view_update = make_ready_future<>();
        // If target_endpoint is engaged by this point, then either the update
        // is not local, or the local update was already applied but we still
        // have pending endpoints to send to.
        if (target_endpoint) {
            size_t updates_pushed_remote = remote_endpoints.size() + 1;
            stats.view_updates_pushed_remote += updates_pushed_remote;
            cf_stats.total_view_updates_pushed_remote += updates_pushed_remote;
            schema_ptr s = mut.s;
            future<> view_update = apply_to_remote_endpoints(*target_endpoint, std::move(remote_endpoints), std::move(mut), base_token, view_token, allow_hints, tr_state).then_wrapped(
                    [s = std::move(s), &stats, &cf_stats, tr_state, base_token, view_token, target_endpoint, updates_pushed_remote,
                            units = sem_units.split(sem_units.count()), wait_for_all] (future<>&& f) mutable {
                if (f.failed()) {
                    stats.view_updates_failed_remote += updates_pushed_remote;
                    cf_stats.total_view_updates_failed_remote += updates_pushed_remote;
                    auto ep = f.get_exception();
                    tracing::trace(tr_state, "Failed to apply view update for {} and {} remote endpoints",
                            *target_endpoint, updates_pushed_remote);
                    vlogger.error("Error applying view update to {} (view: {}.{}, base token: {}, view token: {}): {}",
                            *target_endpoint, s->ks_name(), s->cf_name(), base_token, view_token, ep);
                    return wait_for_all ? make_exception_future<>(std::move(ep)) : make_ready_future<>();
                }
                tracing::trace(tr_state, "Successfully applied view update for {} and {} remote endpoints",
                        *target_endpoint, updates_pushed_remote);
                return make_ready_future<>();
            });
            if (wait_for_all) {
                remote_view_update = std::move(view_update);
            } else {
                // The update is sent to background in order to preserve availability,
                // its parallelism is limited by view_update_concurrency_semaphore
                (void)view_update;
            }
        }
        return when_all_succeed(std::move(local_view_update), std::move(remote_view_update)).discard_result();
    });
}

view_builder::view_builder(replica::database& db, db::system_distributed_keyspace& sys_dist_ks, service::migration_notifier& mn)
        : _db(db)
        , _sys_dist_ks(sys_dist_ks)
        , _mnotifier(mn)
        , _permit(_db.get_reader_concurrency_semaphore().make_tracking_only_permit(nullptr, "view_builder", db::no_timeout)) {
    setup_metrics();
}

void view_builder::setup_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group("view_builder", {
        sm::make_gauge("pending_bookkeeping_ops",
                sm::description("Number of tasks waiting to perform bookkeeping operations"),
                [this] { return _sem.waiters(); }),

        sm::make_derive("steps_performed",
                sm::description("Number of performed build steps."),
                _stats.steps_performed),

        sm::make_derive("steps_failed",
                sm::description("Number of failed build steps."),
                _stats.steps_failed),

        sm::make_gauge("builds_in_progress",
                sm::description("Number of currently active view builds."),
                [this] { return _base_to_build_step.size(); })
    });
}

future<> view_builder::start(service::migration_manager& mm) {
    _started = do_with(view_builder_init_state{}, [this, &mm] (view_builder_init_state& vbi) {
        return seastar::async([this, &mm, &vbi] {
            // Guard the whole startup routine with a semaphore,
            // so that it's not intercepted by `on_drop_view`, `on_create_view`
            // or `on_update_view` events.
            auto units = get_units(_sem, 1).get0();
            // Wait for schema agreement even if we're a seed node.
            while (!mm.have_schema_agreement()) {
                seastar::sleep_abortable(500ms, _as).get();
            }
            auto built = system_keyspace::load_built_views().get0();
            auto in_progress = system_keyspace::load_view_build_progress().get0();
            setup_shard_build_step(vbi, std::move(built), std::move(in_progress));
        }).then_wrapped([this] (future<>&& f) {
            // All shards need to arrive at the same decisions on whether or not to
            // restart a view build at some common token (reshard), and which token
            // to restart at. So we need to wait until all shards have read the view
            // build statuses before they can all proceed to make the (same) decision.
            // If we don't synchronize here, a fast shard may make a decision, start
            // building and finish a build step - before the slowest shard even read
            // the view build information.
            std::exception_ptr eptr;
            if (f.failed()) {
                eptr = f.get_exception();
            }

            return container().invoke_on(0, [eptr = std::move(eptr)] (view_builder& builder) {
                // The &builder is alive, because it can only be destroyed in
                // sharded<view_builder>::stop(), which, in turn, waits for all
                // view_builder::stop()-s to finish, and each stop() waits for
                // the shard's current future (called _started) to resolve.
                if (!eptr) {
                    if (++builder._shards_finished_read == smp::count) {
                        builder._shards_finished_read_promise.set_value();
                    }
                } else {
                    if (builder._shards_finished_read < smp::count) {
                        builder._shards_finished_read = smp::count;
                        builder._shards_finished_read_promise.set_exception(std::move(eptr));
                    }
                }
                return builder._shards_finished_read_promise.get_shared_future();
            });
        }).then([this, &vbi] {
            return calculate_shard_build_step(vbi);
        }).then([this] {
            _mnotifier.register_listener(this);
            _current_step = _base_to_build_step.begin();
            // Waited on indirectly in stop().
            (void)_build_step.trigger();
            return make_ready_future<>();
        });
    }).handle_exception([] (std::exception_ptr eptr) {
        vlogger.error("start failed: {}", eptr);
        return make_ready_future<>();
    });
    return make_ready_future<>();
}

future<> view_builder::drain() {
    if (_as.abort_requested()) {
        return make_ready_future();
    }
    vlogger.info("Draining view builder");
    _as.request_abort();
    return _started.then([this] {
        return _mnotifier.unregister_listener(this).then([this] {
            return _sem.wait();
        }).then([this] {
            _sem.broken();
            return _build_step.join();
        }).handle_exception_type([] (const broken_semaphore&) {
            // ignored
        }).handle_exception_type([] (const semaphore_timed_out&) {
            // ignored
        }).finally([this] {
            return parallel_for_each(_base_to_build_step, [] (std::pair<const utils::UUID, build_step>& p) {
                return p.second.reader.close();
            });
        });
    });
}

future<> view_builder::stop() {
    vlogger.info("Stopping view builder");
    return drain();
}

view_builder::build_step& view_builder::get_or_create_build_step(utils::UUID base_id) {
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
            default_priority_class(),
            nullptr,
            streamed_mutation::forwarding::no,
            mutation_reader::forwarding::no);
  });
}

void view_builder::load_view_status(view_builder::view_build_status status, std::unordered_set<utils::UUID>& loaded_views) {
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
        std::unordered_set<utils::UUID>& loaded_views) {
    // We must reshard. We aim for a simple algorithm, a step above not starting from scratch.
    // Shards build entries at different paces, so both first and last tokens will differ. We
    // want to be conservative when selecting the range that has been built. To do that, we
    // select the intersection of all the previous shard's ranges for each view.
    struct view_ptr_hash {
        std::size_t operator()(const view_ptr& v) const noexcept {
            return std::hash<utils::UUID>()(v->id());
        }
    };
    struct view_ptr_equals {
        bool operator()(const view_ptr& v1, const view_ptr& v2) const noexcept {
            return v1->id() == v2->id();
        }
    };
    std::unordered_map<view_ptr, std::optional<nonwrapping_range<dht::token>>, view_ptr_hash, view_ptr_equals> my_status;
    for (auto& shard_status : view_build_status_per_shard) {
        for (auto& [view, first_token, next_token] : shard_status ) {
            // We start from an open-ended range, which we'll try to restrict.
            auto& my_range = my_status.emplace(
                    std::move(view),
                    nonwrapping_range<dht::token>::make_open_ended_both_sides()).first->second;
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
            wrapping_range<dht::token> other_range(first_token, *next_token);
            if (other_range.is_wrap_around(dht::token_comparator())) {
                // The intersection of a wrapping range with a non-wrapping range may yield more
                // multiple non-contiguous ranges. To avoid the complexity of dealing with more
                // than one range, we'll just take one of the intersections.
                auto [bottom_range, top_range] = other_range.unwrap();
                if (auto bottom_int = my_range->intersection(nonwrapping_interval(std::move(bottom_range)), dht::token_comparator())) {
                    my_range = std::move(bottom_int);
                } else {
                    my_range = my_range->intersection(nonwrapping_interval(std::move(top_range)), dht::token_comparator());
                }
            } else {
                my_range = my_range->intersection(nonwrapping_interval(std::move(other_range)), dht::token_comparator());
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
    auto base_table_exists = [this] (const view_ptr& view) {
        // This is a safety check in case this node missed a create MV statement
        // but got a drop table for the base, and another node didn't get the
        // drop notification and sent us the view schema.
        try {
            _db.find_schema(view->view_info()->base_id());
            return true;
        } catch (const replica::no_such_column_family&) {
            return false;
        }
    };
    auto maybe_fetch_view = [&, this] (system_keyspace_view_name& name) {
        try {
            auto s = _db.find_schema(name.first, name.second);
            if (s->is_view()) {
                auto view = view_ptr(std::move(s));
                if (base_table_exists(view)) {
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
            vbi.bookkeeping_ops.push_back(system_keyspace::remove_built_view(name.first, name.second));
            vbi.bookkeeping_ops.push_back(
                    system_keyspace::remove_view_build_progress_across_all_shards(
                            std::move(name.first),
                            std::move(name.second)));
        }
        return view_ptr(nullptr);
    };

    vbi.built_views = boost::copy_range<std::unordered_set<utils::UUID>>(built
            | boost::adaptors::transformed(maybe_fetch_view)
            | boost::adaptors::filtered([] (const view_ptr& v) { return bool(v); })
            | boost::adaptors::transformed([] (const view_ptr& v) { return v->id(); }));

    for (auto& [view_name, first_token, next_token_opt, cpu_id] : in_progress) {
        if (auto view = maybe_fetch_view(view_name)) {
            if (vbi.built_views.contains(view->id())) {
                if (this_shard_id() == 0) {
                    auto f = _sys_dist_ks.finish_view_build(std::move(view_name.first), std::move(view_name.second)).then([view = std::move(view)] {
                        return system_keyspace::remove_view_build_progress_across_all_shards(view->cf_name(), view->ks_name());
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
    auto base_table_exists = [this] (const view_ptr& view) {
        // This is a safety check in case this node missed a create MV statement
        // but got a drop table for the base, and another node didn't get the
        // drop notification and sent us the view schema.
        try {
            _db.find_schema(view->view_info()->base_id());
            return true;
        } catch (const replica::no_such_column_family&) {
            return false;
        }
    };
    std::unordered_set<utils::UUID> loaded_views;
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
        return base_table_exists(v) && !loaded_views.contains(v->id())
                && !vbi.built_views.contains(v->id());
    };
    for (auto&& view : all_views | boost::adaptors::filtered(is_new)) {
        vbi.bookkeeping_ops.push_back(add_new_view(view, get_or_create_build_step(view->view_info()->base_id())));
    }

    return parallel_for_each(_base_to_build_step, [this] (auto& p) {
        return initialize_reader_at_current_token(p.second);
    }).then([this, &vbi] {
        return seastar::when_all_succeed(vbi.bookkeeping_ops.begin(), vbi.bookkeeping_ops.end()).handle_exception([this] (std::exception_ptr ep) {
            vlogger.warn("Failed to update materialized view bookkeeping while synchronizing view builds on all shards ({}), continuing anyway.", ep);
        });
    });
}

future<std::unordered_map<sstring, sstring>>
view_builder::view_build_statuses(sstring keyspace, sstring view_name) const {
    return _sys_dist_ks.view_status(std::move(keyspace), std::move(view_name)).then([this] (std::unordered_map<utils::UUID, sstring> status) {
        auto& endpoint_to_host_id = service::get_local_storage_proxy().get_token_metadata_ptr()->get_endpoint_to_host_id_map_for_reading();
        return boost::copy_range<std::unordered_map<sstring, sstring>>(endpoint_to_host_id
                | boost::adaptors::transformed([&status] (const std::pair<gms::inet_address, utils::UUID>& p) {
                    auto it = status.find(p.second);
                    auto s = it != status.end() ? std::move(it->second) : "UNKNOWN";
                    return std::pair(p.first.to_sstring(), std::move(s));
                }));
    });
}

future<> view_builder::add_new_view(view_ptr view, build_step& step) {
    vlogger.info0("Building view {}.{}, starting at token {}", view->ks_name(), view->cf_name(), step.current_token());
    step.build_status.emplace(step.build_status.begin(), view_build_status{view, step.current_token(), std::nullopt});
    auto f = this_shard_id() == 0 ? _sys_dist_ks.start_view_build(view->ks_name(), view->cf_name()) : make_ready_future<>();
    return when_all_succeed(
            std::move(f),
            system_keyspace::register_view_for_building(view->ks_name(), view->cf_name(), step.current_token())).discard_result();
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
            return system_keyspace::remove_view_build_progress(ks_name, view_name);
        }
        return when_all_succeed(
                    system_keyspace::remove_view_build_progress(ks_name, view_name),
                    system_keyspace::remove_built_view(ks_name, view_name),
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
            auto units = get_units(_sem, 1).get0();
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
        vlogger.warn("Unexcepted error executing build step: {}. Ignored.", std::current_exception());
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
    build_step& _step;
    built_views _built_views;
    gc_clock::time_point _now;
    std::vector<view_ptr> _views_to_build;
    std::deque<mutation_fragment> _fragments;
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
    consumer(view_builder& builder, build_step& step, gc_clock::time_point now)
            : _builder(builder)
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
                if (partition_key_matches(*_step.reader.schema(), *vs.view->view_info(), _step.current_key)) {
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

    stop_iteration consume(static_row&&, tombstone, bool) {
        inject_failure("view_builder_consume_static_row");
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) {
        inject_failure("view_builder_consume_clustering_row");
        if (_views_to_build.empty() || _builder._as.abort_requested()) {
            return stop_iteration::yes;
        }

        _fragments_memory_usage += cr.memory_usage(*_step.reader.schema());
        _fragments.emplace_back(*_step.reader.schema(), _builder._permit, std::move(cr));
        if (_fragments_memory_usage > batch_memory_max) {
            // Although we have not yet completed the batch of base rows that
            // compact_for_query<> planned for us (view_builder::batchsize),
            // we've still collected enough rows to reach sizeable memory use,
            // so let's flush these rows now.
            flush_fragments();
        }
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&&) {
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
            auto reader = make_flat_mutation_reader_from_fragments(_step.reader.schema(), _builder._permit, std::move(_fragments));
            auto close_reader = defer([&reader] { reader.close().get(); });
            reader.upgrade_schema(base_schema);
            _step.base->populate_views(
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
    auto consumer = compact_for_query<emit_only_live_rows::yes, view_builder::consumer>(
            *step.reader.schema(),
            now,
            step.pslice,
            batch_size,
            query::max_partitions,
            view_builder::consumer{*this, step, now});
    consumer.consume_new_partition(step.current_key); // Initialize the state in case we're resuming a partition
    auto built = step.reader.consume_in_thread(std::move(consumer));

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
                    system_keyspace::update_view_build_progress(view->ks_name(), view->cf_name(), *next_token));
        }
    }
    seastar::when_all_succeed(bookkeeping_ops.begin(), bookkeeping_ops.end()).handle_exception([this] (std::exception_ptr ep) {
        vlogger.warn("Failed to update materialized view bookkeeping ({}), continuing anyway.", ep);
    }).get();
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
                return seastar::when_all_succeed(
                        system_keyspace::mark_view_as_built(view->ks_name(), view->cf_name()),
                        builder._sys_dist_ks.finish_view_build(view->ks_name(), view->cf_name())).then_unpack([view] {
                    // The view is built, so shard 0 can remove the entry in the build progress system table on
                    // behalf of all shards. It is guaranteed to have a higher timestamp than the per-shard entries.
                    return system_keyspace::remove_view_build_progress_across_all_shards(view->ks_name(), view->cf_name());
                }).then([&builder, view] {
                    auto it = builder._build_notifiers.find(std::pair(view->ks_name(), view->cf_name()));
                    if (it != builder._build_notifiers.end()) {
                        it->second.set_value();
                    }
                });
            });
        }
        return system_keyspace::update_view_build_progress(view->ks_name(), view->cf_name(), next_token);
    });
}

future<> view_builder::wait_until_built(const sstring& ks_name, const sstring& view_name) {
    return container().invoke_on(0, [ks_name, view_name] (view_builder& builder) {
        auto v = std::pair(std::move(ks_name), std::move(view_name));
        return builder._build_notifiers[std::move(v)].get_shared_future();
    });
}

update_backlog node_update_backlog::add_fetch(unsigned shard, update_backlog backlog) {
    _backlogs[shard].backlog.store(backlog, std::memory_order_relaxed);
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
    return std::max(backlog, _max.load(std::memory_order_relaxed));
}

future<bool> check_view_build_ongoing(db::system_distributed_keyspace& sys_dist_ks, const sstring& ks_name, const sstring& cf_name) {
    return sys_dist_ks.view_status(ks_name, cf_name).then([] (std::unordered_map<utils::UUID, sstring>&& view_statuses) {
        return boost::algorithm::any_of(view_statuses | boost::adaptors::map_values, [] (const sstring& view_status) {
            return view_status == "STARTED";
        });
    });
}

future<bool> check_needs_view_update_path(db::system_distributed_keyspace& sys_dist_ks, const replica::table& t, streaming::stream_reason reason) {
    if (is_internal_keyspace(t.schema()->ks_name())) {
        return make_ready_future<bool>(false);
    }
    if (reason == streaming::stream_reason::repair && !t.views().empty()) {
        return make_ready_future<bool>(true);
    }
    return do_with(t.views(), [&sys_dist_ks] (auto& views) {
        return map_reduce(views,
                [&sys_dist_ks] (const view_ptr& view) { return check_view_build_ongoing(sys_dist_ks, view->ks_name(), view->cf_name()); },
                false,
                std::logical_or<bool>());
    });
}

const size_t view_updating_consumer::buffer_size_soft_limit{1 * 1024 * 1024};
const size_t view_updating_consumer::buffer_size_hard_limit{2 * 1024 * 1024};

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
    _m = nullptr;
}

void view_updating_consumer::maybe_flush_buffer_mid_partition() {
    if (_buffer_size >= buffer_size_hard_limit) {
        auto m = mutation(_schema, _m->decorated_key(), mutation_partition(_schema));
        do_flush_buffer();
        _buffer.emplace_back(std::move(m));
        _m = &_buffer.back();
    }
}

view_updating_consumer::view_updating_consumer(schema_ptr schema, reader_permit permit, replica::table& table, std::vector<sstables::shared_sstable> excluded_sstables, const seastar::abort_source& as,
        evictable_reader_handle& staging_reader_handle)
    : view_updating_consumer(std::move(schema), std::move(permit), as, staging_reader_handle,
            [table = table.shared_from_this(), excluded_sstables = std::move(excluded_sstables)] (mutation m) mutable {
        auto s = m.schema();
        return table->stream_view_replica_updates(std::move(s), std::move(m), db::no_timeout, excluded_sstables);
    })
{ }

std::vector<db::view::view_and_base> with_base_info_snapshot(std::vector<view_ptr> vs) {
    return boost::copy_range<std::vector<db::view::view_and_base>>(vs | boost::adaptors::transformed([] (const view_ptr& v) {
        return db::view::view_and_base{v, v->view_info()->base_info()};
    }));
}

} // namespace view
} // namespace db
