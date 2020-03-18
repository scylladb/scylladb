/*
 * Copyright (C) 2019 ScyllaDB
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

#include <utility>
#include <algorithm>

#include <boost/range/irange.hpp>
#include <seastar/util/defer.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/metrics.hh>

#include "cdc/log.hh"
#include "cdc/generation.hh"
#include "cdc/split.hh"
#include "bytes.hh"
#include "database.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "partition_slice_builder.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "service/migration_listener.hh"
#include "service/storage_service.hh"
#include "types/tuple.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/multi_column_relation.hh"
#include "cql3/tuples.hh"
#include "cql3/untyped_result_set.hh"
#include "log.hh"
#include "json.hh"
#include "types.hh"
#include "concrete_types.hh"
#include "types/listlike_partial_deserializing_iterator.hh"
#include "tracing/trace_state.hh"
#include "stats.hh"

namespace std {

template<> struct hash<std::pair<net::inet_address, unsigned int>> {
    std::size_t operator()(const std::pair<net::inet_address, unsigned int> &p) const {
        return std::hash<net::inet_address>{}(p.first) ^ std::hash<int>{}(p.second);
    }
};

}

using namespace std::chrono_literals;

logging::logger cdc_log("cdc");

namespace cdc {
static schema_ptr create_log_schema(const schema&, std::optional<utils::UUID> = {});
}

static constexpr auto cdc_group_name = "cdc";

void cdc::stats::parts_touched_stats::register_metrics(seastar::metrics::metric_groups& metrics, std::string_view suffix) {
    namespace sm = seastar::metrics;
    auto register_part = [&] (part_type part, sstring part_name) {
        metrics.add_group(cdc_group_name, {
                sm::make_total_operations(format("operations_on_{}_performed_{}", part_name, suffix), count[(size_t)part],
                        sm::description(format("number of {} CDC operations that processed a {}", suffix, part_name)),
                        {})
            });
    };

    register_part(part_type::STATIC_ROW, "static_row");
    register_part(part_type::CLUSTERING_ROW, "clustering_row");
    register_part(part_type::MAP, "map");
    register_part(part_type::SET, "set");
    register_part(part_type::LIST, "list");
    register_part(part_type::UDT, "udt");
    register_part(part_type::RANGE_TOMBSTONE, "range_tombstone");
    register_part(part_type::PARTITION_DELETE, "partition_delete");
    register_part(part_type::ROW_DELETE, "row_delete");
}

cdc::stats::stats() {
    namespace sm = seastar::metrics;

    auto register_counters = [this] (counters& counters, sstring kind) {
        const auto split_label = sm::label("split");
        _metrics.add_group(cdc_group_name, {
                sm::make_total_operations("operations_" + kind, counters.unsplit_count,
                        sm::description(format("number of {} CDC operations", kind)),
                        {split_label(false)}),

                sm::make_total_operations("operations_" + kind, counters.split_count,
                        sm::description(format("number of {} CDC operations", kind)),
                        {split_label(true)}),

                sm::make_total_operations("preimage_selects_" + kind, counters.preimage_selects,
                        sm::description(format("number of {} preimage queries performed", kind)),
                        {}),

                sm::make_total_operations("operations_with_preimage_" + kind, counters.with_preimage_count,
                        sm::description(format("number of {} operations that included preimage", kind)),
                        {}),

                sm::make_total_operations("operations_with_postimage_" + kind, counters.with_postimage_count,
                        sm::description(format("number of {} operations that included postimage", kind)),
                        {})
            });

        counters.touches.register_metrics(_metrics, kind);
    };
    register_counters(counters_total, "total");
    register_counters(counters_failed, "failed");
}

cdc::operation_result_tracker::~operation_result_tracker() {
    auto update_stats = [this] (stats::counters& counters) {
        if (_details.was_split) {
            counters.split_count++;
        } else {
            counters.unsplit_count++;
        }
        counters.touches.apply(_details.touched_parts);
        if (_details.had_preimage) {
            counters.with_preimage_count++;
        }
        if (_details.had_postimage) {
            counters.with_postimage_count++;
        }
    };

    update_stats(_stats.counters_total);
    if (_failed) {
        update_stats(_stats.counters_failed);
    }
}

class cdc::cdc_service::impl : service::migration_listener::empty_listener {
    friend cdc_service;
    db_context _ctxt;
    bool _stopped = false;
public:
    impl(db_context ctxt)
        : _ctxt(std::move(ctxt))
    {
        _ctxt._migration_notifier.register_listener(this);
    }
    ~impl() {
        assert(_stopped);
    }

    future<> stop() {
        return _ctxt._migration_notifier.unregister_listener(this).then([this] {
            _stopped = true;
        });
    }

    void on_before_create_column_family(const schema& schema, std::vector<mutation>& mutations, api::timestamp_type timestamp) override {
        if (schema.cdc_options().enabled()) {
            auto& db = _ctxt._proxy.get_db().local();
            auto logname = log_name(schema.cf_name());
            check_that_cdc_log_table_does_not_exist(db, schema, logname);

            // in seastar thread
            auto log_schema = create_log_schema(schema);
            auto& keyspace = db.find_keyspace(schema.ks_name());

            auto log_mut = db::schema_tables::make_create_table_mutations(keyspace.metadata(), log_schema, timestamp);

            mutations.insert(mutations.end(), std::make_move_iterator(log_mut.begin()), std::make_move_iterator(log_mut.end()));
        }
    }

    void on_before_update_column_family(const schema& new_schema, const schema& old_schema, std::vector<mutation>& mutations, api::timestamp_type timestamp) override {
        bool is_cdc = new_schema.cdc_options().enabled();
        bool was_cdc = old_schema.cdc_options().enabled();

        // we need to create or modify the log & stream schemas iff either we changed cdc status (was != is)
        // or if cdc is on now unconditionally, since then any actual base schema changes will affect the column 
        // etc.
        if (was_cdc || is_cdc) {
            auto& db = _ctxt._proxy.get_db().local();

            if (!was_cdc) {
                check_that_cdc_log_table_does_not_exist(db, new_schema, log_name(new_schema.cf_name()));
            }
            if (is_cdc) {
                check_for_attempt_to_create_nested_cdc_log(new_schema);
            }

            auto logname = log_name(old_schema.cf_name());
            auto& keyspace = db.find_keyspace(old_schema.ks_name());
            auto log_schema = was_cdc ? db.find_column_family(old_schema.ks_name(), logname).schema() : nullptr;

            if (!is_cdc) {
                auto log_mut = db::schema_tables::make_drop_table_mutations(keyspace.metadata(), log_schema, timestamp);

                mutations.insert(mutations.end(), std::make_move_iterator(log_mut.begin()), std::make_move_iterator(log_mut.end()));
                return;
            }

            auto new_log_schema = create_log_schema(new_schema, log_schema ? std::make_optional(log_schema->id()) : std::nullopt);

            auto log_mut = log_schema 
                ? db::schema_tables::make_update_table_mutations(keyspace.metadata(), log_schema, new_log_schema, timestamp, false)
                : db::schema_tables::make_create_table_mutations(keyspace.metadata(), new_log_schema, timestamp)
                ;

            mutations.insert(mutations.end(), std::make_move_iterator(log_mut.begin()), std::make_move_iterator(log_mut.end()));
        }
    }

    void on_before_drop_column_family(const schema& schema, std::vector<mutation>& mutations, api::timestamp_type timestamp) override {
        if (schema.cdc_options().enabled()) {
            auto logname = log_name(schema.cf_name());
            auto& db = _ctxt._proxy.get_db().local();
            auto& keyspace = db.find_keyspace(schema.ks_name());
            auto log_schema = db.find_column_family(schema.ks_name(), logname).schema();

            auto log_mut = db::schema_tables::make_drop_table_mutations(keyspace.metadata(), log_schema, timestamp);

            mutations.insert(mutations.end(), std::make_move_iterator(log_mut.begin()), std::make_move_iterator(log_mut.end()));
        }
    }

    future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>> augment_mutation_call(
        lowres_clock::time_point timeout,
        std::vector<mutation>&& mutations,
        tracing::trace_state_ptr tr_state
    );

    template<typename Iter>
    future<> append_mutations(Iter i, Iter e, schema_ptr s, lowres_clock::time_point, std::vector<mutation>&);

private:
    static void check_for_attempt_to_create_nested_cdc_log(const schema& schema) {
        const auto& cf_name = schema.cf_name();
        const auto cf_name_view = std::string_view(cf_name.data(), cf_name.size());
        if (is_log_for_some_table(schema.ks_name(), cf_name_view)) {
            throw exceptions::invalid_request_exception(format("Cannot create a CDC log for a table {}.{}, because creating nested CDC logs is not allowed",
                    schema.ks_name(), schema.cf_name()));
        }
    }

    static void check_that_cdc_log_table_does_not_exist(database& db, const schema& schema, const sstring& logname) {
        if (db.has_schema(schema.ks_name(), logname)) {
            throw exceptions::invalid_request_exception(format("Cannot create CDC log table for table {}.{} because a table of name {}.{} already exists",
                    schema.ks_name(), schema.cf_name(),
                    schema.ks_name(), logname));
        }
    }
};

cdc::cdc_service::cdc_service(service::storage_proxy& proxy)
    : cdc_service(db_context::builder(proxy).build())
{}

cdc::cdc_service::cdc_service(db_context ctxt)
    : _impl(std::make_unique<impl>(std::move(ctxt)))
{
    _impl->_ctxt._proxy.set_cdc_service(this);
}

future<> cdc::cdc_service::stop() {
    return _impl->stop();
}

cdc::cdc_service::~cdc_service() = default;

cdc::options::options(const std::map<sstring, sstring>& map) {
    if (map.find("enabled") == std::end(map)) {
        return;
    }

    for (auto& p : map) {
        if (p.first == "enabled") {
            _enabled = p.second == "true";
        } else if (p.first == "preimage") {
            _preimage = p.second == "true";
        } else if (p.first == "postimage") {
            _postimage = p.second == "true";
        } else if (p.first == "ttl") {
            _ttl = std::stoi(p.second);
            if (_ttl < 0) {
                throw exceptions::configuration_exception("Invalid CDC option: ttl must be >= 0");
            }
        } else {
            throw exceptions::configuration_exception("Invalid CDC option: " + p.first);
        }
    }
}

std::map<sstring, sstring> cdc::options::to_map() const {
    if (!_enabled) {
        return {};
    }
    return {
        { "enabled", _enabled ? "true" : "false" },
        { "preimage", _preimage ? "true" : "false" },
        { "postimage", _postimage ? "true" : "false" },
        { "ttl", std::to_string(_ttl) },
    };
}

sstring cdc::options::to_sstring() const {
    return json::to_json(to_map());
}

bool cdc::options::operator==(const options& o) const {
    return _enabled == o._enabled && _preimage == o._preimage && _postimage == o._postimage && _ttl == o._ttl;
}
bool cdc::options::operator!=(const options& o) const {
    return !(*this == o);
}

namespace cdc {

using operation_native_type = std::underlying_type_t<operation>;

static const sstring cdc_log_suffix = "_scylla_cdc_log";
static const sstring cdc_meta_column_prefix = "cdc$";
static const sstring cdc_deleted_column_prefix = cdc_meta_column_prefix + "deleted_";
static const sstring cdc_deleted_elements_column_prefix = cdc_meta_column_prefix + "deleted_elements_";

static bool is_log_name(const std::string_view& table_name) {
    return boost::ends_with(table_name, cdc_log_suffix);
}

bool is_log_for_some_table(const sstring& ks_name, const std::string_view& table_name) {
    if (!is_log_name(table_name)) {
        return false;
    }
    const auto base_name = sstring(table_name.data(), table_name.size() - cdc_log_suffix.size());
    const auto& local_db = service::get_local_storage_proxy().get_db().local();
    if (!local_db.has_schema(ks_name, base_name)) {
        return false;
    }
    const auto base_schema = local_db.find_schema(ks_name, base_name);
    return base_schema->cdc_options().enabled();
}

sstring log_name(const sstring& table_name) {
    return table_name + cdc_log_suffix;
}

sstring log_data_column_name(std::string_view column_name) {
    return sstring(column_name);
}

seastar::sstring log_meta_column_name(std::string_view column_name) {
    return cdc_meta_column_prefix + sstring(column_name);
}

bytes log_data_column_name_bytes(const bytes& column_name) {
    return column_name;
}

bytes log_meta_column_name_bytes(const bytes& column_name) {
    return to_bytes(cdc_meta_column_prefix) + column_name;
}

seastar::sstring log_data_column_deleted_name(std::string_view column_name) {
    return cdc_deleted_column_prefix + sstring(column_name);
}

bytes log_data_column_deleted_name_bytes(const bytes& column_name) {
    return to_bytes(cdc_deleted_column_prefix) + column_name;
}

seastar::sstring log_data_column_deleted_elements_name(std::string_view column_name) {
    return cdc_deleted_elements_column_prefix + sstring(column_name);
}

bytes log_data_column_deleted_elements_name_bytes(const bytes& column_name) {
    return to_bytes(cdc_deleted_elements_column_prefix) + column_name;
}

static schema_ptr create_log_schema(const schema& s, std::optional<utils::UUID> uuid) {
    schema_builder b(s.ks_name(), log_name(s.cf_name()));
    b.set_comment(sprint("CDC log for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column(log_meta_column_name_bytes("stream_id"), bytes_type, column_kind::partition_key);
    b.with_column(log_meta_column_name_bytes("time"), timeuuid_type, column_kind::clustering_key);
    b.with_column(log_meta_column_name_bytes("batch_seq_no"), int32_type, column_kind::clustering_key);
    b.with_column(log_meta_column_name_bytes("operation"), data_type_for<operation_native_type>());
    b.with_column(log_meta_column_name_bytes("ttl"), long_type);
    auto add_columns = [&] (const schema::const_iterator_range_type& columns, bool is_data_col = false) {
        for (const auto& column : columns) {
            auto type = column.type;
            if (is_data_col) {
                type = visit(*type, make_visitor(
                    // lists are represented as map<timeuuid, value_type>. Otherwise we cannot express delta
                    [] (const list_type_impl& type) -> data_type {
                        return map_type_impl::get_instance(type.name_comparator(), type.value_comparator(), false);
                    },
                    // everything else is just frozen self
                    [] (const abstract_type& type) {
                        return type.freeze();
                    }
                ));
                type = type->freeze();
            }
            b.with_column(log_data_column_name_bytes(column.name()), type);
            if (is_data_col) {
                b.with_column(log_data_column_deleted_name_bytes(column.name()), boolean_type);
            }
            if (column.type->is_multi_cell()) {
                auto dtype = visit(*type, make_visitor(
                    // all collection deletes are set<key_type> (i.e. timeuuid for lists)
                    [] (const collection_type_impl& type) -> data_type {
                        return set_type_impl::get_instance(type.name_comparator(), false);
                    },
                    // user types deletes are a set of the indices removed
                    [] (const user_type_impl& type) -> data_type {
                        return set_type_impl::get_instance(short_type, false);
                    },
                    [] (const abstract_type& type) -> data_type {
                        throw std::invalid_argument("Should not reach");
                    }
                ));
                b.with_column(log_data_column_deleted_elements_name_bytes(column.name()), dtype);
            }
        }
    };
    add_columns(s.partition_key_columns());
    add_columns(s.clustering_key_columns());
    add_columns(s.static_columns(), true);
    add_columns(s.regular_columns(), true);

    if (uuid) {
        b.set_uuid(*uuid);
    }
    
    return b.build();
}

db_context::builder::builder(service::storage_proxy& proxy) 
    : _proxy(proxy) 
{}

db_context::builder& db_context::builder::with_migration_notifier(service::migration_notifier& migration_notifier) {
    _migration_notifier = migration_notifier;
    return *this;
}

db_context::builder& db_context::builder::with_token_metadata(locator::token_metadata& token_metadata) {
    _token_metadata = token_metadata;
    return *this;
}

db_context::builder& db_context::builder::with_cdc_metadata(cdc::metadata& cdc_metadata) {
    _cdc_metadata = cdc_metadata;
    return *this;
}

db_context db_context::builder::build() {
    return db_context{
        _proxy,
        _migration_notifier ? _migration_notifier->get() : service::get_local_storage_service().get_migration_notifier(),
        _token_metadata ? _token_metadata->get() : service::get_local_storage_service().get_token_metadata(),
        _cdc_metadata ? _cdc_metadata->get() : service::get_local_storage_service().get_cdc_metadata(),
    };
}

/* Find some timestamp inside the given mutation.
 *
 * If this mutation was created using a single insert/update/delete statement, then it will have a single,
 * well-defined timestamp (even if this timestamp occurs multiple times, e.g. in a cell and row_marker).
 *
 * This function shouldn't be used for mutations that have multiple different timestamps: the function
 * would only find one of them. When dealing with such mutations, the caller should first split the mutation
 * into multiple ones, each with a single timestamp.
 */
// TODO: We need to
// - in the code that calls `augument_mutation_call`, or inside `augument_mutation_call`,
//   split each mutation to a set of mutations, each with a single timestamp.
// - optionally: here, throw error if multiple timestamps are encountered (may degrade performance).
// external linkage for testing
api::timestamp_type find_timestamp(const schema& s, const mutation& m) {
    auto& p = m.partition();
    api::timestamp_type t = api::missing_timestamp;

    t = p.partition_tombstone().timestamp;
    if (t != api::missing_timestamp) {
        return t;
    }

    for (auto& rt: p.row_tombstones()) {
        t = rt.tomb.timestamp;
        if (t != api::missing_timestamp) {
            return t;
        }
    }

    auto walk_row = [&t, &s] (const row& r, column_kind ckind) {
        r.for_each_cell_until([&t, &s, ckind] (column_id id, const atomic_cell_or_collection& cell) {
            auto& cdef = s.column_at(ckind, id);

            if (cdef.is_atomic()) {
                t = cell.as_atomic_cell(cdef).timestamp();
                if (t != api::missing_timestamp) {
                    return stop_iteration::yes;
                }
                return stop_iteration::no;
            }

            return cell.as_collection_mutation().with_deserialized(*cdef.type,
                    [&] (collection_mutation_view_description mview) {
                t = mview.tomb.timestamp;
                if (t != api::missing_timestamp) {
                    return stop_iteration::yes;
                }

                for (auto& kv : mview.cells) {
                    t = kv.second.timestamp();
                    if (t != api::missing_timestamp) {
                        return stop_iteration::yes;
                    }
                }

                return stop_iteration::no;
            });
        });
    };

    walk_row(p.static_row().get(), column_kind::static_column);
    if (t != api::missing_timestamp) {
        return t;
    }

    for (const rows_entry& cr : p.clustered_rows()) {
        const deletable_row& r = cr.row();

        t = r.deleted_at().regular().timestamp;
        if (t != api::missing_timestamp) {
            return t;
        }

        t = r.deleted_at().shadowable().tomb().timestamp;
        if (t != api::missing_timestamp) {
            return t;
        }

        t = r.created_at();
        if (t != api::missing_timestamp) {
            return t;
        }

        walk_row(r.cells(), column_kind::regular_column);
        if (t != api::missing_timestamp) {
            return t;
        }
    }

    throw std::runtime_error("cdc: could not find timestamp of mutation");
}

// iterators for collection merge
template<typename T>
class collection_iterator : public std::iterator<std::input_iterator_tag, const T> {
    bytes_view _v, _next;
    size_t _rem = 0;
    T _current;
public:
    collection_iterator(bytes_view_opt v = {}) 
        : _v(v.value_or(bytes_view{}))
        , _rem(_v.empty() ? 0 : read_collection_size(_v, cql_serialization_format::internal()))
    {
        if (_rem != 0) {
            parse();
        }
    }
    collection_iterator(const collection_iterator&) = default;
    const T& operator*() const {
        return _current;
    }
    const T* operator->() const {
        return &_current;
    }
    collection_iterator& operator++() {
        next();
        if (_rem != 0) {
            parse();
        } else {
            _current = {};
        }
        return *this;
    }
    collection_iterator operator++(int) {
        auto v = *this;
        ++(*this);
        return v;
    }
    bool operator==(const collection_iterator& x) const {
        return _v == x._v;
    }
    bool operator!=(const collection_iterator& x) const {
        return !(*this == x);
    }
private:
    void next() {
        --_rem;
        _v = _next;
    }
    void parse();
};

template<>
void collection_iterator<std::pair<bytes_view, bytes_view>>::parse() {
    assert(_rem > 0);
    _next = _v;
    auto k = read_collection_value(_next, cql_serialization_format::internal());
    auto v = read_collection_value(_next, cql_serialization_format::internal());
    _current = std::make_pair(k, v);
}

template<>
void collection_iterator<bytes_view>::parse() {
    assert(_rem > 0);
    _next = _v;
    auto k = read_collection_value(_next, cql_serialization_format::internal());
    _current = k;
}

template<typename Container, typename T>
class maybe_back_insert_iterator : public std::back_insert_iterator<Container> {
    const abstract_type& _type;
    collection_iterator<T> _s, _e;
public:
    using value_type = typename Container::value_type;
    maybe_back_insert_iterator(Container& c, const abstract_type& type, collection_iterator<T> s)
        : std::back_insert_iterator<Container>(c)
        , _type(type)
        , _s(s)
    {}
    maybe_back_insert_iterator& operator*() {
        return *this;
    }
    maybe_back_insert_iterator& operator=(const value_type& v) {
        if (!find(v)) {
            std::back_insert_iterator<Container>::operator=(v);
        }
        return *this;
    }
    maybe_back_insert_iterator& operator=(value_type&& v) {
        if (!find(v)) {
            std::back_insert_iterator<Container>::operator=(std::move(v));
        }
        return *this;
    }
private:
    bool find(const value_type& v) {
       // cheating - reducing search span, because we know we only append unique values (see below).
       while (_s != _e) {
           auto n = compare(*_s, v);
           if (n <= 0) {
               ++_s;
           }
           if (n == 0) {
               return true;
           }
           if (n > 0) {
               break;
           }
       }
       return false;
    }
    bool compare(const T&, const value_type& v);
};

template<>
bool maybe_back_insert_iterator<std::vector<std::pair<bytes_view, bytes_view>>, bytes_view>::compare(const bytes_view& t, const value_type& v) {
    return _type.compare(t, v.first);
}

template<>
bool maybe_back_insert_iterator<std::vector<bytes_view>, bytes_view>::compare(const bytes_view& t, const value_type& v) {
    return _type.compare(t, v);
}

template<typename Container, typename T>
auto make_maybe_back_inserter(Container& c, const abstract_type& type, collection_iterator<T> s) {
    return maybe_back_insert_iterator<Container, T>(c, type, s);
}

/* Given a timestamp, generates a timeuuid with the following properties:
 * 1. `t1` < `t2` implies timeuuid_type->less(timeuuid_type->decompose(generate_timeuuid(`t1`)),
 *                                            timeuuid_type->decompose(generate_timeuuid(`t2`))),
 * 2. utils::UUID_gen::micros_timestamp(generate_timeuuid(`t`)) == `t`.
 *
 * If `t1` == `t2`, then generate_timeuuid(`t1`) != generate_timeuuid(`t2`),
 * with unspecified nondeterministic ordering.
 */
utils::UUID generate_timeuuid(api::timestamp_type t) {
    return utils::UUID_gen::get_random_time_UUID_from_micros(t);
}

class transformer final {
private:
    db_context _ctx;
    schema_ptr _schema;
    schema_ptr _log_schema;
    const column_definition& _op_col;
    const column_definition& _ttl_col;
    ttl_opt _cdc_ttl_opt;

    clustering_key set_pk_columns(const partition_key& pk, api::timestamp_type ts, bytes decomposed_tuuid, int batch_no, mutation& m) const {
        const auto log_ck = clustering_key::from_exploded(
                *m.schema(), { decomposed_tuuid, int32_type->decompose(batch_no) });
        auto pk_value = pk.explode(*_schema);
        size_t pos = 0;
        for (const auto& column : _schema->partition_key_columns()) {
            assert (pos < pk_value.size());
            auto cdef = m.schema()->get_column_definition(log_data_column_name_bytes(column.name()));
            auto value = atomic_cell::make_live(*column.type,
                                                ts,
                                                bytes_view(pk_value[pos]),
                                                _cdc_ttl_opt);
            m.set_cell(log_ck, *cdef, std::move(value));
            ++pos;
        }
        return log_ck;
    }

    void set_operation(const clustering_key& ck, api::timestamp_type ts, operation op, mutation& m) const {
        m.set_cell(ck, _op_col, atomic_cell::make_live(*_op_col.type, ts, _op_col.type->decompose(operation_native_type(op)), _cdc_ttl_opt));
    }

    void set_ttl(const clustering_key& ck, api::timestamp_type ts, gc_clock::duration ttl, mutation& m) const {
        m.set_cell(ck, _ttl_col, atomic_cell::make_live(*_ttl_col.type, ts, _ttl_col.type->decompose(ttl.count()), _cdc_ttl_opt));
    }

public:
    transformer(db_context ctx, schema_ptr s)
        : _ctx(ctx)
        , _schema(std::move(s))
        , _log_schema(ctx._proxy.get_db().local().find_schema(_schema->ks_name(), log_name(_schema->cf_name())))
        , _op_col(*_log_schema->get_column_definition(log_meta_column_name_bytes("operation")))
        , _ttl_col(*_log_schema->get_column_definition(log_meta_column_name_bytes("ttl")))
    {
        if (_schema->cdc_options().ttl()) {
            _cdc_ttl_opt = std::chrono::seconds(_schema->cdc_options().ttl());
        }
    }
    static size_t collection_size(const bytes_opt& bo) {
        if (bo) {
            bytes_view bv(*bo);
            return read_collection_size(bv, cql_serialization_format::internal());
        }
        return 0;
    }
    template<typename Func>
    static void udt_for_each(const bytes_opt& bo, Func&& f) {
        if (bo) {
            bytes_view bv(*bo);
            std::for_each(tuple_deserializing_iterator::start(bv), tuple_deserializing_iterator::finish(bv), std::forward<Func>(f));
        }
    }
    static bytes merge(const collection_type_impl& ctype, const bytes_opt& prev, const bytes_opt& next, const bytes_opt& deleted) {
        std::vector<std::pair<bytes_view, bytes_view>> res;
        res.reserve(collection_size(prev) + collection_size(next));
        auto type = ctype.name_comparator();
        auto cmp = [&type = *type](const std::pair<bytes_view, bytes_view>& p1, const std::pair<bytes_view, bytes_view>& p2) {
            return type.compare(p1.first, p2.first) < 0;
        };
        collection_iterator<std::pair<bytes_view, bytes_view>> e, i(prev), j(next);
        // note order: set_union, when finding doubles, use value from first1 (j here). So 
        // since this is next, it has prio
        std::set_union(j, e, i, e, make_maybe_back_inserter(res, *type, collection_iterator<bytes_view>(deleted)), cmp);
        return map_type_impl::serialize_partially_deserialized_form(res, cql_serialization_format::internal());
    }
    static bytes merge(const set_type_impl& ctype, const bytes_opt& prev, const bytes_opt& next, const bytes_opt& deleted) {
        std::vector<bytes_view> res;
        res.reserve(collection_size(prev) + collection_size(next));
        auto type = ctype.name_comparator();
        auto cmp = [&type = *type](bytes_view k1, bytes_view k2) {
            return type.compare(k1, k2) < 0;
        };
        collection_iterator<bytes_view> e, i(prev), j(next), d(deleted);
        std::set_union(j, e, i, e, make_maybe_back_inserter(res, *type, d), cmp);
        return set_type_impl::serialize_partially_deserialized_form(res, cql_serialization_format::internal());
    }
    static bytes merge(const user_type_impl& type, const bytes_opt& prev, const bytes_opt& next, const bytes_opt& deleted) {
        std::vector<bytes_view_opt> res(type.size());
        udt_for_each(prev, [&res, i = res.begin()](bytes_view_opt k) mutable {
            *i++ = k;
        });
        udt_for_each(next, [&res, i = res.begin()](bytes_view_opt k) mutable {
            if (k) {
                *i = k;
            }
            ++i;
        });
        collection_iterator<bytes_view> e, d(deleted);
        std::for_each(d, e, [&res](bytes_view k) {
            auto index = deserialize_field_index(k);
            res[index] = std::nullopt;
        });
        return type.build_value(res);
    }
    static bytes merge(const abstract_type& type, const bytes_opt& prev, const bytes_opt& next, const bytes_opt& deleted) {
        throw std::runtime_error(format("cdc merge: unknown type {}", type.name()));
    }

    // TODO: is pre-image data based on query enough. We only have actual column data. Do we need
    // more details like tombstones/ttl? Probably not but keep in mind.
    std::tuple<mutation, stats::part_type_set> transform(const mutation& m, const cql3::untyped_result_set* rs, api::timestamp_type ts, bytes tuuid, int& batch_no) const {
        auto stream_id = _ctx._cdc_metadata.get_stream(ts, m.token());
        mutation res(_log_schema, stream_id.to_partition_key(*_log_schema));
        const auto postimage = _schema->cdc_options().postimage();
        stats::part_type_set touched_parts;
        auto& p = m.partition();
        if (p.partition_tombstone()) {
            // Partition deletion
            touched_parts.set<stats::part_type::PARTITION_DELETE>();
            auto log_ck = set_pk_columns(m.key(), ts, tuuid, 0, res);
            set_operation(log_ck, ts, operation::partition_delete, res);
            ++batch_no;
        } else if (!p.row_tombstones().empty()) {
            // range deletion
            touched_parts.set<stats::part_type::RANGE_TOMBSTONE>();
            for (auto& rt : p.row_tombstones()) {
                auto set_bound = [&] (const clustering_key& log_ck, const clustering_key_prefix& ckp) {
                    auto exploded = ckp.explode(*_schema);
                    size_t pos = 0;
                    for (const auto& column : _schema->clustering_key_columns()) {
                        if (pos >= exploded.size()) {
                            break;
                        }
                        auto cdef = _log_schema->get_column_definition(log_data_column_name_bytes(column.name()));
                        auto value = atomic_cell::make_live(*column.type,
                                                            ts,
                                                            bytes_view(exploded[pos]),
                                                            _cdc_ttl_opt);
                        res.set_cell(log_ck, *cdef, std::move(value));
                        ++pos;
                    }
                };
                {
                    auto log_ck = set_pk_columns(m.key(), ts, tuuid, batch_no, res);
                    set_bound(log_ck, rt.start);
                    const auto start_operation = rt.start_kind == bound_kind::incl_start
                            ? operation::range_delete_start_inclusive
                            : operation::range_delete_start_exclusive;
                    set_operation(log_ck, ts, start_operation, res);
                    ++batch_no;
                }
                {
                    auto log_ck = set_pk_columns(m.key(), ts, tuuid, batch_no, res);
                    set_bound(log_ck, rt.end);
                    const auto end_operation = rt.end_kind == bound_kind::incl_end
                            ? operation::range_delete_end_inclusive
                            : operation::range_delete_end_exclusive;
                    set_operation(log_ck, ts, end_operation, res);
                    ++batch_no;
                }
            }
        } else {
            // should be insert, update or deletion
            auto process_cells = [&](const row& r, column_kind ckind, const clustering_key& log_ck, std::optional<clustering_key> pikey, const cql3::untyped_result_set_row* pirow, std::optional<clustering_key> poikey) -> std::optional<gc_clock::duration> {
                if (postimage && !poikey) {
                    poikey = set_pk_columns(m.key(), ts, tuuid, ++batch_no, res);
                    set_operation(*poikey, ts, operation::post_image, res);
                }
                std::optional<gc_clock::duration> ttl;
                std::unordered_set<column_id> columns_assigned;
                r.for_each_cell([&](column_id id, const atomic_cell_or_collection& cell) {
                    auto& cdef = _schema->column_at(ckind, id);
                    auto* dst = _log_schema->get_column_definition(log_data_column_name_bytes(cdef.name()));
                    auto has_pirow = pirow && pirow->has(cdef.name_as_text());
                    bool is_column_delete = true;
                    bytes_opt value;
                    bytes_opt deleted_elements = std::nullopt;
                    if (cdef.is_atomic()) {
                        value = std::nullopt;
                        auto view = cell.as_atomic_cell(cdef);
                        if (view.is_live()) {
                            is_column_delete = false;
                            value = view.value().linearize();
                            if (view.is_live_and_has_ttl()) {
                                ttl = view.ttl();
                            }
                        }
                    } else {
                        auto mv = cell.as_collection_mutation();
                        is_column_delete = false;
                        std::vector<bytes> buf;
                        value = mv.with_deserialized(*cdef.type, [&](collection_mutation_view_description view) -> bytes_opt {
                            if (view.tomb) {
                                // there is a tombstone with timestamp before this mutation.
                                // this is how a assign collection = <value> is represented.
                                // for non-atomics, a column delete + values in data column
                                // simply means "replace values"
                                is_column_delete = true;
                            }
                            auto process_collection = [&](auto value_callback) {
                                for (auto& [key, value] : view.cells) {
                                    // note: we are assuming that all mutations coming here adhere to
                                    // / are created by the cql machinery or similar, i.e. if we have
                                    // the tombstone above, it preceeds the actual cells, and is in
                                    // fact an "assign" marker. So we only check for explicitly
                                    // dead cells, i.e. null markers.
                                    auto live = value.is_live();
                                    if (!live) {
                                        value_callback(key, bytes_view{}, live);
                                        continue;
                                    }
                                    auto val = value.value().is_fragmented()
                                        ? bytes_view{buf.emplace_back(value.value().linearize())}
                                        : value.value().first_fragment()
                                        ;
                                    value_callback(key, val, live);
                                }
                            };

                            std::vector<bytes_view> deleted;

                            return visit(*cdef.type, make_visitor(
                                // maps and lists are just flattened
                                [&] (const collection_type_impl& type) -> bytes_opt {
                                    touched_parts.set(type.is_list() ? stats::part_type::LIST : stats::part_type::MAP);
                                    std::vector<std::pair<bytes_view, bytes_view>> result;
                                    process_collection([&](const bytes_view& key, const bytes_view& value, bool live) {
                                        if (live) {
                                            result.emplace_back(key, value);
                                        } else {
                                            deleted.emplace_back(key);
                                        }
                                    });
                                    if (!deleted.empty()) {
                                        deleted_elements = set_type_impl::serialize_partially_deserialized_form(deleted, cql_serialization_format::internal());
                                    }
                                    if (result.empty()) {
                                        return std::nullopt;
                                    }
                                    return map_type_impl::serialize_partially_deserialized_form(result, cql_serialization_format::internal());
                                },
                                // set need to transform from mutation view
                                [&] (const set_type_impl& type) -> bytes_opt  {
                                    touched_parts.set<stats::part_type::SET>();
                                    std::vector<bytes_view> result;
                                    process_collection([&](const bytes_view& key, const bytes_view& value, bool live) {
                                        if (live) {
                                            result.emplace_back(key);
                                        } else {
                                            deleted.emplace_back(key);
                                        }
                                    });
                                    if (!deleted.empty()) {
                                        deleted_elements = set_type_impl::serialize_partially_deserialized_form(deleted, cql_serialization_format::internal());
                                    }
                                    if (result.empty()) {
                                        return std::nullopt;
                                    }
                                    return set_type_impl::serialize_partially_deserialized_form(result, cql_serialization_format::internal());
                                },
                                // for user type we collect the fields in the mutation and set to
                                // tuple of value or tuple of null in case of delete.
                                // fields not in the mutation are null in the enclosing tuple, signifying "no info"
                                [&](const user_type_impl& type) -> bytes_opt  {
                                    touched_parts.set<stats::part_type::UDT>();
                                    std::vector<bytes_opt> result(type.size());
                                    process_collection([&](const bytes_view& key, const bytes_view& value, bool live) {
                                        if (live) {
                                            auto idx = deserialize_field_index(key);
                                            result[idx].emplace(value);
                                        } else {
                                            deleted.emplace_back(key);
                                        }
                                    });
                                    if (!deleted.empty()) {
                                        deleted_elements = set_type_impl::serialize_partially_deserialized_form(deleted, cql_serialization_format::internal());
                                    }
                                    if (result.empty()) {
                                        return std::nullopt;
                                    }
                                    return type.build_value(result);
                                },
                                [&] (const abstract_type& o) -> bytes_opt {
                                    throw std::runtime_error(format("cdc transform: unknown type {}", o.name()));
                                }
                            ));
                        });

                        if (deleted_elements) {
                            auto* dc = _log_schema->get_column_definition(log_data_column_deleted_elements_name_bytes(cdef.name()));
                            res.set_cell(log_ck, *dc, atomic_cell::make_live(*dc->type, ts, *deleted_elements, _cdc_ttl_opt));
                        }
                    }

                    if (is_column_delete) {
                        res.set_cell(log_ck, log_data_column_deleted_name_bytes(cdef.name()), data_value(true), ts, _cdc_ttl_opt);
                    }
                    if (value) {
                        res.set_cell(log_ck, *dst, atomic_cell::make_live(*dst->type, ts, *value, _cdc_ttl_opt));
                    }

                    bytes_opt prev;

                    if (has_pirow) {
                        prev = get_preimage_col_value(cdef, pirow);
                        assert(std::addressof(res.partition().clustered_row(*_log_schema, *pikey)) != std::addressof(res.partition().clustered_row(*_log_schema, log_ck)));
                        assert(pikey->explode() != log_ck.explode());
                        res.set_cell(*pikey, *dst, atomic_cell::make_live(*dst->type, ts, *prev, _cdc_ttl_opt));
                    }

                    if (postimage) {
                        // keep track of actually assigning this already
                        columns_assigned.emplace(id);
                        // don't merge with pre-image iff column delete
                        if (is_column_delete) {
                            prev = std::nullopt;
                        }
                        if (cdef.is_atomic() && !is_column_delete && value) {
                            res.set_cell(*poikey, *dst, atomic_cell::make_live(*dst->type, ts, *value, _cdc_ttl_opt));
                        } else if (!cdef.is_atomic() && (value || (deleted_elements && prev))) {
                            auto v = visit(*cdef.type, [&] (const auto& type) -> bytes {
                                return merge(type, prev, value, deleted_elements);
                            });
                            res.set_cell(*poikey, *dst, atomic_cell::make_live(*dst->type, ts, v, _cdc_ttl_opt));
                        }
                    }
                });

                // fill in all columns not already processed. Note that column nulls are also marked.
                if (postimage && pirow) {
                    for (auto& cdef : _schema->columns(ckind)) {
                        if (!columns_assigned.count(cdef.id)) {
                            auto v = pirow->get_view_opt(cdef.name_as_text());
                            if (v) {
                                auto dst = _log_schema->get_column_definition(log_data_column_name_bytes(cdef.name()));
                                res.set_cell(*poikey, *dst, atomic_cell::make_live(*dst->type, ts, *v, _cdc_ttl_opt));
                            }
                        }
                    }
                }

                return ttl;
            };

            if (!p.static_row().empty()) {
                touched_parts.set<stats::part_type::STATIC_ROW>();
                std::optional<clustering_key> pikey, poikey;
                const cql3::untyped_result_set_row * pirow = nullptr;

                if (rs && !rs->empty()) {
                    // For static rows, only one row from the result set is needed
                    pikey = set_pk_columns(m.key(), ts, tuuid, batch_no, res);
                    set_operation(*pikey, ts, operation::pre_image, res);
                    pirow = &rs->front();
                    ++batch_no;
                }

                auto log_ck = set_pk_columns(m.key(), ts, tuuid, batch_no, res);

                if (postimage) {
                     poikey = set_pk_columns(m.key(), ts, tuuid, ++batch_no, res);
                     set_operation(*poikey, ts, operation::post_image, res);
                }

                auto ttl = process_cells(p.static_row().get(), column_kind::static_column, log_ck, pikey, pirow, poikey);

                set_operation(log_ck, ts, operation::update, res);

                if (ttl) {
                    set_ttl(log_ck, ts, *ttl, res);
                }
                ++batch_no;
            } else {
                touched_parts.set_if<stats::part_type::CLUSTERING_ROW>(!p.clustered_rows().empty());
                for (const rows_entry& r : p.clustered_rows()) {
                    auto ck_value = r.key().explode(*_schema);

                    std::optional<clustering_key> pikey, poikey;
                    const cql3::untyped_result_set_row * pirow = nullptr;

                    if (rs) {
                        for (auto& utr : *rs) {
                            bool match = true;
                            for (auto& c : _schema->clustering_key_columns()) {
                                auto rv = utr.get_view(c.name_as_text());
                                auto cv = r.key().get_component(*_schema, c.component_index());
                                if (rv != cv) {
                                    match = false;
                                    break;
                                }
                            }
                            if (match) {
                                pikey = set_pk_columns(m.key(), ts, tuuid, batch_no, res);
                                set_operation(*pikey, ts, operation::pre_image, res);
                                pirow = &utr;
                                ++batch_no;
                                break;
                            }
                        }
                    }

                    auto log_ck = set_pk_columns(m.key(), ts, tuuid, batch_no, res);

                    if (postimage) {
                        poikey = set_pk_columns(m.key(), ts, tuuid, ++batch_no, res);
                        set_operation(*poikey, ts, operation::post_image, res);
                    }

                    size_t pos = 0;
                    for (const auto& column : _schema->clustering_key_columns()) {
                        assert (pos < ck_value.size());
                        auto cdef = _log_schema->get_column_definition(log_data_column_name_bytes(column.name()));
                        res.set_cell(log_ck, *cdef, atomic_cell::make_live(*column.type, ts, bytes_view(ck_value[pos]), _cdc_ttl_opt));

                        if (pirow) {
                            assert(pirow->has(column.name_as_text()));
                            res.set_cell(*pikey, *cdef, atomic_cell::make_live(*column.type, ts, bytes_view(ck_value[pos]), _cdc_ttl_opt));
                        }
                        if (poikey) {
                            res.set_cell(*poikey, *cdef, atomic_cell::make_live(*column.type, ts, bytes_view(ck_value[pos]), _cdc_ttl_opt));
                        }

                        ++pos;
                    }
                    
                    operation cdc_op;
                    if (r.row().deleted_at()) {
                        touched_parts.set<stats::part_type::ROW_DELETE>();
                        cdc_op = operation::row_delete;
                        if (pirow) {
                            for (const column_definition& column: _schema->regular_columns()) {
                                assert(pirow->has(column.name_as_text()));
                                auto& cdef = *_log_schema->get_column_definition(log_data_column_name_bytes(column.name()));
                                auto value = get_preimage_col_value(column, pirow);
                                res.set_cell(*pikey, cdef, atomic_cell::make_live(*column.type, ts, bytes_view(value), _cdc_ttl_opt));
                            }
                        }
                    } else {
                        auto ttl = process_cells(r.row().cells(), column_kind::regular_column, log_ck, pikey, pirow, poikey);
                        const auto& marker = r.row().marker();
                        if (marker.is_live() && marker.is_expiring()) {
                            ttl = marker.ttl();
                        }

                        cdc_op = marker.is_live() ? operation::insert : operation::update;

                        if (ttl) {
                            set_ttl(log_ck, ts, *ttl, res);
                        }
                    }
                    set_operation(log_ck, ts, cdc_op, res);
                    ++batch_no;
                }
            }
        }

        return std::make_tuple(std::move(res), touched_parts);
    }

    static bytes get_preimage_col_value(const column_definition& cdef, const cql3::untyped_result_set_row *pirow) {
        return cdef.is_atomic()
            ? pirow->get_blob(cdef.name_as_text())
            : visit(*cdef.type, make_visitor(
                // flatten set
                [&] (const set_type_impl& type) {
                    auto v = pirow->get_view(cdef.name_as_text());
                    auto f = cql_serialization_format::internal();
                    auto n = read_collection_size(v, f);
                    std::vector<bytes_view> tmp;
                    tmp.reserve(n);
                    while (n--) {
                        tmp.emplace_back(read_collection_value(v, f)); // key
                        read_collection_value(v, f); // value. ignore.
                    }
                    return set_type_impl::serialize_partially_deserialized_form(tmp, f);
                },
                [&] (const abstract_type& o) -> bytes {
                    return pirow->get_blob(cdef.name_as_text());
                }
            ));
    }

    static db::timeout_clock::time_point default_timeout() {
        return db::timeout_clock::now() + 10s;
    }

    future<lw_shared_ptr<cql3::untyped_result_set>> pre_image_select(
            service::client_state& client_state,
            db::consistency_level cl,
            const mutation& m)
    {
        auto& p = m.partition();
        if (p.clustered_rows().empty() && p.static_row().empty()) {
            return make_ready_future<lw_shared_ptr<cql3::untyped_result_set>>();
        }

        dht::partition_range_vector partition_ranges{dht::partition_range(m.decorated_key())};

        auto&& pc = _schema->partition_key_columns();
        auto&& cc = _schema->clustering_key_columns();

        std::vector<query::clustering_range> bounds;
        uint32_t row_limit = query::max_rows;

        const bool has_only_static_row = !p.static_row().empty() && p.clustered_rows().empty();
        if (cc.empty() || has_only_static_row) {
            bounds.push_back(query::clustering_range::make_open_ended_both_sides());
            if (has_only_static_row) {
                row_limit = 1;
            }
        } else {
            for (const rows_entry& r : p.clustered_rows()) {
                auto& ck = r.key();
                bounds.push_back(query::clustering_range::make_singular(ck));
            }
        }

        std::vector<const column_definition*> columns;
        columns.reserve(_schema->all_columns().size());

        std::transform(pc.begin(), pc.end(), std::back_inserter(columns), [](auto& c) { return &c; });
        std::transform(cc.begin(), cc.end(), std::back_inserter(columns), [](auto& c) { return &c; });

        query::column_id_vector static_columns, regular_columns;

        // TODO: this assumes all mutations touch the same set of columns. This might not be true, and we may need to do more horrible set operation here.
        if (!p.static_row().empty()) {
            // for postimage we need everything...
            if (_schema->cdc_options().postimage()) {
                for (const column_definition& c: _schema->static_columns()) {
                    static_columns.emplace_back(c.id);
                    columns.emplace_back(&c);
                }
            } else {
                p.static_row().get().for_each_cell([&] (column_id id, const atomic_cell_or_collection&) {
                    auto& cdef =_schema->column_at(column_kind::static_column, id);
                    static_columns.emplace_back(id);
                    columns.emplace_back(&cdef);
                });
            }
        }
        if (!p.clustered_rows().empty()) {
            const bool has_row_delete = std::any_of(p.clustered_rows().begin(), p.clustered_rows().end(), [] (const rows_entry& re) {
                return re.row().deleted_at();
            });
            // for postimage we need everything...
            if (has_row_delete || _schema->cdc_options().postimage()) {
                for (const column_definition& c: _schema->regular_columns()) {
                    regular_columns.emplace_back(c.id);
                    columns.emplace_back(&c);
                }
            } else {
                p.clustered_rows().begin()->row().cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection&) {
                    const auto& cdef =_schema->column_at(column_kind::regular_column, id);
                    regular_columns.emplace_back(id);
                    columns.emplace_back(&cdef);
                });
            }
        }
        
        auto selection = cql3::selection::selection::for_columns(_schema, std::move(columns));

        auto opts = selection->get_query_options();
        opts.set(query::partition_slice::option::collections_as_maps);
        opts.set_if<query::partition_slice::option::always_return_static_content>(!p.static_row().empty());

        auto partition_slice = query::partition_slice(std::move(bounds), std::move(static_columns), std::move(regular_columns), std::move(opts));
        auto command = ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(), partition_slice, row_limit);

        return _ctx._proxy.query(_schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(default_timeout(), empty_service_permit(), client_state)).then(
                [s = _schema, partition_slice = std::move(partition_slice), selection = std::move(selection)] (service::storage_proxy::coordinator_query_result qr) -> lw_shared_ptr<cql3::untyped_result_set> {
                    cql3::selection::result_set_builder builder(*selection, gc_clock::now(), cql_serialization_format::latest());
                    query::result_view::consume(*qr.query_result, partition_slice, cql3::selection::result_set_builder::visitor(builder, *s, *selection));
                    auto result_set = builder.build();
                    if (!result_set || result_set->empty()) {
                        return {};
                    }
                    return make_lw_shared<cql3::untyped_result_set>(*result_set);
        });
    }
};

template <typename Func>
future<std::vector<mutation>>
transform_mutations(std::vector<mutation>& muts, decltype(muts.size()) batch_size, Func&& f) {
    return parallel_for_each(
            boost::irange(static_cast<decltype(muts.size())>(0), muts.size(), batch_size),
            std::forward<Func>(f))
        .then([&muts] () mutable { return std::move(muts); });
}

} // namespace cdc

future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>>
cdc::cdc_service::impl::augment_mutation_call(lowres_clock::time_point timeout, std::vector<mutation>&& mutations, tracing::trace_state_ptr tr_state) {
    // we do all this because in the case of batches, we can have mixed schemas.
    auto e = mutations.end();
    auto i = std::find_if(mutations.begin(), e, [](const mutation& m) {
        return m.schema()->cdc_options().enabled();
    });

    if (i == e) {
        return make_ready_future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>>(std::make_tuple(std::move(mutations), lw_shared_ptr<cdc::operation_result_tracker>()));
    }

    tracing::trace(tr_state, "CDC: Started generating mutations for log rows");
    mutations.reserve(2 * mutations.size());

    return do_with(std::move(mutations), service::query_state(service::client_state::for_internal_calls(), empty_service_permit()), operation_details{},
            [this, timeout, i, tr_state = std::move(tr_state)] (std::vector<mutation>& mutations, service::query_state& qs, operation_details& details) {
        return transform_mutations(mutations, 1, [this, &mutations, timeout, &qs, tr_state = tr_state, &details] (int idx) mutable {
            auto& m = mutations[idx];
            auto s = m.schema();

            if (!s->cdc_options().enabled()) {
                return make_ready_future<>();
            }

            transformer trans(_ctxt, s);

            auto f = make_ready_future<lw_shared_ptr<cql3::untyped_result_set>>(nullptr);
            if (s->cdc_options().preimage() || s->cdc_options().postimage()) {
                // Note: further improvement here would be to coalesce the pre-image selects into one
                // iff a batch contains several modifications to the same table. Otoh, batch is rare(?)
                // so this is premature.
                tracing::trace(tr_state, "CDC: Selecting preimage for {}", m.decorated_key());
                f = trans.pre_image_select(qs.get_client_state(), db::consistency_level::LOCAL_QUORUM, m).then_wrapped([this] (future<lw_shared_ptr<cql3::untyped_result_set>> f) {
                    auto& cdc_stats = _ctxt._proxy.get_cdc_stats();
                    cdc_stats.counters_total.preimage_selects++;
                    if (f.failed()) {
                        cdc_stats.counters_failed.preimage_selects++;
                    }
                    return f;
                });
            } else {
                tracing::trace(tr_state, "CDC: Preimage not enabled for the table, not querying current value of {}", m.decorated_key());
            }

            return f.then([trans = std::move(trans), &mutations, idx, tr_state = std::move(tr_state), &details] (lw_shared_ptr<cql3::untyped_result_set> rs) {
                auto& m = mutations[idx];
                auto& s = m.schema();
                details.had_preimage |= s->cdc_options().preimage();
                details.had_postimage |= s->cdc_options().postimage();
                tracing::trace(tr_state, "CDC: Generating log mutations for {}", m.decorated_key());
                int generated_count;
                if (should_split(m, *s)) {
                    tracing::trace(tr_state, "CDC: Splitting {}", m.decorated_key());
                    details.was_split = true;
                    generated_count = 0;
                    for_each_change(m, s, [&] (mutation mm, api::timestamp_type ts, bytes tuuid, int& batch_no) {
                        auto [mut, parts] = trans.transform(std::move(mm), rs.get(), ts, tuuid, batch_no);
                        mutations.push_back(std::move(mut));
                        ++generated_count;
                        details.touched_parts.add(parts);
                    });
                } else {
                    tracing::trace(tr_state, "CDC: No need to split {}", m.decorated_key());
                    int batch_no = 0;
                    auto ts = find_timestamp(*s, m);
                    auto tuuid = timeuuid_type->decompose(generate_timeuuid(ts));
                    auto [mut, parts] = trans.transform(m, rs.get(), ts, tuuid, batch_no);
                    mutations.push_back(std::move(mut));
                    generated_count = 1;
                    details.touched_parts.add(parts);
                }
                // `m` might be invalidated at this point because of the push_back to the vector
                tracing::trace(tr_state, "CDC: Generated {} log mutations from {}", generated_count, mutations[idx].decorated_key());
            });
        }).then([this, tr_state, &details](std::vector<mutation> mutations) {
            tracing::trace(tr_state, "CDC: Finished generating all log mutations");
            auto tracker = make_lw_shared<cdc::operation_result_tracker>(_ctxt._proxy.get_cdc_stats(), details);
            return make_ready_future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>>(std::make_tuple(std::move(mutations), std::move(tracker)));
        });
    });
}

bool cdc::cdc_service::needs_cdc_augmentation(const std::vector<mutation>& mutations) const {
    return std::any_of(mutations.begin(), mutations.end(), [](const mutation& m) {
        return m.schema()->cdc_options().enabled();
    });
}

future<std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>>
cdc::cdc_service::augment_mutation_call(lowres_clock::time_point timeout, std::vector<mutation>&& mutations, tracing::trace_state_ptr tr_state) {
    return _impl->augment_mutation_call(timeout, std::move(mutations), std::move(tr_state));
}
