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

#include "cdc/log.hh"
#include "cdc/generation.hh"
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

    future<std::tuple<std::vector<mutation>, result_callback>> augment_mutation_call(
        lowres_clock::time_point timeout,
        std::vector<mutation>&& mutations
    );

    template<typename Iter>
    future<> append_mutations(Iter i, Iter e, schema_ptr s, lowres_clock::time_point, std::vector<mutation>&);

private:
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
using column_op_native_type = std::underlying_type_t<column_op>;

static const sstring cdc_log_suffix = "_scylla_cdc_log";

static bool is_log_name(const std::string_view& table_name) {
    return boost::ends_with(table_name, cdc_log_suffix);
}

bool is_log_for_some_table(const sstring& ks_name, const std::string_view& table_name) {
    if (!is_log_name(table_name)) {
        return false;
    }
    const auto base_name = sstring(table_name.data(), table_name.size() - cdc_log_suffix.size());
    const auto base_schema = service::get_local_storage_proxy().get_db().local().find_schema(ks_name, base_name);
    return bool(base_schema) && base_schema->cdc_options().enabled();
}

sstring log_name(const sstring& table_name) {
    return table_name + cdc_log_suffix;
}

static schema_ptr create_log_schema(const schema& s, std::optional<utils::UUID> uuid) {
    schema_builder b(s.ks_name(), log_name(s.cf_name()));
    b.set_comment(sprint("CDC log for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column("stream_id_1", long_type, column_kind::partition_key);
    b.with_column("stream_id_2", long_type, column_kind::partition_key);
    b.with_column("time", timeuuid_type, column_kind::clustering_key);
    b.with_column("batch_seq_no", int32_type, column_kind::clustering_key);
    b.with_column("operation", data_type_for<operation_native_type>());
    b.with_column("ttl", long_type);
    auto add_columns = [&] (const schema::const_iterator_range_type& columns, bool is_data_col = false) {
        for (const auto& column : columns) {
            auto type = column.type;
            if (is_data_col) {
                type = tuple_type_impl::get_instance({ /* op */ data_type_for<column_op_native_type>(), /* value */ type});
            }
            b.with_column("_" + column.name(), type);
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

/* Given a timestamp, generates a timeuuid with the following properties:
 * 1. `t1` < `t2` implies timeuuid_type->less(timeuuid_type->decompose(generate_timeuuid(`t1`)),
 *                                            timeuuid_type->decompose(generate_timeuuid(`t2`))),
 * 2. utils::UUID_gen::micros_timestamp(generate_timeuuid(`t`)) == `t`.
 *
 * If `t1` == `t2`, then generate_timeuuid(`t1`) != generate_timeuuid(`t2`),
 * with unspecified nondeterministic ordering.
 */
// external linkage for testing
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
            auto cdef = m.schema()->get_column_definition(to_bytes("_" + column.name()));
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
        , _op_col(*_log_schema->get_column_definition(to_bytes("operation")))
        , _ttl_col(*_log_schema->get_column_definition(to_bytes("ttl")))
    {
        if (_schema->cdc_options().ttl()) {
            _cdc_ttl_opt = std::chrono::seconds(_schema->cdc_options().ttl());
        }
    }

    // TODO: is pre-image data based on query enough. We only have actual column data. Do we need
    // more details like tombstones/ttl? Probably not but keep in mind.
    mutation transform(const mutation& m, const cql3::untyped_result_set* rs = nullptr) const {
        auto ts = find_timestamp(*_schema, m);
        auto stream_id = _ctx._cdc_metadata.get_stream(ts, m.token());
        mutation res(_log_schema, stream_id.to_partition_key(*_log_schema));
        auto tuuid = timeuuid_type->decompose(generate_timeuuid(ts));

        auto& p = m.partition();
        if (p.partition_tombstone()) {
            // Partition deletion
            auto log_ck = set_pk_columns(m.key(), ts, tuuid, 0, res);
            set_operation(log_ck, ts, operation::partition_delete, res);
        } else if (!p.row_tombstones().empty()) {
            // range deletion
            int batch_no = 0;
            for (auto& rt : p.row_tombstones()) {
                auto set_bound = [&] (const clustering_key& log_ck, const clustering_key_prefix& ckp) {
                    auto exploded = ckp.explode(*_schema);
                    size_t pos = 0;
                    for (const auto& column : _schema->clustering_key_columns()) {
                        if (pos >= exploded.size()) {
                            break;
                        }
                        auto cdef = _log_schema->get_column_definition(to_bytes("_" + column.name()));
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
            // should be update or deletion
            int batch_no = 0;
            for (const rows_entry& r : p.clustered_rows()) {
                auto ck_value = r.key().explode(*_schema);

                std::optional<clustering_key> pikey;
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

                size_t pos = 0;
                for (const auto& column : _schema->clustering_key_columns()) {
                    assert (pos < ck_value.size());
                    auto cdef = _log_schema->get_column_definition(to_bytes("_" + column.name()));
                    res.set_cell(log_ck, *cdef, atomic_cell::make_live(*column.type, ts, bytes_view(ck_value[pos]), _cdc_ttl_opt));

                    if (pirow) {
                        assert(pirow->has(column.name_as_text()));
                        res.set_cell(*pikey, *cdef, atomic_cell::make_live(*column.type, ts, bytes_view(ck_value[pos]), _cdc_ttl_opt));
                    }

                    ++pos;
                }

                std::vector<bytes_opt> values(2);
                std::optional<gc_clock::duration> ttl;

                auto process_cells = [&](const row& r, column_kind ckind) {
                    r.for_each_cell([&](column_id id, const atomic_cell_or_collection& cell) {
                        auto& cdef = _schema->column_at(ckind, id);
                        auto* dst = _log_schema->get_column_definition(to_bytes("_" + cdef.name()));
                        // TODO: collections.
                        if (cdef.is_atomic()) {
                            column_op op;

                            values[1] = std::nullopt;
                            auto view = cell.as_atomic_cell(cdef);
                            if (view.is_live()) {
                                op = column_op::set;
                                values[1] = view.value().linearize();
                                if (view.is_live_and_has_ttl()) {
                                    ttl = view.ttl();
                                }
                            } else {
                                op = column_op::del;
                            }

                            values[0] = data_type_for<column_op_native_type>()->decompose(data_value(static_cast<column_op_native_type>(op)));
                            res.set_cell(log_ck, *dst, atomic_cell::make_live(*dst->type, ts, tuple_type_impl::build_value(values), _cdc_ttl_opt));

                            if (pirow && pirow->has(cdef.name_as_text())) {
                                values[0] = data_type_for<column_op_native_type>()->decompose(data_value(static_cast<column_op_native_type>(column_op::set)));
                                values[1] = pirow->get_blob(cdef.name_as_text());

                                assert(std::addressof(res.partition().clustered_row(*_log_schema, *pikey)) != std::addressof(res.partition().clustered_row(*_log_schema, log_ck)));
                                assert(pikey->explode() != log_ck.explode());
                                res.set_cell(*pikey, *dst, atomic_cell::make_live(*dst->type, ts, tuple_type_impl::build_value(values), _cdc_ttl_opt));
                            }
                        } else {
                            cdc_log.warn("Non-atomic cell ignored {}.{}:{}", _schema->ks_name(), _schema->cf_name(), cdef.name_as_text());
                        }
                    });
                };

                const auto& marker = r.row().marker();
                if (marker.is_live() && marker.is_expiring()) {
                    ttl = marker.ttl();
                }
                process_cells(r.row().cells(), column_kind::regular_column);
                process_cells(p.static_row().get(), column_kind::static_column);

                set_operation(log_ck, ts, operation::update, res);
                if (ttl) {
                    set_ttl(log_ck, ts, *ttl, res);
                }
                ++batch_no;
            }
        }

        return res;
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
        if (p.partition_tombstone() || !p.row_tombstones().empty() || p.clustered_rows().empty()) {
            return make_ready_future<lw_shared_ptr<cql3::untyped_result_set>>();
        }

        dht::partition_range_vector partition_ranges{dht::partition_range(m.decorated_key())};

        auto&& pc = _schema->partition_key_columns();
        auto&& cc = _schema->clustering_key_columns();

        std::vector<query::clustering_range> bounds;
        if (cc.empty()) {
            bounds.push_back(query::clustering_range::make_open_ended_both_sides());
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

        auto sk = column_kind::static_column;
        auto rk = column_kind::regular_column;
        // TODO: this assumes all mutations touch the same set of columns. This might not be true, and we may need to do more horrible set operation here.
        for (auto& [r, cids, kind] : { std::tie(p.static_row().get(), static_columns, sk), std::tie(p.clustered_rows().begin()->row().cells(), regular_columns, rk) }) {
            r.for_each_cell([&](column_id id, const atomic_cell_or_collection&) {
                auto& cdef =_schema->column_at(kind, id);
                cids.emplace_back(id);
                columns.emplace_back(&cdef);
            });
        }

        auto selection = cql3::selection::selection::for_columns(_schema, std::move(columns));
        auto partition_slice = query::partition_slice(std::move(bounds), std::move(static_columns), std::move(regular_columns), selection->get_query_options());
        auto command = ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(), partition_slice, query::max_partitions);

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

future<std::tuple<std::vector<mutation>, cdc::result_callback>>
cdc::cdc_service::impl::augment_mutation_call(lowres_clock::time_point timeout, std::vector<mutation>&& mutations) {
    // we do all this because in the case of batches, we can have mixed schemas.
    auto e = mutations.end();
    auto i = std::find_if(mutations.begin(), e, [](const mutation& m) {
        return m.schema()->cdc_options().enabled();
    });

    if (i == e) {
        return make_ready_future<std::tuple<std::vector<mutation>, cdc::result_callback>>(std::make_tuple(std::move(mutations), result_callback{}));
    }

    mutations.reserve(2 * mutations.size());

    return do_with(std::move(mutations), service::query_state(service::client_state::for_internal_calls(), empty_service_permit()),
            [this, timeout, i] (std::vector<mutation>& mutations, service::query_state& qs) {
        return transform_mutations(mutations, 1, [this, &mutations, timeout, &qs] (int idx) {
            auto& m = mutations[idx];
            auto s = m.schema();

            if (!s->cdc_options().enabled()) {
                return make_ready_future<>();
            }

            transformer trans(_ctxt, s);

            if (!s->cdc_options().preimage()) {
                mutations.emplace_back(trans.transform(m));
                return make_ready_future<>();
            }

            // Note: further improvement here would be to coalesce the pre-image selects into one
            // iff a batch contains several modifications to the same table. Otoh, batch is rare(?)
            // so this is premature.
            auto f = trans.pre_image_select(qs.get_client_state(), db::consistency_level::LOCAL_QUORUM, m);
            return f.then([trans = std::move(trans), &mutations, idx] (lw_shared_ptr<cql3::untyped_result_set> rs) mutable {
                mutations.push_back(trans.transform(mutations[idx], rs.get()));
            });
        }).then([](std::vector<mutation> mutations) {
            return make_ready_future<std::tuple<std::vector<mutation>, cdc::result_callback>>(std::make_tuple(std::move(mutations), result_callback{}));
        });
    });
}

bool cdc::cdc_service::needs_cdc_augmentation(const std::vector<mutation>& mutations) const {
    return std::any_of(mutations.begin(), mutations.end(), [](const mutation& m) {
        return m.schema()->cdc_options().enabled();
    });
}

future<std::tuple<std::vector<mutation>, cdc::result_callback>>
cdc::cdc_service::augment_mutation_call(lowres_clock::time_point timeout, std::vector<mutation>&& mutations) {
    return _impl->augment_mutation_call(timeout, std::move(mutations));
}
