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

#include <seastar/util/defer.hh>
#include <seastar/core/thread.hh>

#include "cdc/cdc.hh"
#include "bytes.hh"
#include "database.hh"
#include "db/config.hh"
#include "dht/murmur3_partitioner.hh"
#include "partition_slice_builder.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "types/tuple.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/multi_column_relation.hh"
#include "cql3/tuples.hh"
#include "log.hh"

using locator::snitch_ptr;
using locator::token_metadata;
using locator::topology;
using seastar::sstring;
using service::migration_manager;
using service::storage_proxy;

namespace std {

template<> struct hash<std::pair<net::inet_address, unsigned int>> {
    std::size_t operator()(const std::pair<net::inet_address, unsigned int> &p) const {
        return std::hash<net::inet_address>{}(p.first) ^ std::hash<int>{}(p.second);
    }
};

}

using namespace std::chrono_literals;

static logging::logger cdc_log("cdc");

namespace cdc {

using operation_native_type = std::underlying_type_t<operation>;
using column_op_native_type = std::underlying_type_t<column_op>;

sstring log_name(const sstring& table_name) {
    static constexpr auto cdc_log_suffix = "_scylla_cdc_log";
    return table_name + cdc_log_suffix;
}

sstring desc_name(const sstring& table_name) {
    static constexpr auto cdc_desc_suffix = "_scylla_cdc_desc";
    return table_name + cdc_desc_suffix;
}

static future<>
remove_log(db_context ctx, const sstring& ks_name, const sstring& table_name) {
    try {
        return ctx._migration_manager.announce_column_family_drop(
                ks_name, log_name(table_name), false);
    } catch (exceptions::configuration_exception& e) {
        // It's fine if the table does not exist.
        return make_ready_future<>();
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
}

static future<>
remove_desc(db_context ctx, const sstring& ks_name, const sstring& table_name) {
    try {
        return ctx._migration_manager.announce_column_family_drop(
                ks_name, desc_name(table_name), false);
    } catch (exceptions::configuration_exception& e) {
        // It's fine if the table does not exist.
        return make_ready_future<>();
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
}

future<>
remove(db_context ctx, const sstring& ks_name, const sstring& table_name) {
    return when_all(remove_log(ctx, ks_name, table_name),
                    remove_desc(ctx, ks_name, table_name)).discard_result();
}

static future<> setup_log(db_context ctx, const schema& s) {
    schema_builder b(s.ks_name(), log_name(s.cf_name()));
    b.set_default_time_to_live(gc_clock::duration{s.cdc_options().ttl()});
    b.set_comment(sprint("CDC log for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column("stream_id", uuid_type, column_kind::partition_key);
    b.with_column("time", timeuuid_type, column_kind::clustering_key);
    b.with_column("batch_seq_no", int32_type, column_kind::clustering_key);
    b.with_column("operation", data_type_for<operation_native_type>());
    b.with_column("ttl", long_type);
    auto add_columns = [&] (const schema::const_iterator_range_type& columns, bool is_data_col = false) {
        for (const auto& column : columns) {
            auto type = column.type;
            if (is_data_col) {
                type = tuple_type_impl::get_instance({ /* op */ data_type_for<column_op_native_type>(), /* value */ type, /* ttl */long_type});
            }
            b.with_column("_" + column.name(), type);
        }
    };
    add_columns(s.partition_key_columns());
    add_columns(s.clustering_key_columns());
    add_columns(s.static_columns(), true);
    add_columns(s.regular_columns(), true);
    return ctx._migration_manager.announce_new_column_family(b.build(), false);
}

static future<> setup_stream_description_table(db_context ctx, const schema& s) {
    schema_builder b(s.ks_name(), desc_name(s.cf_name()));
    b.set_comment(sprint("CDC description for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column("node_ip", inet_addr_type, column_kind::partition_key);
    b.with_column("shard_id", int32_type, column_kind::partition_key);
    b.with_column("created_at", timestamp_type, column_kind::clustering_key);
    b.with_column("stream_id", uuid_type);
    return ctx._migration_manager.announce_new_column_family(b.build(), false);
}

// This function assumes setup_stream_description_table was called on |s| before the call to this
// function.
static future<> populate_desc(db_context ctx, const schema& s) {
    auto& db = ctx._proxy.get_db().local();
    auto desc_schema =
        db.find_schema(s.ks_name(), desc_name(s.cf_name()));
    auto log_schema =
        db.find_schema(s.ks_name(), log_name(s.cf_name()));
    auto belongs_to = [&](const gms::inet_address& endpoint,
                          const unsigned int shard_id,
                          const int shard_count,
                          const unsigned int ignore_msb_bits,
                          const utils::UUID& stream_id) {
        const auto log_pk = partition_key::from_singular(*log_schema,
                                                         data_value(stream_id));
        const auto token = ctx._partitioner.decorate_key(*log_schema, log_pk).token();
        if (ctx._token_metadata.get_endpoint(ctx._token_metadata.first_token(token)) != endpoint) {
            return false;
        }
        const auto owning_shard_id = dht::murmur3_partitioner(shard_count, ignore_msb_bits).shard_of(token);
        return owning_shard_id == shard_id;
    };

    std::vector<mutation> mutations;
    const auto ts = api::new_timestamp();
    const auto ck = clustering_key::from_single_value(
            *desc_schema, timestamp_type->decompose(ts));
    auto cdef = desc_schema->get_column_definition(to_bytes("stream_id"));

    for (const auto& dc : ctx._token_metadata.get_topology().get_datacenter_endpoints()) {
        for (const auto& endpoint : dc.second) {
            const auto decomposed_ip = inet_addr_type->decompose(endpoint.addr());
            const unsigned int shard_count = ctx._snitch->get_shard_count(endpoint);
            const unsigned int ignore_msb_bits = ctx._snitch->get_ignore_msb_bits(endpoint);
            for (unsigned int shard_id = 0; shard_id < shard_count; ++shard_id) {
                const auto pk = partition_key::from_exploded(
                        *desc_schema, { decomposed_ip, int32_type->decompose(static_cast<int>(shard_id)) });
                mutations.emplace_back(desc_schema, pk);

                auto stream_id = utils::make_random_uuid();
                while (!belongs_to(endpoint, shard_id, shard_count, ignore_msb_bits, stream_id)) {
                    stream_id = utils::make_random_uuid();
                }
                auto value = atomic_cell::make_live(*uuid_type,
                                                    ts,
                                                    uuid_type->decompose(stream_id));
                mutations.back().set_cell(ck, *cdef, std::move(value));
            }
        }
    }
    return ctx._proxy.mutate(std::move(mutations),
                             db::consistency_level::QUORUM,
                             db::no_timeout,
                             nullptr,
                             empty_service_permit());
}

future<> setup(db_context ctx, schema_ptr s) {
    return seastar::async([ctx = std::move(ctx), s = std::move(s)] {
        setup_log(ctx, *s).get();
        auto log_guard = seastar::defer([&] { remove_log(ctx, s->ks_name(), s->cf_name()).get(); });
        setup_stream_description_table(ctx, *s).get();
        auto desc_guard = seastar::defer([&] { remove_desc(ctx, s->ks_name(), s->cf_name()).get(); });
        populate_desc(ctx, *s).get();
        desc_guard.cancel();
        log_guard.cancel();
    });
}

db_context db_context::builder::build() {
    return db_context{
        _proxy,
        _migration_manager ? _migration_manager->get() : service::get_local_migration_manager(),
        _token_metadata ? _token_metadata->get() : service::get_local_storage_service().get_token_metadata(),
        _snitch ? _snitch->get() : locator::i_endpoint_snitch::get_local_snitch_ptr(),
        _partitioner ? _partitioner->get() : dht::global_partitioner()
    };
}

class transformer final {
public:
    using streams_type = std::unordered_map<std::pair<net::inet_address, unsigned int>, utils::UUID>;
private:
    db_context _ctx;
    schema_ptr _schema;
    schema_ptr _log_schema;
    utils::UUID _time;
    bytes _decomposed_time;
    ::shared_ptr<const transformer::streams_type> _streams;
    const column_definition& _op_col;

    clustering_key set_pk_columns(const partition_key& pk, int batch_no, mutation& m) const {
        const auto log_ck = clustering_key::from_exploded(
                *m.schema(), { _decomposed_time, int32_type->decompose(batch_no) });
        auto pk_value = pk.explode(*_schema);
        size_t pos = 0;
        for (const auto& column : _schema->partition_key_columns()) {
            assert (pos < pk_value.size());
            auto cdef = m.schema()->get_column_definition(to_bytes("_" + column.name()));
            auto value = atomic_cell::make_live(*column.type,
                                                _time.timestamp(),
                                                bytes_view(pk_value[pos]));
            m.set_cell(log_ck, *cdef, std::move(value));
            ++pos;
        }
        return log_ck;
    }

    void set_operation(const clustering_key& ck, operation op, mutation& m) const {
        m.set_cell(ck, _op_col, atomic_cell::make_live(*_op_col.type, _time.timestamp(), _op_col.type->decompose(operation_native_type(op))));
    }

    partition_key stream_id(const net::inet_address& ip, unsigned int shard_id) const {
        auto it = _streams->find(std::make_pair(ip, shard_id));
        if (it == std::end(*_streams)) {
                throw std::runtime_error(format("No stream found for node {} and shard {}", ip, shard_id));
        }
        return partition_key::from_exploded(*_log_schema, { uuid_type->decompose(it->second) });
    }
public:
    transformer(db_context ctx, schema_ptr s, ::shared_ptr<const transformer::streams_type> streams)
        : _ctx(ctx)
        , _schema(std::move(s))
        , _log_schema(ctx._proxy.get_db().local().find_schema(_schema->ks_name(), log_name(_schema->cf_name())))
        , _time(utils::UUID_gen::get_time_UUID())
        , _decomposed_time(timeuuid_type->decompose(_time))
        , _streams(std::move(streams))
        , _op_col(*_log_schema->get_column_definition(to_bytes("operation")))
    {}

    // TODO: is pre-image data based on query enough. We only have actual column data. Do we need
    // more details like tombstones/ttl? Probably not but keep in mind.
    mutation transform(const mutation& m, const cql3::untyped_result_set* rs = nullptr) const {
        auto& t = m.token();
        auto&& ep = _ctx._token_metadata.get_endpoint(
                _ctx._token_metadata.first_token(t));
        if (!ep) {
            throw std::runtime_error(format("No owner found for key {}", m.decorated_key()));
        }
        auto shard_id = dht::murmur3_partitioner(_ctx._snitch->get_shard_count(*ep), _ctx._snitch->get_ignore_msb_bits(*ep)).shard_of(t);
        mutation res(_log_schema, stream_id(ep->addr(), shard_id));
        auto& p = m.partition();
        if (p.partition_tombstone()) {
            // Partition deletion
            auto log_ck = set_pk_columns(m.key(), 0, res);
            set_operation(log_ck, operation::partition_delete, res);
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
                                                            _time.timestamp(),
                                                            bytes_view(exploded[pos]));
                        res.set_cell(log_ck, *cdef, std::move(value));
                        ++pos;
                    }
                };
                {
                    auto log_ck = set_pk_columns(m.key(), batch_no, res);
                    set_bound(log_ck, rt.start);
                    // TODO: separate inclusive/exclusive range
                    set_operation(log_ck, operation::range_delete_start, res);
                    ++batch_no;
                }
                {
                    auto log_ck = set_pk_columns(m.key(), batch_no, res);
                    set_bound(log_ck, rt.end);
                    // TODO: separate inclusive/exclusive range
                    set_operation(log_ck, operation::range_delete_end, res);
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
                            pikey = set_pk_columns(m.key(), batch_no, res);
                            set_operation(*pikey, operation::pre_image, res);
                            pirow = &utr;
                            ++batch_no;
                            break;
                        }
                    }
                }

                auto log_ck = set_pk_columns(m.key(), batch_no, res);

                size_t pos = 0;
                for (const auto& column : _schema->clustering_key_columns()) {
                    assert (pos < ck_value.size());
                    auto cdef = _log_schema->get_column_definition(to_bytes("_" + column.name()));
                    res.set_cell(log_ck, *cdef, atomic_cell::make_live(*column.type, _time.timestamp(), bytes_view(ck_value[pos])));

                    if (pirow) {
                        assert(pirow->has(column.name_as_text()));
                        res.set_cell(*pikey, *cdef, atomic_cell::make_live(*column.type, _time.timestamp(), bytes_view(ck_value[pos])));
                    }

                    ++pos;
                }

                std::vector<bytes_opt> values(3);

                auto process_cells = [&](const row& r, column_kind ckind) {
                    r.for_each_cell([&](column_id id, const atomic_cell_or_collection& cell) {
                        auto& cdef = _schema->column_at(ckind, id);
                        auto* dst = _log_schema->get_column_definition(to_bytes("_" + cdef.name()));
                        // todo: collections.
                        if (cdef.is_atomic()) {
                            column_op op;

                            values[1] = values[2] = std::nullopt;
                            auto view = cell.as_atomic_cell(cdef);
                            if (view.is_live()) {
                                op = column_op::set;
                                values[1] = view.value().linearize();
                                if (view.is_live_and_has_ttl()) {
                                    values[2] = long_type->decompose(data_value(view.ttl().count()));
                                }
                            } else {
                                op = column_op::del;
                            }

                            values[0] = data_type_for<column_op_native_type>()->decompose(data_value(static_cast<column_op_native_type>(op)));
                            res.set_cell(log_ck, *dst, atomic_cell::make_live(*dst->type, _time.timestamp(), tuple_type_impl::build_value(values)));

                            if (pirow && pirow->has(cdef.name_as_text())) {
                                values[0] = data_type_for<column_op_native_type>()->decompose(data_value(static_cast<column_op_native_type>(column_op::set)));
                                values[1] = pirow->get_blob(cdef.name_as_text());
                                values[2] = std::nullopt;

                                assert(std::addressof(res.partition().clustered_row(*_log_schema, *pikey)) != std::addressof(res.partition().clustered_row(*_log_schema, log_ck)));
                                assert(pikey->explode() != log_ck.explode());
                                res.set_cell(*pikey, *dst, atomic_cell::make_live(*dst->type, _time.timestamp(), tuple_type_impl::build_value(values)));
                            }
                        } else {
                            cdc_log.warn("Non-atomic cell ignored {}.{}:{}", _schema->ks_name(), _schema->cf_name(), cdef.name_as_text());
                        }
                    });
                };

                process_cells(r.row().cells(), column_kind::regular_column);
                process_cells(p.static_row().get(), column_kind::static_column);

                set_operation(log_ck, operation::update, res);
                ++batch_no;
            }
        }

        return res;
    }

    static db::timeout_clock::time_point default_timeout() {
        return db::timeout_clock::now() + 10s;
    }

    future<lw_shared_ptr<cql3::untyped_result_set>> pre_image_select(
            service::storage_proxy& proxy,
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

        return proxy.query(_schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(default_timeout(), empty_service_permit(), client_state)).then(
                [this, partition_slice = std::move(partition_slice), selection = std::move(selection)] (service::storage_proxy::coordinator_query_result qr) -> lw_shared_ptr<cql3::untyped_result_set> {
                    cql3::selection::result_set_builder builder(*selection, gc_clock::now(), cql_serialization_format::latest());
                    query::result_view::consume(*qr.query_result, partition_slice, cql3::selection::result_set_builder::visitor(builder, *_schema, *selection));
                    auto result_set = builder.build();
                    if (!result_set || result_set->empty()) {
                        return {};
                    }
                    return make_lw_shared<cql3::untyped_result_set>(*result_set);
        });
    }
};

// This class is used to build a mapping from <node ip, shard id> to stream_id
// It is used as a consumer for rows returned by the query to CDC Description Table
class streams_builder {
    const schema& _schema;
    transformer::streams_type _streams;
    net::inet_address _node_ip = net::inet_address();
    unsigned int _shard_id = 0;
    api::timestamp_type _latest_row_timestamp = api::min_timestamp;
    utils::UUID _latest_row_stream_id = utils::UUID();
public:
    streams_builder(const schema& s) : _schema(s) {}

    void accept_new_partition(const partition_key& key, uint32_t row_count) {
        auto exploded = key.explode(_schema);
        _node_ip = value_cast<net::inet_address>(inet_addr_type->deserialize(exploded[0]));
        _shard_id = static_cast<unsigned int>(value_cast<int>(int32_type->deserialize(exploded[1])));
        _latest_row_timestamp = api::min_timestamp;
        _latest_row_stream_id = utils::UUID();
    }

    void accept_new_partition(uint32_t row_count) {
        assert(false);
    }

    void accept_new_row(
            const clustering_key& key,
            const query::result_row_view& static_row,
            const query::result_row_view& row) {
        auto row_iterator = row.iterator();
        api::timestamp_type timestamp = value_cast<db_clock::time_point>(
                timestamp_type->deserialize(key.explode(_schema)[0])).time_since_epoch().count();
        if (timestamp <= _latest_row_timestamp) {
            return;
        }
        _latest_row_timestamp = timestamp;
        for (auto&& cdef : _schema.regular_columns()) {
            if (cdef.name_as_text() != "stream_id") {
                row_iterator.skip(cdef);
                continue;
            }
            auto val_opt = row_iterator.next_atomic_cell();
            assert(val_opt);
            val_opt->value().with_linearized([&] (bytes_view bv) {
                _latest_row_stream_id = value_cast<utils::UUID>(uuid_type->deserialize(bv));
            });
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        assert(false);
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        _streams.emplace(std::make_pair(_node_ip, _shard_id), _latest_row_stream_id);
    }

    transformer::streams_type build() {
        return std::move(_streams);
    }
};

static future<::shared_ptr<transformer::streams_type>> get_streams(
        db_context ctx,
        const sstring& ks_name,
        const sstring& cf_name,
        lowres_clock::time_point timeout,
        service::query_state& qs) {
    auto s =
        ctx._proxy.get_db().local().find_schema(ks_name, desc_name(cf_name));
    query::read_command cmd(
            s->id(),
            s->version(),
            partition_slice_builder(*s).with_no_static_columns().build());
    return ctx._proxy.query(
            s,
            make_lw_shared(std::move(cmd)),
            {dht::partition_range::make_open_ended_both_sides()},
            db::consistency_level::QUORUM,
            {timeout, qs.get_permit(), qs.get_client_state()}).then([s = std::move(s)] (auto qr) mutable {
        return query::result_view::do_with(*qr.query_result,
                [s = std::move(s)] (query::result_view v) {
            auto slice = partition_slice_builder(*s)
                    .with_no_static_columns()
                    .build();
            streams_builder builder{ *s };
            v.consume(slice, builder);
            return ::make_shared<transformer::streams_type>(builder.build());
        });
    });
}

future<std::vector<mutation>> append_log_mutations(
        db_context ctx,
        schema_ptr s,
        service::storage_proxy::clock_type::time_point timeout,
        service::query_state& qs,
        std::vector<mutation> muts) {
    auto mp = ::make_lw_shared<std::vector<mutation>>(std::move(muts));

    return get_streams(ctx, s->ks_name(), s->cf_name(), timeout, qs).then([ctx, s = std::move(s), mp, &qs](::shared_ptr<transformer::streams_type> streams) mutable {
        mp->reserve(2 * mp->size());
        auto trans = make_lw_shared<transformer>(ctx, s, std::move(streams));
        auto i = mp->begin();
        auto e = mp->end();
        return parallel_for_each(i, e, [ctx, &qs, trans, mp](mutation& m) {
            return trans->pre_image_select(ctx._proxy, qs.get_client_state(), db::consistency_level::LOCAL_QUORUM, m).then([trans, mp, &m](lw_shared_ptr<cql3::untyped_result_set> rs) {
                mp->push_back(trans->transform(m, rs.get()));
            });
        }).then([mp] {
            return std::move(*mp);
        });
    });
}

} // namespace cdc
