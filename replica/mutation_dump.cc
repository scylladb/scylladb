/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later)
 */

#include "multishard_mutation_query.hh"
#include "mutation/json.hh"
#include "mutation_query.hh"
#include "partition_slice_builder.hh"
#include "readers/foreign.hh"
#include "replica/database.hh"
#include "replica/mutation_dump.hh"
#include "replica/query_state.hh"
#include "schema/schema_builder.hh"
#include "schema/schema_registry.hh"
#include "sstables/sstables.hh"
#include "utils/hashers.hh"

namespace replica::mutation_dump {

namespace {

class mutation_dump_reader : public mutation_reader::impl {
    struct mutation_source_with_params {
        mutation_source ms;
        std::vector<interval<partition_region>> region_intervals;
        query::partition_slice slice;
    };

    using exploded_clustering_range = interval<std::vector<bytes>>;

private:
    replica::database& _db;
    dht::decorated_key _dk;
    query::partition_slice _ps;
    tracing::trace_state_ptr _ts;
    std::vector<position_range> _pos_ranges;
    schema_ptr _underlying_schema;
    dht::partition_range _underlying_pr;
    // has to be sorted, because it is part of the clustering key
    std::map<sstring, mutation_source_with_params> _underlying_mutation_sources;
    mutation_reader_opt _underlying_reader;
    bool _partition_start_emitted = false;
    bool _partition_end_emitted = false;

private:
    static void set_cell(const ::schema& schema, row& cr, const column_definition& cdef, data_value value) {
        auto ts = api::new_timestamp();
        if (!value.is_null()) {
            cr.apply(cdef, atomic_cell::make_live(*cdef.type, ts, value.serialize_nonnull()));
        }
    }
    static void set_cell(const ::schema& schema, row& cr, const bytes& column_name, data_value value) {
        auto cdef = schema.get_column_definition(column_name);
        set_cell(schema, cr, *cdef, std::move(value));
    }

    std::map<sstring, mutation_source> create_all_mutation_sources() {
        std::map<sstring, mutation_source> all_mutation_sources;
        auto& tbl = _db.find_column_family(_underlying_schema);
        {
            auto mss = tbl.select_memtables_as_mutation_sources(_dk.token());
            for (size_t i = 0; i < mss.size(); ++i) {
                auto current_source = format("memtable:{}", i);
                all_mutation_sources.emplace(std::move(current_source), mss[i]);
            }
        }
        all_mutation_sources.emplace("row-cache", mutation_source([&tbl] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& pr,
                const query::partition_slice& ps,
                tracing::trace_state_ptr ts,
                streamed_mutation::forwarding,
                mutation_reader::forwarding) {
            return tbl.make_nonpopulating_cache_reader(std::move(schema), std::move(permit), pr, ps, ts);
        }));
        {
            auto ssts = tbl.select_sstables(_underlying_pr);
            for (size_t i = 0; i < ssts.size(); ++i) {
                auto current_source = format("sstable:{}", ssts[i]->get_filename());
                all_mutation_sources.emplace(std::move(current_source), ssts[i]->as_mutation_source());
            }
        }
        return all_mutation_sources;
    }

    template <typename T>
    interval<T> transform_range(const exploded_clustering_range& cr, unsigned i) {
        const auto& ck_types = _schema->clustering_key_type()->types();
        auto transform_bound = [&] (std::optional<exploded_clustering_range::bound> b) -> std::optional<typename interval<T>::bound> {
            if (!b) {
                return {};
            }
            const auto& exploded_ck = b->value();
            if (exploded_ck.size() <= i) {
                return {};
            }
            const auto force_inclusive = exploded_ck.size() > i + 1;
            return typename interval<T>::bound(value_cast<T>(ck_types.at(i)->deserialize_value(exploded_ck[i])), force_inclusive || b->is_inclusive());
        };
        return interval<T>(transform_bound(cr.start()), transform_bound(cr.end()));
    }

    query::clustering_range transform_to_underlying_cr(const exploded_clustering_range& cr) {
        const auto& ck_types = _underlying_schema->clustering_key_type()->types();
        auto transform_range_bound = [&] (std::optional<exploded_clustering_range::bound> b, bool is_end) -> std::optional<query::clustering_range::bound> {
            if (!b) {
                return {};
            }
            const auto& exploded_ck = b->value();
            if (exploded_ck.size() < 3) {
                return {};
            }
            const auto underlying_ck_begin = exploded_ck.begin() + 2;
            const auto underlying_ck_end = underlying_ck_begin + std::min(exploded_ck.size() - 2, ck_types.size());
            auto underlying_ck = clustering_key::from_exploded(std::vector<bytes>(underlying_ck_begin, underlying_ck_end));
            bool is_inclusive = b->is_inclusive();
            // Check if inclusiveness override is needed because of position weight
            if (exploded_ck.size() == ck_types.size() + 3) {
                const auto pos_weight = value_cast<int8_t>(byte_type->deserialize_value(exploded_ck.back()));
                if (pos_weight < 0) {
                    if (!is_end) {
                        is_inclusive = true;
                    }
                    if (is_end) {
                        is_inclusive = false;
                    }
                } else if (pos_weight > 0) {
                    if (!is_end) {
                        is_inclusive = false;
                    }
                    if (is_end) {
                        is_inclusive = true;
                    }
                }
            }
            return query::clustering_range::bound(std::move(underlying_ck), is_inclusive);
        };
        return query::clustering_range(transform_range_bound(cr.start(), false), transform_range_bound(cr.end(), true));
    }

    void create_underlying_mutation_sources() {
        const auto all_mutation_sources = create_all_mutation_sources();
        const auto ms_end = all_mutation_sources.end();
        const auto& ranges = _ps.row_ranges(*_schema, _dk.key());

        struct mutation_source_with_slice_parts {
            mutation_source_opt ms;
            std::vector<interval<partition_region>> region_intervals;
            std::vector<query::clustering_range> underlying_crs;
        };
        std::map<sstring, mutation_source_with_slice_parts> prepared_mutation_sources;

        auto maybe_push = [] (auto& intervals, auto&& new_interval, const auto& cmp) {
            if (intervals.empty() || !intervals.back().equal(new_interval, cmp)) {
                intervals.push_back(std::move(new_interval));
            }
        };

        for (const auto& cr : ranges) {
            const auto exploded_cr = cr.transform([this] (const clustering_key& ck) {
                auto elements = ck.explode(*_schema);
                while (!elements.empty() && elements.back().empty()) {
                    elements.pop_back();
                }
                return elements;
            });
            auto ms_it = all_mutation_sources.begin();
            const auto ms_int = transform_range<sstring>(exploded_cr, 0);

            while (ms_it != ms_end && ms_int.before(ms_it->first, std::compare_three_way{})) {
                ++ms_it;
            }
            if (ms_it == ms_end) {
                continue;
            }

            const auto region_int = transform_range<int8_t>(exploded_cr, 1).transform([] (int8_t v) { return partition_region(v); });
            const auto transformed_cr = transform_to_underlying_cr(exploded_cr);
            while (ms_it != ms_end && ms_int.contains(ms_it->first,  std::compare_three_way{})) {
                auto& e = prepared_mutation_sources[ms_it->first];
                e.ms = ms_it->second;
                maybe_push(e.region_intervals, region_int, std::compare_three_way{});
                maybe_push(e.underlying_crs, transformed_cr, clustering_key_view::tri_compare(*_underlying_schema));
                ++ms_it;
            }
        }

        for (auto& [name, ms] : prepared_mutation_sources) {
            auto slice = partition_slice_builder(*_underlying_schema).with_ranges(std::move(ms.underlying_crs)).build();
            _underlying_mutation_sources.emplace(name,
                    mutation_source_with_params{std::move(*ms.ms), std::move(ms.region_intervals), std::move(slice)});
        }
    }

    clustering_key transform_clustering_key(position_in_partition_view pos, const sstring& data_source_name) {
        const auto& underlying_ck_types = _underlying_schema->clustering_key_type()->types();
        const auto underlying_ck_raw_values = pos.has_key() ? pos.key().explode(*_underlying_schema) : std::vector<bytes>{};

        std::vector<bytes> output_ck_raw_values;
        output_ck_raw_values.push_back(data_value(data_source_name).serialize_nonnull());
        output_ck_raw_values.push_back(data_value(static_cast<int8_t>(pos.region())).serialize_nonnull());
        for (unsigned i = 0; i < underlying_ck_types.size(); ++i) {
            if (i < underlying_ck_raw_values.size()) {
                output_ck_raw_values.emplace_back(underlying_ck_raw_values[i]);
            } else {
                output_ck_raw_values.emplace_back(bytes{});
            }
        }
        if (underlying_ck_raw_values.empty()) {
            output_ck_raw_values.push_back(bytes{});
        } else {
            output_ck_raw_values.push_back(data_value(static_cast<int8_t>(pos.get_bound_weight())).serialize_nonnull());
        }

        return clustering_key::from_exploded(*_schema, output_ck_raw_values);
    }

    void add_metadata_column(mutation_fragment_v2& mf, row& r, const column_definition& cdef) {
        std::stringstream ss;
        mutation_json::mutation_partition_json_writer writer(*_underlying_schema, ss);

        switch (mf.mutation_fragment_kind()) {
            case mutation_fragment_v2::kind::partition_start:
                writer.writer().StartObject();
                writer.writer().Key("tombstone");
                writer.write(mf.as_partition_start().partition_tombstone());
                writer.writer().EndObject();
                break;
            case mutation_fragment_v2::kind::static_row:
                writer.write(mf.as_static_row().cells(), column_kind::static_column, false);
                break;
            case mutation_fragment_v2::kind::clustering_row:
                {
                    writer.writer().StartObject();
                    auto& cr = mf.as_clustering_row();
                    if (cr.tomb()) {
                        writer.writer().Key("tombstone");
                        writer.write(cr.tomb().regular());
                        writer.writer().Key("shadowable_tombstone");
                        writer.write(cr.tomb().shadowable().tomb());
                    }
                    if (!cr.marker().is_missing()) {
                        writer.writer().Key("marker");
                        writer.write(cr.marker());
                    }
                    writer.writer().Key("columns");
                    writer.write(cr.cells(), column_kind::regular_column, false);
                    writer.writer().EndObject();
                }
                break;
            case mutation_fragment_v2::kind::range_tombstone_change:
                writer.writer().StartObject();
                writer.writer().Key("tombstone");
                writer.write(mf.as_range_tombstone_change().tombstone());
                writer.writer().EndObject();
                break;
            case mutation_fragment_v2::kind::partition_end:
                // No value set.
                break;
        }

        if (!ss.view().empty()) {
            set_cell(*_schema, r, cdef, std::move(ss).str());
        }
    }

    void add_value_column(mutation_fragment_v2& mf, row& r, const column_definition& cdef) {
        column_kind kind;
        const row* value;
        if (mf.is_static_row()) {
            kind = column_kind::static_column;
            value = &mf.as_static_row().cells();
        } else if (mf.is_clustering_row()) {
            kind = column_kind::regular_column;
            value = &mf.as_clustering_row().cells();
        } else {
            return;
        }

        std::stringstream ss;
        mutation_json::mutation_partition_json_writer writer(*_underlying_schema, ss);

        writer.writer().StartObject();

        value->for_each_cell([this, kind, &writer] (column_id id, const atomic_cell_or_collection& cell) {
            auto& cdef = _underlying_schema->column_at(kind, id);
            writer.writer().Key(cdef.name_as_text());
            if (cdef.is_atomic()) {
                writer.write_atomic_cell_value(cell.as_atomic_cell(cdef), cdef.type);
            } else if (cdef.type->is_collection() || cdef.type->is_user_type()) {
                cell.as_collection_mutation().with_deserialized(*cdef.type, [&] (collection_mutation_view_description mv) {
                    writer.write_collection_value(mv, cdef.type);
                });
            } else {
                writer.writer().Null();
            }
        });

        writer.writer().EndObject();

        set_cell(*_schema, r, cdef, std::move(ss).str());
    }

    mutation_fragment_v2 transform_mutation_fragment(mutation_fragment_v2& mf, const sstring& data_source_name) {
        auto ck = transform_clustering_key(mf.position(), data_source_name);
        auto cr_out = clustering_row(ck);

        set_cell(*_schema, cr_out.cells(), "mutation_fragment_kind", fmt::to_string(mf.mutation_fragment_kind()));

        if (!mf.is_end_of_partition()) {
            auto metadata_cdef = *_schema->get_column_definition("metadata");
            if (std::ranges::find(_ps.regular_columns, metadata_cdef.id) != _ps.regular_columns.end()) {
                add_metadata_column(mf, cr_out.cells(), metadata_cdef);
            }

            auto value_cdef = *_schema->get_column_definition("value");
            if (std::ranges::find(_ps.regular_columns, value_cdef.id) != _ps.regular_columns.end()) {
                add_value_column(mf, cr_out.cells(), value_cdef);
            }
        }

        return mutation_fragment_v2(*_schema, _permit, std::move(cr_out));
    }


    void partition_not_empty() {
        if (!_partition_start_emitted) {
            push_mutation_fragment(*_schema, _permit, partition_start(_dk, {}));
            _partition_start_emitted = true;
        }
    }

    future<> do_fill_buffer() {
        auto cmp = clustering_key::prefix_equal_tri_compare(*_schema);
        while (!is_buffer_full() && !_underlying_mutation_sources.empty()) {
            auto ms_begin = _underlying_mutation_sources.begin();

            if (!_underlying_reader) {
                _underlying_reader = ms_begin->second.ms.make_reader_v2(_underlying_schema, _permit, _underlying_pr, ms_begin->second.slice,
                        _ts, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
            }

            auto mf_opt = co_await (*_underlying_reader)();

            auto contains_mf_region = [&] (const interval<partition_region>& prr) {
                return prr.contains(mf_opt->position().region(), std::compare_three_way{});
            };

            if (mf_opt && std::ranges::any_of(ms_begin->second.region_intervals, contains_mf_region)) {
                partition_not_empty();
                push_mutation_fragment(transform_mutation_fragment(*mf_opt, ms_begin->first));
            } else if (!mf_opt || ms_begin->second.region_intervals.back().after(mf_opt->position().region(), std::compare_three_way{})) {
                // The reader is at EOS or provided all interesting fragments already.
                co_await _underlying_reader->close();
                _underlying_reader = {};
                _underlying_mutation_sources.erase(ms_begin);
                continue;
            }
        }
    }

public:
    mutation_dump_reader(schema_ptr output_schema, schema_ptr underlying_schema, reader_permit permit, replica::database& db, const dht::decorated_key& dk, const query::partition_slice& ps, tracing::trace_state_ptr ts)
        : impl(std::move(output_schema), std::move(permit))
        , _db(db)
        , _dk(dk)
        , _ps(ps)
        , _ts(std::move(ts))
        , _underlying_schema(std::move(underlying_schema))
        , _underlying_pr(dht::partition_range::make_singular(dk))
    {
        create_underlying_mutation_sources();
    }
    virtual future<> fill_buffer() override {
        co_await do_fill_buffer();
        _end_of_stream = _underlying_mutation_sources.empty();
        if (_end_of_stream && _partition_start_emitted && !_partition_end_emitted) {
            push_mutation_fragment(*_schema, _permit, partition_end{});
            _partition_end_emitted = true;
        }
    }
    virtual future<> next_partition() override { throw std::bad_function_call(); }
    virtual future<> fast_forward_to(const dht::partition_range&) override { throw std::bad_function_call(); }
    virtual future<> fast_forward_to(position_range) override { throw std::bad_function_call(); }
    virtual future<> close() noexcept override {
        if (_underlying_reader) {
            return _underlying_reader->close();
        }
        return make_ready_future<>();
    }
};

future<mutation_reader> make_partition_mutation_dump_reader(
        schema_ptr output_schema,
        schema_ptr underlying_schema,
        reader_permit permit,
        distributed<replica::database>& db,
        const dht::decorated_key& dk,
        const query::partition_slice& ps,
        tracing::trace_state_ptr ts,
        db::timeout_clock::time_point timeout) {
    const auto& tbl = db.local().find_column_family(underlying_schema);

    // We can get a request for a token we don't own.
    // Just return empty reader in this case, otherwise we will hit
    // std::terminate because the replica side does not handle requests for
    // un-owned tokens.
    {
        auto erm = tbl.get_effective_replication_map();
        auto& topo = erm->get_topology();
        const auto endpoints = erm->get_endpoints_for_reading(dk.token());
        if (std::ranges::find(endpoints, topo.this_node()->endpoint()) == endpoints.end()) {
            co_return make_empty_flat_reader_v2(output_schema, std::move(permit));
        }
    }

    const auto shard = tbl.shard_for_reads(dk.token());
    if (shard == this_shard_id()) {
        co_return make_mutation_reader<mutation_dump_reader>(std::move(output_schema), std::move(underlying_schema), std::move(permit),
                db.local(), dk, ps, std::move(ts));
    }
    auto gos = global_schema_ptr(output_schema);
    auto gus = global_schema_ptr(underlying_schema);
    auto gts = tracing::global_trace_state_ptr(ts);
    auto remote_reader = co_await db.invoke_on(shard,
            [gos = std::move(gos), gus = std::move(gus), &dk, &ps, gts = std::move(gts), timeout] (replica::database& local_db) -> future<foreign_ptr<std::unique_ptr<mutation_reader>>> {
        auto output_schema = gos.get();
        auto underlying_schema = gus.get();
        auto ts = gts.get();
        auto permit = co_await local_db.obtain_reader_permit(underlying_schema, "mutation-dump-remote-read", timeout, ts);
        auto reader = make_mutation_reader<mutation_dump_reader>(std::move(output_schema), std::move(underlying_schema), std::move(permit),
                local_db, dk, ps, std::move(ts));
        co_return make_foreign(std::make_unique<mutation_reader>(std::move(reader)));
    });
    co_return make_foreign_reader(std::move(output_schema), std::move(permit), std::move(remote_reader));
}

class multi_range_partition_generator {
    distributed<replica::database>& _db;
    schema_ptr _schema;
    circular_buffer<dht::partition_range> _prs;
    tracing::trace_state_ptr _ts;
    db::timeout_clock::time_point _timeout;

    query::read_command _cmd;
    circular_buffer<dht::decorated_key> _dks;
private:
    query::partition_slice make_slice() {
        return partition_slice_builder(*_schema)
                .without_clustering_key_columns()
                .with_no_static_columns()
                .with_no_regular_columns()
                .with_option<query::partition_slice::option::send_partition_key>()
                .with_option<query::partition_slice::option::allow_short_read>()
                .with_option<query::partition_slice::option::bypass_cache>()
                .build();
    }

    future<> read_next_page() {
        const dht::partition_range_vector prs{_prs.front()};
        auto res = co_await query_mutations_on_all_shards(_db, _schema, _cmd, prs, _ts, _timeout);
        const auto& rr = std::get<0>(res);
        for (const auto& p : rr->partitions()) {
            auto mut = p.mut().unfreeze(_schema);
            _dks.emplace_back(mut.decorated_key());
        }
        _cmd.is_first_page = query::is_first_page::no;
        if (rr->is_short_read() || _dks.size() >= _cmd.partition_limit) {
            auto cmp = dht::ring_position_comparator(*_schema);
            if (auto r_opt = _prs.front().trim_front(dht::partition_range::bound(_dks.back(), false), cmp); r_opt) {
                _prs.front() = std::move(*r_opt);
                co_return;
            }
        }
        // fallback: range is exhausted
        _prs.pop_front();
        _cmd.query_uuid = query_id::create_random_id();
        _cmd.is_first_page = query::is_first_page::yes;
    }

    future<> fill_dk_buffer_from_current_range() {
        if (_prs.empty()) {
            co_return;
        }
        if (_prs.front().is_singular()) {
            _dks.emplace_back(_prs.front().start()->value().as_decorated_key());
            _prs.pop_front();
            co_return;
        }
        co_await read_next_page();
    }
public:
    multi_range_partition_generator(distributed<replica::database>& db, schema_ptr schema, const dht::partition_range_vector& prs,
            tracing::trace_state_ptr ts, db::timeout_clock::time_point timeout)
        : _db(db)
        , _schema(std::move(schema))
        , _ts(std::move(ts))
        , _timeout(timeout)
        , _cmd(
                _schema->id(),
                _schema->version(),
                make_slice(),
                query::max_result_size(query::result_memory_limiter::maximum_result_size),
                query::tombstone_limit(1000),
                query::row_limit::max,
                query::partition_limit(1000),
                gc_clock::now(),
                tracing::make_trace_info(_ts),
                query_id::create_random_id(),
                query::is_first_page::yes)
    {
        _prs.reserve(prs.size());
        std::copy(prs.begin(), prs.end(), std::back_inserter(_prs));
    }
    future<std::optional<dht::decorated_key>> operator()() {
        while (_dks.empty() && !_prs.empty()) {
            co_await fill_dk_buffer_from_current_range();
        }
        if (_dks.empty()) {
            co_return std::nullopt;
        }
        auto ret = std::optional(std::move(_dks.front()));
        _dks.pop_front();
        co_return std::move(ret);
    }
};

noncopyable_function<future<std::optional<dht::decorated_key>>()>
make_partition_key_generator(distributed<replica::database>& db, schema_ptr schema, const dht::partition_range_vector& prs,
        tracing::trace_state_ptr ts, db::timeout_clock::time_point timeout) {
    if (prs.size() == 1 && prs.front().is_singular()) {
        auto dk_opt = std::optional(prs.front().start()->value().as_decorated_key());
        return [dk_opt = std::move(dk_opt)] () mutable {
            return make_ready_future<std::optional<dht::decorated_key>>(std::exchange(dk_opt, std::nullopt));
        };
    }
    return multi_range_partition_generator(db, std::move(schema), prs, std::move(ts), timeout);
}

} // anonymous namespace

schema_ptr generate_output_schema_from_underlying_schema(schema_ptr underlying_schema) {
    const auto& ks = underlying_schema->ks_name();
    const auto tbl = format("{}_$mutation_fragments", underlying_schema->cf_name());
    auto sb = schema_builder(ks, tbl, generate_legacy_id(ks, tbl));
    // partition key
    for (const auto& pk_col : underlying_schema->partition_key_columns()) {
        sb.with_column(pk_col.name(), pk_col.type, column_kind::partition_key);
    }
    // clustering key
    sb.with_column("mutation_source", utf8_type, column_kind::clustering_key);
    sb.with_column("partition_region", byte_type, column_kind::clustering_key);
    for (const auto& ck_col : underlying_schema->clustering_key_columns()) {
        sb.with_column(ck_col.name(), ck_col.type, column_kind::clustering_key);
    }
    sb.with_column("position_weight", byte_type, column_kind::clustering_key);
    // regular columns
    sb.with_column("mutation_fragment_kind", utf8_type);
    sb.with_column("metadata", utf8_type);
    sb.with_column("value", utf8_type);

    md5_hasher h;
    feed_hash(h, underlying_schema->id());
    feed_hash(h, sb.uuid());
    feed_hash(h, 0); // bump this on modifications to the schema
    sb.with_version(table_schema_version(utils::UUID_gen::get_name_UUID(h.finalize())));

    return sb.build();
}

future<foreign_ptr<lw_shared_ptr<query::result>>> dump_mutations(
        sharded<database>& db,
        locator::effective_replication_map_ptr erm_keepalive,
        schema_ptr output_schema,
        schema_ptr underlying_schema,
        const dht::partition_range_vector& prs,
        const query::read_command& cmd,
        db::timeout_clock::time_point timeout) {
    // Should be enforced on the CQL level, but double-check here to be sure.
    if (cmd.slice.is_reversed()) {
        throw std::runtime_error("reverse reads are not supported");
    }
    tracing::trace_state_ptr ts;
    if (cmd.trace_info) {
        ts = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
        tracing::begin(ts);
    }

    auto permit = co_await db.local().obtain_reader_permit(underlying_schema, "mutation-dump", timeout, ts);
    auto max_result_size = cmd.max_result_size ? *cmd.max_result_size : db.local().get_query_max_result_size();
    permit.set_max_result_size(max_result_size);

    const auto opts = query::result_options::only_result();
    const auto short_read_allowed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());
    auto accounter = co_await db.local().get_result_memory_limiter().new_data_read(permit.max_result_size(), short_read_allowed);
    query_state qs(output_schema, cmd, opts, prs, std::move(accounter));

    auto compaction_state = make_lw_shared<compact_for_query_state_v2>(*output_schema, qs.cmd.timestamp, qs.cmd.slice, qs.remaining_rows(), qs.remaining_partitions());
    auto partition_key_generator = make_partition_key_generator(db, underlying_schema, prs, ts, timeout);

    auto dk_opt = co_await partition_key_generator();
    while (dk_opt) {
        auto reader_consumer = compact_for_query_v2<query_result_builder>(compaction_state, query_result_builder(*output_schema, qs.builder));
        auto reader = co_await make_partition_mutation_dump_reader(output_schema, underlying_schema, permit, db, *dk_opt, cmd.slice, ts, timeout);

        std::exception_ptr ex;
        try {
            co_await reader.consume(std::move(reader_consumer));
        } catch (...) {
            ex = std::current_exception();
        }

        co_await reader.close();
        if (ex) {
            std::rethrow_exception(std::move(ex));
        }

        if (compaction_state->are_limits_reached() || qs.builder.is_short_read()) {
            dk_opt = {};
        } else {
            dk_opt = co_await partition_key_generator();
        }
    }

    co_return make_lw_shared<query::result>(qs.builder.build(compaction_state->current_full_position()));
}

} // namespace replica::mutation_dump
