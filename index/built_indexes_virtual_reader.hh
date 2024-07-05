/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database.hh"
#include "db/system_keyspace.hh"
#include "readers/mutation_reader.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "query-request.hh"
#include "schema/schema_fwd.hh"
#include "secondary_index_manager.hh"
#include "view_info.hh"

#include <memory>

namespace tracing { class trace_state_ptr; }

namespace db::index {

class built_indexes_virtual_reader {
    replica::database& _db;

    struct built_indexes_reader : mutation_reader::impl {
        replica::database& _db;
        schema_ptr _schema;
        query::partition_slice _view_names_slice;
        mutation_reader _underlying;
        sstring _current_keyspace;

        // Convert a key holding an index name (e.g., xyz) to a key holding
        // the backing materialized-view name (e.g., xyz_index).
        static clustering_key_prefix index_name_to_view_name(
                const clustering_key_prefix& c,
                const schema& built_views_schema,
                const schema& built_indexes_schema)
        {
            sstring index_name = value_cast<sstring>(utf8_type->deserialize(*c.begin(built_indexes_schema)));
            sstring view_name = ::secondary_index::index_table_name(index_name);
            return clustering_key_prefix::from_single_value(built_views_schema, utf8_type->decompose(view_name));
        }

        // Convert a clustering_range holding a range of index names (e.g.,
        // abc..xyz) to a range of backing view names (abc_index..xyz_index).
        static query::clustering_range index_name_range_to_view_name_range(
                const query::clustering_range& range,
                const schema& built_views_schema,
                const schema& built_indexes_schema)
        {
            bool singular = range.is_singular();
            auto start = range.start();
            auto end = range.end();
            if (start) {
                *start = query::clustering_range::bound(
                    index_name_to_view_name(start->value(), built_views_schema, built_indexes_schema),
                    start->is_inclusive());
            }
            if (end) {
                *end = query::clustering_range::bound(
                    index_name_to_view_name(end->value(), built_views_schema, built_indexes_schema),
                    end->is_inclusive());
            }
            return query::clustering_range(std::move(start), std::move(end), singular);
        }

        // The index list reader is created with a slice of index names
        // (e.g., xyz) but the underlying reader needs a slice of view names,
        // e.g., xyz_index, so we need to add the _index suffix with
        // index_table_name().
        static query::partition_slice index_slice_to_view_slice(
                const query::partition_slice& slice,
                const schema& built_views_schema,
                const schema& built_indexes_schema)
        {
            // Most of the slice - such as options and which columns to read -
            // should be copied unchanged. Just the clustering row ranges need
            // changing.
            // Fix _row_ranges (used for WHERE specifying a clustering range):
            query::partition_slice ret = slice;
            for (query::clustering_range &range : ret._row_ranges) {
                range = index_name_range_to_view_name_range(range, built_views_schema, built_indexes_schema);
            }
            // Fix _specific_ranges (used for continuing on paging).
            auto& specific_ranges = ret.get_specific_ranges();
            if (specific_ranges) {
                partition_key pk = specific_ranges->pk();
                std::vector<query::clustering_range> ranges;
                for (const query::clustering_range &range : specific_ranges->ranges()) {
                    ranges.push_back(index_name_range_to_view_name_range(range, built_views_schema, built_indexes_schema));
                }
                ret.clear_ranges();
                ret.set_range(built_indexes_schema, pk, ranges);
            }
            return ret;
        }

        built_indexes_reader(
                replica::database& db,
                schema_ptr schema,
                reader_permit permit,
                replica::column_family& built_views,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding fwd_mr)
                : mutation_reader::impl(schema, permit)
                , _db(db)
                , _schema(std::move(schema))
                , _view_names_slice(index_slice_to_view_slice(slice, *built_views.schema(), *_schema))
                , _underlying(built_views.make_reader_v2(
                        built_views.schema(),
                        std::move(permit),
                        range,
                        _view_names_slice,
                        std::move(trace_state),
                        fwd,
                        fwd_mr)) {
        }

        bool is_index(const clustering_row& built_view_row) const {
            auto view_name = value_cast<sstring>(utf8_type->deserialize(*built_view_row.key().begin(*_underlying.schema())));
            try {
                auto s = _db.find_schema(_current_keyspace, view_name);
                return s->is_view() && _db.find_column_family(s->view_info()->base_id()).get_index_manager().is_index(view_ptr(s));
            } catch (const replica::no_such_column_family&) {
                return false;
            }
        }

        virtual future<> fill_buffer() override {
            return _underlying.fill_buffer().then([this] {
                _end_of_stream = _underlying.is_end_of_stream();
                while (!_underlying.is_buffer_empty()) {
                    auto mf = _underlying.pop_mutation_fragment();
                    if (mf.is_partition_start()) {
                        _current_keyspace = value_cast<sstring>(utf8_type->deserialize(*mf.as_partition_start().key().key().begin(*_underlying.schema())));
                    } else if (mf.is_clustering_row()) {
                        if (is_index(mf.as_clustering_row())) {
                            // mf's clustering key is the materialized view's
                            // name, xyz_index, while we need the index name
                            // xyz. So modify mf:
                            mf.mutate_as_clustering_row(*_underlying.schema(),
                                [&] (clustering_row& cr) mutable {
                                    sstring view_name = value_cast<sstring>(utf8_type->deserialize(*cr.key().begin(*_underlying.schema())));
                                    sstring index_name = ::secondary_index::index_name_from_table_name(view_name);
                                    cr.key() = clustering_key_prefix::from_single_value(*_underlying.schema(), utf8_type->decompose(index_name));
                                });
                        } else {
                            continue;
                        }
                    }
                    push_mutation_fragment(std::move(mf));
                }
            });
        }

        virtual future<> next_partition() override {
            _end_of_stream = false;
            clear_buffer_to_next_partition();
            if (is_buffer_empty()) {
                return _underlying.next_partition();
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            return _underlying.fast_forward_to(pr);
        }

        virtual future<> fast_forward_to(position_range range) override {
            clear_buffer();
            _end_of_stream = false;
            // range contains index names (e.g., xyz) but the underlying table
            // contains view names (e.g., xyz_index) so we need to add the
            // _index suffix with index_table_name():
            position_in_partition start = range.start();
            position_in_partition end = range.end();
            if (start.get_clustering_key_prefix()) {
                clustering_key_prefix ck = index_name_to_view_name(
                    *start.get_clustering_key_prefix(),
                    *_underlying.schema(), *_schema);
                start = position_in_partition(start.region(),
                    start.get_bound_weight(),
                    std::move(ck));
            }
            if (end.get_clustering_key_prefix()) {
                clustering_key_prefix ck = index_name_to_view_name(
                    *end.get_clustering_key_prefix(),
                    *_underlying.schema(), *_schema);
                start = position_in_partition(end.region(),
                    end.get_bound_weight(),
                    std::move(ck));
            }
            range = position_range(std::move(start), std::move(end));
            return _underlying.fast_forward_to(std::move(range));
        }

        virtual future<> close() noexcept override {
            return _underlying.close();
        }
    };

public:
    built_indexes_virtual_reader(replica::database& db)
            : _db(db) {
    }

    mutation_reader operator()(
            schema_ptr s,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        return make_mutation_reader<built_indexes_reader>(
                _db,
                s,
                std::move(permit),
                _db.find_column_family(s->ks_name(), system_keyspace::v3::BUILT_VIEWS),
                range,
                slice,
                std::move(trace_state),
                fwd,
                fwd_mr);
    }
};

}
