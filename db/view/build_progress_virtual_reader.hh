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
#include "mutation/mutation_fragment.hh"
#include "query-request.hh"
#include "schema/schema_fwd.hh"

#include <boost/range/iterator_range.hpp>

#include <iterator>
#include <memory>

namespace tracing { class trace_state_ptr; }

namespace db::view {

// Allows a user to query the views_builds_in_progress system table
// in terms of the scylla_views_builds_in_progress one, which is
// a superset of the former. When querying, we don't have to adjust
// the clustering key, but we have to adjust the requested regular
// columns. When reading the results from the scylla_views_builds_in_progress
// table, we adjust the clustering key (we shed the cpu_id column) and map
// back the regular columns.
// Since mutation fragment consumers expect clustering_row fragments
// not to be duplicated for given primary key, previous clustering key
// is stored between mutation fragments. If the clustering key becomes
// the same as the previous one (as a result of trimming cpu_id),
// the duplicated fragment is ignored.
class build_progress_virtual_reader {
    replica::database& _db;

    struct build_progress_reader : mutation_reader::impl {
        column_id _scylla_next_token_col;
        column_id _scylla_generation_number_col;
        column_id _legacy_last_token_col;
        column_id _legacy_generation_number_col;
        const query::partition_slice& _legacy_slice;
        query::partition_slice _slice;
        mutation_reader _underlying;
        std::optional<clustering_key> _previous_clustering_key;

        build_progress_reader(
                schema_ptr legacy_schema,
                reader_permit permit,
                replica::column_family& scylla_views_build_progress,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding fwd_mr)
                : mutation_reader::impl(std::move(legacy_schema), permit)
                , _scylla_next_token_col(scylla_views_build_progress.schema()->get_column_definition("next_token")->id)
                , _scylla_generation_number_col(scylla_views_build_progress.schema()->get_column_definition("generation_number")->id)
                , _legacy_last_token_col(_schema->get_column_definition("last_token")->id)
                , _legacy_generation_number_col(_schema->get_column_definition("generation_number")->id)
                , _legacy_slice(slice)
                , _slice(adjust_partition_slice())
                , _underlying(scylla_views_build_progress.make_reader_v2(
                        scylla_views_build_progress.schema(),
                        std::move(permit),
                        range,
                        slice,
                        std::move(trace_state),
                        fwd,
                        fwd_mr))
                , _previous_clustering_key() {
        }

        const schema& underlying_schema() const {
            return *_underlying.schema();
        }

        query::partition_slice adjust_partition_slice() {
            auto slice = _legacy_slice;
            query::column_id_vector adjusted_columns;
            for (auto col_id : slice.regular_columns) {
                if (col_id == _legacy_last_token_col) {
                    adjusted_columns.push_back(_scylla_next_token_col);
                } else if (col_id == _legacy_generation_number_col) {
                    adjusted_columns.push_back(_scylla_generation_number_col);
                }
            }
            slice.regular_columns = std::move(adjusted_columns);
            return slice;
        }

        clustering_key adjust_ckey(clustering_key& underlying_ck) {
            if (!underlying_ck.is_full(underlying_schema())) {
                return std::move(underlying_ck);
            }
            // Drop the cpu_id from the clustering key
            auto end = underlying_ck.begin(underlying_schema());
            std::advance(end, underlying_schema().clustering_key_size() - 1);
            auto r = boost::make_iterator_range(underlying_ck.begin(underlying_schema()), std::move(end));
            return clustering_key_prefix::from_exploded(r);
        }

        position_in_partition adjust_ckey(position_in_partition&& underlying_pip) {
            auto underlying_ck_opt = underlying_pip.get_clustering_key_prefix();
            if (!underlying_ck_opt || !underlying_ck_opt->is_full(underlying_schema())) {
                return std::move(underlying_pip);
            }
            return position_in_partition(underlying_pip.region(),
                                         underlying_pip.get_bound_weight(),
                                         adjust_ckey(*underlying_ck_opt));
        }

        virtual future<> fill_buffer() override {
            return _underlying.fill_buffer().then([this] {
                _end_of_stream = _underlying.is_end_of_stream();
                while (!_underlying.is_buffer_empty()) {
                    auto mf = _underlying.pop_mutation_fragment();
                    if (mf.is_clustering_row()) {
                        auto scylla_in_progress_row = std::move(mf).as_clustering_row();
                        auto legacy_in_progress_row = row();
                        // Drop the first_token from the regular columns
                        scylla_in_progress_row.cells().for_each_cell([&, this] (column_id id, atomic_cell_or_collection& c) {
                            if (id == _scylla_next_token_col) {
                                legacy_in_progress_row.append_cell(_legacy_last_token_col, std::move(c));
                            } else if (id == _scylla_generation_number_col) {
                                legacy_in_progress_row.append_cell(_legacy_generation_number_col, std::move(c));
                            }
                        });
                        auto ck = adjust_ckey(scylla_in_progress_row.key());
                        if (_previous_clustering_key && ck.equal(*_schema, *_previous_clustering_key)) {
                            continue;
                        }
                        _previous_clustering_key = ck;
                        mf = mutation_fragment_v2(*_schema, _permit, clustering_row(
                                std::move(ck),
                                scylla_in_progress_row.tomb(),
                                std::move(scylla_in_progress_row.marker()),
                                std::move(legacy_in_progress_row)));
                    } else if (mf.is_range_tombstone_change()) {
                        auto scylla_in_progress_rtc = std::move(mf).as_range_tombstone_change();
                        auto ts = scylla_in_progress_rtc.tombstone();
                        auto pos = std::move(scylla_in_progress_rtc).position();
                        mf = mutation_fragment_v2(*_schema, _permit,
                                range_tombstone_change(adjust_ckey(std::move(pos)), ts));
                    } else if (mf.is_end_of_partition()) {
                        _previous_clustering_key.reset();
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
            return _underlying.fast_forward_to(std::move(range));
        }

        virtual future<> close() noexcept override {
            return _underlying.close();
        }
    };

public:
    build_progress_virtual_reader(replica::database& db)
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
        return mutation_reader(std::make_unique<build_progress_reader>(
                s,
                std::move(permit),
                _db.find_column_family(s->ks_name(), system_keyspace::v3::SCYLLA_VIEWS_BUILDS_IN_PROGRESS),
                range,
                slice,
                std::move(trace_state),
                fwd,
                fwd_mr));
    }
};

}
