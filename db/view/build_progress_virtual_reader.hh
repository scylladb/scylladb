/*
 * Copyright (C) 2018 ScyllaDB
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

#include "database.hh"
#include "db/system_keyspace.hh"
#include "db/timeout_clock.hh"
#include "dht/i_partitioner.hh"
#include "flat_mutation_reader.hh"
#include "mutation_fragment.hh"
#include "mutation_reader.hh"
#include "query-request.hh"
#include "schema.hh"
#include "tracing/tracing.hh"

#include <boost/range/iterator_range.hpp>

#include <iterator>
#include <memory>

namespace db::view {

// Allows a user to query the views_builds_in_progress system table
// in terms of the scylla_views_builds_in_progress one, which is
// a superset of the former. When querying, we don't have to adjust
// the clustering key, but we have to adjust the requested regular
// columns. When reading the results from the scylla_views_builds_in_progress
// table, we adjust the clustering key (we shed the cpu_id column) and map
// back the regular columns.
class build_progress_virtual_reader {
    database& _db;

    struct build_progress_reader : flat_mutation_reader::impl {
        column_id _scylla_next_token_col;
        column_id _scylla_generation_number_col;
        column_id _legacy_last_token_col;
        column_id _legacy_generation_number_col;
        const query::partition_slice& _legacy_slice;
        query::partition_slice _slice;
        flat_mutation_reader _underlying;

        build_progress_reader(
                schema_ptr legacy_schema,
                column_family& scylla_views_build_progress,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding fwd_mr)
                : flat_mutation_reader::impl(std::move(legacy_schema))
                , _scylla_next_token_col(scylla_views_build_progress.schema()->get_column_definition("next_token")->id)
                , _scylla_generation_number_col(scylla_views_build_progress.schema()->get_column_definition("generation_number")->id)
                , _legacy_last_token_col(_schema->get_column_definition("last_token")->id)
                , _legacy_generation_number_col(_schema->get_column_definition("generation_number")->id)
                , _legacy_slice(slice)
                , _slice(adjust_partition_slice())
                , _underlying(scylla_views_build_progress.make_reader(
                        scylla_views_build_progress.schema(),
                        range,
                        slice,
                        pc,
                        std::move(trace_state),
                        fwd,
                        fwd_mr)) {
        }

        const schema& underlying_schema() const {
            return *_underlying.schema();
        }

        query::partition_slice adjust_partition_slice() {
            auto slice = _legacy_slice;
            std::vector<column_id> adjusted_columns;
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

        virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
            return _underlying.fill_buffer(timeout).then([this] {
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
                        mf = clustering_row(
                                adjust_ckey(scylla_in_progress_row.key()),
                                std::move(scylla_in_progress_row.tomb()),
                                std::move(scylla_in_progress_row.marker()),
                                std::move(legacy_in_progress_row));
                    } else if (mf.is_range_tombstone()) {
                        auto scylla_in_progress_rt = std::move(mf).as_range_tombstone();
                        mf = range_tombstone(
                                adjust_ckey(scylla_in_progress_rt.start),
                                scylla_in_progress_rt.start_kind,
                                adjust_ckey(scylla_in_progress_rt.end),
                                scylla_in_progress_rt.end_kind,
                                scylla_in_progress_rt.tomb);
                    }
                    push_mutation_fragment(std::move(mf));
                }
            });
        }

        virtual void next_partition() override {
            _end_of_stream = false;
            clear_buffer_to_next_partition();
            if (is_buffer_empty()) {
                _underlying.next_partition();
            }
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            clear_buffer();
            _end_of_stream = false;
            return _underlying.fast_forward_to(pr, timeout);
        }

        virtual future<> fast_forward_to(position_range range, db::timeout_clock::time_point timeout) override {
            forward_buffer_to(range.start());
            _end_of_stream = false;
            return _underlying.fast_forward_to(std::move(range), timeout);
        }
    };

public:
    build_progress_virtual_reader(database& db)
            : _db(db) {
    }

    flat_mutation_reader operator()(
            schema_ptr s,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        return flat_mutation_reader(std::make_unique<build_progress_reader>(
                std::move(s),
                _db.find_column_family(s->ks_name(), system_keyspace::v3::SCYLLA_VIEWS_BUILDS_IN_PROGRESS),
                range,
                slice,
                pc,
                std::move(trace_state),
                fwd,
                fwd_mr));
    }
};

}