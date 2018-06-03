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
#include "flat_mutation_reader.hh"
#include "mutation_fragment.hh"
#include "mutation_reader.hh"
#include "query-request.hh"
#include "schema.hh"
#include "secondary_index_manager.hh"
#include "tracing/tracing.hh"
#include "view_info.hh"

#include <memory>

namespace db::index {

class built_indexes_virtual_reader {
    database& _db;

    struct built_indexes_reader : flat_mutation_reader::impl {
        database& _db;
        flat_mutation_reader _underlying;
        sstring _current_keyspace;

        built_indexes_reader(
                database& db,
                schema_ptr schema,
                column_family& built_views,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding fwd_mr)
                : flat_mutation_reader::impl(std::move(schema))
                , _db(db)
                , _underlying(built_views.make_reader(
                        built_views.schema(),
                        range,
                        slice,
                        pc,
                        std::move(trace_state),
                        fwd,
                        fwd_mr)) {
        }

        bool is_index(const clustering_row& built_view_row) const {
            auto view_name = value_cast<sstring>(utf8_type->deserialize(*built_view_row.key().begin(*_underlying.schema())));
            try {
                auto s = _db.find_schema(_current_keyspace, view_name);
                return s->is_view() && _db.find_column_family(s->view_info()->base_id()).get_index_manager().is_index(view_ptr(s));
            } catch (const no_such_column_family&) {
                return false;
            }
        }

        virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
            return _underlying.fill_buffer(timeout).then([this] {
                _end_of_stream = _underlying.is_end_of_stream();
                while (!_underlying.is_buffer_empty()) {
                    auto mf = _underlying.pop_mutation_fragment();
                    if (mf.is_partition_start()) {
                        _current_keyspace = value_cast<sstring>(utf8_type->deserialize(*mf.as_partition_start().key().key().begin(*_underlying.schema())));
                    } else if (mf.is_clustering_row() && !is_index(mf.as_clustering_row())) {
                        continue;
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
    built_indexes_virtual_reader(database& db)
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
        return make_flat_mutation_reader<built_indexes_reader>(
                _db,
                std::move(s),
                _db.find_column_family(s->ks_name(), system_keyspace::v3::BUILT_VIEWS),
                range,
                slice,
                pc,
                std::move(trace_state),
                fwd,
                fwd_mr);
    }
};

}
