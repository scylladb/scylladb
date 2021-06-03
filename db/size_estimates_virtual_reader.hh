/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "mutation_reader.hh"
#include "db/system_keyspace.hh"

class database;

namespace db {

namespace size_estimates {

struct token_range {
    bytes start;
    bytes end;
};

class size_estimates_mutation_reader final : public flat_mutation_reader::impl {
    database& _db;
    const dht::partition_range* _prange;
    const query::partition_slice& _slice;
    using ks_range = std::vector<sstring>;
    std::optional<ks_range> _keyspaces;
    ks_range::const_iterator _current_partition;
    streamed_mutation::forwarding _fwd;
    flat_mutation_reader_opt _partition_reader;
public:
    size_estimates_mutation_reader(database& db, schema_ptr, reader_permit, const dht::partition_range&, const query::partition_slice&, streamed_mutation::forwarding);

    virtual future<> fill_buffer(db::timeout_clock::time_point) override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override;
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override;
    virtual future<> close() noexcept override;
private:
    future<> get_next_partition();
    future<> close_partition_reader() noexcept;

    std::vector<db::system_keyspace::range_estimates>
    estimates_for_current_keyspace(std::vector<token_range> local_ranges) const;
};

struct virtual_reader {
    database& db;

    flat_mutation_reader operator()(schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        return make_flat_mutation_reader<size_estimates_mutation_reader>(db, std::move(schema), std::move(permit), range, slice, fwd);
    }

    virtual_reader(database& db_) noexcept : db(db_) {}
};

future<std::vector<token_range>> test_get_local_ranges(database& db);

} // namespace size_estimates

} // namespace db
