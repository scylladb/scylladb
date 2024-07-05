/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "db/system_keyspace.hh"
#include "tracing/trace_state.hh"

namespace replica {
class database;
}

namespace db {

namespace size_estimates {

struct token_range {
    bytes start;
    bytes end;
};

class size_estimates_mutation_reader final : public mutation_reader::impl {
    replica::database& _db;
    db::system_keyspace& _sys_ks;
    const dht::partition_range* _prange;
    const query::partition_slice& _slice;
    using ks_range = std::vector<sstring>;
    std::optional<ks_range> _keyspaces;
    ks_range::const_iterator _current_partition;
    streamed_mutation::forwarding _fwd;
    mutation_reader_opt _partition_reader;
public:
    size_estimates_mutation_reader(replica::database& db, db::system_keyspace& sys_ks, schema_ptr, reader_permit, const dht::partition_range&, const query::partition_slice&, streamed_mutation::forwarding);

    virtual future<> fill_buffer() override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range&) override;
    virtual future<> fast_forward_to(position_range) override;
    virtual future<> close() noexcept override;
private:
    future<> get_next_partition();
    future<> close_partition_reader() noexcept;

    std::vector<db::system_keyspace::range_estimates>
    estimates_for_current_keyspace(std::vector<token_range> local_ranges) const;
};

struct virtual_reader {
    replica::database& db;
    db::system_keyspace& sys_ks;

    mutation_reader operator()(schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        return make_mutation_reader<size_estimates_mutation_reader>(db, sys_ks, std::move(schema), std::move(permit), range, slice, fwd);
    }

    virtual_reader(replica::database& db_, db::system_keyspace& sys_ks_) noexcept : db(db_), sys_ks(sys_ks_) {}
};

future<std::vector<token_range>> test_get_local_ranges(replica::database& db, db::system_keyspace& sys_ks);

} // namespace size_estimates

} // namespace db
