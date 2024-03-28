/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database.hh"

namespace dht {

/// A sharder which uses table's most up to date effective_replication_map in each invocation.
/// As a result, the time during which a single token metadata version is kept is reduced.
/// The purpose of this class is having a sharder which can be passed to long-running processes
/// which do not need a single view on replication for the whole duration of the process,
/// and do not need to block topology change barriers.
///
/// No guarantees are made about how long token metadata version is kept alive.
/// It may be released before the next call.
class auto_refreshing_sharder : public dht::sharder {
    lw_shared_ptr<replica::table> _table;
    locator::effective_replication_map_ptr _erm;
    const dht::sharder* _sharder;
    optimized_optional<seastar::abort_source::subscription> _callback;
    std::optional<write_replica_set_selector> _sel;
private:
    void refresh() {
        _erm = _table->get_effective_replication_map();
        _sharder = &_erm->get_sharder(*_table->schema());
        _callback = _erm->get_validity_abort_source().subscribe([this] () noexcept {
            refresh();
        });
    }
public:
    auto_refreshing_sharder(lw_shared_ptr<replica::table> table, std::optional<write_replica_set_selector> sel = std::nullopt)
        : _table(std::move(table))
        , _sel(sel)
    {
        refresh();
    }

    virtual ~auto_refreshing_sharder() = default;

    virtual unsigned shard_for_reads(const token& t) const override {
        return _sharder->shard_for_reads(t);
    }

    virtual dht::shard_replica_set shard_for_writes(const token& t, std::optional<write_replica_set_selector> sel) const override {
        if (!sel) {
            sel = _sel;
        }
        return _sharder->shard_for_writes(t, sel);
    }

    virtual std::optional<dht::shard_and_token> next_shard_for_reads(const dht::token& t) const override {
        return _sharder->next_shard_for_reads(t);
    }

    virtual dht::token token_for_next_shard_for_reads(const dht::token& t, shard_id shard, unsigned spans = 1) const override {
        return _sharder->token_for_next_shard_for_reads(t, shard, spans);
    }
};

} // namespace dht
