/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/token-sharding.hh"
#include "locator/tablets.hh"
#include "locator/token_metadata.hh"

namespace locator {

/// Implements sharder object which reflects assignment of tablets of a given table to local shards.
/// Token ranges which don't have local tablets are reported to belong to shard 0.
class tablet_sharder : public dht::sharder {
    const token_metadata& _tm;
    table_id _table;
    mutable const tablet_map* _tmap = nullptr;
private:
    // Tablet map is lazily initialized to avoid exceptions during effective_replication_map construction
    // in case tablet mapping is not yet available in token metadata at the time the table is constructed.
    void ensure_tablet_map() const {
        if (!_tmap) {
            _tmap = &_tm.tablets().get_tablet_map(_table);
        }
    }
public:
    tablet_sharder(const token_metadata& tm, table_id table)
            : _tm(tm)
            , _table(table)
    { }

    virtual ~tablet_sharder() = default;

    virtual unsigned shard_of(const dht::token& token) const override {
        ensure_tablet_map();
        auto tid = _tmap->get_tablet_id(token);
        auto shard = _tmap->get_shard(tid, _tm.get_my_id());
        tablet_logger.trace("[{}] shard_of({}) = {}, tablet={}", _table, token, shard, tid);
        return shard.value_or(0);
    }

    virtual std::optional<dht::shard_and_token> next_shard(const token& t) const override {
        ensure_tablet_map();
        auto me = _tm.get_my_id();
        std::optional<tablet_id> tb = _tmap->get_tablet_id(t);
        while ((tb = _tmap->next_tablet(*tb))) {
            auto r = _tmap->get_shard(*tb, me);
            auto next = _tmap->get_first_token(*tb);
            tablet_logger.trace("[{}] token_for_next_shard({}) = {{{}, {}}}, tablet={}", _table, t, next, r, *tb);
            return dht::shard_and_token{r.value_or(0), next};
        }
        tablet_logger.trace("[{}] token_for_next_shard({}) = null", _table, t);
        return std::nullopt;
    }

    virtual token token_for_next_shard(const token& t, shard_id shard, unsigned spans = 1) const override {
        ensure_tablet_map();
        auto token = t;
        while (auto s_a_t = next_shard(token)) {
            token = s_a_t->token;
            if (s_a_t->shard == shard) {
                if (--spans == 0) {
                    tablet_logger.trace("[{}] token_for_next_shard({}, {}, {}) = {}", _table, t, shard, spans, s_a_t->token);
                    return token;
                }
            }
        }
        tablet_logger.trace("[{}] token_for_next_shard({}, {}, {}) = null", _table, t, shard, spans);
        return dht::maximum_token();
    }
};

} // namespace locator
