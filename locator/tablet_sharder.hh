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
#include "utils/to_string.hh"

namespace locator {

/// Implements sharder object which reflects assignment of tablets of a given table to local shards.
/// Token ranges which don't have local tablets are reported to belong to shard 0.
class tablet_sharder : public dht::sharder {
    const token_metadata& _tm;
    table_id _table;
    mutable const tablet_map* _tmap = nullptr;
    host_id _host;
private:
    // Tablet map is lazily initialized to avoid exceptions during effective_replication_map construction
    // in case tablet mapping is not yet available in token metadata at the time the table is constructed.
    void ensure_tablet_map() const {
        if (!_tmap) {
            _tmap = &_tm.tablets().get_tablet_map(_table);
        }
    }

    std::optional<unsigned> get_shard(const tablet_replica_set& replicas, host_id host) const {
        for (auto&& r : replicas) {
            if (r.host == host) {
                return r.shard;
            }
        }
        return std::nullopt;
    };

    dht::shard_replica_set shard_for_writes(tablet_id tid, host_id host, std::optional<write_replica_set_selector> sel = std::nullopt) const {
        auto* trinfo = _tmap->get_tablet_transition_info(tid);
        auto& tinfo = _tmap->get_tablet_info(tid);
        dht::shard_replica_set shards;

        auto push_from = [&](const tablet_replica_set& replicas) {
            for (auto&& r: replicas) {
                if (r.host == host) {
                    shards.push_back(r.shard);
                }
            }
        };

        // See "Shard assignment stability" from doc/dev/topology-over-raft.md for explanation of logic.

        if (trinfo && trinfo->pending_replica && trinfo->pending_replica->host == host) {
            if (trinfo->transition == tablet_transition_kind::intranode_migration) {
                switch (sel.value_or(trinfo->writes)) {
                    case write_replica_set_selector::both:
                        shards.push_back(trinfo->pending_replica->shard);
                        [[fallthrough]];
                    case write_replica_set_selector::previous:
                        push_from(tinfo.replicas);
                        break;
                    case write_replica_set_selector::next:
                        shards.push_back(trinfo->pending_replica->shard);
                        break;
                }
            } else {
                shards.push_back(trinfo->pending_replica->shard);
            }
        } else {
            push_from(tinfo.replicas);
        }

        return shards;
    }

    std::optional<shard_id> shard_for_reads(tablet_id tid, host_id host) const {
        ensure_tablet_map();
        auto* trinfo = _tmap->get_tablet_transition_info(tid);
        auto& tinfo = _tmap->get_tablet_info(tid);

        if (!trinfo) {
            return get_shard(tinfo.replicas, host);
        }

        // See "Shard assignment stability" from doc/dev/topology-over-raft.md for explanation of logic.

        if (trinfo->pending_replica && trinfo->pending_replica->host == host) {
            if (trinfo->transition == tablet_transition_kind::intranode_migration && trinfo->reads == read_replica_set_selector::previous) {
                return get_shard(tinfo.replicas, host);
            }
            return trinfo->pending_replica->shard;
        }

        return get_shard(tinfo.replicas, host);
    }
public:
    tablet_sharder(const token_metadata& tm, table_id table, std::optional<host_id> host = std::nullopt)
            : _tm(tm)
            , _table(table)
            , _host(host.value_or(tm.get_my_id()))
    { }

    virtual ~tablet_sharder() = default;

    virtual unsigned shard_for_reads(const token& t) const override {
        ensure_tablet_map();
        auto tid = _tmap->get_tablet_id(t);
        // FIXME: Consider throwing when there is no owning shard on the current host rather than returning 0.
        // It's a coordination mistake to route requests to non-owners. Topology coordinator should synchronize
        // with request coordinators before moving the shard away.
        auto shard = shard_for_reads(tid, _host).value_or(0);
        tablet_logger.trace("[{}] shard_of({}) = {}, tablet={}", _table, t, shard, tid);
        return shard;
    }

    virtual dht::shard_replica_set shard_for_writes(const token& t, std::optional<write_replica_set_selector> sel = std::nullopt) const override {
        ensure_tablet_map();
        auto tid = _tmap->get_tablet_id(t);
        auto shards = shard_for_writes(tid, _host, sel);
        tablet_logger.trace("[{}] shard_for_writes({}) = {}, tablet={}", _table, t, shards, tid);
        return shards;
    }

    virtual std::optional<dht::shard_and_token> next_shard_for_reads(const token& t) const override {
        ensure_tablet_map();
        std::optional<tablet_id> tb = _tmap->get_tablet_id(t);
        while ((tb = _tmap->next_tablet(*tb))) {
            auto r = shard_for_reads(*tb, _host);
            auto next = _tmap->get_first_token(*tb);
            tablet_logger.trace("[{}] token_for_next_shard({}) = {{{}, {}}}, tablet={}", _table, t, next, r, *tb);
            return dht::shard_and_token{r.value_or(0), next};
        }
        tablet_logger.trace("[{}] token_for_next_shard({}) = null", _table, t);
        return std::nullopt;
    }

    virtual token token_for_next_shard_for_reads(const token& t, shard_id shard, unsigned spans = 1) const override {
        ensure_tablet_map();
        auto token = t;
        while (auto s_a_t = next_shard_for_reads(token)) {
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
