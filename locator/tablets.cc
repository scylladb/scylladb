/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/tablet_replication_strategy.hh"
#include "locator/tablets.hh"
#include "types/types.hh"
#include "types/tuple.hh"
#include "types/set.hh"
#include "utils/hash.hh"
#include "db/system_keyspace.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "replica/database.hh"
#include "utils/stall_free.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

namespace locator {

seastar::logger tablet_logger("tablets");

const tablet_map& tablet_metadata::get_tablet_map(table_id id) const {
    try {
        return _tablets.at(id);
    } catch (const std::out_of_range&) {
        throw std::runtime_error(format("Tablet map not found for table {}", id));
    }
}

tablet_map& tablet_metadata::get_tablet_map(table_id id) {
    return const_cast<tablet_map&>(
        const_cast<const tablet_metadata*>(this)->get_tablet_map(id));
}

void tablet_metadata::set_tablet_map(table_id id, tablet_map map) {
    _tablets.insert_or_assign(id, std::move(map));
}

future<> tablet_metadata::clear_gently() {
    for (auto&& [id, map] : _tablets) {
        co_await map.clear_gently();
    }
    co_return;
}

tablet_map::tablet_map(size_t tablet_count)
        : _log2_tablets(log2ceil(tablet_count)) {
    if (tablet_count != 1ul << _log2_tablets) {
        on_internal_error(tablet_logger, format("Tablet count not a power of 2: {}", tablet_count));
    }
    _tablets.resize(tablet_count);
}

void tablet_map::check_tablet_id(tablet_id id) const {
    if (size_t(id) >= tablet_count()) {
        throw std::logic_error(format("Invalid tablet id: {} >= {}", id, tablet_count()));
    }
}

const tablet_info& tablet_map::get_tablet_info(tablet_id id) const {
    check_tablet_id(id);
    return _tablets[size_t(id)];
}

tablet_id tablet_map::get_tablet_id(token t) const {
    return tablet_id(dht::compaction_group_of(_log2_tablets, t));
}

dht::token tablet_map::get_last_token(tablet_id id) const {
    check_tablet_id(id);
    return dht::last_token_of_compaction_group(_log2_tablets, size_t(id));
}

dht::token tablet_map::get_first_token(tablet_id id) const {
    if (id == first_tablet()) {
        return dht::first_token();
    } else {
        return dht::next_token(get_last_token(tablet_id(size_t(id) - 1)));
    }
}

dht::token_range tablet_map::get_token_range(tablet_id id) const {
    if (id == first_tablet()) {
        return dht::token_range::make({dht::minimum_token(), false}, {get_last_token(id), true});
    } else {
        return dht::token_range::make({get_last_token(tablet_id(size_t(id) - 1)), false}, {get_last_token(id), true});
    }
}

void tablet_map::set_tablet(tablet_id id, tablet_info info) {
    check_tablet_id(id);
    _tablets[size_t(id)] = std::move(info);
}

void tablet_map::set_tablet_transition_info(tablet_id id, tablet_transition_info info) {
    check_tablet_id(id);
    _transitions.insert_or_assign(id, std::move(info));
}

future<> tablet_map::clear_gently() {
    return utils::clear_gently(_tablets);
}

const tablet_transition_info* tablet_map::get_tablet_transition_info(tablet_id id) const {
    auto i = _transitions.find(id);
    if (i == _transitions.end()) {
        return nullptr;
    }
    return &i->second;
}

std::ostream& operator<<(std::ostream& out, tablet_id id) {
    return out << size_t(id);
}

std::ostream& operator<<(std::ostream& out, const tablet_replica& r) {
    return out << r.host << ":" << r.shard;
}

std::ostream& operator<<(std::ostream& out, const tablet_map& r) {
    if (r.tablet_count() == 0) {
        return out << "{}";
    }
    out << "{";
    bool first = true;
    tablet_id tid = r.first_tablet();
    for (auto&& tablet : r._tablets) {
        if (!first) {
            out << ",";
        }
        out << format("\n    [{}]: last_token={}, replicas={}", tid, r.get_last_token(tid), tablet.replicas);
        if (auto tr = r.get_tablet_transition_info(tid)) {
            out << format(", new_replicas={}, pending={}", tr->next, tr->pending_replica);
        }
        first = false;
        tid = *r.next_tablet(tid);
    }
    return out << "\n  }";
}

std::ostream& operator<<(std::ostream& out, const tablet_metadata& tm) {
    out << "{";
    bool first = true;
    for (auto&& [id, map] : tm._tablets) {
        if (!first) {
            out << ",";
        }
        out << "\n  " << id << ": " << map;
        first = false;
    }
    return out << "\n}";
}

size_t tablet_map::external_memory_usage() const {
    size_t result = _tablets.external_memory_usage();
    for (auto&& tablet : _tablets) {
        result += tablet.replicas.external_memory_usage();
    }
    return result;
}

// Estimates the external memory usage of std::unordered_map<>.
// Does not include external memory usage of elements.
template <typename K, typename V>
static size_t estimate_external_memory_usage(const std::unordered_map<K, V>& map) {
    return map.bucket_count() * sizeof(void*) + map.size() * (sizeof(std::pair<const K, V>) + 8);
}

size_t tablet_metadata::external_memory_usage() const {
    size_t result = estimate_external_memory_usage(_tablets);
    for (auto&& [id, map] : _tablets) {
        result += map.external_memory_usage();
    }
    return result;
}

}
