/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "dht/token-sharding.hh"
#include "utils/exceptions.hh"
#include "exceptions/exceptions.hh"

#include "cdc/generation.hh"
#include "cdc/metadata.hh"

extern logging::logger cdc_log;

namespace cdc {
    extern const api::timestamp_clock::duration generation_leeway;
} // namespace cdc

static api::timestamp_type to_ts(db_clock::time_point tp) {
    // This assumes that timestamp_clock and db_clock have the same epochs.
    return std::chrono::duration_cast<api::timestamp_clock::duration>(tp.time_since_epoch()).count();
}

static cdc::stream_id get_stream(
        const cdc::token_range_description& entry,
        dht::token tok) {
    // The ith stream is the stream for the ith shard.
    auto shard_cnt = entry.streams.size();
    auto shard_id = dht::shard_of(shard_cnt, entry.sharding_ignore_msb, tok);

    if (shard_id >= shard_cnt) {
        on_internal_error(cdc_log, "get_stream: shard_id out of bounds");
    }

    return entry.streams[shard_id];
}

// non-static for testing
cdc::stream_id get_stream(
        const std::vector<cdc::token_range_description>& entries,
        dht::token tok) {
    if (entries.empty()) {
        on_internal_error(cdc_log, "get_stream: entries empty");
    }

    auto it = std::lower_bound(entries.begin(), entries.end(), tok,
            [] (const cdc::token_range_description& e, dht::token t) { return e.token_range_end < t; });
    if (it == entries.end()) {
        it = entries.begin();
    }

    return get_stream(*it, tok);
}

cdc::metadata::container_t::const_iterator cdc::metadata::gen_used_at(api::timestamp_type ts) const {
    auto it = _gens.upper_bound(ts);
    if (it == _gens.begin()) {
        // All known generations have higher timestamps than `ts`.
        return _gens.end();
    }

    return std::prev(it);
}

bool cdc::metadata::streams_available() const {
    auto now = api::new_timestamp();
    auto it = gen_used_at(now);
    return  it != _gens.end();
}

cdc::stream_id cdc::metadata::get_stream(api::timestamp_type ts, dht::token tok) {
    auto now = api::new_timestamp();
    if (ts > now + generation_leeway.count()) {
        throw exceptions::invalid_request_exception(format(
                "cdc: attempted to get a stream \"from the future\" ({}; current server time: {})."
                " With CDC you cannot send writes with timestamps arbitrarily into the future, because we don't"
                " know what streams will be used at that time.\n"
                "We *do* allow sending writes into the near future, but our ability to do that is limited."
                " If you really must use your own timestamps, then make sure your clocks are well-synchronized"
               "  with the database's clocks.", format_timestamp(ts), format_timestamp(now)));
        // Note that we might still send a write to a wrong generation, if we learn about the current
        // generation too late (we might think that an earlier generation is the current one).
        // Nothing protects us from that until we start using transactions for generation switching.
    }

    auto it = gen_used_at(now);
    if (it == _gens.end()) {
        throw std::runtime_error(format(
                "cdc::metadata::get_stream: could not find any CDC stream (current time: {})."
                " Are we in the middle of a cluster upgrade?", format_timestamp(now)));
    }

    // Garbage-collect generations that will no longer be used.
    it = _gens.erase(_gens.begin(), it);

    if (it->first > ts) {
        throw exceptions::invalid_request_exception(format(
                "cdc: attempted to get a stream from an earlier generation than the currently used one."
                " With CDC you cannot send writes with timestamps too far into the past, because that would break"
                " consistency properties (write timestamp: {}, current generation started at: {})",
                format_timestamp(ts), format_timestamp(it->first)));
    }

    // With `generation_leeway` we allow sending writes to the near future. It might happen
    // that `ts` doesn't belong to the current generation ("current" according to our clock),
    // but to the next generation. Adjust for this case:
    {
        auto next_it = std::next(it);
        while (next_it != _gens.end() && next_it->first <= ts) {
            it = next_it++;
        }
    }
    // Note: if there is a next generation that `ts` belongs to, but we don't know about it,
    // then too bad. This is no different from the situation in which we didn't manage to learn
    // about the current generation in time. We won't be able to prevent it until we introduce transactions.

    if (!it->second) {
        throw std::runtime_error(format(
                "cdc: attempted to get a stream from a generation that we know about, but weren't able to retrieve"
                " (generation timestamp: {}, write timestamp: {}). Make sure that the replicas which contain"
                " this generation's data are alive and reachable from this node.", format_timestamp(it->first), format_timestamp(ts)));
    }

    auto& gen = *it->second;
    auto ret = ::get_stream(gen.entries(), tok);
    _last_stream_timestamp = ts;
    return ret;
}

bool cdc::metadata::known_or_obsolete(db_clock::time_point tp) const {
    auto ts = to_ts(tp);
    auto it = _gens.lower_bound(ts);

    if (it == _gens.end()) {
        // No known generations with timestamp >= ts.
        return false;
    }

    if (it->first == ts) {
        if (it->second) {
            // We already inserted this particular generation.
            return true;
        }
        ++it;
    }

    // Check if some new generation has already superseded this one.
    return it != _gens.end() && it->first <= api::new_timestamp();
}

bool cdc::metadata::insert(db_clock::time_point tp, topology_description&& gen) {
    if (known_or_obsolete(tp)) {
        return false;
    }

    auto now = api::new_timestamp();
    auto it = gen_used_at(now);

    if (it != _gens.end()) {
        // Garbage-collect generations that will no longer be used.
        it = _gens.erase(_gens.begin(), it);

    }

    _gens.insert_or_assign(to_ts(tp), std::move(gen));
    return true;
}

bool cdc::metadata::prepare(db_clock::time_point tp) {
    if (known_or_obsolete(tp)) {
        return false;
    }

    auto ts = to_ts(tp);
    auto emplaced = _gens.emplace(to_ts(tp), std::nullopt).second;

    if (_last_stream_timestamp != api::missing_timestamp) {
        auto last_correct_gen = gen_used_at(_last_stream_timestamp);
        if (emplaced && last_correct_gen != _gens.end() && last_correct_gen->first == ts) {
            cdc_log.error(
                "just learned about a CDC generation newer than the one used the last time"
                " streams were retrieved. This generation, or some newer one, should have"
                " been used instead (new generation's timestamp: {}, last time streams were retrieved: {})."
                " The new generation probably arrived too late due to a network partition"
                " and we've made a write using the wrong set streams.",
                format_timestamp(ts), format_timestamp(_last_stream_timestamp));
        }
    }

    return emplaced;
}
