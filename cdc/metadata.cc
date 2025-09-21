/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "dht/token-sharding.hh"
#include "exceptions/exceptions.hh"

#include "cdc/generation.hh"
#include "utils/stall_free.hh"
#include "cdc/metadata.hh"

#include <seastar/coroutine/maybe_yield.hh>

extern logging::logger cdc_log;

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
        const utils::chunked_vector<cdc::token_range_description>& entries,
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

static cdc::stream_id get_stream(
        const std::vector<cdc::stream_id>& streams,
        dht::token tok) {
    if (streams.empty()) {
        on_internal_error(cdc_log, "get_stream: streams empty");
    }

    auto it = std::lower_bound(streams.begin(), streams.end(), tok,
            [] (const cdc::stream_id& sid, dht::token t) { return sid.token() < t; });
    if (it == streams.end()) {
        on_internal_error(cdc_log, fmt::format("get_stream: no stream for token {}.", tok));
    }
    return *it;
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

cdc::stream_id cdc::metadata::get_vnode_stream(api::timestamp_type ts, dht::token tok) {
    auto now = api::new_timestamp();
    if (ts > now + get_generation_leeway().count()) {
        throw exceptions::invalid_request_exception(seastar::format(
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

    auto it = gen_used_at(now - get_generation_leeway().count());

    if (it != _gens.end()) {
        // Garbage-collect generations that will no longer be used.
        it = _gens.erase(_gens.begin(), it);
    }

    if (ts <= now - get_generation_leeway().count()) {
        // We reject the write if `ts <= now - generation_leeway` and the write is not to the current generation, which
        // happens iff one of the following is true:
        // - the write is to no generation,
        // - the write is to a generation older than the generation under `it`,
        // - the write is to the generation under `it` and that generation is not the current generation.
        // Note that we cannot distinguish the first and second cases because we garbage-collect obsolete generations,
        // but we can check if one of them takes place (`it == _gens.end() || ts < it->first`). These three conditions
        // are sufficient. The write with `ts <= now - generation_leeway` cannot be to one of the generations following
        // the generation under `it` because that generation was operating at `now - generation_leeway`.
        bool is_previous_gen = it != _gens.end() && std::next(it) != _gens.end() && std::next(it)->first <= now;
        if (it == _gens.end() || ts < it->first || is_previous_gen) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "cdc: attempted to get a stream \"from the past\" ({}; current server time: {})."
                    " With CDC you cannot send writes with timestamps too far into the past, because that would break"
                    " consistency properties.\n"
                    "We *do* allow sending writes into the near past, but our ability to do that is limited."
                    " Are you using client-side timestamps? Make sure your clocks are well-synchronized"
                    " with the database's clocks.", format_timestamp(ts), format_timestamp(now)));
        }
    }

    it = _gens.begin();
    if (it == _gens.end() || ts < it->first) {
        throw std::runtime_error(fmt::format(
                "cdc::metadata::get_stream: could not find any CDC stream for timestamp {}."
                " Are we in the middle of a cluster upgrade?", format_timestamp(ts)));
    }

    // Find the generation operating at `ts`.
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
        throw std::runtime_error(fmt::format(
                "cdc: attempted to get a stream from a generation that we know about, but weren't able to retrieve"
                " (generation timestamp: {}, write timestamp: {}). Make sure that the replicas which contain"
                " this generation's data are alive and reachable from this node.", format_timestamp(it->first), format_timestamp(ts)));
    }

    auto& gen = *it->second;
    auto ret = ::get_stream(gen.entries(), tok);
    _last_stream_timestamp = ts;
    return ret;
}

const std::vector<cdc::stream_id>& cdc::metadata::get_tablet_stream_set(table_id tid, api::timestamp_type ts) const {
    auto now = api::new_timestamp();
    if (ts > now + get_generation_leeway().count()) {
        throw exceptions::invalid_request_exception(seastar::format(
                "cdc: attempted to get a stream \"from the future\" ({}; current server time: {})."
                " With CDC you cannot send writes with timestamps arbitrarily into the future, because we don't"
                " know what streams will be used at that time.\n"
                "We *do* allow sending writes into the near future, but our ability to do that is limited."
                " If you really must use your own timestamps, then make sure your clocks are well-synchronized"
                "  with the database's clocks.", format_timestamp(ts), format_timestamp(now)));
    }

    auto table_it = _tablet_streams.find(tid);
    if (table_it == _tablet_streams.end()) {
        throw std::runtime_error(fmt::format(
                "cdc::metadata::get_stream: could not find stream metadata for table {}.", tid));
    }

    // find the most recent entry of stream sets with timestamp <= write timestamp.
    // start from the most recent entry, which is the most likely candidate, and go
    // back until we find matching entry.
    const auto& table_streams = *table_it->second;
    auto it = table_streams.crbegin();
    for (; it != table_streams.crend() && it->first > ts; ++it);

    if (it == table_streams.crend()) {
        throw std::runtime_error(fmt::format(
                "cdc::metadata::get_stream: could not find any CDC streams for table {} for timestamp {}.", tid, ts));
    }

    return it->second.streams;
}

cdc::stream_id cdc::metadata::get_tablet_stream(table_id tid, api::timestamp_type ts, dht::token tok) {
    return ::get_stream(get_tablet_stream_set(tid, ts), tok);
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

    // Check if the generation is obsolete.
    return it != _gens.end() && it->first <= api::new_timestamp() - get_generation_leeway().count();
}

bool cdc::metadata::insert(db_clock::time_point tp, topology_description&& gen) {
    if (known_or_obsolete(tp)) {
        return false;
    }

    auto now = api::new_timestamp();
    auto it = gen_used_at(now - get_generation_leeway().count());

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
    auto [it, emplaced] = _gens.emplace(to_ts(tp), std::nullopt);

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

    return !it->second;
}

future<std::vector<cdc::stream_id>> cdc::metadata::construct_next_stream_set(
        const std::vector<cdc::stream_id>& prev_stream_set,
        std::vector<cdc::stream_id> opened,
        const std::vector<cdc::stream_id>& closed) {

    if (closed.size() == prev_stream_set.size()) {
        // all previous streams are closed, so the next stream set is just the opened streams.
        co_return std::move(opened);
    }

    // construct the next stream set from the previous one by adding the opened
    // streams and removing the closed streams. we assume each stream set is
    // sorted by token, and the result is sorted as well.

    std::vector<cdc::stream_id> next_stream_set;
    next_stream_set.reserve(prev_stream_set.size() + opened.size() - closed.size());

    auto next_prev = prev_stream_set.begin();
    auto next_closed = closed.begin();
    auto next_opened = opened.begin();

    while (next_prev != prev_stream_set.end() || next_opened != opened.end()) {
        co_await coroutine::maybe_yield();

        if (next_prev == prev_stream_set.end() || (next_opened != opened.end() && next_opened->token() < next_prev->token())) {
            next_stream_set.push_back(std::move(*next_opened));
            next_opened++;
            continue;
        }

        if (next_closed != closed.end() && *next_prev == *next_closed) {
            next_prev++;
            next_closed++;
            continue;
        }

        next_stream_set.push_back(*next_prev);
        next_prev++;
    }

    co_return std::move(next_stream_set);
}

void cdc::metadata::load_tablet_streams_map(table_id tid, table_streams new_table_map) {
    _tablet_streams[tid] = make_lw_shared(std::move(new_table_map));
}

void cdc::metadata::append_tablet_streams_map(table_id tid, table_streams new_table_map) {
    _tablet_streams[tid]->insert(
        std::make_move_iterator(new_table_map.begin()),
        std::make_move_iterator(new_table_map.end()));
}

void cdc::metadata::remove_tablet_streams_map(table_id tid) {
    _tablet_streams.erase(tid);
}

std::vector<table_id> cdc::metadata::get_tables_with_cdc_tablet_streams() const {
    return _tablet_streams | std::views::keys | std::ranges::to<std::vector<table_id>>();
}

future<cdc::cdc_stream_diff> cdc::metadata::generate_stream_diff(const std::vector<stream_id>& before, const std::vector<stream_id>& after) {
    std::vector<stream_id> closed, opened;

    auto before_it = before.begin();
    auto after_it = after.begin();

    while (before_it != before.end()) {
        co_await coroutine::maybe_yield();

        if (after_it == after.end()) {
            while (before_it != before.end()) {
                co_await coroutine::maybe_yield();
                closed.push_back(*before_it++);
            }
            break;
        }

        if (after_it->token() < before_it->token()) {
            opened.push_back(*after_it++);
        } else if (after_it->token() > before_it->token()) {
            closed.push_back(*before_it++);
        } else if (*after_it != *before_it) {
            opened.push_back(*after_it++);
            closed.push_back(*before_it++);
        } else {
            after_it++;
            before_it++;
        }
    }
    while (after_it != after.end()) {
        co_await coroutine::maybe_yield();
        opened.push_back(*after_it++);
    }

    co_return cdc_stream_diff {
        .closed_streams = std::move(closed),
        .opened_streams = std::move(opened)
    };
}
