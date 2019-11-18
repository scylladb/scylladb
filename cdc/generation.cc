/*
 * Copyright (C) 2019 ScyllaDB
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

#include <boost/type.hpp>
#include <random>
#include <unordered_set>

#include "keys.hh"
#include "schema_builder.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "dht/i_partitioner.hh"
#include "locator/token_metadata.hh"
#include "locator/snitch_base.hh"
#include "gms/application_state.hh"
#include "gms/inet_address.hh"
#include "gms/gossiper.hh"

#include "cdc/generation.hh"

extern logging::logger cdc_log;

namespace cdc {

extern const api::timestamp_clock::duration generation_leeway =
    std::chrono::duration_cast<api::timestamp_clock::duration>(std::chrono::seconds(5));

stream_id::stream_id()
    : _first(-1), _second(-1) {}

stream_id::stream_id(int64_t first, int64_t second)
    : _first(first)
    , _second(second) {}

bool stream_id::is_set() const {
    return _first != -1 || _second != -1;
}

bool stream_id::operator==(const stream_id& o) const {
    return _first == o._first && _second == o._second;
}

int64_t stream_id::first() const {
    return _first;
}

int64_t stream_id::second() const {
    return _second;
}

partition_key stream_id::to_partition_key(const schema& log_schema) const {
    return partition_key::from_exploded(log_schema,
            { long_type->decompose(_first), long_type->decompose(_second) });
}


bool token_range_description::operator==(const token_range_description& o) const {
    return token_range_end == o.token_range_end && streams == o.streams
        && sharding_ignore_msb == o.sharding_ignore_msb;
}

topology_description::topology_description(std::vector<token_range_description> entries)
    : _entries(std::move(entries)) {}

bool topology_description::operator==(const topology_description& o) const {
    return _entries == o._entries;
}

const std::vector<token_range_description>& topology_description::entries() const {
    return _entries;
}

static stream_id make_random_stream_id() {
    static thread_local std::mt19937_64 rand_gen(std::random_device().operator()());
    static thread_local std::uniform_int_distribution<int64_t> rand_dist(std::numeric_limits<int64_t>::min());

    return {rand_dist(rand_gen), rand_dist(rand_gen)};
}

/* Given:
 * 1. a set of tokens which split the token ring into token ranges (vnodes),
 * 2. information on how each token range is distributed among its owning node's shards
 * this function tries to generate a set of CDC stream identifiers such that for each
 * shard and vnode pair there exists a stream whose token falls into this
 * vnode and is owned by this shard.
 *
 * It then builds a cdc::topology_description which maps tokens to these
 * found stream identifiers, such that if token T is owned by shard S in vnode V,
 * it gets mapped to the stream identifier generated for (S, V).
 */
// Run in seastar::async context.
topology_description generate_topology_description(
        const std::unordered_set<dht::token>& bootstrap_tokens,
        const locator::token_metadata& token_metadata,
        const dht::i_partitioner& partitioner,
        locator::snitch_ptr& snitch) {
    if (bootstrap_tokens.empty()) {
        throw std::runtime_error(
                "cdc: bootstrap tokens is empty in generate_topology_description");
    }

    auto tokens = token_metadata.sorted_tokens();
    tokens.insert(tokens.end(), bootstrap_tokens.begin(), bootstrap_tokens.end());
    std::sort(tokens.begin(), tokens.end());
    tokens.erase(std::unique(tokens.begin(), tokens.end()), tokens.end());

    std::vector<token_range_description> entries(tokens.size());
    int spots_to_fill = 0;

    for (size_t i = 0; i < tokens.size(); ++i) {
        auto& entry = entries[i];
        entry.token_range_end = tokens[i];

        if (bootstrap_tokens.count(entry.token_range_end) > 0) {
            entry.streams.resize(smp::count);
            entry.sharding_ignore_msb = partitioner.sharding_ignore_msb();
        } else {
            auto endpoint = token_metadata.get_endpoint(entry.token_range_end);
            if (!endpoint) {
                throw std::runtime_error(format("Can't find endpoint for token {}", entry.token_range_end));
            }
            auto sc = snitch->get_shard_count(*endpoint);
            entry.streams.resize(sc > 0 ? sc : 1);
            entry.sharding_ignore_msb = snitch->get_ignore_msb_bits(*endpoint);
        }

        spots_to_fill += entry.streams.size();
    }

    auto schema = schema_builder("fake_ks", "fake_table")
        .with_column("stream_id_1", long_type, column_kind::partition_key)
        .with_column("stream_id_2", long_type, column_kind::partition_key)
        .build();

    auto quota = std::chrono::seconds(spots_to_fill / 2000 + 1);
    auto start_time = std::chrono::system_clock::now();

    // For each pair (i, j), 0 <= i < streams.size(), 0 <= j < streams[i].size(),
    // try to find a stream (stream[i][j]) such that the token of this stream will get mapped to this stream
    // (refer to the comments above topology_description's definition to understand how it describes the mapping).
    // We find the streams by randomly generating them and checking into which pairs they get mapped.
    // NOTE: this algorithm is temporary and will be replaced after per-table-partitioner feature gets merged in.
    repeat([&] {
        for (int i = 0; i < 500; ++i) {
            auto stream_id = make_random_stream_id();
            auto token = partitioner.get_token(*schema, stream_id.to_partition_key(*schema));

            // Find the token range into which our stream_id's token landed.
            auto it = std::lower_bound(tokens.begin(), tokens.end(), token);
            auto& entry = entries[it != tokens.end() ? std::distance(tokens.begin(), it) : 0];

            auto shard_id = partitioner.shard_of(token, entry.streams.size(), entry.sharding_ignore_msb);
            assert(shard_id < entry.streams.size());

            if (!entry.streams[shard_id].is_set()) {
                --spots_to_fill;
                entry.streams[shard_id] = stream_id;
            }
        }

        if (!spots_to_fill) {
            return stop_iteration::yes;
        }

        auto now = std::chrono::system_clock::now();
        auto passed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
        if (passed > quota) {
            return stop_iteration::yes;
        }

        return stop_iteration::no;
    }).get();

    if (spots_to_fill) {
        // We were not able to generate stream ids for each (token range, shard) pair.

        // For each range that has a stream, for each shard for this range that doesn't have a stream,
        // use the stream id of the next shard for this range.

        // For each range that doesn't have any stream,
        // use streams of the first range to the left which does have a stream.

        cdc_log.warn("Generation of CDC streams failed to create streams for some (vnode, shard) pair."
                     " This can lead to worse performance.");

        stream_id some_stream;
        size_t idx = 0;
        for (; idx < entries.size(); ++idx) {
            for (auto s: entries[idx].streams) {
                if (s.is_set()) {
                    some_stream = s;
                    break;
                }
            }
            if (some_stream.is_set()) {
                break;
            }
        }

        assert(idx != entries.size() && some_stream.is_set());

        // Iterate over all ranges in the clockwise direction, starting with the one we found a stream for.
        for (size_t off = 0; off < entries.size(); ++off) {
            auto& ss = entries[(idx + off) % entries.size()].streams;

            int last_set_stream_idx = ss.size() - 1;
            while (last_set_stream_idx > -1 && !ss[last_set_stream_idx].is_set()) {
                --last_set_stream_idx;
            }

            if (last_set_stream_idx == -1) {
                cdc_log.warn(
                        "CDC wasn't able to generate any stream for vnode ({}, {}]. We'll use another vnode's streams"
                        " instead. This might lead to inconsistencies.",
                        tokens[(idx + off + entries.size() - 1) % entries.size()], tokens[(idx + off) % entries.size()]);

                ss[0] = some_stream;
                last_set_stream_idx = 0;
            }

            some_stream = ss[last_set_stream_idx];

            // Replace 'unset' stream ids with indexes below last_set_stream_idx
            for (int s_idx = last_set_stream_idx - 1; s_idx > -1; --s_idx) {
                if (ss[s_idx].is_set()) {
                    some_stream = ss[s_idx];
                } else {
                    ss[s_idx] = some_stream;
                }
            }
            // Replace 'unset' stream ids with indexes above last_set_stream_idx
            for (int s_idx = ss.size() - 1; s_idx > last_set_stream_idx; --s_idx) {
                if (ss[s_idx].is_set()) {
                    some_stream = ss[s_idx];
                } else {
                    ss[s_idx] = some_stream;
                }
            }
        }
    }

    return {std::move(entries)};
}

bool should_propose_first_generation(const gms::inet_address& me, const gms::gossiper& g) {
    auto my_host_id = g.get_host_id(me);
    auto& eps = g.get_endpoint_states();
    return std::none_of(eps.begin(), eps.end(),
            [&] (const std::pair<gms::inet_address, gms::endpoint_state>& ep) {
        return my_host_id < g.get_host_id(ep.first);
    });
}

future<db_clock::time_point> get_local_streams_timestamp() {
    return db::system_keyspace::get_saved_cdc_streams_timestamp().then([] (std::optional<db_clock::time_point> ts) {
        if (!ts) {
            auto err = format("get_local_streams_timestamp: tried to retrieve streams timestamp after bootstrapping, but it's not present");
            cdc_log.error("{}", err);
            throw std::runtime_error(err);
        }
        return *ts;
    });
}

// Run inside seastar::async context.
db_clock::time_point make_new_cdc_generation(
        const std::unordered_set<dht::token>& bootstrap_tokens,
        const locator::token_metadata& tm,
        db::system_distributed_keyspace& sys_dist_ks,
        std::chrono::milliseconds ring_delay,
        bool for_testing) {
    assert(!bootstrap_tokens.empty());

    auto gen = generate_topology_description(
            bootstrap_tokens, tm, dht::global_partitioner(), locator::i_endpoint_snitch::get_local_snitch_ptr());

    // Begin the race.
    auto ts = db_clock::now() + (
            for_testing ? std::chrono::milliseconds(0) : (
                2 * ring_delay + std::chrono::duration_cast<std::chrono::milliseconds>(generation_leeway)));
    sys_dist_ks.insert_cdc_topology_description(ts, std::move(gen), { tm.count_normal_token_owners() }).get();

    return ts;
}

std::optional<db_clock::time_point> get_streams_timestamp_for(const gms::inet_address& endpoint, const gms::gossiper& g) {
    auto streams_ts_string = g.get_application_state_value(endpoint, gms::application_state::CDC_STREAMS_TIMESTAMP);
    cdc_log.trace("endpoint={}, streams_ts_string={}", endpoint, streams_ts_string);

    if (streams_ts_string.empty()) {
        return {};
    }

    return db_clock::time_point(db_clock::duration(std::stoll(streams_ts_string)));
}

} // namespace cdc
