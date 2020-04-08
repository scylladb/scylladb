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
#include <seastar/core/sleep.hh>

#include "keys.hh"
#include "schema_builder.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "dht/token-sharding.hh"
#include "locator/token_metadata.hh"
#include "gms/application_state.hh"
#include "gms/inet_address.hh"
#include "gms/gossiper.hh"

#include "cdc/generation.hh"

extern logging::logger cdc_log;

static int get_shard_count(const gms::inet_address& endpoint, const gms::gossiper& g) {
    auto ep_state = g.get_application_state_ptr(endpoint, gms::application_state::SHARD_COUNT);
    return ep_state ? std::stoi(ep_state->value) : -1;
}

static unsigned get_sharding_ignore_msb(const gms::inet_address& endpoint, const gms::gossiper& g) {
    auto ep_state = g.get_application_state_ptr(endpoint, gms::application_state::IGNORE_MSB_BITS);
    return ep_state ? std::stoi(ep_state->value) : 0;
}

namespace cdc {

extern const api::timestamp_clock::duration generation_leeway =
    std::chrono::duration_cast<api::timestamp_clock::duration>(std::chrono::seconds(5));

static void copy_int_to_bytes(int64_t i, size_t offset, bytes& b) {
    i = net::hton(i);
    std::copy_n(reinterpret_cast<int8_t*>(&i), sizeof(int64_t), b.begin() + offset);
}

stream_id::stream_id(int64_t first, int64_t second)
    : _value(bytes::initialized_later(), 2 * sizeof(int64_t))
{
    copy_int_to_bytes(first, 0, _value);
    copy_int_to_bytes(second, sizeof(int64_t), _value);
}

stream_id::stream_id(bytes b) : _value(std::move(b)) { }

bool stream_id::is_set() const {
    return !_value.empty();
}

bool stream_id::operator==(const stream_id& o) const {
    return _value == o._value;
}

bool stream_id::operator<(const stream_id& o) const {
    return _value < o._value;
}

static int64_t bytes_to_int64(bytes_view b, size_t offset) {
    assert(b.size() >= offset + sizeof(int64_t));
    int64_t res;
    std::copy_n(b.begin() + offset, sizeof(int64_t), reinterpret_cast<int8_t *>(&res));
    return net::ntoh(res);
}

int64_t stream_id::first() const {
    return token_from_bytes(_value);
}

int64_t stream_id::second() const {
    return bytes_to_int64(_value, sizeof(int64_t));
}

int64_t stream_id::token_from_bytes(bytes_view b) {
    return bytes_to_int64(b, 0);
}

const bytes& stream_id::to_bytes() const {
    return _value;
}

partition_key stream_id::to_partition_key(const schema& log_schema) const {
    return partition_key::from_single_value(log_schema, _value);
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
        const db::config& cfg,
        const std::unordered_set<dht::token>& bootstrap_tokens,
        const locator::token_metadata& token_metadata,
        const gms::gossiper& gossiper) {
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
            entry.sharding_ignore_msb = cfg.murmur3_partitioner_ignore_msb_bits();
        } else {
            auto endpoint = token_metadata.get_endpoint(entry.token_range_end);
            if (!endpoint) {
                throw std::runtime_error(format("Can't find endpoint for token {}", entry.token_range_end));
            }
            auto sc = get_shard_count(*endpoint, gossiper);
            entry.streams.resize(sc > 0 ? sc : 1);
            entry.sharding_ignore_msb = get_sharding_ignore_msb(*endpoint, gossiper);
        }

        spots_to_fill += entry.streams.size();
    }

    auto schema = schema_builder("fake_ks", "fake_table")
        .with_column("stream_id", bytes_type, column_kind::partition_key)
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
            auto token = dht::get_token(*schema, stream_id.to_partition_key(*schema));

            // Find the token range into which our stream_id's token landed.
            auto it = std::lower_bound(tokens.begin(), tokens.end(), token);
            auto& entry = entries[it != tokens.end() ? std::distance(tokens.begin(), it) : 0];

            auto shard_id = dht::shard_of(entry.streams.size(), entry.sharding_ignore_msb, token);
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
        const db::config& cfg,
        const std::unordered_set<dht::token>& bootstrap_tokens,
        const locator::token_metadata& tm,
        const gms::gossiper& g,
        db::system_distributed_keyspace& sys_dist_ks,
        std::chrono::milliseconds ring_delay,
        bool for_testing) {
    assert(!bootstrap_tokens.empty());

    auto gen = generate_topology_description(cfg, bootstrap_tokens, tm, g);

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

// Run inside seastar::async context.
static void do_update_streams_description(
        db_clock::time_point streams_ts,
        db::system_distributed_keyspace& sys_dist_ks,
        db::system_distributed_keyspace::context ctx) {
    if (sys_dist_ks.cdc_desc_exists(streams_ts, ctx).get0()) {
        cdc_log.debug("update_streams_description: description of generation {} already inserted", streams_ts);
        return;
    }

    // We might race with another node also inserting the description, but that's ok. It's an idempotent operation.

    auto topo = sys_dist_ks.read_cdc_topology_description(streams_ts, ctx).get0();
    if (!topo) {
        throw std::runtime_error(format("could not find streams data for timestamp {}", streams_ts));
    }

    std::set<cdc::stream_id> streams_set;
    for (auto& entry: topo->entries()) {
        streams_set.insert(entry.streams.begin(), entry.streams.end());
    }

    std::vector<cdc::stream_id> streams_vec(streams_set.begin(), streams_set.end());

    sys_dist_ks.create_cdc_desc(streams_ts, streams_vec, ctx).get();
    cdc_log.info("CDC description table successfully updated with generation {}.", streams_ts);
}

void update_streams_description(
        db_clock::time_point streams_ts,
        shared_ptr<db::system_distributed_keyspace> sys_dist_ks,
        noncopyable_function<unsigned()> get_num_token_owners,
        abort_source& abort_src) {
    try {
        do_update_streams_description(streams_ts, *sys_dist_ks, { get_num_token_owners() });
    } catch(...) {
        cdc_log.warn(
            "Could not update CDC description table with generation {}: {}. Will retry in the background.",
            streams_ts, std::current_exception());

        // It is safe to discard this future: we keep system distributed keyspace alive.
        (void)seastar::async([
            streams_ts, sys_dist_ks, get_num_token_owners = std::move(get_num_token_owners), &abort_src
        ] {
            while (true) {
                sleep_abortable(std::chrono::seconds(60), abort_src).get();
                try {
                    do_update_streams_description(streams_ts, *sys_dist_ks, { get_num_token_owners() });
                    return;
                } catch (...) {
                    cdc_log.warn(
                        "Could not update CDC description table with generation {}: {}. Will try again.",
                        streams_ts, std::current_exception());
                }
            }
        });
    }
}

} // namespace cdc
