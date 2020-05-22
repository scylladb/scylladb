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

static stream_id create_stream_id(dht::token t) {
    static thread_local std::mt19937_64 rand_gen(std::random_device().operator()());
    static thread_local std::uniform_int_distribution<int64_t> rand_dist(std::numeric_limits<int64_t>::min());

    return {dht::token::to_int64(t), rand_dist(rand_gen)};
}

class topology_description_generator final {
    const db::config& _cfg;
    const std::unordered_set<dht::token>& _bootstrap_tokens;
    const locator::token_metadata& _token_metadata;
    const gms::gossiper& _gossiper;

    // Compute a set of tokens that split the token ring into vnodes
    auto get_tokens() const {
        auto tokens = _token_metadata.sorted_tokens();
        auto it = tokens.insert(
                tokens.end(), _bootstrap_tokens.begin(), _bootstrap_tokens.end());
        std::sort(it, tokens.end());
        std::inplace_merge(tokens.begin(), it, tokens.end());
        tokens.erase(std::unique(tokens.begin(), tokens.end()), tokens.end());
        return tokens;
    }

    // Fetch sharding parameters for a node that owns vnode ending with this.end
    // Returns <shard_count, ignore_msb> pair.
    std::pair<size_t, uint8_t> get_sharding_info(dht::token end) const {
        if (_bootstrap_tokens.count(end) > 0) {
            return {smp::count, _cfg.murmur3_partitioner_ignore_msb_bits()};
        } else {
            auto endpoint = _token_metadata.get_endpoint(end);
            if (!endpoint) {
                throw std::runtime_error(
                        format("Can't find endpoint for token {}", end));
            }
            auto sc = get_shard_count(*endpoint, _gossiper);
            return {sc > 0 ? sc : 1, get_sharding_ignore_msb(*endpoint, _gossiper)};
        }
    }

    token_range_description create_description(dht::token start, dht::token end) const {
        token_range_description desc;

        desc.token_range_end = end;

        auto [shard_count, ignore_msb] = get_sharding_info(end);
        desc.streams.reserve(shard_count);
        desc.sharding_ignore_msb = ignore_msb;

        dht::sharder sharder(shard_count, ignore_msb);
        for (size_t shard_idx = 0; shard_idx < shard_count; ++shard_idx) {
            auto t = dht::find_first_token_for_shard(sharder, start, end, shard_idx);
            desc.streams.push_back(create_stream_id(t));
        }

        return desc;
    }
public:
    topology_description_generator(
            const db::config& cfg,
            const std::unordered_set<dht::token>& bootstrap_tokens,
            const locator::token_metadata& token_metadata,
            const gms::gossiper& gossiper)
        : _cfg(cfg)
        , _bootstrap_tokens(bootstrap_tokens)
        , _token_metadata(token_metadata)
        , _gossiper(gossiper)
    {}

    /*
     * Generate a set of CDC stream identifiers such that for each shard
     * and vnode pair there exists a stream whose token falls into this vnode
     * and is owned by this shard. It is sometimes not possible to generate
     * a CDC stream identifier for some (vnode, shard) pair because not all
     * shards have to own tokens in a vnode. Small vnode can be totally owned
     * by a single shard. In such case, a stream identifier that maps to
     * end of the vnode is generated.
     *
     * Then build a cdc::topology_description which maps tokens to generated
     * stream identifiers, such that if token T is owned by shard S in vnode V,
     * it gets mapped to the stream identifier generated for (S, V).
     */
    // Run in seastar::async context.
    topology_description generate() const {
        const auto tokens = get_tokens();

        std::vector<token_range_description> vnode_descriptions;
        vnode_descriptions.reserve(tokens.size());

        vnode_descriptions.push_back(
                create_description(tokens.back(), tokens.front()));
        for (size_t idx = 1; idx < tokens.size(); ++idx) {
            vnode_descriptions.push_back(
                    create_description(tokens[idx - 1], tokens[idx]));
        }

        return {std::move(vnode_descriptions)};
    }
};

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
    auto gen = topology_description_generator(cfg, bootstrap_tokens, tm, g).generate();

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
    return gms::versioned_value::cdc_streams_timestamp_from_string(streams_ts_string);
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
