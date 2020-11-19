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

static constexpr auto stream_id_version_bits = 4;
static constexpr auto stream_id_random_bits = 38;
static constexpr auto stream_id_index_bits = sizeof(uint64_t)*8 - stream_id_version_bits - stream_id_random_bits;

static constexpr auto stream_id_version_shift = 0;
static constexpr auto stream_id_index_shift = stream_id_version_shift + stream_id_version_bits;
static constexpr auto stream_id_random_shift = stream_id_index_shift + stream_id_index_bits;

/**
 * Responsibilty for encoding stream_id moved from factory method to
 * this constructor, to keep knowledge of composition in a single place.
 * Note this is private and friended to topology_description_generator,
 * because he is the one who defined the "order" we view vnodes etc.
 */
stream_id::stream_id(dht::token token, size_t vnode_index)
    : _value(bytes::initialized_later(), 2 * sizeof(int64_t))
{
    static thread_local std::mt19937_64 rand_gen(std::random_device{}());
    static thread_local std::uniform_int_distribution<uint64_t> rand_dist;

    auto rand = rand_dist(rand_gen);
    auto mask_shift = [](uint64_t val, size_t bits, size_t shift) {
        return (val & ((1ull << bits) - 1u)) << shift;
    };
    /**
     *  Low qword:
     * 0-4: version
     * 5-26: vnode index as when created (see generation below). This excludes shards
     * 27-64: random value (maybe to be replaced with timestamp)
     */
    auto low_qword = mask_shift(version_1, stream_id_version_bits, stream_id_version_shift)
        | mask_shift(vnode_index, stream_id_index_bits, stream_id_index_shift)
        | mask_shift(rand, stream_id_random_bits, stream_id_random_shift)
        ;

    copy_int_to_bytes(dht::token::to_int64(token), 0, _value);
    copy_int_to_bytes(low_qword, sizeof(int64_t), _value);
    // not a hot code path. make sure we did not mess up the shifts and masks.
    assert(version() == version_1);
    assert(index() == vnode_index);
}

stream_id::stream_id(bytes b)
    : _value(std::move(b))
{
    // this is not a very solid check. Id:s previous to GA/versioned id:s
    // have fully random bits in low qword, so this could go either way...
    if (version() > version_1) {
        throw std::invalid_argument("Unknown CDC stream id version");
    }
}

bool stream_id::is_set() const {
    return !_value.empty();
}

bool stream_id::operator==(const stream_id& o) const {
    return _value == o._value;
}

bool stream_id::operator!=(const stream_id& o) const {
    return !(*this == o);
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

dht::token stream_id::token() const {
    return dht::token::from_int64(token_from_bytes(_value));
}

int64_t stream_id::token_from_bytes(bytes_view b) {
    return bytes_to_int64(b, 0);
}

static uint64_t unpack_value(bytes_view b, size_t off, size_t shift, size_t bits) {
    return (uint64_t(bytes_to_int64(b, off)) >> shift) & ((1ull << bits) - 1u);
}

uint8_t stream_id::version() const {
    return unpack_value(_value, sizeof(int64_t), stream_id_version_shift, stream_id_version_bits);
}

size_t stream_id::index() const {
    return unpack_value(_value, sizeof(int64_t), stream_id_index_shift, stream_id_index_bits);
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

class topology_description_generator final {
    const db::config& _cfg;
    const std::unordered_set<dht::token>& _bootstrap_tokens;
    const locator::token_metadata_ptr _tmptr;
    const gms::gossiper& _gossiper;

    // Compute a set of tokens that split the token ring into vnodes
    auto get_tokens() const {
        auto tokens = _tmptr->sorted_tokens();
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
        if (_bootstrap_tokens.contains(end)) {
            return {smp::count, _cfg.murmur3_partitioner_ignore_msb_bits()};
        } else {
            auto endpoint = _tmptr->get_endpoint(end);
            if (!endpoint) {
                throw std::runtime_error(
                        format("Can't find endpoint for token {}", end));
            }
            auto sc = get_shard_count(*endpoint, _gossiper);
            return {sc > 0 ? sc : 1, get_sharding_ignore_msb(*endpoint, _gossiper)};
        }
    }

    token_range_description create_description(size_t index, dht::token start, dht::token end) const {
        token_range_description desc;

        desc.token_range_end = end;

        auto [shard_count, ignore_msb] = get_sharding_info(end);
        desc.streams.reserve(shard_count);
        desc.sharding_ignore_msb = ignore_msb;

        dht::sharder sharder(shard_count, ignore_msb);
        for (size_t shard_idx = 0; shard_idx < shard_count; ++shard_idx) {
            auto t = dht::find_first_token_for_shard(sharder, start, end, shard_idx);
            // compose the id from token and the "index" of the range end owning vnode
            // as defined by token sort order. Basically grouping within this
            // shard set.
            desc.streams.emplace_back(stream_id(t, index));
        }

        return desc;
    }
public:
    topology_description_generator(
            const db::config& cfg,
            const std::unordered_set<dht::token>& bootstrap_tokens,
            const locator::token_metadata_ptr tmptr,
            const gms::gossiper& gossiper)
        : _cfg(cfg)
        , _bootstrap_tokens(bootstrap_tokens)
        , _tmptr(std::move(tmptr))
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
                create_description(0, tokens.back(), tokens.front()));
        for (size_t idx = 1; idx < tokens.size(); ++idx) {
            vnode_descriptions.push_back(
                    create_description(idx, tokens[idx - 1], tokens[idx]));
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
        const locator::token_metadata_ptr tmptr,
        const gms::gossiper& g,
        db::system_distributed_keyspace& sys_dist_ks,
        std::chrono::milliseconds ring_delay,
        bool add_delay) {
    using namespace std::chrono;
    auto gen = topology_description_generator(cfg, bootstrap_tokens, tmptr, g).generate();

    // Begin the race.
    auto ts = db_clock::now() + (
            (!add_delay || ring_delay == milliseconds(0)) ? milliseconds(0) : (
                2 * ring_delay + duration_cast<milliseconds>(generation_leeway)));
    sys_dist_ks.insert_cdc_topology_description(ts, std::move(gen), { tmptr->count_normal_token_owners() }).get();

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
