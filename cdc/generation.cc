/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/type.hpp>
#include <random>
#include <unordered_set>
#include <algorithm>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>

#include "gms/endpoint_state.hh"
#include "gms/versioned_value.hh"
#include "keys.hh"
#include "replica/database.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "dht/token-sharding.hh"
#include "locator/token_metadata.hh"
#include "types/set.hh"
#include "gms/application_state.hh"
#include "gms/inet_address.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "utils/UUID_gen.hh"
#include "utils/to_string.hh"

#include "cdc/generation.hh"
#include "cdc/cdc_options.hh"
#include "cdc/generation_service.hh"
#include "cdc/log.hh"

extern logging::logger cdc_log;

static int get_shard_count(const gms::inet_address& endpoint, const gms::gossiper& g) {
    auto ep_state = g.get_application_state_ptr(endpoint, gms::application_state::SHARD_COUNT);
    return ep_state ? std::stoi(ep_state->value()) : -1;
}

static unsigned get_sharding_ignore_msb(const gms::inet_address& endpoint, const gms::gossiper& g) {
    auto ep_state = g.get_application_state_ptr(endpoint, gms::application_state::IGNORE_MSB_BITS);
    return ep_state ? std::stoi(ep_state->value()) : 0;
}

namespace db {
    extern thread_local data_type cdc_streams_set_type;
}

namespace cdc {

api::timestamp_clock::duration get_generation_leeway() {
    static thread_local auto generation_leeway =
            std::chrono::duration_cast<api::timestamp_clock::duration>(std::chrono::seconds(5));

    utils::get_local_injector().inject("increase_cdc_generation_leeway", [&] {
        generation_leeway = std::chrono::duration_cast<api::timestamp_clock::duration>(std::chrono::minutes(5));
    });

    return generation_leeway;
}

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
 * Responsibility for encoding stream_id moved from the create_stream_ids
 * function to this constructor, to keep knowledge of composition in a
 * single place. Note the make_new_generation_description function
 * defines the "order" in which we view vnodes etc.
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
    SCYLLA_ASSERT(version() == version_1);
    SCYLLA_ASSERT(index() == vnode_index);
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

static int64_t bytes_to_int64(bytes_view b, size_t offset) {
    SCYLLA_ASSERT(b.size() >= offset + sizeof(int64_t));
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

topology_description::topology_description(utils::chunked_vector<token_range_description> entries)
    : _entries(std::move(entries)) {}

bool topology_description::operator==(const topology_description& o) const {
    return _entries == o._entries;
}

const utils::chunked_vector<token_range_description>& topology_description::entries() const& {
    return _entries;
}

utils::chunked_vector<token_range_description>&& topology_description::entries() && {
    return std::move(_entries);
}

static std::vector<stream_id> create_stream_ids(
        size_t index, dht::token start, dht::token end, size_t shard_count, uint8_t ignore_msb) {
    std::vector<stream_id> result;
    result.reserve(shard_count);
    dht::static_sharder sharder(shard_count, ignore_msb);
    for (size_t shard_idx = 0; shard_idx < shard_count; ++shard_idx) {
        auto t = dht::find_first_token_for_shard(sharder, start, end, shard_idx);
        // compose the id from token and the "index" of the range end owning vnode
        // as defined by token sort order. Basically grouping within this
        // shard set.
        result.emplace_back(stream_id(t, index));
    }
    return result;
}

bool should_propose_first_generation(const locator::host_id& my_host_id, const gms::gossiper& g) {
    return g.for_each_endpoint_state_until([&] (const gms::inet_address&, const gms::endpoint_state& eps) {
        return stop_iteration(my_host_id < eps.get_host_id());
    }) == stop_iteration::no;
}

bool is_cdc_generation_optimal(const cdc::topology_description& gen, const locator::token_metadata& tm) {
    if (tm.sorted_tokens().size() != gen.entries().size()) {
        // We probably have garbage streams from old generations
        cdc_log.info("Generation size does not match the token ring");
        return false;
    } else {
        std::unordered_set<dht::token> gen_ends;
        for (const auto& entry : gen.entries()) {
            gen_ends.insert(entry.token_range_end);
        }
        for (const auto& metadata_token : tm.sorted_tokens()) {
            if (!gen_ends.contains(metadata_token)) {
                cdc_log.warn("CDC generation missing token {}", metadata_token);
                return false;
            }
        }
        return true;
    }
}

static future<utils::chunked_vector<mutation>> get_common_cdc_generation_mutations(
        schema_ptr s,
        const partition_key& pkey,
        noncopyable_function<clustering_key (dht::token)>&& get_ckey_from_range_end,
        const cdc::topology_description& desc,
        size_t mutation_size_threshold,
        api::timestamp_type ts) {
    utils::chunked_vector<mutation> res;
    res.emplace_back(s, pkey);
    size_t size_estimate = 0;
    size_t total_size_estimate = 0;
    for (auto& e : desc.entries()) {
        if (size_estimate >= mutation_size_threshold) {
            total_size_estimate += size_estimate;
            res.emplace_back(s, pkey);
            size_estimate = 0;
        }

        set_type_impl::native_type streams;
        streams.reserve(e.streams.size());
        for (auto& stream: e.streams) {
            streams.push_back(data_value(stream.to_bytes()));
        }

        size_estimate += e.streams.size() * 20;
        auto ckey = get_ckey_from_range_end(e.token_range_end);
        res.back().set_cell(ckey, to_bytes("streams"), make_set_value(db::cdc_streams_set_type, std::move(streams)), ts);
        res.back().set_cell(ckey, to_bytes("ignore_msb"), int8_t(e.sharding_ignore_msb), ts);

        co_await coroutine::maybe_yield();
    }

    total_size_estimate += size_estimate;

    // Copy mutations n times, where n is picked so that the memory size of all mutations together exceeds `max_command_size`.
    utils::get_local_injector().inject("cdc_generation_mutations_replication", [&res, total_size_estimate, mutation_size_threshold] {
        utils::chunked_vector<mutation> new_res;

        size_t number_of_copies = (mutation_size_threshold / total_size_estimate + 1) * 2;
        for (size_t i = 0; i < number_of_copies; ++i) {
            std::copy(res.begin(), res.end(), std::back_inserter(new_res));
        }

        res = std::move(new_res);
    });

    co_return res;
}

future<utils::chunked_vector<mutation>> get_cdc_generation_mutations_v2(
        schema_ptr s,
        utils::UUID id,
        const cdc::topology_description& desc,
        size_t mutation_size_threshold,
        api::timestamp_type ts) {
    auto pkey = partition_key::from_singular(*s, id);
    auto get_ckey = [s] (dht::token range_end) {
        return clustering_key::from_singular(*s, dht::token::to_int64(range_end));
    };

    auto res = co_await get_common_cdc_generation_mutations(s, pkey, std::move(get_ckey), desc, mutation_size_threshold, ts);
    res.back().set_static_cell(to_bytes("num_ranges"), int32_t(desc.entries().size()), ts);
    co_return res;
}

future<utils::chunked_vector<mutation>> get_cdc_generation_mutations_v3(
        schema_ptr s,
        utils::UUID id,
        const cdc::topology_description& desc,
        size_t mutation_size_threshold,
        api::timestamp_type ts) {
    auto pkey = partition_key::from_singular(*s, CDC_GENERATIONS_V3_KEY);
    auto get_ckey = [&] (dht::token range_end) {
        return clustering_key::from_exploded(*s, {timeuuid_type->decompose(id), long_type->decompose(dht::token::to_int64(range_end))}) ;
    };

    co_return co_await get_common_cdc_generation_mutations(s, pkey, std::move(get_ckey), desc, mutation_size_threshold, ts);
}

// non-static for testing
size_t limit_of_streams_in_topology_description() {
    // Each stream takes 16B and we don't want to exceed 4MB so we can have
    // at most 262144 streams but not less than 1 per vnode.
    return 4 * 1024 * 1024 / 16;
}

// non-static for testing
topology_description limit_number_of_streams_if_needed(topology_description&& desc) {
    uint64_t streams_count = 0;
    for (auto& tr_desc : desc.entries()) {
        streams_count += tr_desc.streams.size();
    }

    size_t limit = std::max(limit_of_streams_in_topology_description(), desc.entries().size());
    if (limit >= streams_count) {
        return std::move(desc);
    }
    size_t streams_per_vnode_limit = limit / desc.entries().size();
    auto entries = std::move(desc).entries();
    auto start = entries.back().token_range_end;
    for (size_t idx = 0; idx < entries.size(); ++idx) {
        auto end = entries[idx].token_range_end;
        if (entries[idx].streams.size() > streams_per_vnode_limit) {
            entries[idx].streams =
                create_stream_ids(idx, start, end, streams_per_vnode_limit, entries[idx].sharding_ignore_msb);
        }
        start = end;
    }
    return topology_description(std::move(entries));
}

// Compute a set of tokens that split the token ring into vnodes.
static auto get_tokens(const std::unordered_set<dht::token>& bootstrap_tokens, const locator::token_metadata_ptr tmptr) {
    auto tokens = tmptr->sorted_tokens();
    auto it = tokens.insert(tokens.end(), bootstrap_tokens.begin(), bootstrap_tokens.end());
    std::sort(it, tokens.end());
    std::inplace_merge(tokens.begin(), it, tokens.end());
    tokens.erase(std::unique(tokens.begin(), tokens.end()), tokens.end());
    return tokens;
}

static token_range_description create_token_range_description(
        size_t index,
        dht::token start,
        dht::token end,
        const noncopyable_function<std::pair<size_t, uint8_t> (dht::token)>& get_sharding_info) {
    token_range_description desc;

    desc.token_range_end = end;

    auto [shard_count, ignore_msb] = get_sharding_info(end);
    desc.streams = create_stream_ids(index, start, end, shard_count, ignore_msb);
    desc.sharding_ignore_msb = ignore_msb;

    return desc;
}

cdc::topology_description make_new_generation_description(
        const std::unordered_set<dht::token>& bootstrap_tokens,
        const noncopyable_function<std::pair<size_t, uint8_t>(dht::token)>& get_sharding_info,
        const locator::token_metadata_ptr tmptr) {
    const auto tokens = get_tokens(bootstrap_tokens, tmptr);

    utils::chunked_vector<token_range_description> vnode_descriptions;
    vnode_descriptions.reserve(tokens.size());

    vnode_descriptions.push_back(create_token_range_description(0, tokens.back(), tokens.front(), get_sharding_info));
    for (size_t idx = 1; idx < tokens.size(); ++idx) {
        vnode_descriptions.push_back(create_token_range_description(idx, tokens[idx - 1], tokens[idx], get_sharding_info));
    }

    return {std::move(vnode_descriptions)};
}

db_clock::time_point new_generation_timestamp(bool add_delay, std::chrono::milliseconds ring_delay) {
    using namespace std::chrono;
    using namespace std::chrono_literals;

    auto ts = db_clock::now();
    if (add_delay && ring_delay != 0ms) {
        ts += 2 * ring_delay + duration_cast<milliseconds>(get_generation_leeway());
    }
    return ts;
}

future<cdc::generation_id> generation_service::legacy_make_new_generation(const std::unordered_set<dht::token>& bootstrap_tokens, bool add_delay) {
    const locator::token_metadata_ptr tmptr = _token_metadata.get();

    // Fetch sharding parameters for a node that owns vnode ending with this token
    // using gossiped application states.
    auto get_sharding_info = [&] (dht::token end) -> std::pair<size_t, uint8_t> {
        if (bootstrap_tokens.contains(end)) {
            return {smp::count, _cfg.ignore_msb_bits};
        } else {
            auto endpoint = tmptr->get_endpoint(end);
            if (!endpoint) {
                throw std::runtime_error(
                        format("Can't find endpoint for token {}", end));
            }
            const auto ep = tmptr->get_endpoint_for_host_id(*endpoint);
            auto sc = get_shard_count(ep, _gossiper);
            return {sc > 0 ? sc : 1, get_sharding_ignore_msb(ep, _gossiper)};
        }
    };

    auto uuid = utils::make_random_uuid();
    auto gen = make_new_generation_description(bootstrap_tokens, get_sharding_info, tmptr);

    // Our caller should ensure that there are normal tokens in the token ring.
    auto normal_token_owners = tmptr->count_normal_token_owners();
    SCYLLA_ASSERT(normal_token_owners);

    if (_feature_service.cdc_generations_v2) {
        cdc_log.info("Inserting new generation data at UUID {}", uuid);
        // This may take a while.
        co_await _sys_dist_ks.local().insert_cdc_generation(uuid, gen, { normal_token_owners });

        // Begin the race.
        cdc::generation_id_v2 gen_id{new_generation_timestamp(add_delay, _cfg.ring_delay), uuid};

        cdc_log.info("New CDC generation: {}", gen_id);
        co_return gen_id;
    }

    // The CDC_GENERATIONS_V2 feature is not enabled: some nodes may still not understand the V2 format.
    // We must create a generation in the old format.

    // If the cluster is large we may end up with a generation that contains
    // large number of streams. This is problematic because we store the
    // generation in a single row (V1 format). For a generation with large number of rows
    // this will lead to a row that can be as big as 32MB. This is much more
    // than the limit imposed by commitlog_segment_size_in_mb. If the size of
    // the row that describes a new generation grows above
    // commitlog_segment_size_in_mb, the write will fail and the new node won't
    // be able to join. To avoid such problem we make sure that such row is
    // always smaller than 4MB. We do that by removing some CDC streams from
    // each vnode if the total number of streams is too large.
    gen = limit_number_of_streams_if_needed(std::move(gen));

    cdc_log.warn(
        "Creating a new CDC generation in the old storage format due to a partially upgraded cluster:"
        " the CDC_GENERATIONS_V2 feature is known by this node, but not enabled in the cluster."
        " The old storage format forces us to create a suboptimal generation."
        " It is recommended to finish the upgrade and then create a new generation either by bootstrapping"
        " a new node or running the checkAndRepairCdcStreams nodetool command.");

    // Begin the race.
    cdc::generation_id_v1 gen_id{new_generation_timestamp(add_delay, _cfg.ring_delay)};

    co_await _sys_dist_ks.local().insert_cdc_topology_description(gen_id, std::move(gen), { normal_token_owners });

    cdc_log.info("New CDC generation: {}", gen_id);
    co_return gen_id;
}

/* Retrieves CDC streams generation timestamp from the given endpoint's application state (broadcasted through gossip).
 * We might be during a rolling upgrade, so the timestamp might not be there (if the other node didn't upgrade yet),
 * but if the cluster already supports CDC, then every newly joining node will propose a new CDC generation,
 * which means it will gossip the generation's timestamp.
 */
static std::optional<cdc::generation_id> get_generation_id_for(const gms::inet_address& endpoint, const gms::endpoint_state& eps) {
    const auto* gen_id_ptr = eps.get_application_state_ptr(gms::application_state::CDC_GENERATION_ID);
    if (!gen_id_ptr) {
        return std::nullopt;
    }
    auto gen_id_string = gen_id_ptr->value();
    cdc_log.trace("endpoint={}, gen_id_string={}", endpoint, gen_id_string);
    return gms::versioned_value::cdc_generation_id_from_string(gen_id_string);
}

static future<std::optional<cdc::topology_description>> retrieve_generation_data_v2(
        cdc::generation_id_v2 id,
        db::system_keyspace& sys_ks,
        db::system_distributed_keyspace& sys_dist_ks) {
    auto cdc_gen = co_await sys_dist_ks.read_cdc_generation(id.id);

    if (!cdc_gen && id.id.is_timestamp()) {
        // If we entered legacy mode due to recovery, we (or some other node)
        // might gossip about a generation that was previously propagated
        // through raft. If that's the case, it will sit in
        // the system.cdc_generations_v3 table.
        //
        // If the provided id is not a timeuuid, we don't want to query
        // the system.cdc_generations_v3 table. This table stores generation
        // ids as timeuuids. If the provided id is not a timeuuid, the
        // generation cannot be in system.cdc_generations_v3. Also, the query
        // would fail with a marshaling error.
        cdc_gen = co_await sys_ks.read_cdc_generation_opt(id.id);
    }

    co_return cdc_gen;
}

static future<std::optional<cdc::topology_description>> retrieve_generation_data(
        cdc::generation_id gen_id,
        db::system_keyspace& sys_ks,
        db::system_distributed_keyspace& sys_dist_ks,
        db::system_distributed_keyspace::context ctx) {
    return std::visit(make_visitor(
    [&] (const cdc::generation_id_v1& id) {
        return sys_dist_ks.read_cdc_topology_description(id, ctx);
    },
    [&] (const cdc::generation_id_v2& id) {
        return retrieve_generation_data_v2(id, sys_ks, sys_dist_ks);
    }
    ), gen_id);
}

static future<> do_update_streams_description(
        cdc::generation_id gen_id,
        db::system_keyspace& sys_ks,
        db::system_distributed_keyspace& sys_dist_ks,
        db::system_distributed_keyspace::context ctx) {
    if (co_await sys_dist_ks.cdc_desc_exists(get_ts(gen_id), ctx)) {
        cdc_log.info("Generation {}: streams description table already updated.", gen_id);
        co_return;
    }

    // We might race with another node also inserting the description, but that's ok. It's an idempotent operation.

    auto topo = co_await retrieve_generation_data(gen_id, sys_ks, sys_dist_ks, ctx);
    if (!topo) {
        throw no_generation_data_exception(gen_id);
    }

    co_await sys_dist_ks.create_cdc_desc(get_ts(gen_id), *topo, ctx);
    cdc_log.info("CDC description table successfully updated with generation {}.", gen_id);
}

/* Inform CDC users about a generation of streams (identified by the given timestamp)
 * by inserting it into the cdc_streams table.
 *
 * Assumes that the cdc_generation_descriptions table contains this generation.
 *
 * Returning from this function does not mean that the table update was successful: the function
 * might run an asynchronous task in the background.
 */
static future<> update_streams_description(
        cdc::generation_id gen_id,
        db::system_keyspace& sys_ks,
        shared_ptr<db::system_distributed_keyspace> sys_dist_ks,
        noncopyable_function<unsigned()> get_num_token_owners,
        abort_source& abort_src) {
    try {
        co_await do_update_streams_description(gen_id, sys_ks, *sys_dist_ks, { get_num_token_owners() });
    } catch (...) {
        cdc_log.warn(
            "Could not update CDC description table with generation {}: {}. Will retry in the background.",
            gen_id, std::current_exception());

        // It is safe to discard this future: we keep system distributed keyspace alive.
        (void)(([] (cdc::generation_id gen_id,
                    db::system_keyspace& sys_ks,
                    shared_ptr<db::system_distributed_keyspace> sys_dist_ks,
                    noncopyable_function<unsigned()> get_num_token_owners,
                    abort_source& abort_src) -> future<> {
            while (true) {
                try {
                    co_await sleep_abortable(std::chrono::seconds(60), abort_src);
                } catch (seastar::sleep_aborted&) {
                    cdc_log.warn( "Aborted update CDC description table with generation {}", gen_id);
                    co_return;
                }
                try {
                    co_await do_update_streams_description(gen_id, sys_ks, *sys_dist_ks, { get_num_token_owners() });
                    co_return;
                } catch (...) {
                    cdc_log.warn(
                        "Could not update CDC description table with generation {}: {}. Will try again.",
                        gen_id, std::current_exception());
                }
            }
        })(gen_id, sys_ks, std::move(sys_dist_ks), std::move(get_num_token_owners), abort_src));
    }
}

static db_clock::time_point as_timepoint(const utils::UUID& uuid) {
    return db_clock::time_point(utils::UUID_gen::unix_timestamp(uuid));
}

static future<std::vector<db_clock::time_point>> get_cdc_desc_v1_timestamps(
        db::system_distributed_keyspace& sys_dist_ks,
        abort_source& abort_src,
        const noncopyable_function<unsigned()>& get_num_token_owners) {
    while (true) {
        try {
            co_return co_await sys_dist_ks.get_cdc_desc_v1_timestamps({ get_num_token_owners() });
        } catch (...) {
            cdc_log.warn(
                    "Failed to retrieve generation timestamps for rewriting: {}. Retrying in 60s.",
                    std::current_exception());
        }
        co_await sleep_abortable(std::chrono::seconds(60), abort_src);
    }
}

// Contains a CDC log table's creation time (extracted from its schema's id)
// and its CDC TTL setting.
struct time_and_ttl {
    db_clock::time_point creation_time;
    int ttl;
};

/*
 * See `maybe_rewrite_streams_descriptions`.
 * This is the long-running-in-the-background part of that function.
 * It returns the timestamp of the last rewritten generation (if any).
 */
static future<std::optional<cdc::generation_id_v1>> rewrite_streams_descriptions(
        std::vector<time_and_ttl> times_and_ttls,
        db::system_keyspace& sys_ks,
        shared_ptr<db::system_distributed_keyspace> sys_dist_ks,
        noncopyable_function<unsigned()> get_num_token_owners,
        abort_source& abort_src) {
    cdc_log.info("Retrieving generation timestamps for rewriting...");
    auto tss = co_await get_cdc_desc_v1_timestamps(*sys_dist_ks, abort_src, get_num_token_owners);
    cdc_log.info("Generation timestamps retrieved.");

    // Find first generation timestamp such that some CDC log table may contain data before this timestamp.
    // This predicate is monotonic w.r.t the timestamps.
    auto now = db_clock::now();
    std::sort(tss.begin(), tss.end());
    auto first = std::partition_point(tss.begin(), tss.end(), [&] (db_clock::time_point ts) {
        // partition_point finds first element that does *not* satisfy the predicate.
        return std::none_of(times_and_ttls.begin(), times_and_ttls.end(),
                [&] (const time_and_ttl& tat) {
            // In this CDC log table there are no entries older than the table's creation time
            // or (now - the table's ttl). We subtract 10s to account for some possible clock drift.
            // If ttl is set to 0 then entries in this table never expire. In that case we look
            // only at the table's creation time.
            auto no_entries_older_than =
                (tat.ttl == 0 ? tat.creation_time : std::max(tat.creation_time, now - std::chrono::seconds(tat.ttl)))
                    - std::chrono::seconds(10);
            return no_entries_older_than < ts;
        });
    });

    // Find first generation timestamp such that some CDC log table may contain data in this generation.
    // This and all later generations need to be written to the new streams table.
    if (first != tss.begin()) {
        --first;
    }

    if (first == tss.end()) {
        cdc_log.info("No generations to rewrite.");
        co_return std::nullopt;
    }

    cdc_log.info("First generation to rewrite: {}", *first);

    bool each_success = true;
    co_await max_concurrent_for_each(first, tss.end(), 10, [&] (db_clock::time_point ts) -> future<> {
        while (true) {
            try {
                co_return co_await do_update_streams_description(cdc::generation_id_v1{ts}, sys_ks, *sys_dist_ks, { get_num_token_owners() });
            } catch (const no_generation_data_exception& e) {
                cdc_log.error("Failed to rewrite streams for generation {}: {}. Giving up.", ts, e);
                each_success = false;
                co_return;
            } catch (...) {
                cdc_log.warn("Failed to rewrite streams for generation {}: {}. Retrying in 60s.", ts, std::current_exception());
            }
            co_await sleep_abortable(std::chrono::seconds(60), abort_src);
        }
    });

    if (each_success) {
        cdc_log.info("Rewriting stream tables finished successfully.");
    } else {
        cdc_log.info("Rewriting stream tables finished, but some generations could not be rewritten (check the logs).");
    }

    if (first != tss.end()) {
        co_return cdc::generation_id_v1{*std::prev(tss.end())};
    }

    co_return std::nullopt;
}

future<> generation_service::maybe_rewrite_streams_descriptions() {
    if (!_db.has_schema(_sys_dist_ks.local().NAME, _sys_dist_ks.local().CDC_DESC_V1)) {
        // This cluster never went through a Scylla version which used this table
        // or the user deleted the table. Nothing to do.
        co_return;
    }

    if (co_await _sys_ks.local().cdc_is_rewritten()) {
        co_return;
    }

    if (_cfg.dont_rewrite_streams) {
        cdc_log.warn("Stream rewriting disabled. Manual administrator intervention may be required...");
        co_return;
    }

    // For each CDC log table get the TTL setting (from CDC options) and the table's creation time
    std::vector<time_and_ttl> times_and_ttls;
    _db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> t) {
        auto& s = *t->schema();
        auto base = cdc::get_base_table(_db, s.ks_name(), s.cf_name());
        if (!base) {
            // Not a CDC log table.
            return;
        }
        auto& cdc_opts = base->cdc_options();
        if (!cdc_opts.enabled()) {
            // This table is named like a CDC log table but it's not one.
            return;
        }

        times_and_ttls.push_back(time_and_ttl{as_timepoint(s.id().uuid()), cdc_opts.ttl()});
    });

    if (times_and_ttls.empty()) {
        // There's no point in rewriting old generations' streams (they don't contain any data).
        cdc_log.info("No CDC log tables present, not rewriting stream tables.");
        co_return co_await _sys_ks.local().cdc_set_rewritten(std::nullopt);
    }

    auto get_num_token_owners = [tm = _token_metadata.get()] { return tm->count_normal_token_owners(); };

    // This code is racing with node startup. At this point, we're most likely still waiting for gossip to settle
    // and some nodes that are UP may still be marked as DOWN by us.
    // Let's sleep a bit to increase the chance that the first attempt at rewriting succeeds (it's still ok if
    // it doesn't - we'll retry - but it's nice if we succeed without any warnings).
    co_await sleep_abortable(std::chrono::seconds(10), _abort_src);

    cdc_log.info("Rewriting stream tables in the background...");
    auto last_rewritten = co_await rewrite_streams_descriptions(
            std::move(times_and_ttls),
            _sys_ks.local(),
            _sys_dist_ks.local_shared(),
            std::move(get_num_token_owners),
            _abort_src);

    co_await _sys_ks.local().cdc_set_rewritten(last_rewritten);
}

static void assert_shard_zero(const sstring& where) {
    if (this_shard_id() != 0) {
        on_internal_error(cdc_log, format("`{}`: must be run on shard 0", where));
    }
}

class and_reducer {
private:
    bool _result = true;
public:
    future<> operator()(bool value) {
        _result = value && _result;
        return make_ready_future<>();
    }
    bool get() {
        return _result;
    }
};

class or_reducer {
private:
    bool _result = false;
public:
    future<> operator()(bool value) {
        _result = value || _result;
        return make_ready_future<>();
    }
    bool get() {
        return _result;
    }
};

class generation_handling_nonfatal_exception : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

constexpr char could_not_retrieve_msg_template[]
        = "Could not retrieve CDC streams with timestamp {} upon gossip event. Reason: \"{}\". Action: {}.";

generation_service::generation_service(
            config cfg, gms::gossiper& g, sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<db::system_keyspace>& sys_ks,
            abort_source& abort_src, const locator::shared_token_metadata& stm, gms::feature_service& f,
            replica::database& db,
            std::function<bool()> raft_topology_change_enabled)
        : _cfg(std::move(cfg))
        , _gossiper(g)
        , _sys_dist_ks(sys_dist_ks)
        , _sys_ks(sys_ks)
        , _abort_src(abort_src)
        , _token_metadata(stm)
        , _feature_service(f)
        , _db(db)
        , _raft_topology_change_enabled(std::move(raft_topology_change_enabled))
{
}

future<> generation_service::stop() {
    try {
        co_await std::move(_cdc_streams_rewrite_complete);
    } catch (...) {
        cdc_log.error("CDC stream rewrite failed: ", std::current_exception());
    }

    if (_joined && (this_shard_id() == 0)) {
        co_await leave_ring();
    }

    _stopped = true;
}

generation_service::~generation_service() {
    SCYLLA_ASSERT(_stopped);
}

future<> generation_service::after_join(std::optional<cdc::generation_id>&& startup_gen_id) {
    assert_shard_zero(__PRETTY_FUNCTION__);

    _gen_id = std::move(startup_gen_id);
    _gossiper.register_(shared_from_this());

    _joined = true;

    // Retrieve the latest CDC generation seen in gossip (if any).
    co_await legacy_scan_cdc_generations();

    // Ensure that the new CDC stream description table has all required streams.
    // See the function's comment for details.
    //
    // Since this depends on the entire cluster (and therefore we cannot guarantee
    // timely completion), run it in the background and wait for it in stop().
    _cdc_streams_rewrite_complete = maybe_rewrite_streams_descriptions();
}

future<> generation_service::leave_ring() {
    assert_shard_zero(__PRETTY_FUNCTION__);
    _joined = false;
    co_await _gossiper.unregister_(shared_from_this());
}

future<> generation_service::on_join(gms::inet_address ep, gms::endpoint_state_ptr ep_state, gms::permit_id pid) {
    return on_change(ep, ep_state->get_application_state_map(), pid);
}

future<> generation_service::on_change(gms::inet_address ep, const gms::application_state_map& states, gms::permit_id pid) {
    assert_shard_zero(__PRETTY_FUNCTION__);

    if (_raft_topology_change_enabled()) {
        return make_ready_future<>();
    }

    return on_application_state_change(ep, states, gms::application_state::CDC_GENERATION_ID, pid, [this] (gms::inet_address ep, const gms::versioned_value& v, gms::permit_id) {
        auto gen_id = gms::versioned_value::cdc_generation_id_from_string(v.value());
        cdc_log.debug("Endpoint: {}, CDC generation ID change: {}", ep, gen_id);

        return legacy_handle_cdc_generation(gen_id);
    });
}

future<> generation_service::check_and_repair_cdc_streams() {
    // FIXME: support Raft group 0-based topology changes
    if (!_joined) {
        throw std::runtime_error("check_and_repair_cdc_streams: node not initialized yet");
    }

    std::optional<cdc::generation_id> latest = _gen_id;
    _gossiper.for_each_endpoint_state([&] (const gms::inet_address& addr, const gms::endpoint_state& state) {
        if (_gossiper.is_left(addr)) {
            cdc_log.info("check_and_repair_cdc_streams ignored node {} because it is in LEFT state", addr);
            return;
        }
        if (!_gossiper.is_normal(addr)) {
            throw std::runtime_error(format("All nodes must be in NORMAL or LEFT state while performing check_and_repair_cdc_streams"
                    " ({} is in state {})", addr, _gossiper.get_gossip_status(state)));
        }

        const auto gen_id = get_generation_id_for(addr, state);
        if (!latest || (gen_id && get_ts(*gen_id) > get_ts(*latest))) {
            latest = gen_id;
        }
    });

    auto tmptr = _token_metadata.get();
    auto sys_dist_ks = get_sys_dist_ks();

    bool should_regenerate = false;

    if (!latest) {
        cdc_log.warn("check_and_repair_cdc_streams: no generation observed in gossip");
        should_regenerate = true;
    } else if (std::holds_alternative<cdc::generation_id_v1>(*latest)
            && _feature_service.cdc_generations_v2) {
        cdc_log.info(
            "Cluster still using CDC generation storage format V1 (id: {}), even though it already understands the V2 format."
            " Creating a new generation using V2.", *latest);
        should_regenerate = true;
    } else {
        cdc_log.info("check_and_repair_cdc_streams: last generation observed in gossip: {}", *latest);

        static const auto timeout_msg = "Timeout while fetching CDC topology description";
        static const auto topology_read_error_note = "Note: this is likely caused by"
                " node(s) being down or unreachable. It is recommended to check the network and"
                " restart/remove the failed node(s), then retry checkAndRepairCdcStreams command";
        static const auto exception_translating_msg = "Translating the exception to `request_execution_exception`";

        std::optional<topology_description> gen;
        try {
            gen = co_await retrieve_generation_data(*latest, _sys_ks.local(), *sys_dist_ks, { tmptr->count_normal_token_owners() });
        } catch (exceptions::request_timeout_exception& e) {
            cdc_log.error("{}: \"{}\". {}.", timeout_msg, e.what(), exception_translating_msg);
            throw exceptions::request_execution_exception(exceptions::exception_code::READ_TIMEOUT,
                    format("{}. {}.", timeout_msg, topology_read_error_note));
        } catch (exceptions::unavailable_exception& e) {
            static const auto unavailable_msg = "Node(s) unavailable while fetching CDC topology description";
            cdc_log.error("{}: \"{}\". {}.", unavailable_msg, e.what(), exception_translating_msg);
            throw exceptions::request_execution_exception(exceptions::exception_code::UNAVAILABLE,
                    format("{}. {}.", unavailable_msg, topology_read_error_note));
        } catch (...) {
            const auto ep = std::current_exception();
            if (is_timeout_exception(ep)) {
                cdc_log.error("{}: \"{}\". {}.", timeout_msg, ep, exception_translating_msg);
                throw exceptions::request_execution_exception(exceptions::exception_code::READ_TIMEOUT,
                        format("{}. {}.", timeout_msg, topology_read_error_note));
            }
            // On exotic errors proceed with regeneration
            cdc_log.error("Exception while reading CDC topology description: \"{}\". Regenerating streams anyway.", ep);
            should_regenerate = true;
        }

        if (!gen) {
            cdc_log.error(
                "Could not find CDC generation with timestamp {} in distributed system tables (current time: {}),"
                " even though some node gossiped about it.",
                latest, db_clock::now());
            should_regenerate = true;
        } else if (!is_cdc_generation_optimal(*gen, *tmptr)) {
            should_regenerate = true;
            cdc_log.info("CDC generation {} needs repair, regenerating", latest);
        }
    }

    if (!should_regenerate) {
        if (latest != _gen_id) {
            co_await legacy_do_handle_cdc_generation(*latest);
        }
        cdc_log.info("CDC generation {} does not need repair", latest);
        co_return;
    }

    const auto new_gen_id = co_await legacy_make_new_generation({}, true);

    // Need to artificially update our STATUS so other nodes handle the generation ID change
    // FIXME: after 0e0282cd nodes do not require a STATUS update to react to CDC generation changes.
    // The artificial STATUS update here should eventually be removed (in a few releases).
    auto status = _gossiper.get_this_endpoint_state_ptr()->get_application_state_ptr(gms::application_state::STATUS);
    if (!status) {
        cdc_log.error("Our STATUS is missing");
        cdc_log.error("Aborting CDC generation repair due to missing STATUS");
        co_return;
    }
    // Update _gen_id first, so that legacy_do_handle_cdc_generation (which will get called due to the status update)
    // won't try to update the gossiper, which would result in a deadlock inside add_local_application_state
    _gen_id = new_gen_id;
    co_await _gossiper.add_local_application_state(
            std::pair(gms::application_state::CDC_GENERATION_ID, gms::versioned_value::cdc_generation_id(new_gen_id)),
            std::pair(gms::application_state::STATUS, *status)
    );
    co_await _sys_ks.local().update_cdc_generation_id(new_gen_id);
}

future<> generation_service::handle_cdc_generation(cdc::generation_id_v2 gen_id) {
    auto ts = get_ts(gen_id);
    if (co_await container().map_reduce(and_reducer(), [ts] (generation_service& svc) {
        return !svc._cdc_metadata.prepare(ts);
    })) {
        co_return;
    }

    auto gen_data = co_await _sys_ks.local().read_cdc_generation(gen_id.id);

    bool using_this_gen = co_await container().map_reduce(or_reducer(), [ts, &gen_data] (generation_service& svc) {
        return svc._cdc_metadata.insert(ts, cdc::topology_description{gen_data});
    });

    if (using_this_gen) {
        cdc_log.info("Started using generation {}.", gen_id);
    }
}

future<> generation_service::legacy_handle_cdc_generation(std::optional<cdc::generation_id> gen_id) {
    assert_shard_zero(__PRETTY_FUNCTION__);

    if (!gen_id) {
        co_return;
    }

    if (!_sys_dist_ks.local_is_initialized() || !_sys_dist_ks.local().started()) {
        on_internal_error(cdc_log, "Legacy handle CDC generation with sys.dist.ks. down");
    }

    // The service should not be listening for generation changes until after the node
    // is bootstrapped and since the node leaves the ring on decommission

    if (co_await container().map_reduce(and_reducer(), [ts = get_ts(*gen_id)] (generation_service& svc) {
        return !svc._cdc_metadata.prepare(ts);
    })) {
        co_return;
    }

    bool using_this_gen = false;
    try {
        using_this_gen = co_await legacy_do_handle_cdc_generation_intercept_nonfatal_errors(*gen_id);
    } catch (generation_handling_nonfatal_exception& e) {
        cdc_log.warn(could_not_retrieve_msg_template, gen_id, e.what(), "retrying in the background");
        legacy_async_handle_cdc_generation(*gen_id);
        co_return;
    } catch (...) {
        cdc_log.error(could_not_retrieve_msg_template, gen_id, std::current_exception(), "not retrying");
        co_return; // Exotic ("fatal") exception => do not retry
    }

    if (using_this_gen) {
        cdc_log.info("Starting to use generation {}", *gen_id);
        co_await update_streams_description(*gen_id, _sys_ks.local(), get_sys_dist_ks(),
                [tmptr = _token_metadata.get()] { return tmptr->count_normal_token_owners(); },
                _abort_src);
    }
}

void generation_service::legacy_async_handle_cdc_generation(cdc::generation_id gen_id) {
    assert_shard_zero(__PRETTY_FUNCTION__);

    (void)(([] (cdc::generation_id gen_id, shared_ptr<generation_service> svc) -> future<> {
        while (true) {
            co_await sleep_abortable(std::chrono::seconds(5), svc->_abort_src);

            try {
                bool using_this_gen = co_await svc->legacy_do_handle_cdc_generation_intercept_nonfatal_errors(gen_id);
                if (using_this_gen) {
                    cdc_log.info("Starting to use generation {}", gen_id);
                    co_await update_streams_description(gen_id, svc->_sys_ks.local(), svc->get_sys_dist_ks(),
                            [tmptr = svc->_token_metadata.get()] { return tmptr->count_normal_token_owners(); },
                            svc->_abort_src);
                }
                co_return;
            } catch (generation_handling_nonfatal_exception& e) {
                cdc_log.warn(could_not_retrieve_msg_template, gen_id, e.what(), "continuing to retry in the background");
            } catch (...) {
                cdc_log.error(could_not_retrieve_msg_template, gen_id, std::current_exception(), "not retrying anymore");
                co_return; // Exotic ("fatal") exception => do not retry
            }

            if (co_await svc->container().map_reduce(and_reducer(), [ts = get_ts(gen_id)] (generation_service& svc) {
                return svc._cdc_metadata.known_or_obsolete(ts);
            })) {
                co_return;
            }
        }
    })(gen_id, shared_from_this()));
}

future<> generation_service::legacy_scan_cdc_generations() {
    assert_shard_zero(__PRETTY_FUNCTION__);

    std::optional<cdc::generation_id> latest;
    _gossiper.for_each_endpoint_state([&] (const gms::inet_address& node, const gms::endpoint_state& eps) {
        auto gen_id = get_generation_id_for(node, eps);
        if (!latest || (gen_id && get_ts(*gen_id) > get_ts(*latest))) {
            latest = gen_id;
        }
    });

    if (latest) {
        cdc_log.info("Latest generation seen during startup: {}", *latest);
        co_await legacy_handle_cdc_generation(latest);
    } else {
        cdc_log.info("No generation seen during startup.");
    }
}

future<bool> generation_service::legacy_do_handle_cdc_generation_intercept_nonfatal_errors(cdc::generation_id gen_id) {
    assert_shard_zero(__PRETTY_FUNCTION__);

    // Use futurize_invoke to catch all exceptions from legacy_do_handle_cdc_generation.
    return futurize_invoke([this, gen_id] {
        return legacy_do_handle_cdc_generation(gen_id);
    }).handle_exception([] (std::exception_ptr ep) -> future<bool> {
        try {
            std::rethrow_exception(ep);
        } catch (exceptions::request_timeout_exception& e) {
            throw generation_handling_nonfatal_exception(e.what());
        } catch (exceptions::unavailable_exception& e) {
            throw generation_handling_nonfatal_exception(e.what());
        } catch (exceptions::read_failure_exception& e) {
            throw generation_handling_nonfatal_exception(e.what());
        } catch (...) {
            const auto ep = std::current_exception();
            if (is_timeout_exception(ep)) {
                throw generation_handling_nonfatal_exception(format("{}", ep));
            }
            throw;
        }
    });
}

future<bool> generation_service::legacy_do_handle_cdc_generation(cdc::generation_id gen_id) {
    assert_shard_zero(__PRETTY_FUNCTION__);

    auto sys_dist_ks = get_sys_dist_ks();
    auto gen = co_await retrieve_generation_data(gen_id, _sys_ks.local(), *sys_dist_ks, { _token_metadata.get()->count_normal_token_owners() });
    if (!gen) {
        throw std::runtime_error(format(
            "Could not find CDC generation {} in distributed system tables (current time: {}),"
            " even though some node gossiped about it.",
            gen_id, db_clock::now()));
    }

    // We always gossip about the generation with the greatest timestamp. Specific nodes may remember older generations,
    // but eventually they forget when their clocks move past the latest generation's timestamp.
    // The cluster as a whole is only interested in the last generation so restarting nodes may learn what it is.
    // We assume that generation changes don't happen ``too often'' so every node can learn about a generation
    // before it is superseded by a newer one which causes nodes to start gossiping the about the newer one.
    // The assumption follows from the requirement of bootstrapping nodes sequentially.
    if (!_gen_id || get_ts(*_gen_id) < get_ts(gen_id)) {
        _gen_id = gen_id;
        co_await _sys_ks.local().update_cdc_generation_id(gen_id);
        co_await _gossiper.add_local_application_state(
                gms::application_state::CDC_GENERATION_ID, gms::versioned_value::cdc_generation_id(gen_id));
    }

    // Return `true` iff the generation was inserted on any of our shards.
    co_return co_await container().map_reduce(or_reducer(), [ts = get_ts(gen_id), &gen] (generation_service& svc) {
        auto gen_ = *gen;
        return svc._cdc_metadata.insert(ts, std::move(gen_));
    });
}

shared_ptr<db::system_distributed_keyspace> generation_service::get_sys_dist_ks() {
    assert_shard_zero(__PRETTY_FUNCTION__);

    if (!_sys_dist_ks.local_is_initialized()) {
        throw std::runtime_error("system distributed keyspace not initialized");
    }

    return _sys_dist_ks.local_shared();
}

db_clock::time_point get_ts(const generation_id& gen_id) {
    return std::visit([] (auto& id) { return id.ts; }, gen_id);
}

} // namespace cdc
