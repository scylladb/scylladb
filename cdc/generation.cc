/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <type_traits>
#include <random>
#include <unordered_set>
#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "keys/keys.hh"
#include "replica/database.hh"
#include "db/system_keyspace.hh"
#include "dht/token-sharding.hh"
#include "locator/token_metadata.hh"
#include "types/set.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "utils/UUID_gen.hh"
#include "utils/stall_free.hh"
#include "utils/to_string.hh"

#include "cdc/generation.hh"
#include "cdc/cdc_options.hh"
#include "cdc/generation_service.hh"
#include "cdc/log.hh"

extern logging::logger cdc_log;

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

stream_state read_stream_state(int8_t val) {
    if (val != std::to_underlying(stream_state::current)
            && val != std::to_underlying(stream_state::closed)
            && val != std::to_underlying(stream_state::opened)) {
        throw std::runtime_error(format("invalid value {} for stream state", val));
    }
    return static_cast<stream_state>(val);
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

bytes stream_id::to_bytes() && {
    return std::move(_value);
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

future<topology_description> topology_description::clone_async() const {
    utils::chunked_vector<token_range_description> vec{};

    co_await utils::reserve_gently(vec, _entries.size());

    for (const auto& entry : _entries) {
        vec.push_back(entry);
        co_await coroutine::maybe_yield();
    }

    co_return topology_description{std::move(vec)};
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
    if (tokens.empty()) {
        on_internal_error(cdc_log, "Attempted to create a CDC generation from an empty list of tokens");
    }

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

generation_service::generation_service(
            config cfg,
            sharded<db::system_keyspace>& sys_ks,
            replica::database& db)
        : _cfg(std::move(cfg))
        , _sys_ks(sys_ks)
        , _db(db)
{
}

future<> generation_service::stop() {
    _stopped = true;
    return make_ready_future<>();
}

generation_service::~generation_service() {
    SCYLLA_ASSERT(_stopped);
}

future<> generation_service::handle_cdc_generation(cdc::generation_id_v2 gen_id) {
    auto ts = get_ts(gen_id);
    if (co_await container().map_reduce(and_reducer(), [ts] (generation_service& svc) {
        return !svc._cdc_metadata.prepare(ts);
    })) {
        co_return;
    }

    auto gen_data = co_await _sys_ks.local().read_cdc_generation(gen_id.id);

    bool using_this_gen = co_await container().map_reduce(or_reducer(), [ts, &gen_data] (generation_service& svc) -> future<bool> {
        // We need to copy it here before awaiting anything to avoid destruction of the captures.
        const auto timestamp = ts;
        topology_description gen_copy = co_await gen_data.clone_async();
        co_return svc._cdc_metadata.insert(timestamp, std::move(gen_copy));
    });

    if (using_this_gen) {
        cdc_log.info("Started using generation {}.", gen_id);
    }
}

db_clock::time_point get_ts(const generation_id& gen_id) {
    return std::visit([] (auto& id) { return id.ts; }, gen_id);
}

future<mutation> create_table_streams_mutation(table_id table, db_clock::time_point stream_ts, const locator::tablet_map& map, api::timestamp_type ts) {
    auto s = db::system_keyspace::cdc_streams_state();

    mutation m(s, partition_key::from_single_value(*s,
        data_value(table.uuid()).serialize_nonnull()
    ));
    m.set_static_cell("timestamp", stream_ts, ts);

    for (auto tid : map.tablet_ids()) {
        auto sid = cdc::stream_id(map.get_last_token(tid), 0);
        auto ck = clustering_key::from_singular(*s, dht::token::to_int64(sid.token()));
        m.set_cell(ck, "stream_id", data_value(std::move(sid).to_bytes()), ts);
        co_await coroutine::maybe_yield();
    }

    co_return std::move(m);
}

future<mutation> create_table_streams_mutation(table_id table, db_clock::time_point stream_ts, const utils::chunked_vector<cdc::stream_id>& stream_ids, api::timestamp_type ts) {
    auto s = db::system_keyspace::cdc_streams_state();

    mutation m(s, partition_key::from_single_value(*s,
        data_value(table.uuid()).serialize_nonnull()
    ));
    m.set_static_cell("timestamp", stream_ts, ts);

    for (const auto& sid : stream_ids) {
        auto ck = clustering_key::from_singular(*s, dht::token::to_int64(sid.token()));
        m.set_cell(ck, "stream_id", data_value(sid.to_bytes()), ts);
        co_await coroutine::maybe_yield();
    }

    co_return std::move(m);
}

utils::chunked_vector<mutation>
make_drop_table_streams_mutations(table_id table, api::timestamp_type ts) {
    utils::chunked_vector<mutation> mutations;
    mutations.reserve(2);
    for (auto s : {db::system_keyspace::cdc_streams_state(),
                   db::system_keyspace::cdc_streams_history()}) {
        mutation m(s, partition_key::from_single_value(*s,
            data_value(table.uuid()).serialize_nonnull()
        ));
        m.partition().apply(tombstone(ts, gc_clock::now()));
        mutations.emplace_back(std::move(m));
    }
    return mutations;
}

future<> generation_service::load_cdc_tablet_streams(std::optional<std::unordered_set<table_id>> changed_tables) {
    // track which tables we expect to get data for, and which we actually get.
    // when a table is dropped we won't get any streams for it. we will use this to
    // know which tables are dropped and remove their stream map from the metadata.
    std::unordered_set<table_id> tables_to_process;
    if (changed_tables) {
        tables_to_process = *changed_tables;
    } else {
        tables_to_process = _cdc_metadata.get_tables_with_cdc_tablet_streams() | std::ranges::to<std::unordered_set<table_id>>();
    }

    auto read_streams_state = [this] (const std::optional<std::unordered_set<table_id>>& tables, noncopyable_function<future<>(table_id, db_clock::time_point, utils::chunked_vector<cdc::stream_id>)> f) -> future<> {
        if (tables) {
            for (auto table : *tables) {
                co_await _sys_ks.local().read_cdc_streams_state(table, [&] (table_id table, db_clock::time_point base_ts, utils::chunked_vector<cdc::stream_id> base_stream_set) -> future<> {
                    return f(table, base_ts, std::move(base_stream_set));
                });
            }
        } else {
            co_await _sys_ks.local().read_cdc_streams_state(std::nullopt, [&] (table_id table, db_clock::time_point base_ts, utils::chunked_vector<cdc::stream_id> base_stream_set) -> future<> {
                return f(table, base_ts, std::move(base_stream_set));
            });
        }
    };

    co_await read_streams_state(changed_tables, [this, &tables_to_process] (table_id table, db_clock::time_point base_ts, utils::chunked_vector<cdc::stream_id> base_stream_set) -> future<> {
        table_streams new_table_map;

        auto append_stream = [&new_table_map] (db_clock::time_point stream_tp, utils::chunked_vector<cdc::stream_id> stream_set) {
            auto ts = std::chrono::duration_cast<api::timestamp_clock::duration>(stream_tp.time_since_epoch()).count();
            new_table_map[ts] = committed_stream_set {stream_tp, std::move(stream_set)};
        };

        // if we already have a loaded streams map, and the base timestamp is unchanged, then read
        // the history entries starting from the latest one we have and append it to the existing map.
        // we can do it because we only append new rows with higher timestamps to the history table.
        std::optional<std::reference_wrapper<const committed_stream_set>> from_streams;
        std::optional<db_clock::time_point> from_ts;
        const auto& all_streams = _cdc_metadata.get_all_tablet_streams();
        if (auto it = all_streams.find(table); it != all_streams.end()) {
            const auto& current_map = *it->second;
            if (current_map.cbegin()->second.ts == base_ts) {
                const auto& latest_entry = current_map.crbegin()->second;
                from_streams = std::cref(latest_entry);
                from_ts = latest_entry.ts;
            }
        }

        if (!from_ts) {
            append_stream(base_ts, std::move(base_stream_set));
        }

        co_await _sys_ks.local().read_cdc_streams_history(table, from_ts, [&] (table_id tid, db_clock::time_point ts, cdc_stream_diff diff) -> future<> {
            const auto& prev_stream_set = new_table_map.empty() ?
                    from_streams->get().streams : std::crbegin(new_table_map)->second.streams;

            append_stream(ts, co_await cdc::metadata::construct_next_stream_set(
                    prev_stream_set, std::move(diff.opened_streams), diff.closed_streams));
        });

        co_await container().invoke_on_all(coroutine::lambda([&] (generation_service& svc) -> future<> {
            table_streams new_table_map_copy;
            for (const auto& [ts, entry] : new_table_map) {
                new_table_map_copy[ts] = entry;
                co_await coroutine::maybe_yield();
            }
            if (!from_ts) {
                svc._cdc_metadata.load_tablet_streams_map(table, std::move(new_table_map_copy));
            } else {
                svc._cdc_metadata.append_tablet_streams_map(table, std::move(new_table_map_copy));
            }
        }));

        tables_to_process.erase(table);
    });

    // the remaining tables have no streams - remove them from the metadata
    co_await container().invoke_on_all([&] (generation_service& svc) {
        for (auto table : tables_to_process) {
            svc._cdc_metadata.remove_tablet_streams_map(table);
        }
    });
}

future<> generation_service::query_cdc_timestamps(table_id table, bool ascending, noncopyable_function<future<>(db_clock::time_point)> f) {
    const auto& all_tables = _cdc_metadata.get_all_tablet_streams();
    auto table_it = all_tables.find(table);
    if (table_it == all_tables.end()) {
        co_return;
    }
    const auto table_streams_ptr = table_it->second; // keep alive
    const auto& table_streams = *table_streams_ptr;

    if (ascending) {
        for (auto it = table_streams.cbegin(); it != table_streams.cend(); ++it) {
            co_await f(it->second.ts);
        }
    } else {
        for (auto it = table_streams.crbegin(); it != table_streams.crend(); ++it) {
            co_await f(it->second.ts);
        }
    }
}

future<> generation_service::query_cdc_streams(table_id table, noncopyable_function<future<>(db_clock::time_point, const utils::chunked_vector<cdc::stream_id>& current, cdc::cdc_stream_diff)> f) {
    const auto& all_tables = _cdc_metadata.get_all_tablet_streams();
    auto table_it = all_tables.find(table);
    if (table_it == all_tables.end()) {
        co_return;
    }
    const auto table_streams_ptr = table_it->second; // keep alive
    const auto& table_streams = *table_streams_ptr;

    auto it_prev = table_streams.end();
    auto it = table_streams.begin();
    while (it != table_streams.end()) {
        const auto& entry = it->second;

        if (it_prev != table_streams.end()) {
            const auto& prev_entry = it_prev->second;
            auto diff = co_await cdc::metadata::generate_stream_diff(prev_entry.streams, entry.streams);
            co_await f(entry.ts, entry.streams, std::move(diff));
        } else {
            co_await f(entry.ts, entry.streams, cdc::cdc_stream_diff{.closed_streams = {}, .opened_streams = entry.streams});
        }
        it_prev = it;
        ++it;
    }
}

future<mutation> get_switch_streams_mutation(table_id table, db_clock::time_point stream_ts, cdc_stream_diff diff, api::timestamp_type ts) {
    auto history_schema = db::system_keyspace::cdc_streams_history();

    auto decomposed_ts = timestamp_type->decompose(stream_ts);
    auto closed_kind = byte_type->decompose(std::to_underlying(stream_state::closed));
    auto opened_kind = byte_type->decompose(std::to_underlying(stream_state::opened));

    mutation m(history_schema, partition_key::from_single_value(*history_schema,
        data_value(table.uuid()).serialize_nonnull()
    ));

    for (auto&& sid : diff.closed_streams) {
        co_await coroutine::maybe_yield();
        auto ck = clustering_key::from_exploded(*history_schema, { decomposed_ts, closed_kind, long_type->decompose(dht::token::to_int64(sid.token())) });
        m.set_cell(ck, "stream_id", data_value(std::move(sid).to_bytes()), ts);
    }
    for (auto&& sid : diff.opened_streams) {
        co_await coroutine::maybe_yield();
        auto ck = clustering_key::from_exploded(*history_schema, { decomposed_ts, opened_kind, long_type->decompose(dht::token::to_int64(sid.token())) });
        m.set_cell(ck, "stream_id", data_value(std::move(sid).to_bytes()), ts);
    }

    co_return std::move(m);
}

future<> generation_service::generate_tablet_resize_update(utils::chunked_vector<canonical_mutation>& muts, table_id table, const locator::tablet_map& new_tablet_map, api::timestamp_type ts) {
    if (!_cdc_metadata.get_all_tablet_streams().contains(table)) {
        // not a CDC table
        co_return;
    }

    utils::chunked_vector<cdc::stream_id> new_streams;
    co_await utils::reserve_gently(new_streams, new_tablet_map.tablet_count());
    for (auto tid : new_tablet_map.tablet_ids()) {
        new_streams.emplace_back(new_tablet_map.get_last_token(tid), 0);
        co_await coroutine::maybe_yield();
    }

    const auto& table_streams = *_cdc_metadata.get_all_tablet_streams().at(table);
    auto current_streams_it = std::crbegin(table_streams);
    if (current_streams_it == std::crend(table_streams)) {
        // no streams at all - this should not happen
        on_internal_error(cdc_log, format("generate_tablet_resize_update: no streams for table {}", table));
    }
    const auto& current_streams = current_streams_it->second;

    auto new_ts = new_generation_timestamp(true, _cfg.ring_delay);
    new_ts = std::max(new_ts, current_streams.ts + std::chrono::milliseconds(1)); // ensure timestamps are increasing

    auto diff = co_await _cdc_metadata.generate_stream_diff(current_streams.streams, new_streams);
    auto mut = co_await get_switch_streams_mutation(table, new_ts, std::move(diff), ts);
    muts.emplace_back(std::move(mut));
}

future<utils::chunked_vector<mutation>> get_cdc_stream_gc_mutations(table_id table, db_clock::time_point base_ts, const utils::chunked_vector<cdc::stream_id>& base_stream_set, api::timestamp_type ts) {
    utils::chunked_vector<mutation> muts;
    muts.reserve(2);

    auto gc_now = gc_clock::now();
    auto tombstone_ts = ts - 1;

    {
        // write the new base stream set to cdc_streams_state
        auto s = db::system_keyspace::cdc_streams_state();
        mutation m(s, partition_key::from_single_value(*s,
            data_value(table.uuid()).serialize_nonnull()
        ));
        m.partition().apply(tombstone(tombstone_ts, gc_now));
        m.set_static_cell("timestamp", data_value(base_ts), ts);

        for (const auto& sid : base_stream_set) {
            co_await coroutine::maybe_yield();
            auto ck = clustering_key::from_singular(*s, dht::token::to_int64(sid.token()));
            m.set_cell(ck, "stream_id", data_value(sid.to_bytes()), ts);
        }
        muts.emplace_back(std::move(m));
    }

    {
        // remove all entries from cdc_streams_history up to the new base
        auto s = db::system_keyspace::cdc_streams_history();
        mutation m(s, partition_key::from_single_value(*s,
            data_value(table.uuid()).serialize_nonnull()
        ));
        auto range = query::clustering_range::make_ending_with({
                clustering_key_prefix::from_single_value(*s, timestamp_type->decompose(base_ts)), true});
        auto bv = bound_view::from_range(range);
        m.partition().apply_delete(*s, range_tombstone{bv.first, bv.second, tombstone{ts, gc_now}});
        muts.emplace_back(std::move(m));
    }

    co_return std::move(muts);
}

table_streams::const_iterator get_new_base_for_gc(const table_streams& streams_map, std::chrono::seconds ttl) {
    // find the most recent timestamp that is older than ttl_seconds, which will become the new base.
    // all streams with older timestamps can be removed because they are closed for more than ttl_seconds
    // (they are all replaced by streams with the newer timestamp).

    auto ts_upper_bound = db_clock::now() - ttl;

    auto it = streams_map.begin();
    while (it != streams_map.end()) {
        auto next_it = std::next(it);
        if (next_it == streams_map.end()) {
            break;
        }

        auto next_tp = next_it->second.ts;
        if (next_tp <= ts_upper_bound) {
            // the next timestamp is older than ttl_seconds, so the current one is obsolete
            it = next_it;
        } else {
            break;
        }
    }

    return it;
}

future<utils::chunked_vector<mutation>> generation_service::garbage_collect_cdc_streams_for_table(table_id table, std::optional<std::chrono::seconds> ttl, api::timestamp_type ts) {
    const auto& table_streams = *_cdc_metadata.get_all_tablet_streams().at(table);

    // if TTL is not provided by the caller then use the table's CDC TTL
    auto base_schema = cdc::get_base_table(_db, *_db.find_schema(table));
    ttl = ttl.or_else([&] -> std::optional<std::chrono::seconds> {
        auto ttl_seconds = base_schema->cdc_options().ttl();
        if (ttl_seconds > 0) {
            return std::chrono::seconds(ttl_seconds);
        } else {
            // ttl=0 means no ttl
            return std::nullopt;
        }
    });
    if (!ttl) {
        co_return utils::chunked_vector<mutation>{};
    }

    auto new_base_it = get_new_base_for_gc(table_streams, *ttl);
    if (new_base_it == table_streams.begin() || new_base_it == table_streams.end()) {
        // nothing to gc
        co_return utils::chunked_vector<mutation>{};
    }

    for (auto it = table_streams.begin(); it != new_base_it; ++it) {
        cdc_log.info("Garbage collecting CDC stream metadata for table {}: removing generation {} because it is older than the CDC TTL of {} seconds",
                table, it->second.ts, *ttl);
    }

    co_return co_await get_cdc_stream_gc_mutations(table, new_base_it->second.ts, new_base_it->second.streams, ts);
}

future<> generation_service::garbage_collect_cdc_streams(utils::chunked_vector<canonical_mutation>& muts, api::timestamp_type ts) {
    for (auto table : _cdc_metadata.get_tables_with_cdc_tablet_streams()) {
        co_await coroutine::maybe_yield();

        auto table_muts = co_await garbage_collect_cdc_streams_for_table(table, std::nullopt, ts);
        for (auto&& m : table_muts) {
            muts.emplace_back(std::move(m));
        }
    }
}

} // namespace cdc
