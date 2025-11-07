/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include <unordered_map>
#include <stdexcept>
#include <regex>
#include <tuple>

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <seastar/core/semaphore.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/abort_source.hh>

#include <fmt/ranges.h>
#include <fmt/std.h>
#include "utils/to_string.hh"

#include "replicated_key_provider.hh"
#include "encryption.hh"
#include "encryption_exceptions.hh"
#include "local_file_provider.hh"
#include "symmetric_key.hh"
#include "replica/database.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include "utils/hash.hh"
#include "service/query_state.hh"
#include "service/storage_service.hh"
#include "service/migration_manager.hh"
#include "compaction/compaction_manager.hh"
#include "replica/distributed_loader.hh"
#include "schema/schema_builder.hh"
#include "db/system_keyspace.hh"
#include "db/extensions.hh"
#include "locator/everywhere_replication_strategy.hh"

namespace encryption {

static auto constexpr KSNAME = "system_replicated_keys";
static auto constexpr TABLENAME = "encrypted_keys";

static logger log("replicated_key_provider");

using utils::UUID;

class replicated_key_provider : public key_provider {
public:
    static constexpr int8_t version = 0;
    /**
     * Header:
     * 1 byte version
     * 16 bytes UUID of key
     * 16 bytes MD5 of UUID
     */
    static const size_t header_size = 33;

    struct key_id {
        key_info info;
        opt_bytes id;

        key_id(key_info k, opt_bytes b = {})
            : info(std::move(k))
            , id(std::move(b))
        {}
        bool operator==(const key_id& v) const {
            return info == v.info && id == v.id;
        }
    };

    struct key_id_hash {
        size_t operator()(const key_id& id) const {
            return utils::tuple_hash()(std::tie(id.info.alg, id.info.len, id.id));
        }
    };

    replicated_key_provider(encryption_context& ctxt, shared_ptr<system_key> system_key, shared_ptr<key_provider> local_provider, const options& opts)
        : _ctxt(ctxt)
        , _system_key(std::move(system_key))
        , _local_provider(std::move(local_provider))
        , _original_options(opts)
        , _upgrade_phaser("replicated_key_provider::upgrade_phaser")
    {
        _ctxt.register_replicated_keys_state_listener([this](db::system_keyspace::replicated_key_provider_version_t version) -> future<> {
            switch (version) {
            case db::system_keyspace::replicated_key_provider_version_t::v1:
                if (!_keys_on.has_value()) {
                    _keys_on = keys_location::sys_repl_keys_ks;
                }
                if (*_keys_on == keys_location::sys_repl_keys_ks) {
                    break;
                }
                on_internal_error(log, seastar::format("Cannot downgrade Replicated Key Provider to version v1 (current state: {})", static_cast<int>(*_keys_on)));
            case db::system_keyspace::replicated_key_provider_version_t::v1_5:
                if (!_keys_on.has_value()) {
                    _keys_on = keys_location::both;
                }
                if (*_keys_on == keys_location::both) {
                    break;
                }
                if (*_keys_on == keys_location::sys_repl_keys_ks) {
                    log.info("Replicated Key Provider upgrading to version v1_5");
                    // Start writing to both tables.
                    _keys_on = keys_location::both;
                    // Drain all write operations to the old table.
                    // Ensures that the topology coordinator will start the data migration
                    // after all writes to the old table have finished.
                    return _upgrade_phaser.advance_and_await();
                }
                on_internal_error(log, seastar::format("Cannot downgrade Replicated Key Provider to version v1_5 (current state: {})", static_cast<int>(*_keys_on)));
            case db::system_keyspace::replicated_key_provider_version_t::v2:
                if (!_keys_on.has_value()) {
                    _keys_on = keys_location::group0;
                }
                if (*_keys_on == keys_location::group0) {
                    break;
                }
                if (*_keys_on == keys_location::both) {
                    log.info("Replicated Key Provider upgrading to version v2");
                    _keys_on = keys_location::group0;
                    break;
                }
                on_internal_error(log, "Cannot upgrade Replicated Key Provider from v1 to v2 directly.");
            }
            return make_ready_future<>();
        });
    }


    future<std::tuple<key_ptr, opt_bytes>> key(const key_info&, opt_bytes = {}) override;
    future<std::tuple<key_ptr, opt_bytes>> key(const key_info&, opt_bytes, utils::chunked_vector<mutation>&, api::timestamp_type) override;
    future<> validate() const override;
    future<> maybe_initialize_tables();
    static future<> do_initialize_tables(::replica::database& db, service::migration_manager&);

    bool should_delay_read(const opt_bytes& id) const override {
        if (!id || _initialized) {
            return false;
        }
        if (!_initialized) {
            return true;
        }
        auto& qp = _ctxt.get_query_processor();
        // This check should be ok, and even somewhat redundant. "Initialized" above
        // will only be set once we've generated/queried a key not passing through here
        // (i.e. a key for write _or_ commit log (should we allow this)). This can only be
        // done if:
        // a.) Encryption was already set up, thus table existed and we waited
        //     for distributed_tables in "ensure_populated"
        // b.) Encryption was added. In which case we are way past bootstrap
        //     and can receive user commands.
        // c.) System table/commit log write, with either first use of this provider,
        //     in which case we're creating the table (here at least) - thus fine,
        //     or again, we've waited through "ensure_populated", so keys are
        //     readable. At worst, we create a few extra keys.
        // Note: currently c.) is not relevant, as we don't support system/commitlog
        //       encryption using repl_prov.
        return !qp.local_is_initialized();
    }

    void print(std::ostream& os) const override {
        os << "system_key=" << _system_key->name() << ", local=" << *_local_provider;
    }

private:
    struct group0_ctx {
        utils::chunked_vector<mutation>& mutations;
        api::timestamp_type timestamp;
    };

    void cache_key(const key_id&, const UUID&, key_ptr);

    static opt_bytes decode_id(const opt_bytes&);
    static bytes encode_id(const UUID&);

    future<std::tuple<UUID, key_ptr>> get_key(const key_info&, opt_bytes = {}, std::optional<group0_ctx> g0_ctx = {});
    future<std::tuple<key_ptr, opt_bytes>> key_impl(const key_info& info, opt_bytes input, std::optional<group0_ctx> g0_ctx = {});
    future<std::optional<std::tuple<UUID, key_ptr>>> find_key(key_id& id, sstring_view cipher, sstring_view ksname, sstring_view tablename);
    future<> create_key(const UUID uuid, key_ptr k, sstring_view cipher);
    future<std::tuple<UUID, key_ptr>> create_key(const key_info&, sstring_view cipher);
    future<std::tuple<UUID, key_ptr>> create_key_in_group0(const key_info&, sstring_view cipher, std::optional<group0_ctx> g0_ctx = {});

    template<typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> query(sstring, Args&& ...);

    future<> force_blocking_flush();

    encryption_context& _ctxt;
    shared_ptr<system_key> _system_key;
    shared_ptr<key_provider> _local_provider;
    std::unordered_map<key_id, std::pair<UUID, key_ptr>, key_id_hash> _keys;
    options _original_options;

    bool _initialized = false;
    bool _use_cache = true;

    enum class keys_location { sys_repl_keys_ks, group0, both };
    std::optional<keys_location> _keys_on;
    utils::phased_barrier _upgrade_phaser;

    // Async/Lazy initialization of `_keys_on` based on the current `replicated_key_provider_version`.
    // Returns the value of the cached optional if engaged, otherwise queries
    // the version from the `encryption_context`. The `encryption_context`
    // returns either a cached value, or loads the value from `system.scylla_local`
    // if not yet cached.
    //
    // Rationale for lazy initialization:
    // 1. The `encryption_context` loads and caches the version from
    //    `system.scylla_local` when it starts (`encryption_context::start()`).
    // 2. Providers can be instantiated very early during node startup while loading
    //    non-system keyspaces (distributed_loader::init_non_system_keyspaces),
    //    *before* `encryption_context::start()` runs. In this case, the version
    //    is not yet cached.
    // 3. The provider cannot know when it is being used, so a mechanism is needed
    //    to load the version on first use, if not already cached.
    future<keys_location> get_keys_location();

    friend class replicated_key_provider_factory;

    static const utils::UUID local_fallback_uuid;
    static const bytes local_fallback_id;
    static const bytes_view local_fallback_bytes;
};

using namespace std::chrono_literals;

static const timeout_config rkp_db_timeout_config {
    5s, 5s, 5s, 5s, 5s, 5s, 5s,
};

static service::query_state& rkp_db_query_state() {
    static thread_local service::client_state cs(service::client_state::internal_tag{}, rkp_db_timeout_config);
    static thread_local service::query_state qs(cs, empty_service_permit());
    return qs;
}

template<typename... Args>
future<::shared_ptr<cql3::untyped_result_set>> replicated_key_provider::query(sstring q, Args&& ...params) {
    auto mode = co_await _ctxt.get_storage_service().local().get_operation_mode();
    if (mode != service::storage_service::mode::STARTING) {
        co_return co_await _ctxt.get_query_processor().local().execute_internal(q, { std::forward<Args>(params)...}, cql3::query_processor::cache_internal::no);
    }
    co_return co_await _ctxt.get_query_processor().local().execute_internal(q, db::consistency_level::ONE, rkp_db_query_state(), { std::forward<Args>(params)...}, cql3::query_processor::cache_internal::no);
}

future<> replicated_key_provider::force_blocking_flush() {
    return _ctxt.get_database().invoke_on_all([](replica::database& db) {
        // if (!Boolean.getBoolean("cassandra.unsafesystem"))
        replica::column_family& cf = db.find_column_family(KSNAME, TABLENAME);
        return cf.flush();
    });
}

void replicated_key_provider::cache_key(const key_id& id, const UUID& uuid, key_ptr k) {
    if (!_use_cache) {
        return;
    }
    _keys[id] = std::make_pair(uuid, k);
    if (!id.id) {
        _keys[key_id(id.info, uuid.serialize())] = std::make_pair(uuid, k);
    }
}

opt_bytes replicated_key_provider::decode_id(const opt_bytes& b) {
    if (b) {
        auto i = b->begin();
        auto v = *i++;
        if (v == version && b->size() == 33) {
            bytes id(i + 1, i + 1 + 16);
            bytes md(i + 1 + 16, b->end());
            if (calculate_md5(id) == md) {
                return id;
            }
        }
    }
    return std::nullopt;
}

bytes replicated_key_provider::encode_id(const UUID& uuid) {
    bytes b{bytes::initialized_later(), header_size};
    auto i = b.begin();
    *i++ = version;
    uuid.serialize(i);
    auto md = calculate_md5(b, 1, 16);
    std::copy(md.begin(), md.end(), i);
    return b;
}

const utils::UUID replicated_key_provider::local_fallback_uuid(0u, 0u); // not valid!
const bytes replicated_key_provider::local_fallback_id = encode_id(local_fallback_uuid);
const bytes_view replicated_key_provider::local_fallback_bytes(local_fallback_id.data() + 1, 16);

future<replicated_key_provider::keys_location> replicated_key_provider::get_keys_location() {
    if (!_keys_on.has_value()) {
        auto version = co_await _ctxt.get_or_load_replicated_keys_version();
        // early return after yield; a value may have been assigned by the state listener while awaiting
        // ensures shard-local atomicity of writes
        if (_keys_on.has_value()) {
            co_return *_keys_on;
        }
        _keys_on = [&] {
            switch (version) {
            case db::system_keyspace::replicated_key_provider_version_t::v1:
                return keys_location::sys_repl_keys_ks;
            case db::system_keyspace::replicated_key_provider_version_t::v1_5:
                return keys_location::both;
            case db::system_keyspace::replicated_key_provider_version_t::v2:
                return keys_location::group0;
            default:
                on_internal_error(log, "Unknown replicated key provider version");
            }
        }();
    }
    co_return *_keys_on;
}

future<std::tuple<key_ptr, opt_bytes>> replicated_key_provider::key(const key_info& info, opt_bytes input) {
    co_return co_await key_impl(info, std::move(input));
}

future<std::tuple<key_ptr, opt_bytes>> replicated_key_provider::key(const key_info& info, opt_bytes input, utils::chunked_vector<mutation>& muts, api::timestamp_type ts) {
    co_return co_await key_impl(info, std::move(input), group0_ctx{muts, ts});
}

future<std::tuple<key_ptr, opt_bytes>> replicated_key_provider::key_impl(const key_info& info, opt_bytes input, std::optional<group0_ctx> g0_ctx) {
    opt_bytes id;

    if (input) { //reading header?
        auto v = *input;
        if (v[0] == version) {
            bytes bid(v.begin() + 1, v.begin() + 1 + 16);
            bytes md(v.begin() + 1 + 16, v.begin() + 1 + 32);
            if (calculate_md5(bid) == md) {
                id = bid;
            }
        }
    }

    bool try_local = id == local_fallback_bytes;

    // if the id indicates the key came from local fallback, don't even
    // try keyspace lookup.
    if (!try_local) {
        try {
            auto [uuid, k] = co_await get_key(info, std::move(id), g0_ctx);
            co_return std::make_tuple(k, encode_id(uuid));
        } catch (std::invalid_argument& e) {
            std::throw_with_nested(configuration_error(e.what()));
        } catch (...) {
            auto ep = std::current_exception();
            log.warn("Exception looking up key {}: {}", info, ep);
            if (_local_provider) {
                try {
                    std::rethrow_exception(ep);
                } catch (replica::no_such_keyspace&) {
                } catch (exceptions::invalid_request_exception&) {
                } catch (exceptions::read_failure_exception&) {
                } catch (...) {
                    std::throw_with_nested(service_error(fmt::format("key: {}", std::current_exception())));
                }
                if (!id) {
                    try_local = true;
                }
            }
            if (!try_local) {
                std::throw_with_nested(service_error(fmt::format("key: {}", std::current_exception())));
            }
        }
    }

    log.warn("Falling back to local key {}", info);
    auto [k, nid] = co_await _local_provider->key(info, id);
    if (nid && nid != id) {
        // local provider does not give ids.
        throw malformed_response_error("Expected null id back from local provider");
    }
    co_return std::make_tuple(k, local_fallback_id);
}

future<std::tuple<UUID, key_ptr>> replicated_key_provider::get_key(const key_info& info, opt_bytes opt_id, std::optional<group0_ctx> g0_ctx) {
    auto keys_on = co_await get_keys_location();
    auto p = _upgrade_phaser.start();

    if (!_initialized) {
        co_await maybe_initialize_tables();
    }

    // Search the key in the local shard's cache first.
    key_id id(info, opt_id);
    auto i = _keys.find(id);
    if (i != _keys.end()) {
        co_return std::tuple(i->second.first, i->second.second);
    }

    // If not found in the cache, search in the `encrypted_keys` table, or create a new one.
    // Creating a key requires a group0 transaction, which can only be executed from shard 0.
    // Therefore, dispatch to shard 0.
    if (this_shard_id() != 0) {
        auto [uuid, key_bytes] = co_await smp::submit_to(0, [this, info, opt_id, g0_ctx]() -> future<std::tuple<UUID, bytes>> {
            auto provider = _ctxt.get_provider(_original_options);
            auto [uuid, key] = co_await dynamic_cast<replicated_key_provider*>(provider.get())->get_key(info, opt_id, g0_ctx);
            co_return std::tuple(uuid, key->key());
        });
        auto key = make_shared<symmetric_key>(info, key_bytes);
        // If we reach here, it means that we are not running in shard 0, so no
        // g0_ctx was given, and therefore the key returned from shard 0 has been
        // committed to Raft. We can safely cache the key here.
        cache_key(id, uuid, key);
        co_return std::tuple(uuid, key);
    }

    // TODO: origin does non-cql acquire of all available keys from
    // replicas in the "host_ids" table iff we get here during boot.
    // For now, ignore this and assume that if we have a sstable with
    // key X, we should have a local replica of X as well, given
    // the "everywhere strategy of the keys table.

    auto cipher = info.alg.substr(0, info.alg.find('/')); // e.g. "AES"

    auto res = co_await [&] -> future<std::optional<std::tuple<UUID, key_ptr>>> {
        switch (keys_on) {
        case keys_location::sys_repl_keys_ks:
        case keys_location::both:
            // Try to find the key in the old table first.
            // If not found, we will call create_key_in_group0 in the write path below,
            // which will check for the key in the group0 table as well.
            co_return co_await find_key(id, cipher, KSNAME, TABLENAME);
        case keys_location::group0:
            co_return co_await find_key(id, cipher, db::system_keyspace::NAME, db::system_keyspace::ENCRYPTED_KEYS);
        }
    }();
    if (res) {
        auto [uuid, k] = *res;
        cache_key(id, uuid, k);
        co_return std::tuple(uuid, k);
    }

    static const char* key_loc_msgs[] = {"", " in group0", " in both tables"};
    log.debug("No key found. Generating new key{}", key_loc_msgs[static_cast<int>(keys_on)]);

    auto [uuid, k] = co_await [&] -> future<std::tuple<UUID, key_ptr>> {
        switch (keys_on) {
        case keys_location::sys_repl_keys_ks:
            co_return co_await create_key(info, cipher);
        case keys_location::both: {
            // First create key in group0 (or retrieve an existing key).
            auto [uuid, k] = co_await create_key_in_group0(info, cipher, g0_ctx);
            // Then write it to the old table as well (in case migration fails and we need to roll back).
            co_await create_key(uuid, k, cipher);
            co_return std::tuple(uuid, k);
        }
        case keys_location::group0:
            co_return co_await create_key_in_group0(info, cipher, g0_ctx);
        }
    }();
    co_return std::tuple(uuid, k);
}

future<std::optional<std::tuple<UUID, key_ptr>>> replicated_key_provider::find_key(key_id& id, sstring_view cipher, sstring_view ksname, sstring_view tablename) {
    shared_ptr<cql3::untyped_result_set> res;
    if (id.id) {
        auto uuid = utils::UUID_gen::get_UUID(*id.id);
        log.debug("Finding key {} ({})", uuid, id.info);
        auto s = fmt::format("SELECT * FROM {}.{} WHERE key_file=? AND cipher=? AND strength=? AND key_id=?;", ksname, tablename);
        res = co_await query(std::move(s), _system_key->name(), cipher, int32_t(id.info.len), uuid);

        // if we find nothing, and we actually queried a specific key (by uuid), we've failed.
        if (res->empty()) {
            log.debug("Could not find key {}", id.id);
            throw std::runtime_error(fmt::format("Unable to find key for cipher={} strength={} id={}", cipher, id.info.len, uuid));
        }
    } else {
        log.debug("Finding key ({})", id.info);
        auto s = fmt::format("SELECT * FROM {}.{} WHERE key_file=? AND cipher=? AND strength=? LIMIT 1;", ksname, tablename);
        res = co_await query(std::move(s), _system_key->name(), cipher, int32_t(id.info.len));

        if (res->empty()) {
            co_return std::nullopt;
        }
    }

    // found it
    auto& row = res->one();
    auto uuid = row.get_as<UUID>("key_id");
    auto ks = row.get_as<sstring>("key");
    auto kb = base64_decode(ks);
    auto b = co_await _system_key->decrypt(kb);
    auto k = make_shared<symmetric_key>(id.info, b);
    co_return std::tuple(uuid, k);
}

future<> replicated_key_provider::create_key(const UUID uuid, key_ptr k, sstring_view cipher) {

    auto b = co_await _system_key->encrypt(k->key());
    auto ks = base64_encode(b);

    auto insert_stmt = fmt::format("INSERT INTO {}.{} (key_file, cipher, strength, key_id, key) VALUES (?, ?, ?, ?, ?)", KSNAME, TABLENAME);
    co_await query(std::move(insert_stmt), _system_key->name(), cipher, int32_t(k->info().len), uuid, ks);
    log.trace("Flushing key table");
    co_await force_blocking_flush();
}

future<std::tuple<UUID, key_ptr>> replicated_key_provider::create_key(const key_info& info, sstring_view cipher) {
    UUID uuid = utils::UUID_gen::get_time_UUID();

    log.debug("Generating key {} ({})", uuid, info);

    auto k = make_shared<symmetric_key>(info);
    co_await create_key(uuid, k, cipher);
    co_return std::tuple(uuid, k);
}

future<std::tuple<UUID, key_ptr>> replicated_key_provider::create_key_in_group0(const key_info& info, sstring_view cipher, std::optional<group0_ctx> g0_ctx) {
    if (this_shard_id() != 0) {
        on_internal_error(log, "create_key_in_group0: must run on shard 0");
    }

    key_id id(info, {});
    UUID uuid = utils::UUID_gen::get_time_UUID();
    auto k = make_shared<symmetric_key>(info);
    auto b = co_await _system_key->encrypt(k->key());
    auto ks = base64_encode(b);

    auto& qp = _ctxt.get_query_processor().local();
    auto insert_stmt = fmt::format("INSERT INTO {}.{} (key_file, cipher, strength, key_id, key) VALUES (?, ?, ?, ?, ?)", db::system_keyspace::NAME, db::system_keyspace::ENCRYPTED_KEYS);
    std::vector<data_value_or_unset> values = {
        data_value_or_unset(_system_key->name()),
        data_value_or_unset(cipher),
        data_value_or_unset(int32_t(info.len)),
        data_value_or_unset(uuid),
        data_value_or_unset(ks)
    };

    if (g0_ctx) {
        // A group0 transaction is in progress; just return the mutations and let the caller handle them.
        // Also do not store the key in the cache, since the key hasn't been stored on the table yet (group0 transaction hasn't been committed).
        auto muts = co_await qp.get_mutations_internal(insert_stmt, rkp_db_query_state(), g0_ctx->timestamp, values);
        std::ranges::move(muts, std::back_inserter(g0_ctx->mutations));
        co_return std::make_tuple(uuid, k);
    }

    // We reach here only when no group0 transaction is in progress.

    auto* group0_client = _ctxt.get_storage_service().local().get_group0_client();
    if (!group0_client) {
        on_internal_error(log, "create_key_in_group0: storage_service has not been initialized");
    }
    auto& as = _ctxt.get_storage_service().local().get_group0_abort_source();

    while (true) {
        as.check();

        auto guard = co_await group0_client->start_operation(as);

        log.debug("Checking if key exists after group0 read barrier");
        auto res = co_await find_key(id, cipher, db::system_keyspace::NAME, db::system_keyspace::ENCRYPTED_KEYS);
        if (res) {
            co_return *res;
        }

        log.debug("Generating key {} ({})", uuid, info);

        auto timestamp = guard.write_timestamp();
        auto muts = co_await qp.get_mutations_internal(insert_stmt, rkp_db_query_state(), timestamp, values);
        utils::chunked_vector<mutation> cmuts = {muts.begin(), muts.end()};

        auto group0_cmd = group0_client->prepare_command(
            ::service::write_mutations{.mutations{cmuts.begin(), cmuts.end()}},
            guard,
            "encryption at rest: insert key"
        );

        try {
            co_await group0_client->add_entry(std::move(group0_cmd), std::move(guard), as, ::service::raft_timeout{});
        } catch (::service::group0_concurrent_modification&) {
            log.debug("Concurrent modification detected when inserting key, retrying.");
            continue;
        }
        break;
    }
    // The group0 transaction has been committed, it's safe to store the key in the cache.
    cache_key(id, uuid, k);

    co_return std::make_tuple(uuid, k);
}

future<> replicated_key_provider::validate() const {
    try {
        co_await _system_key->validate();
    } catch (...) {
        std::throw_with_nested(std::invalid_argument(fmt::format("Could not validate system key: {}", _system_key->name())));
    }
    if (_local_provider){
        co_await _local_provider->validate();
    }
}

schema_ptr encrypted_keys_table() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(KSNAME, TABLENAME);
        return schema_builder(KSNAME, TABLENAME, std::make_optional(id))
                .with_column("key_file", utf8_type, column_kind::partition_key)
                .with_column("cipher", utf8_type, column_kind::partition_key)
                .with_column("strength", int32_type, column_kind::clustering_key)
                .with_column("key_id", timeuuid_type, column_kind::clustering_key)
                .with_column("key", utf8_type)
                .with_hash_version()
                .build();
    }();
    return schema;
}

future<> replicated_key_provider::maybe_initialize_tables() {
    if (_initialized) {
        co_return;
    }
    if (co_await get_keys_location() == keys_location::sys_repl_keys_ks) {
        co_await do_initialize_tables(_ctxt.get_database().local(), _ctxt.get_migration_manager().local());
    }
    _initialized = true;
}

future<> replicated_key_provider::do_initialize_tables(::replica::database& db, service::migration_manager& mm) {
    if (db.has_schema(KSNAME, TABLENAME)) {
        co_return;
    }

    log.debug("Creating keyspace and table");
    if (!db.has_keyspace(KSNAME)) {
        auto group0_guard = co_await mm.start_group0_operation();
        auto ts = group0_guard.write_timestamp();
        try {
            auto ksm = keyspace_metadata::new_keyspace(
                    KSNAME,
                    "org.apache.cassandra.locator.EverywhereStrategy",
                    {},
                    std::nullopt,
                    std::nullopt,
                    true);
            co_await mm.announce(service::prepare_new_keyspace_announcement(db, ksm, ts), std::move(group0_guard), fmt::format("encryption at rest: create keyspace {}", KSNAME));
        } catch (exceptions::already_exists_exception&) {
        }
    }
    auto group0_guard = co_await mm.start_group0_operation();
    auto ts = group0_guard.write_timestamp();
    try {
        co_await mm.announce(co_await service::prepare_new_column_family_announcement(mm.get_storage_proxy(), encrypted_keys_table(), ts), std::move(group0_guard),
                             fmt::format("encryption at rest: create table {}.{}", KSNAME, TABLENAME));
    } catch (exceptions::already_exists_exception&) {
    }
    auto& ks = db.find_keyspace(KSNAME);
    auto& rs = ks.get_replication_strategy();
    // should perhaps check name also..
    if (rs.get_type() != locator::replication_strategy_type::everywhere_topology) {
        // TODO: reset to everywhere + repair.
    }
}

const size_t replicated_key_provider::header_size;

replicated_keys_migration_manager::replicated_keys_migration_manager(encryption_context& ctxt)
    : _ctxt(ctxt)
{}

future<> replicated_keys_migration_manager::migrate_to_v1_5(db::system_keyspace& sys_ks,
        cql3::query_processor& qp, service::raft_group0_client& group0_client, abort_source& as,
        service::group0_guard&& guard) {
    log.info("Starting migration of replicated encryption keys to v1_5");
    auto version_mut = co_await sys_ks.make_replicated_key_provider_version_mutation(
        guard.write_timestamp(),
        db::system_keyspace::replicated_key_provider_version_t::v1_5
    );

    // write the version as topology_change so that we can apply
    // the change to the replicated key providers in topology_state_load
    service::topology_change change {
        .mutations{canonical_mutation(std::move(version_mut))},
    };
    auto group0_cmd = group0_client.prepare_command(
        std::move(change),
        guard,
        "migrate encrypted_keys to v1_5"
    );
    co_await group0_client.add_entry(std::move(group0_cmd), std::move(guard), as);

    log.info("Completed migration of replicated encryption keys to v1_5");
}

future<> replicated_keys_migration_manager::migrate_to_v2(db::system_keyspace& sys_ks,
        cql3::query_processor& qp, service::raft_group0_client& group0_client, abort_source& as,
        service::group0_guard&& guard) {
    log.info("Starting migration of replicated encryption keys to v2");

    utils::chunked_vector<mutation> migration_muts;

    if (qp.db().has_keyspace(KSNAME)) {
        auto sel_stmt = fmt::format("SELECT * FROM {}.{};", KSNAME, TABLENAME);
        auto rows = co_await qp.execute_internal(sel_stmt, db::consistency_level::ALL, rkp_db_query_state(), {}, cql3::query_processor::cache_internal::no);

        migration_muts.reserve(rows->size() + 1);

        for (const auto& row: *rows) {
            auto insert_stmt = fmt::format("INSERT INTO {}.{} (key_file, cipher, strength, key_id, key) VALUES (?, ?, ?, ?, ?)", db::system_keyspace::NAME, db::system_keyspace::ENCRYPTED_KEYS);
            auto key_file = row.get_as<sstring>("key_file");
            auto cipher = row.get_as<sstring>("cipher");
            auto strength = row.get_as<int32_t>("strength");
            auto key_id = row.get_as<UUID>("key_id");
            auto key = row.get_as<sstring>("key");

            std::vector<data_value_or_unset> values = {
                data_value_or_unset(key_file),
                data_value_or_unset(cipher),
                data_value_or_unset(strength),
                data_value_or_unset(key_id),
                data_value_or_unset(key)
            };

            auto muts = co_await qp.get_mutations_internal(insert_stmt, rkp_db_query_state(), guard.write_timestamp(), values);
            std::ranges::move(muts, std::back_inserter(migration_muts));
        }
    } else {
        log.info("Keyspace {} doesn't exist. Nothing to migrate.", KSNAME);
        migration_muts.reserve(1);
    }

    auto version_mut = co_await sys_ks.make_replicated_key_provider_version_mutation(
        guard.write_timestamp(),
        db::system_keyspace::replicated_key_provider_version_t::v2
    );
    migration_muts.push_back(std::move(version_mut));

    service::topology_change change {
        .mutations{migration_muts.begin(), migration_muts.end()}
    };
    auto group0_cmd = group0_client.prepare_command(
        std::move(change),
        guard,
        "migrate encrypted_keys to v2"
    );
    co_await group0_client.add_entry(std::move(group0_cmd), std::move(guard), as);

    log.info("Completed migration of replicated encryption keys to v2");
}

future<> replicated_keys_migration_manager::upgrade_to_v1_5() {
    log.debug("Notifying replicated key providers about state change to version v1.5");
    co_await _ctxt.set_replicated_keys_version(db::system_keyspace::replicated_key_provider_version_t::v1_5);
    co_await _ctxt.notify_replicated_keys_state_change(db::system_keyspace::replicated_key_provider_version_t::v1_5);
}

future<> replicated_keys_migration_manager::upgrade_to_v2() {
    log.debug("Notifying replicated key providers about state change to version v2");
    co_await _ctxt.set_replicated_keys_version(db::system_keyspace::replicated_key_provider_version_t::v2);
    co_await _ctxt.notify_replicated_keys_state_change(db::system_keyspace::replicated_key_provider_version_t::v2);
}


replicated_key_provider_factory::replicated_key_provider_factory()
{}

replicated_key_provider_factory::~replicated_key_provider_factory()
{}

namespace bfs = std::filesystem;

shared_ptr<key_provider> replicated_key_provider_factory::get_provider(encryption_context& ctxt, const options& map) {
    opt_wrapper opts(map);
    auto system_key_name = opts(SYSTEM_KEY_FILE).value_or("system_key");
    if (system_key_name.find('/') != sstring::npos) {
        throw std::invalid_argument("system_key cannot contain '/'");
    }

    auto system_key = ctxt.get_system_key(system_key_name);
    auto local_key_file = bfs::absolute(bfs::path(opts(SECRET_KEY_FILE).value_or(default_key_file_path)));

    if (system_key->is_local() && bfs::absolute(bfs::path(system_key->name())) == local_key_file) {
        throw std::invalid_argument("system key and local key cannot be the same");
    }

    auto name = system_key->name() + ":" + local_key_file.string();
    auto debug = opts("DEBUG");
    if (debug) {
        name = name + ":" + *debug;
    }
    auto p = ctxt.get_cached_provider(name);
    if (!p) {
        auto rp = seastar::make_shared<replicated_key_provider>(ctxt, std::move(system_key), local_file_provider_factory::find(ctxt, local_key_file.string()), map);
        ctxt.cache_provider(name, rp);

        if (debug && debug->find("nocache") != sstring::npos) {
            log.debug("Turn off cache");
            rp->_use_cache = false;
        }
        p = std::move(rp);
    }

    return p;
}

void replicated_key_provider_factory::init(db::extensions& exts) {
    exts.add_extension_internal_keyspace(KSNAME);
}

future<> replicated_key_provider_factory::on_started(encryption_context& ctxt, ::replica::database& db, service::migration_manager& mm) {
    (void)co_await ctxt.get_or_load_replicated_keys_version(); // load the version if not loaded yet
    co_await replicated_key_provider::do_initialize_tables(db, mm);
}

future<std::unique_ptr<replicated_keys_migration_manager>> replicated_key_provider_factory::create_migration_manager_if_needed(encryption_context& ctxt) {
    auto& sys_ks = ctxt.get_storage_service().local().get_system_keyspace();
    auto version = co_await sys_ks.get_replicated_key_provider_version();

    bool needs_migration = (version != db::system_keyspace::replicated_key_provider_version_t::v2);

    if (needs_migration) {
        log.info("Replicated keys migration is needed (current version: {})", static_cast<int64_t>(version));
        co_return std::make_unique<replicated_keys_migration_manager>(ctxt);
    } else {
        log.debug("Replicated keys migration not needed (already on version v2)");
        co_return nullptr;
    }
}

}
