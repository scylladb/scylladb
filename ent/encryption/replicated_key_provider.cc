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

    replicated_key_provider(encryption_context& ctxt, shared_ptr<system_key> system_key, shared_ptr<key_provider> local_provider)
        : _ctxt(ctxt)
        , _system_key(std::move(system_key))
        , _local_provider(std::move(local_provider))
    {}


    future<std::tuple<key_ptr, opt_bytes>> key(const key_info&, opt_bytes = {}) override;
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
        //     readble. At worst, we create a few extra keys.
        // Note: currently c.) is not relevant, as we don't support system/commitlog
        //       encryption using repl_prov.
        return !qp.local_is_initialized();
    }

    void print(std::ostream& os) const override {
        os << "system_key=" << _system_key->name() << ", local=" << *_local_provider;
    }

private:
    void store_key(const key_id&, const UUID&, key_ptr);

    static opt_bytes decode_id(const opt_bytes&);
    static bytes encode_id(const UUID&);

    future<std::tuple<UUID, key_ptr>> get_key(const key_info&, opt_bytes = {});

    future<key_ptr> load_or_create(const key_info&);
    future<key_ptr> load_or_create_local(const key_info&);
    future<> read_key_file();
    future<> write_key_file();

    template<typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> query(sstring, Args&& ...);

    future<> force_blocking_flush();

    encryption_context& _ctxt;
    shared_ptr<system_key> _system_key;
    shared_ptr<key_provider> _local_provider;
    std::unordered_map<key_id, std::pair<UUID, key_ptr>, key_id_hash> _keys;

    bool _initialized = false;
    bool _use_cache = true;

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

void replicated_key_provider::store_key(const key_id& id, const UUID& uuid, key_ptr k) {
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

future<std::tuple<key_ptr, opt_bytes>> replicated_key_provider::key(const key_info& info, opt_bytes input) {
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
            auto [uuid, k] = co_await get_key(info, std::move(id));
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

future<std::tuple<UUID, key_ptr>> replicated_key_provider::get_key(const key_info& info, opt_bytes opt_id) {
    if (!_initialized) {
        co_await maybe_initialize_tables();
    }

    key_id id(info, std::move(opt_id));
    auto i = _keys.find(id);
    if (i != _keys.end()) {
        co_return std::tuple(i->second.first, i->second.second);
    }

    // TODO: origin does non-cql acquire of all available keys from
    // replicas in the "host_ids" table iff we get here during boot.
    // For now, ignore this and assume that if we have a sstable with
    // key X, we should have a local replica of X as well, given
    // the "everywhere strategy of the keys table.

    auto cipher = info.alg.substr(0, info.alg.find('/')); // e.g. "AES"

    UUID uuid;
    shared_ptr<cql3::untyped_result_set> res;

    if (id.id) {
        uuid = utils::UUID_gen::get_UUID(*id.id);
        log.debug("Finding key {} ({})", uuid, info);
        auto s = fmt::format("SELECT * FROM {}.{} WHERE key_file=? AND cipher=? AND strength=? AND key_id=?;", KSNAME, TABLENAME);
        res = co_await query(std::move(s), _system_key->name(), cipher, int32_t(id.info.len), uuid);

        // if we find nothing, and we actually queried a specific key (by uuid), we've failed.
        if (res->empty()) {
            log.debug("Could not find key {}", id.id);
            throw std::runtime_error(fmt::format("Unable to find key for cipher={} strength={} id={}", cipher, id.info.len, uuid));
        }
    } else {
        log.debug("Finding key ({})", info);
        auto s = fmt::format("SELECT * FROM {}.{} WHERE key_file=? AND cipher=? AND strength=? LIMIT 1;", KSNAME, TABLENAME);
        res = co_await query(std::move(s), _system_key->name(), cipher, int32_t(id.info.len));
    }

    // otoh, if we don't need a specific key, we can just create a new one (writing a sstable)
    if (res->empty()) {
        uuid = utils::UUID_gen::get_time_UUID();

        log.debug("No key found. Generating {}", uuid);

        auto k = make_shared<symmetric_key>(id.info);
        store_key(id, uuid, k);

        auto b = co_await _system_key->encrypt(k->key());
        auto ks = base64_encode(b);
        log.trace("Inserting generated key {}", uuid);
        co_await query(fmt::format("INSERT INTO {}.{} (key_file, cipher, strength, key_id, key) VALUES (?, ?, ?, ?, ?)", 
            KSNAME, TABLENAME), _system_key->name(), cipher, int32_t(id.info.len), uuid, ks
        );
        log.trace("Flushing key table");
        co_await force_blocking_flush();

        co_return std::tuple(uuid, k);
    }

    // found it
    auto& row = res->one();
    uuid = row.get_as<UUID>("key_id");
    auto ks = row.get_as<sstring>("key");
    auto kb = base64_decode(ks);
    auto b = co_await _system_key->decrypt(kb);
    auto k = make_shared<symmetric_key>(id.info, b);
    store_key(id, uuid, k);

    co_return std::tuple(uuid, k);
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
    if (!_initialized) {
        co_await do_initialize_tables(_ctxt.get_database().local(), _ctxt.get_migration_manager().local());
        _initialized = true;
    }
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
        auto rp = seastar::make_shared<replicated_key_provider>(ctxt, std::move(system_key), local_file_provider_factory::find(ctxt, local_key_file.string()));
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

future<> replicated_key_provider_factory::on_started(::replica::database& db, service::migration_manager& mm) {
    return replicated_key_provider::do_initialize_tables(db, mm);
}

}
