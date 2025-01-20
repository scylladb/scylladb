/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include <map>
#include <unordered_map>
#include <tuple>
#include <stdexcept>
#include <regex>
#include <algorithm>
#include <ios>

#include <string.h>

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>

#include <boost/range/adaptor/map.hpp>
#include <boost/filesystem.hpp>

#include <seastar/core/seastar.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

#include <fmt/ranges.h>
#include <fmt/ostream.h>
#include <fmt/std.h>
#include "utils/to_string.hh"

#include "compress.hh"
#include "encryption.hh"
#include "symmetric_key.hh"
#include "local_file_provider.hh"
#include "replicated_key_provider.hh"
#include "kmip_key_provider.hh"
#include "kmip_host.hh"
#include "kms_key_provider.hh"
#include "kms_host.hh"
#include "gcp_key_provider.hh"
#include "gcp_host.hh"
#include "bytes.hh"
#include "utils/class_registrator.hh"
#include "cql3/query_processor.hh"
#include "db/extensions.hh"
#include "db/system_keyspace.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "schema/schema.hh"
#include "sstables/sstables.hh"
#include "service/storage_service.hh"
#include "service/migration_manager.hh"
#include "db/commitlog/commitlog_extensions.hh"
#include "encrypted_file_impl.hh"
#include "encryption_config.hh"
#include "utils/UUID_gen.hh"
#include "init.hh"

static seastar::logger logg{"encryption"};

namespace encryption {

static constexpr auto REPLICATED_KEY_PROVIDER_FACTORY = "ReplicatedKeyProviderFactory";
static constexpr auto LOCAL_FILE_SYSTEM_KEY_PROVIDER_FACTORY = "LocalFileSystemKeyProviderFactory";
static constexpr auto KMIP_KEY_PROVIDER_FACTORY = "KmipKeyProviderFactory";
static constexpr auto KMS_KEY_PROVIDER_FACTORY = "KmsKeyProviderFactory";
static constexpr auto GCP_KEY_PROVIDER_FACTORY = "GcpKeyProviderFactory";

bytes base64_decode(const sstring& s, size_t off, size_t len) {
    if (off >= s.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, s.size() - off);
    auto n = (len / 4) * 3;
    bytes b{bytes::initialized_later(), n};

    // EVP_DecodeBlock does not handle padding well (i.e. it returns
    // data with actual padding. This is not what we want, since
    // we need to allow zeros in data.
    // Must thus do decoding the hard way...

    std::unique_ptr<EVP_ENCODE_CTX, void (*)(EVP_ENCODE_CTX*)> ctxt(EVP_ENCODE_CTX_new(), &EVP_ENCODE_CTX_free);

    ::EVP_DecodeInit(ctxt.get());

    int outl = 0;
    auto r = ::EVP_DecodeUpdate(ctxt.get(), reinterpret_cast<uint8_t*>(b.data()), &outl, reinterpret_cast<const uint8_t *>(s.data() + off),
                    int(len));
    if (r < 0) {
        throw std::invalid_argument("Could not decode: " + s);
    }

    int outl2 = 0;
    r = ::EVP_DecodeFinal(ctxt.get(), reinterpret_cast<uint8_t*>(b.data() + outl), &outl2);
    if (r < 0) {
        throw std::invalid_argument("Could not decode: " + s);
    }
    b.resize(outl + outl2);
    return b;
}

sstring base64_encode(const bytes& b, size_t off, size_t len) {
    if (off >= b.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, b.size() - off);
    auto n = ((len + 2) / 3) * 4;
    sstring s{sstring::initialized_later(), n};
    auto r = EVP_EncodeBlock(reinterpret_cast<uint8_t *>(s.data()),
                    reinterpret_cast<const uint8_t*>(b.data() + off), int(len));
    if (r < 0) {
        throw std::invalid_argument("Could not encode");
    }
    s.resize(r);
    return s;
}

bytes calculate_md5(const bytes& b, size_t off, size_t len) {
    if (off >= b.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, b.size() - off);
    bytes res{bytes::initialized_later(), MD5_DIGEST_LENGTH};
#if OPENSSL_VERSION_NUMBER >= (3<<28)
    EVP_MD_CTX *md5 = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md5, EVP_md5(), nullptr);
    EVP_DigestUpdate(md5, b.data() + off, len);
    EVP_DigestFinal_ex(md5, reinterpret_cast<uint8_t *>(res.data()), nullptr);
    EVP_MD_CTX_free(md5);
#else
    MD5(reinterpret_cast<const uint8_t*>(b.data() + off), len, reinterpret_cast<uint8_t *>(res.data()));
#endif
    return res;
}

bytes calculate_sha256(bytes_view b) {
    bytes res{bytes::initialized_later(), SHA256_DIGEST_LENGTH};
    SHA256(reinterpret_cast<const uint8_t*>(b.data()), b.size(), reinterpret_cast<uint8_t *>(res.data()));
    return res;
}

bytes calculate_sha256(const bytes& b, size_t off, size_t len) {
    if (off >= b.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, b.size() - off);
    return calculate_sha256(bytes_view(b.data() + off, len));
}

bytes hmac_sha256(bytes_view msg, bytes_view key) {
    bytes res{bytes::initialized_later(), SHA256_DIGEST_LENGTH};

    unsigned length;
    HMAC(EVP_sha256(),
         key.data(), key.size(),
         reinterpret_cast<const uint8_t*>(msg.data()), msg.size(),
         reinterpret_cast<uint8_t*>(res.data()), &length);
    return res;
}

future<temporary_buffer<char>> read_text_file_fully(const sstring& filename) {
    return open_file_dma(filename, open_flags::ro).then([](file f) {
        return f.size().then([f](size_t s) {
            return do_with(make_file_input_stream(f), [s](input_stream<char>& in) {
                return in.read_exactly(s).then([](temporary_buffer<char> buf) {
                    return make_ready_future<temporary_buffer<char>>(std::move(buf));
                }).finally([&in] {
                    return in.close();
                });
            });
        });
    });
}

future<> write_text_file_fully(const sstring& filename, temporary_buffer<char> buf) {
    return open_file_dma(filename, open_flags::wo|open_flags::create).then([buf = std::move(buf)](file f) mutable {
      return make_file_output_stream(f).then([buf = std::move(buf)] (output_stream<char> out) mutable {
        return do_with(std::move(out), [buf = std::move(buf)](output_stream<char>& out) mutable {
            auto p = buf.get();
            auto s = buf.size();
            return out.write(p, s).finally([&out, buf = std::move(buf)] {
                return out.close();
            });
        });
      });
    });
}

future<> write_text_file_fully(const sstring& filename, const sstring& s) {
    return write_text_file_fully(filename, temporary_buffer<char>(s.data(), s.size()));
}

std::optional<std::chrono::milliseconds> parse_expiry(std::optional<std::string> in) {
    if (!in) {
        return std::nullopt;
    }
    size_t idx = 0;
    auto n = std::stoll(*in, &idx); // we assume seconds

    if (idx != 0) {
        auto unit = in->substr(idx);
        if (unit == "ms") {
            return std::chrono::milliseconds(n);
        } else if (unit == "h") {
            return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::hours(n));
        } else if (unit == "d") {
            return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::days(n));
        } else if (unit == "s") {
            // ok
        } else if (unit != "") {
            throw std::invalid_argument("Unsupported time unit: " + unit);
        }
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::seconds(n));
}


static const sstring namespace_prefix = "com.datastax.bdp.cassandra.crypto.";
static const sstring encryption_attribute = "scylla_encryption_options";

static inline const sstring key_id_attribute = "scylla_key_id";
static inline const sstring encrypted_components_attribute = "encrypted_components";

static inline const sstables::disk_string<uint32_t> encryption_attribute_ds{
    bytes{encryption_attribute.begin(), encryption_attribute.end()}
};
static inline const sstables::disk_string<uint32_t> key_id_attribute_ds{
    bytes{key_id_attribute.begin(), key_id_attribute.end()}
};
static inline const sstables::disk_string<uint32_t> encrypted_components_attribute_ds{
    bytes{encrypted_components_attribute.begin(), encrypted_components_attribute.end()}
};

key_info get_key_info(const options& map) {
    opt_wrapper opts(map);

    auto cipher_name = opts(CIPHER_ALGORITHM).value_or("AES/CBC/PKCS5Padding");
    auto key_strength = std::stoul(opts(SECRET_KEY_STRENGTH).value_or("128"));
    // todo: static constexpr auto KMIP_KEY_PROVIDER_FACTORY = "KmipKeyProviderFactory";
    return key_info{ std::move(cipher_name), unsigned(key_strength) };
}

std::ostream& operator<<(std::ostream& os, const key_provider& p) {
    p.print(os);
    return os;
}

sstring encryption_context::maybe_decrypt_config_value(const sstring& s) const {
    shared_ptr<symmetric_key> k = get_config_encryption_key();
    if (!s.empty() && k != nullptr) {
        auto b = base64_decode(s);
        auto iv = calculate_sha256(k->key());
        iv.resize(k->block_size(), 0);
        bytes dst(bytes::initialized_later(), b.size());
        auto len = k->decrypt(b.data(), b.size(), dst.data(), dst.size(), iv.data());
        return sstring(dst.begin(), dst.begin() + len);
    }
    return s;
}

class encryption_schema_extension;

class encryption_context_impl : public encryption_context {
    // poor mans per-thread instance variable. We need a lookup map
    // per shard, so preallocate it, much like a "sharded" thing would,
    // but without all the fancy start/stop stuff.
    // Allows this object to be effectively stateless, except for the
    // objects in the maps.
    std::vector<std::unordered_map<sstring, shared_ptr<key_provider>>> _per_thread_provider_cache;
    std::vector<std::unordered_map<sstring, shared_ptr<system_key>>> _per_thread_system_key_cache;
    std::vector<std::unordered_map<sstring, shared_ptr<kmip_host>>> _per_thread_kmip_host_cache;
    std::vector<std::unordered_map<sstring, shared_ptr<kms_host>>> _per_thread_kms_host_cache;
    std::vector<std::unordered_map<sstring, shared_ptr<gcp_host>>> _per_thread_gcp_host_cache;
    std::vector<shared_ptr<encryption_schema_extension>> _per_thread_global_user_extension;
    std::unique_ptr<encryption_config> _cfg;
    sharded<cql3::query_processor>* _qp;;
    sharded<service::migration_manager>* _mm;
    sharded<replica::database>* _db;
    sharded<service::storage_service>* _ss;
    shared_ptr<symmetric_key> _cfg_encryption_key;
    bool _allow_per_table_encryption;
public:
    encryption_context_impl(std::unique_ptr<encryption_config> cfg, const service_set& services)
        : _per_thread_provider_cache(smp::count)
        , _per_thread_system_key_cache(smp::count)
        , _per_thread_kmip_host_cache(smp::count)
        , _per_thread_kms_host_cache(smp::count)
        , _per_thread_gcp_host_cache(smp::count)
        , _per_thread_global_user_extension(smp::count)
        , _cfg(std::move(cfg))
        , _qp(find_or_null<cql3::query_processor>(services))
        , _mm(find_or_null<service::migration_manager>(services))
        , _db(find_or_null<replica::database>(services))
        , _ss(find_or_null<service::storage_service>(services))
        , _allow_per_table_encryption(_cfg->allow_per_table_encryption())
    {}

    template<typename T>
    static sharded<T>* find_or_null(const service_set& services) {
        try {
            return std::addressof(services.find<T>());
        } catch (std::out_of_range&) {
            // TODO: would be great if we could verify we are in tool mode here.
            return nullptr;
        }
    }

    shared_ptr<key_provider> get_provider(const options& map) override {
        opt_wrapper opts(map);

        auto provider_class = opts(KEY_PROVIDER);
        if (!provider_class) {
            provider_class = opts(SECRET_KEY_PROVIDER_FACTORY_CLASS).value_or(REPLICATED_KEY_PROVIDER_FACTORY);
        }
        if (provider_class->empty() || ::strcasecmp(provider_class->c_str(), "none") == 0) {
            return {};
        }
        static const std::unordered_map<sstring, std::unique_ptr<key_provider_factory>> providers = [] {
            std::unordered_map<sstring, std::unique_ptr<key_provider_factory>> map;

            map[REPLICATED_KEY_PROVIDER_FACTORY] = std::make_unique<replicated_key_provider_factory>();
            map[LOCAL_FILE_SYSTEM_KEY_PROVIDER_FACTORY] = std::make_unique<local_file_provider_factory>();
            map[KMIP_KEY_PROVIDER_FACTORY] = std::make_unique<kmip_key_provider_factory>();
            map[KMS_KEY_PROVIDER_FACTORY] = std::make_unique<kms_key_provider_factory>();
            map[GCP_KEY_PROVIDER_FACTORY] = std::make_unique<gcp_key_provider_factory>();

            return map;
        }();

        unqualified_name qn(namespace_prefix, *provider_class);

        try {
            return providers.at(qn)->get_provider(*this, map);
        } catch (std::out_of_range&) {
            throw std::invalid_argument("Unknown provider: " + *provider_class);
        }
    }
    shared_ptr<key_provider> get_cached_provider(const sstring& id) const override {
        auto& cache = _per_thread_provider_cache[this_shard_id()];
        auto i = cache.find(id);
        if (i != cache.end()) {
            return i->second;
        }
        return {};
    }
    void cache_provider(const sstring& id, shared_ptr<key_provider> p) override {
        _per_thread_provider_cache[this_shard_id()][id] = std::move(p);
    }

    shared_ptr<system_key> get_system_key(const sstring& name) override {
        auto& cache = _per_thread_system_key_cache[this_shard_id()];
        auto i = cache.find(name);
        if (i != cache.end()) {
            return i->second;
        }

        shared_ptr<encryption::system_key> k;

        if (kmip_system_key::is_kmip_path(name)) {
            k = make_shared<kmip_system_key>(*this, name);
        } else {
            k = make_shared<local_system_key>(*this, name);
        }

        if (k != nullptr) {
            cache[name] = k;
        }

        return k;
    }

    shared_ptr<kmip_host> get_kmip_host(const sstring& host) override {
        auto& cache = _per_thread_kmip_host_cache[this_shard_id()];
        auto i = cache.find(host);
        if (i != cache.end()) {
            return i->second;
        }

        auto j = _cfg->kmip_hosts().find(host);
        if (j != _cfg->kmip_hosts().end()) {
            auto result = ::make_shared<kmip_host>(*this, host, j->second);
            cache.emplace(host, result);
            return result;
        }

        throw std::invalid_argument("No such host: "+ host);
    }

    shared_ptr<kms_host> get_kms_host(const sstring& host) override {
        auto& cache = _per_thread_kms_host_cache[this_shard_id()];
        auto i = cache.find(host);
        if (i != cache.end()) {
            return i->second;
        }

        auto j = _cfg->kms_hosts().find(host);
        if (j != _cfg->kms_hosts().end()) {
            auto result = ::make_shared<kms_host>(*this, host, j->second);
            cache.emplace(host, result);
            return result;
        }

        throw std::invalid_argument("No such host: "+ host);
    }

    shared_ptr<gcp_host> get_gcp_host(const sstring& host) override {
        auto& cache = _per_thread_gcp_host_cache[this_shard_id()];
        auto i = cache.find(host);
        if (i != cache.end()) {
            return i->second;
        }

        auto j = _cfg->gcp_hosts().find(host);
        if (j != _cfg->gcp_hosts().end()) {
            auto result = ::make_shared<gcp_host>(*this, host, j->second);
            cache.emplace(host, result);
            return result;
        }

        throw std::invalid_argument("No such host: "+ host);
    }


    const encryption_config& config() const override {
        return *_cfg;
    }
    shared_ptr<symmetric_key> get_config_encryption_key() const override {
        return _cfg_encryption_key;
    }
    future<> load_config_encryption_key(const sstring & name) {
        return get_system_key(name)->get_key().then([this](auto k) {
            _cfg_encryption_key = std::move(k);
        });
    }
    /**
     * This looks like checking too late, but since these are only used by 
     * replicated provider, they will be checked very early anyway, unless
     * running tool mode, in which case they don't exist.
     */
    template<typename T>
    T& check_service_object(T* t) const {
        if (t == nullptr) {
            throw std::runtime_error(fmt::format("Service {} not registered", typeid(T).name()));
        }
        return *t;
    }
    distributed<cql3::query_processor>& get_query_processor() const override {
        return check_service_object(_qp);
    }
    distributed<service::storage_service>& get_storage_service() const override {
        return check_service_object(_ss);
    }
    distributed<replica::database>& get_database() const override {
        return check_service_object(_db);
    }
    distributed<service::migration_manager>& get_migration_manager() const override {
        return check_service_object(_mm);
    }

    future<> start() override {
        if (_qp && _ss && _db && _mm) {
            co_await replicated_key_provider_factory::on_started(get_database().local(), get_migration_manager().local());
        }
    }
    future<> stop() override {
        return smp::invoke_on_all([this]() -> future<> {
            for (auto&& [id, h] : _per_thread_kmip_host_cache[this_shard_id()]) {
                co_await h->disconnect();
            }
            _per_thread_provider_cache[this_shard_id()].clear();
            _per_thread_system_key_cache[this_shard_id()].clear();
            _per_thread_kmip_host_cache[this_shard_id()].clear();
            _per_thread_kms_host_cache[this_shard_id()].clear();
            _per_thread_gcp_host_cache[this_shard_id()].clear();
            _per_thread_global_user_extension[this_shard_id()] = {};            
        });
    }

    void add_global_user_encryption(shared_ptr<encryption_schema_extension> ext) {
        _per_thread_global_user_extension[this_shard_id()] = std::move(ext);
    }
    
    shared_ptr<encryption_schema_extension> get_global_user_encryption() const {
        return _per_thread_global_user_extension[this_shard_id()];
    }
    bool allow_per_table_encryption() const {
        return _allow_per_table_encryption;
    }
};

class encryption_schema_extension;

std::ostream& operator<<(std::ostream& os, const encryption_schema_extension& ext);

}

template <> struct fmt::formatter<encryption::encryption_schema_extension> : fmt::ostream_formatter {};

namespace encryption {

class encryption_schema_extension : public schema_extension {
    key_info _info;
    shared_ptr<key_provider> _provider;
    std::map<sstring, sstring> _options;
    std::optional<size_t> _key_block_size;

    friend std::ostream& operator<<(std::ostream&, const encryption_schema_extension&);
public:
    encryption_schema_extension(key_info, shared_ptr<key_provider>, std::map<sstring, sstring>);

    using extension_ptr = ::shared_ptr<encryption_schema_extension>;

    static extension_ptr create(encryption_context_impl&, std::map<sstring, sstring>);
    static extension_ptr create(encryption_context_impl&, const bytes&);

    static extension_ptr parse(encryption_context_impl& ctxt, db::extensions::schema_ext_config cfg) {
        struct {
            encryption_context_impl& _ctxt;

            extension_ptr operator()(const sstring&) const {
                throw std::invalid_argument("Malformed extension");
            }
            extension_ptr operator()(const std::map<sstring, sstring>& opts) const {
                return create(_ctxt, opts);
            }
            extension_ptr operator()(const bytes& v) const {
                return create(_ctxt, v);
            }
        } v{ctxt};

        auto res = std::visit(v, cfg);
        // Note: We always allow _disbling_ per-table encryption, i.e. if user encryption is active, we fall back to node-local
        if (res && !ctxt.allow_per_table_encryption() && ctxt.get_global_user_encryption()) {
            throw std::invalid_argument(fmt::format("Node global user encryption is active and per-table encryption attributes have been prohibited ({})", *res));
        }
        return res;
    }

    static options parse_options(const bytes& v) {
        return ser::deserialize_from_buffer(v, std::type_identity<options>(), 0);
    }

    future<::shared_ptr<symmetric_key>> key_for_read(opt_bytes id) const {
        return _provider->key(_info, std::move(id)).then([](std::tuple<key_ptr, opt_bytes> k_id) {
            return std::get<0>(std::move(k_id));
        });
    }
    future<std::tuple<::shared_ptr<symmetric_key>, opt_bytes>> key_for_write(opt_bytes id = {}) const {
        return _provider->key(_info, std::move(id));
    }

    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_options, 0);
    }
    future<> validate(const schema& s) const override {
        try {
            co_await _provider->validate();
            auto k = co_await key_for_write();
            logg.info("Added encryption extension to {}.{}", s.ks_name(), s.cf_name());
            logg.info("   Options: {}", _options);
            logg.info("   Key Algorithm: {}", _info);
            logg.info("   Provider: {}", *_provider);

            auto problems = std::get<0>(k)->validate_exact_info_result();
            if (!problems.empty()) {
                logg.warn("{}", problems);
            }
        } catch (...) {
            std::throw_with_nested(exceptions::configuration_exception((std::stringstream{} << "Validation failed:" << std::current_exception()).str()));
        }
    }

    bool should_delay_read(const opt_bytes& id) {
        return _provider->should_delay_read(id);
    }
    size_t key_block_size() {
        if (!_key_block_size) {
            _key_block_size = symmetric_key(_info).block_size();
        }
        return *_key_block_size;
    }
};

std::ostream& operator<<(std::ostream& os, const encryption_schema_extension& ext) {
    fmt::print(os, "{}, alg={}, provider={}", ext._options, ext._info, *ext._provider);
    return os;
}

encryption_schema_extension::encryption_schema_extension(key_info info, shared_ptr<key_provider> provider, std::map<sstring, sstring> options)
    : _info(std::move(info))
    , _provider(std::move(provider))
    , _options(std::move(options))
{}

::shared_ptr<encryption_schema_extension> encryption_schema_extension::create(encryption_context_impl& ctxt, const bytes& v) {
    auto map = parse_options(v);
    return create(ctxt, map);
}

::shared_ptr<encryption_schema_extension> encryption_schema_extension::create(encryption_context_impl& ctxt, std::map<sstring, sstring> map) {
    key_info info = get_key_info(map);
    auto provider = ctxt.get_provider(map);
    if (!provider) {
        return {};
    }
    return ::make_shared<encryption_schema_extension>(std::move(info), std::move(provider), std::move(map));
}

class encryption_file_io_extension : public sstables::file_io_extension {
    ::shared_ptr<encryption_context_impl> _ctxt;
public:
    encryption_file_io_extension(::shared_ptr<encryption_context_impl> ctxt)
        : _ctxt(std::move(ctxt))
    {}

    attr_value_map get_attributes(const sstables::sstable& sst) const override {
        auto& sc = sst.get_shared_components();
        if (!sc.scylla_metadata) {
            return {};
        }
        auto* exta  = sc.scylla_metadata->get_extension_attributes();
        if (!exta) {
            return {};
        }

        auto i = exta->map.find(encryption_attribute_ds);
        if (i == exta->map.end()) {
            return {};
        }
        auto opts = encryption_schema_extension::parse_options(i->second.value);

        if (exta->map.count(key_id_attribute_ds)) {
            auto id = exta->map.at(key_id_attribute_ds).value;
            auto id_str = id.size() == utils::UUID::serialized_size()
                ? sstring(fmt::format("{}", utils::UUID_gen::get_UUID(id)))
                : to_hex(id)
                ;
            opts["key_id"] = std::move(id_str);
        }

        if (exta->map.count(encrypted_components_attribute_ds)) {
            std::vector<sstables::component_type> ccs;
            ccs.reserve(9);
            auto mask = ser::deserialize_from_buffer(exta->map.at(encrypted_components_attribute_ds).value, std::type_identity<uint32_t>{}, 0);
            for (auto c : { sstables::component_type::Index,
                            sstables::component_type::CompressionInfo,
                            sstables::component_type::Data,
                            sstables::component_type::Summary,
                            sstables::component_type::Digest,
                            sstables::component_type::CRC,
                            sstables::component_type::Filter,
                            sstables::component_type::Statistics,
                            sstables::component_type::TemporaryStatistics,
            }) {
                if (mask & int(c)) {
                    ccs.emplace_back(c);
                }
            }
            opts["components"] = fmt::to_string(fmt::join(ccs, ", "));
        } else {
            opts["components"] = "Data";
        }
        attr_value_map res;
        res["encryption_info"] = std::move(opts);
        return res;
    }

    future<file> wrap_file(sstables::sstable& sst, sstables::component_type type, file f, open_flags flags) override {
        switch (type) {
        case sstables::component_type::Scylla:
        case sstables::component_type::TemporaryTOC:
        case sstables::component_type::TOC:
            co_return file{};
        default:
            break;
        }

        if (flags == open_flags::ro) {
            // open existing. check read opts.
            auto& sc = sst.get_shared_components();
            if (sc.scylla_metadata) {
                auto* exta  = sc.scylla_metadata->get_extension_attributes();
                if (exta) {
                    auto i = exta->map.find(encryption_attribute_ds);
                    // note: earlier builds of encryption extension would only encrypt data component,
                    // so iff we are opening old sstables we need to check if this component is actually
                    // encrypted. We use a bitmask attribute for this.

                    bool ok = i != exta->map.end();
                    if (ok && type != sstables::component_type::Data) {
                        ok = exta->map.count(encrypted_components_attribute_ds) &&
                                        (ser::deserialize_from_buffer(exta->map.at(encrypted_components_attribute_ds).value, std::type_identity<uint32_t>{}, 0) & (1 << int(type)));
                    }

                    if (ok) {
                        auto esx = encryption_schema_extension::create(*_ctxt, i->second.value);
                        opt_bytes id;

                        if (exta->map.count(key_id_attribute_ds)) {
                            id = exta->map.at(key_id_attribute_ds).value;
                        }

                        if (esx->should_delay_read(id)) {
                            logg.debug("Encrypted sstable component {} using delayed opening {} (id: {})", sst.component_basename(type), *esx, id);

                            co_return make_delayed_encrypted_file(f, esx->key_block_size(), [esx, comp = sst.component_basename(type), id = std::move(id)] {
                                logg.trace("Delayed component {} using {} (id: {}) resolve", comp, *esx, id);
                                return esx->key_for_read(id);
                            });
                        }

                        logg.debug("Open encrypted sstable component {} using {} (id: {})", sst.component_basename(type), *esx, id);

                        auto k = co_await esx->key_for_read(std::move(id));
                        co_return make_encrypted_file(f, std::move(k));
                    }
                }
            }
        } else {
            auto s = sst.get_schema();
            shared_ptr<encryption_schema_extension> esx;
            auto e = s->extensions().find(encryption_attribute);
            // #4844 - don't allow schema encryption to be used for writing
            // iff it is disallowed by config -> placeholder here
            // (See schema_tables.cc::prepare_builder_from_table_row - if an extension
            //  is unavailable/non-creatable at load time a dummy object is inserted
            // )
            if (e != s->extensions().end() && !e->second->is_placeholder()) {
                esx = static_pointer_cast<encryption_schema_extension>(e->second);
            } else if (!is_system_keyspace(s->ks_name())) {
                esx = _ctxt->get_global_user_encryption();
            }
            if (esx) {
                auto& sc = sst.get_shared_components();
                if (!sc.scylla_metadata) {
                    sc.scylla_metadata.emplace();
                }
                auto& ext = sc.scylla_metadata->get_or_create_extension_attributes();
                opt_bytes id;

                // We are writing more than one component. If we used a named key before
                // we need to make sure we use the exact same one for all components,
                // even if something like KMIP key invalidation replaced it.
                // This will also speed up key lookup in some cases, as both repl
                // and kmip cache id bound keys.
                if (ext.map.count(key_id_attribute_ds)) {
                    id = ext.map.at(key_id_attribute_ds).value;
                }

                logg.debug("Write encrypted sstable component {} using {} (id: {})", sst.component_basename(type), *esx, id);

                /**
                 * #3954 We can be (and are) called with two components simultaneously (hello index, data).
                 * If this case we could block on the below "key" call and iff provider has certain cache behaviour (hello replicated)
                 * or caches expire, we could end up with different keys for respective components, leading to one
                 * of the components ending up unreadable.
                */
                for (;;) {
                    auto [k, k_id] = co_await esx->key_for_write(std::move(id));

                    if (k_id && ext.map.count(key_id_attribute_ds)) {
                        id = ext.map.at(key_id_attribute_ds).value;
                        if (k_id != id) {
                            continue;
                        }
                    }

                    id = std::move(k_id);

                    if (!ext.map.count(encryption_attribute_ds)) {
                        ext.map.emplace(encryption_attribute_ds, sstables::disk_string<uint32_t>{esx->serialize()});
                    }
                    if (id) {
                        ext.map.emplace(key_id_attribute_ds, sstables::disk_string<uint32_t>{*id});
                    }
                    if (type != sstables::component_type::Data) {
                        uint32_t mask = 0;
                        if (ext.map.count(encrypted_components_attribute_ds)) {
                            mask = ser::deserialize_from_buffer(ext.map.at(encrypted_components_attribute_ds).value, std::type_identity<uint32_t>{}, 0);
                        }
                        mask |= (1 << int(type));
                        // just a marker. see above
                        ext.map[encrypted_components_attribute_ds] = sstables::disk_string<uint32_t>{ser::serialize_to_buffer<bytes>(mask, 0)};
                    }
                    co_return make_encrypted_file(f, std::move(k));
                }
            }
        }

        co_return file{};
    }
};

std::string encryption_provider(const sstables::sstable& sst) {
    auto& sc = sst.get_shared_components();
    if (!sc.scylla_metadata) {
        return {};
    }
    auto* exta  = sc.scylla_metadata->get_extension_attributes();
    if (!exta) {
        return {};
    }

    auto i = exta->map.find(encryption_attribute_ds);
    if (i == exta->map.end()) {
        return {};
    }
    auto options = encryption_schema_extension::parse_options(i->second.value);
    opt_wrapper opts(options);

    return opts(KEY_PROVIDER).value_or(std::string{});
}

namespace bfs = std::filesystem;

class encryption_commitlog_file_extension : public db::commitlog_file_extension {
    const ::shared_ptr<encryption_context> _ctxt;
    const options _opts;

    static const inline std::regex prop_expr = std::regex("^([^=]+)=(\\S+)$");
    static const inline sstring id_key = "key_id";
    static const inline std::string end_of_file_mark = "#-- end of file";

public:
    encryption_commitlog_file_extension(::shared_ptr<encryption_context> ctxt, options opts)
        : _ctxt(ctxt)
        , _opts(std::move(opts))
    {}
    sstring config_name(const sstring& filename) const {
        bfs::path p(filename);
        auto dir = p.parent_path();
        auto file = p.filename();
        return (dir / bfs::path("." + file.string())).string();
    }
    future<file> wrap_file(const sstring& filename, file f, open_flags flags) override {
        auto cfg_file = config_name(filename);

        if (flags == open_flags::ro) {
            return file_exists(cfg_file).then([=, this](bool exists) {
                if (!exists) {
                    // #1681 if file system errors caused the options file to simply not exist,
                    // we can at least hope that the file itself is not very encrypted either. 
                    // But who knows. Will probably cause data corruption.
                    logg.info("Commitlog segment {} has no encryption info. Opening unencrypted.", filename);
                    return make_ready_future<file>(std::move(f));
                }
                return read_text_file_fully(cfg_file).then([f, this, filename](temporary_buffer<char> buf) {
                    std::istringstream ss(std::string(buf.begin(), buf.end()));
                    options opts;
                    std::string line;
                    bool has_eof = false;
                    while (std::getline(ss, line)) {
                        std::smatch m;
                        if (std::regex_match(line, m, prop_expr)) {
                            auto k = m[1].str();
                            auto v = m[2].str();
                            opts[k] = v;
                        } else if (line == end_of_file_mark) {
                            has_eof = true;
                        }
                    }

                    // #1682 - if we crashed while writing the options file,
                    // it is quite possible that we are eventually trying to
                    // open + replay an (empty) CL file, but cannot read the
                    // properties now, since _our_ metadata is empty/truncated
                    if (!has_eof) {
                        // just return the unwrapped file.
                        logg.info("Commitlog segment {} has incomplete encryption info. Opening unencrypted.", filename);
                        return make_ready_future<file>(std::move(f));
                    }
                    opt_bytes id;
                    if (opts.count(id_key)) {
                        id = base64_decode(opts[id_key]);
                    }

                    auto provider = _ctxt->get_provider(opts);

                    logg.debug("Open commitlog segment {} using {} (id: {})", filename, *provider, id);
                    auto info = make_shared<key_info>(get_key_info(opts));
                    return provider->key(*info, id).then([f, info](std::tuple<shared_ptr<symmetric_key>, opt_bytes> k) {
                        return make_ready_future<file>(make_encrypted_file(f, std::get<0>(k)));
                    });
                });
            });
        } else {
            auto provider = _ctxt->get_provider(_opts);
            auto info = make_shared<key_info>(get_key_info(_opts));
            return provider->key(*info).then([f, this, info, cfg_file, filename, &provider = *provider](std::tuple<shared_ptr<symmetric_key>, opt_bytes> k_id) {
                auto&& k = std::get<0>(k_id);
                auto&& id = std::get<1>(k_id);
                std::ostringstream ss;
                for (auto&p : _opts) {
                    ss << p.first << "=" << p.second << std::endl;
                }
                if (id) {
                    ss << id_key << "=" << base64_encode(*id) << std::endl;
                }
                ss << end_of_file_mark << std::endl;

                logg.debug("Creating commitlog segment {} using {} (id: {})", filename, provider, id);

                return write_text_file_fully(cfg_file, ss.str()).then([f, k] {
                    return make_ready_future<file>(make_encrypted_file(f, k));
                });
            });
        }
    }
    future<> before_delete(const sstring& filename) override {
        auto cfg_file = config_name(filename);
        return file_exists(cfg_file).then([cfg_file](bool b) {
            return b ? remove_file(cfg_file) : make_ready_future();
        });
    }
};

future<seastar::shared_ptr<encryption_context>> register_extensions(const db::config&, std::unique_ptr<encryption_config> cfg_in, db::extensions& exts, const ::service_set& services) {
    auto& cfg = *cfg_in;
    auto ctxt = ::make_shared<encryption_context_impl>(std::move(cfg_in), services);
    // Note: extensions are immutable and shared across shards.
    // Object in them must be stateless. We anchor the context in the
    // extension objects, and while it is not as such 100% stateless,
    // it is close enough.
    exts.add_schema_extension(encryption_attribute, [ctxt](auto v) {
        return encryption_schema_extension::parse(*ctxt, std::move(v));
    });
    exts.add_sstable_file_io_extension(encryption_attribute, std::make_unique<encryption_file_io_extension>(ctxt));
    
    auto maybe_get_options = [&](const utils::config_file::string_map& map, const sstring& what) -> std::optional<options> {
        options opts(map.begin(), map.end());
        opt_wrapper get_opt(opts);
        if (!::strcasecmp(get_opt("enabled").value_or("false").c_str(), "false")) {
            return std::nullopt;
        }
        // commitlog/system table encryption/global user encryption should not use replicated keys,
        // We default to local keys, but KMIP/KMS is ok as well (better in fact).
        opts[KEY_PROVIDER] = get_opt(KEY_PROVIDER).value_or(LOCAL_FILE_SYSTEM_KEY_PROVIDER_FACTORY);
        if (opts[KEY_PROVIDER] == LOCAL_FILE_SYSTEM_KEY_PROVIDER_FACTORY && !get_opt(SECRET_KEY_FILE)) {
            // system encryption uses different key folder than user tables.
            // explicitly set the key file path
            opts[SECRET_KEY_FILE] = (bfs::path(cfg.system_key_directory()) / bfs::path("system") / bfs::path(get_opt("key_name").value_or("system_table_keytab"))).string();
        }
        // forbid replicated. we cannot guarantee being able to open sstables on populate
        if (opts[KEY_PROVIDER] == REPLICATED_KEY_PROVIDER_FACTORY) {
            throw std::invalid_argument("Replicated provider is not allowed for " + what);
        }
        return opts;
    };

    future<> f = make_ready_future<>();

    auto opts = maybe_get_options(cfg.system_info_encryption(), "system table encryption");
    
    if (opts) {
        logg.info("Adding system info encryption using {}", *opts);

        exts.add_commitlog_file_extension(encryption_attribute, std::make_unique<encryption_commitlog_file_extension>(ctxt, *opts));

        // modify schemas for tables holding sensitive data to use encryption w. key described
        // by the opts.
        // since schemas are duplicated across shards, we must call to each shard and augment
        // them all.
        // Since we are in pre-init phase, this should be safe.
        f = f.then([opts = *opts, &exts] {
            return smp::invoke_on_all([opts = make_lw_shared<options>(opts), &exts] () mutable {
                auto& f = exts.schema_extensions().at(encryption_attribute);
                for (auto& s : { db::system_keyspace::paxos(), db::system_keyspace::batchlog(), db::system_keyspace::dicts() }) {
                    exts.add_extension_to_schema(s, encryption_attribute, f(*opts));
                }
            });
        });
    }

    if (cfg.config_encryption_active()) {
        f = f.then([&cfg, ctxt] {
           return ctxt->load_config_encryption_key(cfg.config_encryption_key_name());
        });
    }


    if (!cfg.kmip_hosts().empty()) {
        // only pre-create on shard 0.
        f = f.then([&cfg, ctxt] {
            return parallel_for_each(cfg.kmip_hosts(), [ctxt](auto& p) {
                auto host = ctxt->get_kmip_host(p.first);
                return host->connect();
            });
        });
    }

    if (!cfg.kms_hosts().empty()) {
        // only pre-create on shard 0.
        f = f.then([&cfg, ctxt] {
            return parallel_for_each(cfg.kms_hosts(), [ctxt](auto& p) {
                auto host = ctxt->get_kms_host(p.first);
                return host->init();
            });
        });
    }

    if (!cfg.gcp_hosts().empty()) {
        // only pre-create on shard 0.
        f = f.then([&cfg, ctxt] {
            return parallel_for_each(cfg.gcp_hosts(), [ctxt](auto& p) {
                auto host = ctxt->get_gcp_host(p.first);
                return host->init();
            });
        });
    }

    replicated_key_provider_factory::init(exts);

    auto user_opts = maybe_get_options(cfg.user_info_encryption(), "user table encryption");
    
    if (user_opts) {
        logg.info("Adding user info encryption using {}", *user_opts);

        f = f.then([user_opts = *user_opts, ctxt] {
            return smp::invoke_on_all([user_opts = make_lw_shared<options>(user_opts), ctxt]()  {
                auto ext = encryption_schema_extension::create(*ctxt, *user_opts);
                ctxt->add_global_user_encryption(std::move(ext));
            });
        });
    }

    return f.then([ctxt]() -> ::shared_ptr<encryption_context> {
        return ctxt;
    });
}

}

