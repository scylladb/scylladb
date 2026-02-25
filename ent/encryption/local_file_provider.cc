/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include <unordered_map>
#include <stdexcept>
#include <regex>

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <seastar/core/semaphore.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

#include "local_file_provider.hh"
#include "symmetric_key.hh"
#include "encryption.hh"
#include "encryption_exceptions.hh"
#include "encryption_config.hh"
#include "db/config.hh"

namespace encryption {

namespace bfs = std::filesystem;

const sstring default_key_file_path = (bfs::path(db::config::get_conf_dir()) / "data_encryption_keys").string();

static const key_info system_key_info{ "System", 0 };

class local_file_provider : public key_provider {
public:
    local_file_provider(encryption_context& ctxt, const bfs::path& path, bool must_exist = false)
        : local_file_provider(ctxt, sstring(bfs::absolute(path).string()), must_exist)
    {}
    local_file_provider(encryption_context& ctxt, const sstring& path, bool must_exist = false)
        : _ctxt(ctxt)
        , _path(path)
        , _sem(1)
        , _must_exist(must_exist)
    {}
    future<std::tuple<key_ptr, opt_bytes>> key(const key_info& info, opt_bytes = {}) override {
        // TODO: assert options -> my key
        auto i = _keys.find(info);
        if (i != _keys.end()) {
            return make_ready_future<std::tuple<key_ptr, opt_bytes>>(std::tuple(i->second, std::nullopt));
        }
        return load_or_create(info).then([](key_ptr k) {
            return make_ready_future<std::tuple<key_ptr, opt_bytes>>(std::tuple(k, std::nullopt));
        });
    }
    future<> validate() const override {
        auto f = make_ready_future<>();
        if (!_must_exist) {
            return f;
        }
        // if we must exist, we don't change. Ok to open from all shards.
        return f.then([this] {
            return open_file_dma(_path, open_flags::ro).then([](file f) {
                return f.close();
            });
        }).handle_exception([this](auto ep) {
            try {
                std::rethrow_exception(ep);
            } catch (...) {
                std::throw_with_nested(missing_resource_error("Could not read '" + _path + "'"));
            }
        });
    }

    const sstring& path() const {
        return _path;
    }
    void print(std::ostream& os) const override {
        os << "key=" << _path;
    }

private:
    future<key_ptr> load_or_create(const key_info&);
    future<key_ptr> load_or_create_local(const key_info&);
    future<> read_key_file();
    future<key_ptr> write_key_file(key_info);

    std::unordered_map<key_info, key_ptr, key_info_hash> _keys;
    encryption_context& _ctxt;
    sstring _path;
    semaphore _sem;
    bool _read_file = false;
    bool _must_exist = false;
};

shared_ptr<key_provider> local_file_provider_factory::find(encryption_context& ctxt, const sstring& path) {
    auto p = ctxt.get_cached_provider(path);
    if (!p) {
        p = make_shared<local_file_provider>(ctxt, path);
        ctxt.cache_provider(path, p);
    }
    return p;
}

shared_ptr<key_provider> local_file_provider_factory::get_provider(encryption_context& ctxt, const options& map) {
    opt_wrapper opts(map);
    return find(ctxt, opts(SECRET_KEY_FILE).value_or(default_key_file_path));
}

future<key_ptr>
local_file_provider::load_or_create(const key_info& info) {
    // if someone uses a system key as a table key, we could still race
    // here. but that is a user error, so ignore
    if (this_shard_id() == 0 || &info == &system_key_info) {
        return load_or_create_local(info);
    }

    struct data {
        bytes key;
        key_info info;
    };

    /**
     * Key files are singular. Not sharded. This would be ok if we only read from them.
     * But in keeping with dse compat, we don't. So rather than dealing with lock files
     * or whatnot, we simply say that a single file is handled by a single key object,
     * and only on shard 0. So if we are not shard 0, we call to there, find our
     * counterpart object (local_file_provider_factory::find), and as him about the
     * key data instead. He in turn will sync on his semaphore.
     *
     * The downside is that we are not resilient against multiple processes messing
     * with the key file, but neither is dse
     */
    return do_with(data{bytes(bytes::initialized_later(), info.len/8), info}, [this](data& i) {
        return smp::submit_to(0, [this, &i]{
            auto kp = static_pointer_cast<local_file_provider>(local_file_provider_factory::find(_ctxt, _path));
            auto f = kp->load_or_create_local(i.info);
            return f.then([&i, kp](key_ptr k) {
                auto& kd = k->key();
                i.key.resize(kd.size());
                std::copy(kd.begin(), kd.end(), i.key.begin());
            });
        }).then([this, &i] {
            auto k = make_shared<symmetric_key>(i.info, i.key);
            _keys.emplace(i.info, k);
            return make_ready_future<key_ptr>(std::move(k));
        });
    });
}

future<key_ptr>
local_file_provider::load_or_create_local(const key_info& info) {
    if (_keys.count(info)) {
        return make_ready_future<key_ptr>(_keys.at(info));
    }
    return read_key_file().then([this, info] {
        if (_keys.count(info)) {
            return make_ready_future<key_ptr>(_keys.at(info));
        }
        if (info == system_key_info) {
            if (_keys.size() != 1) {
                _keys.clear();
                return make_exception_future<key_ptr>(std::invalid_argument("System key must contain exactly one entry"));
            }
            auto k = _keys.begin()->second;
            _keys.clear();
            _keys.emplace(info, k);
            return make_ready_future<key_ptr>(k);
        }
        // create it.
        return write_key_file(info);
    });
}

future<> local_file_provider::read_key_file() {
    if (_read_file) {
        return make_ready_future();
    }

    // #1923 - a key can have a descriptor string line "AES:128:<data>" iff user relies on
    // defaults. Must match this as well.
    static const std::regex key_line_expr(R"foo((\w+(?:\/\w+)?(?:\/\w+)?)\:(\d+)\:(\S+)\s*)foo");

    return with_semaphore(_sem, 1, [this] {
        // could do this twice, but it is only reading
        return read_text_file_fully(_path).then([this](temporary_buffer<char> buf) {
            auto i = std::cregex_iterator(buf.begin(), buf.end(), key_line_expr);
            auto e = std::cregex_iterator();

            while (i != e) {
                std::cmatch m = *i;
                auto alg = m[1].str();
                auto len = std::stoul(m[2].str());
                auto key = m[3].str();

                auto info = key_info{alg, unsigned(len)};
                if (!_keys.count(info)) {
                    auto kb = base64_decode(key);
                    auto k = make_shared<symmetric_key>(info, kb);
                    _keys.emplace(info, std::move(k));
                }
                ++i;
            }
            _read_file = true;
        }).handle_exception([this](auto ep) {
            try {
                std::rethrow_exception(ep);
            } catch (std::system_error& e) {
                if (e.code() == std::error_code(ENOENT, std::system_category())) {
                    if (!_must_exist) {
                        return;
                    }
                    std::throw_with_nested(configuration_error("Key file '" + _path + "' does not exist"));
                }
                std::throw_with_nested(service_error("read_key_file"));
            } catch (std::invalid_argument& e) {
                std::throw_with_nested(configuration_error(fmt::format("read_key_file: {}", e.what())));
            } catch (...) {
                std::throw_with_nested(service_error(fmt::format("read_key_file: {}", std::current_exception())));
            }
        });
    });
}

future<key_ptr> local_file_provider::write_key_file(key_info info) {
    return with_semaphore(_sem, 1, [this, info] {
        // we can get here more than once if shards race.
        // however, we only need to use/write the first key matching
        // the required info.
        if (_keys.count(info)) {
            return make_ready_future<key_ptr>(_keys.at(info));
        }

        auto k = make_shared<symmetric_key>(info);

        std::ostringstream ss;
        for (auto& p : _keys) {
            ss << p.first.alg << ":" << p.first.len << ":" << base64_encode(p.second->key()) << std::endl;
        }
        ss << info.alg << ":" << info.len << ":" << base64_encode(k->key()) << std::endl;
        auto s = ss.str();
        auto tmpnam = _path + ".tmp";
        auto f = make_ready_future<>();
        if (!_must_exist) {
            f = seastar::recursive_touch_directory((bfs::path(tmpnam).remove_filename()).string());
        }
        return f.then([this, tmpnam, s] {
            return write_text_file_fully(tmpnam, s).then([this, tmpnam] {
                return rename_file(tmpnam, _path);
            });
        }).then([this, k, info] {
            // don't cache until written
            _keys[info] = k;
            return make_ready_future<key_ptr>(k);
        });
    }).handle_exception([this](auto ep) -> key_ptr{
        try {
            std::rethrow_exception(ep);
        } catch (...) {
            std::throw_with_nested(service_error("Could not write key file '" + _path + "'"));
        }
    });
}

local_system_key::local_system_key(encryption_context& ctxt, const sstring& path)
    : _provider(make_shared<local_file_provider>(ctxt, bfs::path(ctxt.config().system_key_directory()) / bfs::path(path), true))
{}

local_system_key::~local_system_key()
{}

future<shared_ptr<symmetric_key>> local_system_key::get_key() {
    return _provider->key(system_key_info).then([](std::tuple<key_ptr, opt_bytes> k_id) {
       return make_ready_future<shared_ptr<symmetric_key>>(std::get<0>(std::move(k_id)));
    });
}

future<> local_system_key::validate() const {
    // first, just validate the file provider itself
    co_await _provider->validate();
    // second, do an early load of the actual key to ensure file contents.
    co_await _provider->key(system_key_info);
}

const sstring& local_system_key::name() const {
    return _provider->path();
}

}
