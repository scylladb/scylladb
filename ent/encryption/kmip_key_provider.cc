/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/lexical_cast.hpp>
#include <regex>

#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"

#include "kmip_key_provider.hh"
#include "kmip_host.hh"

namespace encryption {

class kmip_key_provider : public key_provider {
public:
    kmip_key_provider(::shared_ptr<kmip_host> kmip_host, kmip_host::key_options kopts, sstring name)
        : _kmip_host(std::move(kmip_host))
        , _kopts(std::move(kopts))
        , _name(std::move(name))
    {}
    future<std::tuple<key_ptr, opt_bytes>> key(const key_info& info, opt_bytes id) override {
        if (id) {
            return _kmip_host->get_key_by_id(*id, info).then([id](key_ptr k) {
                return make_ready_future<std::tuple<key_ptr, opt_bytes>>(std::tuple(k, id));
            });
        }
        return _kmip_host->get_or_create_key(info, _kopts).then([](std::tuple<key_ptr, opt_bytes> k_id) {
            return make_ready_future<std::tuple<key_ptr, opt_bytes>>(k_id);
        });
    }
    void print(std::ostream& os) const override {
        os << _name;
        if (!_kopts.key_namespace.empty()) {
            os << ", namespace=" << _kopts.key_namespace;
        }
        if (!_kopts.template_name.empty()) {
            os << ", template=" << _kopts.template_name;
        }
    }

private:
    ::shared_ptr<kmip_host> _kmip_host;
    kmip_host::key_options _kopts;
    sstring _name;
};


shared_ptr<key_provider> kmip_key_provider_factory::get_provider(encryption_context& ctxt, const options& map) {
    opt_wrapper opts(map);
    auto host = opts(HOST_NAME);
    if (!host) {
        throw std::invalid_argument("kmip_host must be provided");
    }
    kmip_host::key_options kopts = {
                    opts(TEMPLATE_NAME).value_or(""),
                    opts(KEY_NAMESPACE).value_or(""),
    };

    auto cache_key = *host + ":" + boost::lexical_cast<std::string>(kopts);
    auto provider = ctxt.get_cached_provider(cache_key);

    if (!provider) {
        provider = ::make_shared<kmip_key_provider>(ctxt.get_kmip_host(*host), std::move(kopts), *host);
        ctxt.cache_provider(cache_key, provider);
    }

    return provider;
}

static std::optional<std::pair<sstring, sstring>> parse_kmip_host_and_path(const sstring & s) {
    static const std::regex kmip_ex("kmip://([^/]+)/([\\w/]+)");

    std::match_results<typename sstring::const_iterator> m;
    if (std::regex_match(s.begin(), s.end(), m, kmip_ex)) {
        return std::make_pair(sstring(m[1]), sstring(m[2]));
    }
    return std::nullopt;
}

kmip_system_key::kmip_system_key(encryption_context& ctxt, const sstring& s) {
    auto p = parse_kmip_host_and_path(s);
    if (!p) {
        throw std::invalid_argument("Not a kmip path: " + s);
    }

    _host = ctxt.get_kmip_host(p->first);
    _name = p->second;
}

kmip_system_key::~kmip_system_key() = default;

bool kmip_system_key::is_kmip_path(const sstring& s) {
    return parse_kmip_host_and_path(s) != std::nullopt;
}

future<shared_ptr<symmetric_key>> kmip_system_key::get_key() {
    if (_key) {
        return make_ready_future<shared_ptr<symmetric_key>>(_key);
    }
    return _host->get_key_by_name(_name).then([this](shared_ptr<symmetric_key> k) {
        _key = k;
        return k;
    });
}

const sstring& kmip_system_key::name() const {
    return _name;
}


}

