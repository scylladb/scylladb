/*
 * Copyright (C) 2024 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/lexical_cast.hpp>
#include <regex>

#include "gcp_key_provider.hh"
#include "gcp_host.hh"

namespace encryption {

class gcp_key_provider : public key_provider {
public:
    gcp_key_provider(::shared_ptr<gcp_host> gcp_host, std::string name, gcp_host::option_override oov)
        : _gcp_host(std::move(gcp_host))
        , _name(std::move(name))
        , _oov(std::move(oov))
    {}
    future<std::tuple<key_ptr, opt_bytes>> key(const key_info& info, opt_bytes id) override {
        if (id) {
            return _gcp_host->get_key_by_id(*id, info, &_oov).then([id](key_ptr k) {
                return make_ready_future<std::tuple<key_ptr, opt_bytes>>(std::tuple(k, id));
            });
        }
        return _gcp_host->get_or_create_key(info, &_oov).then([](std::tuple<key_ptr, opt_bytes> k_id) {
            return make_ready_future<std::tuple<key_ptr, opt_bytes>>(k_id);
        });
    }
    void print(std::ostream& os) const override {
        os << _name;
    }
private:
    ::shared_ptr<gcp_host> _gcp_host;
    std::string _name;
    gcp_host::option_override _oov;
};

shared_ptr<key_provider> gcp_key_provider_factory::get_provider(encryption_context& ctxt, const options& map) {
    opt_wrapper opts(map);
    auto gcp_host = opts("gcp_host");


    gcp_host::option_override oov {
        .master_key =  opts("master_key"),
    };

    oov.gcp_credentials_file = opts("gcp_credentials_file");
    oov.gcp_impersonate_service_account = opts("gcp_impersonate_service_account");

    if (!gcp_host) {
        throw std::invalid_argument("gcp_host must be provided");
    }

    auto host = ctxt.get_gcp_host(*gcp_host);
    auto id = gcp_host.value()
        + ":" + oov.master_key.value_or(host->options().master_key)
        + ":" + oov.gcp_credentials_file.value_or(host->options().gcp_credentials_file)
        + ":" + oov.gcp_impersonate_service_account.value_or(host->options().gcp_impersonate_service_account)
        ;

    auto provider = ctxt.get_cached_provider(id);

    if (!provider) {
        provider = ::make_shared<gcp_key_provider>(host, *gcp_host, std::move(oov));
        ctxt.cache_provider(id, provider);
    }

    return provider;
}

}
