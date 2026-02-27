/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "azure_key_provider.hh"
#include "azure_host.hh"

namespace encryption {

class azure_key_provider : public key_provider {
public:
    azure_key_provider(::shared_ptr<azure_host> azure_host, std::string name, azure_host::option_override oov)
        : _azure_host(std::move(azure_host))
        , _name(std::move(name))
        , _oov(std::move(oov))
    {}
    future<std::tuple<key_ptr, opt_bytes>> key(const key_info& info, opt_bytes id) override {
        if (id) {
            return _azure_host->get_key_by_id(*id, info).then([id](key_ptr k) {
                return make_ready_future<std::tuple<key_ptr, opt_bytes>>(std::tuple(k, id));
            });
        }
        return _azure_host->get_or_create_key(info, &_oov).then([](std::tuple<key_ptr, opt_bytes> k_id) {
            return make_ready_future<std::tuple<key_ptr, opt_bytes>>(k_id);
        });
    }
    void print(std::ostream& os) const override {
        os << _name;
    }
private:
    ::shared_ptr<azure_host> _azure_host;
    std::string _name;
    azure_host::option_override _oov;
};

shared_ptr<key_provider> azure_key_provider_factory::get_provider(encryption_context& ctxt, const options& map) {
    opt_wrapper opts(map);
    auto azure_host = opts("azure_host");


    if (!azure_host) {
        throw std::invalid_argument("azure_host must be provided");
    }

    azure_host::option_override oov {
        .master_key =  opts("master_key"),
    };

    auto host = ctxt.get_azure_host(*azure_host);
    auto id = azure_host.value() + ":" + oov.master_key.value_or(host->options().master_key);

    auto provider = ctxt.get_cached_provider(id);

    if (!provider) {
        provider = ::make_shared<azure_key_provider>(host, *azure_host, std::move(oov));
        ctxt.cache_provider(id, provider);
    }

    return provider;
}

}
