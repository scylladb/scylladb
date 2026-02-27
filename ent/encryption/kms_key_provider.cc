/*
 * Copyright (C) 2022 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/lexical_cast.hpp>
#include <regex>

#include "kms_key_provider.hh"
#include "kms_host.hh"

namespace encryption {

class kms_key_provider : public key_provider {
public:
    kms_key_provider(::shared_ptr<kms_host> kms_host, std::string name, kms_host::option_override oov)
        : _kms_host(std::move(kms_host))
        , _name(std::move(name))
        , _oov(std::move(oov))
    {}
    future<std::tuple<key_ptr, opt_bytes>> key(const key_info& info, opt_bytes id) override {
        if (id) {
            return _kms_host->get_key_by_id(*id, info, &_oov).then([id](key_ptr k) {
                return make_ready_future<std::tuple<key_ptr, opt_bytes>>(std::tuple(k, id));
            });
        }
        return _kms_host->get_or_create_key(info, &_oov).then([](std::tuple<key_ptr, opt_bytes> k_id) {
            return make_ready_future<std::tuple<key_ptr, opt_bytes>>(k_id);
        });
    }
    void print(std::ostream& os) const override {
        os << _name;
    }
private:
    ::shared_ptr<kms_host> _kms_host;
    std::string _name;
    kms_host::option_override _oov;
};

shared_ptr<key_provider> kms_key_provider_factory::get_provider(encryption_context& ctxt, const options& map) {
    opt_wrapper opts(map);
    auto kms_host = opts("kms_host");
    kms_host::option_override oov {
        .master_key = opts("master_key"),
        .aws_assume_role_arn = opts("aws_assume_role_arn"),
    };

    if (!kms_host) {
        throw std::invalid_argument("kms_host must be provided");
    }

    auto host = ctxt.get_kms_host(*kms_host);
    auto id = kms_host.value() 
        + ":" + oov.master_key.value_or(host->options().master_key)
        + ":" + oov.aws_assume_role_arn.value_or(host->options().aws_assume_role_arn)
        ;
    auto provider = ctxt.get_cached_provider(id);

    if (!provider) {
        provider = ::make_shared<kms_key_provider>(host, *kms_host, std::move(oov));
        ctxt.cache_provider(id, provider);
    }

    return provider;
}

}
