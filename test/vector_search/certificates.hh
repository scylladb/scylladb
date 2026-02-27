/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/core/seastar.hh>
#include <seastar/net/tls.hh>
#include <fmt/format.h>

namespace test::vector_search {

class certificates {
public:
    seastar::sstring ca_cert_file() const {
        return prepend_dir("cacert.pem");
    }

    seastar::sstring server_cert_file() const {
        return prepend_dir("scylla.pem");
    }

    seastar::sstring server_key_file() const {
        return prepend_dir("scylla.pem");
    }

    seastar::sstring server_cert_cn() const {
        return "scylla";
    }

private:
    seastar::sstring prepend_dir(const char* filename) const {
        return fmt::format("./test/resource/certs/{}", filename);
    }
};

inline seastar::future<seastar::shared_ptr<seastar::tls::server_credentials>> make_server_credentials(certificates& certs) {
    auto creds = seastar::make_shared<seastar::tls::server_credentials>();
    co_await creds->set_x509_key_file(certs.server_cert_file(), certs.server_key_file(), seastar::tls::x509_crt_format::PEM);
    co_return creds;
}

} // namespace test::vector_search
