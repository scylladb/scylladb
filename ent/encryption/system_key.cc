/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include <stdexcept>
#include <regex>
#include <algorithm>
#include <unordered_map>

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <seastar/core/align.hh>

#include "symmetric_key.hh"
#include "system_key.hh"

future<> encryption::system_key::validate() const {
    return make_ready_future<>();
}

future<sstring> encryption::system_key::decrypt(const sstring& s) {
    auto b = base64_decode(s);
    return decrypt(b).then([](bytes b) {
        return make_ready_future<sstring>(sstring(b.begin(), b.end()));
    });
}

future<sstring> encryption::system_key::encrypt(const sstring& s) {
    return encrypt(bytes(s.begin(), s.end())).then([](bytes b) {
        return make_ready_future<sstring>(base64_encode(b));
    });
}

future<bytes> encryption::system_key::encrypt(const bytes& b) {
    return get_key().then([b](shared_ptr<symmetric_key> k) {
        auto i = k->iv_len();
        auto n = k->encrypted_size(b.size());
        bytes res(bytes::initialized_later(), n + i);
        k->generate_iv(reinterpret_cast<char*>(res.data()), i);
        n = k->encrypt(reinterpret_cast<const char*>(b.data()), b.size()
                        , reinterpret_cast<char*>(res.data()) + i, res.size() - i
                        , reinterpret_cast<const char*>(res.data()));
        res.resize(n + i);
        return make_ready_future<bytes>(std::move(res));
    });

}

future<bytes> encryption::system_key::decrypt(const bytes& b) {
    return get_key().then([b](shared_ptr<symmetric_key> k) {
        auto i = k->iv_len();
        bytes res(bytes::initialized_later(), b.size() - i);
        auto n = k->decrypt(reinterpret_cast<const char*>(b.data()) + i,
                        b.size() - i, reinterpret_cast<char*>(res.data()),
                        res.size(), reinterpret_cast<const char*>(b.data()));
        res.resize(n);
        return make_ready_future<bytes>(std::move(res));
    });
}

