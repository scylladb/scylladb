/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "auth/verified_password_cache.hh"

#include <memory>

#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/manual_clock.hh>

#include "auth/passwords.hh"

namespace auth {

template <typename Clock>
basic_verified_password_cache<Clock>::basic_verified_password_cache(utils::updateable_value<uint32_t> max_entries,
        utils::updateable_value<uint32_t> validity_in_ms)
        : _max_entries(std::move(max_entries))
        , _validity_in_ms(std::move(validity_in_ms))
        , _expiry(on_expiry{this})
        , _max_entries_observer(_max_entries.observe([this] (const uint32_t&) { shrink(0); }))
        , _validity_in_ms_observer(_validity_in_ms.observe([this] (const uint32_t&) { shrink(0); })) {
}

template <typename Clock>
basic_verified_password_cache<Clock>::~basic_verified_password_cache() {
    for (auto& [_, e] : _entries) {
        OPENSSL_cleanse(e.digest.data(), e.digest.size());
    }
}

template <typename Clock>
std::optional<typename basic_verified_password_cache<Clock>::digest_type>
basic_verified_password_cache<Clock>::compute_digest(const sstring& salted_hash, const sstring& password) {
    struct md_deleter {
        void operator()(EVP_MD* m) const { EVP_MD_free(m); }
        void operator()(EVP_MD_CTX* c) const { EVP_MD_CTX_free(c); }
    };
    static thread_local std::unique_ptr<EVP_MD, md_deleter> sha256;
    if (!sha256) {
        sha256.reset(EVP_MD_fetch(nullptr, OSSL_DIGEST_NAME_SHA2_256, nullptr));
        if (!sha256) {
            return std::nullopt;
        }
    }
    std::unique_ptr<EVP_MD_CTX, md_deleter> ctx(EVP_MD_CTX_new());
    if (!ctx) {
        return std::nullopt;
    }
    // The salted hash never contains a NUL, which makes the encoding of the pair unambiguous.
    const unsigned char separator = 0;
    digest_type digest;
    unsigned int digest_len = 0;
    if (EVP_DigestInit_ex2(ctx.get(), sha256.get(), nullptr) != 1
            || EVP_DigestUpdate(ctx.get(), salted_hash.data(), salted_hash.size()) != 1
            || EVP_DigestUpdate(ctx.get(), &separator, 1) != 1
            || EVP_DigestUpdate(ctx.get(), password.data(), password.size()) != 1
            || EVP_DigestFinal_ex(ctx.get(), digest.data(), &digest_len) != 1
            || digest_len != digest.size()) {
        return std::nullopt;
    }
    return digest;
}

template <typename Clock>
future<bool> basic_verified_password_cache<Clock>::check(std::string_view role_name, const sstring& password, const sstring& salted_hash) {
    shrink(enabled() ? _max_entries() : 0);
    const auto digest = enabled() ? compute_digest(salted_hash, password) : std::nullopt;
    if (!digest) {
        co_return co_await passwords::check(password, salted_hash);
    }
    if (auto it = _entries.find(role_name); it != _entries.end()) {
        const entry& e = it->second;
        if (e.salted_hash == salted_hash && CRYPTO_memcmp(e.digest.data(), digest->data(), digest->size()) == 0) {
            ++_stats.hits;
            co_return true;
        }
    }
    ++_stats.misses;
    if (!co_await passwords::check(password, salted_hash)) {
        co_return false;
    }
    insert(role_name, salted_hash, *digest);
    co_return true;
}

template <typename Clock>
void basic_verified_password_cache<Clock>::insert(std::string_view role_name, const sstring& salted_hash, const digest_type& digest) noexcept {
    if (!enabled()) {
        return;
    }
    shrink(_max_entries() - 1);
    const auto generation = _next_generation++;
    try {
        // Inserting the token first leaves no entry without one. A token without an entry
        // is harmless, so the replaced entry's token stays and expires as a no-op.
        _expiry.push_back(expiry_token{sstring(role_name), generation}, Clock::now() + std::chrono::milliseconds(_validity_in_ms()));
        entry e{salted_hash, digest, generation};
        _entries.insert_or_assign(sstring(role_name), std::move(e));
        OPENSSL_cleanse(e.digest.data(), e.digest.size());
    } catch (...) {
        // The role may be left with no entry, or with the previous one.
    }
}

template <typename Clock>
bool basic_verified_password_cache<Clock>::forget(const expiry_token& t) noexcept {
    if (auto it = _entries.find(t.role_name); it != _entries.end() && it->second.generation == t.generation) {
        OPENSSL_cleanse(it->second.digest.data(), it->second.digest.size());
        _entries.erase(it);
        return true;
    }
    return false;
}

template <typename Clock>
void basic_verified_password_cache<Clock>::shrink(size_t max_size) noexcept {
    while (_expiry.size() > max_size) {
        if (forget(_expiry.front())) {
            ++_stats.evictions;
        }
        _expiry.pop_front();
    }
}

template class basic_verified_password_cache<seastar::lowres_clock>;
template class basic_verified_password_cache<seastar::manual_clock>;

} // namespace auth
