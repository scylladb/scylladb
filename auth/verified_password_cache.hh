/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <string_view>
#include <unordered_map>

#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include "utils/updateable_value.hh"
#include "seastarx.hh"

namespace auth {

///
/// Remembers credentials which recently passed \ref passwords::check, so that a client
/// reconnecting with the same credentials does not pay for SHA-crypt again.
///
/// For each role, an entry holds the salted hash the password was verified against and the
/// digest SHA-256(salted_hash || '\0' || password). The cache never stores the password; the
/// digest is a verifier for it which is cheap to test, unlike the salted hash, so it is only
/// as safe as the memory of the process.
///
/// A check hits only when both the salted hash and the digest match, so changing the password
/// (which generates a new salt) makes the old password miss. A miss pays for the full
/// \ref passwords::check, and only a successful one inserts.
///
/// An entry expires `validity_in_ms` after the check which inserted it; hits do not extend
/// it, so a role which keeps logging in is still fully verified once per validity period.
/// Expired entries are removed when they expire, also when no login comes.
///
/// The cache holds at most `max_entries` entries and evicts the oldest one. Zero
/// `max_entries` or `validity_in_ms` disables it. Changing either option clears the cache.
///
template <typename Clock>
class basic_verified_password_cache {
public:
    using digest_type = std::array<uint8_t, 32>;

    struct stats {
        uint64_t hits = 0;
        uint64_t misses = 0;
        // Entries removed before they expired, by the size bound or a config change.
        uint64_t evictions = 0;
    };
private:
    struct entry {
        sstring salted_hash;
        digest_type digest;
        uint64_t generation;
    };
    // Expires the entry of role_name, unless a newer one replaced it.
    struct expiry_token {
        sstring role_name;
        uint64_t generation;
    };
    struct on_expiry {
        basic_verified_password_cache* cache;
        void operator()(expiry_token& t) noexcept {
            cache->forget(t);
        }
    };

    utils::updateable_value<uint32_t> _max_entries;
    utils::updateable_value<uint32_t> _validity_in_ms;
    struct role_name_hash {
        using is_transparent = void;
        size_t operator()(std::string_view s) const noexcept {
            return std::hash<std::string_view>()(s);
        }
    };
    std::unordered_map<sstring, entry, role_name_hash, std::equal_to<>> _entries;
    // One token per insertion, oldest first. Bounds the cache, since there are at least as many
    // tokens as entries.
    expiring_fifo<expiry_token, on_expiry, Clock> _expiry;
    uint64_t _next_generation = 0;
    stats _stats;
    // Declared last, so that they detach before the state they clear is destroyed.
    utils::observer<uint32_t> _max_entries_observer;
    utils::observer<uint32_t> _validity_in_ms_observer;
public:
    basic_verified_password_cache(utils::updateable_value<uint32_t> max_entries, utils::updateable_value<uint32_t> validity_in_ms);
    ~basic_verified_password_cache();

    basic_verified_password_cache(const basic_verified_password_cache&) = delete;
    basic_verified_password_cache& operator=(const basic_verified_password_cache&) = delete;

    ///
    /// Equivalent to \ref passwords::check(password, salted_hash), where `salted_hash` is
    /// the current salted hash of `role_name`.
    ///
    future<bool> check(std::string_view role_name, const sstring& password, const sstring& salted_hash);

    size_t size() const noexcept {
        return _entries.size();
    }

    const stats& get_stats() const noexcept {
        return _stats;
    }
private:
    bool enabled() const noexcept {
        return _max_entries() && _validity_in_ms();
    }
    // Returns std::nullopt when the digest cannot be computed, which bypasses the cache.
    static std::optional<digest_type> compute_digest(const sstring& salted_hash, const sstring& password);
    // Best effort: when out of memory, the role ends up without an entry.
    void insert(std::string_view role_name, const sstring& salted_hash, const digest_type& digest) noexcept;
    // Returns true if it removed an entry.
    bool forget(const expiry_token& t) noexcept;
    // Expires the oldest tokens until at most `max_size` remain.
    void shrink(size_t max_size) noexcept;
};

using verified_password_cache = basic_verified_password_cache<seastar::lowres_clock>;

} // namespace auth
