/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/hashers.hh"
#include "utils/xx_hasher.hh"
#include "utils/simple_hashers.hh"

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>
#include <cryptopp/sha.h>

static_assert(Hasher<hasher>);

static_assert(HasherReturningBytes<md5_hasher>);
static_assert(HasherReturningBytes<sha256_hasher>);
static_assert(HasherReturningBytes<xx_hasher>);

static_assert(SimpleHasher<simple_xx_hasher>);

template <typename T> struct hasher_traits;
template <> struct hasher_traits<md5_hasher> { using impl_type = CryptoPP::Weak::MD5; };
template <> struct hasher_traits<sha256_hasher> { using impl_type = CryptoPP::SHA256; };

template<typename H>
concept HashUpdater =
    requires(typename hasher_traits<H>::impl_type& h, const CryptoPP::byte* ptr, size_t size) {
        // We need Update() not to throw, but it isn't marked noexcept
        // in CryptoPP source. We'll just hope it doesn't throw.
        { h.Update(ptr, size) } -> std::same_as<void>;
    };

template <typename T, size_t size>
struct cryptopp_hasher<T, size>::impl {
    static_assert(HashUpdater<T>);
    using impl_type = typename hasher_traits<T>::impl_type;

    impl_type hash{};

    void update(const char* ptr, size_t length) noexcept {
        using namespace CryptoPP;
        static_assert(sizeof(char) == sizeof(byte), "Assuming lengths will be the same");
        hash.Update(reinterpret_cast<const byte*>(ptr), length * sizeof(byte));
    }

    bytes finalize() {
        bytes digest{bytes::initialized_later(), size};
        hash.Final(reinterpret_cast<unsigned char*>(digest.begin()));
        return digest;
    }

    std::array<uint8_t, size> finalize_array() {
        std::array<uint8_t, size> array;
        hash.Final(reinterpret_cast<unsigned char*>(array.data()));
        return array;
    }
};

template <typename T, size_t size> cryptopp_hasher<T, size>::cryptopp_hasher() : _impl(std::make_unique<impl>()) {}

template <typename T, size_t size> cryptopp_hasher<T, size>::~cryptopp_hasher() = default;

template <typename T, size_t size> cryptopp_hasher<T, size>::cryptopp_hasher(cryptopp_hasher&& o) noexcept = default;

template <typename T, size_t size> cryptopp_hasher<T, size>::cryptopp_hasher(const cryptopp_hasher& o) : _impl(std::make_unique<cryptopp_hasher<T, size>::impl>(*o._impl)) {}

template <typename T, size_t size> cryptopp_hasher<T, size>& cryptopp_hasher<T, size>::operator=(cryptopp_hasher&& o) noexcept = default;

template <typename T, size_t size> cryptopp_hasher<T, size>& cryptopp_hasher<T, size>::operator=(const cryptopp_hasher& o) {
    _impl = std::make_unique<cryptopp_hasher<T, size>::impl>(*o._impl);
    return *this;
}

template <typename T, size_t size> bytes cryptopp_hasher<T, size>::finalize() { return _impl->finalize(); }

template <typename T, size_t size> std::array<uint8_t, size> cryptopp_hasher<T, size>::finalize_array() {
    return _impl->finalize_array();
}

template <typename T, size_t size> void cryptopp_hasher<T, size>::update(const char* ptr, size_t length) noexcept { _impl->update(ptr, length); }

template <typename T, size_t size> bytes cryptopp_hasher<T, size>::calculate(const std::string_view& s) {
    typename cryptopp_hasher<T, size>::impl::impl_type hash;
    unsigned char digest[size];
    hash.CalculateDigest(digest, reinterpret_cast<const unsigned char*>(s.data()), s.size());
    return bytes{reinterpret_cast<const int8_t*>(digest), size};
}

template class cryptopp_hasher<md5_hasher, 16>;
template class cryptopp_hasher<sha256_hasher, 32>;
