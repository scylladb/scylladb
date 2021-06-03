/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <vector>
#include <memory>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include <seastar/core/bitops.hh>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include "seastarx.hh"

namespace bi = boost::intrusive;

namespace utils {

struct do_nothing_loading_shared_values_stats {
    static void inc_hits() noexcept {} // Increase the number of times entry was found ready
    static void inc_misses() noexcept {} // Increase the number of times entry was not found
    static void inc_blocks() noexcept {} // Increase the number of times entry was not ready (>= misses)
    static void inc_evictions() noexcept {} // Increase the number of times entry was evicted
};

// Entries stay around as long as there is any live external reference (entry_ptr) to them.
// Supports asynchronous insertion, ensures that only one entry will be loaded.
// InitialBucketsCount is required to be greater than zero. Otherwise a constructor will throw an
// std::invalid_argument exception.
template<typename Key,
         typename Tp,
         typename Hash = std::hash<Key>,
         typename EqualPred = std::equal_to<Key>,
         typename Stats = do_nothing_loading_shared_values_stats,
         size_t InitialBucketsCount = 16>
requires requires () {
    Stats::inc_hits();
    Stats::inc_misses();
    Stats::inc_blocks();
    Stats::inc_evictions();
}
class loading_shared_values {
public:
    using key_type = Key;
    using value_type = Tp;
    static constexpr size_t initial_buckets_count = InitialBucketsCount;

private:
    class entry : public bi::unordered_set_base_hook<bi::store_hash<true>>, public enable_lw_shared_from_this<entry> {
    private:
        loading_shared_values& _parent;
        key_type _key;
        std::optional<value_type> _val;
        shared_promise<> _loaded;

    public:
        const key_type& key() const noexcept {
            return _key;
        }

        const value_type& value() const noexcept {
            return *_val;
        }

        value_type& value() noexcept {
            return *_val;
        }

        /// \brief "Release" the object from the contained value.
        /// After this call the state of the value kept inside this object is undefined and it may no longer be used.
        ///
        /// \return The r-value reference to the value kept inside this object.
        value_type&& release() {
            return *std::move(_val);
        }

        void set_value(value_type new_val) {
            _val.emplace(std::move(new_val));
        }

        shared_promise<>& loaded() {
            return _loaded;
        }

        bool ready() const noexcept {
            return bool(_val);
        }

        entry(loading_shared_values& parent, key_type k)
                : _parent(parent), _key(std::move(k)) {}

        ~entry() {
            _parent._set.erase(_parent._set.iterator_to(*this));
            Stats::inc_evictions();
        }

        friend bool operator==(const entry& a, const entry& b){
            return EqualPred()(a.key(), b.key());
        }

        friend std::size_t hash_value(const entry& v) {
            return Hash()(v.key());
        }
    };

    template<typename KeyType, typename KeyEqual>
    struct key_eq {
        bool operator()(const KeyType& k, const entry& c) const {
           return KeyEqual()(k, c.key());
        }

        bool operator()(const entry& c, const KeyType& k) const {
           return KeyEqual()(c.key(), k);
        }
    };

    using set_type = bi::unordered_set<entry, bi::power_2_buckets<true>, bi::compare_hash<true>>;
    using bi_set_bucket_traits = typename set_type::bucket_traits;
    using set_iterator = typename set_type::iterator;
    struct value_extractor_fn {
        value_type& operator()(entry& e) const {
            return e.value();
        }
    };
    enum class shrinking_is_allowed { no, yes };

public:
    using iterator = boost::transform_iterator<value_extractor_fn, set_iterator>;

public:
    // Pointer to entry value
    class entry_ptr {
        lw_shared_ptr<entry> _e;
    public:
        using element_type = value_type;
        entry_ptr() = default;
        explicit entry_ptr(lw_shared_ptr<entry> e) : _e(std::move(e)) {}
        entry_ptr& operator=(std::nullptr_t) noexcept {
            _e = nullptr;
            return *this;
        }
        explicit operator bool() const noexcept { return bool(_e); }
        element_type& operator*() const noexcept { return _e->value(); }
        element_type* operator->() const noexcept { return &_e->value(); }

        /// \brief Get the wrapped value. Avoid the copy if this is the last reference to this value.
        /// If this is the last reference then the wrapped value is going to be std::move()ed. Otherwise it's going to
        /// be copied.
        /// \return The wrapped value.
        element_type release() {
            auto res = _e.owned() ? _e->release() : _e->value();
            _e = nullptr;
            return res;
        }

        friend class loading_shared_values;
    };

private:
    std::vector<typename set_type::bucket_type> _buckets;
    set_type _set;
    value_extractor_fn _value_extractor_fn;

public:
    static const key_type& to_key(const entry_ptr& e_ptr) noexcept {
        return e_ptr._e->key();
    }

    /// \throw std::invalid_argument if InitialBucketsCount is zero
    loading_shared_values()
        : _buckets(InitialBucketsCount)
        , _set(bi_set_bucket_traits(_buckets.data(), _buckets.size()))
    {
        static_assert(noexcept(Stats::inc_evictions()), "Stats::inc_evictions must be non-throwing");
        static_assert(noexcept(Stats::inc_hits()), "Stats::inc_hits must be non-throwing");
        static_assert(noexcept(Stats::inc_misses()), "Stats::inc_misses must be non-throwing");
        static_assert(noexcept(Stats::inc_blocks()), "Stats::inc_blocks must be non-throwing");

        static_assert(InitialBucketsCount && ((InitialBucketsCount & (InitialBucketsCount - 1)) == 0), "Initial buckets count should be a power of two");
    }
    loading_shared_values(loading_shared_values&&) = default;
    loading_shared_values(const loading_shared_values&) = delete;
    ~loading_shared_values() {
         assert(!_set.size());
    }

    /// \brief
    /// Returns a future which resolves with a shared pointer to the entry for the given key.
    /// Always returns a valid pointer if succeeds.
    ///
    /// If entry is missing, the loader is invoked. If entry is already loading, this invocation
    /// will wait for prior loading to complete and use its result when it's done.
    ///
    /// The loader object does not survive deferring, so the caller must deal with its liveness.
    template<typename Loader>
    future<entry_ptr> get_or_load(const key_type& key, Loader&& loader) noexcept {
        static_assert(std::is_same<future<value_type>, typename futurize<std::result_of_t<Loader(const key_type&)>>::type>::value, "Bad Loader signature");
        try {
            auto i = _set.find(key, Hash(), key_eq<key_type, EqualPred>());
            lw_shared_ptr<entry> e;
            future<> f = make_ready_future<>();
            if (i != _set.end()) {
                e = i->shared_from_this();
                // take a short cut if the value is ready
                if (e->ready()) {
                    Stats::inc_hits();
                    return make_ready_future<entry_ptr>(entry_ptr(std::move(e)));
                }
                f = e->loaded().get_shared_future();
            } else {
                Stats::inc_misses();
                e = make_lw_shared<entry>(*this, key);
                rehash_before_insert();
                _set.insert(*e);
                // get_shared_future() may throw, so make sure to call it before invoking the loader(key)
                f = e->loaded().get_shared_future();
                // Future indirectly forwarded to `e`.
                (void)futurize_invoke([&] { return loader(key); }).then_wrapped([e](future<value_type>&& val_fut) mutable {
                    if (val_fut.failed()) {
                        e->loaded().set_exception(val_fut.get_exception());
                    } else {
                        e->set_value(val_fut.get0());
                        e->loaded().set_value();
                    }
                });
            }
            if (!f.available()) {
                Stats::inc_blocks();
                return f.then([e]() mutable {
                    return entry_ptr(std::move(e));
                });
            } else if (f.failed()) {
                return make_exception_future<entry_ptr>(std::move(f).get_exception());
            } else {
                Stats::inc_hits();
                return make_ready_future<entry_ptr>(entry_ptr(std::move(e)));
            }
        } catch (...) {
            return make_exception_future<entry_ptr>(std::current_exception());
        }
    }

    /// \brief Try to rehash the container so that the load factor is between 0.25 and 0.75.
    /// \throw May throw if allocation of a new buckets array throws.
    void rehash() {
        rehash<shrinking_is_allowed::yes>(_set.size());
    }

    size_t buckets_count() const {
        return _buckets.size();
    }

    size_t size() const {
        return _set.size();
    }

    iterator end() {
        return boost::make_transform_iterator(_set.end(), _value_extractor_fn);
    }

    iterator begin() {
        return boost::make_transform_iterator(_set.begin(), _value_extractor_fn);
    }

    template<typename KeyType, typename KeyHasher, typename KeyEqual>
    iterator find(const KeyType& key, KeyHasher key_hasher_func, KeyEqual key_equal_func) noexcept {
        set_iterator it = _set.find(key, std::move(key_hasher_func), key_eq<KeyType, KeyEqual>());
        if (it == _set.end() || !it->ready()) {
            return end();
        }
        return boost::make_transform_iterator(it, _value_extractor_fn);
    };

    // keep the default non-templated overloads to ease on the compiler for specifications
    // that do not require the templated find().
    iterator find(const key_type& key) noexcept {
        return find(key, Hash(), EqualPred());
    }

private:
    void rehash_before_insert() noexcept {
        try {
            rehash<shrinking_is_allowed::no>(_set.size() + 1);
        } catch (...) {
            // if rehashing fails - continue with the current buckets array
        }
    }

    template <shrinking_is_allowed ShrinkingIsAllowed>
    void rehash(size_t new_size) {
        size_t new_buckets_count = 0;

        // Try to keep the load factor between 0.25 (when shrinking is allowed) and 0.75.
        if (ShrinkingIsAllowed == shrinking_is_allowed::yes && new_size < buckets_count() / 4) {
            if (!new_size) {
                new_buckets_count = 1;
            } else {
                new_buckets_count = size_t(1) << log2floor(new_size * 4);
            }
        } else if (new_size > 3 * buckets_count() / 4) {
            new_buckets_count = buckets_count() * 2;
        }

        if (new_buckets_count < InitialBucketsCount) {
            return;
        }

        std::vector<typename set_type::bucket_type> new_buckets(new_buckets_count);
        _set.rehash(bi_set_bucket_traits(new_buckets.data(), new_buckets.size()));
        _buckets = std::move(new_buckets);
    }
};

}
