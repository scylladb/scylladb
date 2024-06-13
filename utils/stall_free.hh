/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <list>
#include <algorithm>
#include <seastar/core/thread.hh>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/do_with.hh>
#include "utils/collection-concepts.hh"

using namespace seastar;

namespace utils {


// Similar to std::merge but it does not stall. Must run inside a seastar
// thread. It merges items from list2 into list1. Items from list2 can only be copied.
template<class T, class Compare>
requires LessComparable<T, T, Compare>
void merge_to_gently(std::list<T>& list1, const std::list<T>& list2, Compare comp) {
    auto first1 = list1.begin();
    auto first2 = list2.begin();
    auto last1 = list1.end();
    auto last2 = list2.end();
    while (first2 != last2) {
        seastar::thread::maybe_yield();
        if (first1 == last1) {
            // Copy remaining items of list2 into list1
            list1.insert(last1, *first2);
            ++first2;
            continue;
        }
        if (comp(*first2, *first1)) {
            list1.insert(first1, *first2);
            ++first2;
        } else {
            ++first1;
        }
    }
}

// The clear_gently functions are meant for
// gently destroying the contents of containers.
// The containers can be re-used after clear_gently
// or may be destroyed.  But unlike e.g. std::vector::clear(),
// clear_gently will not necessarily keep the object allocation.

template <typename T>
concept HasClearGentlyMethod = requires (T x) {
    { x.clear_gently() } -> std::same_as<future<>>;
};

template <typename T>
concept SmartPointer = requires (T x) {
    { x.get() } -> std::same_as<typename T::element_type*>;
    { *x } -> std::same_as<typename T::element_type&>;
};

template <typename T>
concept SharedPointer = SmartPointer<T> && requires (T x) {
    { x.use_count() } -> std::convertible_to<long>;
};

template <typename T>
concept StringLike = requires (T x) {
    std::is_same_v<typename T::traits_type, std::char_traits<typename T::value_type>>;
};

template <typename T>
concept Iterable = requires (T x) {
    { x.empty() } -> std::same_as<bool>;
    { x.begin() } -> std::same_as<typename T::iterator>;
    { x.end() } -> std::same_as<typename T::iterator>;
};

template <typename T>
concept Sequence = Iterable<T> && requires (T x, size_t n) {
    { x.back() } -> std::same_as<typename T::value_type&>;
    { x.pop_back() } -> std::same_as<void>;
};

template <typename T>
concept TriviallyClearableSequence =
    Sequence<T> && std::is_trivially_destructible_v<typename T::value_type> && !HasClearGentlyMethod<typename T::value_type> && requires (T s) {
        { s.clear() } -> std::same_as<void>;
    };

template <typename T>
concept Container = Iterable<T> && requires (T x, typename T::iterator it) {
    x.erase(it);
};

template <typename T>
concept MapLike = Container<T> && requires (T x) {
    std::is_same_v<typename T::value_type, std::pair<const typename T::key_type, typename T::mapped_type>>;
};

template <HasClearGentlyMethod T>
future<> clear_gently(T& o) noexcept;

template <typename T>
future<> clear_gently(foreign_ptr<T>& o) noexcept;

template <SharedPointer T>
future<> clear_gently(T& o) noexcept;

template <SmartPointer T>
future<> clear_gently(T& o) noexcept;

template <typename T, std::size_t N>
future<> clear_gently(std::array<T, N>&a) noexcept;

template <typename T>
requires (StringLike<T> || TriviallyClearableSequence<T>)
future<> clear_gently(T& s) noexcept;

template <Sequence T>
requires (!StringLike<T> && !TriviallyClearableSequence<T>)
future<> clear_gently(T& v) noexcept;

template <MapLike T>
future<> clear_gently(T& c) noexcept;

template <Container T>
requires (!StringLike<T> && !Sequence<T> && !MapLike<T>)
future<> clear_gently(T& c) noexcept;

template <typename T>
future<> clear_gently(std::optional<T>& opt) noexcept;

template <typename T>
future<> clear_gently(seastar::optimized_optional<T>& opt) noexcept;

namespace internal {

template <typename T>
concept HasClearGentlyImpl = requires (T x) {
    { clear_gently(x) } -> std::same_as<future<>>;
};

template <typename T>
requires HasClearGentlyImpl<T>
future<> clear_gently(T& x) noexcept {
    return utils::clear_gently(x);
}

// This default implementation of clear_gently
// is required to "terminate" recursive clear_gently calls
// at trivial objects
template <typename T>
future<> clear_gently(T&) noexcept {
    return make_ready_future<>();
}

} // namespace internal

template <HasClearGentlyMethod T>
future<> clear_gently(T& o) noexcept {
    return futurize_invoke(std::bind(&T::clear_gently, &o));
}

template <typename T>
future<> clear_gently(foreign_ptr<T>& o) noexcept {
    return smp::submit_to(o.get_owner_shard(), [&o] {
        return internal::clear_gently(*o);
    });
}

template <typename... T>
requires (std::is_rvalue_reference_v<T&&> && ...)
future<> clear_gently(T&&... o) {
    return do_with(std::move(o)..., [](auto&... args) {
        return when_all(clear_gently(args)...).discard_result();
    });
}

template <SharedPointer T>
future<> clear_gently(T& o) noexcept {
    if (o.use_count() == 1) {
        return internal::clear_gently(*o);
    }
    return make_ready_future<>();
}

template <SmartPointer T>
future<> clear_gently(T& o) noexcept {
    if (auto p = o.get()) {
        return internal::clear_gently(*p);
    } else {
        return make_ready_future<>();
    }
}

template <typename T, std::size_t N>
future<> clear_gently(std::array<T, N>& a) noexcept {
    return do_for_each(a, [] (T& o) {
        return internal::clear_gently(o);
    });
}

// Trivially destructible elements can be safely cleared in bulk
template <typename T>
requires (StringLike<T> || TriviallyClearableSequence<T>)
future<> clear_gently(T& s) noexcept {
    // Note: clear() is pointless in this case since it keeps the allocation
    // and since the values are trivially destructible it achieves nothing.
    // `s = {}` will free the vector/string allocation.
    s = {};
    return make_ready_future<>();
}

// Clear the elements gently and destroy them one-by-one
// in reverse order, to avoid copying.
template <Sequence T>
requires (!StringLike<T> && !TriviallyClearableSequence<T>)
future<> clear_gently(T& v) noexcept {
    return do_until([&v] { return v.empty(); }, [&v] {
        return internal::clear_gently(v.back()).then([&v] {
            v.pop_back();
        });
    });
    return make_ready_future<>();
}

template <MapLike T>
future<> clear_gently(T& c) noexcept {
    return do_until([&c] { return c.empty(); }, [&c] {
        auto it = c.begin();
        return internal::clear_gently(it->second).then([&c, it = std::move(it)] () mutable {
            c.erase(it);
        });
    });
}

template <Container T>
requires (!StringLike<T> && !Sequence<T> && !MapLike<T>)
future<> clear_gently(T& c) noexcept {
    return do_until([&c] { return c.empty(); }, [&c] {
        auto it = c.begin();
        return internal::clear_gently(*it).then([&c, it = std::move(it)] () mutable {
            c.erase(it);
        });
    });
}

template <typename T>
future<> clear_gently(std::optional<T>& opt) noexcept {
    if (opt) {
        return utils::clear_gently(*opt);
    } else {
        return make_ready_future<>();
    }
}

template <typename T>
future<> clear_gently(seastar::optimized_optional<T>& opt) noexcept {
    if (opt) {
        return utils::clear_gently(*opt);
    } else {
        return make_ready_future<>();
    }
}

namespace internal {

template <typename T>
concept gently_reservable = requires(T x) {
    { x.capacity() } -> std::same_as<size_t>;
    { x.reserve_partial(10) } -> std::same_as<void>;

};

} // namespace internal

// reserve_gently gently reserves memory in containers which support partial reserve.
future<> reserve_gently(internal::gently_reservable auto& container, size_t size) {
    return seastar::do_until([&container, size] { return container.capacity() == size; }, [&container, size]() {
        container.reserve_partial(size);
        return seastar::make_ready_future();
    });
}

}

