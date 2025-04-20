/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <vector>

#include <seastar/core/iostream.hh>
#include <seastar/core/format.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/simple-stream.hh>

#include "bytes.hh"
#include "bytes_ostream.hh"
#include "contiguous_shared_buffer.hh"
#include "fragment_range.hh"

/// Fragmented buffer consisting of multiple Buffer objects.
template <ContiguousSharedBuffer Buffer>
class basic_fragmented_buffer {
    using vector_type = std::vector<Buffer>;
    vector_type _fragments;
    size_t _size_bytes = 0;
public:
    static constexpr size_t default_fragment_size = 128 * 1024;

    class view;
    class istream;
    class reader;
    using ostream = seastar::memory_output_stream<typename vector_type::iterator>;

    basic_fragmented_buffer() = default;

    basic_fragmented_buffer(std::vector<Buffer> fragments, size_t size_bytes) noexcept
        : _fragments(std::move(fragments)), _size_bytes(size_bytes)
    { }

    basic_fragmented_buffer(const char* str, size_t size)
    {
        *this = allocate_to_fit(size);
        size_t pos = 0;
        for (auto& frag : _fragments) {
            std::memcpy(frag.get_write(), str + pos, frag.size());
            pos += frag.size();
        }
    }

    explicit operator view() const noexcept;

    istream get_istream() const noexcept;

    ostream get_ostream() noexcept {
        if (_fragments.size() != 1) {
            return typename ostream::fragmented(_fragments.begin(), _size_bytes);
        }
        auto& current = *_fragments.begin();
        return typename ostream::simple(reinterpret_cast<char*>(current.get_write()), current.size());
    }

    using const_fragment_iterator = typename vector_type::const_iterator;

    const_fragment_iterator begin() const {
        return _fragments.begin();
    }
    const_fragment_iterator end() const {
        return _fragments.end();
    }

    size_t size_bytes() const { return _size_bytes; }
    bool empty() const { return !_size_bytes; }

    // Linear complexity, invalidates views and istreams
    void remove_prefix(size_t n) noexcept {
        _size_bytes -= n;
        auto it = _fragments.begin();
        while (it->size() < n) {
            n -= it->size();
            ++it;
        }
        if (n) {
            it->trim_front(n);
        }
        _fragments.erase(_fragments.begin(), it);
    }

    // Linear complexity, invalidates views and istreams
    void remove_suffix(size_t n) noexcept {
        _size_bytes -= n;
        auto it = _fragments.rbegin();
        while (it->size() < n) {
            n -= it->size();
            ++it;
        }
        if (n) {
            it->trim(it->size() - n);
        }
        _fragments.erase(it.base(), _fragments.end());
    }

    // Creates a fragmented buffer of a specified size, supplied as a parameter.
    // Max chunk size is limited to 128kb (the same limit as `bytes_stream` has).
    static basic_fragmented_buffer allocate_to_fit(size_t data_size) {
        constexpr size_t max_fragment_size = default_fragment_size; // 128KB

        const size_t full_fragment_count = data_size / max_fragment_size; // number of max-sized fragments
        const size_t last_fragment_size = data_size % max_fragment_size;

        std::vector<Buffer> fragments;
        fragments.reserve(full_fragment_count + !!last_fragment_size);
        for (size_t i = 0; i < full_fragment_count; ++i) {
            fragments.emplace_back(Buffer(max_fragment_size));
        }
        if (last_fragment_size) {
            fragments.emplace_back(Buffer(last_fragment_size));
        }
        return basic_fragmented_buffer(std::move(fragments), data_size);
    }

    vector_type release() && noexcept {
        return std::move(_fragments);
    }
};

template <ContiguousSharedBuffer Buffer>
class basic_fragmented_buffer<Buffer>::view {
    vector_type::const_iterator _current;
    const char* _current_position = nullptr;
    size_t _current_size = 0;
    size_t _total_size = 0;
public:
    view() = default;
    view(vector_type::const_iterator it, size_t position, size_t total_size)
        : _current(it)
        , _current_position(it->get() + position)
        , _current_size(std::min(it->size() - position, total_size))
        , _total_size(total_size)
    { }

    explicit view(bytes_view bv) noexcept
        : _current_position(reinterpret_cast<const char*>(bv.data()))
        , _current_size(bv.size())
        , _total_size(bv.size())
    { }

    using fragment_type = bytes_view;

    class iterator {
        vector_type::const_iterator _it;
        size_t _left = 0;
        bytes_view _current;
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = bytes_view;
        using difference_type = ptrdiff_t;
        using pointer = const bytes_view*;
        using reference = const bytes_view&;

        iterator() = default;
        iterator(vector_type::const_iterator it, bytes_view current, size_t left) noexcept
            : _it(it)
            , _left(left)
            , _current(current)
        { }

        reference operator*() const noexcept { return _current; }
        pointer operator->() const noexcept { return &_current; }

        iterator& operator++() noexcept {
            _left -= _current.size();
            if (_left) {
                ++_it;
                _current = bytes_view(reinterpret_cast<const bytes::value_type*>(_it->get()),
                                      std::min(_left, _it->size()));
            }
            return *this;
        }

        iterator operator++(int) noexcept {
            auto it = *this;
            operator++();
            return it;
        }

        bool operator==(const iterator& other) const noexcept {
            return _left == other._left;
        }
    };

    using const_iterator = iterator;

    iterator begin() const noexcept {
        return iterator(_current,
                        bytes_view(reinterpret_cast<const bytes::value_type*>(_current_position), _current_size),
                        _total_size);
    }
    iterator end() const noexcept {
        return iterator();
    }

    bool empty() const noexcept { return !size_bytes(); }
    size_t size_bytes() const noexcept { return _total_size; }

    void remove_prefix(size_t n) noexcept {
        if (!_total_size) {
            return;
        }
        while (n > _current_size) {
            _total_size -= _current_size;
            n -= _current_size;
            ++_current;
            _current_size = std::min(_current->size(), _total_size);
            _current_position = _current->get();
        }
        _total_size -= n;
        _current_size -= n;
        _current_position += n;
        if (!_current_size && _total_size) {
            ++_current;
            _current_size = std::min(_current->size(), _total_size);
            _current_position = _current->get();
        }
    }

    void remove_current() noexcept {
        _total_size -= _current_size;
        if (_total_size) {
            ++_current;
            _current_size = std::min(_current->size(), _total_size);
            _current_position = _current->get();
        } else {
            _current_size = 0;
            _current_position = nullptr;
        }
    }

    view prefix(size_t n) const {
        auto tmp = *this;
        tmp._total_size = std::min(tmp._total_size, n);
        tmp._current_size = std::min(tmp._current_size, n);
        return tmp;
    }

    bytes_view current_fragment() const noexcept {
        return bytes_view(reinterpret_cast<const bytes_view::value_type*>(_current_position), _current_size);
    }

    // Invalidates iterators
    void remove_suffix(size_t n) noexcept {
        _total_size -= n;
        _current_size = std::min(_current_size, _total_size);
    }

    bool operator==(const basic_fragmented_buffer::view& other) const noexcept {
        auto this_it = begin();
        auto other_it = other.begin();

        if (empty() || other.empty()) {
            return empty() && other.empty();
        }

        auto this_fragment = *this_it;
        auto other_fragment = *other_it;
        while (this_it != end() && other_it != other.end()) {
            if (this_fragment.empty()) {
                ++this_it;
                if (this_it != end()) {
                    this_fragment = *this_it;
                }
            }
            if (other_fragment.empty()) {
                ++other_it;
                if (other_it != other.end()) {
                    other_fragment = *other_it;
                }
            }
            auto length = std::min(this_fragment.size(), other_fragment.size());
            if (!std::equal(this_fragment.data(), this_fragment.data() + length, other_fragment.data())) {
                return false;
            }
            this_fragment.remove_prefix(length);
            other_fragment.remove_prefix(length);
        }
        return this_it == end() && other_it == other.end();
    }
};

using fragmented_temporary_buffer = basic_fragmented_buffer<temporary_buffer<char>>;

static_assert(FragmentRange<fragmented_temporary_buffer::view>);
static_assert(FragmentedView<fragmented_temporary_buffer::view>);

template <ContiguousSharedBuffer Buffer>
inline basic_fragmented_buffer<Buffer>::operator view() const noexcept
{
    if (!_size_bytes) {
        return view();
    }
    return view(_fragments.begin(), 0, _size_bytes);
}

namespace fragmented_temporary_buffer_concepts {

template<typename T>
concept ExceptionThrower = requires(T obj, size_t n) {
    obj.throw_out_of_range(n, n);
};

}

template <ContiguousSharedBuffer Buffer>
class basic_fragmented_buffer<Buffer>::istream {
    vector_type::const_iterator _current;
    const char* _current_position;
    const char* _current_end;
    size_t _bytes_left = 0;
private:
    size_t contig_remain() const {
        return _current_end - _current_position;
    }
    void next_fragment() {
        _bytes_left -= _current->size();
        if (_bytes_left) {
            _current++;
            _current_position = _current->get();
            _current_end = _current->get() + _current->size();
        } else {
            _current_position = nullptr;
            _current_end = nullptr;
        }
    }

    template<typename ExceptionThrower>
    requires fragmented_temporary_buffer_concepts::ExceptionThrower<ExceptionThrower>
    void check_out_of_range(ExceptionThrower& exceptions, size_t n) {
        if (__builtin_expect(bytes_left() < n, false)) {
            exceptions.throw_out_of_range(n, bytes_left());
            // Let's allow skipping this check if the user trusts its input
            // data.
        }
    }

    template<typename T, typename ExceptionThrower>
    [[gnu::noinline]] [[gnu::cold]]
    T read_slow(ExceptionThrower&& exceptions) {
        check_out_of_range(exceptions, sizeof(T));

        T obj;
        size_t left = sizeof(T);
        while (left) {
            auto this_length = std::min<size_t>(left, _current_end - _current_position);
            std::copy_n(_current_position, this_length, reinterpret_cast<char*>(&obj) + sizeof(T) - left);
            left -= this_length;
            if (left) {
                next_fragment();
            } else {
                _current_position += this_length;
            }
        }
        return obj;
    }

    [[gnu::noinline]] [[gnu::cold]]
    void skip_slow(size_t n) noexcept {
        auto left = std::min<size_t>(n, bytes_left());
        while (left) {
            auto this_length = std::min<size_t>(left, _current_end - _current_position);
            left -= this_length;
            if (left) {
                next_fragment();
            } else {
                _current_position += this_length;
            }
        }
    }
public:
    struct default_exception_thrower {
        [[noreturn]] [[gnu::cold]]
        static void throw_out_of_range(size_t attempted_read, size_t actual_left) {
            throw std::out_of_range(format("attempted to read {:d} bytes from a {:d} byte buffer", attempted_read, actual_left));
        }
    };
    static_assert(fragmented_temporary_buffer_concepts::ExceptionThrower<default_exception_thrower>);

    istream(const vector_type& fragments, size_t total_size) noexcept
        : _current(fragments.begin())
        , _current_position(total_size ? _current->get() : nullptr)
        , _current_end(total_size ? _current->get() + _current->size() : nullptr)
        , _bytes_left(total_size)
    { }

    size_t bytes_left() const noexcept {
        return _bytes_left ? _bytes_left - (_current_position - _current->get()) : 0;
    }

    void skip(size_t n) noexcept {
        if (__builtin_expect(contig_remain() < n, false)) {
            return skip_slow(n);
        }
        _current_position += n;
    }

    template<typename T, typename ExceptionThrower = default_exception_thrower>
    requires fragmented_temporary_buffer_concepts::ExceptionThrower<ExceptionThrower>
    T read(ExceptionThrower&& exceptions = default_exception_thrower()) {
        if (__builtin_expect(contig_remain() < sizeof(T), false)) {
            return read_slow<T>(std::forward<ExceptionThrower>(exceptions));
        }
        T obj;
        std::copy_n(_current_position, sizeof(T), reinterpret_cast<char*>(&obj));
        _current_position += sizeof(T);
        return obj;
    }

    template<typename Output, typename ExceptionThrower = default_exception_thrower>
    requires fragmented_temporary_buffer_concepts::ExceptionThrower<ExceptionThrower>
    Output read_to(size_t n, Output out, ExceptionThrower&& exceptions = default_exception_thrower()) {
        if (__builtin_expect(contig_remain() >= n, true)) {
            out = std::copy_n(_current_position, n, out);
            _current_position += n;
            return out;
        }
        check_out_of_range(exceptions, n);
        out = std::copy(_current_position, _current_end, out);
        n -= _current_end - _current_position;
        next_fragment();
        while (n > _current->size()) {
            out = std::copy(_current_position, _current_end, out);
            n -= _current->size();
            next_fragment();
        }
        out = std::copy_n(_current_position, n, out);
        _current_position += n;
        return out;
    }

    template<typename ExceptionThrower = default_exception_thrower>
    requires fragmented_temporary_buffer_concepts::ExceptionThrower<ExceptionThrower>
    view read_view(size_t n, ExceptionThrower&& exceptions = default_exception_thrower()) {
        if (__builtin_expect(contig_remain() >= n, true)) {
            auto v = view(_current, _current_position - _current->get(), n);
            _current_position += n;
            return v;
        }
        check_out_of_range(exceptions, n);
        auto v = view(_current, _current_position - _current->get(), n);
        n -= _current_end - _current_position;
        next_fragment();
        while (n > _current->size()) {
            n -= _current->size();
            next_fragment();
        }
        _current_position += n;
        return v;
    }

    template<typename ExceptionThrower = default_exception_thrower>
    requires fragmented_temporary_buffer_concepts::ExceptionThrower<ExceptionThrower>
    bytes_view read_bytes_view(size_t n, bytes_ostream& linearization_buffer, ExceptionThrower&& exceptions = default_exception_thrower()) {
        if (__builtin_expect(contig_remain() >= n, true)) {
            auto v = bytes_view(reinterpret_cast<const bytes::value_type*>(_current_position), n);
            _current_position += n;
            return v;
        }
        check_out_of_range(exceptions, n);
        auto ptr = linearization_buffer.write_place_holder(n);
        read_to(n, ptr, std::forward<ExceptionThrower>(exceptions));
        return bytes_view(reinterpret_cast<const bytes::value_type*>(ptr), n);
    }
};

template <ContiguousSharedBuffer Buffer>
inline basic_fragmented_buffer<Buffer>::istream basic_fragmented_buffer<Buffer>::get_istream() const noexcept // allow empty (ut for that)
{
    return istream(_fragments, _size_bytes);
}

template <ContiguousSharedBuffer Buffer>
class basic_fragmented_buffer<Buffer>::reader {
    using FragBuffer = basic_fragmented_buffer<Buffer>;
    FragBuffer::vector_type _fragments;
    size_t _left = 0;
public:
    future<FragBuffer> read_exactly(input_stream<char>& in, size_t length) {
        _fragments = FragBuffer::vector_type();
        _left = length;
        return repeat_until_value([this, length, &in] {
            if (!_left) {
                return make_ready_future<std::optional<FragBuffer>>(FragBuffer(std::move(_fragments), length));
            }
            return in.read_up_to(_left).then([this] (temporary_buffer<char> buf) {
                if (buf.empty()) {
                    return std::make_optional(FragBuffer());
                }
                _left -= buf.size();
                _fragments.emplace_back(Buffer(std::move(buf)));
                return std::optional<FragBuffer>();
            });
        });
    }
};

// The operator below is used only for logging

template <ContiguousSharedBuffer Buffer>
inline std::ostream& operator<<(std::ostream& out, const typename basic_fragmented_buffer<Buffer>::view& v) {
    for (bytes_view frag : fragment_range(v)) {
        out << to_hex(frag);
    }
    return out;
}
