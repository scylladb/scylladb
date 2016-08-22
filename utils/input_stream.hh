/*
 * Copyright (C) 2016 ScyllaDB
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

#include <seastar/core/simple-stream.hh>

#include "bytes_ostream.hh"

namespace utils {

class fragmented_input_stream {
    bytes_ostream::fragment_iterator _it;
    bytes_view _current;
    size_t _size;
private:
    template<typename Func>
    //requires requires(Func f, bytes_view bv) { { f(bv) } -> void; }
    void for_each_fragment(size_t size, Func&& func) {
        if (size > _size) {
            throw std::out_of_range("deserialization buffer underflow");
        }
        _size -= size;
        while (size) {
            if (_current.empty()) {
                _current = *_it++;
            }
            auto this_size = std::min(_current.size(), size);
            func(_current.substr(0, this_size));
            _current.remove_prefix(this_size);
            size -= this_size;
        }
    }
public:
    explicit fragmented_input_stream(bytes_view bv)
        : _it(nullptr), _current(bv), _size(_current.size()) { }
    fragmented_input_stream(bytes_ostream::fragment_iterator it, size_t size)
        : _it(it), _current(size ? *_it++ : bytes_view()), _size(size) { }
    fragmented_input_stream(bytes_ostream::fragment_iterator it, bytes_view bv, size_t size)
        : _it(it), _current(bv), _size(size) { }

    void skip(size_t size) {
        for_each_fragment(size, [] (auto) { });
    }
    fragmented_input_stream read_substream(size_t size) {
        if (size > _size) {
            throw std::out_of_range("deserialization buffer underflow");
        }
        fragmented_input_stream substream(_it, _current, size);
        skip(size);
        return substream;
    }
    void read(char* p, size_t size) {
        for_each_fragment(size, [&p] (auto bv) {
            p = std::copy_n(bv.data(), bv.size(), p);
        });
    }
    template<typename Output>
    void copy_to(Output& out) {
        for_each_fragment(_size, [&out] (auto bv) {
            out.write(reinterpret_cast<const char*>(bv.data()), bv.size());
        });
    }
    const size_t size() const {
        return _size;
    }
};

/*
template<typename Visitor>
concept bool StreamVisitor() {
    return requires(Visitor visitor, seastar::simple_input_stream& simple, fragmented_input_stream& fragmented) {
        visitor(simple);
        visitor(fragmented);
    };
}
*/
// input_stream performs type erasure optimised for cases where
// seastar::simple_input_stream is used.
// By using a lot of [[gnu::always_inline]] attributes this class attempts to
// make the compiler generate code with simple_input_stream functions inlined
// directly in the user of the intput_stream.
class input_stream {
    const bool _is_simple;
    union {
        seastar::simple_input_stream _simple;
        fragmented_input_stream _fragmented;
    };
private:
    template<typename StreamVisitor>
    [[gnu::always_inline]]
    decltype(auto) with_stream(StreamVisitor&& visitor) {
        if (__builtin_expect(_is_simple, true)) {
            return visitor(_simple);
        }
        return visitor(_fragmented);
    }

    template<typename StreamVisitor>
    [[gnu::always_inline]]
    decltype(auto) with_stream(StreamVisitor&& visitor) const {
        if (__builtin_expect(_is_simple, true)) {
            return visitor(_simple);
        }
        return visitor(_fragmented);
    }
public:
    input_stream(seastar::simple_input_stream stream)
            : _is_simple(true), _simple(std::move(stream)) { }
    input_stream(fragmented_input_stream stream)
            : _is_simple(false), _fragmented(std::move(stream)) { }

    [[gnu::always_inline]]
    input_stream(const input_stream& other) noexcept : _is_simple(other._is_simple) {
        // Making this copy constructor noexcept makes copy assignment simpler.
        // Besides, performance of input_stream relies on the fact that both
        // fragmented and simple input stream are PODs and the branch below
        // is optimized away, so throwable copy constructors aren't something
        // we want.
        static_assert(std::is_nothrow_copy_constructible<fragmented_input_stream>::value,
                      "fragmented_input_stream should be copy constructible");
        static_assert(std::is_nothrow_copy_constructible<seastar::simple_input_stream>::value,
                      "seastar::simple_input_stream should be copy constructible");
        if (_is_simple) {
            new (&_simple) seastar::simple_input_stream(other._simple);
        } else {
            new (&_fragmented) fragmented_input_stream(other._fragmented);
        }
    }

    [[gnu::always_inline]]
    input_stream(input_stream&& other) noexcept : _is_simple(other._is_simple) {
        if (_is_simple) {
            new (&_simple) seastar::simple_input_stream(std::move(other._simple));
        } else {
            new (&_fragmented) fragmented_input_stream(std::move(other._fragmented));
        }
    }

    [[gnu::always_inline]]
    input_stream& operator=(const input_stream& other) noexcept {
        // Copy constructor being noexcept makes copy assignment simpler.
        static_assert(std::is_nothrow_copy_constructible<input_stream>::value,
                      "input_stream copy constructor shouldn't throw");
        if (this != &other) {
            this->~input_stream();
            new (this) input_stream(other);
        }
        return *this;
    }

    [[gnu::always_inline]]
    input_stream& operator=(input_stream&& other) noexcept {
        if (this != &other) {
            this->~input_stream();
            new (this) input_stream(std::move(other));
        }
        return *this;
    }

    [[gnu::always_inline]]
    ~input_stream() {
        if (_is_simple) {
            _simple.~simple_input_stream();
        } else {
            _fragmented.~fragmented_input_stream();
        }
    }

    [[gnu::always_inline]]
    void skip(size_t size) {
        with_stream([size] (auto& stream) {
            stream.skip(size);
        });
    }

    [[gnu::always_inline]]
    input_stream read_substream(size_t size) {
        return with_stream([size] (auto& stream) -> input_stream {
            return stream.read_substream(size);
        });
    }

    [[gnu::always_inline]]
    void read(char* p, size_t size) {
        with_stream([p, size] (auto& stream) {
            stream.read(p, size);
        });
    }

    template<typename Output>
    [[gnu::always_inline]]
    void copy_to(Output& out) {
        with_stream([&out] (auto& stream) {
            stream.copy_to(out);
        });
    }

    [[gnu::always_inline]]
    size_t size() const {
        return with_stream([] (auto& stream) {
            return stream.size();
        });
    }
public:
    // The purpose of the with_stream() is to minimize number of dynamic
    // dispatches. For example, a lot of IDL-generated code looks like this:
    // auto some_value() const {
    //     return utils::input_stream::with_stream(v, [] (auto& v) {
    //         auto in = v;
    //         ser::skip(in, boost::type<type1>());
    //         ser::skip(in, boost::type<type2>());
    //         return deserialize(in, boost::type<type3>());
    //     });
    // }
    // Using with_stream() there is at most one dynamic dispatch per such
    // function, instead of one per each skip() and deserialize() call.

    template<typename Stream, typename StreamVisitor>
    [[gnu::always_inline]]
    static std::enable_if_t<std::is_same<std::decay_t<Stream>, input_stream>::value, std::result_of_t<StreamVisitor(Stream&)>>
    with_stream(Stream& stream, StreamVisitor&& visitor) {
        return stream.with_stream(std::forward<StreamVisitor>(visitor));
    }

    template<typename Stream, typename StreamVisitor>
    [[gnu::always_inline]]
    static std::enable_if_t<!std::is_same<std::decay_t<Stream>, input_stream>::value, std::result_of_t<StreamVisitor(Stream&)>>
    with_stream(Stream& stream, StreamVisitor&& visitor) {
        return visitor(stream);
    }
};

}