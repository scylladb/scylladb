/*
 * Copyright (C) 2018-present ScyllaDB
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

#include <seastar/core/memory.hh>

#include "bytes.hh"
#include "bytes_ostream.hh"
#include "utils/fragmented_temporary_buffer.hh"

#include <boost/range/algorithm/for_each.hpp>

namespace utils {

/// A reusable buffer, for temporary linearisation of bytes_ostream
///
/// This class provides helpers for temporary linearisation of bytes_ostream.
/// Both cases when bytes_ostream holds input data as well as when it is
/// an output are supported. Copies are avoided if the buffers are not
/// fragmented.
///
/// Example of reading a possibly fragmented bytes_ostream:
///
/// ```c++
/// thread_local reusable_buffer rb;
/// bytes_ostream potentially_fragmented_buffer;
/// bytes_view view = rb.get_linearized_view(potentially_fragmented_buffer);
/// // view is a view of a buffer that holds the same data as potentially_fragmented buffer
/// ```
///
/// Example of writing to a bytes_ostream:
///
/// ```c++
/// thread_local reusable_buffer rb;
/// size_t maximum_size = compute_maximum_size();
/// bytes_ostream destination = rb.make_buffer([&] (bytes_mutable_view view) {
///     // view is a mutable view of some buffer which content will be in
///     // the bytes_ostream returned by make_buffer.
///     return actual_length_of_the_buffer;
/// });
/// ```
class reusable_buffer { // extract to utils
    // FIXME: We should start using std::byte for these things.
    std::unique_ptr<int8_t[]> _buffer;
    size_t _size = 0;
private:
    bytes_mutable_view reserve(size_t n) {
        if (_size < n) {
            // Reusable buffers are expected to be used when large contiguous
            // allocations are unavoidable. There is not much point in warning
            // about them since there isn't much that can be done.
            seastar::memory::scoped_large_allocation_warning_disable g;
            // std::make_unique would zero-initialise the buffer which is
            // just a waste of cycles. We can, however, summon an ancient
            // entity from the elder days of C++ to help us.
            _buffer.reset(new int8_t[n]);
            _size = n;
        }
        return bytes_mutable_view(_buffer.get(), n);
    }
public:
    /// Returns a linearised view of the provided bytes_ostream
    ///
    /// This function returns a linearised view of the data stored in the
    /// provided bytes_ostream. If it is fragmented the linearisation uses
    /// a buffer owned by this.
    /// The returned view remains valid as long as the original bytes_ostream
    /// is not modifed and no other member functions of this are called.
    bytes_view get_linearized_view(const bytes_ostream& data) {
        if (data.is_linearized()) {
            return data.view();
        }
        auto mutable_view = reserve(data.size());
        auto dst = mutable_view.begin();
        for (bytes_view fragment : data) {
            dst = std::copy(fragment.begin(), fragment.end(), dst);
        }
        return bytes_view(mutable_view);
    }


    bytes_view get_linearized_view(const fragmented_temporary_buffer::view& data) {
        if (data.empty()) {
            return { };
        } else if (std::next(data.begin()) == data.end()) {
            return *data.begin();
        }
        auto mutable_view = reserve(data.size_bytes());
        auto dst = mutable_view.begin();
        using boost::range::for_each;
        for_each(data, [&] (bytes_view fragment) {
            dst = std::copy(fragment.begin(), fragment.end(), dst);
        });
        return bytes_view(mutable_view);
    }

    /// Creates a bytes_ostream
    ///
    /// make_buffer calls provided function object and gives it a mutable
    /// view of some buffer. Data written to that view will be in the returned
    /// bytes_ostream. The function object is expected to return the actual
    /// size of the buffer (less than or equal the previously specified maximum
    /// length).
    template<typename Function>
    requires requires(Function fn, bytes_mutable_view view) {
        { fn(view) } -> std::convertible_to<size_t>;
    }
    bytes_ostream make_buffer(size_t maximum_length, Function&& fn) {
        bytes_ostream output;
        bytes_mutable_view view = [&] {
            if (maximum_length && maximum_length <= bytes_ostream::max_chunk_size()) {
                auto ptr = output.write_place_holder(maximum_length);
                return bytes_mutable_view(ptr, maximum_length);
            }
            return reserve(maximum_length);
        }();
        size_t actual_length = fn(view);
        if (output.empty()) {
            output.write(bytes_view(view.data(), actual_length));
        } else {
            output.remove_suffix(output.size() - actual_length);
        }
        return output;
    }

    template<typename Function>
    requires requires(Function fn, bytes_mutable_view view) {
        { fn(view) } -> std::same_as<size_t>;
    }
    fragmented_temporary_buffer make_fragmented_temporary_buffer(size_t maximum_length, size_t maximum_fragment_size, Function&& fn) {
        std::vector<temporary_buffer<char>> fragments;
        bytes_mutable_view view = [&] {
            if (maximum_length <= maximum_fragment_size) {
                fragments.emplace_back(maximum_length);
                return bytes_mutable_view(reinterpret_cast<bytes::pointer>(fragments.back().get_write()), maximum_length);
            }
            return reserve(maximum_length);
        }();
        size_t actual_length = fn(view);
        if (fragments.empty()) {
            auto left = actual_length;
            auto src = reinterpret_cast<const bytes::value_type*>(_buffer.get());
            while (left) {
                auto this_length = std::min(left, maximum_fragment_size);
                fragments.emplace_back(reinterpret_cast<const char*>(src), this_length);
                src += this_length;
                left -= this_length;
            }
        } else {
            fragments.back().trim(actual_length);
        }
        return fragmented_temporary_buffer(std::move(fragments), actual_length);
    }

    /// Releases all allocated memory.
    void clear() noexcept {
        _buffer.reset();
        _size = 0;
    }
};

}
