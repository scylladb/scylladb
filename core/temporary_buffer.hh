/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef TEMPORARY_BUFFER_HH_
#define TEMPORARY_BUFFER_HH_

#include "deleter.hh"
#include "util/eclipse.hh"
#include <malloc.h>

/// \addtogroup memory-module
/// @{

/// Temporary, self-managed byte buffer.
///
/// A \c temporary_buffer is similar to an \c std::string or a \c std::unique_ptr<char[]>,
/// but provides more flexible memory management.  A \c temporary_buffer can own the memory
/// it points to, or it can be shared with another \c temporary_buffer, or point at a substring
/// of a buffer.  It uses a \ref deleter to manage its memory.
///
/// A \c temporary_buffer should not be held indefinitely.  It can be held while a request
/// is processed, or for a similar duration, but not longer, as it can tie up more memory
/// that its size indicates.
///
/// A buffer can be shared: two \c temporary_buffer objects will point to the same data,
/// or a subset of it.  See the \ref temporary_buffer::share() method.
///
/// Unless you created a \c temporary_buffer yourself, do not modify its contents, as they
/// may be shared with another user that does not expect the data to change.
///
/// Use cases for a \c temporary_buffer include:
///  - passing a substring of a tcp packet for the user to consume (zero-copy
///    tcp input)
///  - passing a refcounted blob held in memory to tcp, ensuring that when the TCP ACK
///    is received, the blob is released (by decrementing its reference count) (zero-copy
///    tcp output)
///
/// \tparam CharType underlying character type (must be a variant of \c char).
template <typename CharType>
class temporary_buffer {
    static_assert(sizeof(CharType) == 1, "must buffer stream of bytes");
    CharType* _buffer;
    size_t _size;
    deleter _deleter;
public:
    /// Creates a \c temporary_buffer of a specified size.  The buffer is not shared
    /// with anyone, and is not initialized.
    ///
    /// \param size buffer size, in bytes
    explicit temporary_buffer(size_t size)
        : _buffer(static_cast<CharType*>(malloc(size * sizeof(CharType)))), _size(size)
        , _deleter(make_free_deleter(_buffer)) {
        if (size && !_buffer) {
            throw std::bad_alloc();
        }
    }
    //explicit temporary_buffer(CharType* borrow, size_t size) : _buffer(borrow), _size(size) {}
    /// Creates an empty \c temporary_buffer that does not point at anything.
    temporary_buffer()
        : _buffer(nullptr)
        , _size(0) {}
    temporary_buffer(const temporary_buffer&) = delete;
    /// Moves a \c temporary_buffer.
    temporary_buffer(temporary_buffer&& x) : _buffer(x._buffer), _size(x._size), _deleter(std::move(x._deleter)) {
        x._buffer = nullptr;
        x._size = 0;
    }
    /// Creates a \c temporary_buffer with a specific deleter.
    ///
    /// \param buf beginning of the buffer held by this \c temporary_buffer
    /// \param size size of the buffer
    /// \param d deleter controlling destruction of the  buffer.  The deleter
    ///          will be destroyed when there are no longer any users for the buffer.
    temporary_buffer(CharType* buf, size_t size, deleter d)
        : _buffer(buf), _size(size), _deleter(std::move(d)) {}
    void operator=(const temporary_buffer&) = delete;
    /// Moves a \c temporary_buffer.
    temporary_buffer& operator=(temporary_buffer&& x) {
        if (this != &x) {
            _buffer = x._buffer;
            _size = x._size;
            _deleter = std::move(x._deleter);
            x._buffer = nullptr;
            x._size = 0;
        }
        return *this;
    }
    /// Gets a pointer to the beginning of the buffer.
    const CharType* get() const { return _buffer; }
    /// Gets a writable pointer to the beginning of the buffer.  Use only
    /// when you are certain no user expects the buffer data not to change.
    CharType* get_write() { return _buffer; }
    /// Gets the buffer size.
    size_t size() const { return _size; }
    /// Gets a pointer to the beginning of the buffer.
    const CharType* begin() { return _buffer; }
    /// Gets a pointer to the end of the buffer.
    const CharType* end() { return _buffer + _size; }
    /// Returns the buffer, but with a reduced size.  The original
    /// buffer is consumed by this call and can no longer be used.
    ///
    /// \param size New size; must be smaller than current size.
    /// \return the same buffer, with a prefix removed.
    temporary_buffer prefix(size_t size) && {
        auto ret = std::move(*this);
        ret._size = size;
        return ret;
    }
    /// Reads a character from a specific position in the buffer.
    ///
    /// \param pos position to read character from; must be less than size.
    CharType operator[](size_t pos) const {
        return _buffer[pos];
    }
    /// Checks whether the buffer is empty.
    bool empty() const { return !size(); }
    /// Checks whether the buffer is not empty.
    operator bool() { return size(); }
    /// Create a new \c temporary_buffer object referring to the same
    /// underlying data.  The underlying \ref deleter will not be destroyed
    /// until both the original and the clone have been destroyed.
    ///
    /// \return a clone of the buffer object.
    temporary_buffer share() {
        return temporary_buffer(_buffer, _size, _deleter.share());
    }
    /// Create a new \c temporary_buffer object referring to a substring of the
    /// same underlying data.  The underlying \ref deleter will not be destroyed
    /// until both the original and the clone have been destroyed.
    ///
    /// \param pos Position of the first character to share.
    /// \param len Length of substring to share.
    /// \return a clone of the buffer object, referring to a substring.
    temporary_buffer share(size_t pos, size_t len) {
        auto ret = share();
        ret._buffer += pos;
        ret._size = len;
        return ret;
    }
    /// Remove a prefix from the buffer.  The underlying data
    /// is not modified.
    ///
    /// \param pos Position of first character to retain.
    void trim_front(size_t pos) {
        _buffer += pos;
        _size -= pos;
    }
    /// Remove a suffix from the buffer.  The underlying data
    /// is not modified.
    ///
    /// \param pos Position of first character to drop.
    void trim(size_t pos) {
        _size = pos;
    }
    /// Stops automatic memory management.  When the \c temporary_buffer
    /// object is destroyed, the underlying \ref deleter will not be called.
    /// Instead, it is the caller's responsibility to destroy the deleter object
    /// when the data is no longer needed.
    ///
    /// \return \ref deleter object managing the data's lifetime.
    deleter release() {
        return std::move(_deleter);
    }
    /// Creates a \c temporary_buffer object with a specified size, with
    /// memory aligned to a specific boundary.
    ///
    /// \param alignment Required alignment; must be a power of two.
    /// \param size Required size.
    /// \return a new \c temporary_buffer object.
    static temporary_buffer aligned(size_t alignment, size_t size) {
        void *ptr = nullptr;
        auto ret = ::posix_memalign(&ptr, alignment, size * sizeof(CharType));
        auto buf = static_cast<CharType*>(ptr);
        if (ret) {
            throw std::bad_alloc();
        }
        return temporary_buffer(buf, size, make_free_deleter(buf));
    }
};

/// @}

#endif /* TEMPORARY_BUFFER_HH_ */
