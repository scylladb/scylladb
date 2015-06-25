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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "future.hh"
#include "queue.hh"

#include <experimental/optional>

/// \defgroup fiber-module Fibers
///
/// \brief Fibers of execution
///
/// Seastar continuations are normally short, but often chained to one
/// another, so that one continuation does a bit of work and then schedules
/// another continuation for later. Such chains can be long, and often even
/// involve loopings - see for example \ref do_with. We call such chains
/// "fibers" of execution.
///
/// These fibers are not threads - each is just a string of continuations -
/// but they share some common requirements with traditional threads.
/// For example, we want to avoid one fiber getting starved while a second
/// fiber continuously runs its continuations one after another.
/// As another example, fibers may want to communicate - e.g., one fiber
/// produces data that a second fiber consumes, and we wish to ensure that
/// both fibers get a chance to run, and that if one stops prematurely,
/// the other doesn't hang forever.


/// Seastar API namespace
namespace seastar {

/// \addtogroup fiber-module
/// @{

class broken_pipe_exception : public std::exception {
public:
    virtual const char* what() const noexcept {
        return "Broken pipe";
    }
};

/// \cond internal
namespace internal {
template <typename T>
class pipe_buffer {
private:
    queue<std::experimental::optional<T>> _buf;
    bool _read_open = true;
    bool _write_open = true;
public:
    pipe_buffer(size_t size) : _buf(size) {}
    future<std::experimental::optional<T>> read() {
        return _buf.pop_eventually();
    }
    future<> write(T&& data) {
        return _buf.push_eventually(std::move(data));
    }
    bool readable() const {
        return _write_open || !_buf.empty();
    }
    bool writeable() const {
        return _read_open;
    }
    bool close_read() {
        // If a writer blocking (on a full queue), need to stop it.
        if (_buf.full()) {
            _buf.abort(std::make_exception_ptr(broken_pipe_exception()));
        }
        _read_open = false;
        return !_write_open;
    }
    bool close_write() {
        // If the queue is empty, write the EOF (disengaged optional) to the
        // queue to wake a blocked reader. If the queue is not empty, there is
        // no need to write the EOF to the queue - the reader will return an
        // EOF when it sees that _write_open == false.
        if (_buf.empty()) {
            _buf.push({});
        }
        _write_open = false;
        return !_read_open;
    }
};
} // namespace internal
/// \endcond

template <typename T>
class pipe;

/// \brief Read side of a \ref seastar::pipe
///
/// The read side of a pipe, which allows only reading from the pipe.
/// A pipe_reader object cannot be created separately, but only as part of a
/// reader/writer pair through \ref seastar::pipe.
template <typename T>
class pipe_reader {
private:
    internal::pipe_buffer<T> *_bufp;
    pipe_reader(internal::pipe_buffer<T> *bufp) : _bufp(bufp) { }
    friend class pipe<T>;
public:
    /// \brief Read next item from the pipe
    ///
    /// Returns a future value, which is fulfilled when the pipe's buffer
    /// becomes non-empty, or the write side is closed. The value returned
    /// is an optional<T>, which is disengaged to mark and end of file
    /// (i.e., the write side was closed, and we've read everything it sent).
    future<std::experimental::optional<T>> read() {
        if (_bufp->readable()) {
            return _bufp->read();
        } else {
            return make_ready_future<std::experimental::optional<T>>();
        }
    }
    ~pipe_reader() {
        if (_bufp && _bufp->close_read()) {
            delete _bufp;
        }
    }
    // Allow move, but not copy, of pipe_reader
    pipe_reader(pipe_reader&& other) : _bufp(other._bufp) {
        other._bufp = nullptr;
    }
    pipe_reader& operator=(pipe_reader&& other) {
        std::swap(_bufp, other._bufp);
    }
};

/// \brief Write side of a \ref seastar::pipe
///
/// The write side of a pipe, which allows only writing to the pipe.
/// A pipe_writer object cannot be created separately, but only as part of a
/// reader/writer pair through \ref seastar::pipe.
template <typename T>
class pipe_writer {
private:
    internal::pipe_buffer<T> *_bufp;
    pipe_writer(internal::pipe_buffer<T> *bufp) : _bufp(bufp) { }
    friend class pipe<T>;
public:
    /// \brief Write an item to the pipe
    ///
    /// Returns a future value, which is fulfilled when the data was written
    /// to the buffer (when it become non-full). If the data could not be
    /// written because the read side was closed, an exception
    /// \ref broken_pipe_exception is returned in the future.
    future<> write(T&& data) {
        if (_bufp->writeable()) {
            return _bufp->write(std::move(data));
        } else {
            return make_exception_future<>(broken_pipe_exception());
        }
    }
    ~pipe_writer() {
        if (_bufp && _bufp->close_write()) {
            delete _bufp;
        }
    }
    // Allow move, but not copy, of pipe_writer
    pipe_writer(pipe_writer&& other) : _bufp(other._bufp) {
        other._bufp = nullptr;
    }
    pipe_writer& operator=(pipe_writer&& other) {
        std::swap(_bufp, other._bufp);
    }
};

/// \brief A fixed-size pipe for communicating between two fibers.
///
/// A pipe<T> is a mechanism to transfer data between two fibers, one
/// producing data, and the other consuming it. The fixed-size buffer also
/// ensures a balanced execution of the two fibers, because the producer
/// fiber blocks when it writes to a full pipe, until the consumer fiber gets
/// to run and read from the pipe.
///
/// A pipe<T> resembles a Unix pipe, in that it has a read side, a write side,
/// and a fixed-sized buffer between them, and supports either end to be closed
/// independently (and EOF or broken pipe when using the other side).
/// A pipe<T> object holds the reader and write sides of the pipe as two
/// separate objects. These objects can be moved into two different fibers.
/// Importantly, if one of the pipe ends is destroyed (i.e., the continuations
/// capturing it end), the other end of the pipe will stop blocking, so the
/// other fiber will not hang.
///
/// The pipe's read and write interfaces are future-based blocking. I.e., the
/// write() and read() methods return a future which is fulfilled when the
/// operation is complete. The pipe is single-reader single-writer, meaning
/// that until the future returned by read() is fulfilled, read() must not be
/// called again (and same for write).
///
/// Note: The pipe reader and writer are movable, but *not* copyable. It is
/// often convenient to wrap each end in a shared pointer, so it can be
/// copied (e.g., used in an std::function which needs to be copyable) or
/// easily captured into multiple continuations.
template <typename T>
class pipe {
public:
    pipe_reader<T> reader;
    pipe_writer<T> writer;
    explicit pipe(size_t size) : pipe(new internal::pipe_buffer<T>(size)) { }
private:
    pipe(internal::pipe_buffer<T> *bufp) : reader(bufp), writer(bufp) { }
};


/// @}

} // namespace seastar
