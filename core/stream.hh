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

#ifndef STREAM_HH_
#define STREAM_HH_

#include "future.hh"
#include <exception>
#include <cassert>

// A stream/subscription pair is similar to a promise/future pair,
// but apply to a sequence of values instead of a single value.
//
// A stream<> is the producer side.  It may call produce() as long
// as the future<> returned from the previous invocation is ready.
// To signify no more data is available, call close().
//
// A subscription<> is the consumer side.  It is created by a call
// to stream::listen().  Calling subscription::start(),
// which registers the data processing callback, starts processing
// events.  It may register for end-of-stream notifications by
// chaining the when_done() future, which also delivers error
// events (as exceptions).
//
// The consumer can pause generation of new data by returning
// a non-ready future; when the future becomes ready, the producer
// will resume processing.

template <typename... T>
class stream;

template <typename... T>
class subscription;

template <typename... T>
class stream {
    subscription<T...>* _sub = nullptr;
    promise<> _done;
    promise<> _ready;
public:
    using next_fn = std::function<future<> (T...)>;
    stream() = default;
    stream(const stream&) = delete;
    stream(stream&&) = delete;
    ~stream();
    void operator=(const stream&) = delete;
    void operator=(stream&&) = delete;

    // Returns a subscription that reads value from this
    // stream.
    subscription<T...> listen();

    // Returns a subscription that reads value from this
    // stream, and also sets up the listen function.
    subscription<T...> listen(next_fn next);

    // Becomes ready when the listener is ready to accept
    // values.  Call only once, when beginning to produce
    // values.
    future<> started();

    // Produce a value.  Call only after started(), and after
    // a previous produce() is ready.
    future<> produce(T... data);

    // End the stream.   Call only after started(), and after
    // a previous produce() is ready.  No functions may be called
    // after this.
    void close();

    // Signal an error.   Call only after started(), and after
    // a previous produce() is ready.  No functions may be called
    // after this.
    template <typename E>
    void set_exception(E ex);
private:
    void pause(future<> can_continue);
    void start();
    friend class subscription<T...>;
};

template <typename... T>
class subscription {
public:
    using next_fn = typename stream<T...>::next_fn;
private:
    stream<T...>* _stream;
    next_fn _next;
private:
    explicit subscription(stream<T...>* s);
public:
    subscription(subscription&& x);
    ~subscription();

    /// \brief Start receiving events from the stream.
    ///
    /// \param next Callback to call for each event
    void start(std::function<future<> (T...)> next);

    // Becomes ready when the stream is empty, or when an error
    // happens (in that case, an exception is held).
    future<> done();

    friend class stream<T...>;
};


template <typename... T>
inline
stream<T...>::~stream() {
    if (_sub) {
        _sub->_stream = nullptr;
    }
}

template <typename... T>
inline
subscription<T...>
stream<T...>::listen() {
    return subscription<T...>(this);
}

template <typename... T>
inline
subscription<T...>
stream<T...>::listen(next_fn next) {
    auto sub = subscription<T...>(this);
    sub.start(std::move(next));
    return sub;
}

template <typename... T>
inline
future<>
stream<T...>::started() {
    return _ready.get_future();
}

template <typename... T>
inline
future<>
stream<T...>::produce(T... data) {
    try {
        return _sub->_next(std::move(data)...);
    } catch (...) {
        _done.set_exception(std::current_exception());
        // FIXME: tell the producer to stop producing
        abort();
    }
}

template <typename... T>
inline
void
stream<T...>::close() {
    _done.set_value();
}

template <typename... T>
template <typename E>
inline
void
stream<T...>::set_exception(E ex) {
    _sub->_done.set_exception(ex);
}

template <typename... T>
inline
subscription<T...>::subscription(stream<T...>* s)
        : _stream(s) {
    assert(!_stream->_sub);
    _stream->_sub = this;
}

template <typename... T>
inline
void
subscription<T...>::start(std::function<future<> (T...)> next) {
    _next = std::move(next);
    _stream->_ready.set_value();
}

template <typename... T>
inline
subscription<T...>::~subscription() {
    if (_stream) {
        _stream->_sub = nullptr;
    }
}

template <typename... T>
inline
subscription<T...>::subscription(subscription&& x)
    : _stream(x._stream), _next(std::move(x._next)) {
    x._stream = nullptr;
    if (_stream) {
        _stream->_sub = this;
    }
}

template <typename... T>
inline
future<>
subscription<T...>::done() {
    return _stream->_done.get_future();
}

#endif /* STREAM_HH_ */
