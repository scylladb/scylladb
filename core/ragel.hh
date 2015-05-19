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

#ifndef RAGEL_HH_
#define RAGEL_HH_

#include "sstring.hh"
#include "temporary_buffer.hh"
#include "util/eclipse.hh"
#include <algorithm>
#include <memory>
#include <cassert>
#include <experimental/optional>
#include "future.hh"

// Support classes for Ragel parsers

// Builds an sstring that can be scattered across multiple packets.
//
// Use a sstring_build::guard variable to designate each scattered
// char array, and call mark_start() and mark_end() at the start
// and end points, respectively.  sstring_builder will collect data
// from intervening segments, if needed.
//
// After mark_end() has been called, use the get() method to obtain
// the built string.
//
// FIXME: switch to string_view.
//
class sstring_builder {
    sstring _value;
    const char* _start = nullptr;
public:
    class guard;
public:
    sstring get() && {
        return std::move(_value);
    }
    void reset() {
        _value.reset();
        _start = nullptr;
    }
    friend class guard;
};

class sstring_builder::guard {
    sstring_builder& _builder;
    const char* _block_end;
public:
    guard(sstring_builder& builder, const char* block_start, const char* block_end)
        : _builder(builder), _block_end(block_end) {
        if (!_builder._value.empty()) {
            mark_start(block_start);
        }
    }
    ~guard() {
        if (_builder._start) {
            mark_end(_block_end);
        }
    }
    void mark_start(const char* p) {
        _builder._start = p;
    }
    void mark_end(const char* p) {
        if (_builder._value.empty()) {
            // avoid an allocation in the common case
            _builder._value = sstring(_builder._start, p);
        } else {
            _builder._value += sstring(_builder._start, p);
        }
        _builder._start = nullptr;
    }
};


// CRTP
template <typename ConcreteParser>
class ragel_parser_base {
protected:
    int _fsm_cs;
    std::unique_ptr<int[]> _fsm_stack = nullptr;
    int _fsm_stack_size = 0;
    int _fsm_top;
    int _fsm_act;
    char* _fsm_ts;
    char* _fsm_te;
    sstring_builder _builder;
protected:
    void init_base() {
        _builder.reset();
    }
    void prepush() {
        if (_fsm_top == _fsm_stack_size) {
            auto old = _fsm_stack_size;
            _fsm_stack_size = std::max(_fsm_stack_size * 2, 16);
            assert(_fsm_stack_size > old);
            std::unique_ptr<int[]> new_stack{new int[_fsm_stack_size]};
            std::copy(_fsm_stack.get(), _fsm_stack.get() + _fsm_top, new_stack.get());
            std::swap(_fsm_stack, new_stack);
        }
    }
    void postpop() {}
    sstring get_str() {
        auto s = std::move(_builder).get();
        return std::move(s);
    }
public:
    using unconsumed_remainder = std::experimental::optional<temporary_buffer<char>>;
    future<unconsumed_remainder> operator()(temporary_buffer<char> buf) {
        char* p = buf.get_write();
        char* pe = p + buf.size();
        char* eof = buf.empty() ? pe : nullptr;
        char* parsed = static_cast<ConcreteParser*>(this)->parse(p, pe, eof);
        if (parsed) {
            buf.trim_front(parsed - p);
            return make_ready_future<unconsumed_remainder>(std::move(buf));
        }
        return make_ready_future<unconsumed_remainder>();
    }
};

#endif /* RAGEL_HH_ */
