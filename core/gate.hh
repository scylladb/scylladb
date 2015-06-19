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
 * Copyright 2014 Cloudius Systems
 */

#pragma once

#include "future.hh"
#include <experimental/optional>
#include <exception>

namespace seastar {

namespace stdx = std::experimental;

/// Exception thrown when a \ref gate object has been closed
/// by the \ref gate::close() method.
class gate_closed_exception : public std::exception {
    virtual const char* what() const noexcept override {
        return "gate closed";
    }
};

/// Facility to stop new requests, and to tell when existing requests are done.
///
/// When stopping a service that serves asynchronous requests, we are faced with
/// two problems: preventing new requests from coming in, and knowing when existing
/// requests have completed.  The \c gate class provides a solution.
class gate {
    size_t _count = 0;
    stdx::optional<promise<>> _stopped;
public:
    /// Registers an in-progress request.
    ///
    /// If the gate is not closed, the request is registered.  Othewise,
    /// a \ref gate_closed_exception is thrown.
    void enter() {
        if (_stopped) {
            throw gate_closed_exception();
        }
        ++_count;
    }
    /// Unregisters an in-progress request.
    ///
    /// If the gate is closed, and there are no more in-progress requests,
    /// the \ref closed() promise will be fulfilled.
    void leave() {
        --_count;
        if (!_count && _stopped) {
            _stopped->set_value();
        }
    }
    /// Closes the gate.
    ///
    /// Future calls to \ref enter() will fail with an exception, and when
    /// all current requests call \ref leave(), the returned future will be
    /// made ready.
    future<> close() {
        _stopped = stdx::make_optional(promise<>());
        if (!_count) {
            _stopped->set_value();
        }
        return _stopped->get_future();
    }
};

}
