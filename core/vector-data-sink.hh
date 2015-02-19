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

#ifndef VECTOR_DATA_SINK_HH_
#define VECTOR_DATA_SINK_HH_

#include "core/reactor.hh"

class vector_data_sink final : public data_sink_impl {
public:
    using vector_type = std::vector<net::packet>;
private:
    vector_type& _v;
public:
    vector_data_sink(vector_type& v) : _v(v) {}

    virtual future<> put(net::packet p) override {
        _v.push_back(std::move(p));
        return make_ready_future<>();
    }

    virtual future<> close() override {
        // TODO: close on local side
        return make_ready_future<>();
    }
};

#endif
