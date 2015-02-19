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
#ifndef _PACKET_DATA_SOURCE_HH
#define _PACKET_DATA_SOURCE_HH

#include "core/reactor.hh"
#include "net/packet.hh"

namespace net {

class packet_data_source final : public data_source_impl {
    size_t _cur_frag = 0;
    packet _p;
public:
    explicit packet_data_source(net::packet&& p)
        : _p(std::move(p))
    {}

    virtual future<temporary_buffer<char>> get() override {
        if (_cur_frag != _p.nr_frags()) {
            auto& f = _p.fragments()[_cur_frag++];
            return make_ready_future<temporary_buffer<char>>(
                    temporary_buffer<char>(f.base, f.size,
                            make_deleter(deleter(), [p = _p.share()] () mutable {})));
        }
        return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>());
    }
};

static inline
input_stream<char> as_input_stream(packet&& p) {
    return input_stream<char>(data_source(std::make_unique<packet_data_source>(std::move(p))));
}

}

#endif
