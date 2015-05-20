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

#include <unordered_map>
#include "core/future.hh"
#include "net/api.hh"
#include "core/reactor.hh"
#include "core/iostream.hh"
#include "core/shared_ptr.hh"

namespace rpc {

using id_type = int64_t;

struct SerializerConcept {
    template<typename T>
    future<> operator()(output_stream<char>& out, T&& v);
    template<typename T>
    future<> operator()(input_stream<char>& in, T& v);
    // id_type and sstring are needed for compilation to succeed
    future<> operator()(output_stream<char>& out, id_type& v);
    future<> operator()(input_stream<char>& in, id_type& v);
    future<> operator()(output_stream<char>& out, sstring& v);
    future<> operator()(input_stream<char>& in, sstring& v);
};

struct client_info {
    socket_address addr;
};

// MsgType is a type that holds type of a message. The type should be hashable
// and serializable. It is preferable to use enum for message types, but
// do not forget to provide hash function for it
template<typename Serializer, typename MsgType = uint32_t>
class protocol {
    class connection {
    protected:
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        future<> _output_ready = make_ready_future<>();
        bool _error = false;
        protocol& _proto;
    public:
        connection(connected_socket&& fd, protocol& proto) : _fd(std::move(fd)), _read_buf(_fd.input()), _write_buf(_fd.output()), _proto(proto) {}
        connection(protocol& proto) : _proto(proto) {}
        // functions below are public because they are used by external heavily templated functions
        // and I am not smart enough to know how to define them as friends
        auto& in() { return _read_buf; }
        auto& out() { return _write_buf; }
        auto& out_ready() { return _output_ready; }
        bool error() { return _error; }
        auto& serializer() { return _proto._serializer; }
        auto& get_protocol() { return _proto; }
    };
    friend connection;

public:
    class server {
    private:
        protocol& _proto;
    public:
        class connection : public protocol::connection, public enable_lw_shared_from_this<connection> {
            server& _server;
            MsgType _type;
            client_info _info;
        public:
            connection(server& s, connected_socket&& fd, socket_address&& addr, protocol& proto);
            future<> process();
            auto& info() { return _info; }
        };
        server(protocol& proto, ipv4_addr addr);
        void accept(server_socket&& ss);
        friend connection;
    };

    class client : public protocol::connection {
        promise<> _connected;
        id_type _message_id = 1;
        id_type _rcv_msg_id = 0;
        struct reply_handler_base {
            virtual future<> operator()(client&, id_type) = 0;
            virtual ~reply_handler_base() {};
        };
    public:
        struct stats {
            using counter_type = uint64_t;
            counter_type replied = 0;
            counter_type pending = 0;
            counter_type exception_received = 0;
            counter_type sent_messages = 0;
            counter_type wait_reply = 0;
        };

        template<typename Reply, typename Func>
        struct reply_handler final : reply_handler_base {
            Func func;
            Reply reply;
            reply_handler(Func&& f) : func(std::move(f)) {}
            virtual future<> operator()(client& client, id_type msg_id) override {
                return func(reply, client, msg_id);
            }
            virtual ~reply_handler() {}
        };
    private:
        std::unordered_map<id_type, std::unique_ptr<reply_handler_base>> _outstanding;
        stats _stats;
    public:
        client(protocol& proto, ipv4_addr addr, ipv4_addr local = ipv4_addr());

        stats get_stats() const {
            stats res = _stats;
            res.wait_reply = _outstanding.size();
            return res;
        }

        stats& get_stats_internal() {
            return _stats;
        }
        auto next_message_id() { return _message_id++; }
        void wait_for_reply(id_type id, std::unique_ptr<reply_handler_base>&& h) {
            _outstanding.emplace(id, std::move(h));
        }
    };
    friend server;
private:
    using rpc_handler = std::function<future<>(lw_shared_ptr<typename server::connection>)>;
    std::unordered_map<MsgType, rpc_handler> _handlers;
    Serializer _serializer;
    std::function<void(const sstring&)> _logger;
public:
    protocol(Serializer&& serializer) : _serializer(std::forward<Serializer>(serializer)) {}
    template<typename Func>
    auto make_client(MsgType t);

    // returns a function which type depends on Func
    // if Func == Ret(Args...) then return function is
    // future<Ret>(protocol::client&, Args...)
    template<typename Func>
    auto register_handler(MsgType t, Func&& func);

    void set_logger(std::function<void(const sstring&)> logger) {
        _logger = logger;
    }

    void log(const sstring& str) {
        if (_logger) {
            _logger(str);
            _logger("\n");
        }
    }

    void log(const client_info& info, id_type msg_id, const sstring& str) {
        log(to_sstring("client ") + inet_ntoa(info.addr.as_posix_sockaddr_in().sin_addr) + " msg_id " + to_sstring(msg_id) + ": " + str);
    }
private:
    void register_receiver(MsgType t, rpc_handler&& handler) {
        _handlers.emplace(t, std::move(handler));
    }
};

class error : public std::runtime_error {
public:
    error(const std::string& msg) : std::runtime_error(msg) {}
};

class closed_error : public error {
public:
    closed_error() : error("connection is closed") {}
};

struct no_wait_type {};

// return this from a callback if client does not want to waiting for a reply
extern no_wait_type no_wait;
}

#include "rpc_impl.hh"
